# octo-message-export-api 对接规范 v0.1

> 本文为 caller 实现 client 的接口文档。
> Base URL：由部署方配置（默认监听 `:8080`，路径前缀 `/v1`）
> 状态：草案，与协议同步迭代

---

## 1. 鉴权与公共 Header

请求 header：

| Header | 必填 | 取值 | 说明 |
|---|---|---|---|
| `Authorization` | ✅ | `Bearer <s2s_token>` | 固定长效 s2s token；由部署方带外配置 caller→token 映射，不过期、不刷新；轮换走运维变更流程 |
| `X-Request-Id` | ⚠ 可选 | UUID v4 | 全链路追踪 id；client 不传时 server 自动生成 UUID；server **一律在响应头中回写** `X-Request-Id`（无论是 client 传的还是 server 生成的），便于排障 |
| `Content-Type` | ✅（POST） | `application/json` | — |

**鉴权模型：服务间互信 + 固定长效 token**。本服务在配置中预加载一份 caller→token 映射表（带外配置，运维负责轮换），收到请求时查 `Authorization` 头中的 token 是否命中表，命中即认定 caller 身份并通过反查得到 caller 名（用于审计透传与日志/指标 label）；token 由 server 在内存中按明文比对 caller_token_map。token 由 caller 写在自身配置/环境变量中。caller（示例：`smart-summary`，可换成任意字符串）必须在自己的业务层完成用户级 access control（例如在权限校验阶段过滤掉调用方用户不可见的 channel）后再调本接口。

---

## 2. POST /v1/messages/batch — 提交任务

### 2.1 Request Body

| 字段 | 类型 | 必填 | 默认 | 约束 | 说明 |
|---|---|---|---|---|---|
| `scope.channels` | array | ✅ | — | 1 ≤ len ≤ 30 | 待查询 channel 列表 |
| `scope.channels[].channel_id` | string | ✅ | — | 见 §6.1 | 以 octo-search indexer 实际写入口径为准 |
| `time_range.start_ts` | int64 | ✅ | — | 秒级 unix ts | 含端点 |
| `time_range.end_ts` | int64 | ✅ | — | end - start ≤ 31 天 | 含端点 |

### 2.2 Response（HTTP 202）

所有提交统一走异步路径，结果通过 S3 NDJSON parts 返回。不存在 inline messages 路径。

```json
{
  "task_id": "ost_01HXYZ...",
  "request_id": "..."
}
```

### 2.3 示例

Request：

```json
POST /v1/messages/batch
Authorization: Bearer <s2s_token>
X-Request-Id: 5f1d-...        # 可选；不传则 server 自动生成

{
  "scope": {"channels": [{"channel_id": "<by-indexer-format>"}]},
  "time_range": {"start_ts": 1717862400, "end_ts": 1717948800}
}
```

Response 见 §2.2。Client 按 `<提交路径>/{task_id}`（即 `/v1/messages/batch/{task_id}`）轮询，建议间隔 5s，总时长不超过 §4 SLA。

---

## 3. GET /v1/messages/batch/{task_id} — 查询状态

### 3.1 Path & Header

| 位置 | 字段 | 说明 |
|---|---|---|
| path | `task_id` | §2.3 返回的 task_id |
| header | `Authorization` / `X-Request-Id`（可选） | 同 §1 |

### 3.2 Response（HTTP 200）

| 字段 | 类型 | 出现条件 | 说明 |
|---|---|---|---|
| `status` | string | always | `queued` / `running` / `completed` / `failed` / `cancelled` / `partial` |
| `actual_count` | int64 | completed/partial | 实际返回条数 |
| `parts` | array | completed/partial | NDJSON 分片，见 §5 |
| `parts[].url` | string | — | S3 presigned GET URL |
| `parts[].size_bytes` | int64 | — | gzip 后字节数 |
| `parts[].message_count` | int | — | 该 part 行数 |
| `parts[].sha256` | string | — | gzip 内容 sha256（十六进制小写） |
| `parts[].expires_at` | int64 | — | URL 失效时间（秒级 unix ts，建议 ≥ 30min） |
| `parts[].channel_ids` | []string | — | 该 part 覆盖的 channel id；同 channel 不跨 part |
| `warnings` | array | partial/completed | 见下 |
| `warnings[].channel_id` | string | — | 受影响 channel |
| `warnings[].code` | string | — | `per_channel_truncated` / `total_truncated` 等 |
| `warnings[].limit` | int | — | 截断阈值 |
| `warnings[].actual_seen` | int | — | 实际命中数（截断前） |
| `error_code` | string | failed | 见 §7 |
| `error_message` | string | failed | 用户可读，不泄漏内部 |

> 截断丢弃端：单 channel 命中超过 `per_channel_truncated` 阈值时，**保留最新 N 条、丢弃较老的部分**（与 §5.2 排序口径一致，按 `message_seq` 降序保留最新优先）。消费方据此知道被丢弃的是较早的消息。

### 3.3 状态机

```
queued ──► running ──► completed
              │   └──► partial
              ├──► failed
              └──► cancelled
```

终态：completed / partial / failed / cancelled，状态机终止。Parts URL 在 `expires_at` 前可重复 GET。

---

## 4. DELETE /v1/messages/batch/{task_id} — 取消

幂等。对已终态 task 返回 200 + 当前 status。

| HTTP | 含义 |
|---|---|
| 200 | 取消请求已接受（已终态则原样返回） |
| 404 | task 不存在或非本 caller 创建 |

Response：

```json
{"task_id": "ost_01HXYZ...", "status": "cancelled", "request_id": "..."}
```

---

## 5. NDJSON Part 格式

每个 `parts[].url` 返回一个 gzip 流，解压后每行一条 JSON 消息，行尾 `\n`。

### 5.1 行 Schema

**字段列表按下表约定**，每行固定包含以下 5 个字段；server 始终返回全集，不支持 caller 投影。

| 字段 | 类型 | 必填 | MySQL 对应列 | 业务含义 |
|---|---|---|---|---|
| `message_seq` | int64 | ✅ | `message_seq` | 频道内严格递增的消息序号；同一 channel 内 strictly monotonic，可用作增量定位 / 分页游标；不同 channel 之间不可比 |
| `from_uid` | string | ✅ | `from_uid` | 消息发送方的用户 uid（IM 系统全局唯一）；caller 通常要再调 IM DB / octo-server 反查显示名 |
| `channel_id` | string | ✅ | `channel_id` | 消息所在频道 id；具体规范化口径见 §6.1（DM 与子区拼接规则由 upstream indexer 维护者确认） |
| `timestamp` | int64 | ✅ | `timestamp` | 消息发送的服务端时间戳，秒级 unix ts |
| `payload` | string | ✅ | `payload`（已解析） | 消息正文；**octo-search 已在 indexer 侧将原始 JSON payload 解析为纯文本**：type=1 (text) 直接取 content；type=14 (RichText 图文混排) 按 block 顺序拼接 text 块，图片块占位 `[图片]`；其它 type 返回空串（caller 据此跳过 LLM 处理）；caller 无需再做 JSON 解析、type 分支、占位符替换 |

### 5.2 分片约束

| 约束 | 数值 |
|---|---|
| 单 part 解压后 | ≤ 500MB |
| 单 part 行数 | ≤ 30000 |
| 同一 channel 不跨 part | ✅ 强约束 |
| part 内排序 | 按 `message_seq` 降序（保留最新优先）；消费方如需升序自行排序 |
| 多 part 间排序 | parts[] 仍按 `channel_id` 字典序排 |

part 大小由 (解压后字节数 ≤ 500MB, 行数 ≤ 30000) 双计数触发 rollover；gzip 是落地编码方式，最终 part 文件大小（即 `size_bytes`）受压缩比影响，仅作观察值不参与切片决策。

### 5.3 完整性校验

Client 必须用 `sha256` 校验 gzip 内容；不匹配视为 part 损坏，可重试 GET 同 URL 最多 2 次,仍失败上报 warning 并跳过该 part。

---

## 6. 字段口径强约束

### 6.1 channel_id 规范化

**口径以上游 indexer 实际写入口径为准**。具体的 DM / 群（group）/ 子区（thread）三种 channel_type 的拼接规则，由部署方与 upstream indexer 维护者对齐并书面文档化。

**强约束**：请与 upstream indexer 维护者确认 channel_id 规范化口径；caller 必须按该口径转换后再传入 `scope.channels`。任何口径变更必须先在该文档落字。

handler 不对 channel_id 做格式校验（service-to-service trust）。若 caller 传入与 indexer 写入口径不一致的 channel_id，会得到空结果而非 4xx 错误，请 caller 在调用前自行确保口径一致。

### 6.2 payload 字段处理规则

| 输入 | 输出 `payload` |
|---|---|
| type=1（纯文本） | content 原样 |
| type=14（富文本），含 text+image 多 block | block 顺序拼接；image block 用 `[图片]` 占位 |
| type=14，仅 image | `[图片]` |
| type=14，top-level `plain` 字段非空 | 直接用 `plain` |
| 其它 type | 该消息整条不索引（不会出现在结果中） |

### 6.3 timestamp 单位

秒级 unix ts；毫秒值视为非法（HTTP 400 `invalid_request`）。

---

## 7. 错误码

| HTTP | error_code | 含义 | Client 推荐行为 |
|---|---|---|---|
| 400 | `invalid_request` | 字段格式/类型错 | fail-fast，修正请求 |
| 401 | `unauthorized` | s2s token 不在白名单 / token 错误 / 缺失 Authorization header | 检查本地 token 配置；token 已轮换需联系 octo-search 运维同步 |
| 413 | `single_task_too_large` | scope.channels > 30 或预估总命中 > 300000（单 task 上限，系统保护参数） | fail-fast；上层缩窗或拆任务 |
| 422 | `time_range_too_long` | end_ts - start_ts > 31 天 | fail-fast |
| 500 | `internal_error` | server 内部错 | 退避重试 ≤ 3 次；带原 X-Request-Id |
| 503 | `service_degraded` | server 过载 / 全局并发上限排队超时 / OS/S3 依赖降级 | 退避重试 ≤ 3 次（间隔 ≥ 10s） |

所有 task 均为异步路径，failed status 的 `error_code` 沿用错误码表中 5xx 类（提交期 4xx 错误在 POST 同步返回，不进入异步 failed 状态）。

---

## 8. 系统保护参数

当前示例部署仅有一个 caller，服务间互信，**不做按 caller 维度的配额**。仅保留两项系统级保护参数，用于防止单 task 拖垮服务 / 防止全局雪崩：

| 维度 | 阈值 | 触发后 |
|---|---|---|
| 单 task 命中上限 | 300000 | 提交时 413 `single_task_too_large` |
| 全局并发 executor 上限 | 50（建议固定值，按机器规格可调） | 先排队等待；排队超 N 秒仍无空闲 worker → 503 `service_degraded` |

未来引入多 caller 时，再单独设计 per-caller 配额方案；本版本不预留相关字段。

---

## 9. Client 集成 checklist（9 项）

实施前对照确认：

1. [ ] s2s token 注入：从配置/环境变量读取固定长效 token，写入 `Authorization: Bearer <token>` 头；token 不刷新；401 视为配置错误或运维侧轮换未同步，直接 fail 并告警。
2. [ ] X-Request-Id：建议每次请求生成 UUID v4 并落业务日志（poll 沿用同一 id 便于排障）；若不传，server 会自动生成；server 一律在响应头中回写 `X-Request-Id`。
3. [ ] channel_id 口径对齐：调用前必须按 **upstream indexer 实际写入口径**规范化所有 channel_id（DM / group / thread 三种 channel_type 各自的拼接规则）。该口径以部署方与 upstream indexer 维护者书面确认为准，上线前完成口径对账；handler 不校验格式，传错只会得空结果。
4. [ ] 异步流程：所有提交均返回 202 + `task_id`，必须实现 poll + 下载 S3 parts 完整流程；poll 路径由 client 按 `<提交路径>/{task_id}`（即 `/v1/messages/batch/{task_id}`）自行拼接；不存在 inline messages 路径。
5. [ ] poll 间隔与上限：建议 5s 起步、20min 兜底；超时主动 DELETE。
6. [ ] DELETE 兜底：context cancel / 主动取消时必发 DELETE（独立 5s timeout，失败仅 warning）。
7. [ ] part 下载并发：建议 4，按内存预算调；每 part `sha256` 必须校验。
8. [ ] partial 处理：`status=partial` 也下载 parts；`warnings[]` 必须透传到上游业务（否则用户感知不到截断）。
9. [ ] 系统保护响应：413 `single_task_too_large` 直接 fail-fast 并把缩窗/拆 task 责任交回业务层；503 `service_degraded` 按 §7 退避重试。

---

## 附录 A：payload_type 枚举

| type | 含义 | 进入索引 |
|---|---|---|
| 1 | 纯文本 | ✅ |
| 14 | 富文本（text/image/at 混合） | ✅ |
| 其它 | 系统消息 / 卡片 / 文件 等 | ❌（indexer 侧丢弃） |

附录 A 列出 indexer 处理 payload 时按 type 分支的约定（caller 不需感知；§5.1 行 schema 已是 caller 可见的完整字段集）。

如需扩展索引范围，请走协议变更流程，本文不做扩展定义。

---

## 附录 B：协议版本

- v0.1：初版。
- 字段新增：minor 升版（v0.2 …），向后兼容（默认值不变）。
- 字段语义变更 / 删除：major 升版。
- 版本号通过 response header `X-OctoSearch-Proto-Version` 返回。

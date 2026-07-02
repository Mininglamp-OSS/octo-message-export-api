package result

import (
	"encoding/json"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
)

// extractPayload 把 OS doc 的 payload object 提取为对外的纯文本字符串。
//
// 规则（api-spec §6.2 / dev-guide §6.2，以 indexer 实际写入口径为准）：
//   - type==1（纯文本）：取 payload.text.content；缺失时 fallback payloadRaw.content
//   - type==14（富文本）：取 payload.richText.searchText —— indexer(buildRichText) 已把富文本
//     收敛成拼好的纯文本（plain + 各 image/file block name/caption，image 已占位 "[图片]"），
//     batch 直接取，不再自己拼 block；空富文本 indexer 不挂 richText → 返回空串仍算命中。
//   - 其它 type：整条消息不写入（返回 ok=false）
//
// 返回 (text, ok)。ok=false 表示该消息应被跳过（不写 NDJSON）。
// 解析异常一律按"跳过 + 记 payload_parse_skip_total"处理，绝不 panic。
func extractPayload(payload, payloadRaw map[string]any) (text string, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PayloadParseSkip.Inc()
			text, ok = "", false
		}
	}()

	if payload == nil {
		metrics.PayloadParseSkip.Inc()
		return "", false
	}

	switch toInt(payload["type"]) {
	case 1:
		if c := nestedContent(payload); c != "" {
			return c, true
		}
		// fallback：type=1 时 payloadRaw 上常带 content（实测）。
		if payloadRaw != nil {
			if c := getStr(payloadRaw, "content"); c != "" {
				return c, true
			}
		}
		// 纯文本但内容为空：仍算 type=1 命中，返回空串（caller 据空串跳过下游）。
		return "", true

	case 14:
		if rt, ok := payload["richText"].(map[string]any); ok {
			if st := getStr(rt, "searchText"); st != "" {
				return st, true
			}
		}
		// searchText 为空（indexer 对空富文本返 nil → payload 无 richText）：
		// 仍算 type=14 命中，返回空串（caller 据空串跳过下游），与 type=1 空内容口径一致。
		return "", true

	default:
		// 其它 type：整条不索引。
		metrics.PayloadParseSkip.Inc()
		return "", false
	}
}

// nestedContent 取 payload.text.content。
func nestedContent(payload map[string]any) string {
	if t, ok := payload["text"].(map[string]any); ok {
		return getStr(t, "content")
	}
	return ""
}

func getStr(m map[string]any, key string) string {
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

func toInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

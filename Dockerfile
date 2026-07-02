# octo-message-export-api Go 单镜像构建（多阶段）
# 范式参考 octo-search：CGO_ENABLED=0 静态二进制 + scratch runtime。
#

ARG GOLANG_IMAGE=golang:1.25.3

# ---------- build ----------
FROM ${GOLANG_IMAGE} AS builder

ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG GIT_TAG=
ARG BUILD_DATE=unknown

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY=https://goproxy.cn,direct \
    GO111MODULE=on

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download || true

COPY . .

RUN mkdir -p /out && \
    go build -trimpath \
      -ldflags "-s -w \
        -X 'main.GitCommit=${GIT_COMMIT}' \
        -X 'main.GitBranch=${GIT_BRANCH}' \
        -X 'main.GitTag=${GIT_TAG}' \
        -X 'main.BuildDate=${BUILD_DATE}'" \
      -o /out/octo-message-export-api ./cmd/octo-message-export-api

# ---------- runtime ----------
# scratch: 零基镜像。K8s 已配 runAsNonRoot + readOnlyRootFilesystem。
# 用数字 UID 65532（非 root）。健康检查交给 K8s probe。
FROM scratch

ENV TZ=Asia/Shanghai \
    HTTP_ADDR=:8080

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /out/octo-message-export-api /octo-message-export-api

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/octo-message-export-api"]

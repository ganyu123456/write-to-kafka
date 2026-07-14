# ─────────────────────────────────────────────
# Stage 1: Build
# ─────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /build

# 安装 git（go mod download 需要）
RUN apk add --no-cache git ca-certificates

# 先复制依赖文件，利用 Docker 层缓存
COPY go.mod go.sum ./
RUN go mod download

# 再复制源码并编译
COPY . .
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w" \
    -o /out/write-to-kafka .

# ─────────────────────────────────────────────
# Stage 2: Runtime
# 使用 alpine 最小镜像
# ─────────────────────────────────────────────
FROM alpine:3.20

WORKDIR /app

# 安装调试工具、CA 证书
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    curl \
    bind-tools \
    busybox-extras

ENV TZ=Asia/Shanghai

# 复制编译产物
COPY --from=builder /out/write-to-kafka .

# 健康检查：检测进程是否存活
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep write-to-kafka || exit 1

ENTRYPOINT ["/app/write-to-kafka"]

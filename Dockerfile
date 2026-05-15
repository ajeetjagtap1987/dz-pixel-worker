# syntax=docker/dockerfile:1.6

# ============ Build stage ============
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copy module files first for better layer caching
COPY go.mod go.sum* ./
RUN go mod download && go mod tidy

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=$(cat /tmp/version 2>/dev/null || echo dev)" \
    -o /out/server .

# ============ Runtime stage ============
FROM alpine:3.20
RUN apk --no-cache add ca-certificates curl tzdata && \
    addgroup -S app && adduser -S app -G app

WORKDIR /app
COPY --from=builder /out/server /app/server

USER app
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/server"]

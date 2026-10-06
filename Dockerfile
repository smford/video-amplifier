# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Install build dependencies and certificates
RUN apk add --no-cache git ca-certificates tzdata

# Cache go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary with zero CGO
ARG VERSION=1.0.0
ARG COMMIT=docker
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.GitCommit=${COMMIT} -X main.BuildDate=$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    -o /build/video-amplifier \
    ./cmd/video-amplifier

# Final Minimal Runtime Stage
FROM alpine:3.21

# Install CA certificates for secure RTSP/HTTPS connections
RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/video-amplifier /app/video-amplifier
COPY --from=builder /build/config.example.yaml /app/config.example.yaml

USER 10001:10001

# Expose HTTP, RTSP, and Metrics ports
EXPOSE 8080 8554 9090

ENTRYPOINT ["/app/video-amplifier"]
CMD ["-config", "/app/config.yaml"]

# xx provides cross-compilation toolchains for CGO builds
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.6.1@sha256:923441d7c25f1e2eb5789f82d987693c47b8ed987c4ab3b075d6ed2b5d6779a3 AS xx

# Stage 1: Build binaries — pinned to BUILDPLATFORM so Go runs natively (fast)
FROM --platform=$BUILDPLATFORM golang:1.26.0-alpine3.23@sha256:d4c4845f5d60c6a974c6000ce58ae079328d03ab7f721a0734277e69905473e5 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG TARGETPLATFORM
ARG VERSION=0.0.0
ARG CHANNEL=dev

# Copy xx scripts for cross-compilation
COPY --from=xx / /

WORKDIR /app

# Install cross-compilation toolchain via xx
RUN apk add --no-cache clang21=21.1.2-r2 lld21=21.1.2-r1 && \
    xx-apk add --no-cache \
        gcc=15.2.0-r2 \
        g++=15.2.0-r2 \
        musl-dev=1.2.5-r23 \
        fuse-dev=2.9.9-r7

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x

COPY . .

# Build main binary — xx-go sets CC/CXX/GOOS/GOARCH automatically
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 \
    xx-go build -trimpath \
    -ldflags="-w -s -X github.com/sirrobot01/decypharr/pkg/version.Version=${VERSION} -X github.com/sirrobot01/decypharr/pkg/version.Channel=${CHANNEL}" \
    -o /decypharr && \
    xx-verify /decypharr

# Build healthcheck (no CGO needed, plain cross-compile)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-w -s" \
    -o /healthcheck cmd/healthcheck/main.go

# Stage 2: Final image
FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

ARG VERSION=0.0.0
ARG CHANNEL=dev

LABEL version="${VERSION}-${CHANNEL}"
LABEL org.opencontainers.image.source="https://github.com/sirrobot01/decypharr"
LABEL org.opencontainers.image.title="decypharr"
LABEL org.opencontainers.image.authors="sirrobot01"
LABEL org.opencontainers.image.documentation="https://github.com/sirrobot01/decypharr/blob/main/README.md"

# Install a repository-verified rclone plus both FUSE ABIs. cgofuse is built
# against FUSE2; the pure-Go/default and rclone paths use FUSE3.
RUN apk add --no-cache \
        ca-certificates=20260611-r0 \
        fuse=2.9.9-r7 \
        fuse3=3.17.3-r1 \
        rclone=1.72.1-r4 \
        shadow=4.18.0-r0 \
        su-exec=0.3-r0 \
        tzdata=2026c-r0 && \
    echo "user_allow_other" >> /etc/fuse.conf

# Copy binaries and entrypoint
COPY --from=builder /decypharr /usr/bin/decypharr
COPY --from=builder /healthcheck /usr/bin/healthcheck
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set environment variables
ENV PUID=1000
ENV PGID=1000
ENV LOG_PATH=/app/logs

EXPOSE 8282
VOLUME ["/app"]

HEALTHCHECK --interval=10s --retries=10 CMD ["/usr/bin/healthcheck", "--config", "/app"]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/bin/decypharr", "--config", "/app"]

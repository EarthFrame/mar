# syntax=docker/dockerfile:1.4
# Multi-stage, multi-platform Dockerfile for mar (Rust engine)
# Targets: linux/amd64, linux/arm64 (runs natively on Linux x86_64, Linux ARM64, and macOS Apple Silicon)

ARG RUST_VERSION=1.85-alpine

# Builder stage: statically compile mar binary with musl
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION} AS builder

RUN apk add --no-cache musl-dev

WORKDIR /workspace

# Copy workspace configuration and Rust source code
COPY Cargo.toml Cargo.lock* ./
COPY rust/ ./rust/

ARG TARGETARCH

# Build static binary targeting musl for the target architecture
RUN case "${TARGETARCH}" in \
        "amd64") \
            RUST_TARGET="x86_64-unknown-linux-musl" ;; \
        "arm64") \
            RUST_TARGET="aarch64-unknown-linux-musl" ;; \
        *) \
            echo "Unsupported target architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    rustup target add "${RUST_TARGET}" && \
    cargo build --release --target "${RUST_TARGET}" --manifest-path rust/Cargo.toml --bin mar && \
    cp "target/${RUST_TARGET}/release/mar" /mar && \
    chmod +x /mar

# Final stage: minimal, slim distroless image
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="mar" \
      org.opencontainers.image.description="High-performance archive utility (Rust engine)" \
      org.opencontainers.image.source="https://github.com/earthframe/mar" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /data

COPY --from=builder /mar /usr/local/bin/mar

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/mar"]
CMD ["--help"]

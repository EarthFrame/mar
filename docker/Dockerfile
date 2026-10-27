# syntax=docker/dockerfile:1.4
# Multi-stage, multi-platform Dockerfile for mar (Rust engine)
# Targets: linux/amd64, linux/arm64 (runs natively on Linux x86_64, Linux ARM64, and macOS Apple Silicon)

ARG RUST_VERSION=1.85-alpine

# Builder stage: statically compile mar binary with musl for target platform
FROM --platform=$TARGETPLATFORM rust:${RUST_VERSION} AS builder

RUN apk add --no-cache musl-dev

WORKDIR /workspace

# Copy workspace configuration and Rust source code
COPY Cargo.toml Cargo.lock* ./
COPY rust/ ./rust/
COPY python/ ./python/

ARG TARGETPLATFORM
ARG TARGETARCH

# Build static binary targeting musl for the target architecture
RUN ARCH="${TARGETARCH}"; \
    if [ -z "$ARCH" ]; then \
        case "$(uname -m)" in \
            x86_64) ARCH="amd64" ;; \
            aarch64|arm64) ARCH="arm64" ;; \
            *) echo "Unsupported uname arch: $(uname -m)" && exit 1 ;; \
        esac; \
    fi; \
    case "${ARCH}" in \
        "amd64") \
            RUST_TARGET="x86_64-unknown-linux-musl" ;; \
        "arm64") \
            RUST_TARGET="aarch64-unknown-linux-musl" ;; \
        *) \
            echo "Unsupported target architecture: ${ARCH}" && exit 1 ;; \
    esac && \
    rustup target add "${RUST_TARGET}" || true && \
    cargo build --release --target "${RUST_TARGET}" --manifest-path rust/Cargo.toml --bin mar && \
    (cp "target/${RUST_TARGET}/release/mar" /mar 2>/dev/null || cp "target/release/mar" /mar) && \
    chmod +x /mar

# Final stage: Debian-based runtime with POSIX shell and procps (compatible with Nextflow and task runners)
FROM --platform=$TARGETPLATFORM debian:bookworm-slim

ARG VERSION=0.3.1

LABEL org.opencontainers.image.title="mar" \
      org.opencontainers.image.description="High-performance archive utility (Rust engine)" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.ref.name="earthframe/mar" \
      org.opencontainers.image.source="https://github.com/earthframe/mar" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update && \
    apt-get install -y --no-install-recommends procps ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /mar /usr/local/bin/mar

ENTRYPOINT []
CMD ["mar", "--help"]

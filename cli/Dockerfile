# Spanly CLI — multi-stage Docker build.
#
# Stage 1: build the static binary against a known Go toolchain.
# Stage 2: copy the binary into a minimal distroless image (~15 MB total).
#
# Build context expected: this directory (apps/cli).
#
#   docker build -f Dockerfile -t spanly/spanly:latest .
#
# CI uses goreleaser to publish multi-arch images; this Dockerfile is the
# canonical local-build path and the source of truth for the runtime
# image layout.

# ---- Stage 1: build ----
FROM golang:1.26-alpine AS builder

ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/spanly .

# ---- Stage 2: runtime ----
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/spanly /usr/local/bin/spanly

# Distroless images don't have a shell. ENTRYPOINT is the binary itself
# so users pass subcommands as `docker run image proxy <upstream> <bind>`
# or `docker run image run -- <cmd>`.
ENTRYPOINT ["/usr/local/bin/spanly"]

# ── Stage 1: Download dependencies ───────────────────────────────────────────
# Separate from the build stage so Docker can cache the dependency layer.
# The layer is only invalidated when go.mod or go.sum changes — not on
# every source file edit. This makes rebuilds after code changes ~10x faster.
FROM golang:1.26-alpine AS deps

WORKDIR /app

# Copy module files first — if these haven't changed, Docker reuses the cache.
COPY go.mod go.sum ./
RUN go mod download

# ── Stage 2: Build the binary ─────────────────────────────────────────────────
FROM deps AS build

# Copy the full source tree (respects .dockerignore).
COPY . .

# CGO_ENABLED=0: produce a fully static binary with no libc dependency.
#   Required for the scratch runtime image which has no libc.
# GOOS=linux: explicit target OS (the build host may be macOS/Windows).
# -ldflags="-s -w": strip debug symbols (-s) and DWARF info (-w).
#   Reduces binary size by ~30% at no runtime cost.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /app/server \
    ./cmd/api

# ── Stage 3: Minimal runtime image ───────────────────────────────────────────
# scratch is an empty image — no shell, no package manager, no OS.
# The final image contains only: the binary + CA certificates.
# Typical size: 8–12 MB vs ~300 MB for a golang:alpine image.
FROM scratch

# CA certificates are required for TLS connections to external services
# (Stepstone, Indeed, Stripe, etc.). Without this file, all HTTPS calls fail.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled binary.
COPY --from=build /app/server /server

# Copy migrations directory.
COPY --from=build /app/migrations /migrations

# Expose the API port. Documentation only — does not publish the port.
EXPOSE 8080

# Use exec form (JSON array) to ensure the binary receives OS signals directly.
# Shell form (/bin/sh -c ...) would make the shell PID 1, which swallows
# SIGTERM and prevents graceful shutdown.
ENTRYPOINT ["/server"]
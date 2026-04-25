FROM golang:1.26-alpine AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /app/crawler \
    ./cmd/crawler

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/crawler /crawler

# Crawler exposes only a metrics port — it has no REST API.
# Prometheus scrapes /metrics on this port via a headless Service.
EXPOSE 9090

ENTRYPOINT ["/crawler"]
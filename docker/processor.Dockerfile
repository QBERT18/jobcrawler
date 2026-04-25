FROM golang:1.26-alpine AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /app/processor \
    ./cmd/processor

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/processor /processor
COPY --from=build /app/migrations /migrations

EXPOSE 9091

ENTRYPOINT ["/processor"]
FROM golang:1.25 AS build

WORKDIR /src

RUN go install github.com/bufbuild/buf/cmd/buf@v1.66.0

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY buf.gen.yaml buf.yaml ./
RUN buf generate buf.build/agynio/api \
    --include-imports \
    --path agynio/api/terminal_proxy/v1 \
    --path agynio/api/runner/v1 \
    --path agynio/api/runners/v1 \
    --path agynio/api/agents/v1

COPY . .

ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w" -o /out/terminal-proxy ./cmd/terminal-proxy

FROM alpine:3.21 AS runtime

WORKDIR /app
COPY --from=build /out/terminal-proxy /app/terminal-proxy
RUN addgroup -g 10001 -S app && adduser -u 10001 -S app -G app
USER 10001
ENTRYPOINT ["/app/terminal-proxy"]

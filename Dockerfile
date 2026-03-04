FROM golang:1.25-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -ldflags="-s -w" -o dlockss ./cmd/dlockss

FROM alpine:3.21 AS final

COPY --from=builder /build/dlockss /usr/local/bin/dlockss

WORKDIR /data
ENV DLOCKSS_DATA_DIR=/data/ingest

EXPOSE 5050

CMD ["dlockss"]

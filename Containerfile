FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /redtailfox ./cmd/redtailfox

FROM scratch

COPY --from=builder /redtailfox /redtailfox

ENTRYPOINT ["/redtailfox"]

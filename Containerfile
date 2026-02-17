# Build the redtailfox binary
FROM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Copy the go source
COPY cmd/ cmd/
COPY internal/ internal/

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o redtailfox ./cmd/redtailfox

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/redtailfox .
# Default to non-root. The manager service overrides this to root via
# compose.yaml "user: 0:0" because it needs access to the container
# runtime socket for spawning worker containers.
USER 65532:65532

ENTRYPOINT ["/redtailfox"]

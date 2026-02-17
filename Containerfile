# Build the redtailfox binary
FROM golang:1.26@sha256:c83e68f3ebb6943a2904fa66348867d108119890a2c6a2e6f07b38d0eb6c25c5 AS builder
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

# Create minimal passwd/group for the runtime stage (scratch has no user db).
RUN echo "nonroot:x:65532:65532:nonroot:/:/sbin/nologin" > /workspace/passwd && \
    echo "nonroot:x:65532:" > /workspace/group

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o redtailfox ./cmd/redtailfox

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /workspace/passwd /etc/passwd
COPY --from=builder /workspace/group /etc/group
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /workspace/redtailfox /redtailfox
# Default to non-root. The manager connects to the container runtime
# via a TCP socket proxy, so it does not need root either.
USER 65532:65532

ENTRYPOINT ["/redtailfox"]

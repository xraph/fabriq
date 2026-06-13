# syntax=docker/dockerfile:1

# Hermetic build. The repo carries local `replace` directives pointing at
# grove checkouts OUTSIDE this tree (until grove cuts a tagged release —
# see ADR-0005 / hardening item #10), so a plain `docker build` could not
# resolve them. We build from a vendored tree instead: `make docker-build`
# runs `go mod vendor` first, after which this Dockerfile needs no network
# and no out-of-tree paths. Vendoring is harmless once the replaces are
# dropped, so this Dockerfile keeps working unchanged after the grove tag.
FROM golang:1.26 AS build
WORKDIR /src

# Module graph + vendored deps first (cached unless deps change).
COPY go.mod go.sum ./
COPY vendor/ vendor/

# Source.
COPY . .

# Static, stripped single binary: worker + operator CLI in one (forge/cli
# RunApp). The default entrypoint serves the worker; the migrate Job
# overrides command to "fabriq migrate up".
RUN CGO_ENABLED=0 GOFLAGS=-mod=vendor go build -trimpath -ldflags "-s -w" \
      -o /out/fabriq ./cmd/fabriq

# Minimal, non-root runtime.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fabriq /usr/local/bin/fabriq
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/fabriq", "serve"]

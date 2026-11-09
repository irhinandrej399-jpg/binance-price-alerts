FROM golang:1.26.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -o /out/service ./cmd/${SERVICE}

FROM debian:bookworm-slim AS runtime
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/service /app/service
COPY infra/deploy/entrypoint.sh /app/entrypoint.sh
USER 65532:65532
ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]

FROM build AS smoke-build
COPY infra/deploy/smoke ./infra/deploy/smoke
RUN CGO_ENABLED=0 go test -c -o /out/smoke ./infra/deploy/smoke

FROM runtime AS smoke
COPY --from=smoke-build /out/smoke /app/smoke
ENTRYPOINT ["/app/smoke", "-test.v", "-test.timeout=8m"]

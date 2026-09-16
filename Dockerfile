FROM golang:1.25.6-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /pipeline ./cmd/pipeline

FROM alpine:3.23
WORKDIR /app
COPY --from=build /pipeline /app/pipeline
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY data/telemetry.csv /app/data/telemetry.csv
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/pipeline"]
CMD ["api"]

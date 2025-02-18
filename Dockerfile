FROM golang:1.23 as build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/
COPY Makefile ./
RUN make

FROM ubuntu:25.04 as run
COPY --from=build /app/bin/* /
ENTRYPOINT ["/ktab"]

FROM golang:1.16 as builder

ARG GO_FILE=out_clickhouse

WORKDIR /go/src

COPY . .
RUN go build -buildmode=c-shared -o /go/bin/out_clickhouse.so -- *.go

########################################################

FROM fluent/fluent-bit:1.8.12

COPY --from=builder /go/bin/out_clickhouse.so /out_clickhouse.so
EXPOSE 8888
EXPOSE 2020
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/out_clickhouse.so"]
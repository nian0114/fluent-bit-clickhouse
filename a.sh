#go must > 1.16
go build -buildmode=c-shared -o out_clickhouse.so -- *.go


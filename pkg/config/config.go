package config

import (
	"time"
	"unsafe"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/fluent/fluent-bit-go/output"
)

type ClickhouseParams struct {
	Addr          []string
	Table         string
	Username      string
	Password      string
	Auth_database string
	Database      string
	Collection    string
}

func GetAddress(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "endpoint")
}

func GetCollection(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "table")
}

func GetUsername(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "username")
}

func GetPassword(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "password")
}

func GetAuthDatabase(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "auth_database")
}

func GetDatabase(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "database")
}

func GetParams(ctx unsafe.Pointer) *ClickhouseParams {
	return &ClickhouseParams{
		Addr:          []string{GetAddress(ctx)},
		Table:         GetCollection(ctx),
		Username:      GetUsername(ctx),
		Password:      GetPassword(ctx),
		Auth_database: GetAuthDatabase(ctx),
		Database:      GetDatabase(ctx),
		Collection:    GetCollection(ctx),
	}
}

func GetConfig(ctx unsafe.Pointer) *clickhouse.Options {
	return &clickhouse.Options{
		Addr: []string{GetAddress(ctx)},
		Auth: clickhouse.Auth{
			Database: GetAuthDatabase(ctx),
			Username: GetUsername(ctx),
			Password: GetPassword(ctx),
		},
		// Debug:           true,
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Minute,
		// Compression: &clickhouse.Compression{
		// 	Method: clickhouse.CompressionLZ4,
		// },
	}
}

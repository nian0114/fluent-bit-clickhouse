package main

import (
	"C"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	clickhousedb "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/boxyhq/fluent-bit-clickhouse/pkg/config"
	flbcontext "github.com/boxyhq/fluent-bit-clickhouse/pkg/context"
	"github.com/boxyhq/fluent-bit-clickhouse/pkg/entry"
	"github.com/boxyhq/fluent-bit-clickhouse/pkg/log"
	"github.com/fluent/fluent-bit-go/output"
	"time"
	"unsafe"
)

const PluginID = "clickhouse"

//export FLBPluginRegister
func FLBPluginRegister(ctxPointer unsafe.Pointer) int {
	logger, err := log.New(log.OutputPlugin, PluginID)
	if err != nil {
		fmt.Printf("error initializing logger: %s\n", err)
		return output.FLB_ERROR
	}

	logger.Info("Registering plugin", nil)

	result := output.FLBPluginRegister(ctxPointer, PluginID, "Clickhouse")

	switch result {
	case output.FLB_OK:
		flbcontext.Set(ctxPointer, &flbcontext.Value{
			Logger: logger,
		})
	default:
		// nothing to do
	}

	return result
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctxPointer unsafe.Pointer) int {
	value, err := flbcontext.Get(ctxPointer)
	if err != nil {
		logger, err := log.New(log.OutputPlugin, PluginID)
		if err != nil {
			fmt.Printf("error initializing logger: %s\n", err)
			return output.FLB_ERROR
		}

		logger.Info("New logger initialized", nil)

		value.Logger = logger
	}

	value.Logger.Info("Initializing plugin", nil)

	value.Config = config.GetConfig(ctxPointer)
	value.Collection = config.GetCollection(ctxPointer)
	value.Params = config.GetParams(ctxPointer)

	flbcontext.Set(ctxPointer, value)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	panic(errors.New("not supported call"))
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctxPointer, data unsafe.Pointer, length C.int, tag *C.char) (result int) {
	value, err := flbcontext.Get(ctxPointer)
	if err != nil {
		fmt.Printf("error getting value: %s\n", err)
		return output.FLB_ERROR
	}

	logger := value.Logger
	ctx := log.WithLogger(context.TODO(), logger)

	// Open clickhouse session
	_config := value.Config.(*clickhousedb.Options)
	params := value.Params.(*config.ClickhouseParams)

	session, err := clickhousedb.Open(_config)
	if err != nil {
		logger.Error("Failed to connect to clickhousedb", map[string]interface{}{
			"error": err,
		})
		return output.FLB_RETRY
	}

	defer session.Close()

	dec := output.NewDecoder(data, int(length)) // Create Fluent Bit decoder
	// processor := clickhouse.New(session)

	if err := ProcessAll(ctx, dec, session, params.Database, params.Collection); err != nil {
		logger.Error("ProcessAll Failed", map[string]interface{}{
			"error": err,
		})

		if errors.Is(err, &entry.ErrRetry{}) {
			return output.FLB_RETRY
		}

		return output.FLB_ERROR
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

func ProcessAll(ctx context.Context, dec *output.FLBDecoder, session clickhousedb.Conn, db string, table string) error {
	// For log purpose
	startTime := time.Now()
	total := 0
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	// Iterate Records
	for {
		err = GetCount(ctx, session, db, table)
		if err != nil {
			return &entry.ErrRetry{Cause: err}
		}
		// Extract Record
		_, record, err := GetRecord(dec)
		if err != nil {
			if errors.Is(err, entry.ErrNoRecord) {
				logger.Debug("Records flushed", map[string]interface{}{
					"count":    total,
					"duration": time.Since(startTime),
					"record":   record,
				})

				break
			}

			return fmt.Errorf("get record: %w", err)
		}

		total++

		if err := ProcessRecord(ctx, record, db, table, session); err != nil {
			if errors.Is(err, &entry.ErrRetry{}) {
				return err
			}
			return fmt.Errorf("ProcessRecord Failed: %w", err)
		}
	}

	return nil
}

var ErrNoRecord = errors.New("failed to decode entry")

func ProcessRecord(ctx context.Context, record map[interface{}]interface{}, db string, table string, session clickhousedb.Conn) error {

	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	// if err := session.Ping(ctx); err != nil {
	// 	if exception, ok := err.(*clickhousedb.Exception); ok {
	// 		fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
	// 		logger.Error("Failed to ping clickhouse", map[string]interface{}{
	// 			"Code":       exception.Code,
	// 			"Message":    exception.Message,
	// 			"StackTrace": exception.StackTrace,
	// 		})
	// 		return &entry.ErrRetry{Cause: exception}
	// 	}
	// }

	err = GetCount(ctx, session, db, table)
	if err != nil {
		return &entry.ErrRetry{Cause: err}
	}

	data := make(map[string]interface{})

	for key, value := range record {
		strKey := fmt.Sprintf("%v", key)
		strValue := fmt.Sprintf("%s", value)
		data[strKey] = strValue
	}

	b, err := json.Marshal(data)
	if err != nil {
		logger.Error("json.Marshal failed", map[string]interface{}{
			"error":      err,
		})
		return &entry.ErrRetry{Cause: err}
	}
	mString :=string(b)

	query := fmt.Sprintf(`INSERT INTO %s.%s FORMAT JSONEachRow %s`, db, table, mString)

	logger.Error("Failed to save document", map[string]interface{}{
		"query":      mString,
	})
	// logger.Info("[Generated Query]", map[string]interface{}{
	// 	"query": query,
	// })
	if errInsert := session.AsyncInsert(ctx, query, false); errInsert != nil {
		logger.Error("Failed to save document", map[string]interface{}{
			"query":      query,
			"collection": table,
			"error":      errInsert,
		})

		return &entry.ErrRetry{Cause: err}
	}
	return nil
}

func GetRecord(dec *output.FLBDecoder) (time.Time, map[interface{}]interface{}, error) {
	ret, ts, record := output.GetRecord(dec)

	switch ret {
	default:
		return ts.(output.FLBTime).Time, record, nil
	case -1:
		return time.Time{}, nil, errors.New("failed to decode entry")
	case -2:
		return time.Time{}, nil, errors.New("unexpected entry type")
	}
}

func GetCount(ctx context.Context, session clickhousedb.Conn, db string, table string) error {
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	rows, err := session.Query(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s", db, table))
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			count uint64
		)
		if err := rows.Scan(&count); err != nil {
			return err
		}
		logger.Info("Sent Count Query", map[string]interface{}{
			"count": count,
		})
	}
	rows.Close()
	return rows.Err()
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

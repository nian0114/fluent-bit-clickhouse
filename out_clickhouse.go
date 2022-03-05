package main

import (
	"C"
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	clickhousedb "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/ukrocks007/fluent-bit-clickhouse/pkg/config"
	flbcontext "github.com/ukrocks007/fluent-bit-clickhouse/pkg/context"
	"github.com/ukrocks007/fluent-bit-clickhouse/pkg/entry"
	"github.com/ukrocks007/fluent-bit-clickhouse/pkg/log"
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

	result := output.FLBPluginRegister(ctxPointer, PluginID, "Go clickhouse go")

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

	// logger.Info("Connecting to clickhousedb", map[string]interface{}{
	// 	"host": _config.Addr,
	// })

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

	if err := ProcessAll(ctx, dec, session, params.Database, params.Collection, _config); err != nil {
		logger.Error("Failed to process logs", map[string]interface{}{
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

func ProcessAll(ctx context.Context, dec *output.FLBDecoder, session clickhousedb.Conn, db string, table string, config *clickhousedb.Options) error {
	// For log purpose
	startTime := time.Now()
	total := 0
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	// Iterate Records
	for {
		// Extract Record
		ts, record, err := GetRecord(dec)
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

		if err := ProcessRecord(ctx, ts, record, db, table, config); err != nil {
			return fmt.Errorf("process record: %w", err)
		}
	}

	return nil
}

var ErrNoRecord = errors.New("failed to decode entry")

func GetRecord(dec *output.FLBDecoder) (time.Time, map[interface{}]interface{}, error) {
	ret, ts, record := output.GetRecord(dec)

	switch ret {
	default:
		return ts.(output.FLBTime).Time, record, nil
	case -1:
		return time.Time{}, nil, ErrNoRecord
	case -2:
		return time.Time{}, nil, errors.New("unexpected entry type")
	}
}

func ProcessRecord(ctx context.Context, ts time.Time, record map[interface{}]interface{}, db string, table string, _config *clickhousedb.Options) error {

	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	session, err := clickhousedb.Open(_config)
	if err != nil {
		logger.Error("Failed to connect to clickhousedb", map[string]interface{}{
			"error": err,
		})
	}

	if err := session.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhousedb.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			logger.Error("Failed to ping clickhouse", map[string]interface{}{
				"Code":       exception.Code,
				"Message":    exception.Message,
				"StackTrace": exception.StackTrace,
			})
		}
	}

	data := make(map[string]interface{})

	for key, value := range record {
		strKey := fmt.Sprintf("%v", key)
		if strKey == "tenantId" || strKey == "timestamp" {
			intValue := fmt.Sprintf("%d", value)
			data[strKey] = intValue
		} else {
			strValue := fmt.Sprintf("%s", value)
			data[strKey] = strValue
		}
	}

	query := fmt.Sprintf(`INSERT INTO %s.%s (tenantId, timestamp, actor, actor_type, group,
		where, where_type, when, target, target_id, action, action_type, name, description) VALUES
		(%s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, db, table,
		data["tenantId"], data["timestamp"],
		data["actor"], data["actor_type"], data["group"],
		data["where"], data["where_type"], data["when"], data["target"], data["target_id"], data["action"], data["action_type"], data["name"], data["description"])
	// logger.Info("[Generated Query]", map[string]interface{}{
	// 	"query": query,
	// })
	err = session.AsyncInsert(ctx, query, false)
	if err != nil {
		logger.Error("Failed to save document", map[string]interface{}{
			"query":      query,
			"collection": table,
			"error":      err,
		})

		return &entry.ErrRetry{Cause: err}
	}
	return nil
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

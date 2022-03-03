package clickhouse

import (
	"context"
	"time"

	mgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ukrocks007/fluent-bit-clickhouse/pkg/entry"
)

type processor struct {
	clickhouseSession *mgo.Conn
}

func New(session *mgo.Conn) entry.Processor {
	return &processor{
		clickhouseSession: session,
	}
}

const ClickhouseDefaultDB = ""

func (p *processor) ProcessRecord(ctx context.Context, ts time.Time, record map[interface{}]interface{}, collection_name string) error {
	// logger, err := log.GetLogger(ctx)
	// if err != nil {
	// 	return fmt.Errorf("get logger: %w", err)
	// }

	// logDoc, err := Convert(ctx, record)
	// if err != nil {
	// 	logger.Error("Failed to convert record to document", map[string]interface{}{
	// 		"error": err,
	// 	})

	// 	return fmt.Errorf("new document: %w", err)
	// }
	// collection := p.clickhouseSession.DB(ClickhouseDefaultDB).C(logDoc.CollectionName())
	// if collection_name != "" {
	// 	collection = p.clickhouseSession.DB(ClickhouseDefaultDB).C(collection_name)
	// }

	// logger.Debug("Flushing to clickhouse", map[string]interface{}{
	// 	"document.id": logDoc.Id,
	// })

	// if err := logDoc.SaveTo(collection); err != nil {
	// 	logger.Error("Failed to save document", map[string]interface{}{
	// 		"document":   logDoc,
	// 		"collection": collection.FullName,
	// 		"error":      err,
	// 	})

	// 	return &entry.ErrRetry{Cause: err}
	// }

	return nil
}

package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/saagie/fluent-bit-clickhouse/pkg/log"
	"github.com/saagie/fluent-bit-clickhouse/pkg/parse"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const TimeFormat = time.RFC3339Nano

type Document struct {
	Id          bson.ObjectId `bson:"_id,omitempty"`
	ActionType  string        `bson:"action_type"`
	Actor       string        `bson:"actor"`
	ActorType   string        `bson:"actor_type"`
	Description string        `bson:"description"`
	Group       string        `bson:"group"`
	Name        string        `bson:"name"`
	Target      string        `bson:"target"`
	TargetId    string        `bson:"target_id"`
	When        string        `bson:"when"`
	Where       string        `bson:"where"`
	WhereType   string        `bson:"where_type"`
}

func Convert(ctx context.Context, record map[interface{}]interface{}) (*Document, error) {
	doc := &Document{}

	if err := doc.Populate(ctx, record); err != nil {
		return nil, fmt.Errorf("populate document: %w", err)
	}

	return doc, nil
}

const (
	ActionTypeKey  = "action_type"
	ActorKey       = "actor"
	ActorTypeKey   = "actor_type"
	DescriptionKey = "description"
	GroupKey       = "group"
	MetadataKey    = "metadata"
	NameKey        = "name"
	TargetKey      = "target"
	TargetIdKey    = "target_id"
	WhenKey        = "when"
	WhereKey       = "where"
	WhereTypeKey   = "where_type"
)

func (d *Document) Populate(ctx context.Context, record map[interface{}]interface{}) (err error) {
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	d.ActionType, err = parse.ExtractStringValue(record, ActionTypeKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{
			LookingFor: ActionTypeKey,
		}) {
			return fmt.Errorf("parse %s: %w", ActionTypeKey, err)
		}

		logger.Debug("Key not found", map[string]interface{}{
			"error": err,
		})

		d.ActionType = ""
	}

	d.Actor, err = parse.ExtractStringValue(record, ActorKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", ActorKey, err)
	}

	d.ActorType, err = parse.ExtractStringValue(record, ActorTypeKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", ActorTypeKey, err)
	}

	d.Description, err = parse.ExtractStringValue(record, DescriptionKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", DescriptionKey, err)
	}

	d.Group, err = parse.ExtractStringValue(record, GroupKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", GroupKey, err)
	}

	d.Name, err = parse.ExtractStringValue(record, NameKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", NameKey, err)
	}

	d.Target, err = parse.ExtractStringValue(record, TargetKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", TargetKey, err)
	}

	d.TargetId, err = parse.ExtractStringValue(record, TargetIdKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", TargetIdKey, err)
	}

	d.When, err = parse.ExtractStringValue(record, WhenKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", WhenKey, err)
	}

	d.Where, err = parse.ExtractStringValue(record, WhereKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", WhereKey, err)
	}

	d.WhereType, err = parse.ExtractStringValue(record, WhereTypeKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", WhereTypeKey, err)
	}

	return d.generateObjectID()
}

func (d *Document) generateObjectID() error {
	logJson, err := json.Marshal(d)
	if err != nil {
		return err
	}

	h64bytes, h32bytes, err := parse.GetHashesFromBytes(logJson)
	if err != nil {
		return err
	}

	id := fmt.Sprintf("%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
		h64bytes[0], h64bytes[1], h64bytes[2], h64bytes[3], h64bytes[4], h64bytes[5], h64bytes[6], h64bytes[7],
		h32bytes[0], h32bytes[1], h32bytes[2], h32bytes[3],
	)
	d.Id = bson.ObjectIdHex(id)
	return nil
}

func (d *Document) CollectionName() string {
	return strings.Replace(fmt.Sprintf("%s_%s_%s", d.Name, d.Target, d.Group), "-", "_", -1)
}

func (d *Document) SaveTo(collection *mgo.Collection) error {
	if _, err := collection.UpsertId(d.Id, d); err != nil {
		return fmt.Errorf("upsert %s: %w", d.Id, err)
	}

	indexes := []string{"job_execution_id", "time"}

	if err := collection.EnsureIndexKey(indexes...); err != nil {
		return fmt.Errorf("ensure indexes %v: %w", indexes, err)
	}

	return nil
}

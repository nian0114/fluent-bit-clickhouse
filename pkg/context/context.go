package context

import (
	"errors"
	"unsafe"

	"github.com/boxyhq/fluent-bit-clickhouse/pkg/log"
	"github.com/fluent/fluent-bit-go/output"
)

type Value struct {
	Logger     log.Logger
	Config     interface{}
	Collection string
	Params     interface{}
}

func Get(ctxPointer unsafe.Pointer) (*Value, error) {
	value := output.FLBPluginGetContext(ctxPointer)
	if value == nil {
		return &Value{}, errors.New("no value found")
	}

	return value.(*Value), nil
}

func Set(ctxPointer unsafe.Pointer, value *Value) {
	output.FLBPluginSetContext(ctxPointer, value)
}

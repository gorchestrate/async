package async

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type JsonEvent struct {
	Handler interface{}
}

func (h JsonEvent) Type() string {
	return ""
}

func (h JsonEvent) Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()

	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input")
	}
	if ft.NumOut() != 2 {
		return nil, fmt.Errorf("async http handler should have 2 outputs")
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("input param is not a struct"))
	}
	if ft.Out(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("first output param is not a struct"))
	}
	dstInput := reflect.New(ft.In(0))
	err := json.Unmarshal(input.(json.RawMessage), dstInput.Interface())
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal input: %v", err)
	}
	res := fv.Call([]reflect.Value{dstInput.Elem()})
	outErr, ok := res[1].Interface().(error)
	if !ok {
		return nil, fmt.Errorf("second output param is not an error")
	}
	if ok && outErr != nil {
		return nil, fmt.Errorf("err in handler: %v", err)
	}
	d, err := json.Marshal(res[1].Interface())
	if err != nil {
		return nil, fmt.Errorf("err marshaling output: %v", err)
	}
	return json.RawMessage(d), nil
}

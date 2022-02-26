package async

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

func OnEvent(name string, h interface{}, stmts ...Stmt) Event {
	return Event{
		Callback: CallbackRequest{
			Name: name,
		},
		Handler: &ReflectHandler{
			Handler: h,
		},
		Stmt: Section(stmts),
	}
}

type Empty struct {
}

// This is an example of how to create your custom events
type ReflectHandler struct {
	Handler interface{}
}

func (h ReflectHandler) inputSchema() ([]byte, error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("input param is not a struct"))
	}
	return json.Marshal(jsonschema.ReflectFromType(ft.In(0)))
}

func (h ReflectHandler) Schemas() (in *jsonschema.Schema, out *jsonschema.Schema, err error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumOut() != 2 {
		return nil, nil, fmt.Errorf("async http handler should have 2 outputs")
	}
	if ft.Out(0).Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf(("input param is not a struct"))
	}
	if ft.NumIn() != 1 {
		return nil, nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf(("input param is not a struct"))
	}
	r := jsonschema.Reflector{
		FullyQualifyTypeNames: true,
	}
	return r.ReflectFromType(ft.In(0)), r.ReflectFromType(ft.Out(0)), nil
}

func (h ReflectHandler) MarshalJSON() ([]byte, error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
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
	r := jsonschema.Reflector{
		FullyQualifyTypeNames: true,
	}
	in := r.ReflectFromType(ft.In(0))
	out := r.ReflectFromType(ft.Out(0))
	return json.Marshal(struct {
		Type   string
		Input  *jsonschema.Schema
		Output *jsonschema.Schema
	}{
		Type:   "handler",
		Input:  in,
		Output: out,
	})
}

const HandlerTypeReflect = "reflect"

func (h *ReflectHandler) Type() string {
	return HandlerTypeReflect
}

// code that will be executed when event is received
func (h *ReflectHandler) Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error) {
	in, err := h.inputSchema()
	if err != nil {
		return nil, fmt.Errorf("input schema: %v", err)
	}
	vRes, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(in), gojsonschema.NewBytesLoader(input.([]byte)))
	if err != nil {
		return nil, fmt.Errorf("jsonschema validate failure: %v using %v", err, string(in))
	}
	if !vRes.Valid() {
		return nil, fmt.Errorf("jsonschema validate: %v", vRes.Errors())
	}
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
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
	err = json.Unmarshal(input.([]byte), dstInput.Interface())
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal input: %v", err)
	}
	res := fv.Call([]reflect.Value{dstInput.Elem()})
	if res[1].Interface() != nil {
		outErr, ok := res[1].Interface().(error)
		if !ok {
			return nil, fmt.Errorf("second output param is not an error")
		}
		if outErr != nil {
			return nil, fmt.Errorf("err in handler: %w", outErr)
		}
	}
	d, err := json.Marshal(res[0].Interface())
	if err != nil {
		return nil, fmt.Errorf("err marshaling output: %v", err)
	}
	return json.RawMessage(d), nil
}

// when we will start listening for this event - Setup() will be called for us to setup this event on external services
func (t *ReflectHandler) Setup(ctx context.Context, req CallbackRequest) (string, error) {
	// we will receive event via http call, no setup is needed
	return "", nil
}

// when we will stop listening for this event - Teardown() will be called for us to remove this event on external services
func (t *ReflectHandler) Teardown(ctx context.Context, req CallbackRequest, handled bool) error {
	// we will receive event via http call, no teardown is needed
	return nil
}

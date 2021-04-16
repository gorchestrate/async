package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/alecthomas/jsonschema"
)

type CBHandler struct {
	Handler http.HandlerFunc
	Docs    HandlerDocs
}

// following function inputs are supported for handlers
// func()
// func(Context.Context)
// func(Struct)
// func(Context.Context, Struct)

// following function outputs are supported for handlers
// func() {}
// func() {}  error
// func() {}  Struct
// func() {}  (Struct,error)

// following function signature is allowed for exceptional use-cases, but discouraged to be used
// CBHandler

func isContext(t reflect.Type) bool {
	return t.Implements(reflect.TypeOf((*context.Context)(nil)).Elem())
}
func isError(t reflect.Type) bool {
	return t.Implements(reflect.TypeOf((*error)(nil)).Elem())
}
func ReflectDoc(handler Handler) HandlerDocs {
	r := jsonschema.Reflector{
		FullyQualifyTypeNames:     true,
		AllowAdditionalProperties: true,
	}
	// check if this is a custom handler
	custom, ok := handler.(CBHandler)
	if ok {
		return custom.Docs
	}
	h := reflect.ValueOf(handler)
	ht := h.Type()
	docs := HandlerDocs{}
	switch {
	case ht.NumIn() == 0:
	case ht.NumIn() == 1 && isContext(ht.In(0)):
	case ht.NumIn() == 1 && !isContext(ht.In(0)):
		docs.Input = r.ReflectFromType(ht.In(0))
	case ht.NumIn() == 2 && isContext(ht.In(0)):
		docs.Input = r.ReflectFromType(ht.In(1))
	default:
		panic(`following function inputs are supported for handlers
func()
func(Context.Context)
func(Struct)
func(Context.Context, Struct)`)
	}

	switch {
	case ht.NumOut() == 0:
	case ht.NumOut() == 1 && isError(ht.Out(0)):
	case ht.NumOut() == 1 && !isError(ht.Out(0)):
		docs.Output = r.ReflectFromType(ht.Out(0))
	case ht.NumOut() == 2 && isError(ht.Out(1)):
		docs.Output = r.ReflectFromType(ht.Out(0))
	default:
		panic(`following function outputs are supported for handlers
func() {}
func() {}  error
func() {}  Struct
func() {}  (Struct,error)`)
	}

	return docs
}

func reflectCall(handler Handler, ctx *ResumeContext) error {
	h := reflect.ValueOf(handler)
	ht := h.Type()

	in := []reflect.Value{}
	if ht.NumIn() == 1 {
		v := reflect.New(ht.In(0)).Interface()
		err := json.Unmarshal(ctx.CallbackInput, v)
		if err != nil {
			return err
		}
		in = []reflect.Value{reflect.ValueOf(v).Elem()}
	}
	res := h.Call(in)
	if len(res) == 1 {
		return res[0].Interface().(error)
	}
	if len(res) == 2 {
		d, err := json.Marshal(res[0].Interface())
		if err != nil {
			return fmt.Errorf("err mashaling callback output: %v", err)
		}
		ctx.CallbackOutput = d
		if res[1].Interface() == nil {
			return nil
		}
		return res[1].Interface().(error)
	}
	return nil
}

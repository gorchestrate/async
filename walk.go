package async

import (
	"fmt"
	"reflect"

	"github.com/alecthomas/jsonschema"
)

func FindStep(name string, sec Stmt) Stmt {
	var ret Stmt
	Walk(sec, func(s Stmt) bool {
		switch x := s.(type) {
		case StmtStep:
			if x.Name == name {
				ret = x
				return true
			}
		}
		return false
	})
	return ret
}

func Walk(s Stmt, f func(s Stmt) bool) bool {
	if f(s) {
		return true
	}
	switch x := s.(type) {
	case nil:
		return false
	case ReturnStmt:
		return false
	case BreakStmt:
		return false
	case StmtStep:
		return false
	case SelectStmt:
		for _, v := range x.Cases {
			if Walk(v.Stmt, f) {
				return true
			}
		}
	case GoStmt:
		return Walk(x.Stmt, f)
	case ForStmt:
		return Walk(x.Stmt, f)
	case SwitchStmt:
		for _, v := range x {
			if Walk(v.Stmt, f) {
				return true
			}
		}
	case Section:
		for _, v := range x {
			if Walk(v, f) {
				return true
			}
		}
	default:
		panic(fmt.Sprintf("unknown statement: %v", reflect.TypeOf(s)))
	}
	return false
}

type HandlerDocs struct {
	Input  *jsonschema.Schema
	Output *jsonschema.Schema
}

type DocWorkflow struct {
	Handlers map[string]HandlerDocs
	Input    *jsonschema.Schema
	Output   *jsonschema.Schema
}

func cleanVersion(t *jsonschema.Type) *jsonschema.Type {
	if t == nil {
		return t
	}
	t.Version = ""
	if t.Properties != nil {
		for _, k := range t.Properties.Keys() {
			v, _ := t.Properties.Get(k)
			vt, ok := v.(*jsonschema.Type)
			if !ok {
				continue
			}
			vt = cleanVersion(vt)
			t.Properties.Set(k, vt)
		}
	}
	t.Items = cleanVersion(t.Items)
	return t
}

func Docs(def WorkflowDefinition) DocWorkflow {
	doc := DocWorkflow{
		Handlers: map[string]HandlerDocs{
			"/": ReflectDoc(def.New, false),
		},
	}
	Walk(def.Body, func(s Stmt) bool {
		switch x := s.(type) {
		case SelectStmt:
			for _, v := range x.Cases {
				if v.CallbackName != "" {
					doc.Handlers[v.CallbackName] = ReflectDoc(v.Handler, false)
					return false
				}
			}
		}
		return false
	})
	return doc
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
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
	if t.Properties == nil {
		return t
	}
	for _, k := range t.Properties.Keys() {
		v, _ := t.Properties.Get(k)
		vt, ok := v.(*jsonschema.Type)
		if !ok {
			continue
		}
		vt.Version = ""
		vt = cleanVersion(vt)
		t.Properties.Set(k, vt)
	}
	return t
}

func Swagger(ww map[string]Workflow) ([]byte, error) {
	paths := map[string]interface{}{}
	defs := map[string]interface{}{
		"Error": map[string]interface{}{
			"properties": map[string]interface{}{
				"code": map[string]string{
					"type": "string",
				},
				"message": map[string]string{
					"type": "string",
				},
			},
		},
	}
	for _, w := range ww {
		wdoc := Docs(w.InitState().Definition())
		for handler, schema := range wdoc.Handlers {
			for k, def := range schema.Input.Definitions {
				defs[k] = cleanVersion(def)
				log.Printf("add definition: %v", k)
			}
			for k, def := range schema.Output.Definitions {
				defs[k] = cleanVersion(def)
				log.Printf("add definition: %v", k)
			}
			paths[fmt.Sprintf("/%v/{id}%v", w.Name, handler)] = map[string]interface{}{
				"post": map[string]interface{}{
					"summary":     fmt.Sprintf("%v Workflow action: %v", w.Name, handler),
					"description": "DESCR",
					"consumes":    []string{"application/json"},
					"produces":    []string{"application/json"},
					"parameters": []interface{}{
						map[string]interface{}{
							"in":   "body",
							"name": fmt.Sprintf("%v", w.Name),
							"schema": map[string]string{
								"$ref": schema.Input.Ref,
							},
						},
						map[string]interface{}{
							"required": true,
							"in":       "path",
							"name":     "id",
							"type":     "string",
						},
					},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "OK",
							"schema": map[string]string{
								"$ref": schema.Output.Ref,
							},
						},
						"400": map[string]interface{}{
							"description": "Error",
							"schema": map[string]string{
								"$ref": "#/definitions/Error",
							},
						},
					},
				},
			}

		}
	}

	res := map[string]interface{}{
		"swagger": "2.0",
		"info": map[string]interface{}{
			"title":   "SampleAPI",
			"version": "1.0.0",
		},
		"host":        "api.example.com",
		"basePath":    "/workflows",
		"schemes":     []string{"https"},
		"paths":       paths,
		"definitions": defs,
	}

	return json.MarshalIndent(res, "", " ")
}

func Docs(def WorkflowDefinition) DocWorkflow {
	doc := DocWorkflow{
		Handlers: map[string]HandlerDocs{
			"/": ReflectDoc(def.New),
		},
	}
	Walk(def.Body, func(s Stmt) bool {
		switch x := s.(type) {
		case SelectStmt:
			for _, v := range x.Cases {
				if v.CaseEvent != "" {
					doc.Handlers[v.CaseEvent] = ReflectDoc(v.Handler)
					return false
				}
			}
		}
		return false
	})
	return doc
}

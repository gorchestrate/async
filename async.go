package main

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"
)

// Workflow defines how we create/resume our workflow state.
type Workflow struct {
	Name      string               // used to init proper workflow state from available workflows
	InitState func() WorkflowState // create new workflow state object - current workflow state will be unmarshalled into it.
}

// WorkflowState should be a Go struct supporting JSON unmarshalling into it.
// When process is resumed - current state is unmarshalled into it and then Workflow() is called.
// With such technique all usages of receiver withing Workflow() function will refer to current values, so there's no need for lasy parameters i.e.  instead of 'If( func() bool { return s.IsAvailable}' we can write 'If(s.IsAvailable)'
type WorkflowState interface {
	Workflow() Section // Return current workflow definition. This function can be called multiple times, so be careful with doing real code execution inside.
}

// ResumeContext is used during workflow execution
// It contains resume input as well as current state of the execution.
type ResumeContext struct {
	Running        bool            // Running means process is already resumed and we are executing statements. If process is not running - we are searching for the step we should resume from.
	CurStep        string          // CurStep of the workflow we are resuming from.
	CallbackIndex  int             // In case we are resuming a Select - this is and index of the select case to resume
	CallbackInput  json.RawMessage // In case we are resuming a Select with a callback event - this is the data to unmarshall into callback function parameters via reflect.
	CallbackOutput json.RawMessage // In case we are resuming a Select with a callback event - this is the data to marshall back to client in case workflow was successfully saved.
	Break          bool            // Used for loop management

	//CurThread     string  // TODO: Goroutines inside process
	//T       *Thread       // TODO: Goroutines inside process
}

// Stop tells us that syncronous part of the workflow has finished. It means we either:
type Stop struct {
	Step   string      // waiting for step execution to complete
	Select *SelectStmt // waiting for event
	Return interface{} // returning from process

	//SubWorkflow string   // TODO: SubWorkflow support
}

// Section is similar to code block {} with a list of statements.
type Section []Stmt

// S is a syntax sugar to properly align statement sections
func S(ss ...Stmt) Section {
	return ss
}

// Stmt is async statement definition that should support workflow resuming & search.
type Stmt interface {
	// Resume continues execution of the process, based on ResumeContext
	// It walks the tree searching for CurStep and then continues the process
	// stopping at some point or exiting at the end of it.
	// If callback not found *Stop will be nil and ctx.Running will be false
	// If callback is found, but process has finished - *Stop will be nil and ctx.Running will be true
	// Otherwise Resume should always return *Stop or err != nil
	Resume(ctx *ResumeContext) (*Stop, error)

	// Find walks the tree, searching for Stmt with approapriate CurStep and returns it
	Find(name string) Stmt
}

func (s Section) Resume(ctx *ResumeContext) (*Stop, error) {
	for _, stmt := range s {
		b, err := stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
	}
	return nil, nil
}

func (s Section) Find(name string) Stmt {
	for _, stmt := range s {
		stmt := stmt.Find(name)
		if stmt != nil {
			return stmt
		}
	}
	return nil
}

type ActionResult struct {
	Success       bool
	Error         string
	Retries       int
	RetryInterval time.Duration
}

// type RecoverResult struct {
// 	Success bool
// 	Error   string
// 	Retry   int
// }

type ActionFunc func() ActionResult

// type RecoverFunc func() RecoverResult

type StmtStep struct {
	Name   string
	Action ActionFunc
	// Recover RecoverFunc
}

func (s StmtStep) Find(name string) Stmt {
	if s.Name == name {
		return s
	}
	return nil
}

func (s StmtStep) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		return &Stop{Step: s.Name}, nil
	}

	if ctx.CurStep == s.Name {
		ctx.Running = true
	}
	return nil, nil
}

func Step(name string, action ActionFunc) StmtStep {
	return StmtStep{
		Name:   name,
		Action: action,
	}
}

// func (s StmtStep) WithRecovery(recover RecoverFunc) StmtStep {
// 	s.Recover = recover
// 	return s
// }

type SwitchCase struct {
	Cond bool
	Stmt Stmt
}

type SwitchStmt []SwitchCase

func Switch(ss ...SwitchCase) SwitchStmt {
	return ss
}

func (s SwitchStmt) Find(name string) Stmt {
	for _, v := range s {
		stmt := v.Stmt.Find(name)
		if stmt != nil {
			return stmt
		}
	}
	return nil
}

func (s SwitchStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		for _, v := range s {
			if v.Cond {
				b, err := v.Stmt.Resume(ctx)
				if err != nil || b != nil {
					return b, err
				}
				break
			}
		}
		return nil, nil
	}
	for _, v := range s {
		b, err := v.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
		break
	}
	return nil, nil
}

func If(cond bool, sec Stmt) SwitchStmt {
	return SwitchStmt{
		SwitchCase{
			Cond: cond,
			Stmt: sec,
		},
	}
}
func Case(cond bool, sec Stmt) SwitchCase {
	return SwitchCase{
		Cond: cond,
		Stmt: sec,
	}
}

func Default(sec Stmt) SwitchCase {
	return SwitchCase{
		Cond: true,
		Stmt: sec,
	}
}

type ForStmt struct {
	Cond bool // nil cond for infinite loop
	Stmt Stmt
}

func For(cond bool, sec Stmt) Stmt {
	return ForStmt{
		Cond: cond,
		Stmt: sec,
	}
}

func (f ForStmt) Find(name string) Stmt {
	return f.Stmt.Find(name)
}

func (f ForStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if !ctx.Running {
		b, err := f.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
	}
	if ctx.Running {
		if !f.Cond {
			return nil, nil
		}

		if ctx.Break {
			ctx.Break = false
			return nil, nil
		}

		b, err := f.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
		panic("at least 1 stmt should be executed in For loop. Otherwise condition will never change and we have infinite async loop. TODO: panics should be handled via Stop/Resume actions")
	}
	return nil, nil
}

type SelectStmt struct {
	Name  string
	Cases []WaitCond
}

func Select(name string, ss ...WaitCond) SelectStmt {
	return SelectStmt{
		Name:  name,
		Cases: ss,
	}
}

func (s SelectStmt) Find(name string) Stmt {
	for _, v := range s.Cases {
		stmt := v.Stmt.Find(name)
		if stmt != nil {
			return stmt
		}
	}
	return nil
}

// there are 3 options for input params
// 1. func()
// 2. func(in Struct)
// 2. func(ctx Context.Context, in Struct)

// and there are 3 options for output params
// 1. func() {}
// 2. func() error {}
// 3. func() (Struct, error) {}

func reflectCall(handler interface{}, ctx *ResumeContext) error {
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

func (s SelectStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		return &Stop{Select: &s}, nil
	}

	if s.Name == ctx.CurStep {
		if ctx.CallbackIndex >= len(s.Cases) {
			return nil, fmt.Errorf("index out ouf bounds for select callback: %#v %v", *ctx, s.Cases)
		}
		ctx.Running = true
		log.Print("WTF ", ctx.CallbackIndex)
		resCase := s.Cases[ctx.CallbackIndex]
		if resCase.Handler != nil { // Execute syncronous handler for validation purposes
			err := reflectCall(resCase.Handler, ctx)
			if err != nil {
				return nil, fmt.Errorf("err during handler call: %v", err)
			}
		}
		return resCase.Stmt.Resume(ctx)
	}

	for _, v := range s.Cases {
		b, err := v.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
		if ctx.Running {
			break
		}
	}
	return nil, nil
}

type WaitCond struct {
	CaseAfter time.Duration // wait for time
	CaseEvent string        // wait for event
	Handler   interface{}

	Stmt Stmt
}

// After waits for specified time and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func After(d time.Duration, sec Stmt) WaitCond {
	return WaitCond{
		CaseAfter: d,
		Stmt:      sec,
	}
}

// On waits for event to come and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func On(event string, handler interface{}, sec Stmt) WaitCond {
	return WaitCond{
		CaseEvent: event,
		Stmt:      sec,
		Handler:   handler,
	}
}

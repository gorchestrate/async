package async

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Workflow defines how we create/resume our workflow state.
type Workflow func() WorkflowState

// WorkflowState should be a Go struct supporting JSON unmarshalling into it.
// When process is resumed - current state is unmarshalled into it and then Definition() is called.
// This is needed to eliminate lasy parameters i.e.
// instead of 'If( func() bool { return s.IsAvailable})' we can write 'If(s.IsAvailable)'.
type WorkflowState interface {
	// Definition func may be called multiple times so it should be idempotent.
	// All actions should done in callbacks or steps.
	Definition() Section
}

// Handler is a generic function that is analyzed using reflection
// It's a convenient way to specify input/output types as well as the implementation
//type Handler interface{}

// ResumeContext is used during workflow execution
// It contains resume input as well as current state of the execution.
type ResumeContext struct {
	s *State
	t *Thread // current thread to resume

	// Running means process is already resumed and we are executing statements.
	// process is not running - we are searching for the step we should resume from.
	Running bool

	// CallbackName is the name of callback that is need to be resumed
	CallbackName string

	CallbackInput  json.RawMessage
	CallbackOutput json.RawMessage

	// Used for loop management
	Break bool
}

// Stop tells us that syncronous part of the workflow has finished. It means we either:
type Stop struct {
	Step   string      // waiting for step execution to complete
	Select *SelectStmt // waiting for event
	Return interface{} // returning from process
}

// Section is similar to code block {} with a list of statements.
type Section []Stmt

// S is a syntax sugar to properly indent statement sections when using gofmt
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
}

// for block of code - simply try to resume/exec all stmts until we get blocked somewhere
func (s Section) Resume(ctx *ResumeContext) (*Stop, error) {
	for _, stmt := range s {
		b, err := stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
	}
	return nil, nil
}

type ActionResult struct {
	Success       bool
	Error         string
	Retries       int
	RetryInterval time.Duration
}

type ActionFunc func() ActionResult

type StmtStep struct {
	Name   string
	Action ActionFunc
}

func (s StmtStep) Resume(ctx *ResumeContext) (*Stop, error) {
	// resuming Step consists of 3 parts:
	// 1. During execution we get blocked on Stmt and return
	if ctx.Running {
		return &Stop{Step: s.Name}, nil
	}

	// 2. Separate routine will pickup blocked steps and execute them

	// 3. We resume from this step and continue
	if ctx.t.CurStep == s.Name {
		ctx.Running = true
	}

	return nil, nil
}

// Execute step and retry it on failure.
func Step(name string, action ActionFunc) StmtStep {
	return StmtStep{
		Name:   name,
		Action: action,
	}
}

type SwitchCase struct {
	CondLabel string
	Cond      bool
	Stmt      Stmt
}

type SwitchStmt []SwitchCase

// execute statements based on condition
func Switch(ss ...SwitchCase) SwitchStmt {
	return ss
}

func (s SwitchStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	// if running - conditions are already evaluated - let's just loop through them
	// and see which are true
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

	// if not running - try to resume everything
	for _, v := range s {
		b, err := v.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
	}
	return nil, nil
}

// execute statements if ...
func If(cond bool, condLabel string, sec Stmt) SwitchStmt {
	return SwitchStmt{
		SwitchCase{
			CondLabel: condLabel,
			Cond:      cond,
			Stmt:      sec,
		},
	}
}

// execute statements if ...
func Case(cond bool, condLabel string, sec Stmt) SwitchCase {
	return SwitchCase{
		CondLabel: condLabel,
		Cond:      cond,
		Stmt:      sec,
	}
}

// execute statements if none of previous statements matched
func Default(sec Stmt) SwitchCase {
	return SwitchCase{
		CondLabel: "default",
		Cond:      true,
		Stmt:      sec,
	}
}

type ForStmt struct {
	CondLabel string
	Cond      bool // nil cond for infinite loop
	Stmt      Stmt
}

// execute statements in the loop while condition is met
func For(cond bool, condLabel string, sec Stmt) Stmt {
	return ForStmt{
		CondLabel: condLabel,
		Cond:      cond,
		Stmt:      sec,
	}
}

func (f ForStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	// if resuming - try resume all stmts in for loop
	if !ctx.Running {
		b, err := f.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
	}

	// if running - let's simulate for loop behaviour
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

// wait for multiple conditions and execute only one
func Select(name string, ss ...WaitCond) SelectStmt {
	return SelectStmt{
		Name:  name,
		Cases: ss,
	}
}

func (s SelectStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	// block on this select statement immediately
	if ctx.Running {
		for _, c := range s.Cases {
			if c.Handler != nil {
				err := c.Handler.Setup(CallbackRequest{
					WorkflowID: ctx.s.ID,
					ThreadID:   ctx.t.ID,
					Callback:   c.CallbackName,
					PC:         ctx.t.PC,
				})
				if err != nil {
					return nil, err
				}
			}
		}
		return &Stop{Select: &s}, nil
	}

	// in case this select case was triggered - let's unblock on specific step
	if s.Name == ctx.t.CurStep {
		ctx.Running = true
		for _, c := range s.Cases {
			if c.Handler != nil {
				err := c.Handler.Teardown(CallbackRequest{
					WorkflowID: ctx.s.ID,
					ThreadID:   ctx.t.ID,
					Callback:   c.CallbackName,
					PC:         ctx.t.PC, // TODO: different PC?
				})
				// TODO:  With current implementation - if we get error during execution
				// teardown will be executed, but workflow won't be saved, resulting in missed callback.
				// Option 1: handle callback asynchronously (i.e. save first, execute later)
				// Option 2: teardown only after callback was resumed and saved, for example
				// we can save teardown functions into execution context
				if err != nil {
					return nil, err
				}
			}
		}
		for _, c := range s.Cases {
			if c.CallbackName == ctx.CallbackName {
				if c.Handler != nil { // Execute syncronous handler for validation purposes
					out, err := c.Handler.Handler(CallbackRequest{
						WorkflowID: ctx.s.ID,
						ThreadID:   ctx.t.ID,
						Callback:   c.CallbackName,
						PC:         ctx.t.PC, // TODO: different PC?
					}, ctx.CallbackInput)
					ctx.CallbackOutput = out
					if err != nil {
						return nil, err
					}
				}
				return c.Stmt.Resume(ctx)
			}
		}
		panic(fmt.Sprintf("callback %v for case %v  not found", ctx.CallbackName, ctx.t.CurStep))
	}

	// try to resume on stmts inside this select
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

type Handler interface {
	Setup(req CallbackRequest) error
	Teardown(req CallbackRequest) error
	Handler(req CallbackRequest, input json.RawMessage) (json.RawMessage, error)
}

type WaitCond struct {
	CallbackName string
	Handler      Handler

	Stmt Stmt
}

// On waits for event to come and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func Event(event string, handler Handler, sec Stmt) WaitCond {
	return WaitCond{
		CallbackName: event,
		Stmt:         sec,
		Handler:      handler,
	}
}

type BreakStmt struct {
}

func (s BreakStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	ctx.Break = true
	return nil, nil
}

// Break for loop
func Break() BreakStmt {
	return BreakStmt{}
}

type ReturnStmt struct {
	Value interface{}
}

func (s ReturnStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	return &Stop{
		Return: s.Value,
	}, nil
}

// Finish workflow and return result
func Return(v interface{}) ReturnStmt {
	return ReturnStmt{
		Value: v,
	}
}

type GoStmt struct {
	ID   func() string
	Name string // name of goroutine
	Stmt Stmt
}

// Run statements in a separate Thread
func Go(name string, body Stmt, id func() string) GoStmt {
	return GoStmt{
		ID:   id,
		Name: name,
		Stmt: body,
	}
}

// When we meet Go stmt - we simply create threads and continue execution.
func (s GoStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		id := ""
		if s.ID != nil {
			id = s.ID()
		}
		ctx.s.Threads.Add(&Thread{
			ID:     id,
			Name:   s.Name,
			Status: ThreadResuming,
		})
		return nil, nil
	}
	return s.Stmt.Resume(ctx)
}

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

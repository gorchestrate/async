package async

import (
	"encoding/json"
	"fmt"
	"log"
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
type Handler interface{}

// ResumeContext is used during workflow execution
// It contains resume input as well as current state of the execution.
type ResumeContext struct {
	s *State
	t *Thread // current thread to resume

	// Running means process is already resumed and we are executing statements.
	// process is not running - we are searching for the step we should resume from.
	Running bool

	// If thread is resumed by callback - additinal info is required

	// CallbackName is the name of callback that is need to be resumed
	CallbackName string
	// CallbackInput is an input to a syncronous Handler is executed to validate input & do immediate actions
	CallbackInput json.RawMessage
	// Return result from this Handler. Result is returned only after wokflow was updated
	CallbackOutput json.RawMessage

	Break bool // Used for loop management
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
		return &Stop{Select: &s}, nil
	}

	// in case this select case was triggered - let's unblock on specific step
	if s.Name == ctx.t.CurStep {
		ctx.Running = true
		for _, c := range s.Cases {
			if c.CallbackName == ctx.CallbackName {
				if c.Handler != nil { // Execute syncronous handler for validation purposes
					err := reflectCall(c.Handler, ctx)
					if err != nil {
						return nil, fmt.Errorf("err during handler call: %v", err)
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

type WaitCond struct {
	CaseAfter    time.Duration // wait for time
	CallbackName string        // wait for event
	CaseRecv     string        // wait for receive channel
	CaseSend     string        // wait for send channels
	CaseWait     bool          // wait for custom condition. evaluated during func parsing
	SendData     json.RawMessage

	Handler Handler

	Stmt Stmt
}

// After waits for specified time and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func After(d time.Duration, sec Stmt) WaitCond {
	return WaitCond{
		CaseAfter:    d,
		CallbackName: fmt.Sprintf("_timeout_%v", d.Seconds()),
		Stmt:         sec,
	}
}

// On waits for event to come and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func On(event string, handler Handler, sec Stmt) WaitCond {
	return WaitCond{
		CallbackName: event,
		Stmt:         sec,
		Handler:      handler,
	}
}

// func Wait(event string, cond bool, sec Stmt) WaitCond {
// 	return WaitCond{
// 		CaseWait: cond,
// 		Stmt:     sec,
// 	}
// }

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
	// TODO: return from function has to be scoped
	// especially for Go stmt
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
		log.Print("THREADS", len(ctx.s.Threads))
		log.Print("ADD THREAD", id, s.Name)
		ctx.s.Threads.Add(&Thread{
			ID:     id,
			Name:   s.Name,
			Status: ThreadResuming,
		})
		log.Print("THREADS", len(ctx.s.Threads))
		return nil, nil
	}
	return s.Stmt.Resume(ctx)
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

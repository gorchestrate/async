package async

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// WorkflowState should be a Go struct supporting JSON unmarshalling into it.
// When process is resumed - current state is unmarshalled into it and then Definition() is called.
// This is needed to eliminate lasy parameters and conditions i.e.
// instead of 'If( func() bool { return s.IsAvailable})' we can write 'If(s.IsAvailable)'.
type WorkflowState interface {
	// Definition func may be called multiple times so it should be idempotent.
	// All actions should done in callbacks or steps.
	Definition() Section
}

// ResumeContext is used during workflow execution
// It contains resume input as well as current state of the execution.
type ResumeContext struct {
	ctx context.Context
	s   *State
	t   *Thread // current thread to resume

	// Running means process is already resumed and we are executing statements.
	// process is not running - we are searching for the step we should resume from.
	Running bool

	// In case workflow is resumed by a callback
	Callback       CallbackRequest
	CallbackInput  interface{}
	CallbackOutput interface{}

	Return bool
}

// Stop tells us that syncronous part of the workflow has finished. It means we either:
type Stop struct {
	Step   string      // execute step
	Select *SelectStmt // wait for Events
	Return bool        // stop this thread
	Cond   string      // wait for cond
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
	// If ctx.Running == true Resume should execute the statement or continue execution after.
	// If ctx.Running = false Resume should not execute the statement, but recursively search for the statement that needs to be resumed.
	// If it needs to be resumed - don't execute it, but continue execution from this statement.
	//
	// If stmt not found *Stop will be nil and ctx.Running will be false
	// If stmt is found, but process has finished - *Stop will be nil and ctx.Running will be true
	// Otherwise Resume should always return *Stop or err != nil
	Resume(ctx *ResumeContext) (*Stop, error)
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

type StmtStep struct {
	Name   string
	Action func() error
}

func (s StmtStep) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		return &Stop{Step: s.Name}, nil
	}
	if ctx.t.CurStep == s.Name {
		ctx.Running = true
	}
	return nil, nil
}

func Step(name string, action func() error) StmtStep {
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

type SwitchStmt struct {
	Cases []SwitchCase
}

// execute statements based on condition
func Switch(ss ...SwitchCase) *SwitchStmt {
	return &SwitchStmt{
		Cases: ss,
	}
}

func (s *SwitchStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		for _, v := range s.Cases {
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

	for _, v := range s.Cases {
		b, err := v.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
		if ctx.Running { // if select case was resumed - don't execute next ones
			return nil, nil
		}
	}
	return nil, nil
}

func If(cond bool, sec ...Stmt) *SwitchStmt {
	return &SwitchStmt{
		Cases: []SwitchCase{{
			Cond: cond,
			Stmt: Section(sec),
		}},
	}
}

func (s *SwitchStmt) ElseIf(cond bool, sec ...Stmt) *SwitchStmt {
	s.Cases = append(s.Cases, SwitchCase{
		Cond: cond,
		Stmt: Section(sec),
	})
	return s
}

func (s *SwitchStmt) Else(sec ...Stmt) *SwitchStmt {
	s.Cases = append(s.Cases, SwitchCase{
		Cond: true,
		Stmt: Section(sec),
	})
	return s
}

func Case(cond bool, sec Stmt) SwitchCase {
	return SwitchCase{
		Cond: cond,
		Stmt: sec,
	}
}

func Default(sec Stmt) SwitchCase {
	return SwitchCase{
		CondLabel: "default",
		Cond:      true,
		Stmt:      sec,
	}
}

type WaitStmt struct {
	CondLabel string
	Cond      bool
	Handler   func() // executed when cond is true
}

// Wait statement wait for condition to be true.
func Wait(cond bool, label string, handler func()) WaitStmt {
	return WaitStmt{
		Cond:      cond,
		CondLabel: label,
		Handler:   handler,
	}
}

func (f WaitStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running && !f.Cond { // block only if cond == false
		return &Stop{Cond: f.CondLabel}, nil
	}
	if ctx.t.CurStep == f.CondLabel {
		if !f.Cond {
			return nil, fmt.Errorf("resuming waiting condition that is false")
		}
		ctx.Running = true
	}
	return nil, nil
}

type ForStmt struct {
	CondLabel string
	Cond      bool // nil cond for infinite loop
	Stmt      Stmt
}

func For(cond bool, ss ...Stmt) Stmt {
	return ForStmt{
		Cond: cond,
		Stmt: Section(ss),
	}
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

		if ctx.t.Break {
			ctx.t.Break = false
			return nil, nil
		}

		b, err := f.Stmt.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
		return nil, fmt.Errorf("at least 1 stmt should be executed in For loop. Otherwise condition will never change and we have infinite async loop")
	}
	return nil, nil
}

type SelectStmt struct {
	Name  string
	Cases []WaitCond
}

// Select for multiple conditions and execute only one
func Select(name string, ss ...WaitCond) SelectStmt {
	return SelectStmt{
		Name:  name,
		Cases: ss,
	}
}

func safeResume(ctx *ResumeContext, s Stmt) (*Stop, error) {
	if s == nil {
		return nil, nil
	}
	return s.Resume(ctx)
}

func (s SelectStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		return &Stop{Select: &s}, nil
	}

	if s.Name == ctx.t.CurStep {
		ctx.Running = true
		for i := 0; i < len(ctx.t.WaitEvents); i++ {
			if ctx.t.WaitEvents[i].Req.Name == s.Name && ctx.t.WaitEvents[i].Status == EventSetup {
				ctx.t.WaitEvents[i].Status = EventPendingTeardown
			}
		}
		for _, c := range s.Cases {
			if c.Callback.Name == ctx.Callback.Name {
				if c.Handler != nil {
					out, err := c.Handler.Handle(ctx.ctx, ctx.Callback, ctx.CallbackInput)
					if err != nil {
						return nil, err
					}
					ctx.CallbackOutput = out
				}
				return safeResume(ctx, c.Stmt)
			}
		}
		return nil, fmt.Errorf("callback %v for case %v  not found", ctx.Callback.Name, ctx.t.CurStep)
	}

	for _, v := range s.Cases {
		b, err := safeResume(ctx, v.Stmt)
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
	Setup(ctx context.Context, req CallbackRequest) (json.RawMessage, error)

	Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error)

	Teardown(ctx context.Context, req CallbackRequest) error
}

type WaitCond struct {
	Callback CallbackRequest
	Handler  Handler
	Stmt     Stmt
}

func On(event string, handler Handler, stmts ...Stmt) WaitCond {
	return WaitCond{
		Callback: CallbackRequest{
			Name: event,
		},
		Stmt:    Section(stmts),
		Handler: handler,
	}
}

type BreakStmt struct {
}

func (s BreakStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if !ctx.Running {
		return nil, nil
	}
	ctx.t.Break = true
	return nil, nil
}

// Break for loop
func Break() BreakStmt {
	return BreakStmt{}
}

type ReturnStmt struct {
}

func (s ReturnStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if !ctx.Running {
		return nil, nil
	}
	return &Stop{
		Return: true,
	}, nil
}

// Return stops this trhead
func Return() ReturnStmt {
	return ReturnStmt{}
}

type GoStmt struct {
	// ID is needed to identify threads when there are many threads running with the same name.
	// (for example they were created in a loop)
	ID   func() string
	Name string
	Stmt Stmt
}

func Go(name string, body Stmt) *GoStmt {
	return &GoStmt{
		ID: func() string {
			return name + "_threadID"
		},
		Name: name,
		Stmt: body,
	}
}

func (s *GoStmt) WithID(id func() string) *GoStmt {
	s.ID = id
	return s
}

func (s *GoStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	if ctx.Running {
		return nil, ctx.s.Threads.Add(&Thread{
			ID:     s.ID(),
			Name:   s.Name,
			Status: ThreadResuming,
		})
	}
	return s.Stmt.Resume(ctx)
}

func FindStep(name string, sec Stmt) (*StmtStep, error) {
	var ret StmtStep
	_, err := Walk(sec, func(s Stmt) bool {
		switch x := s.(type) {
		case StmtStep:
			if x.Name == name {
				ret = x
				return true
			}
		}
		return false
	})
	return &ret, err
}

func FindWaitingStep(name string, sec Stmt) (WaitStmt, error) {
	var ret WaitStmt
	_, err := Walk(sec, func(s Stmt) bool {
		switch x := s.(type) {
		case WaitStmt:
			if x.CondLabel == name {
				ret = x
				return true
			}
		}
		return false
	})
	return ret, err
}

func Walk(s Stmt, f func(s Stmt) bool) (bool, error) {
	if f(s) {
		return true, nil
	}
	switch x := s.(type) {
	case nil:
		return false, nil
	case ReturnStmt:
		return false, nil
	case BreakStmt:
		return false, nil
	case StmtStep:
		return false, nil
	case WaitStmt:
		return false, nil
	case SelectStmt:
		for _, v := range x.Cases {
			stop, err := Walk(v.Stmt, f)
			if err != nil || stop {
				return stop, err
			}
		}
	case *GoStmt:
		stop, err := Walk(x.Stmt, f)
		if err != nil || stop {
			return stop, err
		}
	case ForStmt:
		stop, err := Walk(x.Stmt, f)
		if err != nil || stop {
			return stop, err
		}
	case *SwitchStmt:
		for _, v := range x.Cases {
			stop, err := Walk(v.Stmt, f)
			if err != nil || stop {
				return stop, err
			}
		}
	case Section:
		for _, v := range x {
			stop, err := Walk(v, f)
			if err != nil || stop {
				return stop, err
			}
		}
	default:
		return true, fmt.Errorf("unknown statement: %v", reflect.TypeOf(s))
	}
	return false, nil
}

func FindHandler(req CallbackRequest, sec Stmt) (Handler, error) {
	var ret Handler
	_, err := Walk(sec, func(s Stmt) bool {
		switch x := s.(type) {
		case SelectStmt:
			for _, v := range x.Cases {
				if v.Callback.Name == req.Name {
					ret = v.Handler
					return true
				}
			}
		}
		return false
	})
	return ret, err
}

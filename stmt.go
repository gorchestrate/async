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

// resumeContext is used during workflow execution
// It contains resume input as well as current state of the execution.
type resumeContext struct {
	ctx context.Context
	s   *State
	t   *Thread // current thread to resume

	// Running means process is already resumed and we are executing statements.
	// process is not running - we are searching for the step we should resume from.
	Running bool
	Return  bool
}

// Stop tells us that syncronous part of the workflow has finished. It means we either:
type Stop struct {
	Step       string          // execute step
	WaitEvents *WaitEventsStmt // wait for Events
	Return     bool            // stop this thread
	Cond       string          // wait for cond
}

// Section is similar to code block {} with a list of statements.
type Section []Stmt

// S is a syntax sugar to properly indent statement sections when using gofmt
func S(ss ...Stmt) Section {
	return ss
}

// Stmt is async statement definition that should support workflow resuming & search.
type Stmt interface {
	// Resume continues execution of the process, based on resumeContext
	// If ctx.Running == true Resume should execute the statement or continue execution after.
	// If ctx.Running = false Resume should not execute the statement, but recursively search for the statement that needs to be resumed.
	// If it needs to be resumed - don't execute it, but continue execution from this statement.
	//
	// If stmt not found *Stop will be nil and ctx.Running will be false
	// If stmt is found, but process has finished - *Stop will be nil and ctx.Running will be true
	// Otherwise Resume should always return *Stop or err != nil
	Resume(ctx *resumeContext) (*Stop, error)
}

func (s Section) Resume(ctx *resumeContext) (*Stop, error) {
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
	Action func() error `json:"-"`
}

func (s StmtStep) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
	}{
		Type: "step",
		Name: s.Name,
	})
}

func (s StmtStep) Resume(ctx *resumeContext) (*Stop, error) {
	if ctx.Running {
		return &Stop{Step: s.Name}, nil
	}
	if ctx.t.CurStep == s.Name {
		ctx.Running = true
	}
	return nil, nil
}

// if action func returns error - step will be retried automatically.
func Step(name string, action func() error) StmtStep {
	return StmtStep{
		Name:   name,
		Action: action,
	}
}

type SwitchCase struct {
	Name    string
	Cond    bool
	Stmt    Stmt
	Default bool
}

func (s SwitchCase) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string
		Name    string
		Stmt    Stmt
		Default bool
	}{
		Type:    "case",
		Name:    s.Name,
		Stmt:    s.Stmt,
		Default: s.Default,
	})
}

type SwitchStmt struct {
	Cases []SwitchCase
}

func (s SwitchStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string
		Cases []SwitchCase
	}{
		Type:  "switch",
		Cases: s.Cases,
	})
}

// execute statements based on condition
func Switch(ss ...SwitchCase) *SwitchStmt {
	return &SwitchStmt{
		Cases: ss,
	}
}

func (s *SwitchStmt) Resume(ctx *resumeContext) (*Stop, error) {
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

func If(cond bool, name string, sec ...Stmt) *SwitchStmt {
	return &SwitchStmt{
		Cases: []SwitchCase{{
			Cond: cond,
			Name: name,
			Stmt: Section(sec),
		}},
	}
}

func (s *SwitchStmt) ElseIf(cond bool, name string, sec ...Stmt) *SwitchStmt {
	s.Cases = append(s.Cases, SwitchCase{
		Cond: cond,
		Name: name,
		Stmt: Section(sec),
	})
	return s
}

func (s *SwitchStmt) Else(name string, sec ...Stmt) *SwitchStmt {
	s.Cases = append(s.Cases, SwitchCase{
		Cond:    true,
		Name:    name,
		Stmt:    Section(sec),
		Default: true,
	})
	return s
}

func Case(cond bool, name string, sec Stmt) SwitchCase {
	return SwitchCase{
		Cond: cond,
		Name: name,
		Stmt: sec,
	}
}

func Default(name string, sec Stmt) SwitchCase {
	return SwitchCase{
		Name:    name,
		Cond:    true,
		Stmt:    sec,
		Default: true,
	}
}

type WaitCondStmt struct {
	Name    string
	Cond    bool
	Handler func() `json:"-"` // executed when cond is true.
}

func (s WaitCondStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
	}{
		Type: "waitCond",
		Name: s.Name,
		// TODO: get handler json schema and put it here
	})
}

// Wait statement wait for condition to be true.
func WaitFor(label string, cond bool, handler func()) WaitCondStmt {
	return WaitCondStmt{
		Cond:    cond,
		Name:    label,
		Handler: handler,
	}
}

func (f WaitCondStmt) Resume(ctx *resumeContext) (*Stop, error) {
	if ctx.Running && f.Cond {
		f.Handler() // if condition is already met - execute handler immediately
		return nil, nil
	}
	if ctx.Running && !f.Cond { // if condition is false - let's wait for it
		return &Stop{Cond: f.Name}, nil
	}
	if ctx.t.CurStep == f.Name {
		if !f.Cond {
			return nil, fmt.Errorf("resuming waiting condition that is false")
		}
		ctx.Running = true
	}
	return nil, nil
}

type ForStmt struct {
	Name    string
	Cond    bool // nil cond for infinite loop
	Section Section
}

func (s ForStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string
		Name    string
		Section Section
	}{
		Type:    "for",
		Name:    s.Name,
		Section: s.Section,
	})
}

func For(name string, cond bool, ss ...Stmt) Stmt {
	return ForStmt{
		Cond:    cond,
		Name:    name,
		Section: Section(ss),
	}
}

func (f ForStmt) Resume(ctx *resumeContext) (*Stop, error) {
	if !ctx.Running {
		b, err := f.Section.Resume(ctx)
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

		b, err := f.Section.Resume(ctx)
		if err != nil || b != nil {
			return b, err
		}
		return nil, fmt.Errorf("at least 1 stmt should be executed in For loop. Otherwise condition will never change and we have infinite async loop")
	}
	return nil, nil
}

type WaitEventsStmt struct {
	Name  string
	Cases []Event
}

func (s WaitEventsStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string
		Name  string
		Cases []Event
	}{
		Type:  "waitEvent",
		Name:  s.Name,
		Cases: s.Cases,
	})
}

// Wait for multiple events exclusively
func Wait(name string, ss ...Event) WaitEventsStmt {
	return WaitEventsStmt{
		Name:  name,
		Cases: ss,
	}
}

func safeResume(ctx *resumeContext, s Stmt) (*Stop, error) {
	if s == nil {
		return nil, nil
	}
	return s.Resume(ctx)
}

func (s WaitEventsStmt) Resume(ctx *resumeContext) (*Stop, error) {
	if ctx.Running {
		return &Stop{WaitEvents: &s}, nil
	}
	for _, c := range s.Cases {
		if s.Name == ctx.t.CurStep && c.Callback.Name == ctx.t.CurCallback {
			ctx.Running = true
			ctx.t.CurCallback = ""
			return safeResume(ctx, c.Stmt)
		}
		b, err := safeResume(ctx, c.Stmt)
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
	Type() string

	Setup(ctx context.Context, req CallbackRequest) (string, error)

	Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error)

	Teardown(ctx context.Context, req CallbackRequest, handled bool) error
}

type Event struct {
	Callback CallbackRequest
	Handler  Handler
	Stmt     Stmt
}

func (s Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string
		Name    string
		Stmt    Stmt
		Handler Handler
	}{
		Type:    "event",
		Name:    s.Callback.Name,
		Stmt:    s.Stmt,
		Handler: s.Handler,
	})
}

func On(event string, handler Handler, stmts ...Stmt) Event {
	return Event{
		Callback: CallbackRequest{
			Name: event,
		},
		Stmt:    Section(stmts),
		Handler: handler,
	}
}

type BreakStmt struct {
}

func (s BreakStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "break",
	})
}

func (s BreakStmt) Resume(ctx *resumeContext) (*Stop, error) {
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

type ContinueStmt struct {
}

func (s ContinueStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "break",
	})
}

func (s ContinueStmt) Resume(ctx *resumeContext) (*Stop, error) {
	if !ctx.Running {
		return nil, nil
	}
	ctx.t.Continue = true
	return nil, nil
}

// Continue for loop
func Continue() ContinueStmt {
	return ContinueStmt{}
}

type ReturnStmt struct {
}

func (s ReturnStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "return",
	})
}

func (s ReturnStmt) Resume(ctx *resumeContext) (*Stop, error) {
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
	ID   func() string `json:"-"`
	Name string
	Stmt Stmt
}

func (s GoStmt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
		Stmt Stmt
	}{
		Type: "go",
		Name: s.Name,
		Stmt: s.Stmt,
	})
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

func (s *GoStmt) Resume(ctx *resumeContext) (*Stop, error) {
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

func FindWaitingStep(name string, sec Stmt) (WaitCondStmt, error) {
	var ret WaitCondStmt
	_, err := Walk(sec, func(s Stmt) bool {
		switch x := s.(type) {
		case WaitCondStmt:
			if x.Name == name {
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
	case ContinueStmt:
		return false, nil
	case StmtStep:
		return false, nil
	case WaitCondStmt:
		return false, nil
	case WaitEventsStmt:
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
		stop, err := Walk(x.Section, f)
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
		case WaitEventsStmt:
			for _, v := range x.Cases {
				if v.Callback.Name == req.Name {
					ret = v.Handler
					return true
				}
			}
		}
		return false
	})
	if ret == nil {
		return nil, fmt.Errorf("callback handler %v not found", req)
	}
	return ret, err
}

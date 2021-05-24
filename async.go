package main

import (
	"encoding/json"
	"fmt"
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
	Definition() WorkflowDefinition // Return current workflow definition. This function can be called multiple times, so be careful with doing real code execution inside.
}

type WorkflowDefinition struct {
	// New is called when the workflow is created
	// It's also used to construct the API definition for input/output
	New Handler

	// Body is the asyncronous part of the workflow
	Body Section
}

// Handler is a generic function that is analyzed using reflection
// It's a convenient way to specify input/output types as well as the implementation
type Handler interface{}

// ResumeContext is used during workflow execution
// It contains resume input as well as current state of the execution.
type ResumeContext struct {
	s *State
	t *Thread

	newThreads map[string]*Thread

	Running        bool            // Running means process is already resumed and we are executing statements. If process is not running - we are searching for the step we should resume from.
	CurStep        string          // CurStep of the workflow we are resuming from.
	CallbackIndex  int             // In case we are resuming a Select - this is and index of the select case to resume
	CallbackInput  json.RawMessage // In case we are resuming a Select with a callback event - this is the data to unmarshall into callback function parameters via reflect.
	CallbackOutput json.RawMessage // In case we are resuming a Select with a callback event - this is the data to marshall back to client in case workflow was successfully saved.
	Break          bool            // Used for loop management
}

// Stop tells us that syncronous part of the workflow has finished. It means we either:
type Stop struct {
	Step   string      // waiting for step execution to complete
	Select *SelectStmt // waiting for event
	Return interface{} // returning from process

	//SubWorkflow string   // TODO: SubWorkflow support. Subworkflow is just a prefix to all statuses
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

type SwitchCase struct {
	CondLabel string
	Cond      bool
	Stmt      Stmt
}

type SwitchStmt []SwitchCase

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
		break
	}
	return nil, nil
}

func If(cond bool, condLabel string, sec Stmt) SwitchStmt {
	return SwitchStmt{
		SwitchCase{
			CondLabel: condLabel,
			Cond:      cond,
			Stmt:      sec,
		},
	}
}

func Case(cond bool, condLabel string, sec Stmt) SwitchCase {
	return SwitchCase{
		CondLabel: condLabel,
		Cond:      cond,
		Stmt:      sec,
	}
}

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
	if s.Name == ctx.CurStep {
		if ctx.CallbackIndex >= len(s.Cases) {
			return nil, fmt.Errorf("index out ouf bounds for select callback: %#v %v", *ctx, s.Cases)
		}
		ctx.Running = true
		resCase := s.Cases[ctx.CallbackIndex]
		if resCase.Handler != nil { // Execute syncronous handler for validation purposes
			err := reflectCall(resCase.Handler, ctx)
			if err != nil {
				return nil, fmt.Errorf("err during handler call: %v", err)
			}
		}
		return resCase.Stmt.Resume(ctx)
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
	CaseAfter time.Duration // wait for time
	CaseEvent string        // wait for event
	CaseRecv  string        // wait for receive channel
	CaseSend  string        // wait for send channels
	SendData  json.RawMessage

	Handler Handler

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
func On(event string, handler Handler, sec Stmt) WaitCond {
	return WaitCond{
		CaseEvent: event,
		Stmt:      sec,
		Handler:   handler,
	}
}

// After waits for specified time and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func WaitFor(name string, d time.Duration, sec ...Stmt) SelectStmt {
	return SelectStmt{
		Name: "waitfor-" + name,
		Cases: []WaitCond{{
			CaseAfter: d,
			Stmt:      Section(sec),
		}},
	}
}

// On waits for event to come and then resumes the workflow. If multiple conditions are specified - only one that is fired first will fire.
func WaitEvent(event string, handler Handler, sec ...Stmt) SelectStmt {
	return SelectStmt{
		Name: "select-" + event,
		Cases: []WaitCond{
			{
				CaseEvent: event,
				Stmt:      Section(sec),
				Handler:   handler,
			},
		},
	}
}

type BreakStmt struct {
}

func (s BreakStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	ctx.Break = true
	return nil, nil
}

func Break() BreakStmt {
	return BreakStmt{}
}

type ReturnStmt struct {
}

func (s ReturnStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	// TODO: return from function has to be scoped
	// especially for Go stmt
	return nil, nil
}

func Return() ReturnStmt {
	return ReturnStmt{}
}

type GoStmt struct {
	ID   func() string
	Name string // name of goroutine
	Stmt Stmt
}

func Go(name string, id func() string, body Stmt) GoStmt {
	return GoStmt{
		ID:   id,
		Name: name,
		Stmt: body,
	}
}

// When we meet Go stmt - we simply create threads and continue execution.
func (s GoStmt) Resume(ctx *ResumeContext) (*Stop, error) {
	ctx.newThreads[s.Name] = &Thread{
		ID:     s.ID(),
		Name:   s.Name,
		Status: ThreadResuming,
	}
	return nil, nil
}

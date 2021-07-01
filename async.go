package async

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type WaitEventStatus string

const (
	EventPending       WaitEventStatus = "Pending"       // event is just created
	EventSetup         WaitEventStatus = "Setup"         // event was successfully setup
	EventSetupError    WaitEventStatus = "SetupError"    // there was an error during setup
	EventTeardownError WaitEventStatus = "TeardownError" // there was an error during teardown
)

type WaitEvent struct {
	Req    CallbackRequest
	Status string
	Error  string
}

type State struct {
	ID       string         // id of workflow instance
	Workflow string         // name of workflow definition. Used to choose proper state type to unmarshal & resume on
	State    interface{}    // json body of workflow state
	Status   WorkflowStatus // current status
	Input    interface{}    // json input of the workflow
	Output   interface{}    // json output of the finished workflow. Valid only if Status = Finished
	LockTill time.Time      // optimistic locking
	Threads  Threads
	PC       int
}

type Thread struct {
	ID          string
	Name        string
	Status      ThreadStatus // current status
	CurStep     string       // current step of the workflow
	ExecRetries int          // TODO: better retries approach
	ExecError   string       // TODO: better retries approach
	ExecBackoff time.Time    // TODO: better retries approach
	WaitEvents  []WaitEvent  // events workflow is waiting for. Valid only if Status = Waiting, otherwise should be empty.
	PC          int
}

type Threads []*Thread

func (tt *Threads) Add(t *Thread) {
	tr := *tt
	for _, t2 := range tr {
		if t.ID == t2.ID && t.Name == t2.Name {
			panic("duplicate thread is created " + t.ID)
		}
	}
	*tt = append(tr, t)
}

func (tt *Threads) Remove(id string) {
	tr := *tt
	for i, t := range tr {
		if t.ID == id {
			tr = append(tr[:i], tr[i+1:]...)
		}
	}
	*tt = tr
}

func (tt *Threads) Find(id string) (*Thread, bool) {
	for _, t := range *tt {
		if t.ID == id {
			return t, true
		}
	}
	return nil, false
}

type WorkflowStatus string
type ThreadStatus string

const (
	WorkflowRunning  WorkflowStatus = "Running"
	WorkflowWaiting  WorkflowStatus = "Waiting"
	WorkflowFinished WorkflowStatus = "Finished"
)

const (
	ThreadExecuting ThreadStatus = "Executing"
	ThreadResuming  ThreadStatus = "Resuming"
	ThreadWaiting   ThreadStatus = "Waiting"
	ThreadPaused    ThreadStatus = "Paused"
)

type UpdaterFunc func(ctx context.Context, s *State, save func() error) error

type DB interface {
	Create(ctx context.Context, id string, s *State) error
	RunLocked(ctx context.Context, id string, f UpdaterFunc) error
}

// Resumer makes sure that workflow will be resumed:
// - after workflow was created
// - after callback was handled
// - after previous resume didn't fully finish
// - after any failures during processing
type Resumer interface {
	// ScheduleResume should schedule workflow for resume.
	// After that it should try to resume workflow multiple times until it succeeds
	// To make sure there there are no zombie workflows in DB, ScheduleResume can be called:
	// - before workflow was added to DB
	// - before new workflow state was saved to DB
	// That's why errors can happen during Resume() calls and they should be retried.
	// You'd probably want to log them and monitor workflows that are stuck in certain states.
	ScheduleResume(r *Runner, id string) error
}

type Runner struct {
	Workflows        map[string]Workflow
	CallbackManagers map[string]CallbackManager
	Resumer          Resumer
	DB               DB
}

type CallbackManager interface {
	Setup(req CallbackRequest) error
	Teardown(req CallbackRequest) error
}

var ErrPCMismatch = fmt.Errorf("PC doesn't match (callback is late)")

// Resume state with specified thread. Thread can be resumed in following cases:
// 1. Thread just started
// 2. Thread timed out - unblocked on time select
// 3. Thread unblocked on select - (i.e. handler/event was triggered)
// 4. Step has finished execution and we are resuming thread from that spot.
func (r *Runner) ResumeState(ctx *ResumeContext) (WorkflowState, error) {
	if ctx.s.Status != WorkflowRunning {
		return nil, fmt.Errorf("unexpected status for state: %v", ctx.s.Status)
	}
	wf, ok := r.Workflows[ctx.s.Workflow]
	if !ok {
		return nil, fmt.Errorf("workflow not found: %v", ctx.s.Workflow)
	}
	state := wf()
	di, err := json.Marshal(ctx.s.State)
	if err != nil {
		return nil, fmt.Errorf("err marshal/unmarshal state")
	}
	err = json.Unmarshal(di, &state)
	if err != nil {
		return nil, fmt.Errorf("state unmarshal err: %v", err)
	}
	if ctx.t.CurStep == "" { // thread has just started, let's make it running
		ctx.Running = true
	}
	resumeThread := Stmt(state.Definition())
	if ctx.t.Name != "_main_" {
		Walk(state.Definition(), func(s Stmt) bool {
			gStmt, ok := s.(GoStmt)
			if ok && gStmt.Name == ctx.t.Name {
				resumeThread = gStmt.Stmt
				return true
			}
			return false
		})
	}
	stop, err := resumeThread.Resume(ctx)
	if err != nil {
		return state, fmt.Errorf("err during workflow execution: %v", err)
	}
	if stop == nil && !ctx.Running {
		return state, fmt.Errorf("callback not found: %v", ctx)
	}

	// workflow finished
	// Workflow definition should always explicitly return result.
	// You should only be allowed to return from main thread
	if stop != nil && stop.Return != nil && ctx.t.ID == "_main_" {
		log.Printf("MAIN THREAD FINISHED!!!")
		ctx.s.Status = "Finished"
		ctx.s.Output = stop.Return
		return state, nil
	}

	// thread returned
	if stop != nil && stop.Return != nil {
		ctx.s.Threads.Remove(ctx.t.ID)
		return state, nil
	}

	// thread has finished
	if stop == nil && ctx.Running {
		ctx.s.Threads.Remove(ctx.t.ID)
		return state, nil
	}

	// blocked on step
	if stop.Step != "" {
		ctx.t.CurStep = stop.Step
		ctx.t.Status = "Executing"
		return state, nil
	}

	// blocked on select
	ctx.t.CurStep = stop.Select.Name
	for _, c := range stop.Select.Cases {
		c.Callback.PC = ctx.t.PC
		c.Callback.WorkflowID = ctx.s.ID
		c.Callback.ThreadID = ctx.t.ID
		ctx.t.WaitEvents = append(ctx.t.WaitEvents, WaitEvent{Req: c.Callback, Status: "Pending"})
	}
	ctx.t.Status = "Waiting"
	return state, nil
}

type CallbackRequest struct {
	Type       string
	WorkflowID string
	ThreadID   string
	Name       string
	PC         int
	Data       json.RawMessage
}

func (r *Runner) NewWorkflow(ctx context.Context, id, name string, state interface{}) error {
	s := &State{
		ID:       id,
		Workflow: name,
		State:    state,
		Status:   WorkflowRunning,
		Output:   nil,
		Threads: []*Thread{
			{
				ID:         "_main_",
				Name:       "_main_",
				Status:     ThreadResuming,
				WaitEvents: []WaitEvent{},
			},
		},
	}
	err := r.Resumer.ScheduleResume(r, id)
	if err != nil {
		return err
	}
	return r.DB.Create(ctx, id, s)
}

func (r *Runner) resume(ctx context.Context, s *State) (found bool, err error) {
	for _, t := range s.Threads {
		switch t.Status {
		case ThreadExecuting:
			wf, ok := r.Workflows[s.Workflow]
			if !ok {
				return true, fmt.Errorf("workflow not found: %v", s.Workflow)
			}
			state := wf()
			di, err := json.Marshal(s.State)
			if err != nil {
				return true, fmt.Errorf("err marshal/unmarshal state")
			}
			err = json.Unmarshal(di, &state)
			if err != nil {
				return true, fmt.Errorf("state unmarshal err: %v", err)
			}
			step, ok := FindStep(t.CurStep, state.Definition()).(StmtStep)
			if !ok {
				return true, fmt.Errorf("can't find step %v", t.CurStep)
			}
			t.PC++
			s.PC++
			res := step.Action()
			if res.Success {
				t.Status = "Resuming"
				t.ExecError = ""
				t.ExecRetries = 0
				t.ExecBackoff = time.Time{}
			} else if t.ExecRetries < res.Retries { // retries available
				t.ExecError = res.Error
				t.ExecRetries++
				t.ExecBackoff = time.Now().Add(res.RetryInterval)
			} else {
				t.Status = "Paused"
			}
			s.State = state
			return true, nil //r.ScheduleTimeoutTasks(ctx, s, t)
		case ThreadResuming:
			rCtx := &ResumeContext{
				s: s,
				t: t,
				//CallbackName: resReq.Callback,
				Running: false,
			}
			t.PC++
			s.PC++
			state, err := r.ResumeState(rCtx)
			if err != nil {
				return true, err
			}
			s.State = state
			return true, nil //r.ScheduleTimeoutTasks(ctx, s, t)
		case ThreadWaiting:
			// ignore callbacks
			continue
		case ThreadPaused:
			return true, fmt.Errorf("TODO: formalize paused threads")

		}
	}
	return false, nil
}

func (t *Thread) WaitingForCallback(cb CallbackRequest) bool {
	for _, evt := range t.WaitEvents {
		if evt.Req.Name == cb.Name && evt.Status == "Setup" {
			return true
		}
	}
	return false
}

func (r *Runner) Resume(ctx context.Context, dur time.Duration, steps int, id string) error {
	start := time.Now()
	return r.DB.RunLocked(ctx, id, func(ctx context.Context, s *State, save func() error) error {
		if s.Status == WorkflowFinished {
			log.Printf("workflow has finished, skipping resume")
			return nil
		}

		// before resuming workflow - make sure all previous teardowns are executed
		for _, t := range s.Threads {
			for i := 0; i < len(t.WaitEvents); i++ {
				if t.WaitEvents[i].Status != "Setup" {
					continue
				}
				mgr, ok := r.CallbackManagers[t.WaitEvents[i].Req.Type]
				if !ok {
					t.WaitEvents[i].Status = "TeardownError"
					t.WaitEvents[i].Error = fmt.Sprintf("callback manager not found: %v", t.WaitEvents[i].Req.Type)
					save()
					continue
				}
				err := mgr.Teardown(t.WaitEvents[i].Req)
				if err != nil {
					t.WaitEvents[i].Status = "TeardownError"
					t.WaitEvents[i].Error = err.Error()
					save()
					continue
				}
				t.WaitEvents = append(t.WaitEvents[:i], t.WaitEvents[i+1:]...) // remove successful teardown
				save()
			}
		}

		for i := 0; i < steps && s.Status == WorkflowRunning; i++ {
			found, err := r.resume(ctx, s)
			if err != nil {
				return fmt.Errorf("err during resume: %v", err)
			}
			if !found {
				break
			}
			err = save()
			if err != nil {
				return err
			}

			// we are running for long time, let's schedule another resume
			if time.Since(start) > dur/2 {
				err := r.Resumer.ScheduleResume(r, id)
				if err != nil {
					return err
				}
				return nil
			}
		}

		if s.Status == WorkflowRunning { // in case process didn't resume on X steps - schedule another one
			// TODO: stop process with LOOP error
		}

		// after resume is blocked - setup callbacks
		for _, t := range s.Threads {
			for i := 0; i < len(t.WaitEvents); i++ {
				if t.WaitEvents[i].Status != "Pending" {
					continue
				}
				mgr, ok := r.CallbackManagers[t.WaitEvents[i].Req.Type]
				if !ok {
					t.WaitEvents[i].Status = "SetupError"
					t.WaitEvents[i].Error = fmt.Sprintf("callback manager not found: %v", t.WaitEvents[i].Req.Type)
					save()
					continue
				}
				err := mgr.Setup(t.WaitEvents[i].Req)
				if err != nil {
					t.WaitEvents[i].Status = "SetupError"
					t.WaitEvents[i].Error = err.Error()
					save()
					continue
				}
				t.WaitEvents[i].Status = "Setup"
				save()
			}
		}
		return nil
	})
}

func (r *Runner) OnCallback(ctx context.Context, req CallbackRequest) error {
	log.Printf("On callback: %v", req)
	return r.DB.RunLocked(ctx, req.WorkflowID, func(ctx context.Context, s *State, save func() error) error {
		t, ok := s.Threads.Find(req.ThreadID)
		if !ok {
			return fmt.Errorf("thread %v not found", req.ThreadID)
		}
		if req.PC != t.PC {
			return fmt.Errorf("callback PC mismatch: got %v, expected %v : request: %v ", req, req.PC, t.PC)
		}
		if !t.WaitingForCallback(req) {
			return fmt.Errorf("thread is not waiting for callback: %v", req.ThreadID)
		}
		rCtx := &ResumeContext{
			s:        s,
			t:        t,
			Callback: req,
			Running:  false,
		}
		state, err := r.ResumeState(rCtx)
		if err != nil {
			return err
		}
		t.PC++
		s.PC++
		s.State = state
		err = r.Resumer.ScheduleResume(r, req.WorkflowID)
		if err != nil {
			return err
		}
		return save()
	})
}

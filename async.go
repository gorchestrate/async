package async

import (
	"context"
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
	Status   WorkflowStatus // current status
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

type Runner struct {
	CallbackManagers map[string]CallbackManager
}

type CallbackManager interface {
	Setup(ctx context.Context, req CallbackRequest) error
	Teardown(ctx context.Context, req CallbackRequest) error
}

type EventLookupFunc func(s *State) (CallbackRequest, error)

type Engine interface {
	OnResume(ctx context.Context, id string) error
	OnCallback(ctx context.Context, id string, lkp EventLookupFunc, input interface{}) (interface{}, error)
}

type Scheduler interface {
	Schedule(ctx context.Context, id string) error
}

// Resume state with specified thread. Thread can be resumed in following cases:
// 1. Thread just started
// 2. Thread timed out - unblocked on time select
// 3. Thread unblocked on select - (i.e. handler/event was triggered)
// 4. Step has finished execution and we are resuming thread from that spot.
func (r *Runner) ResumeState(ctx *ResumeContext, state WorkflowState) error {
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
		return fmt.Errorf("err during workflow execution: %v", err)
	}
	if stop == nil && !ctx.Running {
		return fmt.Errorf("callback not found: %v", ctx)
	}

	// workflow finished
	// Workflow definition should always explicitly return result.
	// You should only be allowed to return from main thread
	if stop != nil && stop.Return && ctx.t.ID == "_main_" {
		log.Printf("MAIN THREAD FINISHED!!!")
		ctx.s.Status = "Finished"
		return nil
	}

	// thread returned
	if stop != nil && stop.Return {
		ctx.s.Threads.Remove(ctx.t.ID)
		return nil
	}

	// thread has finished
	if stop == nil && ctx.Running {
		ctx.s.Threads.Remove(ctx.t.ID)
		return nil
	}

	// blocked on step
	if stop.Step != "" {
		ctx.t.CurStep = stop.Step
		ctx.t.Status = "Executing"
		return nil
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
	return nil
}

type CallbackRequest struct {
	Type       string
	WorkflowID string
	ThreadID   string
	Name       string
	PC         int
	Handler    interface{} `json:"-"`
}

func NewState(id, name string) State {
	return State{
		ID:       id,
		Workflow: name,
		Status:   WorkflowRunning,
		Threads: []*Thread{
			{
				ID:         "_main_",
				Name:       "_main_",
				Status:     ThreadResuming,
				WaitEvents: []WaitEvent{},
			},
		},
	}
}

func (r *Runner) resume(ctx context.Context, state WorkflowState, s *State) (found bool, err error) {
	for _, t := range s.Threads {
		switch t.Status {
		case ThreadExecuting:
			step, ok := FindStep(t.CurStep, state.Definition()).(StmtStep)
			if !ok {
				return true, fmt.Errorf("can't find step %v", t.CurStep)
			}
			t.PC++
			s.PC++
			res := step.Action()
			if res != nil { // TODO: more sophisticated error handling with retries and recovery
				t.Status = "Resuming"
				t.ExecError = ""
				t.ExecRetries = 0
				t.ExecBackoff = time.Time{}
			} else {
				t.Status = "Paused"
			}
			return true, nil //r.ScheduleTimeoutTasks(ctx, s, t)
		case ThreadResuming:
			rCtx := &ResumeContext{
				ctx: ctx,
				s:   s,
				t:   t,
				//CallbackName: resReq.Callback,
				Running: false,
			}
			t.PC++
			s.PC++
			err := r.ResumeState(rCtx, state)
			if err != nil {
				return true, err
			}
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

type Checkpoint func(scheduleResume bool) error

func (r *Runner) OnResume(ctx context.Context, wf WorkflowState, s *State, save Checkpoint) error {
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
			if t.WaitEvents[i].Req.Type != "" { // some handlers may not need setup/teardown
				mgr, ok := r.CallbackManagers[t.WaitEvents[i].Req.Type]
				if !ok {
					t.WaitEvents[i].Status = "TeardownError"
					t.WaitEvents[i].Error = fmt.Sprintf("callback manager not found: %v", t.WaitEvents[i].Req.Type)
					save(false)
					continue
				}
				err := mgr.Teardown(ctx, t.WaitEvents[i].Req)
				if err != nil {
					t.WaitEvents[i].Status = "TeardownError"
					t.WaitEvents[i].Error = err.Error()
					save(false)
					continue
				}
			}
			t.WaitEvents = append(t.WaitEvents[:i], t.WaitEvents[i+1:]...) // remove successful teardown
			save(false)
		}
	}

	for i := 0; i < 10000 && s.Status == WorkflowRunning; i++ {
		found, err := r.resume(ctx, wf, s)
		if err != nil {
			return fmt.Errorf("err during resume: %v", err)
		}
		if !found {
			break
		}
		err = save(false)
		if err != nil {
			return err
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
			if t.WaitEvents[i].Req.Type != "" { // some handlers may not need setup/teardown
				mgr, ok := r.CallbackManagers[t.WaitEvents[i].Req.Type]
				if !ok {
					t.WaitEvents[i].Status = "SetupError"
					t.WaitEvents[i].Error = fmt.Sprintf("callback manager not found: %v", t.WaitEvents[i].Req.Type)
					save(false)
					continue
				}
				err := mgr.Setup(ctx, t.WaitEvents[i].Req)
				if err != nil {
					t.WaitEvents[i].Status = "SetupError"
					t.WaitEvents[i].Error = err.Error()
					save(false)
					continue
				}
			}
			t.WaitEvents[i].Status = "Setup"
			save(false)
		}
	}
	return nil
}

func (r *Runner) OnCallback(ctx context.Context, req CallbackRequest, wf WorkflowState, s *State, input interface{}, save Checkpoint) (interface{}, error) {
	t, ok := s.Threads.Find(req.ThreadID)
	if !ok {
		return nil, fmt.Errorf("thread %v not found", req.ThreadID)
	}
	if req.PC != t.PC {
		return nil, fmt.Errorf("callback PC mismatch: got %v, expected %v : request: %v ", req, req.PC, t.PC)
	}
	if !t.WaitingForCallback(req) {
		return nil, fmt.Errorf("thread is not waiting for callback: %v", req.ThreadID)
	}
	rCtx := &ResumeContext{
		ctx:           ctx,
		s:             s,
		t:             t,
		Callback:      req,
		CallbackInput: input,
		Running:       false,
	}
	if rCtx.s.Status != WorkflowRunning {
		return nil, fmt.Errorf("unexpected status for state: %v", rCtx.s.Status)
	}
	err := r.ResumeState(rCtx, wf)
	if err != nil {
		return nil, err
	}
	t.PC++
	s.PC++
	return rCtx.CallbackOutput, save(true)

}

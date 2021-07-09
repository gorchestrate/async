package async

import (
	"context"
	"encoding/json"
	"fmt"
)

type WaitEvent struct {
	Req    CallbackRequest
	Status WaitEventStatus
	Error  string
}

type WaitEventStatus string

const (
	EventPending       WaitEventStatus = "Pending"       // event is just created
	EventSetup         WaitEventStatus = "Setup"         // event was successfully setup
	EventSetupError    WaitEventStatus = "SetupError"    // there was an error during setup
	EventTeardownError WaitEventStatus = "TeardownError" // there was an error during teardown
)

type State struct {
	ID       string         // id of workflow instance
	Workflow string         // name of workflow definition. Used to choose proper state type to unmarshal & resume on
	Status   WorkflowStatus // current status
	Threads  Threads
	PC       int
}

type WorkflowStatus string

const (
	WorkflowRunning  WorkflowStatus = "Running"
	WorkflowFinished WorkflowStatus = "Finished"
)

type Thread struct {
	ID         string
	Name       string
	Status     ThreadStatus // current status
	CurStep    string       // current step of the workflow
	WaitEvents []WaitEvent  // events workflow is waiting for. Valid only if Status = Waiting, otherwise should be empty.
	PC         int
}

const MainThread = "_main_"

type ThreadStatus string

const (
	ThreadExecuting ThreadStatus = "Executing" // next step for this thread is to execute "CurStep" step
	ThreadResuming  ThreadStatus = "Resuming"  // next step for this thread is to continue from "CurStep" step
	ThreadWaiting   ThreadStatus = "Waiting"   // thread is waiting for "CurStep" wait condition and will be resumed via OnCallback()
)

type Threads []*Thread

func (tt *Threads) Add(t *Thread) error {
	tr := *tt
	for _, t2 := range tr {
		if t.ID == t2.ID && t.Name == t2.Name {
			return fmt.Errorf("duplicate thread is created " + t.ID)
		}
	}
	*tt = append(tr, t)
	return nil
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

func resumeState(ctx *ResumeContext, state WorkflowState) error {
	if ctx.t.CurStep == "" { // thread has just started, let's make it running
		ctx.Running = true
	}
	resumeThread := Stmt(state.Definition())
	// if we are resuming non-main thread - find it's definition in the AST
	if ctx.t.Name != MainThread {
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

	// workflow finished (i.e. main thread finished)
	if stop != nil && stop.Return && ctx.t.ID == MainThread {
		ctx.s.Status = WorkflowFinished
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
		ctx.t.Status = ThreadExecuting
		return nil
	}

	// blocked on select
	ctx.t.CurStep = stop.Select.Name
	for _, c := range stop.Select.Cases {
		c.Callback.PC = ctx.t.PC
		c.Callback.WorkflowID = ctx.s.ID
		c.Callback.ThreadID = ctx.t.ID
		ctx.t.WaitEvents = append(ctx.t.WaitEvents, WaitEvent{Req: c.Callback, Status: EventPending})
	}
	ctx.t.Status = ThreadWaiting
	return nil
}

type CallbackRequest struct {
	WorkflowID string
	ThreadID   string
	Name       string
	PC         int
	SetupData  json.RawMessage
}

func NewState(id, name string) State {
	return State{
		ID:       id,
		Workflow: name,
		Status:   WorkflowRunning,
		Threads: []*Thread{
			{
				ID:         MainThread,
				Name:       MainThread,
				Status:     ThreadResuming,
				WaitEvents: []WaitEvent{},
			},
		},
	}
}

func resumeOnce(ctx context.Context, state WorkflowState, s *State) (found bool, err error) {
	for _, t := range s.Threads {
		switch t.Status {
		case ThreadExecuting:
			step, ok := FindStep(t.CurStep, state.Definition()).(StmtStep)
			if !ok {
				return true, fmt.Errorf("can't find step %v", t.CurStep)
			}
			t.PC++
			s.PC++
			err := step.Action()
			if err != nil {
				return true, fmt.Errorf("err during step %v execution: %v", t.CurStep, err)
			}
			t.Status = ThreadResuming
			return true, nil
		case ThreadResuming:
			rCtx := &ResumeContext{
				ctx:     ctx,
				s:       s,
				t:       t,
				Running: false,
			}
			t.PC++
			s.PC++
			err := resumeState(rCtx, state)
			if err != nil {
				return true, err
			}
			return true, nil
		case ThreadWaiting:
			// thread that are waiting fore event don't need to be resumed
			continue
		}
	}
	return false, nil
}

func (t *Thread) WaitingForCallback(cb CallbackRequest) error {
	for _, evt := range t.WaitEvents {
		if evt.Req.Name != cb.Name {
			continue
		}
		if evt.Status == EventSetup || evt.Status == EventPending {
			return nil
		}
		return fmt.Errorf("got callback on event with unexpected status: %v", evt.Status)
	}
	return fmt.Errorf("thead %v is not waiting for callback %v", t.ID, cb.Name)
}

type Checkpoint func(scheduleResume bool) error

// Resume the workflow after
// - workflow creation
// - successful callback handling
// - previously failed Resume() call
//
// This method can be called multiple times. If there's nothing to resume - it will return 'nil'
func Resume(ctx context.Context, wf WorkflowState, s *State, save Checkpoint) error {
	if s.Status == WorkflowFinished {
		return nil
	}

	//before resuming workflow - make sure all previous teardowns are executed
	for _, t := range s.Threads {
		for i := 0; i < len(t.WaitEvents); i++ {
			if t.WaitEvents[i].Status == EventPending { // event wasn't setup yet, no need to call Teardown
				t.WaitEvents = append(t.WaitEvents[:i], t.WaitEvents[i+1:]...) // remove successful teardown
				err := save(false)
				if err != nil {
					return err
				}
			}
			h, err := FindHandler(t.WaitEvents[i].Req, wf.Definition())
			if err != nil {
				return fmt.Errorf("can' find handler: %v", err)
			}
			if h == nil {
				t.WaitEvents[i].Status = EventTeardownError
				t.WaitEvents[i].Error = fmt.Sprintf("callback handler not found: %v", t.WaitEvents[i].Req.Name)
				err := save(false)
				if err != nil {
					return err
				}
				continue
			}
			err = h.Teardown(ctx, t.WaitEvents[i].Req)
			if err != nil {
				t.WaitEvents[i].Status = EventTeardownError
				t.WaitEvents[i].Error = err.Error()
				err := save(false)
				if err != nil {
					return err
				}
				continue
			}
			t.WaitEvents = append(t.WaitEvents[:i], t.WaitEvents[i+1:]...) // remove successful teardown
			err = save(false)
			if err != nil {
				return err
			}
		}
	}

	for i := 0; s.Status == WorkflowRunning; i++ {
		found, err := resumeOnce(ctx, wf, s)
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
		if i > 10000 {
			return fmt.Errorf("resume didn't finish after 10k steps")
		}
	}

	for _, t := range s.Threads {
		for i := 0; i < len(t.WaitEvents); i++ {
			if t.WaitEvents[i].Status != EventPending {
				continue
			}
			h, err := FindHandler(t.WaitEvents[i].Req, wf.Definition())
			if err != nil {
				return fmt.Errorf("can' find handler: %v", err)
			}
			if h == nil {
				t.WaitEvents[i].Status = EventSetupError
				t.WaitEvents[i].Error = fmt.Sprintf("callback handler not found: %v", t.WaitEvents[i].Req.Name)
				err := save(false)
				if err != nil {
					return err
				}
				continue
			}
			d, err := h.Setup(ctx, t.WaitEvents[i].Req)
			if err != nil {
				t.WaitEvents[i].Status = EventSetupError
				t.WaitEvents[i].Error = err.Error()
				t.WaitEvents[i].Req.SetupData = d
				err := save(false)
				if err != nil {
					return err
				}
				continue
			}
			t.WaitEvents[i].Status = EventSetup
			err = save(false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func HandleCallback(ctx context.Context, req CallbackRequest, wf WorkflowState, s *State, input interface{}, save Checkpoint) (interface{}, error) {
	if s.Status != WorkflowRunning {
		return nil, fmt.Errorf("received callback on workflow that is not running: %v", s.Status)
	}
	t, ok := s.Threads.Find(req.ThreadID)
	if !ok {
		return nil, fmt.Errorf("thread %v not found", req.ThreadID)
	}

	// In case our callback was waiting within a loop - we make sure that callback was waiting for specific event.
	// If we don't care about this - we can use HandleEvent() function.
	if req.PC != t.PC {
		return nil, fmt.Errorf("callback PC mismatch: got %v, expected %v : request: %v ", req, req.PC, t.PC)
	}

	err := t.WaitingForCallback(req)
	if err != nil {
		return nil, err
	}

	// When we receive valid callback - it may arrive before Setup() events were called.
	// This could happen if Resume() execution was interrupted in the middle and then callback arrived.
	// Therefore we should first try to resume the state to make sure all Setup() functions were executed first
	// and only then handle the callback.
	// We will add retries for Setup() and Teardown() in future to make sure they are always executed.
	// If everything was fine - Resume will not execute anything
	err = Resume(ctx, wf, s, save)
	if err != nil {
		return nil, fmt.Errorf("err resuming state before handling callback: %v", err)
	}
	rCtx := &ResumeContext{
		ctx:           ctx,
		s:             s,
		t:             t,
		Callback:      req,
		CallbackInput: input,
		Running:       false,
	}
	err = resumeState(rCtx, wf)
	if err != nil {
		return nil, err
	}
	t.PC++
	s.PC++
	return rCtx.CallbackOutput, save(true)

}

// HandleEvent is shortcut for calling HandleCallback() by event name.
// Be careful - if you have events with the same name waiting - you will not have
// control over which event will be called back. If this is important for you - you should use OnCallback() instead
func HandleEvent(ctx context.Context, name string, wf WorkflowState, s *State, input interface{}, save Checkpoint) (interface{}, error) {
	var req CallbackRequest
	for _, tr := range s.Threads {
		for _, e := range tr.WaitEvents {
			if e.Req.Name == name && e.Status == EventPending || e.Status == EventSetup {
				req = e.Req
			}
		}
	}
	if req.Name == "" {
		return nil, fmt.Errorf("event %v not found", name)
	}
	return HandleCallback(ctx, req, wf, s, input, save)
}

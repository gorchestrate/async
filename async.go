package async

import (
	"context"
	"fmt"
)

type WaitEvent struct {
	Type    string
	Req     CallbackRequest
	Status  WaitEventStatus
	Handled bool
	Error   string
}

// Event statuses are needed to make sure that the process of setting up,
// tearing down and error handling do not interfere with each other.
// So if setup or teardown of one event fails - we don't retry other events
type WaitEventStatus string

const (
	EventPendingSetup    WaitEventStatus = "PendingSetup"    // event is just created
	EventSetup           WaitEventStatus = "Setup"           // event was successfully setup
	EventPendingTeardown WaitEventStatus = "PendingTeardown" // event was successfully setup
	EventSetupError      WaitEventStatus = "SetupError"      // there was an error during setup
	EventTeardownError   WaitEventStatus = "TeardownError"   // there was an error during teardown
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
	WorkflowResuming WorkflowStatus = "Resuming" // at least 1 thread is not waiting
	WorkflowWaiting  WorkflowStatus = "Waiting"  // all threads are waiting
	WorkflowFinished WorkflowStatus = "Finished"
)

type Thread struct {
	ID          string
	Name        string
	Status      ThreadStatus // current status
	CurStep     string       // current step
	CurCallback string
	WaitEvents  []WaitEvent // waiting for Select() stmt conditions
	Break       bool
	Continue    bool
	PC          int
}

const MainThread = "_main_"

type ThreadStatus string

const (
	ThreadExecuting        ThreadStatus = "Executing"        // next step for this thread is to execute "CurStep" step
	ThreadResuming         ThreadStatus = "Resuming"         // next step for this thread is to continue from "CurStep" step
	ThreadWaitingEvent     ThreadStatus = "WaitingEvent"     // thread is waiting for "CurStep" wait condition and will be resumed via OnCallback()
	ThreadWaitingCondition ThreadStatus = "WaitingCondition" // thread is waiting for condition to happen. i.e. it's waiting for other thread to update some data

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

// TODO: more robust validations
func Validate(s Section) error {
	var oErr error
	sections := map[string]bool{}
	_, err := Walk(s, func(s Stmt) bool {
		switch x := s.(type) {
		case StmtStep:
			if sections[x.Name] {
				oErr = fmt.Errorf("duplicate step name: %v", x.Name)
				return true
			}
			sections[x.Name] = true
		case WaitCondStmt:
			if sections[x.Name] {
				oErr = fmt.Errorf("duplicate wait step name: %v", x.Name)
				return true
			}
			sections[x.Name] = true
		case WaitEventsStmt:
			if sections[x.Name] {
				oErr = fmt.Errorf("duplicate select name: %v", x.Name)
				return true
			}
			sections[x.Name] = true
			for _, v := range x.Cases {
				if sections[v.Callback.Name] {
					oErr = fmt.Errorf("duplicate select name: %v", v.Callback.Name)
					return true
				}
				sections[v.Callback.Name] = true
			}
		case *GoStmt:
			if sections[x.Name] {
				oErr = fmt.Errorf("duplicate goroutine name: %v", x.Name)
				return true
			}
			sections[x.Name] = true
		}
		return false
	})
	if err != nil {
		return err
	}
	return oErr
}

func resumeState(ctx *resumeContext, state WorkflowState) error {
	if ctx.t.CurStep == "" { // thread has just started, let's make it running
		ctx.Running = true
	}
	def := state.Definition()
	resumeThread := Stmt(def)
	// if we are resuming non-main thread - find it's definition in the AST
	if ctx.t.Name != MainThread {
		_, err := Walk(def, func(s Stmt) bool {
			gStmt, ok := s.(*GoStmt)
			if ok && gStmt.Name == ctx.t.Name {
				resumeThread = gStmt.Stmt
				return true
			}
			return false
		})
		if err != nil {
			return fmt.Errorf("can't find thread definition %v", ctx.t.Name)
		}
	}
	ctx.t.PC++
	ctx.s.PC++
	stop, err := resumeThread.Resume(ctx)
	if err != nil {
		return fmt.Errorf("err during workflow execution: %w", err)
	}
	if stop == nil && !ctx.Running {
		return fmt.Errorf("callback not found: %#v", ctx)
	}

	// thread returned
	if stop == nil || (stop != nil && stop.Return) {
		ctx.s.Threads.Remove(ctx.t.ID)
		if ctx.t.ID == MainThread {
			ctx.s.Status = WorkflowFinished
		}
		return nil
	}

	// waiting for step to be executed
	if stop.Step != "" {
		ctx.t.CurStep = stop.Step
		ctx.t.Status = ThreadExecuting
		return nil
	}

	// waiting for condition
	if stop.Cond != "" {
		ctx.t.CurStep = stop.Cond
		ctx.t.Status = ThreadWaitingCondition
		return nil
	}

	// waiting for events
	ctx.t.CurStep = stop.WaitEvents.Name
	for _, c := range stop.WaitEvents.Cases {
		c.Callback.PC = ctx.t.PC
		c.Callback.WorkflowID = ctx.s.ID
		c.Callback.ThreadID = ctx.t.ID
		ctx.t.WaitEvents = append(ctx.t.WaitEvents, WaitEvent{Type: c.Handler.Type(), Req: c.Callback, Status: EventPendingSetup})
	}
	ctx.t.Status = ThreadWaitingEvent
	return nil
}

type CallbackRequest struct {
	WorkflowID string
	ThreadID   string
	Name       string
	PC         int
	SetupData  string
}

func NewState(id, name string) State {
	return State{
		ID:       id,
		Workflow: name,
		Status:   WorkflowResuming,
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

type ErrExec struct {
	Step string
	e    error
}

func (e ErrExec) Unwrap() error {
	return e.e
}

func (e ErrExec) Error() string {
	return fmt.Sprintf("err during execution: %v", e.e)
}

func exec(ctx context.Context, state WorkflowState, t *Thread, s *State) error {
	step, err := FindStep(t.CurStep, state.Definition())
	if err != nil {
		return fmt.Errorf("err finding step: %v", err)
	}
	if step == nil {
		return fmt.Errorf("can't find step: %v", err)
	}
	err = step.Action()
	if err != nil {
		return ErrExec{e: err, Step: t.CurStep}
	}
	t.Status = ThreadResuming
	t.PC++
	s.PC++
	return nil
}

func checkConditions(ctx context.Context, state WorkflowState, s *State) (bool, error) {
	for _, t := range s.Threads {
		switch t.Status {
		case ThreadWaitingCondition:
			// check if condition is met
			s, err := FindWaitingStep(t.CurStep, state.Definition())
			if err != nil {
				return true, err
			}
			if s.Cond {
				if s.Handler != nil {
					s.Handler()
				}
				// we should have syncronous handler to avoid concurrency issues.
				// i.e. If condition was true, but then another thread resumes and it becomes false.
				t.Status = ThreadResuming
				return true, nil
			}
			continue
		}
	}
	return false, nil
}

// Type of checkpoint event
type CheckpointType string

const (
	// Step was executed successfully. You probably want to save your workflow after this event
	CheckpointAfterStep      CheckpointType = "afterStep"
	CheckpointAfterSetup     CheckpointType = "afterSetup"
	CheckpointAfterTeardown  CheckpointType = "afterTeardown"
	CheckpointAfterResume    CheckpointType = "afterResume"
	CheckpointAfterCondition CheckpointType = "afterCondition"
)

// Checkpoint is used to save workflow state while it's being processed
// You may want to checkpoint/save your workflow only for specific checkpoint types
// to increase performance and avoid unnecessary saves.
type Checkpoint func(t CheckpointType) error

func teardown(ctx context.Context, wf WorkflowState, s *State, save Checkpoint) error {
	for _, t := range s.Threads {
		for i := 0; i < len(t.WaitEvents); i++ {
			if t.WaitEvents[i].Status != EventPendingTeardown {
				continue
			}
			h, err := FindHandler(t.WaitEvents[i].Req, wf.Definition())
			if err != nil {
				t.WaitEvents[i].Error = err.Error()
				t.WaitEvents[i].Status = EventTeardownError
				continue
			}
			err = h.Teardown(ctx, t.WaitEvents[i].Req, t.WaitEvents[i].Handled)
			if err != nil {
				t.WaitEvents[i].Error = err.Error()
				t.WaitEvents[i].Status = EventTeardownError
				continue
			}
			err = save(CheckpointAfterTeardown)
			if err != nil {
				return err
			}
			t.WaitEvents = append(t.WaitEvents[:i], t.WaitEvents[i+1:]...)
			i--
		}
	}
	// we don't return errors, since thread can continue execution and teardown can be retried later
	return nil
}

func setup(ctx context.Context, wf WorkflowState, s *State, save Checkpoint) error {
	errs := []string{}
	for _, t := range s.Threads {
		for i := 0; i < len(t.WaitEvents); i++ {
			if t.WaitEvents[i].Status != EventPendingSetup {
				continue
			}
			h, err := FindHandler(t.WaitEvents[i].Req, wf.Definition())
			if err != nil {
				t.WaitEvents[i].Error = err.Error()
				t.WaitEvents[i].Status = EventSetupError
				errs = append(errs, err.Error())
				continue
			}
			d, err := h.Setup(ctx, t.WaitEvents[i].Req)
			if err != nil {
				t.WaitEvents[i].Error = err.Error()
				t.WaitEvents[i].Status = EventSetupError
				errs = append(errs, err.Error())
				continue
			}
			t.WaitEvents[i].Status = EventSetup
			t.WaitEvents[i].Req.SetupData = d
			err = save(CheckpointAfterSetup)
			if err != nil {
				return err
			}
		}
	}
	// we have to return error if any of events was not setup, because this may affect
	// the workflow logic & correctness
	if len(errs) > 0 {
		return fmt.Errorf("errs during setup: %v", errs)
	}
	return nil
}

// Resume will continue workflow, executing steps in a process.
// Resume may fail in the middle of the processing. To avoid data loss
// and out-of-order duplicated step execution - save() will be called to
// checkpoint current state of the workflow.
func Resume(ctx context.Context, wf WorkflowState, s *State, save Checkpoint) error {
	if s.Status == WorkflowFinished {
		return fmt.Errorf("workflow has already finished")
	}
	def := wf.Definition()
	err := Validate(def)
	if err != nil {
		return err
	}
	s.PC++

	// make sure all previous teardowns are executed
	err = teardown(ctx, wf, s, save)
	if err != nil {
		return nil
	}

loop:
	for i := 0; s.Status == WorkflowResuming; i++ {
		if i > 10000 {
			return fmt.Errorf("resume didn't finish after 10000 steps")
		}
		for _, t := range s.Threads {
			switch t.Status {
			case ThreadExecuting:
				err := exec(ctx, wf, t, s)
				if err != nil {
					return err
				}
				err = save(CheckpointAfterStep)
				if err != nil {
					return err
				}
				continue loop
			case ThreadResuming:
				err := resumeState(&resumeContext{
					ctx:     ctx,
					s:       s,
					t:       t,
					Running: false,
				}, wf)
				if err != nil {
					return err
				}
				err = save(CheckpointAfterResume)
				if err != nil {
					return err
				}
				continue loop
			}
		}
		found, err := checkConditions(ctx, wf, s)
		if err != nil {
			return err
		}
		if found {
			err = save(CheckpointAfterCondition)
			if err != nil {
				return err
			}
			continue
		}
		break
	}

	err = setup(ctx, wf, s, save)
	if err != nil {
		return err
	}
	s.UpdateWorkflowsStatus()
	return nil
}

func (s *State) UpdateWorkflowsStatus() {
	if s.Status == WorkflowFinished {
		return
	}
	s.Status = WorkflowWaiting
	for _, t := range s.Threads {
		if t.Status == ThreadResuming {
			s.Status = WorkflowResuming
		}
	}
}

// Handle incoming event. This func will only execute callback handler and update the state
// After this function completes successfully - workflow should be resumed using Resume()
func HandleCallback(ctx context.Context, req CallbackRequest, wf WorkflowState, s *State, input interface{}) (interface{}, error) {
	def := wf.Definition()
	err := Validate(def)
	if err != nil {
		return nil, err
	}
	s.PC++
	if s.Status == WorkflowFinished {
		return nil, fmt.Errorf("received callback on workflow that is finished: %v", s.Status)
	}
	for _, t := range s.Threads {
		if req.ThreadID != "" && req.ThreadID != t.ID {
			continue
		}
		if t.Status != ThreadWaitingEvent {
			return nil, fmt.Errorf("thread %v is not waiting for events", t.ID)
		}
		for i, evt := range t.WaitEvents {
			if evt.Req.Name != req.Name {
				continue
			}
			if req.PC != 0 && req.PC != evt.Req.PC {
				return nil, fmt.Errorf("stored & supplied callback PC mismatch")
			}
			if evt.Status != EventSetup && evt.Status != EventPendingSetup {
				return nil, fmt.Errorf("got callback on event with unexpected status: %v", evt.Status)
			}
			h, err := FindHandler(req, wf.Definition())
			if err != nil {
				return nil, fmt.Errorf("callback not found")
			}
			out, err := h.Handle(ctx, req, input)
			if err != nil {
				return nil, fmt.Errorf("handle err: %w", err)
			}
			t.WaitEvents[i].Handled = true
			t.CurCallback = req.Name
			for i, evt := range t.WaitEvents {
				if evt.Status == EventSetup {
					t.WaitEvents[i].Status = EventPendingTeardown
				}
			}
			t.Status = ThreadResuming
			t.PC++
			s.UpdateWorkflowsStatus()
			return out, nil
		}
	}
	return nil, fmt.Errorf("callback not found")
}

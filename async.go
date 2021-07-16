package async

import (
	"context"
	"encoding/json"
	"fmt"
)

type WaitEvent struct {
	Req     CallbackRequest
	Status  WaitEventStatus
	Handled bool
	Error   string
}

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
	WorkflowRunning  WorkflowStatus = "Running"
	WorkflowFinished WorkflowStatus = "Finished"
)

type Thread struct {
	ID         string
	Name       string
	Status     ThreadStatus // current status
	CurStep    string       // current step
	WaitEvents []WaitEvent  // waiting for Select() stmt conditions
	Break      bool
	PC         int
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
		case WaitStmt:
			if sections[x.CondLabel] {
				oErr = fmt.Errorf("duplicate wait step name: %v", x.CondLabel)
				return true
			}
			sections[x.CondLabel] = true
		case SelectStmt:
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

func resumeState(ctx *ResumeContext, state WorkflowState) error {
	if ctx.t.CurStep == "" { // thread has just started, let's make it running
		ctx.Running = true
	}
	def := state.Definition()
	err := Validate(def)
	if err != nil {
		return err
	}
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
		return fmt.Errorf("err during workflow execution: %v", err)
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

	// thread returned implicitly (all statements were finished)
	if stop == nil && err == nil {
		ctx.s.Threads.Remove(ctx.t.ID)
		return nil
	}

	// thread has finished
	if stop == nil && ctx.Running {
		ctx.s.Threads.Remove(ctx.t.ID)
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

	// waiting for event
	ctx.t.CurStep = stop.Select.Name
	for _, c := range stop.Select.Cases {
		c.Callback.PC = ctx.t.PC
		c.Callback.WorkflowID = ctx.s.ID
		c.Callback.ThreadID = ctx.t.ID
		ctx.t.WaitEvents = append(ctx.t.WaitEvents, WaitEvent{Req: c.Callback, Status: EventPendingSetup})
	}
	ctx.t.Status = ThreadWaitingEvent
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
			def := state.Definition()
			err := Validate(def)
			if err != nil {
				return true, err
			}
			step, err := FindStep(t.CurStep, def)
			if err != nil {
				return false, fmt.Errorf("err finding step: %v", err)
			}
			if step == nil {
				return false, fmt.Errorf("can't find step: %v", err)
			}
			err = step.Action()
			if err != nil {
				return true, fmt.Errorf("err during step %v execution: %v", t.CurStep, err)
			}
			t.Status = ThreadResuming
			t.PC++
			s.PC++
			return true, nil
		case ThreadResuming:
			rCtx := &ResumeContext{
				ctx:     ctx,
				s:       s,
				t:       t,
				Running: false,
			}
			err := resumeState(rCtx, state)
			if err != nil {
				return true, err
			}
			return true, nil
		case ThreadWaitingEvent:
			// nothing needs to be done for thread waiting for event
		case ThreadWaitingCondition:
			// conditional thread should be executed last
			// this gives consistency in parallel thread execution
			// i.e. parallel threads are able to resume & finish resume execution after condition was met.
		}
	}
	for _, t := range s.Threads {
		switch t.Status {
		case ThreadWaitingCondition:
			// check if condition is met
			def := state.Definition()
			err := Validate(def)
			if err != nil {
				return true, err
			}
			s, err := FindWaitingStep(t.CurStep, def)
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
			if t.WaitEvents[i].Status != EventPendingTeardown {
				continue
			}
			def := wf.Definition()
			err := Validate(def)
			if err != nil {
				return err
			}
			h, err := FindHandler(t.WaitEvents[i].Req, def)
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
			err = h.Teardown(ctx, t.WaitEvents[i].Req, t.WaitEvents[i].Handled)
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
			i--
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
		if i > 1000 {
			return fmt.Errorf("resume didn't finish after 1000 steps")
		}
	}

	for _, t := range s.Threads {
		for i := 0; i < len(t.WaitEvents); i++ {
			if t.WaitEvents[i].Status != EventPendingSetup {
				continue
			}
			def := wf.Definition()
			err := Validate(def)
			if err != nil {
				return err
			}
			h, err := FindHandler(t.WaitEvents[i].Req, def)
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
				err := save(false)
				if err != nil {
					return err
				}
				continue
			}
			t.WaitEvents[i].Status = EventSetup
			t.WaitEvents[i].Req.SetupData = d
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

	for i, evt := range t.WaitEvents {
		if evt.Req.Name != req.Name {
			continue
		}
		if evt.Req.PC != req.PC {
			return nil, fmt.Errorf("stored & supplied callback PC mismatch")
		}
		if evt.Status != EventSetup && evt.Status != EventPendingSetup {
			return nil, fmt.Errorf("got callback on event with unexpected status: %v", evt.Status)
		}
		t.WaitEvents[i].Handled = true
		rCtx := &ResumeContext{
			ctx:           ctx,
			s:             s,
			t:             t,
			Callback:      evt.Req,
			CallbackInput: input,
			Running:       false,
		}
		err := resumeState(rCtx, wf)
		if err != nil {
			return nil, err
		}
		return rCtx.CallbackOutput, save(true)
	}
	return nil, fmt.Errorf("thead %v is not waiting for callback %v", t.ID, req.Name)
}

// HandleEvent is shortcut for calling HandleCallback() by event name.
// Be careful - if you're using goroutines - there may be multiple events waiting with the same name.
// Also, if you're waiting for event in a loop - event handlers from previous iterations could arrive late and trigger
// events for future iterations.
// For better control over which event will be called back - you should use OnCallback() instead and specify PC & Thread explicitly.
func HandleEvent(ctx context.Context, name string, wf WorkflowState, s *State, input interface{}, save Checkpoint) (interface{}, error) {
	var req CallbackRequest
	for _, tr := range s.Threads {
		for _, e := range tr.WaitEvents {
			if e.Req.Name == name && (e.Status == EventPendingSetup || e.Status == EventSetup) {
				req = e.Req
			}
		}
	}
	if req.Name == "" {
		return nil, fmt.Errorf("event %v not found", name)
	}
	return HandleCallback(ctx, req, wf, s, input, save)
}

package async

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

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
	ExecRetries int
	ExecError   string
	ExecBackoff time.Time
	WaitEvents  []string // events workflow is waiting for. Valid only if Status = Waiting, otherwise should be empty.
	//Receive     []*ReceiveOp
	//Send        []*SendOp
	PC int
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

// Get(id)
// Lock(id)
// UpdateLocked(id)
// Unlock(id)

// Callback:
// ScheduleCallback()
// SetResume()

// OnCallback()
// OnResume()

type UpdaterFunc func(ctx context.Context, s *State, save func() error) error

type DB interface {
	//Get(ctx context.Context, id string) (*State, error)
	Create(ctx context.Context, id string, s *State) error
	RunLocked(ctx context.Context, id string, f UpdaterFunc) error
}

type TaskMgr interface {
	SetResume(ctx context.Context, r ResumeRequest) error
	SetTimeout(ctx context.Context, d time.Duration, r CallbackRequest) error
}

type Runner struct {
	Workflows map[string]Workflow
	DB        DB
	TaskMgr   TaskMgr
}

// func (r *Runner) createTask(s *State, url string, body interface{}, schedule time.Time) error {
// 	d, err := json.Marshal(body)
// 	if err != nil {
// 		panic(err)
// 	}
// 	//log.Printf("create task: %v", string(d))
// 	//return nil
// 	sTime := time.Now().Add(time.Millisecond * 100).Format(time.RFC3339)
// 	if !schedule.IsZero() {
// 		sTime = schedule.Format(time.RFC3339)
// 	}
// 	_, err = r.tasks.Projects.Locations.Queues.Tasks.Create(
// 		fmt.Sprintf("projects/%v/locations/%v/queues/%v",
// 			r.cfg.ProjectID, r.cfg.LocationID, r.cfg.QueueName),
// 		&cloudtasks.CreateTaskRequest{
// 			Task: &cloudtasks.Task{
// 				ScheduleTime: sTime,
// 				HttpRequest: &cloudtasks.HttpRequest{
// 					Url:        url,
// 					HttpMethod: "POST",
// 					Body:       base64.StdEncoding.EncodeToString(d),
// 				},
// 			},
// 		}).Do()
// 	return err
// }

// Return OK only if all steps were completed
// If steps were completed partially - still resume workflow

// For callbacks it's a different story? - NO!

// When callback is handled - lock, process, sendResumeEvent, unlock
// When resume event is handled - lock, processALL, sendTimeoutEvent(optional), unlock

func (r *Runner) ScheduleTimeoutTasks(s *State, t *Thread) error {
	if s.Status != WorkflowRunning { // workflow finished, no event scheduling is needed
		return nil
	}
	if t.Status == ThreadWaiting {
		for _, evt := range t.WaitEvents {
			if !strings.HasPrefix(evt, "_timeout_") {
				log.Print("skipping non timeout ", evt, " ")
				continue
			}
			seconds, err := strconv.Atoi(strings.TrimPrefix(evt, "_timeout_"))
			if err != nil {
				log.Printf("wtf parse: %v", err)
				continue
			}
			err = r.TaskMgr.SetTimeout(context.TODO(), time.Second*time.Duration(seconds), CallbackRequest{
				WorkflowID: s.ID,
				ThreadID:   t.ID,
				Callback:   evt,
				PC:         t.PC, // todo: make sure PC is the same when we change something
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var ErrDuplicate = fmt.Errorf("PC doesn't match (duplicate/out of time event)")

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
	ctx.t.WaitEvents = []string{}
	//ctx.t.Receive = nil
	//ctx.t.Send = nil

	// workflow finished
	// Workflow definition should always explicitly return result.
	// You should only be allowed to return from main thread
	if stop != nil && stop.Return != nil && ctx.t.ID == "_main_" {
		log.Printf("MAIN THREAD FINISHED!!!")
		ctx.s.Status = "Finished"
		ctx.s.Output = stop.Return
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
		ctx.t.WaitEvents = append(ctx.t.WaitEvents, c.CallbackName)
	}
	ctx.t.Status = "Waiting"
	return state, nil
}

func (r *Runner) ExecStep(s *State, req ResumeRequest) error {
	if s.Status != WorkflowRunning {
		return fmt.Errorf("unexpected status for state: %v", s.Status)
	}
	t, ok := s.Threads.Find(req.ThreadID)
	if !ok {
		return fmt.Errorf("thread not found: %v", req.ThreadID)
	}
	if t.Status != ThreadExecuting && t.Status != ThreadPaused {
		return fmt.Errorf("unexpected status for state: %v", s.Status)
	}
	wf, ok := r.Workflows[s.Workflow]
	if !ok {
		return fmt.Errorf("workflow not found: %v", s.Workflow)
	}
	state := wf()
	di, err := json.Marshal(s.State)
	if err != nil {
		return fmt.Errorf("err marshal/unmarshal state")
	}
	err = json.Unmarshal(di, &state)
	if err != nil {
		return fmt.Errorf("state unmarshal err: %v", err)
	}
	step, ok := FindStep(t.CurStep, state.Definition()).(StmtStep)
	if !ok {
		return fmt.Errorf("can't find step %v", t.CurStep)
	}
	t.PC++
	s.PC++
	res := step.Action()
	if res.Success {
		t.Status = "Resuming"
		t.ExecError = ""
		t.ExecRetries = 0
		t.ExecBackoff = time.Time{}
		s.State = state
		return nil
	}
	if t.ExecRetries < res.Retries { // retries available
		t.ExecError = res.Error
		t.ExecRetries++
		t.ExecBackoff = time.Now().Add(res.RetryInterval)
		return nil // don't dump state, because step errored
	}
	t.Status = "Paused"
	return nil // don't dump state, because step errored
}

type ResumeRequest struct {
	WorkflowID string
	ThreadID   string
	PC         int

	Callback string
}

type CallbackRequest struct {
	WorkflowID string
	ThreadID   string
	Callback   string
	PC         int
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
				WaitEvents: []string{},
			},
		},
	}
	err := r.TaskMgr.SetResume(ctx, ResumeRequest{
		WorkflowID: s.ID,
		ThreadID:   "_main_",
		PC:         s.PC,
	})
	if err != nil {
		panic(err)
	}
	err = r.DB.Create(ctx, id, s)
	return err
}

func (r *Runner) Resume(s *State) (found bool, err error) {
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
			return true, r.ScheduleTimeoutTasks(s, t)
		case ThreadResuming:
			ctx := &ResumeContext{
				s: s,
				t: t,
				//CallbackName: resReq.Callback,
				Running: false,
			}
			state, err := r.ResumeState(ctx)
			if err != nil {
				return true, err
			}
			t.PC++
			s.PC++
			s.State = state
			return true, r.ScheduleTimeoutTasks(s, t)
		case ThreadWaiting:
			// ignore callbacks
			continue
		case ThreadPaused:
			return true, fmt.Errorf("TODO: formalize paused threads")

		}
	}
	return false, nil
}

func (t *Thread) WaitingForCallback(cb string) bool {
	for _, evt := range t.WaitEvents {
		if evt == cb {
			return true
		}
	}
	return false
}

func (r *Runner) OnResume(ctx context.Context, req ResumeRequest) error {
	return r.DB.RunLocked(ctx, req.WorkflowID, func(ctx context.Context, s *State, save func() error) error {
		if s.Status == WorkflowFinished {
			return fmt.Errorf("workflow has finished")
		}
		for i := 0; i < 1000 && s.Status == WorkflowRunning; i++ {
			found, err := r.Resume(s)
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
		}
		return nil
	})
}

func (r *Runner) OnCallback(ctx context.Context, req CallbackRequest) error {
	return r.DB.RunLocked(ctx, req.WorkflowID, func(ctx context.Context, s *State, save func() error) error {
		t, ok := s.Threads.Find(req.ThreadID)
		if !ok {
			return fmt.Errorf("thread %v not found", req.ThreadID)
		}
		if !t.WaitingForCallback(req.Callback) {
			return fmt.Errorf("thread is not waiting for callback: %v", req.ThreadID)
		}
		rCtx := &ResumeContext{
			s:            s,
			t:            t,
			CallbackName: req.Callback,
			Running:      false,
		}
		state, err := r.ResumeState(rCtx)
		if err != nil {
			return err
		}
		t.PC++
		s.PC++
		s.State = state
		err = r.ScheduleTimeoutTasks(s, t)
		if err != nil {
			return err
		}
		err = r.TaskMgr.SetResume(ctx, ResumeRequest{
			WorkflowID: s.ID,
		})
		if err != nil {
			return err
		}
		return save()
	})
}

// func (r *Runner) ResumeSteps(ctx context.Context) error {
// 	tx, err := r.DB.Beginx()
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Print(".")
// 	states := []State{}
// 	err = tx.Select(&states, `
// 	SELECT *
// 	FROM states s
// 	WHERE
// 		waittill < $1
// 	FOR UPDATE SKIP LOCKED`, time.Now().Unix())
// 	if err != nil {
// 		return fmt.Errorf("state select query error: %v", err)
// 	}
// 	defer tx.Rollback()
// 	for _, s := range states {
// 		// TODO: err array, parallel execution
// 		// d, _ := json.MarshalIndent(s, "", " ")
// 		// log.Printf("GOT %v", string(d))
// 		_, err := r.ResumeStateHandler(&s, "", nil, "")
// 		if err != nil {
// 			return fmt.Errorf("state %v handle failed: %v", s.ID, err)
// 		}
// 		err = s.Update(ctx, tx)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return tx.Commit()
// }

// func (r *Runner) ResumeStateHandler(s *State, callback string, callbackData json.RawMessage, tID string) (json.RawMessage, error) {
// 	ctx := &ResumeContext{
// 		s:       s,
// 		Running: false,
// 	}
// 	sort.Slice(s.Threads, func(i, j int) bool {
// 		return s.Threads[i].WaitTill < s.Threads[j].WaitTill
// 	})
// 	if callback != "" {
// 		for _, t := range s.Threads { // choose thread we should resume
// 			if tID != "" && tID != t.ID { // if tID was supplied - it should also match
// 				continue
// 			}
// 			for i, evt := range t.WaitEvents {
// 				if evt == callback {
// 					ctx.t = t
// 					ctx.CurStep = t.CurStep
// 					ctx.CallbackIndex = i
// 					ctx.CallbackInput = callbackData
// 				}
// 			}
// 		}
// 	} else {
// 		for _, t := range s.Threads {
// 			if t.Status == ThreadExecuting {
// 				wf, ok := r.Workflows[ctx.s.Workflow]
// 				if !ok {
// 					return nil, fmt.Errorf("workflow not found: %v", ctx.s.Workflow)
// 				}
// 				state := wf.InitState()
// 				err := json.Unmarshal(ctx.s.State, &state)
// 				if err != nil {
// 					return nil, fmt.Errorf("state unmarshal err: %v", err)
// 				}
// 				stmt := FindStep(t.CurStep, state.Definition().Body)
// 				if stmt == nil {
// 					panic("step to execute is nil")
// 				}
// 				step, ok := stmt.(StmtStep)
// 				if !ok {
// 					panic("step to execute is not a step")
// 				}
// 				result := step.Action()
// 				if result.Error != "" {
// 					panic("TODO: handle errors")
// 				}
// 				t.Status = ThreadResuming
// 				return nil, ctx.s.DumpState(state)
// 			}
// 		}

// 		for _, t := range s.Threads {
// 			if t.WaitTill != MaxWaittTill {
// 				ctx.t = t
// 				ctx.CurStep = t.CurStep
// 				break
// 			}
// 		}
// 	}
// 	if ctx.t == nil {
// 		return nil, fmt.Errorf("thread callback not found: %v", callback)
// 	}
// 	log.Printf("RESUMING %v %v", ctx.t.Name, ctx.t.ID)
// 	state, err := r.ResumeState(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = ctx.s.DumpState(state)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if ctx.s.Status == WorkflowFinished {
// 		return ctx.CallbackOutput, nil
// 	}

// 	return ctx.CallbackOutput, nil
// }

// type CBHandler struct {
// 	Handler http.HandlerFunc
// 	//Docs    HandlerDocs
// }

// following function inputs are supported for handlers
// func()
// func(Context.Context)
// func(Struct)
// func(Context.Context, Struct)

// following function outputs are supported for handlers
// func() {}
// func() {}  error
// func() {}  Struct
// func() {}  (Struct,error)

// following function signature is allowed for exceptional use-cases, but discouraged to be used
// CBHandler
// type Empty struct {
// }

// func isContext(t reflect.Type) bool {
// 	return t.Implements(reflect.TypeOf((*context.Context)(nil)).Elem())
// }
// func isError(t reflect.Type) bool {
// 	return t.Implements(reflect.TypeOf((*error)(nil)).Elem())
// }

// func ReflectDoc(handler Handler, inline bool) HandlerDocs {
// 	r := jsonschema.Reflector{
// 		FullyQualifyTypeNames:     true,
// 		AllowAdditionalProperties: true,
// 		DoNotReference:            inline,
// 	}
// 	docs := HandlerDocs{
// 		Input:  r.Reflect(Empty{}),
// 		Output: r.Reflect(Empty{}),
// 	}
// 	if handler == nil {
// 		return docs
// 	}
// 	h := reflect.ValueOf(handler)
// 	ht := h.Type()
// 	switch {
// 	case ht.NumIn() == 0:
// 	case ht.NumIn() == 1 && isContext(ht.In(0)):
// 	case ht.NumIn() == 1 && !isContext(ht.In(0)):
// 		docs.Input = r.ReflectFromType(ht.In(0))
// 	case ht.NumIn() == 2 && isContext(ht.In(0)):
// 		docs.Input = r.ReflectFromType(ht.In(1))
// 	default:
// 		panic(`following function inputs are supported for handlers
// func()
// func(Context.Context)
// func(Struct)
// func(Context.Context, Struct)`)
// 	}

// 	switch {
// 	case ht.NumOut() == 0:
// 	case ht.NumOut() == 1 && isError(ht.Out(0)):
// 	case ht.NumOut() == 1 && !isError(ht.Out(0)):
// 		docs.Output = r.ReflectFromType(ht.Out(0))
// 	case ht.NumOut() == 2 && isError(ht.Out(1)):
// 		docs.Output = r.ReflectFromType(ht.Out(0))
// 	default:
// 		panic(`following function outputs are supported for handlers
// func() {}
// func() {}  error
// func() {}  Struct
// func() {}  (Struct,error)`)
// 	}

// 	return docs
// }

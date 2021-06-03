package async

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/gorilla/mux"
	cloudtasks "google.golang.org/api/cloudtasks/v2beta3"
)

// Store state in FireStore / any other DB that supports optimistic locks and FIND

// Deploy to Cloud RUN / NGROK

// Cloud task for managing time.After - will call our Cloud Run function. URL should be known

// Handlers are processed immediately.
// PostAction: Event arc event is created to resume process - will call our Cloud Run function. URL should be known.

// For all updates in Workflows, where new ctx was added
// We check that ctx wasn't already canceled and if it was - we schedule a cloud task

// For updates in CTX
// we create cloud task for all processes in DB to handle canceled ctx

const MaxWaittTill = 999999999999

func DefaultQueueNameFunc(s *State) string {
	return s.Workflow
}

func DefaultCollectionName(s *State) string {
	return s.Workflow
}

type RunnerConfig struct {
	QueueName  string
	Collection string
	BaseURL    string
	ProjectID  string
	LocationID string
	Workflows  map[string]Workflow
}

func (cfg *RunnerConfig) SetDefaultsAndValidate() error {
	if cfg.Collection == "" {
		return fmt.Errorf("collection name is not set")
	}
	if cfg.BaseURL == "" {
		return fmt.Errorf("baseurl is not set")
	}
	if cfg.ProjectID == "" {
		return fmt.Errorf("projectID is not set")
	}
	if cfg.LocationID == "" {
		return fmt.Errorf("locationID is not set")
	}
	if len(cfg.Workflows) == 0 {
		return fmt.Errorf("no workflows configured")
	}
	return nil
}

func NewRunner(cfg RunnerConfig, db *firestore.Client, tasks *cloudtasks.Service) (*Runner, error) {
	err := cfg.SetDefaultsAndValidate()
	if err != nil {
		return nil, err
	}
	return &Runner{cfg: cfg, db: db, tasks: tasks}, nil
}

type Runner struct {
	cfg   RunnerConfig
	db    *firestore.Client
	tasks *cloudtasks.Service
}

func (r *Runner) createTask(s *State, url string, body interface{}, schedule time.Time) error {
	d, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	//log.Printf("create task: %v", string(d))
	//return nil
	sTime := time.Now().Add(time.Millisecond * 100).Format(time.RFC3339)
	if !schedule.IsZero() {
		sTime = schedule.Format(time.RFC3339)
	}

	_, err = r.tasks.Projects.Locations.Queues.Tasks.Create(
		fmt.Sprintf("projects/%v/locations/%v/queues/%v",
			r.cfg.ProjectID, r.cfg.LocationID, r.cfg.QueueName),
		&cloudtasks.CreateTaskRequest{
			Task: &cloudtasks.Task{
				ScheduleTime: sTime,
				HttpRequest: &cloudtasks.HttpRequest{
					Url:        url,
					HttpMethod: "POST",
					Body:       base64.StdEncoding.EncodeToString(d),
				},
			},
		}).Do()
	return err
}

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
			err = r.createTask(s, r.cfg.BaseURL+"/callback", ResumeRequest{
				WorkflowID: s.ID,
				ThreadID:   t.ID,
				PC:         t.PC,
				Callback:   evt,
			}, time.Now().Add(time.Second*time.Duration(seconds)))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var ErrDuplicate = fmt.Errorf("PC doesn't match (duplicate/out of time event)")

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

// Resume state with specified thread. Thread can be resumed in following cases:
// 1. Thread just started
// 2. Thread timed out - unblocked on time select
// 3. Thread unblocked on select - (i.e. handler/event was triggered)
// 4. Step has finished execution and we are resuming thread from that spot.
func (r *Runner) ResumeState(ctx *ResumeContext) (WorkflowState, error) {
	if ctx.s.Status != WorkflowRunning {
		return nil, fmt.Errorf("unexpected status for state: %v", ctx.s.Status)
	}
	wf, ok := r.cfg.Workflows[ctx.s.Workflow]
	if !ok {
		return nil, fmt.Errorf("workflow not found: %v", ctx.s.Workflow)
	}
	state := wf.InitState()
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
	resumeThread := Stmt(state.Definition().Body)
	if ctx.t.Name != "_main_" {
		Walk(state.Definition().Body, func(s Stmt) bool {
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
	wf, ok := r.cfg.Workflows[s.Workflow]
	if !ok {
		return fmt.Errorf("workflow not found: %v", s.Workflow)
	}
	state := wf.InitState()
	di, err := json.Marshal(s.State)
	if err != nil {
		return fmt.Errorf("err marshal/unmarshal state")
	}
	err = json.Unmarshal(di, &state)
	if err != nil {
		return fmt.Errorf("state unmarshal err: %v", err)
	}
	step, ok := FindStep(t.CurStep, state.Definition().Body).(StmtStep)
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

// func (r *Runner) ResumeSteps(ctx context.Context) error {
// 	tx, err := r.db.Beginx()
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
	err := r.createTask(s, r.cfg.BaseURL+"/resume", ResumeRequest{
		WorkflowID: s.ID,
		ThreadID:   "_main_",
		PC:         s.PC,
	}, time.Time{})
	if err != nil {
		panic(err)
	}
	_, err = r.db.Collection(r.cfg.Collection).Doc(id).Set(ctx, s)
	log.Printf("ADDED")
	return err
}

func (r *Runner) LockWorkflow(ctx context.Context, id string) (*State, error) {
	var err error
	var doc *firestore.DocumentSnapshot
	for i := 0; ; i++ {
		doc, err = r.db.Collection(r.cfg.Collection).Doc(id).Get(ctx)
		if err != nil {
			return nil, err
		}
		var s State
		err = doc.DataTo(&s)
		if err != nil {
			return nil, fmt.Errorf("err unmarshaling workflow: %v", err)
		}
		if time.Since(s.LockTill) < 0 {
			if i > 50 {
				return nil, fmt.Errorf("workflow is locked. can't unlock with 50 retries")
			} else {
				log.Printf("workflow is locked, waiting and trying again...")
				time.Sleep(time.Millisecond * 100 * time.Duration(i))
				continue
			}
		}
		_, err = r.db.Collection(r.cfg.Collection).Doc(id).Update(ctx,
			[]firestore.Update{
				{
					Path:  "LockTill",
					Value: time.Now().Add(time.Minute),
				},
			},
			firestore.LastUpdateTime(doc.UpdateTime),
		)
		if err != nil && strings.Contains(err.Error(), "FailedPrecondition") {
			log.Printf("workflow was locked concurrently, waiting and trying again...")
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("err locking workflow: %v", err)
		}
		return &s, nil
	}

}

func (r *Runner) UnlockWorkflow(ctx context.Context, id string, s *State) error {
	doc, err := r.db.Collection(r.cfg.Collection).Doc(id).Get(ctx)
	if err != nil {
		return err
	}

	s.LockTill = time.Time{}
	// TODO: check locktill
	_, err = r.db.Collection(r.cfg.Collection).Doc(id).Update(ctx,
		[]firestore.Update{
			{
				Path:  "State",
				Value: s.State,
			},
			{
				Path:  "Status",
				Value: s.Status,
			},
			{
				Path:  "Output",
				Value: s.Output,
			},
			{
				Path:  "LockTill",
				Value: s.LockTill,
			},
			{
				Path:  "Threads",
				Value: s.Threads,
			},
			{
				Path:  "PC",
				Value: s.PC,
			},
		},
		firestore.LastUpdateTime(doc.UpdateTime),
	)
	if err != nil {
		return fmt.Errorf("err unlocking workflow: %v", err)
	}
	return nil
}

func (r *Runner) UpdateStateAndExtendLock(ctx context.Context, id string, s *State) error {
	// TODO: locks should be maintained in parallel thread while the workflow is running
	// TODO: curcuitbreaker should be in place for pausing the task

	doc, err := r.db.Collection(r.cfg.Collection).Doc(id).Get(ctx)
	if err != nil {
		return err
	}

	s.LockTill = time.Now().Add(time.Minute)
	// TODO: check locktill
	_, err = r.db.Collection(r.cfg.Collection).Doc(id).Update(ctx,
		[]firestore.Update{
			{
				Path:  "State",
				Value: s.State,
			},
			{
				Path:  "Status",
				Value: s.Status,
			},
			{
				Path:  "Output",
				Value: s.Output,
			},
			{
				Path:  "LockTill",
				Value: s.LockTill,
			},
			{
				Path:  "Threads",
				Value: s.Threads,
			},
			{
				Path:  "PC",
				Value: s.PC,
			},
		},
		firestore.LastUpdateTime(doc.UpdateTime),
	)
	if err != nil {
		return fmt.Errorf("err unlocking workflow: %v", err)
	}
	return nil
}

func (r *Runner) Resume(s *State) (found bool, err error) {
	for _, t := range s.Threads {
		switch t.Status {
		case ThreadExecuting:
			wf, ok := r.cfg.Workflows[s.Workflow]
			if !ok {
				return true, fmt.Errorf("workflow not found: %v", s.Workflow)
			}
			state := wf.InitState()
			di, err := json.Marshal(s.State)
			if err != nil {
				return true, fmt.Errorf("err marshal/unmarshal state")
			}
			err = json.Unmarshal(di, &state)
			if err != nil {
				return true, fmt.Errorf("state unmarshal err: %v", err)
			}
			step, ok := FindStep(t.CurStep, state.Definition().Body).(StmtStep)
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

// TODO: new workflow!?
func (r *Runner) Router() *mux.Router {
	mr := mux.NewRouter()
	mr.HandleFunc("/callback", func(w http.ResponseWriter, req *http.Request) {
		var resReq ResumeRequest
		err := json.NewDecoder(req.Body).Decode(&resReq)
		if err != nil {
			log.Printf("resume parse error: %v", err)
		}
		log.Printf("got callback %v %v %v", resReq.WorkflowID, resReq.ThreadID, resReq.PC)
		s, err := r.LockWorkflow(req.Context(), resReq.WorkflowID)
		if err == ErrDuplicate {
			log.Printf("Skiping duplicate resume event")
			return
		}
		if err != nil {
			panic(err)
		}
		t, ok := s.Threads.Find(resReq.ThreadID)
		if !ok {
			panic(fmt.Errorf("thread %v not found", resReq.ThreadID))
		}
		if !t.WaitingForCallback(resReq.Callback) {
			panic(fmt.Errorf("thread is not waiting for callback: %v", resReq.ThreadID))
		}
		// TODO: time.After select case should be guaranteed to be first in definition!!!
		ctx := &ResumeContext{
			s:            s,
			t:            t,
			CallbackName: resReq.Callback,
			Running:      false,
		}
		state, err := r.ResumeState(ctx)
		if err != nil {
			panic(err)
		}
		t.PC++
		s.PC++
		s.State = state
		err = r.ScheduleTimeoutTasks(s, t)
		if err != nil {
			panic(err)
		}
		r.createTask(s, r.cfg.BaseURL+"/resume", ResumeRequest{
			WorkflowID: s.ID,
		}, time.Time{})
		err = r.UnlockWorkflow(req.Context(), s.ID, s)
		if err != nil {
			panic(err)
		}
	})
	mr.HandleFunc("/resume", func(w http.ResponseWriter, req *http.Request) {
		ctx := context.Background()
		var resReq CallbackRequest
		err := json.NewDecoder(req.Body).Decode(&resReq)
		if err != nil {
			w.WriteHeader(200)
			log.Printf("resume parse error: %v", err)
			return
		}
		log.Printf("got resume %v  ", resReq.WorkflowID)
		s, err := r.LockWorkflow(ctx, resReq.WorkflowID)
		if err != nil {
			w.WriteHeader(400)
			log.Printf("failed to lock workflow: %v", err)
			return
		}
		if s.Status == WorkflowFinished {
			w.WriteHeader(200)
			log.Printf("workflow has finished: %v", err)
			return
		}
		for i := 0; i < 1000 && s.Status == WorkflowRunning; i++ {
			found, err := r.Resume(s)
			if err != nil {
				w.WriteHeader(400)
				log.Printf("err during resume: %v", err)
				return
			}
			if !found {
				break
			}
			r.UpdateStateAndExtendLock(ctx, s.ID, s)
		}
		err = r.UnlockWorkflow(req.Context(), s.ID, s)
		if err != nil {
			w.WriteHeader(400)
			log.Printf("failed to unlock workflow: %v", err)
			return
		}
	})
	return mr
}

package main

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

type Runner struct {
	DB        *firestore.Client
	Tasks     *cloudtasks.Service
	Workflows map[string]Workflow
	BaseURL   string
}

func (r *Runner) ScheduleTasks(s *State, tID string) error {
	for _, t := range s.Threads {
		if t.ID == tID || t.PC > 0 {
			continue
		}
		t.PC++
		log.Print("THREAD WAS ADDED ", t.ID)
		// if new threads were added - let's schedule resume event for them
		_, err := r.Tasks.Projects.Locations.Queues.Tasks.Create(
			"projects/async-315408/locations/us-central1/queues/resuming",
			&cloudtasks.CreateTaskRequest{
				Task: &cloudtasks.Task{
					Name: fmt.Sprintf("projects/async-315408/locations/us-central1/queues/resuming/tasks/%v_%v_%v_%v_%v", s.Workflow, s.ID, strings.ReplaceAll(t.CurStep, " ", "_"), t.ID, t.PC),
					HttpRequest: &cloudtasks.HttpRequest{
						Url:        r.BaseURL + "/resume",
						HttpMethod: "POST",
						Body: ResumeRequest{
							WorkflowID: s.ID,
							ThreadID:   t.ID,
							PC:         t.PC,
						}.ToJSONBase64(),
					},
				},
			}).Do()
		if err != nil {
			return err
		}
	}
	if s.Status == WorkflowFinished { // workflow finished, no event scheduling is needed
		return nil
	}
	t, ok := s.Threads.Find(tID)
	if !ok { // thread was finished and removed
		log.Print("THREAD FINISHED")
		return nil
		// TODO: if process was finished - we should notify all processes that were waiting for us?
		// Or make any other way to manage hierarchical tasks?
	}
	log.Printf("State %v THREAD %v %v %v", s.Status, tID, t.Status, t.WaitEvents)

	switch t.Status {
	case ThreadExecuting:
		_, err := r.Tasks.Projects.Locations.Queues.Tasks.Create(
			"projects/async-315408/locations/us-central1/queues/executing",
			&cloudtasks.CreateTaskRequest{
				Task: &cloudtasks.Task{
					Name: fmt.Sprintf("projects/async-315408/locations/us-central1/queues/executing/tasks/%v_%v_%v_%v_%v", s.Workflow, s.ID, strings.ReplaceAll(t.CurStep, " ", "_"), t.ID, t.PC),
					HttpRequest: &cloudtasks.HttpRequest{
						Url:        r.BaseURL + "/execute",
						HttpMethod: "POST",
						Body: ExecuteRequest{
							WorkflowID: s.ID,
							ThreadID:   t.ID,
							PC:         t.PC,
						}.ToJSONBase64(),
					},
				},
			}).Do()
		if err != nil {
			panic(err)
		}
	case ThreadWaiting:
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
			_, err = r.Tasks.Projects.Locations.Queues.Tasks.Create(
				"projects/async-315408/locations/us-central1/queues/timeouts",
				&cloudtasks.CreateTaskRequest{
					Task: &cloudtasks.Task{
						Name:         fmt.Sprintf("projects/async-315408/locations/us-central1/queues/timeouts/tasks/%v_%v_%v_%v_%v", s.Workflow, s.ID, strings.ReplaceAll(t.CurStep, " ", "_"), t.ID, t.PC),
						ScheduleTime: time.Now().Add(time.Second * time.Duration(seconds)).Format(time.RFC3339),
						HttpRequest: &cloudtasks.HttpRequest{
							Url:        r.BaseURL + "/callback",
							HttpMethod: "POST",
							Body: CallbackRequest{
								WorkflowID: s.ID,
								ThreadID:   t.ID,
								PC:         t.PC,
								Callback:   evt,
							}.ToJSONBase64(),
						},
					},
				}).Do()
			if err != nil {
				return err
			}
		}
	case ThreadResuming:
		_, err := r.Tasks.Projects.Locations.Queues.Tasks.Create(
			"projects/async-315408/locations/us-central1/queues/resuming",
			&cloudtasks.CreateTaskRequest{
				Task: &cloudtasks.Task{
					Name: fmt.Sprintf("projects/async-315408/locations/us-central1/queues/resuming/tasks/%v_%v_%v_%v_%v", s.Workflow, s.ID, strings.ReplaceAll(t.CurStep, " ", "_"), t.ID, t.PC),
					HttpRequest: &cloudtasks.HttpRequest{
						Url:        r.BaseURL + "/resume",
						HttpMethod: "POST",
						Body: ResumeRequest{
							WorkflowID: s.ID,
							ThreadID:   t.ID,
							PC:         t.PC,
						}.ToJSONBase64(),
					},
				},
			}).Do()
		if err != nil {
			return err
		}
	}

	// log.Print("scheduling one more resume")
	// _, err := r.Tasks.Projects.Locations.Queues.Tasks.Create(
	// 	"projects/async-315408/locations/us-central1/queues/resuming",
	// 	&cloudtasks.CreateTaskRequest{
	// 		Task: &cloudtasks.Task{
	// 			HttpRequest: &cloudtasks.HttpRequest{
	// 				Url:        r.BaseURL + "/resuming",
	// 				HttpMethod: "POST",
	// 				Body: ResumeRequest{
	// 					WorkflowID: s.ID,
	// 					ThreadID:   "_main_",
	// 					Version:    s.Version,
	// 				}.ToJSONBase64(),
	// 			},
	// 		},
	// 	}).Do()
	// if err != nil {
	// 	panic(err)
	// }
	return nil
}

var ErrDuplicate = fmt.Errorf("PC doesn't match (duplicate/out of time event)")

// Resume process (after start or step that finished executing)
func (r *Runner) ResumeStateHandler(s *State, req ResumeRequest) error {
	t, ok := s.Threads.Find(req.ThreadID)
	if !ok {
		return fmt.Errorf("thread %v not found", req.ThreadID)
	}
	for i := 0; i < 100 && t.Status == ThreadResuming && s.Status == WorkflowRunning; i++ {
		ctx := &ResumeContext{
			s:       s,
			t:       t,
			Running: false,
		}
		state, err := r.ResumeState(ctx)
		if err != nil {
			return err
		}
		t.PC++
		s.PC++
		err = ctx.s.DumpState(state)
		if err != nil {
			return err
		}
	}

	return nil
}

// resume process using callback
func (r *Runner) CallbackStateHandler(s *State, req CallbackRequest) error {
	t, ok := s.Threads.Find(req.ThreadID)
	if !ok {
		return fmt.Errorf("thread %v not found", req.ThreadID)
	}
	// TODO: time.After select case should be guaranteed to be first in definition!!!
	ctx := &ResumeContext{
		s:            s,
		t:            t,
		CallbackName: req.Callback,
		Running:      false,
	}
	state, err := r.ResumeState(ctx)
	if err != nil {
		return err
	}
	t.PC++
	s.PC++
	err = ctx.s.DumpState(state)
	if err != nil {
		return err
	}
	return nil
}

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
	wf, ok := r.Workflows[ctx.s.Workflow]
	if !ok {
		return nil, fmt.Errorf("workflow not found: %v", ctx.s.Workflow)
	}
	state := wf.InitState()
	err := json.Unmarshal(ctx.s.State, &state)
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
	ctx.t.Receive = nil
	ctx.t.Send = nil

	// workflow finished
	// Workflow definition should always explicitly return result.
	// You should only be allowed to return from main thread
	if stop != nil && stop.Return != nil && ctx.t.ID == "_main_" {
		log.Printf("MAIN THREAD FINISHED!!!")
		ctx.s.Status = "Finished"
		d, err := json.Marshal(stop.Return)
		if err != nil {
			return state, fmt.Errorf("output marshal error: %v", err)
		}
		ctx.s.Output = d
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

func (r *Runner) ExecStep(s *State, req ExecuteRequest) error {
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
	state := wf.InitState()
	err := json.Unmarshal(s.State, &state)
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
		return s.DumpState(state)
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

func (r *Runner) NewWorkflow(ctx context.Context, id, name string, state interface{}) error {
	d, err := json.Marshal(state)
	if err != nil {
		return err
	}
	s := State{
		ID:       id,
		Workflow: name,
		State:    json.RawMessage(d),
		Status:   WorkflowRunning,
		Output:   json.RawMessage(`{}`),
		Threads: []*Thread{
			{
				ID:         "_main_",
				Name:       "_main_",
				Status:     ThreadResuming,
				WaitEvents: []string{},
			},
		},
	}
	task, err := r.Tasks.Projects.Locations.Queues.Tasks.Create(
		"projects/async-315408/locations/us-central1/queues/resuming",
		&cloudtasks.CreateTaskRequest{
			Task: &cloudtasks.Task{
				Name: fmt.Sprintf("projects/async-315408/locations/us-central1/queues/resuming/tasks/%v_%v_%v_%v_%v", s.Workflow, s.ID, "", "_main_", 0),
				HttpRequest: &cloudtasks.HttpRequest{
					Url:        r.BaseURL + "/resume",
					HttpMethod: "POST",
					Body: ResumeRequest{
						WorkflowID: s.ID,
						ThreadID:   "_main_",
						PC:         s.PC,
					}.ToJSONBase64(),
				},
			},
		}).Do()
	if err != nil {
		panic(err)
	}
	log.Printf("created initial task: %v", task.Name)
	_, err = r.DB.Collection("workflows").Doc(id).Set(ctx, s)
	log.Printf("ADDED")
	return err
}

type ResumeRequest struct {
	WorkflowID string
	ThreadID   string
	PC         int
}

func (t ResumeRequest) ToJSONBase64() string {
	d, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(d)
}

func (r *Runner) LockWorkflow(ctx context.Context, id string, thread string, pc int) (*State, error) {
	var err error
	var doc *firestore.DocumentSnapshot
	for i := 0; ; i++ {
		doc, err = r.DB.Collection("workflows").Doc(id).Get(ctx)
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
		t, ok := s.Threads.Find(thread)
		if !ok {
			return nil, fmt.Errorf("thread not found for unlock: %v", thread)
		}
		if t.PC > pc { // callback is in the past
			return nil, ErrDuplicate
		}
		if t.PC < pc { // callback is in the future
			return nil, fmt.Errorf("Lock has expired after Cloud task creation, but workflow wasn't saved. We should kill this event")
		}
		_, err = r.DB.Collection("workflows").Doc(id).Update(ctx,
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
	doc, err := r.DB.Collection("workflows").Doc(id).Get(ctx)
	if err != nil {
		return err
	}

	s.LockTill = time.Time{}
	// TODO: check locktill
	_, err = r.DB.Collection("workflows").Doc(id).Update(ctx,
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

// TODO: new workflow!?
func (r *Runner) Router() *mux.Router {
	mr := mux.NewRouter()

	mr.HandleFunc("/execute", func(w http.ResponseWriter, req *http.Request) {
		log.Printf("got execute")
		var execReq ExecuteRequest
		err := json.NewDecoder(req.Body).Decode(&execReq)
		if err != nil {
			log.Printf("resume parse error: %v", err)
		}
		s, err := r.LockWorkflow(req.Context(), execReq.WorkflowID, execReq.ThreadID, execReq.PC)
		if err == ErrDuplicate {
			log.Printf("Skiping duplicate: %v", err)
			return
		}
		if err != nil {
			panic(err)
		}
		err = r.ExecStep(s, execReq)
		if err != nil {
			panic(err)
		}
		err = r.ScheduleTasks(s, execReq.ThreadID)
		if err != nil {
			panic(err)
		}
		err = r.UnlockWorkflow(req.Context(), s.ID, s)
		if err != nil {
			panic(err)
		}
	})
	mr.HandleFunc("/resume", func(w http.ResponseWriter, req *http.Request) {
		var resReq ResumeRequest
		err := json.NewDecoder(req.Body).Decode(&resReq)
		if err != nil {
			log.Printf("resume parse error: %v", err)
		}
		log.Printf("got resuming %v %v %v", resReq.WorkflowID, resReq.ThreadID, resReq.PC)
		s, err := r.LockWorkflow(req.Context(), resReq.WorkflowID, resReq.ThreadID, resReq.PC)
		if err != nil {
			panic(err)
		}
		err = r.ResumeStateHandler(s, resReq)
		if err != nil {
			panic(err)
		}
		err = r.ScheduleTasks(s, resReq.ThreadID)
		if err != nil {
			panic(err)
		}
		err = r.UnlockWorkflow(req.Context(), s.ID, s)
		if err != nil {
			panic(err)
		}
	})
	mr.HandleFunc("/callback", func(w http.ResponseWriter, req *http.Request) {
		log.Printf("got callback")
		var resReq CallbackRequest
		err := json.NewDecoder(req.Body).Decode(&resReq)
		if err != nil {
			log.Printf("resume parse error: %v", err)
		}
		s, err := r.LockWorkflow(req.Context(), resReq.WorkflowID, resReq.ThreadID, resReq.PC)
		if err != nil {
			panic(err)
		}
		err = r.CallbackStateHandler(s, resReq)
		if err != nil {
			panic(err)
		}
		err = r.ScheduleTasks(s, resReq.ThreadID)
		if err != nil {
			panic(err)
		}
		err = r.UnlockWorkflow(req.Context(), s.ID, s)
		if err != nil {
			panic(err)
		}
	})
	return mr
}

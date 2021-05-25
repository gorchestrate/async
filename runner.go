package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
)

const MaxWaittTill = 999999999999

type Runner struct {
	db        *sqlx.DB
	Workflows map[string]Workflow
}

func (r *Runner) ResumeStateHandler(s *State, callback string, callbackData json.RawMessage, tID string) (json.RawMessage, error) {
	ctx := &ResumeContext{
		s:       s,
		Running: false,
	}
	sort.Slice(s.Threads, func(i, j int) bool {
		return s.Threads[i].WaitTill < s.Threads[j].WaitTill
	})
	if callback != "" {
		for _, t := range s.Threads { // choose thread we should resume
			if tID != "" && tID != t.ID { // if tID was supplied - it should also match
				continue
			}
			for i, evt := range t.WaitEvents {
				if evt == callback {
					ctx.t = t
					ctx.CurStep = t.CurStep
					ctx.CallbackIndex = i
					ctx.CallbackInput = callbackData
				}
			}
		}
	} else {
		for _, t := range s.Threads {
			if t.Status == ThreadExecuting {
				wf, ok := r.Workflows[ctx.s.Workflow]
				if !ok {
					return nil, fmt.Errorf("workflow not found: %v", ctx.s.Workflow)
				}
				state := wf.InitState()
				err := json.Unmarshal(ctx.s.State, &state)
				if err != nil {
					return nil, fmt.Errorf("state unmarshal err: %v", err)
				}
				stmt := FindStep(t.CurStep, state.Definition().Body)
				if stmt == nil {
					panic("step to execute is nil")
				}
				step, ok := stmt.(StmtStep)
				if !ok {
					panic("step to execute is not a step")
				}
				result := step.Action()
				if result.Error != "" {
					panic("TODO: handle errors")
				}
				t.Status = ThreadResuming
				return nil, nil
			}
		}

		for _, t := range s.Threads {
			if t.WaitTill != MaxWaittTill {
				ctx.t = t
				ctx.CurStep = t.CurStep
				break
			}
		}
	}
	if ctx.t == nil {
		return nil, fmt.Errorf("thread callback not found: %v", callback)
	}
	log.Printf("RESUMING %v %v", ctx.t.Name, ctx.t.ID)
	state, err := r.ResumeState(ctx)
	if err != nil {
		return nil, err
	}
	l, _ := json.Marshal(s)
	log.Print(string(l))
	err = ctx.s.DumpState(state)
	if err != nil {
		return nil, err
	}
	ctx.s.WaitTill = 999999999999
	if ctx.s.Status == WorkflowFinished {
		return ctx.CallbackOutput, nil
	}
	for _, t := range ctx.s.Threads {
		switch t.Status {
		case ThreadExecuting:
			ctx.s.WaitTill = 0
		case ThreadResuming:
			ctx.s.WaitTill = 0
		case ThreadWaiting:
			if t.WaitTill < int64(ctx.s.WaitTill) {
				ctx.s.WaitTill = t.WaitTill
			}
		}
	}

	return ctx.CallbackOutput, nil

}

// Resume state with specified thread. Thread can be resumed in following cases:
// 1. Thread just started
// 2. Thread timed out - unblocked on time select
// 3. Thread unblocked on select - (i.e. handler/event was triggered)
// 4. Step has finished execution and we are resuming thread from that spot.
func (r *Runner) ResumeState(ctx *ResumeContext) (WorkflowState, error) {
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
	ctx.t.WaitTill = 0
	ctx.t.Receive = nil
	ctx.t.Send = nil

	// workflow finished
	// Workflow definition should always explicitly return result.
	// You should only be allowed to return from main thread
	if stop != nil && stop.Return != nil && ctx.t.ID == "_main_" {
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
		if c.CaseAfter != 0 {
			ctx.t.WaitTill = time.Now().Add(c.CaseAfter).Unix()
			ctx.t.WaitEvents = append(ctx.t.WaitEvents, "_waittill_")
		}
		if c.CaseEvent != "" {
			ctx.t.WaitEvents = append(ctx.t.WaitEvents, c.CaseEvent)
		}
	}
	ctx.t.Status = "Waiting"
	ctx.t.CurStepError = ""
	ctx.t.CurStepRetries = 0

	return state, nil
}

func (r *Runner) ExecStep(s *State, t *Thread) error {
	wf, ok := r.Workflows[s.Workflow]
	if !ok {
		return fmt.Errorf("workflow not found: %v", s.Workflow)
	}
	state := wf.InitState()
	err := json.Unmarshal(s.State, &state)
	if err != nil {
		return fmt.Errorf("state unmarshal err: %v", err)
	}
	if s.Status != "Executing" { // TODO: handle more exec step statuses
		return fmt.Errorf("unexpected status for state: %v", s.Status)
	}
	step, ok := FindStep(t.CurStep, state.Definition().Body).(StmtStep)
	if !ok {
		return fmt.Errorf("can't find step %v", t.CurStep)
	}
	res := step.Action()
	if res.Success {
		t.Status = "Resuming"
		t.CurStepError = ""
		t.CurStepRetries = 0
		return s.DumpState(state)
	}
	if t.CurStepRetries < res.Retries { // retries available
		t.CurStepError = res.Error
		t.CurStepRetries++
		t.WaitTill = time.Now().Add(res.RetryInterval).Unix()
		return nil // don't dump state, because step errored
	}

	t.CurStepError = res.Error
	t.Status = "Paused"
	return nil // don't dump state, because step errored
}

func (r *Runner) ResumeSteps(ctx context.Context) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	fmt.Print(".")
	states := []State{}
	err = tx.Select(&states, `
	SELECT *
	FROM states s 
	WHERE  
		waittill < $1
	FOR UPDATE SKIP LOCKED`, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("state select query error: %v", err)
	}
	defer tx.Rollback()
	for _, s := range states {
		// TODO: err array, parallel execution
		// d, _ := json.MarshalIndent(s, "", " ")
		// log.Printf("GOT %v", string(d))
		_, err := r.ResumeStateHandler(&s, "", nil, "")
		if err != nil {
			return fmt.Errorf("state %v handle failed: %v", s.ID, err)
		}
		err = s.Update(ctx, tx)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

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
		WaitTill: 0,
		Threads: []*Thread{
			{
				ID:         "_main_",
				Name:       "_main_",
				Status:     ThreadResuming,
				WaitEvents: []string{},
			},
		},
	}
	return s.Insert(ctx, r.db)
}

// TODO: new workflow!?
func (r *Runner) Router() *mux.Router {
	mr := mux.NewRouter()
	mr.HandleFunc("/state/{id}/callback/{callback}", func(w http.ResponseWriter, req *http.Request) {
		d, err := ioutil.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "body read err: %v", err)
			return
		}
		out, err := r.Handler(req.Context(), mux.Vars(req)["id"], "/"+mux.Vars(req)["callback"], mux.Vars(req)["thread"], d)
		if err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":  err.Error(),
				"output": out,
			})
			return
		}
		w.Write(out)
	})
	return mr
}

func (r *Runner) Handler(ctx context.Context, id, callback, tID string, data json.RawMessage) (json.RawMessage, error) {
	tx, err := r.db.Beginx()
	if err != nil {
		return nil, err
	}
	ss := []State{}
	err = tx.Select(&ss, `SELECT * FROM states WHERE  id = $1 FOR UPDATE NOWAIT`, id)
	if err != nil {
		return nil, fmt.Errorf("state by id select query error: %v", err)
	}
	defer tx.Rollback()
	if len(ss) == 0 {
		return nil, fmt.Errorf("not found")
	}
	s := ss[0]
	out, err := r.ResumeStateHandler(&s, callback, data, tID)
	if err != nil {
		return out, fmt.Errorf("state %v handle failed: %v", s.ID, err)
	}
	err = s.Update(ctx, tx)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func NewRunner(ctx context.Context, connStr string, ww map[string]Workflow) (*Runner, error) {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening db: %v", err)
	}
	_, err = db.ExecContext(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("error during schema init: %v", err)
	}
	r := &Runner{
		db:        db,
		Workflows: map[string]Workflow{},
	}
	for _, v := range ww {
		r.Workflows[v.Name] = v
	}
	return r, nil
}

func (r *Runner) Manage(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			err := r.ResumeSteps(ctx)
			if err != nil {
				log.Printf("err in resume loop: %v", err)
				time.Sleep(time.Second * 1)
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

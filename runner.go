package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
)

type Runner struct {
	db        *sqlx.DB
	Workflows map[string]Workflow
}

// Resume state with specified thread. Thread can be resumed in following cases:
// 1. Thread just started
// 2. Thread timed out - unblocked on time select
// 3. Thread unblocked on select - (i.e. handler/event was triggered)
// 4. Step has finished execution and we are resuming thread from that spot.
func (r *Runner) ResumeState(ctx *ResumeContext) error {
	wf, ok := r.Workflows[ctx.s.Workflow]
	if !ok {
		return fmt.Errorf("workflow not found: %v", ctx.s.Workflow)
	}
	state := wf.InitState()
	err := json.Unmarshal(ctx.s.State, &state)
	if err != nil {
		return fmt.Errorf("state unmarshal err: %v", err)
	}
	if ctx.t.CurStep == "" { // thread has just started, let's make it running
		ctx.Running = true
	}
	stop, err := state.Definition().Body.Resume(ctx)
	if err != nil {
		return fmt.Errorf("err during workflow execution: %v", err)
	}
	if stop == nil && !ctx.Running {
		return fmt.Errorf("callback not found: %v", ctx)
	}
	ctx.t.WaitEvents = []string{}
	ctx.t.WaitTill = 0
	ctx.t.Receive = nil
	ctx.t.Send = nil
	if stop == nil && ctx.Running {
		ctx.t.CurStep = ""
		ctx.t.Status = "Finished"
		ctx.t.CurStepError = ""
		ctx.t.CurStepRetries = 0
		if ctx.t.ID == "_main_" {
			ctx.s.Status = "Finished"
			if stop != nil {
				d, err := json.Marshal(stop.Return)
				if err != nil {
					return fmt.Errorf("output marshal error: %v", err)
				}
				ctx.s.Output = d
			}
		}
		return ctx.s.DumpState(state)
	}

	if stop.Step != "" {
		ctx.t.CurStep = stop.Step
		ctx.t.Status = "Executing"
		return nil
	}
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

	return ctx.s.DumpState(state)
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
	states := []State{}
	err = tx.Select(&states, `
	SELECT *
	FROM states s 
	WHERE  
		waittill < $1
	FOR UPDATE SKIP LOCKED
		`, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("state select query error: %v", err)
	}
	defer tx.Rollback()
	for _, s := range states { // TODO: err array, parallel execution
		d, _ := json.MarshalIndent(s, "", " ")
		log.Printf("GOT %v", string(d))
		for _, t := range s.Threads {
			if t.Status == "Resuming" {
				// err := r.ExecStep(&s, s.Threads[123]) // TODO: err array, parallel execution
				// if err != nil {
				// 	return fmt.Errorf("state %v handle failed: %v", s.ID, err)
				// }
				// OR
				err := r.ResumeState(&ResumeContext{
					s:       &s,
					t:       t,
					Running: false,
					CurStep: t.CurStep,
				})
				if err != nil {
					return fmt.Errorf("state %v handle failed: %v", s.ID, err)
				}

			}
			break
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
		Status:   "Resuming",
		Output:   json.RawMessage(`{}`),
		WaitTill: 999999999999,
		Threads: []*Thread{
			{
				ID:         "_main_",
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
		out, err := r.Handler(req.Context(), mux.Vars(req)["id"], "/"+mux.Vars(req)["callback"], d)
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

func (r *Runner) Handler(ctx context.Context, id, callback string, data json.RawMessage) (json.RawMessage, error) {
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
	if s.Status != "Waiting" {
		return nil, fmt.Errorf("state is not waiting: %v", callback)
	}
	if s.Threads[123].EventIndex(callback) == -1 {
		return nil, fmt.Errorf("state is not waiting for this callback: %v %v", callback, s.Threads[123].WaitEvents)
	}
	rCtx := &ResumeContext{
		s:             &s,
		t:             s.Threads[123],
		Running:       false,
		CurStep:       s.Threads[123].CurStep,
		CallbackIndex: s.Threads[123].EventIndex(callback),
		CallbackInput: data,
	}
	err = r.ResumeState(rCtx)
	if err != nil {
		return rCtx.CallbackOutput, fmt.Errorf("state %v handle failed: %v", s.ID, err)
	}
	err = s.Update(ctx, tx)
	if err != nil {
		return rCtx.CallbackOutput, err
	}
	return rCtx.CallbackOutput, tx.Commit()
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

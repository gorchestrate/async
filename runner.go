package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

type Runner struct {
	db        *sqlx.DB
	Workflows map[string]Workflow
}

func (r *Runner) ResumeState(s *State, rCtx *ResumeContext) error {
	wf, ok := r.Workflows[s.Workflow]
	if !ok {
		return fmt.Errorf("workflow not found: %v", s.Workflow)
	}
	state := wf.InitState()
	err := json.Unmarshal(s.State, &state)
	if err != nil {
		return fmt.Errorf("state unmarshal err: %v", err)
	}
	stop, err := state.Workflow().Resume(rCtx)
	if err != nil {
		return fmt.Errorf("err during workflow execution: %v", err)
	}
	if stop == nil && !rCtx.Running {
		return fmt.Errorf("callback not found: %v", rCtx)
	}
	s.WaitEvents = []string{}
	s.WaitTill = 0
	if stop == nil && rCtx.Running {
		s.CurStep = ""
		s.Status = "Finished"
		s.CurStepError = ""
		s.CurStepRetries = 0
		if stop != nil {
			d, err := json.Marshal(stop.Return)
			if err != nil {
				return fmt.Errorf("output marshal error: %v", err)
			}
			s.Output = d
		}
		return s.DumpState(state)
	}

	if stop.Step != "" {
		s.CurStep = stop.Step
		s.Status = "Executing"
		return nil
	}

	s.CurStep = stop.Select.Name
	for _, c := range stop.Select.Cases {
		if c.CaseAfter != 0 {
			s.WaitTill = time.Now().Add(c.CaseAfter).Unix()
			s.WaitEvents = append(s.WaitEvents, "_waittill_")
		}
		if c.CaseEvent != "" {
			s.WaitEvents = append(s.WaitEvents, c.CaseEvent)
		}
	}
	s.Status = "Waiting"
	s.CurStepError = ""
	s.CurStepRetries = 0

	return s.DumpState(state)
}

func (r *Runner) ExecStep(s *State) error {
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
	step := state.Workflow().Find(s.CurStep).(StmtStep)
	res := step.Action()
	if res.Success {
		s.Status = "Resuming"
		s.CurStepError = ""
		s.CurStepRetries = 0
		return s.DumpState(state)
	}
	if s.CurStepRetries < res.Retries { // retries available
		s.CurStepError = res.Error
		s.CurStepRetries++
		s.WaitTill = time.Now().Add(res.RetryInterval).Unix()
		return nil // don't dump state, because step errored
	}

	s.CurStepError = res.Error
	s.Status = "Paused"
	return nil // don't dump state, because step errored
}

func (r *Runner) ResumeTimedOut(ctx context.Context) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	states := []State{}
	err = tx.Select(&states, `SELECT * FROM states
	WHERE   
		status = 'Waiting' AND  
		waittill != 0 AND           /* waittill can be 0 for selects that wait just for events */
		waittill < $1
	FOR UPDATE SKIP LOCKED
		`, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("state select query error: %v", err)
	}
	defer tx.Rollback()
	for _, s := range states {
		err := r.ResumeState(&s, &ResumeContext{
			Running:       false,
			CurStep:       s.CurStep,
			CallbackIndex: s.EventIndex("_waittill_"),
		}) // TODO: err array, parallel execution
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

func (r *Runner) ExecuteSteps(ctx context.Context) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	states := []State{}
	err = tx.Select(&states, `SELECT * FROM states
	WHERE  
		status = 'Executing' AND
		waittill < $1                /* limit by waittil for retry logic */
	FOR UPDATE SKIP LOCKED
		`, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("state select query error: %v", err)
	}
	defer tx.Rollback()
	for _, s := range states {
		err := r.ExecStep(&s) // TODO: err array, parallel execution
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

func (r *Runner) ResumeAfterStepsExecuted(ctx context.Context) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	states := []State{}
	err = tx.Select(&states, `SELECT * FROM states
	WHERE  
		status = 'Resuming'
	FOR UPDATE SKIP LOCKED
		`)
	if err != nil {
		return fmt.Errorf("state select query error: %v", err)
	}
	defer tx.Rollback()
	for _, s := range states {
		err := r.ResumeState(&s, &ResumeContext{
			Running: false,
			CurStep: s.CurStep,
		}) // TODO: err array, parallel execution
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
	if s.EventIndex(callback) == -1 {
		return nil, fmt.Errorf("state is not waiting for this callback: %v %v", callback, s.WaitEvents)
	}
	rCtx := &ResumeContext{
		Running:       false,
		CurStep:       s.CurStep,
		CallbackIndex: s.EventIndex(callback),
		CallbackInput: data,
	}
	err = r.ResumeState(&s, rCtx)
	if err != nil {
		return rCtx.CallbackOutput, fmt.Errorf("state %v handle failed: %v", s.ID, err)
	}
	err = s.Update(ctx, tx)
	if err != nil {
		return rCtx.CallbackOutput, err
	}
	return rCtx.CallbackOutput, tx.Commit()
}

func NewRunner(ctx context.Context, connStr string, ww []Workflow) (*Runner, error) {
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := r.ResumeTimedOut(ctx)
				if err != nil {
					log.Printf("err in resume time loop: %v", err)
					time.Sleep(time.Second * 1)
				}
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := r.ResumeAfterStepsExecuted(ctx)
				if err != nil {
					log.Printf("err in resume loop: %v", err)
					time.Sleep(time.Second * 1)
				}
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := r.ExecuteSteps(ctx)
				if err != nil {
					log.Printf("err in execute loop: %v", err)
					time.Sleep(time.Second * 1)
				}
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()
	wg.Wait()
	return nil
}

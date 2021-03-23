package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type State struct {
	ID       string          `db:"id"`       // id of workflow instance
	Workflow string          `db:"workflow"` // name of workflow definition. Used to choose proper state type to unmarshal & resume on
	State    json.RawMessage `db:"state"`    // json body of workflow state
	Status   WorkflowStatus  `db:"status"`   // current status
	Output   json.RawMessage `db:"output"`   // json output of the finished workflow. Valid only if Status = Finished

	CurStep        string `db:"curstep"`        // current step of the workflow
	CurStepRetries int    `db:"curstepretries"` // TODO
	CurStepError   string `db:"cursteperror"`   // TODO

	WaitEvents pq.StringArray `db:"waitevents"` // events workflow is waiting for. Valid only if Status = Waiting, otherwise should be empty.
	WaitTill   int64          `db:"waittill"`   // unix time process is waiting for.   For Executing step - this is next retry time. For After() event - this is time we unblock if no other event came.
}

type WorkflowStatus string

const (
	Executing WorkflowStatus = "Executing"
	Resuming  WorkflowStatus = "Resuming"
	Waiting   WorkflowStatus = "Waiting"
	Finished  WorkflowStatus = "Finished"
	Paused    WorkflowStatus = "Paused"
)

func (s *State) DumpState(v interface{}) error {
	d, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.State = json.RawMessage(d)
	return nil
}

func (s State) EventIndex(name string) int {
	for i, e := range s.WaitEvents {
		if e == name {
			return i
		}
	}
	return -1
}

var schema = `
DROP TABLE IF EXISTS states;

CREATE TABLE IF NOT EXISTS states (
	id text UNIQUE PRIMARY KEY,
	workflow text NOT NULL,
	state JSON  NOT NULL,
	status text NOT NULL,
	output JSON NOT NULL,

	curstep text NOT NULL,
	curstepretries int NOT NULL,
	cursteperror text NOT NULL,

	waittill bigint,
	waitevents text [] NOT NULL
 );`

func (s State) Insert(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `INSERT INTO states(
			id,
			workflow,
			state, 
			curstep, 
			waittill, 
			waitevents,
			curstepretries, 
			cursteperror,
			status,
			output
		) VALUES(
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10
		);`,
		s.ID,
		s.Workflow,
		s.State,
		s.CurStep,
		s.WaitTill,
		s.WaitEvents,
		s.CurStepRetries,
		s.CurStepError,
		s.Status,
		s.Output,
	)
	if err != nil {
		return fmt.Errorf("err during state insert: %v", err)
	}
	return nil
}

func (s State) Update(ctx context.Context, tx *sqlx.Tx) error {
	log.Printf("update: %#v", s)
	r, err := tx.ExecContext(ctx, `UPDATE states SET 
			workflow = $2,
			state = $3,
			curstep = $4,
			waittill = $5,
			waitevents = $6,
			curstepretries = $7,
			cursteperror = $8,
			status = $9,
			output = $10
		WHERE
			id = $1
		`,
		s.ID,
		s.Workflow,
		s.State,
		s.CurStep,
		s.WaitTill,
		s.WaitEvents,
		s.CurStepRetries,
		s.CurStepError,
		s.Status,
		s.Output,
	)
	if err != nil {
		return fmt.Errorf("err during state update: %v", err)
	}
	n, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("err row affected: %v", err)
	}
	if n != 1 {
		return fmt.Errorf("state not found: %v", s.ID)
	}
	return nil
}

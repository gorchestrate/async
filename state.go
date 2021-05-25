package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Thread struct {
	ID             string         `db:"id"`
	Name           string         `db:"name"`
	Status         ThreadStatus   `db:"status"`         // current status
	CurStep        string         `db:"curstep"`        // current step of the workflow
	CurStepRetries int            `db:"curstepretries"` // TODO
	CurStepError   string         `db:"cursteperror"`   // TODO
	WaitEvents     pq.StringArray `db:"waitevents"`     // events workflow is waiting for. Valid only if Status = Waiting, otherwise should be empty.
	WaitTill       int64          `db:"waittill"`
	Receive        []*ReceiveOp   `db:"recv"`
	Send           []*SendOp      `db:"send"`
}

type State struct {
	ID       string          `db:"id"`       // id of workflow instance
	Workflow string          `db:"workflow"` // name of workflow definition. Used to choose proper state type to unmarshal & resume on
	State    json.RawMessage `db:"state"`    // json body of workflow state
	Status   WorkflowStatus  `db:"status"`   // current status
	Input    json.RawMessage `db:"input"`    // json input of the workflow
	Output   json.RawMessage `db:"output"`   // json output of the finished workflow. Valid only if Status = Finished
	WaitTill int64           `db:"waittill"` // earliest waittill timestamp of all threads

	Threads Threads `db:"threads"`
}

type Threads []*Thread

func (tt *Threads) Add(t *Thread) {
	tr := *tt
	for _, t2 := range tr {
		if t.ID == t2.ID {
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

func (tt Threads) Value() (driver.Value, error) {
	return driver.Value(string(jsonbarr(tt))), nil
}

func (tt *Threads) Scan(src interface{}) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("Incompatible type for GzippedText")
	}
	return json.Unmarshal(source, tt)
}

type ReceiveOp struct {
}
type SendOp struct {
}

type WorkflowStatus string
type ThreadStatus string

const (
	WorkflowRunning  WorkflowStatus = "Running"
	WorkflowFinished WorkflowStatus = "Finished"
	//WorkflowPaused   WorkflowStatus = "Paused"
)

const (
	ThreadExecuting ThreadStatus = "Executing"
	ThreadResuming  ThreadStatus = "Resuming"
	ThreadWaiting   ThreadStatus = "Waiting"
)

func (s *State) DumpState(v interface{}) error {
	d, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.State = json.RawMessage(d)
	return nil
}

func jsonbarr(v interface{}) []byte {
	if v == nil {
		return []byte("[]")
	}
	d, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return d
}

func (t Thread) EventIndex(name string) int {
	for i, e := range t.WaitEvents {
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
	threads JSONB NOT NULL,
	waittill bigint NOT NULL
 );
 `

func (s State) Insert(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `INSERT INTO states(
			id,
			workflow,
			state, 
			status,
			output,
			threads,
			waittill
		)VALUES(
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7
		);`,
		s.ID,
		s.Workflow,
		s.State,
		s.Status,
		s.Output,
		s.Threads,
		s.WaitTill,
	)
	if err != nil {
		return fmt.Errorf("err during state insert: %v", err)
	}
	return nil
}

func (s State) Update(ctx context.Context, tx *sqlx.Tx) error {
	r, err := tx.ExecContext(ctx, `UPDATE states SET 
			workflow = $2,
			state = $3,
			status = $4,
			output = $5,
			threads = $6,
			waittill = $7
		WHERE
			id = $1
		`,
		s.ID,
		s.Workflow,
		s.State,
		s.Status,
		s.Output,
		s.Threads,
		s.WaitTill,
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

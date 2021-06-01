package async

import (
	"encoding/json"
	"time"
)

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

type State struct {
	ID       string         // id of workflow instance
	Workflow string         // name of workflow definition. Used to choose proper state type to unmarshal & resume on
	State    interface{}    // json body of workflow state
	Status   WorkflowStatus // current status
	Input    interface{}    // json input of the workflow
	Output   interface{}    // json output of the finished workflow. Valid only if Status = Finished
	LockTill time.Time
	Threads  Threads
	PC       int
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
	WorkflowFinished WorkflowStatus = "Finished"
)

const (
	ThreadExecuting ThreadStatus = "Executing"
	ThreadResuming  ThreadStatus = "Resuming"
	ThreadWaiting   ThreadStatus = "Waiting"
	ThreadPaused    ThreadStatus = "Paused"
)

func (s *State) DumpState(v interface{}) error {
	d, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.State = json.RawMessage(d)
	return nil
}

func (t Thread) EventIndex(name string) int {
	for i, e := range t.WaitEvents {
		if e == name {
			return i
		}
	}
	return -1
}

// type ReceiveOp struct {
// }
// type SendOp struct {
// }

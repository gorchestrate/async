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
)

func main() {
	runner, err := NewRunner(context.Background(), "postgres://user:pass@localhost/async?sslmode=disable", []Workflow{
		{
			Name: "exampleFlow",
			InitState: func() WorkflowState {
				return &ExampleFlow{}
			},
		},
	})
	if err != nil {
		panic(err)
	}
	s := State{
		ID:         "1",
		Workflow:   "exampleFlow",
		State:      json.RawMessage(`{}`),
		WaitEvents: []string{},
		CurStep:    "init",
		Status:     "Resuming",
		Output:     json.RawMessage(`{}`),
	}
	err = s.Insert(context.Background(), runner.db)
	if err != nil {
		panic(err)
	}

	go func() {
		r := mux.NewRouter()
		r.HandleFunc("/state/{id}/callback/{callback}", func(w http.ResponseWriter, r *http.Request) {
			d, err := ioutil.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(400)
				fmt.Fprintf(w, "body read err: %v", err)
				return
			}
			out, err := runner.Handler(r.Context(), mux.Vars(r)["id"], "/"+mux.Vars(r)["callback"], d)
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
		log.Fatal(http.ListenAndServe(":8081", r))
	}()
	err = runner.Manage(context.Background())
	if err != nil {
		panic(err)
	}
}

type ExampleFlow struct {
	A string
	B int
}

func (e *ExampleFlow) Workflow() Section {
	log.Printf("RESUME %#v", *e)
	return S(
		// Step is an asyncronous task that is automatically retried on failure.
		Step("init", func() ActionResult {
			log.Print("sending request")
			return ActionResult{Success: true}
		}),

		// Conditional logic
		If(e.A == "2",
			Step("if match", func() ActionResult {
				log.Print("----------if")
				e.B = 0
				return ActionResult{}
			}),
		),

		// Loops
		For(e.B < 5, S(
			Step("increment", func() ActionResult {
				e.B++
				if e.B == 3 {
					// log.Print("RETRY TTYRRR")
					// return ActionResult{
					// 	Error:   "SOME ERROR",
					// 	Retries: 3,
					// }
				}
				return ActionResult{
					Success: true,
				}
			}),
			Step("log", func() ActionResult {
				log.Printf("---------- e.B = %v", e.B)
				return ActionResult{
					Success: true,
				}
			}),
		)),

		// Waiting for condition
		Select("waiting",
			After(time.Second*100, S(
				Step("timeout", func() ActionResult {
					log.Printf("---------- TIMEOUTTTT")
					return ActionResult{
						Success: true,
					}
				}),
			)), // Timeout
			On("/confirm", func(in string) (*ExampleFlow, error) { // HTTP handler
				log.Print("IN: ", in)
				// Syncronous code handling HTTP request, for ex. request validation,
				e.A = in
				return e, nil
			}, S( // continue workflow execution in asyncronous way
				Step("after", func() ActionResult {
					log.Printf("---------- after1")
					return ActionResult{
						Success: true,
					}
				}),
				Step("after2", func() ActionResult {
					log.Printf("---------- after2")
					return ActionResult{
						Success: true,
					}
				}),
			)),
		),
	)
}

/*
import (
	"log"
	"math/rand"
	"net/http"
	"testing"
	"time"
)

func TestFlow(t *testing.T) {
	rand.Seed(time.Now().Unix())
	e := ExampleFlow{
		A: "1",
		B: 40,
	}

	ctx := &ResumeContext{
		Running: true,
	}
	for {
		log.Printf("Resume: %#v", ctx)
		s, err := e.Workflow().Resume(ctx)
		if err != nil {
			panic(err)
		}
		if s == nil {
			log.Printf("Process exited")
			break
		}
		if s.Return != nil {
			log.Printf("Process returned: %v", s.Return)
			break
		}
		if s.Select != nil {
			i := rand.Intn(len(s.Select.Cases))
			log.Printf("Process stopped on select: %v, unblock random select %v", s.Select, s.Select.Cases[i])
			ctx = &ResumeContext{
				Running:       false,
				CallbackName:  s.Select.Name,
				CallbackIndex: i,
			}
			continue
		}
		if s.Step != "" {
			log.Printf("Process stopped on step: %v, continue", s.Step)
			ctx = &ResumeContext{
				Running:      false,
				CallbackName: s.Step,
			}
			continue
		}
		log.Fatalf("unexpected stop: %v", s)
	}
}


func NewExampleFlow() func() Workflow {
	return func() Workflow {
		return &ExampleFlow{}
	}
}

*/

// TODO: Step execution handling -> add 3rd mode?
// TODO: Callback execution -> Synchronous vs Asyncronous?
/*
Async execution -> simpler

Sync execution -> easier to integrate with REST API.


*/

// TODO: Subworkflows
// TODO: Goroutines

//SubWorkflow("wtf", e.DoStuff()),

/*
import (
	"encoding/json"
	"fmt"
	"time"
)

type ExampleFlow struct {
	A string
	B int
}

func (e *ExampleFlow) Definition() Definition {
	return func() []Stmt {
		return []Stmt{
			Step("Start", func() ActionResult {
				if true {
					return Fail(fmt.Errorf("failure"))
				} else {
					return Skip(fmt.Errorf("just skip"))
				}
				return Retry(fmt.Errorf("just retry"), 5, time.Minute)
			}).OnFailure(func(FailureReason) RecoverResult {
				return Stop(fmt.Errorf("failed or retries exceeded"))
			}),
			For(e.A == "true", []Stmt{
				On("/reverse", []Stmt{
					Return(),
				}),
				On("/update", []Stmt{
					Step("update", func() ActionResult {
						return nil
					}),
					Continue(),
				}),
				After(time.Minute, func() error {
					return nil
				}),
				Break(),
			}),
			Select("Waiting", []WaitCond{
				On("/reverse", func() error {
					return nil
				}),
				On("/update", []Stmt{
					Step("update", func() ActionResult {
						return nil
					}),
				}),
				After(time.Minute, func() error {
					return nil
				}),
			}),
		}
		// Go("Thread2",
		// 	On("/reverse", func() error { return nil }),
		// 	//Input("UserInput", "InputDefinition{}", func(result string) error { return nil }),
		// 	On("/update",
		// 		Step("Update", nil),
		// 		For(true, "Polling",
		// 			Step("Polling", nil),
		// 			On("/add", "Adding", func() error { return nil }),
		// 		),
		// 	),
		// 	Send("Done", true),
		// ),
		// Select("Waiting",
		// 	On("/reverse", func() error { return nil }),
		// 	On("/update",
		// 		Step("Update", nil),
		// 		For(true, "Polling",
		// 			Step("Polling", nil),
		// 			On("/add", "Adding", func() error { return nil }),
		// 		),
		// 	),
		// ),
		// Select("Waiting2",
		// 	Recv("Done", nil),
		// 	On("/cancel", nil),
		// 	After(time.Hour, "SS", nil),
		// ),
	}
}

/*
	Simpler approach:




*/

// URLS are Callbacks
// List of valid callbacks can be found by getting workflow URL

// Documentation -> Generated
// API -> more or less done

// Authentication:
// All non-public actions require authentication before any request can be made
// JWT, APIKey can be used for authentication

// ACL:
// Global roles: Global access based on ProcessType, Thread & CallbackName
// User1:  Process1:stop  Process1:/callback
// SELECT * FROM Workflows, where ProcessType in ..., Thread in ..., CallbackName in ...,

// Workflow roles: Workflow specifies which identity has right to execute specific action
// On("/callback").ACL(o.Owner, "READ_ONLY")
// SELECT * FROM Workflows, where "User1" in ACL

/*
GET /workflows
{ 	QueryParams	 }
[ 	...workflows... ]

GET /workflows/:type
{
	"input": ...
	"output": ...
	"state": ...
	"definition" : {},
}

PUT /workflows/:type
{
	"input": ...
	"output": ...
	"state": ...
	"definition": ...
	"version": ...
}

POST /workflows/:type
{
	"input": "processInput",
	"metadata": ...,
}
{
	... workflow ...
}

GET /workflows/:type/:id
{
	"state": {},
	"actions": {
		"/callback": {JSON SCHEMA}
	},
	"metadata": ...,
}

POST /workflows/:type/:id/action/:thread/:callback -> execute "On" action

If we need more functionality - let's just write custom handlers
Resume(WorkflowType, WorkflowID, ThreadID, CallbackID, JSONData) (JSONData, error)
Create(WorkflowType, JSONData) (JSONData, error)


Manual actions
POST /workflows/:type/:id/stop -> stop process execution & block callbacks
POST /workflows/:type/:id/resume -> resume after manual stop

POST /workflows/:type/:id/skip -> skip failed step and go forward
POST /workflows/:type/:id/retry -> retry failed step


Workflow statements:
- On
	Name: "/callback"
	Do: func() error
	Then: []Stmt
- Step
	Name: "step",
	Do: func() StepResult,
	Recover: func() RecoverResult,
- Select
	Name: "waiting",
	Conditions: []On|Wait|Send|Recv|Default
	Then: []Stmt
- Wait
	Then []Stmt
- For
	Stmts []Stmt

- Go
	Stmts []Stmt
- Send
	Then []Stmt
- Recv
	Then []Stmt
- Default
	Then []Stmt



type FacebookFlow struct {
}

type LinkedInFlow struct {
}

// Callback
// Do -> if error - simply return error

// Step
// Do -> if error - retry
// Recover -> if step failed -> Wait some time and try to get status of the action

// func (e *LinkedInFlow) Definition() Definition {
// 	return S(
// 		Step(func(o Order) StepResult {
// 			// Save to DB
// 			// Return Redirect URL
// 			return Retry("error, but we retry")
// 			return StopWorkflow("error, and we stop execution")
// 			return Skip("error, but we continue")
// 		}), func() RecoverResult { // if step was skipped - execute this step.
// 			return RetryStep("fixed issue, good to go again")
// 			return StopWorkflow("failed process, need to fix manually")
// 			return SkipStep("step was applied, we are good to go again")
// 		})
// 			Select(
// 				On("/callback", func() error {
// 					// Check credentials, save, redirect back
// 					return nil
// 				}),
// 				After(time.Minute*5, func() error {
// 					// Expire auth user
// 					return nil
// 				}),
// 			),
// 	)
// }

type ResumeData struct {
	Workflow   string
	WorkflowID string
	Thread     string
	Callback   string
	Data       json.RawMessage
}

// Definition
// Resume

/*
Resume(ResumeData) (json.RawMessage{}, error)
Create(WorkflowType, JSONData) (JSONData, error)

func ExampleService() Service {
	return Service{
		Workflows: map[string]Workflow{
			"linkedin": {
				Setup: func() WorkflowState {
					return &LinkedInFlow{}
				},
			},
		},
	}
}

*/

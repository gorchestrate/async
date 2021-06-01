package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/afdalwahyu/gonnel"
	cloudtasks "google.golang.org/api/cloudtasks/v2beta3"
)

var counter = 0

func jsonString(w interface{}) string {
	d, _ := json.MarshalIndent(w, "", " ")
	return string(d)
}

type CallbackRequest struct {
	WorkflowID string
	PC         int    // Make sure not actions were made while waiting for timeout
	ThreadID   string // Thread to resume
	Callback   string
}

func (t CallbackRequest) ToJSONBase64() string {
	d, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(d)
}

type ExecuteRequest struct {
	WorkflowID string
	PC         int    // Make sure not actions were made while waiting for Execute
	ThreadID   string // Thread to resume
	Step       string // step to execute
}

func (t ExecuteRequest) ToJSONBase64() string {
	d, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(d)
}

func main() {
	rand.Seed(time.Now().Unix())
	ctx := context.Background()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "secret.json")
	os.Setenv("GOOGLE_CLOUD_PROJECT", "async-315408")

	cTasks, err := cloudtasks.NewService(ctx)
	if err != nil {
		panic(err)
	}
	db, err := firestore.NewClient(ctx, "async-315408")
	if err != nil {
		panic(err)
	}

	ngrok, err := gonnel.NewClient(gonnel.Options{
		BinaryPath: "./ngrok",
	})
	if err != nil {
		panic(err)
	}
	defer ngrok.Close()

	done := make(chan bool)
	go ngrok.StartServer(done)
	<-done

	ngrok.AddTunnel(&gonnel.Tunnel{
		Proto:        gonnel.HTTP,
		Name:         "async",
		LocalAddress: "127.0.0.1:8080",
		//Auth:         "username:password",
	})

	err = ngrok.ConnectAll()
	if err != nil {
		panic(err)
	}
	url := ngrok.Tunnel[0].RemoteAddress
	log.Printf("URL: %v", url)
	defer ngrok.DisconnectAll()

	r := Runner{
		DB:      db,
		BaseURL: url,
		Tasks:   cTasks,
		Workflows: map[string]Workflow{
			"order": {
				Name: "order",
				InitState: func() WorkflowState {
					return &PizzaOrderWorkflow{}
				},
			},
		},
	}

	err = r.NewWorkflow(context.Background(), fmt.Sprint(rand.Intn(10000)), "order", PizzaOrderWorkflow{})
	if err != nil {
		panic(err)
	}
	log.Print("MANAGING")
	err = http.ListenAndServe(":8080", r.Router())
	if err != nil {
		panic(err)
	}

	// ww := map[string]Workflow{
	// 	"order": {
	// 		Name: "order",
	// 		InitState: func() WorkflowState {
	// 			return &PizzaOrderWorkflow{}
	// 		},
	// 	},
	// }
	// log.Printf("START")
	// runner, err := NewRunner(context.Background(), "postgres://user:pass@localhost/async?sslmode=disable", ww)
	// if err != nil {
	// 	panic(err)
	// }
	// go func() {
	// 	log.Fatal(http.ListenAndServe(":8081", runner.Router()))
	// }()

	// err = runner.NewWorkflow(context.Background(), "1", "order", PizzaOrderWorkflow{})
	// if err != nil {
	// 	panic(err)
	// }
	// log.Print("MANAGING")
	// err = runner.Manage(context.Background())
	// if err != nil {
	// 	panic(err)
	// }
}

type PizzaOrderWorkflow struct {
	ID          string
	OrderNumber string
	Created     time.Time
	Status      string
	Request     PizzaOrderRequest
	I           int
	Wg          int
}

type Pizza struct {
	ID    string
	Name  string
	Sause string
	Qty   int
}
type PizzaOrderResponse struct {
	OrderNumber string
}

type PizzaOrderRequest struct {
	User      string
	OrderTime time.Time
	Pizzas    []Pizza
}

// func RefundCustomer(reason string) Stmt {
// 	return S(
// 		Step("call manager - "+reason, func() ActionResult {
// 			log.Printf("call manager")
// 			return ActionResult{Success: true}
// 		}),
// 		Step("refund customer - "+reason, func() ActionResult {
// 			log.Printf("refund customer")
// 			return ActionResult{Success: true}
// 		}),
// 		Return(),
// 	)
// }

// Add WAIT() condition function that is evaluated  each time after process is updated
// useful to simulate sync.WorkGroup
// can be added to Select() stmt

/*
// GCLOUD:
Firestore as storage
	- Optimistic locking
Cloud tasks to delay time.After
	- if select canceled - we try to cancel task
		- if can't cancel - no problem - it should cancel itself

Context canceling?
	- Canceling is batch write operation, updates both task and canceled ctx
	- After ctx has been updated via cloud - we create cloud tasks for each workflow to update
		all processes that were affected. (for now can be done as post-action, later listen for event)
	- We also monitor writes for all processes and if they have new ctx added
		we will create cloud task for them. (for now can be done as post-action, later listen for event)


// CONCLUSION:
	- TRY OUT FIRESTORE & Cloud Tasks time.After
	- PERFECT CLIENTSIDE LIBRARY
	- WAIT FOR JUNE & CHECK HOW EVENT ARC UPDATE NOTIFYING WORKS (latency)


Sending/Receiving channels:
	JUST DO IN-PROCESS channels
	AND IN-PROCESS WAIT
*/

// Add channels? not sure we need them.
// All our process is executed in 1 thread and most of the time they are useless.
// Make them scoped only to 1 process (for now, because it's easy to screw it up)

func (e *PizzaOrderWorkflow) Definition() WorkflowDefinition {
	return WorkflowDefinition{
		New: func(req PizzaOrderRequest) (*PizzaOrderResponse, error) {
			log.Printf("got pizza order")
			return &PizzaOrderResponse{}, nil
		},
		Body: S(
			Step("init", func() ActionResult {
				log.Printf("init step")
				return ActionResult{Success: true}
			}),
			For(e.I < 2, "creating threads", S(
				Step("create thread1", func() ActionResult {
					e.I++
					e.Wg++
					log.Print("INCREMENT")
					return ActionResult{Success: true}
				}),
				Go("parallel thread", S(
					Step("init parallel1", func() ActionResult {
						log.Printf("init  parallel1 step !!@!#!@#!@#!@")
						return ActionResult{Success: true}
					}),
					Select("wait 1 seconds",
						After(time.Second*5, S())),
					Step("init parallel2", func() ActionResult {
						log.Printf("init  parallel1 step 2 !@#!#!@#!@")
						e.Wg--
						return ActionResult{Success: true}
					}),
				), func() string {
					log.Print("I : ", e.I)
					return fmt.Sprint(e.I)
				}),
			)),
			Select("wait 5 seconds",
				Wait("cond1", e.Wg == 0, S(
					Step("WG STEP", func() ActionResult {
						log.Printf("WG step2")
						return ActionResult{Success: true}
					}),
				)),
				After(time.Second*60, S())),
			Step("init2", func() ActionResult {
				log.Printf("init step2")
				return ActionResult{Success: true}
			}),
			Return("result"),
		),
	}

	// 	New: func(req PizzaOrderRequest) (*PizzaOrderResponse, error) {
	// 		log.Printf("got pizza order")
	// 		if len(req.Pizzas) == 0 {
	// 			return nil, fmt.Errorf("0 pizzas supplied")
	// 		}
	// 		counter++
	// 		e.OrderNumber = fmt.Sprint(counter)
	// 		e.Request = req
	// 		return &PizzaOrderResponse{OrderNumber: e.OrderNumber}, nil
	// 	},
	// 	Body: S(
	// 		Select("waiting in queue",
	// 			After(time.Minute*5, S(
	// 				Step("order timed out", func() ActionResult {
	// 					e.Status = "NotTaken"
	// 					return ActionResult{Success: true}
	// 				}),
	// 				Step("notify restaraunt manager", func() ActionResult {
	// 					return ActionResult{Success: true}
	// 				}),
	// 				Step("notify client", func() ActionResult {
	// 					return ActionResult{Success: true}
	// 				}),
	// 				Select("solve prolem",
	// 					After(time.Hour*24, S(Step("NotTakenOrder timed out", func() ActionResult {
	// 						log.Printf("order was not taken and manager did not made any action")
	// 						return ActionResult{Success: true}
	// 					}), Return())),
	// 					On("/confirmTimedOutOrder", nil, nil),
	// 					On("/failOrder", nil, S(Step("failOrder", func() ActionResult {
	// 						log.Printf("order failed")
	// 						return ActionResult{Success: true}
	// 					}), Return())),
	// 				),
	// 			)),
	// 			On("/confirmOrder", nil, nil),
	// 		),
	// 		Step("Move To Kitchen", func() ActionResult {
	// 			e.Status = "InKitchen"
	// 			return ActionResult{Success: true}
	// 		}),
	// 		Select("wait for cook to take pizza",
	// 			After(time.Minute*30, RefundCustomer("pizza not taken")),
	// 			On("/startPreparing", func() {
	// 				e.Status = "Cooking"
	// 			}, nil),
	// 		),
	// 		Select("wait for pizza to be prepared",
	// 			After(time.Minute*30, RefundCustomer("pizza not prepared")),
	// 			On("/startPreparing", func() {
	// 				e.Status = "Prepared"
	// 			}, nil),
	// 		),
	// 		Go("thread2", nil, S(
	// 			Step("step1 inside thread2", func() ActionResult {
	// 				log.Printf("step1 inside thread2")
	// 				return ActionResult{Success: true}
	// 			}),
	// 			Step("step2 inside thread2", func() ActionResult {
	// 				log.Printf("step2 inside thread2")
	// 				return ActionResult{Success: true}
	// 			}),
	// 		)),
	// 		Step("wait to delivery", func() ActionResult {
	// 			log.Printf("add to delivery queue")
	// 			return ActionResult{Success: true}
	// 		}),
	// 		Select("wait for thread2 steps",
	// 			After(time.Minute*1, Return()),
	// 		),
	// 	),
	// }
}

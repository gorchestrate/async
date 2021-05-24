package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

var counter = 0

func jsonString(w interface{}) string {
	d, _ := json.MarshalIndent(w, "", " ")
	return string(d)
}

// func SwaggerHandler(ww map[string]Workflow) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		paths := map[string]interface{}{}
// 		defs := map[string]interface{}{
// 			"Error": map[string]interface{}{
// 				"properties": map[string]interface{}{
// 					"code": map[string]string{
// 						"type": "string",
// 					},
// 					"message": map[string]string{
// 						"type": "string",
// 					},
// 				},
// 			},
// 		}
// 		for _, w := range ww {
// 			wdoc := Docs(w.InitState().Definition())
// 			for handler, schema := range wdoc.Handlers {
// 				for k, def := range schema.Input.Definitions {
// 					defs[k] = cleanVersion(def)
// 					log.Printf("add definition: %v", k)
// 				}
// 				for k, def := range schema.Output.Definitions {
// 					defs[k] = cleanVersion(def)
// 					log.Printf("add definition: %v", k)
// 				}
// 				paths[fmt.Sprintf("/%v/{id}%v", w.Name, handler)] = map[string]interface{}{
// 					"post": map[string]interface{}{
// 						"consumes": []string{"application/json"},
// 						"produces": []string{"application/json"},
// 						"parameters": []interface{}{
// 							map[string]interface{}{
// 								"in":   "body",
// 								"name": fmt.Sprintf("%v", w.Name),
// 								"schema": map[string]string{
// 									"$ref": schema.Input.Ref,
// 								},
// 							},
// 							map[string]interface{}{
// 								"required": true,
// 								"in":       "path",
// 								"name":     "id",
// 								"type":     "string",
// 							},
// 						},
// 						"responses": map[string]interface{}{
// 							"200": map[string]interface{}{
// 								"description": "OK",
// 								"schema": map[string]string{
// 									"$ref": schema.Output.Ref,
// 								},
// 							},
// 							"400": map[string]interface{}{
// 								"description": "Error",
// 								"schema": map[string]string{
// 									"$ref": "#/definitions/Error",
// 								},
// 							},
// 						},
// 					},
// 				}

// 			}
// 		}

// 		res := map[string]interface{}{
// 			"swagger": "2.0",
// 			"info": map[string]interface{}{
// 				"title":   "SampleAPI",
// 				"version": "1.0.0",
// 			},
// 			"host":        "api.example.com",
// 			"basePath":    "/workflows",
// 			"schemes":     []string{"https"},
// 			"paths":       paths,
// 			"definitions": defs,
// 		}
// 		e := json.NewEncoder(w)
// 		e.SetIndent("", " ")
// 		e.Encode(res)
// 	}
// }

//r := mux.NewRouter()
//r.HandleFunc("/swagger", SwaggerHandler(ww))
//http.ListenAndServe(":8080", r)

func main() {
	ww := map[string]Workflow{
		"order": {
			Name: "order",
			InitState: func() WorkflowState {
				return &PizzaOrderWorkflow{}
			},
		},
	}
	log.Printf("START")
	runner, err := NewRunner(context.Background(), "postgres://user:pass@localhost/async?sslmode=disable", ww)
	if err != nil {
		panic(err)
	}
	go func() {
		log.Fatal(http.ListenAndServe(":8081", runner.Router()))
	}()

	err = runner.NewWorkflow(context.Background(), "1", "order", PizzaOrderWorkflow{})
	if err != nil {
		panic(err)
	}
	log.Print("MANAGING")
	err = runner.Manage(context.Background())
	if err != nil {
		panic(err)
	}
}

type PizzaOrderWorkflow struct {
	ID          string
	OrderNumber string
	Created     time.Time
	Status      string
	Request     PizzaOrderRequest
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

func RefundCustomer(reason string) Stmt {
	return S(
		Step("call manager - "+reason, func() ActionResult {
			log.Printf("call manager")
			return ActionResult{Success: true}
		}),
		Step("refund customer - "+reason, func() ActionResult {
			log.Printf("refund customer")
			return ActionResult{Success: true}
		}),
		Return(),
	)
}

func (e *PizzaOrderWorkflow) Definition() WorkflowDefinition {
	return WorkflowDefinition{
		New: func(req PizzaOrderRequest) (*PizzaOrderResponse, error) {
			log.Printf("got pizza order")
			if len(req.Pizzas) == 0 {
				return nil, fmt.Errorf("0 pizzas supplied")
			}
			counter++
			e.OrderNumber = fmt.Sprint(counter)
			e.Request = req
			return &PizzaOrderResponse{OrderNumber: e.OrderNumber}, nil
		},
		Body: S(
			Select("waiting in queue",
				After(time.Minute*5, S(
					Step("order timed out", func() ActionResult {
						e.Status = "NotTaken"
						return ActionResult{Success: true}
					}),
					Step("notify restaraunt manager", func() ActionResult {
						return ActionResult{Success: true}
					}),
					Step("notify client", func() ActionResult {
						return ActionResult{Success: true}
					}),
					Select("solve prolem",
						After(time.Hour*24, S(Step("NotTakenOrder timed out", func() ActionResult {
							log.Printf("order was not taken and manager did not made any action")
							return ActionResult{Success: true}
						}), Return())),
						On("/confirmTimedOutOrder", nil, nil),
						On("/failOrder", nil, S(Step("failOrder", func() ActionResult {
							log.Printf("order failed")
							return ActionResult{Success: true}
						}), Return())),
					),
				)),
				On("/confirmOrder", nil, nil),
			),
			Step("Move To Kitchen", func() ActionResult {
				e.Status = "InKitchen"
				return ActionResult{Success: true}
			}),
			Select("wait for cook to take pizza",
				After(time.Minute*30, RefundCustomer("pizza not taken")),
				On("/startPreparing", func() {
					e.Status = "Cooking"
				}, nil),
			),
			Select("wait for pizza to be prepared",
				After(time.Minute*30, RefundCustomer("pizza not prepared")),
				On("/startPreparing", func() {
					e.Status = "Prepared"
				}, nil),
			),
			Go("thread2", nil, S(
				Step("step1 inside thread2", func() ActionResult {
					log.Printf("step1 inside thread2")
					return ActionResult{Success: true}
				}),
				Step("step2 inside thread2", func() ActionResult {
					log.Printf("step2 inside thread2")
					return ActionResult{Success: true}
				}),
			)),
			Step("wait to delivery", func() ActionResult {
				log.Printf("add to delivery queue")
				return ActionResult{Success: true}
			}),
			Select("wait for thread2 steps",
				After(time.Minute*1, Return()),
			),
		),
	}
}

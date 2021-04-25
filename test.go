package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"github.com/gorilla/mux"
)

var g = graphviz.New()

var counter = 0

func newNodeWithID(graph *cgraph.Graph, id, stmt, name string) *cgraph.Node {
	n, err := graph.CreateNode(id)
	if err != nil {
		panic(err)
	}
	n.SetShape(cgraph.BoxShape)
	switch stmt {
	case "Handler":
		n.SetShape(cgraph.Shape("record"))
	case "Task":
		n.SetShape(cgraph.BoxShape)
	case "Wait":
		n.SetShape(cgraph.MdiamondShape)
	case "XOR":
		n.SetShape(cgraph.DiamondShape)
	case "START":
		n.SetShape(cgraph.CircleShape)
		n.SetArea(0.1)
		n.SetFontSize(10)
	case "END":
		n.SetShape(cgraph.DoubleCircleShape)
		n.SetArea(0.1)
		n.SetFontSize(10)
	}
	n.SetLabel(name)
	return n
}

func newNode(graph *cgraph.Graph, stmt, name string) *cgraph.Node {
	counter++
	return newNodeWithID(graph, fmt.Sprint(counter), stmt, name)
}

func makeEdges(graph *cgraph.Graph, src []*cgraph.Node, dst *cgraph.Node, name string) {
	counter++
	for _, prev := range src {
		e, _ := graph.CreateEdge(fmt.Sprint(counter), prev, dst)
		if name != "" {
			e.SetLabel(name)
		}
	}
}

func jsonString(w interface{}) string {
	d, _ := json.MarshalIndent(w, "", " ")
	return string(d)
}
func GWalk(graph *cgraph.Graph, pp []*cgraph.Node, s Stmt, ename string) ([]*cgraph.Node, string) {
	log.Print("GWALK: ", reflect.TypeOf(s))
	switch x := s.(type) {
	case nil:
		return pp, ""
	case ReturnStmt:
		n := newNode(graph, "END", "END")
		makeEdges(graph, pp, n, ename)
		return nil, ""
	case StmtStep:
		n := newNode(graph, "Step", x.Name)
		makeEdges(graph, pp, n, ename)
		return []*cgraph.Node{n}, ""
	case SelectStmt:
		n := newNode(graph, "Wait", "Wait")
		makeEdges(graph, pp, n, ename)
		ret := []*cgraph.Node{}
		for _, v := range x.Cases {
			name := "{<f0> POST " + v.CaseEvent +
				"|<f1>" + strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(jsonString(ReflectDoc(v.Handler, true).Input), "{", "\\{"), "}", "\\}"), "\n", "\\n") +
				"|<f2> Output: " + strings.TrimPrefix(ReflectDoc(v.Handler, true).Output.Ref, "#/definitions/") +
				"  }"
			if v.CaseEvent == "" {
				name = fmt.Sprintf("after %.0f secs", v.CaseAfter.Seconds())
			}
			wn := newNode(graph, "Handler", name)
			// if v.CaseEvent != "" {
			// 	nIn := newNode(graph, "Doc", "Input")
			// 	nOut := newNode(graph, "Doc", "Output")
			// 	makeEdges(graph, []*cgraph.Node{nIn}, wn, "")
			// 	makeEdges(graph, []*cgraph.Node{wn}, nOut, "")
			// }

			graph.CreateEdge("", n, wn)
			localPP, _ := GWalk(graph, []*cgraph.Node{wn}, v.Stmt, "")
			ret = append(ret, localPP...)
		}
		return ret, ""
	case ForStmt:
		n := newNode(graph, "XOR", x.CondLabel+"?")
		makeEdges(graph, pp, n, ename)
		localPP, _ := GWalk(graph, []*cgraph.Node{n}, x.Stmt, "Yes")
		for _, p := range localPP {
			graph.CreateEdge("", p, n)
		}
		return []*cgraph.Node{n}, "No"
	case SwitchStmt:
		n := newNode(graph, "XOR", "XOR")
		makeEdges(graph, pp, n, ename)
		ret := []*cgraph.Node{n}
		for _, v := range x {
			localPP, _ := GWalk(graph, []*cgraph.Node{n}, v.Stmt, v.CondLabel)
			ret = append(ret, localPP...)
		}
		return ret, "default"
	case Section:
		log.Print("SECTION")
		localPP := pp
		for _, v := range x {
			localPP, ename = GWalk(graph, localPP, v, ename)
		}
		return localPP, ename
	default:
		panic(fmt.Sprintf("unknown statement: %v", reflect.TypeOf(s)))
	}
	return pp, ename
}

func jsonErr(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(struct {
		Code    string
		Message string
	}{
		Code:    "GeneralError",
		Message: err.Error(),
	})
}

func GraphVizHandler(ww map[string]Workflow) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		wf, ok := ww[mux.Vars(r)["name"]]
		if !ok {
			jsonErr(w, fmt.Errorf("workflow not found"), 404)
			return
		}
		err := renderGraphvizSVG(wf, w)
		if err != nil {
			jsonErr(w, fmt.Errorf("render err: %v", err), 500)
			return
		}
	}
}

func SwaggerHandler(ww map[string]Workflow) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		paths := map[string]interface{}{}
		defs := map[string]interface{}{
			"Error": map[string]interface{}{
				"properties": map[string]interface{}{
					"code": map[string]string{
						"type": "string",
					},
					"message": map[string]string{
						"type": "string",
					},
				},
			},
		}
		for _, w := range ww {
			wdoc := Docs(w.InitState().Definition())
			for handler, schema := range wdoc.Handlers {
				for k, def := range schema.Input.Definitions {
					defs[k] = cleanVersion(def)
					log.Printf("add definition: %v", k)
				}
				for k, def := range schema.Output.Definitions {
					defs[k] = cleanVersion(def)
					log.Printf("add definition: %v", k)
				}
				paths[fmt.Sprintf("/%v/{id}%v", w.Name, handler)] = map[string]interface{}{
					"post": map[string]interface{}{
						"consumes": []string{"application/json"},
						"produces": []string{"application/json"},
						"parameters": []interface{}{
							map[string]interface{}{
								"in":   "body",
								"name": fmt.Sprintf("%v", w.Name),
								"schema": map[string]string{
									"$ref": schema.Input.Ref,
								},
							},
							map[string]interface{}{
								"required": true,
								"in":       "path",
								"name":     "id",
								"type":     "string",
							},
						},
						"responses": map[string]interface{}{
							"200": map[string]interface{}{
								"description": "OK",
								"schema": map[string]string{
									"$ref": schema.Output.Ref,
								},
							},
							"400": map[string]interface{}{
								"description": "Error",
								"schema": map[string]string{
									"$ref": "#/definitions/Error",
								},
							},
						},
					},
				}

			}
		}

		res := map[string]interface{}{
			"swagger": "2.0",
			"info": map[string]interface{}{
				"title":   "SampleAPI",
				"version": "1.0.0",
			},
			"host":        "api.example.com",
			"basePath":    "/workflows",
			"schemes":     []string{"https"},
			"paths":       paths,
			"definitions": defs,
		}
		e := json.NewEncoder(w)
		e.SetIndent("", " ")
		e.Encode(res)
	}
}

func renderGraphvizSVG(w Workflow, out io.Writer) error {
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	prev := newNodeWithID(graph, "START", "START", "START")
	nn, ename := GWalk(graph, []*cgraph.Node{prev}, w.InitState().Definition().Body, "")
	end := newNode(graph, "END", "END")
	makeEdges(graph, nn, end, ename)
	var buf bytes.Buffer
	if err := g.Render(graph, graphviz.SVG, &buf); err != nil {
		log.Fatal(err)
	}
	graph.SetRankSeparator(0.4)

	return g.Render(graph, graphviz.SVG, out)
}

func main() {
	ww := map[string]Workflow{
		"order": {
			Name: "order",
			InitState: func() WorkflowState {
				return &PizzaOrderWorkflow{}
			},
		},
	}
	// r := mux.NewRouter()
	// r.HandleFunc("/diagram/{name}", GraphVizHandler(ww))
	// r.HandleFunc("/swagger", SwaggerHandler(ww))
	// http.ListenAndServe(":8080", r)
	// log.Printf("START")
	runner, err := NewRunner(context.Background(), "postgres://user:pass@localhost/async?sslmode=disable", ww)
	if err != nil {
		panic(err)
	}
	go func() {
		log.Fatal(http.ListenAndServe(":8081", runner.Router()))
	}()

	err = runner.NewWorkflow(context.Background(), "1", "PizzaOrderWorkflow", PizzaOrderWorkflow{})
	// s := State{
	// 	ID:         "1",
	// 	Workflow:   "PizzaOrderWorkflow",
	// 	State:      json.RawMessage(`{}`),
	// 	WaitEvents: []string{},
	// 	CurStep:    "init",
	// 	Status:     "Resuming",
	// 	Output:     json.RawMessage(`{}`),
	// }
	// err = s.Insert(context.Background(), runner.db)
	// if err != nil {
	// 	panic(err)
	// }

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
			Step("wait to delivery", func() ActionResult {
				log.Printf("add to delivery queue")
				return ActionResult{Success: true}
			}),
		),
	}
}

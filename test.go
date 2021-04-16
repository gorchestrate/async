package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"time"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
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
		n.SetShape(cgraph.RArrowShape)
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

func GWalk(graph *cgraph.Graph, pp []*cgraph.Node, s Stmt, ename string) ([]*cgraph.Node, string) {
	log.Print("GWALK: ", reflect.TypeOf(s))
	switch x := s.(type) {
	case nil:
		return nil, ""
	case ReturnStmt:
		log.Print("RETURN")
		n := newNode(graph, "END", "END")
		makeEdges(graph, pp, n, ename)
		return nil, ""
	case StmtStep:
		log.Print("STEP")
		n := newNode(graph, "Step", x.Name)
		makeEdges(graph, pp, n, ename)
		return []*cgraph.Node{n}, ""
	case SelectStmt:
		log.Print("SELECT")
		n := newNode(graph, "Wait", "Wait")
		makeEdges(graph, pp, n, ename)
		ret := []*cgraph.Node{}
		for _, v := range x.Cases {
			name := "POST " + v.CaseEvent + "  "
			if v.CaseEvent == "" {
				name = fmt.Sprintf("after %.0f secs", v.CaseAfter.Seconds())
			}
			wn := newNode(graph, "Handler", name)
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

func GraphVizDot(w Workflow) string {
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	prev := newNodeWithID(graph, "START", "START", "START")
	nn, ename := GWalk(graph, []*cgraph.Node{prev}, w.InitState().Definition().Body, "")
	end := newNode(graph, "END", "END")
	makeEdges(graph, nn, end, ename)
	var buf bytes.Buffer
	if err := g.Render(graph, graphviz.PNG, &buf); err != nil {
		log.Fatal(err)
	}
	graph.SetRankSeparator(0)

	// 3. write to file directly
	if err := g.RenderFilename(graph, graphviz.PNG, "graph.png"); err != nil {
		log.Fatal(err)
	}
	return ""
}

func main() {
	GraphVizDot(Workflow{
		Name: "transaction",
		InitState: func() WorkflowState {
			return &TransactionFlow{}
		},
	})
	panic("WTF")
	d, err := Swagger(map[string]Workflow{
		"transaction": {
			Name: "transaction",
			InitState: func() WorkflowState {
				return &TransactionFlow{}
			},
		},
	})
	if err != nil {
		panic(err)
	}
	log.Print(string(d))
	return
	runner, err := NewRunner(context.Background(), "postgres://user:pass@localhost/async?sslmode=disable", []Workflow{})
	if err != nil {
		panic(err)
	}
	go func() {
		log.Fatal(http.ListenAndServe(":8081", runner.Router()))
	}()

	err = runner.NewWorkflow(context.Background(), "1", "TransactionFlow", TransactionFlow{})
	// s := State{
	// 	ID:         "1",
	// 	Workflow:   "TransactionFlow",
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

type TransactionFlow struct {
	Created     time.Time
	Status      string
	PaymentInfo PaymentInfo
	DstAccount  AccountInfo
	Amount      int
}

type AccountInfo struct {
	Token      string
	ExpMonth   int
	ExpYear    int
	NameOnCard string
}

type PaymentInfo struct {
	BankAccount string
}

type TransactionInput struct {
	PaymentInfo PaymentInfo
	Amount      int
}

func (e *TransactionFlow) Definition() WorkflowDefinition {
	return WorkflowDefinition{
		New: func(input TransactionInput) (*TransactionFlow, error) {
			log.Printf("transaction created")
			if e.Amount < 0 {
				return nil, fmt.Errorf("zmount less than 0")
			}
			e.Amount = input.Amount
			e.PaymentInfo = input.PaymentInfo
			e.Status = "Created"
			return e, nil
		},
		Body: S(
			// Step is an asyncronous task that is automatically retried on failure.
			Step("start", func() ActionResult {
				log.Print("sending request")
				return ActionResult{Success: true}
			}),
			For(e.Status == "Ready", "trans ready", S(
				Step("s1", func() ActionResult {
					log.Print("sending request")
					return ActionResult{Success: true}
				}),
				Step("s2", func() ActionResult {
					log.Print("sending request")
					return ActionResult{Success: true}
				}),
			)),
			WaitEvent("/capture", func(string) string {
				e.Status = "Captured"
				return "OK"
			}),
			Select("grace period",
				On("/cancel", func(string) string {
					e.Status = "Canceled"
					return "OK"
				}, Return()),
				After(time.Hour, Step("captureTransaction", func() ActionResult {
					log.Print("Capture transaction")
					return ActionResult{Success: true}
				})),
			),
			WaitFor("ready to process", time.Minute*5),
			For(e.Status == "Captured", "Trans in Captured State",
				Select("waiting for trans status",
					After(time.Hour, Step("poll transaction status",
						func() ActionResult {
							log.Print("poll transaciton")
							e.Status = "Rejected" // e.Status == "Completed"
							return ActionResult{Success: true}
						}),
					),
					On("/refund", func(amount int) string {
						e.Amount = e.Amount - amount
						e.Status = "refunded"
						return "OK"
					}, Return()),
				),
			),
			If(e.Status == "Rejected", "Trans Rejected", WaitEvent("/forceDeposit", func(string) string {
				e.Status = "ForceDeposited"
				return "OK"
			}, Return())),
		),
	}
}

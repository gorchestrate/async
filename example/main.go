package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gladkikhartem/async"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var services = []async.Service{
	{
		Name: "pizza",
		Types: []*async.Type{
			async.ReflectType("pizza.Order", &Order{}),
			async.ReflectType("pizza.ConfirmedOrder", &ConfirmedOrder{}),
			async.ReflectType("pizza.OrderPizzaProcess", &OrderPizzaProcess{}),
		},
		APIs: []async.API{
			{
				NewProcState: func() interface{} {
					return &OrderPizzaProcess{}
				},
				API: &async.ProcessAPI{
					Name:        "pizza.OrderPizza()",
					Description: "Order new pizza",
					Input:       "pizza.Order",
					Service:     "pizza",
					Output:      "pizza.ConfirmedOrder",
					State:       "pizza.OrderPizzaProcess",
				},
			},
		},
	},
}

type Service struct {
	c async.RuntimeClient
}

func (s Service) NewOrderHandler(w http.ResponseWriter, r *http.Request) {
	var o Order
	err := json.NewDecoder(r.Body).Decode(&o)
	if err != nil {
		fmt.Fprintf(w, "error creating process: %v", err)
		return
	}
	if o.Address == "" {
		fmt.Fprintf(w, "missing order address")
		return
	}
	if len(o.Pizzas) == 0 {
		fmt.Fprintf(w, "at least 1 pizza required")
		return
	}
	for i, p := range o.Pizzas {
		if p.Name == "" {
			fmt.Fprintf(w, "pizza  %v name missing", i)
			return
		}
		if p.Size == 0 {
			fmt.Fprintf(w, "pizza  %v size missing", i)
			return
		}
	}
	body, _ := json.Marshal(o)
	_, err = s.c.NewProcess(context.Background(), &async.NewProcessReq{
		Call: &async.Call{
			Id:    mux.Vars(r)["id"],
			Name:  "pizza.OrderPizza()",
			Input: body,
		},
	})
	if err != nil {
		fmt.Fprintf(w, "error creating process: %v", err)
	}
}

func (s Service) GetOrderHandler(w http.ResponseWriter, r *http.Request) {
	p, err := s.c.GetProcess(r.Context(), &async.GetProcessReq{
		Id: mux.Vars(r)["id"],
	})
	if err != nil {
		fmt.Fprintf(w, "error creating process: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"Status": p.Status.String(),
		"Output": json.RawMessage(p.Output),
	})

}

func main() {
	ctx := context.Background()
	conn, err := grpc.Dial(":9090", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := async.NewRuntimeClient(conn)

	s := Service{
		c: client,
	}
	r := mux.NewRouter()
	r.HandleFunc("/order/{id}", s.NewOrderHandler).Methods("POST")
	r.HandleFunc("/order/{id}", s.GetOrderHandler).Methods("GET")

	go func() {
		err := http.ListenAndServe(":8080", r)
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	err = async.Manage(ctx, client, services...)
	if err != nil {
		log.Fatal(err)
	}
}

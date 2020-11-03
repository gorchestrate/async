package main

import (
	"fmt"
	"time"

	"github.com/gladkikhartem/async"
	"github.com/gladkikhartem/async/plugins/mail"
)

type Pizza struct {
	Name string
	Size int
}

type Order struct {
	Pizzas  []Pizza
	Address string
}

func (s Order) Type() *async.Type {
	return async.ReflectType("pizza.Order", s)
}

type ConfirmedOrder struct {
	Order    Order
	Approved bool
	Message  string
}

func (s ConfirmedOrder) Type() *async.Type {
	return async.ReflectType("pizza.ConfirmedOrder", s)
}

type OrderPizzaProcess struct {
	Order   Order
	Thread2 string
}

func (s OrderPizzaProcess) Type() *async.Type {
	return async.ReflectType("pizza.OrderPizzaProcess", s)
}

func (s *OrderPizzaProcess) Start(p *async.P, order Order) error {
	s.Order = order
	p.Call("mail.Approve()", mail.ApprovalRequest{
		Message: fmt.Sprintf("Please approve order: %#v", order),
		To:      []string{"artem.gladkikh@idt.net"},
	}).To(s.Done)

	p.Go("Thread2", func(p *async.P) {
		p.After(time.Second * 30).To(s.Aborted)
	})
	return nil
}

func (s *OrderPizzaProcess) Done(p *async.P, resp mail.ApprovalResponse) error {
	p.Finish(ConfirmedOrder{
		Order:    s.Order,
		Approved: resp.Approved,
		Message:  resp.Comments,
	})
	return nil
}

func (s *OrderPizzaProcess) Aborted(p *async.P) error {
	p.Finish(ConfirmedOrder{
		Order:    s.Order,
		Approved: false,
		Message:  "order was not confirmed in time",
	})
	return nil
}

package async

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
	"time"

	"golang.org/x/net/context"
)

type W struct {
	workflow      *Workflow
	resumedThread *Thread
	newThread     *Thread
	procStruct    interface{}
	client        RuntimeClient
	counter       *int // generate stable ID's
	lockLeft      time.Duration
}

func (p *W) NewID() string {
	return fmt.Sprintf("%v_%v", p.resumedThread.UnblockedAt, p.counter)
}

func (p *W) MakeChan(t *Type, bufsize int) Channel {
	id := p.NewID()
	ch := Channel{
		Id:         id,
		DataType:   t.Id,
		BufMaxSize: uint64(bufsize),
	}
	_, err := p.client.MakeChan(context.Background(), &MakeChanReq{
		Chan: &ch,
	})
	if err != nil {
		panic(err)
	}
	return ch
}

func (p *W) ID() string {
	return p.workflow.Id
}

// Name is a workflow name (not ID).
func (p *W) Name() string {
	return p.workflow.Name
}

// Name is a workflow name (not ID).
func (p *W) Service() string {
	return p.workflow.Service
}

func (p *W) resume(s interface{}) error {
	switch p.workflow.Status {
	case Workflow_Started:
		err := p.call(s, "Main", p.workflow.Input)
		if err != nil {
			return err
		}
	case Workflow_Running:
		var input []byte
		switch {
		case p.resumedThread.Call != nil:
			input = p.resumedThread.Call.Output
		case p.resumedThread.Select != nil:
			input = p.resumedThread.Select.RecvData
		}
		if p.resumedThread.ToStatus != "" {
			err := p.call(s, p.resumedThread.ToStatus, input)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected resumed workflow status: %v", p.workflow.Status)
	}
	return nil
}

func (p *W) call(s interface{}, name string, input []byte) error {
	method := reflect.ValueOf(s).MethodByName(name)
	//TODO: method not found error
	inputs := method.Type().NumIn()
	if inputs != 1 && inputs != 2 {
		return fmt.Errorf("%v method %v should have input in format Thread_Status(p *W [,Input])", reflect.TypeOf(s), name)
	}

	if inputs == 1 {
		err := method.Call([]reflect.Value{reflect.ValueOf(p)})[0].Interface()
		if err == nil {
			return nil
		}
		return err.(error)
	}

	// TODO: better type validations (input, output types)
	in := reflect.New(method.Type().In(1))
	if len(input) != 0 {
		err := json.Unmarshal(input, in.Interface())
		if err != nil {
			return err
		}
	}
	log.Print(in, in.Type())
	err := method.Call([]reflect.Value{reflect.ValueOf(p), in.Elem()})[0].Interface()
	if err == nil {
		return nil
	}
	return err.(error)
}

func (p *W) Finish(result interface{}) *W {
	out, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	p.workflow.Output = out
	p.workflow.Status = Workflow_Finished
	return p
}

func (p *W) lastSel() *Select {
	if p.newThread == nil {
		p.newThread = &Thread{
			Id:       p.resumedThread.Id,
			Workflow: p.resumedThread.Workflow,
			Service:  p.workflow.Service,
			Status:   Thread_Blocked,
		}
	}
	if p.newThread.Select == nil {
		p.newThread.Select = &Select{}
	}
	return p.newThread.Select
}

func (p *W) checkNoDefault() {
	for _, c := range p.lastSel().Cases {
		if c.Op == Case_Default {
			panic("default case should always be the last one")
		}
	}
}

// for readability
func (p *W) Select() *W {
	return p
}

func (p *W) After(after time.Duration) *W {
	return p.At(time.Now().Add(after))
}

func (p *W) At(at time.Time) *W {
	p.checkNoDefault()
	ls := p.lastSel()
	ls.Cases = append(ls.Cases, &Case{
		Op:   Case_Time,
		Time: uint64(at.Unix()),
	})
	return p
}

func (p *W) To(cb interface{}) *W { // TODO: check data type, based on Channel in previous operation?
	to := GetFunctionName(cb)
	to = to[strings.LastIndex(to, ".")+1:]
	to = strings.TrimSuffix(to, "-fm")

	method := reflect.ValueOf(p.procStruct).MethodByName(to)
	if method.IsZero() {
		panic(fmt.Sprintf("To status %v does not match any method", to))
	}
	inputType := "async.None"
	if method.Type().NumIn() > 1 {
		inputType = reflect.New(method.Type().In(1)).Interface().(AsyncType).Type().Id
	}

	if p.newThread.Select != nil {
		ls := p.lastSel()
		cs := ls.Cases[len(ls.Cases)-1]
		cs.ToStatus = to
		if cs.Op == Case_Recv {
			cs.DataType = inputType
		}
	}
	if p.newThread.Call != nil {
		p.newThread.ToStatus = to
		p.newThread.Call.OutputType = inputType
	}
	return p
}

func (p *W) Recv(channel Channel) *W {
	p.checkNoDefault()
	ls := p.lastSel()
	ls.Cases = append(ls.Cases, &Case{
		Chan: channel.Id,
		Op:   Case_Recv,
	})
	return p
}

func (p *W) Send(channel Channel, data interface{}) *W {
	p.checkNoDefault()
	d, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	ls := p.lastSel()
	ls.Cases = append(ls.Cases, &Case{
		Chan:     channel.Id,
		Op:       Case_Send,
		Data:     d,
		DataType: data.(AsyncType).Type().Id,
	})
	return p
}

func (p *W) Go(id string, f func(p *W)) {
	for _, v := range p.workflow.Threads {
		if v.Id == id {
			panic("starting goroutine twice")
		}
	}
	t := &Thread{
		Id:       id,
		Workflow: p.resumedThread.Workflow,
		Service:  p.workflow.Service,
		Status:   Thread_Blocked,
	}
	p.workflow.Threads = append(p.workflow.Threads, t)
	newThread := &W{
		workflow:      p.workflow,
		newThread:     t,
		procStruct:    p.procStruct,
		client:        p.client,
		resumedThread: p.resumedThread,
		counter:       p.counter,
	}
	f(newThread)
}

func (p *W) Default() *W {
	p.checkNoDefault()
	ls := p.lastSel()
	ls.Cases = append(ls.Cases, &Case{
		Op: Case_Default,
	})
	return p
}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func (p *W) Call(name string, input interface{}) *W {
	in, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}
	if p.newThread == nil { // operation on same thread
		p.newThread = &Thread{
			Id:       p.resumedThread.Id,
			Workflow: p.resumedThread.Workflow,
			Service:  p.workflow.Service,
			Status:   Thread_Blocked,
		}
	}
	p.newThread.Call = &Call{
		Id:        p.NewID(),
		Name:      name,
		Input:     in,
		InputType: input.(AsyncType).Type().Id,
	}
	return p
}

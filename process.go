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

type P struct {
	process       *Process
	resumedThread *Thread
	newThread     *Thread
	procStruct    interface{}
	client        RuntimeClient
	counter       *int // generate stable ID's
}

func (p *P) NewID() string {
	return fmt.Sprintf("%v_%v", p.resumedThread.UnblockedAt, p.counter)
}

func (p *P) MakeChan(t *Type, bufsize int) Channel {
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

func (p *P) ID() string {
	return p.process.Id
}

// Name is a process name (not ID).
func (p *P) Name() string {
	return p.process.Name
}

// Name is a process name (not ID).
func (p *P) Service() string {
	return p.process.Service
}

func (p *P) resume(s interface{}) error {
	switch p.process.Status {
	case Process_Started:
		err := p.call(s, "Main", p.process.Input)
		if err != nil {
			return err
		}
	case Process_Running:
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
		return fmt.Errorf("unexpected resumed process status: %v", p.process.Status)
	}
	return nil
}

func (p *P) call(s interface{}, name string, input []byte) error {
	method := reflect.ValueOf(s).MethodByName(name)
	//TODO: method not found error
	inputs := method.Type().NumIn()
	if inputs != 1 && inputs != 2 {
		return fmt.Errorf("%v method %v should have input in format Thread_Status(p *P [,Input])", reflect.TypeOf(s), name)
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

func (p *P) Finish(result interface{}) *P {
	out, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	p.process.Output = out
	p.process.Status = Process_Finished
	return p
}

func (p *P) lastSel() *Select {
	if p.newThread == nil {
		p.newThread = &Thread{
			Id:      p.resumedThread.Id,
			Process: p.resumedThread.Process,
			Service: p.process.Service,
			Status:  Thread_Blocked,
		}
	}
	if p.newThread.Select == nil {
		p.newThread.Select = &Select{}
	}
	return p.newThread.Select
}

func (p *P) checkNoDefault() {
	for _, c := range p.lastSel().Cases {
		if c.Op == Case_Default {
			panic("default case should always be the last one")
		}
	}
}

// for readability
func (p *P) Select() *P {
	return p
}

func (p *P) After(after time.Duration) *P {
	return p.At(time.Now().Add(after))
}

func (p *P) At(at time.Time) *P {
	p.checkNoDefault()
	ls := p.lastSel()
	ls.Cases = append(ls.Cases, &Case{
		Op:   Case_Time,
		Time: uint64(at.Unix()),
	})
	return p
}

func (p *P) To(cb interface{}) *P { // TODO: check data type, based on Channel in previous operation?
	to := GetFunctionName(cb)
	to = to[strings.LastIndex(to, ".")+1:]
	to = strings.TrimSuffix(to, "-fm")

	method := reflect.ValueOf(p.procStruct).MethodByName(to)
	if method.IsZero() {
		panic(fmt.Sprintf("To status %v does not match any method", to))
	}
	inputType := ""
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

func (p *P) Recv(channel Channel) *P {
	p.checkNoDefault()
	ls := p.lastSel()
	ls.Cases = append(ls.Cases, &Case{
		Chan: channel.Id,
		Op:   Case_Recv,
	})
	return p
}

func (p *P) Send(channel Channel, data interface{}) *P {
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

func (p *P) Go(id string, f func(p *P)) {
	for _, v := range p.process.Threads {
		if v.Id == id {
			panic("starting goroutine twice")
		}
	}
	t := &Thread{
		Id:      id,
		Process: p.resumedThread.Process,
		Service: p.process.Service,
		Status:  Thread_Blocked,
	}
	p.process.Threads = append(p.process.Threads, t)
	newThread := &P{
		process:       p.process,
		newThread:     t,
		procStruct:    p.procStruct,
		client:        p.client,
		resumedThread: p.resumedThread,
		counter:       p.counter,
	}
	f(newThread)
}

func (p *P) Default() *P {
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

func (p *P) Call(name string, input interface{}) *P {
	in, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}
	if p.newThread == nil { // operation on same thread
		p.newThread = &Thread{
			Id:      p.resumedThread.Id,
			Process: p.resumedThread.Process,
			Service: p.process.Service,
			Status:  Thread_Blocked,
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

### async
Go API & libraries for working with [SLCT runtime](https://github.com/gladkikhartem/slct), that allow Go developers to share asynchronous code (for example send message to Slack and return user input) with consistency and "only-once" processing guarantees.


### Usage
```Go
package main

import (
	"log"
	"github.com/gladkikhartem/async"
	"google.golang.org/grpc"
)

var defs = async.Definitions{
	"counter": async.StandardDefinition(CounterProcess{}),
}

type CounterProcess struct {
	Counter int64
}

func (c *CounterProcess) Main_Count(p *async.Process) error {
    c.Counter++
    p.Recv("counter").To(c.Main_Count).
        .After(time.Hour).To(c.Main_Done)
    return nil
}

func (c *CounterProcess) Main_Done(p *async.Process) error {
    log.Print("done")
    return nil
}

func main() {
    conn, err := grpc.Dial("runtime_addr:9090", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
    defer conn.Close()
    
	err := async.ManageDefs(context.Background(), conn, "1", defs)
    if err != nil {
        log.Fatal(err)
    }
}
```


### Architecture
![](https://storage.googleapis.com/artem_and_co/SLCT%20diagram.svg)

Asynchronous process is described as set of goroutines(FSM) with a shared state. In the code it's represented as Go struct with methods in a format  {GoroutineName_FSMStatus}.
```Go
type ExampleProcess struct {
    // local variables  i.e. state
}

func (c *ExampleProcess) Main_Start(p *async.Process) error { 
    p.Go(c.Goroutine1_Start) // run parallel process in this state
	return slack.Approve(p, c.Main_CheckApprove, "Approve?", time.Hour)
}

func (c *ExampleProcess) Main_CheckApprove(p *async.Process, result slack.Result) error {
	// handle approval result
}

func (c *ExampleProcess) Goroutine1_Start(p *async.Process) error { 
    // ... 
}
```
When async process is resumed:
- Current state is unmarshalled into struct
- Correct method is called to continue goroutine execution .
- Method executes, updates the state and specifies conditions on when to unblock current goroutine. (New goroutines can also be created during execution)
- Updated state is saved in runtime.


Method can specify following unblocking conditions (same as in Go):
- send/recv on *channel*
- send/recv on buffered *channel*
- recv on <-time.After channel
- *default* statement
- handling of channel close
- cases are evaluated in sequential order

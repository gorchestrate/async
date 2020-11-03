### [Gorchestrate](https://github.com/gorchestrate/core) Go SDK

Usage example: [https://github.com/gorchestrate/pizzaapp](https://github.com/gorchestrate/pizzaapp)

### Architecture
![](https://storage.googleapis.com/artem_and_co/SLCT%20diagram.svg)

Using **async.Manage** your service will publish it's API to Gorchestrate Core and execute new or existing processes.
Process is defined as a Go struct with methods, that will be called when process starts/unblocks. On initial run 'Start' method is called.

When method is called it could:
* execute arbitrary code
* call other Gorchestrate API's (aka **func()**)
* specify blocking conditions (aka **select{}** or)
* create channels (aka **make chan(type)**)
* create new threads for existing process (aka **go func(){}**)
* finish process with result. This will stop execution of the process and unblock all selects aquired by this process

All semantics for operations are similar to Go ones and have same guarantees.
* Select operation is exlusive and have same linearized consistency guarantees (All events are strictly ordered using HLC clock)
* You can close channels, create buffered channels, pass channels inside channels.
* You can have multiple Threads(goroutines) running in your process. They will always be executed(unblocked) exlusively one after another.
* You can specify <-time.After() conditions in select statement.

All state management is done on Gorchestrate Core side. Process is locked, sent for execution, processed by service and then unlocked.
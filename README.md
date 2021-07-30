### gorchestrate your workflows
![link to docs](https://avatars.githubusercontent.com/u/73880988?s=200&u=4836092ec2bbec14ecd6597f41d4e69c9061309f&v=1)

Gorchestrate makes stateful workflow management in Go incredibly easy. 
I've spend 4 years to come up with this particular approach of handling workflows, and i'll apreciate if you'll try it out.



#### Example App
https://github.com/gorchestrate/pizzaapp
This is an example of how you can use Google Cloud to build fully serverless stateful workflows, that could be scaled horizontally without too much efforts.

#### Example Code
```Go
type MyWorkflowState struct {
	User     string
	Approved bool
	Error    string
}

func (s *MyWorkflow) Definition() Section {
	return S(
		Step("first action", func() error {
			s.User = "You can execute arbitrary code right in the workflow definition"
			log.Printf("No need for fancy definition/action separation")
			return nil
		}),
		If(s.User == "",
			Step("second action", func() error {
				log.Printf("use if/else to branch your workflow")
				return nil
			}),
			Return(),
		),
		Select("and then wait for some events",
			On("myEvent", MyEvent{
				F: func() {
					log.Printf("this is executed synchronously when HandleEvent() is Called")
				},
			},
				Step("and then continue the workflow", func() error {
					return nil
				}),
				s.Finish("timed out"),
			),
		),
		Go("thread2", S(
			Step("you can also run parallel threads in your workflow", func() error {
				s.User = "this allows you to orchestrate your workflow and run multiple asnychrounous actions in parallel"
				return nil
			}),
		)),
		s.Finish("finished successfully"),
	)
}

func (s *MyWorkflow) Finish(output string) Stmt {
	return S(
		Step("you can also build sub steps for your workflows", func() error {
			return nil
		}),
		Step("for example if you want to execute same actions for multiple workflow steps", func() error {
			return nil
		}),
		Step("or make your workflow definion clean and readable", func() error {
			return nil
		}),
		Return(),
	)
}
```


## Architecture
Gorchestrate has stateless API and does not come with a database or scheduler. 
It's intended to be integrated into applications, which will determine how & when workflows will be executed.

Think this is a stateful script - you want to execute some steps and want to save the state of the script in persistent storage and then resume the script when some event comes in.

There are 2 ways to execute workflow:
* Scheduled call of Resume() method. This is called when workflow starts or event was handled and we need to continue execution.
* Explicit call of HandleEvent() method. This is called whenever some event happens.

Brief description of how workflows are executed:
0. Workflow is created and Resume() call is scheduled.
1. Scheduled Resume() will execute workflow till point when workflow is blocked and waiting for events. 
2. Then it will execute Setup() for all events we are waiting for. This allows you to register your events on external services.
3. When event arrives - you pass it to HandleEvent() method. This handles event and schedules Resume() call
4. Resume() will execute Teardown() for all events we were waiting for. This allows you to deregister your events on external services.
5. Go to point 1 if main thread is not yet exited.



### How library knows where to resume the workflow from?
* All threads and their current steps are located in workflows State.
* When Resume() is called - we try to continue all steps in all threads, from CurStep specified in thread. So if you add a new step to the workflow - it will not break the execution flow. Removing step is harder, since you need to make sure that the step is not currently executed and it's removal won't break the logic of your workflow.
* When HandleEvent() is called - we find step waiting for this callback and continue execution. Calling HandleEvent() with event that workflow is not waiting for will return an error.

### What about performance?
Performance is mainly limited by DB operations, rather than library itself.
To build stateful workflows you have to lock, save & unlock the workflow for each execution step, which may be slow and resource-hungry.

Here's a benchmark for a complete workflow execution consisting of 19 execution steps, including loops, steps & event handlers.
```
go test -bench=. -benchtime=1s
cpu: AMD Ryzen 7 4700U with Radeon Graphics         
BenchmarkXxx-8   	   33121	     35601 ns/op
```

### Is it production ready?
It will reliably work for straightforward workflows that are just doing simple steps. There's 80% coverage and tests are covering basic flows.

For advanced usage, such as workflows with concurrency or custom event handlers - you should write tests and make sure library does what you want. 
(see async_test.go on how to test your workflows)

I'm keeping this library as 0.9.x version, since API may change a little bit in the future (for example we may want to pass context to callbacks in future), but it should be easy to fix.

I'd be glad if some really smart guy will help me out to make this library better or more popular)))


Feedback is welcome!
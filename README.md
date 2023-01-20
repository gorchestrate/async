### gorchestrate your workflows
![link to docs](https://avatars.githubusercontent.com/u/73880988?s=200&u=4836092ec2bbec14ecd6597f41d4e69c9061309f&v=1)

Gorchestrate makes stateful workflow management in Go incredibly easy. 
I've spend 4 years to come up with this particular approach of handling workflows, and i'll apreciate if you'll try it out.



#### Example App
https://github.com/gorchestrate/pizzaapp
This is an example of how you can use Google Cloud to build serverless workflows.

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
			log.Printf("No need for abstractions or fancy separation of actions from workflow logic")
			return nil
		}),
		If(s.User == "", "if user is empty",
			Step("second action", func() error {
				log.Printf("execute some code")
				return nil
			}),
			Return(),
		),
		Wait("for external events",
			OnEvent("genericEvent", func(input Info) (Info, error) {
				log.Print("received generic event")
				return input, nil
			}),
			On("myCustomEvent", MyEvent{
				F: func() {
					log.Printf("received custom event, that has 'Setup()' and 'Teardown()' functions")
				},
			},
				Step("separate branch of cations after event received", func() error {
					return nil
				}),
				s.Finish("workflow finished"),
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
Gorchestrate workflow consists of 3 parts:

* Workflow data
	* `type MyWorkflow struct {`
* Workflow code
	* `func (s *MyWorkflow) Definition() Section {`
* Workflow state 
	* `async.State`

How to create new worfkflow instance:
* Populate your Workflow data with initial values.
* Create new state via `async.NewState()`
* Call Resume()
* Save result to DB

How to resume existing workflow:
* Load workflow that needs to be resumed from DB
* Call Resume()
* Save result to DB

How to submit event to workflow:
* Load workflow from DB
* Call HandleCallback()
* Save result to DB

### How to update workflow code?
* Each statement in workflow has a name
* You can update workflow as you want, with a few exceptions, but don't:
	* Rename/delete/move in-flight statements (steps that are executing or events we are waiting for)
	* Rename/delete active threads (Go statements)
	* Change the type of statements (i.e. same name, but different type)
* Following process is advised for deletion of the workflow code:
	* Skip statements you want to delete with If(false) condition
	* Deploy new code
	* Check DB that no workflow state contains these statements
	* Delete statements
	* Deploy new code with deleted statements

### How to update workflow execution state?
* If for some reason your workflow is invalid state and you want to fix it - you can easily do that
	* Update workflow data with proper values
	* Update workflow state with the name of step you want to execute next (i.e. goto)
	* Update all thread of the workflow, add / delete threads if necessary.
	* Save to DB


### What about performance?
Performance is mainly limited by DB operations and workflow actions, rather than library itself.
To build stateful workflows you have to lock, save & unlock the workflow for each execution step, which may be slow and resource-hungry.

Here's a benchmark for a complete workflow execution consisting of 19 execution steps, including loops, steps & event handlers. 
```
go test -bench=. -benchtime=1s
cpu: AMD Ryzen 7 4700U with Radeon Graphics         
BenchmarkXxx-8   	   33121	     35601 ns/op
```
1.9 microseconds per step or 500k step/second.

### Is it production ready?
It will reliably work for straightforward workflows that are just doing simple steps. There's 80% coverage and tests are covering basic flows.

For advanced usage, such as workflows with concurrency or custom event handlers - you should write tests and make sure library does what you want. 
(see async_test.go on how to test your workflows)

I'm keeping this library as 0.9.x version, since API may change a little bit in the future (for example we may want to pass context to callbacks in future), but it should be easy to fix.

I'd be glad if some really smart guy will help me out to make this library better or more popular)))

Feedback is welcome!
### gorchestrate your workflows

Gorchestrate makes stateful workflow management in Go incredibly easy. 

I've spend 4 years to come up with this particular approach of handling workflows, and i'll apreciate if you'll try it out.

#### Here's how it looks
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

Think this is a stateful scripting library - you want to execute some steps and want to save the state in persistent storage.

Workflow is resumed(executed) by following events:
* Scheduled call of Resume() method. It's scheduled right after workflow creation and event handling.
* Explicit call of HandleEvent() method. It's called whenever some event happens.

Brief description of how workflows are executed:
0. Workflow is created and Resume() call is scheduled.
1. Scheduled Resume() will execute workflow till point when workflow is blocked and waiting for events. 
2. Then it will execute Setup() for all events we are waiting for. This allows you to register your events on external services.
3. When event arrives - you pass it to HandleEvent() method. This handles event and schedules Resume() call
4. Resume() will execute Teardown() for all events we were waiting for. This allows you to deregister your events on external services.
5. Go to point 1 if main thread is not yet exited.




#### Features
* Define all your workflow actions **right in the Go code**. Logic/action separation is done by using labels and closures.
* You can update your workflow definition **while it is running**. Workflow state & definition are stored separately.
* You can have multiple threads in single workflow and orchestrate them. 
* You can test your workflows via unit-tests.


### Google Run & Google Cloud Tasks 
By using Google Cloud Run & Google Cloud Tasks & Google Datastore it's possible to have fully serverless workflow setup.
This provides all the benefits of serverless applications and at the same time solves issues related to concurrent execution.
Example setup using this stack: https://github.com/gorchestrate/pizzaapp


### How library knows where to resume the workflow from?
* All threads and their current steps are located in workflows State.
* When Resume() is called - we try to continue all steps in all threads, from CurStep specified in thread. So if you add a new step to the workflow - it will not break the execution flow. Removing step is harder, since you need to make sure that the step is not currently executed and it's removal won't break the logic of your workflow.
* When HandleEvent() is called - we find step waiting for this callback and continue execution. Calling HandleEvent() with event that workflow is not waiting for will return an error.

### Is it production ready?
It will reliably work for straightforward workflows that are just doing simple steps. There's 80% coverage and tests are covering basic flows.

For advanced usage, such as workflows with concurrency or custom event handlers - you should write tests and make sure library does what you want. 
(see async_test.go on how to test your workflows)

I'm keeping this library as 0.9.x version, since API may change a little bit in the future (for example we may want to pass context to callbacks in future), but it should be easy to fix.

I'd be glad if some really smart guy will help me out to make this library better or more popular)))


Feedback is welcome!
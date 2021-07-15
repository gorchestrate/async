### Gorchestrate your workflows! 

**async** is a Go library to integrate workflow management into your application using **your existing infrastructure**.

I've spend 4 years to come up with this particular approach, and i'll apreciate if you'll try it out.

#### Features
* **This is a library, not a framework**. You can integrate it into your existing project infrastructure.
* You can define all your workflow actions **right in the Go code**
* You can update your workflow definition **while it is running**
* Run multiple threads in single workflow using Golang semantics.

#### Here's how it looks
```Go
type MyWorkflowState struct {
	User     string
	Approved bool
	Error    string
}

func (s *MyWorkflowState) Definition() Section {
	return S(
		Step("first action", func() error {
			s.User = "You can execute arbitrary code right in the workflow definition"
			log.Printf("No need for fancy definition/action separation")
			return nil
		}),
		If(s.User == "", Step("second action", func() error {
			log.Printf("use if/else to branch your workflow")
			return nil
		}),
			Return(),
		),
		Wait("and then wait for some events",
			On("wait some time", Timeout{
				After: time.Minute * 10,
			},
				Step("and then continue the workflow", func() error {
					return nil
				}),
				s.Finish("timed out"),
			),
			On("or wait for your custom event", WaitForUserAction{
				Message: "plz approve",
				Approve: func() {
					s.Approved = true
					log.Printf("And handle it's results")
				},
				Deny: func(reason string) {
					s.Approved = false
					s.Error = reason
				},
			}),
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

func (s *MyWorkflowState) Finish(output string) Stmt {
	return S(
		Step("you can also build reusable workflows", func() error {
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
Gorchestrate has stateless API. This allows framework to be adaptable to any infrastructure, including Serverless applications.

Workflow is resumed(executed) by following events:
* Explicit call of HandleEvent() method. It's called whenever some event happens.
* Scheduled call of Resume() method. We schedule such after HandleEvent() method and after workflow creation.

1. Resume() will execute workflow till point when workflow is blocked and waiting for events. 
2. Then it will execute Setup() for all events we are waiting for. This allows you to register your events on external services.
3. When event arrives - you pass it to HandleEvent() method. This handles event and schedules Resume() call
4. When Resume() is called we execute Teardown() for all events we were waiting. This allows you to deregister your events on external services.
5. If workflow is not finished - go back to point 1.


### Google Run & Google Cloud Tasks 
By using Google Cloud Run & Google Cloud Tasks & Google Datastore it's possible to have fully serverless workflow setup.
This provides all the benefits of serverless applications and at the same time solves issues related to concurrent execution.
Example setup using this stack: https://github.com/gorchestrate/pizzaapp

### Is it production ready?
It will reliably work for straightforward workflows that are just doing simple steps. There's 80% coverage and tests are covering basic flows.

For advanced usage, such as workflows with concurrency or custom event handlers - you should write tests and make sure library does what you want. 
(see async_test.go on how to test your workflows)

I'm keeping this library as 0.9.x version, since API may change a little bit in the future (for example we may want to pass context to callbacks in future), but it should be easy to fix.

I'd be glad if some really smart guy will help me out to make this library better or more popular)))


Feedback is welcome!
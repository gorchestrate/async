### Gorchestrate your workflows! 

**async** is a Go library to integrate workflow management into your application using **your existing infrastructure**.

I've spend 4 years to come up with this particular approach, and i'll apreciate if you'll try it out.

#### Here's how it looks
```Go
type MyWorkflowState struct {
    User string
    Approved bool
    Error string
}

func (s* MyWorkflowState) Definition() async.Section {
    return S(
        Step("first action", func() error {
            s.User = "You can execute arbitrary code right in the workflow definition"
            log.Printf("No need for fancy definition/action separation")
        }),
        If(s.User == "", 
            log.Printf("use if/else to branch your workflow")
            Return(nil)
        ),
        Wait("and then wait for some events",
            On("wait some time", Timeout{
                After: time.Minute*10,
            },
                Step("and then continue the workflow", func() error {
                    return nil
                }),
                s.Finish("timed out"),
            )
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
                s.User = "this allows you to orchestrate your workflow and run multiple asnychrounous actions in parallel")
            }),
        ), "g1"),
        s.Finish("finished successfully"),
    )
}

func (s *MyWorkflowState) Finish(output string) async.Stmt{
    Step("you can also build reusable workflows", func() error {
        return nil
    }).
    Step("for example if you want to execute same actions for multiple workflow steps", func() error {
        return nil
    }).
    Step("or make your workflow definion clean and readable", func() error {
        return nil
    }).
    Return()
}
```

#### Features
* You can run this on serverless
* You can define all your workflow actions **right in the Go code**
* You can update workflow **while it is running** 
* Full control of how your workflows are handled

## Architecture
Gorchestrate was created with being stateless in mind. This allows framework to be adaptable to any infrastructure, including Serverless applications.

Workflow is resumed(executed) by following events:
1. Explicit call of OnCallback() method. It's called whenever you want it to, for example when custom event happens.
2. Scheduled call of OnResume() method. We schedule such after OnCallback() method and after workflow creation.

* OnResume() will continue workflow till point when it's waiting for events. 
* Then we execute Setup() for all events we are waiting for
* Then we handle event via OnCallback() method
* Then we execute Teardown() for all eents we were waiting for previously
* Then we continue the workflow... (back to beginning)

OnCallback() will execute callback handler for the workflow and schedule OnResume() call to continue the workflow using flow above. 

By using external Scheduler(for ex. Google Cloud Run) it's possible to have fully serverless workflows.
This avoids infrastructure burden of monitoring and setting up servers and at the same time solves common issues related to serverless applications.

* No need to monitor servers & scale your cluster. 
* No need for singleton patterns to avoid concurrency issues.
* No need for cron or similar schedulers. Just create workflow with a loop.

### Is it production ready?
Not yet.

API may change and there are definitely some bugs that has to be fixed.

I'd be glad if some really smart guy will help me out to make this library better. 

Feedback is welcome!
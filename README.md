### Gorchestrate your workflows! 

**async** is a Go library to integrate workflow management into your application using your existing infrastructure.


## Example
```Go

type MyWorkflowState struct {
    Approved bool
    Error string
}

func (s* MyWorkflowState) Definition() async.Section {
    return S(
        Step("first action", func() error {
            log.Printf("workflow has started")
        }),
        Go("notification goroutine", S(
            Step("parallel thread", func() error {
                log.Printf("execute stuff in parallel, while we are waiting for approval")
            }),
        ), "g1"),
        Wait("waiting for approval",
            On("slackApproval", WaitForUserAction{
                Approve: func() {
                    s.Approved = true
                },
                Deny: func(reason string) {
                    s.Approved = false
                    s.Error = reason
                },
            }),
            On("timeout", Timeout{
                After: time.Minute*10,
            },
                Step("abort workflow", func() error {
                    log.Print("workflow aborted")
                    s.Approved = false
                    s.Error = "approval timed out"
                    return nil
                }),
                Return(fmt.Errorf("workflow finished with timeout")),
            )
        ),
        If(s.Approved, 
            Return(nil)
        ),
        Return(fmt.Errorf("workflow was not approved: %v", s.Error)),
    )
}

```

## Why you want to use this
#### This is a library, not a framework
* Store workflows in DB of **your** choice
* Schedule workflows using **your** approach
* Handle callbacks with API **you** create
* It's just 800 lines of code, customize it for **your** needs


#### It has some really good features 
* Update workflow definitions **while they are running**. If hotfix needed - you can deploy new version without having to restart workflows.
* Define all your workflow actions **right in the Go code**. No need for boring workflow<>action separation boilerplates.
* Run multiple goroutines(threads) in your workflow. No need to create multiple workflows to do parallel tasks.

#### Extensible
* Add **your** workflow routines and reuse them:  Approvals, Timers, Exporting, Logging, Notifications, Verifications... 
* Add **your** wait conditions:  Wait for user input, Wait for approval, Wait for timeout, Wait for task finished...
* You can share implementation for common routines and conditions. This can help create a repository of async libraries.


## Architecture
Gorchestrate was created with Serverless in mind. Specifically Google Cloud Run & Google Cloud Tasks. This allows to build fully serverless workflow management system that costs nothing when you start and scales well when you have more load.

No need to monitor servers & scale your cluster. You pay only for what you use.

All actions on workflow are stateless, so no monitoring loop exists in the library - you call all workflow actions explicitly.

To resume workflow when needed (timeout/event) - external scheduler should be used to continue workflow execution when it's needed and retry failed executions (due to bugs, connectivity or locking issues). This will make your workflows reliable.

External scheduler can also be serverless, for example you can use Google Cloud Tasks or similar scheduling service to call your public API to resume workflow execution when needed.


### Is it production ready?
Not at this stage. API may change and there are definitely some bugs that has to be fixed.

I'd be glad if some really smart guy will help me out to make this library better. 

Feedback is welcome!
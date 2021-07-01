### [Gorchestrate](https://github.com/gorchestrate/core) Go SDK

Usage example: [https://github.com/gorchestrate/pizzaapp](https://github.com/gorchestrate/pizzaapp)

## Why you want to use this
#### Infrastructure-agnostic
* Store workflows in DB or **your** choice
* Schedule workflows using **your** way
* Handle callbacks using **your** architecture

#### Flexible
* Define all your workflow actions **right in the Go code**. No need for boring workflow<>action separation boilerplates.
* All workflow execution states are saved in DB.  If you have corrupted data in workflows - you can fix it inplace. If server goes down - workflow will continue execution on other server.
* Update workflow definitions **while they are running**. If hotfix needed - you can deploy new version without having to restart workflows.

#### Extensible
* Add **your** workflow routines and reuse them:  Approvals, Timers, Exporting, Logging, Notifications, Verifications... 
* Add **your** wait conditions:  Wait for user input, Wait for approval, Wait for timeout, Wait for task finished ...

## Example
```Go

type MyWorkflowState struct {
    Approved bool
    Error string
}

func (s* MyWorkflowState) Definition() async.Section {
    return S(
        Wait("my condition",
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


## Architecture
#### NewWorkflow() 
Creates new workflow and schedules it for Execution.

#### Execute()
All scheduled Executions should call Execute() for workflow until it finishes without error.
This makes sure that workflows are not simply stored in DB, but actually executed when it's needed.

#### OnCallback() 
When workflow blocks on waiting for condition - you setup your condition and wait for it to happen. 
When it happends - you call OnCallback() to resume workflow execution.

#### **Resumer** interface
Resumer is responsible for scheduling workflow execution in reliable way. It should retry execution 
of workflow to make sure no workflow is lost in zombie state. (when it's just stored in DB, but not executed when it's needed)

#### **DB** interface
DB is responsible to run workflow in exlusive lock. Since actual code execution can happend during workflow execution - it should 
lock state and only then allow to update it.
You can achieve that using optimistic locking or "SELECT FOR UPDATE".

#### **CallbackManager** interface
CallbackManager allows you to creat your own waiting conditions, for example WaitForUserAction:
* Setup() method is called create action on external service and notify user that he has to do something.
* Teardown() method  is called to remove action on external service, so that it's no longer available

##### **Handler** interface
Handler is a specific instance of wait condition for CallbackManager, which is identified using Type() method.
Data from Handler is passed to it's CallbackManager to Setup() a waiting condition.
When CallbackManager receives event and calls OnCallback() - Handle()  method will be executed to handle this callback.

It's very convenient to define callbacks with closures, since handling different callback conditions can be done inside the workflow

### Gorchestrate your workflows! 

**async** is a Go library to integrate workflow management into your application using your existing infrastructure.

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

#### This is a library, not a framework


* Store workflows in DB of **your** choice
* Schedule workflows using **your** approach
* Handle callbacks with API **you** create

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



### Tips

It's up to you to define how you workflow will stored and executed.
Typically you will define this once for all your workflows and forget about it.

```Go
type MyWorkflowRecord struct {
    State MyWofkflowState // workflow local variables
    Meta async.Meta       // workflow current execution state
}

type MyEngine struct {
    db MyDB 
    scheduler MyScheduler 
}

func (e *MyEngine) OnResume(ctx context.Context, id string) error {
	wfRecord, err := e.db.Lock(ctx, id)
	if err != nil {
		return err
	}
	err = e.r.OnResume(ctx, &wfRecord.State, &wfRecord.Meta, e.Checkpoint(ctx, &wfRecord))
	if err != nil {
		return fmt.Errorf("err during workflow processing: %v", err)
	}
	return e.db.Unlock(ctx, id)
}

// Checkpoint will be called after each successful step of the workflow. 
// If resume == true - you will have to schedule another "e.OnResume()" call.
// This is needed when event fires and workflow continues to execute asynchronous actions after event was handled.
func (e Engine) Checkpoint(ctx context.Context, wf *DBStruct) func(bool) error {
	return func(resume bool) error {
		if resume {
			err := fs.scheduler.Schedule(wf.Meta.ID)
			if err != nil {
				return err
			}
		}
		return e.db.Save(wf)
	}
}
```


You can also define customer events for your workflow
```Go
type WaitForUserAction struct {
    Message string
    Approve func()
    Deny func()
}

// and how it will be handled
func (t *WaitForUserAction) Handle(req async.CallbackRequest, input interface{}) (interface{}, error) {
    if (input.(MyEvent)).Approved {
        t.Approve()
    } else {
        t.Deny()
    }
	return nil, nil
}

// for each event you have to define how event will be created, destroyed and passed to your service
type WaitForUserActionManager struct {
}

func (mgr *WaitForUserActionManager) Setup(req async.CallbackRequest) error {
    // do external calls to the service that will ask user to do some action (for example create some UserTask)
    return nil
}

func (t *WaitForUserActionManager) Teardown(req async.CallbackRequest) error {
    // when event was handled or workflow is not waiting for it any more - we should destroy it. (i.e. delete UserTask)
	return nil
}

// and if event fires - we should be able to handled it.
// in this case event will be passed to our service via HTTP and we forward it to Workflow
func (mgr *WaitForUserActionManager) UserActionHTTPHandler(w http.ResponseWriter, r *http.Request) {
	var req MyEvent
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Printf("err: %v", err)
		return
    }
	_, err = mgr.engine.OnCallback(r.Context(), req.Callback, req)
	if err != nil {
		log.Printf("err: %v", err)
		w.WriteHeader(500)
		return
	}
}

```


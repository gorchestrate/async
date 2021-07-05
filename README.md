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


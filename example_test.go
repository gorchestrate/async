package async

import (
	"context"
	"log"
)

type Info struct {
	A string
	B int
}

type MyWorkflow struct {
	State    State
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

// Custom event allows you to simplify work with external systems.
type MyEvent struct {
	F func()
}

func (t MyEvent) Type() string {
	return "myevent"
}

// First you register event in external system, so that external system knows that you're waiting for event
//
// For example we create a timer that will trigger event after 1 hour.
func (t MyEvent) Setup(ctx context.Context, req CallbackRequest) (string, error) {
	return "", nil
}

// This handler will be called when we receive and event.
//
// For example timer had expired and triggered a callback event
func (t MyEvent) Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error) {
	t.F()
	return input, nil
}

// If we are no longer wait for event - this function is called.
//
// For example we received an event before 1 hour has expired. Now we need to go an
// delete the timer
func (t MyEvent) Teardown(ctx context.Context, req CallbackRequest, handled bool) error {
	return nil
}

func Example() {
	wf := MyWorkflow{
		State: NewState("1", "empty"),
	}

	// Run workflow till the point when all workflow threads are blocked
	err := Resume(context.Background(), &wf, &wf.State, func(ct CheckpointType) error {
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Submit an event to a workflow
	_, err = HandleCallback(context.Background(), CallbackRequest{Name: "myEvent"}, &wf, &wf.State, "input")
	if err != nil {
		log.Fatal(err)
	}
}

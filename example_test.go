package async

import (
	"context"
	"encoding/json"
	"log"
)

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
			log.Printf("No need for fancy definition/action separation")
			return nil
		}),
		If(s.User == "", "check1",
			Step("second action", func() error {
				log.Printf("use if/else to branch your workflow")
				return nil
			}),
			Return(),
		),
		Wait("and then wait for some events",
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

type MyEvent struct {
	F func()
}

func (t MyEvent) Setup(ctx context.Context, req CallbackRequest) (json.RawMessage, error) {
	return nil, nil
}

func (t MyEvent) Teardown(ctx context.Context, req CallbackRequest, handled bool) error {
	return nil
}

func (t MyEvent) Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error) {
	t.F()
	return input, nil
}

func Example() {
	wf := MyWorkflow{
		State: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.State, func(scheduleResume bool) error {
		// this is callback to save updated &wf to persistent storage
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = HandleEvent(context.Background(), "myEvent", &wf, &wf.State, nil, func(scheduleResume bool) error {
		// this is callback to schedule another Resume() call and save updated &wf to persistent storage
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

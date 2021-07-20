package async

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type TestEmptyWorkflow struct {
	Meta State
}

func (t *TestEmptyWorkflow) Definition() Section {
	return S()
}

func TestEmpty(t *testing.T) {
	wf := TestEmptyWorkflow{
		Meta: NewState("1", "empty"),
	}
	before := wf.Meta
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, before.ID, wf.Meta.ID)
	require.Equal(t, before.Workflow, wf.Meta.Workflow)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0) // workflow will finish, so there should be no threads left in our case
	require.Equal(t, 1, wf.Meta.PC)    // we did 1 step, so PC should increment
}

type TestStepWorkflow struct {
	Meta State
	Err  string
}

func (t *TestStepWorkflow) Definition() Section {
	return S(
		Step("step1", func() error {
			if t.Err != "" {
				return fmt.Errorf(t.Err)
			}
			t.Err = "updated"
			return nil
		}),
	)
}

func TestStep(t *testing.T) {
	wf := TestStepWorkflow{
		Meta: NewState("1", "empty"),
		Err:  "",
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0)
	require.Equal(t, 3, wf.Meta.PC) // we did 3 steps here: 1 to stop on Execution. 2 Execute, 3 Resume and return
	require.Equal(t, "updated", wf.Err)

	wf = TestStepWorkflow{
		Meta: NewState("1", "empty"),
		Err:  "err1",
	}
	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "err1")
}

type TestHandlerWorkflow struct {
	Meta State
	Log  string
}

type DumbHandler struct {
	F func()
}

func (t DumbHandler) Setup(ctx context.Context, req CallbackRequest) (json.RawMessage, error) {
	return []byte(`{"a":"b"}`), nil
}

func (t DumbHandler) Teardown(ctx context.Context, req CallbackRequest, handled bool) error {
	if !bytes.Equal([]byte(`{"a":"b"}`), req.SetupData) {
		return fmt.Errorf("Setup Or Teardown issue")
	}
	return nil
}

func (t DumbHandler) Handle(ctx context.Context, req CallbackRequest, input interface{}) (interface{}, error) {
	if t.F != nil {
		t.F()
	}
	return input, nil
}

func (t *TestHandlerWorkflow) Definition() Section {
	return S(
		Step("start", func() error {
			t.Log += "start"
			return nil
		}),
		Wait("wait",
			On("event", DumbHandler{
				F: func() {
					t.Log += ",sync"
				},
			}, Step("after handler", func() error {
				t.Log += ",async"
				return nil
			}))),
	)
}

func TestHandler(t *testing.T) {
	wf := TestHandlerWorkflow{
		Meta: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 1)
	require.Equal(t, 3, wf.Meta.PC) // we did 3 steps here: 1 to stop on Execution. 2 Execute, 3 Resume and stop on Handler
	tr := wf.Meta.Threads[0]
	require.Equal(t, "wait", tr.CurStep)
	require.Equal(t, MainThread, tr.ID)
	require.Equal(t, MainThread, tr.Name)
	require.Equal(t, ThreadWaitingEvent, tr.Status)
	require.Equal(t, 3, tr.PC) // same PC as in state
	require.Len(t, tr.WaitEvents, 1)
	require.Equal(t, "", tr.WaitEvents[0].Error)
	require.Equal(t, EventSetup, tr.WaitEvents[0].Status)
	require.Equal(t, "event", tr.WaitEvents[0].Req.Name)
	require.Equal(t, 3, tr.WaitEvents[0].Req.PC)
	require.Equal(t, json.RawMessage(`{"a":"b"}`), tr.WaitEvents[0].Req.SetupData)
	require.Equal(t, EventSetup, tr.WaitEvents[0].Status)
	require.Equal(t, MainThread, tr.WaitEvents[0].Req.ThreadID)
	require.Equal(t, "1", tr.WaitEvents[0].Req.WorkflowID)

	// if we resume on waiting state - nothing should change
	before := wf
	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, wf, before)
	require.Equal(t, EventSetup, tr.WaitEvents[0].Status)
	require.Equal(t, 3, wf.Meta.PC)

	// if we call on event that doesn't exist - expect an error
	_, err = HandleEvent(context.Background(), "blabla", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.Fail(t, "should not call save on error")
		return nil
	})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not found")

	wf = before // recover from previous error
	out, err := HandleEvent(context.Background(), "event", &wf, &wf.Meta, "abc", func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "abc", out)
	require.Equal(t, "start,sync", wf.Log)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, 4, tr.PC) // increase PC by 1 since handler was executed and resumed

	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "start,sync,async", wf.Log)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, tr.WaitEvents, 0)
	require.Equal(t, 6, wf.Meta.PC) // increase PC by 2 since we stopped at async step and then resumed
	require.Len(t, wf.Meta.Threads, 0)
}

type TestLoopWorkflow struct {
	Meta State
	I    int
	Log  string
}

func (t *TestLoopWorkflow) Definition() Section {
	return S(
		For(t.I < 5,
			Step("step1", func() error {
				t.I++
				t.Log += "l"
				return nil
			}),
		),
		Step("reset i", func() error {
			t.I = 0
			return nil
		}),
		For(t.I < 2,
			Wait("wait in loop for event",
				On("event", DumbHandler{
					F: func() {
						t.Log += "h"
						t.I++
					},
				},
					Step("step2", func() error {
						t.Log += "a"
						return nil
					}),
				),
			),
		),
	)
}

func TestLoop(t *testing.T) {
	wf := TestLoopWorkflow{
		Meta: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 1)
	require.Len(t, wf.Meta.Threads[0].WaitEvents, 1)
	require.Equal(t, ThreadWaitingEvent, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllll", wf.Log)

	_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadExecuting, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllllh", wf.Log)

	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadWaitingEvent, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllllha", wf.Log)

	_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadExecuting, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllllhah", wf.Log)

	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Equal(t, "lllllhaha", wf.Log)
}

type TestBreakWorkflow struct {
	Meta State
	I    int
	Log  string
}

func (t *TestBreakWorkflow) Definition() Section {
	return S(
		For(true,
			Step("step1", func() error {
				t.I++
				t.Log += "l"
				return nil
			}),
			If(t.I >= 5, Break()),
		),
		Step("reset i", func() error {
			t.I = 0
			return nil
		}),
		For(t.I < 2,
			Wait("wait in loop for event",
				On("event", DumbHandler{
					F: func() {
						t.Log += "h"
						t.I++
					},
				},
					Step("step2", func() error {
						t.Log += "a"
						return nil
					}),
				),
			),
		),
	)
}

func TestBreak(t *testing.T) {
	wf := TestBreakWorkflow{
		Meta: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 1)
	require.Len(t, wf.Meta.Threads[0].WaitEvents, 1)
	require.Equal(t, ThreadWaitingEvent, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllll", wf.Log)

	_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadExecuting, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllllh", wf.Log)

	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadWaitingEvent, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllllha", wf.Log)

	_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadExecuting, wf.Meta.Threads[0].Status)
	require.Equal(t, "lllllhah", wf.Log)

	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Equal(t, "lllllhaha", wf.Log)
}

type TestIfElseWorkflow struct {
	Meta State
	Str  string
}

func (t *TestIfElseWorkflow) Definition() Section {
	return S(
		If(t.Str == "",
			Step("set to b", func() error {
				t.Str = "b"
				return nil
			}),
		).ElseIf(t.Str == "c",
			Return(),
		).Else(
			Step("duplicate", func() error {
				t.Str += t.Str
				return nil
			}),
			Return(),
		),
		Switch(
			Case(t.Str == "a", Step("add a", func() error {
				t.Str += "a"
				return nil
			})),
			Case(t.Str == "b", S(
				Step("add b", func() error {
					t.Str += "b"
					return nil
				}),
				Wait("wait after b",
					On("event", DumbHandler{F: func() {
						t.Str += "h"
					}}),
				),
			),
			),
		),
	)
}

func TestIfElse(t *testing.T) {
	wf := TestIfElseWorkflow{
		Meta: NewState("1", "empty"),
		Str:  "a",
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "aa", wf.Str)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0)

	wf = TestIfElseWorkflow{
		Meta: NewState("1", "empty"),
		Str:  "",
	}
	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "bb", wf.Str)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadWaitingEvent, wf.Meta.Threads[0].Status)

	_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "bbh", wf.Str)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0)
}

type TestGoWorkflow struct {
	Meta State
	I    int
	WG   int
	Log  string
}

func (t *TestGoWorkflow) Definition() Section {
	return S(
		For(t.I < 5,
			Step("inc", func() error {
				t.I++
				t.WG++
				return nil
			}),
			Go("parallel", S(
				Step("log", func() error {
					t.Log += "a"
					t.WG--
					return nil
				}),
			)).WithID(func() string { return fmt.Sprint(t.I) }),
		),
		Wait("wait for goroutines to finish",
			On("event", DumbHandler{}),
		),
		Step("done", func() error {
			t.Log += "_done"
			return nil
		}),
	)
}

func TestGo(t *testing.T) {
	wf := TestGoWorkflow{
		Meta: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "aaaaa", wf.Log)
	require.Equal(t, WorkflowRunning, wf.Meta.Status)
	require.Equal(t, ThreadWaitingEvent, wf.Meta.Threads[0].Status)

	_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
		require.True(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)

	err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "aaaaa_done", wf.Log)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0)
}

type TestWaitWorkflow struct {
	Meta State
	I    int
	WG   WG
	Log  string
}

func (t *TestWaitWorkflow) Definition() Section {
	return S(
		For(t.I < 5,
			Step("inc", func() error {
				t.I++
				t.WG.Add(1)
				return nil
			}),
			Go("parallel", S(
				Step("log", func() error {
					t.Log += "a"
					t.WG.Done()
					return nil
				}),
			)).WithID(func() string { return fmt.Sprint(t.I) }),
		),
		t.WG.Wait("waiting for WG"),
		Step("done", func() error {
			t.Log += "_done"
			return nil
		}),
	)
}

func TestWait(t *testing.T) {
	wf := TestWaitWorkflow{
		Meta: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, "aaaaa_done", wf.Log)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0)
}

type TestSubWorkflow struct {
	Meta State
	Log  string
}

func (t *TestSubWorkflow) Definition() Section {
	return S(
		Step("step1", func() error {
			t.Log += "1"
			return nil
		}),
		t.SubWorkflow1("sub1_"),
		t.SubWorkflow1("sub1,2_"),
	)
}

func (t *TestSubWorkflow) SubWorkflow1(prefix string) Section {
	return S(
		Step(prefix+"_step2", func() error {
			t.Log += "2"
			return nil
		}),
		t.SubWorkflow2(prefix+"_sub2,1_"),
		t.SubWorkflow2(prefix+"_sub2,2_"),
	)
}

func (t *TestSubWorkflow) SubWorkflow2(prefix string) Section {
	return S(
		Step(prefix+"_step3", func() error {
			t.Log += "3"
			return nil
		}),
	)
}

func TestSub(t *testing.T) {
	wf := TestSubWorkflow{
		Meta: NewState("1", "empty"),
	}
	err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
		require.False(t, scheduleResume)
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, WorkflowFinished, wf.Meta.Status)
	require.Len(t, wf.Meta.Threads, 0)
	require.Equal(t, "1233233", wf.Log)
}

func BenchmarkXxx(b *testing.B) {
	for i := 0; i < b.N; i++ {
		wf := TestLoopWorkflow{
			Meta: NewState("1", "empty"),
		}
		err := Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
			return nil
		})
		if err != nil {
			b.FailNow()
		}
		_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
			return nil
		})
		if err != nil {
			b.FailNow()
		}
		err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
			return nil
		})
		if err != nil {
			b.FailNow()
		}
		_, err = HandleEvent(context.Background(), "event", &wf, &wf.Meta, nil, func(scheduleResume bool) error {
			return nil
		})
		if err != nil {
			b.FailNow()
		}
		err = Resume(context.Background(), &wf, &wf.Meta, func(scheduleResume bool) error {
			return nil
		})
		if err != nil {
			b.FailNow()
		}
	}
}

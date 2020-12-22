package async

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type Service struct {
	Name      string
	Types     []*Type
	Workflows []WorkflowDefinition
}

type WorkflowDefinition struct {
	API      *WorkflowAPI
	Setup    func() (interface{}, error)
	Teardown func(interface{}) error
}

type AsyncType interface {
	Type() *Type
}

const None = "async.None"

func logW(w *Workflow) *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"module":     "gorchestrate-async",
		"workflowId": w.ID,
		"workflow":   w.API,
		"service":    w.Service,
		"version":    w.Version,
	})
}

func handleReq(c RuntimeClient, req *LockedWorkflow, new interface{}, teardown func(interface{}) error) {
	logW(req.Workflow).Infof("resume workflow")
	if req.Workflow.Status == Workflow_Running {
		err := json.Unmarshal(req.Workflow.State, &new)
		if err != nil {
			logW(req.Workflow).Errorf("workflow unmarshal: %v", err)
			return
		}
	}
	for i, t := range req.Workflow.Threads {
		if req.Thread.ID == t.ID {
			req.Workflow.Threads = append(req.Workflow.Threads[:i], req.Workflow.Threads[i+1:]...)
		}
	}
	var counter int
	w := W{
		workflow:      req.Workflow,
		resumedThread: req.Thread,
		procStruct:    new,
		client:        c,
		counter:       &counter,
	}

	// process resume logic can take long time, so we will extend our lock periodically until resume is done
	// there's no easy way to handle failure of extending the lock. In case it fails we will always have ambiguious situation
	// of current process loosing connectivity to server. In this case we never know if process is hanging or working normally.
	// If it's important for workflow to maintain lock for some duration - it's always possible to extend the lock on a server side for desired amount of time
	resumeDone := make(chan struct{}, 1)
	go func() {
		t := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				_, err := c.ExtendLock(ctx, &ExtendLockReq{
					ID:      req.Workflow.ID,
					LockID:  req.LockID,
					Seconds: 30,
				})
				if err != nil {
					logW(req.Workflow).Errorf("error extending lock: %v", err)
				}
				cancel()
			case <-resumeDone:
				return
			}
		}
	}()
	err := w.resume(new)
	resumeDone <- struct{}{}
	if err != nil {
		logW(req.Workflow).Errorf("workflow resume: %v", err)
		return
	}
	s, err := json.Marshal(new)
	if err != nil {
		logW(req.Workflow).Errorf("workflow marshal: %v", err)
		return
	}
	if req.Workflow.Status == Workflow_Started {
		req.Workflow.Status = Workflow_Running
	}
	if w.newThread != nil {
		req.Workflow.Threads = append(req.Workflow.Threads, w.newThread)
	}
	req.Workflow.State = s
	req.Workflow.Version++

	if teardown != nil {
		err = teardown(new)
		if err != nil {
			logW(req.Workflow).Errorf("workflow teardown: %v", err)
			return
		}
	}
	_, err = c.UpdateWorkflow(context.Background(), &UpdateWorkflowReq{
		Workflow:    req.Workflow,
		LockID:      req.LockID,
		UnblockedAt: req.Thread.UnblockedAt,
	})
	if err != nil {
		logW(req.Workflow).Errorf("workflow update: %v", err)
		return
	}
	logW(req.Workflow).Infof("workflow updated")
}

func Setup(ctx context.Context, client RuntimeClient, s Service) error {
	for _, v := range s.Types {
		_, err := client.PutType(ctx, v)
		if err != nil {
			return fmt.Errorf("put type: %v %v", v, err)
		}
	}
	for _, v := range s.Workflows {
		_, err := client.PutWorkflowAPI(ctx, v.API)
		if err != nil {
			return fmt.Errorf("put api: %v %v", v, err)
		}
	}
	return nil
}

func ManageWorkflows(ctx context.Context, client RuntimeClient, s Service) error {
	stream, err := client.RegisterWorkflowHandler(ctx, &RegisterWorkflowHandlerReq{Service: s.Name, Pool: 1000, PollIntervalMs: 1})
	if err != nil {
		return err
	}
	apis := map[string]WorkflowDefinition{}
	for _, v := range s.Workflows {
		apis[v.API.Name] = v
	}
	for {
		select {
		case <-ctx.Done():
		default:
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			api := apis[req.Workflow.API]
			if api.Setup == nil {
				logW(req.Workflow).Errorf("no setup code for workflow: %v", err)
				continue
			}
			new, err := api.Setup()
			if err != nil {
				logW(req.Workflow).Errorf("workflow setup: %v", err)
				continue
			}
			go handleReq(client, req, new, api.Teardown)
		}
	}
}

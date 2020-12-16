package async

import (
	"encoding/json"
	"fmt"

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

func logW(w *Workflow) *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"module":     "gorchestrate-async",
		"workflowId": w.Id,
		"workflow":   w.Name,
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
		if req.Thread.Id == t.Id {
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
	err := w.resume(new)
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
		LockId:      req.LockId,
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
			api := apis[req.Workflow.Name]
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

package async

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

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

func handleReq(c RuntimeClient, req *LockedWorkflow, new interface{}, teardown func(interface{}) error) {
	if req.Workflow.Status == Workflow_Running {
		err := json.Unmarshal(req.Workflow.State, &new)
		if err != nil {
			log.Printf("json parse: %v %v", err, req.Workflow.Id)
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
		log.Printf("process resume: %v %v", err, req.Workflow.Id)
		return
	}
	s, err := json.Marshal(new)
	if err != nil {
		log.Printf("json marshal: %v %v", err, req.Workflow.Id)
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
			log.Printf("error during workflow teardown: %v %v", req.Workflow.Id, err)
			return
		}
	}

	ddd, _ := json.MarshalIndent(req.Workflow, "", " ")
	log.Printf("SENT TO WORKFLOW SERVER: %v", string(ddd))
	_, err = c.UpdateWorkflow(context.Background(), &UpdateWorkflowReq{
		Workflow:    req.Workflow,
		LockId:      req.LockId,
		UnblockedAt: req.Thread.UnblockedAt,
	})
	if err != nil {
		log.Printf("update process: %v %v", err, req.Workflow.Id)
		return
	}
}

func Manage(ctx context.Context, client RuntimeClient, ss ...Service) error {
	for _, s := range ss {
		for _, v := range s.Types {
			_, err := client.PutType(ctx, v)
			if err != nil {
				return fmt.Errorf("put type: %v %v", v, err)
			}
		}
		for _, v := range s.Workflows {
			log.Printf("PUT API: %v", v.API.Name)
			_, err := client.PutWorkflowAPI(ctx, v.API)
			if err != nil {
				return fmt.Errorf("put api: %v %v", v, err)
			}
		}
	}

	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Add(1)
		go func(s Service) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := manage(ctx, client, s)
					if err != nil {
						log.Printf("error in service %v manage: %v ", s.Name, err)
					}
					time.Sleep(time.Second * 5)
				}
			}
		}(s)
	}
	wg.Wait()
	return nil
}

func manage(ctx context.Context, client RuntimeClient, s Service) error {
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
			// ddd, _ := json.MarshalIndent(req, "", " ")
			// log.Printf("RECEIVED FROM WORKFLOW SERVER: %v", string(ddd))
			api := apis[req.Workflow.Name]
			if api.Setup == nil {
				log.Printf("no setup code for struct: %v", req.Workflow.Id)
				continue
			}
			new, err := api.Setup()
			if err != nil {
				log.Printf("error during workflow setup: %v %v", req.Workflow.Id, err)
				continue
			}
			go handleReq(client, req, new, api.Teardown)
		}
	}
}

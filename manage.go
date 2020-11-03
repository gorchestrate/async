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
	Name         string
	Types        []*Type
	APIs         []API
	Init         func(ctx context.Context) error
	SingletonRun func(ctx context.Context, c RuntimeClient) error
}

type API struct {
	API          *ProcessAPI
	NewProcState func() interface{}
}

type AsyncType interface {
	Type() *Type
}

func handleReq(c RuntimeClient, req *LockedProcess, new interface{}) {
	if req.Process.Status == Process_Running {
		err := json.Unmarshal(req.Process.State, &new)
		if err != nil {
			log.Printf("json parse: %v %v", err, req.Process.Id)
			return
		}
	}
	for i, t := range req.Process.Threads {
		if req.Thread.Id == t.Id {
			req.Process.Threads = append(req.Process.Threads[:i], req.Process.Threads[i+1:]...)
		}
	}
	var counter int
	p := P{
		process:       req.Process,
		resumedThread: req.Thread,
		procStruct:    new,
		client:        c,
		counter:       &counter,
	}
	err := p.resume(new)
	if err != nil {
		log.Printf("process resume: %v %v", err, req.Process.Id)
		return
	}
	s, err := json.Marshal(new)
	if err != nil {
		log.Printf("json marshal: %v %v", err, req.Process.Id)
		return
	}
	if req.Process.Status == Process_Started {
		req.Process.Status = Process_Running
	}
	if p.newThread != nil {
		req.Process.Threads = append(req.Process.Threads, p.newThread)
	}
	req.Process.State = s
	req.Process.Version++

	ddd, _ := json.MarshalIndent(req.Process, "", " ")
	log.Printf("SENT TO WORKFLOW SERVER: %v", string(ddd))
	_, err = c.UpdateProcess(context.Background(), &UpdateProcessReq{
		Process:     req.Process,
		LockId:      req.LockId,
		UnblockedAt: req.Thread.UnblockedAt,
	})
	if err != nil {
		log.Printf("update process: %v %v", err, req.Process.Id)
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
		for _, v := range s.APIs {
			log.Printf("PUT API: %v", v.API.Name)
			_, err := client.PutAPI(ctx, v.API)
			if err != nil {
				return fmt.Errorf("put api: %v %v", v, err)
			}
		}
	}

	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Add(2)
		go func(s Service) {
			defer wg.Done()
			if s.SingletonRun == nil {
				return
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := s.SingletonRun(ctx, client)
					if err != nil {
						log.Printf("error in service %v run: %v ", s.Name, err)
					}
					time.Sleep(time.Second * 5)
				}
			}
		}(s)
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
	stream, err := client.RegisterProcessHandler(ctx, &RegisterProcessHandlerReq{Service: s.Name, Pool: 1000, PollIntervalMs: 1})
	if err != nil {
		return err
	}

	apis := map[string]API{}
	for _, v := range s.APIs {
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
			api := apis[req.Process.Name]
			if api.NewProcState == nil {
				log.Printf("unexpected api request: %v", req.Process.Name)
				continue
			}
			go handleReq(client, req, api.NewProcState())
		}
	}
}

package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JobStatus int

const (
	UnAssigned JobStatus = iota
	Assigned
	Failed
	Done
)

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	InputFiles        []string
	IntermediateFiles [][]string
	MapStatus         []JobStatus
	ReduceStatus      []JobStatus
	NumReduce         int
	MapDone           bool
	ReduceDone        bool
}


func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Data {
		if args.Job == Map {
			c.MapStatus[args.TaskNo] = Done
			for _, file := range args.Files {
				redNo, _ := strconv.Atoi(strings.Split(file, "-")[3])
				c.IntermediateFiles[redNo] = append(c.IntermediateFiles[redNo], file)
			}
		} else {
			c.ReduceStatus[args.ReduceNo] = Done
		}
	}
	// Check Map
	if !c.MapDone {
		c.MapDone = true
		for i, x := range c.MapStatus {
			if x == UnAssigned || x == Failed {
				c.MapDone = false
				reply.Files = make([]string, 1)
				reply.Files[0] = c.InputFiles[i]
				reply.Job = Map
				reply.NumReduce = c.NumReduce
				reply.TaskNo = i
				c.MapStatus[i] = Assigned
				go func(check int) {
					<-time.After(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.MapStatus[check] != Done {
						c.MapStatus[check] = Failed
					}
				}(i)
				return nil
			}
			if c.MapDone && x == Assigned {
				c.MapDone = false
			}
		}
		// All Assigned or Done
	}
	// Previous condition checks again if Map Done or not
	// If not ask to wait
	if !c.MapDone {
		reply.Job = Wait
		return nil
	}
	// Check Reduce
	if !c.ReduceDone {
		// Still Map phase
		c.ReduceDone = true
		for i, x := range c.ReduceStatus {
			if x == UnAssigned || x == Failed {
				c.ReduceDone = false
				reply.Files = c.IntermediateFiles[i]
				reply.Job = Reduce
				reply.ReduceNo = i
				c.ReduceStatus[i] = Assigned
				go func(check int) {
					<-time.After(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.ReduceStatus[check] != Done {
						c.ReduceStatus[check] = Failed
					}
				}(i)
				return nil
			}
			if c.ReduceDone && x == Assigned {
				c.ReduceDone = false
			}
		}
		// All Assigned or Done
	}
	// Still reduces are left to be finished
	if !c.ReduceDone {
		reply.Job = Wait
		return nil
	}
	// All reduces done
	reply.Quit = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Println(c.MapStatus, c.ReduceStatus)
	return c.ReduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nFiles := len(files)
	c := Coordinator{InputFiles: files, NumReduce: nReduce, IntermediateFiles: make([][]string, nReduce), MapStatus: make([]JobStatus, nFiles), ReduceStatus: make([]JobStatus, nReduce), MapDone: false, ReduceDone: false}

	// Your code here.

	c.server()
	return &c
}

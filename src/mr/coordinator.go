package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	inputFiles []string
	nReduces int
	mapTasks []MapReduceTask
	reduceTasks []MapReduceTask
	mapFinished bool
	reduceFinished bool
	mutex sync.Mutex
}

type MapReduceTask struct {
	Type string
	State int	// 0代表unassign，1代表assign，2代表finished
	Index int
	StartTime time.Time
	InputFile string
	InterNum int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func WorkerWaiting(c *Coordinator) bool {
	if !c.mapFinished {
		for i := 0; i < len(c.inputFiles); i++ {
			if c.mapTasks[i].State != 1 {
				return false
			}
		}
		return true
	} else {
		for i := 0; i < c.nReduces; i++ {
			if c.reduceTasks[i].State != 1 {
				return false
			}
		}
		return true
	}
}

func (c *Coordinator) WorkerHandler(args *MapReduceArgs, reply *MapReduceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	messageType := args.MessageType
	switch messageType {
	case 0:
		if WorkerWaiting(c) {
			reply.Task = MapReduceTask{
				Type: "Wait",
			}
			return nil
		}
		if !c.mapFinished {
			for i := 0; i < len(c.inputFiles); i++ {
				if c.mapTasks[i].State == 0 || (c.mapTasks[i].State == 1 && time.Since(c.mapTasks[i].StartTime) > 10*time.Second) {
					task := &c.mapTasks[i]
					task.StartTime = time.Now()
					task.State = 1

					reply.NReduces = c.nReduces
					reply.Task = *task
					return nil
				}
			}

		} else {
			if !c.reduceFinished {
				for i:=0; i < c.nReduces; i++ {
					if c.reduceTasks[i].State == 0 || (c.reduceTasks[i].State == 1 && time.Since(c.reduceTasks[i].StartTime) > 10*time.Second) {
						task := &c.reduceTasks[i]
						task.StartTime = time.Now()
						task.State = 1
						reply.NReduces = c.nReduces
						reply.Task = *task
						return nil
					}
				}
			}
		}

	case 1:
		task := args.Task
		task.State = 2
		fmt.Printf("该任务的索引为%v，属于%v任务\n", task.Index,task.Type)
		fmt.Printf("map任务是否都已经完成        %v \n", c.mapFinished)
		if c.mapFinished {
			c.reduceTasks[task.Index] = task
			for _, v := range c.reduceTasks {
				if v.State != 2 {
					return nil
				}
			}
			c.reduceFinished = true
		} else {
			c.mapTasks[task.Index] = task
			for _, v := range c.mapTasks {
				if v.State != 2 {
					return nil
				}
			}
			c.mapFinished = true
		}
	}
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.mapFinished && c.reduceFinished {
		return true
	}
	// Your code here.
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("Coordinator初始化")
	c := Coordinator{
		inputFiles: files,
		nReduces:   nReduce,
		mapFinished: false,
		reduceFinished: false,
	}
	fmt.Println(nReduce)
	for i:= 0; i < len(files); i++ {
		mapTask := MapReduceTask{
			Type:     "Map",
			State:     0,
			Index:     i,
			InputFile: files[i],
		}
		c.mapTasks = append(c.mapTasks, mapTask)
	}

	for i:= 0; i < nReduce; i++ {
		reduceTask := MapReduceTask{
			Type: "Reduce",
			State:  0,
			Index:  i,
			InterNum: len(c.inputFiles),
		}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}


	// Your code here.

	c.server()
	return &c
}

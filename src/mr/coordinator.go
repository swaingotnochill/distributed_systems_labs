package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Co-ordinator assigns map task.
// Map Task is based on file names.
// Then we create intermediate files with a specific nReduce number.
// Worker executes Reduce Task partitioned with nReduce.

type Coordinator struct {
	mtx         sync.Mutex
	nReduce     int
	mapTasks    map[string]*Task
	reduceTasks map[int]*Task
	phase       Phase
	done        bool
	files       []string
	taskTimeout time.Duration
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	CompletePhase
)

type TaskStatus int

const (
	IDLE TaskStatus = iota
	IN_PROGRESS
	COMPLETED
)

type Task struct {
	ID        int
	Type      TaskType
	File      string
	Status    TaskStatus
	StartTime time.Time
	WorkerId  int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if args.taskType == MAP {
		err := c.getMapTask(args, reply)
		if err != nil {
			return err
		}
	}

	if args.taskType == REDUCE {
		err := c.getReduceTask(args, reply)
		if err != nil {
			return err
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Initialize co-ordinator with Map phase, since we create a map task
	// first.
	c := Coordinator{
		nReduce:     nReduce,
		mapTasks:    make(map[string]*Task),
		reduceTasks: make(map[int]*Task),
		phase:       MapPhase,
		files:       files,
		taskTimeout: 10 * time.Second,
	}

	// Initialize map tasks.
	for i, file := range files {
		c.mapTasks[file] = &Task{
			ID:     i,
			Type:   MAP,
			File:   file,
			Status: IDLE,
		}
	}

	// Initialize reduce tasks.
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			ID:     i,
			Type:   REDUCE,
			Status: IDLE,
		}
	}

	c.server()
	// IMPORTANT: When all the files are processed, set c.job to "true".
	return &c
}

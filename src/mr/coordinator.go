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
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.phase == CompletePhase
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

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.checkTimeOuts()

	switch c.phase {
	case MapPhase:
		return c.assignMapTask(args, reply)
	case ReducePhase:
		return c.assignReduceTask(args, reply)
	case CompletePhase:
		reply.JobDone = true
		return nil
	}
	return nil
}

func (c *Coordinator) assignMapTask(args *GetTaskArgs, reply *GetTaskReply) error {

	// some checks.
	allCompleted := true
	hasInProgress := false
	hasIdle := false

	for _, task := range c.mapTasks {
		switch task.Status {
		case COMPLETED:
			continue
		case IN_PROGRESS:
			allCompleted = false
			hasInProgress = true
		case IDLE:
			hasIdle = true
			allCompleted = false
		}
	}

	// if all map tasks are done, move to reduce phase.
	if allCompleted {
		c.phase = ReducePhase
		return c.assignReduceTask(args, reply)
	}

	// if some are in progress and none are IDLE, we wait.
	if hasInProgress && !hasIdle {
		return nil // can exit from this function.
	}

	// Find an IDLE map task to assign.
	for filename, task := range c.mapTasks {
		if task.Status == IDLE {
			task.Status = IN_PROGRESS
			task.StartTime = time.Now()
			task.WorkerId = args.WorkerId

			reply.TaskId = task.ID
			reply.TaskType = MAP
			reply.File = filename
			reply.NReduce = c.nReduce
			reply.Phase = c.phase

			return nil // exit after assigning one task.
		}
	}
	log.Printf("NO MAP TASKS TO ASSIGN.")
	return nil
}

func (c *Coordinator) assignReduceTask(args *GetTaskArgs, reply *GetTaskReply) error {
	allCompleted := true
	hasIdle := false

	for _, task := range c.reduceTasks {
		switch task.Status {
		case COMPLETED:
			continue
		case IN_PROGRESS:
			allCompleted = false
		case IDLE:
			hasIdle = true
			allCompleted = false
		}
	}

	if allCompleted {
		c.phase = CompletePhase
		reply.JobDone = true
		return nil
	}

	// Don't wait if there are IDLE tasks - assign them immediately
	// Even if some are in progress
	if hasIdle {
		// look for IDLE reduce task to assign.
		for id, task := range c.reduceTasks {
			if task.Status == IDLE {
				task.Status = IN_PROGRESS
				task.StartTime = time.Now()
				task.WorkerId = args.WorkerId

				reply.TaskId = id
				reply.TaskType = REDUCE
				reply.NReduce = c.nReduce
				reply.Phase = c.phase
				reply.NMap = len(c.files)

				return nil // exit after assigning one reduce task.
			}
		}
	}

	// If we get here, all tasks are in-progress or completed, but not all completed
	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch args.TaskType {
	case MAP:
		if task, exists := c.mapTasks[c.files[args.TaskId]]; exists && task.WorkerId == args.WorkerId {
			// Only mark as completed if the worker ID matches
			task.Status = COMPLETED
		}
	case REDUCE:
		if task, exists := c.reduceTasks[args.TaskId]; exists && task.WorkerId == args.WorkerId {
			// Only mark as completed if the worker ID matches
			task.Status = COMPLETED
		}
	}

	// Check if we should transition phases
	allMapDone := true
	if c.phase == MapPhase {
		for _, task := range c.mapTasks {
			if task.Status != COMPLETED {
				allMapDone = false
				break
			}
		}
		if allMapDone {
			c.phase = ReducePhase
		}
	}

	allReduceDone := true
	if c.phase == ReducePhase {
		for _, task := range c.reduceTasks {
			if task.Status != COMPLETED {
				allReduceDone = false
				break
			}
		}
		if allReduceDone {
			c.phase = CompletePhase
		}
	}

	reply.Success = true
	return nil
}

func (c *Coordinator) checkTimeOuts() {
	now := time.Now()

	if c.phase == MapPhase {
		for _, task := range c.mapTasks {
			if task.Status == IN_PROGRESS && now.Sub(task.StartTime) > c.taskTimeout {
				log.Printf("Map task %d (worker %d) timed out, resetting to IDLE", task.ID, task.WorkerId)
				task.Status = IDLE
				task.WorkerId = 0
			}
		}
	} else if c.phase == ReducePhase {
		for _, task := range c.reduceTasks {
			if task.Status == IN_PROGRESS && now.Sub(task.StartTime) > c.taskTimeout {
				log.Printf("Reduce task %d (worker %d) timed out, resetting to IDLE", task.ID, task.WorkerId)
				task.Status = IDLE
				task.WorkerId = 0
			}
		}
	}
}

func (c *Coordinator) getMapTaskCount() int {
	return len(c.files)
}

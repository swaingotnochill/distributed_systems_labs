package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"

type Coordinator struct {
	nReduce            int
	fileMap            map[string]bool
	taskIdMap          map[string]int
	mapReduceTasksDone bool
}

// RPC handler for the worker to call and get task.
func (c *Coordinator) GetMapTask(request *TaskRequest, response *TaskResponse) error {
	var unProcessedFile string
	var taskId int
	for file, processed := range c.fileMap {
		if !processed {
			unProcessedFile = file
			taskId = c.taskIdMap[file]
			c.fileMap[file] = true
			break
		}
	}
	if request.WorkerState == 0 { // IDLE
		response.WorkerState = IN_PROGRESS
		response.NReduce = c.nReduce
		response.FileName = unProcessedFile
		response.TaskId = taskId
		response.TaskType = "map"
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

	if c.mapReduceTasksDone {
		ret = true
	}

	fmt.Println("All map reduce tasks are done. Thank you.")

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Create a filemap.
	fileMap := make(map[string]bool)
	taskIdMap := make(map[string]int)
	for idx, file := range files {
		fileMap[file] = false
		taskIdMap[file] = idx // This is the current file index in the files array/list.
	}

	c := Coordinator{nReduce: nReduce, fileMap: fileMap, taskIdMap: taskIdMap}

	c.server()
	defer c.Done() // TODO: IS this the coorect way to exit.
	return &c
}

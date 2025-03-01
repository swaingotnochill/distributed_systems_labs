package mr

//
// RPC definitions.

import "os"
import "strconv"

type TaskRequest struct {
	WorkerState WorkerState
}

type TaskResponse struct {
	TaskId            int
	FileName          string
	NReduce           int
	WorkerState       WorkerState
	TaskType          string
	AllFilesProcessed bool
}

type NotifyTaskRequest struct {
}

type NotifyTaskResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

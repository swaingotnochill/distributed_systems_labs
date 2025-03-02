package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	TaskId   int
	TaskType TaskType
	File     string
	NReduce  int
	NMap     int
	Phase    Phase
	JobDone  bool
}

type TaskCompleteArgs struct {
	TaskId   int
	TaskType TaskType
	WorkerId int
}

type TaskCompleteReply struct {
	Success bool
}

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

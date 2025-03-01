package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "plugin"
import "io"
import "fmt"

type Coordinator struct {
	// Your definitions here.

}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.fileName = "69"
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
	c := Coordinator{}

	intermediate := []KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		_, err = io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}

	fmt.Println("[DEBGU]: Intermediate files:")
	for k, v := range intermediate {
		fmt.Printf("\n %v : %v ", k, v)
	}
	c.server()
	return &c
}

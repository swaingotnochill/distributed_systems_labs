package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Global Intermediate Container to store files.
type IntermediateKV []KeyValue

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// can use pid as workerid since its running in same machine. when
	// running in a distributed environment, can use a combination
	// of machin_id + pid
	workerId := os.Getpid()

	for {
		// Get task from coordinator.
		args := GetTaskArgs{WorkerId: workerId}
		reply := GetTaskReply{}

		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		if reply.JobDone {
			// Only exit when coordinator explicitly says job is done
			return
		}

		switch reply.TaskType {
		case MAP:
			doMap(reply.TaskId, reply.File, reply.NReduce, mapf)
		case REDUCE:
			doReduce(reply.TaskId, reply.NReduce, reducef)
		default:
			// If no task is assigned yet, wait a bit before asking again
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// notify once tasks are completed.
		completeArgs := TaskCompleteArgs{
			TaskId:   reply.TaskId,
			TaskType: reply.TaskType,
			WorkerId: workerId,
		}
		if !call("Coordinator.TaskComplete", &completeArgs, &TaskCompleteReply{}) {
			log.Printf("TaskComplete RPC failed for task %v", reply.TaskId)
			// Even if notification fails, don't exit - try to get a new task
		}
	}
}

func doMap(taskId int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("cannot open %v", fileName)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", fileName)
		return
	}

	// apply map function.
	kva := mapf(fileName, string(content))

	// store intermediate key-value pairs in files so that it can be read
	// back properly for reduce tasks. json/encoder can be used to
	// encode the kva to json files and then decode them when apply reduce
	// function.
	encoders := make([]*json.Encoder, nReduce)
	tempFiles := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		tempname := fmt.Sprintf("mr-%d-%d-temp", taskId, i)
		tempFiles[i], _ = os.Create(tempname) // note this opens up a file. need to close post writing to it.
		encoders[i] = json.NewEncoder(tempFiles[i])
	}

	// write to intermediate files. all of them are json encoded.
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		_ = encoders[bucket].Encode(kv)
	}

	// close the files.
	for i := 0; i < nReduce; i++ {
		tempName := fmt.Sprintf("mr-%d-%d-temp", taskId, i)
		oname := fmt.Sprintf("mr-%d-%d", taskId, i)
		tempFiles[i].Close()
		_ = os.Rename(tempName, oname)
	}
}

func doReduce(taskId int, nReduce int, reducef func(string, []string) string) {
	// read all the intermediate files.
	intermediateKV := []KeyValue{}

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, taskId)
		file, err := os.Open(fileName)
		if err != nil {
			// Just log and continue - don't crash
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediateKV = append(intermediateKV, kv)
		}
		file.Close()
	}

	// sort them by keys.
	sort.Sort(ByKey(intermediateKV))

	// create output file.
	oname := fmt.Sprintf("mr-out-%d", taskId)
	tempFile := fmt.Sprintf("mr-out-%d-temp", taskId)
	temp, err := os.Create(tempFile)
	if err != nil {
		log.Printf("Failed to create temp file: %v", err)
		return
	}

	// process for each key group.
	i := 0
	for i < len(intermediateKV) {
		j := i + 1
		for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateKV[k].Value)
		}
		output := reducef(intermediateKV[i].Key, values)
		fmt.Fprintf(temp, "%v %v\n", intermediateKV[i].Key, output)
		i = j
	}
	temp.Close()
	if err := os.Rename(tempFile, oname); err != nil {
		log.Printf("Failed to create output file: %v", err)
	}
}

func CallGetTask(task TaskType) {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("RPC: GetTask: %v\n", reply.File)
	} else {
		fmt.Printf("ERROR: RPC Call to \"GetTask\" failed.")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

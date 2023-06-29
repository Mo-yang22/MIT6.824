package mr

import "fmt"
import "os"
import "log"
import "io/ioutil"
import "sort"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "encoding/json"
import "time"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	for {
		reply := Task{}
		for !AskTask(&reply) {
			break
		}
		if reply.TaskType == MapTask {
			MapWorker(mapf, &reply)
		} else if reply.TaskType == ReduceTask {
			ReduceWorker(reducef, &reply)
		} else if reply.TaskType == WaitingTask {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}
func MapWorker(mapf func(string, string) []KeyValue, reply *Task) {
	intermediate := []KeyValue{}
	for _, filename := range reply.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))
	onames := make([]string, reply.ReduceCount)
	ofiles := make([]*os.File, reply.ReduceCount)
	encs := make([]*json.Encoder, reply.ReduceCount)

	for i := 0; i < reply.ReduceCount; i++ {
		tempFile, err := ioutil.TempFile("", "mr-tmp-"+strconv.Itoa(reply.TaskId)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		onames[i] = tempFile.Name()
		ofiles[i] = tempFile
		encs[i] = json.NewEncoder(tempFile)
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % reply.ReduceCount
		err := encs[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for i, ofile := range ofiles {
		newFilePath := "mr-tmp-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
		err := os.Rename(ofile.Name(), newFilePath)
		if err != nil {
			log.Fatalf("cannot rename file: %v", err)
		}
		ofiles[i].Close()
	}

	// 删除临时文件
	for _, oname := range onames {
		os.Remove(oname)
	}
	InformTaskDone(reply.TaskId)
}
func ReduceWorker(reducef func(string, []string) string, reply *Task) {

	intermediate := []KeyValue{}
	for _, filename := range reply.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	//
	oname := "mr-out-" + strconv.Itoa(reply.ReduceId)
	tempFile, err := ioutil.TempFile("", "mr-out-*"+strconv.Itoa(reply.ReduceId))
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}
	ofileName := tempFile.Name()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// Write to the temporary file
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// Rename the temporary file to the desired output file
	err = os.Rename(ofileName, oname)
	if err != nil {
		log.Fatalf("cannot rename file: %v", err)
	}
	tempFile.Close()
	os.Remove(ofileName)
	InformTaskDone(reply.TaskId)
}
func AskTask(t *Task) bool {
	args := Args{}

	ok := call("Coordinator.TellTask", &args, t)
	if ok {
		if t.TaskType == MapTask || t.TaskType == ReduceTask {
			fmt.Printf("receive: %v\n", t.TaskId)
		}
		return true
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
}

func InformTaskDone(taskId int) {
	args := Args{}

	args.TaskId = taskId
	ok := call("Coordinator.TaskDone", &args, &Task{})
	if ok {
		fmt.Printf("Task Done: %v\n", taskId)
	} else {
		fmt.Println("InformTaskDone Failed\n")
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

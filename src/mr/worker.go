package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(reply *MapReduceReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("map任务%v处理开始\n", reply.Task.Index)
	mapTask := reply.Task
	file, err := os.Open(mapTask.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", mapTask.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTask.InputFile)
	}
	file.Close()
	kva := mapf(mapTask.InputFile, string(content))
	kvas := Partition(kva, reply.NReduces)

	for i:= 0; i < len(kvas); i++ {
		writeToJson(kvas[i], mapTask.Index, i)
	}

	args := MapReduceArgs{}
	args.MessageType = FinishTask
	args.Task = mapTask
	finishReply := MapReduceReply{}
	fmt.Printf("map任务%v处理完成\n", reply.Task.Index)
	call("Coordinator.WorkerHandler", &args, &finishReply)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func writeToJson(kvs[]KeyValue, mapTaskNum, reduceTaskNum int){
	filename := fmt.Sprintf("%s%d%s%d", "mr-", mapTaskNum, "-", reduceTaskNum)
	f, err := ioutil.TempFile("", filename)
	check(err)
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("error: ",err)
		}
	}
	os.Rename(f.Name(), filename)
}


func Partition(kva []KeyValue, nReduce int)[][]KeyValue {
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvas[v] = append(kvas[v], kv)
	}
	return kvas
}


func doReduce(reply *MapReduceReply, reducef func(string, []string) string) {
	fmt.Printf("reduce任务%v处理开始\n", reply.Task.Index)
	task := &reply.Task
	intermediate := []KeyValue{}
	for i:=0; i < task.InterNum; i++ {
		filename := fmt.Sprintf("%s%d%s%d", "mr-", i, "-", task.Index)
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
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	outputFileName := fmt.Sprintf("%s%d", "mr-out-",task.Index)

	ofile, _ := os.Create(outputFileName)

	i := 0
	for i < len(intermediate) {
		j := i+1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	args := MapReduceArgs{}
	args.MessageType = FinishTask
	args.Task = *task
	finishReply := MapReduceReply{}
	fmt.Printf("reduce任务%v处理完毕\n", reply.Task.Index)
	call("Coordinator.WorkerHandler", &args, &finishReply)
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for true {
		args := MapReduceArgs{}
		args.MessageType = RequestTask

		reply := MapReduceReply{}

		res := call("Coordinator.WorkerHandler", &args, &reply)
		if !res {
			break
		}

		switch reply.Task.Type {
		case "Map":
			doMap(&reply, mapf)
		case "Reduce":
			doReduce(&reply, reducef)
		case "Wait":
			time.Sleep(time.Second)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

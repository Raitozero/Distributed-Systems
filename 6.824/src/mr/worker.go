package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		args := MyArgs{}
		reply := MyReply{}
		res := call("Master.CallHandler", &args, &reply)
		if !res {
			return
		}
		switch reply.TaskType {
		case "map":
			doMapTask(mapf, &reply)
		case "reduce":
			doReduceTask(reducef, &reply)
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply *MyReply) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := mapf(reply.Filename, string(content))
	kvas := partition(kva, reply.NReduce)
	//create intermediate files: "mr-1-2", 1 is the MapTaskNum, 2 is ReduceTaskNum
	//put MapTask 1 into N reducers
	for idx := range kvas {
		oname := "mr-" + strconv.Itoa(reply.MapTaskNum) + "-" + strconv.Itoa(idx)
		ofile, _ := os.CreateTemp("", oname+"*")
		enc := json.NewEncoder(ofile)
		//NewEncoder returns a new encoder that writes to ofile.
		for _, kva := range kvas[idx] {
			err := enc.Encode(&kva)
			//Encode writes the JSON encoding of kva to the stream, followed by a newline character
			if err != nil {
				log.Fatalf("cannot write to %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}
	//The above code: write each kva to a single file named mr-mapTaskNum-reduceTaskNum
	finishedArg := MyArgs{callForMapFinish, reply.MapTaskNum, -1}
	finishedReply := MyReply{}
	call("Master.CallHandler", &finishedArg, &finishedReply)
}

/* Reduce function of wordcount
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
*/

//{{"hello","1"}, {"hello", "1"}, {"otherword", "1"},{"...","1"}}

func doReduceTask(reducef func(string, []string) string, reply *MyReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNum)
		file, err := os.Open(iname)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}

		dec := json.NewDecoder(file)
		//NewDecoder returns a new decoder that reads from file.
		for {
			kv := KeyValue{}
			err := dec.Decode(&kv)
			//decode from dec(file) and write to &kv
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	//Now, this intermediate file contains kvs of reply.ReduceTaskNum

	sort.Sort(ByKey(intermediate))
	//sort to ensure the kvs with the same key are adjacent

	oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNum)
	ofile, _ := os.CreateTemp("", oname+"*")
	i := 0
	for i < len(intermediate) {
		values := []string{}
		j := i
		for j < len(intermediate) {
			if intermediate[j].Key == intermediate[i].Key {
				values = append(values, intermediate[j].Value)
				j++
			} else {
				break
			}
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
	finishedArg := MyArgs{callForReduceFinish, reply.ReduceTaskNum, -1}
	finishedReply := MyReply{}
	call("Master.CallHandler", &finishedArg, &finishedReply)
}

func partition(kva []KeyValue, nReduce int) [][]KeyValue {
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		kvas[ihash(kv.Key)%nReduce] = append(kvas[ihash(kv.Key)%nReduce], kv)
	}
	return kvas
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

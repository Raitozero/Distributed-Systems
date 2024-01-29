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

type Master struct {
	NMap             int //number of map tasks
	NReduce          int //number of reduce tasks
	NMapFinished     int
	NReduceFinished  int
	MapTaskStatus    map[string]int //MapTaskStatus[filename] = 0->unallocated, 1->allocated, 2->finished
	ReduceTaskStatus map[int]int    //same
	mu               sync.Mutex
}

const (
	Unallocated = iota
	Allocated
	Finished
)

// Your code here -- RPC handlers for the worker to call.

// 1.Receive a Call from Worker, then handle it
func (m *Master) CallHandler(args *MyArgs, reply *MyReply) {
	workerRequestType := args.RequestType
	switch workerRequestType {
	case callForTask:
		m.AssignTask(reply)
	case callForMapFinish:
		m.mu.Lock()
		m.NMapFinished++
		m.MapTaskStatus[args.MapTaskFileName] = Finished
		m.mu.Unlock()
	case callForReduceFinish:
		m.mu.Lock()
		m.NReduceFinished++
		m.ReduceTaskStatus[args.ReduceTaskNum] = Finished
		m.mu.Unlock()
	}
}

func (m *Master) AssignTask(reply *MyReply) error {
	m.mu.Lock()
	//1. check if maptasks all done
	if m.NMapFinished < m.NMap {
		taskToAssign := "NULL"
		for filename, status := range m.MapTaskStatus {
			if status == Unallocated {
				taskToAssign = filename
				break
			}
		}
		if taskToAssign == "NULL" {
			reply.TaskType = "waiting"
			m.mu.Unlock()
		} else {
			reply.Filename = taskToAssign
			reply.NReduce = m.NReduce
			reply.TaskType = "map"
			m.MapTaskStatus[taskToAssign] = Allocated
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				m.mu.Lock()
				if m.MapTaskStatus[taskToAssign] == Allocated {
					// still waiting, assume the map worker is died
					m.MapTaskStatus[taskToAssign] = Unallocated
				}
				m.mu.Unlock()
			}()
		}
	} else if m.NMapFinished == m.NMap && m.NReduceFinished < m.NReduce {
		//2. If maptasks done, check reducetasks
		taskToAssign := -1
		for idx, status := range m.ReduceTaskStatus {
			if status == Unallocated {
				taskToAssign = idx
				break
			}
		}
		if taskToAssign == -1 {
			reply.TaskType = "waiting"
			m.mu.Unlock()
		} else {
			reply.NMap = m.NMap
			reply.TaskType = "reduce"
			reply.ReduceTaskNum = taskToAssign
			m.ReduceTaskStatus[taskToAssign] = Allocated
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				m.mu.Lock()
				if m.ReduceTaskStatus[taskToAssign] == Allocated {
					// still waiting, assume the reduce worker is died
					m.ReduceTaskStatus[taskToAssign] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else {
		reply.TaskType = "finished"
		m.mu.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	//initiate a master
	m := Master{}
	m.NMap = len(files)
	m.NReduce = nReduce
	m.NMapFinished = 0
	m.NReduceFinished = 0
	m.MapTaskStatus = make(map[string]int)
	m.ReduceTaskStatus = make(map[int]int)
	//set map, reduce taskstatus to unallocated
	for _, filename := range files {
		m.MapTaskStatus[filename] = Unallocated
	}
	for i := 0; i < nReduce; i++ {
		m.ReduceTaskStatus[i] = Unallocated
	}
	m.server()
	return &m
}

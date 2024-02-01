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
	MapTaskStatus    []int //0->unallocated, 1->allocated, 2->finished
	ReduceTaskStatus []int //same
	MapFileNames     []string
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
		m.MapTaskStatus[args.MapTaskNum] = Finished
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
		taskToAssign := -1
		for idx, status := range m.MapTaskStatus {
			if status == Unallocated {
				taskToAssign = idx
				break
			}
		}
		if taskToAssign == -1 {
			reply.TaskType = "waiting"
			m.mu.Unlock()
		} else {
			reply.Filename = m.MapFileNames[taskToAssign]
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
	m.MapTaskStatus = make([]int, m.NMap)
	m.ReduceTaskStatus = make([]int, m.NReduce)
	//set map, reduce taskstatus to unallocated
	m.server()
	return &m
}

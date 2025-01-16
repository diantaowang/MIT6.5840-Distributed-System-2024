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

type MRState int

const (
	Unalloc = iota
	Alloc
	Finish
)

const (
	NoTask = iota
	MapTask
	ReduceTask
)

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	NMap             int
	NReduce          int
	MapFiles         []string
	MapStates        []MRState // need mutex
	ReduceStates     []MRState // need mutex
	MapStartTimes    []float64 // need mutex
	ReduceStartTimes []float64 // need mutex
	AllMapFinish     bool      // need mutex
	AllReduceFinish  bool      // need mutex
	TimeLimit        int
	MapCnt           int // need mutex
	ReduceCnt        int // need mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetJob(arg *ExampleArgs, reply *GetJobReply) error {
	t0 := time.Now()
	ts := float64(t0.Unix()) + (float64(t0.Nanosecond()) / 1000000000.0)

	c.mu.Lock()
	allMapFinish := c.AllMapFinish
	allReduceFinish := c.AllReduceFinish
	c.mu.Unlock()

	if !allMapFinish {
		for i := 0; i < c.NMap; i++ {
			c.mu.Lock()
			alloc := c.MapStates[i] == Unalloc ||
				c.MapStates[i] != Finish && ts-c.MapStartTimes[i] > float64(c.TimeLimit)
			c.mu.Unlock()
			if alloc {
				c.mu.Lock()
				c.MapStartTimes[i] = ts
				c.MapStates[i] = Alloc
				c.mu.Unlock()
				reply.Job = MapTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				reply.FileName = c.MapFiles[i]
				reply.MapID = i
				break
			}
		}
	} else if !allReduceFinish {
		for i := 0; i < c.NReduce; i++ {
			c.mu.Lock()
			alloc := c.ReduceStates[i] == Unalloc ||
				c.ReduceStates[i] != Finish && ts-c.ReduceStartTimes[i] > float64(c.TimeLimit)
			c.mu.Unlock()
			if alloc {
				c.mu.Lock()
				c.ReduceStartTimes[i] = ts
				c.ReduceStates[i] = Alloc
				c.mu.Unlock()
				reply.Job = ReduceTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				reply.ReduceID = i
				break
			}
		}
	} else {
		reply.Job = NoTask
	}
	return nil
}

func (c *Coordinator) MapFinish(arg *ExampleArgs, reply *ExampleReply) error {
	mapID := arg.X
	c.mu.Lock()
	if c.MapStates[mapID] != Finish {
		c.MapStates[mapID] = Finish
		c.MapCnt++
		c.AllMapFinish = (c.MapCnt == c.NMap)
	}
	c.mu.Unlock()
	// fmt.Printf("map finish id: %d, all map finish: %t\n", mapID, c.AllMapFinish)
	reply.Y = arg.X
	return nil
}

func (c *Coordinator) ReduceFinish(arg *ExampleArgs, reply *ExampleReply) error {
	reduceID := arg.X
	c.mu.Lock()
	if c.ReduceStates[reduceID] != Finish {
		c.ReduceStates[reduceID] = Finish
		c.ReduceCnt++
		c.AllReduceFinish = (c.ReduceCnt == c.NReduce)
	}
	c.mu.Unlock()
	// fmt.Printf("reduce finish id: %d, all map finish: %t\n", reduceID, c.AllReduceFinish)
	reply.Y = arg.X
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := true

	// Your code here.
	ret = c.AllReduceFinish

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NMap = len(files)
	c.NReduce = nReduce
	c.MapFiles = files
	c.MapStates = make([]MRState, c.NMap)
	c.ReduceStates = make([]MRState, c.NReduce)
	c.MapStartTimes = make([]float64, c.NMap)
	c.ReduceStartTimes = make([]float64, c.NReduce)
	c.AllMapFinish = false
	c.AllReduceFinish = false
	c.TimeLimit = 10
	c.MapCnt = 0
	c.ReduceCnt = 0

	c.server()
	return &c
}

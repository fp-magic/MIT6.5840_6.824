package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// map work: Type="map", Name=fileName, MapId=mapId, ReduceId=reduceNum
// reduce work: Type="reduce", MapId=mapNum, ReduceId=reduceId
type Task struct {
	Type     string
	FileName string
	MapId    int
	ReduceId int
	Id       int
}

type Coordinator struct {
	// Your definitions here.
	TodoWork chan Task
	DoneWork chan Task
	State    map[int]int //0:TODO, 1:DOING, 2:DONE, 3:FAIL
	StateMutex  sync.Mutex
	Mid      int
	MidMutex sync.Mutex
	NxtId    int
	IdMutex  sync.Mutex
	Fin      int
	FinMutex sync.Mutex
	nReduce  int
	nMap     int
	DoneFlag bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// worker request a new task to do
func (c *Coordinator) Get(args *GetArgs, reply *GetReply) error {
	if c.DoneFlag {
		reply.Done = true
		return nil
	}
	reply.Done = false 
	select {
	case t, ok := <-c.TodoWork:
		if ok {
			reply.T = t
			reply.Succ = true
			c.StateMutex.Lock()
			if c.State[t.Id] == 0 {
				c.State[t.Id] = 1
				go c.Recall(t)
			} else {
				fmt.Printf("error in task state! should be 0 but is %d", c.State[t.Id])
				reply.Succ = false
			}
			c.StateMutex.Unlock()
		} else {
			reply.Succ = false
		}
	default:
		reply.Succ = false
	}
	return nil
}

// worker finish a task
func (c *Coordinator) Set(args *SetArgs, reply *SetReply) error {
	c.StateMutex.Lock()
	//fmt.Printf("set %v was %v\n",args.T,c.State[args.T.Id])
	if c.State[args.T.Id] == 1 {
		c.State[args.T.Id] = 2
		c.DoneWork <- args.T
	}
	c.StateMutex.Unlock()
	return nil
}

// check if the task finish in 10 seconds, throw away otherwise
func (c *Coordinator) Recall(t Task) {
	time.Sleep(10 * time.Second)
	c.StateMutex.Lock()
	if c.State[t.Id] != 2 { // timeout, retry
		//fmt.Printf("timeout")
		c.State[t.Id] = 3
		c.CreateTask(t.Type, t.FileName, t.MapId, t.ReduceId)
	}
	c.StateMutex.Unlock()
}

// check finished works
func (c *Coordinator) Check() {
	for t := range c.DoneWork {
		//.Print("check %v\n",t)
		if t.Type == "map" {
			//fmt.Println("done map")
			newReduce := false
			c.MidMutex.Lock()
			c.Mid++
			if c.Mid == c.nMap {
				newReduce = true
			}
			c.MidMutex.Unlock()
			//fmt.Printf("mid %v\n",c.Mid)
			if newReduce {
				for i := 0; i < c.nReduce; i++ {
					c.StateMutex.Lock()
					c.CreateTask("reduce", "", c.nMap, i)
					c.StateMutex.Unlock()
				}

			}
		} else if t.Type == "reduce" {
			c.FinMutex.Lock()
			c.Fin++
			c.FinMutex.Unlock()
		} else {
			log.Fatalf("Check: invalid task type %v", t.Type)
		}
	}
	//fmt.Println("check finished wtf")
}

// initialize new task, send to todochan
// add state mutex before call
func (c *Coordinator) CreateTask(tp string, fileName string, mapId int, reduceId int) {
	task := Task{
		Type:     tp,
		FileName: fileName,
		MapId:    mapId,
		ReduceId: reduceId,
	}
	//fmt.Printf("new task %v\n",task)
	c.IdMutex.Lock()
	task.Id = c.NxtId
	c.NxtId++
	c.IdMutex.Unlock()
	c.State[task.Id] = 0
	c.TodoWork <- task
}

//
// start a thread that listens for RPCs from worker.go
//
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

//TODO: function of init first tasks, solving done tasks

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.FinMutex.Lock()
	if c.Fin == c.nReduce {
		ret = true
		c.DoneFlag = true
		time.Sleep(1*time.Second) // let worker exit gracefully
		close(c.TodoWork)
		close(c.DoneWork)
	}
	c.FinMutex.Unlock()
	

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//parameters
	c.State = make(map[int]int)
	c.NxtId = 0
	c.Mid = 0
	c.Fin = 0
	c.DoneWork = make(chan Task, nReduce*len(files)*2)
	c.TodoWork = make(chan Task, nReduce*len(files)*2)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.DoneFlag = false

	// read files and split into nReduce*small files
	//fmt.Print(files, len(files), nReduce)
	for i, fileName := range files {
		c.CreateTask("map", fileName, i, nReduce)
	}
	go c.Check()
	c.server()
	return &c
}

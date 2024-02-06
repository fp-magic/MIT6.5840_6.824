package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

func readFile(fileName string) []byte {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	//fmt.Printf("read %v=%v\n",fileName, content[:20])
	return content
}

func writeFile(fileName string, content []byte) {
	ioutil.WriteFile(fileName,content,0777)
	//fmt.Printf("write %v=%v\n",fileName, content[:20])
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	print("worker running\n")
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		t, done := CallGet()
		if done {
			break
		}
		//print("getting work\n")
		if t != nil {
			//fmt.Printf("%v\n", t)
			if t.Type == "map" {
				content := readFile(t.FileName)
				kva := mapf(t.FileName, string(content))
				kvas := make([][]KeyValue, t.ReduceId)
				for _, kv := range kva {
					kvas[ihash(kv.Key)%t.ReduceId] = append(kvas[ihash(kv.Key)%t.ReduceId], kv)
				}
				for i := 0; i < t.ReduceId; i++ {
					data, err := json.Marshal(kvas[i])
					if err != nil {
						log.Fatalf("Worker: json marshal err=%v", err)
					}
					oname := "mr-" + strconv.Itoa(t.MapId) + "-" + strconv.Itoa(i)
					writeFile(oname, data)
				}
				CallSet(*t)
			} else if t.Type == "reduce" {
				intermediate := []KeyValue{}
				for i := 0; i < t.MapId; i++ {
					rname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(t.ReduceId)
					content := readFile(rname)
					kva := []KeyValue{}
					err := json.Unmarshal(content, &kva)
					if err != nil {
						log.Fatal("Worker: json unmarshal err=%v", err)
					}
					intermediate = append(intermediate, kva...)
				}
				sort.Sort(ByKey(intermediate))
				oname := "mr-out-" + strconv.Itoa(t.ReduceId)
				ofile, _ := os.Create(oname)
				// copied from mrsequential.go start
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

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				// copied from mrsequential.go end
				ofile.Close()
				CallSet(*t)
			} else {
				log.Fatalf("Worker: invalid task type %v", t.Type)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func CallGet() (*Task, bool) {
	args := GetArgs{}
	reply := GetReply{}
	ok := call("Coordinator.Get", &args, &reply)
	if ok {
		if reply.Done {
			return nil, true
		} else if reply.Succ {
			return &reply.T, false
		}
	} else {
		//fmt.Printf("get call failed!\n")
	}
	return nil, false
}

func CallSet(t Task) {
	args := &SetArgs{}
	reply := &SetReply{}
	args.T = t
	ok := call("Coordinator.Set", &args, &reply)
	if !ok {
		//fmt.Printf("set call failed!\n")
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data map[string]string
	task map[int64]int64//true: executed
	preRes map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	kv.task[args.CId]=args.Id
	kv.preRes[args.CId]=""
	reply.Value=kv.data[args.Key]
	kv.mu.Unlock()

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.data[args.Key]=args.Value
	kv.task[args.CId]=args.Id
	kv.preRes[args.CId]=""
	reply.Value=args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	id:=kv.task[args.CId]
	if(id!=args.Id){
		reply.Value=kv.data[args.Key]
		kv.data[args.Key]+=args.Value
		kv.task[args.CId]=args.Id
		kv.preRes[args.CId]=reply.Value
	}else{
		reply.Value=kv.preRes[args.CId]
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data=make(map[string]string)
	kv.task=make(map[int64]int64)
	kv.preRes=make(map[int64]string)
	return kv
}

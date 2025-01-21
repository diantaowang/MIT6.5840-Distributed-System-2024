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

type KVHist struct {
	id   int64
	oldv string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs   map[string]string
	temps map[int64]KVHist
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Printf("Put: key = %s, taskID = %d\n", args.Key, args.TaskID)
	clerkID := args.ClerkID
	kv.mu.Lock()
	if _, ok := kv.temps[clerkID]; ok {
		kv.temps[clerkID] = KVHist{-1, ""}
	}
	reply.Value = kv.kvs[args.Key]
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Printf("Put: key = %s, size = %d, clerkID = %d, taskID = %d\n", args.Key, len(args.Value), args.ClerkID, args.TaskID)
	clerkID := args.ClerkID
	taskID := args.TaskID
	kv.mu.Lock()
	if kv.temps[clerkID].id != taskID {
		kv.temps[clerkID] = KVHist{-1, ""}
		kv.kvs[args.Key] = args.Value
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Printf("Append: key = %s, value = %s\n", args.Key, args.Value)
	clerkID := args.ClerkID
	taskID := args.TaskID
	kv.mu.Lock()
	if kv.temps[clerkID].id == taskID {
		reply.Value = kv.temps[clerkID].oldv
	} else {
		oldv := kv.kvs[args.Key]
		kv.kvs[args.Key] = oldv + args.Value
		kv.temps[clerkID] = KVHist{taskID, oldv}
		reply.Value = oldv
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.temps = make(map[int64]KVHist)

	return kv
}

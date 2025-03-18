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

type Task struct {
	taskID int64
	oldv   string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs       map[string]string
	lastTasks map[int64]Task
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Printf("Put: key = %s, taskID = %d\n", args.Key, args.TaskID)
	clerkID := args.ClerkID
	kv.mu.Lock()
	//
	if _, ok := kv.lastTasks[clerkID]; ok {
		kv.lastTasks[clerkID] = Task{args.TaskID, ""}
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
	if kv.lastTasks[clerkID].taskID != taskID {
		kv.lastTasks[clerkID] = Task{taskID, ""}
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
	if kv.lastTasks[clerkID].taskID == taskID {
		reply.Value = kv.lastTasks[clerkID].oldv
	} else {
		oldv := kv.kvs[args.Key]
		kv.kvs[args.Key] = oldv + args.Value
		kv.lastTasks[clerkID] = Task{taskID, oldv}
		reply.Value = oldv
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.lastTasks = make(map[int64]Task)

	return kv
}

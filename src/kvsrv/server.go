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
	kvs map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	v, _ := kv.kvs[args.Key]
	kv.mu.Unlock()
	reply.Value = v
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Printf("Put: key = %s, value = %s\n", args.Key, args.Value)
	kv.mu.Lock()
	kv.kvs[args.Key] = args.Value
	kv.mu.Unlock()
	reply.Value = "ok"
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Printf("Append: key = %s, value = %s\n", args.Key, args.Value)
	kv.mu.Lock()
	old := kv.kvs[args.Key]
	kv.kvs[args.Key] = old + args.Value
	kv.mu.Unlock()
	reply.Value = old
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)

	return kv
}

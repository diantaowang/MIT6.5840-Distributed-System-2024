package kvraft

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	GET = iota
	PUT
	APPEND
)

const (
	SUCCESSFUL = true
	FAILED     = false
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd   int
	Key   string
	Value string
}

type Task struct {
	id    int64
	value string
	state bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs       map[string]string
	taskmu    sync.Mutex
	lastTasks map[int64]Task
	log       []int
}

func (kv *KVServer) atomicReadTask(clerkId int64) (Task, bool) {
	kv.taskmu.Lock()
	defer kv.taskmu.Unlock()
	lastTask, ok := kv.lastTasks[clerkId]
	return lastTask, ok
}

func (kv *KVServer) atomicWriteTask(clerkId int64, task Task) {
	kv.taskmu.Lock()
	defer kv.taskmu.Unlock()
	kv.lastTasks[clerkId] = task
}

// Get/Put/Append from the same clerk has no concurrency promised by labrpc
// (this situation does not match the actual network).
// and Get/Put/Append from different clerks has concurrency.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	log.Printf("Server-%d Get start", kv.me)

	clerkId := args.ClerkId
	taskId := args.TaskId
	lastTask, ok := kv.atomicReadTask(clerkId)

	if ok && lastTask.id > taskId {
		DPrintf("Server Get(): concurrency not supposed by labrpc\n")
	}
	if ok && lastTask.id == taskId && lastTask.state == SUCCESSFUL {
		reply.Err = OK
		reply.Value = lastTask.value
	} else {
		op := Op{}
		op.Cmd = GET
		op.Key = args.Key
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.Value = ""
			kv.atomicWriteTask(clerkId, Task{taskId, "", FAILED})
		} else {
			log.Printf("  -- waiting for consensus: server-%d, get op", kv.me)
			for len(kv.log) < index {
				time.Sleep(10 * time.Millisecond)
			}
			log.Printf("  -- wait end: server-%d, get op", kv.me)
			if term == kv.log[index-1] {
				kv.mu.Lock()
				value, ok2 := kv.kvs[args.Key]
				kv.mu.Unlock()
				if ok2 {
					reply.Err = OK
					reply.Value = value
					kv.atomicWriteTask(clerkId, Task{taskId, value, SUCCESSFUL})
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
					kv.atomicWriteTask(clerkId, Task{taskId, "", SUCCESSFUL})
				}
			} else {
				reply.Err = ErrWrongLeader
				reply.Value = ""
				kv.atomicWriteTask(clerkId, Task{taskId, "", FAILED})
			}
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	log.Printf("Server-%d Put start", kv.me)

	clerkId := args.ClerkId
	taskId := args.TaskId
	lastTask, ok := kv.atomicReadTask(clerkId)

	if ok && lastTask.id > taskId {
		DPrintf("Server Put(): concurrency not supposed by labrpc\n")
	}
	if ok && lastTask.id == taskId && lastTask.state == SUCCESSFUL {
		reply.Err = OK
	} else {
		op := Op{}
		op.Cmd = PUT
		op.Key = args.Key
		op.Value = args.Value
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.atomicWriteTask(clerkId, Task{taskId, "", FAILED})
		} else {
			log.Printf("  -- waiting for consensus: server-%d, put op", kv.me)
			for len(kv.log) < index {
				time.Sleep(10 * time.Millisecond)
			}
			log.Printf("  -- wait end: server-%d, put op", kv.me)
			if term == kv.log[index-1] {
				reply.Err = OK
				kv.atomicWriteTask(clerkId, Task{taskId, "", SUCCESSFUL})
			} else {
				reply.Err = ErrWrongLeader
				kv.atomicWriteTask(clerkId, Task{taskId, "", FAILED})
			}
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	log.Printf("Server-%d Append start", kv.me)

	clerkId := args.ClerkId
	taskId := args.TaskId
	lastTask, ok := kv.atomicReadTask(clerkId)

	if ok && lastTask.id > taskId {
		DPrintf("Server Append(): concurrency not supposed by labrpc\n")
	}
	if ok && lastTask.id == taskId && lastTask.state == SUCCESSFUL {
		reply.Err = OK
	} else {
		op := Op{}
		op.Cmd = APPEND
		op.Key = args.Key
		op.Value = args.Value
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.atomicWriteTask(clerkId, Task{taskId, "", FAILED})
		} else {
			log.Printf("  -- waiting for consensus: server-%d, append op", kv.me)
			for len(kv.log) < index {
				time.Sleep(10 * time.Millisecond)
			}
			log.Printf("  -- wait end: server-%d, append op", kv.me)
			if term == kv.log[index-1] {
				reply.Err = OK
				kv.atomicWriteTask(clerkId, Task{taskId, "", SUCCESSFUL})
			} else {
				reply.Err = ErrWrongLeader
				kv.atomicWriteTask(clerkId, Task{taskId, "", FAILED})
			}
		}
	}
}

func (kv *KVServer) applier() {
	// raft will send close signal by chan when kvserver is killed.
	for m := range kv.applyCh {
		if m.CommandValid {
			if m.CommandIndex != len(kv.log)+1 {
				DPrintf("applier ERROR: commandIndex does not match with log's length")
			}
			kv.mu.Lock()
			kv.log = append(kv.log, m.CommandTerm)
			kv.mu.Unlock()
			op := reflect.ValueOf(m.Command)
			cmd := op.FieldByName("Cmd").Int()
			key := op.FieldByName("Key").String()
			value := op.FieldByName("Value").String()
			if cmd == GET {
				// do nothing
			} else if cmd == PUT {
				kv.mu.Lock()
				kv.kvs[key] = value
				kv.mu.Unlock()
			} else if cmd == APPEND {
				kv.mu.Lock()
				kv.kvs[key] = kv.kvs[key] + value
				kv.mu.Unlock()
			} else {
				DPrintf("applier: unknown command\n")
			}
		} else {
			DPrintf("applier ERROR: invalid ApplyMsg\n")
		}
	}
	fmt.Printf("kvserver-%d is killed\n", kv.me)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = map[string]string{}
	kv.lastTasks = map[int64]Task{}
	kv.log = []int{}

	go kv.applier()

	return kv
}

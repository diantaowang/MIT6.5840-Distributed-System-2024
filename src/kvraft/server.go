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
	GET    = '0'
	PUT    = '1'
	APPEND = '2'
)

var OpCode = map[string]int{
	"GET":    GET,
	"PUT":    PUT,
	"APPEND": APPEND,
}

const TIMEOUT = 1*time.Second + 500*time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd     int
	Key     string
	Value   string
	ClerkId int64
	TaskId  int64
}

type DoneMsg struct {
	applyId int
	taskId  int64
	value   string
}

type Task struct {
	id    int64
	value string
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
	doneChs   map[int64]chan DoneMsg
	lastTasks map[int64]Task
}

// Get/Put/Append from the same clerk has no concurrency promised by labrpc
// (this situation does not match the actual network).
// and Get/Put/Append from different clerks has concurrency.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	log.Printf("  -- Server-%d Get start", kv.me)

	done := make(chan DoneMsg)
	kv.mu.Lock()
	if _, ok := kv.doneChs[args.ClerkId]; ok {
		fmt.Printf("ERROR")
	}
	kv.doneChs[args.ClerkId] = done
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.doneChs, args.ClerkId)
		kv.mu.Unlock()
	}()

	op := Op{GET, args.Key, "", args.ClerkId, args.TaskId}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		log.Printf("  -- Server-%d Get end: ErrWrongLeader-1", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	log.Printf("  -- Server-%d Get waiting... index=%d", kv.me, index)

	select {
	case msg := <-done:
		if msg.applyId == index && msg.taskId == args.TaskId {
			log.Printf("  -- Server-%d Get end: OK", kv.me)
			reply.Err = OK
			reply.Value = msg.value
		} else {
			log.Printf("  -- Server-%d Get end: ErrWrongLeader-2", kv.me)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(TIMEOUT):
		log.Printf("  -- Server-%d Get end: ErrTimeout, index=%d", kv.me, index)
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, oper string) {
	log.Printf("  -- Server-%d %s start", kv.me, oper)

	done := make(chan DoneMsg)
	kv.mu.Lock()
	if _, ok := kv.doneChs[args.ClerkId]; ok {
		fmt.Printf("ERROR")
	}
	kv.doneChs[args.ClerkId] = done
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.doneChs, args.ClerkId)
		kv.mu.Unlock()
	}()

	op := Op{OpCode[oper], args.Key, args.Value, args.ClerkId, args.TaskId}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		log.Printf("  -- Server-%d %s end: ErrWrongLeader-1", kv.me, oper)
		reply.Err = ErrWrongLeader
		return
	}

	log.Printf("  -- Server-%d %s waiting... index=%d", kv.me, oper, index)

	select {
	case msg := <-done:
		if msg.applyId == index && msg.taskId == args.TaskId {
			log.Printf("  -- Server-%d %s end: OK", kv.me, oper)
			reply.Err = OK
		} else {
			log.Printf("  -- Server-%d %s end: ErrWrongLeader-2", kv.me, oper)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(TIMEOUT):
		log.Printf("  -- Server-%d %s end: ErrTimeout, index=%d", kv.me, oper, index)
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply, "PUT")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply, "APPEND")
}

func (kv *KVServer) applier() {
	// raft will send close signal by chan when kvserver is killed.
	for m := range kv.applyCh {
		if m.CommandValid {
			op := reflect.ValueOf(m.Command)
			cmd := op.FieldByName("Cmd").Int()
			key := op.FieldByName("Key").String()
			value := op.FieldByName("Value").String()
			clerkId := op.FieldByName("ClerkId").Int()
			taskId := op.FieldByName("TaskId").Int()

			log.Printf("applier: server-%d, commandIndex=%d finished", kv.me, m.CommandIndex)

			doneMsg := DoneMsg{}
			doneMsg.applyId = m.CommandIndex
			doneMsg.taskId = taskId
			lastTask, ok := kv.lastTasks[clerkId]
			// duplicate, do nothing
			if ok && lastTask.id >= taskId {
				if cmd == GET {
					doneMsg.value = lastTask.value
				}
			} else {
				if cmd == GET {
					doneMsg.value = kv.kvs[key]
				} else if cmd == PUT {
					kv.kvs[key] = value
				} else {
					kv.kvs[key] = kv.kvs[key] + value
				}
				kv.lastTasks[clerkId] = Task{taskId, doneMsg.value}
			}
			kv.mu.Lock()
			ch, ok2 := kv.doneChs[clerkId]
			kv.mu.Unlock()
			if ok2 {
				ch <- doneMsg
			}
		}
	}
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
	kv.doneChs = map[int64]chan DoneMsg{}
	kv.lastTasks = map[int64]Task{}

	go kv.applier()

	return kv
}

package kvraft

import (
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

const TIMEOUT = 1500 * time.Millisecond
const RoundTime = 10 * time.Millisecond
const TryCount = 1500 / 10

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

type Entry struct {
	clerkId int64
	taskId  int64
	value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdb        map[string]string
	lastTaskIds map[int64]int64
	log         []Entry
}

// Get/Put/Append from the same clerk has no concurrency promised by labrpc
// (this situation does not match the actual network).
// and Get/Put/Append from different clerks has concurrency.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{GET, args.Key, "", args.ClerkId, args.TaskId}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if len(kv.log) >= index {
			if kv.log[index-1].clerkId == args.ClerkId && kv.log[index-1].taskId == args.TaskId {
				reply.Err = OK
				reply.Value = kv.log[index-1].value
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.Err = ErrTimeout
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, oper string) {
	op := Op{OpCode[oper], args.Key, args.Value, args.ClerkId, args.TaskId}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if len(kv.log) >= index {
			if kv.log[index-1].clerkId == args.ClerkId && kv.log[index-1].taskId == args.TaskId {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.Err = ErrTimeout
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

			lastTaskId, ok := kv.lastTaskIds[clerkId]
			getValue := ""
			if ok && lastTaskId >= taskId {
				// duplicate, PUT/APPEND do nothing
				if cmd == GET {
					getValue = kv.kvdb[key]
				}
			} else {
				if cmd == GET {
					getValue = kv.kvdb[key]
				} else if cmd == PUT {
					kv.kvdb[key] = value
				} else {
					kv.kvdb[key] = kv.kvdb[key] + value
				}
				kv.lastTaskIds[clerkId] = taskId
			}

			/*fmt.Printf("@: clerkId=%d, taskId=%d, lastTaskId=%d, op=%d, key=%s, value=%s, newValue=%s\n",
			clerkId, taskId, lastTaskId, cmd, key, value, kv.kvdb[key])*/

			kv.mu.Lock()
			kv.log = append(kv.log, Entry{clerkId, taskId, getValue})
			kv.mu.Unlock()
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
	kv.kvdb = map[string]string{}
	kv.lastTaskIds = map[int64]int64{}
	kv.log = []Entry{}

	go kv.applier()

	return kv
}

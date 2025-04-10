package kvraft

import (
	"bytes"
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

const debugServer = false

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

const TIMEOUT = 1200 * time.Millisecond
const RoundTime = 10 * time.Millisecond
const TryCount = 1200 / 10

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	TaskId  int64
	Cmd     int
	Key     string
	Value   string
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
	kvdb         map[string]string
	lastTaskIds  map[int64]int64
	log          []Entry
	prevLogIndex int
	persister    *raft.Persister
}

type SnapShot struct {
	Kvdb             map[string]string
	LastTaskIds      map[int64]int64
	LastIncludeIndex int // for debug
}

// Get/Put/Append from the same clerk has no concurrency promised by labrpc
// (this situation does not match the actual network).
// and Get/Put/Append from different clerks has concurrency.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{args.ClerkId, args.TaskId, GET, args.Key, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if index <= kv.prevLogIndex {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		} else if kv.prevLogIndex+len(kv.log) >= index {
			truncIndex := index - kv.prevLogIndex
			if kv.log[truncIndex-1].clerkId == args.ClerkId && kv.log[truncIndex-1].taskId == args.TaskId {
				reply.Err = OK
				reply.Value = kv.log[truncIndex-1].value
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
	op := Op{args.ClerkId, args.TaskId, OpCode[oper], args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if index <= kv.prevLogIndex {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		} else if kv.prevLogIndex+len(kv.log) >= index {
			truncIndex := index - kv.prevLogIndex
			if kv.log[truncIndex-1].clerkId == args.ClerkId && kv.log[truncIndex-1].taskId == args.TaskId {
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

func (kv *KVServer) parseCmd(command interface{}) (int64, int64, int64, string, string) {
	op := reflect.ValueOf(command)
	clerkId := op.FieldByName("ClerkId").Int()
	taskId := op.FieldByName("TaskId").Int()
	cmd := op.FieldByName("Cmd").Int()
	key := op.FieldByName("Key").String()
	value := op.FieldByName("Value").String()
	return clerkId, taskId, cmd, key, value
}

func (kv *KVServer) applySnapshot(data []byte, index int) {
	if data == nil {
		log.Fatalf("nil snapshot")
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot SnapShot
	if d.Decode(&snapshot) != nil {
		log.Fatalf("snapshot decode error")
	}
	if index != snapshot.LastIncludeIndex {
		msg := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", kv.me)
		log.Panic(msg)
	}
	kv.kvdb = snapshot.Kvdb
	kv.lastTaskIds = snapshot.LastTaskIds
	kv.mu.Lock()
	kv.log = []Entry{}
	kv.prevLogIndex = snapshot.LastIncludeIndex
	kv.mu.Unlock()
}

func (kv *KVServer) sendSnapshot(lastIncludeIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := SnapShot{}
	kvdb := map[string]string{}
	for k, v := range kv.kvdb {
		kvdb[k] = v
	}
	lastTaskIds := map[int64]int64{}
	for k, v := range kv.lastTaskIds {
		lastTaskIds[k] = v
	}
	snapshot.Kvdb = kvdb
	snapshot.LastTaskIds = lastTaskIds
	snapshot.LastIncludeIndex = lastIncludeIndex
	e.Encode(snapshot)
	kv.rf.Snapshot(lastIncludeIndex, w.Bytes())
}

func (kv *KVServer) applier() {
	// raft will send close signal by chan when kvserver is killed.
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.applySnapshot(m.Snapshot, m.SnapshotIndex)
		} else if m.CommandValid {
			clerkId, taskId, cmd, key, value := kv.parseCmd(m.Command)
			lastTaskId, ok := kv.lastTaskIds[clerkId]

			if debugServer {
				fmt.Printf("@ apply begin: server-%d, clerkId=%d, taskId=%d, lastTaskId=%d, op=%d, key=%s, value=%s, newValue=%s\n",
					kv.me, clerkId, taskId, lastTaskId, cmd-'0', key, value, kv.kvdb[key])
			}

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
			kv.mu.Lock()
			kv.log = append(kv.log, Entry{clerkId, taskId, getValue})
			kv.mu.Unlock()

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= 3*kv.maxraftstate {
				kv.sendSnapshot(m.CommandIndex)
			}
			if debugServer {
				fmt.Printf("@ apply end: server-%d, clerkId=%d, taskId=%d\n", kv.me, clerkId, taskId)
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
	kv.rf = raft.Make(servers, -1, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvdb = map[string]string{}
	kv.lastTaskIds = map[int64]int64{}
	kv.log = []Entry{}
	kv.prevLogIndex = 0
	kv.persister = persister

	go kv.applier()

	return kv
}

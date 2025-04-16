package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const debugShardkv = false

const (
	GET     = '0'
	PUT     = '1'
	APPEND  = '2'
	SYNC    = '3'
	REMOVE  = '4' // remove outdated kv pairs
	INSTALL = '5' // install shards
)

var OpCode = map[string]int{
	"Get":    GET,
	"Put":    PUT,
	"Append": APPEND,
}

var OpName = map[int]string{
	GET:     "Get",
	PUT:     "Put",
	APPEND:  "Append",
	SYNC:    "Sync",
	REMOVE:  "Remove",
	INSTALL: "Install",
}

const TIMEOUT = 1200 * time.Millisecond
const RoundTime = 10 * time.Millisecond
const TryCount = 1200 / 10

const NShards = shardctrler.NShards

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	TaskId  int64
	Cmd     int
	Key     string
	Value   string
	// for shards
	Gid         int
	ConfigNum   int
	Shards      []int
	To          [][]string
	Kvs         []map[string]string
	LastTaskIds map[int64]int64
}

type Entry struct {
	clerkId   int64
	taskId    int64
	value     string
	cancel    bool
	gid       int
	configNum int
}

type MoveTask struct {
	ConfigNum   int
	To          [][]string // to[i] -> to group-i
	Shards      [][]int
	Kvs         [][]map[string]string
	LastTaskIds map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead         int32 // set by Kill()
	kvdb         [NShards]map[string]string
	lastTaskIds  map[int64]int64
	log          []Entry //need mutex
	prevLogIndex int
	persister    *raft.Persister

	shards         [NShards]bool // current shards, need mutex.
	lastSendShards [NShards]int  // last movement task agreed by the current group.
	lastRecvShards map[int]int   // shards have been received from other groups.
	movingTasks    []MoveTask    // shards being sent to other groups.
}

type Snapshot struct {
	Kvdb             [NShards]map[string]string
	LastTaskIds      map[int64]int64
	LastIncludeIndex int // for debug

	Shards         [NShards]bool
	LastSendShards [NShards]int
	LastRecvShards map[int]int
	MovingTasks    []MoveTask
}

func copyMap[K comparable, V any](orig map[K]V) map[K]V {
	copied := make(map[K]V, len(orig))
	for key, value := range orig {
		copied[key] = value
	}
	return copied
}

func printMoveTask(task MoveTask) string {
	msg := "configNum=" + strconv.Itoa(task.ConfigNum) + ": "
	for i := 0; i < len(task.To)-1; i++ {
		msg += fmt.Sprintf("[%v, %v], ", task.To[i], task.Shards[i])
	}
	msg += fmt.Sprintf("[%v, %v]", task.To[len(task.To)-1], task.Shards[len(task.Shards)-1])
	return msg
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.shards[key2shard(args.Key)] {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	op := Op{args.ClerkId, args.TaskId, GET, args.Key, "", -1, -1, []int{}, [][]string{}, []map[string]string{}, map[int64]int64{}}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount && !kv.killed(); i++ {
		kv.mu.Lock()
		if index <= kv.prevLogIndex {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		} else if kv.prevLogIndex+len(kv.log) >= index {
			truncIndex := index - kv.prevLogIndex
			if kv.log[truncIndex-1].clerkId == args.ClerkId && kv.log[truncIndex-1].taskId == args.TaskId {
				if kv.log[truncIndex-1].cancel {
					reply.Err = ErrWrongGroup
				} else {
					reply.Err = OK
					reply.Value = kv.log[truncIndex-1].value
				}
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if i != TryCount && !kv.killed() {
			time.Sleep(RoundTime)
		}
	}
	reply.Err = ErrTimeout
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.shards[key2shard(args.Key)] {
		kv.mu.Unlock()
		//fmt.Printf("gid-%d: server-%d, ErrWrongGroup-1\n", kv.gid, kv.me)
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	uop := OpCode[args.Op]
	op := Op{args.ClerkId, args.TaskId, uop, args.Key, args.Value, -1, -1, []int{}, [][]string{}, []map[string]string{}, map[int64]int64{}}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount && !kv.killed(); i++ {
		kv.mu.Lock()
		if index <= kv.prevLogIndex {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		} else if kv.prevLogIndex+len(kv.log) >= index {
			truncIndex := index - kv.prevLogIndex
			if kv.log[truncIndex-1].clerkId == args.ClerkId && kv.log[truncIndex-1].taskId == args.TaskId {
				if kv.log[truncIndex-1].cancel {
					//fmt.Printf("gid-%d: server-%d, ErrWrongGroup-2\n", kv.gid, kv.me)
					reply.Err = ErrWrongGroup
				} else {
					reply.Err = OK
				}
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if i != TryCount && !kv.killed() {
			time.Sleep(RoundTime)
		}
	}
	reply.Err = ErrTimeout
}

// slice/map in args is safe.
// because during the remote call, encode/decode will copy the underlying data.
// args sent by client and args received by InstallShards are decoupled.
// so slice/map in args can be used safely.
func (kv *ShardKV) InstallShards(args *InstallShardsArgs, reply *InstallShardsReply) {
	if debugShardkv {
		fmt.Printf("InstallShards RPC: to gid-%d, server-%d; from args.Gid=%d, args.ConfigNum=%d, args.Shards=%v\n",
			kv.gid, kv.me, args.Gid, args.ConfigNum, args.Shards)
	}
	op := Op{-1, -1, INSTALL, "", "", args.Gid, args.ConfigNum, args.Shards, [][]string{}, args.Kvs, args.LastTaskIds}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for i := 0; i <= TryCount && !kv.killed(); i++ {
		kv.mu.Lock()
		if index <= kv.prevLogIndex {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		} else if kv.prevLogIndex+len(kv.log) >= index {
			truncIndex := index - kv.prevLogIndex
			if kv.log[truncIndex-1].gid == args.Gid && kv.log[truncIndex-1].configNum == args.ConfigNum {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if i != TryCount && !kv.killed() {
			time.Sleep(RoundTime)
		}
	}
	// time out or server is killed.
	reply.Err = ErrTimeout
}

func (kv *ShardKV) scanConfig() {
	// create Clerk to pull config.
	sm := shardctrler.MakeClerk(kv.ctrlers)
	for !kv.killed() {
		// ask controller for the latest configuration (only leader need).
		//if _, isLeader := kv.rf.GetState(); isLeader {
		_, isLeader := kv.rf.GetState()
		if debugShardkv {
			fmt.Printf("scanConfig: gid-%d: server-%d is leader = %v\n", kv.gid, kv.me, isLeader)
		}
		if isLeader {
			newConfig := sm.Query(-1)
			if debugShardkv {
				kv.mu.Lock()
				fmt.Printf("scanConfig: gid-%d: server-%d, kv.shards=%v, newConfig=%v, movingTasks=%v\n",
					kv.gid, kv.me, kv.shards, newConfig, kv.movingTasks)
				kv.mu.Unlock()
			}
			if newConfig.Num > 0 {
				diff := false
				var movingShards []int
				var to [][]string
				// NOTE: we need promise that read of the whole kv.shards[] is atomic in scanConfig().
				// because execution of scanConfig() and update of kv.shards[] is concurrent.
				kv.mu.Lock()
				for shard, have := range kv.shards {
					newGid := newConfig.Shards[shard]
					if have && kv.gid != newGid {
						movingShards = append(movingShards, shard)
						to = append(to, newConfig.Groups[newGid])
					}
					diff = diff || (have != (kv.gid == newGid))
				}
				kv.mu.Unlock()
				if len(movingShards) != 0 {
					op := Op{-1, -1, SYNC, "", "", -1, newConfig.Num, movingShards, to, []map[string]string{}, map[int64]int64{}}
					kv.rf.Start(op)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyGetPutAppend(cmd int, clerkId int64, taskId int64, key string, value string) (bool, string) {
	// NOTE: read kv.shards and change kv.kvdb don't need mutex.
	// because "apply...()"  is sequential rather than concurrent.
	shard := key2shard(key)
	if !kv.shards[shard] {
		return true, ""
	}

	lastTaskId, ok := kv.lastTaskIds[clerkId]
	getValue := ""
	if ok && lastTaskId >= taskId {
		//fmt.Printf("applyGetPutAppend-1: group-%d, server-%d, clerkId=%d, taskId=%d, key=%s, value=%s\n",
		//	kv.gid, kv.me, clerkId, taskId, key, value)
		// duplicate, PUT/APPEND do nothing
		if cmd == GET {
			getValue = kv.kvdb[shard][key]
		}
	} else {
		/*if ok {
			fmt.Printf("applyGetPutAppend-2: group-%d, server-%d, clerkId=%d, taskId=%d, key=%s, value=%s\n",
				kv.gid, kv.me, clerkId, taskId, key, value)
		} else {
			fmt.Printf("applyGetPutAppend-3: group-%d, server-%d, clerkId=%d, taskId=%d, key=%s, value=%s\n",
				kv.gid, kv.me, clerkId, taskId, key, value)
		}*/
		if cmd == GET {
			getValue = kv.kvdb[shard][key]
		} else if cmd == PUT {
			kv.kvdb[shard][key] = value
		} else {
			kv.kvdb[shard][key] = kv.kvdb[shard][key] + value
		}
		kv.lastTaskIds[clerkId] = taskId
	}
	return false, getValue
}

// the only entrance to add shards.
func (kv *ShardKV) applyInstallShards(gid int, configNum int, shards []int, kvs []map[string]string, lastTaskIds map[int64]int64) {
	if debugShardkv {
		fmt.Printf("applyInstallShards: gid-%d, server-%d, shards=%v, lastRecvShards=%v; configNum=%d, install shards=%v\n",
			kv.gid, kv.me, kv.shards, kv.lastRecvShards, configNum, shards)
	}
	for clerkId, lastTaskId := range lastTaskIds {
		if taskId, ok := kv.lastTaskIds[clerkId]; !ok || taskId < lastTaskId {
			kv.lastTaskIds[clerkId] = lastTaskId
		}
	}
	for i, shard := range shards {
		if kv.lastRecvShards[shard] < configNum {
			// only kv.shards need lock.
			kv.mu.Lock()
			if kv.shards[shard] {
				msg := fmt.Sprintf("ERROR: applyInstallShards: shard-%d (from group-%d) already exists in group-%d\n",
					shard, gid, kv.gid)
				log.Panic(msg)
			}
			kv.shards[shard] = true
			kv.mu.Unlock()
			kv.lastRecvShards[shard] = configNum
			// NOTE: must make a copy of kvs. otherwise kvs[] will exists in kv.kvdb and raft's log.
			// there is a race between key/value server modifying the map/slice and raft reading it
			// while persisting its log.
			kv.kvdb[shard] = copyMap(kvs[i])
		}
	}
	if debugShardkv {
		fmt.Printf("after applyInstallShards: gid-%d, server-%d, shards=%v\n", kv.gid, kv.me, kv.shards)
	}
}

// the only entrance to delete shards(kv.shards and kv.kvdb).
func (kv *ShardKV) applySync(configNum int, shards []int, to [][]string) {
	// filter out expired shards.
	// when apply a Sync, the server may lose shards in the Sync.
	// we can't move these shards to other groups.
	realShards := []int{}
	realTo := [][]string{}
	for i, shard := range shards {
		if kv.shards[shard] && kv.lastSendShards[shard] < configNum {
			realShards = append(realShards, shard)
			realTo = append(realTo, to[i])
			kv.lastSendShards[shard] = configNum
		}
	}
	// generate moving task.
	if len(realShards) != 0 {
		if debugShardkv {
			fmt.Printf("applySync: gid-%d, server-%d, configNum=%d, raw-shards=%v; real-shards=%v, kv.shards=%v\n",
				kv.gid, kv.me, configNum, shards, realShards, kv.shards)
		}
		to2 := map[string][]string{}
		shards2 := map[string][]int{}
		for i := 0; i < len(realTo); i++ {
			servers := realTo[i]
			to2[servers[0]] = servers
			shards2[servers[0]] = append(shards2[servers[0]], realShards[i])
		}
		task := MoveTask{}
		task.ConfigNum = configNum
		task.LastTaskIds = copyMap(kv.lastTaskIds)
		for key, servers := range to2 {
			task.To = append(task.To, servers)
			task.Shards = append(task.Shards, shards2[key])
			kvdbs := []map[string]string{}
			for _, shard := range shards2[key] {
				kvdbs = append(kvdbs, kv.kvdb[shard])
				kv.kvdb[shard] = map[string]string{} // remove some kv.kvdb
			}
			task.Kvs = append(task.Kvs, kvdbs)
		}
		kv.mu.Lock()
		for _, shard := range shards {
			kv.shards[shard] = false // remove some kv.shards
		}
		kv.movingTasks = append(kv.movingTasks, task)
		kv.mu.Unlock()
		if debugShardkv {
			fmt.Printf("gid-%d: after removing kv.shards=%v\n", kv.gid, kv.shards)
		}
	}
}

func (kv *ShardKV) applyRemove(configNum int, shards []int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if debugShardkv {
		fmt.Printf("gid-%d: server-%d, applyRemove: configNum=%d, old movingTasks=%v\n",
			kv.gid, kv.me, configNum, kv.movingTasks)
	}
	if len(kv.movingTasks) != 0 && kv.movingTasks[0].ConfigNum == configNum {
		// for debug
		shards2 := []int{}
		for _, slice := range kv.movingTasks[0].Shards {
			shards2 = append(shards2, slice...)
		}
		exist, existCnt := false, 0
		for _, a := range shards {
			for _, b := range shards2 {
				if a == b {
					exist = true
					existCnt++
					break
				}
			}
		}
		if kv.movingTasks[0].ConfigNum == configNum && exist {
			if len(shards) != len(shards2) || len(shards) != existCnt {
				msg := fmt.Sprintf("ERROR: applyRemove-1: unmatched shards: configNum=%d, recv=%v, pending=%v\n",
					configNum, shards, shards2)
				log.Panic(msg)
			}
			kv.movingTasks = kv.movingTasks[1:]
		} else if kv.movingTasks[0].ConfigNum < configNum {
			msg := fmt.Sprintf("ERROR-2: applyRemove-2: first task configNum %d < %d\n",
				kv.movingTasks[0].ConfigNum, configNum)
			log.Panic(msg)
		}
		// ... outdated REMOVE
	}
	if debugShardkv {
		fmt.Printf("gid-%d: server-%d, after applyRemove: new movingTasks=%v\n",
			kv.gid, kv.me, kv.movingTasks)
	}
}

func (kv *ShardKV) applySnapshot(data []byte, index int) {
	if data == nil {
		log.Fatalf("nil snapshot")
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot Snapshot
	if d.Decode(&snapshot) != nil {
		log.Fatalf("snapshot decode error")
	}
	if index != snapshot.LastIncludeIndex {
		msg := fmt.Sprintf("gid=%d, server %v snapshot doesn't match m.SnapshotIndex", kv.gid, kv.me)
		log.Fatal(msg)
	}
	kv.kvdb = snapshot.Kvdb
	kv.lastTaskIds = snapshot.LastTaskIds
	kv.prevLogIndex = snapshot.LastIncludeIndex
	kv.lastSendShards = snapshot.LastSendShards
	kv.lastRecvShards = snapshot.LastRecvShards
	kv.mu.Lock()
	kv.shards = snapshot.Shards
	kv.movingTasks = snapshot.MovingTasks
	if debugShardkv {
		fmt.Printf("after apply snapshot: gid-%d, server-%d, shards=%v, movingTasks=%v\n",
			kv.gid, kv.me, kv.shards, kv.movingTasks)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) sendShardsToGroup(wg *sync.WaitGroup, ch chan bool, configNum int, servers []string,
	shards []int, kvs []map[string]string, lastTaskIds map[int64]int64) {
	defer wg.Done()
	for !kv.killed() {
		for _, server := range servers {
			if kv.killed() {
				break
			}
			srv := kv.make_end(server)
			var args InstallShardsArgs
			var reply InstallShardsReply
			args.Gid = kv.gid
			args.ConfigNum = configNum
			args.Shards = shards
			args.Kvs = kvs
			args.LastTaskIds = lastTaskIds
			ok := srv.Call("ShardKV.InstallShards", &args, &reply)
			if ok && (reply.Err == OK) {
				ch <- true
				return
			}
			if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrNoKey) {
				msg := "ERROR: sendShardsToGroup, reply.Err=" + reply.Err
				log.Panic(msg)
			}
			// ... not ok, or ErrWrongLeader, or ErrTimeout, or server is killed.
		}
	}
	ch <- false
}

func (kv *ShardKV) shardsMovement() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			task := MoveTask{}
			task.ConfigNum = -1
			kv.mu.Lock()
			if len(kv.movingTasks) != 0 {
				task = kv.movingTasks[0]
			}
			kv.mu.Unlock()
			if task.ConfigNum != -1 {
				//fmt.Printf("shardsMovement begin: gid-%d, server-%d, task=%v\n", kv.gid, kv.me, task)
				var wg sync.WaitGroup
				ch := make(chan bool, len(task.To))
				removedShards := []int{}
				for i := 0; i < len(task.To); i++ {
					wg.Add(1)
					removedShards = append(removedShards, task.Shards[i]...)
					go kv.sendShardsToGroup(&wg, ch, task.ConfigNum, task.To[i], task.Shards[i], task.Kvs[i], task.LastTaskIds)
				}
				wg.Wait()
				close(ch)
				allok := true
				for ok := range ch {
					allok = allok && ok
				}
				if allok {
					op := Op{-1, -1, REMOVE, "", "", -1, task.ConfigNum, removedShards, [][]string{}, []map[string]string{}, map[int64]int64{}}
					// TODO: this server may not be leader, so Start() may fail.
					// for performance, REMOVE should success except server crash.
					kv.rf.Start(op)
				}
				//fmt.Printf("shardsMovement end: gid-%d, server-%d\n", kv.gid, kv.me)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) parseOp(oper interface{}) (int64, int64, int64, string, string,
	int64, int64, []int, [][]string, []map[string]string, map[int64]int64) {
	op := reflect.ValueOf(oper)
	clerkId := op.FieldByName("ClerkId").Int()
	taskId := op.FieldByName("TaskId").Int()
	cmd := op.FieldByName("Cmd").Int()
	key := op.FieldByName("Key").String()
	value := op.FieldByName("Value").String()
	gid := op.FieldByName("Gid").Int()
	configNum := op.FieldByName("ConfigNum").Int()
	shards := op.FieldByName("Shards").Interface().([]int)
	to := op.FieldByName("To").Interface().([][]string)
	kvs := op.FieldByName("Kvs").Interface().([]map[string]string)
	lastTaskIds := op.FieldByName("LastTaskIds").Interface().(map[int64]int64)
	return clerkId, taskId, cmd, key, value, gid, configNum, shards, to, kvs, lastTaskIds
}

// NOTE: don't need mutex.
func (kv *ShardKV) sendSnapshot(lastIncludeIndex int) {
	if debugShardkv {
		fmt.Printf("sendSnapshot begin: gid-%d, server-%d, lastIncludeIndex=%d\n", kv.gid, kv.me, lastIncludeIndex)
		if len(kv.movingTasks) != 0 {
			fmt.Printf("  kv.movingTasks:\n")
			fmt.Printf("  %s\n", printMoveTask(kv.movingTasks[0]))
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := Snapshot{}
	for i := 0; i < NShards; i++ {
		snapshot.Kvdb[i] = copyMap(kv.kvdb[i])
	}
	snapshot.LastTaskIds = copyMap(kv.lastTaskIds)
	snapshot.LastIncludeIndex = lastIncludeIndex
	snapshot.Shards = kv.shards
	snapshot.LastSendShards = kv.lastSendShards
	snapshot.LastRecvShards = copyMap(kv.lastRecvShards)
	snapshot.MovingTasks = kv.movingTasks // NOTE: gob.Encode will perform a deep copy.
	e.Encode(snapshot)
	kv.rf.Snapshot(lastIncludeIndex, w.Bytes())
	if debugShardkv {
		fmt.Printf("sendSnapshot end: gid-%d, server-%d, lastIncludeIndex=%d\n", kv.gid, kv.me, lastIncludeIndex)
	}
}

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			if debugShardkv {
				fmt.Printf("applier snapshot: gid-%d, server-%d, m.SnapshotIndex=%d\n", kv.gid, kv.me, m.SnapshotIndex)
			}
			kv.applySnapshot(m.Snapshot, m.SnapshotIndex)
		} else if m.CommandValid && m.Command == nil {
			if debugShardkv {
				fmt.Printf("applier: no-op, gid-%d, server-%d\n", kv.gid, kv.me)
			}
			kv.mu.Lock()
			kv.log = append(kv.log, Entry{-1, -1, "", false, -1, -1})
			kv.mu.Unlock()
		} else if m.CommandValid {
			clerkId, taskId, cmd, key, value, gid, configNum, shards, to, kvs, lastTaskIds := kv.parseOp(m.Command)
			if debugShardkv {
				fmt.Printf("applier: gid-%d, server-%d, committed=%d, clerkId=%d, taskId=%d, cmd=%s, key=%s, value=%s, gid=%d, configNum=%d, shards=%v\n",
					kv.gid, kv.me, len(kv.log)+1, clerkId, taskId, OpName[int(cmd)], key, value, gid, configNum, shards)
			}
			getValue := ""
			cancel := false
			if cmd == SYNC {
				kv.applySync(int(configNum), shards, to)
				gid = -1
			} else if cmd == REMOVE {
				kv.applyRemove(int(configNum), shards)
				gid = -1
			} else if cmd == INSTALL {
				kv.applyInstallShards(int(gid), int(configNum), shards, kvs, lastTaskIds)
			} else {
				cancel, getValue = kv.applyGetPutAppend(int(cmd), clerkId, taskId, key, value)
			}
			kv.mu.Lock()
			kv.log = append(kv.log, Entry{clerkId, taskId, getValue, cancel, int(gid), int(configNum)})
			kv.mu.Unlock()

			// log compaction
			raftstatesize := kv.persister.RaftStateSize()
			if kv.maxraftstate != -1 && raftstatesize >= 3*kv.maxraftstate {
				//fmt.Printf("applier: one log size=%d, current raft state size=%d\n",
				//	reflect.TypeOf(m.Command).Size(), raftstatesize)
				kv.sendSnapshot(m.CommandIndex)
			}
		} else {
			log.Fatal("applier ERROR: unknown msg\n")
		}
	}
	if debugShardkv {
		fmt.Printf("applier end: gid-%d, server-%d\n", kv.gid, kv.me)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	if debugShardkv {
		fmt.Printf("Kill: gid-%d, server-%d, shards=%v, movingTasks=%v\n",
			kv.gid, kv.me, kv.shards, kv.movingTasks)
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, gid, me, persister, kv.applyCh)

	for i := 0; i < NShards; i++ {
		kv.kvdb[i] = map[string]string{}
	}
	kv.lastTaskIds = map[int64]int64{}
	kv.log = []Entry{}
	kv.prevLogIndex = 0
	kv.persister = persister

	kv.shards = [NShards]bool{}
	// initially group-0 owns all shards by default,
	// which is consistent with config[0].
	if gid == 100 {
		for i := 0; i < NShards; i++ {
			kv.shards[i] = true
		}
	}
	kv.lastSendShards = [NShards]int{}
	kv.movingTasks = []MoveTask{}
	kv.lastRecvShards = map[int]int{} // default value is 0.

	go kv.scanConfig()
	go kv.applier()
	go kv.shardsMovement()

	return kv
}

package shardkv

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
	"6.5840/shardctrler"
)

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
	Gid       int
	ConfigNum int
	Shards    []int
	To        [][]string
	Kvs       map[string]string
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
	configNum int
	to        [][]string
	shards    [][]int
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
	dead        int32 // set by Kill()
	kvdb        map[string]string
	lastTaskIds map[int64]int64
	log         []Entry

	shards          [NShards]bool // current shards, need persist.
	lastSyncNum     int           // last movement task agreed by the current group.
	movingTasks     []MoveTask    // shards being sent to other groups, need persist.
	LastRecvMovTask map[int]int   // shards have been received from other groups, need persist.
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

	op := Op{args.ClerkId, args.TaskId, GET, args.Key, "", -1, -1, []int{}, [][]string{}, map[string]string{}}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if len(kv.log) >= index {
			if kv.log[index-1].clerkId == args.ClerkId && kv.log[index-1].taskId == args.TaskId {
				if kv.log[index-1].cancel {
					reply.Err = ErrWrongGroup
				} else {
					reply.Err = OK
					reply.Value = kv.log[index-1].value
				}
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.shards[key2shard(args.Key)] {
		kv.mu.Unlock()
		fmt.Printf("gid-%d: server-%d, ErrWrongGroup-1\n", kv.gid, kv.me)
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	uop := OpCode[args.Op]
	op := Op{args.ClerkId, args.TaskId, uop, args.Key, args.Value, -1, -1, []int{}, [][]string{}, map[string]string{}}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if len(kv.log) >= index {
			if kv.log[index-1].clerkId == args.ClerkId && kv.log[index-1].taskId == args.TaskId {
				if kv.log[index-1].cancel {
					fmt.Printf("gid-%d: server-%d, ErrWrongGroup-2\n", kv.gid, kv.me)
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
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.Err = ErrTimeout
}

func (kv *ShardKV) InstallShards(args *InstallShardsArgs, reply *InstallShardsReply) {
	op := Op{-1, -1, INSTALL, "", "", args.Gid, args.ConfigNum, args.Shards, [][]string{}, args.Kvs}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if len(kv.log) >= index {
			if kv.log[index-1].gid == args.Gid && kv.log[index-1].configNum == args.ConfigNum {
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

func (kv *ShardKV) sendSync() {
	// create Clerk to pull config.
	sm := shardctrler.MakeClerk(kv.ctrlers)
	for !kv.killed() {
		// ask controller for the latest configuration (only leader need).
		//if _, isLeader := kv.rf.GetState(); isLeader {
		_, isLeader := kv.rf.GetState()
		fmt.Printf("gid-%d: server-%d is leader = %v\n", kv.gid, kv.me, isLeader)
		if isLeader {
			newConfig := sm.Query(-1)
			fmt.Printf("gid-%d: server-%d, kv.shards=%v, newConfig=%v\n", kv.gid, kv.me, kv.shards, newConfig)
			if newConfig.Num > 0 {
				var movingShards []int
				var to [][]string
				//fmt.Printf("gid-%d: kv.shards=%v, newConfig=%v\n", kv.gid, kv.shards, newConfig)
				//fmt.Printf("gid-%d: server-%d: --------1\n", kv.gid, kv.me)
				kv.mu.Lock()
				//fmt.Printf("gid-%d: server-%d: --------2\n", kv.gid, kv.me)
				for shard, have := range kv.shards {
					newGid := newConfig.Shards[shard]
					if have && kv.gid != newGid {
						movingShards = append(movingShards, shard)
						to = append(to, newConfig.Groups[newGid])
					}
				}
				kv.mu.Unlock()
				if len(movingShards) != 0 {
					op := Op{-1, -1, SYNC, "", "", -1, newConfig.Num, movingShards, to, map[string]string{}}
					kv.rf.Start(op)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyGetPutAppend(cmd int, clerkId int64, taskId int64, key string, value string) (bool, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// NOTE: read kv.shards and change kv.kvdb must be atomic.
	// otherwise, operation on kv.kvdb may be lost.
	if !kv.shards[key2shard(key)] {
		return true, ""
	}

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
	return false, getValue
}

func (kv *ShardKV) applyInstallShards(gid int, configNum int, shards []int, kvs map[string]string) {
	if configNum > kv.LastRecvMovTask[gid] {
		kv.LastRecvMovTask[gid] = configNum
		kv.mu.Lock()
		defer kv.mu.Unlock()
		for _, i := range shards {
			if kv.shards[i] {
				msg := fmt.Sprintf("ERROR: applyInstallShards: shard-%d (from group-%d) already exists in group-%d\n",
					i, gid, kv.gid)
				log.Panic(msg)
			}
			kv.shards[i] = true
		}
		for k, v := range kvs {
			kv.kvdb[k] = v
		}
	}
}

func (kv *ShardKV) applySync(configNum int, shards []int, to [][]string) {
	if configNum > kv.lastSyncNum {
		kv.lastSyncNum = configNum
		task := MoveTask{}
		to2 := map[string][]string{}
		shards2 := map[string][]int{}
		for i := 0; i < len(to); i++ {
			servers := to[i]
			to2[servers[0]] = servers
			shards2[servers[0]] = append(shards2[servers[0]], shards[i])
		}
		for key, servers := range to2 {
			task.to = append(task.to, servers)
			task.shards = append(task.shards, shards2[key])
		}
		task.configNum = configNum
		kv.mu.Lock()
		//fmt.Printf("group-%d: removing shards = %v, kv.shards=%v\n", kv.gid, shards, kv.shards)
		for _, i := range shards {
			if !kv.shards[i] {
				msg := fmt.Sprintf("ERROR: applySync: lost shard-%d\n", i)
				log.Panic(msg)
			}
			kv.shards[i] = false
		}
		//fmt.Printf("group-%d: after removing kv.shards=%v\n", kv.gid, kv.shards)
		kv.movingTasks = append(kv.movingTasks, task) // TODO: kv.shards split with kv.kvdb. is the data consistent?
		// TODO: do persist
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyRemove(configNum int, shards []int) {
	newTasks := []MoveTask{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("applyRemove: configNum=%d, shards=%v\n", configNum, shards)
	for _, task := range kv.movingTasks {
		if task.configNum > configNum {
			newTasks = append(newTasks, task)
		}
	}
	fmt.Printf("gid-%d: server-%d, applyRemove: old movingTasks=%v, new movingTasks=%v\n",
		kv.gid, kv.me, kv.movingTasks, newTasks)
	if len(newTasks) != len(kv.movingTasks) {
		newkvs := map[string]string{}
		for k, v := range kv.kvdb {
			if !exist(key2shard(k), shards) {
				newkvs[k] = v
			}
		}
		kv.movingTasks = newTasks
		kv.kvdb = newkvs
	}
	fmt.Printf("gid-%d: server-%d, after applyRemove: new movingTasks=%v\n",
		kv.gid, kv.me, kv.movingTasks)
}

func exist(i int, shards []int) bool {
	for _, shard := range shards {
		if i == shard {
			return true
		}
	}
	return false
}

func (kv *ShardKV) sendInstallShards(wg *sync.WaitGroup, configNum int, servers []string, shards []int) {
	defer wg.Done()
	kvs := map[string]string{}
	kv.mu.Lock()
	for k, v := range kv.kvdb {
		if exist(key2shard(k), shards) {
			kvs[k] = v
		}
	}
	kv.mu.Unlock()
	for !kv.killed() {
		for _, server := range servers {
			srv := kv.make_end(server)
			var args InstallShardsArgs
			var reply InstallShardsReply
			args.Gid = kv.gid
			args.ConfigNum = configNum
			args.Shards = shards
			args.Kvs = kvs
			ok := srv.Call("ShardKV.InstallShards", &args, &reply)
			if ok && (reply.Err == OK) {
				return
			}
			if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrNoKey) {
				msg := "ERROR: sendInstallShards, reply.Err=" + reply.Err
				log.Panic(msg)
			}
			// ... not ok, or ErrWrongLeader, or ErrTimeout
		}
	}
}

func (kv *ShardKV) remove() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			task := MoveTask{}
			task.configNum = -1
			kv.mu.Lock()
			if len(kv.movingTasks) != 0 {
				task = kv.movingTasks[0]
			}
			kv.mu.Unlock()
			if task.configNum != -1 {
				var wg sync.WaitGroup
				for i := 0; i < len(task.to); i++ {
					wg.Add(1)
					go kv.sendInstallShards(&wg, task.configNum, task.to[i], task.shards[i])
				}
				wg.Wait()
				shards := []int{}
				for _, ss := range task.shards {
					shards = append(shards, ss...)
				}
				op := Op{-1, -1, REMOVE, "", "", -1, task.configNum, shards, [][]string{}, map[string]string{}}
				// TODO: this server may not be leader, so Start() may fail.
				// for performance, REMOVE should success except server crash.
				kv.rf.Start(op)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) parseOp(oper interface{}) (int64, int64, int64, string, string, int64, int64, []int, [][]string, map[string]string) {
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
	kvs := op.FieldByName("Kvs").Interface().(map[string]string)
	return clerkId, taskId, cmd, key, value, gid, configNum, shards, to, kvs
}

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			clerkId, taskId, cmd, key, value, gid, configNum, shards, to, kvs := kv.parseOp(m.Command)
			fmt.Printf("applier: gid-%d, node-%d, clerkId=%d, taskId=%d, cmd=%s, key=%s, value=%s, gid=%d, configNum=%d, shards=%v, to=%v, kvs=%v\n",
				kv.gid, kv.me, clerkId, taskId, OpName[int(cmd)], key, value, gid, configNum, shards, to, kvs)
			getValue := ""
			cancel := false
			if cmd == SYNC {
				kv.applySync(int(configNum), shards, to)
				gid = -1
			} else if cmd == REMOVE {
				kv.applyRemove(int(configNum), shards)
				gid = -1
			} else if cmd == INSTALL {
				kv.applyInstallShards(int(gid), int(configNum), shards, kvs)
			} else {
				cancel, getValue = kv.applyGetPutAppend(int(cmd), clerkId, taskId, key, value)
			}
			kv.mu.Lock()
			kv.log = append(kv.log, Entry{clerkId, taskId, getValue, cancel, int(gid), int(configNum)})
			kv.mu.Unlock()
			// TODO: log compaction
		}
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// TODO: restore from persister.
	kv.kvdb = map[string]string{}
	kv.lastTaskIds = map[int64]int64{}
	kv.log = []Entry{}

	kv.shards = [NShards]bool{}
	// initially group-0 owns all shards by default,
	// which is consistent with config[0].
	if gid == 100 {
		for i := 0; i < NShards; i++ {
			kv.shards[i] = true
		}
	}
	kv.lastSyncNum = -1
	kv.movingTasks = []MoveTask{}
	kv.LastRecvMovTask = map[int]int{} // default value is 0.

	go kv.sendSync()
	go kv.applier()
	go kv.remove()

	return kv
}

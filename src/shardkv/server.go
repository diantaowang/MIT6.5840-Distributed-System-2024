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
	configNum   int
	to          [][]string // to[i] -> to group-i
	shards      [][]int
	kvs         [][]map[string]string
	lastTaskIds map[int64]int64
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
	kvdb        [NShards]map[string]string
	lastTaskIds map[int64]int64
	log         []Entry //need mutex

	shards         [NShards]bool // current shards, need mutex.
	lastSyncNum    [NShards]int  // last movement task agreed by the current group.
	LastRecvShards map[int]int   // shards have been received from other groups.
	movingTasks    []MoveTask    // shards being sent to other groups.
}

func copyMap[K comparable, V any](orig map[K]V) map[K]V {
	copied := make(map[K]V, len(orig))
	for key, value := range orig {
		copied[key] = value
	}
	return copied
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

	for i := 0; i <= TryCount; i++ {
		kv.mu.Lock()
		if len(kv.log) >= index {
			if kv.log[index-1].clerkId == args.ClerkId && kv.log[index-1].taskId == args.TaskId {
				if kv.log[index-1].cancel {
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
		if i != TryCount {
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
	op := Op{-1, -1, INSTALL, "", "", args.Gid, args.ConfigNum, args.Shards, [][]string{}, args.Kvs, args.LastTaskIds}
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

func (kv *ShardKV) scanSync() {
	// create Clerk to pull config.
	sm := shardctrler.MakeClerk(kv.ctrlers)
	for !kv.killed() {
		// ask controller for the latest configuration (only leader need).
		//if _, isLeader := kv.rf.GetState(); isLeader {
		_, isLeader := kv.rf.GetState()
		if debugShardkv {
			fmt.Printf("scanSync: gid-%d: server-%d is leader = %v\n", kv.gid, kv.me, isLeader)
		}
		if isLeader {
			newConfig := sm.Query(-1)
			if debugShardkv {
				fmt.Printf("scanSync: gid-%d: server-%d, kv.shards=%v, newConfig=%v\n", kv.gid, kv.me, kv.shards, newConfig)
			}
			if newConfig.Num > 0 {
				diff := false
				var movingShards []int
				var to [][]string
				// NOTE: we need promise that read of the whole kv.shards[] is atomic in scanSync().
				// because execution of scanSync() and update of kv.shards[] is concurrent.
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
				// TODO: send too much empty SYNC operation.
				// we need send SYNC operation periodically even it is empty. otherwise, the server may
				// not receive any committed log when it recovers form crash. the situation is as follow:
				// (1) group-102 has shard-a, but the last configuration shows that group-101 should own
				//     shard-a and group-102 should not own any shard.
				// (2) group-102 crashes before sends shard-a to group-101.
				// (3) group-102 recovers from crash. initially, the shard it owns is empty. And the latest
				//     configuration shows that the shard it owns should also be empty. but it actually owns
				//     shard-a.
				// (4) the server in group-102 must send at least one empty SYNC operation to drive raft to
				//     commit the previous log. otherwise, raft will not commit any log and shard-a will never
				//     send to group-101.
				op := Op{-1, -1, SYNC, "", "", -1, newConfig.Num, movingShards, to, []map[string]string{}, map[int64]int64{}}
				kv.rf.Start(op)
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
		//fmt.Printf("applyGetPutAppend-2: group-%d, server-%d, clerkId=%d, taskId=%d, key=%s, value=%s\n",
		//	kv.gid, kv.me, clerkId, taskId, key, value)
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
			kv.gid, kv.me, kv.shards, kv.LastRecvShards, configNum, shards)
	}
	for clerkId, lastTaskId := range lastTaskIds {
		if kv.lastTaskIds[clerkId] < lastTaskId {
			kv.lastTaskIds[clerkId] = lastTaskId
		}
	}
	kv.mu.Lock()
	for i, shard := range shards {
		if kv.LastRecvShards[shard] < configNum {
			if kv.shards[shard] {
				msg := fmt.Sprintf("ERROR: applyInstallShards: shard-%d (from group-%d) already exists in group-%d\n",
					shard, gid, kv.gid)
				log.Panic(msg)
			}
			kv.LastRecvShards[shard] = configNum
			kv.shards[shard] = true
			// NOTE: must make a copy of kvs. otherwise kvs[] will exists in kv.kvdb and raft's log.
			// there is a race between key/value server modifying the map/slice and raft reading it
			// while persisting its log.
			kv.kvdb[shard] = copyMap(kvs[i])
			/*kvs2 := map[string]string{}
			for k, v := range kvs[i] {
				kvs2[k] = v
			}
			kv.kvdb[shard] = kvs2*/
		}
	}
	kv.mu.Unlock()
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
		if kv.shards[shard] && kv.lastSyncNum[shard] < configNum {
			realShards = append(realShards, shard)
			realTo = append(realTo, to[i])
			kv.lastSyncNum[shard] = configNum
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
		task.configNum = configNum
		task.lastTaskIds = copyMap(kv.lastTaskIds)
		for key, servers := range to2 {
			task.to = append(task.to, servers)
			task.shards = append(task.shards, shards2[key])
			kvdbs := []map[string]string{}
			for _, shard := range shards2[key] {
				kvdbs = append(kvdbs, kv.kvdb[shard])
				kv.kvdb[shard] = map[string]string{} // remove some kv.kvdb
			}
			task.kvs = append(task.kvs, kvdbs)
		}
		kv.mu.Lock()
		// NOTE: We must promise that update of kv.shards[] is atomic.
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

func (kv *ShardKV) applyRemove(configNum int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if debugShardkv {
		fmt.Printf("gid-%d: server-%d, applyRemove: configNum=%d, old movingTasks=%v\n",
			kv.gid, kv.me, configNum, kv.movingTasks)
	}
	if len(kv.movingTasks) != 0 && kv.movingTasks[0].configNum == configNum {
		if kv.movingTasks[0].configNum == configNum {
			kv.movingTasks = kv.movingTasks[1:]
		} else if kv.movingTasks[0].configNum < configNum {
			msg := fmt.Sprintf("ERROR: applyRemove: first task configNum %d < %d\n",
				kv.movingTasks[0].configNum, configNum)
			log.Panic(msg)
		}
		// ... outdated REMOVE
	}
	if debugShardkv {
		fmt.Printf("gid-%d: server-%d, after applyRemove: new movingTasks=%v\n",
			kv.gid, kv.me, kv.movingTasks)
	}
}

func (kv *ShardKV) sendInstallShards(wg *sync.WaitGroup, configNum int, servers []string, shards []int,
	kvs []map[string]string, lastTaskIds map[int64]int64) {
	defer wg.Done()
	for !kv.killed() {
		for _, server := range servers {
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
					go kv.sendInstallShards(&wg, task.configNum, task.to[i], task.shards[i], task.kvs[i], task.lastTaskIds)
				}
				wg.Wait()
				op := Op{-1, -1, REMOVE, "", "", -1, task.configNum, []int{}, [][]string{}, []map[string]string{}, map[int64]int64{}}
				// TODO: this server may not be leader, so Start() may fail.
				// for performance, REMOVE should success except server crash.
				kv.rf.Start(op)
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

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			clerkId, taskId, cmd, key, value, gid, configNum, shards, to, kvs, lastTaskIds := kv.parseOp(m.Command)
			if debugShardkv {
				fmt.Printf("applier: gid-%d, node-%d, index=%d, clerkId=%d, taskId=%d, cmd=%s, key=%s, value=%s, gid=%d, configNum=%d, shards=%v, to=%v, kvs=%v\n",
					kv.gid, kv.me, len(kv.log), clerkId, taskId, OpName[int(cmd)], key, value, gid, configNum, shards, to, kvs)
			}
			getValue := ""
			cancel := false
			if cmd == SYNC {
				kv.applySync(int(configNum), shards, to)
				gid = -1
			} else if cmd == REMOVE {
				kv.applyRemove(int(configNum))
				gid = -1
			} else if cmd == INSTALL {
				kv.applyInstallShards(int(gid), int(configNum), shards, kvs, lastTaskIds)
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

	for i := 0; i < NShards; i++ {
		kv.kvdb[i] = map[string]string{}
	}
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
	kv.lastSyncNum = [NShards]int{}
	kv.movingTasks = []MoveTask{}
	kv.LastRecvShards = map[int]int{} // default value is 0.

	go kv.scanSync()
	go kv.applier()
	go kv.remove()

	return kv
}

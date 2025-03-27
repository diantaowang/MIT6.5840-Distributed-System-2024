package shardctrler

import (
	"bytes"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Entry struct {
	clerkId int64
	taskId  int64
	config  Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs     []Config // indexed by config num
	lastTaskIds map[int64]int64
	log         []Entry
}

const (
	Join  = '0'
	Leave = '1'
	Move  = '2'
	Query = '3'
)

const TIMEOUT = 1200 * time.Millisecond
const RoundTime = 10 * time.Millisecond
const TryCount = 1200 / 10

type Op struct {
	// Your data here.
	ClerkId int64
	TaskId  int64
	Cmd     int8
	Context []byte
}

type Pair struct {
	first  int
	second int
}

func (sc *ShardCtrler) encode(cmd int, context interface{}) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(context)
	return w.Bytes()
}

func (sc *ShardCtrler) decode(cmd int, data []byte) interface{} {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if cmd == Join {
		servers := map[int][]string{}
		if d.Decode(&servers) != nil {
			log.Fatalf("decode Join error")
		}
		return servers
	} else if cmd == Leave {
		gids := []int{}
		if d.Decode(&gids) != nil {
			log.Fatalf("decode Leave error")
		}
		return gids
	} else if cmd == Move {
		mov := MoveContext{}
		if d.Decode(&mov) != nil {
			log.Fatalf("decode Move error")
		}
		return mov
	} else {
		num := 0
		if d.Decode(&num) != nil {
			log.Fatalf("decode Query error")
		}
		return num
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	context := sc.encode(Join, args.Servers)
	op := Op{args.ClerkId, args.TaskId, Join, context}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i <= TryCount; i++ {
		sc.mu.Lock()
		if len(sc.log) >= index {
			reply.WrongLeader = sc.log[index-1].clerkId != args.ClerkId || sc.log[index-1].taskId != args.TaskId
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.WrongLeader = true
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	context := sc.encode(Leave, args.GIDs)
	op := Op{args.ClerkId, args.TaskId, Leave, context}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i <= TryCount; i++ {
		sc.mu.Lock()
		if len(sc.log) >= index {
			reply.WrongLeader = sc.log[index-1].clerkId != args.ClerkId || sc.log[index-1].taskId != args.TaskId
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.WrongLeader = true
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	context := sc.encode(Move, MoveContext{args.Shard, args.GID})
	op := Op{args.ClerkId, args.TaskId, Move, context}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i <= TryCount; i++ {
		sc.mu.Lock()
		if len(sc.log) >= index {
			reply.WrongLeader = sc.log[index-1].clerkId != args.ClerkId || sc.log[index-1].taskId != args.TaskId
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.WrongLeader = true
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	context := sc.encode(Query, args.Num)
	op := Op{args.ClerkId, args.TaskId, Query, context}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i <= TryCount; i++ {
		sc.mu.Lock()
		if len(sc.log) >= index {
			reply.WrongLeader = sc.log[index-1].clerkId != args.ClerkId || sc.log[index-1].taskId != args.TaskId
			reply.Config = sc.log[index-1].config
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		if i != TryCount {
			time.Sleep(RoundTime)
		}
	}
	reply.WrongLeader = true
}

func (sc *ShardCtrler) parseCmd(command interface{}) (int64, int64, int64, []byte) {
	op := reflect.ValueOf(command)
	clerkId := op.FieldByName("ClerkId").Int()
	taskId := op.FieldByName("TaskId").Int()
	cmd := op.FieldByName("Cmd").Int()
	context := op.FieldByName("Context").Interface().([]byte)
	return clerkId, taskId, cmd, context
}

func (sc *ShardCtrler) applyJoin(servers map[int][]string) {
	config := Config{}
	config.Groups = map[int][]string{}
	config.Num = len(sc.configs)

	prevConfig := sc.configs[len(sc.configs)-1]

	// update config.Groups
	for k, v := range prevConfig.Groups {
		config.Groups[k] = v
	}
	for k, v := range servers {
		config.Groups[k] = v
	}

	// update config.servers
	// 1. generate deterministic gid sequence.
	oldGIDs := []Pair{}
	if len(prevConfig.Groups) != 0 {
		cnts := map[int]int{}
		for _, gid := range prevConfig.Shards {
			if gid != 0 {
				cnts[gid]++
			}
		}
		for gid, cnt := range cnts {
			oldGIDs = append(oldGIDs, Pair{cnt, gid})
		}
		sort.Slice(oldGIDs, func(i, j int) bool {
			return oldGIDs[i].first > oldGIDs[j].first ||
				oldGIDs[i].first == oldGIDs[j].first && oldGIDs[i].second < oldGIDs[j].second
		})
	}
	gids, appendGIDs := []int{}, []int{}
	for _, pair := range oldGIDs {
		gids = append(gids, pair.second)
	}
	for gid, _ := range servers {
		appendGIDs = append(appendGIDs, gid)
	}
	sort.Ints(appendGIDs)
	gids = append(gids, appendGIDs...)

	// 2. realloc
	lowerBound := NShards / (len(gids))
	targets := map[int]int{}
	for i, gid := range gids {
		if i < NShards%(len(gids)) {
			targets[gid] = lowerBound + 1
		} else {
			targets[gid] = lowerBound
		}
	}
	for i, gid := range prevConfig.Shards {
		if targets[gid] == 0 {
			config.Shards[i] = 0
		} else {
			config.Shards[i] = gid
			targets[gid]--
		}
	}
	for i, j := len(prevConfig.Groups), 0; i < len(gids); i++ {
		gid := gids[i]
		for targets[gid] > 0 {
			for config.Shards[j] != 0 {
				j++
			}
			config.Shards[j] = gid
			targets[gid]--
		}
	}
	/*fmt.Printf("applyJoin: server=%d, config.Num=%d, config.Shards=%v, config.Groups=%v\n",
	sc.me, config.Num, config.Shards, config.Groups)*/
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) applyLeave(gids []int) {
	config := Config{}
	config.Groups = map[int][]string{}
	config.Num = len(sc.configs)

	prevConfig := sc.configs[len(sc.configs)-1]

	isDeleteGid := func(gids []int, gid int) bool {
		for _, v := range gids {
			if gid == v {
				return true
			}
		}
		return false
	}

	// update config.Groups
	cnts := map[int]int{}
	for gid, servers := range prevConfig.Groups {
		if !isDeleteGid(gids, gid) {
			config.Groups[gid] = servers
			cnts[gid] = 0
		}
	}

	// update config.Shards
	newGroupsNum := len(config.Groups)
	if newGroupsNum != 0 {
		for i, gid := range prevConfig.Shards {
			if isDeleteGid(gids, gid) {
				config.Shards[i] = 0
			} else {
				cnts[gid] = cnts[gid] + 1
				config.Shards[i] = prevConfig.Shards[i]
			}
		}
		pairs := []Pair{}
		for gid, cnt := range cnts {
			pairs = append(pairs, Pair{cnt, gid})
		}
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].first < pairs[j].first ||
				pairs[i].first == pairs[j].first && pairs[i].second < pairs[j].second
		})
		for i, j := 0, 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				config.Shards[i] = pairs[j].second
				j = (j + 1) % len(pairs)
			}
		}
	}
	/*fmt.Printf("applyLeave: server=%d, config.Num=%d, config.Shards=%v, config.Groups=%v\n",
	sc.me, config.Num, config.Shards, config.Groups)*/
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) applyMove(mov MoveContext) {
	config := Config{}
	config.Groups = map[int][]string{}
	config.Num = len(sc.configs)

	prevConfig := sc.configs[len(sc.configs)-1]
	// update config.Groups
	for k, v := range prevConfig.Groups {
		config.Groups[k] = v
	}
	// update config.Shards
	for i, gid := range prevConfig.Shards {
		if i != mov.Shard {
			config.Shards[i] = gid
		} else {
			config.Shards[i] = mov.GID
		}
	}
	/*fmt.Printf("applyMove: server=%d, config.Num=%d, config.Shards=%v, config.Groups=%v\n",
	sc.me, config.Num, config.Shards, config.Groups)*/
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) applyQuery(num int) Config {
	if num == 0 {
		log.Printf("Query: invalid num (0)")
		return Config{}
	} else if num == -1 || num > len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[num]
	}
}

func (sc *ShardCtrler) applier() {
	// raft will send close signal by chan when kvserver is killed.
	for m := range sc.applyCh {
		if m.CommandValid {
			clerkId, taskId, cmd, data := sc.parseCmd(m.Command)
			lastTaskId, ok := sc.lastTaskIds[clerkId]
			config := Config{}
			if ok && lastTaskId >= taskId {
				// duplicate, PUT/APPEND do nothing
				if cmd == Query {
					config = sc.applyQuery(sc.decode(Query, data).(int))
				}
			} else {
				if cmd == Join {
					sc.applyJoin(sc.decode(Join, data).(map[int][]string))
				} else if cmd == Leave {
					sc.applyLeave(sc.decode(Leave, data).([]int))
				} else if cmd == Move {
					sc.applyMove(sc.decode(Move, data).(MoveContext))
				} else {
					config = sc.applyQuery(sc.decode(Query, data).(int))
				}
				sc.lastTaskIds[clerkId] = taskId
			}
			sc.mu.Lock()
			sc.log = append(sc.log, Entry{clerkId, taskId, config})
			sc.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastTaskIds = map[int64]int64{}
	sc.log = []Entry{}

	go sc.applier()

	return sc
}

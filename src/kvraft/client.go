package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId    int64
	nextTaskId int64
	leader     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.nextTaskId = 0
	ck.leader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.ClerkId = ck.clerkId
	args.TaskId = ck.nextTaskId
	ck.nextTaskId++
	server := ck.leader
	for {
		reply.Err, reply.Value = "", ""
		log.Printf("Client-%d Get start: key=%s, to node-%d\n", ck.clerkId, key, server)
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		// what situations lead to return false?
		// (1) request loss (2) reply loss (3) server crash.
		if ok {
			if reply.Err == OK || reply.Err == ErrNoKey {
				ck.leader = server
				log.Printf("Client-%d Get finish: key=%s, value=%s, to server-%d\n", ck.clerkId, key, reply.Value, server)
				return reply.Value
			} else {
				server = (server + 1) % len(ck.servers)
			}
		}
	}
}

/*func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	args.ClerkId = ck.clerkId
	args.TaskId = ck.nextTaskId
	ck.nextTaskId++
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.leader = i
			return reply.Value
		} else if ok && reply.Err == ErrNoKey {
			ck.leader = i
			return ""
		} else if ok && reply.Err != ErrWrongLeader {
			fmt.Printf("ERROR: Get() return undefined reply\n")
		}
	}

	var i, tryCnt = ck.leader, 0
	for {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leader = i
				return reply.Value
			} else if ok && reply.Err == ErrNoKey {
				ck.leader = i
				return ""
			} else if ok && reply.Err == ErrWrongLeader {
				tryCnt = 0
				i = (i + 1) % len(ck.servers)
			} else if ok {
				fmt.Printf("Get: unknown reply.Err\n")
			}
		} else {
			tryCnt++
			if tryCnt == 2 {
				tryCnt = 0
				i = (i + 1) % len(ck.servers)
			}
		}
	}
}*/

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.ClerkId = ck.clerkId
	args.TaskId = ck.nextTaskId
	ck.nextTaskId++
	server := ck.leader
	for {
		reply.Err = ""
		log.Printf("Client-%d %s begin: key=%s, value=%s, to node-%d\n", ck.clerkId, op, key, value, server)
		// what situations lead to return false?
		// (1) request loss (2) reply loss (3) server crash.
		ok := ck.servers[server].Call("KVServer."+op, &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leader = server
				log.Printf("Client-%d %s finish: key=%s, value=%s, to node-%d\n", ck.clerkId, op, key, value, server)
				break
			} else if reply.Err == ErrWrongLeader {
				server = (server + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond) // TODO: for debug
			} else {
				DPrintf("Client ERROR: PutAppend() return %s\n", reply.Err)
			}
		}
	}
}

/*func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.ClerkId = ck.clerkId
	args.TaskId = ck.nextTaskId
	ck.nextTaskId++
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			ck.leader = i
			break
		} else if ok && reply.Err == ErrNoKey {
			fmt.Printf("ERROR: PutAppend() return ErrNoKey\n")
		} else if ok && reply.Err != ErrWrongLeader {
			fmt.Printf("ERROR: PutAppend() return undefined reply\n")
		}
	}
}*/

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

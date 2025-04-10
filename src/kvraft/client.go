package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const debug = false

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

		if debug {
			fmt.Printf("Get begin: Client-%d, taskId=%d, key=%s, to server-%d\n",
				ck.clerkId, args.TaskId, key, ck.servers[server].Server)
		}

		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

		if debug {
			if !ok {
				fmt.Printf("Get end: loss, Client-%d, taskId=%d, key=%s, to server-%d\n",
					ck.clerkId, args.TaskId, key, ck.servers[server].Server)
			} else if reply.Err == OK || reply.Err == ErrNoKey {
				fmt.Printf("Get end: successful, Client-%d, taskId=%d, key=%s, to server-%d\n",
					ck.clerkId, args.TaskId, key, ck.servers[server].Server)
			} else {
				fmt.Printf("Get end: wrong, Client-%d, taskId=%d, key=%s, to server-%d\n",
					ck.clerkId, args.TaskId, key, ck.servers[server].Server)
			}
		}

		// what situations lead to return false?
		// (1) request loss (2) reply loss (3) server crash.
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.leader = server
			return reply.Value
		}
		if ok && reply.Err == ErrWrongLeader {
			time.Sleep(10 * time.Millisecond)
		}
		server = (server + 1) % len(ck.servers)
	}
}

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

		if debug {
			fmt.Printf("%s begin: Client-%d, taskId=%d, key=%s, value=%s, to server-%d\n",
				op, ck.clerkId, args.TaskId, key, value, ck.servers[server].Server)
		}

		// what situations lead to return false?
		// (1) request loss (2) reply loss (3) server crash.
		ok := ck.servers[server].Call("KVServer."+op, &args, &reply)

		if debug {
			if !ok {
				fmt.Printf("%s end: loss, Client-%d, taskId=%d, to server-%d\n",
					op, ck.clerkId, args.TaskId, ck.servers[server].Server)
			} else if reply.Err == OK {
				fmt.Printf("%s end: successful, Client-%d, taskId=%d, to server-%d\n",
					op, ck.clerkId, args.TaskId, ck.servers[server].Server)
			} else {
				fmt.Printf("%s end: wrong, Client-%d, taskId=%d, to server-%d\n",
					op, ck.clerkId, args.TaskId, ck.servers[server].Server)
			}
		}

		if ok && reply.Err == OK {
			ck.leader = server
			return
		}
		if ok && reply.Err == ErrNoKey {
			fmt.Printf("unknown error")
			return
		}
		if ok && reply.Err == ErrWrongLeader {
			time.Sleep(10 * time.Millisecond)
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

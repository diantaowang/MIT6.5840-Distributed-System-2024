package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log Entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int64
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int64
	votedFor    int
	log         []Entry

	commitIndex int64
	lastApplied int64
	applyCh     chan ApplyMsg

	nextIndex   []int64
	matchIndex  []int64
	sortedIndex []int

	state        int
	votes        int
	startTime    int64
	electionTime int

	cond *sync.Cond
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []Entry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) uptodate(args *RequestVoteArgs) bool {
	var term, index int64
	if len(rf.log) != 0 {
		index = int64(len(rf.log))
		term = rf.log[len(rf.log)-1].Term
	} else {
		index = 0
		term = 0
	}
	return args.LastLogTerm > term || args.LastLogTerm == term && args.LastLogIndex >= index
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/*fmt.Println("RequestVote RPC:")
	fmt.Printf("  Req Context: CandidateId=%d, Term=%d, LastLogIndex=%d, LastLogTerm=%d\n",
		args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	fmt.Printf("  Raft Node:  Id=%d, state=%d, currentTerm=%d, votedFor=%d\n",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)*/

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		goto aftervote
	}

	// a raft node must be Follower when voteFor is null.
	if rf.votedFor == -1 && rf.state != Follower {
		fmt.Printf("impossible: votedFor == -1 && state != Follower\n")
	}

	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		fmt.Printf("impossible: votedFor == CandidateId\n")
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = args.Term
		reply.VoteGranted = false
	} else if rf.uptodate(args) {
		// case 1: args.Term == rf.currentTerm && rf.votedFor == -1 && up-to-date
		// case 2: args.Term >  rf.currentTerm && up-to-date
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.startTime = time.Now().UnixMilli()
		rf.electionTime = rf.genElectionTime()
		reply.Term = args.Term
		reply.VoteGranted = true
	} else {
		// case 1: args.Term == rf.currentTerm && rf.votedFor == -1 && !up-to-date
		// case 2: args.Term >  rf.currentTerm && !up-to-date
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		reply.Term = args.Term
		reply.VoteGranted = false
	}

aftervote:
	/*fmt.Printf("  After Vote: Id=%d, state=%d, currentTerm=%d, votedFor=%d\n",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)
	fmt.Printf("  Vote Reply: reply.Term=%d, reply.VoteGranted=%t\n",
		reply.Term, reply.VoteGranted)*/
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/*fmt.Println("AppendEntries RPC:")
	fmt.Printf("  Append Context: LeaderId=%d, Term=%d, PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d, EntryLen=%d\n",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	fmt.Printf("  Raft Node:  Id=%d, state=%d, currentTerm=%d, logLen=%d\n",
		rf.me, rf.state, rf.currentTerm, len(rf.log))*/

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		goto afterappend
	}

	if args.Term == rf.currentTerm && rf.state == Leader {
		fmt.Printf("impossible: AppendEntries, two leader in one Term\n")
		return
	}

	rf.state = Follower
	rf.currentTerm = args.Term
	if rf.state == Candidate || rf.state == Leader {
		rf.votedFor = -1
	}
	rf.startTime = time.Now().UnixMilli()
	rf.electionTime = rf.genElectionTime()
	reply.Term = args.Term
	if len(rf.log) < int(args.PrevLogIndex) {
		reply.Success = false
	} else if args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
		i := 0
		// checkout point
		for j := int(args.PrevLogIndex); i < len(args.Entries) && j < len(rf.log); j++ {
			if args.Entries[i].Term != rf.log[j].Term {
				rf.log[j] = args.Entries[i]
			}
			i++
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		if args.LeaderCommit > rf.commitIndex {
			nextCommitIndex := min(args.LeaderCommit, int64(len(rf.log)))
			if nextCommitIndex > rf.commitIndex {
				rf.commitIndex = nextCommitIndex
				rf.cond.Signal()
			}
		}
		reply.Success = true
	} else {
		rf.log = rf.log[:args.PrevLogIndex-1]
		reply.Success = false
	}

afterappend:
	/*fmt.Printf("  Vote Reply: reply.Term=%d, reply.Success=%t\n",
	reply.Term, reply.Success)*/
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	/*if !ok {
		fmt.Printf("Vote Failed: node-%d didn't get node-%d vote, package loss\n", rf.me, server)
	} else if !reply.VoteGranted {
		fmt.Printf("Vote Failed: node-%d didn't get node-%d vote, reject\n", rf.me, server)
	}*/

	rf.mu.Lock()
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if ok && rf.currentTerm < reply.Term {
		// how about election time? reset or not?
		// only leader need
		if rf.state == Leader {
			rf.startTime = time.Now().UnixMilli()
			rf.electionTime = rf.genElectionTime()
		}
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	} else if ok && rf.currentTerm > reply.Term {
		// do nothing
	} else if ok {
		if rf.state == Candidate && reply.VoteGranted {
			rf.votes = rf.votes + 1
			if rf.votes > len(rf.peers)/2 {
				rf.initLeader()
			}
		}
	}
	rf.mu.Unlock()

	return ok
}

func (rf *Raft) advLeaderCommitIndex() {
	// must hold mutex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.sortedIndex[i] = len(rf.log)
		} else {
			rf.sortedIndex[i] = int(rf.matchIndex[i])
		}
	}
	sort.Ints(rf.sortedIndex)
	nextCommitIndex := rf.sortedIndex[len(rf.peers)/2]
	/*if nextCommitIndex > 0 {
		fmt.Printf("advance: commitIndex=%d, nextCommitIndex=%d; currentTerm=%d, nextTerm=%d\n",
			rf.commitIndex, nextCommitIndex, rf.currentTerm, rf.log[nextCommitIndex-1].Term)
	}*/
	if nextCommitIndex > int(rf.commitIndex) && rf.log[nextCommitIndex-1].Term == rf.currentTerm {
		rf.commitIndex = int64(nextCommitIndex)
		rf.cond.Signal()
	}
}

func (rf *Raft) applyToSM() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
		}
		/*fmt.Printf("node-%d apply entires to SM: lastApplied=%d, commitIndex=%d\n",
		rf.me, rf.lastApplied, rf.commitIndex)*/
		newLastApplied := rf.commitIndex
		rf.mu.Unlock()
		//fmt.Printf("node-%d apply entires begin\n", rf.me)
		for i := rf.lastApplied + 1; i <= newLastApplied; i++ {
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.log[i-1].Command
			msg.CommandIndex = int(i)
			rf.applyCh <- msg
		}
		rf.lastApplied = newLastApplied
		//fmt.Printf("node-%d apply entires end\n", rf.me)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()

	/*if !ok {
		fmt.Printf("Append Entries Failed: node-%d (term=%d) --> node-%d, package loss\n",
			args.LeaderId, args.Term, server)
	} else if !reply.Success {
		fmt.Printf("Append Entries Failed: node-%d (term=%d) --> node-%d, reject\n",
			args.LeaderId, args.Term, server)
	}*/

	if ok && rf.currentTerm < reply.Term {
		// How about election time? reset or not?
		// only leader need.
		if rf.state == Leader {
			rf.startTime = time.Now().UnixMilli()
			rf.electionTime = rf.genElectionTime()
		}
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	} else if ok && rf.currentTerm > reply.Term {
		// do nothing
	} else if ok {
		if rf.state == Leader && reply.Success {
			newNextIndex := args.PrevLogIndex + int64(len(args.Entries)) + 1
			if rf.nextIndex[server] != newNextIndex {
				rf.nextIndex[server] = newNextIndex
				rf.matchIndex[server] = newNextIndex - 1
				rf.advLeaderCommitIndex()
			}
		} else if rf.state == Leader {
			rf.nextIndex[server] = args.PrevLogIndex
		}
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) genAppendEntriesArgs(server int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = rf.nextIndex[server] - 1
	//fmt.Printf("genAppendEntriesArgs: to node-%d, args.PrevLogIndex=%d\n", server, args.PrevLogIndex)
	if args.PrevLogIndex == 0 {
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	}
	// Note: args.Entries can't shared the underline elements with log.
	args.Entries = append(args.Entries, rf.log[args.PrevLogIndex:]...)
	return args
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		// send heartbeat
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			//args := AppendEntriesArgs{}
			args := rf.genAppendEntriesArgs(i)
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) appendEntires() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			lastLogIndex := int64(len(rf.log))
			if lastLogIndex >= rf.nextIndex[i] {
				args := rf.genAppendEntriesArgs(i)
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Microsecond)
	}
}

func (rf *Raft) initLeader() {
	// must hold mutex
	rf.state = Leader
	//fmt.Printf("node-%d (term=%d) become leader\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = int64(len(rf.log)) + 1
			rf.matchIndex[i] = 0
		}
	}
	go rf.heartbeat()
	go rf.appendEntires()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		isLeader = false
	} else {
		rf.log = append(rf.log, Entry{rf.currentTerm, command})
		index = len(rf.log)
		term = int(rf.currentTerm)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) genElectionTime() int {
	return 300 + (rand.Int() % 300)
}

func (rf *Raft) election() {
	// init.
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.votes = 1
	rf.startTime = time.Now().UnixMilli()
	rf.electionTime = rf.genElectionTime()

	// send RequestVote RPC.
	n := len(rf.peers)
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	if len(rf.log) != 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
		args.LastLogIndex = int64(len(rf.log))
	} else {
		args.LastLogTerm = 0
		args.LastLogIndex = 0
	}
	for i := 0; i < n; i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		timeNow := time.Now().UnixMilli()
		rf.mu.Lock()
		timeout := timeNow-rf.startTime > int64(rf.electionTime)
		//fmt.Printf("node-%d: state=%d, now=%d, startTime=%d, electionTime=%d, passedTime=%d\n",
		//	rf.me, rf.state, timeNow, rf.startTime, rf.electionTime, timeNow-rf.startTime)
		if rf.state != Leader && timeout {
			//fmt.Printf("node-%d timeout\n", rf.me)
			rf.election()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send
//
//	messages.
//
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.nextIndex = make([]int64, len(rf.peers))
	rf.matchIndex = make([]int64, len(rf.peers))
	rf.sortedIndex = make([]int, len(rf.peers))

	rf.state = Follower
	rf.votes = 0
	rf.startTime = time.Now().UnixMilli()
	rf.electionTime = rf.genElectionTime()

	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start apply committed log to state machine
	go rf.applyToSM()

	return rf
}

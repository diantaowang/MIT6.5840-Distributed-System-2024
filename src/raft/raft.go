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

	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// for debug
var debugCommon = false
var debugVote = false
var debugAppend = false
var debugSnapshout = false

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
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
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
	currentTerm int
	votedFor    int
	log         []Entry
	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	nextIndex   []int
	matchIndex  []int
	sortedIndex []int

	state        int
	votes        int
	startTime    int64
	electionTime int
	cond         *sync.Cond
	startCh      chan byte
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func assert(satisfied bool, s string) {
	if !satisfied {
		fmt.Println("assert failed: " + s)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
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

	// must hold mutex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log = []Entry{}
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("readPersist(): decode currentTerm: %v\n", err)
	}
	if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("readPersist(): decode voteFor: %v\n", err)
	}
	if err := d.Decode(&log); err != nil {
		fmt.Printf("readPersist(): decode log: %v\n", err)
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		fmt.Printf("readPersist(): decode lastIncludedIndex: %v\n", err)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		fmt.Printf("readPersist(): decode lastIncludedTerm: %v\n", err)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dropNum := index - rf.lastIncludedIndex
	/*fmt.Printf("Snapshot: node-%d, dropNum= %d, index=%d, rf.lastIncludedIndex=%d\n",
	rf.me, dropNum, index, rf.lastIncludedIndex)*/
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[dropNum-1].Term
	rf.snapshot = snapshot
	rf.log = rf.log[dropNum:]
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
	ID           int // for debug
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
	ID      int // for debug
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Data              []byte
	Done              bool
	ID                int // for debug
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) uptodate(args *RequestVoteArgs) bool {
	index := rf.lastIncludedIndex + len(rf.log)
	term := rf.lastIncludedTerm
	if len(rf.log) != 0 {
		term = rf.log[len(rf.log)-1].Term
	}
	return args.LastLogTerm > term || args.LastLogTerm == term && args.LastLogIndex >= index
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if debugVote {
		fmt.Println("RequestVote RPC:")
		fmt.Printf("  Vote Req Context: CandidateId=%d, Term=%d, LastLogIndex=%d, LastLogTerm=%d\n",
			args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
		fmt.Printf("  Vote Raft Node:  node-%d, state=%d, currentTerm=%d, votedFor=%d\n",
			rf.me, rf.state, rf.currentTerm, rf.votedFor)
	}

	oldCurrentTerm := rf.currentTerm
	oldVotedFor := rf.votedFor

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		goto aftervote
	}

	// a raft node must be Follower when voteFor is null.
	assert(!(rf.votedFor == -1 && rf.state != Follower), "votedFor == -1 && state != Follower")

	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		fmt.Printf("impossible: term == currentTerm && votedFor == CandidateId\n")
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
	if oldCurrentTerm != rf.currentTerm || oldVotedFor != rf.votedFor {
		rf.persist()
	}
	if debugVote {
		fmt.Printf("  After Vote: Id=%d, state=%d, currentTerm=%d, votedFor=%d\n",
			rf.me, rf.state, rf.currentTerm, rf.votedFor)
		fmt.Printf("  Vote Reply: reply.Term=%d, reply.VoteGranted=%t\n",
			reply.Term, reply.VoteGranted)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if debugAppend {
		firstEntry := Entry{}
		if len(args.Entries) != 0 {
			firstEntry = args.Entries[0]
		}
		fmt.Println("AppendEntries RPC:")
		fmt.Printf("  Append Context: ID=%d, LeaderId=%d, Term=%d, PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d, EntryLen=%d, FirstEntry=%v, to-node-%d\n",
			args.ID, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries), firstEntry, rf.me)
		fmt.Printf("  Append Raft Node:  node-%d, state=%d, currentTerm=%d, logLen=%d\n",
			rf.me, rf.state, rf.currentTerm, len(rf.log))
	}

	oldCurrentTerm := rf.currentTerm
	oldVotedFor := rf.votedFor
	logChanged := false

	completeLogLen := rf.lastIncludedIndex + len(rf.log)
	reply.XTerm = 0
	reply.XIndex = 0
	reply.XLen = completeLogLen
	reply.ID = args.ID

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if args.Term == rf.currentTerm && rf.state == Leader {
		fmt.Printf("impossible: AppendEntries, two leader in one Term\n")
	} else {
		rf.state = Follower
		rf.currentTerm = args.Term
		if rf.state == Candidate || rf.state == Leader {
			rf.votedFor = -1
		}
		rf.startTime = time.Now().UnixMilli()
		rf.electionTime = rf.genElectionTime()
		reply.Term = args.Term
		// change rf.log[], rf.commmitIndex, etc...
		logIndex := args.PrevLogIndex - rf.lastIncludedIndex
		if completeLogLen < args.PrevLogIndex {
			reply.Success = false
		} else if logIndex == 0 {
			// NOTE: when rf log is empty, rf.lastIncludedTerm == args.PrevLogTerm == 0.
			assert(rf.lastIncludedTerm == args.PrevLogTerm, "term conflict with committed log")
			logChanged = rf.appendEntries(0, 0, args)
			reply.Success = true
		} else if logIndex < 0 {
			if args.PrevLogIndex+len(args.Entries) >= rf.lastIncludedIndex {
				// in this branch, start > 0 and args.Entries is non-empty.
				start := rf.lastIncludedIndex - args.PrevLogIndex
				assert(args.Entries[start-1].Term == rf.lastIncludedTerm, "term conflict with committed log (2)")
				logChanged = rf.appendEntries(0, start, args)
			}
			reply.Success = true
		} else {
			if rf.log[logIndex-1].Term == args.PrevLogTerm {
				logChanged = rf.appendEntries(logIndex, 0, args)
				reply.Success = true
			} else {
				// term conflict with log
				conflict_term := rf.log[logIndex-1].Term
				i := args.PrevLogIndex - rf.lastIncludedIndex
				for i > 0 && rf.log[i-1].Term == conflict_term {
					i--
				}
				rf.log = rf.log[:logIndex-1]
				logChanged = true
				reply.XTerm = conflict_term
				if i == 0 && rf.lastIncludedTerm == conflict_term {
					reply.XIndex = rf.lastIncludedIndex
				} else {
					reply.XIndex = rf.lastIncludedIndex + i + 1
				}
				reply.Success = false
			}
		}
	}

	if oldCurrentTerm != rf.currentTerm || oldVotedFor != rf.votedFor || logChanged {
		rf.persist()
	}
	if debugAppend {
		fmt.Printf("  Append Reply: node-%d, reply.Term=%d, reply.Success=%t, reply.XTerm=%d, reply.XIndex=%d, reply.XLen=%d\n",
			rf.me, reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if debugSnapshout {
		fmt.Println("InstallSnapshot RPC:")
		fmt.Printf("  Install Context: ID=%d, LeaderId=%d, Term=%d, lastIncludedIndex=%d, lastIncludedTerm=%d, FirstData=%v, DataLen=%d\n",
			args.ID, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludeTerm, args.Data[0], len(args.Data))
		fmt.Printf("  Install Raft Node:  node-%d, state=%d, currentTerm=%d, logLen=%d, lastIncludedIndex=%d, lastIncludedTerm=%d\n",
			rf.me, rf.state, rf.currentTerm, len(rf.log), rf.lastIncludedIndex, rf.lastIncludedTerm)
	}

	oldCurrentTerm := rf.currentTerm
	oldVotedFor := rf.votedFor
	othersChanged := false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else if args.Term == rf.currentTerm && rf.state == Leader {
		fmt.Printf("impossible: InstallSnapshot, two leader in one Term\n")
	} else {
		rf.state = Follower
		rf.currentTerm = args.Term
		if rf.state == Candidate || rf.state == Leader {
			rf.votedFor = -1
		}
		rf.startTime = time.Now().UnixMilli()
		rf.electionTime = rf.genElectionTime()
		reply.Term = args.Term
		if rf.lastIncludedIndex < args.LastIncludedIndex {
			rf.snapshot = args.Data
			logIndex := args.LastIncludedIndex - rf.lastIncludedIndex
			if logIndex <= len(rf.log) && rf.log[logIndex-1].Term == args.LastIncludeTerm {
				rf.log = rf.log[logIndex:]
			} else {
				rf.log = []Entry{}
			}
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludeTerm
			othersChanged = true
		}
	}

	if oldCurrentTerm != rf.currentTerm || oldVotedFor != rf.votedFor || othersChanged {
		rf.persist()
	}
	if debugSnapshout {
		fmt.Printf("  Install Reply: node-%d, reply.Term=%d,  rf.state=%v, rf.lastIncludedIndex=%d, rf.lastIncludedTerm=%d\n",
			rf.me, reply.Term, rf.state, rf.lastIncludedIndex, rf.lastIncludedTerm)
	}
}

// rf.log[logStart:] <- args.Entries[entryStart:]
func (rf *Raft) appendEntries(logStart int, entryStart int, args *AppendEntriesArgs) bool {
	logChanged := false
	j := entryStart
	for i := logStart; j < len(args.Entries) && i < len(rf.log); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			rf.log[i] = args.Entries[j]
			logChanged = true
		}
	}
	if len(args.Entries[j:]) != 0 {
		rf.log = append(rf.log, args.Entries[j:]...)
		logChanged = true
	}
	if args.LeaderCommit > rf.commitIndex {
		nextCommitIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if nextCommitIndex > rf.commitIndex {
			rf.commitIndex = nextCommitIndex
			rf.cond.Signal()
		}
	}
	return logChanged
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
	defer rf.mu.Unlock()

	// if args.Term < rf.currentTerm, the reply must be expired.
	// expired reply does not need to be processed.
	if args.Term < rf.currentTerm {
		return ok
	}

	oldCurrentTerm := rf.currentTerm
	oldVotedFor := rf.votedFor
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
	if oldCurrentTerm != rf.currentTerm || oldVotedFor != rf.votedFor {
		rf.persist()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term < rf.currentTerm, the reply must be expired.
	// expired reply does not need to be processed.
	if args.Term < rf.currentTerm {
		return ok
	}
	if debugAppend && ok {
		fmt.Printf("**** reply from node-%d: ID=%d, reply.Term=%d, reply.Success=%v, reply.XTerm=%d, reply.XIndex=%d, reply.XLen=%d\n",
			server, reply.ID, reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
	}

	oldCurrentTerm := rf.currentTerm
	oldVotedFor := rf.votedFor

	if ok && rf.currentTerm < reply.Term {
		// How about election time? reset or not?
		// only leader need reset election time.
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
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		newNextIndex := newMatchIndex + 1
		// update nextIndex[i] need the follow 3 conditions when reply is successful.
		// (1) the node is still leader.
		// (2) reply is successful.
		// (3) nextIndex[i] < prevLogIndex + len(entries) + 1.
		if rf.state == Leader && reply.Success && rf.nextIndex[server] < newNextIndex {
			rf.nextIndex[server] = newNextIndex
			rf.matchIndex[server] = newMatchIndex
			rf.advLeaderCommitIndex()
		}
		// update nextIndex[i] need the follow 3 conditions when reply is failed.
		// (1) the node is still leader.
		// (2) reply is fresh and failed.
		// (3) matchIndex[i] < prevLogIndex + len(entries) and nextIndex[i] is decreasing.
		if rf.state == Leader && !reply.Success && rf.matchIndex[server] < newMatchIndex {
			if reply.XTerm == 0 {
				// reply.XLen may be 0.
				rf.nextIndex[server] = min(rf.nextIndex[server], max(reply.XLen, 1))
			} else {
				i := len(rf.log)
				for ; i > 0 && rf.log[i-1].Term != reply.XTerm; i-- {
				}
				if i == 0 {
					// rf.lastIncludedIndex may be 0, but the minimum of nextIndex[x] is 1.
					rf.nextIndex[server] = min(rf.nextIndex[server], min(max(rf.lastIncludedIndex, 1), reply.XIndex))
				} else {
					rf.nextIndex[server] = min(rf.nextIndex[server], i+rf.lastIncludedIndex)
				}
			}
		}
	}
	if oldCurrentTerm != rf.currentTerm || oldVotedFor != rf.votedFor {
		rf.persist()
	}
	if debugAppend && ok {
		fmt.Printf("**** after reply: node-%d, nextIndex[%d]=%d, matchIndex[%d]=%d\n\n",
			server, server, rf.nextIndex[server], server, rf.matchIndex[server])
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term < rf.currentTerm, the reply must be expired.
	// expired reply does not need to be processed.
	if args.Term < rf.currentTerm {
		return ok
	}

	oldCurrentTerm := rf.currentTerm
	oldVotedFor := rf.votedFor

	if ok && rf.currentTerm < reply.Term {
		if rf.state == Leader {
			rf.startTime = time.Now().UnixMilli()
			rf.electionTime = rf.genElectionTime()
		}
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	} else if ok && rf.currentTerm > reply.Term {
		// do nothing
	} else {
		if rf.nextIndex[server] < args.LastIncludedIndex+1 {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	}

	if oldCurrentTerm != rf.currentTerm || oldVotedFor != rf.votedFor {
		rf.persist()
	}

	return ok
}

func (rf *Raft) advLeaderCommitIndex() {
	// must hold mutex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.sortedIndex[i] = rf.lastIncludedIndex + len(rf.log)
		} else {
			rf.sortedIndex[i] = rf.matchIndex[i]
		}
	}
	sort.Ints(rf.sortedIndex)
	nextCommitIndex := rf.sortedIndex[len(rf.peers)/2]
	/*if nextCommitIndex > 0 {
		fmt.Printf("advance: commitIndex=%d, nextCommitIndex=%d; currentTerm=%d, nextTerm=%d\n",
		rf.commitIndex, nextCommitIndex, rf.currentTerm, rf.log[nextCommitIndex-1].Term)
	}*/
	if nextCommitIndex > rf.commitIndex &&
		rf.log[nextCommitIndex-rf.lastIncludedIndex-1].Term == rf.currentTerm {
		rf.commitIndex = nextCommitIndex
		rf.cond.Signal()
	}
}

func (rf *Raft) applyToSM() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
		}
		if rf.lastApplied < rf.lastIncludedIndex {
			msg := ApplyMsg{}
			msg.CommandValid = false
			msg.SnapshotValid = true
			msg.Snapshot = rf.snapshot
			msg.SnapshotIndex = rf.lastIncludedIndex
			msg.SnapshotTerm = rf.lastIncludedTerm
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			msgs := []ApplyMsg{}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.SnapshotValid = false
				msg.Command = rf.log[i-rf.lastIncludedIndex-1].Command
				msg.CommandIndex = i
				msg.CommandTerm = rf.log[i-rf.lastIncludedIndex-1].Term
				msgs = append(msgs, msg)
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
		}
	}
	close(rf.applyCh)
}

func (rf *Raft) genAppendEntriesArgs(server int, heartbeat bool) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.ID = rand.Int()
	truncLogIndex := args.PrevLogIndex - rf.lastIncludedIndex
	/*fmt.Printf("genAppendEntriesArgs: to node-%d, args.PrevLogIndex=%d, rf.lastIncludedIndex=%d\n",
	server, args.PrevLogIndex, rf.lastIncludedIndex)*/
	if truncLogIndex <= 0 {
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		/*if truncLogIndex < 0 {
			fmt.Printf("node-%d need snapshot\n", server)
		}*/
	} else {
		args.PrevLogTerm = rf.log[truncLogIndex-1].Term
	}
	// Note: args.Entries can't shared the underline elements with log.
	if !heartbeat && truncLogIndex >= 0 {
		args.Entries = append(args.Entries, rf.log[truncLogIndex:]...)
	}
	return args
}

func (rf *Raft) genInstallSnapshotArgs() InstallSnapshotArgs {
	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.Done = true
	assert(len(rf.snapshot) != 0, "genInstallSnapshot: snapshot is empty")
	args.Data = append(args.Data, rf.snapshot...)
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludeTerm = rf.lastIncludedTerm
	args.ID = rand.Int()
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
			args := rf.genAppendEntriesArgs(i, true)
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(40) * time.Millisecond)
	}
}

func (rf *Raft) appendEntiresOrSnapshot() {
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
			lastLogIndex := rf.lastIncludedIndex + len(rf.log)
			assert(rf.nextIndex[i] > 0, "appendEntiresOrSnapshot: rf.nextIndex[?] <= 0")
			// can pass figure-8. "committing entries from previous terms."
			// send AppendEntries RPC or InstallSnapshot RPC.
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := rf.genInstallSnapshotArgs()
				reply := InstallSnapshotReply{}
				go rf.sendInstallSnapshot(i, &args, &reply)
			} else if len(rf.log) != 0 && rf.log[len(rf.log)-1].Term == rf.currentTerm &&
				lastLogIndex >= rf.nextIndex[i] && rf.nextIndex[i] > rf.lastIncludedIndex {
				args := rf.genAppendEntriesArgs(i, false)
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
		rf.mu.Unlock()
		ticker := time.NewTicker(time.Duration(120) * time.Millisecond)
		select {
		case <-rf.startCh:
			ticker.Stop()
		case <-ticker.C:
		}
		//time.Sleep(time.Duration(120) * time.Millisecond)
	}
	// release hanging go routine
	for range rf.startCh {
	}
}

func (rf *Raft) initLeader() {
	// must hold mutex
	rf.state = Leader
	if debugCommon {
		fmt.Printf("node-%d (term=%d) become leader\n", rf.me, rf.currentTerm)
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log) + 1
			rf.matchIndex[i] = 0
		}
	}
	go rf.heartbeat()
	go rf.appendEntiresOrSnapshot()
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
	index := 0
	term := 0
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		isLeader = false
	} else {
		/*fmt.Printf("Start: node-%d, logLen=%d, currentTerm=%d, command=%v\n",
		rf.me, len(rf.log), rf.currentTerm, command)*/
		rf.log = append(rf.log, Entry{rf.currentTerm, command})
		index = rf.lastIncludedIndex + len(rf.log)
		term = int(rf.currentTerm)
		go func() {
			if !rf.killed() {
				rf.startCh <- 'a'
			}
		}()
	}
	rf.mu.Unlock()

	if isLeader {
		rf.persist()
	}

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
	close(rf.startCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) genElectionTime() int {
	return 400 + (rand.Int() % 400)
}

func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.lastIncludedIndex + len(rf.log)
	args.LastLogTerm = rf.lastIncludedTerm
	if len(rf.log) != 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return args
}

func (rf *Raft) election() {
	// init.
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.persist()
	rf.votes = 1
	rf.startTime = time.Now().UnixMilli()
	rf.electionTime = rf.genElectionTime()

	// send RequestVote RPC.
	args := rf.genRequestVoteArgs()
	for i := 0; i < len(rf.peers); i++ {
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
			if debugCommon {
				fmt.Printf("node-%d timeout\n", rf.me)
			}
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sortedIndex = make([]int, len(rf.peers))

	rf.state = Follower
	rf.votes = 0
	rf.startTime = time.Now().UnixMilli()
	rf.electionTime = rf.genElectionTime()

	rf.cond = sync.NewCond(&rf.mu)
	rf.startCh = make(chan byte)

	// initialize from state persisted before a crash
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.log = []Entry{}
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	// start apply committed log to state machine
	go rf.applyToSM()

	return rf
}

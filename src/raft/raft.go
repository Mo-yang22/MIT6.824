package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	// "bytes"
	"sync"
	"sync/atomic"

	// "6.824/labgob"
	"6.824/labrpc"
	"fmt"
	"math/rand"
	"time"
)

const HeartBeatInterval = 120 * time.Millisecond
const InvalidInterval = 10000 * time.Second

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	return fmt.Sprintf("Valid is %t, Command is %v, CommandIndex is %v,",
		a.CommandValid, a.Command, a.CommandIndex)
}

type LogEntry struct {
	Command interface{}
	// term的初始值为1
	Term int
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("Command is %v, Term is %v", entry.Command, entry.Term)
}

const (
	FollowerState  = 1
	CandidateState = 2
	LeaderState    = 3
)

var states = [...]string{
	FollowerState:  "Follow",
	CandidateState: "Candidate",
	LeaderState:    "Leader",
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on aloo servers
	log         []*LogEntry
	currentTerm int // 初始值要设置为0
	votedFor    int //每次currentTerm变动都要初始化

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// 额外自己加的
	state          int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	cond           sync.Cond
	applyChan      chan ApplyMsg
	CanApply       bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = false
	if rf.state == LeaderState {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int
	// 下面两个用来判断candidate是不是up-to-date
	LastLogIndex int
	LastLogTerm  int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d", args.Term, args.CandidateId,
		args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // currentTerm, for candidate to update itself
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("Term: %d, VoteGranted: %t", reply.Term, reply.VoteGranted)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("{Node :%v} with term %v receive RequestVote from {Node %v} with term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	// Your code here (2A, 2B).

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FollowerState)
	}
	reply.Term = rf.currentTerm

	if rf.canVote(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// DPrintf("{Node %v} votes for {Node %v}", rf.me, args.CandidateId)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	} else {
		reply.VoteGranted = false
	}
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
	return ok
}

// AppendEntries RPC 的结构体,发送和接收函数
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int //term of prevLogIndex entry
	Entries      []*LogEntry
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term is %v, LeaderId is %v, PrevLogIndex is %v, PreLogTerm is %v\n, Entries is %v,LeaderCommit is %v",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// ?操,在任何情况下都要重置计时器
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	// ?忘记了此时rf的状态为leader或者candidate的情况了
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FollowerState)
	}
	if rf.state == CandidateState {
		rf.changeState(FollowerState)
	}
	if len(rf.log)-1 < args.PrevLogIndex || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("{Node %v} with term %v receive AppendEntries from {Node %v} with term %v, Return False", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}
	isDifferent := false
	j := args.PrevLogIndex + 1
	for i := range args.Entries {
		if j >= len(rf.log) || rf.log[j].Term != args.Entries[i].Term {
			isDifferent = true
			break
		}
		j++
	}
	if isDifferent {
		rf.log = rf.log[0 : args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
	}
	oldCommitIndex := rf.commitIndex
	if args.LeaderCommit > oldCommitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		if rf.commitIndex > oldCommitIndex {
			DPrintf("{Node %v} change its commitIndex from %v to %v", rf.me, oldCommitIndex, rf.commitIndex)
			rf.CanApply = true
			rf.cond.Broadcast()
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("AppendEntries RPC Args is %v", args)
	if len(args.Entries) == 0 {
		DPrintf("{Node %v} with term %v receive HeartBeat from {Node %v} with term %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	} else {
		DPrintf("{Node %v} with term %v receive AppendEntries from {Node %v} with term %v, Return True", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("{Node %v} with term %v is broadcasting AppendEntries", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			args := rf.genAppendEntriesArgs(peer)
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.currentTerm && rf.state == LeaderState {
					if reply.Term > rf.currentTerm {
						rf.changeState(FollowerState)
						rf.currentTerm = reply.Term
					} else {
						if reply.Success {
							rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
							rf.leaderIncreaseCommit()
						} else {
							rf.nextIndex[peer]--
						}
					}
				}
			}
		}(i)
	}
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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != LeaderState {
		isLeader = false
		// DPrintf("{Node %v} start failed ", rf.me)
	} else {

		l := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, &l)
		isLeader = true
		term = rf.currentTerm
		index = len(rf.log) - 1
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.broadcastAppendEntries()
		rf.heartbeatTimer.Reset(HeartBeatInterval)
		// DPrintf("{Node %v} start successed index is %v, command is %v, term is %v", rf.me, index, command, term)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != LeaderState {
				rf.changeState(CandidateState)
				rf.currentTerm++
				rf.startNewElection()
				rf.electionTimer.Reset(RandomizedElectionTimeout())
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LeaderState {
				rf.broadcastAppendEntries()
				rf.heartbeatTimer.Reset(HeartBeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) startNewElection() {
	args := rf.genRequestVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteArgs %v\n", rf.me, args)
	votes := 1
	rf.votedFor = rf.me
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == CandidateState {
					if reply.VoteGranted {
						votes++
						if votes > len(rf.peers)/2 {
							// DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.changeState(LeaderState)
							rf.broadcastAppendEntries()
							votes = 0
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, i, reply.Term, rf.currentTerm)
						rf.changeState(FollowerState)
						rf.currentTerm = reply.Term
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		// oldCommitIndex := rf.commitIndex
		for !rf.CanApply {
			rf.cond.Wait()
		}
		applyMsgSet := make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			applyMsgSet = append(applyMsgSet, applyMsg)
		}
		rf.CanApply = false
		rf.mu.Unlock()
		DPrintf("{Node %v} is applying MsgSet %v", rf.me, applyMsgSet)
		for i := range applyMsgSet {
			rf.applyChan <- applyMsgSet[i]
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		commitIndex:    0,
		lastApplied:    0,
		state:          FollowerState,
		electionTimer:  time.NewTimer(InvalidInterval),
		heartbeatTimer: time.NewTimer(InvalidInterval),
		applyChan:      applyCh,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		CanApply:       false,
	}
	rf.changeState(FollowerState)
	rf.cond = *sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, &LogEntry{})

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()
	return rf
}

// 一些功能函数

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	// 生成600ms到900ms之间的随机时间间隔
	minDuration := 400 * time.Millisecond
	maxDuration := 600 * time.Millisecond
	randomDuration := time.Duration(rand.Int63n(int64(maxDuration-minDuration)) + int64(minDuration))
	return randomDuration
}
func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	return args
}
func (rf *Raft) genAppendEntriesArgs(peer int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,

		// 从nextIndex开始
		Entries:      rf.log[rf.nextIndex[peer]:],
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// 改变状态发现不能只将state改了,还应该改变raft中其他一些属性,遂添加一个辅助函数
func (rf *Raft) changeState(target int) {
	DPrintf("{Node %v} change its state to %v", rf.me, states[target])
	if target == FollowerState {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.heartbeatTimer.Reset(InvalidInterval)
	} else if target == CandidateState {
		rf.state = CandidateState
		rf.heartbeatTimer.Reset(InvalidInterval)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	} else if target == LeaderState {
		rf.state = LeaderState
		rf.electionTimer.Reset(InvalidInterval)
		rf.heartbeatTimer.Reset(HeartBeatInterval)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
	}
}
func (rf *Raft) canVote(args *RequestVoteArgs) bool {
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return false
	}
	if rf.log[len(rf.log)-1].Term < args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && args.LastLogIndex >= len(rf.log)-1) {
		return true
	}
	return false
}

func (rf *Raft) leaderIncreaseCommit() {
	isIncrease := false
	i := len(rf.log) - 1
	oldCommitIndex := rf.commitIndex
	for ; i >= 0; i-- {
		if rf.currentTerm != rf.log[i].Term || i <= rf.commitIndex {
			break
		}
		// 自己本地肯定有
		count := 0
		for j := range rf.matchIndex {
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			isIncrease = true
			break
		}
	}
	if isIncrease {
		rf.commitIndex = i
		DPrintf("{Node %v} change its commitIndex from %v to %v", rf.me, oldCommitIndex, rf.commitIndex)
		rf.CanApply = true
		rf.cond.Broadcast()
		return
	}
}

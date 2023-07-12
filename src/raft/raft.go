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
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
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
	return fmt.Sprintf(" CMD is %v, CI is %v,", a.Command, a.CommandIndex)
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("CMD is %v, T is %v", entry.Command, entry.Term)
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
	log         []LogEntry
	currentTerm int // 初始值要设置为0
	votedFor    int //每次currentTerm变动都要初始化

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Log Compaction
	lastIncludedIndex int
	lastIncludedTerm  int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "S%d Saved State T: %d, VF: %d", rf.me, rf.currentTerm, rf.votedFor)
}
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		Debug(dError, "Error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		Debug(dPersist, "S%d Read State: T: %d, VF: %d, LII: %d, LIT: %d", rf.me, rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm)
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
				Command:      rf.log[rf.global2Local(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			applyMsgSet = append(applyMsgSet, applyMsg)
		}
		rf.CanApply = false
		rf.mu.Unlock()
		// DPrintf("{Node %v} is applying MsgSet %v", rf.me, applyMsgSet)
		Debug(dLog2, "S%d Saved Log %v", rf.me, applyMsgSet)
		// go func(applyMsgSet []ApplyMsg) {
		// 	rf.applyMu.Lock()
		// 	for i := range applyMsgSet {
		// 		rf.applyChan <- applyMsgSet[i]
		// 	}
		// 	rf.applyMu.Unlock()
		// }(applyMsgSet)
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
		dead:              0,
		currentTerm:       0,
		votedFor:          -1,
		commitIndex:       0,
		lastApplied:       0,
		state:             FollowerState,
		electionTimer:     time.NewTimer(InvalidInterval),
		heartbeatTimer:    time.NewTimer(InvalidInterval),
		applyChan:         applyCh,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		CanApply:          false,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}
	rf.cond = *sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, LogEntry{})
	rf.state = FollowerState
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	rf.heartbeatTimer.Reset(InvalidInterval)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.log[0].Term = rf.lastIncludedTerm
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()
	return rf
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
				rf.persist()
				rf.electionTimer.Reset(RandomizedElectionTimeout())
				rf.startNewElection()
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

// -----------------------------------------------------------------------------------------------
// SNAP
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastIncludedIndex >= index || index > rf.commitIndex {
		return
	}
	Debug(dSnap, "S%d Snapshot before I%d", rf.me, index)
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.log[rf.global2Local(i)])
	}
	if index == rf.getLastIndex()+1 {
		rf.lastIncludedTerm = rf.log[rf.global2Local(rf.getLastIndex())].Term
	} else {
		rf.lastIncludedTerm = rf.log[rf.global2Local(index)].Term
	}

	rf.lastIncludedIndex = index
	sLogs[0].Term = rf.lastIncludedTerm
	rf.log = sLogs

	// apply了快照就应该重置commitIndex、lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	// nextIndex应该也要更新,?考虑一个场景,follow刚从crash中恢复,log落后leader太多,则其对应的GlobalIndex已经不在log中,之后对应的要发snapshot给follower也说明了这一点
	for i := range rf.peers {
		rf.nextIndex[i] = max(rf.nextIndex[i], rf.lastIncludedIndex+1)
	}
	Debug(dSnap, "S%d after snap LII: %d, CI: %d, nextIndex is %v", rf.me, rf.lastIncludedIndex, rf.commitIndex, rf.nextIndex)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	Debug(dSnap, "S%d C%d InstallSnapshot, T%d,LII is %d, LIT is %d", rf.me, args.LeaderId, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		Debug(dSnap, "S%d InstallSnapshot Failed for(T%d > T%d)", rf.me, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.changeState(FollowerState)
	}
	if rf.state == CandidateState {
		rf.changeState(FollowerState)
	}

	reply.Term = rf.currentTerm
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		Debug(dSnap, "S%d InstallSnapshot Failed for(LII%d > T%d)", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
		return
	}
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{})
	if rf.getLastIndex() > args.LastIncludedIndex && rf.log[rf.global2Local(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		newLog = append(newLog, rf.log[rf.global2Local(args.LastIncludedIndex)+1:]...)
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	newLog[0].Term = rf.lastIncludedTerm
	rf.log = newLog
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Snapshot)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()
	Debug(dSnap, "S%d Apply Snapshot, LII is %d", rf.me, msg.SnapshotIndex)
	rf.applyChan <- msg
}

func (rf *Raft) leaderSendSnapshot(peer int) {
	rf.mu.Lock()
	Debug(dSnap, "S%d is Sending Snapshot to S%d", rf.me, peer)
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	if rf.sendSnapshot(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term == rf.currentTerm && rf.state == LeaderState {
			if reply.Term > rf.currentTerm {
				Debug(dSnap, "S%d Bad Snapshot and T change(%d -> %d)", rf.me, rf.currentTerm, reply.Term)
				rf.changeState(FollowerState)
				rf.electionTimer.Reset(RandomizedElectionTimeout())
				rf.currentTerm = reply.Term
				rf.persist()
			} else {
				// 为什么要max呢,我这么设计的原因是Snapshot RPC并不会返回false,不知道到底成功了没
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
				rf.nextIndex[peer] = max(args.LastIncludedIndex+1, rf.nextIndex[peer])
				Debug(dSnap, "S%d <- S%d OK Snapshot MI: %d, NI: %d", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
				// rf.leaderIncreaseCommit()
			}
		}
	}

}

//-------------------------------------------------------------------------
// VOTE

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
	return fmt.Sprintf("T: %d, CId: %d, LLI: %d, LLT: %d", args.Term, args.CandidateId,
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
	Debug(dVote, "S%d C%d asking for vote, T%d", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d Refusing Vote to C%d for T(%d > %d)", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d T Updated(%d -> %d)", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.persist()
		if rf.state == LeaderState {
			rf.electionTimer.Reset(RandomizedElectionTimeout())
		}
		rf.changeState(FollowerState)
	}
	reply.Term = rf.currentTerm

	if rf.canVote(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		// DPrintf("{Node %v} votes for {Node %v}", rf.me, args.CandidateId)
		Debug(dVote, "S%d Grangting Vote to C%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	} else {
		Debug(dVote, "S%d Refusing Vote to C%d", rf.me, args.CandidateId)
		reply.VoteGranted = false
	}
}
func (rf *Raft) startNewElection() {
	args := rf.genRequestVoteArgs()
	Debug(dVote, "S%d starts election at T%d", rf.me, rf.currentTerm)
	votes := 1
	rf.votedFor = rf.me
	rf.persist()
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
						Debug(dVote, "S%d <- S%d Got Vote", rf.me, i)
						votes++
						if votes > len(rf.peers)/2 {
							// DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							Debug(dLeader, "S%d Achieved Majority for T%d (%d), converting to Leader",
								rf.me, rf.currentTerm, votes)
							rf.changeState(LeaderState)
							rf.broadcastAppendEntries()
							votes = 0
						}
					} else {
						Debug(dVote, "S%d <- Not Got Vote", rf.me, i)
						if reply.Term > rf.currentTerm {
							// DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, i, reply.Term, rf.currentTerm)
							Debug(dLeader, "S%d with T%d finds a new leader S%d with T%d, converting to Follower",
								rf.me, rf.currentTerm, i, reply.Term)
							rf.changeState(FollowerState)
							rf.electionTimer.Reset(RandomizedElectionTimeout())
							rf.currentTerm = reply.Term
							rf.persist()
						}
					}
				}
			}
		}(i)
	}
}

// --------------------------------------------------------------------------------------
// APPENDENTRIES

// AppendEntries RPC 的结构体,发送和接收函数
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int //term of prevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term is %v, LeaderId is %v, PrevLogIndex is %v, PreLogTerm is %v,LeaderCommit is %v",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 直接返回
	if args.Term < rf.currentTerm {
		Debug(dTimer, "S%d  At T%d Received AppEnt From S%d At T%d, Return False",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	Debug(dTimer, "S%d <- C%d args is %v", rf.me, args.LeaderId, args)
	// ?在任何情况下都要重置计时器
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	// ?忘记了此时rf的状态为leader或者candidate的情况了
	// 状态转化
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.changeState(FollowerState)
	}
	if rf.state == CandidateState {
		rf.changeState(FollowerState)
	}

	reply.Term = rf.currentTerm

	// Success为false的情况
	if rf.getLastIndex() < args.PrevLogIndex || args.PrevLogIndex >= rf.lastIncludedIndex && (rf.log[rf.global2Local(args.PrevLogIndex)].Term != args.PrevLogTerm) {
		reply.Success = false
		Debug(dTimer, "S%d  At T%d Received AppEnt From S%d At T%d, Return False",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		// 进行快速恢复
		if rf.getLastIndex() < args.PrevLogIndex {
			reply.Xterm = -1
			reply.XIndex = -1
			reply.XLen = (args.PrevLogIndex - rf.getLastIndex())
		} else {
			reply.Xterm = rf.log[rf.global2Local(args.PrevLogIndex)].Term
			XIndex := args.PrevLogIndex
			for XIndex-1 >= rf.lastIncludedIndex && rf.log[rf.global2Local(XIndex-1)].Term == rf.log[rf.global2Local(args.PrevLogIndex)].Term {
				XIndex--
			}
			reply.XIndex = XIndex
			reply.XLen = -1
		}

		return
	}
	// 进行日志的截断
	rf.trimLog(args)

	// 更新commitIndex
	oldCommitIndex := rf.commitIndex
	if args.LeaderCommit > oldCommitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		if rf.commitIndex > oldCommitIndex {
			Debug(dCommit, "S%d change commitIndex from %d to %d", rf.me, oldCommitIndex, rf.commitIndex)
			rf.CanApply = true
			rf.cond.Broadcast()
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d  At T%d Received HearBeat From S%d At T%d",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
	} else {
		Debug(dTimer, "S%d  At T%d Received AppEnt From S%d At T%d, Return True",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	Debug(dTimer, "S%d Leader, heartbeat at T%d", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := rf.genAppendEntriesArgs(i)
		nextTerm := 0
		if args.PrevLogIndex > rf.getLastIndex() {
			nextTerm = rf.log[rf.global2Local(args.PrevLogIndex+1)].Term
		}
		Debug(dError, "S%d args.LC is %d", rf.me, args.LeaderCommit)
		go func(args AppendEntriesArgs, peer int, nextTerm int) {
			rf.mu.Lock()
			Debug(dLog, "S%d -> S%d Sending PLI: %d, PLT: %d, LC: %d LogLen: %d at T%d",
				rf.me, peer, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries), args.Term)
			reply := AppendEntriesReply{}
			// 先考虑快照
			if rf.nextIndex[peer]-1 < rf.lastIncludedIndex {
				go rf.leaderSendSnapshot(peer)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if rf.sendAppendEntries(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.currentTerm && rf.state == LeaderState {
					if reply.Term > rf.currentTerm {
						Debug(dLog, "S%d <- S%d Bad Append and T Change(%d -> %d)",
							rf.me, peer, rf.currentTerm, reply.Term)
						rf.changeState(FollowerState)
						rf.electionTimer.Reset(RandomizedElectionTimeout())
						rf.currentTerm = reply.Term
						rf.persist()
					} else {
						if reply.Success {
							rf.matchIndex[peer] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])
							rf.nextIndex[peer] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[peer])
							Debug(dLog, "S%d <- S%d OK Append MI: %d, NI: %d", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
							rf.leaderIncreaseCommit()
						} else {
							// 情况三
							nextIndex := args.PrevLogIndex + 1
							if reply.XLen != -1 {
								rf.nextIndex[peer] = min(nextIndex-reply.XLen, rf.nextIndex[peer])
							} else {

								// 先得到本地的log上一个不同的term
								lastIndex := nextIndex - 1
								for lastIndex > 0 && rf.log[rf.global2Local(lastIndex)].Term == nextTerm {
									lastIndex--
								}
								// 情况一
								if rf.log[rf.global2Local(lastIndex)].Term != reply.Xterm {
									rf.nextIndex[peer] = min(reply.XIndex, rf.nextIndex[peer])
								} else {
									//情况二
									rf.nextIndex[peer] = min(lastIndex+1, rf.nextIndex[peer])
								}
							}
							Debug(dLog, "S%d <- S%d Bad Append MI: %d, NI: %d", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
						}
					}
				}
			}
		}(args, i, nextTerm)
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
		rf.log = append(rf.log, l)
		rf.persist()
		isLeader = true
		term = rf.currentTerm
		index = rf.getLastIndex()
		rf.nextIndex[rf.me] = rf.getLastIndex() + 1
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.broadcastAppendEntries()
		rf.heartbeatTimer.Reset(HeartBeatInterval)
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

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	// 生成600ms到900ms之间的随机时间间隔
	minDuration := 400 * time.Millisecond
	maxDuration := 600 * time.Millisecond
	randomDuration := time.Duration(rand.Int63n(int64(maxDuration-minDuration)) + int64(minDuration))
	return randomDuration
}

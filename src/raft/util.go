package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Persistent

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// const debug = 1

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// send函数
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 获取最大的GlobalIndex
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

// 给定GlobalIndex,得到LocalIndex
func (rf *Raft) global2Local(curIndex int) int {
	return curIndex - rf.lastIncludedIndex
}

func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}
func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) trimLog(args *AppendEntriesArgs) {
	// i表示本地的索引,j表示entries中的索引
	i := args.PrevLogIndex + 1
	j := 0
	for ; j < len(args.Entries); j++ {
		if (i - rf.lastIncludedIndex) < 1 {
			i++
			continue
		}
		if i > rf.getLastIndex() {
			rf.log = rf.log[:rf.global2Local(i)]
			rf.log = append(rf.log, args.Entries[j:]...)
			break
		}
		if rf.log[rf.global2Local(i)].Term == args.Entries[j].Term {
			i++
		} else if rf.log[rf.global2Local(i)].Term != args.Entries[j].Term {
			rf.log = rf.log[:rf.global2Local(i)]
			rf.log = append(rf.log, args.Entries[j:]...)
			break
		}
	}
	rf.persist()
}
func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.log[rf.global2Local(rf.getLastIndex())].Term
	return args
}
func (rf *Raft) genAppendEntriesArgs(peer int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	newEntryBeginIndex := rf.nextIndex[peer] - 1
	newEntryBeginIndex = max(newEntryBeginIndex, rf.lastIncludedIndex)
	newEntryBeginIndex = min(newEntryBeginIndex, rf.getLastIndex())
	args.PrevLogIndex = newEntryBeginIndex
	args.PrevLogTerm = rf.log[rf.global2Local(newEntryBeginIndex)].Term
	entries := make([]LogEntry, 0)
	entries = append(entries, rf.log[rf.global2Local(newEntryBeginIndex)+1:]...)
	args.Entries = entries
	return args
}

// 改变状态发现不能只将state改了,还应该改变raft中其他一些属性,遂添加一个辅助函数
func (rf *Raft) changeState(target int) {
	// DPrintf("{Node %v} change its state to %v", rf.me, states[target])
	Debug(dInfo, "S%d change state(%v -> %v)", rf.me, states[rf.state], states[target])
	if target == FollowerState {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.persist()
		rf.heartbeatTimer.Reset(InvalidInterval)
	} else if target == CandidateState {
		rf.state = CandidateState
		rf.heartbeatTimer.Reset(InvalidInterval)
	} else if target == LeaderState {
		rf.state = LeaderState
		rf.electionTimer.Reset(InvalidInterval)
		rf.heartbeatTimer.Reset(HeartBeatInterval)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastIndex() + 1
			rf.matchIndex[i] = 0
		}
	}
}
func (rf *Raft) canVote(args *RequestVoteArgs) bool {
	Debug(dVote, "S%d args is %v,my LLI is %d LLT is %d", rf.me, args, rf.getLastIndex(), rf.log[len(rf.log)-1].Term)
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return false
	}
	if rf.log[rf.global2Local(rf.getLastIndex())].Term < args.LastLogTerm || (rf.log[rf.global2Local(rf.getLastIndex())].Term == args.LastLogTerm && args.LastLogIndex >= rf.getLastIndex()) {
		return true
	}
	return false
}

func (rf *Raft) leaderIncreaseCommit() {
	isIncrease := false
	i := rf.getLastIndex()
	oldCommitIndex := rf.commitIndex
	for ; i >= 0; i-- {
		if rf.currentTerm != rf.log[rf.global2Local(i)].Term || i <= rf.commitIndex {
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
		Debug(dCommit, "S%d change commitIndex from %d to %d", rf.me, oldCommitIndex, rf.commitIndex)
		rf.CanApply = true
		rf.cond.Broadcast()
		return
	}
}

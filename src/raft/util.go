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

// 获取最大的GlobalIndex
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

// // 给定GlobalIndex,得到LogEntry
//
//	func (rf *Raft) restoreLog(curIndex int) LogEntry {
//		return rf.log[curIndex-rf.lastIncludedIndex]
//	}
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
	// isDifferent := false
	// j := args.PrevLogIndex + 1
	// for i := range args.Entries {
	// 	if j >= rf.getLastIndex()+1 || j > rf.lastIncludedIndex && rf.log[rf.global2Local(j)].Term != args.Entries[i].Term {
	// 		isDifferent = true
	// 		break
	// 	}
	// 	j++
	// }
	// if isDifferent {
	// 	rf.log = rf.log[0:rf.global2Local(args.PrevLogIndex+1)]
	// 	rf.log = append(rf.log, args.Entries...)
	// 	rf.persist()
	// }
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

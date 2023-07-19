package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

func (a PutAppendReply) String() string {
	return fmt.Sprintf("Err is %v, LeaderId is %d", a.Err, a.LeaderId)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

func (a GetReply) String() string {
	return fmt.Sprintf("Err is %v, value is %v, LeaderId is %d", a.Err, a.Value, a.LeaderId)
}

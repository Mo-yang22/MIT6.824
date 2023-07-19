package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Command   string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	replyChMap   map[int]chan ApplyNotifyMsg
	finishSet    map[int64]CommandContext
	kvs          map[string]string
}

// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	Term  int
}

type CommandContext struct {
	Command int            //该client的目前的commandId
	Reply   ApplyNotifyMsg //该command的响应
}

// 读请求
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// kv.mu.Lock()
	defer func() {
		DPrintf("kvserver[%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	}()
	DPrintf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)
	op := Op{
		Command:   "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	// kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver[%d]: 拒绝Get RPC请求,args=[%v] 因为Err\n", kv.me, args)
		return
	}
	// kv.mu.Lock()
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	// kv.mu.Unlock()
	select {
	case msg := <-replyCh:
		if msg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			reply.LeaderId = kv.me
			reply.Value = msg.Value
		}
	case <-time.After(time.Millisecond * 500):
		reply.Err = ErrTimeout
	}
	// kv.mu.Lock()
	delete(kv.replyChMap, index)
	// kv.mu.Unlock()
}

// 写请求
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer func() {
		DPrintf("kvserver[%d]: 返回PutAppend RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	}()
	DPrintf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args)
	op := Op{
		Command:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case msg := <-replyCh:
		if msg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			reply.LeaderId = kv.me
		}
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.replyChMap, index)
	kv.mu.Unlock()
}
func (kv *KVServer) Apply(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op, ok := applyMsg.Command.(Op)
	if !ok {
		DPrintf("类型转换出现了问题")
		return
	}
	commandIndex := applyMsg.CommandIndex
	commandTerm := applyMsg.CommandTerm
	applyNotifyMsg := ApplyNotifyMsg{}
	if op.Command == "Get" {
		if value, ok := kv.kvs[op.Key]; ok {
			applyNotifyMsg.Err = OK
			applyNotifyMsg.Term = commandTerm
			applyNotifyMsg.Value = value
		} else {
			applyNotifyMsg.Err = ErrNoKey
			applyNotifyMsg.Term = commandTerm
			applyNotifyMsg.Value = ""
		}
	} else if op.Command == "Put" {
		kv.kvs[op.Key] = op.Value
		applyNotifyMsg.Err = OK
		applyNotifyMsg.Term = commandTerm
	} else {
		kv.kvs[op.Key] = kv.kvs[op.Key] + op.Value
		applyNotifyMsg.Err = OK
		applyNotifyMsg.Term = commandTerm
	}
	kv.replyChMap[commandIndex] <- applyNotifyMsg
}
func (kv *KVServer) listener() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
			if applyMsg.CommandValid {
				kv.Apply(applyMsg)
			}
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.finishSet = make(map[int64]CommandContext)
	kv.kvs = make(map[string]string)
	go kv.listener()
	// You may need initialization code here.

	return kv
}

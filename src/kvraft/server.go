package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
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
	RequestId int
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
	lastApplied  int
}

// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	Term  int
}

type CommandContext struct {
	RequestId int            //该client的目前的commandId
	Reply     ApplyNotifyMsg //该command的响应
}

// 读请求
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// defer func() {
	// 	DPrintf("kvserver[%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	// }()
	// DPrintf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)
	if context, ok := kv.finishSet[args.ClientId]; ok {
		if context.RequestId == args.RequestId {
			reply.Err = context.Reply.Err
			reply.Value = context.Reply.Value
			reply.LeaderId = kv.me
			kv.mu.Unlock()
			return
		}
	}
	op := Op{
		Command:   "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case msg := <-replyCh:
		if msg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = msg.Err
			reply.LeaderId = kv.me
			reply.Value = msg.Value
		}
	case <-time.After(time.Millisecond * 600):
		reply.Err = ErrTimeout
	}
	go kv.closeChan(index)
}

// 写请求
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// defer func() {
	// 	DPrintf("kvserver[%d]: 返回PutAppend RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	// }()
	// DPrintf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args)
	if context, ok := kv.finishSet[args.ClientId]; ok {
		if context.RequestId == args.RequestId {
			reply.Err = context.Reply.Err
			kv.mu.Unlock()
			return
		}
	}
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
			reply.Err = msg.Err
			reply.LeaderId = kv.me
		}
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
	go kv.closeChan(index)
}

func (kv *KVServer) ApplyLog(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applyMsg.Command.(Op)

	commandIndex := applyMsg.CommandIndex
	commandTerm := applyMsg.CommandTerm
	//当命令已经被应用过了
	if commandContext, ok := kv.finishSet[op.ClientId]; ok && commandContext.RequestId >= op.RequestId {
		DPrintf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.me, applyMsg, commandContext)
		return
	}
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
	// 一定要判断是不是ok,应该接收方可能不存在了,去做超时的逻辑了
	if replyCh, ok := kv.replyChMap[commandIndex]; ok {
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.me, applyMsg, commandIndex)
		replyCh <- applyNotifyMsg
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.me, applyMsg, commandIndex)
	}
	// 更新finishSet
	kv.finishSet[op.ClientId] = CommandContext{op.RequestId, applyNotifyMsg}
	if commandIndex > kv.lastApplied {
		kv.lastApplied = applyMsg.CommandIndex
		if kv.needSnapshot() {
			kv.startSnapshot(commandIndex)
		}
	}
}
func (kv *KVServer) ApplySnapshot(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kvserver[%d]: 接收到leader的快照\n", kv.me)
	if applyMsg.SnapshotIndex < kv.lastApplied {
		DPrintf("kvserver[%d]: 接收到旧的日志,snapshotIndex=[%d],状态机的lastApplied=[%d]\n", kv.me, applyMsg.SnapshotIndex, kv.lastApplied)
		return
	}
	kv.lastApplied = applyMsg.SnapshotIndex
	//将快照中的service层数据进行加载
	kv.readSnapshot(applyMsg.Snapshot)
	DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)

}
func (kv *KVServer) listener() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
			if applyMsg.CommandValid {
				kv.ApplyLog(applyMsg)
			} else if applyMsg.SnapshotValid {
				kv.ApplySnapshot(applyMsg)
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
	kv.lastApplied = 0
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.listener()
	// You may need initialization code here.

	return kv
}
func (kv *KVServer) closeChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.replyChMap[index]
	if !ok {
		//若该index没有保存通道,直接结束
		// DPrintf("kvserver[%d]: 无该通道index: %d\n", kv.me, index)
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
	// DPrintf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
}
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	proportion := float32(kv.rf.ReadRaftSize() / kv.maxraftstate)
	return proportion > 0.9
}
func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	//编码kv数据
	err := e.Encode(kv.kvs)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode kvData error: %v\n", kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.finishSet)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode clientReply error: %v\n", kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
}

// 主动开始snapshot(由leader在maxRaftState不为-1,而且目前接近阈值的时候调用)
func (kv *KVServer) startSnapshot(index int) {
	DPrintf("kvserver[%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.me, kv.rf.ReadRaftSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvs map[string]string
	var finishSet map[int64]CommandContext
	if d.Decode(&kvs) != nil || d.Decode(&finishSet) != nil {
		DPrintf("kvserver[%d]: decode error\n", kv.me)
	} else {
		kv.kvs = kvs
		kv.finishSet = finishSet
	}
}

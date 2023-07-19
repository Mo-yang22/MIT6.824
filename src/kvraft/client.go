package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderHint int
	clientId   int64
	requestId  int64
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderHint = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	serverIndex := ck.leaderHint
	DPrintf("C%d: 开始发送Get RPC;args=[%v]\n", ck.clientId, args)
	for ; ; serverIndex = (serverIndex + 1) % len(ck.servers) {
		reply := GetReply{}
		DPrintf("C%d: 开始发送Get RPC;args=[%v]到S%d\n", ck.clientId, args, serverIndex)
		ok := ck.servers[serverIndex].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			DPrintf("C%d: 发送Get RPC;args=[%v]到S%d失败,ok = %v,Reply=[%v]\n", ck.clientId, args, serverIndex, ok, reply)
			continue
		}
		DPrintf("C%d: 发送Get RPC;args=[%v]到S%d成功,ok = %v,Reply=[%v]\n", ck.clientId, args, serverIndex, ok, reply)
		ck.leaderHint = serverIndex
		ck.requestId++
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
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
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	serverIndex := ck.leaderHint
	DPrintf("C%d: 开始发送PutAppend RPC;args=[%v]\n", ck.clientId, args)
	for ; ; serverIndex = (serverIndex + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		DPrintf("C%d: 开始发送PutAppend RPC;args=[%v]到S%d\n", ck.clientId, args, serverIndex)
		ok := ck.servers[serverIndex].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			DPrintf("C%d: 发送PutAppend RPC;args=[%v]到S%d失败,ok = %v,Reply=[%v]\n", ck.clientId, args, serverIndex, ok, reply)
			continue
		}
		DPrintf("C%d: 发送Get RPC;args=[%v]到S%d成功,ok = %v,Reply=[%v]\n", ck.clientId, args, serverIndex, ok, reply)
		ck.leaderHint = serverIndex
		ck.requestId++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("C%d trying to Put [%v, %v] ", ck.clientId, key, value)
	ck.PutAppend(key, value, "Put")
	DPrintf("C%d finish to Put [%v, %v] ", ck.clientId, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("C%d trying to Append [%v, %v] ", ck.clientId, key, value)
	ck.PutAppend(key, value, "Append")
	DPrintf("C%d finish to Append [%v, %v] ", ck.clientId, key, value)
}

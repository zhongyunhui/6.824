package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd

	mu sync.Mutex
	leaderId int
	clientId int64
	seqId int64
	// You will have to modify this struct.
}

func (ck *Clerk) lock() {
	ck.mu.Lock()
}

func (ck *Clerk) unLock() {
	ck.mu.Unlock()
}



func (ck *Clerk) getLeader() int {
	ck.lock()
	defer ck.unLock()
	return ck.leaderId
}

func (ck *Clerk) changeLeader() int {
	ck.lock()
	defer ck.unLock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
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
	ck.clientId = nrand()
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
		ClientId: ck.clientId,
	}
	DPrintf("client[%d]发送的Get   RPC序列为[%d]", ck.clientId, ck.seqId)

	leaderId := ck.getLeader()
	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case ErrNoKey:
				DPrintf("client[%d]发送的Get   RPC序列为[%d],收到server[%d]的reply为[%v]", ck.clientId, ck.seqId, ck.leaderId, ErrNoKey)
				return ""
			case OK:
				DPrintf("client[%d]发送的Get   RPC序列为[%d],收到server[%d]的reply为[%v]", ck.clientId, ck.seqId,  ck.leaderId,OK)
				return reply.Value
			case ErrWrongLeader:
				DPrintf("client[%d]发送的Get   RPC序列为[%d],收到server[%d]的reply为[%v]", ck.clientId, ck.seqId,  ck.leaderId,ErrWrongLeader)
				leaderId = ck.changeLeader()
				time.Sleep(1 * time.Millisecond)
			}
		} else {
			DPrintf("client[%d]发送的Get   RPC序列为[%d],收到server[%d]的reply超时", ck.clientId, ck.seqId,  ck.leaderId)
			leaderId = ck.changeLeader()
			time.Sleep(1 * time.Millisecond)
		}

	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
		ClientId: ck.clientId,
	}

	leaderId := ck.getLeader()
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {

			case ErrWrongLeader:
				DPrintf("client[%d]发送的[%v]   RPC序列为[%d],收到server[%d]的reply为[%v]", ck.clientId, op, ck.seqId,  ck.leaderId,ErrWrongLeader)
				leaderId = ck.changeLeader()
				time.Sleep(1 * time.Millisecond)
			case OK:
				DPrintf("client[%d]发送的[%v]   RPC序列为[%d],收到server[%d]的reply为[%v]", ck.clientId, op, ck.seqId,  ck.leaderId,OK)
				return
			}
		} else {
			leaderId = ck.changeLeader()
			time.Sleep(1 * time.Millisecond)
		}

	}


	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}


func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

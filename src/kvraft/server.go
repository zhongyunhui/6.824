package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", uint8(91), s)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetPrefix(red("lab3-----"))
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unLock() {
	kv.mu.Unlock()
}

type Op struct {
	Key  string
	Value string
	TypeOp int
	ClientId int64
	SeqId	int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type RPCStruct struct {
	op Op
	committed chan bool
	escaped bool
	wrongLeader bool
	value string
	existsKey bool
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	datas  map[string]string

	rpcRequests map[int]*RPCStruct
	seqMap map[int64]int64
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:      args.Key,
		TypeOp:   GET,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}


	index, _, isLeader := kv.rf.Start(op)

	DPrintf("server[%d]接收到来自client[%d]的get RPC request，seqId为[%d], 请求的key为[%v]", kv.me, args.ClientId, args.SeqId, args.Key)
	if !isLeader {
		DPrintf("server[%d]不是leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	getRPC := RPCStruct{
		op:          op,
		committed:   make(chan bool),
		escaped:     false,
		wrongLeader: false,
	}

	func(){
		kv.lock()
		defer kv.unLock()
		kv.rpcRequests[index] = &getRPC
	}()

	defer func() {
		kv.lock()
		defer kv.unLock()
		if request, existsRPC := kv.rpcRequests[index]; existsRPC {
			if &getRPC == request {
				delete(kv.rpcRequests, index)
			}
		}
	}()
	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	DPrintf("等待序列[%v]................", op.SeqId)
	select {
	case <- getRPC.committed:
		if getRPC.wrongLeader {
			reply.Err = ErrWrongLeader
		} else if !getRPC.existsKey {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = getRPC.value
		}
	case <- timer.C:
		reply.Err = ErrWrongLeader
	}

	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op {
		Key: args.Key,
		Value: args.Value,
		SeqId: args.SeqId,
		ClientId: args.ClientId,
	}
	if args.Op == "Put" {
		op.TypeOp = PUT
	}
	if args.Op == "Append" {
		op.TypeOp = APPEND
	}

	index, _, isLeader := kv.rf.Start(op)
	DPrintf("server[%d]接收到来自client[%d]的[%v] RPC request，seqId为[%d], 请求的key为[%v]", kv.me, args.ClientId,args.Op, args.SeqId, args.Key)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	putAppendRPC := RPCStruct{
		op:          op,
		committed:   make(chan bool),
		escaped:     false,
		wrongLeader: false,
		value:       args.Value,
		existsKey:   false,
	}

	func(){
		kv.lock()
		defer kv.unLock()
		kv.rpcRequests[index] = &putAppendRPC
	}()

	defer func(){
		kv.lock()
		defer kv.unLock()
		if request, existsRPC := kv.rpcRequests[index]; existsRPC {
			if &putAppendRPC == request {
				delete(kv.rpcRequests, index)
			}
		}
	}()
	DPrintf("等待序列[%v]................", op.SeqId)
	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case finish := <- putAppendRPC.committed:
		if finish {
			DPrintf("putAppendRPC【%v】", putAppendRPC)
			if putAppendRPC.wrongLeader {
				DPrintf("wrongLeader")
				reply.Err = ErrWrongLeader
			} else {
				DPrintf("提交[%v]成功", args.Op)
				reply.Err = OK
			}
		}
	case <- timer.C:
		reply.Err = ErrWrongLeader
	}

	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.datas = make(map[string]string)
	kv.seqMap = make(map[int64]int64)
	kv.rpcRequests = make(map[int]*RPCStruct)
	//// You may need initialization code here.
	//
	applyCh := make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, applyCh)
	DPrintf("启动kvserver[%d]", kv.me)
	go kv.applier(applyCh)
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) applier(applyCh chan raft.ApplyMsg) {
	for !kv.killed() {
		for m := range applyCh {

			if m.SnapshotValid {
				// do some snapshot
			} else if m.CommandValid {
				NewMonitor()
				op := m.Command.(Op)
				DPrintf("server[%d]已经提交了该命令，op[%v]", kv.me, op)
				index := m.CommandIndex
				func() {
					kv.lock()
					defer kv.unLock()
					rpcRequest, existsRPC := kv.rpcRequests[index]
					prevseqId, existsSeq := kv.seqMap[op.ClientId]

					if existsRPC {
						if rpcRequest.op != op {
							DPrintf("server[%d]已经提交了Seq序列[%d]命令[%v]，但是他的状态已经被改变，在提交前已经不是leader", kv.me, op.SeqId, op)
							rpcRequest.wrongLeader = true
						}
					}


					if !existsSeq || prevseqId < op.SeqId {
						switch op.TypeOp {
						case PUT:
							DPrintf("server[%d]已经提交了Seq序列[%d]Put命令[%v]", kv.me, op.SeqId,op)
							kv.datas[op.Key] = op.Value
						case APPEND:
							if value, existsKey := kv.datas[op.Key]; existsKey {
								kv.datas[op.Key] = value + op.Value
							} else {
								kv.datas[op.Key] = op.Value
							}
							DPrintf("server[%d]已经提交了Seq序列[%d]Put命令[%v]", kv.me, op.SeqId,op)
						}
					} else if existsRPC {
						DPrintf("server[%d]已经完成了这次RPC，跳过此次RPC过程Seq序列[%d]，命令[%v]", kv.me, op.SeqId,op)
						rpcRequest.escaped = true
					}
					if op.TypeOp == GET && existsRPC{
						if value, existsKey := kv.datas[op.Key]; existsKey {
							rpcRequest.value = value
							rpcRequest.existsKey = true
							DPrintf("server[%d]已经提交了Seq序列[%d]Get命令[%v]，得到的值为[%v]", kv.me, op.SeqId,op, value)

						} else {
							rpcRequest.existsKey = false
							DPrintf("server[%d]已经提交了Seq序列[%d]Get命令[%v]，但是没有key[%v]", kv.me, op.SeqId,op, op.Key)
						}
					}


					kv.seqMap[op.ClientId] = op.SeqId
					if existsRPC {
						rpcRequest.committed <- true
						close(rpcRequest.committed)
					}
				}()
			}
		}
	}
}
package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) startElection() {
	for rf.killed() == false {
		//DPrintf("节点%d有没有断开连接%v", rf.me, rf.killed())
		time.Sleep(1 * time.Millisecond)
		if !rf.checkState(CandidateState) {
			return
		}

		timeout := time.Duration(400+rand.Int31n(150))*time.Millisecond
		if time.Since(rf.getTimerReset()) >= timeout {
			rf.lock()
			DPrintf("candidate[%d]一轮选举结束,term为[%d]", rf.me, rf.currentTerm)
			rf.unLock()
			var args = RequestVoteArgs{}
			func() {
				rf.lock()
				defer rf.unLock()
				rf.TimerReset = time.Now()
				rf.currentTerm += 1
				rf.votedCount = 1
				rf.persist()
				args = RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: len(rf.logEntries),
				}
				if args.LastLogIndex != 0 {
					args.LastLogTerm = rf.logEntries[args.LastLogIndex-1].Term
				}
			}()

			for k, _ := range rf.peers {
				if k == rf.me {
					continue
				}
				reply := RequestVoteReply{}

				go rf.sendRequestVote(k, &args, &reply)
			}
		}
		// 改写到这里，什么时候return出函数
	}
}


//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Candidate[%d]向节点[%d]获取requestVote请求[%v]，请求args为[%v]，响应reply为[%v]", rf.me, server, ok, args, reply)
	if ok {
		func(){
			rf.lock()
			defer rf.unLock()
			if rf.myState != CandidateState || rf.currentTerm != args.Term {
				DPrintf("Candidate[%d]的状态或term已经改变", rf.me)
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("Candidate[%d]的term[%d]小于reply的term[%d]，从candidate变成了follower", rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.myState = FollowerState
				rf.votedFor = -1
				rf.persist()
				return
			}
			if reply.VoteGranted {
				rf.votedCount += 1
				peersLen := len(rf.peers)
				if rf.votedCount > peersLen/2 {
					rf.leaderInitialize()
				}
			}
		}()
	}
	return ok
}
package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) startElection() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond)
		if !rf.checkState(CandidateState) {
			return
		}

		timeout := time.Duration(300+rand.Int31n(150))*time.Millisecond

		if time.Since(rf.getTimerReset()) >= timeout {


			var args = RequestVoteArgs{}
			func() {
				rf.lock()
				defer rf.unLock()
				rf.timerReset = time.Now()
				rf.currentTerm += 1
				rf.votedCount = 1
				rf.persist()
				DPrintf("candidate[%d]一轮选举结束,term为[%d]", rf.me, rf.currentTerm)
				args = RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: rf.lastIncludedIndex+len(rf.logEntries),
				}
				if len(rf.logEntries) > 0 {
					_, _, logEntry := rf.getLogEntry(args.LastLogIndex)
					args.LastLogTerm = logEntry.Term
				} else {
					args.LastLogTerm = rf.lastIncludedTerm
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
	DPrintf("Candidate[%d] sendRequestVote to server[%d], args[%v], reply[%v]", rf.me, server, args, reply)
	if ok {
		func(){
			rf.lock()
			defer rf.unLock()
			if rf.myState != CandidateState || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
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
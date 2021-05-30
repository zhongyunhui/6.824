package raft

import (
	"time"
)

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unLock() {
	rf.mu.Unlock()
}

func (rf *Raft) checkState(state int) bool {
	rf.lock()
	defer rf.unLock()
	if rf.myState != state {
		//DPrintf("2A   节点%d的state从%d变成了%d", rf.me, state, rf.myState)
		rf.timerReset = true
	}
	if rf.timerReset {
		return true
	}
	return false
}

func (rf *Raft) checkTimerReset(state int) bool {
	//now := time.Now()
	rf.electionTimeout = RandInt(400, 500)
	rf.lock()
	rf.timerReset = false
	rf.unLock()
	checkSum := float64(300)
	checkTime := float64(rf.electionTimeout) / checkSum

	for i := 0; i < 300; i++ {
		if rf.checkState(state) {
			return true
		}
		time.Sleep(time.Duration(checkTime*1000) * time.Microsecond)
	}
	//DPrintf("2A   节点%d的timerout为%v", me, time.Since(now))
	if rf.checkState(state) {
		return true
	} else {
		return false
	}
}

func (rf *Raft) leaderInitialize() {
	rf.nextIndex = make([]int, len(rf.peers))
	for _, v := range rf.nextIndex {
		v = 
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.lock()
		//DPrintf("2A   节点%d进入ticker循环", rf.me)
		switch rf.myState {
		case FollowerState:
			rf.unLock()
			rf.checkFollower()
			break
		case CandidateState:
			rf.unLock()
			rf.startElection()
			break
		case LeaderState:
			// 发送心跳
			rf.unLock()
			//DPrintf("2A   节点%d变成leader\n", rf.me)
			rf.sendHeartBeats()
			break
		default:
			panic("不合法的state")
		}
	}
	DPrintf("跳出循环")
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
}

func (rf *Raft) toCandidate() {
	rf.lock()
	defer rf.unLock()
	if rf.votedFor == -1 || !rf.timerReset {
		rf.myState = CandidateState
		//DPrintf("2A   节点%d未收到Leader发送的消息\n", rf.me)
	}
	//DPrintf("2A   节点%d变成Candidate", rf.me)
}
func (rf *Raft) checkFollower() {
	for rf.killed() == false {
		//fmt.Println("该节点为Follower")
		//fmt.Printf("节点%dcheckFollower获得锁\n", rf.me)
		// 当election timer没有复位，而且rf没有投票的话，则将该节点变成Candidate
		//DPrintf("2A   节点%d的状态为%d", rf.me, rf.myState)
		if !rf.checkTimerReset(FollowerState) {
			rf.toCandidate()
			return
		}
	}
}

func (rf *Raft) startElection() {
	for rf.killed() == false {
		rf.sendRequestVotes()
		if rf.checkTimerReset(CandidateState) {
			return
		}
		//DPrintf("2A   candidate一轮选举结束")
	}
}

func (rf *Raft) sendHeartBeat() bool {
	rf.lock()
	if rf.myState != LeaderState {
		rf.unLock()
		return false
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		Entries:      []logEntry{},
		LeaderCommit: rf.commitIndex,
	}
	rf.unLock()
	for k, _ := range rf.peers {
		if k == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		go rf.sendEmptyEntries(k, &args, &reply)
	}
	return true
}

func (rf *Raft) sendHeartBeats() {
	for rf.killed() == false {
		if !rf.sendHeartBeat() {
			//DPrintf("2A   节点%d的状态从leader变成了%d", rf.me, rf.myState)
			return
		}
		time.Sleep(time.Duration(heartBeatTimeout) * time.Millisecond)
	}
}

func (rf *Raft) sendEmptyEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("2A   leader%d向节点%d发送心跳\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("2A  请求%v", ok)
	rf.lock()
	DPrintf("2A  节点%d向节点%d发送心跳%v", rf.me, server, reply.Success)
	if ok {
		if !reply.Success && reply.Term > rf.currentTerm {
			DPrintf("2A  leader%d的term%d<server%d的term%d", rf.me, rf.currentTerm, server, reply.Term)
			rf.currentTerm = reply.Term
			rf.myState = FollowerState
		}
	}
	rf.unLock()
	return ok
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

func (rf *Raft) sendRequestVotes() {
	rf.lock()
	if rf.myState != CandidateState {
		rf.unLock()
		return
	}
	rf.timerReset = false
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votedCount = 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.unLock()
	for k, _ := range rf.peers {
		if k == rf.me {
			continue
		}
		reply := RequestVoteReply{}

		go rf.sendRequestVote(k, &args, &reply)
		// voteGranted 使用
	}
}

func (rf *Raft) turnState(reply *RequestVoteReply) {
	rf.lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.myState = FollowerState
	}
	if reply.VoteGranted {
		rf.votedCount += 1
		peersLen := len(rf.peers)
		if rf.votedCount > peersLen/2 {
			rf.myState = LeaderState
			rf.timerReset = true
			rf.unLock()
			rf.sendHeartBeat()
			return
		}
	}
	rf.unLock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.turnState(reply)
	}
	return ok
}

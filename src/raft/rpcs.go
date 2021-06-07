package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ReplyLogIndex int
	ReplyLogTerm int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unLock()
	DPrintf("2A  Candidate节点%d的Term%d和Follower节点%d的Term%d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	DPrintf("2A  节点%d向节点%d请求投票\n", args.CandidateId, rf.me)
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//DPrintf("2A   节点%d>Candidate的term，状态为%d, term为%d", rf.me, rf.myState, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.myState = FollowerState
		rf.persist()
		//DPrintf("2A   节点%d变为Follower，状态为%d, term为%d, votedFor为%d", rf.me, rf.myState, rf.currentTerm, rf.votedFor)
	} else {
		//DPrintf("2A   两个节点term相等\n")
	}
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		// candidate's logEntries is at least as up-to-date as receiver's logEntries, grant vote
		lastLogTerm := 0
		if len(rf.logEntries) != 0 {
			lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		}

		if lastLogTerm < args.LastLogTerm  || (lastLogTerm == args.LastLogTerm && args.LastLogIndex >= len(rf.logEntries)){
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.timerReset = true
			DPrintf("2A   Candidate节点%d获得Follower节点%d的投票\n", rf.votedFor, rf.me)
			rf.persist()
			return
		}
		DPrintf("节点%d的lastLogTerm为%d，candidate%d的lastLogTerm为%d", rf.me, lastLogTerm, args.CandidateId, args.LastLogTerm)
		DPrintf("2A Candidate节点%d未获得Follower节点%d的投票", args.CandidateId, rf.me)
		reply.VoteGranted = false
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// leader 向 follower发送appendEntry的请求，args为leader
	//DPrintf("Leader  %d  向Follower  %d  发送log entry数据长度为%d\n", args.LeaderId, rf.me, len(args.Entries))
	//DPrintf("args为%d\n", args)
	//DPrintf("follower的logs为%v", rf.logEntries)
	rf.lock()
	reply.ReplyLogIndex = -1
	reply.ReplyLogTerm = -1
	//DPrintf("2A   Leader%d的term为%d\n", args.LeaderId, args.Term)
	if len(args.Entries) > 0 {
		DPrintf("节点【%d】收到来自leader【%d】的appendEntries请求", rf.me, args.LeaderId)
	} else {
		DPrintf("节点【%d】收到来自leader【%d】的heartBeat请求", rf.me, args.LeaderId)
	}
	reply.Term = rf.currentTerm
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("args.Term【%d】 < 节点【%d】的term【%d】\n", args.Term, rf.me, rf.currentTerm)
		reply.Success = false
		rf.unLock()
		return
	}
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.myState = FollowerState
		rf.votedFor = -1
		rf.persist()
	}
	reply.Success = true
	rf.timerReset = true
	DPrintf("节点%d timerReset", rf.me)
	// 不包含在pervLogIndex matches pervLogTerm
	if args.PrevLogIndex > len(rf.logEntries) {
		DPrintf("节点%d的log长度%d小于leader%d的prevLogIndex%d", rf.me, len(rf.logEntries), args.LeaderId, args.PrevLogIndex)
		reply.ReplyLogIndex = len(rf.logEntries)
		reply.Success = false
		rf.unLock()
		return
	}
	if args.PrevLogIndex > 0 && rf.logEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		//DPrintf("args的prevLogindex为%d,logLen-1为%d", args.PrevLogIndex, logLen-1)
		//DPrintf("rf的log的term为%d，args的prevLogTerm为%d\n", rf.logEntries[args.PrevLogIndex].Term, args.PrevLogTerm)
		DPrintf("节点%d不包含在prevLogIndex matches prevLogTerm", rf.me)
		reply.ReplyLogTerm = rf.logEntries[args.PrevLogIndex - 1].Term
		for i := 1; i <= args.PrevLogIndex; i++ {
			if rf.logEntries[i-1].Term == reply.ReplyLogTerm {
				reply.ReplyLogIndex = i
				break
			}
		}
		reply.Success = false
		rf.unLock()
		return
	}
	// 将candidate转换为followerstate

	// entry和新的entry存在冲突，则删除entry和之后的所有条目
	for k, v := range args.Entries {
		index := args.PrevLogIndex + 1 + k
		if index > len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, v)
		} else {
			if rf.logEntries[index-1].Term != v.Term {
				rf.logEntries = rf.logEntries[:index-1]
				rf.logEntries = append(rf.logEntries, v)
			}
		}
	}
	rf.persist()

	// if leaderCommit > commitIndex, set CommitIndex = min(leaderCommit, index of last new entry
	DPrintf("leader[%d]的leaderCommit为[%d], 节点[%d]的commitIndex为[%d],log为%v", args.LeaderId, args.LeaderCommit, rf.me, rf.commitIndex, rf.logEntries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logEntries))
		DPrintf("follower%d检查是否更新commitIndex为", rf.me)
		rf.unLock()
		rf.checkCanApply()
	} else {
		rf.unLock()
	}

	//DPrintf("Leader  %d  和Follower  %d  的term一样，它们的term为%d\n", args.LeaderId, rf.me, args.Term)
}
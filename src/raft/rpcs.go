package raft

import "time"

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


type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	offset int
	data []byte
	done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock()
	defer rf.unLock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unLock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.myState = FollowerState
		rf.persist()
		DPrintf("RequestVote中节点[%d]变成了follower,当前的term为[%d]", rf.me, rf.currentTerm)
	}

	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		// candidate's logEntries is at least as up-to-date as receiver's logEntries, grant vote
		lastLogTerm := -1
		if len(rf.logEntries) != 0 {
			lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		}
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < len(rf.logEntries) && args.LastLogTerm == lastLogTerm) {
			return
		} else {
			DPrintf("节点[%d]的投票votedFor为[%d]", rf.me, rf.votedFor)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.TimerReset = time.Now()
			rf.persist()
			return
		}
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// leader 向 follower发送appendEntry的请求，args为leader
	DPrintf("Leader  %d  向Follower  %d  发送log entry数据长度为%d\n", args.LeaderId, rf.me, len(args.Entries))
	//DPrintf("args为%d\n", args)
	//DPrintf("follower的logs为%v", rf.logEntries)
	rf.lock()
	reply.ReplyLogIndex = -1
	reply.ReplyLogTerm = -1
	//DPrintf("2A   Leader%d的term为%d\n", args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.unLock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.myState = FollowerState
		rf.persist()
		DPrintf("AppendEntries中节点[%d]变成了follower,当前的term为[%d]", rf.me, rf.currentTerm)
	}

	if rf.myState == CandidateState {
		rf.myState = FollowerState
		rf.persist()
	}

	reply.Success = true
	rf.TimerReset = time.Now()
	// 不包含在pervLogIndex matches pervLogTerm
	if args.PrevLogIndex > len(rf.logEntries) {
		reply.ReplyLogIndex = len(rf.logEntries)
		reply.Success = false
		rf.unLock()
		return
	}
	if args.PrevLogIndex > 0 && rf.logEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ReplyLogTerm = rf.logEntries[args.PrevLogIndex-1].Term
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
	terms := make([]int, len(rf.logEntries))
	for k, v := range rf.logEntries {
		terms[k] = v.Term
	}
	DPrintf("leader[%d]的leaderCommit为[%d], 节点[%d]的commitIndex为[%d],log为%v", args.LeaderId, args.LeaderCommit, rf.me, rf.commitIndex, terms)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logEntries))
	}
	rf.unLock()
}
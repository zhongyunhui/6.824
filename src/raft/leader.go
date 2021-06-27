package raft

import (
	"sort"
	"time"
)

func (rf *Raft) leaderInitialize() {
	rf.myState = LeaderState
	DPrintf("Candidate[%d] 获取大多数选票，变为leader", rf.me)
	// reinitialized after election，初始化nextIndex和matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i:= 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastIncludedIndex+len(rf.logEntries)+1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	DPrintf("leader[%d]进行初始化完毕，nextIndex初始化为[%d]", rf.me, rf.nextIndex[0])
}

func (rf *Raft) updateCommitIndex() {
	matchIndexs := make([]int, len(rf.matchIndex))
	for k, v := range rf.matchIndex {
		matchIndexs[k] = v
	}
	sort.Ints(matchIndexs)

	commitIndex := matchIndexs[len(rf.peers)/2]
	DPrintf("CommitIndex[%d], matchIndexs[%v]", commitIndex, matchIndexs)
	_, _, _, term := rf.getLogEntry(commitIndex)
	if commitIndex > rf.commitIndex && term == rf.currentTerm {
		rf.commitIndex = commitIndex
		DPrintf("leader%d更新commitIndex为%d", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("leader[%d]向节点[%d]发送InstallSnapshot RPC请求", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	func(){
		rf.lock()
		defer rf.unLock()
		DPrintf("leader[%d]向节点[%d]发送InstallSnapshot RPC请求[%v]", rf.me, server, ok)
		if ok {
			if args.Term != rf.currentTerm || rf.myState != LeaderState {
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("InstallSnapshot中leader[%d]的term[%d]<reply[%d]的term[%d]，变为了followerState", rf.me, rf.currentTerm, server, reply.Term)
				rf.currentTerm = reply.Term
				rf.myState = FollowerState
				rf.votedFor = -1
				rf.persist()
			} else {
				rf.nextIndex[server] = args.LastIncludedIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		}
	}()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock()
	if rf.currentTerm != args.Term || rf.myState != LeaderState{
		rf.unLock()
		return
	}
	if server == rf.me {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndex()
		rf.unLock()
		return
	}
	rf.unLock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.lock()
	if len(args.Entries)>0 {
		terms := make([]int, len(args.Entries))
		for k, v := range args.Entries {
			terms[k] = v.Term
		}
		DPrintf("leader[%d]向节点[%d]发送entries请求[%v]，appendEntries的args为[%v], reply为[%v]", rf.me, server, ok, terms, reply)
	} else {
		DPrintf("leader[%d]向节点[%d]发送heartBeat请求[%v]，appendEntries的args为[%v], reply为[%v]", rf.me, server, ok, args, reply)
	}
	if ok {
		if rf.currentTerm != args.Term || args.PrevLogIndex != rf.nextIndex[server]-1 {
			DPrintf("leader[%d]向节点[%d]发送的term[%d]过期,现在的term为[%d]", rf.me, server, args.Term, rf.currentTerm)
			rf.unLock()
			return
		}
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			DPrintf("leader[%d]更新节点[%d]的nextIndex为[%d]，matchIndex变为[%d]", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
			rf.updateCommitIndex()
			rf.unLock()
		} else {
			if args.Term < reply.Term {
				DPrintf("leader[%d]的term[%d]<reply[%d]的term[%d]，变为了followerState", rf.me, rf.currentTerm, server, reply.Term)
				rf.currentTerm = reply.Term
				rf.myState = FollowerState
				rf.votedFor = -1
				rf.persist()
				rf.unLock()
			} else {
				if reply.ReplyLogTerm != -1 {
					nextIndex := -1
					for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
						_, _, _, term := rf.getLogEntry(i)
						if term == reply.ReplyLogTerm {
							nextIndex = i
							break
						}
					}
					if nextIndex != -1 {
						rf.nextIndex[server] = nextIndex + 1
					} else {
						rf.nextIndex[server] = reply.ReplyLogIndex
					}
				} else {
					rf.nextIndex[server] = reply.ReplyLogIndex + 1
				}
				DPrintf("leader[%d]将节点[%d]nextIndex减少，并重新发送nextIndex%d", rf.me, server, rf.nextIndex[server]-1)
				rf.unLock()
				//DPrintf("发送给节点%d的entries为%v", server, args.Entries)
				//ok = rf.sendAppendEntries(server, args, reply)
			}
		}
		return
	}
	rf.unLock()
}


func (rf *Raft) sendAppendEntriesToFollower() {
	for !rf.killed(){
		rf.lock()
		if rf.myState != LeaderState {
			rf.unLock()
			DPrintf("leader[%d]退出sendAppendEntriesToFollower函数", rf.me)
			return
		}
		term := rf.currentTerm
		leaderId := rf.me
		leaderCommit := rf.commitIndex
		lastIncludedIndex := rf.lastIncludedIndex
		lastIncludedTerm := rf.lastIncludedTerm
		DPrintf("leader%d发送AppendEntries给follower，term为%d", rf.me, term)
		for k, fNextIndex := range rf.nextIndex {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				LeaderCommit: leaderCommit,
				PrevLogIndex: fNextIndex-1, // 紧挨着新entry的logIndex
			}

			if args.PrevLogIndex > rf.lastIncludedIndex {
				ok, _, _, term := rf.getLogEntry(args.PrevLogIndex)
				if ok {
					args.PrevLogTerm = term
				}
			} else if args.PrevLogIndex == rf.lastIncludedIndex {
				args.PrevLogTerm = rf.lastIncludedTerm
			} else {
				DPrintf("args.PrevLogIndex为[%d], rf.lastIncludedIndex为[%d]", args.PrevLogIndex, rf.lastIncludedIndex)
				snapshotArgs := InstallSnapshotArgs{
					Term:              term,
					LeaderId:          rf.me,
					LastIncludedIndex: lastIncludedIndex,
					LastIncludedTerm:  lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply:=InstallSnapshotReply{}
				rf.unLock()
				go rf.sendInstallSnapshot(k, &snapshotArgs, &reply)
				rf.lock()
				continue
			}
			if rf.lastIncludedIndex+len(rf.logEntries) >= fNextIndex {
				args.Entries = rf.logEntries[fNextIndex-rf.lastIncludedIndex-1:]
			}
			reply := AppendEntriesReply{}
			rf.unLock()
			go rf.sendAppendEntries(k, &args, &reply)
			rf.lock()
		}
		rf.unLock()
		time.Sleep(heartBeatTimeout * time.Millisecond)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logEntries. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logEntries, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 不保证该命令提交到Raft的log中，因为leader可能会失败或输掉选举。
	if rf.killed() {
		return -1, -1, false
	}
	rf.lock()
	defer rf.unLock()
	isLeader := rf.myState == LeaderState
	//DPrintf("节点%d为leader%v", rf.me, rf.myState == LeaderState)
	if !isLeader {
		return -1, -1, false
	}
	DPrintf("将command[%v]提交到leader[%d]的log上，term为[%d]，index为[%d]", command, rf.me, rf.currentTerm, len(rf.logEntries))
	term := rf.currentTerm
	index := rf.lastIncludedIndex+len(rf.logEntries)+1
	rf.logEntries = append(rf.logEntries, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist()
	//DPrintf("leader%d的log的length为%d", rf.me, len(rf.logEntries))
	// Your code here (2B).

	return index, term, isLeader
}
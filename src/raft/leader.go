package raft

import (
	"time"
)

func (rf *Raft) leaderInitialize() {
	// reinitialized after election，初始化nextIndex和matchIndex
	rf.lock()
	defer rf.unLock()
	DPrintf("初始化leader%d", rf.me)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i:= 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.logEntries)+1
		rf.matchIndex[i] = 0
	}
	DPrintf("初始化的nextIndex为%d", rf.nextIndex[0])
}

func (rf *Raft) updateCommitIndex() {
	rf.lock()
	lenLog:=len(rf.logEntries)
	DPrintf("检查是否更新commitIndex, rf.log为%d", lenLog)
	for i := rf.commitIndex+1; i <= lenLog; i++ {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= i {
				count++
			}
		}
		//DPrintf("count的值为%d", count)
		//DPrintf("lenLog的值为%d", lenLog)
		if count > len(rf.peers)/2 && rf.logEntries[i-1].Term == rf.currentTerm {
			rf.commitIndex = i
			//DPrintf("leader%d更新commitIndex为%d", rf.me, i)
			rf.unLock()
			rf.checkCanApply()
			return
		}
	}
	//DPrintf("leader的commitIndex为%d", rf.commitIndex)
	rf.unLock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.lock()
	DPrintf("leader【%d】向节点【%d】发送entries，args的term【%d】，leaderID【%d】，prevLogIndex【%d】，prevLogTerm【%d】，entries【%d】，leaderCommit【%d】", rf.me, server, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	if server == rf.me {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		DPrintf("rf.nextIndex[%d][%d]", server, rf.nextIndex[server])
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		DPrintf("在sendAppendEntries中更新节点%d的nextIndex为%d，matchIndex变为%d", server, rf.nextIndex[server], rf.matchIndex[server])
		rf.unLock()
		rf.updateCommitIndex()
		return true
	}
	//DPrintf("2A   leader%d向节点%d发送appendEntries请求，请求在%d处添加log\n", rf.me, server, rf.nextIndex[server])
	rf.unLock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("leader【%d】向节点【%d】发送entries  %v", rf.me, server, ok)
	rf.lock()
	//DPrintf("rf的log为%v", rf.logEntries)
	//DPrintf("2A  节点%d向节点%d发送心跳%v", rf.me, server, reply.Success)
	if ok {
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			DPrintf("rf.nextIndex[%d][%d]", server, rf.nextIndex[server])
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			DPrintf("在sendAppendEntries中更新节点%d的nextIndex为%d，matchIndex变为%d", server, rf.nextIndex[server], rf.matchIndex[server])
			rf.unLock()
			rf.updateCommitIndex()
		} else {
			if args.Term < reply.Term {
				rf.currentTerm = reply.Term
				rf.myState = FollowerState
				rf.votedFor = server
				rf.persist()
				rf.unLock()
			} else {
				DPrintf("reply%d的replyLogTerm%d和replyLogIndex%d", server, reply.ReplyLogTerm, reply.ReplyLogIndex)
				if reply.ReplyLogTerm != -1 {
					nextIndex := -1
					for i := args.PrevLogIndex; i >= 1; i-- {
						if rf.logEntries[i-1].Term == reply.ReplyLogTerm {
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
				DPrintf("给节点【%d】 sendAppendEntries将nextIndex减少，并重新发送nextIndex%d", server, rf.nextIndex[server]-1)
				rf.unLock()
				//DPrintf("发送给节点%d的entries为%v", server, args.Entries)
				//ok = rf.sendAppendEntries(server, args, reply)
			}
		}
		return ok
	}
	rf.unLock()

	return ok
}


func (rf *Raft) sendAppendEntriesToFollower() {
	for !rf.killed(){
		rf.lock()
		if rf.myState != LeaderState {
			rf.unLock()
			return
		}
		term := rf.currentTerm
		leaderId := rf.me
		leaderCommit := rf.commitIndex

		for k, fNextIndex := range rf.nextIndex {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				LeaderCommit: leaderCommit,
				PrevLogIndex: fNextIndex-1, // 紧挨着新entry的logIndex
			}

			if args.PrevLogIndex > 0 {
				DPrintf("sendAppendEntriesToFollower%d，leader%d的log的长度为%d", k, rf.me, len(rf.logEntries))
				if args.PrevLogIndex-1 >= len(rf.logEntries) {
					DPrintf("args的prevLogIndex为%d，log的长度为%d", args.PrevLogIndex, len(rf.logEntries))
				}
				args.PrevLogTerm = rf.logEntries[args.PrevLogIndex-1].Term
			}
			if len(rf.logEntries) >= fNextIndex {
				DPrintf("fNextIndex为%d", fNextIndex)
				args.Entries = rf.logEntries[fNextIndex-1:]
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
	DPrintf("Start中，向节点【%d】输入命令【%v】", rf.me, command)
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
	DPrintf("start后，将command【%v】提交到leader【%d】的log上，term为【%d】，index为【%d】", command, rf.me, rf.currentTerm, len(rf.logEntries))
	term := rf.currentTerm
	index := len(rf.logEntries)+1
	rf.logEntries = append(rf.logEntries, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist()
	//DPrintf("leader%d的log的length为%d", rf.me, len(rf.logEntries))
	// Your code here (2B).

	return index, term, isLeader
}
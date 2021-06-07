package raft

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
	for !rf.killed() {
		//fmt.Println("该节点为Follower")
		//fmt.Printf("节点%dcheckFollower获得锁\n", rf.me)
		// 当election timer没有复位，而且rf没有投票的话，则将该节点变成Candidate
		DPrintf("2A  节点【%d】进入checkFollower的状态为%d", rf.me, rf.myState)
		if !rf.checkTimerReset(FollowerState) {
			rf.toCandidate()
			return
		}
	}
}

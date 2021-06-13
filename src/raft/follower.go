package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) toCandidate() {
	rf.lock()
	defer rf.unLock()
	rf.myState = CandidateState
	rf.votedFor = rf.me
	DPrintf("节点[%d]未收到Leader发送的消息，变为Candidate", rf.me)
	//DPrintf("2A   节点%d变成Candidate", rf.me)
}


func (rf *Raft) checkFollower() {
	for !rf.killed() {
		//fmt.Println("该节点为Follower")
		//fmt.Printf("节点%dcheckFollower获得锁\n", rf.me)
		// 当election timer没有复位，而且rf没有投票的话，则将该节点变成Candidate
		rf.initTimerReset()
		timeout := time.Duration(400+rand.Int31n(150))*time.Millisecond
		for time.Since(rf.getTimerReset()) <= timeout && rf.getMyState() == FollowerState {
			time.Sleep(time.Millisecond)
		}
		DPrintf("节点[%d]的timerout为[%v]", rf.me, time.Since(rf.getTimerReset()))
		rf.toCandidate()
		return

		//
		//if !rf.checkTimerReset(FollowerState) {
		//	rf.toCandidate()
		//	return
		//}
	}
}

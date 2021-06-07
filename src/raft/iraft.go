package raft

type IRaft interface {
	GetMyState() int
	SetMyState(state int)

	GetTerm() int
	SetTerm(term int)

	GetVotedFor() int
	SetVotedFor(votedFor int)

	GetCommitIndex() int
	SetCommitIndex(index int)

	GetTimerReset() bool
	SetTimerReset(timerReset bool)

	GetVotedCount() int
	SetVotedCount(count int)
}


func (rf *Raft) GetMyState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.myState
}
func (rf *Raft) SetMyState(num int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.myState = num
}


func (rf *Raft) GetTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}
func (rf *Raft) SetTerm(num int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = num
}



func (rf *Raft) GetVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}
func (rf *Raft) SetVotedFor(num int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = num
}



func (rf *Raft) GetTimerReset() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.timerReset
}
func (rf *Raft) SetTimerReset(reset bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timerReset = reset
}






package raft

import "sync"

type Persister struct {
	rmu       sync.RWMutex
	raftState []byte
	snapShot  []byte
}

func NewPersister() *Persister {
	return &Persister{}
}
func (p *Persister) XGRaftState(state []byte) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	p.raftState = clone(state)
}
func (p *Persister) GetRaftState() []byte {
	p.rmu.RLock()
	defer p.rmu.RLock()
	return clone(p.raftState)
}
func (p *Persister) GetRaftSize() int {
	p.rmu.RLock()
	defer p.rmu.RUnlock()
	return len(p.raftState)
}
func (p *Persister) XGsnapShot(shot []byte) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	p.snapShot = clone(shot)
}
func (p *Persister) GetsnapShot() []byte {
	p.rmu.RLock()
	defer p.rmu.RLock()
	return clone(p.snapShot)
}
func (p *Persister) GetsnapSize() int {
	p.rmu.RLock()
	defer p.rmu.RUnlock()
	return len(p.snapShot)
}
func clone(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
func (p *Persister) XGRaftAndSnap(state []byte, shot []byte) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	p.raftState = clone(state)
	p.snapShot = clone(shot)
}

// 这个还不知道有什么作用
func (p *Persister) Copy() *Persister {
	s := NewPersister()
	p.rmu.RLock()
	defer p.rmu.RUnlock()
	s.raftState = p.raftState
	s.snapShot = p.snapShot
}

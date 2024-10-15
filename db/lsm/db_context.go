package lsm

import "sync"

type dbCtx struct {
	ReadLockCounter  int
	WriteLockCounter int
}

func (me *dbCtx) Lock(lock *sync.RWMutex) {
	if me.ReadLockCounter != 0 {
		panic("cannot upgrade from reader lock to writer lock")
	}
	if me.WriteLockCounter == 0 {
		lock.Lock()
	}
	me.WriteLockCounter++
}

func (me *dbCtx) Unlock(lock *sync.RWMutex) {
	if me.ReadLockCounter != 0 {
		panic("cannot downgrade from writer lock to reader lock")
	}
	if me.WriteLockCounter == 1 {
		lock.Unlock()
	}
	me.WriteLockCounter--
}

func (me *dbCtx) RLock(lock *sync.RWMutex) {
	if me.WriteLockCounter != 0 {
		return
	}
	if me.ReadLockCounter == 0 {
		lock.Lock()
	}
	me.ReadLockCounter++
}

func (me *dbCtx) RUnlock(lock *sync.RWMutex) {
	if me.WriteLockCounter != 0 {
		return
	}
	if me.ReadLockCounter == 1 {
		lock.Unlock()
	}
	me.ReadLockCounter--
}

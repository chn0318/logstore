package memorylog

import (
	"fmt"
	"sync"

	"github.com/chn0318/logstore/sharedlog"
)

type MemoryLog struct {
	dataRecs   map[uint64]sharedlog.DataRecord
	commitRecs map[uint64]sharedlog.CommitRecord
	tail       uint64
	mu         sync.RWMutex
}

func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		dataRecs:   make(map[uint64]sharedlog.DataRecord),
		commitRecs: make(map[uint64]sharedlog.CommitRecord),
	}
}

func (l *MemoryLog) AppendData(rec sharedlog.DataRecord) (sharedlog.RecordRef, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tail++
	l.dataRecs[l.tail] = rec

	return sharedlog.RecordRef{
		GSN: l.tail,
	}, nil
}

func (l *MemoryLog) AppendCommit(rec sharedlog.CommitRecord) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tail++
	l.commitRecs[l.tail] = rec
	return l.tail, nil
}

func (l *MemoryLog) ReadData(ref sharedlog.RecordRef) (sharedlog.DataRecord, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	rec, ok := l.dataRecs[ref.GSN]
	if !ok {
		return sharedlog.DataRecord{}, fmt.Errorf("data record not found: gsn=%d", ref.GSN)
	}
	return rec, nil
}

func (l *MemoryLog) ReplayCommits(from, to uint64, handler func(uint64, sharedlog.CommitRecord) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for gsn := from; gsn <= to; gsn++ {
		rec, ok := l.commitRecs[gsn]
		if !ok {
			continue
		}
		if err := handler(gsn, rec); err != nil {
			return err
		}
	}
	return nil
}

func (l *MemoryLog) Head() uint64 { return 1 }
func (l *MemoryLog) Tail() uint64 { l.mu.RLock(); defer l.mu.RUnlock(); return l.tail }

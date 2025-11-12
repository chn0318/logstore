package mapservice

import (
	"sync"

	"github.com/chn0318/logstore/sharedlog"
)

// KeyMeta stores the latest data GSN and the commit GSN
// that last updated this key.
type KeyMeta struct {
	Ref       sharedlog.RecordRef
	CommitGSN uint64
}

// CommitEntry describes a single key->data_gsn pair inside a commit.
// Coordinator 可以把 sharedlog.CommitEntry 转成这个类型传进来。
type CommitEntry struct {
	Key string
	Ref sharedlog.RecordRef
}

// MapService is an in-memory implementation of the mapping service.
// It maintains a mapping from key to (data_gsn, commit_gsn).
type MapService struct {
	mu sync.RWMutex
	m  map[string]KeyMeta

	// 记录 map-service 已经处理过的最大 commit_gsn（方便以后做 checkpoint/recover）
	maxCommitGSN uint64
}

// New creates a new in-memory MapService.
func NewMapService() *MapService {
	return &MapService{
		m: make(map[string]KeyMeta),
	}
}

// ApplyCommit applies a commit record atomically.
//
// For each key in entries, it compares the commitGSN with the key's current
// CommitGSN. If commitGSN is larger, it updates the mapping to the new DataGSN.
// If commitGSN is smaller or equal, the update for that key is skipped.
//
// 整个函数在一个写锁下执行，保证“原子地应用这一次 commit”。
func (s *MapService) ApplyCommit(commitGSN uint64, entries []CommitEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if commitGSN > s.maxCommitGSN {
		s.maxCommitGSN = commitGSN
	}
	for _, e := range entries {
		meta, ok := s.m[e.Key]
		if !ok || commitGSN > meta.CommitGSN {
			s.m[e.Key] = KeyMeta{Ref: e.Ref, CommitGSN: commitGSN}
		}
	}
}

func (s *MapService) GetOffsets(keys []string) map[string]sharedlog.RecordRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]sharedlog.RecordRef, len(keys))
	for _, k := range keys {
		if meta, ok := s.m[k]; ok {
			res[k] = meta.Ref
		}
	}
	return res
}

// MaxCommitGSN returns the largest commit GSN that has been applied so far.
// 以后做 checkpoint / recovery 时会用到这个值。
func (s *MapService) MaxCommitGSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxCommitGSN
}

package mapservice

import "sync"

// KeyMeta stores the latest data GSN and the commit GSN
// that last updated this key.
type KeyMeta struct {
	DataGSN   uint64
	CommitGSN uint64
}

// CommitEntry describes a single key->data_gsn pair inside a commit.
// Coordinator 可以把 sharedlog.CommitEntry 转成这个类型传进来。
type CommitEntry struct {
	Key     string
	DataGSN uint64
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

	// 维护全局见过的最大 commitGSN
	if commitGSN > s.maxCommitGSN {
		s.maxCommitGSN = commitGSN
	}

	for _, e := range entries {
		meta, ok := s.m[e.Key]
		// 如果 key 还不存在，或者这是更晚的 commit，就更新
		if !ok || commitGSN > meta.CommitGSN {
			s.m[e.Key] = KeyMeta{
				DataGSN:   e.DataGSN,
				CommitGSN: commitGSN,
			}
		}
		// 否则：当前 key 已经被更大的 commit_gsn 更新过了，跳过这个 key
	}
}

// GetOffsets returns the latest DataGSN for a batch of keys.
//
// 返回的这组 key->DataGSN 是在同一把读锁下读取的，
// 等价于来自 MapService 的一个原子快照。
func (s *MapService) GetOffsets(keys []string) map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make(map[string]uint64, len(keys))
	for _, k := range keys {
		if meta, ok := s.m[k]; ok {
			res[k] = meta.DataGSN
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

package sharedlog

// RecordRef 抽象“如何在底层日志中定位一条记录”。
// - Scalog: 需要 {ShardID, GSN}
// - CORFU / 内存实现: 只用 GSN，ShardID=0 即可
type RecordRef struct {
	GSN     uint64
	ShardID uint32 // 可选；无分片实现可置 0
}

// 一些辅助函数（可选）
func ShardlessRef(gsn uint64) RecordRef             { return RecordRef{GSN: gsn} }
func ShardedRef(shard uint32, gsn uint64) RecordRef { return RecordRef{ShardID: shard, GSN: gsn} }

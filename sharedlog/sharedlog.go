package sharedlog

// DataRecord represents a key-value write operation stored in the log.
type DataRecord struct {
	Key   string
	Value []byte
}

// CommitEntry links a key to its corresponding DataRecord's GSN.
type CommitEntry struct {
	Key string
	Ref RecordRef
}

// CommitRecord represents a multi-key atomic transaction commit.
type CommitRecord struct {
	Entries []CommitEntry
}

// SharedLog defines the abstraction of an append-only shared log system.
// Implementations can be backed by CORFU, Scalog, or other log-based systems.
type SharedLog interface {
	// AppendData appends a DATA record (key-value pair) to the shared log.
	// Returns the assigned global sequence number (GSN).
	AppendData(rec DataRecord) (ref RecordRef, err error)

	// AppendCommit appends a COMMIT record to the log,
	// referencing multiple DataRecords via their GSNs.
	// Returns the commit record's own GSN.
	AppendCommit(rec CommitRecord) (commitGSN uint64, err error)

	// ReadData retrieves a DATA record by its GSN.
	ReadData(ref RecordRef) (DataRecord, error)

	// // ReplayCommits replays COMMIT records in GSN order from [fromGSN, toGSN].
	// // The provided handler is called for each commit record.
	// ReplayCommits(fromGSN, toGSN uint64, handler func(commitGSN uint64, rec CommitRecord) error) error

	// // Head returns the smallest GSN currently available (useful for log trimming).
	// Head() uint64

	// // Tail returns the largest GSN written so far.
	// Tail() uint64
}

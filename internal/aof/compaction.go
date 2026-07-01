package aof

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

// CompactionSnapshot is the minimal state needed to write a compacted AOF.
// The queue package populates this when it triggers compaction.
type CompactionSnapshot struct {
	// PendingJobs maps jobID → raw JSON payload for jobs in pending state.
	PendingJobs map[string]string
	// ProcessingJobs maps jobID → raw JSON payload for jobs in processing state.
	ProcessingJobs map[string]string
}

// writeCompacted writes a compacted AOF to a temp file and returns its path.
// The caller is responsible for renaming it over the live AOF path.
// This is an internal helper called by AOF.Compact() after the writeLoop is paused.
func writeCompacted(aofPath string, snap CompactionSnapshot) (string, error) {
	dir := filepath.Dir(aofPath)
	tmp, err := os.CreateTemp(dir, "anvil-aof-compact-*.tmp")
	if err != nil {
		return "", fmt.Errorf("aof: compaction create temp: %w", err)
	}
	tmpPath := tmp.Name()

	buf := bufio.NewWriterSize(tmp, 64*1024)

	// Pending jobs: emit ENQUEUE only.
	// On replay, they land back in the pending list ready to be dequeued.
	for jobID, payload := range snap.PendingJobs {
		fmt.Fprintf(buf, "ENQUEUE %s %s\n", jobID, payload)
	}

	// Processing jobs: emit ENQUEUE + DEQUEUE.
	// On replay they land in processing; the Reaper rescues them since
	// no worker will be sending heartbeats after a crash.
	for jobID, payload := range snap.ProcessingJobs {
		fmt.Fprintf(buf, "ENQUEUE %s %s\n", jobID, payload)
		fmt.Fprintf(buf, "DEQUEUE %s\n", jobID)
	}

	if err := buf.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("aof: compaction flush: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("aof: compaction fsync: %w", err)
	}
	tmp.Close()
	return tmpPath, nil
}

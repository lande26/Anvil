// Package aof implements Anvil's Append-Only File persistence layer.
// Unlike Valkyr's general-purpose RESP-based AOF, this is purpose-built
// for the queue's six verbs: ENQUEUE, DEQUEUE, COMPLETE, FAIL, REQUEUE, DEAD.
//
// Architecture:
//   - A single-writer goroutine (writeLoop) owns the file descriptor.
//   - All callers send entries to a buffered channel and return immediately.
//   - The in-memory queue mutex is NEVER held while waiting on disk I/O.
//   - Under SyncAlways, Log() blocks until the entry is fsynced (durability guarantee).
//   - Compaction coordinates with the writeLoop via a two-step handshake to
//     ensure no entries are lost between snapshot and file rename.
package aof

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// SyncPolicy controls how aggressively AOF writes are fsynced to disk.
type SyncPolicy string

const (
	// SyncAlways fsyncs after every single write. Safest, slowest.
	// Log() blocks until the write is confirmed on disk.
	SyncAlways SyncPolicy = "always"
	// SyncEverySecond fsyncs once per second. Redis-like tradeoff (default).
	SyncEverySecond SyncPolicy = "everysec"
	// SyncNo lets the OS decide when to flush. Fastest, least durable.
	SyncNo SyncPolicy = "no"
)

// Entry is a single AOF command.
type Entry struct {
	Verb    string // ENQUEUE | DEQUEUE | COMPLETE | FAIL | REQUEUE | DEAD
	JobID   string
	Payload string    // optional: JSON payload for ENQUEUE, error for FAIL
	done    chan error // non-nil only under SyncAlways — signals write confirmation
}

// pauseReq is sent on pauseCh to initiate a compaction pause.
type pauseReq struct {
	flushed chan struct{} // writeLoop closes this when it has flushed+fsynced and is paused
	newFile *os.File     // compaction sends the new file for the writeLoop to use after resume
}

// AOF manages writing and replaying the append-only queue log.
type AOF struct {
	path    string
	policy  SyncPolicy
	ch      chan Entry
	pauseCh chan *pauseReq // compaction sends pause request, writeLoop handles it
	wg      sync.WaitGroup
	closed  chan struct{}
}

// Open creates or opens an AOF file at path, starts the write loop, and
// returns a ready-to-use AOF. Call Close() to flush and shut down cleanly.
func Open(path string, policy SyncPolicy, bufSize int) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("aof: open %s: %w", path, err)
	}

	a := &AOF{
		path:    path,
		policy:  policy,
		ch:      make(chan Entry, bufSize),
		pauseCh: make(chan *pauseReq, 1),
		closed:  make(chan struct{}),
	}

	a.wg.Add(1)
	go a.writeLoop(f)
	return a, nil
}

// Log sends an AOF entry to the write goroutine.
//
// Under SyncAlways policy, Log blocks until the entry has been written AND
// fsynced to disk — giving the caller a hard durability guarantee.
// Under everysec/no policy, Log returns immediately (fire-and-forget).
func (a *AOF) Log(e Entry) error {
	if a.policy == SyncAlways {
		e.done = make(chan error, 1)
		select {
		case a.ch <- e:
		case <-a.closed:
			return fmt.Errorf("aof: closed")
		}
		return <-e.done // block until writeLoop confirms fsync
	}
	select {
	case a.ch <- e:
	case <-a.closed:
	}
	return nil
}

// Close drains the channel, fsyncs, and closes the file.
// After Close returns, no further writes will be accepted.
func (a *AOF) Close() {
	close(a.closed)
	a.wg.Wait()
}

// Compact rewrites the AOF using a snapshot of current queue state.
// It coordinates with the writeLoop via a two-step handshake:
//  1. Send a pause request. The writeLoop flushes+fsyncs and signals "paused".
//  2. Write the compacted file, rename it atomically, reopen it, then send
//     the new file descriptor back to the writeLoop so it can resume writing.
//
// During the pause window, new Log() calls queue into the channel and are
// processed as soon as the writeLoop resumes with the new file — no entries
// are lost, no entries land in the old file after rename.
func (a *AOF) Compact(snap CompactionSnapshot) error {
	// Step 1: Ask writeLoop to flush and pause.
	req := &pauseReq{
		flushed: make(chan struct{}),
	}
	select {
	case a.pauseCh <- req:
	case <-a.closed:
		return fmt.Errorf("aof: closed")
	}

	// Wait for the writeLoop to signal it has flushed and is paused.
	<-req.flushed

	// Step 2: Write compacted file to a temp path.
	newPath, err := writeCompacted(a.path, snap)
	if err != nil {
		// Resume writeLoop without changing file on error.
		req.newFile = nil
		a.pauseCh <- req // signal resume with nil = same file
		return err
	}

	// Atomic rename: new compacted file replaces old AOF.
	if err := os.Rename(newPath, a.path); err != nil {
		os.Remove(newPath)
		req.newFile = nil
		a.pauseCh <- req
		return fmt.Errorf("aof: compact rename: %w", err)
	}

	// Reopen the file (now at the same path, but pointing to the compacted content).
	newFile, err := os.OpenFile(a.path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("aof: compact reopen: %w", err)
	}

	// Step 3: Hand the new file to the writeLoop and let it resume.
	req.newFile = newFile
	a.pauseCh <- req

	slog.Info("aof: compaction complete",
		"pending", len(snap.PendingJobs),
		"processing", len(snap.ProcessingJobs),
	)
	return nil
}

func (a *AOF) writeLoop(f *os.File) {
	defer a.wg.Done()
	defer func() {
		// Final drain on shutdown — write anything remaining in the channel.
		for len(a.ch) > 0 {
			e := <-a.ch
			fmt.Fprintf(f, "%s %s %s\n", e.Verb, e.JobID, e.Payload)
		}
		f.Sync()
		f.Close()
	}()

	buf := bufio.NewWriterSize(f, 64*1024)

	var ticker *time.Ticker
	var tickCh <-chan time.Time
	if a.policy == SyncEverySecond {
		ticker = time.NewTicker(time.Second)
		tickCh = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case e := <-a.ch:
			fmt.Fprintf(buf, "%s %s %s\n", e.Verb, e.JobID, e.Payload)
			if a.policy == SyncAlways {
				buf.Flush()
				syncErr := f.Sync()
				if e.done != nil {
					e.done <- syncErr
				}
			}

		case req := <-a.pauseCh:
			// Compaction pause handshake — Step 1:
			// Flush and fsync so the snapshot taken by Compact() is
			// consistent with what's actually on disk.
			buf.Flush()
			f.Sync()
			// Signal Compact() that we're paused.
			close(req.flushed)

			// Wait for Compact() to finish and hand us the new file.
			// It sends on the same pauseCh with req.newFile populated.
			resumeReq := <-a.pauseCh

			// Step 2: switch to the new file and reset the buffer.
			f.Close()
			if resumeReq.newFile != nil {
				f = resumeReq.newFile
			} else {
				// Compaction failed — reopen original file to continue.
				f, _ = os.OpenFile(a.path, os.O_RDWR|os.O_APPEND, 0644)
			}
			buf.Reset(f)

		case <-tickCh:
			buf.Flush()
			f.Sync()

		case <-a.closed:
			buf.Flush()
			return
		}
	}
}

// --- Replay ---

// ReplayFunc is called for each command during AOF replay.
type ReplayFunc func(verb, jobID, payload string) error

// Replay reads the AOF file at path and calls fn for each valid entry.
// Called once at startup before the write loop begins.
func Replay(path string, fn ReplayFunc) (int, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("aof: replay open %s: %w", path, err)
	}
	defer f.Close()

	count := 0
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			slog.Warn("aof: skipping malformed line", "line", lineNum, "content", line)
			continue
		}
		verb := parts[0]
		jobID := parts[1]
		payload := ""
		if len(parts) == 3 {
			payload = parts[2]
		}
		if err := fn(verb, jobID, payload); err != nil {
			slog.Warn("aof: replay command failed", "line", lineNum, "verb", verb, "job_id", jobID, "err", err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("aof: replay scan error: %w", err)
	}
	slog.Info("aof: replay complete", "commands", count)
	return count, nil
}

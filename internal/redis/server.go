package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	maxConnections = 128
	readTimeout    = 5 * time.Minute
	writeTimeout   = 30 * time.Second
)

// Options configures storage accounting and persistence. MaxBytes is a logical
// storage budget, not a limit on process memory. Zero leaves it unlimited.
type Options struct {
	AOFPath      string
	SnapshotPath string // empty defaults to AOFPath + ".snap" when AOF is enabled
	MaxBytes     int64
}

// Serve retains the optional-AOF API for existing embedded callers.
func Serve(ctx context.Context, listener net.Listener, aofPath ...string) error {
	if len(aofPath) > 1 {
		listener.Close()
		return errors.New("expected at most one AOF path")
	}
	options := Options{}
	if len(aofPath) == 1 {
		options.AOFPath = aofPath[0]
	}
	return ServeWithOptions(ctx, listener, options)
}

// ServeWithOptions owns listener and closes it and all connections on return.
func ServeWithOptions(ctx context.Context, listener net.Listener, options Options) error {
	if options.MaxBytes < 0 || (options.SnapshotPath != "" && options.AOFPath == "") {
		listener.Close()
		return errors.New("invalid storage limit or snapshot without AOF")
	}
	data := newStore()
	data.maxBytes = options.MaxBytes
	if options.AOFPath != "" {
		if options.SnapshotPath == "" {
			options.SnapshotPath = options.AOFPath + ".snap"
		}
		journal, recovered, err := openPersistence(options.AOFPath, options.SnapshotPath, options.MaxBytes)
		if err != nil {
			listener.Close()
			return err
		}
		defer journal.file.Close()
		data = recovered
	}
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	stopping := false
	stop := func() {
		mu.Lock()
		defer mu.Unlock()
		stopping = true
		listener.Close()
		for conn := range connections {
			conn.Close()
		}
	}
	done := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		select {
		case <-ctx.Done():
			stop()
		case <-done:
		}
	}()
	defer func() {
		stop()
		close(done)
		watcher.Wait()
		workers.Wait()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		mu.Lock()
		if stopping || len(connections) >= maxConnections {
			mu.Unlock()
			conn.Close()
			continue
		}
		connections[conn] = struct{}{}
		workers.Add(1)
		mu.Unlock()
		go func() {
			defer workers.Done()
			defer func() {
				conn.Close()
				mu.Lock()
				delete(connections, conn)
				mu.Unlock()
			}()
			handleConnection(conn, data)
		}()
	}
}

func handleConnection(conn net.Conn, data *store) {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return
		}
		args, err := readCommand(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				failure("ERR Protocol error: invalid or oversized request").write(w)
				w.Flush()
			}
			return
		}
		result := data.execute(args)
		if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return
		}
		if err := result.write(w); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

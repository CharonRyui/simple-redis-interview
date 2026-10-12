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

// Serve owns listener and closes it and all active connections on return.
// An optional AOF path enables recovery and synchronous persistence; omitting
// it retains the in-memory API used by embedded callers and tests.
func Serve(ctx context.Context, listener net.Listener, aofPath ...string) error {
	data := newStore()
	if len(aofPath) > 1 {
		listener.Close()
		return errors.New("expected at most one AOF path")
	}
	if len(aofPath) == 1 {
		journal, recovered, err := openAOF(aofPath[0])
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

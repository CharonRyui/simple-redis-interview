package redis

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, listener) }()
	t.Cleanup(cancel)
	return listener.Addr().String(), cancel, done
}

func dialTestServer(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func expectBytes(t *testing.T, r io.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTCPFragmentationPipelineAndCommandErrors(t *testing.T) {
	addr, cancel, done := startTestServer(t)
	conn := dialTestServer(t, addr)
	// Split every byte to verify parsing does not depend on packet boundaries.
	for _, b := range []byte(request("HSET", "key", "field", "\x00\r\n")) {
		if _, err := conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	expectBytes(t, conn, ":1\r\n")
	pipeline := request("HGET", "key", "field") + request("HGET", "key") + request("UNKNOWN") + request("PING")
	if _, err := io.WriteString(conn, pipeline); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, conn, "$3\r\n\x00\r\n\r\n-ERR wrong number of arguments for 'hget' command\r\n-ERR unknown command\r\n+PONG\r\n")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop with an idle client connected")
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("client connection remained open after shutdown")
	}
}

func TestProtocolErrorClosesOnlyOffendingConnection(t *testing.T) {
	addr, _, _ := startTestServer(t)
	bad := dialTestServer(t, addr)
	if _, err := io.WriteString(bad, "*1\r\n$8388608\r\n"); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, bad, "-ERR Protocol error: invalid or oversized request\r\n")
	if _, err := bad.Read(make([]byte, 1)); err == nil {
		t.Fatal("malformed connection was not closed")
	}
	good := dialTestServer(t, addr)
	if _, err := io.WriteString(good, request("PING")); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, good, "+PONG\r\n")
}

func TestConcurrentTCPClientsShareData(t *testing.T) {
	addr, _, _ := startTestServer(t)
	const clients = 8
	connections := make([]net.Conn, clients)
	for i := range connections {
		connections[i] = dialTestServer(t, addr)
	}
	var wg sync.WaitGroup
	for id, conn := range connections {
		wg.Add(1)
		go func(id int, conn net.Conn) {
			defer wg.Done()
			field := fmt.Sprintf("field:%d", id)
			_, err := io.WriteString(conn, request("HSET", "shared", field, "value")+request("HGET", "shared", field))
			if err != nil {
				t.Error(err)
				return
			}
			want := ":1\r\n$5\r\nvalue\r\n"
			got := make([]byte, len(want))
			if _, err := io.ReadFull(conn, got); err != nil || string(got) != want {
				t.Errorf("client %d: got %q, %v", id, got, err)
			}
		}(id, conn)
	}
	wg.Wait()
	if _, err := io.WriteString(connections[0], request("HLEN", "shared")); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, connections[0], fmt.Sprintf(":%d\r\n", clients))
}

func TestPartialRequestDoesNotApplyWrites(t *testing.T) {
	addr, _, _ := startTestServer(t)
	conn := dialTestServer(t, addr)
	input := request("HSET", "key", "field", "value")
	if _, err := io.WriteString(conn, strings.TrimSuffix(input, "\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, conn, "-ERR Protocol error: invalid or oversized request\r\n")
	other := dialTestServer(t, addr)
	if _, err := io.WriteString(other, request("HLEN", "key")); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, other, ":0\r\n")
}

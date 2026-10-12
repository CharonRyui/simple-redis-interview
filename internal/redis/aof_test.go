package redis

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAOFRecoveryAndExclusiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openAOF(path); err == nil {
		t.Fatal("second process/file handle acquired AOF")
	}
	for _, args := range [][]string{
		{"HSET", "k", "a", "1", "b", "2"},
		{"HSET", "k", "a", "\x00\r\n张三"},
		{"HDEL", "k", "b"},
		{"HSET", "deleted", "f", "v"},
		{"DEL", "deleted"},
		{"HSET", "", "", ""},
	} {
		if got := s.execute(args); got.kind == '-' {
			t.Fatal(got.text)
		}
	}
	before, _ := a.file.Seek(0, io.SeekCurrent)
	s.execute([]string{"HGET", "k", "a"})
	s.execute([]string{"HSET", "k", "a"})
	after, _ := a.file.Seek(0, io.SeekCurrent)
	if before != after {
		t.Fatal("read or invalid command appended a log record")
	}
	a.file.Close()
	a, s, err = openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"HGET", "k", "a"}, "$9\r\n\x00\r\n张三\r\n"},
		{[]string{"HGET", "k", "b"}, "$-1\r\n"},
		{[]string{"HLEN", "k"}, ":1\r\n"},
		{[]string{"HLEN", "deleted"}, ":0\r\n"},
		{[]string{"HGET", "", ""}, "$0\r\n\r\n"},
	} {
		if got := wire(t, s.execute(step.args)); got != step.want {
			t.Fatalf("%q: %q != %q", step.args, got, step.want)
		}
	}
}

func TestConcurrentPersistentWritesRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				value := fmt.Sprintf("%d:%d", id, i)
				if v := s.execute([]string{"HSET", "shared", "a", value, "b", value}); v.kind != ':' {
					t.Error(v.text)
				}
				if v := s.execute([]string{"HSET", "fields", value, value}); v.kind != ':' {
					t.Error(v.text)
				}
			}
		}(worker)
	}
	wg.Wait()
	last := s.execute([]string{"HGET", "shared", "a"}).text
	a.file.Close()
	a, s, err = openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.execute([]string{"HLEN", "fields"}).n != 40 {
		t.Fatal("concurrent writes lost during recovery")
	}
	for _, field := range []string{"a", "b"} {
		if s.execute([]string{"HGET", "shared", field}).text != last {
			t.Fatal("log and memory execution order diverged")
		}
	}
}

func TestAOFRepairsEveryIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "kept", "f", "yes"})
	boundary, _ := a.file.Seek(0, io.SeekCurrent)
	s.execute([]string{"HSET", "unfinished", "a", "1", "b", "2"})
	a.file.Close()
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for cut := int(boundary); cut < len(original); cut++ {
		if err := os.WriteFile(path, original[:cut], 0600); err != nil {
			t.Fatal(err)
		}
		a, s, err = openAOF(path)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if s.execute([]string{"HLEN", "unfinished"}).n != 0 {
			t.Fatal("partial command was replayed")
		}
		if s.execute([]string{"HGET", "kept", "f"}).text != "yes" {
			t.Fatal("lost earlier write")
		}
		if got := s.execute([]string{"HSET", "later", "f", "ok"}); got.n != 1 || got.kind != ':' {
			t.Fatal(got)
		}
		a.file.Close()
		a, s, err = openAOF(path)
		if err != nil {
			t.Fatalf("reopen after repair at %d: %v", cut, err)
		}
		if s.execute([]string{"HGET", "later", "f"}).text != "ok" {
			t.Fatal("append after repair lost")
		}
		a.file.Close()
	}
}

func TestAOFCorruptionRefusesStartupWithoutChangingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "k", "f", "value"})
	s.execute([]string{"HSET", "next", "f", "value"})
	a.file.Close()
	original, _ := os.ReadFile(path)
	for _, offset := range []int{0, len(aofMagic), len(aofMagic) + 4, len(aofMagic) + 8, len(aofMagic) + 16, len(original) - 1} {
		bad := append([]byte(nil), original...)
		bad[offset] ^= 0xff
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openAOF(path); err == nil {
			t.Fatalf("accepted corruption at %d", offset)
		}
		after, _ := os.ReadFile(path)
		if !bytes.Equal(bad, after) {
			t.Fatal("modified corrupt log")
		}
	}
}

type failingLogFile struct {
	logFile
	failSync bool
	writes   int
}

func (f *failingLogFile) Write(p []byte) (int, error) {
	f.writes++
	if !f.failSync && f.writes == 2 {
		n, _ := f.logFile.Write(p[:len(p)/2])
		return n, errors.New("injected disk full")
	}
	return f.logFile.Write(p)
}
func (f *failingLogFile) Sync() error {
	if f.failSync {
		return errors.New("injected sync error")
	}
	return f.logFile.Sync()
}

func TestAOFDiskFailureDisablesWritesButKeepsReads(t *testing.T) {
	for _, syncFailure := range []bool{false, true} {
		t.Run(fmt.Sprint("sync=", syncFailure), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.aof")
			a, s, err := openAOF(path)
			if err != nil {
				t.Fatal(err)
			}
			s.execute([]string{"HSET", "existing", "f", "kept"})
			a.file = &failingLogFile{logFile: a.file, failSync: syncFailure}
			if v := s.execute([]string{"HSET", "uncertain", "f", "v"}); v.kind != '-' {
				t.Fatal("failed persistence returned success")
			}
			if v := s.execute([]string{"HGET", "uncertain", "f"}); v.kind != 0 {
				t.Fatal("failed write changed memory")
			}
			if v := s.execute([]string{"HGET", "existing", "f"}); v.text != "kept" {
				t.Fatal("reads unavailable")
			}
			for _, args := range [][]string{{"HSET", "later", "f", "v"}, {"HDEL", "existing", "f"}, {"DEL", "existing"}} {
				if v := s.execute(args); v.kind != '-' {
					t.Fatal("allowed write after I/O error")
				}
			}
			a.file.Close()
			a, s, err = openAOF(path)
			if err != nil {
				t.Fatal(err)
			}
			defer a.file.Close()
			if s.execute([]string{"HGET", "existing", "f"}).text != "kept" {
				t.Fatal("lost acknowledged write")
			}
			if s.execute([]string{"HLEN", "later"}).n != 0 {
				t.Fatal("replayed rejected subsequent write")
			}
			if !syncFailure && s.execute([]string{"HLEN", "uncertain"}).n != 0 {
				t.Fatal("replayed partial write")
			}
			if s.execute([]string{"HSET", "resumed", "f", "v"}).kind != ':' {
				t.Fatal("writes not restored after reopen")
			}
		})
	}
}

// The test executable is also a server subprocess so SIGKILL exercises the
// actual persistence path without relying on shutdown hooks or shell tools.
func TestAOFProcessHelper(t *testing.T) {
	path := os.Getenv("MINI_REDIS_TEST_AOF")
	if path == "" {
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(listener.Addr().String())
	var options Options
	options.AOFPath = path
	if limit := os.Getenv("MINI_REDIS_TEST_LIMIT"); limit != "" {
		options.MaxBytes, err = strconv.ParseInt(limit, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := ServeWithOptions(context.Background(), listener, options); err != nil {
		t.Fatal(err)
	}
}

func launchAOFProcess(t *testing.T, path string) (string, func()) {
	return launchAOFProcessEnv(t, path)
}

func launchAOFProcessEnv(t *testing.T, path string, env ...string) (string, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAOFProcessHelper$")
	cmd.Env = append(os.Environ(), "MINI_REDIS_TEST_AOF="+path)
	cmd.Env = append(cmd.Env, env...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	kill := func() {
		if !stopped {
			stopped = true
			cmd.Process.Kill() // SIGKILL on Linux: no defers or graceful shutdown.
			cmd.Wait()
		}
	}
	t.Cleanup(kill)
	address := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			address <- scanner.Text()
		} else {
			address <- ""
		}
	}()
	select {
	case addr := <-address:
		if !strings.Contains(addr, ":") {
			kill()
			t.Fatalf("child failed: %s", stderr.String())
		}
		return addr, kill
	case <-time.After(5 * time.Second):
		kill()
		t.Fatal("child did not start")
	}
	return "", kill
}

func TestAcknowledgedWritesSurviveSIGKILL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	addr, kill := launchAOFProcess(t, path)
	conn := dialTestServer(t, addr)
	steps := []struct {
		args []string
		want string
	}{
		{[]string{"HSET", "user", "name", "old", "age", "20"}, ":2\r\n"},
		{[]string{"HSET", "user", "name", "张三"}, ":0\r\n"},
		{[]string{"HDEL", "user", "age"}, ":1\r\n"},
		{[]string{"HSET", "deleted", "f", "v"}, ":1\r\n"},
		{[]string{"DEL", "deleted"}, ":1\r\n"},
		{[]string{"HSET", "last-field", "f", "v"}, ":1\r\n"},
		{[]string{"SAVE"}, "+OK\r\n"},
		{[]string{"HDEL", "last-field", "f"}, ":1\r\n"},
	}
	for _, step := range steps {
		if _, err := io.WriteString(conn, request(step.args...)); err != nil {
			t.Fatal(err)
		}
		expectBytes(t, conn, step.want)
	}
	kill()
	addr, kill = launchAOFProcess(t, path)
	conn = dialTestServer(t, addr)
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"HGET", "user", "name"}, "$6\r\n张三\r\n"},
		{[]string{"HGET", "user", "age"}, "$-1\r\n"},
		{[]string{"HLEN", "user"}, ":1\r\n"},
		{[]string{"HLEN", "deleted"}, ":0\r\n"},
		{[]string{"DEL", "last-field"}, ":0\r\n"},
		{[]string{"HSET", "after-restart", "f", "v"}, ":1\r\n"},
		{[]string{"SAVE"}, "+OK\r\n"},
	} {
		if _, err := io.WriteString(conn, request(step.args...)); err != nil {
			t.Fatal(err)
		}
		expectBytes(t, conn, step.want)
	}
	kill()
	addr, kill = launchAOFProcess(t, path)
	defer kill()
	conn = dialTestServer(t, addr)
	if _, err := io.WriteString(conn, request("HGET", "after-restart", "f")); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, conn, "$1\r\nv\r\n")
}

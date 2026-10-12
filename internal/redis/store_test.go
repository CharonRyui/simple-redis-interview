package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func wire(t *testing.T, v response) string {
	t.Helper()
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	if err := v.write(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestCommandSemantics(t *testing.T) {
	s := newStore()
	steps := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "+PONG\r\n"},
		{[]string{"ping", "hello"}, "$5\r\nhello\r\n"},
		{[]string{"HGET", "user:1", "name"}, "$-1\r\n"},
		{[]string{"HGETALL", "user:1"}, "*0\r\n"},
		{[]string{"HLEN", "user:1"}, ":0\r\n"},
		{[]string{"HEXISTS", "user:1", "name"}, ":0\r\n"},
		{[]string{"HDEL", "user:1", "name"}, ":0\r\n"},
		{[]string{"DEL", "user:1"}, ":0\r\n"},
		{[]string{"hset", "user:1", "name", "张三", "age", "20"}, ":2\r\n"},
		{[]string{"HGET", "user:1", "name"}, "$6\r\n张三\r\n"},
		{[]string{"HSET", "user:1", "age", "21"}, ":0\r\n"},
		{[]string{"HGET", "user:1", "age"}, "$2\r\n21\r\n"},
		{[]string{"HSET", "user:1", "empty", ""}, ":1\r\n"},
		{[]string{"HGET", "user:1", "empty"}, "$0\r\n\r\n"},
		{[]string{"HEXISTS", "user:1", "empty"}, ":1\r\n"},
		{[]string{"HLEN", "user:1"}, ":3\r\n"},
		{[]string{"HSET", "user:1", "dup", "first", "dup", "last"}, ":1\r\n"},
		{[]string{"HGET", "user:1", "dup"}, "$4\r\nlast\r\n"},
		{[]string{"HDEL", "user:1", "dup", "dup", "absent"}, ":1\r\n"},
		{[]string{"HSET", "user:1", "name", "bad", "dangling"}, "-ERR wrong number of arguments for 'hset' command\r\n"},
		{[]string{"HGET", "user:1", "name"}, "$6\r\n张三\r\n"},
		{[]string{"HDEL", "user:1", "name", "age", "empty"}, ":3\r\n"},
		{[]string{"DEL", "user:1"}, ":0\r\n"}, // Last field removed the key.
		{[]string{"HSET", "a", "f", "1"}, ":1\r\n"},
		{[]string{"HSET", "b", "f", "2"}, ":1\r\n"},
		{[]string{"DEL", "a", "a", "b", "missing"}, ":2\r\n"},
		{[]string{"HSET", "", "", "\x00\r\n"}, ":1\r\n"},
		{[]string{"HGET", "", ""}, "$3\r\n\x00\r\n\r\n"},
		{[]string{"SET", "a", "b"}, "-ERR unknown command\r\n"},
	}
	for i, step := range steps {
		if got := wire(t, s.execute(step.args)); got != step.want {
			t.Fatalf("step %d (%q): got %q, want %q", i, step.args, got, step.want)
		}
	}
}

func TestInvalidArityDoesNotModifyData(t *testing.T) {
	s := newStore()
	for _, args := range [][]string{
		{}, {"PING", "a", "b"}, {"HSET"}, {"HSET", "k", "f"},
		{"HGET", "k"}, {"HGET", "k", "f", "extra"}, {"HDEL", "k"},
		{"HEXISTS", "k"}, {"HLEN"}, {"HLEN", "k", "extra"},
		{"HGETALL"}, {"HGETALL", "k", "extra"}, {"DEL"},
	} {
		if got := s.execute(args); got.kind != '-' {
			t.Fatalf("expected error for %q, got %+v", args, got)
		}
	}
	if len(s.hashes) != 0 {
		t.Fatal("invalid commands mutated the store")
	}
}

func TestHashIsolationAndSnapshot(t *testing.T) {
	s := newStore()
	s.execute([]string{"HSET", "a", "f", "1", "g", "2"})
	s.execute([]string{"HSET", "b", "f", "3"})
	snapshot := s.execute([]string{"HGETALL", "a"})
	s.execute([]string{"HSET", "a", "f", "changed"})
	s.execute([]string{"DEL", "a"})
	got := make(map[string]string)
	for i := 0; i < len(snapshot.items); i += 2 {
		got[snapshot.items[i].text] = snapshot.items[i+1].text
	}
	if len(got) != 2 || got["f"] != "1" || got["g"] != "2" {
		t.Fatalf("snapshot changed: %v", got)
	}
	if v := s.execute([]string{"HGET", "b", "f"}); v.text != "3" {
		t.Fatal("operation on a affected b")
	}
}

func TestConcurrentCommandsAreAtomic(t *testing.T) {
	s := newStore()
	s.execute([]string{"HSET", "shared", "a", "initial", "b", "initial"})
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				value := fmt.Sprintf("%d:%d", id, i)
				s.execute([]string{"HSET", "shared", "a", value, "b", value})
				snapshot := s.execute([]string{"HGETALL", "shared"})
				if len(snapshot.items) != 4 || snapshot.items[1].text != snapshot.items[3].text {
					t.Error("observed a partially applied multi-field HSET")
					return
				}
				field := fmt.Sprintf("worker:%d:%d", id, i)
				s.execute([]string{"HSET", "other", field, value})
				s.execute([]string{"HGET", "other", field})
				s.execute([]string{"HDEL", "other", field})
				s.execute([]string{"DEL", field})
			}
		}(worker)
	}
	wg.Wait()
	if got := s.execute([]string{"HLEN", "other"}); got.n != 0 {
		t.Fatalf("expected all temporary fields deleted, got %d", got.n)
	}
}

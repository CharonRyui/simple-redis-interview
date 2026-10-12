package redis

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

func TestStorageLimitAtomicityAndReclaim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openPersistence(path, "", 132) // key k: 65, field f/value 12: 67
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if got := s.execute([]string{"HSET", "k", "f", "12"}); got.kind != ':' || got.n != 1 {
		t.Fatal(got)
	}
	if s.usedBytes != 132 {
		t.Fatalf("used %d, want 132", s.usedBytes)
	}
	position, _ := a.file.Seek(0, io.SeekCurrent)
	for _, args := range [][]string{
		{"HSET", "k", "f", "123"},
		{"HSET", "k", "f", "x", "g", ""}, // shrinking one field cannot fit another
		{"HSET", "new", "f", ""},
	} {
		if got := s.execute(args); got.kind != '-' || got.text != "OOM storage limit exceeded" {
			t.Fatalf("expected OOM for %q: %+v", args, got)
		}
		if s.execute([]string{"HGET", "k", "f"}).text != "12" || s.usedBytes != 132 {
			t.Fatal("rejected command modified data or accounting")
		}
	}
	if after, _ := a.file.Seek(0, io.SeekCurrent); after != position {
		t.Fatal("rejected command was appended to AOF")
	}
	// Repeated fields are counted by final value, not intermediate allocations.
	if got := s.execute([]string{"HSET", "k", "f", "too long", "f", ""}); got.kind != ':' || got.n != 0 {
		t.Fatal(got)
	}
	if s.usedBytes != 130 {
		t.Fatalf("shrink/repeated-field accounting: %d", s.usedBytes)
	}
	if s.execute([]string{"HDEL", "k", "f", "f"}).n != 1 || s.usedBytes != 0 {
		t.Fatal("last-field deletion did not reclaim key and field exactly once")
	}
	if s.execute([]string{"HSET", "", "", ""}).n != 1 || s.usedBytes != 128 {
		t.Fatal("empty strings must still have accounted entry overhead")
	}
	if s.execute([]string{"DEL", "", ""}).n != 1 || s.usedBytes != 0 {
		t.Fatal("DEL failed to reclaim storage exactly once")
	}
	if s.execute([]string{"HSET", "k", "f", "12"}).kind != ':' {
		t.Fatal("capacity was not reusable after deletion")
	}
}

func TestCapacityUsesFinalStateOfMultiFieldUpdate(t *testing.T) {
	s := newStore()
	s.execute([]string{"HSET", "k", "a", "12345", "b", ""})
	s.maxBytes = s.usedBytes
	// Growing a while shrinking b (or the reverse) must not depend on order.
	for _, args := range [][]string{
		{"HSET", "k", "b", "12345", "a", ""},
		{"HSET", "k", "a", "12345", "b", ""},
	} {
		if s.execute(args).kind != ':' || s.usedBytes != s.maxBytes {
			t.Fatal("atomic replacement with unchanged final size was rejected")
		}
	}
	if s.execute([]string{"HDEL", "k", "a"}).n != 1 {
		t.Fatal("HDEL failed")
	}
	if s.usedBytes != keyBytes("k")+fieldBytes("b", "") {
		t.Fatal("HDEL accounting incorrect")
	}
	s.execute([]string{"DEL", "k"})
	if s.usedBytes != 0 {
		t.Fatal("DEL remaining fields accounting incorrect")
	}
}

func TestLimitRecoveryChecksFinalState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "k", "f", "larger historical value"})
	s.execute([]string{"HSET", "k", "f", "12"})
	a.file.Close()
	if a, _, err := openPersistence(path, "", 131); err == nil {
		a.file.Close()
		t.Fatal("accepted recovered data over configured limit")
	}
	a, s, err = openPersistence(path, "", 132)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.usedBytes != 132 || s.execute([]string{"HGET", "k", "f"}).text != "12" {
		t.Fatal("recovery dropped data or applied limit to historical writes")
	}
	if s.execute([]string{"HSET", "k", "f", "123"}).kind != '-' {
		t.Fatal("limit not enforced after recovery")
	}
}

func TestConcurrentWritersCannotOverrunLimit(t *testing.T) {
	s := newStore()
	// Each unique Hash occupies exactly 130 bytes.
	s.maxBytes = 130
	var wg sync.WaitGroup
	success := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if v := s.execute([]string{"HSET", fmt.Sprint(i), "f", ""}); v.kind == ':' {
				success <- struct{}{}
			}
		}(i)
	}
	wg.Wait()
	if len(success) != 1 || len(s.hashes) != 1 || s.usedBytes != 130 {
		t.Fatalf("overrun: successes=%d keys=%d used=%d", len(success), len(s.hashes), s.usedBytes)
	}
}

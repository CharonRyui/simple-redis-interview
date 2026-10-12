package redis

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSnapshotRoundTripWithAOFChangesAndBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, DefaultMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"HSET", "k", "name", "张三\x00\r\n", "age", "20"},
		{"HSET", "deleted", "f", "v"},
		{"HSET", "", "", ""},
	} {
		s.execute(args)
	}
	if got := wire(t, s.execute([]string{"save"})); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if _, err := os.Stat(snap); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"HSET", "k", "name", "new"},
		{"HDEL", "k", "age"},
		{"DEL", "deleted"},
		{"HSET", "later", "f", "v"},
	} {
		s.execute(args)
	}
	used := s.usedBytes
	a.file.Close()
	a, s, err = openPersistence(path, snap, used)
	if err != nil {
		t.Fatal(err)
	}
	if s.execute([]string{"HGET", "k", "name"}).text != "new" || s.execute([]string{"HLEN", "k"}).n != 1 || s.execute([]string{"HLEN", "deleted"}).n != 0 || s.execute([]string{"HGET", "later", "f"}).text != "v" {
		t.Fatal("snapshot plus tail did not restore final state")
	}
	if s.usedBytes != used || s.execute([]string{"HSET", "extra", "f", "v"}).kind != '-' {
		t.Fatal("budget not restored")
	}
	if s.execute([]string{"HGET", "", ""}).kind != '$' {
		t.Fatal("empty field lost")
	}
	if s.execute([]string{"SAVE"}).kind != '+' {
		t.Fatal("could not replace existing snapshot")
	}
	a.file.Close()
	a, s, err = openPersistence(path, snap, used)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.usedBytes != used {
		t.Fatal("second snapshot changed accounting")
	}
}

func TestSnapshotCheckpointVerifiesAOFPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "old", "f", "discarded"})
	s.execute([]string{"DEL", "old"})
	s.execute([]string{"HSET", "snapshot", "f", "kept"})
	if s.execute([]string{"SAVE"}).kind != '+' {
		t.Fatal("save failed")
	}
	s.execute([]string{"HSET", "later", "f", "tail"})
	a.file.Close()
	a, s, err = openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.execute([]string{"HLEN", "old"}).n != 0 || s.execute([]string{"HGET", "snapshot", "f"}).text != "kept" || s.execute([]string{"HGET", "later", "f"}).text != "tail" {
		t.Fatal("checkpoint prefix was replayed or suffix was skipped")
	}
	a.file.Close()
	contents, _ := os.ReadFile(path)
	contents[len(aofMagic)+16] ^= 1
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	if a, _, err := openPersistence(path, snap, 0); err == nil {
		a.file.Close()
		t.Fatal("snapshot accepted with mismatched AOF prefix")
	}
}

func TestSnapshotCorruptionAndTruncationRefuseStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "k", "f", "v"})
	if s.execute([]string{"SAVE"}).kind != '+' {
		t.Fatal("save failed")
	}
	a.file.Close()
	original, _ := os.ReadFile(snap)
	var cases [][]byte
	for _, n := range []int{0, 7, 55, len(original) - 1} {
		cases = append(cases, append([]byte(nil), original[:n]...))
	}
	for _, n := range []int{0, 8, 16, 48, 60, len(original) - 1} {
		bad := append([]byte(nil), original...)
		bad[n] ^= 1
		cases = append(cases, bad)
	}
	// Even with a valid overall checksum, malformed metadata must be rejected.
	for _, n := range []int{0, 8, 48} {
		bad := append([]byte(nil), original...)
		bad[n] ^= 0xff
		sum := sha256.Sum256(bad[:len(bad)-sha256.Size])
		copy(bad[len(bad)-sha256.Size:], sum[:])
		cases = append(cases, bad)
	}
	for i, bad := range cases {
		if err := os.WriteFile(snap, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if a, _, err := openPersistence(path, snap, 0); err == nil {
			a.file.Close()
			t.Fatalf("accepted bad snapshot %d", i)
		}
		after, _ := os.ReadFile(snap)
		if !bytes.Equal(after, bad) {
			t.Fatal("modified corrupt snapshot")
		}
	}
	// Removing the damaged checkpoint recovers from the still-complete AOF.
	if err := os.Remove(snap); err != nil {
		t.Fatal(err)
	}
	a, s, err = openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.execute([]string{"HGET", "k", "f"}).text != "v" {
		t.Fatal("AOF fallback lost data")
	}
}

func TestFailedSnapshotPublicationPreservesPreviousCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.snap")
	if err := atomicSnapshot(path, func(w io.Writer) error { return writeFull(w, []byte("old")) }); err != nil {
		t.Fatal(err)
	}
	err := atomicSnapshot(path, func(w io.Writer) error {
		writeFull(w, []byte("partial replacement"))
		return errors.New("injected write failure")
	})
	if err == nil {
		t.Fatal("failed writer returned success")
	}
	got, _ := os.ReadFile(path)
	if string(got) != "old" {
		t.Fatal("failed save replaced previous snapshot")
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatal("failed save left temporary files")
	}
}

func TestSnapshotFailureDoesNotDisableWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openPersistence(path, path+".snap", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.execute([]string{"SAVE", "extra"}).kind != '-' {
		t.Fatal("SAVE accepted extra argument")
	}
	s.save = func(s *store) error {
		return a.saveSnapshot(filepath.Join(filepath.Dir(path), "missing", "data.snap"), s)
	}
	if s.execute([]string{"SAVE"}).kind != '-' {
		t.Fatal("failed SAVE returned success")
	}
	if s.execute([]string{"HSET", "k", "f", "v"}).kind != ':' {
		t.Fatal("snapshot failure disabled AOF writes")
	}
	if newStore().execute([]string{"SAVE"}).kind != '-' {
		t.Fatal("in-memory mode cannot persist snapshots")
	}
}

func TestSnapshotAndAOFPathsCannotAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, _, err := openAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	a.file.Close()
	for _, link := range []string{"hard", "soft"} {
		alias := filepath.Join(filepath.Dir(path), link)
		if link == "hard" {
			err = os.Link(path, alias)
		} else {
			err = os.Symlink(path, alias)
		}
		if err != nil {
			t.Fatal(err)
		}
		if a, _, err := openPersistence(path, alias, 0); err == nil {
			a.file.Close()
			t.Fatal("accepted aliased snapshot path")
		}
	}
	if a, _, err := openPersistence(path, path, 0); err == nil {
		a.file.Close()
		t.Fatal("accepted identical paths")
	}
}

func TestEmptyAndLargeHashSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.execute([]string{"SAVE"}).kind != '+' {
		t.Fatal("empty snapshot failed")
	}
	// More fields than one request permits; snapshot records must be independent
	// of Hash size. This checks format limits, not throughput.
	for start := 0; start < maxArguments; start += 400 {
		args := []string{"HSET", "k"}
		for i := start; i < start+400 && i < maxArguments; i++ {
			args = append(args, fmt.Sprint(i), "v")
		}
		if s.execute(args).kind != ':' {
			t.Fatal("write failed")
		}
	}
	used := s.usedBytes
	if s.execute([]string{"SAVE"}).kind != '+' {
		t.Fatal("large Hash snapshot failed")
	}
	a.file.Close()
	a, s, err = openPersistence(path, snap, used)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.execute([]string{"HLEN", "k"}).n != maxArguments || s.usedBytes != used {
		t.Fatal("large Hash lost fields")
	}
}

func TestConcurrentSnapshotsAndWritesRecoverConsistently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for id := 0; id < 3; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 6; i++ {
				v := fmt.Sprintf("%d:%d", id, i)
				if s.execute([]string{"HSET", "k", "a", v, "b", v}).kind != ':' {
					t.Error("write failed")
				}
				if id == 0 && s.execute([]string{"SAVE"}).kind != '+' {
					t.Error("SAVE failed")
				}
			}
		}(id)
	}
	wg.Wait()
	last := s.execute([]string{"HGET", "k", "a"}).text
	a.file.Close()
	a, s, err = openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	for _, field := range []string{"a", "b"} {
		if s.execute([]string{"HGET", "k", field}).text != last {
			t.Fatal("snapshot/log order diverged")
		}
	}
}

func TestSnapshotOffsetRejectsShorterAOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "k", "f", strings.Repeat("v", 100)})
	s.execute([]string{"SAVE"})
	a.file.Close()
	if err := os.Truncate(path, int64(len(aofMagic))); err != nil {
		t.Fatal(err)
	}
	if a, _, err := openPersistence(path, snap, 0); err == nil {
		a.file.Close()
		t.Fatal("snapshot accepted with truncated AOF")
	}
}

func TestSnapshotRejectsDuplicateFieldsWithValidChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "k", "f", "v"})
	s.execute([]string{"SAVE"})
	a.file.Close()
	contents, _ := os.ReadFile(snap)
	body := contents[:len(contents)-sha256.Size]
	duplicate := append([]byte(nil), body...)
	duplicate = append(duplicate, body[56:]...)
	binary.BigEndian.PutUint64(duplicate[48:56], 2)
	sum := sha256.Sum256(duplicate)
	duplicate = append(duplicate, sum[:]...)
	if err := os.WriteFile(snap, duplicate, 0600); err != nil {
		t.Fatal(err)
	}
	if a, _, err := openPersistence(path, snap, 0); err == nil {
		a.file.Close()
		t.Fatal("duplicate field accepted")
	}
}

// Stop after flushing a partial temporary snapshot but before publication.
// The real atomicSnapshot writer is used; a pipe synchronizes SIGKILL timing.
func TestSnapshotInterruptedProcessHelper(t *testing.T) {
	path := os.Getenv("MINI_REDIS_TEST_INTERRUPTED_SNAPSHOT")
	if path == "" {
		return
	}
	err := atomicSnapshot(path, func(w io.Writer) error {
		if err := writeFull(w, []byte("incomplete new checkpoint")); err != nil {
			return err
		}
		if err := w.(*bufio.Writer).Flush(); err != nil {
			return err
		}
		fmt.Println("partial snapshot flushed")
		select {}
	})
	t.Fatal(err)
}

func TestSIGKILLDuringSnapshotPublicationPreservesData(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint("existing=", existing), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.aof")
			snap := path + ".snap"
			a, s, err := openPersistence(path, snap, 0)
			if err != nil {
				t.Fatal(err)
			}
			s.execute([]string{"HSET", "k", "f", "before"})
			if existing && s.execute([]string{"SAVE"}).kind != '+' {
				t.Fatal("save failed")
			}
			s.execute([]string{"HSET", "k", "f", "acknowledged"})
			a.file.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestSnapshotInterruptedProcessHelper$")
			cmd.Env = append(os.Environ(), "MINI_REDIS_TEST_INTERRUPTED_SNAPSHOT="+snap)
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			stopped := false
			kill := func() {
				if !stopped {
					stopped = true
					cmd.Process.Kill()
					cmd.Wait()
				}
			}
			defer kill()
			ready := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(output)
				ready <- scanner.Scan() && scanner.Text() == "partial snapshot flushed"
			}()
			select {
			case ok := <-ready:
				if !ok {
					t.Fatal("child failed before partial write")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("child snapshot timed out")
			}
			kill()
			a, s, err = openPersistence(path, snap, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer a.file.Close()
			if s.execute([]string{"HGET", "k", "f"}).text != "acknowledged" {
				t.Fatal("interrupted snapshot lost acknowledged AOF data")
			}
			if s.execute([]string{"SAVE"}).kind != '+' {
				t.Fatal("unable to save after interrupted snapshot")
			}
		})
	}
}

func TestTCPStorageLimitSnapshotAndSIGKILL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	addr, kill := launchAOFProcessEnv(t, path, "MINI_REDIS_TEST_LIMIT=132")
	conn := dialTestServer(t, addr)
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"HSET", "k", "f", "12"}, ":1\r\n"},
		{[]string{"HSET", "k", "f", "long"}, "-OOM storage limit exceeded\r\n"},
		{[]string{"SAVE"}, "+OK\r\n"},
		{[]string{"PING"}, "+PONG\r\n"},
	} {
		if _, err := io.WriteString(conn, request(step.args...)); err != nil {
			t.Fatal(err)
		}
		expectBytes(t, conn, step.want)
	}
	kill()
	addr, kill = launchAOFProcessEnv(t, path, "MINI_REDIS_TEST_LIMIT=132")
	defer kill()
	conn = dialTestServer(t, addr)
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"HGET", "k", "f"}, "$2\r\n12\r\n"},
		{[]string{"HSET", "k", "g", ""}, "-OOM storage limit exceeded\r\n"},
		{[]string{"DEL", "k"}, ":1\r\n"},
		{[]string{"HSET", "n", "f", "12"}, ":1\r\n"},
	} {
		if _, err := io.WriteString(conn, request(step.args...)); err != nil {
			t.Fatal(err)
		}
		expectBytes(t, conn, step.want)
	}
}

func TestRecoveryBeginsAtSnapshotOffsetAndRepairsTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	snap := path + ".snap"
	a, s, err := openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.execute([]string{"HSET", "k", "f", "before"})
	s.execute([]string{"SAVE"})
	contents, err := os.ReadFile(snap)
	if err != nil {
		t.Fatal(err)
	}
	expected := int64(binary.BigEndian.Uint64(contents[8:16]))
	loaded := newStore()
	if err := a.loadSnapshot(snap, loaded); err != nil {
		t.Fatal(err)
	}
	if position, _ := a.file.Seek(0, io.SeekCurrent); position != expected {
		t.Fatal("loader did not seek to checkpoint offset")
	}
	s.execute([]string{"HSET", "k", "f", "tail"})
	boundary, _ := a.file.Seek(0, io.SeekCurrent)
	a.file.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	a, s, err = openPersistence(path, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	if s.execute([]string{"HGET", "k", "f"}).text != "tail" {
		t.Fatal("tail repair lost complete suffix")
	}
	if position, _ := a.file.Seek(0, io.SeekCurrent); position != boundary {
		t.Fatal("tail repair used wrong offset")
	}
}

func TestFailedSnapshotHashLeavesAppendPositionUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	a, s, err := openPersistence(path, path+".snap", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.file.Close()
	s.execute([]string{"HSET", "k", "f", "v"})
	before, _ := a.file.Seek(0, io.SeekCurrent)
	if _, err := a.prefixHash(before + 1); err == nil {
		t.Fatal("hash accepted missing bytes")
	}
	if after, _ := a.file.Seek(0, io.SeekCurrent); after != before {
		t.Fatal("failed hash changed AOF append cursor")
	}
	if s.execute([]string{"HSET", "k", "f", "after"}).kind != ':' {
		t.Fatal("write failed after hash error")
	}
}

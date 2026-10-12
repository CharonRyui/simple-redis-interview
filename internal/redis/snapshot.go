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
	"path/filepath"
)

var snapshotMagic = []byte("MRSNAP01")

// A snapshot checkpoints an immutable prefix of the full AOF. Keeping that AOF
// makes publishing a snapshot a single atomic rename, with no two-file commit.
// Snapshot files are caches of state, not independent replacements for the AOF.
func (a *aof) saveSnapshot(path string, data *store) error {
	offset, err := a.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	prefix, err := a.prefixHash(offset)
	if err != nil {
		return err
	}
	return atomicSnapshot(path, func(w io.Writer) error {
		digest := sha256.New()
		out := io.MultiWriter(w, digest)
		if err := writeFull(out, snapshotMagic); err != nil {
			return err
		}
		if err := binary.Write(out, binary.BigEndian, uint64(offset)); err != nil {
			return err
		}
		if err := writeFull(out, prefix); err != nil {
			return err
		}
		var count uint64
		for _, hash := range data.hashes {
			count += uint64(len(hash))
		}
		if err := binary.Write(out, binary.BigEndian, count); err != nil {
			return err
		}
		for key, hash := range data.hashes {
			for field, value := range hash {
				// One field per record avoids limits on the total size of a Hash.
				if err := writeRecord(out, []string{"HSET", key, field, value}); err != nil {
					return err
				}
			}
		}
		return writeFull(w, digest.Sum(nil))
	})
}

func atomicSnapshot(path string, write func(io.Writer) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".mini-redis-snapshot-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	defer f.Close()
	w := bufio.NewWriter(f)
	if err := write(w); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (a *aof) prefixHash(offset int64) ([]byte, error) {
	if offset < int64(len(aofMagic)) {
		return nil, errors.New("snapshot offset precedes AOF header")
	}
	digest := sha256.New()
	// ReaderAt leaves the append cursor unchanged even when hashing fails.
	if _, err := io.CopyN(digest, io.NewSectionReader(a.file, 0, offset), offset); err != nil {
		return nil, fmt.Errorf("snapshot AOF prefix: %w", err)
	}
	return digest.Sum(nil), nil
}

func (a *aof) loadSnapshot(path string, data *store) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	const headerBytes = 8 + 8 + sha256.Size + 8
	if info.Size() < headerBytes+sha256.Size {
		return errors.New("truncated snapshot")
	}
	bodySize := info.Size() - sha256.Size
	digest := sha256.New()
	if _, err := io.CopyN(digest, f, bodySize); err != nil {
		return err
	}
	var checksum [sha256.Size]byte
	if _, err := io.ReadFull(f, checksum[:]); err != nil {
		return err
	}
	if !bytes.Equal(digest.Sum(nil), checksum[:]) {
		return errors.New("snapshot checksum mismatch")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(io.LimitReader(f, bodySize))
	var header [headerBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	if !bytes.Equal(header[:8], snapshotMagic) {
		return errors.New("unsupported snapshot version")
	}
	offset := binary.BigEndian.Uint64(header[8:16])
	if offset > uint64(1<<63-1) {
		return errors.New("invalid snapshot AOF offset")
	}
	prefix, err := a.prefixHash(int64(offset))
	if err != nil {
		return err
	}
	if !bytes.Equal(prefix, header[16:48]) {
		return errors.New("snapshot does not match AOF prefix")
	}
	count := binary.BigEndian.Uint64(header[48:])
	loaded := newStore()
	for i := uint64(0); i < count; i++ {
		args, _, err := readRecord(r)
		if err != nil {
			return fmt.Errorf("snapshot record %d: %w", i, err)
		}
		if len(args) != 4 || args[0] != "HSET" || loaded.execute(args).n != 1 {
			return errors.New("invalid or duplicate snapshot field")
		}
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected snapshot trailing data")
	}
	if _, err := a.file.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	data.hashes, data.usedBytes = loaded.hashes, loaded.usedBytes
	return nil
}

func checkSnapshotPaths(aofPath, snapshotPath string) error {
	canonical := func(path string) (string, error) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		dir, err := filepath.EvalSymlinks(filepath.Dir(abs))
		if err != nil {
			return "", err
		}
		return filepath.Join(dir, filepath.Base(abs)), nil
	}
	a, err := canonical(aofPath)
	if err != nil {
		return err
	}
	s, err := canonical(snapshotPath)
	if err != nil {
		return err
	}
	if a == s {
		return errors.New("snapshot and AOF paths must differ")
	}
	snapshotInfo, err := os.Lstat(s)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !snapshotInfo.Mode().IsRegular() {
		return errors.New("snapshot must be a regular file, not a symlink")
	}
	aofInfo, err := os.Stat(a)
	if err != nil {
		return fmt.Errorf("snapshot exists but AOF is unavailable: %w", err)
	}
	if os.SameFile(snapshotInfo, aofInfo) {
		return errors.New("snapshot and AOF refer to the same file")
	}
	return nil
}

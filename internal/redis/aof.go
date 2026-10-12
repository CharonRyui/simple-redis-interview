package redis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"syscall"
)

var aofMagic = []byte("MRHASH01")

// journal is called only while holding the store's write lock.
type journal interface {
	append([]string) error
}

type logFile interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
	Truncate(int64) error
	Sync() error
	Close() error
}

type aof struct{ file logFile }

func openAOF(path string) (*aof, *store, error) {
	return openPersistence(path, "", 0)
}

func openPersistence(path, snapshotPath string, maxBytes int64) (*aof, *store, error) {
	if snapshotPath != "" {
		if err := checkSnapshotPaths(path, snapshotPath); err != nil {
			return nil, nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*aof, *store, error) {
		f.Close()
		return nil, nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(fmt.Errorf("lock AOF (another process may own it): %w", err))
	}
	header := make([]byte, len(aofMagic))
	n, err := io.ReadFull(f, header)
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return fail(err)
		}
		if !bytes.Equal(header[:n], aofMagic[:n]) {
			return fail(errors.New("invalid AOF header"))
		}
		// A crash during first-time initialization cannot have acknowledged writes.
		if err := f.Truncate(0); err != nil {
			return fail(err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fail(err)
		}
		if err := writeFull(f, aofMagic); err != nil {
			return fail(err)
		}
		if err := f.Sync(); err != nil {
			return fail(err)
		}
	} else if !bytes.Equal(header, aofMagic) {
		return fail(errors.New("unsupported or invalid AOF version"))
	}
	data := newStore()
	log := &aof{file: f}
	if snapshotPath != "" {
		if err := log.loadSnapshot(snapshotPath, data); err != nil {
			return fail(err)
		}
	}
	if err := log.replay(data); err != nil {
		return fail(err)
	}
	if maxBytes > 0 && data.usedBytes > maxBytes {
		return fail(fmt.Errorf("recovered storage uses %d bytes, exceeds limit %d", data.usedBytes, maxBytes))
	}
	data.maxBytes = maxBytes
	data.journal = log
	if snapshotPath != "" {
		data.save = func(s *store) error { return log.saveSnapshot(snapshotPath, s) }
	}
	return log, data, nil
}

func (a *aof) replay(data *store) error {
	offset, err := a.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	for {
		args, size, err := readRecord(a.file)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return a.trimTail(offset)
		}
		if err != nil {
			return fmt.Errorf("AOF at offset %d: %w", offset, err)
		}
		if !isWrite(args[0]) || data.execute(args).kind != ':' {
			return fmt.Errorf("invalid AOF command at offset %d", offset)
		}
		offset += size
	}
}

func (a *aof) trimTail(offset int64) error {
	if err := a.file.Truncate(offset); err != nil {
		return err
	}
	if _, err := a.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return a.file.Sync()
}

func (a *aof) append(args []string) error {
	if err := writeRecord(a.file, args); err != nil {
		return err
	}
	return a.file.Sync()
}

func writeRecord(w io.Writer, args []string) error {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint32(len(args)))
	for _, arg := range args {
		binary.Write(&payload, binary.BigEndian, uint32(len(arg)))
		payload.WriteString(arg)
	}
	if payload.Len() > maxRequestBytes {
		return errors.New("AOF record too large")
	}
	var header [12]byte
	binary.BigEndian.PutUint32(header[:4], uint32(payload.Len()))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload.Bytes()))
	binary.BigEndian.PutUint32(header[8:], crc32.ChecksumIEEE(header[:8]))
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, payload.Bytes())
}

func readRecord(r io.Reader) ([]string, int64, error) {
	// Protect the length itself, so corrupt lengths cannot masquerade as tails.
	var header [12]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if crc32.ChecksumIEEE(header[:8]) != binary.BigEndian.Uint32(header[8:]) || length > maxRequestBytes || length < 4 {
		return nil, 0, errors.New("corrupt record header")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, incomplete(err)
	}
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:8]) {
		return nil, 0, errors.New("corrupt record payload")
	}
	args, err := decodeRecord(payload)
	return args, int64(len(header)) + int64(length), err
}

func writeFull(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

func decodeRecord(payload []byte) ([]string, error) {
	r := bytes.NewReader(payload)
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil || count < 1 || count > maxArguments {
		return nil, errProtocol
	}
	args := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil || uint64(length) > uint64(r.Len()) {
			return nil, errProtocol
		}
		start := len(payload) - r.Len()
		args = append(args, string(payload[start:start+int(length)]))
		r.Seek(int64(length), io.SeekCurrent)
	}
	if r.Len() != 0 {
		return nil, errProtocol
	}
	return args, nil
}

func isWrite(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "HSET", "HDEL", "DEL":
		return true
	}
	return false
}

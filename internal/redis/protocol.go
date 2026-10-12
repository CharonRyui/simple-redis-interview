package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	maxRequestBytes = 8 << 20
	maxArguments    = 1024
	maxHeaderBytes  = 64
)

var errProtocol = errors.New("invalid or oversized RESP request")

// readCommand accepts RESP2 arrays of non-null bulk strings. Each call consumes
// exactly one request, so fragmented packets and pipelined requests work alike.
func readCommand(r *bufio.Reader) ([]string, error) {
	remaining := maxRequestBytes
	header, err := readHeader(r, &remaining)
	if err != nil {
		return nil, err
	}
	count, err := parseLength(header, '*')
	if err != nil || count < 1 || count > maxArguments {
		return nil, errProtocol
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err = readHeader(r, &remaining)
		if err != nil {
			return nil, incomplete(err)
		}
		n, err := parseLength(header, '$')
		if err != nil || n > remaining-2 {
			return nil, errProtocol
		}
		data := make([]byte, n+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, incomplete(err)
		}
		remaining -= len(data)
		if data[n] != '\r' || data[n+1] != '\n' {
			return nil, errProtocol
		}
		args = append(args, string(data[:n]))
	}
	return args, nil
}

func readHeader(r *bufio.Reader, remaining *int) (string, error) {
	line := make([]byte, 0, maxHeaderBytes)
	for len(line) < maxHeaderBytes && *remaining > 0 {
		b, err := r.ReadByte()
		if err != nil {
			if len(line) > 0 {
				return "", incomplete(err)
			}
			return "", err
		}
		*remaining -= 1
		line = append(line, b)
		if b == '\n' {
			if len(line) < 2 || line[len(line)-2] != '\r' {
				return "", errProtocol
			}
			return string(line[:len(line)-2]), nil
		}
	}
	return "", errProtocol
}

func parseLength(header string, prefix byte) (int, error) {
	if len(header) < 2 || header[0] != prefix {
		return 0, errProtocol
	}
	for i := 1; i < len(header); i++ {
		if header[i] < '0' || header[i] > '9' {
			return 0, errProtocol
		}
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil {
		return 0, errProtocol
	}
	return n, nil
}

func incomplete(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

type response struct {
	kind  byte
	text  string
	n     int
	items []response
}

func integer(n int) response    { return response{kind: ':', n: n} }
func bulk(s string) response    { return response{kind: '$', text: s} }
func failure(s string) response { return response{kind: '-', text: s} }

func (v response) write(w *bufio.Writer) error {
	var err error
	switch v.kind {
	case '+', '-':
		_, err = fmt.Fprintf(w, "%c%s\r\n", v.kind, v.text)
	case ':':
		_, err = fmt.Fprintf(w, ":%d\r\n", v.n)
	case '$':
		if _, err = fmt.Fprintf(w, "$%d\r\n", len(v.text)); err == nil {
			if _, err = w.WriteString(v.text); err == nil {
				_, err = w.WriteString("\r\n")
			}
		}
	case '*':
		if _, err = fmt.Fprintf(w, "*%d\r\n", len(v.items)); err == nil {
			for _, item := range v.items {
				if err = item.write(w); err != nil {
					break
				}
			}
		}
	default: // Zero value denotes a missing field, not an empty string.
		_, err = w.WriteString("$-1\r\n")
	}
	return err
}

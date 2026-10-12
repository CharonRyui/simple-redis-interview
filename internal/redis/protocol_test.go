package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func request(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return b.String()
}

func TestReadCommandBinaryAndPipeline(t *testing.T) {
	want := []string{"HSET", "", "\x00field", "张三\r\n\x00"}
	r := bufio.NewReader(strings.NewReader(request(want...) + request("PING")))
	for _, expected := range [][]string{want, {"PING"}} {
		got, err := readCommand(r)
		if err != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("got %q, %v; want %q", got, err, expected)
		}
	}
	if _, err := readCommand(r); !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean EOF, got %v", err)
	}
}

func TestReadCommandRejectsMalformedAndOversizedInput(t *testing.T) {
	for _, input := range []string{
		"PING\r\n", "*0\r\n", "*-1\r\n", "*+1\r\n", "*1\n",
		"*1\r\n$-1\r\n", "*1\r\n+PING\r\n", "*1\r\n$1\r\naXX",
		"*1025\r\n", "*9999999999999999999999999\r\n",
		"*1\r\n$8388608\r\n", "*1\r\n$" + strings.Repeat("0", 70) + "\r\n",
	} {
		if _, err := readCommand(bufio.NewReader(strings.NewReader(input))); err == nil {
			t.Fatalf("accepted malformed input %q", input)
		}
	}
}

func TestTruncatedRequests(t *testing.T) {
	full := request("HSET", "k", "f", "value")
	for i := 1; i < len(full); i++ {
		_, err := readCommand(bufio.NewReader(strings.NewReader(full[:i])))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated at %d: expected unexpected EOF, got %v", i, err)
		}
	}
}

func TestRequestByteLimitIncludesAllArguments(t *testing.T) {
	// Each value is individually below the limit, but their sum exceeds it.
	half := strings.Repeat("v", maxRequestBytes/2)
	input := request("HSET", "k", "a", half, "b", half)
	if _, err := readCommand(bufio.NewReader(strings.NewReader(input))); err == nil {
		t.Fatal("accepted oversized aggregate request")
	}
}

func FuzzReadCommand(f *testing.F) {
	for _, seed := range []string{request("PING"), request("HSET", "k", "f", "v"), "*1\r\n$-1\r\n", ""} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		readCommand(bufio.NewReader(bytes.NewReader(input)))
	})
}

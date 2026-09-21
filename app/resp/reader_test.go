package resp

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// oneByteReader delivers input one byte per Read call, simulating maximal
// TCP fragmentation. A correct parser must behave identically.
type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.r.Read(p)
}

func args(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [][]byte
		wantErr error // nil means "any non-nil error is a failure"
		errIs   func(error) bool
	}{
		{
			name:  "ping",
			input: "*1\r\n$4\r\nPING\r\n",
			want:  args("PING"),
		},
		{
			name:  "set foo bar",
			input: "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			want:  args("SET", "foo", "bar"),
		},
		{
			name:  "empty bulk arg",
			input: "*2\r\n$3\r\nGET\r\n$0\r\n\r\n",
			want:  args("GET", ""),
		},
		{
			name:  "binary safe payload with CRLF inside",
			input: "*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n",
			want:  args("ECHO", "a\r\nb"),
		},
		{
			name:  "empty array skipped, next command returned",
			input: "*0\r\n*1\r\n$4\r\nPING\r\n",
			want:  args("PING"),
		},
		{
			name:  "inline command",
			input: "PING\r\n",
			want:  args("PING"),
		},
		{
			name:  "inline with args and extra spaces",
			input: "SET  foo   bar\r\n",
			want:  args("SET", "foo", "bar"),
		},
		{
			name:  "blank inline line skipped",
			input: "\r\n*1\r\n$4\r\nPING\r\n",
			want:  args("PING"),
		},
		{
			name:  "clean EOF",
			input: "",
			errIs: func(err error) bool { return err == io.EOF },
		},
		{
			name:  "bare LF is protocol error",
			input: "*1\n$4\r\nPING\r\n",
			errIs: isProtoErr,
		},
		{
			name:  "missing CR after payload",
			input: "*1\r\n$4\r\nPINGX\n",
			errIs: isProtoErr,
		},
		{
			name:  "negative bulk length in command",
			input: "*1\r\n$-1\r\n",
			errIs: isProtoErr,
		},
		{
			name:  "negative multibulk length",
			input: "*-1\r\n",
			errIs: isProtoErr,
		},
		{
			name:  "non-numeric array count",
			input: "*abc\r\n",
			errIs: isProtoErr,
		},
		{
			name:  "huge bulk length rejected",
			input: "*1\r\n$99999999999999\r\n",
			errIs: isProtoErr,
		},
		{
			name:  "array element not a bulk string",
			input: "*1\r\n:123\r\n",
			errIs: isProtoErr,
		},
		{
			name:  "EOF mid payload",
			input: "*1\r\n$4\r\nPI",
			errIs: func(err error) bool { return err == io.ErrUnexpectedEOF },
		},
		{
			name:  "EOF mid header",
			input: "*2\r\n$3\r\nSET\r\n",
			errIs: func(err error) bool { return err == io.ErrUnexpectedEOF },
		},
	}

	for _, tt := range tests {
		// Run every case twice: whole input at once, and 1 byte per read.
		for _, mode := range []string{"whole", "1byte"} {
			t.Run(tt.name+"/"+mode, func(t *testing.T) {
				var src io.Reader = strings.NewReader(tt.input)
				if mode == "1byte" {
					src = oneByteReader{src}
				}
				r := NewReader(src)

				got, err := r.ReadCommand()
				if tt.errIs != nil {
					if err == nil || !tt.errIs(err) {
						t.Fatalf("want matching error, got %v (cmd=%q)", err, got)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("got %q, want %q", got, tt.want)
				}
			})
		}
	}
}

func TestPipelinedCommands(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	r := NewReader(oneByteReader{strings.NewReader(input)})

	want := [][][]byte{args("PING"), args("SET", "k", "v"), args("GET", "k")}
	for i, w := range want {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, w) {
			t.Fatalf("command %d: got %q, want %q", i, got, w)
		}
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("want io.EOF after last command, got %v", err)
	}
}

// Every truncation of a valid frame must fail cleanly, never succeed.
func TestTornInput(t *testing.T) {
	full := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	for i := 1; i < len(full); i++ {
		r := NewReader(strings.NewReader(full[:i]))
		if cmd, err := r.ReadCommand(); err == nil {
			t.Fatalf("truncation at %d unexpectedly succeeded: %q", i, cmd)
		}
	}
}

func isProtoErr(err error) bool {
	var pe *ProtocolError
	return errors.As(err, &pe)
}

func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*1\r\n$-1\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(strings.NewReader(string(data)))
		for i := 0; i < 100; i++ { // bounded: no hangs
			if _, err := r.ReadCommand(); err != nil {
				return // any clean error is fine; panics/hangs are not
			}
		}
	})
}

package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

const (
	maxLineLength = 64 * 1024
	maxBulkLength = 512 * 1024 * 1024
	maxArrayLen   = 1024 * 1024
)

type ProtocolError struct {
	msg string
}

func (e *ProtocolError) Error() string {
	return "Protocol error: " + e.msg
}

func protoErr(format string, args ...any) error {
	return &ProtocolError{msg: fmt.Sprintf(format, args...)}
}

type Reader struct {
	rd *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(r),
	}
}

func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		b, err := r.rd.ReadByte()
		// fmt.Println("[ReadCommand] - r.rd.ReadByte", string(b))
		if err != nil {
			return nil, err
		}

		if b == '*' {
			cmd, err := r.readCommandArray()
			if err != nil {
				return nil, err
			}
			if cmd == nil {
				// Empty array (*0): ignore and read the next command,
				// mirroring real Redis behaviour.
				continue
			}
			return cmd, nil
		}

		// Not an array: treat as an inline command.
		if err := r.rd.UnreadByte(); err != nil {
			return nil, err
		}
		cmd, err := r.readInlineCommand()
		if err != nil {
			return nil, err
		}
		if cmd == nil {
			continue // blank line: skip
		}
		return cmd, nil
	}
}

func (r *Reader) readCommandArray() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	n, err := parseInt(line)
	if err != nil {
		return nil, protoErr("invalid multibulk length")
	}

	if n < 0 || n > maxArrayLen {
		return nil, protoErr("invalid multibulk length")
	}
	if n == 0 {
		return nil, nil
	}

	args := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		b, err := r.rd.ReadByte()
		if err != nil {
			return nil, unexpectedEOF(err)
		}
		if b != '$' {
			return nil, protoErr("expected '$', got '%c'", b)
		}
		arg, err := r.readBulkString()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func (r *Reader) readBulkString() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	n, err := parseInt(line)
	if err != nil || n < 0 || n > maxBulkLength {
		return nil, protoErr("invalid bulk length")
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.rd, buf); err != nil {
		return nil, unexpectedEOF(err)
	}

	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, protoErr("expected CRLF after bulk payload")
	}
	return buf[:n], nil

}

func (r *Reader) readInlineCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	fields := bytes.Fields(line)
	if len(fields) == 0 {
		return nil, nil
	}

	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = append([]byte(nil), f...)
	}
	return args, nil

}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.rd.ReadBytes('\n')

	if err != nil {
		return nil, unexpectedEOF(err)
	}

	if len(line) > maxLineLength {
		return nil, protoErr("line too long")
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErr("expected CLRF line terminator")
	}

	return line[:len(line)-2], nil
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, protoErr("empty integer")
	}

	neg := false
	i := 0

	if b[0] == '-' {
		neg = true
		i++
		if len(b) == i {
			return 0, protoErr("invalid")
		}
	}

	var n int64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, protoErr("invalid integer")
		}
		n = n*10 + int64(c-'0')
		if n > maxBulkLength {
			return 0, protoErr("integer overflow")
		}
	}

	if neg {
		n = -n
	}
	return n, nil

}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

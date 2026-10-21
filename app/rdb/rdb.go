package rdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	opAux       = 0xFA
	opResizeDB  = 0xFB
	opExpireMs  = 0xFC
	opExpireSec = 0xFD
	opSelectDB  = 0xFE
	opEOF       = 0xFF

	typeString = 0x00
)

type RDBReader struct {
	rd *bufio.Reader
}

func newRDBReader(r io.Reader) *RDBReader {
	return &RDBReader{
		rd: bufio.NewReader(r),
	}
}

func LoadRDBFile(dir string, filename string, set SetFunc) error {
	path := filepath.Join(dir, filename)

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil // no RDB yet: start empty
	}
	if err != nil {
		return err
	}

	defer f.Close()
	r := newRDBReader(f)
	return r.Load(set)
}

type SetFunc func(key, value string, expiresAt time.Time)

func (r *RDBReader) Load(set SetFunc) error {
	var hdr [9]byte

	if _, err := io.ReadFull(r.rd, hdr[:]); err != nil {
		return fmt.Errorf("reading header: %w", err)
	}

	if string(hdr[:5]) != "REDIS" {
		return errors.New("not an RDB file")
	}

	var expiresAt time.Time

	for {
		op, err := r.rd.ReadByte()
		if err != nil {
			return fmt.Errorf("reading opcode: %w", err)
		}

		switch op {
		case opAux:
			if _, err := r.readString(); err != nil {
				return fmt.Errorf("aux name: %w", err)
			}
			if _, err := r.readString(); err != nil {
				return fmt.Errorf("aux value: %w", err)
			}

		case opSelectDB: // DB index, ignored (only DB 0 matters)
			if _, err := r.readPlainSize(); err != nil {
				return fmt.Errorf("db index: %w", err)
			}

		case opResizeDB: // hash table size hints, ignored
			if _, err := r.readPlainSize(); err != nil {
				return fmt.Errorf("db size: %w", err)
			}
			if _, err := r.readPlainSize(); err != nil {
				return fmt.Errorf("expires size: %w", err)
			}
		case opExpireMs:
			var b [8]byte
			if _, err := io.ReadFull(r.rd, b[:]); err != nil {
				return fmt.Errorf("expiry ms: %w", err)
			}

			expiresAt = time.UnixMilli(int64(binary.LittleEndian.Uint64(b[:])))

		case opExpireSec:
			var b [4]byte
			if _, err := io.ReadFull(r.rd, b[:]); err != nil {
				return fmt.Errorf("expiry sec: %w", err)
			}
			expiresAt = time.Unix(int64(binary.LittleEndian.Uint32(b[:])), 0)

		case typeString:
			key, err := r.readString()
			if err != nil {
				return fmt.Errorf("key: %w", err)
			}
			value, err := r.readString()
			if err != nil {
				return fmt.Errorf("value for %q: %w", key, err)
			}
			set(key, value, expiresAt)
			expiresAt = time.Time{} // expiry applies to one key only

		case opEOF:
			return nil

		default:
			return fmt.Errorf("unsupported opcode 0x%02X", op)
		}
	}
}

func (r *RDBReader) readSize() (n uint64, special bool, err error) {
	b, err := r.rd.ReadByte()

	if err != nil {
		return 0, false, err
	}

	switch b >> 6 {
	case 0:
		return uint64(b & 0x3F), false, nil
	case 1:
		next, err := r.rd.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return uint64(b&0x3F)<<8 | uint64(next), false, nil
	case 2:
		var buf [4]byte
		if _, err := io.ReadFull(r.rd, buf[:]); err != nil {
			return 0, false, err
		}

		return uint64(binary.BigEndian.Uint32(buf[:])), false, nil

	default:
		return uint64(b & 0x3F), true, nil
	}
}

func (r *RDBReader) readString() (string, error) {
	n, special, err := r.readSize()
	if err != nil {
		return "", err
	}

	if !special {
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.rd, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	}

	switch n {
	case 0:
		b, err := r.rd.ReadByte()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(int8(b))), nil
	case 1:
		var buf [2]byte

		if _, err := io.ReadFull(r.rd, buf[:]); err != nil {
			return "", nil
		}

		return strconv.Itoa(int(int16(binary.LittleEndian.Uint16(buf[:])))), nil

	case 2:
		var buf [4]byte
		if _, err := io.ReadFull(r.rd, buf[:]); err != nil {
			return "", err
		}
		return strconv.Itoa(int(int32(binary.LittleEndian.Uint32(buf[:])))), nil

	default: // C3 = LZF, not needed for CodeCrafters
		return "", fmt.Errorf("unsupported string encoding %d", n)
	}
}

func (r *RDBReader) readPlainSize() (uint64, error) {
	n, special, err := r.readSize()
	if err != nil {
		return 0, err
	}

	if special {
		return 0, fmt.Errorf("unexpected special encoding 0x%02X", n)
	}

	return 0, nil
}

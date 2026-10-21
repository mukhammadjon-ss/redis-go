package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type AOF struct {
	mu    sync.Mutex
	file  *os.File
	fsync string
	path  string
}

func (a *AOF) Append(raw []byte) error {
	if a == nil {
		return nil // AOF disabled
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.file.Write(raw); err != nil {
		return err
	}

	if a.fsync == "always" {
		return a.file.Sync()
	}

	return nil
}

func (a *AOF) Replay(apply func(cmd [][]byte)) error {
	if a == nil {
		return nil // AOF disabled
	}

	aofFile, err := os.Open(a.path)
	defer aofFile.Close()
	if err != nil {
		return err
	}

	r := resp.NewReader(aofFile)

	for {
		cmd, err := r.ReadCommand()
		if errors.Is(err, io.EOF) {
			return nil // reached the end of the file: replay finished
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil // nothing to replay yet
		}
		if err != nil {
			return fmt.Errorf("aof replay: %w", err)
		}

		apply(cmd)
	}

	return nil
}

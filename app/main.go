package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
)

func getIncrFileNameFromManifest(manifestPath string) (string, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", err
	}

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		var name, typ string
		for i := 0; i+1 < len(fields); i++ {
			switch fields[i] {
			case "file":
				name = fields[i+1]
			case "type":
				typ = fields[i+1]
			}
		}

		if typ == "i" && name != "" {
			return name, nil
		}
	}

	return "", fmt.Errorf("no incr file in manifest %q", manifestPath)
}

func main() {

	cfg, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}

	srv := newServer(cfg)
	db := newStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := fmt.Sprintf("0.0.0.0:%d", srv.cfg.port)
	fmt.Println("Logs from your program will appear here!")
	// db := newStore()

	/*  READ RDB FILES AND POPULATE STORE */
	if srv.cfg.dbFilename != "" {
		if err := rdb.LoadRDBFile(srv.cfg.dir, srv.cfg.dbFilename, func(key, value string, expiresAt time.Time) {
			db.Set(key, value, expiresAt)
		}); err != nil {
			fmt.Println("Failed to read rdb file")
			os.Exit(1)
		}
	}

	var aof *AOF
	if srv.cfg.appendOnly {
		path := srv.cfg.aofDir
		if err := os.MkdirAll(path, 0o755); err != nil {
			log.Fatalf("Error creating folder: %v", err)
		}
		manifestFilePath := filepath.Join(path, srv.cfg.appendFilename+".manifest")
		incrName, err := getIncrFileNameFromManifest(manifestFilePath)
		if errors.Is(err, os.ErrNotExist) {
			incrName = srv.cfg.appendFilename + ".1.incr.aof"
			if err := os.WriteFile(manifestFilePath, []byte(
				fmt.Sprintf("file %s seq 1 type i\n", incrName)), 0o644); err != nil {
				log.Fatalf("write manifest: %v", err)
			}
		} else if err != nil {
			log.Fatalf("read manifest: %v", err)
		}

		incrPath := filepath.Join(path, incrName)
		aofFile, err := os.OpenFile(incrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Fatalf("open aof file %q: %v", incrName, err)
		}
		aof = &AOF{file: aofFile, fsync: srv.cfg.appendFsync, path: incrPath}

		if err := aof.Replay(func(cmd [][]byte) {
			handleCommands(ctx, io.Discard, db, srv, cmd)
		}); err != nil {
			log.Fatal(err)
		}

		defer aofFile.Close()
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Println("\nshutdown signal received")
		cancel()
		l.Close()
	}()

	var wg sync.WaitGroup
	db.startSweeper(ctx, 100*time.Millisecond, &wg)

	if srv.cfg.isReplica {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startReplication(ctx, srv, db)
		}()
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				fmt.Println("no longer accepting connections")
				wg.Wait()
				fmt.Println("clean exit, goroutines:", runtime.NumGoroutine())
				return
			default:
				fmt.Println("accept error:", err)
				continue
			}
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleClient(ctx, c, db, srv, aof)
		}(conn)
		// go handleClient(conn, db)
	}
}

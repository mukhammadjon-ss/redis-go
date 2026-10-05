package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type transaction struct {
	active  bool
	queued  [][][]byte
	watched []string
	dirty   bool
}

func handleClient(ctx context.Context, conn net.Conn, db *store, srv *server) {
	defer conn.Close()
	defer srv.replicas.remove(conn)

	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.SetReadDeadline(time.Now()) // makes any blocked Read fail now
		case <-watchDone:
		}
	}()
	r := resp.NewReader(conn)
	var tx transaction

	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				conn.Write([]byte("-ERR " + pe.Error() + "\r\n"))
			}
			return
		}

		commandName := strings.ToUpper(string(cmd[0]))

		if tx.active && commandName != "MULTI" && commandName != "EXEC" && commandName != "DISCARD" && commandName != "WATCH" {
			tx.queued = append(tx.queued, cmd)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		switch commandName {
		case "MULTI":
			if tx.active {
				continue
			}

			tx.active = true
			conn.Write([]byte("+OK\r\n"))

		case "EXEC":
			if !tx.active {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			aborted := db.CheckDirty(&tx)
			db.Unwatch(&tx)

			queued := tx.queued
			tx = transaction{}

			if aborted {
				conn.Write([]byte("*-1\r\n"))
				continue
			}
			conn.Write([]byte("*" + strconv.Itoa(len(queued)) + "\r\n"))
			for _, c := range queued {
				handleCommands(ctx, conn, db, srv, c)
			}
		case "DISCARD":
			if !tx.active {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				continue
			}

			tx = transaction{}
			conn.Write([]byte("+OK\r\n"))
		case "WATCH":
			if len(cmd) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'watch' command\r\n"))
				continue
			}

			if tx.active {
				conn.Write([]byte("-ERR WATCH inside MULTI is not allowed\r\n"))
				continue
			}

			keys := make([]string, 0, len(cmd)-1)

			for _, k := range cmd[1:] {
				keys = append(keys, string(k))
			}
			db.Watch(&tx, keys)
			conn.Write([]byte("+OK\r\n"))

		case "UNWATCH":
			db.Unwatch(&tx)
			conn.Write([]byte("+OK\r\n"))

		case "PSYNC":
			log.Println("PSYNC received")
			const emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

			var emptyRDB = func() []byte {
				b, err := hex.DecodeString(emptyRDBHex)
				if err != nil {
					panic(err)
				}

				return b
			}()

			body := fmt.Sprintf("+FULLRESYNC %s %d\r\n", srv.replicationId, 0)
			conn.Write([]byte(body))

			conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(emptyRDB))))
			conn.Write(emptyRDB)

			srv.replicas.add(conn)

		case "REPLCONF":
			if len(cmd) >= 3 && strings.EqualFold(string(cmd[1]), "ACK") {
				offset, err := strconv.ParseInt(string(cmd[2]), 10, 64)
				if err == nil {
					srv.replicas.ack(conn, offset)
				}
				continue
			}

			conn.Write([]byte("+OK\r\n"))

		case "WAIT":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR invalid number of arguments for 'wait'\r\n"))
			}

			// target := srv.replicas.currentOffset()

			numOfReplicas, _ := strconv.Atoi(string(cmd[1]))
			timeoutInt, _ := strconv.Atoi(string(cmd[2]))

			n := srv.replicas.wait(numOfReplicas, time.Duration(timeoutInt)*time.Millisecond)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

		default:
			handleCommands(ctx, conn, db, srv, cmd)
			if writeCommands[commandName] {
				srv.replicas.propagate(cmd)
			}
		}
	}
}

func writeStream(w io.Writer, entries []StreamEntry) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(entries))

	for _, e := range entries {
		b.WriteString("*2\r\n")

		id := e.ID.String()
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(id), id)

		fmt.Fprintf(&b, "*%d\r\n", len(e.Fields))
		for _, f := range e.Fields {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(f), f)
		}
	}

	w.Write([]byte(b.String()))
}

type streamResult struct {
	key     string
	entries []StreamEntry
}

func writeXRead(w io.Writer, results []streamResult) {
	var b strings.Builder

	fmt.Fprintf(&b, "*%d\r\n", len(results)) // one per stream key

	for _, r := range results {
		b.WriteString("*2\r\n") // [key, entries]
		bulk(&b, r.key)

		fmt.Fprintf(&b, "*%d\r\n", len(r.entries)) // the entries array
		for _, e := range r.entries {
			b.WriteString("*2\r\n") // [id, fields]
			bulk(&b, e.ID.String())

			fmt.Fprintf(&b, "*%d\r\n", len(e.Fields))
			for _, f := range e.Fields {
				bulk(&b, f)
			}
		}
	}

	w.Write([]byte(b.String()))
}

func bulk(b *strings.Builder, s string) {
	fmt.Fprintf(b, "$%d\r\n%s\r\n", len(s), s)
}

func writeArray(w io.Writer, popped []string) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(popped))

	for _, item := range popped {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(item), item)
	}

	w.Write([]byte(b.String()))
}

func encodeCommand(args ...string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))

	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}

	return []byte(b.String())
}

func main() {
	port := flag.Int("port", 6379, "port to listen on")
	replicaOf := flag.String("replicaof", "", "indicating service is replica")
	flag.Parse()

	srv := &server{
		port:              *port,
		replicaof:         *replicaOf,
		isReplica:         *replicaOf != "",
		replicas:          &replicaSet{ackCh: make(chan struct{})},
		replicationId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replicationOffset: 0,
	}

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	fmt.Println("Logs from your program will appear here!")
	db := newStore()

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	if srv.isReplica {
		host, port, ok := strings.Cut(*replicaOf, " ")
		if !ok {
			log.Fatal("--replicaof must be \"<host> <port>\"")
		}
		srv.master_host = host
		srv.master_port = port

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
			handleClient(ctx, c, db, srv)
		}(conn)
		// go handleClient(conn, db)
	}
}

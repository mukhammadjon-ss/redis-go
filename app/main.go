package main

import (
	"context"
	"errors"
	"fmt"
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

func handleClient(ctx context.Context, conn net.Conn, db *store) {
	defer conn.Close()
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

	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				conn.Write([]byte("-ERR " + pe.Error() + "\r\n"))
			}
			return
		}

		switch strings.ToUpper(string(cmd[0])) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			if len(cmd) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}
			arg := cmd[1]
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)))

		case "SET":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}

			var expiresAt time.Time // zero value: no expiry

			if len(cmd) > 3 {
				opts := cmd[3:]
				if len(opts) != 2 || strings.ToUpper(string(opts[0])) != "PX" {
					conn.Write([]byte("-ERR syntax error\r\n"))
					continue
				}
				ms, err := strconv.ParseInt(string(opts[1]), 10, 64)
				if err != nil || ms < 0 {
					conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}
				expiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
			}

			db.Set(string(cmd[1]), string(cmd[2]), expiresAt)
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			got, ok := db.Get(string(cmd[1]))
			if !ok {
				conn.Write([]byte("$-1\r\n"))
				continue
			}

			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(got), got)))
			continue

		case "RPUSH":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				continue
			}
			raw := cmd[2:]
			values := make([]string, len(raw))
			for i, b := range raw {
				values[i] = string(b)
			}
			n := db.RPush(string(cmd[1]), values...)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
			continue

		case "LPUSH":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				continue
			}
			raw := cmd[2:]
			values := make([]string, len(raw))
			for i, b := range raw {
				values[i] = string(b)
			}
			n := db.LPush(string(cmd[1]), values...)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
			continue

		case "LRANGE":
			if len(cmd) != 4 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
				continue
			}

			start, _ := strconv.Atoi(string(cmd[2]))
			end, _ := strconv.Atoi(string(cmd[3]))

			slice := db.LRange(string(cmd[1]), start, end)
			if len(slice) == 0 {
				conn.Write([]byte("*0\r\n"))
				continue
			}

			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(slice))
			for _, item := range slice {
				fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(item), item)
			}
			conn.Write([]byte(b.String()))

		case "LLEN":
			n := db.LLen(string(cmd[1]))
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
			continue

		case "LPOP":
			withCount := len(cmd) >= 3
			n := 1
			if withCount {
				n, err = strconv.Atoi(string(cmd[2]))

				if err != nil || n < 0 {
					conn.Write([]byte(fmt.Sprintf("$-1\r\n")))
					continue
				}
			}
			ans, ok := db.LPop(string(cmd[1]), n)
			if !ok {
				conn.Write([]byte(fmt.Sprintf("$-1\r\n")))
				continue
			}

			if !withCount {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(ans[0]), ans[0])))
				continue
			}
			writeArray(conn, ans)
			continue

		case "BLPOP":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
				continue
			}

			secs, err := strconv.ParseFloat(string(cmd[len(cmd)-1]), 64)
			if err != nil || secs < 0 {
				conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
				continue
			}

			keys := make([]string, 0, len(cmd)-2)
			for _, k := range cmd[1 : len(cmd)-1] {
				keys = append(keys, string(k))
			}

			p, ok := db.BLpop(ctx, keys, time.Duration(secs*float64(time.Second)))
			if !ok {
				conn.Write([]byte("*-1\r\n"))
				continue
			}
			writeArray(conn, []string{p.key, p.value})

		case "TYPE":
			if len(cmd) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
				continue
			}

			p, ok := db.EntryType(string(cmd[1]))
			if !ok {
				conn.Write([]byte("+none\r\n"))
				continue
			}

			conn.Write([]byte("+" + p + "\r\n"))

		case "XADD":
			if len(cmd) < 5 || (len(cmd)-3)%2 != 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'XADD' command\r\n"))
				continue
			}

			// streamId, err := parseID(string(cmd[2]))
			if err != nil {
				conn.Write([]byte("-ERR stream ID is invalid\r\n"))
				continue
			}

			var fields []string

			for _, v := range cmd[3:] {
				fields = append(fields, string(v))
			}
			e, err := db.XAdd(string(cmd[1]), string(cmd[2]), fields)
			if err != nil {
				conn.Write([]byte("-" + err.Error() + "\r\n"))
				continue
			}

			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(e), e)))
		case "XRANGE":
			if len(cmd) < 4 {
				conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
				continue
			}

			key := string(cmd[1])
			start := string(cmd[2])
			end := string(cmd[3])

			entries, err := db.XRange(key, start, end)
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			}

			writeStream(conn, entries)
		default:
			conn.Write([]byte("-ERR unknown command '" + string(cmd[0]) + "'\r\n"))
		}
	}
}

func writeStream(conn net.Conn, entries []StreamEntry) {
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

	conn.Write([]byte(b.String()))
}

func writeArray(conn net.Conn, popped []string) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(popped))

	for _, item := range popped {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(item), item)
	}

	conn.Write([]byte(b.String()))
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	db := newStore()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
			handleClient(ctx, c, db)
		}(conn)
	}
}

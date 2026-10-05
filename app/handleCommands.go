package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// handleCommands executes exactly one data command and writes its reply.
// Transaction control commands (MULTI/EXEC/DISCARD) are NOT handled here —
// they mutate per-connection state and stay in handleClient.
func handleCommands(ctx context.Context, w io.Writer, db *store, srv *server, cmd [][]byte) {
	if len(cmd) == 0 {
		return
	}

	commandName := strings.ToUpper(string(cmd[0]))
	switch commandName {
	case "PING":
		w.Write([]byte("+PONG\r\n"))

	case "ECHO":
		if len(cmd) != 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
			return
		}
		arg := cmd[1]
		w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)))

	case "SET":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
			return
		}

		var expiresAt time.Time // zero value: no expiry

		if len(cmd) > 3 {
			opts := cmd[3:]
			if len(opts) != 2 || strings.ToUpper(string(opts[0])) != "PX" {
				w.Write([]byte("-ERR syntax error\r\n"))
				return
			}
			ms, err := strconv.ParseInt(string(opts[1]), 10, 64)
			if err != nil || ms < 0 {
				w.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				return
			}
			expiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
		}

		db.Set(string(cmd[1]), string(cmd[2]), expiresAt)
		w.Write([]byte("+OK\r\n"))

	case "GET":
		if len(cmd) != 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
			return
		}

		got, ok := db.Get(string(cmd[1]))
		if !ok {
			w.Write([]byte("$-1\r\n"))
			return
		}
		w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(got), got)))

	case "INCR":
		if len(cmd) != 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'incr' command\r\n"))
			return
		}

		incr, err := db.Increment(string(cmd[1]))
		if err != nil {
			w.Write([]byte("-" + err.Error() + "\r\n"))
			return
		}
		w.Write([]byte(fmt.Sprintf(":%d\r\n", incr)))

	case "TYPE":
		if len(cmd) != 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'type' command\r\n"))
			return
		}

		p, ok := db.EntryType(string(cmd[1]))
		if !ok {
			w.Write([]byte("+none\r\n"))
			return
		}
		w.Write([]byte("+" + p + "\r\n"))

	case "RPUSH":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
			return
		}

		raw := cmd[2:]
		values := make([]string, len(raw))
		for i, b := range raw {
			values[i] = string(b)
		}
		n := db.RPush(string(cmd[1]), values...)
		w.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

	case "LPUSH":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
			return
		}

		raw := cmd[2:]
		values := make([]string, len(raw))
		for i, b := range raw {
			values[i] = string(b)
		}
		n := db.LPush(string(cmd[1]), values...)
		w.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

	case "LLEN":
		if len(cmd) != 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n"))
			return
		}

		n := db.LLen(string(cmd[1]))
		w.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

	case "LRANGE":
		if len(cmd) != 4 {
			w.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
			return
		}

		start, err := strconv.Atoi(string(cmd[2]))
		if err != nil {
			w.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			return
		}
		end, err := strconv.Atoi(string(cmd[3]))
		if err != nil {
			w.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			return
		}

		slice := db.LRange(string(cmd[1]), start, end)
		if len(slice) == 0 {
			w.Write([]byte("*0\r\n"))
			return
		}

		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(slice))
		for _, item := range slice {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(item), item)
		}
		w.Write([]byte(b.String()))

	case "LPOP":
		if len(cmd) < 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
			return
		}

		withCount := len(cmd) >= 3
		n := 1
		if withCount {
			parsed, err := strconv.Atoi(string(cmd[2]))
			if err != nil || parsed < 0 {
				w.Write([]byte("-ERR value is out of range, must be positive\r\n"))
				return
			}
			n = parsed
		}

		ans, ok := db.LPop(string(cmd[1]), n)
		if !ok {
			if withCount {
				w.Write([]byte("*-1\r\n"))
			} else {
				w.Write([]byte("$-1\r\n"))
			}
			return
		}

		if !withCount {
			w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(ans[0]), ans[0])))
			return
		}
		writeArray(w, ans)

	case "BLPOP":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
			return
		}

		secs, err := strconv.ParseFloat(string(cmd[len(cmd)-1]), 64)
		if err != nil || secs < 0 {
			w.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
			return
		}

		keys := make([]string, 0, len(cmd)-2)
		for _, k := range cmd[1 : len(cmd)-1] {
			keys = append(keys, string(k))
		}

		p, ok := db.BLpop(ctx, keys, time.Duration(secs*float64(time.Second)))
		if !ok {
			w.Write([]byte("*-1\r\n"))
			return
		}
		writeArray(w, []string{p.key, p.value})

	case "XADD":
		if len(cmd) < 5 || (len(cmd)-3)%2 != 0 {
			w.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
			return
		}

		fields := make([]string, 0, len(cmd)-3)
		for _, v := range cmd[3:] {
			fields = append(fields, string(v))
		}

		id, err := db.XAdd(string(cmd[1]), string(cmd[2]), fields)
		if err != nil {
			w.Write([]byte("-" + err.Error() + "\r\n"))
			return
		}
		w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))

	case "XRANGE":
		if len(cmd) != 4 {
			w.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
			return
		}

		entries, err := db.XRange(string(cmd[1]), string(cmd[2]), string(cmd[3]))
		if err != nil {
			w.Write([]byte("-" + err.Error() + "\r\n"))
			return
		}
		writeStream(w, entries)

	case "XREAD":
		streamIdx := -1
		block := time.Duration(0)
		hasBlock := false

		for i := 1; i < len(cmd); i++ {
			switch strings.ToUpper(string(cmd[i])) {
			case "BLOCK":
				if i+1 >= len(cmd) {
					w.Write([]byte("-ERR syntax error\r\n"))
					return
				}
				ms, err := strconv.ParseInt(string(cmd[i+1]), 10, 64)
				if err != nil || ms < 0 {
					w.Write([]byte("-ERR timeout is not an integer or out of range\r\n"))
					return
				}
				block = time.Duration(ms) * time.Millisecond
				hasBlock = true
				i++
			case "STREAMS":
				streamIdx = i
			}

			if streamIdx != -1 {
				break
			}
		}

		if streamIdx == -1 {
			w.Write([]byte("-ERR syntax error\r\n"))
			return
		}

		args := cmd[streamIdx+1:]
		if len(args) == 0 || len(args)%2 != 0 {
			w.Write([]byte("-ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.\r\n"))
			return
		}

		half := len(args) / 2
		keys := make([]string, half)
		rawIDs := make([]string, half)
		for i := 0; i < half; i++ {
			keys[i] = string(args[i])
			rawIDs[i] = string(args[half+i])
		}

		var results []streamResult
		if hasBlock {
			results = db.XReadBlock(ctx, keys, rawIDs, block)
		} else {
			results, _ = db.XRead(keys, rawIDs)
		}

		if len(results) == 0 {
			w.Write([]byte("*-1\r\n"))
			return
		}
		writeXRead(w, results)

	case "INFO":
		body := fmt.Sprintf("# Replication\r\nrole:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", srv.role(), srv.replicationId, 0)
		w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)))
	default:
		w.Write([]byte("-ERR unknown command '" + string(cmd[0]) + "'\r\n"))
	}
}

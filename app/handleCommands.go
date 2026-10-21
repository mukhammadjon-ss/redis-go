package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
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
		// aofSync(aof, cmd)
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
		body := fmt.Sprintf("# Replication\r\nrole:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", srv.role(), srv.replicationID, 0)
		w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)))

	case "KEYS":
		pattern := string(cmd[1])

		keys := db.Keys(pattern)

		bulkString(w, keys...)

	case "CONFIG":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR invalid number of arguments for 'config'\r\n"))
			return
		}
		if len(cmd) >= 3 && strings.EqualFold(string(cmd[1]), "GET") {
			v := string(cmd[2])

			if v == "dir" {
				bulkString(w, "dir", srv.cfg.dir)
			} else if v == "dbfilename" {
				bulkString(w, "dbfilename", srv.cfg.dbFilename)
			} else if v == "appenddirname" {
				bulkString(w, "appenddirname", srv.cfg.appenddirname)
			} else if v == "appendonly" {
				appendonly := "no"
				if srv.cfg.appendOnly {
					appendonly = "yes"
				}
				bulkString(w, "appendonly", appendonly)
			} else if v == "appendfilename" {
				bulkString(w, "appendfilename", srv.cfg.appendFilename)
			} else if v == "appendfsync" {
				bulkString(w, "appendfsync", srv.cfg.appendFsync)
			}
		}

	case "ZADD":
		if len(cmd) < 4 {
			w.Write([]byte("-ERR invalid number of arguments for 'zadd'\r\n"))
			return
		}

		zsetKey := string(cmd[1])
		score, _ := strconv.ParseFloat(string(cmd[2]), 64)
		member := string(cmd[3])

		n, err := db.ZAdd(zsetKey, score, member)
		if err != nil {
			w.Write([]byte("-ERR " + err.Error() + "\r\n"))
			return
		}

		w.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

	case "ZRANK":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR invalid number of arguments for 'zrange'\r\n"))
			return
		}

		rank := db.ZRank(string(cmd[1]), string(cmd[2]))
		if rank == -1 {
			w.Write([]byte("$-1\r\n"))
			return
		}

		w.Write([]byte(fmt.Sprintf(":%d\r\n", rank)))

	case "ZRANGE":
		if len(cmd) < 4 {
			w.Write([]byte("-ERR invalid number of arguments for 'zrange'\r\n"))
			return
		}

		key := string(cmd[1])
		start, _ := strconv.Atoi(string(cmd[2]))
		stop, _ := strconv.Atoi(string(cmd[3]))

		scores := db.ZRange(key, start, stop)

		bulkString(w, scores...)

	case "ZCARD":
		n := db.ZCard(string(cmd[1]))
		w.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

	case "ZSCORE":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR invalid number of arguments for 'zscore'\r\n"))
			return
		}

		score := db.ZScore(string(cmd[1]), string(cmd[2]))
		if score == "" {
			w.Write([]byte("$-1\r\n"))
			return
		}

		w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(score), score)))

	case "ZREM":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR invalid number of arguments for 'zrem'\r\n"))
			return
		}

		n := db.ZRem(string(cmd[1]), string(cmd[2]))
		w.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

	case "SETBIT":
		if len(cmd) != 4 {
			w.Write([]byte("-ERR wrong number of arguments for 'setbit' command\r\n"))
			return
		}
		offset, err := strconv.ParseUint(string(cmd[2]), 10, 64)
		if err != nil {
			w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
			return
		}
		value, err := strconv.Atoi(string(cmd[3]))
		if err != nil {
			w.Write([]byte("-" + ErrBitValue.Error() + "\r\n"))
			return
		}
		old, err := db.SetBit(string(cmd[1]), offset, value)
		if err != nil {
			w.Write([]byte("-" + err.Error() + "\r\n"))
			return
		}
		fmt.Fprintf(w, ":%d\r\n", old)

	case "GETBIT":
		if len(cmd) != 3 {
			w.Write([]byte("-ERR wrong number of arguments for 'getbit' command\r\n"))
			return
		}
		offset, err := strconv.ParseUint(string(cmd[2]), 10, 64)
		if err != nil {
			w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
			return
		}
		bit, err := db.GetBit(string(cmd[1]), offset)
		if err != nil {
			w.Write([]byte("-" + err.Error() + "\r\n"))
			return
		}
		fmt.Fprintf(w, ":%d\r\n", bit)

	case "STRLEN":
		if len(cmd) != 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'getbit' command\r\n"))
			return
		}

		key := string(cmd[1])

		l, err := db.Strlen(key)
		if err != nil {
			w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
			return
		}
		fmt.Fprintf(w, ":%d\r\n", l)

	case "BITCOUNT":
		if len(cmd) < 2 {
			w.Write([]byte("-ERR wrong number of arguments for 'getbit' command\r\n"))
			return
		}

		count := 0
		if len(cmd) < 3 {
			count = db.CountBit(string(cmd[1]), 0, 0)
			fmt.Fprintf(w, ":%d\r\n", count)
			return
		}

		start, err := strconv.Atoi(string(cmd[2]))
		if err != nil {
			w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
			return
		}
		stop, err := strconv.Atoi(string(cmd[3]))
		if err != nil {
			w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
			return
		}

		count = db.CountBit(string(cmd[1]), start, stop)

		fmt.Fprintf(w, ":%d\r\n", count)

	case "BITOP":
		if string(cmd[1]) == "AND" {
			destKey := string(cmd[2])
			key1 := string(cmd[3])
			key2 := string(cmd[4])

			n, err := db.BitOpAnd(destKey, key1, key2)

			if err != nil {
				w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
				return
			}

			fmt.Fprintf(w, ":%d\r\n", n)
		} else if string(cmd[1]) == "OR" {
			destKey := string(cmd[2])
			key1 := string(cmd[3])
			key2 := string(cmd[4])

			n, err := db.BitOpOr(destKey, key1, key2)

			if err != nil {
				w.Write([]byte("-" + ErrBitOffset.Error() + "\r\n"))
				return
			}

			fmt.Fprintf(w, ":%d\r\n", n)
		}

	case "GEOADD":
		if len(cmd) < 5 {
			w.Write([]byte("-ERR invalid number of arguments for 'geoadd'\r\n"))
			return
		}

		key := string(cmd[1])
		long, _ := strconv.ParseFloat(string(cmd[2]), 64)
		lat, _ := strconv.ParseFloat(string(cmd[3]), 64)
		member := string(cmd[4])

		n, err := db.GeoAdd(key, long, lat, member)
		if err != nil {
			fmt.Fprintf(w, "-%s\r\n", err.Error())
			return
		}

		fmt.Fprintf(w, ":%d\r\n", n)

	case "GEOPOS":
		if len(cmd) < 3 {
			w.Write([]byte("-ERR wrong number of arguments for 'geopos' command\r\n"))
			return
		}

		members := make([]string, len(cmd)-2)
		for i, m := range cmd[2:] {
			members[i] = string(m)
		}

		found, err := db.GeoExists(string(cmd[1]), members)
		if err != nil {
			w.Write([]byte("-" + err.Error() + "\r\n"))
			return
		}

		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(members)) // one entry per member requested
		for _, loc := range found {
			if len(loc) == 0 {
				b.WriteString("*-1\r\n") // null array, not $-1
				continue
			}
			b.WriteString("*2\r\n")
			bulk(&b, loc[0]) // longitude, hardcoded for this stage
			bulk(&b, loc[1]) // latitude
		}
		w.Write([]byte(b.String()))

	case "GEODIST":
		if len(cmd) < 4 {
			w.Write([]byte("-ERR wrong number of arguments for 'geopos' command\r\n"))
			return
		}

		members := make([]string, len(cmd)-2)
		for i, m := range cmd[2:] {
			members[i] = string(m)
		}

		dist := db.GeoDist(string(cmd[1]), members)

		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(dist), dist)

	case "GEOSEARCH":
		if len(cmd) < 8 {
			w.Write([]byte("-ERR wrong number of arguments for 'geopos' command\r\n"))
			return
		}

		key := string(cmd[1])
		var long, lat, byRadius float64
		if string(cmd[2]) == "FROMLONLAT" {
			long, _ = strconv.ParseFloat(string(cmd[3]), 64)
			lat, _ = strconv.ParseFloat(string(cmd[4]), 64)
			if string(cmd[5]) == "BYRADIUS" {
				byRadius, _ = strconv.ParseFloat(string(cmd[6]), 64)
			}
		}

		locations := db.GeoSearch(key, long, lat, byRadius)

		var b strings.Builder

		fmt.Fprintf(&b, "*%d\r\n", len(locations))
		for _, loc := range locations {
			bulk(&b, loc)
		}

		w.Write([]byte(b.String()))

	case "ACL":
		switch strings.ToUpper(string(cmd[1])) {
		case "WHOAMI":
			user := srv.getUser("default")
			fmt.Fprintf(w, "$%d\r\n%s\r\n", len(user.name), user.name)
			return
		case "GETUSER":
			user := srv.getUser("default")
			var b strings.Builder
			// flags := []string{"nopass"}
			// passwords := []string{}

			b.WriteString("*4\r\n")
			b.WriteString("$5\r\nflags\r\n")
			b.WriteString(fmt.Sprintf("*%d\r\n", len(user.flags)))

			for _, flag := range user.flags {
				bulk(&b, flag)
			}
			b.WriteString("$9\r\npasswords\r\n")
			b.WriteString(fmt.Sprintf("*%d\r\n", len(user.passwords)))
			for _, password := range user.passwords {
				bulk(&b, password)
			}

			w.Write([]byte(b.String()))

		case "SETUSER":
			user := srv.getUser("default")
			//
			if pw, ok := strings.CutPrefix(string(cmd[3]), ">"); ok {
				sum := sha256.Sum256([]byte(pw))
				hash := hex.EncodeToString(sum[:])

				user.passwords = append(user.passwords, hash)
				user.flags = slices.DeleteFunc(user.flags, func(f string) bool {
					return f == "nopass"
				})
			}

			w.Write([]byte("+OK\r\n"))
		}
	default:
		w.Write([]byte("-ERR unknown command '" + string(cmd[0]) + "'\r\n"))
	}
}

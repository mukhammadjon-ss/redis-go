package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type transaction struct {
	active  bool
	queued  [][][]byte
	watched []string
	dirty   bool
}

var subscribeCommands = map[string]bool{
	"SUBSCRIBE":    true,
	"UNSUBSCRIBE":  true,
	"PSUBSCRIBE":   true,
	"PUNSUBSCRIBE": true,
	"QUIT":         true,
}

// subscribed := map[string]struct{}{}

func handleClient(ctx context.Context, conn net.Conn, db *store, srv *server, aof *AOF) {
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
	var sub *subscriber
	subscribed := map[string]struct{}{}

	var currentUser *user
	if u, ok := srv.acl.defaultIfNoPass(); ok {
		currentUser = u
	}
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

		if currentUser == nil && commandName != "AUTH" {
			conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
			continue
		}
		if tx.active && commandName != "MULTI" && commandName != "EXEC" && commandName != "DISCARD" && commandName != "WATCH" {
			tx.queued = append(tx.queued, cmd)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		if len(subscribed) > 0 {
			if commandName == "PING" {
				if len(cmd) < 2 {
					conn.Write([]byte("*2\r\n$4\r\npong\r\n$0\r\n\r\n"))
				} else {
					conn.Write([]byte(fmt.Sprintf("*2\r\n$4\r\nPONG\r\n$%d\r\n%s\r\n", len(cmd[1]), string(cmd[1]))))
				}
				continue
			} else if !subscribeCommands[commandName] {
				conn.Write([]byte(fmt.Sprintf("-ERR Can't execute '%s': only only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", commandName)))
				continue
			}
		}

		switch commandName {
		case "MULTI":
			if tx.active {
				conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
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

			body := fmt.Sprintf("+FULLRESYNC %s %d\r\n", srv.replicationID, 0)
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
				continue
			}

			numOfReplicas, _ := strconv.Atoi(string(cmd[1]))
			timeoutInt, _ := strconv.Atoi(string(cmd[2]))

			n := srv.replicas.wait(numOfReplicas, time.Duration(timeoutInt)*time.Millisecond)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

		case "SUBSCRIBE":
			if len(cmd) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'subscribe' command\r\n"))
				continue
			}

			if sub == nil {
				sub = &subscriber{conn: conn}
			}

			for _, ch := range cmd[1:] {
				name := string(ch)
				srv.pubsub.subscribe(name, sub)
				subscribed[name] = struct{}{}
				sub.write(encodeSubscribeReply("subscribe", name, len(subscribed)))
			}

		case "PUBLISH":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'publish' command\r\n"))
				continue
			}

			n := srv.pubsub.deliver(string(cmd[1]), string(cmd[2]))

			conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))

		case "UNSUBSCRIBE":

			for _, ch := range cmd[1:] {
				name := string(ch)
				n := srv.pubsub.unsubscibe(name, sub)
				delete(subscribed, name)
				sub.write(encodeSubscribeReply("unsubscribe", name, n))
			}

		case "AUTH":
			var name, password string
			switch len(cmd) {
			case 2:
				name, password = "default", string(cmd[1])
			case 3:
				name, password = string(cmd[1]), string(cmd[2])
			default:
				conn.Write([]byte("-ERR wrong number of arguments for 'auth' command\r\n"))
				continue
			}

			u, allowed := srv.acl.checkPassword(name, password)
			if !allowed {
				conn.Write([]byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n"))
				continue
			}
			currentUser = u
			conn.Write([]byte("+OK\r\n"))

		default:
			handleCommands(ctx, conn, db, srv, cmd)
			if writeCommands[commandName] {
				payload := EncodeRaw(cmd)
				srv.replicas.propagate(cmd)
				aof.Append(payload)
			}
		}
	}
}

package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type server struct {
	isReplica         bool
	port              int
	replicaof         string
	master_host       string
	master_port       string
	replicas          *replicaSet
	replicationId     string
	replicationOffset int64
}
type replicaSet struct {
	mu       sync.Mutex
	replicas []*replica
	offset   int64
	ackCh    chan struct{}
}

type replica struct {
	conn      net.Conn
	accOffset int64
}

var writeCommands = map[string]bool{
	"SET":   true,
	"INCR":  true,
	"RPUSH": true,
	"LPUSH": true,
	"LPOP":  true,
	"BLPOP": true,
	"XADD":  true,
}

func (s *server) role() string {
	if s.isReplica {
		return "slave"
	}

	return "master"
}

var getAckCmd = [][]byte{
	[]byte("REPLCONF"), []byte("GETACK"), []byte("*"),
}

func (r *replicaSet) add(conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicas = append(r.replicas, &replica{conn: conn})
}

func (r *replicaSet) propagate(cmd [][]byte) {
	args := make([]string, len(cmd))

	for i, arg := range cmd {
		args[i] = string(arg)
	}

	payload := encodeCommand(args...)

	r.mu.Lock()
	defer r.mu.Unlock()
	log.Printf("propagating %q to %d replicas", payload, len(r.replicas))
	for _, c := range r.replicas {
		c.conn.Write(payload)
	}

	r.offset += int64(len(payload))
}

func (r *replicaSet) remove(c net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := r.replicas[:0]

	for _, existing := range r.replicas {
		if existing.conn != c {
			out = append(out, existing)
		}
	}

	r.replicas = out
}

func (r *replicaSet) ack(conn net.Conn, offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rep := range r.replicas {
		if rep.conn == conn {
			rep.accOffset = offset
			close(r.ackCh)
			r.ackCh = make(chan struct{})
			break
		}
	}
}

func (r *replicaSet) wait(numReplicas int, timeout time.Duration) int {
	target := r.currentOffset()

	if target == 0 {
		r.mu.Lock()
		n := len(r.replicas)
		r.mu.Unlock()
		return n
	}

	r.propagate(getAckCmd)

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}

	for {
		r.mu.Lock()
		count := 0

		for _, rep := range r.replicas {
			if rep.accOffset >= target {
				count++
			}
		}
		ch := r.ackCh

		r.mu.Unlock()

		if count >= numReplicas {
			return count
		}

		select {
		case <-ch:
		case <-timeoutCh:
			return count
		}
	}

}

func (r *replicaSet) currentOffset() int64 {
	r.mu.Lock()
	r.mu.Unlock()
	return r.offset
}

func startReplication(ctx context.Context, srv *server, db *store) {
	addr := net.JoinHostPort(srv.master_host, srv.master_port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println("connect to master:", err)
		return
	}

	defer conn.Close()

	r := bufio.NewReader(conn)

	if _, err := conn.Write(encodeCommand("PING")); err != nil {
		log.Println("handshake ping:", err)
		return
	}

	if _, err := r.ReadString('\n'); err != nil {
		log.Println("handshake ping reply:", err)
		return
	}

	if _, err := conn.Write(encodeCommand("REPLCONF", "listening-port", strconv.Itoa(srv.port))); err != nil {
		log.Println("handshake replconf:", err)
		return
	}

	if _, err := r.ReadString('\n'); err != nil {
		log.Println("handshake ping reply:", err)
		return
	}

	if _, err := conn.Write(encodeCommand("REPLCONF", "capa", "psync2")); err != nil {
		log.Println("handshake replconf:", err)
		return
	}

	if _, err := r.ReadString('\n'); err != nil {
		log.Println("handshake replconf reply:", err)
		return
	}

	if _, err := conn.Write(encodeCommand("PSYNC", "?", "-1")); err != nil {
		log.Println("handshake psync:", err)
		return
	}

	if _, err := r.ReadString('\n'); err != nil { // +FULLRESYNC <replid> 0
		log.Println("handshake psync reply:", err)
		return
	}

	header, err := r.ReadString('\n')
	if err != nil {
		log.Println("read rdb header:", err)
		return
	}

	n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil {
		log.Println("bad rdb header:", header)
		return
	}

	if _, err := io.ReadFull(r, make([]byte, n)); err != nil {
		log.Println("read rdb:", err)
		return
	}

	cr := resp.NewReader(r)
	var offset int64
	for {
		cmd, err := cr.ReadCommand()
		if err != nil {
			log.Println("replication stream ended:", err)
			return
		}

		args := make([]string, len(cmd))

		for i, arg := range cmd {
			args[i] = string(arg)
		}

		size := len(encodeCommand(args...))
		if isGetAck(cmd) {
			conn.Write(encodeCommand("REPLCONF", "ACK", strconv.Itoa(int(offset))))
		} else {
			handleCommands(ctx, io.Discard, db, srv, cmd)
		}

		offset += int64(size)
	}
}

func isGetAck(cmd [][]byte) bool {
	return len(cmd) >= 2 &&
		strings.EqualFold(string(cmd[0]), "REPLCONF") &&
		strings.EqualFold(string(cmd[1]), "GETACK")
}

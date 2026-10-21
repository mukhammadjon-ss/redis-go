package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

type subscriber struct {
	mu   sync.Mutex
	conn net.Conn
}

type pubsub struct {
	mu       sync.Mutex
	channels map[string]map[*subscriber]struct{}
}

func newPubSub() *pubsub {
	return &pubsub{
		channels: make(map[string]map[*subscriber]struct{}),
	}
}

func (ps *pubsub) subscribe(channel string, sub *subscriber) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	set, ok := ps.channels[channel]
	if !ok {
		set = make(map[*subscriber]struct{})
		ps.channels[channel] = set
	}

	set[sub] = struct{}{}
}

func (ps *pubsub) deliver(channel string, message string) int {
	ps.mu.Lock()
	subs := make([]*subscriber, 0, len(ps.channels[channel]))

	for sub := range ps.channels[channel] {
		subs = append(subs, sub)
	}

	ps.mu.Unlock()
	encodedMessage := encodeDeliveryMessage(channel, message)

	for _, sub := range subs {
		sub.write(encodedMessage)
	}

	return len(subs)
}
func (ps *pubsub) unsubscibe(channel string, sub *subscriber) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	set, _ := ps.channels[channel]

	delete(set, sub)

	// sub.write(encodeSubscribeReply("unsubscribe", channel, len(ps.channels[channel])))
	return len(ps.channels[channel])
}

func (sub *subscriber) write(reply []byte) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	_, err := sub.conn.Write(reply)
	return err
}

func encodeSubscribeReply(action, name string, numSubscribers int) []byte {
	var b strings.Builder
	b.WriteString("*3\r\n")
	fmt.Fprintf(&b, "$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n", len(action), action, len(name), string(name), numSubscribers)
	return []byte(b.String())
}

func encodeDeliveryMessage(channel string, message string) []byte {
	var b strings.Builder
	b.WriteString("*3\r\n")
	fmt.Fprintf(&b, "$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(message), message)
	return []byte(b.String())
}

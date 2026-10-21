package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"slices"
	"strings"
	"sync"
)

type server struct {
	cfg           config
	replicationID string
	replicas      *replicaSet
	pubsub        *pubsub
	acl           *acl
}

type user struct {
	name      string
	flags     []string
	passwords []string
}

type acl struct {
	mu    sync.RWMutex
	users map[string]*user
}

func newAcl() *acl {
	return &acl{
		users: map[string]*user{
			"default": {
				name:  "default",
				flags: []string{"nopass"},
			},
		},
	}
}

func newServer(cfg config) *server {
	return &server{
		cfg:           cfg,
		replicationID: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replicas:      &replicaSet{ackCh: make(chan struct{})},
		pubsub:        newPubSub(),
		acl:           newAcl(),
	}
}

func (srv *server) getUser(user string) *user {
	return srv.acl.users[user]
}

func (a *acl) checkPassword(name, password string) (*user, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	u, ok := a.users[name]

	if !ok {
		return nil, false
	}

	if slices.Contains(u.flags, "nopass") {
		return u, true // any password is accepted
	}

	sum := sha256.Sum256([]byte(password))
	hash := hex.EncodeToString(sum[:])

	for _, stored := range u.passwords {
		if subtle.ConstantTimeCompare([]byte(hash), []byte(stored)) == 1 {
			return u, true
		}
	}

	return nil, false
}

func (a *acl) defaultIfNoPass() (*user, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	u, ok := a.users["default"]
	if !ok {
		return nil, false
	}
	if slices.Contains(u.flags, "nopass") {
		return u, true
	}
	return nil, false
}

func (a *acl) setUser(name, password string) {
	user := a.users[name]

	if pw, ok := strings.CutPrefix(string(password), ">"); ok {
		sum := sha256.Sum256([]byte(pw))
		hash := hex.EncodeToString(sum[:])

		user.passwords = append(user.passwords, hash)
		user.flags = slices.DeleteFunc(user.flags, func(f string) bool {
			return f == "nopass"
		})
	}
}

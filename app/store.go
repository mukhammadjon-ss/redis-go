package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrIDZero     = errors.New("ERR The ID specified in XADD must be greater than 0-0")
	ErrIDTooSmall = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	ErrInvalidID  = errors.New("ERR Invalid stream ID specified as stream command argument")
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
)

type entry struct {
	kind      string
	value     string
	list      []string
	stream    *Stream
	expiresAt time.Time
}

type popped struct {
	key, value string
}
type store struct {
	mu       sync.RWMutex
	data     map[string]entry
	waiters  map[string][]chan popped
	notify   chan struct{}
	watchers map[string][]*transaction
}

type StreamID struct {
	Ms, Seq uint64
}

type StreamEntry struct {
	ID     StreamID
	Fields []string
}

type Stream struct {
	Entries []StreamEntry
	LastID  StreamID
}

func newStore() *store {
	return &store{data: make(map[string]entry), waiters: make(map[string][]chan popped), notify: make(chan struct{}), watchers: make(map[string][]*transaction)}
}

func (s *store) Set(key string, value string, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markWatchersLocked(key)
	s.data[key] = entry{value: value, expiresAt: expiresAt, kind: "string"}
}

func (s *store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]

	if !ok {
		return "", false
	}

	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		return "", false
	}

	return e.value, true
}

func (s *store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.data[key]
	handed := 0
	for _, v := range values {
		if len(s.waiters[key]) > 0 {
			ch := s.waiters[key][0]
			s.waiters[key] = s.waiters[key][1:]
			ch <- popped{key, v}
			handed++
		} else {
			e.list = append(e.list, v)
		}
	}
	s.data[key] = e

	return len(e.list) + handed
}

func (s *store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return []string{}
	}
	n := len(e.list)

	// 1. normalize: negative means "from the end"
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}

	// 2. clamp into range
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}

	// 3. validate
	if start > stop || start >= n {
		return []string{}
	}

	out := make([]string, stop-start+1)
	copy(out, e.list[start:stop+1])
	return out
}

func (s *store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.data[key]
	for _, v := range values {
		e.list = append([]string{v}, e.list...)
	}
	e.expiresAt = time.Time{}
	s.data[key] = e
	return len(e.list)
}

func (s *store) LLen(key string) int {
	e, ok := s.data[key]
	if !ok {
		return 0
	}

	return len(e.list)
}

func (s *store) LPop(key string, n int) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]

	if !ok {
		return []string{}, false
	}

	popped := e.list[0:n]
	temp := e.list[n:]
	e.list = temp
	s.data[key] = e
	return popped, true

}

func (s *store) BLpop(ctx context.Context, keys []string, timeout time.Duration) (popped, bool) {
	s.mu.Lock()

	for _, k := range keys {
		e := s.data[k]

		if len(e.list) == 0 {
			continue
		}

		v := e.list[0]
		e.list = e.list[1:]

		if len(e.list) == 0 {
			delete(s.data, k)
		} else {
			s.data[k] = e
		}

		s.mu.Unlock()
		return popped{k, v}, true

	}

	ch := make(chan popped, 1)
	for _, k := range keys {
		s.waiters[k] = append(s.waiters[k], ch)
	}
	s.mu.Unlock()

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}

	select {
	case v := <-ch:
		return v, true
	case <-timeoutCh:
		return s.cancelWait(keys, ch)
	case <-ctx.Done():
		return s.cancelWait(keys, ch)
	}
}

func (s *store) EntryType(key string) (string, bool) {
	e, ok := s.data[key]
	if !ok {
		return "", false
	}

	return e.kind, true
}

func (s *store) XAdd(key string, rawID string, fields []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.data[key]

	if ok && st.kind != "stream" {
		return "", ErrWrongType
	}

	if !ok {
		st = entry{kind: "stream", stream: &Stream{}}
	}

	lastID := st.stream.LastID
	var streamId StreamID
	var err error
	switch {
	case rawID == "*":
		ms := uint64(time.Now().UnixMilli())

		if ms < lastID.Ms {
			ms = lastID.Ms
		}

		streamId = StreamID{Ms: ms, Seq: nextSeq(ms, lastID)}

	case strings.HasSuffix(rawID, "-*"):
		msStr, _, _ := strings.Cut(rawID, "-")
		ms, err := strconv.ParseUint(msStr, 10, 64)
		if err != nil {
			return "", ErrInvalidID
		}
		streamId = StreamID{Ms: ms, Seq: nextSeq(ms, lastID)}

	default:
		streamId, err = parseID(rawID)

		if err != nil {
			return "", err
		}
	}

	if streamId.Ms == 0 && streamId.Seq == 0 {
		return "", ErrIDZero
	}

	validSequance := isValidStreamId(st.stream.LastID, streamId)
	if !validSequance {
		return "", ErrIDTooSmall
	}
	st.stream.Entries = append(st.stream.Entries, StreamEntry{ID: streamId, Fields: fields})
	st.stream.LastID = streamId
	s.data[key] = st

	close(s.notify)
	s.notify = make(chan struct{})

	return streamId.String(), nil
}

func (s *store) XRange(key string, start string, end string) ([]StreamEntry, error) {
	var startID StreamID
	var endID StreamID
	var err error
	switch {
	case start == "-":
		startID = StreamID{Ms: 0, Seq: 0}
	default:
		startID, err = parseID(start)
	}

	if err != nil {
		return nil, err
	}

	switch {
	case end == "+":
		endID = StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}
	default:
		endID, err = parseID(end)
	}

	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return nil, nil // empty array, not an error
	}
	if e.kind != "stream" {
		return nil, ErrWrongType
	}

	found := e.stream.Filter(startID, endID)
	out := make([]StreamEntry, len(found))
	copy(out, found)
	return out, nil
}

func (s *store) XRead(keys []string, rawIDs []string) ([]streamResult, error) {
	afters := make([]StreamID, len(keys))
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i, k := range keys {
		if rawIDs[i] == "$" {
			if e, ok := s.data[k]; ok && e.kind == "stream" {
				afters[i] = e.stream.LastID
			}
		} else {
			id, err := parseID(rawIDs[i])
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			afters[i] = id
		}
	}

	var out []streamResult

	for i, k := range keys {
		entries := s.readAfterLocked(k, afters[i])
		if len(entries) > 0 {
			out = append(out, streamResult{key: k, entries: entries})
		}
	}

	return out, nil
}

func (s *store) XReadBlock(ctx context.Context, keys []string, rawIDs []string, timeout time.Duration) []streamResult {
	afters := make([]StreamID, len(keys))

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}

	s.mu.Lock()

	for i, k := range keys {
		if rawIDs[i] == "$" {
			if e, ok := s.data[k]; ok && e.kind == "stream" {
				afters[i] = e.stream.LastID
			}
		} else {
			id, err := parseID(rawIDs[i])
			if err != nil {
				s.mu.Unlock()
				return nil
			}

			afters[i] = id
		}
	}

	s.mu.Unlock()

	for {
		s.mu.Lock()
		var out []streamResult
		for i, k := range keys {
			if found := s.readAfterLocked(k, afters[i]); len(found) > 0 {
				out = append(out, streamResult{k, found})
			}
		}

		if len(out) > 0 {
			s.mu.Unlock()
			return out
		}

		ch := s.notify
		s.mu.Unlock()

		select {
		case <-ch:
			// nothing here
		case <-timeoutCh:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *store) Increment(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if ok && e.kind != "string" {
		return 0, ErrWrongType
	}

	var v int64
	if ok {
		var err error
		v, err = strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
	}
	v++

	e.kind = "string"
	e.value = strconv.FormatInt(v, 10)
	s.data[key] = e
	s.markWatchersLocked(key)
	return v, nil
}

func (s *store) Watch(tx *transaction, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		s.watchers[key] = append(s.watchers[key], tx)
		tx.watched = append(tx.watched, key)
	}
}

func (s *store) CheckDirty(tx *transaction) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return tx.dirty
}

func (s *store) Unwatch(tx *transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, k := range tx.watched {
		queue := s.watchers[k]

		out := queue[:0]

		for _, w := range queue {
			if w != tx {
				delete(s.watchers, k)
			} else {
				s.watchers[k] = out
			}
		}
		tx.watched = nil
		tx.dirty = false
	}
}

func (s *store) cancelWait(keys []string, ch chan popped) (popped, bool) {
	s.mu.Lock()
	stillWaiting := s.removeWaiter(keys, ch)
	s.mu.Unlock()

	if stillWaiting {
		return popped{}, false
	}

	return <-ch, true
}

func (s *store) removeWaiter(keys []string, ch chan popped) bool {
	found := false

	for _, key := range keys {
		queue := s.waiters[key]
		for i, w := range queue {
			if w == ch {
				s.waiters[key] = append(queue[:i], queue[i+1:]...)
				found = true
				break
			}
		}
	}

	return found
}

func (s *store) startSweeper(ctx context.Context, interval time.Duration, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.evictExpired()
			case <-ctx.Done():
				log.Println("sweeper stopped", ctx.Err())
				return
			}
		}
	}()
}

func (s *store) evictExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, e := range s.data {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			s.markWatchersLocked(k)
			delete(s.data, k)
		}
	}
}

func (s *store) readAfterLocked(key string, after StreamID) []StreamEntry {
	e, ok := s.data[key]
	if !ok || e.kind != "stream" {
		return nil
	}

	entries := e.stream.Entries

	i := 0
	for i < len(entries) && !less(after, entries[i].ID) {
		i++
	}

	out := make([]StreamEntry, len(entries)-i)
	copy(out, entries[i:])
	return out
}

func (s *store) markWatchersLocked(key string) {
	for _, tx := range s.watchers[key] {
		tx.dirty = true
	}
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}
func parseID(s string) (StreamID, error) {
	msStr, seqStr, ok := strings.Cut(s, "-")

	if !ok {
		return StreamID{}, ErrInvalidID
	}
	ms, err := strconv.ParseUint(msStr, 10, 64)
	if err != nil {
		return StreamID{}, ErrInvalidID
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return StreamID{}, ErrInvalidID
	}
	return StreamID{ms, seq}, nil
}

func isValidStreamId(lastId StreamID, newId StreamID) bool {
	if newId.Ms < lastId.Ms {
		return false
	} else if newId.Ms == lastId.Ms {
		if newId.Seq <= lastId.Seq {
			return false
		}
	}

	return true
}

func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.Ms, id.Seq)
}

func nextSeq(ms uint64, last StreamID) uint64 {
	if ms == last.Ms {
		return last.Seq + 1
	}
	if ms == 0 {
		return 1 // 0-0 is never a valid ID
	}
	return 0
}

func (st Stream) Filter(startId StreamID, endId StreamID) []StreamEntry {
	entries := st.Entries
	lo := 0

	for lo < len(entries) && less(entries[lo].ID, startId) {
		lo++
	}

	hi := lo

	for hi < len(entries) && !less(endId, entries[hi].ID) {
		hi++
	}

	return st.Entries[lo:hi]
}

func less(a StreamID, b StreamID) bool {
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}

	return a.Seq < b.Seq
}

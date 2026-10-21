package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/bits"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrBitValue   = errors.New("ERR bit is not an integer or out of range")
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrBitOffset  = errors.New("ERR bit offset is not an integer or out of range")
	ErrIDZero     = errors.New("ERR The ID specified in XADD must be greater than 0-0")
	ErrInvalidID  = errors.New("ERR Invalid stream ID specified as stream command argument")
	ErrWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrIDTooSmall = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
)

const maxBitOffset = 1<<32 - 1
const (
	MIN_LATITUDE  = -85.05112878
	MAX_LATITUDE  = 85.05112878
	MIN_LONGITUDE = -180.0
	MAX_LONGITUDE = 180.0

	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
)

type entry struct {
	kind      string
	value     string
	list      []string
	stream    *Stream
	expiresAt time.Time
	zset      *Zset
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

type ZsetEntry struct {
	name  string
	score float64
}

type Zset struct {
	scores map[string]float64
	sorted []ZsetEntry
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

func (s *store) ZAdd(key string, score float64, member string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dt, ok := s.data[key]

	if ok && dt.kind != "zset" {
		return 0, ErrWrongType
	}

	s.markWatchersLocked(key)

	if !ok {
		dt = entry{kind: "zset", zset: &Zset{scores: make(map[string]float64)}}
	}

	zSetEntry := ZsetEntry{score: score, name: member}

	_, exists := dt.zset.scores[member]

	if exists {
		dt.zset.updateMemberScore(member, score)
		return 0, nil
	}
	dt.zset.scores[member] = score
	i := sort.Search(len(dt.zset.sorted), func(i int) bool {
		return !compare(dt.zset.sorted[i], zSetEntry)
	})

	dt.zset.sorted = slices.Insert(dt.zset.sorted, i, zSetEntry)

	s.data[key] = dt

	return 1, nil
}

func (s *store) ZRank(key, member string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dt, ok := s.data[key]

	if !ok {
		return -1
	}

	_, ok = dt.zset.scores[member]
	if !ok {
		return -1
	}

	sorted := dt.zset.sorted
	entry := ZsetEntry{name: member, score: dt.zset.scores[member]}
	i := sort.Search(len(sorted), func(i int) bool {
		return !compare(sorted[i], entry)
	})

	return i
}

func (s *store) ZRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dt, ok := s.data[key]

	if !ok {
		return []string{}
	}

	n := len(dt.zset.sorted)

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

	out := make([]string, 0, stop-start+1)
	for _, e := range dt.zset.sorted[start : stop+1] {
		out = append(out, e.name)
	}

	return out
}

func (s *store) ZCard(key string) int {
	dt, ok := s.data[key]

	if !ok {
		return 0
	}

	return len(dt.zset.sorted)
}

func (s *store) ZScore(key string, member string) string {
	dt, ok := s.data[key]

	if !ok {
		return ""
	}

	score, ok := dt.zset.scores[member]
	if !ok {
		return ""
	}

	return strconv.FormatFloat(score, 'f', -1, 64)
}

func (s *store) ZRem(key, member string) int {
	dt, ok := s.data[key]
	if !ok {
		return 0
	}

	_, ok = dt.zset.scores[member]
	if !ok {
		return 0
	}

	delete(dt.zset.scores, member)

	i := 0
	for j, entry := range dt.zset.sorted {
		if entry.name == member {
			i = j
			break
		}
	}

	dt.zset.sorted = slices.Delete(dt.zset.sorted, i, i+1)
	return 1
}

func (s *store) SetBit(key string, offset uint64, value int) (int, error) {
	if value != 0 && value != 1 {
		return 0, ErrBitValue
	}

	if offset > maxBitOffset {
		return 0, ErrBitOffset
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]

	if ok && e.expired(time.Now()) {
		ok = false
		e = entry{}
	}

	if ok && e.kind != "string" {
		return 0, ErrWrongType
	}

	s.markWatchersLocked(key)

	byteIdx, mask := bitPosition(offset)
	buf := []byte(e.value)
	if uint64(len(buf)) <= byteIdx {
		grown := make([]byte, byteIdx+1)
		copy(grown, buf)
		buf = grown
	}

	old := 0

	if buf[byteIdx]&mask != 0 {
		old = 1
	}

	if value == 1 {
		buf[byteIdx] |= mask
	} else {
		buf[byteIdx] &^= mask
	}

	e.kind = "string"
	e.value = string(buf)
	s.data[key] = e

	return old, nil
}

func (s *store) GetBit(key string, offset uint64) (int, error) {
	if offset > maxBitOffset {
		return 0, ErrBitOffset
	}

	s.mu.RLock()
	s.mu.RUnlock()

	e, ok := s.data[key]

	if !ok || e.expired(time.Now()) {
		return 0, nil
	}

	if e.kind != "string" {
		return 0, ErrWrongType
	}

	byteIdx, mask := bitPosition(offset)
	if uint64(len(e.value)) <= byteIdx {
		return 0, nil // past the end: read as zero, never grow on a read
	}
	if e.value[byteIdx]&mask != 0 {
		return 1, nil
	}
	return 0, nil
}

func (s *store) Strlen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.expired(time.Now()) {
		return 0, nil
	}
	if e.kind != "string" {
		return 0, ErrWrongType
	}
	buf := []byte(e.value)
	return len(buf), nil
}

func (s *store) CountBit(key string, start, stop int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return 0
	}

	buf := []byte(e.value)

	if start > len(buf) {
		return 0
	}

	if start > stop {
		return 0
	}

	if stop >= len(buf) {
		stop = len(buf) - 1
	}

	if start == 0 && stop == 0 {
		stop = len(buf) - 1
	}

	var n int

	for i := start; i <= stop; i++ {
		n += countBits(buf[i])
	}

	return n
}

func (s *store) BitOpAnd(destKey, key1, key2 string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e1, ok := s.data[key1]
	str1 := ""
	if ok {
		str1 = e1.value
	}

	e2, ok := s.data[key2]

	str2 := ""
	if ok {
		str2 = e2.value
	}

	dest := make([]byte, max(len(str1), len(str2)))

	for i := 0; i < max(len(str1), len(str2)); i++ {
		dest[i] = byteAt(str1, i) & byteAt(str2, i)
	}

	if len(dest) == 0 {
		delete(s.data, destKey)
		return 0, nil
	}

	_, ok = s.data[destKey]
	if !ok {
		s.data[destKey] = entry{kind: "string", value: string(dest)}
	}

	return len(dest), nil
}

func (s *store) BitOpOr(destKey, key1, key2 string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e1, ok := s.data[key1]
	str1 := ""
	if ok {
		str1 = e1.value
	}

	e2, ok := s.data[key2]

	str2 := ""
	if ok {
		str2 = e2.value
	}

	dest := make([]byte, max(len(str1), len(str2)))

	for i := 0; i < max(len(str1), len(str2)); i++ {
		dest[i] = byteAt(str1, i) | byteAt(str2, i)
	}

	if len(dest) == 0 {
		delete(s.data, destKey)
		return 0, nil
	}

	_, ok = s.data[destKey]
	if !ok {
		s.data[destKey] = entry{kind: "string", value: string(dest)}
	}

	return len(dest), nil
}

func byteAt(s string, i int) byte {
	if i < len(s) {
		return s[i]
	}

	return 0
}

func countBits(b byte) int {
	return bits.OnesCount8(b)
}

func bitPosition(offset uint64) (bytIdx uint64, mask byte) {
	return offset / 8, 1 << (7 - offset%8)
}

func (zset *Zset) updateMemberScore(member string, newScore float64) {
	oldScore := zset.scores[member]
	if oldScore == newScore {
		return
	}

	sorted := zset.sorted

	i := sort.Search(len(sorted), func(i int) bool {
		return !compare(sorted[i], ZsetEntry{name: member, score: oldScore})
	})

	sorted = slices.Delete(sorted, i, i+1)

	zset.scores[member] = newScore
	newEntry := ZsetEntry{name: member, score: newScore}
	j := sort.Search(len(sorted), func(i int) bool {
		return !compare(sorted[i], newEntry)
	})

	sorted = slices.Insert(sorted, j, newEntry)

	zset.sorted = sorted
}

func (zset *Zset) sortZset(newEntry ZsetEntry) int {
	i := sort.Search(len(zset.sorted), func(i int) bool {
		return !compare(zset.sorted[i], newEntry)
	})

	return i
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

func (s *store) Keys(pattern string) []string {
	var keys []string

	for key, _ := range s.data {
		keys = append(keys, key)
	}

	return keys
}

func (s *store) GeoAdd(key string, long float64, lat float64, member string) (int, error) {
	if long < MIN_LONGITUDE || long > MAX_LONGITUDE || lat < MIN_LATITUDE || lat > MAX_LATITUDE {
		return 0, fmt.Errorf("ERR invalid longitude,latitude pair %f,%f", long, lat)
	}

	return s.ZAdd(key, float64(encode(lat, long)), member)
}

func (s *store) GeoExists(key string, members []string) ([][]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	found := make([][]string, len(members)) // all false by default

	e, ok := s.data[key]
	if !ok || e.expired(time.Now()) {
		return found, nil // missing key: every member is missing
	}
	if e.kind != "zset" {
		return nil, ErrWrongType
	}

	for i, m := range members {
		score, ok := e.zset.scores[m]
		if !ok {
			found[i] = []string{}
		} else {
			coordinates := decode(uint64(score))
			found[i] = []string{strconv.FormatFloat(coordinates.Longitude, 'f', -1, 64), strconv.FormatFloat(coordinates.Latitude, 'f', -1, 64)}
		}
	}
	return found, nil
}

func (s *store) GeoDist(key string, places []string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dt, _ := s.data[key]

	place1 := dt.zset.scores[places[0]]
	place2 := dt.zset.scores[places[1]]

	coordinates1 := decode(uint64(place1))
	coordinates2 := decode(uint64(place2))

	dist := geohashGetDistance(coordinates1.Longitude, coordinates1.Latitude, coordinates2.Longitude, coordinates2.Latitude)
	return strconv.FormatFloat(dist, 'f', -1, 64)
}

func (s *store) GeoSearch(key string, longitude, latitude float64, byRadius float64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dt := s.data[key]
	var locations []string

	for member, score := range dt.zset.scores {
		coordinates := decode(uint64(score))
		dist := haversine(longitude, latitude, coordinates.Longitude, coordinates.Latitude)
		if dist <= byRadius {
			locations = append(locations, member)
		}
	}

	return locations
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

func compare(a, b ZsetEntry) bool {
	if a.score != b.score {
		return a.score < b.score
	}

	return a.name < b.name
}

func spread(v uint32) uint64 {
	result := uint64(v)
	result = (result | (result << 16)) & 0x0000FFFF0000FFFF
	result = (result | (result << 8)) & 0x00FF00FF00FF00FF
	result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555
	return result
}

func interleave(x, y uint32) uint64 {
	xSpread := spread(x)
	ySpread := spread(y)
	yShifted := ySpread << 1
	return xSpread | yShifted
}

func encode(latitude, longitude float64) uint64 {
	// Normalize to the range 0-2^26
	normalizedLatitude := math.Pow(2, 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
	normalizedLongitude := math.Pow(2, 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

	// Truncate to integers
	latInt := uint32(normalizedLatitude)
	lonInt := uint32(normalizedLongitude)

	return interleave(latInt, lonInt)
}

type Coordinates struct {
	Latitude  float64
	Longitude float64
}

func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) Coordinates {
	// Calculate the grid boundaries
	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/math.Pow(2, 26))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/math.Pow(2, 26))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/math.Pow(2, 26))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/math.Pow(2, 26))

	// Calculate the center point of the grid cell
	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2

	return Coordinates{Latitude: latitude, Longitude: longitude}
}

func decode(geoCode uint64) Coordinates {
	// Align bits of both latitude and longitude to take even-numbered position
	y := geoCode >> 1
	x := geoCode

	// Compact bits back to 32-bit ints
	gridLatitudeNumber := compactInt64ToInt32(x)
	gridLongitudeNumber := compactInt64ToInt32(y)

	return convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber)
}

const degToRad = math.Pi / 180.0
const earthRadiusMeters = 6372797.560856

func geohashGetDistance(lon1, lat1, lon2, lat2 float64) float64 {
	lat1r := lat1 * degToRad
	lat2r := lat2 * degToRad
	u := math.Sin((lat2r - lat1r) / 2)
	v := math.Sin((lon2*degToRad - lon1*degToRad) / 2)
	return 2.0 * earthRadiusMeters * math.Asin(math.Sqrt(u*u+math.Cos(lat1r)*math.Cos(lat2r)*v*v))
}

func haversine(lon1, lat1, lon2, lat2 float64) float64 {
	toRad := func(d float64) float64 { return d * math.Pi / 180 }
	dLat := toRad(lat2 - lat1)
	dLon := toRad(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusMeters * math.Asin(math.Sqrt(a))
}

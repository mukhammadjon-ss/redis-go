package main

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestStoreSetGet(t *testing.T) {
	t.Run("miss on fresh store", func(t *testing.T) {
		s := newStore()
		got, ok := s.Get("nope")

		if ok || got != "" {
			t.Errorf(`Get("nope") = %q, %v; want "", false`, got, ok)
		}
	})

	t.Run("hit after set", func(t *testing.T) {
		s := newStore()
		s.Set("foo", "bar", time.Time{})
		got, ok := s.Get("foo")

		if !ok || got != "bar" {
			t.Errorf(`Get("k") = %q, %v; want "v", true`, got, ok)
		}
	})

	t.Run("see overwrites silently", func(t *testing.T) {
		s := newStore()
		s.Set("foo", "bar", time.Time{})
		s.Set("foo", "baz", time.Time{})
		got, ok := s.Get("foo")
		if !ok || got != "baz" {
			t.Errorf(`Get("k") = %q, %v; want "v2", true`, got, ok)
		}
	})

	t.Run("empty string value is not a miss", func(t *testing.T) {
		s := newStore()
		s.Set("foo", "", time.Time{})
		got, ok := s.Get("foo")
		if !ok || got != "" {
			// ok must be true: the key exists, its value happens to be "".
			// This is the case that distinguishes $0\r\n\r\n from $-1\r\n.
			t.Errorf(`Get("k") = %q, %v; want "", true`, got, ok)
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		s := newStore()
		s.Set("a", "1", time.Time{})
		s.Set("b", "2", time.Time{})
		if got, _ := s.Get("a"); got != "1" {
			t.Errorf(`Get("a") = %q; want "1"`, got)
		}
		if got, _ := s.Get("b"); got != "2" {
			t.Errorf(`Get("b") = %q; want "2"`, got)
		}
	})
}

func TestStoreRPush(t *testing.T) {
	t.Run("Push single element", func(t *testing.T) {
		s := newStore()
		s.RPush("l", "a")
		e := s.data["l"]
		if len(e.list) != 1 {
			t.Errorf(`RPush("a") = %d; want "1"`, len(e.list))
		}
	})

	t.Run("Push multiple element", func(t *testing.T) {
		s := newStore()
		s.RPush("l", "a", "b", "c", "d", "e")
		e := s.data["l"]
		if len(e.list) != 5 {
			t.Errorf(`RPush("a") = %d; want "5"`, len(e.list))
		}

		want := []string{"a", "b", "c", "d", "e"}

		if !slices.Equal(e.list, want) {
			t.Errorf("got %v, want %v", e.list, want)
		}
	})
}

func TestStoreLPush(t *testing.T) {
	t.Run("Push single element", func(t *testing.T) {
		s := newStore()
		s.LPush("l", "a")
		e := s.data["l"]
		if len(e.list) != 1 {
			t.Errorf(`RPush("a") = %d; want "1"`, len(e.list))
		}
	})

	t.Run("LPush multiple element", func(t *testing.T) {
		s := newStore()
		s.LPush("l", "a", "b", "c", "d", "e")
		e := s.data["l"]
		if len(e.list) != 5 {
			t.Errorf(`RPush("a") = %d; want "5"`, len(e.list))
		}

		want := []string{"e", "d", "c", "b", "a"}

		if !slices.Equal(e.list, want) {
			t.Errorf("got %v, want %v", e.list, want)
		}
	})
}

func TestStoreLLen(t *testing.T) {
	t.Run("Llen non-existant list", func(t *testing.T) {
		s := newStore()
		length := s.LLen("l")

		if length != 0 {
			t.Errorf(`Llen("l") = %d; want "0"`, length)
		}
	})

	t.Run("Llen list with multiple elements", func(t *testing.T) {
		s := newStore()
		s.RPush("l", "a", "b", "c", "d", "e")
		length := s.LLen("l")
		if length != 5 {
			t.Errorf(`Llen("l") = %d; want "5"`, length)
		}
	})
}

func TestStoreLPop(t *testing.T) {
	t.Run("Llen non-existant list", func(t *testing.T) {
		s := newStore()
		_, ok := s.LPop("l", 1)

		if ok {
			t.Errorf(`Should not be ok`)
		}

	})

	t.Run("Llen list with multiple elements", func(t *testing.T) {
		s := newStore()
		s.RPush("l", "a", "b", "c", "d", "e")
		n := 2
		ans, ok := s.LPop("l", n)

		if !ok {
			t.Errorf(`Should be ok`)
		}

		e, _ := s.data["l"]
		want := []string{"a", "b"}
		fmt.Println(ans)
		if !slices.Equal(ans, want) {
			t.Errorf(`LPop("l") = %v; want [a b]`, ans)
		}

		if len(e.list) != 3 {
			t.Errorf(`Len("l") = %d; want "4"`, len(e.list))
		}
	})
}

func TestStoreLRange(t *testing.T) {
	t.Run("LRange - the list doesn't exist", func(t *testing.T) {
		s := newStore()

		e := s.LRange("list_key", 1, 4)

		if len(e) > 0 {
			t.Errorf(`Lrange("list_key") = %d; want "0"`, len(e))
		}
	})

	t.Run("LRange - start index is greater than or equal to the list's length", func(t *testing.T) {
		s := newStore()
		s.RPush("list_key", "a")
		e := s.LRange("list_key", 3, 4)

		if len(e) > 0 {
			t.Errorf(`Lrange("list_key") = %d; want "0"`, len(e))
		}
	})

	t.Run("LRange - the stop index is greater than or equal to the list's length", func(t *testing.T) {
		s := newStore()
		s.RPush("list_key", "a", "b", "c")
		got := s.LRange("list_key", 1, 4)
		want := []string{"b", "c"}
		if !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("LRange - the start index is greater than the stop index", func(t *testing.T) {
		s := newStore()
		s.RPush("list_key", "a", "b", "c")
		got := s.LRange("list_key", 4, 1)
		if len(got) > 0 {
			t.Errorf("got %v but want empty list", got)
		}
	})

	t.Run("LRange - the start and stop are inside list", func(t *testing.T) {
		s := newStore()
		s.RPush("list_key", "a", "b", "c", "d", "e")
		got := s.LRange("list_key", 1, 3)
		want := []string{"b", "c", "d"}
		if !slices.Equal(got, want) {
			t.Errorf("got %v but want %v", got, want)
		}
	})
}

func TestStoreConcurrent(t *testing.T) {
	s := newStore()

	const (
		goroutines = 10
		iterations = 1000
	)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.Set("shared", fmt.Sprintf("g%d-%d", id, j), time.Time{})
				s.Get("shared")
				s.Set(fmt.Sprintf("key-%d", id), "v", time.Time{})
				s.Get(fmt.Sprintf("key-%d", goroutines-1-id))
			}
		}(i)
	}
	wg.Wait()
	fmt.Println(s.Get("shared"))
	if _, ok := s.Get("shared"); !ok {
		t.Error(`Get("shared") after concurrent writes: key missing`)
	}
}

func TestStoreExpiry(t *testing.T) {
	t.Run("future deadline still hits", func(t *testing.T) {
		s := newStore()
		s.Set("k", "v", time.Now().Add(time.Hour))
		got, ok := s.Get("k")
		if !ok || got != "v" {
			t.Errorf(`Get("k") = %q, %v; want "v", true`, got, ok)
		}
	})

	t.Run("past deadline is a miss", func(t *testing.T) {
		s := newStore()
		s.Set("k", "v", time.Now().Add(-time.Second))
		got, ok := s.Get("k")
		if ok || got != "" {
			t.Errorf(`Get("k") = %q, %v; want "", false`, got, ok)
		}
	})

	t.Run("zero time never expires", func(t *testing.T) {
		s := newStore()
		s.Set("k", "v", time.Time{})
		got, ok := s.Get("k")
		if !ok || got != "v" {
			t.Errorf(`Get("k") = %q, %v; want "v", true`, got, ok)
		}
	})

	t.Run("expiry exactly now is treated as expired", func(t *testing.T) {
		s := newStore()
		s.Set("k", "v", time.Now())
		time.Sleep(time.Millisecond) // ensure "now" has passed
		got, ok := s.Get("k")
		if ok || got != "" {
			t.Errorf(`Get("k") = %q, %v; want "", false (boundary case)`, got, ok)
		}
	})
}

func TestStoreXAdd(t *testing.T) {
	t.Run("XAdd - wrong key", func(t *testing.T) {
		s := newStore()
		s.data["l"] = entry{kind: "string"}
		_, err := s.XAdd("l", "1-1", []string{})

		if err == nil {
			t.Errorf(`Type of Entry must be stream but got string`)
		}
	})

	t.Run("XAdd - wrong type", func(t *testing.T) {
		s := newStore()
		s.data["l"] = entry{kind: "string"}
		_, err := s.XAdd("l", "1-1", []string{})

		if !errors.Is(err, ErrWrongType) {
			t.Errorf(`Type of Entry must be stream but got string`)
		}
	})

	t.Run("XAdd - ErrIDZero", func(t *testing.T) {
		s := newStore()
		// streamId := StreamID{Ms: 0, Seq: 0}
		s.data["l"] = entry{kind: "stream", stream: &Stream{}}
		_, err := s.XAdd("l", "0-0", []string{})

		if !errors.Is(err, ErrIDZero) {
			t.Errorf(`Should get ErrIDZero but got %v`, err.Error())
		}
	})

	t.Run("XAdd - ErrIDTooSmall", func(t *testing.T) {
		s := newStore()
		// streamId := StreamID{Ms: 0, Seq: 0}
		s.data["l"] = entry{kind: "stream", stream: &Stream{}}
		_, err := s.XAdd("l", "1-1", []string{})
		_, err = s.XAdd("l", "1-2", []string{})
		_, err = s.XAdd("l", "1-1", []string{})
		if !errors.Is(err, ErrIDTooSmall) {
			t.Errorf(`Should get ErrIDZero but got %v`, err.Error())
		}
	})
}

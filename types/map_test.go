package types

import (
	"sync"
	"testing"
)

var (
	sinkMap   *Map[any, any]
	sinkValue any
)

// BenchmarkNewMap measures building the per-operation Args map from static route args.
func BenchmarkNewMap(b *testing.B) {
	b.Run("with_data_nil", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkMap = NewMapWithData[any, any](nil)
		}
	})
	b.Run("with_data", func(b *testing.B) {
		data := map[any]any{"tenant": "acme", "recorder_disabled": false}
		b.ReportAllocs()
		for b.Loop() {
			sinkMap = NewMapWithData(data)
		}
	})
	b.Run("copy_on_write_nil", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkMap = NewMapCopyOnWrite[any, any](nil)
		}
	})
	b.Run("copy_on_write", func(b *testing.B) {
		data := map[any]any{"tenant": "acme", "recorder_disabled": false}
		b.ReportAllocs()
		for b.Loop() {
			sinkMap = NewMapCopyOnWrite(data)
		}
	})
}

func BenchmarkMapGet(b *testing.B) {
	b.Run("empty", func(b *testing.B) {
		m := NewMap[any, any]()
		b.ReportAllocs()
		for b.Loop() {
			sinkValue, _ = m.Get("missing")
		}
	})
	b.Run("hit", func(b *testing.B) {
		m := NewMapWithData(map[any]any{"tenant": "acme"})
		b.ReportAllocs()
		for b.Loop() {
			sinkValue, _ = m.Get("tenant")
		}
	})
}

func TestMapCopyOnWrite(t *testing.T) {
	base := map[string]int{"a": 1, "b": 2}
	m := NewMapCopyOnWrite(base)

	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v, %v; want 1, true", v, ok)
	}

	m.Delete("missing")
	if !m.borrowed {
		t.Fatal("deleting a missing key cloned the borrowed data")
	}

	m.Set("a", 10)
	m.Delete("b")
	if base["a"] != 1 || base["b"] != 2 || len(base) != 2 {
		t.Fatalf("writes reached the borrowed map: %v", base)
	}
	if v, _ := m.Get("a"); v != 10 {
		t.Fatalf("Get(a) = %v after Set; want 10", v)
	}
	if _, ok := m.Get("b"); ok {
		t.Fatal("Get(b) found a deleted key")
	}

	m.Delete("a")
	if _, ok := m.Get("a"); ok {
		t.Fatal("Get(a) found a key deleted from a now empty map")
	}
}

func TestMapZeroValue(t *testing.T) {
	var m Map[string, int]
	if _, ok := m.Get("a"); ok {
		t.Fatal("zero Map has a key")
	}
	m.Delete("a")
	m.Set("a", 1)
	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v, %v; want 1, true", v, ok)
	}
}

// TestJSONMapScanUpdatesLookups guards the lock-free empty check: Scan replaces data and must update its size.
func TestJSONMapScanUpdatesLookups(t *testing.T) {
	m := NewJSONMap[any]()
	if err := m.Scan([]byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("a"); !ok {
		t.Fatal("Get(a) missed a scanned key")
	}
	if err := m.Scan(nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("Get(a) found a key after scanning NULL")
	}
}

func TestMapConcurrentCopyOnWrite(t *testing.T) {
	base := map[int]int{0: 0}
	m := NewMapCopyOnWrite(base)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				if g%2 == 0 {
					m.Set(i, i)
					m.Delete(i)
				} else {
					m.Get(i)
				}
			}
		})
	}
	wg.Wait()

	if len(base) != 1 || base[0] != 0 {
		t.Fatalf("writes reached the borrowed map: %v", base)
	}
}

func TestMapRange(t *testing.T) {
	m := NewMapCopyOnWrite(map[string]int{"a": 1, "b": 2, "c": 3})

	seen := map[string]int{}
	m.Range(func(k string, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) != 3 || seen["a"] != 1 || seen["b"] != 2 || seen["c"] != 3 {
		t.Fatalf("Range saw %v", seen)
	}

	calls := 0
	m.Range(func(string, int) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("Range called f %d times after it returned false, want 1", calls)
	}

	sum := 0
	for _, v := range m.Range {
		sum += v
	}
	if sum != 6 {
		t.Fatalf("range over m.Range summed %d, want 6", sum)
	}

	var empty Map[string, int]
	empty.Range(func(string, int) bool {
		t.Fatal("Range called f on an empty map")
		return true
	})
}

// TestMapRangeReleasesLock checks that Range unlocks after an early break and after a panic in f.
func TestMapRangeReleasesLock(t *testing.T) {
	m := NewMapWithData(map[string]int{"a": 1, "b": 2})

	for range m.Range {
		break
	}
	func() {
		defer func() { _ = recover() }()
		m.Range(func(string, int) bool { panic("boom") })
	}()

	m.Set("c", 3) // deadlocks if Range kept the read lock
}

func TestMapConcurrentRange(t *testing.T) {
	m := NewMap[int, int]()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				if g%2 == 0 {
					m.Set(i, i)
					m.Delete(i - 1)
				} else {
					m.Range(func(k, v int) bool { return k == v })
				}
			}
		})
	}
	wg.Wait()
}

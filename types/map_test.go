package types

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
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

func TestNewMapWithDataCopies(t *testing.T) {
	data := map[string]int{"a": 1}
	m := NewMapWithData(data)
	data["a"], data["b"] = 10, 2
	if v, _ := m.Get("a"); v != 1 || m.Len() != 1 {
		t.Fatalf("Get(a) = %d, Len() = %d after the input changed; want 1, 1", v, m.Len())
	}

	m.Set("c", 3)
	if _, ok := data["c"]; ok {
		t.Fatal("Set wrote to the input map")
	}

	if c := NewMapWithData[string, int](nil).Clone(); c == nil || len(c) != 0 {
		t.Fatalf("NewMapWithData(nil).Clone() = %#v, want an empty map", c)
	}
}

func TestMapLen(t *testing.T) {
	m := NewMapCopyOnWrite(map[string]int{"a": 1, "b": 2})
	steps := []struct {
		name string
		do   func()
		want int
	}{
		{"borrowed", func() {}, 2},
		{"new key", func() { m.Set("c", 3) }, 3},
		{"existing key", func() { m.Set("a", 10) }, 3},
		{"missing key", func() { m.Delete("x") }, 3},
		{"delete", func() { m.Delete("a") }, 2},
	}
	for _, step := range steps {
		step.do()
		if got := m.Len(); got != step.want {
			t.Fatalf("%s: Len() = %d, want %d", step.name, got, step.want)
		}
	}
}

func TestMapClone(t *testing.T) {
	var zero Map[string, int]
	if c := zero.Clone(); c != nil {
		t.Fatalf("zero Map Clone() = %#v, want nil", c)
	}

	m := NewMapWithData(map[string]int{"a": 1})
	c := m.Clone()
	c["a"], c["b"] = 10, 2
	if v, _ := m.Get("a"); v != 1 || m.Len() != 1 {
		t.Fatalf("writing to the clone changed the map: Get(a) = %d, Len() = %d", v, m.Len())
	}
}

// TestMapNilReceiver checks that a nil *Map reads like a nil Go map.
func TestMapNilReceiver(t *testing.T) {
	var m *Map[string, int]

	if v, ok := m.Get("a"); ok || v != 0 {
		t.Fatalf("Get(a) = %d, %v; want 0, false", v, ok)
	}
	if v := m.GetTyped[int]("a"); v != 0 {
		t.Fatalf("GetTyped(a) = %d, want 0", v)
	}
	if n := m.Len(); n != 0 {
		t.Fatalf("Len() = %d, want 0", n)
	}
	if c := m.Clone(); c != nil {
		t.Fatalf("Clone() = %#v, want nil", c)
	}
	m.Range(func(string, int) bool {
		t.Fatal("Range called f")
		return true
	})
	m.Delete("a")

	defer func() {
		if recover() == nil {
			t.Fatal("Set on a nil *Map did not panic")
		}
	}()
	m.Set("a", 1)
}

type stringer string

func (s stringer) String() string { return string(s) }

type port int

func TestMapGetTyped(t *testing.T) {
	m := NewMapWithData(map[string]any{
		"int":      json.Number("-42"),
		"maxInt64": json.Number(strconv.FormatInt(math.MaxInt64, 10)),
		"maxUint":  json.Number(strconv.FormatUint(math.MaxUint64, 10)),
		"int8Over": json.Number("128"),
		"negative": json.Number("-1"),
		"fraction": json.Number("1.5"),
		"exponent": json.Number("1e3"),
		"bad":      json.Number("abc"),
		"bool":     true,
		"string":   "text",
		"stringer": stringer("s"),
		"float":    float64(7),
	})

	tests := []struct {
		name string
		get  func() any
		want any
	}{
		{"int", func() any { return m.GetTyped[int]("int") }, -42},
		{"int8", func() any { return m.GetTyped[int8]("int") }, int8(-42)},
		{"int16", func() any { return m.GetTyped[int16]("int") }, int16(-42)},
		{"int32", func() any { return m.GetTyped[int32]("int") }, int32(-42)},
		{"int64 keeps precision", func() any { return m.GetTyped[int64]("maxInt64") }, int64(math.MaxInt64)},
		{"uint64 keeps precision", func() any { return m.GetTyped[uint64]("maxUint") }, uint64(math.MaxUint64)},
		{"uint", func() any { return m.GetTyped[uint]("maxUint") }, uint(math.MaxUint)},
		{"uint8", func() any { return m.GetTyped[uint8]("int8Over") }, uint8(128)},
		{"uint16", func() any { return m.GetTyped[uint16]("int8Over") }, uint16(128)},
		{"uint32", func() any { return m.GetTyped[uint32]("int8Over") }, uint32(128)},
		{"float32", func() any { return m.GetTyped[float32]("fraction") }, float32(1.5)},
		{"float64 from an exponent", func() any { return m.GetTyped[float64]("exponent") }, float64(1000)},
		{"out of range", func() any { return m.GetTyped[int8]("int8Over") }, int8(0)},
		{"negative to unsigned", func() any { return m.GetTyped[uint]("negative") }, uint(0)},
		{"fraction to integer", func() any { return m.GetTyped[int]("fraction") }, 0},
		{"exponent to integer", func() any { return m.GetTyped[int]("exponent") }, 0},
		{"not a number", func() any { return m.GetTyped[float64]("bad") }, float64(0)},
		{"number to string", func() any { return m.GetTyped[string]("int") }, ""},
		{"number kept", func() any { return m.GetTyped[json.Number]("int") }, json.Number("-42")},
		{"named type", func() any { return m.GetTyped[port]("int") }, port(0)},
		{"bool", func() any { return m.GetTyped[bool]("bool") }, true},
		{"string", func() any { return m.GetTyped[string]("string") }, "text"},
		{"interface", func() any { return m.GetTyped[fmt.Stringer]("stringer") }, fmt.Stringer(stringer("s"))},
		{"float64 is not converted", func() any { return m.GetTyped[int]("float") }, 0},
		{"mismatched type", func() any { return m.GetTyped[bool]("string") }, false},
		{"missing key", func() any { return m.GetTyped[string]("missing") }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.get(); got != tt.want {
				t.Fatalf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestMapResetCopyOnWrite(t *testing.T) {
	base := map[string]int{"a": 1}
	var m Map[string, int]
	m.Set("old", 1)
	m.ResetCopyOnWrite(base)
	if _, ok := m.Get("old"); ok || m.Len() != 1 {
		t.Fatalf("kept what it held: %v", m.Clone())
	}
	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v, %v; want 1, true", v, ok)
	}
	m.Set("a", 2)
	if base["a"] != 1 {
		t.Fatalf("a write reached the borrowed map: %v", base)
	}

	m.ResetCopyOnWrite(nil)
	if m.Len() != 0 {
		t.Fatal("not empty")
	}
	m.Set("b", 1)
	if v, _ := m.Get("b"); v != 1 {
		t.Fatalf("Get(b) = %v; want 1", v)
	}
}

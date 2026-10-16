package types

import (
	"encoding/json"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
)

// Map is a concurrency-safe generic map. The zero Map is empty and ready to use.
//
// A nil *Map reads like a nil Go map: Get, GetTyped, Len, Range and Clone find nothing and Delete does
// nothing, while Set panics.
type Map[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
	// borrowed reports that data was handed to NewMapCopyOnWrite and must be cloned before the first write.
	borrowed bool
	// size mirrors len(data) so lookups in an empty map skip the lock.
	size atomic.Int64
}

// NewMap creates an initialized empty Map.
func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		data: make(map[K]V),
	}
}

// NewMapWithData creates a Map initialized with the given data.
//
// The provided map is shallow-cloned so subsequent mutations to the original
// map do not bypass Map's synchronization.
func NewMapWithData[K comparable, V any](data map[K]V) *Map[K, V] {
	if data == nil {
		return NewMap[K, V]()
	}

	m := &Map[K, V]{}
	m.setData(maps.Clone(data), false)
	return m
}

// NewMapCopyOnWrite creates a Map that reads data in place and clones it before the first write.
//
// It avoids copying static data that most users of the Map only read, such as route args shared by
// every request. data is shared by every Map built from it, so nothing may modify it afterwards.
func NewMapCopyOnWrite[K comparable, V any](data map[K]V) *Map[K, V] {
	m := &Map[K, V]{}
	m.setData(data, data != nil)
	return m
}

// ResetCopyOnWrite makes m read data in place and clone it before its first write, like a Map that
// NewMapCopyOnWrite returns, and drops what m held. It lets a Map live inside a struct that a transport
// allocates for each operation along with the operation's other values, rather than in an allocation of
// its own. data is shared, so nothing may modify it afterwards.
func (m *Map[K, V]) ResetCopyOnWrite(data map[K]V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setData(data, data != nil)
}

// setData replaces the underlying map. m.mu must be held for writing once m is shared.
func (m *Map[K, V]) setData(data map[K]V, borrowed bool) {
	m.data = data
	m.borrowed = borrowed
	m.size.Store(int64(len(data)))
}

// own makes data writable, allocating it or cloning borrowed data. m.mu must be held for writing.
func (m *Map[K, V]) own() {
	if m.borrowed {
		m.data = maps.Clone(m.data)
		m.borrowed = false
	}
	if m.data == nil {
		m.data = make(map[K]V)
	}
}

// Get returns the value associated with key and whether the key exists.
func (m *Map[K, V]) Get(key K) (V, bool) {
	// Writers store size before unlocking, so zero means the map was empty when this Get took effect.
	if m.Len() == 0 {
		var zero V
		return zero, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[key]
	return value, exists
}

// GetTyped returns the value associated with key as a T, and the zero T when the key does not exist
// or its value cannot be returned as a T.
//
// A value that is a T is returned as is. For an interface T, that is a value that implements T. A
// json.Number, which JSONMap holds for the numbers in interface values, is converted when T is exactly
// one of int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32 or float64.
// Integer types keep the full precision of the number, but take only numbers written without a
// fraction or an exponent and within their range. A named type, such as a type Port int, is not
// converted.
func (m *Map[K, V]) GetTyped[T any](key K) (res T) {
	value, ok := m.Get(key)
	if !ok {
		return res
	}

	anyVal := any(value)

	// Fast path: value already has exactly the requested type.
	if typed, ok := anyVal.(T); ok {
		return typed
	}

	// Special handling for JSON numbers.
	number, ok := anyVal.(json.Number)
	if !ok {
		return res
	}

	switch dst := any(&res).(type) {
	case *int:
		v, err := strconv.ParseInt(number.String(), 10, strconv.IntSize)
		if err == nil {
			*dst = int(v)
		}

	case *int8:
		v, err := strconv.ParseInt(number.String(), 10, 8)
		if err == nil {
			*dst = int8(v)
		}

	case *int16:
		v, err := strconv.ParseInt(number.String(), 10, 16)
		if err == nil {
			*dst = int16(v)
		}

	case *int32:
		v, err := strconv.ParseInt(number.String(), 10, 32)
		if err == nil {
			*dst = int32(v)
		}

	case *int64:
		v, err := strconv.ParseInt(number.String(), 10, 64)
		if err == nil {
			*dst = v
		}

	case *uint:
		v, err := strconv.ParseUint(number.String(), 10, strconv.IntSize)
		if err == nil {
			*dst = uint(v)
		}

	case *uint8:
		v, err := strconv.ParseUint(number.String(), 10, 8)
		if err == nil {
			*dst = uint8(v)
		}

	case *uint16:
		v, err := strconv.ParseUint(number.String(), 10, 16)
		if err == nil {
			*dst = uint16(v)
		}

	case *uint32:
		v, err := strconv.ParseUint(number.String(), 10, 32)
		if err == nil {
			*dst = uint32(v)
		}

	case *uint64:
		v, err := strconv.ParseUint(number.String(), 10, 64)
		if err == nil {
			*dst = v
		}

	case *float32:
		v, err := strconv.ParseFloat(number.String(), 32)
		if err == nil {
			*dst = float32(v)
		}

	case *float64:
		v, err := strconv.ParseFloat(number.String(), 64)
		if err == nil {
			*dst = v
		}
	}

	return res
}

// Len returns the number of keys. It takes no lock.
func (m *Map[K, V]) Len() int {
	if m == nil {
		return 0
	}
	return int(m.size.Load())
}

// Set associates value with key.
func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.own()
	m.data[key] = value
	m.size.Store(int64(len(m.data)))
}

// Delete removes key if it exists.
func (m *Map[K, V]) Delete(key K) {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return
	}

	m.own()
	delete(m.data, key)
	m.size.Store(int64(len(m.data)))
}

// Range calls f for each key and value until f returns false. It holds the read lock throughout, so f
// sees a consistent view without the map being copied. f must not call methods of m: a write
// deadlocks, and so can a read while a writer waits.
//
// Range is an iter.Seq2, so `for k, v := range m.Range` works too.
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	if m.Len() == 0 {
		return
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for key, value := range m.data {
		if !f(key, value) {
			return
		}
	}
}

// Clone returns a shallow copy of the underlying map. It returns nil when there is none, as for the
// zero Map, a nil *Map or a JSONMap scanned from NULL, so check the result before writing to it.
func (m *Map[K, V]) Clone() map[K]V {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.data == nil {
		return nil
	}

	return maps.Clone(m.data)
}

package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
)

// Map is a concurrency-safe generic map. The zero Map is empty and ready to use.
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
	if m.size.Load() == 0 {
		var zero V
		return zero, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[key]
	return value, exists
}

// GetTyped returns the value associated with key as T.
//
// If the stored value already has type T, it is returned directly.
// json.Number values are additionally converted to supported numeric types
// while preserving integer precision.
//
// If the key does not exist, the value has an incompatible type, or a numeric
// conversion fails, the zero value of T is returned.
func (m *Map[K, V]) GetTyped[T any](key K) (res T) {
	if m == nil {
		return res
	}

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

// Set associates value with key.
//
// This operation is safe for concurrent use.
func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.own()
	m.data[key] = value
	m.size.Store(int64(len(m.data)))
}

// Delete removes key if it exists.
//
// This operation is safe for concurrent use.
func (m *Map[K, V]) Delete(key K) {
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
	if m.size.Load() == 0 {
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

// Clone returns a shallow copy of the underlying map.
func (m *Map[K, V]) Clone() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.data == nil {
		return nil
	}

	return maps.Clone(m.data)
}

// JSONMap is a concurrency-safe map intended for JSON data.
//
// It additionally implements:
//   - driver.Valuer
//   - sql.Scanner
//
// When values are decoded into any, JSON numbers are stored as json.Number
// rather than float64 to prevent precision loss for large integers such as uint64.
type JSONMap[V any] struct {
	Map[string, V]
}

// NewJSONMap creates an initialized empty JSONMap.
func NewJSONMap[V any]() *JSONMap[V] {
	return &JSONMap[V]{
		Map: Map[string, V]{
			data: make(map[string]V),
		},
	}
}

// NewJSONMapWithData creates a JSONMap initialized with the given data.
//
// The provided map is shallow-cloned so subsequent mutations to the original
// map do not bypass JSONMap's synchronization.
func NewJSONMapWithData[V any](data map[string]V) *JSONMap[V] {
	if data == nil {
		return NewJSONMap[V]()
	}

	m := &JSONMap[V]{}
	m.setData(maps.Clone(data), false)
	return m
}

// Value implements driver.Valuer.
//
// This allows JSONMap to be passed directly to database/sql:
//
//	_, err := db.ExecContext(
//		ctx,
//		"INSERT INTO example (metadata) VALUES (?)",
//		metadata,
//	)
//
// A nil JSONMap or JSONMap with nil underlying data is stored as SQL NULL.
func (m *JSONMap[V]) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.data == nil {
		return nil, nil
	}

	data, err := json.Marshal(m.data)
	if err != nil {
		return nil, fmt.Errorf("json map: marshal: %w", err)
	}

	return data, nil
}

// Scan implements sql.Scanner.
//
// Supported database values are:
//   - NULL
//   - []byte
//   - string
//
// When values are decoded into any, JSON numbers are stored as json.Number
// rather than float64 to prevent precision loss for large integers.
func (m *JSONMap[V]) Scan(src any) error {
	if m == nil {
		return fmt.Errorf("json map: Scan called on nil receiver")
	}

	if src == nil {
		m.mu.Lock()
		m.setData(nil, false)
		m.mu.Unlock()

		return nil
	}

	var data []byte

	switch value := src.(type) {
	case []byte:
		data = value

	case string:
		data = []byte(value)

	default:
		return fmt.Errorf(
			"json map: unsupported Scan source type %T",
			src,
		)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))

	// When decoding into interface values, preserve JSON numbers as
	// json.Number instead of converting them to float64.
	decoder.UseNumber()

	var decoded map[string]V
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("json map: decode: %w", err)
	}

	m.mu.Lock()
	m.setData(decoded, false)
	m.mu.Unlock()

	return nil
}

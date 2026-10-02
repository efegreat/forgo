package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"sync"
)

// Map is a concurrency-safe generic map.
type Map[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
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

	return &Map[K, V]{
		data: maps.Clone(data),
	}
}

// Get returns the value associated with key and whether the key exists.
func (m *Map[K, V]) Get(key K) (V, bool) {
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

	if m.data == nil {
		m.data = make(map[K]V)
	}

	m.data[key] = value
}

// Delete removes key if it exists.
//
// This operation is safe for concurrent use.
func (m *Map[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data != nil {
		delete(m.data, key)
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

	return &JSONMap[V]{
		Map: Map[string, V]{
			data: maps.Clone(data),
		},
	}
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
		m.data = nil
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
	m.data = decoded
	m.mu.Unlock()

	return nil
}

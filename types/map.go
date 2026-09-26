package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
)

import "sync"

type Map struct {
	data map[string]any
	mu   sync.RWMutex
}

func NewMap() *Map {
	return &Map{data: make(map[string]any)}
}

func NewMapWithData(data map[string]any) *Map {
	return &Map{data: data}
}

func (m *Map) Get(key string) (value any, exists bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists = m.data[key]
	return
}

func (m *Map) GetTyped[T any](key string) (res T) {
	value, ok := m.Get(key)
	if !ok {
		return res
	}

	// Fast path: value already has exactly the requested type.
	if typed, ok := value.(T); ok {
		return typed
	}

	number, ok := value.(json.Number)
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

func (m *Map) Clone() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data := make(map[string]any, len(m.data))
	maps.Copy(data, m.data)

	return data
}

func (m *Map) Set(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = make(map[string]any)
	}
	m.data[key] = value
}

// Delete deletes the key from the Data, if it exists.
// This operation is safe to be used by concurrent go-routines
func (m *Map) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data != nil {
		delete(m.data, key)
	}
}

// Value implements driver.Valuer.
//
// This allows Map to be passed directly to database/sql:
//
//	_, err := db.ExecContext(ctx,
//	    "INSERT INTO example (metadata) VALUES (?)",
//	    metadata,
func (m *Map) Value() (driver.Value, error) {
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
		return nil, fmt.Errorf("map: marshal: %w", err)
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
// Numbers are decoded as json.Number rather than float64 to prevent
// precision loss for values such as uint64.
func (m *Map) Scan(src any) error {
	if m == nil {
		return fmt.Errorf("map: Scan called on nil receiver")
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
			"map: unsupported Scan source type %T",
			src,
		)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))

	// Critical:
	// Without this, JSON numbers decoded into `any` become float64.
	//
	// uint64(math.MaxUint64), for example, cannot be represented exactly
	// by a float64.
	decoder.UseNumber()

	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("map: decode: %w", err)
	}

	m.mu.Lock()
	m.data = decoded
	m.mu.Unlock()

	return nil
}

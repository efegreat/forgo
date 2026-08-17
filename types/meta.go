package types

import (
	"sync"
	"time"
)

type Map struct {
	Data map[any]any
	mu   sync.RWMutex
}

func NewMap() *Map {
	return &Map{Data: make(map[any]any)}
}

func (m *Map) Get(key any) (value any, exists bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists = m.Data[key]
	return
}

func (m *Map) Set(key any, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Data == nil {
		m.Data = make(map[any]any)
	}
	m.Data[key] = value
}

// Delete deletes the key from the Data, if it exists.
// This operation is safe to be used by concurrent go-routines
func (m *Map) Delete(key any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Data != nil {
		delete(m.Data, key)
	}
}

func getTyped[T any](m *Map, key any) (res T) {
	if val, ok := m.Get(key); ok && val != nil {
		res, _ = val.(T)
	}
	return
}

// GetString returns the value associated with the key as a string.
func (m *Map) GetString(key any) string {
	return getTyped[string](m, key)
}

// GetBool returns the value associated with the key as a boolean.
func (m *Map) GetBool(key any) bool {
	return getTyped[bool](m, key)
}

// GetInt returns the value associated with the key as an integer.
func (m *Map) GetInt(key any) int {
	return getTyped[int](m, key)
}

// GetInt8 returns the value associated with the key as an integer 8.
func (m *Map) GetInt8(key any) int8 {
	return getTyped[int8](m, key)
}

// GetInt16 returns the value associated with the key as an integer 16.
func (m *Map) GetInt16(key any) int16 {
	return getTyped[int16](m, key)
}

// GetInt32 returns the value associated with the key as an integer 32.
func (m *Map) GetInt32(key any) int32 {
	return getTyped[int32](m, key)
}

// GetInt64 returns the value associated with the key as an integer 64.
func (m *Map) GetInt64(key any) int64 {
	return getTyped[int64](m, key)
}

// GetUint returns the value associated with the key as an unsigned integer.
func (m *Map) GetUint(key any) uint {
	return getTyped[uint](m, key)
}

// GetUint8 returns the value associated with the key as an unsigned integer 8.
func (m *Map) GetUint8(key any) uint8 {
	return getTyped[uint8](m, key)
}

// GetUint16 returns the value associated with the key as an unsigned integer 16.
func (m *Map) GetUint16(key any) uint16 {
	return getTyped[uint16](m, key)
}

// GetUint32 returns the value associated with the key as an unsigned integer 32.
func (m *Map) GetUint32(key any) uint32 {
	return getTyped[uint32](m, key)
}

// GetUint64 returns the value associated with the key as an unsigned integer 64.
func (m *Map) GetUint64(key any) uint64 {
	return getTyped[uint64](m, key)
}

// GetFloat32 returns the value associated with the key as a float32.
func (m *Map) GetFloat32(key any) float32 {
	return getTyped[float32](m, key)
}

// GetFloat64 returns the value associated with the key as a float64.
func (m *Map) GetFloat64(key any) float64 {
	return getTyped[float64](m, key)
}

// GetTime returns the value associated with the key as time.
func (m *Map) GetTime(key any) time.Time {
	return getTyped[time.Time](m, key)
}

// GetDuration returns the value associated with the key as a duration.
func (m *Map) GetDuration(key any) time.Duration {
	return getTyped[time.Duration](m, key)
}

// GetError returns the value associated with the key as an error.
func (m *Map) GetError(key any) error {
	return getTyped[error](m, key)
}

// GetIntSlice returns the value associated with the key as a slice of integers.
func (m *Map) GetIntSlice(key any) []int {
	return getTyped[[]int](m, key)
}

// GetInt8Slice returns the value associated with the key as a slice of int8 integers.
func (m *Map) GetInt8Slice(key any) []int8 {
	return getTyped[[]int8](m, key)
}

// GetInt16Slice returns the value associated with the key as a slice of int16 integers.
func (m *Map) GetInt16Slice(key any) []int16 {
	return getTyped[[]int16](m, key)
}

// GetInt32Slice returns the value associated with the key as a slice of int32 integers.
func (m *Map) GetInt32Slice(key any) []int32 {
	return getTyped[[]int32](m, key)
}

// GetInt64Slice returns the value associated with the key as a slice of int64 integers.
func (m *Map) GetInt64Slice(key any) []int64 {
	return getTyped[[]int64](m, key)
}

// GetUintSlice returns the value associated with the key as a slice of unsigned integers.
func (m *Map) GetUintSlice(key any) []uint {
	return getTyped[[]uint](m, key)
}

// GetUint8Slice returns the value associated with the key as a slice of uint8 integers.
func (m *Map) GetUint8Slice(key any) []uint8 {
	return getTyped[[]uint8](m, key)
}

// GetUint16Slice returns the value associated with the key as a slice of uint16 integers.
func (m *Map) GetUint16Slice(key any) []uint16 {
	return getTyped[[]uint16](m, key)
}

// GetUint32Slice returns the value associated with the key as a slice of uint32 integers.
func (m *Map) GetUint32Slice(key any) []uint32 {
	return getTyped[[]uint32](m, key)
}

// GetUint64Slice returns the value associated with the key as a slice of uint64 integers.
func (m *Map) GetUint64Slice(key any) []uint64 {
	return getTyped[[]uint64](m, key)
}

// GetFloat32Slice returns the value associated with the key as a slice of float32 numbers.
func (m *Map) GetFloat32Slice(key any) []float32 {
	return getTyped[[]float32](m, key)
}

// GetFloat64Slice returns the value associated with the key as a slice of float64 numbers.
func (m *Map) GetFloat64Slice(key any) []float64 {
	return getTyped[[]float64](m, key)
}

// GetStringSlice returns the value associated with the key as a slice of strings.
func (m *Map) GetStringSlice(key any) []string {
	return getTyped[[]string](m, key)
}

// GetErrorSlice returns the value associated with the key as a slice of errors.
func (m *Map) GetErrorSlice(key any) []error {
	return getTyped[[]error](m, key)
}

// GetStringMap returns the value associated with the key as a map of interfaces.
func (m *Map) GetStringMap(key any) map[string]any {
	return getTyped[map[string]any](m, key)
}

// GetStringMapString returns the value associated with the key as a map of strings.
func (m *Map) GetStringMapString(key any) map[string]string {
	return getTyped[map[string]string](m, key)
}

// GetStringMapStringSlice returns the value associated with the key as a map to a slice of strings.
func (m *Map) GetStringMapStringSlice(key any) map[string][]string {
	return getTyped[map[string][]string](m, key)
}

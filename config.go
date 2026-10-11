package forgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// Env returns the value of the environment variable key, or defaultValue when it is unset or empty.
func Env(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// LiveConfig is a configuration that can change while the application runs, such as feature flags or
// limits an admin edits. T is the implementing type itself, and both methods should have value
// receivers:
//
//	type Checkout struct {
//		Enabled  bool `json:"enabled"`
//		MaxItems int  `json:"max_items"`
//	}
//
//	func (Checkout) Name() string      { return "checkout" }
//	func (Checkout) Default() Checkout { return Checkout{Enabled: true, MaxItems: 50} }
//
// It is stored as the JSON encoding of T, so its fields need json tags like any JSON document.
type LiveConfig[T any] interface {
	// Name is the key the configuration is stored under. It must be non-empty, unique among live
	// configurations and constant: it is called on the zero value of T.
	Name() string
	// Default returns the configuration that LiveConfigs.Load completes with the stored one. Fields the
	// stored document lacks, because nothing was saved yet or the field was added later, keep its value.
	Default() T
}

// LiveConfigStore is the source of truth of live configurations, such as a database table. It holds one
// JSON document per name, with a version that every save increments. It only moves bytes: LiveConfigs
// encodes and decodes them, so a store needs no knowledge of the types.
type LiveConfigStore interface {
	// Get returns the document stored under name and its version. Version 0 means nothing is stored,
	// which is not an error. The caller keeps data, so the store must not modify it afterwards.
	Get(ctx context.Context, name string) (data []byte, version uint64, err error)
	// Set stores data under name as version+1, only while the stored version is version (0: nothing is
	// stored yet). Otherwise it returns ErrLiveConfigConflict and changes nothing.
	Set(ctx context.Context, name string, data []byte, version uint64) error
}

// LiveConfigCache is a cache of live configurations shared by every instance of the application, such as
// Redis. Entries never expire: LiveConfigs writes one only after a save to the LiveConfigStore succeeds.
type LiveConfigCache interface {
	// Get returns the document cached under name, and false when there is none. A missing entry is not
	// an error. The caller keeps data, so the cache must not modify it afterwards.
	Get(ctx context.Context, name string) (data []byte, found bool, err error)
	// Set caches data under name at version, atomically, unless the entry already holds the same or a
	// newer version. Skipping is not an error. The guard keeps saves that reach the cache out of order
	// from caching an older version over a newer one.
	Set(ctx context.Context, name string, data []byte, version uint64) error
}

var (
	// ErrLiveConfigConflict is returned by LiveConfigStore.Set, and by LiveConfigs.Save, when the stored
	// version is not the one the save started from, because another save came first.
	ErrLiveConfigConflict = errors.New("liveconfig: version conflict")
	// ErrLiveConfigNotCached is returned by LiveConfigs.Save when the document was stored but the cache
	// could not be updated. The cache may serve the previous version until the configuration is saved
	// again.
	ErrLiveConfigNotCached = errors.New("liveconfig: stored, but not cached")

	errEmptyLiveConfigName = errors.New("liveconfig: empty name")
)

// LiveConfigMemoryTTL is implemented by live configurations that each instance may serve from its own
// memory, which saves the round trip to the cache:
//
//	func (Checkout) MemoryTTL() time.Duration { return 30 * time.Second }
//
// LiveConfigs.Load keeps what it read from the cache or the store, including the absence of a document,
// for MemoryTTL. Other instances then serve a save up to MemoryTTL late, so leave it out of configurations
// that must change everywhere at once. Zero or less keeps nothing in memory.
type LiveConfigMemoryTTL interface {
	MemoryTTL() time.Duration
}

// LiveConfigs loads and saves live configurations. It is safe for concurrent use. It keeps in memory only
// the configurations that implement LiveConfigMemoryTTL: every other Load reads the cache, and the store
// when the cache has nothing to serve.
type LiveConfigs struct {
	store LiveConfigStore
	cache LiveConfigCache

	mu     sync.Mutex
	memory map[string]*liveConfigMemory
}

// liveConfigMemory is the memory of one configuration. It holds the document, not a decoded value, so
// every Load decodes a value of its own.
type liveConfigMemory struct {
	data    []byte
	found   bool
	expires time.Time // zero while nothing is remembered
	// saves counts the saves of the configuration through this LiveConfigs, so a read that started before
	// one is not remembered over it.
	saves uint64
}

// NewLiveConfigs returns LiveConfigs that read through cache to store. It is a provider: list it after the
// providers of the store and the cache. A nil cache is the same as NewUncachedLiveConfigs.
func NewLiveConfigs(store LiveConfigStore, cache LiveConfigCache) *LiveConfigs {
	return &LiveConfigs{store: store, cache: cache, memory: make(map[string]*liveConfigMemory)}
}

// NewUncachedLiveConfigs returns LiveConfigs without a cache: every Load reads store, unless the
// configuration is kept in memory. It is a provider: list it after the provider of the store.
func NewUncachedLiveConfigs(store LiveConfigStore) *LiveConfigs {
	return NewLiveConfigs(store, nil)
}

// Load returns the configuration T: its Default, completed with the document stored under its Name.
//
// The document comes from the cache, or from the store when the cache has none. Load never writes the
// cache. When neither holds a document, Load returns Default. Fields of the document replace the
// defaults, including zero values such as false or 0, and fields it lacks keep them. Maps are the
// exception: encoding/json adds the stored keys to the default map, so a key set by Default cannot be
// removed.
//
// When T implements LiveConfigMemoryTTL, Load serves the document from memory while it is younger than
// MemoryTTL, and otherwise keeps what it read in memory. Every call decodes a value of its own.
//
// Cache failures are not critical: Load logs them at moderate severity, including a cached document that
// does not decode, and reads the store instead. Load returns an error only when the store fails or its
// document does not decode into T. The configuration returned with an error is Default.
func (c *LiveConfigs) Load[T LiveConfig[T]](ctx context.Context) (T, error) {
	var zero T
	name := zero.Name()
	if name == "" {
		return zero.Default(), errEmptyLiveConfigName
	}

	ttl := liveConfigMemoryTTL(&zero)
	if ttl <= 0 {
		cfg, _, _, err := c.loadShared[T](ctx, name)
		return cfg, err
	}

	data, found, saves, ok := c.recall(name)
	if ok {
		return decodeLiveConfig[T](name, data, found)
	}
	cfg, data, found, err := c.loadShared[T](ctx, name)
	if err == nil {
		c.remember(name, saves, data, found, ttl)
	}
	return cfg, err
}

// loadShared loads T from the cache, then the store. It also returns the document it decoded, and false
// when nothing is stored.
func (c *LiveConfigs) loadShared[T LiveConfig[T]](ctx context.Context, name string) (T, []byte, bool, error) {
	if c.cache != nil {
		data, found, err := c.cache.Get(ctx, name)
		switch {
		case err != nil:
			logLiveConfigCacheFailure(ctx, name, err, "live config cache read failed, reading the store")
		case found:
			cfg, err := decodeLiveConfig[T](name, data, true)
			if err == nil {
				return cfg, data, true, nil
			}
			logLiveConfigCacheFailure(ctx, name, err, "live config cache document does not decode, reading the store")
		}
	}

	data, version, err := c.store.Get(ctx, name)
	if err != nil {
		var zero T
		return zero.Default(), nil, false, fmt.Errorf("liveconfig: load %q: %w", name, err)
	}
	found := version != 0
	cfg, err := decodeLiveConfig[T](name, data, found)
	return cfg, data, found, err
}

// decodeLiveConfig returns the Default of T completed with data, or Default alone when found is false.
func decodeLiveConfig[T LiveConfig[T]](name string, data []byte, found bool) (T, error) {
	var zero T
	cfg := zero.Default()
	if !found {
		return cfg, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return zero.Default(), fmt.Errorf("liveconfig: decode %q: %w", name, err)
	}
	return cfg, nil
}

// liveConfigMemoryTTL returns how long cfg, a pointer to a live configuration, may be served from memory.
// Through the pointer, a MemoryTTL method with either receiver is found.
func liveConfigMemoryTTL(cfg any) time.Duration {
	if m, ok := cfg.(LiveConfigMemoryTTL); ok {
		return m.MemoryTTL()
	}
	return 0
}

// recall returns the document of name kept in memory, and true while it is younger than its TTL. saves is
// the number of saves of name so far, which remember checks.
func (c *LiveConfigs) recall(name string) (data []byte, found bool, saves uint64, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.memory[name]
	if m == nil {
		return nil, false, 0, false
	}
	if time.Now().Before(m.expires) {
		return m.data, m.found, m.saves, true
	}
	return nil, false, m.saves, false
}

// remember keeps a document that Load read in memory for ttl, unless name was saved after recall returned
// saves: the read may predate that save.
func (c *LiveConfigs) remember(name string, saves uint64, data []byte, found bool, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.memoryOf(name)
	if m.saves != saves {
		return
	}
	m.data, m.found, m.expires = data, found, time.Now().Add(ttl)
}

// saved records a save of name. live reports whether the cache serves data now: if so, data is kept for
// ttl. Otherwise the memory is cleared, so this instance serves what the cache serves, like the others.
func (c *LiveConfigs) saved(name string, data []byte, live bool, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.memoryOf(name)
	m.saves++
	if live {
		m.data, m.found, m.expires = data, true, time.Now().Add(ttl)
		return
	}
	m.data, m.found, m.expires = nil, false, time.Time{}
}

// memoryOf returns the memory of name, creating it. c.mu must be held.
func (c *LiveConfigs) memoryOf(name string) *liveConfigMemory {
	m := c.memory[name]
	if m == nil {
		m = &liveConfigMemory{}
		c.memory[name] = m
	}
	return m
}

// logLiveConfigCacheFailure logs a cache failure that Load recovers from by reading the store. It is
// moderate: Load still works, but every call for name now reads the store.
func logLiveConfigCacheFailure(ctx context.Context, name string, err error, msg string) {
	logger.FromCtx(ctx).Error().
		Err(err).
		Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
		Str(logger.LiveConfigLogKey, name).
		Msg(msg)
}

// Save stores cfg under its Name as version+1, replacing the whole document, then caches it. version is
// the version of the document the change started from, 0 when nothing was stored.
//
// When another save stored a different version first, Save returns ErrLiveConfigConflict and changes
// nothing. When the cache cannot be updated after the store succeeded, Save returns
// ErrLiveConfigNotCached: the change is stored, but the cache may serve the previous version until the
// configuration is saved again. The cache is updated even if ctx is canceled once the store succeeded.
//
// When T implements LiveConfigMemoryTTL, this instance serves the saved document from memory at once,
// unless the cache could not be updated: it then serves what the cache serves, like the other instances.
func (c *LiveConfigs) Save[T LiveConfig[T]](ctx context.Context, cfg T, version uint64) error {
	var zero T
	name := zero.Name()
	if name == "" {
		return errEmptyLiveConfigName
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("liveconfig: encode %q: %w", name, err)
	}
	if err := c.store.Set(ctx, name, data, version); err != nil {
		return fmt.Errorf("liveconfig: save %q: %w", name, err)
	}

	var cacheErr error
	if c.cache != nil {
		cacheErr = c.cache.Set(context.WithoutCancel(ctx), name, data, version+1)
	}
	if ttl := liveConfigMemoryTTL(&zero); ttl > 0 {
		c.saved(name, data, cacheErr == nil, ttl)
	}
	if cacheErr != nil {
		return fmt.Errorf("%w: %q: %w", ErrLiveConfigNotCached, name, cacheErr)
	}
	return nil
}

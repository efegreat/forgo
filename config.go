package forgo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
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
	// Load decodes the document into the value Default returns, so Default must build its maps, slices
	// and pointers on every call: one shared with a package variable would receive the stored values,
	// and concurrent loads would race on it.
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
	// ErrLiveConfigInvalid is returned by LiveConfigType.Decode when a document is not a configuration of the
	// type.
	ErrLiveConfigInvalid = errors.New("liveconfig: invalid document")

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

	// memory maps names to *liveConfigMemory. An entry is created once per name and then only read, so
	// loads from memory take no lock.
	memory sync.Map
}

// liveConfigMemory is the memory of one configuration.
type liveConfigMemory struct {
	// mu orders the writes of remember and saved, which read the state they replace. It is never held
	// around I/O.
	mu    sync.Mutex
	state atomic.Pointer[liveConfigState] // nil until the first write
}

// liveConfigState is what the memory of a configuration holds at one time. It is never modified. It holds
// the document, not a decoded value, so every Load decodes a value of its own.
type liveConfigState struct {
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
	return &LiveConfigs{store: store, cache: cache}
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
// Load once per operation and pass the value on: a save between two Loads of one operation would make it
// act on two versions. Keeping the value beyond the operation stops it from following saves.
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
	m, exists := c.memory.Load(name)
	if !exists {
		return nil, false, 0, false
	}
	st := m.(*liveConfigMemory).state.Load()
	if st == nil {
		return nil, false, 0, false
	}
	if time.Now().Before(st.expires) {
		return st.data, st.found, st.saves, true
	}
	return nil, false, st.saves, false
}

// remember keeps a document that Load read in memory for ttl, unless name was saved after recall returned
// saves: the read may predate that save.
func (c *LiveConfigs) remember(name string, saves uint64, data []byte, found bool, ttl time.Duration) {
	m := c.memoryOf(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.Load().savesSoFar() != saves {
		return
	}
	m.state.Store(&liveConfigState{data: data, found: found, expires: time.Now().Add(ttl), saves: saves})
}

// saved records a save of name. live reports whether the cache serves data now: if so, data is kept for
// ttl. Otherwise the memory is cleared, so this instance serves what the cache serves, like the others.
func (c *LiveConfigs) saved(name string, data []byte, live bool, ttl time.Duration) {
	m := c.memoryOf(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	st := &liveConfigState{saves: m.state.Load().savesSoFar() + 1}
	if live {
		st.data, st.found, st.expires = data, true, time.Now().Add(ttl)
	}
	m.state.Store(st)
}

// memoryOf returns the memory of name, creating it.
func (c *LiveConfigs) memoryOf(name string) *liveConfigMemory {
	m, exists := c.memory.Load(name)
	if !exists {
		m, _ = c.memory.LoadOrStore(name, &liveConfigMemory{})
	}
	return m.(*liveConfigMemory)
}

// savesSoFar returns the number of saves st records, 0 for a nil st.
func (st *liveConfigState) savesSoFar() uint64 {
	if st == nil {
		return 0
	}
	return st.saves
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

// LoadStored returns the configuration T as stored, with its version, read from the store and never from the
// cache or memory: it is the state that saves are checked against, such as the one an admin edits. Version 0
// means nothing is stored and the configuration is Default. Unlike Load, it returns the store's failures and
// documents that do not decode as errors, never a configuration from elsewhere.
func (c *LiveConfigs) LoadStored[T LiveConfig[T]](ctx context.Context) (T, uint64, error) {
	cfg, _, version, err := c.loadStored[T](ctx)
	return cfg, version, err
}

// loadStored is LoadStored, which also returns the stored document.
func (c *LiveConfigs) loadStored[T LiveConfig[T]](ctx context.Context) (T, []byte, uint64, error) {
	var zero T
	name := zero.Name()
	if name == "" {
		return zero.Default(), nil, 0, errEmptyLiveConfigName
	}
	data, version, err := c.store.Get(ctx, name)
	if err != nil {
		return zero.Default(), nil, 0, fmt.Errorf("liveconfig: load %q: %w", name, err)
	}
	cfg, err := decodeLiveConfig[T](name, data, version != 0)
	if err != nil {
		return cfg, nil, 0, err
	}
	return cfg, data, version, nil
}

// SaveChanged saves cfg like Save when it differs from the stored configuration, and returns the version stored
// afterwards and whether it saved. Configurations are compared by their JSON encodings, so one that is not stored
// equals its Default.
//
// When cfg equals the stored configuration, nothing is stored and version is not checked, since there is no
// change to protect. SaveChanged then only makes sure the cache holds the stored version, which completes an
// earlier save that returned ErrLiveConfigNotCached, and returns ErrLiveConfigNotCached when it cannot. When a
// save returns ErrLiveConfigNotCached, the returned version is the one stored.
func (c *LiveConfigs) SaveChanged[T LiveConfig[T]](ctx context.Context, cfg T, version uint64) (uint64, bool, error) {
	stored, data, storedVersion, err := c.loadStored[T](ctx)
	if err != nil {
		return 0, false, err
	}
	var zero T
	name := zero.Name()
	next, err := json.Marshal(cfg)
	if err != nil {
		return 0, false, fmt.Errorf("liveconfig: encode %q: %w", name, err)
	}
	current, err := json.Marshal(stored)
	if err != nil {
		return 0, false, fmt.Errorf("liveconfig: encode %q: %w", name, err)
	}

	if !bytes.Equal(next, current) {
		err := c.Save(ctx, cfg, version)
		switch {
		case err == nil:
			return version + 1, true, nil
		case errors.Is(err, ErrLiveConfigNotCached):
			return version + 1, true, err
		default:
			return 0, false, err
		}
	}

	if storedVersion == 0 || c.cache == nil {
		return storedVersion, false, nil
	}
	// The cache keeps the newest version, so this changes nothing unless an earlier save missed it.
	if err := c.cache.Set(context.WithoutCancel(ctx), name, data, storedVersion); err != nil {
		return storedVersion, false, fmt.Errorf("%w: %q: %w", ErrLiveConfigNotCached, name, err)
	}
	return storedVersion, false, nil
}

// LiveConfigCatalog lists the live configurations that admin endpoints read and change, such as
// http.LiveConfigsHandler and http.SetLiveConfigHandler. Declare it once, as a package variable:
//
//	var Configs = forgo.LiveConfigCatalog{
//		forgo.LiveConfigOf[Checkout](),
//		forgo.LiveConfigOf[Pricing](),
//	}
//
// LiveConfigs itself needs no catalog: Load and Save work with every configuration, listed or not.
type LiveConfigCatalog []LiveConfigType

// Validate returns an error when the catalog lists a name twice, or a type with an empty name, such as a
// LiveConfigType not created by LiveConfigOf.
func (c LiveConfigCatalog) Validate() error {
	seen := make(map[string]bool, len(c))
	for _, t := range c {
		if t.name == "" {
			return errors.New("liveconfig: catalog: a type has an empty name or was not created by LiveConfigOf")
		}
		if seen[t.name] {
			return fmt.Errorf("liveconfig: catalog: %q is listed twice", t.name)
		}
		seen[t.name] = true
	}
	return nil
}

// LiveConfigType is a live configuration type of a LiveConfigCatalog, created by LiveConfigOf. Its methods work
// on configurations behind any, for transports that only know the type by its name.
type LiveConfigType struct {
	name        string
	memoryTTL   time.Duration
	loadStored  func(context.Context, *LiveConfigs) (any, uint64, error)
	decode      func([]byte) (any, error)
	saveChanged func(context.Context, *LiveConfigs, any, uint64) (uint64, bool, error)
}

// LiveConfigOf returns the LiveConfigType of T, for a LiveConfigCatalog.
func LiveConfigOf[T LiveConfig[T]]() LiveConfigType {
	var zero T
	name := zero.Name()
	return LiveConfigType{
		name:      name,
		memoryTTL: liveConfigMemoryTTL(&zero),
		loadStored: func(ctx context.Context, c *LiveConfigs) (any, uint64, error) {
			cfg, version, err := c.LoadStored[T](ctx)
			return cfg, version, err
		},
		decode: func(data []byte) (any, error) {
			cfg, err := decodeLiveConfigStrict[T](name, data)
			if err != nil {
				return nil, err
			}
			return &cfg, nil
		},
		saveChanged: func(ctx context.Context, c *LiveConfigs, cfg any, version uint64) (uint64, bool, error) {
			p, ok := cfg.(*T)
			if !ok {
				return 0, false, fmt.Errorf("liveconfig: save %q: got %T, want the result of Decode", name, cfg)
			}
			return c.SaveChanged(ctx, *p, version)
		},
	}
}

// Name is the Name of the type.
func (t LiveConfigType) Name() string {
	return t.name
}

// MemoryTTL is the MemoryTTL of the type, or 0 when it keeps nothing in memory.
func (t LiveConfigType) MemoryTTL() time.Duration {
	return t.memoryTTL
}

// LoadStored is LiveConfigs.LoadStored of the type. The configuration is a value of the type.
func (t LiveConfigType) LoadStored(ctx context.Context, c *LiveConfigs) (any, uint64, error) {
	return t.loadStored(ctx, c)
}

// Decode returns a pointer to the Default of the type completed with data, a JSON document. Unlike Load, it
// rejects fields the type does not have and anything after the document, because a misspelled field would
// otherwise be saved without effect. Its errors wrap ErrLiveConfigInvalid.
func (t LiveConfigType) Decode(data []byte) (any, error) {
	return t.decode(data)
}

// SaveChanged is LiveConfigs.SaveChanged of the type, for cfg, a pointer returned by Decode.
func (t LiveConfigType) SaveChanged(ctx context.Context, c *LiveConfigs, cfg any, version uint64) (uint64, bool, error) {
	return t.saveChanged(ctx, c, cfg, version)
}

// decodeLiveConfigStrict returns the Default of T completed with data, which must hold one JSON object of T's
// fields only.
func decodeLiveConfigStrict[T LiveConfig[T]](name string, data []byte) (T, error) {
	var zero T
	cfg := zero.Default()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return zero.Default(), fmt.Errorf("%w %q: %w", ErrLiveConfigInvalid, name, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return zero.Default(), fmt.Errorf("%w %q: data after the document", ErrLiveConfigInvalid, name)
	}
	return cfg, nil
}

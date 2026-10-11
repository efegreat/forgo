package forgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

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

// LiveConfigs loads and saves live configurations. It is safe for concurrent use, and holds no state of
// its own: every Load reads the cache, and the store when the cache has nothing to serve.
type LiveConfigs struct {
	store LiveConfigStore
	cache LiveConfigCache
}

// NewLiveConfigs returns LiveConfigs that read through cache to store. It is a provider: list it after the
// providers of the store and the cache. A nil cache is the same as NewUncachedLiveConfigs.
func NewLiveConfigs(store LiveConfigStore, cache LiveConfigCache) *LiveConfigs {
	return &LiveConfigs{store: store, cache: cache}
}

// NewUncachedLiveConfigs returns LiveConfigs without a cache: every Load reads store. It is a provider:
// list it after the provider of the store.
func NewUncachedLiveConfigs(store LiveConfigStore) *LiveConfigs {
	return &LiveConfigs{store: store}
}

// Load returns the configuration T: its Default, completed with the document stored under its Name.
//
// The document comes from the cache, or from the store when the cache has none. Load never writes the
// cache. When neither holds a document, Load returns Default. Fields of the document replace the
// defaults, including zero values such as false or 0, and fields it lacks keep them. Maps are the
// exception: encoding/json adds the stored keys to the default map, so a key set by Default cannot be
// removed.
//
// Cache failures are not critical: Load logs them at moderate severity, including a cached document that does not decode,
// and reads the store instead. Load returns an error only when the store fails or its document does not
// decode into T. The configuration returned with an error is Default.
func (c *LiveConfigs) Load[T LiveConfig[T]](ctx context.Context) (T, error) {
	var zero T
	name := zero.Name()
	if name == "" {
		return zero.Default(), errEmptyLiveConfigName
	}

	if c.cache != nil {
		data, found, err := c.cache.Get(ctx, name)
		switch {
		case err != nil:
			logLiveConfigCacheFailure(ctx, name, err, "live config cache read failed, reading the store")
		case found:
			cfg := zero.Default()
			err := json.Unmarshal(data, &cfg)
			if err == nil {
				return cfg, nil
			}
			logLiveConfigCacheFailure(ctx, name, err, "live config cache document does not decode, reading the store")
		}
	}

	data, version, err := c.store.Get(ctx, name)
	if err != nil {
		return zero.Default(), fmt.Errorf("liveconfig: load %q: %w", name, err)
	}
	cfg := zero.Default()
	if version == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return zero.Default(), fmt.Errorf("liveconfig: decode %q: %w", name, err)
	}
	return cfg, nil
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
	if c.cache == nil {
		return nil
	}
	if err := c.cache.Set(context.WithoutCancel(ctx), name, data, version+1); err != nil {
		return fmt.Errorf("%w: %q: %w", ErrLiveConfigNotCached, name, err)
	}
	return nil
}

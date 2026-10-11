package forgo

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"

	"github.com/efegreat/forgo/logger"
)

type checkout struct {
	Enabled  bool   `json:"enabled"`
	MaxItems int    `json:"max_items"`
	Budget   uint64 `json:"budget"`
}

func (checkout) Name() string      { return "checkout" }
func (checkout) Default() checkout { return checkout{Enabled: true, MaxItems: 50} }

type unnamed struct{}

func (unnamed) Name() string     { return "" }
func (unnamed) Default() unnamed { return unnamed{} }

type memDoc struct {
	data    string
	version uint64
}

// memStore is a LiveConfigStore in memory. err fails every call, and onSet runs before a Set stores.
type memStore struct {
	mu    sync.Mutex
	docs  map[string]memDoc
	gets  int
	err   error
	onSet func()
}

func newMemStore() *memStore {
	return &memStore{docs: map[string]memDoc{}}
}

func (s *memStore) Get(_ context.Context, name string) ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.err != nil {
		return nil, 0, s.err
	}
	doc, ok := s.docs[name]
	if !ok {
		return nil, 0, nil
	}
	return []byte(doc.data), doc.version, nil
}

func (s *memStore) Set(_ context.Context, name string, data []byte, version uint64) error {
	if s.onSet != nil {
		s.onSet()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.docs[name].version != version {
		return ErrLiveConfigConflict
	}
	s.docs[name] = memDoc{data: string(data), version: version + 1}
	return nil
}

// memCache is a LiveConfigCache in memory that keeps the newest version. getErr and setErr fail its
// calls, setCtxErr is the context error the last Set saw, and onGet runs after a Get read its entry.
type memCache struct {
	mu        sync.Mutex
	docs      map[string]memDoc
	gets      int
	sets      int
	getErr    error
	setErr    error
	setCtxErr error
	onGet     func()
}

func newMemCache() *memCache {
	return &memCache{docs: map[string]memDoc{}}
}

func (c *memCache) Get(_ context.Context, name string) ([]byte, bool, error) {
	c.mu.Lock()
	c.gets++
	doc, ok := c.docs[name]
	err, onGet := c.getErr, c.onGet
	c.mu.Unlock()
	if onGet != nil {
		onGet()
	}
	if err != nil {
		return nil, false, err
	}
	return []byte(doc.data), ok, nil
}

// reads returns how many times the cache and the store were read.
func reads(c *memCache, s *memStore) (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return c.gets, s.gets
}

func (c *memCache) Set(ctx context.Context, name string, data []byte, version uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	c.setCtxErr = ctx.Err()
	if c.setErr != nil {
		return c.setErr
	}
	if c.docs[name].version < version {
		c.docs[name] = memDoc{data: string(data), version: version}
	}
	return nil
}

func TestEnv(t *testing.T) {
	t.Setenv("FORGO_TEST_ENV", "set")
	t.Setenv("FORGO_TEST_EMPTY", "")
	tests := []struct {
		key  string
		want string
	}{
		{"FORGO_TEST_ENV", "set"},
		{"FORGO_TEST_EMPTY", "default"},
		{"FORGO_TEST_UNSET", "default"},
	}
	for _, tt := range tests {
		if got := Env(tt.key, "default"); got != tt.want {
			t.Errorf("Env(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestLiveConfigsLoad(t *testing.T) {
	cacheErr := errors.New("cache down")
	tests := []struct {
		name       string
		cached     string // empty: no entry
		stored     string // empty: nothing stored
		cacheErr   error
		want       checkout
		storeReads int
		wantLog    string // empty: nothing logged
	}{
		{
			name:       "nothing cached or stored gives the default",
			want:       checkout{Enabled: true, MaxItems: 50},
			storeReads: 1,
		},
		{
			name:   "cached document is served without reading the store",
			cached: `{"max_items":1}`,
			stored: `{"max_items":2}`,
			want:   checkout{Enabled: true, MaxItems: 1},
		},
		{
			name:   "stored fields replace defaults, zero values included",
			cached: `{"enabled":false,"max_items":0}`,
			want:   checkout{},
		},
		{
			name:       "cache miss reads the store; fields it lacks keep their default",
			stored:     `{"enabled":false}`,
			want:       checkout{MaxItems: 50},
			storeReads: 1,
		},
		{
			name:       "uint64 keeps its precision",
			stored:     `{"budget":18446744073709551615}`,
			want:       checkout{Enabled: true, MaxItems: 50, Budget: math.MaxUint64},
			storeReads: 1,
		},
		{
			name:       "cache failure is logged and the store is read",
			cached:     `{"max_items":1}`,
			stored:     `{"max_items":2}`,
			cacheErr:   cacheErr,
			want:       checkout{Enabled: true, MaxItems: 2},
			storeReads: 1,
			wantLog:    "live config cache read failed",
		},
		{
			name:       "cache failure with nothing stored gives the default",
			cacheErr:   cacheErr,
			want:       checkout{Enabled: true, MaxItems: 50},
			storeReads: 1,
			wantLog:    "live config cache read failed",
		},
		{
			name:       "cached document that does not decode is logged and the store is read",
			cached:     `{"max_items":"many"}`,
			stored:     `{"enabled":false}`,
			want:       checkout{MaxItems: 50},
			storeReads: 1,
			wantLog:    "live config cache document does not decode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, cache := newMemStore(), newMemCache()
			if tt.stored != "" {
				store.docs["checkout"] = memDoc{data: tt.stored, version: 3}
			}
			if tt.cached != "" {
				cache.docs["checkout"] = memDoc{data: tt.cached, version: 3}
			}
			cache.getErr = tt.cacheErr
			var logs bytes.Buffer
			ctx := logger.NewCtx(context.Background(), zerolog.New(&logs))

			got, err := NewLiveConfigs(store, cache).Load[checkout](ctx)
			if err != nil || got != tt.want {
				t.Fatalf("Load = %+v, %v; want %+v, nil", got, err, tt.want)
			}
			if store.gets != tt.storeReads {
				t.Errorf("store read %d times, want %d", store.gets, tt.storeReads)
			}
			if cache.sets != 0 {
				t.Errorf("Load wrote the cache %d times", cache.sets)
			}
			switch {
			case tt.wantLog == "" && logs.Len() > 0:
				t.Errorf("unexpected log: %s", logs.String())
			case tt.wantLog != "":
				for _, want := range []string{tt.wantLog, `"level":"error"`, `"severity":1`, `"live_config":"checkout"`} {
					if !strings.Contains(logs.String(), want) {
						t.Errorf("log = %s, want it to contain %s", logs.String(), want)
					}
				}
			}
		})
	}
}

func TestLiveConfigsLoadErrors(t *testing.T) {
	storeErr := errors.New("store down")
	tests := []struct {
		name    string
		prepare func(*memStore)
		wantErr error
	}{
		{
			name:    "store fails",
			prepare: func(s *memStore) { s.err = storeErr },
			wantErr: storeErr,
		},
		{
			name: "stored document does not decode",
			prepare: func(s *memStore) {
				s.docs["checkout"] = memDoc{data: `{"enabled":false,"max_items":"many"}`, version: 1}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMemStore()
			tt.prepare(store)
			got, err := NewLiveConfigs(store, newMemCache()).Load[checkout](context.Background())
			if err == nil || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != (checkout{}).Default() {
				t.Fatalf("got %+v with the error, want the default", got)
			}
		})
	}
}

func TestLiveConfigsWithoutCache(t *testing.T) {
	for name, c := range map[string]func(LiveConfigStore) *LiveConfigs{
		"uncached": NewUncachedLiveConfigs,
		"nil cache": func(s LiveConfigStore) *LiveConfigs {
			return NewLiveConfigs(s, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			configs := c(store)
			if err := configs.Save(context.Background(), checkout{MaxItems: 1}, 0); err != nil {
				t.Fatalf("save: %v", err)
			}
			got, err := configs.Load[checkout](context.Background())
			if err != nil || got.MaxItems != 1 || store.gets != 1 {
				t.Fatalf("Load = %+v, %v after %d store reads", got, err, store.gets)
			}
		})
	}
}

func TestLiveConfigsSave(t *testing.T) {
	store, cache := newMemStore(), newMemCache()
	c := NewLiveConfigs(store, cache)
	ctx := context.Background()

	first := checkout{MaxItems: 7, Budget: math.MaxUint64}
	if err := c.Save(ctx, first, 0); err != nil {
		t.Fatalf("first save: %v", err)
	}
	want := memDoc{data: `{"enabled":false,"max_items":7,"budget":18446744073709551615}`, version: 1}
	if store.docs["checkout"] != want || cache.docs["checkout"] != want {
		t.Fatalf("stored %+v, cached %+v; want %+v in both", store.docs["checkout"], cache.docs["checkout"], want)
	}
	if got, err := c.Load[checkout](ctx); err != nil || got != first || store.gets != 0 {
		t.Fatalf("Load = %+v, %v after %d store reads; want the saved config from the cache", got, err, store.gets)
	}

	// A second save from version 0 lost the race to the first one.
	if err := c.Save(ctx, checkout{MaxItems: 8}, 0); !errors.Is(err, ErrLiveConfigConflict) {
		t.Fatalf("conflicting save: err = %v", err)
	}
	if store.docs["checkout"] != want || cache.docs["checkout"] != want || cache.sets != 1 {
		t.Fatalf("conflicting save changed stored %+v or cached %+v", store.docs["checkout"], cache.docs["checkout"])
	}

	if err := c.Save(ctx, checkout{MaxItems: 9}, 1); err != nil {
		t.Fatalf("save from version 1: %v", err)
	}
	if store.docs["checkout"].version != 2 || cache.docs["checkout"].version != 2 {
		t.Fatalf("stored %+v, cached %+v; want version 2", store.docs["checkout"], cache.docs["checkout"])
	}
}

func TestLiveConfigsSaveFailures(t *testing.T) {
	failure := errors.New("down")
	tests := []struct {
		name       string
		storeErr   error
		cacheErr   error
		wantErr    []error
		wantStored bool
	}{
		{
			name:     "store fails: nothing is cached",
			storeErr: failure,
			wantErr:  []error{failure},
		},
		{
			name:       "cache fails after the store succeeded",
			cacheErr:   failure,
			wantErr:    []error{ErrLiveConfigNotCached, failure},
			wantStored: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, cache := newMemStore(), newMemCache()
			store.err, cache.setErr = tt.storeErr, tt.cacheErr
			err := NewLiveConfigs(store, cache).Save(context.Background(), checkout{}, 0)
			for _, want := range tt.wantErr {
				if !errors.Is(err, want) {
					t.Errorf("err = %v, want %v", err, want)
				}
			}
			if _, stored := store.docs["checkout"]; stored != tt.wantStored {
				t.Errorf("stored = %v, want %v", stored, tt.wantStored)
			}
			if _, cached := cache.docs["checkout"]; cached {
				t.Errorf("cached a failed save")
			}
		})
	}
}

func TestLiveConfigsSaveCachesOnceStored(t *testing.T) {
	store, cache := newMemStore(), newMemCache()
	ctx, cancel := context.WithCancel(context.Background())
	store.onSet = cancel // the caller goes away while the store saves
	if err := NewLiveConfigs(store, cache).Save(ctx, checkout{}, 0); err != nil {
		t.Fatalf("save: %v", err)
	}
	if cache.setCtxErr != nil || cache.docs["checkout"].version != 1 {
		t.Fatalf("cache Set saw %v and cached %+v; want the stored version", cache.setCtxErr, cache.docs["checkout"])
	}
}

func TestLiveConfigsEmptyName(t *testing.T) {
	c := NewLiveConfigs(newMemStore(), newMemCache())
	if _, err := c.Load[unnamed](context.Background()); !errors.Is(err, errEmptyLiveConfigName) {
		t.Fatalf("load: err = %v", err)
	}
	if err := c.Save(context.Background(), unnamed{}, 0); !errors.Is(err, errEmptyLiveConfigName) {
		t.Fatalf("save: err = %v", err)
	}
}

func TestLiveConfigsProviders(t *testing.T) {
	providers := [][]any{
		{newMemStore, newMemCache, NewLiveConfigs},
		{newMemStore, NewUncachedLiveConfigs},
	}
	for _, p := range providers {
		r, err := NewBootstrap(nil, p).Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		c, err := r.Resolve[*LiveConfigs]()
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if _, err := c.Load[checkout](context.Background()); err != nil {
			t.Fatalf("load: %v", err)
		}
	}
}

// memCheckout is kept in memory for a minute.
type memCheckout struct {
	MaxItems int      `json:"max_items"`
	Tags     []string `json:"tags"`
}

func (memCheckout) Name() string             { return "mem_checkout" }
func (memCheckout) Default() memCheckout     { return memCheckout{MaxItems: 50} }
func (memCheckout) MemoryTTL() time.Duration { return time.Minute }

// ptrCheckout declares its MemoryTTL on the pointer receiver.
type ptrCheckout struct {
	MaxItems int `json:"max_items"`
}

func (ptrCheckout) Name() string              { return "ptr_checkout" }
func (ptrCheckout) Default() ptrCheckout      { return ptrCheckout{MaxItems: 50} }
func (*ptrCheckout) MemoryTTL() time.Duration { return time.Minute }

// offCheckout implements LiveConfigMemoryTTL but keeps nothing in memory.
type offCheckout struct {
	MaxItems int `json:"max_items"`
}

func (offCheckout) Name() string             { return "off_checkout" }
func (offCheckout) Default() offCheckout     { return offCheckout{MaxItems: 50} }
func (offCheckout) MemoryTTL() time.Duration { return 0 }

var (
	_ LiveConfigMemoryTTL = memCheckout{}
	_ LiveConfigMemoryTTL = (*ptrCheckout)(nil)
)

func mustLoad[T LiveConfig[T]](t *testing.T, c *LiveConfigs) T {
	t.Helper()
	cfg, err := c.Load[T](context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return cfg
}

func TestLiveConfigsMemory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, cache := newMemStore(), newMemCache()
		c := NewLiveConfigs(store, cache)
		cache.docs["mem_checkout"] = memDoc{data: `{"max_items":1}`, version: 1}

		if got := mustLoad[memCheckout](t, c); got.MaxItems != 1 {
			t.Fatalf("first load: got %+v", got)
		}
		cache.docs["mem_checkout"] = memDoc{data: `{"max_items":2}`, version: 2} // saved by another instance
		time.Sleep(time.Minute - time.Nanosecond)
		if got := mustLoad[memCheckout](t, c); got.MaxItems != 1 {
			t.Fatalf("within the TTL: got %+v", got)
		}
		if cacheReads, storeReads := reads(cache, store); cacheReads != 1 || storeReads != 0 {
			t.Fatalf("within the TTL: %d cache and %d store reads, want 1 and 0", cacheReads, storeReads)
		}

		time.Sleep(time.Nanosecond)
		if got := mustLoad[memCheckout](t, c); got.MaxItems != 2 {
			t.Fatalf("after the TTL: got %+v", got)
		}
		if cacheReads, _ := reads(cache, store); cacheReads != 2 {
			t.Fatalf("after the TTL: %d cache reads, want 2", cacheReads)
		}
	})
}

func TestLiveConfigsMemoryKeepsAbsence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, cache := newMemStore(), newMemCache()
		c := NewLiveConfigs(store, cache)
		for range 3 {
			if got := mustLoad[memCheckout](t, c); got.MaxItems != 50 {
				t.Fatalf("got %+v, want the default", got)
			}
		}
		if cacheReads, storeReads := reads(cache, store); cacheReads != 1 || storeReads != 1 {
			t.Fatalf("%d cache and %d store reads, want 1 each", cacheReads, storeReads)
		}
	})
}

func TestLiveConfigsMemoryReturnsOwnValues(t *testing.T) {
	store, cache := newMemStore(), newMemCache()
	cache.docs["mem_checkout"] = memDoc{data: `{"tags":["a","b"]}`, version: 1}
	c := NewLiveConfigs(store, cache)

	first := mustLoad[memCheckout](t, c)
	first.Tags[0] = "changed"
	if second := mustLoad[memCheckout](t, c); second.Tags[0] != "a" {
		t.Fatalf("second load shares the first one's slice: %+v", second)
	}
}

func TestLiveConfigsMemoryKeepsOnlyShared(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*memStore, *memCache)
		wantErr bool
	}{
		{
			name:    "store fails",
			prepare: func(s *memStore, _ *memCache) { s.err = errors.New("store down") },
			wantErr: true,
		},
		{
			name: "stored document does not decode",
			prepare: func(s *memStore, _ *memCache) {
				s.docs["mem_checkout"] = memDoc{data: `{"max_items":"many"}`, version: 1}
			},
			wantErr: true,
		},
		{
			name: "cache fails: the stored document is kept",
			prepare: func(s *memStore, c *memCache) {
				c.getErr = errors.New("cache down")
				s.docs["mem_checkout"] = memDoc{data: `{"max_items":1}`, version: 1}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, cache := newMemStore(), newMemCache()
			tt.prepare(store, cache)
			c := NewLiveConfigs(store, cache)
			for range 2 {
				if _, err := c.Load[memCheckout](context.Background()); (err != nil) != tt.wantErr {
					t.Fatalf("load: err = %v, want error %v", err, tt.wantErr)
				}
			}
			wantStoreReads := 1
			if tt.wantErr {
				wantStoreReads = 2 // a failed load keeps nothing
			}
			if _, storeReads := reads(cache, store); storeReads != wantStoreReads {
				t.Fatalf("%d store reads, want %d", storeReads, wantStoreReads)
			}
		})
	}
}

func TestLiveConfigsMemoryTTLDetection(t *testing.T) {
	store, cache := newMemStore(), newMemCache()
	c := NewLiveConfigs(store, cache)
	for range 2 {
		mustLoad[ptrCheckout](t, c)
		mustLoad[offCheckout](t, c)
		mustLoad[checkout](t, c)
	}
	// ptrCheckout is read once; offCheckout and checkout, which keep nothing in memory, twice each.
	if cacheReads, _ := reads(cache, store); cacheReads != 5 {
		t.Fatalf("%d cache reads, want 5", cacheReads)
	}
}

func TestLiveConfigsMemoryAfterSave(t *testing.T) {
	tests := []struct {
		name      string
		cacheErr  error
		wantSaved bool // this instance serves the saved document without reading the cache
	}{
		{name: "cached save is served at once", wantSaved: true},
		{name: "save the cache missed is served like the cache does", cacheErr: errors.New("cache down")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, cache := newMemStore(), newMemCache()
			cache.docs["mem_checkout"] = memDoc{data: `{"max_items":1}`, version: 1}
			store.docs["mem_checkout"] = memDoc{data: `{"max_items":1}`, version: 1}
			c := NewLiveConfigs(store, cache)
			mustLoad[memCheckout](t, c)

			cache.setErr = tt.cacheErr
			if err := c.Save(context.Background(), memCheckout{MaxItems: 2}, 1); (err != nil) != (tt.cacheErr != nil) {
				t.Fatalf("save: %v", err)
			}
			got := mustLoad[memCheckout](t, c)
			cacheReads, _ := reads(cache, store)
			if tt.wantSaved && (got.MaxItems != 2 || cacheReads != 1) {
				t.Fatalf("got %+v after %d cache reads; want the saved config from memory", got, cacheReads)
			}
			if !tt.wantSaved && (got.MaxItems != 1 || cacheReads != 2) {
				t.Fatalf("got %+v after %d cache reads; want the cached config", got, cacheReads)
			}
		})
	}
}

func TestLiveConfigsMemoryReadBeforeSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, cache := newMemStore(), newMemCache()
		cache.docs["mem_checkout"] = memDoc{data: `{"max_items":1}`, version: 1}
		store.docs["mem_checkout"] = memDoc{data: `{"max_items":1}`, version: 1}
		c := NewLiveConfigs(store, cache)

		// A load reads version 1 from the cache, and is held there while version 2 is saved.
		release := make(chan struct{})
		cache.onGet = func() { <-release }
		var wg sync.WaitGroup
		wg.Go(func() { mustLoad[memCheckout](t, c) })
		synctest.Wait()
		cache.mu.Lock()
		cache.onGet = nil
		cache.mu.Unlock()
		if err := c.Save(context.Background(), memCheckout{MaxItems: 2}, 1); err != nil {
			t.Fatalf("save: %v", err)
		}
		close(release)
		wg.Wait()

		if got := mustLoad[memCheckout](t, c); got.MaxItems != 2 {
			t.Fatalf("the read that started before the save replaced it in memory: got %+v", got)
		}
	})
}

func TestLiveConfigsLoadStored(t *testing.T) {
	store, cache := newMemStore(), newMemCache()
	c := NewLiveConfigs(store, cache)
	ctx := context.Background()

	if cfg, version, err := c.LoadStored[checkout](ctx); err != nil || version != 0 || cfg != (checkout{}).Default() {
		t.Fatalf("nothing stored: LoadStored = %+v, %d, %v", cfg, version, err)
	}

	// The store is read even when the cache holds another document.
	store.docs["checkout"] = memDoc{data: `{"max_items":2}`, version: 4}
	cache.docs["checkout"] = memDoc{data: `{"max_items":1}`, version: 3}
	if cfg, version, err := c.LoadStored[checkout](ctx); err != nil || version != 4 || cfg.MaxItems != 2 || !cfg.Enabled {
		t.Fatalf("LoadStored = %+v, %d, %v; want the stored version 4", cfg, version, err)
	}
	if cache.gets != 0 {
		t.Fatalf("LoadStored read the cache")
	}

	store.err = errors.New("store down")
	if _, _, err := c.LoadStored[checkout](ctx); !errors.Is(err, store.err) {
		t.Fatalf("failing store: err = %v", err)
	}
}

func TestLiveConfigsSaveChanged(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("down")
	tests := []struct {
		name        string
		stored      *memDoc // nil: nothing stored
		cached      *memDoc // nil: nothing cached
		cacheErr    error
		cfg         checkout
		version     uint64
		wantVersion uint64
		wantChanged bool
		wantErr     error
		wantStored  *memDoc
		wantCached  *memDoc
	}{
		{
			name:        "default while nothing is stored stores nothing",
			cfg:         checkout{Enabled: true, MaxItems: 50},
			wantVersion: 0,
		},
		{
			name:        "change is saved",
			cfg:         checkout{MaxItems: 1},
			wantVersion: 1,
			wantChanged: true,
			wantStored:  &memDoc{data: `{"enabled":false,"max_items":1,"budget":0}`, version: 1},
			wantCached:  &memDoc{data: `{"enabled":false,"max_items":1,"budget":0}`, version: 1},
		},
		{
			name:        "unchanged ignores a stale version and completes the cache",
			stored:      &memDoc{data: `{"max_items":1}`, version: 2},
			cfg:         checkout{Enabled: true, MaxItems: 1},
			version:     1,
			wantVersion: 2,
			wantStored:  &memDoc{data: `{"max_items":1}`, version: 2},
			wantCached:  &memDoc{data: `{"max_items":1}`, version: 2},
		},
		{
			name:        "unchanged leaves a newer cached version",
			stored:      &memDoc{data: `{"max_items":1}`, version: 2},
			cached:      &memDoc{data: `{"max_items":7}`, version: 3},
			cfg:         checkout{Enabled: true, MaxItems: 1},
			version:     2,
			wantVersion: 2,
			wantStored:  &memDoc{data: `{"max_items":1}`, version: 2},
			wantCached:  &memDoc{data: `{"max_items":7}`, version: 3},
		},
		{
			name:       "change from a stale version conflicts",
			stored:     &memDoc{data: `{"max_items":1}`, version: 2},
			cfg:        checkout{MaxItems: 9},
			version:    1,
			wantErr:    ErrLiveConfigConflict,
			wantStored: &memDoc{data: `{"max_items":1}`, version: 2},
		},
		{
			name:        "change the cache missed",
			cacheErr:    failure,
			cfg:         checkout{MaxItems: 1},
			wantVersion: 1,
			wantChanged: true,
			wantErr:     ErrLiveConfigNotCached,
			wantStored:  &memDoc{data: `{"enabled":false,"max_items":1,"budget":0}`, version: 1},
		},
		{
			name:        "unchanged while the cache fails",
			stored:      &memDoc{data: `{"max_items":1}`, version: 2},
			cacheErr:    failure,
			cfg:         checkout{Enabled: true, MaxItems: 1},
			version:     2,
			wantVersion: 2,
			wantErr:     ErrLiveConfigNotCached,
			wantStored:  &memDoc{data: `{"max_items":1}`, version: 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, cache := newMemStore(), newMemCache()
			if tt.stored != nil {
				store.docs["checkout"] = *tt.stored
			}
			if tt.cached != nil {
				cache.docs["checkout"] = *tt.cached
			}
			cache.setErr = tt.cacheErr

			version, changed, err := NewLiveConfigs(store, cache).SaveChanged(ctx, tt.cfg, tt.version)
			if !errors.Is(err, tt.wantErr) || version != tt.wantVersion || changed != tt.wantChanged {
				t.Fatalf("SaveChanged = %d, %v, %v; want %d, %v, %v", version, changed, err, tt.wantVersion, tt.wantChanged, tt.wantErr)
			}
			if doc, ok := store.docs["checkout"]; ok != (tt.wantStored != nil) || (ok && doc != *tt.wantStored) {
				t.Errorf("stored %+v (%v), want %+v", doc, ok, tt.wantStored)
			}
			if doc, ok := cache.docs["checkout"]; ok != (tt.wantCached != nil) || (ok && doc != *tt.wantCached) {
				t.Errorf("cached %+v (%v), want %+v", doc, ok, tt.wantCached)
			}
		})
	}
}

func TestLiveConfigCatalogValidate(t *testing.T) {
	tests := []struct {
		name    string
		catalog LiveConfigCatalog
		wantErr string
	}{
		{name: "empty"},
		{name: "distinct names", catalog: LiveConfigCatalog{LiveConfigOf[checkout](), LiveConfigOf[memCheckout]()}},
		{name: "duplicate name", catalog: LiveConfigCatalog{LiveConfigOf[checkout](), LiveConfigOf[checkout]()}, wantErr: `"checkout" is listed twice`},
		{name: "empty name", catalog: LiveConfigCatalog{LiveConfigOf[unnamed]()}, wantErr: "empty name"},
		{name: "zero type", catalog: LiveConfigCatalog{{}}, wantErr: "not created by LiveConfigOf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.catalog.Validate()
			if (tt.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("Validate = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestLiveConfigTypeDecode(t *testing.T) {
	typ := LiveConfigOf[checkout]()
	if typ.Name() != "checkout" || typ.MemoryTTL() != 0 || LiveConfigOf[memCheckout]().MemoryTTL() != time.Minute {
		t.Fatalf("Name %q, MemoryTTL %v", typ.Name(), typ.MemoryTTL())
	}

	got, err := typ.Decode([]byte(`{"enabled":false}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if p, ok := got.(*checkout); !ok || *p != (checkout{MaxItems: 50}) {
		t.Fatalf("Decode = %#v, want a *checkout completing the default", got)
	}

	for _, data := range []string{``, `{"max_item":1}`, `{"max_items":"many"}`, `{} {}`, `[]`} {
		if _, err := typ.Decode([]byte(data)); !errors.Is(err, ErrLiveConfigInvalid) {
			t.Errorf("Decode(%q): err = %v, want ErrLiveConfigInvalid", data, err)
		}
	}
}

func TestLiveConfigTypeStored(t *testing.T) {
	store, cache := newMemStore(), newMemCache()
	c := NewLiveConfigs(store, cache)
	typ := LiveConfigOf[checkout]()
	ctx := context.Background()

	cfg, err := typ.Decode([]byte(`{"max_items":3}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if version, changed, err := typ.SaveChanged(ctx, c, cfg, 0); err != nil || version != 1 || !changed {
		t.Fatalf("SaveChanged = %d, %v, %v", version, changed, err)
	}
	value, version, err := typ.LoadStored(ctx, c)
	if err != nil || version != 1 || value != (checkout{Enabled: true, MaxItems: 3}) {
		t.Fatalf("LoadStored = %#v, %d, %v", value, version, err)
	}
	if _, _, err := typ.SaveChanged(ctx, c, checkout{}, 1); err == nil || !strings.Contains(err.Error(), "want the result of Decode") {
		t.Fatalf("SaveChanged of a value that Decode did not return: err = %v", err)
	}
}

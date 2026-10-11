package forgo

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

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
// calls, and setCtxErr is the context error the last Set saw.
type memCache struct {
	mu        sync.Mutex
	docs      map[string]memDoc
	sets      int
	getErr    error
	setErr    error
	setCtxErr error
}

func newMemCache() *memCache {
	return &memCache{docs: map[string]memDoc{}}
}

func (c *memCache) Get(_ context.Context, name string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	doc, ok := c.docs[name]
	return []byte(doc.data), ok, nil
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

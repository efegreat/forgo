package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/efegreat/forgo"
)

type adminCheckout struct {
	Enabled  bool `json:"enabled"`
	MaxItems int  `json:"max_items" validate:"max=500,even"`
}

func (adminCheckout) Name() string           { return "checkout" }
func (adminCheckout) Default() adminCheckout { return adminCheckout{Enabled: true, MaxItems: 50} }

type adminPricing struct {
	Rate float64 `json:"rate"`
}

func (adminPricing) Name() string             { return "pricing" }
func (adminPricing) Default() adminPricing    { return adminPricing{Rate: 1.5} }
func (adminPricing) MemoryTTL() time.Duration { return 30 * time.Second }

var adminConfigs = forgo.LiveConfigCatalog{
	forgo.LiveConfigOf[adminCheckout](),
	forgo.LiveConfigOf[adminPricing](),
}

type liveDoc struct {
	data    string
	version uint64
}

// liveStore is a forgo.LiveConfigStore in memory.
type liveStore struct {
	mu   sync.Mutex
	docs map[string]liveDoc
}

func (s *liveStore) Get(_ context.Context, name string) ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, ok := s.docs[name]
	if !ok {
		return nil, 0, nil
	}
	return []byte(doc.data), doc.version, nil
}

func (s *liveStore) Set(_ context.Context, name string, data []byte, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.docs[name].version != version {
		return forgo.ErrLiveConfigConflict
	}
	s.docs[name] = liveDoc{data: string(data), version: version + 1}
	return nil
}

// liveCache is a forgo.LiveConfigCache in memory that keeps the newest version. err fails its Set.
type liveCache struct {
	mu   sync.Mutex
	docs map[string]liveDoc
	err  error
}

func (c *liveCache) Get(_ context.Context, name string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	doc, ok := c.docs[name]
	return []byte(doc.data), ok, nil
}

func (c *liveCache) Set(_ context.Context, name string, data []byte, version uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if c.docs[name].version < version {
		c.docs[name] = liveDoc{data: string(data), version: version}
	}
	return nil
}

func (c *liveCache) doc(name string) liveDoc {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.docs[name]
}

func (c *liveCache) fail(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
}

func newAdminServer(t *testing.T, base bool) (*Server, *liveStore, *liveCache) {
	t.Helper()
	return newAdminServerWith(t, base, Config{})
}

// newAdminServerWith is newAdminServer with the settings of cfg other than its validators and routes.
func newAdminServerWith(t *testing.T, base bool, cfg Config) (*Server, *liveStore, *liveCache) {
	t.Helper()
	store := &liveStore{docs: map[string]liveDoc{}}
	cache := &liveCache{docs: map[string]liveDoc{}}
	cfg = Config{
		MaxBodyBytes: cfg.MaxBodyBytes,
		Validators: map[string]validator.Func{
			"even": func(fl validator.FieldLevel) bool { return fl.Field().Int()%2 == 0 },
		},
		Routes: []RouteConfig{
			{Name: "live_configs", Method: "GET", Path: "/admin/configs", Handler: LiveConfigsHandler(adminConfigs)},
			{Name: "set_live_config", Method: "PUT", Path: "/admin/configs/{name}", Handler: SetLiveConfigHandler(adminConfigs)},
		},
	}
	cfg.DisableBaseMiddlewares = !base
	s := newTestServer(t, cfg,
		func() *liveStore { return store },
		func() *liveCache { return cache },
		forgo.NewLiveConfigs,
	)
	return s, store, cache
}

func putConfig(name, ifMatch, body string) *http.Request {
	r := httptest.NewRequest("PUT", "/admin/configs/"+name, strings.NewReader(body))
	if ifMatch != "" {
		r.Header.Set("If-Match", ifMatch)
	}
	return r
}

func TestLiveConfigsHandler(t *testing.T) {
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		s, store, _ := newAdminServer(t, base)
		store.docs["checkout"] = liveDoc{data: `{"max_items":20}`, version: 3}

		w, envelope := do(t, s, httptest.NewRequest("GET", "/admin/configs", nil))
		if w.Code != 200 {
			t.Fatalf("status %d, body %s", w.Code, w.Body)
		}
		result, _ := envelope["result"].(map[string]any)
		want := map[string]any{
			"checkout": map[string]any{"version": float64(3), "value": map[string]any{"enabled": true, "max_items": float64(20)}},
			"pricing":  map[string]any{"version": float64(0), "value": map[string]any{"rate": 1.5}, "memory_ttl": "30s"},
		}
		for name, view := range want {
			if got := result[name]; !reflect.DeepEqual(got, view) {
				t.Errorf("%s = %v, want %v", name, got, view)
			}
		}
		if len(result) != len(want) {
			t.Errorf("result has %d configs, want %d", len(result), len(want))
		}
	})
}

func TestSetLiveConfigHandler(t *testing.T) {
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		s, store, cache := newAdminServer(t, base)
		steps := []struct {
			name        string
			req         *http.Request
			wantStatus  int
			wantCode    string
			wantMsg     string
			wantVersion float64
			wantChanged bool
		}{
			{
				name:        "first change completes the defaults",
				req:         putConfig("checkout", "0", `{"enabled":false}`),
				wantStatus:  200,
				wantVersion: 1,
				wantChanged: true,
			},
			{
				name:        "unchanged config stores nothing, whatever the version",
				req:         putConfig("checkout", "0", `{"enabled":false,"max_items":50}`),
				wantStatus:  200,
				wantVersion: 1,
			},
			{
				name:       "change from a stale version",
				req:        putConfig("checkout", "0", `{"max_items":10}`),
				wantStatus: 412,
				wantCode:   "live_config_conflict",
			},
			{
				name:        "quoted entity tag",
				req:         putConfig("checkout", `"1"`, `{"max_items":10}`),
				wantStatus:  200,
				wantVersion: 2,
				wantChanged: true,
			},
			{
				name:       "unknown config",
				req:        putConfig("shipping", "0", `{}`),
				wantStatus: 404,
				wantCode:   "live_config_not_found",
			},
			{
				name:       "missing If-Match",
				req:        putConfig("checkout", "", `{}`),
				wantStatus: 400,
				wantCode:   "live_config_version",
			},
			{
				name:       "If-Match that is not a version",
				req:        putConfig("checkout", "*", `{}`),
				wantStatus: 400,
				wantCode:   "live_config_version",
			},
			{
				name:       "misspelled field",
				req:        putConfig("checkout", "2", `{"max_item":10}`),
				wantStatus: 400,
				wantCode:   "live_config_invalid",
				wantMsg:    `unknown field "max_item"`,
			},
			{
				name:       "malformed document",
				req:        putConfig("checkout", "2", `{"max_items":`),
				wantStatus: 400,
				wantCode:   "live_config_invalid",
			},
			{
				name:       "document over the server's body limit",
				req:        putConfig("checkout", "2", `{"enabled":false}`+strings.Repeat(" ", maxLiveConfigBody)),
				wantStatus: 413,
				wantCode:   "body_too_large",
			},
			{
				name:       "validate tag",
				req:        putConfig("checkout", "2", `{"max_items":502}`),
				wantStatus: 400,
				wantCode:   "validation",
			},
			{
				name:       "custom validator of the server",
				req:        putConfig("checkout", "2", `{"max_items":7}`),
				wantStatus: 400,
				wantCode:   "validation",
			},
		}
		for _, step := range steps {
			w, envelope := do(t, s, step.req)
			if w.Code != step.wantStatus || errCode(envelope) != step.wantCode {
				t.Fatalf("%s: status %d, body %s", step.name, w.Code, w.Body)
			}
			if msg, _ := envelope["errMsg"].(string); !strings.Contains(msg, step.wantMsg) {
				t.Fatalf("%s: message %q, want %q", step.name, msg, step.wantMsg)
			}
			if step.wantStatus != 200 {
				continue
			}
			result, _ := envelope["result"].(map[string]any)
			if result["version"] != step.wantVersion || result["changed"] != step.wantChanged {
				t.Fatalf("%s: result %v, want version %v changed %v", step.name, result, step.wantVersion, step.wantChanged)
			}
		}

		want := liveDoc{data: `{"enabled":true,"max_items":10}`, version: 2}
		if got := store.docs["checkout"]; got != want {
			t.Fatalf("stored %+v, want %+v", got, want)
		}
		if got := cache.doc("checkout"); got != want {
			t.Fatalf("cached %+v, want %+v", got, want)
		}
	})
}

// TestSetLiveConfigHandlerSizeLimit checks that the handler bounds documents itself on a server without a body
// limit.
func TestSetLiveConfigHandlerSizeLimit(t *testing.T) {
	s, _, _ := newAdminServerWith(t, true, Config{MaxBodyBytes: -1})
	w, envelope := do(t, s, putConfig("checkout", "0", `{"enabled":false}`+strings.Repeat(" ", maxLiveConfigBody)))
	if msg, _ := envelope["errMsg"].(string); w.Code != 400 || errCode(envelope) != "live_config_invalid" || !strings.Contains(msg, "larger than 1 MiB") {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
}

func TestSetLiveConfigHandlerNotCached(t *testing.T) {
	s, store, cache := newAdminServer(t, true)
	cache.fail(errors.New("cache down"))

	w, envelope := do(t, s, putConfig("pricing", "0", `{"rate":2}`))
	if w.Code != 503 || errCode(envelope) != "live_config_not_cached" {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	if got := store.docs["pricing"]; got.version != 1 {
		t.Fatalf("stored %+v, want version 1", got)
	}

	// Sending the same config again stores nothing, and completes the cache.
	cache.fail(nil)
	w, envelope = do(t, s, putConfig("pricing", "0", `{"rate":2}`))
	if result, _ := envelope["result"].(map[string]any); w.Code != 200 || result["version"] != float64(1) || result["changed"] != false {
		t.Fatalf("resend: status %d, body %s", w.Code, w.Body)
	}
	if got := cache.doc("pricing"); got != (liveDoc{data: `{"rate":2}`, version: 1}) {
		t.Fatalf("cached %+v after the resend", got)
	}
}

func TestLiveConfigHandlersNew(t *testing.T) {
	duplicate := forgo.LiveConfigCatalog{forgo.LiveConfigOf[adminCheckout](), forgo.LiveConfigOf[adminCheckout]()}
	tests := []struct {
		name      string
		handler   HandlerFactory
		providers []any
		wantErr   string
	}{
		{
			name:      "duplicate name",
			handler:   LiveConfigsHandler(duplicate),
			providers: []any{func() *liveStore { return &liveStore{} }, forgo.NewUncachedLiveConfigs},
			wantErr:   `"checkout" is listed twice`,
		},
		{
			name:    "no LiveConfigs",
			handler: SetLiveConfigHandler(adminConfigs),
			wantErr: "missing dependency",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Routes: []RouteConfig{{Method: "GET", Path: "/admin/configs", Handler: tt.handler}}}
			err := newServerErr(t, cfg, tt.providers...)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("New = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestParseIfMatch(t *testing.T) {
	tests := []struct {
		header string
		want   uint64
		wantOK bool
	}{
		{"3", 3, true},
		{` "3" `, 3, true},
		{"0", 0, true},
		{"", 0, false},
		{`""`, 0, false},
		{"*", 0, false},
		{`W/"3"`, 0, false},
		{"-1", 0, false},
	}
	for _, tt := range tests {
		if got, ok := parseIfMatch(tt.header); got != tt.want || ok != tt.wantOK {
			t.Errorf("parseIfMatch(%q) = %d, %v; want %d, %v", tt.header, got, ok, tt.want, tt.wantOK)
		}
	}
}

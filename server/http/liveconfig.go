package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

// maxLiveConfigBody bounds the document SetLiveConfigHandler reads.
const maxLiveConfigBody = 1 << 20

var (
	errLiveConfigNotFound = operation.Error{
		Code:     "live_config_not_found",
		Status:   operation.StatusNotFound,
		Message:  "live config not found",
		Severity: operation.SeverityLight,
	}
	errLiveConfigVersion = operation.Error{
		Code:     "live_config_version",
		Status:   operation.StatusInvalidArgument,
		Message:  "If-Match must hold the version of the live config the change started from",
		Severity: operation.SeverityLight,
	}
	errLiveConfigInvalid = operation.Error{
		Code:     "live_config_invalid",
		Status:   operation.StatusInvalidArgument,
		Message:  "%s",
		Severity: operation.SeverityLight,
	}
	errLiveConfigConflict = operation.Error{
		Code:     "live_config_conflict",
		Status:   operation.StatusPreconditionFailed,
		Message:  "the live config was changed since the given version",
		Severity: operation.SeverityLight,
	}
	errLiveConfigNotCached = operation.Error{
		Code:     "live_config_not_cached",
		Status:   operation.StatusUnavailable,
		Message:  "the change is stored but not live yet: send it again",
		Severity: operation.SeverityModerate,
	}
)

// LiveConfigView is one configuration in the response of LiveConfigsHandler.
type LiveConfigView struct {
	// Version is the stored version, 0 while nothing is stored.
	Version uint64 `json:"version"`
	// Value is the Default of the configuration completed with the stored document.
	Value any `json:"value"`
	// MemoryTTL is how long instances may serve an older value after a change, such as "30s", for a
	// configuration kept in memory.
	MemoryTTL string `json:"memory_ttl,omitempty"`
}

// LiveConfigsResp is the response of LiveConfigsHandler, keyed by configuration name.
type LiveConfigsResp map[string]LiveConfigView

// SetLiveConfigResp is the response of SetLiveConfigHandler.
type SetLiveConfigResp struct {
	// Version is the stored version after the request.
	Version uint64 `json:"version"`
	// Changed reports whether the request stored a new version.
	Changed bool `json:"changed"`
}

// setLiveConfigParams documents the parameters of SetLiveConfigHandler for the OpenAPI generator. The handler
// reads them itself, and its body is the configuration.
type setLiveConfigParams struct {
	Name    string `uri:"name" validate:"required"`
	IfMatch string `header:"If-Match" validate:"required"`
}

// LiveConfigsHandler answers with every configuration of catalog, keyed by name: its value, the Default of its
// type completed with the stored document, and its version, 0 while nothing is stored. It reads the store, never
// the cache or memory, so admins see the state that saves are checked against. A configuration kept in memory
// also has its MemoryTTL.
//
// Anyone who reaches the route reads every configuration of catalog: protect it with middlewares. New fails when
// the catalog is invalid or no *forgo.LiveConfigs is provided.
func LiveConfigsHandler(catalog forgo.LiveConfigCatalog) HandlerFactory {
	return HandlerFactory{
		build: func(r forgo.Resolver, _ *forgo.TermSig) (handlerWrapper, error) {
			types, configs, err := resolveLiveConfigCatalog(r, catalog)
			if err != nil {
				return nil, err
			}
			return func(_ IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
				return func(ctx *operation.Context) error {
					s := sf(ctx.ID, ctx.State.(*State).Writer)
					resp := make(LiveConfigsResp, len(types))
					for _, t := range types {
						value, version, err := t.LoadStored(ctx, configs)
						if err != nil {
							return s.Error(em.Map(err))
						}
						view := LiveConfigView{Version: version, Value: value}
						if ttl := t.MemoryTTL(); ttl > 0 {
							view.MemoryTTL = ttl.String()
						}
						resp[t.Name()] = view
					}
					return s.Success(resp)
				}
			}, nil
		},
		contract: HandlerContract{Response: reflect.TypeFor[LiveConfigsResp]()},
	}
}

// SetLiveConfigHandler changes the configuration of catalog named by the {name} path wildcard. The body is the
// whole configuration, as LiveConfigsHandler answers it: it completes the Default of the type, and fields the
// type does not have are rejected. The configuration is validated by the server's Binder, like a request body.
// The If-Match header holds the version the change started from, such as 3 or "3".
//
// A configuration equal to the stored one stores nothing, whatever the version. Otherwise it is saved only while
// the stored version is still the one in If-Match. Either way, the response holds the stored version and whether
// it changed. See forgo.LiveConfigs.SaveChanged.
//
// Anyone who reaches the route changes the configurations of catalog: protect it with middlewares. New fails
// when the catalog is invalid or no *forgo.LiveConfigs is provided.
func SetLiveConfigHandler(catalog forgo.LiveConfigCatalog) HandlerFactory {
	return HandlerFactory{
		build: func(r forgo.Resolver, _ *forgo.TermSig) (handlerWrapper, error) {
			types, configs, err := resolveLiveConfigCatalog(r, catalog)
			if err != nil {
				return nil, err
			}
			byName := make(map[string]forgo.LiveConfigType, len(types))
			for _, t := range types {
				byName[t.Name()] = t
			}
			return func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
				return func(ctx *operation.Context) error {
					st := ctx.State.(*State)
					s := sf(ctx.ID, st.Writer)
					resp, err := setLiveConfig(ctx, st.Request, b, configs, byName)
					if err != nil {
						return s.Error(em.Map(err))
					}
					return s.Success(resp)
				}
			}, nil
		},
		contract: HandlerContract{
			Request:  reflect.TypeFor[setLiveConfigParams](),
			Response: reflect.TypeFor[SetLiveConfigResp](),
		},
	}
}

// resolveLiveConfigCatalog validates catalog and resolves the LiveConfigs its handlers work with.
func resolveLiveConfigCatalog(r forgo.Resolver, catalog forgo.LiveConfigCatalog) ([]forgo.LiveConfigType, *forgo.LiveConfigs, error) {
	if err := catalog.Validate(); err != nil {
		return nil, nil, fmt.Errorf("http: live config handler: %w", err)
	}
	configs, err := r.Resolve[*forgo.LiveConfigs]()
	if err != nil {
		return nil, nil, fmt.Errorf("http: live config handler: %w", err)
	}
	return append([]forgo.LiveConfigType(nil), catalog...), configs, nil
}

func setLiveConfig(
	ctx context.Context,
	r *http.Request,
	b IBinder,
	configs *forgo.LiveConfigs,
	byName map[string]forgo.LiveConfigType,
) (SetLiveConfigResp, error) {
	t, ok := byName[r.PathValue("name")]
	if !ok {
		return SetLiveConfigResp{}, errLiveConfigNotFound
	}
	version, ok := parseIfMatch(r.Header.Get("If-Match"))
	if !ok {
		return SetLiveConfigResp{}, errLiveConfigVersion
	}

	data, err := io.ReadAll(io.LimitReader(r.Body, maxLiveConfigBody+1))
	if err != nil {
		return SetLiveConfigResp{}, fmt.Errorf("http: read live config: %w", err)
	}
	if len(data) > maxLiveConfigBody {
		return SetLiveConfigResp{}, errLiveConfigInvalid.Format("the live config is larger than 1 MiB")
	}
	cfg, err := t.Decode(data)
	if err != nil {
		return SetLiveConfigResp{}, errLiveConfigInvalid.Format(err.Error()).Wrap(err)
	}
	if err := validateLiveConfig(r, b, t, cfg); err != nil {
		return SetLiveConfigResp{}, err
	}

	stored, changed, err := t.SaveChanged(ctx, configs, cfg, version)
	switch {
	case errors.Is(err, forgo.ErrLiveConfigConflict):
		return SetLiveConfigResp{}, errLiveConfigConflict.Wrap(err)
	case errors.Is(err, forgo.ErrLiveConfigNotCached):
		return SetLiveConfigResp{}, errLiveConfigNotCached.Wrap(err)
	case err != nil:
		return SetLiveConfigResp{}, err
	}
	return SetLiveConfigResp{Version: stored, Changed: changed}, nil
}

// validateLiveConfig validates cfg, a configuration decoded by t, with the server's binder, so the validate
// tags, custom validators and error codes are those of every request body. A binder decodes the body into a new
// value, so it binds the complete JSON encoding of cfg into a second value of the type, which is discarded.
func validateLiveConfig(r *http.Request, b IBinder, t forgo.LiveConfigType, cfg any) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("http: encode live config: %w", err)
	}
	probe, err := t.Decode(data)
	if err != nil {
		return fmt.Errorf("http: decode live config: %w", err)
	}
	req := r.Clone(r.Context())
	req.Body = io.NopCloser(bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	return b.Bind(req, probe)
}

// parseIfMatch returns the version in an If-Match header: a decimal number, optionally quoted like an entity tag.
func parseIfMatch(h string) (uint64, bool) {
	h = strings.TrimSpace(h)
	if len(h) >= 2 && h[0] == '"' && h[len(h)-1] == '"' {
		h = h[1 : len(h)-1]
	}
	if h == "" {
		return 0, false
	}
	version, err := strconv.ParseUint(h, 10, 64)
	return version, err == nil
}

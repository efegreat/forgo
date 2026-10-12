package worker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/redact"
	"github.com/efegreat/forgo/operation"
)

const defaultPublisherName = "default"

type ProducerConfig struct {
	forgo.ServerConfig

	// Maximum number of messages claimed per query.
	Limit int

	// Wait when there is no full backlog to process.
	Interval time.Duration

	// Maximum publish error percentage. Triggers OnErrorPercentage if reached.
	ErrorPercentage int

	// Triggers when publish error percentage >= ErrorPercentage. When nil, the breach is logged.
	OnErrorPercentage func(ctx context.Context)

	// How long a claim is considered owned by an outbox instance.
	// After this duration another instance may reclaim it.
	ClaimTTL time.Duration

	Clean CleanConfig

	DefaultPublisher PublisherConfig
	Publishers       []PublisherConfig
}

func (c ProducerConfig) Sanitize() ProducerConfig {
	c.ServerConfig = c.ServerConfig.Sanitize()
	if c.Limit == 0 {
		c.Limit = 100
	}
	if c.Interval == 0 {
		c.Interval = time.Second
	}
	if c.ClaimTTL == 0 {
		c.ClaimTTL = time.Minute
	}
	c.Clean = c.Clean.Sanitize()
	c.DefaultPublisher = c.DefaultPublisher.Sanitize()
	if c.DefaultPublisher.Name == "" {
		c.DefaultPublisher.Name = defaultPublisherName
	}

	for i, pb := range c.Publishers {
		c.Publishers[i] = pb.Sanitize()
	}

	return c
}

func (c ProducerConfig) Name() string {
	return ServerNameProducer
}

func (c ProducerConfig) New(r forgo.Resolver) (forgo.IServer, error) {
	broker, err := r.Resolve[IBroker]()
	if err != nil {
		return nil, err
	}
	storage, err := r.Resolve[OutboxStorage]()
	if err != nil {
		return nil, err
	}

	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &ProducerServer{cfg: cfg, publishers: make(map[string]*publisher), broker: broker, storage: storage}, nil
}

// validate rejects the negative values that would only fail once serving: a stalled relay, a
// claim that never ends or a busy loop.
func (c ProducerConfig) validate() error {
	if err := notNegative("", field{"Limit", int64(c.Limit)}, field{"Interval", int64(c.Interval)}, field{"ClaimTTL", int64(c.ClaimTTL)}); err != nil {
		return err
	}
	if err := c.Clean.validate(); err != nil {
		return err
	}
	for _, pc := range append([]PublisherConfig{c.DefaultPublisher}, c.Publishers...) {
		scope := fmt.Sprintf("publisher %q: ", pc.Name)
		if err := notNegative(scope, field{"Concurrency", int64(pc.Concurrency)}, field{"Timeout", int64(pc.Timeout)}); err != nil {
			return err
		}
	}
	return nil
}

type ConsumerConfig struct {
	forgo.ServerConfig
	Clean  CleanConfig
	Binder IBinder
	// SensitiveFields names the fields whose values are masked as [*] in logs: in message headers,
	// JSON payloads at any depth and binding errors. Names match case-insensitively. The credential
	// headers Authorization, Proxy-Authorization, Cookie, Set-Cookie and X-Api-Key are always masked.
	SensitiveFields     []string
	Subscribers         []SubscriberConfig
	resolvedSubscribers []resolvedSubscriber
}

func (c ConsumerConfig) Sanitize() ConsumerConfig {
	c.ServerConfig = c.ServerConfig.Sanitize()
	c.Clean = c.Clean.Sanitize()
	if c.Binder == nil {
		c.Binder = newDefaultBinder()
	}
	for i, sb := range c.Subscribers {
		c.Subscribers[i] = sb.Sanitize()
	}

	return c
}

func (c ConsumerConfig) Name() string {
	return ServerNameConsumer
}

func (c ConsumerConfig) New(r forgo.Resolver) (forgo.IServer, error) {
	locker, err := r.Resolve[operation.ILocker]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}
	broker, err := r.Resolve[IBroker]()
	if err != nil {
		return nil, err
	}
	storage, err := r.Resolve[InboxStorage]()
	if err != nil {
		return nil, err
	}
	transactor, err := r.Resolve[operation.Transactor]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}

	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := cfg.validateDependencies(locker, transactor); err != nil {
		return nil, err
	}
	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc
	if len(cfg.SensitiveFields) > 0 {
		cfg.Binder = maskingBinder{IBinder: cfg.Binder, sensitive: redact.NewFields(cfg.SensitiveFields...)}
	}
	cfg, err = cfg.resolveSubscribers(r)
	if err != nil {
		return nil, err
	}

	return &ConsumerServer{cfg: cfg, subscribers: make([]*subscriber, 0), locker: locker, broker: broker, storage: storage, transactor: transactor}, nil
}

// validate rejects the negative values that would only fail once serving, such as a concurrency the
// broker cannot consume with.
func (c ConsumerConfig) validate() error {
	if err := c.Clean.validate(); err != nil {
		return err
	}
	for _, sb := range c.Subscribers {
		scope := fmt.Sprintf("subscriber %q: ", sb.Name)
		if err := notNegative(scope, field{"Concurrency", int64(sb.Concurrency)}, field{"MaxAttempt", int64(sb.MaxAttempt)}); err != nil {
			return err
		}
		for _, h := range sb.Handlers {
			if err := notNegative(scope+fmt.Sprintf("handler %q: ", h.Name), field{"Timeout", int64(h.Timeout)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateDependencies fails fast when a handler needs an optional dependency that is not provided.
func (c ConsumerConfig) validateDependencies(locker operation.ILocker, transactor operation.Transactor) error {
	for _, sb := range c.Subscribers {
		for _, h := range sb.Handlers {
			if locker == nil && h.Lock.Enabled() && !c.DisableBaseMiddlewares {
				return fmt.Errorf("worker: handler %q of subscriber %q has a lock but no operation.ILocker is provided", h.Name, sb.Name)
			}
			if transactor == nil && !h.TransactionDisabled {
				return fmt.Errorf("worker: handler %q of subscriber %q runs in a transaction but no operation.Transactor is provided; provide one or set TransactionDisabled", h.Name, sb.Name)
			}
		}
	}
	return nil
}

func (c ConsumerConfig) resolveSubscribers(r forgo.Resolver) (ConsumerConfig, error) {
	for _, sb := range c.Subscribers {
		var handlers []resolvedHandler

		for _, h := range sb.Handlers {
			handler, err := h.Handler(r)
			if err != nil {
				return ConsumerConfig{}, err
			}
			middlewares, err := forgo.ResolveMiddlewares(r, h.Middlewares)
			if err != nil {
				return ConsumerConfig{}, err
			}
			handlers = append(handlers, resolvedHandler{
				Name:                h.Name,
				Lock:                h.Lock,
				TransactionDisabled: h.TransactionDisabled,
				Timeout:             h.Timeout,
				Middlewares:         middlewares,
				Handler:             handler,
				Args:                maps.Clone(h.Args), // shared by every message until one writes to its Args
			})
		}

		middlewares, err := forgo.ResolveMiddlewares(r, sb.Middlewares)
		if err != nil {
			return ConsumerConfig{}, err
		}
		c.resolvedSubscribers = append(c.resolvedSubscribers, resolvedSubscriber{
			Name:        sb.Name,
			Concurrency: sb.Concurrency,
			MaxAttempt:  sb.MaxAttempt,
			AckTimeout:  sb.AckTimeout,
			Middlewares: middlewares,
			Handlers:    handlers,
		})
	}
	return c, nil
}

type CleanConfig struct {
	// Successfully processed messages older than this are deleted.
	// Zero uses the default (30 days); a negative value disables cleanup.
	MaxTTL time.Duration

	// Maximum number of old processed messages to be deleted per query.
	CleanupLimit int

	// How often old processed messages are deleted.
	// Only used when MaxTTL > 0.
	CleanupInterval time.Duration
}

// validate rejects the negative values that would fail every deletion or delete in a busy loop.
// A negative MaxTTL disables the cleanup.
func (c CleanConfig) validate() error {
	return notNegative("Clean.", field{"CleanupLimit", int64(c.CleanupLimit)}, field{"CleanupInterval", int64(c.CleanupInterval)})
}

func (c CleanConfig) Sanitize() CleanConfig {
	if c.MaxTTL == 0 {
		c.MaxTTL = 30 * 24 * time.Hour
	}
	if c.CleanupLimit == 0 {
		c.CleanupLimit = 100
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = time.Minute * 10
	}
	return c
}

type PublisherConfig struct {
	// Name routes messages with the same Message.Name to this publisher and is passed to IBroker.NewPublisher.
	// The default publisher is created with IBroker.NewDefaultPublisher; its name only labels its logs.
	Name string
	// Concurrency is the number of broker publishers publishing in parallel.
	Concurrency int
	// Timeout bounds a single publish.
	Timeout time.Duration
}

func (c PublisherConfig) Sanitize() PublisherConfig {
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.Timeout == 0 {
		c.Timeout = time.Second * 3
	}
	return c
}

type SubscriberConfig struct {
	// Name of the subscriber, passed to IBroker.NewSubscriber.
	Name string
	// Concurrency is the number of messages handled in parallel.
	Concurrency int
	// MaxAttempt is the number of attempts before a message is marked failed and rejected.
	MaxAttempt int
	// AckTimeout bounds each acknowledgement sent to the broker: complete, retry or reject.
	AckTimeout  time.Duration
	Middlewares []forgo.MiddlewareFactory
	Handlers    []HandlerConfig
}

func (s SubscriberConfig) Sanitize() SubscriberConfig {
	if s.Concurrency == 0 {
		s.Concurrency = 1
	}
	if s.MaxAttempt == 0 {
		s.MaxAttempt = 3
	}
	if s.AckTimeout == 0 {
		s.AckTimeout = time.Second * 3
	}

	return s
}

type HandlerConfig struct {
	Name                string
	Lock                operation.Lock
	TransactionDisabled bool
	// Timeout ends the context the handler receives after this long, and the message then fails
	// like on any handler error. Zero means no limit. Go cannot stop a goroutine: a handler that
	// ignores its context keeps running and holds its concurrency slot. The inbox mark and the
	// commit that follow the handler are not bound by Timeout.
	Timeout     time.Duration
	Middlewares []forgo.MiddlewareFactory
	Handler     HandlerFactory
	Args        map[any]any
}

type resolvedSubscriber struct {
	Name        string
	Concurrency int
	MaxAttempt  int
	AckTimeout  time.Duration
	Middlewares []operation.Middleware
	Handlers    []resolvedHandler
}

type resolvedHandler struct {
	Name                string
	Lock                operation.Lock
	TransactionDisabled bool
	Timeout             time.Duration
	Middlewares         []operation.Middleware
	Handler             handlerWrapper
	Args                map[any]any
}

func HandlerFor[S, Req any](
	fn func(
		S,
		context.Context,
		Req,
	) error,
) HandlerFactory {
	return func(
		r forgo.Resolver,
	) (handlerWrapper, error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}

		return newHandlerWrapper(
			func(
				ctx context.Context,
				req Req,
			) error {
				return fn(
					service,
					ctx,
					req,
				)
			},
		), nil
	}
}

func HandlerAs[Req any](
	constructor any,
) HandlerFactory {
	return func(
		r forgo.Resolver,
	) (handlerWrapper, error) {
		fn, err := r.Call[Handler[Req]](
			constructor,
		)
		if err != nil {
			return nil, err
		}

		return newHandlerWrapper(fn), nil
	}
}

type HandlerFactory func(r forgo.Resolver) (handlerWrapper, error)

// field is a numeric config value and its name, for notNegative.
type field struct {
	name  string
	value int64
}

// notNegative returns an error naming the first negative field, prefixed by scope.
func notNegative(scope string, fields ...field) error {
	for _, f := range fields {
		if f.value < 0 {
			return fmt.Errorf("worker: %s%s must not be negative", scope, f.name)
		}
	}
	return nil
}

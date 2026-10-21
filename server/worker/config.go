package worker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/redact"

	"github.com/go-playground/validator/v10"
)

const defaultPublisherName = "default"

// ProducerConfig declares an outbox relay: a server that publishes the messages the application stored in
// its outbox. It is a forgo.ServerFactory, so it can be declared as a package variable and passed to
// forgo.WithServers. Zero fields get the defaults of Sanitize. It needs an IBroker and an OutboxStorage in
// the bootstrap.
type ProducerConfig struct {
	forgo.ServerConfig

	// Limit is the number of messages claimed per batch. Default: 100.
	Limit int

	// Interval is the pause after a batch that was not full or that failed. Full batches are processed
	// back to back. Default: 1 second.
	Interval time.Duration

	// ErrorPercentage is the share of failed publishes in a batch, in percent, at which OnErrorPercentage
	// is called. Zero turns it off.
	ErrorPercentage int

	// OnErrorPercentage is called when a batch reaches ErrorPercentage, on the relay's goroutine, so it
	// must return quickly. When nil, the batch is logged with err_pct.
	OnErrorPercentage func(ctx context.Context)

	// ClaimTTL is how long a claim belongs to one producer instance. After it, another instance may take
	// the claim over, for example after a crash. A batch stops publishing at half its ClaimTTL, so that it
	// completes its claim before another instance can take it. Default: 1 minute.
	ClaimTTL time.Duration

	// BacklogInterval is how often the producer samples the backlog of the outbox for an IBacklogObserver,
	// when the OutboxStorage is an OutboxBacklogStorage. Default: 15 seconds.
	BacklogInterval time.Duration

	// BacklogLimit is the most pending messages a backlog sample counts. The count reads every pending row up
	// to it, so it keeps samples cheap however far the relay is behind, and a sample at the limit means at
	// least that many. Default: 10000.
	BacklogLimit int

	// Clean deletes the processed outbox rows.
	Clean CleanConfig

	// DefaultPublisher publishes the messages whose Name matches none of Publishers. Its Name only
	// labels its logs. Default name: "default".
	DefaultPublisher PublisherConfig
	// Publishers publish the messages whose Name equals theirs. Names must be unique and non-empty.
	Publishers []PublisherConfig
}

// Sanitize returns a copy of c with defaults applied, the publishers included.
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
	if c.BacklogInterval == 0 {
		c.BacklogInterval = 15 * time.Second
	}
	if c.BacklogLimit == 0 {
		c.BacklogLimit = 10000
	}
	c.Clean = c.Clean.Sanitize()
	c.DefaultPublisher = c.DefaultPublisher.Sanitize()
	if c.DefaultPublisher.Name == "" {
		c.DefaultPublisher.Name = defaultPublisherName
	}

	// A copy of its own, so that the caller's config keeps its zero values.
	c.Publishers = slices.Clone(c.Publishers)
	for i, pb := range c.Publishers {
		c.Publishers[i] = pb.Sanitize()
	}

	return c
}

// Name returns ServerConfig.Name, ServerNameProducer by default.
func (c ProducerConfig) Name() string {
	return c.ServerConfig.NameOr(ServerNameProducer)
}

// New validates the configuration and resolves the broker, the outbox and the optional operation.IObserver,
// which the relay reports its batches to when it implements IRelayObserver, and the backlog of an
// OutboxBacklogStorage to when it implements IBacklogObserver, so that a configuration error fails at
// startup.
func (c ProducerConfig) New(r forgo.Resolver) (forgo.IServer, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	broker, err := r.Resolve[IBroker]()
	if err != nil {
		return nil, err
	}
	storage, err := r.Resolve[OutboxStorage]()
	if err != nil {
		return nil, err
	}
	observer, err := r.Resolve[operation.IObserver]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}
	relays, _ := observer.(IRelayObserver)
	s := &producerServer{
		cfg:        cfg,
		publishers: make(map[string]*publisher),
		broker:     broker,
		storage:    storage,
		relays:     relays,
	}
	if backlogs, ok := observer.(IBacklogObserver); ok {
		if backlog, ok := storage.(OutboxBacklogStorage); ok {
			s.backlogs, s.backlog = backlogs, backlog
		}
	}
	return s, nil
}

// validate rejects the publishers that cannot be told apart, and the negative values that would only fail
// once serving: a stalled relay, a claim that never ends or a busy loop.
func (c ProducerConfig) validate() error {
	if err := notNegative("", field{"Limit", int64(c.Limit)}, field{"Interval", int64(c.Interval)}, field{"ClaimTTL", int64(c.ClaimTTL)},
		field{"BacklogInterval", int64(c.BacklogInterval)}, field{"BacklogLimit", int64(c.BacklogLimit)}); err != nil {
		return err
	}
	if err := c.Clean.validate(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(c.Publishers))
	for i, pc := range c.Publishers {
		if pc.Name == "" {
			return fmt.Errorf("worker: publisher %d has no name", i)
		}
		if _, ok := seen[pc.Name]; ok {
			return fmt.Errorf("worker: duplicate publisher %q", pc.Name)
		}
		seen[pc.Name] = struct{}{}
	}
	for _, pc := range append([]PublisherConfig{c.DefaultPublisher}, c.Publishers...) {
		scope := fmt.Sprintf("publisher %q: ", pc.Name)
		if err := notNegative(scope, field{"Concurrency", int64(pc.Concurrency)}, field{"Timeout", int64(pc.Timeout)}); err != nil {
			return err
		}
	}
	return nil
}

// ConsumerConfig declares a consumer: a server that handles the messages its subscribers receive from a
// broker, and keeps track of them in an inbox. It is a forgo.ServerFactory, so it can be declared as a
// package variable and passed to forgo.WithServers. Zero fields get the defaults of Sanitize. It needs an
// IBroker and an InboxStorage in the bootstrap, an operation.Transactor unless every handler sets
// TransactionDisabled, and an operation.ILocker when a handler sets a Lock.
type ConsumerConfig struct {
	forgo.ServerConfig
	// Clean deletes the completed and rejected inbox rows.
	Clean CleanConfig
	// Validators are the custom validation tags of the default binder, by tag name.
	Validators map[string]validator.Func
	// Binder decodes and validates the payload of every message into its handler's request. Default:
	// encoding/json and go-playground/validator, see IBinder.
	Binder IBinder
	// SensitiveFields names the fields whose values are masked as [*] in logs: in message headers,
	// JSON payloads at any depth and binding errors. Names match case-insensitively. The credential
	// headers Authorization, Proxy-Authorization, Cookie, Set-Cookie and X-Api-Key are always masked.
	SensitiveFields []string
	// Subscribers are the subscribers the server consumes with. Names must be unique and non-empty.
	Subscribers []SubscriberConfig

	resolvedSubscribers []resolvedSubscriber
}

// Sanitize returns a copy of c with defaults applied, the subscribers included. The default Binder is built
// by New, because building it fails on invalid Validators.
func (c ConsumerConfig) Sanitize() ConsumerConfig {
	c.ServerConfig = c.ServerConfig.Sanitize()
	c.Clean = c.Clean.Sanitize()
	// A copy of its own, so that the caller's config keeps its zero values.
	c.Subscribers = slices.Clone(c.Subscribers)
	for i, sb := range c.Subscribers {
		c.Subscribers[i] = sb.Sanitize()
	}

	return c
}

// Name returns ServerConfig.Name, ServerNameConsumer by default.
func (c ConsumerConfig) Name() string {
	return c.ServerConfig.NameOr(ServerNameConsumer)
}

// New validates the configuration, builds the binder and resolves the dependencies, the middlewares and the
// handler of every subscriber, so that a configuration error fails at startup with the subscriber and the
// handler it is about.
func (c ConsumerConfig) New(r forgo.Resolver) (forgo.IServer, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

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
	if err := cfg.validateDependencies(locker, transactor); err != nil {
		return nil, err
	}
	observer, err := r.Resolve[operation.IObserver]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}

	if cfg.Binder == nil {
		binder, err := newDefaultBinder(cfg.Validators)
		if err != nil {
			return nil, err
		}
		cfg.Binder = binder
	}
	if len(cfg.SensitiveFields) > 0 {
		cfg.Binder = maskingBinder{IBinder: cfg.Binder, sensitive: redact.NewFields(cfg.SensitiveFields...)}
	}
	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc
	cfg, err = cfg.resolveSubscribers(r)
	if err != nil {
		return nil, err
	}

	deliveries, _ := observer.(IDeliveryObserver)
	return &consumerServer{
		cfg:        cfg,
		locker:     operation.ObserveLocks(locker, observer),
		observer:   observer,
		deliveries: deliveries,
		broker:     broker,
		storage:    storage,
		transactor: transactor,
	}, nil
}

// validate rejects the subscribers and handlers that cannot run or cannot be told apart, and the negative
// values that would only fail once serving, such as a concurrency the broker cannot consume with.
func (c ConsumerConfig) validate() error {
	if err := c.Clean.validate(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(c.Subscribers))
	for i, sb := range c.Subscribers {
		if sb.Name == "" {
			return fmt.Errorf("worker: subscriber %d has no name", i)
		}
		if _, ok := seen[sb.Name]; ok {
			return fmt.Errorf("worker: duplicate subscriber %q", sb.Name)
		}
		seen[sb.Name] = struct{}{}
		if err := sb.validate(); err != nil {
			return err
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
			handler, err := h.Handler.build(r)
			if err != nil {
				return ConsumerConfig{}, fmt.Errorf("worker: subscriber %q: handler %q: %w", sb.Name, h.Name, err)
			}
			middlewares, err := forgo.ResolveMiddlewares(r, h.Middlewares)
			if err != nil {
				return ConsumerConfig{}, fmt.Errorf("worker: subscriber %q: handler %q: %w", sb.Name, h.Name, err)
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
			return ConsumerConfig{}, fmt.Errorf("worker: subscriber %q: %w", sb.Name, err)
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

// CleanConfig declares the periodic deletion of old inbox or outbox rows.
type CleanConfig struct {
	// Successfully processed messages older than this are deleted.
	// Zero uses the default (30 days); a negative value disables cleanup.
	MaxTTL time.Duration

	// Maximum number of old processed messages to be deleted per query. Default: 100.
	CleanupLimit int

	// How often old processed messages are deleted. Default: 10 minutes.
	// Only used when MaxTTL > 0.
	CleanupInterval time.Duration
}

// validate rejects the negative values that would fail every deletion or delete in a busy loop.
// A negative MaxTTL disables the cleanup.
func (c CleanConfig) validate() error {
	return notNegative("Clean.", field{"CleanupLimit", int64(c.CleanupLimit)}, field{"CleanupInterval", int64(c.CleanupInterval)})
}

// Sanitize returns a copy of c with defaults applied.
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

// PublisherConfig declares a publisher of the producer.
type PublisherConfig struct {
	// Name routes messages with the same Message.Name to this publisher and is passed to IBroker.NewPublisher.
	// The default publisher is created with IBroker.NewDefaultPublisher; its name only labels its logs.
	Name string
	// Concurrency is the number of broker publishers publishing in parallel. Default: 1.
	Concurrency int
	// Timeout bounds a single publish. Default: 3 seconds.
	Timeout time.Duration
}

// Sanitize returns a copy of c with defaults applied.
func (c PublisherConfig) Sanitize() PublisherConfig {
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.Timeout == 0 {
		c.Timeout = time.Second * 3
	}
	return c
}

// SubscriberConfig declares a subscriber of the consumer and the handlers of the messages it receives.
type SubscriberConfig struct {
	// Name is passed to IBroker.NewSubscriber. The inbox keeps the messages of every subscriber apart by
	// it, so each subscriber handles a message once, whatever the others did with it.
	Name string
	// Concurrency is the number of messages handled in parallel. Default: 1.
	Concurrency int
	// MaxAttempt is the number of attempts before a message is marked failed and rejected. Default: 3.
	MaxAttempt int
	// AckTimeout bounds each acknowledgement sent to the broker: complete, retry or reject. Default: 3
	// seconds.
	AckTimeout time.Duration
	// Middlewares run on every message of the subscriber, inside the base and server middlewares.
	Middlewares []forgo.MiddlewareFactory
	// Handlers handle the messages by Message.Name. A message whose name has no handler is rejected.
	Handlers []HandlerConfig
}

// Sanitize returns a copy of s with defaults applied.
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

// validate rejects the handlers that cannot run or cannot be told apart, and the negative values.
func (s SubscriberConfig) validate() error {
	scope := fmt.Sprintf("subscriber %q: ", s.Name)
	if err := notNegative(scope, field{"Concurrency", int64(s.Concurrency)}, field{"MaxAttempt", int64(s.MaxAttempt)}); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(s.Handlers))
	for i, h := range s.Handlers {
		if h.Name == "" {
			return fmt.Errorf("worker: %shandler %d has no name", scope, i)
		}
		if h.Handler.build == nil {
			return fmt.Errorf("worker: %shandler %q has no Handler", scope, h.Name)
		}
		if _, ok := seen[h.Name]; ok {
			return fmt.Errorf("worker: %sduplicate handler %q", scope, h.Name)
		}
		seen[h.Name] = struct{}{}
		if err := notNegative(scope+fmt.Sprintf("handler %q: ", h.Name), field{"Timeout", int64(h.Timeout)}); err != nil {
			return err
		}
	}
	return nil
}

// HandlerConfig declares the handler of the messages with one name.
type HandlerConfig struct {
	// Name is the Message.Name of the messages the handler receives, and the operation name of their
	// handling, op_name in logs. Names must be unique and non-empty within a subscriber.
	Name string
	// Lock keeps two deliveries of one message from being handled at once, in this process or another,
	// when both Key and TTL are set. Its key is <Key>-<subscriber>-<message ID>. It needs an
	// operation.ILocker in the bootstrap. A delivery whose lock is held is retried. The base chain applies
	// it, so DisableBaseMiddlewares ignores it.
	Lock operation.Lock
	// TransactionDisabled runs the handler without the operation.Transactor. By default, the handler and
	// the inbox's completed mark run in one transaction.
	TransactionDisabled bool
	// Timeout ends the context the handler receives after this long, and the message then fails
	// like on any handler error. Zero means no limit. Go cannot stop a goroutine: a handler that
	// ignores its context keeps running and holds its concurrency slot. The inbox mark and the
	// commit that follow the handler are not bound by Timeout.
	Timeout time.Duration
	// Middlewares run on the handler's messages, inside the base, server and subscriber middlewares.
	Middlewares []forgo.MiddlewareFactory
	// Handler handles the messages. Create it with HandlerFor or HandlerAs. Required.
	Handler HandlerFactory
	// Args are static values in the Args of every message of the handler, such as
	// ArgKeyRecorderDisabled.
	Args map[any]any
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

// HandlerFor handles messages with a method of a service the bootstrap provides, such as
// HandlerFor((*billing.Service).OnOrderPlaced). The receiver S is resolved once, in New, and can be an
// interface. Req is bound and validated from the payload.
func HandlerFor[S, Req any](fn func(S, context.Context, Req) error) HandlerFactory {
	return HandlerFactory{
		build: func(r forgo.Resolver) (handlerWrapper, error) {
			service, err := r.Resolve[S]()
			if err != nil {
				return nil, err
			}

			return newHandlerWrapper(func(ctx context.Context, req Req) error {
				return fn(service, ctx, req)
			}), nil
		},
	}
}

// HandlerAs handles messages with the Handler that constructor returns. The constructor's parameters are
// resolved from the bootstrap, once, in New.
func HandlerAs[Req any](constructor any) HandlerFactory {
	return HandlerFactory{
		build: func(r forgo.Resolver) (handlerWrapper, error) {
			fn, err := r.Call[Handler[Req]](constructor)
			if err != nil {
				return nil, err
			}

			return newHandlerWrapper(fn), nil
		},
	}
}

// HandlerFactory builds the handler of a HandlerConfig when the server is created. Create one with
// HandlerFor or HandlerAs.
type HandlerFactory struct {
	build func(forgo.Resolver) (handlerWrapper, error)
}

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

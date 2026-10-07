package worker

import (
	"context"
	"errors"
	"fmt"
	"forgo"
	"forgo/operation"
	"time"
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

	return &ProducerServer{cfg: c.Sanitize(), publishers: make(map[string]*publisher), broker: broker, storage: storage}, nil
}

type ConsumerConfig struct {
	forgo.ServerConfig
	Clean               CleanConfig
	Binder              IBinder
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
	if err := cfg.validateDependencies(locker, transactor); err != nil {
		return nil, err
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

	return &ConsumerServer{cfg: cfg, subscribers: make([]*subscriber, 0), locker: locker, broker: broker, storage: storage, transactor: transactor}, nil
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
				Middlewares:         middlewares,
				Handler:             handler,
				Args:                h.Args,
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
	Middlewares         []forgo.MiddlewareFactory
	Handler             HandlerFactory
	Args                map[any]any
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

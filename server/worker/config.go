package worker

import (
	"context"
	"forgo"
	"forgo/operation"
	"time"
)

type ProducerConfig struct {
	forgo.ServerConfig

	// Maximum number of messages claimed per query.
	Limit int

	// Wait when there is no full backlog to process.
	Interval time.Duration

	// Maximum publish error percentage. Triggers OnErrorPercentage if reached.
	ErrorPercentage int

	// Triggers when publish error percentage >= ErrorPercentage
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
	Clean       CleanConfig
	Binder      IBinder
	Subscribers []SubscriberConfig
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
	if err != nil {
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
	if err != nil {
		return nil, err
	}

	cfg := c.Sanitize()
	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc

	return &ConsumerServer{cfg: cfg, subscribers: make([]*subscriber, 0), locker: locker, broker: broker, storage: storage, transactor: transactor}, nil
}

type CleanConfig struct {
	// Successfully processed messages older than this are deleted.
	// Zero disables cleanup.
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
	Name        string
	Concurrency int
	Timeout     time.Duration
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
	Name        string
	Concurrency int
	MaxAttempt  int
	AckTimeout  time.Duration
	Middlewares []operation.Middleware
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
	Middlewares         []operation.Middleware
	Handler             Handler
	Args                map[string]any
}

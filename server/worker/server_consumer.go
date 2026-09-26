package worker

import (
	"context"
	"errors"
	"fmt"
	"forgo"
	"forgo/logger"
	"forgo/middleware"
	"forgo/operation"
	"forgo/process"
	"forgo/types"
	"sync"
	"time"

	"github.com/rs/xid"
)

const ServerNameConsumer = "worker_consumer"

type ConsumerServer struct {
	*forgo.Server
	broker            IBroker
	storage           InboxStorage
	transactor        operation.Transactor
	clean             CleanConfig
	subscriberConfigs []SubscriberConfig
	subscribers       []*subscriber
	binder            IBinder
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewConsumerServer(opts ...ConsumerServerOption) *ConsumerServer {
	ws := &ConsumerServer{Server: &forgo.Server{}, clean: defaultCleanConfig(), subscriberConfigs: make([]SubscriberConfig, 0), subscribers: make([]*subscriber, 0)}
	for _, opt := range opts {
		opt(ws)
	}
	if ws.Server.Tt == 0 {
		ws.Server.Tt = forgo.DefaultServerTimeout
	}
	if ws.binder == nil {
		ws.binder = newDefaultBinder()
	}

	return ws
}

func (s *ConsumerServer) Name() string {
	return ServerNameConsumer
}

func (s *ConsumerServer) Init(ctx context.Context) error {
	if s.broker == nil {
		return errors.New("worker_consumer: broker is not set")
	}
	if s.storage == nil {
		return errors.New("worker_consumer: storage is not set")
	}
	if err := s.broker.Init(ctx); err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	for _, sc := range s.subscriberConfigs {
		ls, err := s.createSubscriber(sc)
		if err != nil {
			return err
		}
		s.subscribers = append(s.subscribers, ls)
	}

	return nil
}

func (s *ConsumerServer) Serve() error {
	lg := logger.FromCtx(s.ctx)
	s.wg.Go(func() {
		if err := s.cleanup(s.ctx); err != nil {
			lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("cleanup failed")
		}
	})
	for _, sb := range s.subscribers {
		s.wg.Go(func() {
			sb.Listen()
			sb.Close()
		})
	}

	<-s.ctx.Done()
	return nil
}

func (s *ConsumerServer) Terminate(context.Context) error {
	s.cancel()
	s.wg.Wait()
	s.broker.Close()
	return nil
}

func (s *ConsumerServer) Kill() {
	s.broker.Close()
}

func (s *ConsumerServer) createSubscriber(sb SubscriberConfig) (*subscriber, error) {
	id := xid.New().String()
	subs, err := s.broker.NewSubscriber(sb.Name, id)
	if err != nil {
		return nil, err
	}

	lg := *logger.FromCtx(s.ctx)
	lg = lg.With().
		Str(logger.BrokerLogKey, s.broker.Name()).
		Str(logger.SubscriberLogKey, sb.Name).
		Str(logger.SubscriberIDLogKey, id).
		Logger()
	loggerCtx := logger.NewCtx(s.ctx, lg)

	handlers := map[string]HandlerFunc{}
	for _, h := range sb.Handlers {
		handlers[h.Name] = s.createHandler(h, sb)
	}
	nfHandler := s.createHandler(HandlerConfig{Handler: NotFoundHandler}, sb)

	handler := func(ctx context.Context, d IDelivery) {
		msg := d.Message()
		h, ok := handlers[msg.Name]
		if !ok {
			nfHandler(ctx, d)
			return
		}
		h(ctx, d)
	}

	if sb.Concurrency == 0 {
		sb.Concurrency = 1
	}

	return &subscriber{
		logger: lg,
		subs:   subs,
		ctx:    loggerCtx,
		cs: &ConsumerState{
			Handler:     handler,
			Concurrency: sb.Concurrency,
			Wg:          &sync.WaitGroup{},
			Sem:         process.NewChannelSemaphore(sb.Concurrency),
		},
	}, nil
}

func (s *ConsumerServer) createHandler(h HandlerConfig, sb SubscriberConfig) HandlerFunc {
	var middlewares []operation.Middleware
	if !s.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(LoggerFieldAdder()),
			middleware.Locker(s.Locker, LockCrafter(sb.Name, h.Lock), OnLockConflict()),
			middleware.Recorder(RecorderDisabled(), BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit), AfterMessageLogFields()),
			middleware.Recovery(PanicRecovery(s.storage, sb)),
			middleware.Guard(IdempotencyCheck(s.storage, sb.Name), IdempotencyReject()),
		)
	}

	// add server middlewares
	middlewares = append(middlewares, s.Middlewares...)

	// add subscriber middlewares
	middlewares = append(middlewares, sb.Middlewares...)

	// add handler middlewares
	middlewares = append(middlewares, h.Middlewares...)

	// create final handler
	var tx operation.Transactor
	if !h.TransactionDisabled {
		tx = s.transactor
	}

	handler := h.Handler(s.binder, tx, s.storage, sb)
	handler = operation.Chain(handler, middlewares...)

	return func(ctx context.Context, d IDelivery) {
		msg := d.Message()
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    msg.Name,
			Type:    OperationTypeMessageHandler,
			Args:    types.NewMapWithData(h.Args),
			State: &State{
				Delivery: d,
			},
		}

		handler(opCtx)
	}
}

func (s *ConsumerServer) cleanup(ctx context.Context) error {
	if s.clean.MaxTTL <= 0 {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			before := time.Now().Add(-s.clean.MaxTTL)

			for {
				deleted, err := s.storage.DeleteBefore(
					ctx,
					before,
					s.clean.CleanupLimit,
				)
				if err != nil {
					return fmt.Errorf("outbox: cleanup: %w", err)
				}

				if deleted < int64(s.clean.CleanupLimit) {
					break
				}

				// Keep deleting immediately while full cleanup batches exist.
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(s.clean.CleanupInterval):
			}
		}
	}
}

type ConsumerServerOption func(*ConsumerServer)

func WithConsumerBaseOption(opt forgo.ServerOption) ConsumerServerOption {
	return func(s *ConsumerServer) {
		opt(s.Server)
	}
}

func WithConsumerBroker(broker IBroker) ConsumerServerOption {
	return func(s *ConsumerServer) {
		s.broker = broker
	}
}

func WithSubscribers(subscribers ...SubscriberConfig) ConsumerServerOption {
	return func(s *ConsumerServer) {
		s.subscriberConfigs = append(s.subscriberConfigs, subscribers...)
	}
}

func WithBinder(binder IBinder) ConsumerServerOption {
	return func(s *ConsumerServer) {
		s.binder = binder
	}
}

func WithInboxStorage(st InboxStorage) ConsumerServerOption {
	return func(s *ConsumerServer) {
		s.storage = st
	}
}

func WithTransactor(tx operation.Transactor) ConsumerServerOption {
	return func(s *ConsumerServer) {
		s.transactor = tx
	}
}

func WithCleanConfig(cc CleanConfig) ConsumerServerOption {
	return func(s *ConsumerServer) {
		s.clean = cc
	}
}

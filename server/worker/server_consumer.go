package worker

import (
	"context"
	"sync"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/xid"
)

const ServerNameConsumer = "worker_consumer"

type ConsumerServer struct {
	cfg         ConsumerConfig
	subscribers []*subscriber

	locker     operation.ILocker
	broker     IBroker
	storage    InboxStorage
	transactor operation.Transactor

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *ConsumerServer) Term() forgo.TermConfig {
	return s.cfg.Term
}

func (s *ConsumerServer) Name() string {
	return ServerNameConsumer
}

func (s *ConsumerServer) Init(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	for _, sc := range s.cfg.resolvedSubscribers {
		ls, err := s.createSubscriber(sc)
		if err != nil {
			return err
		}
		s.subscribers = append(s.subscribers, ls)
	}

	return nil
}

// Serve returns the error of the first subscriber that stops consuming on its own, for example
// because its broker gave up reconnecting. That stops the app, so that an orchestrator restarts it
// instead of leaving it up without consuming.
func (s *ConsumerServer) Serve() error {
	s.wg.Go(func() {
		cleanup(s.ctx, s.cfg.Clean, s.storage.DeleteBefore)
	})
	failed := make(chan error, len(s.subscribers))
	for _, sb := range s.subscribers {
		s.wg.Go(func() {
			if err := sb.Listen(); err != nil {
				failed <- err
			}
			sb.Close()
		})
	}

	select {
	case <-s.ctx.Done():
		return nil
	case err := <-failed:
		return err
	}
}

func (s *ConsumerServer) Terminate(context.Context) error {
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *ConsumerServer) Kill() {
}

func (s *ConsumerServer) createSubscriber(sb resolvedSubscriber) (*subscriber, error) {
	id := xid.New().String()
	subs, err := s.broker.NewSubscriber(sb.Name)
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

	handlers := map[string]handlerFunc{}
	for _, h := range sb.Handlers {
		handlers[h.Name] = s.createHandler(h, sb)
	}
	nfHandler := s.createHandler(resolvedHandler{Handler: NotFoundHandler}, sb)

	handler := func(ctx context.Context, d IDelivery) {
		msg := d.Message()
		h, ok := handlers[msg.Name]
		if !ok {
			nfHandler(ctx, d)
			return
		}
		h(ctx, d)
	}

	return &subscriber{
		name:   sb.Name,
		logger: lg,
		subs:   subs,
		ctx:    loggerCtx,
		cs: ConsumerState{
			Handler:     handler,
			Concurrency: sb.Concurrency,
		},
	}, nil
}

func (s *ConsumerServer) createHandler(h resolvedHandler, sb resolvedSubscriber) handlerFunc {
	var middlewares []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(LoggerFieldAdder()),
			middleware.Locker(s.locker, LockCrafter(sb.Name, h.Lock), OnLockConflict()),
			middleware.Recorder(
				RecorderDisabled(),
				BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit, s.cfg.SensitiveFields),
				AfterMessageLogFields(),
			),
			middleware.Recovery(PanicRecovery(s.storage, sb)),
			middleware.Guard(IdempotencyCheck(s.storage, sb.Name), IdempotencyReject(sb.AckTimeout)),
		)
	}

	// add server middlewares
	middlewares = append(middlewares, s.cfg.ResolvedMiddlewares()...)

	// add subscriber middlewares
	middlewares = append(middlewares, sb.Middlewares...)

	// add handler middlewares
	middlewares = append(middlewares, h.Middlewares...)

	// create final handler
	var tx operation.Transactor
	if !h.TransactionDisabled {
		tx = s.transactor
	}

	handler := h.Handler(s.cfg.Binder, tx, s.storage, sb, h.Timeout)
	handler = operation.Chain(handler, middlewares...)

	return func(ctx context.Context, d IDelivery) {
		msg := d.Message()
		st := newState(d)
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    msg.Name,
			Type:    OperationTypeMessageHandler,
			Args:    types.NewMapCopyOnWrite(h.Args),
			State:   st,
		}

		// Settled outside the chain, so that it also covers the middlewares outside Recorder and
		// servers with DisableBaseMiddlewares.
		err := handler(opCtx)
		if !st.delivery.settled.Load() {
			settleUnsettled(ctx, opCtx, st.Delivery, sb, err)
		}
	}
}

package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

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
			middleware.Locker(s.locker, LockCrafter(sb.Name, h.Lock), OnLockConflict(sb.AckTimeout)),
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

	handler := h.Handler(s.cfg.Binder, tx, s.storage, sb)
	handler = operation.Chain(handler, middlewares...)

	return func(ctx context.Context, d IDelivery) {
		msg := d.Message()
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    msg.Name,
			Type:    OperationTypeMessageHandler,
			Args:    types.NewMapCopyOnWrite(h.Args),
			State: &State{
				Delivery: d,
			},
		}

		handler(opCtx)
	}
}

func (s *ConsumerServer) cleanup(ctx context.Context) error {
	if s.cfg.Clean.MaxTTL <= 0 {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			before := time.Now().Add(-s.cfg.Clean.MaxTTL)

			for {
				deleted, err := s.storage.DeleteBefore(
					ctx,
					before,
					s.cfg.Clean.CleanupLimit,
				)
				if err != nil {
					return fmt.Errorf("outbox: cleanup: %w", err)
				}

				if deleted < int64(s.cfg.Clean.CleanupLimit) {
					break
				}

				// Keep deleting immediately while full cleanup batches exist.
				select {
				case <-ctx.Done():
					return nil
				default:
				}
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(s.cfg.Clean.CleanupInterval):
			}
		}
	}
}

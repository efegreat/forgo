package worker

import (
	"context"
	"fmt"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/inprocess"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/xid"
)

// ServerNameConsumer is the default name of the consumer, the server_name of its logs.
const ServerNameConsumer = "worker_consumer"

// consumerServer consumes with the subscribers of a ConsumerConfig. ConsumerConfig.New creates it.
type consumerServer struct {
	lifecycle

	cfg         ConsumerConfig
	subscribers []*subscriber

	locker   operation.ILocker
	observer operation.IObserver
	// deliveries is observer when it implements IDeliveryObserver, nil otherwise.
	deliveries IDeliveryObserver
	broker     IBroker
	storage    InboxStorage
	transactor operation.Transactor
}

// Term returns the termination settings of the ConsumerConfig.
func (s *consumerServer) Term() forgo.TermConfig {
	return s.cfg.Term
}

// Name returns the name of the config.
func (s *consumerServer) Name() string {
	return s.cfg.Name()
}

// Init creates the subscribers at the broker and builds the chain of every handler, once.
func (s *consumerServer) Init(ctx context.Context) error {
	s.setup(ctx)
	for _, sc := range s.cfg.resolvedSubscribers {
		ls, err := s.createSubscriber(sc)
		if err != nil {
			return err
		}
		s.subscribers = append(s.subscribers, ls)
	}

	return nil
}

// Serve starts consuming with every subscriber, and the inbox cleanup, and blocks until Terminate. It
// returns the error of the first subscriber that stops consuming on its own, for example because its
// broker gave up reconnecting. That stops the app, so that an orchestrator restarts it instead of leaving
// it up without consuming. After Terminate, it closes the subscribers and returns at once.
func (s *consumerServer) Serve() error {
	failed := make(chan error, len(s.subscribers))
	fns := []func(){func() {
		cleanup(s.ctx, s.cfg.Clean, s.storage.DeleteBefore)
	}}
	for _, sb := range s.subscribers {
		fns = append(fns, func() {
			if err := sb.Listen(); err != nil {
				failed <- err
			}
			sb.Close()
		})
	}
	if !s.start(fns...) {
		for _, sb := range s.subscribers {
			sb.Close()
		}
		return nil
	}

	select {
	case <-s.ctx.Done():
		return nil
	case err := <-failed:
		return err
	}
}

// Terminate stops consuming and waits for the messages in flight to be handled. When ctx ends first, it
// returns ctx's error and leaves them running, for App to Kill the server.
func (s *consumerServer) Terminate(ctx context.Context) error {
	if err := s.terminate(ctx); err != nil {
		return fmt.Errorf("worker: waiting for the messages in flight: %w", err)
	}
	return nil
}

// Kill cancels the context of the message handlers in flight, which ends every handler that honors it.
// App calls it when Terminate failed or timed out. It does not wait for the handlers.
func (s *consumerServer) Kill() {
	s.kill()
}

func (s *consumerServer) createSubscriber(sb resolvedSubscriber) (*subscriber, error) {
	id := xid.New().String()
	subs, err := s.broker.NewSubscriber(sb.Name, SubscriberOptions{Concurrency: sb.Concurrency})
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

	handlers := make(map[string]handleFunc, len(sb.Handlers))
	for _, h := range sb.Handlers {
		handlers[h.Name] = s.createHandler(h, sb)
	}
	nfHandler := s.createHandler(resolvedHandler{Name: notFoundName, Handler: notFoundHandler}, sb)

	dispatch := func(ctx context.Context, d IDelivery, work context.Context) error {
		msg := d.Message()
		h, ok := handlers[msg.Name]
		if !ok {
			return nfHandler(ctx, d, work)
		}
		return h(ctx, d, work)
	}

	return &subscriber{
		name:     sb.Name,
		logger:   lg,
		subs:     subs,
		ctx:      loggerCtx,
		dispatch: dispatch,
		handle: func(ctx context.Context, d IDelivery) {
			_ = dispatch(ctx, d, s.work)
		},
	}, nil
}

// Subscribers returns a function per subscriber, by name, that handles a delivery as the subscriber does when
// the broker delivers it: through the chain of the handler of the message's name, then the settlement of a
// delivery the chain left unsettled. It returns the error of the chain. forgotest calls it after Init. The
// handling ends with ctx, or when the server is killed, and looks a value up in ctx, then in the
// subscriber's context, such as its logger.
func (s *consumerServer) Subscribers() map[string]func(ctx context.Context, d IDelivery) error {
	subs := make(map[string]func(context.Context, IDelivery) error, len(s.subscribers))
	for _, sb := range s.subscribers {
		subs[sb.name] = func(ctx context.Context, d IDelivery) error {
			work, cancel := context.WithCancel(ctx)
			defer cancel()
			stop := context.AfterFunc(s.work, cancel)
			defer stop()
			return sb.dispatch(inprocess.Context(ctx, sb.ctx), d, work)
		}
	}
	return subs
}

// handleFunc handles a delivery to its end, and returns the error of the chain. Only the end of work cancels
// the handling, not the end of ctx.
type handleFunc func(ctx context.Context, d IDelivery, work context.Context) error

// handledMessage holds the values the consumer allocates for each message, in one allocation: its
// operation, the operation's State and its Args.
type handledMessage struct {
	op   operation.Context
	st   State
	args types.Map[any, any]
}

func (s *consumerServer) createHandler(h resolvedHandler, sb resolvedSubscriber) handleFunc {
	var middlewares []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(LoggerFieldAdder()),
			middleware.Locker(s.locker, lockCrafter(sb.Name, h.Lock), onLockConflict),
			middleware.Recorder(
				recorderDisabled,
				BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit, s.cfg.SensitiveFields),
				AfterMessageLogFields(),
			),
			middleware.Observe(s.observer),
			middleware.Recovery(recoverPanic(s.storage, sb)),
			idempotency(s.storage, sb.Name, sb.AckTimeout),
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

	return func(ctx context.Context, d IDelivery, work context.Context) error {
		// A received message is handled to its end: the end of ctx, when the subscriber stops consuming,
		// does not cancel its handling. Only the end of work does, which Kill cancels.
		ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		stop := context.AfterFunc(work, cancel)
		defer stop()

		hm := new(handledMessage)
		st := &hm.st
		st.setDelivery(d)
		st.Subscriber = sb.Name
		hm.args.ResetCopyOnWrite(h.Args)
		hm.op = operation.Context{
			Context: ctx,
			ID:      "",
			Name:    h.Name, // the message's name, or notFoundName
			Type:    OperationTypeMessageHandler,
			Args:    &hm.args,
			State:   st,
		}
		opCtx := &hm.op
		if s.deliveries != nil {
			st.delivery.observer, st.delivery.op, st.delivery.st = s.deliveries, opCtx, st
		}

		// Settled outside the chain, so that it also covers the middlewares outside Recorder and
		// servers with DisableBaseMiddlewares.
		err := handler(opCtx)
		if !st.delivery.settled.Load() {
			settleUnsettled(opCtx, st, sb, err)
		}
		return err
	}
}

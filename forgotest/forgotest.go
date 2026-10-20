package forgotest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

// defaultTermTimeout is the TermTimeout of servers whose Term returns zero or less, as in forgo.App.
const defaultTermTimeout = 30 * time.Second

var (
	// ErrNotFound is returned when nothing has the name looked up: no server for Env.Addr and Env.Handler,
	// no job for Env.RunJob, no subscriber for Env.Deliver, and no resource factory of the type for
	// ReplaceResource.
	ErrNotFound = errors.New("forgotest: not found")
	// ErrAmbiguous is returned when more than one has the name looked up, such as two HTTP servers for
	// Env.Addr, or the type for ReplaceResource.
	ErrAmbiguous = errors.New("forgotest: ambiguous")
	// ErrClaimedTwice is returned by Env.Outbox when the app's worker.OutboxStorage returns a message that an
	// earlier claim of the call already took, although a claim must skip the messages another claim holds.
	ErrClaimedTwice = errors.New("forgotest: outbox: a claim returned a message already claimed")
)

// IListenerServer is a server that serves on a listener it is given, such as the HTTP and gRPC servers. An
// Env serves it on a loopback listener of its own, whatever address its configuration names, and tests
// reach it through Env.Addr. Implement it on a custom server to have it served in tests too.
type IListenerServer interface {
	forgo.IServer
	// ServeListener serves on ln as Serve does on a listener of its own: until Terminate or Kill, then it
	// returns nil. It closes ln.
	ServeListener(ln net.Listener) error
}

// IHandlerServer is a server whose router can serve requests in process, such as the HTTP server. Env.Handler
// returns it, so that tests choose, request by request, between the loopback listener of Env.Addr and no
// network at all. Implement it on a custom HTTP server to offer the same choice.
type IHandlerServer interface {
	forgo.IServer
	// Handler returns the router of the server, with the chain of every route. The env calls it once, after
	// Init. A request it serves gets the values of the server's context, such as its logger.
	Handler() http.Handler
}

// IJobServer is a server of jobs that run on their own, such as the cron server. An env never serves it, so
// nothing runs on a schedule, and Env.RunJob runs one of its jobs once instead.
type IJobServer interface {
	forgo.IServer
	// Jobs returns a function per job, by name, that runs the job once through its chain, as its schedule
	// would, and returns the error of the chain. The env calls it once, after Init.
	Jobs() map[string]func(ctx context.Context) error
}

// IConsumerServer is a server that handles the messages a broker delivers, such as the worker's consumer. An
// env never serves it, so nothing is consumed, and Env.Deliver hands a message to one of its subscribers
// instead.
type IConsumerServer interface {
	forgo.IServer
	// Subscribers returns a function per subscriber, by name, that handles a delivery as the subscriber
	// would, settles it, and returns the error of the chain. The env calls it once, after Init.
	Subscribers() map[string]func(ctx context.Context, d worker.IDelivery) error
}

// Env runs the servers of an app for the tests of a package. Create it with New and tear it down with
// Close. Unlike forgo.App.Run, it catches no signal, serves only the IListenerServer servers, on loopback,
// and does not sleep for TermSig.Wait when it terminates them. Tests reach a server through the network with
// Addr, or in process with Handler. It is safe for concurrent use.
type Env struct {
	boot *forgo.Bootstrap
	// served are the servers the env serves, in the order of the app's servers.
	served []*servedServer
	// handlers are the routers of the IHandlerServer servers, in the order of the app's servers.
	handlers []named[http.Handler]
	// jobs are the runs of the jobs of the IJobServer servers.
	jobs []named[func(context.Context) error]
	// subscribers handle the deliveries of the subscribers of the IConsumerServer servers.
	subscribers []named[func(context.Context, worker.IDelivery) error]
	// outbox is the app's worker.OutboxStorage, for Outbox, or nil, and outboxErr the error of resolving it.
	outbox    worker.OutboxStorage
	outboxErr error

	closeOnce sync.Once
}

// servedServer is a server the env serves on a loopback listener.
type servedServer struct {
	IListenerServer
	lg   zerolog.Logger
	addr string
	// served is closed when ServeListener returned, and err holds its error.
	served chan struct{}
	err    error
}

// Option configures an Env.
type Option func(*options)

type options struct {
	logger    *logger.Config
	resources []resourceReplacement
}

// WithLogger sets the configuration of the env's logger, for example to raise its level or to read the logs
// from a buffer through Output. Without it, the env logs like the app, but to os.Stderr and to no file, so
// tests write no log files, and go test shows the logs of a package that passes only with -v.
func WithLogger(cfg logger.Config) Option {
	return func(o *options) {
		o.logger = &cfg
	}
}

// ReplaceResource puts factory in place of the app's resource factory whose type is F, before anything is
// built, so the replaced factory never runs and nothing connects: for example the stand-in Broker in place of
// the configuration of a broker resource, since tests run no broker. New fails with ErrNotFound when no
// resource factory of the app has type F, and with ErrAmbiguous when more than one has. A provider that asks
// for the replaced resource's own type, rather than an interface it implements, such as worker.IBroker, then
// fails in New with forgo.ErrMissingDependency.
func ReplaceResource[F forgo.ResourceFactory](factory forgo.ResourceFactory) Option {
	return func(o *options) {
		o.resources = append(o.resources, resourceReplacement{typ: reflect.TypeFor[F](), factory: factory})
	}
}

// resourceReplacement puts factory in place of the resource factory of type typ.
type resourceReplacement struct {
	typ     reflect.Type
	factory forgo.ResourceFactory
}

// apply replaces the one factory of resources whose type is r.typ.
func (r resourceReplacement) apply(resources []forgo.ResourceFactory) error {
	if isNil(r.factory) {
		return fmt.Errorf("forgotest: ReplaceResource[%s]: nil factory", r.typ)
	}
	at := -1
	for i, rf := range resources {
		if reflect.TypeOf(rf) != r.typ {
			continue
		}
		if at >= 0 {
			return fmt.Errorf("%w: more than one resource factory has type %s", ErrAmbiguous, r.typ)
		}
		at = i
	}
	if at < 0 {
		return fmt.Errorf("%w: no resource factory has type %s", ErrNotFound, r.typ)
	}
	resources[at] = r.factory
	return nil
}

// New builds the bootstrap of app's definition, creates and initializes every server in order with a
// logger that carries app_name, app_id, server_name and server_id, seals the bootstrap, and serves the
// IListenerServer servers on loopback listeners, as forgo.App.Run does before it serves. Before the servers,
// it resolves the app's worker.OutboxStorage, if it has one, for Outbox. A failed New terminates the servers
// it served and closes the resources it created.
//
// Unlike Run, New does not install its logger as the default one (logger.SetDefault), so two envs in one
// test binary do not swap each other's.
func New(app *forgo.App, opts ...Option) (*Env, error) {
	if app == nil {
		return nil, errors.New("forgotest: nil app")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	def := app.Definition()
	if err := validate(def.Servers); err != nil {
		return nil, err
	}
	for _, r := range o.resources {
		if err := r.apply(def.Resources); err != nil {
			return nil, err
		}
	}

	cfg := def.Logger
	cfg.Output = os.Stderr
	cfg.File = logger.FileConfig{}
	if o.logger != nil {
		cfg = *o.logger
	}
	lg := cfg.New().
		With().
		Str(logger.AppNameLogKey, def.Name).
		Str(logger.AppIDLogKey, xid.New().String()).
		Logger()

	boot := forgo.NewBootstrap(def.Resources, def.Providers)
	resolver, err := boot.Build()
	if err != nil {
		return nil, err
	}
	env := &Env{boot: boot}
	// The outbox is resolved before the bootstrap is sealed, like the optional dependencies of servers. Only
	// Outbox needs it, so an app that has none, or two, fails Outbox rather than New: Run starts it all the
	// same.
	if env.outbox, err = resolver.Resolve[worker.OutboxStorage](); err != nil {
		env.outboxErr = fmt.Errorf("forgotest: outbox: %w", err)
	}
	if err := env.start(lg, resolver, def.Servers); err != nil {
		return nil, errors.Join(err, env.Close())
	}
	return env, nil
}

// validate checks the server list before anything is bootstrapped, as forgo.App.Run does.
func validate(servers []forgo.ServerFactory) error {
	if len(servers) == 0 {
		return errors.New("forgotest: no servers")
	}
	for i, sf := range servers {
		if isNil(sf) {
			return fmt.Errorf("forgotest: server %d is nil", i)
		}
	}
	return nil
}

// start creates and initializes every server, seals the bootstrap, then serves the IListenerServer servers
// on loopback listeners.
func (e *Env) start(lg zerolog.Logger, r forgo.Resolver, factories []forgo.ServerFactory) error {
	var listeners []*servedServer
	for _, sf := range factories {
		s, err := sf.New(r)
		if err != nil {
			return fmt.Errorf("forgotest: server %q: new: %w", sf.Name(), err)
		}
		if isNil(s) {
			return fmt.Errorf("forgotest: server %q: New returned nil", sf.Name())
		}

		sLg := lg.With().
			Str(logger.ServerIDLogKey, xid.New().String()).
			Str(logger.ServerNameLogKey, s.Name()).
			Logger()
		if err := s.Init(logger.NewCtx(context.Background(), sLg)); err != nil {
			return fmt.Errorf("forgotest: server %q: init: %w", s.Name(), err)
		}
		if hs, ok := s.(IHandlerServer); ok {
			e.handlers = append(e.handlers, named[http.Handler]{name: s.Name(), value: hs.Handler()})
		}
		if js, ok := s.(IJobServer); ok {
			for name, run := range js.Jobs() {
				e.jobs = append(e.jobs, named[func(context.Context) error]{name: name, value: run})
			}
		}
		if cs, ok := s.(IConsumerServer); ok {
			for name, handle := range cs.Subscribers() {
				e.subscribers = append(e.subscribers, named[func(context.Context, worker.IDelivery) error]{name: name, value: handle})
			}
		}
		if ls, ok := s.(IListenerServer); ok {
			listeners = append(listeners, &servedServer{IListenerServer: ls, lg: sLg, served: make(chan struct{})})
		}
	}

	if err := e.boot.Seal(); err != nil {
		return err
	}

	for _, s := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("forgotest: server %q: listen: %w", s.Name(), err)
		}
		s.addr = ln.Addr().String()
		e.served = append(e.served, s)
		go s.serve(ln)
	}
	return nil
}

// Addr returns the host:port of the loopback listener the env serves the server named name on, such as
// http.ServerNameHTTP or grpc.ServerNameGRPC, for tests that go through the network. It fails with
// ErrNotFound when the env serves no server of that name, as for servers that do not implement
// IListenerServer, and with ErrAmbiguous when it serves more than one.
func (e *Env) Addr(name string) (string, error) {
	addrs := make([]named[string], len(e.served))
	for i, s := range e.served {
		addrs[i] = named[string]{name: s.Name(), value: s.addr}
	}
	return lookup("served servers", name, addrs)
}

// Handler returns the router of the server named name, such as http.ServerNameHTTP, for tests that serve
// requests in process, without the network, such as with httptest.NewRecorder. A request runs the chain of
// its route, as over the listener of Addr, and the env serves both at once, so tests choose either. But
// net/http's server does not run: none of its timeouts and limits apply, and a handler can hijack the
// connection only when the ResponseWriter supports it. Handler fails with ErrNotFound when no server of that
// name implements IHandlerServer, as for the gRPC server, and with ErrAmbiguous when more than one does.
func (e *Env) Handler(name string) (http.Handler, error) {
	return lookup("servers with a Handler", name, e.handlers)
}

// RunJob runs the job named name once, through its chain, as its schedule would, and returns the error of
// the chain: what the job returned once the retries of its Tolerance ran out, or the *middleware.PanicError
// of a panic. A run whose Lock another holder has is skipped, and returns nil. The run ends with ctx, and gets
// the values of ctx, then those of the server's context, such as its logger. RunJob fails with ErrNotFound
// when no IJobServer server has a job of that name, and with ErrAmbiguous when more than one has.
func (e *Env) RunJob(ctx context.Context, name string) error {
	run, err := lookup("jobs", name, e.jobs)
	if err != nil {
		return err
	}
	return run(ctx)
}

// Settlement is how the consumer settled a delivery.
type Settlement int

const (
	// Unsettled is a delivery nothing settled. The consumer settles every delivery, so only a custom
	// IConsumerServer can leave one unsettled.
	Unsettled Settlement = iota
	// Completed is a delivery acknowledged as handled, with worker.IDelivery.Complete.
	Completed
	// Retried is a delivery handed back to the broker for redelivery, with worker.IDelivery.Retry.
	Retried
	// Rejected is a delivery the broker does not redeliver, with worker.IDelivery.Reject.
	Rejected
)

// String returns the settlement's name in lower case, such as "completed".
func (s Settlement) String() string {
	switch s {
	case Completed:
		return "completed"
	case Retried:
		return "retried"
	case Rejected:
		return "rejected"
	default:
		return "unsettled"
	}
}

// Delivery is what became of a message Env.Deliver handed to a subscriber.
type Delivery struct {
	// Settlement is how the consumer settled the delivery.
	Settlement Settlement
	// Attempt is the attempt the consumer passed to Retry: the one that failed, as the inbox counted it, or 0
	// when it failed before the inbox counted it. It is 0 unless Settlement is Retried.
	Attempt int
	// Err is the error the chain of the handling returned, such as the handler's, or the nf_mh error of a
	// message whose name has no handler. It is nil when the handling succeeded, including when
	// the inbox already marked the message completed and the handler did not run.
	Err error
}

// Deliver hands msg to the subscriber named subscriber, as the broker would, and returns how the consumer
// settled it once the handling ended: the handler of the message's name runs through its chain, with the
// inbox, the transaction and the retry rules of production, and with the application's own InboxStorage, so
// a message delivered twice is handled once. msg needs the ID of worker.NewMessage, which the inbox keeps
// messages apart by.
//
// The handling ends with ctx, and gets the values of ctx, then those of the subscriber's context, such as
// its logger. Deliver fails with ErrNotFound when no IConsumerServer server has a subscriber of that name,
// and with ErrAmbiguous when more than one has. The error of the handling itself is Delivery.Err.
func (e *Env) Deliver(ctx context.Context, subscriber string, msg worker.Message) (Delivery, error) {
	if msg.ID == "" {
		return Delivery{}, errors.New("forgotest: the message has no ID: create it with worker.NewMessage")
	}
	handle, err := lookup("subscribers", subscriber, e.subscribers)
	if err != nil {
		return Delivery{}, err
	}
	d := &delivery{msg: msg}
	err = handle(ctx, d)
	d.mu.Lock()
	defer d.mu.Unlock()
	return Delivery{Settlement: d.settlement, Attempt: d.attempt, Err: err}, nil
}

// delivery is the worker.IDelivery that Deliver hands to the consumer. It records the first settlement,
// which is the one the consumer passes on to a broker.
type delivery struct {
	msg worker.Message

	mu         sync.Mutex
	settlement Settlement
	attempt    int
}

func (d *delivery) Message() worker.Message {
	return d.msg
}

func (d *delivery) Complete(context.Context) error {
	d.settle(Completed, 0)
	return nil
}

func (d *delivery) Retry(_ context.Context, attempt int) error {
	d.settle(Retried, attempt)
	return nil
}

func (d *delivery) Reject(context.Context) error {
	d.settle(Rejected, 0)
	return nil
}

func (d *delivery) settle(s Settlement, attempt int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settlement == Unsettled {
		d.settlement, d.attempt = s, attempt
	}
}

// outboxBatch is the Limit of the claims of Outbox, the default ProducerConfig.Limit.
const outboxBatch = 100

// Outbox returns the messages saved to the app's worker.OutboxStorage since the previous call, in claim
// order, as the producer's relay would publish them: it reads them through OutboxStorage.Claim, as the relay
// does, so a test sees what the relay would get, such as for Deliver. The claims have a zero ExpiredBefore,
// so they take only the messages no claim holds, and the messages stay claimed and are never published:
// the next call returns only the messages saved after this one. An env never serves the producer, so nothing
// else claims them.
//
// It fails when the bootstrap has no OutboxStorage or more than one, with forgo.ErrMissingDependency or
// forgo.ErrAmbiguousDependency, with the error of a failed Claim, and with ErrClaimedTwice when the storage
// returns a message twice, which would make the relay publish it twice.
func (e *Env) Outbox(ctx context.Context) ([]worker.Message, error) {
	if e.outboxErr != nil {
		return nil, e.outboxErr
	}
	claim := worker.Claim{ID: "forgotest-" + xid.New().String(), Limit: outboxBatch}
	var msgs []worker.Message
	// The claims must skip the messages the earlier ones took: without this check, a storage that does not
	// would be claimed from forever.
	seen := make(map[string]struct{})
	for {
		batch, err := e.outbox.Claim(ctx, claim)
		if err != nil {
			return nil, fmt.Errorf("forgotest: outbox: claim: %w", err)
		}
		for _, m := range batch {
			if _, ok := seen[m.ID]; ok {
				return nil, fmt.Errorf("%w: message %q", ErrClaimedTwice, m.ID)
			}
			seen[m.ID] = struct{}{}
		}
		msgs = append(msgs, batch...)
		if len(batch) < claim.Limit {
			return msgs, nil
		}
	}
}

// named is a value by its name: that of a server, a job or a subscriber.
type named[T any] struct {
	name  string
	value T
}

// lookup returns the value of the one server named name among values, as the bootstrap resolves: one match,
// otherwise ErrNotFound or ErrAmbiguous. what names the servers in errors.
func lookup[T any](what, name string, values []named[T]) (T, error) {
	var found []T
	for _, v := range values {
		if v.name == name {
			found = append(found, v.value)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		var zero T
		return zero, fmt.Errorf("%w: none of the %s is named %q", ErrNotFound, what, name)
	default:
		var zero T
		return zero, fmt.Errorf("%w: %d of the %s are named %q", ErrAmbiguous, len(found), what, name)
	}
}

// Close terminates the served servers in parallel, waits for the goroutines started with process.Go, then
// closes the resources in reverse order, and returns the failures joined: the errors of ServeListener, of
// Terminate, of servers killed after their TermTimeout, and of closing resources. Like App, it signals each
// server's TermSig before Terminate, but it does not wait for TermSig.Wait, since no load balancer drains a
// test's traffic. Later calls return nil.
func (e *Env) Close() error {
	var err error
	e.closeOnce.Do(func() {
		errs := make([]error, len(e.served))
		var wg sync.WaitGroup
		for i, s := range e.served {
			wg.Go(func() {
				errs[i] = s.terminate()
			})
		}
		wg.Wait()
		process.WaitGoroutines()
		err = errors.Join(append(errs, e.boot.Close())...)
	})
	return err
}

// serve serves on ln until the server terminates, and keeps the error of ServeListener for Close.
func (s *servedServer) serve(ln net.Listener) {
	defer close(s.served)
	if err := s.ServeListener(ln); err != nil {
		logFailure(s.lg, err, operation.SeverityCritical, "serve error")
		s.err = fmt.Errorf("forgotest: server %q: serve: %w", s.Name(), err)
	}
}

// terminate signals the server's TermSig, terminates the server, and kills it when Terminate failed, or
// Terminate and ServeListener did not both return within TermTimeout.
func (s *servedServer) terminate() error {
	term := s.Term()
	if ts := term.TermSig; ts != nil {
		ts.Signal()
	}
	timeout := term.TermTimeout
	if timeout <= 0 {
		timeout = defaultTermTimeout
	}

	// the context of Terminate carries the server's logger
	ctx, cancel := context.WithTimeout(logger.NewCtx(context.Background(), s.lg), timeout)
	defer cancel()

	stopped := make(chan error, 1)
	go func() {
		err := s.Terminate(ctx)
		<-s.served
		stopped <- err
	}()

	select {
	case err := <-stopped:
		if err != nil {
			logFailure(s.lg, err, operation.SeverityModerate, "server termination error, killing")
			s.Kill()
			err = fmt.Errorf("forgotest: server %q: terminate: %w", s.Name(), err)
		}
		// ServeListener returned, so its error is set.
		return errors.Join(s.err, err)
	case <-ctx.Done():
		err := fmt.Errorf("forgotest: server %q: terminate: %w", s.Name(), ctx.Err())
		logFailure(s.lg, err, operation.SeverityModerate, "server termination timeout, killing")
		s.Kill()
		return err
	}
}

// logFailure logs a failure outside any operation.
func logFailure(lg zerolog.Logger, err error, severity operation.ErrorSeverity, msg string) {
	lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(severity)).Msg(msg)
}

// isNil reports whether v is nil, or holds a nil pointer, map, slice, channel or function.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

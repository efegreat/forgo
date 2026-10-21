package forgotest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/cron"
	"github.com/efegreat/forgo/server/worker"
)

type ctxKey struct{}

// funcJob is a cron job that runs a function.
type funcJob func(ctx context.Context) error

func (funcJob) Tolerance() operation.Tolerance  { return operation.Tolerance{} }
func (f funcJob) Run(ctx context.Context) error { return f(ctx) }

func jobConfig(name string, run func(ctx context.Context) error) cron.JobConfig {
	return cron.JobConfig{Name: name, Schedule: "@every 1h", Job: cron.As(func() cron.IJob { return funcJob(run) })}
}

func TestRunJob(t *testing.T) {
	errDown := errors.New("down")
	out := &syncBuffer{}
	env := newEnv(t, forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithServers(cron.Config{Jobs: []cron.JobConfig{
			jobConfig("report", func(ctx context.Context) error {
				logger.FromCtx(ctx).Info().Msgf("value %v", ctx.Value(ctxKey{}))
				return errDown
			}),
		}}),
	), out)
	defer env.Close()

	if err := env.RunJob(context.WithValue(context.Background(), ctxKey{}, "test"), "report"); !errors.Is(err, errDown) {
		t.Fatalf("RunJob: %v", err)
	}
	l := out.line(t, "message", "value test")
	if l[logger.AppNameLogKey] != "shop" || l[logger.ServerNameLogKey] != cron.ServerNameCron || l[logger.OperationNameLogKey] != "report" {
		t.Errorf("job line %v", l)
	}
	if err := env.RunJob(context.Background(), "missing"); !errors.Is(err, forgotest.ErrNotFound) {
		t.Errorf("RunJob of a missing job: %v", err)
	}
}

func TestRunJobAmbiguous(t *testing.T) {
	run := func(context.Context) error { return nil }
	env := newEnv(t, forgo.NewApp(forgo.WithServers(
		cron.Config{Jobs: []cron.JobConfig{jobConfig("report", run)}},
		cron.Config{Jobs: []cron.JobConfig{jobConfig("report", run)}},
	)), io.Discard)
	defer env.Close()

	if err := env.RunJob(context.Background(), "report"); !errors.Is(err, forgotest.ErrAmbiguous) {
		t.Fatalf("RunJob of a job of two servers: %v", err)
	}
}

// heldLocker is an operation.ILocker whose locks another holder has while held is set. It counts the locks
// and unlocks of each key.
type heldLocker struct {
	mu       sync.Mutex
	held     bool
	locks    map[string]int
	unlocks  map[string]int
	acquired map[string]bool
}

func newHeldLocker() *heldLocker {
	return &heldLocker{locks: map[string]int{}, unlocks: map[string]int{}, acquired: map[string]bool{}}
}

func (l *heldLocker) hold(held bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = held
}

func (l *heldLocker) counts(key string) (locks, unlocks int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.locks[key], l.unlocks[key]
}

func (l *heldLocker) Lock(_ context.Context, key string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.locks[key]++
	if l.held || l.acquired[key] {
		return false, nil
	}
	l.acquired[key] = true
	return true, nil
}

func (l *heldLocker) WaitLock(ctx context.Context, key string, _ int, _, ttl time.Duration) (bool, error) {
	return l.Lock(ctx, key, ttl)
}

func (l *heldLocker) Unlock(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.acquired[key] {
		return errors.New("not acquired")
	}
	l.unlocks[key]++
	l.acquired[key] = false
	return nil
}

// toleranceJob is a cron job with a Tolerance, which runs a function.
type toleranceJob struct {
	tolerance operation.Tolerance
	run       func(ctx context.Context) error
}

func (j toleranceJob) Tolerance() operation.Tolerance { return j.tolerance }
func (j toleranceJob) Run(ctx context.Context) error  { return j.run(ctx) }

// RunJob runs a job through the chain its schedule runs: a run whose lock another holder has is skipped,
// the retries of the job's Tolerance run under one lock, and the run fails with the last attempt's error
// once they ran out.
func TestRunJobLockAndTolerance(t *testing.T) {
	locker := newHeldLocker()
	var reports, flaky, broken atomic.Int64
	lock := func(key string) operation.Lock { return operation.Lock{Key: key, TTL: time.Minute} }
	retries := operation.Tolerance{Retry: true, RetryAttempts: 3}
	env := newEnv(t, forgo.NewApp(
		forgo.WithProviders(func() operation.ILocker { return locker }),
		forgo.WithServers(cron.Config{Jobs: []cron.JobConfig{
			{Name: "report", Schedule: "@every 1h", Lock: lock("report"), Job: cron.As(func() cron.IJob {
				return funcJob(func(context.Context) error {
					reports.Add(1)
					return nil
				})
			})},
			{Name: "flaky", Schedule: "@every 1h", Lock: lock("flaky"), Job: cron.As(func() cron.IJob {
				return toleranceJob{tolerance: retries, run: func(context.Context) error {
					if flaky.Add(1) < 3 {
						return errDown
					}
					return nil
				}}
			})},
			{Name: "broken", Schedule: "@every 1h", Job: cron.As(func() cron.IJob {
				return toleranceJob{tolerance: retries, run: func(context.Context) error {
					broken.Add(1)
					return fmt.Errorf("attempt %d: %w", broken.Load(), errDown)
				}}
			})},
		}}),
	), io.Discard)
	defer env.Close()
	ctx := context.Background()

	locker.hold(true)
	if err := env.RunJob(ctx, "report"); err != nil || reports.Load() != 0 {
		t.Fatalf("RunJob with the lock held elsewhere: %v, %d runs", err, reports.Load())
	}
	locker.hold(false)
	if err := env.RunJob(ctx, "report"); err != nil || reports.Load() != 1 {
		t.Fatalf("RunJob: %v, %d runs", err, reports.Load())
	}
	if locks, unlocks := locker.counts("report"); locks != 2 || unlocks != 1 {
		t.Fatalf("report: %d locks, %d unlocks", locks, unlocks)
	}

	if err := env.RunJob(ctx, "flaky"); err != nil || flaky.Load() != 3 {
		t.Fatalf("RunJob of a job that succeeds on its third attempt: %v, %d attempts", err, flaky.Load())
	}
	if locks, unlocks := locker.counts("flaky"); locks != 1 || unlocks != 1 {
		t.Fatalf("flaky: %d locks, %d unlocks, want one lock around the three attempts", locks, unlocks)
	}

	err := env.RunJob(ctx, "broken")
	if !errors.Is(err, errDown) || !strings.Contains(err.Error(), "attempt 3") || broken.Load() != 3 {
		t.Fatalf("RunJob of a job that always fails: %v, %d attempts", err, broken.Load())
	}
}

type inboxKey struct{ id, subscriber string }

// memoryInbox is a worker.InboxStorage in memory, with the transitions of the SQL inboxes.
type memoryInbox struct {
	mu       sync.Mutex
	status   map[inboxKey]worker.InboxStatus
	attempts map[inboxKey]int
}

func newMemoryInbox() *memoryInbox {
	return &memoryInbox{status: map[inboxKey]worker.InboxStatus{}, attempts: map[inboxKey]int{}}
}

func (b *memoryInbox) Status(_ context.Context, id, subscriber string) (worker.InboxStatus, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.status[inboxKey{id, subscriber}]
	return st, ok, nil
}

func (b *memoryInbox) IncrementAttempt(_ context.Context, id, subscriber string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := inboxKey{id, subscriber}
	if st, ok := b.status[k]; ok && st != worker.InboxWaiting {
		return 0, errors.New("not waiting")
	}
	b.status[k] = worker.InboxWaiting
	b.attempts[k]++
	return b.attempts[k], nil
}

func (b *memoryInbox) mark(id, subscriber string, st worker.InboxStatus) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := inboxKey{id, subscriber}
	if cur, ok := b.status[k]; ok && cur != worker.InboxWaiting {
		return errors.New("not waiting")
	}
	b.status[k] = st
	return nil
}

func (b *memoryInbox) MarkCompleted(_ context.Context, id, subscriber string) error {
	return b.mark(id, subscriber, worker.InboxCompleted)
}

func (b *memoryInbox) MarkFailed(_ context.Context, id, subscriber string) error {
	return b.mark(id, subscriber, worker.InboxFailed)
}

func (b *memoryInbox) MarkRejected(_ context.Context, id, subscriber string) error {
	return b.mark(id, subscriber, worker.InboxRejected)
}

func (b *memoryInbox) DeleteBefore(context.Context, time.Time, int) (int64, error) { return 0, nil }

// passTransactor runs fn without a transaction.
type passTransactor struct{}

func (passTransactor) Do(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

type orderCreated struct {
	OrderID string `json:"order_id" validate:"required"`
}

var errDown = errors.New("down")

// orders handles order_created: it fails the orders "down" and "gone", the latter for good.
type orders struct {
	mu      sync.Mutex
	handled []string
	value   any // the ctxKey value of the last context it handled
}

func (o *orders) Handle(ctx context.Context, e orderCreated) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handled = append(o.handled, e.OrderID)
	o.value = ctx.Value(ctxKey{})
	switch e.OrderID {
	case "down":
		return errDown
	case "gone":
		return fmt.Errorf("order gone: %w", worker.ErrNoRetry)
	}
	return nil
}

func (o *orders) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.handled)
}

// shippingConsumer is a consumer whose subscriber "shipping" handles order_created with the *orders of the
// bootstrap.
func shippingConsumer() worker.ConsumerConfig {
	return worker.ConsumerConfig{Subscribers: []worker.SubscriberConfig{{
		Name:     "shipping",
		Handlers: []worker.HandlerConfig{{Name: "order_created", Handler: worker.HandlerFor((*orders).Handle)}},
	}}}
}

// consumerProviders are the providers of shippingConsumer, which handles with o.
func consumerProviders(o *orders) []any {
	return []any{
		func() worker.InboxStorage { return newMemoryInbox() },
		func() operation.Transactor { return passTransactor{} },
		func() *orders { return o },
	}
}

// consumerApp is an app with a shippingConsumer that handles with o, with broker as its resource.
func consumerApp(o *orders, broker forgo.ResourceFactory) *forgo.App {
	return forgo.NewApp(
		forgo.WithResources(broker),
		forgo.WithProviders(consumerProviders(o)...),
		forgo.WithServers(shippingConsumer()),
	)
}

func orderMessage(orderID string) worker.Message {
	return worker.NewMessage("order_created", "", "orders", fmt.Appendf(nil, `{"order_id":%q}`, orderID))
}

func TestDeliver(t *testing.T) {
	o := &orders{}
	env := newEnv(t, consumerApp(o, forgotest.Broker("broker")), io.Discard)
	defer env.Close()
	ctx := context.WithValue(context.Background(), ctxKey{}, "test")

	shipped := orderMessage("shipped")
	for _, tc := range []struct {
		name    string
		msg     worker.Message
		want    forgotest.Settlement
		attempt int
		err     func(error) bool
		handled bool // the handler ran
	}{
		{name: "handled", msg: shipped, want: forgotest.Completed, err: isNil, handled: true},
		{name: "handled already", msg: shipped, want: forgotest.Completed, err: isNil},
		{name: "handler error", msg: orderMessage("down"), want: forgotest.Retried, attempt: 1, err: is(errDown), handled: true},
		{name: "no retry", msg: orderMessage("gone"), want: forgotest.Rejected, err: is(worker.ErrNoRetry), handled: true},
		{name: "invalid payload", msg: worker.NewMessage("order_created", "", "orders", []byte(`{}`)), want: forgotest.Rejected, err: notNil},
		{name: "no handler", msg: worker.NewMessage("order_deleted", "", "orders", []byte(`{}`)), want: forgotest.Rejected, err: code("nf_mh")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(o.list())
			got, err := env.Deliver(ctx, "shipping", tc.msg)
			if err != nil {
				t.Fatal(err)
			}
			if got.Settlement != tc.want || got.Attempt != tc.attempt || !tc.err(got.Err) {
				t.Fatalf("delivery %+v (%s), want %s, attempt %d", got, got.Settlement, tc.want, tc.attempt)
			}
			if ran := len(o.list()) > before; ran != tc.handled {
				t.Fatalf("handler ran %t, want %t", ran, tc.handled)
			}
		})
	}
	if o.value != "test" {
		t.Errorf("the handler's context lacks the value of Deliver's: %v", o.value)
	}

	if _, err := env.Deliver(ctx, "missing", shipped); !errors.Is(err, forgotest.ErrNotFound) {
		t.Errorf("Deliver to a missing subscriber: %v", err)
	}
	if _, err := env.Deliver(ctx, "shipping", worker.Message{Name: "order_created"}); err == nil {
		t.Error("Deliver of a message without an ID returned nil")
	}
}

func isNil(err error) bool  { return err == nil }
func notNil(err error) bool { return err != nil }
func is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}
func code(c string) func(error) bool {
	return func(err error) bool { return operation.ErrorOf(err).Code == c }
}

func TestSettlementString(t *testing.T) {
	for s, want := range map[forgotest.Settlement]string{
		forgotest.Unsettled: "unsettled", forgotest.Completed: "completed",
		forgotest.Retried: "retried", forgotest.Rejected: "rejected",
	} {
		if s.String() != want {
			t.Errorf("%d: %q, want %q", s, s.String(), want)
		}
	}
}

// realBroker is the configuration of a broker resource that connects, which tests replace.
type realBroker struct{}

func (realBroker) New() (forgo.Resource, error) { return nil, errors.New("connected to a broker") }

// otherFactory is a resource factory the app does not have.
type otherFactory struct{}

func (otherFactory) New() (forgo.Resource, error) { return nil, errors.New("not used") }

func TestReplaceResource(t *testing.T) {
	o := &orders{}
	env, err := forgotest.New(consumerApp(o, realBroker{}),
		forgotest.WithLogger(logger.Config{Output: io.Discard}),
		forgotest.ReplaceResource[realBroker](forgotest.Broker("broker")))
	if err != nil {
		t.Fatal(err)
	}
	got, err := env.Deliver(context.Background(), "shipping", orderMessage("shipped"))
	if err != nil || got.Settlement != forgotest.Completed {
		t.Fatalf("Deliver through the stand-in broker: %+v, %v", got, err)
	}
	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		app  *forgo.App
		opt  forgotest.Option
		want func(error) bool
	}{
		{
			name: "missing type",
			app:  consumerApp(o, realBroker{}),
			opt:  forgotest.ReplaceResource[otherFactory](forgotest.Broker("broker")),
			want: is(forgotest.ErrNotFound),
		},
		{
			name: "two of the type",
			app: forgo.NewApp(
				forgo.WithResources(realBroker{}, realBroker{}),
				forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: &events{}})),
			),
			opt:  forgotest.ReplaceResource[realBroker](forgotest.Broker("broker")),
			want: is(forgotest.ErrAmbiguous),
		},
		{
			name: "nil factory",
			app:  consumerApp(o, realBroker{}),
			opt:  forgotest.ReplaceResource[realBroker](nil),
			want: notNil,
		},
		{
			name: "provider of the replaced type",
			app: forgo.NewApp(
				forgo.WithResources(realBroker{}),
				forgo.WithProviders(func(realBroker) *strings.Builder { return nil }),
				forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: &events{}})),
			),
			opt:  forgotest.ReplaceResource[realBroker](forgotest.Broker("broker")),
			want: is(forgo.ErrMissingDependency),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, err := forgotest.New(tc.app, forgotest.WithLogger(logger.Config{Output: io.Discard}), tc.opt)
			if env != nil || !tc.want(err) {
				t.Fatalf("New: %v, %v", env, err)
			}
		})
	}
}

// The stand-in broker is a worker.IBroker whose publishers discard and whose subscribers consume nothing.
func TestBroker(t *testing.T) {
	r, err := forgotest.Broker("rabbitmq").New()
	if err != nil {
		t.Fatal(err)
	}
	if r.Name() != "rabbitmq" || r.Close() != nil {
		t.Fatalf("resource %q", r.Name())
	}
	b, ok := r.Value().(worker.IBroker)
	if !ok || r.Value() != r.Value() {
		t.Fatalf("Value %T", r.Value())
	}
	for _, newPublisher := range []func() (worker.IPublisher, error){
		b.NewDefaultPublisher,
		func() (worker.IPublisher, error) { return b.NewPublisher("orders") },
	} {
		p, err := newPublisher()
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Publish(context.Background(), orderMessage("shipped")); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	s, err := b.NewSubscriber("shipping")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Consume(ctx, worker.ConsumerState{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

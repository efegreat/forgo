package forgotest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/server/cron"
	"github.com/efegreat/forgo/server/http"
	"github.com/efegreat/forgo/server/worker"
)

// memoryOutbox is a worker.OutboxStorage in memory, which claims like the SQL outboxes: in the order of the
// saves, at most Limit of the messages no claim holds or whose claim is older than ExpiredBefore.
type memoryOutbox struct {
	mu       sync.Mutex
	messages []worker.Message
	claimed  map[string]time.Time // by message ID, when it was claimed
	claims   []worker.Claim
	err      error // of Claim
	reclaim  bool  // Claim takes the claimed messages too, as a broken storage would
}

func newMemoryOutbox() *memoryOutbox {
	return &memoryOutbox{claimed: map[string]time.Time{}}
}

func (o *memoryOutbox) save(msgs ...worker.Message) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.messages = append(o.messages, msgs...)
}

// saved returns the IDs of the messages saved, in order.
func (o *memoryOutbox) saved() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return ids(o.messages)
}

func (o *memoryOutbox) Claim(ctx context.Context, c worker.Claim) ([]worker.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claims = append(o.claims, c)
	if o.err != nil {
		return nil, o.err
	}
	var msgs []worker.Message
	for _, m := range o.messages {
		if len(msgs) == c.Limit {
			break
		}
		if at, ok := o.claimed[m.ID]; ok && !o.reclaim && !at.Before(c.ExpiredBefore) {
			continue
		}
		o.claimed[m.ID] = time.Now()
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (o *memoryOutbox) Complete(context.Context, string, []string) error { return nil }

func (o *memoryOutbox) DeleteProcessedBefore(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func ids(msgs []worker.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

// orderAPI creates orders, and saves an order_created message for each to its outbox.
type orderAPI struct {
	box *memoryOutbox
}

func (a *orderAPI) Create(_ context.Context, req orderCreated) (orderCreated, error) {
	a.box.save(orderMessage(req.OrderID))
	return req, nil
}

// apiProviders provide an orderAPI over box, which is the app's outbox.
func apiProviders(box *memoryOutbox) []any {
	return []any{
		func() *memoryOutbox { return box },
		func(box *memoryOutbox) *orderAPI { return &orderAPI{box: box} },
	}
}

// createOrderRoute is POST /orders, which creates an order with the orderAPI of the bootstrap.
func createOrderRoute() http.RouteConfig {
	return http.RouteConfig{Name: "create_order", Method: "POST", Path: "/orders", Handler: http.HandlerFor((*orderAPI).Create)}
}

// apiApp is an app with an HTTP server whose createOrderRoute saves to box.
func apiApp(box *memoryOutbox) *forgo.App {
	return forgo.NewApp(
		forgo.WithProviders(apiProviders(box)...),
		forgo.WithServers(http.Config{Routes: []http.RouteConfig{createOrderRoute()}}),
	)
}

// postOrder creates the order orderID through h, the router of an HTTP server with createOrderRoute.
func postOrder(h nethttp.Handler, orderID string) error {
	req := httptest.NewRequest("POST", "/orders", strings.NewReader(fmt.Sprintf(`{"order_id":%q}`, orderID)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != nethttp.StatusOK {
		return fmt.Errorf("POST /orders: %d %s", rec.Code, rec.Body)
	}
	return nil
}

// createOrder creates the order orderID through the HTTP server of env, in process.
func createOrder(t *testing.T, env *forgotest.Env, orderID string) {
	t.Helper()
	h, err := env.Handler(http.ServerNameHTTP)
	if err != nil {
		t.Fatal(err)
	}
	if err := postOrder(h, orderID); err != nil {
		t.Fatal(err)
	}
}

func TestOutbox(t *testing.T) {
	box := newMemoryOutbox()
	env := newEnv(t, apiApp(box), io.Discard)
	defer env.Close()
	ctx := context.Background()

	if msgs, err := env.Outbox(ctx); err != nil || len(msgs) != 0 {
		t.Fatalf("Outbox of an empty outbox: %v, %v", msgs, err)
	}

	createOrder(t, env, "a")
	msgs, err := env.Outbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := orderMessage("a")
	if len(msgs) != 1 || msgs[0].ID != box.saved()[0] || msgs[0].Name != want.Name ||
		string(msgs[0].Payload) != string(want.Payload) || msgs[0].Destination != want.Destination {
		t.Fatalf("Outbox after an order: %+v", msgs)
	}
	if msgs, err := env.Outbox(ctx); err != nil || len(msgs) != 0 {
		t.Fatalf("Outbox read the same messages again: %v, %v", msgs, err)
	}

	// Claims no other claim can take from, and that never take a claimed message back, however old its
	// claim is.
	for _, c := range box.claims {
		if !c.ExpiredBefore.IsZero() || c.Limit <= 0 || !strings.HasPrefix(c.ID, "forgotest-") {
			t.Fatalf("claim %+v", c)
		}
	}

	createOrder(t, env, "b")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if msgs, err := env.Outbox(canceled); msgs != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Outbox with a canceled context: %v, %v", msgs, err)
	}
	// the message the canceled call did not claim is still there
	if msgs, err := env.Outbox(ctx); err != nil || len(msgs) != 1 || msgs[0].ID != box.saved()[1] {
		t.Fatalf("Outbox after a canceled call: %v, %v", msgs, err)
	}

	errDB := errors.New("db down")
	box.mu.Lock()
	box.err = errDB
	box.mu.Unlock()
	if msgs, err := env.Outbox(ctx); msgs != nil || !errors.Is(err, errDB) {
		t.Fatalf("Outbox of a failed claim: %v, %v", msgs, err)
	}
}

// outboxApp is an app whose outbox is box, with a server that is never served.
func outboxApp(box *memoryOutbox) *forgo.App {
	return forgo.NewApp(
		forgo.WithProviders(func() *memoryOutbox { return box }),
		forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: &events{}})),
	)
}

// Outbox claims as many times as it takes, and no more, and returns every message once, in order, around the
// size of its claims, 100: a claim is a transaction on a real outbox.
func TestOutboxBatches(t *testing.T) {
	for _, n := range []int{1, 99, 100, 101, 200, 250} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			box := newMemoryOutbox()
			env := newEnv(t, outboxApp(box), io.Discard)
			defer env.Close()
			for i := range n {
				box.save(orderMessage(fmt.Sprint(i)))
			}

			msgs, err := env.Outbox(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got := ids(msgs); !slices.Equal(got, box.saved()) {
				t.Fatalf("Outbox returned %d messages, want the %d saved, in order", len(got), n)
			}
			if claims, want := len(box.claims), n/100+1; claims != want {
				t.Fatalf("Outbox claimed %d times, want %d", claims, want)
			}
			if msgs, err := env.Outbox(context.Background()); err != nil || len(msgs) != 0 {
				t.Fatalf("second Outbox: %d messages, %v", len(msgs), err)
			}
		})
	}
}

// A storage whose claims take claimed messages again fails Outbox instead of being claimed from forever.
func TestOutboxClaimedTwice(t *testing.T) {
	box := newMemoryOutbox()
	box.reclaim = true
	env := newEnv(t, outboxApp(box), io.Discard)
	defer env.Close()
	for i := range 150 {
		box.save(orderMessage(fmt.Sprint(i)))
	}

	done := make(chan error, 1)
	go func() {
		_, err := env.Outbox(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, forgotest.ErrClaimedTwice) || !strings.Contains(err.Error(), box.saved()[0]) {
			t.Fatalf("Outbox: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Outbox claims from a storage that takes claimed messages again forever")
	}
}

// The app starts without an outbox, or with two, as Run would start it, and only Outbox fails.
func TestOutboxResolution(t *testing.T) {
	for _, tc := range []struct {
		name      string
		providers []any
		want      error
	}{
		{name: "none", want: forgo.ErrMissingDependency},
		{
			name: "two",
			providers: []any{
				func() *memoryOutbox { return newMemoryOutbox() },
				func() worker.OutboxStorage { return newMemoryOutbox() },
			},
			want: forgo.ErrAmbiguousDependency,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnv(t, forgo.NewApp(
				forgo.WithProviders(tc.providers...),
				forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: &events{}})),
			), io.Discard)
			defer env.Close()
			msgs, err := env.Outbox(context.Background())
			if msgs != nil || !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "worker.OutboxStorage") {
				t.Fatalf("Outbox: %v, %v", msgs, err)
			}
		})
	}
}

// Calls at the same time return each message once between them.
func TestOutboxConcurrent(t *testing.T) {
	box := newMemoryOutbox()
	env := newEnv(t, outboxApp(box), io.Discard)
	defer env.Close()
	for i := range 300 {
		box.save(orderMessage(fmt.Sprint(i)))
	}

	var mu sync.Mutex
	var read []string
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			msgs, err := env.Outbox(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			read = append(read, ids(msgs)...)
		})
	}
	wg.Wait()
	slices.Sort(read)
	want := box.saved()
	slices.Sort(want)
	if !slices.Equal(read, want) {
		t.Fatalf("the calls returned %d messages, want each of the %d saved once", len(read), len(want))
	}
}

// The contract between a publishing endpoint and the handler of its message, as two envs over one outbox
// test it: the HTTP app saves the message, and the consumer's app handles it.
func TestOutboxToDeliver(t *testing.T) {
	box := newMemoryOutbox()
	api := newEnv(t, apiApp(box), io.Discard)
	defer api.Close()
	o := &orders{}
	consumer := newEnv(t, consumerApp(o, forgotest.Broker("broker")), io.Discard)
	defer consumer.Close()

	createOrder(t, api, "a")
	msgs, err := api.Outbox(context.Background())
	if err != nil || len(msgs) != 1 {
		t.Fatalf("Outbox: %v, %v", msgs, err)
	}
	got, err := consumer.Deliver(context.Background(), "shipping", msgs[0])
	if err != nil || got.Settlement != forgotest.Completed || got.Err != nil {
		t.Fatalf("Deliver: %+v, %v", got, err)
	}
	if !slices.Equal(o.list(), []string{"a"}) {
		t.Fatalf("handled %v", o.list())
	}
}

// The tests of a package share one env, so they may call it from several goroutines at once: requests, over
// the network and in process, job runs, outbox reads and deliveries.
func TestConcurrentUse(t *testing.T) {
	box := newMemoryOutbox()
	o := &orders{}
	var runs atomic.Int64
	env := newEnv(t, forgo.NewApp(
		forgo.WithResources(forgotest.Broker("broker")),
		forgo.WithProviders(append(apiProviders(box), consumerProviders(o)...)...),
		forgo.WithServers(
			http.Config{Routes: []http.RouteConfig{healthRoute(), createOrderRoute()}},
			cron.Config{Jobs: []cron.JobConfig{jobConfig("report", func(context.Context) error {
				runs.Add(1)
				return nil
			})}},
			shippingConsumer(),
		),
	), io.Discard)
	defer env.Close()
	h, err := env.Handler(http.ServerNameHTTP)
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + addr(t, env, http.ServerNameHTTP) + "/health"
	ctx := context.Background()

	var mu sync.Mutex
	read := map[string]int{} // how many times Outbox returned each message
	deliver := func(msgs []worker.Message) {
		for _, m := range msgs {
			mu.Lock()
			read[m.ID]++
			mu.Unlock()
			if got, err := env.Deliver(ctx, "shipping", m); err != nil || got.Settlement != forgotest.Completed {
				t.Errorf("Deliver %s: %+v, %v", m.ID, got, err)
			}
		}
	}

	const workers = 16
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			if err := postOrder(h, fmt.Sprint(i)); err != nil {
				t.Error(err)
				return
			}
			resp, err := testClient.Get(url)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != nethttp.StatusOK {
				t.Errorf("GET /health: %d", resp.StatusCode)
			}
			if err := env.RunJob(ctx, "report"); err != nil {
				t.Error(err)
			}
			msgs, err := env.Outbox(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			deliver(msgs)
		})
	}
	wg.Wait()
	// what the goroutines saved after the others read the outbox
	msgs, err := env.Outbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deliver(msgs)

	if len(read) != workers {
		t.Errorf("Outbox returned %d messages, want %d", len(read), workers)
	}
	for id, n := range read {
		if n != 1 {
			t.Errorf("Outbox returned %s %d times", id, n)
		}
	}
	handled := o.list()
	slices.Sort(handled)
	var want []string
	for i := range workers {
		want = append(want, fmt.Sprint(i))
	}
	slices.Sort(want)
	if !slices.Equal(handled, want) {
		t.Errorf("handled %v, want every order once", handled)
	}
	if runs.Load() != workers {
		t.Errorf("the job ran %d times, want %d", runs.Load(), workers)
	}
}

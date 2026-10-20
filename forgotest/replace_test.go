package forgotest_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/server/cron"
)

// gateway charges payments: the client of a third-party service, which tests cannot call.
type gateway interface {
	Charge(ctx context.Context, cents int) error
}

// settings are what the provider of the gateway reads.
type settings struct {
	currency string
}

// fakeGateway records the charges.
type fakeGateway struct {
	mu       sync.Mutex
	currency string // of the settings it was built with
	charges  []int
}

func (g *fakeGateway) Charge(_ context.Context, cents int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.charges = append(g.charges, cents)
	return nil
}

func (g *fakeGateway) charged() []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.charges)
}

// checkout charges through the gateway.
type checkout struct {
	gw gateway
}

var errConnected = errors.New("connected to the payment service")

// paymentApp is an app whose provider of the gateway connects to the payment service, which fails in tests,
// and whose job "charge" charges 100 through a checkout.
func paymentApp() *forgo.App {
	return forgo.NewApp(
		forgo.WithProviders(
			func() settings { return settings{currency: "eur"} },
			func(settings) (gateway, error) { return nil, errConnected },
			func(gw gateway) *checkout { return &checkout{gw: gw} },
		),
		forgo.WithServers(cron.Config{Jobs: []cron.JobConfig{{
			Name:     "charge",
			Schedule: "@every 1h",
			Job: cron.As(func(c *checkout) cron.IJob {
				return funcJob(func(ctx context.Context) error { return c.gw.Charge(ctx, 100) })
			}),
		}}}),
	)
}

func newReplacedEnv(app *forgo.App, opts ...forgotest.Option) (*forgotest.Env, error) {
	return forgotest.New(app, append([]forgotest.Option{forgotest.WithLogger(logger.Config{Output: io.Discard})}, opts...)...)
}

// The fake takes the place of the provider, which never runs, and depends on what the providers before it
// provide. The app keeps its own provider.
func TestReplace(t *testing.T) {
	app := paymentApp()
	fake := &fakeGateway{}
	env, err := newReplacedEnv(app, forgotest.Replace(func(s settings) gateway {
		fake.currency = s.currency
		return fake
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	if err := env.RunJob(context.Background(), "charge"); err != nil {
		t.Fatal(err)
	}
	if got := fake.charged(); !slices.Equal(got, []int{100}) || fake.currency != "eur" {
		t.Fatalf("the fake charged %v in %q", got, fake.currency)
	}

	if env, err := newReplacedEnv(app); env != nil || !errors.Is(err, errConnected) {
		t.Fatalf("New of the app without the replacement: %v, %v", env, err)
	}
}

// A provider of a concrete type is replaced by another value of it, together with a resource.
func TestReplaceConcrete(t *testing.T) {
	o := &orders{}
	env, err := newReplacedEnv(consumerApp(&orders{}, realBroker{}),
		forgotest.ReplaceResource[realBroker](forgotest.Broker("broker")),
		forgotest.Replace(func() *orders { return o }))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	got, err := env.Deliver(context.Background(), "shipping", orderMessage("shipped"))
	if err != nil || got.Settlement != forgotest.Completed {
		t.Fatalf("Deliver: %+v, %v", got, err)
	}
	if !slices.Equal(o.list(), []string{"shipped"}) {
		t.Fatalf("the replacement handled %v", o.list())
	}
}

func TestReplaceFailures(t *testing.T) {
	fake := &fakeGateway{}
	toFake := func() gateway { return fake }
	errFake := errors.New("fake failed")
	replaceError := func(err error) bool { return err != nil && strings.HasPrefix(err.Error(), "forgotest: Replace: ") }
	for _, tc := range []struct {
		name string
		app  *forgo.App
		opts []forgotest.Option
		want func(error) bool
	}{
		{
			name: "no provider of the type",
			opts: []forgotest.Option{forgotest.Replace(func() *strings.Builder { return nil })},
			want: is(forgotest.ErrNotFound),
		},
		{
			// the fake's own type replaces nothing, and the error names the interface to declare
			name: "the type of the fake",
			opts: []forgotest.Option{forgotest.Replace(func() *fakeGateway { return fake })},
			want: func(err error) bool {
				return errors.Is(err, forgotest.ErrNotFound) && strings.Contains(err.Error(), "implements forgotest_test.gateway")
			},
		},
		{
			name: "two replacements",
			opts: []forgotest.Option{forgotest.Replace(toFake, func() (gateway, error) { return fake, nil })},
			want: is(forgotest.ErrAmbiguous),
		},
		{
			name: "two options",
			opts: []forgotest.Option{forgotest.Replace(toFake), forgotest.Replace(toFake)},
			want: is(forgotest.ErrAmbiguous),
		},
		{
			name: "two providers of the app",
			app: forgo.NewApp(
				forgo.WithProviders(func() gateway { return nil }, func() gateway { return nil }),
				forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: &events{}})),
			),
			opts: []forgotest.Option{forgotest.Replace(toFake)},
			want: is(forgotest.ErrAmbiguous),
		},
		{
			name: "nil",
			opts: []forgotest.Option{forgotest.Replace(nil)},
			want: replaceError,
		},
		{
			name: "nil function",
			opts: []forgotest.Option{forgotest.Replace((func() gateway)(nil))},
			want: replaceError,
		},
		{
			name: "not a function",
			opts: []forgotest.Option{forgotest.Replace(fake)},
			want: replaceError,
		},
		{
			name: "no result",
			opts: []forgotest.Option{forgotest.Replace(func() {})},
			want: replaceError,
		},
		{
			// the replacement runs where the provider ran, before the providers that follow it
			name: "a dependency provided later",
			opts: []forgotest.Option{forgotest.Replace(func(*checkout) gateway { return fake })},
			want: is(forgo.ErrMissingDependency),
		},
		{
			name: "a failing replacement",
			opts: []forgotest.Option{forgotest.Replace(func() (gateway, error) { return nil, errFake })},
			want: func(err error) bool { return err != nil && strings.Contains(err.Error(), errFake.Error()) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := tc.app
			if app == nil {
				app = paymentApp()
			}
			env, err := newReplacedEnv(app, tc.opts...)
			if env != nil || !tc.want(err) {
				t.Fatalf("New: %v, %v", env, err)
			}
		})
	}
}

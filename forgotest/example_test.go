package forgotest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/cron"
	"github.com/efegreat/forgo/server/http"
)

// newApp builds the app, as main does with the same function.
func newApp() *forgo.App {
	return forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithServers(http.Config{
			Addr: ":8080", // the env serves on a loopback port of its own
			Routes: []http.RouteConfig{
				{Name: "health", Method: "GET", Path: "/health", Handler: http.HealthHandler()},
			},
		}),
	)
}

func ExampleNew() {
	// In a test package, this runs in TestMain, once for all of its tests.
	env, err := forgotest.New(newApp(), forgotest.WithLogger(logger.Config{Output: io.Discard}))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer env.Close()

	addr, err := env.Addr(http.ServerNameHTTP)
	if err != nil {
		fmt.Println(err)
		return
	}
	resp, err := nethttp.Get("http://" + addr + "/health")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	// Output: 200
}

func ExampleEnv_Handler() {
	env, err := forgotest.New(newApp(), forgotest.WithLogger(logger.Config{Output: io.Discard}))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer env.Close()

	// the router, with every route's chain, without the network
	h, err := env.Handler(http.ServerNameHTTP)
	if err != nil {
		fmt.Println(err)
		return
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	fmt.Println(rec.Code)
	// Output: 200
}

// reportJob sends the daily report.
type reportJob struct{}

func (reportJob) Tolerance() operation.Tolerance { return operation.Tolerance{} }

func (reportJob) Run(context.Context) error {
	fmt.Println("report sent")
	return nil
}

func ExampleEnv_RunJob() {
	app := forgo.NewApp(forgo.WithServers(cron.Config{Jobs: []cron.JobConfig{
		{Name: "report", Schedule: "@daily", Job: cron.As(func() cron.IJob { return reportJob{} })},
	}}))
	env, err := forgotest.New(app, forgotest.WithLogger(logger.Config{Output: io.Discard}))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer env.Close()

	// The schedule never runs the job in tests: RunJob runs it once, now, through its chain.
	fmt.Println(env.RunJob(context.Background(), "report"))
	// Output:
	// report sent
	// <nil>
}

// mailer sends emails through a third-party service.
type mailer interface {
	Send(to, subject string) error
}

// newSMTPMailer connects to the SMTP server of the provider, which tests do not run.
func newSMTPMailer() (mailer, error) {
	return nil, errors.New("dial smtp: connection refused")
}

// fakeMailer prints what it would send.
type fakeMailer struct{}

func (fakeMailer) Send(to, subject string) error {
	fmt.Println("sent", subject, "to", to)
	return nil
}

// welcomeJob welcomes the new users.
type welcomeJob struct {
	mailer mailer
}

func (welcomeJob) Tolerance() operation.Tolerance { return operation.Tolerance{} }

func (j welcomeJob) Run(context.Context) error {
	return j.mailer.Send("ada@example.com", "welcome")
}

func ExampleReplace() {
	app := forgo.NewApp(
		forgo.WithProviders(newSMTPMailer),
		forgo.WithServers(cron.Config{Jobs: []cron.JobConfig{
			{Name: "welcome", Schedule: "@hourly", Job: cron.As(func(m mailer) cron.IJob { return welcomeJob{mailer: m} })},
		}}),
	)
	// The fake's constructor declares the provider's result type, mailer, not the fake's own.
	env, err := forgotest.New(app,
		forgotest.WithLogger(logger.Config{Output: io.Discard}),
		forgotest.Replace(func() mailer { return fakeMailer{} }))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer env.Close()

	fmt.Println(env.RunJob(context.Background(), "welcome"))
	// Output:
	// sent welcome to ada@example.com
	// <nil>
}

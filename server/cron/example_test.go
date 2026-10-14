package cron_test

import (
	"context"
	"os"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/cron"
)

type CartExpirer struct{}

// NewCartExpirer is resolved by the bootstrap, so it can take providers and resources as parameters.
func NewCartExpirer() *CartExpirer {
	return &CartExpirer{}
}

// Tolerance gives each run up to three attempts, a minute apart.
func (j *CartExpirer) Tolerance() operation.Tolerance {
	return operation.Tolerance{Retry: true, RetryAttempts: 3, RetryDelay: time.Minute}
}

func (j *CartExpirer) Run(ctx context.Context) error {
	// The logger of every run carries the job's op_name and op_id.
	logger.FromCtx(ctx).Info().Msg("expiring carts")
	return nil
}

func ExampleConfig() {
	conf := cron.Config{
		Jobs: []cron.JobConfig{
			{Name: "expireCarts", Schedule: "*/5 * * * *", Job: cron.As(NewCartExpirer)},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("shop-cron"),
		forgo.WithServers(conf),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

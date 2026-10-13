package cron

import (
	"context"
	"io"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"

	"github.com/rs/zerolog"
)

// BenchmarkRun measures a run of a job that does nothing, as the scheduler calls it.
func BenchmarkRun(b *testing.B) {
	for _, disabled := range []bool{false, true} {
		name := "base"
		if disabled {
			name = "no_base"
		}
		b.Run(name, func(b *testing.B) {
			boot := forgo.NewBootstrap(nil, nil)
			r, err := boot.Build()
			if err != nil {
				b.Fatal(err)
			}
			cfg := Config{
				ServerConfig: forgo.ServerConfig{DisableBaseMiddlewares: disabled},
				Jobs:         []JobConfig{{Name: "job", Schedule: "@daily", Job: As(newNoopJob)}},
			}
			srv, err := cfg.New(r)
			if err != nil {
				b.Fatal(err)
			}
			s := srv.(*Server)
			if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(io.Discard))); err != nil {
				b.Fatal(err)
			}
			run := s.createRun(s.cfg.jobs[0])
			b.ReportAllocs()
			for b.Loop() {
				run()
			}
		})
	}
}

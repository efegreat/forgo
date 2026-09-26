package worker

import (
	"context"
	"forgo/logger"
	"forgo/operation"
	"forgo/process"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type SubscriberConfig struct {
	Name        string
	Concurrency int
	MaxAttempt  int
	AckTimeout  time.Duration
	Middlewares []operation.Middleware
	Handlers    []HandlerConfig
}

type HandlerConfig struct {
	Name                string
	Lock                operation.Lock
	TransactionDisabled bool
	Middlewares         []operation.Middleware
	Handler             Handler
}

type ConsumerState struct {
	Handler     HandlerFunc
	Concurrency int
	Wg          *sync.WaitGroup
	Sem         process.ISemaphore
}

type subscriber struct {
	subs   ISubscriber
	logger zerolog.Logger
	ctx    context.Context
	cs     *ConsumerState
}

func (s *subscriber) Listen() {
	s.logger.Info().Msg("subscriber started")

	err := s.subs.Consume(s.ctx, s.cs)

	s.logger.Info().Msg("subscriber stopping")
	s.cs.Wg.Wait()
	s.logger.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("subscriber stopped")
}

func (s *subscriber) Close() error {
	return s.subs.Close()
}

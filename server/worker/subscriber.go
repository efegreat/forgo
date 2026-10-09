package worker

import (
	"context"
	"fmt"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

type ConsumerState struct {
	Handler     handlerFunc
	Concurrency int
}

type subscriber struct {
	name   string
	subs   ISubscriber
	logger zerolog.Logger
	ctx    context.Context
	cs     ConsumerState
}

// Listen consumes until the server stops and returns the error of a subscriber that stopped
// consuming on its own.
func (s *subscriber) Listen() error {
	s.logger.Info().Msg("subscriber started")

	err := s.subs.Consume(s.ctx, s.cs)

	s.logger.Info().Msg("subscriber stopped")
	if err != nil {
		s.logger.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityCritical)).Msg("subscriber error")
		return fmt.Errorf("worker: subscriber %s: %w", s.name, err)
	}
	return nil
}

func (s *subscriber) Close() error {
	return s.subs.Close()
}

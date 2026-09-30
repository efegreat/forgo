package worker

import (
	"context"
	"forgo/logger"
	"forgo/operation"

	"github.com/rs/zerolog"
)

type ConsumerState struct {
	Handler     HandlerFunc
	Concurrency int
}

type subscriber struct {
	subs   ISubscriber
	logger zerolog.Logger
	ctx    context.Context
	cs     ConsumerState
}

func (s *subscriber) Listen() {
	s.logger.Info().Msg("subscriber started")

	err := s.subs.Consume(s.ctx, s.cs)

	s.logger.Info().Msg("subscriber stopped")
	if err != nil {
		s.logger.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("subscriber error")
	}
}

func (s *subscriber) Close() error {
	return s.subs.Close()
}

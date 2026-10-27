package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

// errStoppedConsuming fails a subscriber whose Consume returned nil before the server stopped.
var errStoppedConsuming = errors.New("stopped consuming before the server stopped")

type subscriber struct {
	name   string
	subs   ISubscriber
	logger zerolog.Logger
	ctx    context.Context
	// handle is what the subscriber hands to ISubscriber.Consume. It calls dispatch.
	handle func(ctx context.Context, d IDelivery)
	// dispatch handles a delivery with the handler of its message's name.
	dispatch handleFunc
}

// Listen consumes until the server stops and returns the error of a subscriber that stopped
// consuming on its own.
func (s *subscriber) Listen() error {
	s.logger.Info().Msg("subscriber started")

	err := s.subs.Consume(s.ctx, s.handle)

	s.logger.Info().Msg("subscriber stopped")
	if err == nil && s.ctx.Err() == nil {
		err = errStoppedConsuming
	}
	if err != nil {
		s.logger.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityCritical)).Msg("subscriber error")
		return fmt.Errorf("worker: subscriber %s: %w", s.name, err)
	}
	return nil
}

// Close closes the broker's subscriber, and logs its failure.
func (s *subscriber) Close() {
	if err := s.subs.Close(); err != nil {
		s.logger.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("subscriber close failed")
	}
}

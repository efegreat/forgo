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

// ConsumerState is what the consumer hands to ISubscriber.Consume.
type ConsumerState struct {
	// Handler handles one delivery to its end, and settles it, before it returns. ctx passes on the values
	// of the broker, if it has any: the handling goes on when ctx ends, and only killing the server
	// cancels it.
	Handler func(ctx context.Context, d IDelivery)
	// Concurrency is the number of deliveries to handle at once, at most.
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

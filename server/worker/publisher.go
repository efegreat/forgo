package worker

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

type waitingMessage struct {
	msg Message
	// deadline ends the publish, so that it ends before another instance can take the claim over.
	deadline  time.Time
	wg        *sync.WaitGroup
	successCh chan string
}

type publisher struct {
	pbCh     chan IPublisher
	closers  []io.Closer
	messages chan waitingMessage
	logger   zerolog.Logger
	cfg      PublisherConfig
	ctx      context.Context
}

func newPublisher(ctx context.Context, msgLimit int, pbs []IPublisher, logger zerolog.Logger, cfg PublisherConfig) *publisher {
	pbCh := make(chan IPublisher, len(pbs))
	closers := make([]io.Closer, 0, len(pbs))
	for _, pb := range pbs {
		closers = append(closers, pb)
		pbCh <- pb
	}
	messages := make(chan waitingMessage, msgLimit)

	return &publisher{
		closers:  closers,
		pbCh:     pbCh,
		messages: messages,
		logger:   logger,
		cfg:      cfg,
		ctx:      ctx,
	}
}

func (p *publisher) Listen() {
	p.logger.Info().Msg("publisher started")

	p.loop()

	p.logger.Info().Msg("publisher stopped")
}

func (p *publisher) Receive(msg waitingMessage) {
	p.messages <- msg
}

// Close closes the broker's publishers, and logs their failures.
func (p *publisher) Close() {
	errs := make([]error, 0, len(p.closers))
	for _, c := range p.closers {
		errs = append(errs, c.Close())
	}
	if err := errors.Join(errs...); err != nil {
		p.logger.Error().
			Err(err).
			Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
			Msg("publisher close failed")
	}
}

func (p *publisher) publish(pb IPublisher, msg Message, deadline time.Time) (success bool) {
	timeout := min(p.cfg.Timeout, time.Until(deadline))
	if timeout <= 0 {
		// The batch ran out of time: Complete releases the message for the next batch.
		return false
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	if err := pb.Publish(ctx, msg); err != nil {
		p.logger.Error().
			Err(err).
			Str(logger.MessageIDLogKey, msg.ID).
			Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
			Msg("publish failed")
		return false
	}
	return true
}

func (p *publisher) loop() {
	for msg := range p.messages {
		// Wait for a publisher to be available
		pb := <-p.pbCh
		go func() {
			defer func() {
				p.pbCh <- pb
				msg.wg.Done()
			}()
			if ok := p.publish(pb, msg.msg, msg.deadline); ok {
				msg.successCh <- msg.msg.ID
			}
		}()
	}
}

func (p *publisher) stop() {
	close(p.messages)
}

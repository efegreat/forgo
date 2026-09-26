package worker

import (
	"context"
	"fmt"
	"forgo/logger"
	"forgo/operation"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type PublisherConfig struct {
	Name        string
	Concurrency int
	Timeout     time.Duration
}

type waitingMessage struct {
	msg       Message
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
	wg       sync.WaitGroup
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
		wg:       sync.WaitGroup{},
	}
}

func (p *publisher) Listen() {
	p.logger.Info().Msg("publisher started")

	err := p.loop()

	p.logger.Info().Msg("publisher stopping")
	p.wg.Wait()
	p.logger.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("publisher stopped")
}

func (p *publisher) Receive(msg waitingMessage) {
	p.messages <- msg
}

func (p *publisher) Close() error {
	var errs error
	for _, c := range p.closers {
		if err := c.Close(); err != nil {
			if errs == nil {
				errs = err
			} else {
				errs = fmt.Errorf("%w, %w", errs, err)
			}
		}
	}
	return errs
}

func (p *publisher) publish(pb IPublisher, msg Message) (success bool) {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.Timeout)
	defer cancel()

	if err := pb.Publish(ctx, msg); err != nil {
		p.logger.Error().
			Err(err).
			Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
			Msg("publish failed")
		return false
	}
	return true
}

func (p *publisher) loop() error {
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case msg := <-p.messages:
			// Wait for a publisher to be available
			pb := <-p.pbCh
			p.wg.Go(func() {
				defer func() {
					p.pbCh <- pb
					msg.wg.Done()
				}()
				if ok := p.publish(pb, msg.msg); ok {
					msg.successCh <- msg.msg.ID
				}
			})
		}
	}
}

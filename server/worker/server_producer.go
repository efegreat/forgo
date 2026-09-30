package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"forgo"
	"forgo/logger"
	"forgo/operation"
	"sync"
	"time"

	"github.com/rs/xid"
)

const ServerNameProducer = "worker_producer"

type ProducerServer struct {
	cfg ProducerConfig

	publishers       map[string]*publisher
	defaultPublisher *publisher

	broker  IBroker
	storage OutboxStorage

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *ProducerServer) Term() forgo.TermConfig {
	return s.cfg.Term
}

func (s *ProducerServer) Name() string {
	return ServerNameProducer
}

func (s *ProducerServer) Init(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	dpb, err := s.createPublisher(s.cfg.DefaultPublisher)
	if err != nil {
		return err
	}
	s.defaultPublisher = dpb
	for _, pc := range s.cfg.Publishers {
		pb, err := s.createPublisher(pc)
		if err != nil {
			return err
		}
		s.publishers[pc.Name] = pb
	}
	return nil
}

func (s *ProducerServer) Serve() error {
	lg := logger.FromCtx(s.ctx)
	s.wg.Go(func() {
		if err := s.cleanup(s.ctx); err != nil {
			lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("cleanup failed")
		}
	})
	s.wg.Go(func() {
		s.defaultPublisher.Listen()
		s.defaultPublisher.Close()
	})
	for _, pb := range s.publishers {
		s.wg.Go(func() {
			pb.Listen()
			pb.Close()
		})
	}
	s.wg.Go(func() {
		if err := s.process(s.ctx); err != nil {
			lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("process failed")
		}
	})

	<-s.ctx.Done()
	return nil
}

func (s *ProducerServer) Terminate(context.Context) error {
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *ProducerServer) Kill() {
}

func (s *ProducerServer) processBatch(ctx context.Context) (int, error) {
	claimID, err := newClaimID()
	if err != nil {
		return 0, fmt.Errorf("outbox: generate claim id: %w", err)
	}

	now := time.Now()

	messages, err := s.storage.Claim(
		context.WithoutCancel(ctx),
		Claim{
			ID:            claimID,
			ExpiredBefore: now.Add(-s.cfg.ClaimTTL),
			Limit:         s.cfg.Limit,
		},
	)
	if err != nil {
		return 0, fmt.Errorf("outbox: claim messages: %w", err)
	}

	ln := len(messages)
	if ln == 0 {
		return 0, nil
	}

	successfulIDs := s.publish(messages)
	lnSuccess := len(successfulIDs)

	// Complete also releases the unsuccessful messages belonging to this claim.
	if err := s.storage.Complete(
		context.WithoutCancel(ctx),
		claimID,
		successfulIDs,
	); err != nil {
		return ln, fmt.Errorf(
			"outbox: complete claim %s: %w",
			claimID,
			err,
		)
	}

	failed := ln - lnSuccess
	if failed > 0 && s.cfg.ErrorPercentage != 0 {
		per := (failed * 100) / ln
		if per >= s.cfg.ErrorPercentage {
			s.cfg.OnErrorPercentage(ctx)
		}
	}

	return lnSuccess, nil
}

func (s *ProducerServer) process(ctx context.Context) error {
	defer func() {
		s.defaultPublisher.stop()
		for _, pb := range s.publishers {
			pb.stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			for {
				count, err := s.processBatch(ctx)
				if err != nil {
					return err
				}

				if count < s.cfg.Limit {
					break
				}

				// Keep processing immediately while full batches exist.
				select {
				case <-ctx.Done():
					return nil
				default:
				}
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(s.cfg.Interval):
			}
		}
	}
}

func (s *ProducerServer) cleanup(ctx context.Context) error {
	if s.cfg.Clean.MaxTTL <= 0 {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			before := time.Now().Add(-s.cfg.Clean.MaxTTL)

			for {
				deleted, err := s.storage.DeleteProcessedBefore(
					ctx,
					before,
					s.cfg.Clean.CleanupLimit,
				)
				if err != nil {
					return fmt.Errorf("outbox: cleanup: %w", err)
				}

				if deleted < int64(s.cfg.Clean.CleanupLimit) {
					break
				}

				// Keep deleting immediately while full cleanup batches exist.
				select {
				case <-ctx.Done():
					return nil
				default:
				}
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(s.cfg.Clean.CleanupInterval):
			}
		}
	}
}

func (s *ProducerServer) createPublisher(pc PublisherConfig) (*publisher, error) {
	id := xid.New().String()

	var pbs []IPublisher
	for range pc.Concurrency {
		pb, err := s.broker.NewPublisher(pc.Name)
		if err != nil {
			return nil, err
		}
		pbs = append(pbs, pb)
	}

	lg := *logger.FromCtx(s.ctx)
	lg = lg.With().
		Str(logger.BrokerLogKey, s.broker.Name()).
		Str(logger.PublisherLogKey, pc.Name).
		Str(logger.PublisherIDLogKey, id).
		Logger()
	loggerCtx := logger.NewCtx(s.ctx, lg)

	return newPublisher(
		loggerCtx,
		s.cfg.Limit,
		pbs,
		lg,
		pc,
	), nil
}

func (s *ProducerServer) publish(messages []Message) []string {
	ln := len(messages)
	wg := &sync.WaitGroup{}
	wg.Add(ln)
	successCh := make(chan string, ln)

	for _, msg := range messages {
		wm := waitingMessage{msg: msg, wg: wg, successCh: successCh}
		if pb, ok := s.publishers[msg.Name]; ok {
			pb.Receive(wm)
			continue
		}

		s.defaultPublisher.Receive(wm)
	}

	wg.Wait()
	close(successCh)

	successIds := make([]string, 0, len(messages))
	for id := range successCh {
		successIds = append(successIds, id)
	}

	return successIds
}

func newClaimID() (string, error) {
	var id [16]byte

	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(id[:]), nil
}

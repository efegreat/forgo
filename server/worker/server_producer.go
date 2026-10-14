package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/xid"
)

// ServerNameProducer is the server_name of the producer's logs.
const ServerNameProducer = "worker_producer"

// producerServer relays the outbox of a ProducerConfig. ProducerConfig.New creates it.
type producerServer struct {
	lifecycle

	cfg ProducerConfig

	publishers       map[string]*publisher
	defaultPublisher *publisher

	broker  IBroker
	storage OutboxStorage
}

// Term returns the termination settings of the ProducerConfig.
func (s *producerServer) Term() forgo.TermConfig {
	return s.cfg.Term
}

// Name returns ServerNameProducer.
func (s *producerServer) Name() string {
	return ServerNameProducer
}

// Init creates the publishers at the broker.
func (s *producerServer) Init(ctx context.Context) error {
	s.setup(ctx)
	dpb, err := s.createPublisher(s.cfg.DefaultPublisher, s.broker.NewDefaultPublisher)
	if err != nil {
		return err
	}
	s.defaultPublisher = dpb
	for _, pc := range s.cfg.Publishers {
		pb, err := s.createPublisher(pc, func() (IPublisher, error) {
			return s.broker.NewPublisher(pc.Name)
		})
		if err != nil {
			return err
		}
		s.publishers[pc.Name] = pb
	}
	return nil
}

// Serve starts the relay, the publishers and the outbox cleanup, and blocks until Terminate. After
// Terminate, it closes the publishers and returns at once.
func (s *producerServer) Serve() error {
	fns := []func(){
		func() {
			cleanup(s.ctx, s.cfg.Clean, s.storage.DeleteProcessedBefore)
		},
		func() {
			s.defaultPublisher.Listen()
			s.defaultPublisher.Close()
		},
		func() {
			s.process(s.ctx)
		},
	}
	for _, pb := range s.publishers {
		fns = append(fns, func() {
			pb.Listen()
			pb.Close()
		})
	}
	if !s.start(fns...) {
		s.defaultPublisher.Close()
		for _, pb := range s.publishers {
			pb.Close()
		}
		return nil
	}

	<-s.ctx.Done()
	return nil
}

// Terminate stops claiming messages, and waits for the batch in flight to be published and completed.
// When ctx ends first, it returns ctx's error and leaves the batch running, for App to Kill the server.
func (s *producerServer) Terminate(ctx context.Context) error {
	if err := s.terminate(ctx); err != nil {
		return fmt.Errorf("worker: waiting for the batch in flight: %w", err)
	}
	return nil
}

// Kill cancels the publishes in flight. The batch then completes its claim, which gives the messages
// that were not published back to the outbox. App calls it when Terminate failed or timed out. It does
// not wait for the batch.
func (s *producerServer) Kill() {
	s.kill()
}

func (s *producerServer) processBatch(ctx context.Context) (int, error) {
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

	// Publishing stops at half the claim's lifetime, so that the claim is completed before another
	// instance can take it over and publish the same messages, however slow the broker is.
	successfulIDs := s.publish(messages, now.Add(s.cfg.ClaimTTL/2))
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
	if failed > 0 && s.cfg.ErrorPercentage > 0 {
		per := (failed * 100) / ln
		if per >= s.cfg.ErrorPercentage {
			s.onErrorPercentage(ctx, per)
		}
	}

	return lnSuccess, nil
}

func (s *producerServer) onErrorPercentage(ctx context.Context, percentage int) {
	if s.cfg.OnErrorPercentage != nil {
		s.cfg.OnErrorPercentage(ctx)
		return
	}
	logger.FromCtx(ctx).Error().
		Int(logger.ErrorPercentageLogKey, percentage).
		Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
		Msg("publish error percentage reached")
}

// process relays batches until ctx ends. Full batches are processed back to back. A failed batch is
// logged and retried after Interval, so a storage outage does not stop the relay for the life of
// the process.
func (s *producerServer) process(ctx context.Context) {
	defer func() {
		s.defaultPublisher.stop()
		for _, pb := range s.publishers {
			pb.stop()
		}
	}()

	for {
		for ctx.Err() == nil {
			count, err := s.processBatch(ctx)
			if err != nil {
				logger.FromCtx(ctx).Error().
					Err(err).
					Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
					Msg("outbox batch failed")
				break
			}
			if count < s.cfg.Limit {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.Interval):
		}
	}
}

func (s *producerServer) createPublisher(pc PublisherConfig, newBrokerPublisher func() (IPublisher, error)) (*publisher, error) {
	id := xid.New().String()

	var pbs []IPublisher
	for range pc.Concurrency {
		pb, err := newBrokerPublisher()
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
	// Publishes run on the context of the work in flight, so that Terminate lets them finish.
	loggerCtx := logger.NewCtx(s.work, lg)

	return newPublisher(
		loggerCtx,
		s.cfg.Limit,
		pbs,
		lg,
		pc,
	), nil
}

func (s *producerServer) publish(messages []Message, deadline time.Time) []string {
	ln := len(messages)
	wg := &sync.WaitGroup{}
	wg.Add(ln)
	successCh := make(chan string, ln)

	for _, msg := range messages {
		wm := waitingMessage{msg: msg, deadline: deadline, wg: wg, successCh: successCh}
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

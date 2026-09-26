package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"forgo"
	"forgo/logger"
	"forgo/operation"
	"sync"
	"time"

	"github.com/rs/xid"
)

const ServerNameProducer = "worker_producer"

var DefaultPublisherConfig = PublisherConfig{
	Name:    "default",
	Timeout: time.Second * 3,
}

type Config struct {
	// Maximum number of messages claimed per query.
	Limit int

	// Wait when there is no full backlog to process.
	Interval time.Duration

	// Maximum publish error percentage. Triggers OnErrorPercentage if reached.
	ErrorPercentage int

	// Triggers when publish error percentage >= ErrorPercentage
	OnErrorPercentage func(ctx context.Context)

	// How long a claim is considered owned by an outbox instance.
	// After this duration another instance may reclaim it.
	ClaimTTL time.Duration

	Clean CleanConfig
}

type CleanConfig struct {
	// Successfully processed messages older than this are deleted.
	// Zero disables cleanup.
	MaxTTL time.Duration

	// Maximum number of old processed messages to be deleted per query.
	CleanupLimit int

	// How often old processed messages are deleted.
	// Only used when MaxTTL > 0.
	CleanupInterval time.Duration
}

func defaultConfig() Config {
	return Config{
		Limit:           100,
		Interval:        time.Second,
		ErrorPercentage: 25,
		OnErrorPercentage: func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute * 10):
			}
		},
		ClaimTTL: time.Minute,
		Clean:    defaultCleanConfig(),
	}
}

func defaultCleanConfig() CleanConfig {
	return CleanConfig{
		MaxTTL:          30 * 24 * time.Hour,
		CleanupLimit:    100,
		CleanupInterval: time.Minute * 10,
	}
}

type ProducerServer struct {
	*forgo.Server
	broker                 IBroker
	storage                OutboxStorage
	publisherConfigs       []PublisherConfig
	publishers             map[string]*publisher
	defaultPublisherConfig PublisherConfig
	defaultPublisher       *publisher
	cfg                    Config
	wg                     sync.WaitGroup
	ctx                    context.Context
	cancel                 context.CancelFunc
}

func NewProducerServer(opts ...ProducerServerOption) *ProducerServer {
	ws := &ProducerServer{Server: &forgo.Server{}, cfg: defaultConfig(), publisherConfigs: make([]PublisherConfig, 0), publishers: make(map[string]*publisher)}
	for _, opt := range opts {
		opt(ws)
	}
	if ws.Server.Tt == 0 {
		ws.Server.Tt = forgo.DefaultServerTimeout
	}
	if ws.defaultPublisherConfig == (PublisherConfig{}) {
		ws.defaultPublisherConfig = DefaultPublisherConfig
	}

	return ws
}

func (s *ProducerServer) Name() string {
	return ServerNameProducer
}

func (s *ProducerServer) Init(ctx context.Context) error {
	if s.broker == nil {
		return errors.New("worker_producer: broker is not set")
	}
	if s.storage == nil {
		return errors.New("worker_producer: storage is not set")
	}
	if err := s.broker.Init(ctx); err != nil {
		return err
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	dpb, err := s.createPublisher(s.defaultPublisherConfig)
	if err != nil {
		return err
	}
	s.defaultPublisher = dpb
	for _, pc := range s.publisherConfigs {
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
	s.broker.Close()
	return nil
}

func (s *ProducerServer) Kill() {
	s.broker.Close()
}

func (s *ProducerServer) processBatch(ctx context.Context) (int, error) {
	claimID, err := newClaimID()
	if err != nil {
		return 0, fmt.Errorf("outbox: generate claim id: %w", err)
	}

	now := time.Now()

	messages, err := s.storage.Claim(
		ctx,
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

	if pc.Concurrency == 0 {
		pc.Concurrency = 1
	}

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

type ProducerServerOption func(*ProducerServer)

func WithProducerBaseOption(opt forgo.ServerOption) ProducerServerOption {
	return func(s *ProducerServer) {
		opt(s.Server)
	}
}

func WithProducerBroker(broker IBroker) ProducerServerOption {
	return func(s *ProducerServer) {
		s.broker = broker
	}
}

func WithOutboxStorage(storage OutboxStorage) ProducerServerOption {
	return func(s *ProducerServer) {
		s.storage = storage
	}
}

func WithProducerConfig(cfg Config) ProducerServerOption {
	return func(s *ProducerServer) {
		s.cfg = cfg
	}
}

func WithPublishers(pb ...PublisherConfig) ProducerServerOption {
	return func(s *ProducerServer) {
		s.publisherConfigs = append(s.publisherConfigs, pb...)
	}
}

func WithDefaultPublisher(pb PublisherConfig) ProducerServerOption {
	return func(s *ProducerServer) {
		s.defaultPublisherConfig = pb
	}
}

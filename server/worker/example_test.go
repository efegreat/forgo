package worker_test

import (
	"context"
	"os"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"
)

// OrderPlaced is the payload of an order.placed message. The default binder decodes it from JSON and
// validates it.
type OrderPlaced struct {
	OrderID string `json:"orderId" validate:"required"`
}

type BillingService struct{}

func NewBillingService() *BillingService { return &BillingService{} }

// OnOrderPlaced handles one order.placed message. An error retries the message, up to the subscriber's
// MaxAttempt.
func (s *BillingService) OnOrderPlaced(ctx context.Context, evt OrderPlaced) error {
	return nil
}

// forgo ships no broker and no storage, so these stand for forgo-adapter's: databaseConf and brokerConf
// for sqldb.Config and rabbitmq.Config, and the constructors for sqltx.NewTransactor, inbox.NewPostgres
// and outbox.NewPostgres.
var (
	databaseConf, brokerConf           forgo.ResourceFactory
	newTransactor, newInbox, newOutbox any
)

func Example() {
	// The consumer handles the messages of the broker's billing subscriber. Each one runs in a
	// transaction with its inbox row, so a redelivery of a completed message is acknowledged without
	// running again.
	consumer := worker.ConsumerConfig{
		Subscribers: []worker.SubscriberConfig{{
			Name:        "billing",
			Concurrency: 8,
			Handlers: []worker.HandlerConfig{
				{Name: "order.placed", Handler: worker.HandlerFor((*BillingService).OnOrderPlaced)},
			},
		}},
	}
	// The producer relays the messages that operations saved to the outbox to the broker.
	producer := worker.ProducerConfig{
		Interval:         500 * time.Millisecond,
		DefaultPublisher: worker.PublisherConfig{Concurrency: 4},
	}

	app := forgo.NewApp(
		forgo.WithName("billing-worker"),
		forgo.WithResources(databaseConf, brokerConf),
		forgo.WithProviders(newTransactor, newInbox, newOutbox, NewBillingService),
		forgo.WithServers(consumer, producer),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

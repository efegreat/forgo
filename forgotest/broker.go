package forgotest

import (
	"context"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"
)

// Broker returns the factory of a stand-in broker, to put in place of the app's broker with ReplaceResource,
// since tests run no broker:
//
//	forgotest.New(app.New(config.Consumer), forgotest.ReplaceResource[rabbitmq.Config](forgotest.Broker("rabbitmq")))
//
// Its Value is a worker.IBroker that never delivers. Its publishers discard what they are given, and its
// subscribers never consume, since an env serves neither the producer nor the consumer. Tests hand messages
// to the consumer with Env.Deliver instead. name is the Name of the resource it replaces, so that
// `boot:"name"` tags still find it.
//
// Whether the broker routes what the producer publishes to the subscribers is the broker's topology, which
// application tests do not cover.
func Broker(name string) forgo.ResourceFactory {
	return brokerFactory{name: name}
}

type brokerFactory struct {
	name string
}

func (f brokerFactory) New() (forgo.Resource, error) {
	return brokerResource{name: f.name, broker: &broker{}}, nil
}

// brokerResource holds the stand-in broker.
type brokerResource struct {
	name   string
	broker *broker
}

func (r brokerResource) Name() string { return r.name }
func (r brokerResource) Value() any   { return r.broker }
func (r brokerResource) Close() error { return nil }

// broker is the stand-in worker.IBroker.
type broker struct{}

func (*broker) Name() string                                     { return "forgotest" }
func (*broker) NewDefaultPublisher() (worker.IPublisher, error)  { return publisher{}, nil }
func (*broker) NewPublisher(string) (worker.IPublisher, error)   { return publisher{}, nil }
func (*broker) NewSubscriber(string) (worker.ISubscriber, error) { return subscriber{}, nil }

// publisher discards what it is given.
type publisher struct{}

func (publisher) Publish(context.Context, worker.Message) error { return nil }
func (publisher) Close() error                                  { return nil }

// subscriber consumes nothing until ctx ends.
type subscriber struct{}

func (subscriber) Consume(ctx context.Context, _ worker.ConsumerState) error {
	<-ctx.Done()
	return nil
}

func (subscriber) Close() error { return nil }

// Package worker handles the messages of a broker with two servers.
//
// The consumer, declared by a ConsumerConfig, receives the messages of its subscribers and dispatches them
// by name to typed handlers. An inbox records every message per subscriber, so that the attempts of a
// message are counted across deliveries and a message already handled is skipped. Handling a message is
// an operation of type OperationTypeMessageHandler, so the middlewares of every other server work on it
// too.
//
// The producer, declared by a ProducerConfig, is an outbox relay. It publishes the messages that the
// application wrote to its outbox in the transaction of the change that produced them, so that a message
// is published if and only if the change commits.
//
// Brokers and storages are interfaces: IBroker, InboxStorage and OutboxStorage. forgo-adapter implements
// them for RabbitMQ, Google Cloud Pub/Sub, Redis Streams, PostgreSQL and MySQL.
package worker

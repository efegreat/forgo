package operation

import "context"

// Transactor runs a function inside a transaction.
//
// Do must commit when fn returns nil and roll back otherwise. fn must be called
// with a context derived from ctx that carries the transaction, so that every
// repository or storage called with it can join the transaction.
type Transactor interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

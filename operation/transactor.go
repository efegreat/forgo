package operation

import "context"

type Transactor interface {
	Do(context.Context, func() error) error
}

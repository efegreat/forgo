package cron

import (
	"context"
	"forgo/operation"
)

const operationTypeCronJob = "job"

// IJob is an interface that defines the structure and behavior of a cron job. It includes methods for retrieving the job's name, tolerance, lock mechanism, schedule, and execution logic.
type IJob interface {
	Name() string
	Tolerance() operation.Tolerance
	Lock() operation.Lock
	Schedule() string
	Run(ctx context.Context) operation.Error
}

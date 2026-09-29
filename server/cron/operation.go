package cron

import (
	"context"
	"forgo/operation"
)

const OperationTypeCronJob = "cron_job"

// IJob is an interface that defines the structure and behavior of a cron job.
type IJob interface {
	Tolerance() operation.Tolerance
	Run(ctx context.Context) error
}

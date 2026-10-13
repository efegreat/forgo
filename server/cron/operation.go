package cron

import (
	"context"
)

// OperationTypeCronJob is the operation type of a job's run.
const OperationTypeCronJob = "cron_job"

// IJob is a job the cron server runs on its schedule.
type IJob interface {
	// Run does the work of one run. Its context is the run's *operation.Context. It is not cancelled
	// when the app starts terminating, so that the run can finish, but it is when the server's
	// TermTimeout runs out.
	Run(ctx context.Context) error
}

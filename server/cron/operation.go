package cron

import (
	"context"

	"github.com/efegreat/forgo/operation"
)

// OperationTypeCronJob is the operation type of a job's run.
const OperationTypeCronJob = "cron_job"

// IJob is a job the cron server runs on its schedule.
type IJob interface {
	// Tolerance returns how failed runs are retried, or operation.Tolerance{} for no retries. It is
	// read once, when the server is created, and its hooks can use the job's dependencies. The base
	// chain applies it, so DisableBaseMiddlewares ignores it.
	Tolerance() operation.Tolerance
	// Run does the work of one run. Its context is the run's *operation.Context. Terminating the server
	// does not cancel it, so that the run can finish, but killing the server, when the run outlasts its
	// TermTimeout, does.
	Run(ctx context.Context) error
}

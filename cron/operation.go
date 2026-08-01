package cron

import (
	"context"
	"forgo/operation"
)

const operationTypeCronJob = "job"

type IJob interface {
	Name() string
	Tolerance() operation.Tolerance
	Lock() operation.Lock
	Schedule() string
	Run(ctx context.Context) operation.Error
}

type JobOperation struct {
	job IJob
}

func (c JobOperation) ID() string {
	return ""
}

func (c JobOperation) Name() string {
	return c.job.Name()
}

func (c JobOperation) Type() string {
	return operationTypeCronJob
}

func (c JobOperation) Tolerance() operation.Tolerance {
	return c.job.Tolerance()
}

func (c JobOperation) Lock() operation.Lock {
	return c.job.Lock()
}

func (c JobOperation) BeforeRun(context.Context) operation.Error {
	return operation.Error{}
}

func (c JobOperation) Run(ctx context.Context) operation.Error {
	return c.job.Run(ctx)
}

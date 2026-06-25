package forgo

import (
	"context"
	"forgo/operation"
)

type ICronJob interface {
	Name() string
	Tolerance() operation.Tolerance
	Lock() operation.Lock
	Schedule() string
	Run(ctx context.Context) operation.Error
}

type CronJobOperation struct {
	job ICronJob
}

func (c CronJobOperation) ID() string {
	return ""
}

func (c CronJobOperation) Name() string {
	return c.job.Name()
}

func (c CronJobOperation) Type() operation.Type {
	return operation.TypeCronJob
}

func (c CronJobOperation) Tolerance() operation.Tolerance {
	return c.job.Tolerance()
}

func (c CronJobOperation) Lock() operation.Lock {
	return c.job.Lock()
}

func (c CronJobOperation) BeforeRun(context.Context) operation.Error {
	return operation.Error{}
}

func (c CronJobOperation) Run(ctx context.Context) operation.Error {
	return c.job.Run(ctx)
}

type CronServer struct {
}

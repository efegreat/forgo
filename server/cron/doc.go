// Package cron runs jobs on cron schedules, with robfig/cron v3. Each run of a job is an operation
// of type OperationTypeCronJob, so the middlewares of every other server work on it too.
//
// Declare the jobs in a Config and pass it to forgo.WithServers. A job is an IJob built by its
// constructor through As, which resolves the constructor's parameters from the bootstrap. A run of a
// job never overlaps another run of the same job in the process, and a job's Lock keeps it from
// overlapping across processes.
package cron

package worker

import (
	"context"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// deleteBefore deletes up to limit rows older than before and returns how many it deleted.
type deleteBefore func(ctx context.Context, before time.Time, limit int) (int64, error)

// cleanup deletes the rows older than c.MaxTTL every c.CleanupInterval until ctx ends. Full batches
// are deleted back to back. An error is logged and the deletion retried at the next interval, so a
// database outage does not stop the cleanup for the life of the process.
func cleanup(ctx context.Context, c CleanConfig, del deleteBefore) {
	if c.MaxTTL <= 0 {
		return
	}

	for {
		if err := cleanBatches(ctx, c, del); err != nil && ctx.Err() == nil {
			logger.FromCtx(ctx).Error().
				Err(err).
				Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
				Msg("cleanup failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.CleanupInterval):
		}
	}
}

// cleanBatches deletes batches until one is not full or ctx ends.
func cleanBatches(ctx context.Context, c CleanConfig, del deleteBefore) error {
	before := time.Now().Add(-c.MaxTTL)
	for ctx.Err() == nil {
		deleted, err := del(ctx, before, c.CleanupLimit)
		if err != nil {
			return err
		}
		if deleted < int64(c.CleanupLimit) {
			return nil
		}
	}
	return nil
}

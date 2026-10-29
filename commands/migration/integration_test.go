package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	sqlmigrate "github.com/rubenv/sql-migrate"
)

// Enable only against dedicated disposable test databases:
// MIGRATION_TEST_POSTGRES_DSN='...' MIGRATION_TEST_MYSQL_DSN='...?parseTime=true&loc=UTC' go test -run TestIntegration -v
// These tests create/drop uniquely named tables and acquire unique advisory locks.
func TestIntegration(t *testing.T) {
	for _, tc := range []struct {
		dialect Dialect
		env     string
	}{
		{Postgres, "MIGRATION_TEST_POSTGRES_DSN"}, {MySQL, "MIGRATION_TEST_MYSQL_DSN"},
	} {
		t.Run(string(tc.dialect), func(t *testing.T) {
			dsn := os.Getenv(tc.env)
			if dsn == "" {
				t.Skip("set " + tc.env + " for a dedicated test database")
			}
			db, err := sql.Open(string(tc.dialect), dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := db.PingContext(ctx); err != nil {
				t.Fatal(err)
			}
			id := time.Now().UnixNano()
			tracking := fmt.Sprintf("forgo_track_%d", id)
			users := fmt.Sprintf("forgo_users_%d", id)
			var locker Locker = NewPostgresLocker(id)
			if tc.dialect == MySQL {
				locker = NewMySQLLocker(fmt.Sprintf("forgo-test:%d", id))
			}
			src := &sqlmigrate.MemoryMigrationSource{Migrations: []*sqlmigrate.Migration{
				{Id: "001_users.sql", Up: []string{fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY)", users)}, Down: []string{"DROP TABLE " + users}},
				{Id: "002_seed.sql", Up: []string{"INSERT INTO " + users + " (id) VALUES (1)"}, Down: []string{"DELETE FROM " + users + " WHERE id = 1"}},
			}}
			m, err := New(db, src, locker, Config{Dialect: tc.dialect, Table: tracking, Timeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+users)
				db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+m.table)
			}()

			if plan, err := m.Plan(ctx, Up, 0); err != nil || len(plan) != 2 {
				t.Fatalf("plan=%+v, err=%v", plan, err)
			}
			if statuses, err := m.Status(ctx); err != nil || len(statuses) != 2 || statuses[0].State != StatusPending {
				t.Fatalf("status=%+v, err=%v", statuses, err)
			}
			// Inspection must not create the tracking table.
			if _, err := db.ExecContext(ctx, "SELECT 1 FROM "+m.table); err == nil {
				t.Fatal("read-only inspection created tracking table")
			}
			if count, err := m.Up(ctx, 0); err != nil || count != 2 {
				t.Fatalf("up=%d, err=%v", count, err)
			}
			if count, err := m.Up(ctx, 0); err != nil || count != 0 {
				t.Fatalf("second up=%d, err=%v", count, err)
			}
			if statuses, err := m.Status(ctx); err != nil || statuses[1].State != StatusApplied || statuses[1].AppliedAt == nil {
				t.Fatalf("status=%+v, err=%v", statuses, err)
			}
			if count, err := m.Down(ctx, 1); err != nil || count != 1 {
				t.Fatalf("down=%d, err=%v", count, err)
			}
			if count, err := m.Down(ctx, 1); err != nil || count != 1 {
				t.Fatalf("second down=%d, err=%v", count, err)
			}

			// Two live connections demonstrate cross-session advisory exclusion.
			db.SetMaxOpenConns(2)
			held, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			unlock, err := locker.TryLock(ctx, held)
			if err != nil || unlock == nil {
				t.Fatalf("hold lock: held=%v, err=%v", unlock != nil, err)
			}
			blockedCtx, blockedCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, blockedErr := m.Up(blockedCtx, 1)
			blockedCancel()
			if !errors.Is(blockedErr, context.DeadlineExceeded) {
				t.Fatalf("expected exclusion, got %v", blockedErr)
			}
			if err := unlock(ctx); err != nil {
				t.Fatal(err)
			}
			if count, err := m.Up(ctx, 1); err != nil || count != 1 {
				t.Fatalf("up after release=%d, err=%v", count, err)
			}

			// An applied migration edited in the source blocks up until it is rolled back.
			if count, err := m.Up(ctx, 0); err != nil || count != 1 {
				t.Fatalf("up of the seed=%d, err=%v", count, err)
			}
			src.Migrations[1].Up = []string{"INSERT INTO " + users + " (id) VALUES (2)"}
			src.Migrations[1].Down = []string{"DELETE FROM " + users + " WHERE id IN (1, 2)"}
			if statuses, err := m.Status(ctx); err != nil || statuses[0].State != StatusApplied || statuses[1].State != StatusModified {
				t.Fatalf("status after the edit=%+v, err=%v", statuses, err)
			}
			if count, err := m.Up(ctx, 0); count != 0 || !errors.Is(err, ErrModifiedMigrations) {
				t.Fatalf("up after the edit=%d, err=%v", count, err)
			}
			if count, err := m.Down(ctx, 1); err != nil || count != 1 {
				t.Fatalf("down of the edited seed=%d, err=%v", count, err)
			}
			if count, err := m.Up(ctx, 0); err != nil || count != 1 {
				t.Fatalf("up of the edited seed=%d, err=%v", count, err)
			}
			if statuses, err := m.Status(ctx); err != nil || statuses[1].State != StatusApplied {
				t.Fatalf("status after reapplying=%+v, err=%v", statuses, err)
			}
			var ids []int64
			rows, err := db.QueryContext(ctx, "SELECT id FROM "+users+" ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil || len(ids) != 1 || ids[0] != 2 {
				t.Fatalf("users after reapplying=%v, err=%v", ids, err)
			}
		})
	}
}

package migration

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	sqlmigrate "github.com/rubenv/sql-migrate"
)

type sourceFunc func() ([]*sqlmigrate.Migration, error)

func (f sourceFunc) FindMigrations() ([]*sqlmigrate.Migration, error) { return f() }

type lockerFunc func(context.Context, *sql.Conn) (UnlockFunc, error)

func (f lockerFunc) Lock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error) {
	return f(ctx, conn)
}

func testMigrator(t *testing.T, dialect string, source sqlmigrate.MigrationSource) (*Migrator, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	var locker Locker = NewPostgresLocker(42)
	if dialect == "mysql" {
		locker = NewMySQLLocker("test:migrations")
	}
	m, err := New(db, source, locker, Config{Dialect: dialect})
	if err != nil {
		t.Fatal(err)
	}
	return m, mock
}

func source() sqlmigrate.MigrationSource {
	return &sqlmigrate.MemoryMigrationSource{Migrations: []*sqlmigrate.Migration{
		{Id: "001_first.sql", Up: []string{"CREATE TABLE first (id BIGINT);"}, Down: []string{"DROP TABLE first;"}},
		{Id: "002_second.sql", Up: []string{"CREATE TABLE second (id BIGINT);"}, Down: []string{"DROP TABLE second;"}},
		{Id: "010_last.sql", Up: []string{"CREATE TABLE last_table (id BIGINT);"}, Down: []string{"DROP TABLE last_table;"}, DisableTransactionUp: true},
	}}
}

func expectRecords(mock sqlmock.Sqlmock, dialect string, exists bool, ids ...string) {
	if dialect == "mysql" {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ?)")).
			WithArgs("", "migrations").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
	} else {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT to_regclass($1) IS NOT NULL")).WithArgs(`"migrations"`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
	}
	if exists {
		rows := sqlmock.NewRows([]string{"id", "applied_at"})
		for _, id := range ids {
			rows.AddRow(id, time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC))
		}
		mock.ExpectQuery("SELECT .* FROM .*migrations").WillReturnRows(rows)
	}
}

func expectLock(mock sqlmock.Sqlmock, dialect string) {
	if dialect == "mysql" {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WithArgs("test:migrations").WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(1))
	} else {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock($1)")).WithArgs(int64(42)).WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(true))
	}
}

func expectUnlock(mock sqlmock.Sqlmock, dialect string) {
	if dialect == "mysql" {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs("test:migrations").WillReturnRows(sqlmock.NewRows([]string{"unlock"}).AddRow(1))
	} else {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock($1)")).WithArgs(int64(42)).WillReturnRows(sqlmock.NewRows([]string{"unlock"}).AddRow(true))
	}
}

func assertExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanReadOnlyOnEmptyDatabase(t *testing.T) {
	for _, dialect := range []string{"mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			m, mock := testMigrator(t, dialect, source())
			expectRecords(mock, dialect, false)
			plan, err := m.Plan(context.Background(), Up, 0)
			if err != nil || len(plan) != 3 {
				t.Fatalf("plan = %+v, %v", plan, err)
			}
			if plan[0].Statements[0] != "CREATE TABLE first (id BIGINT);" || plan[2].Transaction {
				t.Fatalf("unexpected plan: %+v", plan)
			}
			assertExpectations(t, mock) // No lock, DDL, begin, or SQL execution permitted.
		})
	}
}

func TestPlanLimitsIncludeEarlierPendingMigrations(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	expectRecords(mock, "postgres", true, "002_second.sql")
	plan, err := m.Plan(context.Background(), Up, 1)
	if err != nil || len(plan) != 1 || plan[0].ID != "001_first.sql" {
		t.Fatalf("plan = %+v, %v", plan, err)
	}
	assertExpectations(t, mock)
}

func TestDownPlanOnlyRollsBackAppliedMigrations(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	expectRecords(mock, "postgres", true, "001_first.sql", "010_last.sql")
	plan, err := m.Plan(context.Background(), Down, 10)
	if err != nil || len(plan) != 2 || plan[0].ID != "010_last.sql" || plan[1].ID != "001_first.sql" {
		t.Fatalf("plan = %+v, %v", plan, err)
	}
	if plan[0].Statements[0] != "DROP TABLE last_table;" {
		t.Fatalf("wrong down SQL: %+v", plan)
	}
	assertExpectations(t, mock)
}

func TestUnknownStatusVisibleAndPlanRejected(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	expectRecords(mock, "postgres", true, "001_first.sql", "999_missing.sql")
	statuses, err := m.Status(context.Background())
	if err != nil || len(statuses) != 4 || statuses[0].State != StatusApplied || statuses[0].AppliedAt == nil || statuses[1].State != StatusPending || statuses[3].State != StatusUnknown {
		t.Fatalf("statuses = %+v, %v", statuses, err)
	}
	expectRecords(mock, "postgres", true, "999_missing.sql")
	if _, err := m.Plan(context.Background(), Up, 0); err == nil || !strings.Contains(err.Error(), "999_missing.sql") {
		t.Fatalf("expected unknown migration error, got %v", err)
	}
	assertExpectations(t, mock)
}

func TestUpTransactionalAndNontransactional(t *testing.T) {
	for _, dialect := range []string{"mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			m, mock := testMigrator(t, dialect, source())
			expectLock(mock, dialect)
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
			expectRecords(mock, dialect, true, "002_second.sql")
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE first (id BIGINT);")).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("INSERT INTO .*migrations").WithArgs("001_first.sql", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE last_table (id BIGINT);")).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("INSERT INTO .*migrations").WithArgs("010_last.sql", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			expectUnlock(mock, dialect)
			count, err := m.Up(context.Background(), 0)
			if err != nil || count != 2 {
				t.Fatalf("count = %d, err = %v", count, err)
			}
			if m.db.Stats().InUse != 0 {
				t.Fatal("migration connection leaked")
			}
			assertExpectations(t, mock)
		})
	}
}

func TestPartialFailureRollsBackAndPreservesCount(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	failed := errors.New("DDL rejected")
	expectLock(mock, "postgres")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRecords(mock, "postgres", true)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE first").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO").WithArgs("001_first.sql", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE second").WillReturnError(failed)
	mock.ExpectRollback()
	expectUnlock(mock, "postgres")
	count, err := m.Up(context.Background(), 0)
	var executionErr *ExecutionError
	if count != 1 || !errors.Is(err, failed) || !errors.As(err, &executionErr) || executionErr.ID != "002_second.sql" || executionErr.Statement != 1 {
		t.Fatalf("count=%d, err=%v", count, err)
	}
	assertExpectations(t, mock)
}

func TestTrackingFailureRollsBackSQL(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	failed := errors.New("tracking insert failed")
	expectLock(mock, "postgres")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRecords(mock, "postgres", true)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE first").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO").WillReturnError(failed)
	mock.ExpectRollback()
	expectUnlock(mock, "postgres")
	count, err := m.Up(context.Background(), 1)
	if count != 0 || !errors.Is(err, failed) {
		t.Fatalf("count=%d, err=%v", count, err)
	}
	assertExpectations(t, mock)
}

func TestFailedCommitDiscardsSession(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	failed := errors.New("commit response lost")
	expectLock(mock, "postgres")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRecords(mock, "postgres", true)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE first").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(failed)
	mock.ExpectClose()
	count, err := m.Up(context.Background(), 1)
	// The physical session has been discarded; no unlock query is attempted.
	if count != 0 || !errors.Is(err, failed) {
		t.Fatalf("count=%d, err=%v", count, err)
	}
	assertExpectations(t, mock)
}

func TestFailedRollbackPreservedAndSessionDiscarded(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	sqlErr, rollbackErr := errors.New("SQL failed"), errors.New("rollback failed")
	expectLock(mock, "postgres")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRecords(mock, "postgres", true)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE first").WillReturnError(sqlErr)
	mock.ExpectRollback().WillReturnError(rollbackErr)
	mock.ExpectClose()
	count, err := m.Up(context.Background(), 1)
	if count != 0 || !errors.Is(err, sqlErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("count=%d, err=%v", count, err)
	}
	assertExpectations(t, mock)
}

func TestDownExecutesOnlyLastApplied(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	expectLock(mock, "postgres")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRecords(mock, "postgres", true, "001_first.sql", "010_last.sql")
	mock.ExpectBegin()
	mock.ExpectExec("DROP TABLE last_table").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DELETE FROM").WithArgs("010_last.sql").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectUnlock(mock, "postgres")
	count, err := m.Down(context.Background(), 1)
	if err != nil || count != 1 {
		t.Fatalf("count=%d, err=%v", count, err)
	}
	assertExpectations(t, mock)
}

func TestUnlockFailureReportedAndConnectionDiscarded(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	expectLock(mock, "postgres")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRecords(mock, "postgres", true, "001_first.sql", "002_second.sql", "010_last.sql")
	failed := errors.New("unlock response lost")
	mock.ExpectQuery("SELECT pg_advisory_unlock").WillReturnError(failed)
	mock.ExpectClose()
	count, err := m.Up(context.Background(), 0)
	if count != 0 || !errors.Is(err, failed) {
		t.Fatalf("count=%d, err=%v", count, err)
	}
	assertExpectations(t, mock)
}

func TestCleanupUsesUncancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), "trace", "kept"))
	defer cancel()
	m, mock := testMigrator(t, "postgres", source())
	cleaned := false
	m.locker = lockerFunc(func(ctx context.Context, conn *sql.Conn) (UnlockFunc, error) {
		cancel()
		return func(ctx context.Context) error {
			cleaned = true
			if ctx.Err() != nil || ctx.Value("trace") != "kept" {
				t.Errorf("invalid cleanup context: %v", ctx.Err())
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Error("cleanup must be bounded")
			}
			return nil
		}, nil
	})
	_, err := m.Up(ctx, 0)
	if !errors.Is(err, context.Canceled) || !cleaned {
		t.Fatalf("cleanup=%v, err=%v", cleaned, err)
	}
	assertExpectations(t, mock)
}

func TestValidationBeforeDatabaseAccess(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	for _, f := range []func() error{
		func() error { _, err := m.Up(context.Background(), -1); return err },
		func() error { _, err := m.Down(context.Background(), 0); return err },
		func() error { _, err := m.Plan(context.Background(), Direction(9), 0); return err },
		func() error { _, err := m.Plan(context.Background(), Down, 0); return err },
	} {
		if err := f(); err == nil {
			t.Fatal("expected validation failure")
		}
	}
	assertExpectations(t, mock)
}

func TestInvalidSourcesReturnErrors(t *testing.T) {
	for name, migrations := range map[string][]*sqlmigrate.Migration{
		"nil":       {nil},
		"empty":     {{Id: ""}},
		"duplicate": {{Id: "001_a.sql"}, {Id: "001_a.sql"}},
		"overflow":  {{Id: "99999999999999999999999_x.sql"}},
	} {
		t.Run(name, func(t *testing.T) {
			m, mock := testMigrator(t, "postgres", sourceFunc(func() ([]*sqlmigrate.Migration, error) { return migrations, nil }))
			if _, err := m.Plan(context.Background(), Up, 0); err == nil {
				t.Fatal("expected source error")
			}
			assertExpectations(t, mock)
		})
	}
}

func TestSourcePanicConvertedToError(t *testing.T) {
	m, mock := testMigrator(t, "postgres", sourceFunc(func() ([]*sqlmigrate.Migration, error) { panic("invalid source") }))
	if _, err := m.Status(context.Background()); err == nil || !strings.Contains(err.Error(), "source panic") {
		t.Fatalf("err=%v", err)
	}
	assertExpectations(t, mock)
}

func TestSQLParserPreservesStatementBlocks(t *testing.T) {
	input := `-- +migrate Up notransaction
-- +migrate StatementBegin
CREATE FUNCTION sample() RETURNS void AS $$
BEGIN
  PERFORM 1;
  PERFORM 2;
END;
$$ LANGUAGE plpgsql;
-- +migrate StatementEnd
-- +migrate Down
DROP FUNCTION sample();
`
	mig, err := sqlmigrate.ParseMigration("001_function.sql", strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	m, mock := testMigrator(t, "postgres", &sqlmigrate.MemoryMigrationSource{Migrations: []*sqlmigrate.Migration{mig}})
	expectRecords(mock, "postgres", false)
	plan, err := m.Plan(context.Background(), Up, 0)
	if err != nil || len(plan) != 1 || len(plan[0].Statements) != 1 || plan[0].Transaction || !strings.Contains(plan[0].Statements[0], "PERFORM 2;") {
		t.Fatalf("plan=%+v, err=%v", plan, err)
	}
	assertExpectations(t, mock)
}

func TestConfigRejectsUnsafeIdentifiers(t *testing.T) {
	m, _ := testMigrator(t, "postgres", source())
	for _, cfg := range []Config{
		{Dialect: "sqlite3"}, {Dialect: "postgres", Table: "x; DROP TABLE users"},
		{Dialect: "mysql", Schema: "a.b"}, {Dialect: "postgres", Timeout: -time.Second},
	} {
		if _, err := New(m.db, source(), m.locker, cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
}

package migration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	sqlmigrate "github.com/rubenv/sql-migrate"
)

const (
	defaultTable     = "migrations"
	lockPollInterval = 100 * time.Millisecond
	cleanupTimeout   = 5 * time.Second
)

var identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// driverDialects maps the package of a database/sql driver to its dialect. Matching the package
// path, rather than the driver type, keeps the drivers out of this package's imports.
var driverDialects = map[string]string{
	"github.com/lib/pq":              "postgres",
	"github.com/jackc/pgx/v5/stdlib": "postgres",
	"github.com/jackc/pgx/v4/stdlib": "postgres",
	"github.com/go-sql-driver/mysql": "mysql",
}

// Migrator applies, rolls back and inspects the migrations of one source on one database. It is
// safe for concurrent use, and runs in different processes take turns through its Locker.
type Migrator struct {
	db     *sql.DB
	source sqlmigrate.MigrationSource
	locker Locker
	cfg    Config
	table  string // Quoted, optionally schema-qualified.
}

// New returns a Migrator for db and source. It neither connects nor creates tables, and it never
// closes db, which the caller owns.
//
// A nil locker uses the dialect's default lock, which every Migrator built with a nil locker
// shares: NewMySQLLocker("forgo:migrations") or NewPostgresLocker(0x466F72476F). Migrators that
// target the same tracking table must use the same lock.
func New(db *sql.DB, source sqlmigrate.MigrationSource, locker Locker, cfg Config) (*Migrator, error) {
	if db == nil || source == nil {
		return nil, errors.New("migration: db and source are required")
	}
	if cfg.Dialect == "" {
		dialect, err := DetectDialect(db)
		if err != nil {
			return nil, err
		}
		cfg.Dialect = dialect
	}
	if cfg.Dialect != "mysql" && cfg.Dialect != "postgres" {
		return nil, fmt.Errorf("%w %q", ErrUnsupportedDialect, cfg.Dialect)
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("migration: timeout cannot be negative")
	}
	if cfg.Table == "" {
		cfg.Table = defaultTable
	}
	if !identifierPattern.MatchString(cfg.Table) || (cfg.Schema != "" && !identifierPattern.MatchString(cfg.Schema)) {
		return nil, errors.New("migration: table and schema must be ASCII SQL identifiers of at most 63 characters")
	}
	if locker == nil {
		locker = defaultLocker(cfg.Dialect)
	}
	m := &Migrator{db: db, source: source, locker: locker, cfg: cfg}
	m.table = m.quote(cfg.Table)
	if cfg.Schema != "" {
		m.table = m.quote(cfg.Schema) + "." + m.table
	}
	return m, nil
}

// DetectDialect returns the dialect of db's driver: "postgres" for github.com/lib/pq and the
// pgx stdlib driver, "mysql" for github.com/go-sql-driver/mysql. Any other driver, including one
// wrapped for tracing, returns an error wrapping ErrUnsupportedDialect: set Config.Dialect then.
func DetectDialect(db *sql.DB) (string, error) {
	if db == nil {
		return "", errors.New("migration: database is required")
	}
	if dialect, ok := driverDialects[driverPackage(db.Driver())]; ok {
		return dialect, nil
	}
	return "", fmt.Errorf("%w: cannot detect it from driver %T, set Config.Dialect", ErrUnsupportedDialect, db.Driver())
}

func driverPackage(d driver.Driver) string {
	t := reflect.TypeOf(d)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return ""
	}
	return t.PkgPath()
}

// Up applies pending migrations in source order: all of them when steps is zero, otherwise at
// most steps. A pending migration that sorts before applied ones is applied too. Up returns the
// number of migrations it completed, also when it fails.
//
// Cancelling ctx stops Up between migrations: the running migration and its tracking row
// complete first, so a stop never leaves a migration applied but unrecorded. Config.Timeout and
// a deadline on ctx interrupt the running migration too. Report progress with WithProgress.
func (m *Migrator) Up(ctx context.Context, steps int) (int, error) {
	return m.execute(ctx, Up, steps)
}

// Down rolls back at most steps applied migrations, in reverse source order, not in reverse
// order of application. steps must be positive; zero never means unlimited rollback. Down stops
// like Up and returns the number of migrations it rolled back.
func (m *Migrator) Down(ctx context.Context, steps int) (int, error) {
	return m.execute(ctx, Down, steps)
}

func validateSteps(direction Direction, steps int) error {
	if direction != Up && direction != Down {
		return fmt.Errorf("migration: invalid direction %d", direction)
	}
	if steps < 0 || (direction == Down && steps == 0) {
		return errors.New("migration: steps must be zero or positive for up, and positive for down")
	}
	return nil
}

func (m *Migrator) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if m.cfg.Timeout > 0 {
		return context.WithTimeout(ctx, m.cfg.Timeout)
	}
	return context.WithCancel(ctx)
}

// uncancelled returns a context with ctx's values and deadline that ignores ctx's cancellation.
func uncancelled(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(detached, deadline)
	}
	return context.WithCancel(detached)
}

func (m *Migrator) execute(ctx context.Context, direction Direction, steps int) (count int, err error) {
	if err = validateSteps(direction, steps); err != nil {
		return 0, err
	}
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	progress := progressFrom(ctx)
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("migration: acquire connection: %w", err)
	}
	defer conn.Close()
	unlock, err := m.lock(ctx, conn, progress)
	if err != nil {
		return 0, fmt.Errorf("migration: acquire lock: %w", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cleanupCancel()
		// A failed commit or rollback discards the session, which releases its lock with it.
		if unlockErr := unlock(cleanupCtx); unlockErr != nil && !errors.Is(unlockErr, sql.ErrConnDone) {
			_ = discardConnection(conn)
			err = errors.Join(err, fmt.Errorf("migration: release lock: %w", unlockErr))
		}
	}()

	// Parse before creating metadata, so bad source files cannot initialize it.
	migrations, err := m.findMigrations(ctx)
	if err != nil {
		return 0, err
	}
	if err = m.ensureTable(ctx, conn); err != nil {
		return 0, err
	}
	records, err := m.records(ctx, conn)
	if err != nil {
		return 0, err
	}
	plan, err := buildPlan(migrations, records, direction, steps)
	if err != nil {
		return 0, err
	}

	// Migrations run on a context that ignores cancellation, so a stop request ends the run
	// between migrations. A MySQL driver that cancels a statement closes the socket, and the
	// server may still finish the DDL, leaving it applied but unrecorded.
	runCtx, runCancel := uncancelled(ctx)
	defer runCancel()
	for _, item := range plan {
		if err = ctx.Err(); err != nil {
			return count, fmt.Errorf("migration: stopped after %d of %d migrations: %w", count, len(plan), err)
		}
		if progress.Start != nil {
			progress.Start(item)
		}
		started := time.Now()
		if err = m.apply(runCtx, conn, item); err != nil {
			return count, err
		}
		count++
		if progress.Done != nil {
			progress.Done(item, time.Since(started))
		}
	}
	return count, nil
}

// lock takes the lock on conn, trying again every lockPollInterval while another session holds it.
func (m *Migrator) lock(ctx context.Context, conn *sql.Conn, progress Progress) (UnlockFunc, error) {
	waiting := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		unlock, err := m.locker.TryLock(ctx, conn)
		if err != nil {
			// The server may have taken the lock even if its response was lost.
			// Conn.Close alone would return the session, and the lock, to the pool.
			_ = discardConnection(conn)
			return nil, err
		}
		if unlock != nil {
			return unlock, nil
		}
		if !waiting {
			waiting = true
			if progress.LockWait != nil {
				progress.LockWait()
			}
		}
		timer := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Plan is read-only, including against an empty database. It does not acquire
// an advisory lock or execute migration SQL. It is a snapshot, not a reservation.
func (m *Migrator) Plan(ctx context.Context, direction Direction, steps int) ([]PlannedMigration, error) {
	if err := validateSteps(direction, steps); err != nil {
		return nil, err
	}
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	migrations, err := m.findMigrations(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("migration: acquire connection: %w", err)
	}
	defer conn.Close()
	records, err := m.records(ctx, conn)
	if err != nil {
		return nil, err
	}
	return buildPlan(migrations, records, direction, steps)
}

// Status reports every migration of the source in source order, followed by applied migrations
// that the source does not have, as StatusUnknown. Like Plan, it is read-only and takes no lock.
func (m *Migrator) Status(ctx context.Context) ([]Status, error) {
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	migrations, err := m.findMigrations(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("migration: acquire connection: %w", err)
	}
	defer conn.Close()
	records, err := m.records(ctx, conn)
	if err != nil {
		return nil, err
	}
	result := make([]Status, 0, len(migrations)+len(records))
	for _, mig := range migrations {
		s := Status{ID: mig.Id, State: StatusPending}
		if at, ok := records[mig.Id]; ok {
			s.State, s.AppliedAt = StatusApplied, &at
			delete(records, mig.Id)
		}
		result = append(result, s)
	}
	unknown := make([]string, 0, len(records))
	for id := range records {
		unknown = append(unknown, id)
	}
	sort.Strings(unknown)
	for _, id := range unknown {
		at := records[id]
		result = append(result, Status{ID: id, State: StatusUnknown, AppliedAt: &at})
	}
	return result, nil
}

func (m *Migrator) findMigrations(ctx context.Context) (result []*sqlmigrate.Migration, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// sql-migrate's sources can panic while sorting an overflowing numeric ID.
	defer func() {
		if p := recover(); p != nil {
			result = nil
			err = fmt.Errorf("migration: source panicked: %v", p)
		}
	}()
	found, err := m.source.FindMigrations()
	if err != nil {
		return nil, fmt.Errorf("migration: find migrations: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(found))
	result = make([]*sqlmigrate.Migration, 0, len(found))
	for _, mig := range found {
		if mig == nil || mig.Id == "" || strings.TrimSpace(mig.Id) != mig.Id || len(mig.Id) > 255 || !utf8.ValidString(mig.Id) {
			return nil, errors.New("migration: source contains a nil migration or an invalid ID (maximum 255 bytes)")
		}
		if _, ok := seen[mig.Id]; ok {
			return nil, fmt.Errorf("migration: duplicate ID %q", mig.Id)
		}
		seen[mig.Id] = struct{}{}
		if matches := mig.NumberPrefixMatches(); len(matches) > 1 {
			if _, err := strconv.ParseInt(matches[1], 10, 64); err != nil {
				return nil, fmt.Errorf("migration: invalid numeric version %q: %w", mig.Id, err)
			}
		}
		copyMigration := *mig
		copyMigration.Up = append([]string(nil), mig.Up...)
		copyMigration.Down = append([]string(nil), mig.Down...)
		result = append(result, &copyMigration)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Less(result[j]) })
	return result, nil
}

func buildPlan(migrations []*sqlmigrate.Migration, records map[string]time.Time, direction Direction, steps int) ([]PlannedMigration, error) {
	known := make(map[string]struct{}, len(migrations))
	for _, mig := range migrations {
		known[mig.Id] = struct{}{}
	}
	unknown := make([]string, 0)
	for id := range records {
		if _, ok := known[id]; !ok {
			unknown = append(unknown, id)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("%w: %s", ErrUnknownMigrations, strings.Join(unknown, ", "))
	}
	result := make([]PlannedMigration, 0)
	for n := 0; n < len(migrations); n++ {
		i := n
		if direction == Down {
			i = len(migrations) - n - 1
		}
		mig := migrations[i]
		_, applied := records[mig.Id]
		if (direction == Up && applied) || (direction == Down && !applied) {
			continue
		}
		statements, transaction := mig.Up, !mig.DisableTransactionUp
		if direction == Down {
			statements, transaction = mig.Down, !mig.DisableTransactionDown
		}
		result = append(result, PlannedMigration{
			ID: mig.Id, Direction: direction, Transaction: transaction,
			Statements: append([]string(nil), statements...),
		})
		if steps > 0 && len(result) == steps {
			break
		}
	}
	return result, nil
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (m *Migrator) apply(ctx context.Context, conn *sql.Conn, item PlannedMigration) (err error) {
	wrap := func(statement int, err error) error {
		return &ExecutionError{ID: item.ID, Direction: item.Direction, Statement: statement, Err: err}
	}
	var exec executor = conn
	var tx *sql.Tx
	if item.Transaction {
		// The transaction ends through Commit or Rollback below, never through its context.
		// database/sql would roll it back on another goroutine when the context ends, racing the
		// unlock that reuses the session. Statements still run on ctx, deadline included.
		tx, err = conn.BeginTx(context.WithoutCancel(ctx), nil)
		if err != nil {
			return wrap(0, err)
		}
		defer func() {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = errors.Join(err, wrap(0, fmt.Errorf("rollback: %w", rollbackErr)))
				_ = discardConnection(conn)
			}
		}()
		exec = tx
	}
	for i, statement := range item.Statements {
		if _, err := exec.ExecContext(ctx, statement); err != nil {
			return wrap(i+1, err)
		}
	}
	var query string
	args := []any{item.ID}
	if item.Direction == Up {
		query = "INSERT INTO " + m.table + " (" + m.quote("id") + ", " + m.quote("applied_at") + ") VALUES (" + m.param(1) + ", " + m.param(2) + ")"
		args = append(args, time.Now().UTC())
	} else {
		query = "DELETE FROM " + m.table + " WHERE " + m.quote("id") + " = " + m.param(1)
	}
	if _, err := exec.ExecContext(ctx, query, args...); err != nil {
		return wrap(0, err)
	}
	if tx != nil {
		if err := tx.Commit(); err != nil {
			// A failed commit has an ambiguous outcome. Do not reuse the session.
			_ = discardConnection(conn)
			return wrap(0, err)
		}
	}
	return nil
}

func (m *Migrator) quote(identifier string) string {
	if m.cfg.Dialect == "mysql" {
		return "`" + identifier + "`"
	}
	return `"` + identifier + `"`
}

func (m *Migrator) param(n int) string {
	if m.cfg.Dialect == "mysql" {
		return "?"
	}
	return fmt.Sprintf("$%d", n)
}

func (m *Migrator) ensureTable(ctx context.Context, conn *sql.Conn) error {
	var query string
	if m.cfg.Dialect == "mysql" {
		query = "CREATE TABLE IF NOT EXISTS " + m.table + " (`id` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY, `applied_at` DATETIME(6) NOT NULL) ENGINE=InnoDB"
	} else {
		query = "CREATE TABLE IF NOT EXISTS " + m.table + ` ("id" VARCHAR(255) NOT NULL PRIMARY KEY, "applied_at" TIMESTAMPTZ NOT NULL)`
	}
	if _, err := conn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("migration: create tracking table: %w", err)
	}
	return nil
}

func (m *Migrator) records(ctx context.Context, conn *sql.Conn) (map[string]time.Time, error) {
	var exists bool
	var err error
	if m.cfg.Dialect == "mysql" {
		err = conn.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ?)",
			m.cfg.Schema, m.cfg.Table).Scan(&exists)
	} else {
		err = conn.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", m.table).Scan(&exists)
	}
	if err != nil {
		return nil, fmt.Errorf("migration: check tracking table: %w", err)
	}
	result := make(map[string]time.Time)
	if !exists {
		return result, nil
	}
	rows, err := conn.QueryContext(ctx, "SELECT "+m.quote("id")+", "+m.quote("applied_at")+" FROM "+m.table)
	if err != nil {
		return nil, fmt.Errorf("migration: read tracking table: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var at appliedAt
		if err := rows.Scan(&id, &at); err != nil {
			return nil, fmt.Errorf("migration: read tracking table: %w", err)
		}
		result[id] = at.Time
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migration: read tracking table: %w", err)
	}
	return result, nil
}

// appliedAtLayout is the text form of MySQL's DATETIME(6).
const appliedAtLayout = "2006-01-02 15:04:05.999999999"

// appliedAt scans the applied_at column. MySQL drivers return DATETIME as text unless the DSN
// sets parseTime=true, and the tracking table stores UTC.
type appliedAt struct{ time.Time }

func (a *appliedAt) Scan(value any) error {
	var text string
	switch v := value.(type) {
	case time.Time:
		a.Time = v
		return nil
	case []byte:
		text = string(v)
	case string:
		text = v
	default:
		return fmt.Errorf("unsupported applied_at value %T", value)
	}
	t, err := time.Parse(appliedAtLayout, text)
	if err != nil {
		return fmt.Errorf("parse applied_at: %w", err)
	}
	a.Time = t
	return nil
}

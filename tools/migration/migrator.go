package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	sqlmigrate "github.com/rubenv/sql-migrate"
)

var identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

type Migrator struct {
	db     *sql.DB
	source sqlmigrate.MigrationSource
	locker Locker
	cfg    Config
	table  string // Quoted, optionally schema-qualified.
}

// New does not connect or create tables. The caller owns db and its lifetime.
// A nonnil advisory locker is mandatory. Reuse the same lock identity for all
// migrators which target the same tracking table.
func New(db *sql.DB, source sqlmigrate.MigrationSource, locker Locker, cfg Config) (*Migrator, error) {
	if db == nil || source == nil || locker == nil {
		return nil, errors.New("db, migration source, and locker are required")
	}
	if cfg.Dialect != "mysql" && cfg.Dialect != "postgres" {
		return nil, fmt.Errorf("unsupported migration dialect %q", cfg.Dialect)
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("timeout cannot be negative")
	}
	if cfg.Table == "" {
		cfg.Table = "migrations"
	}
	if !identifierPattern.MatchString(cfg.Table) || (cfg.Schema != "" && !identifierPattern.MatchString(cfg.Schema)) {
		return nil, errors.New("table/schema must be ASCII SQL identifiers of at most 63 characters")
	}
	m := &Migrator{db: db, source: source, locker: locker, cfg: cfg}
	m.table = m.quote(cfg.Table)
	if cfg.Schema != "" {
		m.table = m.quote(cfg.Schema) + "." + m.table
	}
	return m, nil
}

// Up applies all pending migrations when steps is zero, otherwise at most steps.
func (m *Migrator) Up(ctx context.Context, steps int) (int, error) {
	return m.execute(ctx, Up, steps)
}

// Down rolls back at most steps applied migrations, in reverse source order.
// steps must be positive; zero never means unlimited rollback.
func (m *Migrator) Down(ctx context.Context, steps int) (int, error) {
	return m.execute(ctx, Down, steps)
}

func validateSteps(direction Direction, steps int) error {
	if direction != Up && direction != Down {
		return fmt.Errorf("invalid migration direction %d", direction)
	}
	if steps < 0 || (direction == Down && steps == 0) {
		return errors.New("steps must be nonnegative for up and positive for down")
	}
	return nil
}

func (m *Migrator) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if m.cfg.Timeout > 0 {
		return context.WithTimeout(ctx, m.cfg.Timeout)
	}
	return context.WithCancel(ctx)
}

func (m *Migrator) execute(ctx context.Context, direction Direction, steps int) (count int, err error) {
	if err = validateSteps(direction, steps); err != nil {
		return 0, err
	}
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Close()
	unlock, err := m.locker.Lock(ctx, conn)
	if err != nil {
		_ = discardConnection(conn)
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	if unlock == nil {
		_ = discardConnection(conn)
		return 0, errors.New("locker returned a nil unlock function")
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cleanupCancel()
		if unlockErr := unlock(cleanupCtx); unlockErr != nil {
			_ = discardConnection(conn)
			err = errors.Join(err, fmt.Errorf("release migration lock: %w", unlockErr))
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
	for _, item := range plan {
		if err = m.apply(ctx, conn, item); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
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
		return nil, err
	}
	defer conn.Close()
	records, err := m.records(ctx, conn)
	if err != nil {
		return nil, err
	}
	return buildPlan(migrations, records, direction, steps)
}

// Status reports applied IDs missing from the source as StatusUnknown.
func (m *Migrator) Status(ctx context.Context) ([]Status, error) {
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	migrations, err := m.findMigrations(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, err
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
			err = fmt.Errorf("migration source panic: %v", p)
		}
	}()
	found, err := m.source.FindMigrations()
	if err != nil {
		return nil, fmt.Errorf("find migrations: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(found))
	result = make([]*sqlmigrate.Migration, 0, len(found))
	for _, mig := range found {
		if mig == nil || mig.Id == "" || strings.TrimSpace(mig.Id) != mig.Id || len(mig.Id) > 255 || !utf8.ValidString(mig.Id) {
			return nil, errors.New("source contains a nil migration or invalid ID (maximum 255 bytes)")
		}
		if _, ok := seen[mig.Id]; ok {
			return nil, fmt.Errorf("duplicate migration ID %q", mig.Id)
		}
		seen[mig.Id] = struct{}{}
		if matches := mig.NumberPrefixMatches(); len(matches) > 1 {
			if _, err := strconv.ParseInt(matches[1], 10, 64); err != nil {
				return nil, fmt.Errorf("invalid numeric migration version %q: %w", mig.Id, err)
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
		return nil, fmt.Errorf("applied migrations missing from source: %s", strings.Join(unknown, ", "))
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
		tx, err = conn.BeginTx(ctx, nil)
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
		return fmt.Errorf("initialize migration table: %w", err)
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
		return nil, fmt.Errorf("check migration table: %w", err)
	}
	result := make(map[string]time.Time)
	if !exists {
		return result, nil
	}
	rows, err := conn.QueryContext(ctx, "SELECT "+m.quote("id")+", "+m.quote("applied_at")+" FROM "+m.table)
	if err != nil {
		return nil, fmt.Errorf("read migration records: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var at time.Time
		if err := rows.Scan(&id, &at); err != nil {
			return nil, fmt.Errorf("scan migration record (MySQL requires parseTime=true): %w", err)
		}
		result[id] = at
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read migration records: %w", err)
	}
	return result, nil
}

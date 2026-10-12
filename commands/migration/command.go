package migration

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/cli"

	sqlmigrate "github.com/rubenv/sql-migrate"
)

// action is the value of -a.
type action string

const (
	actionUp     action = "up"
	actionDown   action = "down"
	actionStatus action = "status"
	actionPlan   action = "plan"
	actionNew    action = "new"
)

const defaultDir = "migrations"

// Runner runs and inspects migrations for Command. Migrator implements it.
type Runner interface {
	Up(ctx context.Context, steps int) (int, error)
	Down(ctx context.Context, steps int) (int, error)
	Status(ctx context.Context) ([]Status, error)
	Plan(ctx context.Context, direction Direction, steps int) ([]PlannedMigration, error)
}

// FileCreator writes new migration files for the new action. Creator implements it.
type FileCreator interface {
	// Create writes the migration name to dir, or to its own directory when dir is empty, and
	// returns the path of the file.
	Create(dir, name string) (string, error)
}

// CommandConfig configures the migrate command that Factory builds. Its zero value migrates the
// only *sql.DB of the bootstrap with the files in ./migrations, and detects the dialect from
// the driver.
type CommandConfig struct {
	// Config configures the Migrator: dialect, tracking table and schema, and timeout.
	Config
	// Database names the *sql.DB resource to migrate. When empty, the bootstrap must hold
	// exactly one *sql.DB.
	Database string
	// Dir is the directory of the migration files, "migrations" by default. It is read from FS
	// when FS is set, and from disk, relative to the working directory, otherwise. The new
	// action always writes to Dir on disk.
	Dir string
	// FS holds the migration files, for example an embed.FS, so the binary does not need them on
	// disk.
	FS fs.FS
	// Locker keeps migrators from running at the same time. Nil uses the dialect's default lock
	// (see New).
	Locker Locker
	// Output receives results and progress. Nil means os.Stdout.
	Output io.Writer
}

func (c CommandConfig) withDefaults() CommandConfig {
	if c.Dir == "" {
		c.Dir = defaultDir
	}
	if c.Output == nil {
		c.Output = os.Stdout
	}
	return c
}

// Factory returns the cli.Factory of the migrate command that cfg describes:
//
//	{Name: "migrate", Command: migration.Factory(migration.CommandConfig{})}
//
// It resolves the database and builds the Migrator once, when the CLI resolves its command, and
// fails there when the configuration is invalid.
func Factory(cfg CommandConfig) cli.Factory {
	cfg = cfg.withDefaults()
	return func(r forgo.Resolver) (cli.CommandFactory, error) {
		db, err := cfg.database(r)
		if err != nil {
			return nil, err
		}
		source, err := cfg.source()
		if err != nil {
			return nil, err
		}
		migrator, err := New(db, source, cfg.Locker, cfg.Config)
		if err != nil {
			return nil, err
		}
		creator := NewCreator(cfg.Dir)
		return func() cli.Command { return NewCommand(migrator, creator, cfg.Output) }, nil
	}
}

func (c CommandConfig) database(r forgo.Resolver) (*sql.DB, error) {
	var db *sql.DB
	var err error
	if c.Database != "" {
		db, err = r.ResolveResource[*sql.DB](c.Database)
	} else {
		db, err = r.Resolve[*sql.DB]()
	}
	if err != nil {
		return nil, fmt.Errorf("migration: resolve database: %w", err)
	}
	return db, nil
}

func (c CommandConfig) source() (sqlmigrate.MigrationSource, error) {
	if c.FS == nil {
		return sqlmigrate.FileMigrationSource{Dir: c.Dir}, nil
	}
	sub, err := fs.Sub(c.FS, path.Clean(filepath.ToSlash(c.Dir)))
	if err != nil {
		return nil, fmt.Errorf("migration: directory %q in FS: %w", c.Dir, err)
	}
	return sqlmigrate.HttpFileSystemMigrationSource{FileSystem: http.FS(sub)}, nil
}

// Command is the migrate command. It runs the action chosen with -a on a Runner and prints the
// result. A Command holds the flags of one run: create one per run with NewCommand, or let
// Factory do it.
type Command struct {
	migrator  Runner
	creator   FileCreator
	output    io.Writer
	set       *flag.FlagSet
	action    string
	direction string
	steps     int
	name      string
	dir       string
}

// NewCommand returns a migrate command. A nil output means os.Stdout. Dependencies are only
// required by the actions that use them: new needs creator, every other action needs migrator.
func NewCommand(migrator Runner, creator FileCreator, output io.Writer) *Command {
	if output == nil {
		output = os.Stdout
	}
	return &Command{migrator: migrator, creator: creator, output: output}
}

// Init declares the flags of the migrate command.
func (c *Command) Init(set *flag.FlagSet) {
	c.set = set
	set.StringVar(&c.action, "a", "", "action: up, down, status, plan, new (required)")
	set.StringVar(&c.action, "action", "", "alias for -a")
	set.IntVar(&c.steps, "steps", 0, "up/plan-up: 0 = all; down/plan-down: omitted = 1, explicit value must be positive")
	set.StringVar(&c.direction, "direction", "up", "plan direction: up or down")
	set.StringVar(&c.name, "name", "", "new migration name: letters, digits and underscores, starting with a letter")
	set.StringVar(&c.dir, "dir", "", "new migration directory (optional override)")
}

// Validate rejects a missing or unknown action, flags the action does not use, positional
// arguments and invalid values. It defaults -steps to 1 for down.
func (c *Command) Validate() error {
	if c.set == nil || !c.set.Parsed() {
		return errors.New("migration: command must be initialized and parsed before validation")
	}
	if c.set.NArg() != 0 {
		return errors.New("migration: unexpected positional arguments; pass the action with -a and the migration name with -name")
	}
	var allowed map[string]bool
	switch action(c.action) {
	case actionUp, actionDown:
		allowed = map[string]bool{"steps": true}
	case actionStatus:
		allowed = map[string]bool{}
	case actionPlan:
		allowed = map[string]bool{"steps": true, "direction": true}
	case actionNew:
		allowed = map[string]bool{"name": true, "dir": true}
	case "":
		return errors.New("migration: action required: use -a=up|down|status|plan|new")
	default:
		return fmt.Errorf("migration: invalid action %q: expected up, down, status, plan or new", c.action)
	}
	allowed["a"], allowed["action"] = true, true
	var invalid []string
	explicitSteps := false
	c.set.Visit(func(f *flag.Flag) {
		if !allowed[f.Name] {
			invalid = append(invalid, "-"+f.Name)
		}
		if f.Name == "steps" {
			explicitSteps = true
		}
	})
	if len(invalid) > 0 {
		return fmt.Errorf("migration: flags %s cannot be used with action %q", strings.Join(invalid, ", "), c.action)
	}
	if action(c.action) == actionNew {
		if strings.TrimSpace(c.name) == "" {
			return errors.New("migration: name required: use -name=create_users")
		}
		if c.creator == nil {
			return errors.New("migration: creator is required for new")
		}
		return nil
	}
	if c.migrator == nil {
		return fmt.Errorf("migration: migrator is required for %s", c.action)
	}
	if action(c.action) == actionStatus {
		return nil
	}
	if c.steps < 0 {
		return errors.New("migration: steps cannot be negative")
	}
	if action(c.action) == actionPlan && c.direction != "up" && c.direction != "down" {
		return errors.New("migration: plan direction must be up or down")
	}
	if action(c.action) == actionDown || (action(c.action) == actionPlan && c.direction == "down") {
		if !explicitSteps {
			c.steps = 1
		} else if c.steps <= 0 {
			return errors.New("migration: explicit steps must be greater than zero for down")
		}
	}
	return nil
}

// Run runs the action. It validates again, so a Command run outside the CLI cannot skip
// Validate.
func (c *Command) Run(ctx context.Context) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch action(c.action) {
	case actionUp, actionDown:
		return c.migrate(ctx)
	case actionStatus:
		return c.status(ctx)
	case actionPlan:
		return c.plan(ctx)
	case actionNew:
		path, err := c.creator.Create(c.dir, c.name)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(c.output, "created migration: %s\n", path)
		return err
	}
	return fmt.Errorf("migration: unsupported action %q", c.action)
}

// migrate runs up or down and prints each migration as it runs.
func (c *Command) migrate(ctx context.Context) error {
	out := &lockedWriter{w: c.output}
	// The first signal stops the run after the running migration. Say so, or the signal looks
	// ignored and a second one kills the process in the middle of a migration.
	stopNotice := context.AfterFunc(ctx, func() {
		_, _ = fmt.Fprintln(out, "stop requested: stopping after the running migration")
	})
	defer stopNotice()

	starting, done := "applying", "applied"
	if action(c.action) == actionDown {
		starting, done = "rolling back", "rolled back"
	}
	ctx = WithProgress(ctx, Progress{
		LockWait: func() {
			_, _ = fmt.Fprintln(out, "waiting for the migration lock, held by another run")
		},
		Start: func(m PlannedMigration) {
			_, _ = fmt.Fprintf(out, "%s %s\n", starting, m.ID)
		},
		Done: func(m PlannedMigration, elapsed time.Duration) {
			_, _ = fmt.Fprintf(out, "%s %s (%s)\n", done, m.ID, elapsed.Round(time.Millisecond))
		},
	})

	var count int
	var err error
	if action(c.action) == actionUp {
		count, err = c.migrator.Up(ctx, c.steps)
	} else {
		count, err = c.migrator.Down(ctx, c.steps)
	}
	// Report confirmed progress even when a later migration fails.
	noun := "migrations"
	if count == 1 {
		noun = "migration"
	}
	_, outputErr := fmt.Fprintf(out, "%s: %d %s completed\n", c.action, count, noun)
	return errors.Join(err, outputErr)
}

func (c *Command) status(ctx context.Context) error {
	statuses, err := c.migrator.Status(ctx)
	if err != nil {
		return err
	}
	if len(statuses) == 0 {
		_, err := fmt.Fprintln(c.output, "no migrations")
		return err
	}
	w := tabwriter.NewWriter(c.output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "ID\tSTATE\tAPPLIED AT"); err != nil {
		return err
	}
	for _, s := range statuses {
		at := "-"
		if s.AppliedAt != nil {
			at = s.AppliedAt.UTC().Format(time.RFC3339Nano)
		}
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\n", s.ID, s.State, at); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (c *Command) plan(ctx context.Context) error {
	direction := Up
	if c.direction == "down" {
		direction = Down
	}
	plan, err := c.migrator.Plan(ctx, direction, c.steps)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(c.output, "Direction: %s\nMigrations: %d\n", strings.ToUpper(direction.String()), len(plan)); err != nil {
		return err
	}
	for n, item := range plan {
		transaction := "no"
		if item.Transaction {
			transaction = "requested (MySQL DDL may implicitly commit)"
		}
		if _, err := fmt.Fprintf(c.output, "\n[%d] %s\n    Transaction: %s\n", n+1, item.ID, transaction); err != nil {
			return err
		}
		if len(item.Statements) == 0 {
			if _, err := fmt.Fprintln(c.output, "    No SQL statements (tracking update only)."); err != nil {
				return err
			}
		}
		for i, statement := range item.Statements {
			// Add indentation for display; the migrator executes the original text.
			sql := strings.ReplaceAll(strings.TrimSpace(statement), "\n", "\n       ")
			if _, err := fmt.Fprintf(c.output, "\n    %d. %s\n", i+1, sql); err != nil {
				return err
			}
		}
	}
	return nil
}

// lockedWriter serializes writes: the stop notice is written from its own goroutine.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

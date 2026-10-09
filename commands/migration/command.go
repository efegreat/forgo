package migration

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/efegreat/forgo/cli"

	sqlmigrate "github.com/rubenv/sql-migrate"
)

type Action string

const (
	ActionUp     Action = "up"
	ActionDown   Action = "down"
	ActionStatus Action = "status"
	ActionPlan   Action = "plan"
	ActionNew    Action = "new"
)

type Runner interface {
	Up(context.Context, int) (int, error)
	Down(context.Context, int) (int, error)
	Status(context.Context) ([]Status, error)
	Plan(context.Context, Direction, int) ([]PlannedMigration, error)
}

type FileCreator interface {
	Create(dir, name string) (string, error)
}

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

// NewDefaultMigrateCommand returns the default migrate command. Dialect is detected by DetectDialect. Any error will panic.
func NewDefaultMigrateCommand(db *sql.DB) cli.CommandFactory {
	dialect, err := DetectDialect(db)
	if err != nil {
		panic(err)
	}

	var locker Locker

	switch dialect {
	case "mysql":
		locker = NewMySQLLocker("forgo:migrations")
	case "postgres":
		locker = NewPostgresLocker(0x466F72476F)
	}

	const dir = "./migrations"

	migrator, err := New(
		db,
		&sqlmigrate.FileMigrationSource{Dir: dir},
		locker,
		Config{Dialect: dialect},
	)
	if err != nil {
		panic(err)
	}

	return func() cli.Command {
		return NewCommand(
			migrator,
			NewCreator(dir),
			os.Stdout,
		)
	}
}

// NewCommand returns a new cli.Command. A nil output defaults to stdout.
// Dependencies are only required for their corresponding actions: new needs
// creator; all other actions need migrator.
func NewCommand(migrator Runner, creator FileCreator, output io.Writer) cli.Command {
	if output == nil {
		output = os.Stdout
	}
	return &Command{migrator: migrator, creator: creator, output: output}
}

func (c *Command) Init(set *flag.FlagSet) {
	c.set = set
	set.StringVar(&c.action, "a", "", "action: up, down, status, plan, new (required)")
	set.StringVar(&c.action, "action", "", "alias for -a")
	set.IntVar(&c.steps, "steps", 0, "up/plan-up: 0 = all; down/plan-down: omitted = 1, explicit value must be positive")
	set.StringVar(&c.direction, "direction", "up", "plan direction: up or down")
	set.StringVar(&c.name, "name", "", "new migration name")
	set.StringVar(&c.dir, "dir", "", "new migration directory (optional override)")
}

func (c *Command) Validate() error {
	if c.set == nil || !c.set.Parsed() {
		return errors.New("migration command must be initialized and parsed before validation")
	}
	if c.set.NArg() != 0 {
		return errors.New("unexpected positional arguments; pass the action using -a and the migration name using -name")
	}
	var allowed map[string]bool
	switch Action(c.action) {
	case ActionUp, ActionDown:
		allowed = map[string]bool{"steps": true}
	case ActionStatus:
		allowed = map[string]bool{}
	case ActionPlan:
		allowed = map[string]bool{"steps": true, "direction": true}
	case ActionNew:
		allowed = map[string]bool{"name": true, "dir": true}
	case "":
		return errors.New("migration action required: use -a=up|down|status|plan|new")
	default:
		return fmt.Errorf("invalid migration action %q: expected up, down, status, plan, or new", c.action)
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
		return fmt.Errorf("flags %s cannot be used with action %q", strings.Join(invalid, ", "), c.action)
	}
	if Action(c.action) == ActionNew {
		if strings.TrimSpace(c.name) == "" {
			return errors.New("migration name required: use -name=create_users")
		}
		if c.creator == nil {
			return errors.New("migration creator is required for new")
		}
		return nil
	}
	if c.migrator == nil {
		return errors.New("migrator is required for this action")
	}
	if Action(c.action) == ActionStatus {
		return nil
	}
	if c.steps < 0 {
		return errors.New("steps cannot be negative")
	}
	if Action(c.action) == ActionPlan && c.direction != "up" && c.direction != "down" {
		return errors.New("plan direction must be up or down")
	}
	if Action(c.action) == ActionDown || (Action(c.action) == ActionPlan && c.direction == "down") {
		if !explicitSteps {
			c.steps = 1
		} else if c.steps <= 0 {
			return errors.New("explicit steps must be greater than zero for down")
		}
	}
	return nil
}

func (c *Command) Run(ctx context.Context) error {
	// Preserve validation if Run is called directly rather than through ForGo.
	if err := c.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch Action(c.action) {
	case ActionUp, ActionDown:
		var count int
		var err error
		if Action(c.action) == ActionUp {
			count, err = c.migrator.Up(ctx, c.steps)
		} else {
			count, err = c.migrator.Down(ctx, c.steps)
		}
		// Report confirmed progress even when a later migration fails.
		_, outputErr := fmt.Fprintf(c.output, "%s: %d migrations completed\n", c.action, count)
		return errors.Join(err, outputErr)
	case ActionStatus:
		return c.status(ctx)
	case ActionPlan:
		return c.plan(ctx)
	case ActionNew:
		path, err := c.creator.Create(c.dir, c.name)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(c.output, "created migration: %s\n", path)
		return err
	}
	return fmt.Errorf("unsupported migration action %q", c.action)
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

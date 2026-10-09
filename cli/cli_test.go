package cli

import (
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

type nameCommand struct{ name string }

func (c *nameCommand) Init(fs *flag.FlagSet) { fs.StringVar(&c.name, "name", "", "") }
func (c *nameCommand) Validate() error {
	if c.name == "" {
		return errors.New("-name is required")
	}
	return nil
}
func (c *nameCommand) Run(context.Context) error { return nil }

func newNameCommand() CommandFactory { return func() Command { return &nameCommand{} } }

func TestResolveLockRequiresLocker(t *testing.T) {
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer boot.Close()

	c := New()
	cmd := CommandConfig{Name: "locked", Lock: operation.Lock{Key: "cmd", Ttl: time.Minute}, Command: As(newNameCommand)}
	if _, err := c.resolve(cmd, r); err == nil || !strings.Contains(err.Error(), "operation.ILocker") {
		t.Fatalf("error %v, want missing locker", err)
	}
}

func TestCommandErrorsHaveMessages(t *testing.T) {
	c := New()
	cmd := resolvedCommand{Name: "greet", Command: newNameCommand()}
	run := c.createHandler(context.Background(), cmd)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"app", "greet", "-unknown"}, "invalid arguments: "},
		{[]string{"app", "greet"}, "invalid command: -name is required"},
	} {
		err := run(tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("args %v: error %q, want it to contain %q", tc.args, err, tc.want)
		}
	}
}

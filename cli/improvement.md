# cli: improvements

Findings from the pre-v1 review of `cli/`.

## Must have

None.

## Should have

None.

## Could have

### Command descriptions and a usage listing

Add `CommandConfig.Description`, and print the commands with their descriptions to stderr when the command is missing, unknown, or `help`. Today the lookup error lists only the names.

### Every command bootstraps every resource

`Run` builds every resource and provider, even for commands that use none of them. `openapi` needs no database, but generating documents in CI fails when a database or Redis resource pings on startup. `-h` and flag errors also surface only after the bootstrap, because the flags are declared by a `Command` built from resolved dependencies.

Options, simplest first:

- Document the workaround: run such commands from a second `cli.New` without resources, chosen in `main` by `os.Args[1]`.
- A per-command opt-out of the bootstrap. This adds API surface, and a command that opts out must have a constructor without dependencies.

### Positional arguments are not documented

A command that takes positional arguments keeps the `*flag.FlagSet` it gets in `Init` and reads `fs.Args()` after parsing, as `commands/migration` does. Add one line to REFERENCE.md's "Writing a command".

### A recovered panic puts the stack trace in the error message

The base chain uses `middleware.Recovery(nil)`, which returns `panic: <value>, stack: <stack>`. A `main` that prints `err` dumps the whole stack. Log the stack and return a short `operation.Error` (for example code `command_panic`) instead.

### Exit codes

Show in the `main` example how to exit 2 for usage errors, as `flag` does: `ErrNoCommand`, `ErrUnknownCommand`, and `operation.Error`s with `StatusInvalidArgument` (`command_parse`, `command_validate`).

### `resolve` stores the locker on the CLI

`CLI.resolve` sets `c.locker` as a side effect (`cli/cli.go`), and `createHandler` reads it back. Carry the locker in `resolvedCommand` instead, so both functions only use their inputs.

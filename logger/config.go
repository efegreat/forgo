// Package logger configures forgo's zerolog logger and carries it through contexts.
//
// App and CLI create the logger from a Config and put it into the context of every server and
// operation, each layer adding the fields that trace where a line comes from: app, server,
// operation. Code takes it with FromCtx. Every key forgo logs under is a constant of this package.
//
// Importing the package sets zerolog.TimeFieldFormat to RFC 3339 with microseconds and the zone
// offset. zerolog reads it on every event, so it is set once, before main runs. Set it in main to
// change it.
package logger

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	// timeFieldFormat is RFC 3339 with microseconds and the zone offset ("Z" when the process runs in UTC).
	timeFieldFormat = "2006-01-02T15:04:05.000000Z07:00"

	// fallbackFileName is the log file when the executable's name is unknown.
	fallbackFileName = "app.log"
)

func init() {
	zerolog.TimeFieldFormat = timeFieldFormat
}

// Config configures the logger of an App or a CLI. The zero value writes JSON lines to stdout at
// debug level.
type Config struct {
	// Level is the minimum level logged. The zero value is zerolog.DebugLevel.
	Level zerolog.Level

	// Caller adds the file and line of the log call to every entry, at the cost of a runtime.Caller
	// per entry.
	Caller bool

	// StaticFields are added to every entry, for example "service", "env" or "version".
	StaticFields map[string]string

	// Output replaces os.Stdout as the destination of the JSON lines, or of the console output when
	// Console is enabled. Nil means os.Stdout, except in a CLI, where it means os.Stderr so that logs
	// do not mix with a command's output. File is not affected. Tests set a buffer here to read the logs.
	Output io.Writer

	// Console configures human-readable output, for development.
	Console ConsoleConfig

	// File configures rotating log files.
	File FileConfig
}

// New returns the logger c describes, with a timestamp on every entry.
//
// It writes JSON lines to Output. Console replaces them with human-readable output on Output, and
// File writes JSON lines to rotating files, in place of Output when Console is disabled.
func (c Config) New() zerolog.Logger {
	ctx := zerolog.
		New(c.writer()).
		Level(c.Level).
		With().
		Timestamp()

	for key, value := range c.StaticFields {
		ctx = ctx.Str(key, value)
	}

	if c.Caller {
		ctx = ctx.Caller()
	}

	return ctx.Logger()
}

func (c Config) writer() io.Writer {
	output := c.Output
	if output == nil {
		output = os.Stdout
	}

	var writers []io.Writer

	if c.Console.Enabled {
		writers = append(writers, zerolog.ConsoleWriter{
			Out:     output,
			NoColor: c.Console.NoColor,
		})
	}

	if c.File.Enabled {
		writers = append(writers, &lumberjack.Logger{
			Filename:   c.File.filename(),
			MaxSize:    c.File.MaxSize,
			MaxBackups: c.File.MaxBackups,
			MaxAge:     c.File.MaxAge,
			Compress:   c.File.Compress,
		})
	}

	switch len(writers) {
	case 0:
		return output

	case 1:
		return writers[0]

	default:
		return zerolog.MultiLevelWriter(writers...)
	}
}

// ConsoleConfig configures human-readable output to Config.Output. It is slower than JSON lines,
// since zerolog decodes every line to format it.
type ConsoleConfig struct {
	// Enabled writes human-readable output instead of JSON lines.
	Enabled bool

	// NoColor disables ANSI colors, for outputs that are not a terminal.
	NoColor bool
}

// FileConfig configures logging to rotating files of JSON lines, through lumberjack.
type FileConfig struct {
	// Enabled enables logging to files.
	Enabled bool

	// Filename is the file written to. Rotated files are kept next to it, with a timestamp added to
	// the name. A relative path is relative to the working directory. Empty means "<program>.log",
	// where <program> is the name of the executable without ".exe", so that a server and a CLI built
	// from one project write to different files: two loggers rotating one file lose lines.
	Filename string

	// MaxSize is the size in megabytes at which the file is rotated. Zero means 100.
	MaxSize int

	// MaxBackups is the number of rotated files kept. Zero keeps all of them, so set MaxBackups or
	// MaxAge to bound the disk the logs take.
	MaxBackups int

	// MaxAge is the number of days a rotated file is kept, by the timestamp in its name. Zero keeps
	// them whatever their age.
	MaxAge int

	// Compress gzips rotated files.
	Compress bool
}

func (c FileConfig) filename() string {
	if c.Filename != "" {
		return c.Filename
	}

	var program string
	if len(os.Args) > 0 {
		program = os.Args[0]
	}
	return defaultFileName(program)
}

// defaultFileName returns "<name>.log" for the executable at path, or fallbackFileName when path
// names none.
func defaultFileName(path string) string {
	name := strings.TrimSuffix(filepath.Base(path), ".exe")
	if name == "" || name == "." || name == string(filepath.Separator) {
		return fallbackFileName
	}
	return name + ".log"
}

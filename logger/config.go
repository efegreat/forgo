package logger

import (
	"io"
	"os"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

const defaultFileName = "log"

type Config struct {
	// Level is the minimum log level.
	Level zerolog.Level

	// Caller adds the source file and line number to log entries.
	Caller bool

	// StaticFields are fields that are added to every log entry.
	//
	// Examples:
	//   service: payment
	//   env: production
	//   version: 1.2.0
	StaticFields map[string]string

	// Console configures human-readable console logging.
	Console ConsoleConfig

	// File configures rotating JSON file logging.
	File FileConfig
}

func (c Config) New() zerolog.Logger {
	// RFC 3339 with microseconds and the zone offset ("Z" when the process runs in UTC).
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000000Z07:00"

	writer := c.writer()

	ctx := zerolog.
		New(writer).
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
	var writers []io.Writer

	if c.Console.Enabled {
		writers = append(writers, zerolog.ConsoleWriter{
			Out:     os.Stdout,
			NoColor: c.Console.NoColor,
		})
	}

	if c.File.Enabled {
		writers = append(writers, &lumberjack.Logger{
			Filename:   c.File.filename(),
			MaxSize:    c.File.MaxSize,
			MaxBackups: c.File.MaxBackup,
			MaxAge:     c.File.MaxAge,
			Compress:   c.File.Compress,
		})
	}

	switch len(writers) {
	case 0:
		return os.Stdout

	case 1:
		return writers[0]

	default:
		return zerolog.MultiLevelWriter(writers...)
	}
}

type ConsoleConfig struct {
	// Enabled enables human-readable console output.
	Enabled bool

	// NoColor disables ANSI color output.
	NoColor bool
}

type FileConfig struct {
	// Enabled enables logging to a rotating file via lumberjack.
	Enabled bool

	// NameFunc generates the log file name.
	NameFunc func(FileConfig) string

	// MaxSize is the maximum size of a log file in megabytes
	// before it is rotated.
	MaxSize int

	// MaxBackup is the maximum number of old log files to retain.
	MaxBackup int

	// MaxAge is the maximum number of days to retain old log files.
	MaxAge int

	// Compress determines whether rotated log files are compressed.
	Compress bool
}

func (c FileConfig) filename() string {
	if c.NameFunc != nil {
		return c.NameFunc(c)
	}

	return defaultFileName
}

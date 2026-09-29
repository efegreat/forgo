package logger

import (
	"io"
	"os"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	Name string // logger identifier
	File FileConfig
}

func (c Config) New() zerolog.Logger {
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000000Z"
	if c.Name == "" {
		c.Name = defaultLoggerName
	}

	var writer io.Writer
	if c.File.Enabled {
		writer = &lumberjack.Logger{
			Filename:   c.File.NameFunc(c.File),
			MaxSize:    c.File.MaxSize,
			MaxBackups: c.File.MaxBackup,
			MaxAge:     c.File.MaxAge,
			Compress:   c.File.Compress,
		}
	} else {
		writer = os.Stdout
	}

	return zerolog.New(writer).With().Str(NameLogKey, c.Name).Timestamp().Logger()
}

type FileConfig struct {
	Enabled   bool                    // if enabled, logs will be written to the rotating file via lumberjack.
	NameFunc  func(FileConfig) string // func for generating file name
	MaxSize   int                     // in MBs
	MaxBackup int
	MaxAge    int // in days
	Compress  bool
}

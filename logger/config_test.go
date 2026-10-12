package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestConfigNewOutputs(t *testing.T) {
	tests := []struct {
		name       string
		console    bool
		file       bool
		wantOutput string // "json", "console" or "" for nothing
	}{
		{name: "none", wantOutput: "json"},
		{name: "console", console: true, wantOutput: "console"},
		{name: "file", file: true},
		{name: "console and file", console: true, file: true, wantOutput: "console"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			filename := filepath.Join(t.TempDir(), "app.log")
			cfg := Config{
				Output:  &out,
				Console: ConsoleConfig{Enabled: tt.console, NoColor: true},
				File:    FileConfig{Enabled: tt.file, Filename: filename},
			}

			lg := cfg.New()
			lg.Info().Msg("hello")

			switch tt.wantOutput {
			case "json":
				decodeLine(t, out.Bytes())
			case "console":
				if got := out.String(); !strings.Contains(got, "INF hello") || strings.HasPrefix(got, "{") {
					t.Errorf("output = %q, want console output", got)
				}
			default:
				if out.Len() != 0 {
					t.Errorf("output = %q, want nothing", out.String())
				}
			}

			data, err := os.ReadFile(filename)
			switch {
			case tt.file && err != nil:
				t.Fatalf("log file not written: %v", err)
			case tt.file:
				decodeLine(t, data)
			case !os.IsNotExist(err):
				t.Errorf("log file written without File, err = %v", err)
			}
		})
	}
}

func TestConfigNewFields(t *testing.T) {
	timestamp := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}(Z|[+-]\d{2}:\d{2})$`)

	var out bytes.Buffer
	lg := Config{
		Caller:       true,
		StaticFields: map[string]string{"service": "payment", "env": "production"},
		Output:       &out,
	}.New()
	lg.Debug().Msg("hello") // the zero level is debug

	line := decodeLine(t, out.Bytes())
	if line["level"] != "debug" {
		t.Errorf("level = %v, want debug", line["level"])
	}
	if line["service"] != "payment" || line["env"] != "production" {
		t.Errorf("static fields missing: %v", line)
	}
	if caller, _ := line["caller"].(string); !strings.Contains(caller, "logger/config_test.go:") {
		t.Errorf("caller = %q, want this file", caller)
	}
	if ts, _ := line["time"].(string); !timestamp.MatchString(ts) {
		t.Errorf("time = %q, want RFC 3339 with microseconds", ts)
	}
}

func TestConfigNewLevel(t *testing.T) {
	var out bytes.Buffer
	lg := Config{Level: zerolog.WarnLevel, Output: &out}.New()
	lg.Info().Msg("dropped")
	lg.Warn().Msg("kept")

	if line := decodeLine(t, out.Bytes()); line["message"] != "kept" {
		t.Errorf("logged %v, want only the warn line", line)
	}
}

// TestConfigNewKeepsTimeFieldFormat checks that New leaves the zerolog global alone, so that main can
// override the format set at import.
func TestConfigNewKeepsTimeFieldFormat(t *testing.T) {
	if zerolog.TimeFieldFormat != timeFieldFormat {
		t.Fatalf("TimeFieldFormat = %q after import, want %q", zerolog.TimeFieldFormat, timeFieldFormat)
	}
	t.Cleanup(func() { zerolog.TimeFieldFormat = timeFieldFormat })
	zerolog.TimeFieldFormat = time.RFC3339

	var out bytes.Buffer
	lg := Config{Output: &out}.New()
	lg.Info().Msg("hello")

	if ts, _ := decodeLine(t, out.Bytes())["time"].(string); strings.Contains(ts, ".") {
		t.Errorf("time = %q, New overrode the format set in main", ts)
	}
}

// TestConfigNewConcurrent creates loggers while another one writes, as parallel tests of CLIs do. Run
// it with -race.
func TestConfigNewConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			_ = Config{Output: io.Discard}.New()
		}
	})
	wg.Go(func() {
		lg := Config{Output: io.Discard}.New()
		for range 100 {
			lg.Info().Msg("hello")
		}
	})
	wg.Wait()
}

func TestDefaultFileName(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{path: "/usr/local/bin/shop-api", want: "shop-api.log"},
		{path: "./shop-cli", want: "shop-cli.log"},
		{path: "shop.exe", want: "shop.log"},
		{path: "logger.test", want: "logger.test.log"},
		{path: "", want: fallbackFileName},
		{path: "/", want: fallbackFileName},
	}
	for _, tt := range tests {
		if got := defaultFileName(tt.path); got != tt.want {
			t.Errorf("defaultFileName(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// TestFileDefaultNameKeepsLogDir guards against a default file name that a directory of the working
// directory can hold: lumberjack renames whatever is in the way of its file.
func TestFileDefaultNameKeepsLogDir(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.Mkdir("log", 0o755); err != nil {
		t.Fatal(err)
	}

	lg := Config{File: FileConfig{Enabled: true}}.New()
	lg.Info().Msg("hello")

	if info, err := os.Stat("log"); err != nil || !info.IsDir() {
		t.Fatalf("log directory was replaced: %v", err)
	}
	data, err := os.ReadFile(defaultFileName(os.Args[0]))
	if err != nil {
		t.Fatalf("default log file not written: %v", err)
	}
	decodeLine(t, data)
}

// decodeLine decodes the only line of data, a JSON log entry with the message "hello" or "kept".
func decodeLine(t *testing.T, data []byte) map[string]any {
	t.Helper()
	if bytes.Count(data, []byte("\n")) != 1 {
		t.Fatalf("want one line, got %q", data)
	}
	var line map[string]any
	if err := json.Unmarshal(data, &line); err != nil {
		t.Fatalf("line is not JSON: %v: %q", err, data)
	}
	if msg := line["message"]; msg != "hello" && msg != "kept" {
		t.Fatalf("message = %v", msg)
	}
	return line
}

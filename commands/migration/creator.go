package migration

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var migrationNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

const migrationTemplate = `-- +migrate Up
-- Write your UP migration SQL statements here. End statements with semicolons.

-- +migrate Down
-- Write your DOWN migration SQL statements here. End statements with semicolons.
`

// Creator writes new migration files from a sql-migrate template. It is safe for concurrent use.
type Creator struct {
	dir         string
	mu          sync.Mutex
	lastVersion int64
}

// NewCreator returns a Creator that writes to dir, or to "migrations" when dir is empty.
func NewCreator(dir string) *Creator {
	if dir == "" {
		dir = defaultDir
	}
	return &Creator{dir: dir}
}

// Create writes a sql-migrate template with an exclusive create. An empty dir
// uses the configured directory. Names are lowercased and spaces become '_'.
// The 17-digit version is UTC yyyyMMddHHmmssSSS. Within one Creator, successive
// versions increase even if the clock moves backward or calls share a millisecond.
func (c *Creator) Create(dir, name string) (string, error) {
	if c == nil {
		return "", errors.New("migration: creator is nil")
	}
	name = strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(name))), "_")
	// Keep the entire file/ID under the runner's 255-byte limit.
	if !migrationNamePattern.MatchString(name) || len(name) > 233 {
		return "", fmt.Errorf("migration: invalid name %q: use letters, digits and underscores (max 233 bytes), starting with a letter", name)
	}
	if dir == "" {
		dir = c.dir
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("migration: create directory: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	// In Go, "20060102150405000" has literal trailing zeros. Format the actual
	// milliseconds separately so they are represented in the migration version.
	version, _ := strconv.ParseInt(fmt.Sprintf("%s%03d", now.Format("20060102150405"), now.Nanosecond()/int(time.Millisecond)), 10, 64)
	if version <= c.lastVersion {
		version = c.lastVersion + 1
	}
	for attempt := 0; attempt < 1000; attempt++ {
		path := filepath.Join(dir, fmt.Sprintf("%017d_%s.sql", version, name))
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, os.ErrExist) {
			version++
			continue
		}
		if err != nil {
			return "", fmt.Errorf("migration: create file: %w", err)
		}
		_, writeErr := file.WriteString(migrationTemplate)
		closeErr := file.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			removeErr := os.Remove(path)
			return "", errors.Join(fmt.Errorf("migration: write file: %w", err), removeErr)
		}
		c.lastVersion = version
		return path, nil
	}
	return "", errors.New("migration: no free file name after 1000 attempts")
}

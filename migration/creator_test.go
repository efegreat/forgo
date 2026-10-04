package migration

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	sqlmigrate "github.com/rubenv/sql-migrate"
)

func TestCreatorWritesParseableTemplate(t *testing.T) {
	dir := t.TempDir()
	c := NewCreator(dir)
	path, err := c.Create("", " Create Users ")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9]{17}_create_users\.sql$`).MatchString(filepath.Base(path)) {
		t.Fatalf("unexpected filename %q", path)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := sqlmigrate.ParseMigration(filepath.Base(path), f); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "../../escape", "123_test", "create-users", strings.Repeat("a", 234)} {
		if _, err := c.Create("", name); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
}

func TestCreatorConcurrentCallsDoNotOverwrite(t *testing.T) {
	c := NewCreator(t.TempDir())
	var wg sync.WaitGroup
	paths := make(chan string, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path, err := c.Create("", "add_users")
			if err != nil {
				t.Error(err)
				return
			}
			paths <- path
		}()
	}
	wg.Wait()
	close(paths)
	seen := make(map[string]bool)
	for path := range paths {
		if seen[path] {
			t.Fatalf("duplicate filename %q", path)
		}
		seen[path] = true
		data, err := os.ReadFile(path)
		if err != nil || string(data) != migrationTemplate {
			t.Fatalf("incomplete template %q: %v", path, err)
		}
	}
	if len(seen) != 24 {
		t.Fatalf("created %d files", len(seen))
	}
}

func TestCreatorDirectoryOverride(t *testing.T) {
	c := NewCreator(filepath.Join(t.TempDir(), "default"))
	dir := filepath.Join(t.TempDir(), "override")
	path, err := c.Create(dir, "add_users")
	if err != nil || filepath.Dir(path) != dir {
		t.Fatalf("path=%q, err=%v", path, err)
	}
}

package migration

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLockWaitCancellation(t *testing.T) {
	for _, dialect := range []string{"mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			m, mock := testMigrator(t, dialect, source())
			conn, err := m.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if dialect == "mysql" {
				mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(0))
			} else {
				mock.ExpectQuery("SELECT pg_try_advisory_lock").WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(false))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			unlock, err := m.locker.Lock(ctx, conn)
			if unlock != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected lock result: %v", err)
			}
			assertExpectations(t, mock)
		})
	}
}

func TestUnlockIsIdempotent(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	expectLock(mock, "postgres")
	expectUnlock(mock, "postgres")
	unlock, err := m.locker.Lock(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := unlock(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	assertExpectations(t, mock)
}

func TestAmbiguousAcquireDiscardsSession(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	failed := errors.New("response lost")
	mock.ExpectQuery("SELECT pg_try_advisory_lock").WillReturnError(failed)
	mock.ExpectClose()
	if _, err := m.locker.Lock(context.Background(), conn); !errors.Is(err, failed) {
		t.Fatalf("err=%v", err)
	}
	assertExpectations(t, mock)
}

func TestMySQLNullResultDiscardsSession(t *testing.T) {
	m, mock := testMigrator(t, "mysql", source())
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(nil))
	mock.ExpectClose()
	if _, err := m.locker.Lock(context.Background(), conn); err == nil {
		t.Fatal("expected NULL result failure")
	}
	assertExpectations(t, mock)
}

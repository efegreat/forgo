package inprocess

import (
	"context"
	"testing"
)

type key string

func TestContext(t *testing.T) {
	server := context.WithValue(context.WithValue(context.Background(), key("server"), "server"), key("both"), "server")
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key("both"), "ctx"))
	c := Context(ctx, server)

	for k, want := range map[key]any{"server": "server", "both": "ctx", "missing": nil} {
		if got := c.Value(k); got != want {
			t.Errorf("Value(%q) = %v, want %v", k, got, want)
		}
	}
	if c.Err() != nil {
		t.Fatal("ended before ctx")
	}
	cancel()
	<-c.Done()
	if c.Err() != context.Canceled {
		t.Fatalf("Err %v", c.Err())
	}
}

package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// slowFollow stands in for a follower caught mid-write when it is told to stop,
// and records whether it got as far as returning.
func slowFollow(finished *atomic.Bool) func(context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		finished.Store(true)
		return nil
	}
}

func TestRunServingWaitsForTheFollowerOnASignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(20*time.Millisecond, cancel)

	var finished atomic.Bool
	srv := &http.Server{Addr: "127.0.0.1:0"}
	if err := runServing(ctx, srv, slowFollow(&finished), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("runServing = %v, want nil", err)
	}
	if !finished.Load() {
		t.Error("runServing returned while the follower was still running")
	}
}

// A server that cannot bind never sees a signal, so nothing but runServing can
// stop the follower before the store closes.
func TestRunServingStopsTheFollowerWhenTheServerFails(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer taken.Close()

	var finished atomic.Bool
	srv := &http.Server{Addr: taken.Addr().String()}
	err = runServing(context.Background(), srv, slowFollow(&finished), slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "http server") {
		t.Fatalf("runServing = %v, want the http server error", err)
	}
	if !finished.Load() {
		t.Error("runServing returned while the follower was still running")
	}
}

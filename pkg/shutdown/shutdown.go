package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// hook holds a named shutdown function registered by a caller.
type hook struct {
	name string
	fn   func(ctx context.Context) error
}

// GracefulShutdown coordinates an ordered, concurrent shutdown of all
// registered resources when the process receives SIGINT or SIGTERM.
//
// Design pattern: Registry + Fan-Out
//   Components register their cleanup functions via Register().
//   On signal receipt, all functions are called concurrently within a
//   shared timeout context. Each result is logged individually.
//
// Usage:
//
//	gs := shutdown.New()
//	gs.Register("http-server", srv.Shutdown)
//	gs.Register("db", func(ctx context.Context) error { return db.Close() })
//	gs.Register("tracer", tracerShutdown)
//	gs.ListenAndShutdown(ctx, 30*time.Second) // blocks until signal
type GracefulShutdown struct {
	mu    sync.Mutex
	hooks []hook
}

// New creates a GracefulShutdown with no registered hooks.
func New() *GracefulShutdown {
	return &GracefulShutdown{}
}

// Register adds a named shutdown function to the registry.
// Functions are called concurrently on shutdown — order is not guaranteed.
// This method is safe to call from multiple goroutines.
func (gs *GracefulShutdown) Register(name string, fn func(ctx context.Context) error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.hooks = append(gs.hooks, hook{name: name, fn: fn})
}

// ListenAndShutdown blocks until SIGINT or SIGTERM is received, then
// calls all registered shutdown functions concurrently within timeout.
//
// Shutdown sequence:
//  1. Signal received → log it
//  2. Create a context with timeout
//  3. Call all hooks concurrently via goroutines
//  4. Wait for all to complete or timeout to expire
//  5. Log the result of each hook
//  6. Return (caller should os.Exit if needed)
func (gs *GracefulShutdown) ListenAndShutdown(ctx context.Context, timeout time.Duration) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal arrives or the parent context is cancelled.
	select {
	case sig := <-quit:
		slog.Info("shutdown signal received",
			slog.String("signal", sig.String()),
		)
	case <-ctx.Done():
		slog.Info("context cancelled — initiating shutdown")
	}

	// Create a timeout context for the shutdown phase.
	// All hooks must complete within this window.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	gs.mu.Lock()
	hooks := make([]hook, len(gs.hooks))
	copy(hooks, gs.hooks)
	gs.mu.Unlock()

	slog.Info("shutting down",
		slog.Int("hooks", len(hooks)),
		slog.Duration("timeout", timeout),
	)

	// Fan-out: run all shutdown hooks concurrently.
	var wg sync.WaitGroup
	for _, h := range hooks {
		wg.Add(1)
		go func(h hook) {
			defer wg.Done()

			start := time.Now()
			err := h.fn(shutdownCtx)
			elapsed := time.Since(start)

			if err != nil {
				slog.Error("shutdown hook failed",
					slog.String("name", err.Error()),
					slog.String("hook", h.name),
					slog.Duration("elapsed", elapsed),
				)
			} else {
				slog.Info("shutdown hook completed",
					slog.String("hook", h.name),
					slog.Duration("elapsed", elapsed),
				)
			}
		}(h)
	}

	// Wait for all hooks or timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("all shutdown hooks completed cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("shutdown timeout exceeded — some resources may not be released cleanly",
			slog.Duration("timeout", timeout),
		)
	}
}
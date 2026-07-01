package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lande26/anvil"
)

func main() {
	a, err := anvil.New(anvil.Config{
		DataDir:            getEnv("ANVIL_DATA_DIR", "./anvil-data"),
		AOFSync:            getEnv("ANVIL_AOF_SYNC", "everysec"),
		Concurrency:        10,
		HeartbeatInterval:  5 * time.Second,
		StalenessThreshold: 60 * time.Second,
		JobTimeout:         30 * time.Second,
		ReaperInterval:     10 * time.Second,
		HTTPAddr:           getEnv("ANVIL_HTTP_ADDR", ":8080"),
	})
	if err != nil {
		slog.Error("failed to initialize anvil", "error", err)
		os.Exit(1)
	}

	// Register job handlers
	a.RegisterHandler("echo", func(ctx context.Context, payload json.RawMessage) error {
		slog.Info("echo", "payload", string(payload))
		return nil
	})

	a.RegisterHandler("sleep", func(ctx context.Context, payload json.RawMessage) error {
		var req struct {
			Duration int `json:"duration"`
		}
		if err := json.Unmarshal(payload, &req); err != nil {
			return err
		}
		select {
		case <-time.After(time.Duration(req.Duration) * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	a.RegisterHandler("fail", func(ctx context.Context, payload json.RawMessage) error {
		return fmt.Errorf("intentional failure")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("received signal", "signal", sig)
		cancel()
	}()

	if err := a.Run(ctx); err != nil {
		slog.Error("anvil exited with error", "error", err)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

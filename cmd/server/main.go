package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ansh1693/evtq/internal/api"
	queuesvc "github.com/Ansh1693/evtq/internal/queue"
	"github.com/Ansh1693/evtq/internal/store"
	"github.com/Ansh1693/evtq/internal/trigger"
	"github.com/Ansh1693/evtq/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	// Configuration from environment.
	dbURL := getEnv("DATABASE_URL", "postgres://localhost:5432/sqsclone?sslmode=disable")
	listenAddr := getEnv("LISTEN_ADDR", ":8080")
	expiryInterval := 60 * time.Second      // soft-delete expired messages every 60s
	dedupExpiryInterval := 60 * time.Second // clear expired dedup IDs every 60s
	hardDeleteInterval := 24 * time.Hour    // permanently remove soft-deleted rows once a day
	triggerRefreshInterval := 10 * time.Second

	// Connect to Postgres.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	poolConfig.MaxConns = 20

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	// Verify connection.
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	logger.Info("connected to database")

	// Build layers.
	st := store.New(pool)
	svc := queuesvc.New(st, nil)
	router := api.NewRouter(svc, logger)

	// Start background cleanup worker (soft-delete expired every 60s, hard-delete daily).
	cleaner := worker.NewCleanupWorker(st, expiryInterval, dedupExpiryInterval, hardDeleteInterval, logger)
	go cleaner.Run(ctx)

	// Start trigger manager (loads enabled triggers and runs pollers).
	triggerManager := trigger.NewManager(st, logger, triggerRefreshInterval)
	go triggerManager.Run(ctx)

	// Start HTTP server.
	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-shutdownCh
		logger.Info("shutdown signal received", "signal", sig)
		cancel() // stop the cleanup worker

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http server shutdown error", "error", err)
		}
	}()

	logger.Info("starting server", "addr", listenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server: %w", err)
	}

	logger.Info("server stopped")
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

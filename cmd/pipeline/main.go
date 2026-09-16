// Command pipeline runs one independently deployable pipeline role.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/api"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/broker"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/config"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("pipeline stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: pipeline {broker|collector|streamer|api|migrate|openapi}")
	}
	role := os.Args[1]
	if role == "openapi" {
		mux := http.NewServeMux()
		a := api.Register(mux, nil)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(a.OpenAPI())
	}
	if role != "broker" && role != "collector" && role != "streamer" && role != "api" && role != "migrate" {
		return errors.New("unknown role")
	}
	c, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var db *store.Store
	if role != "streamer" {
		if c.DSN == "" {
			return errors.New("DATABASE_URL is required")
		}
		db, err = store.Open(ctx, c.DSN, int32(c.Connections))
		if err != nil {
			return err
		}
		defer db.Pool.Close()
		db.Capacity = c.Capacity
		if role == "migrate" {
			migration, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			return db.Migrate(migration)
		}
	}
	mux := http.NewServeMux()
	var draining atomic.Bool
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if draining.Load() {
			http.Error(w, "draining", 503)
			return
		}
		if db != nil {
			check, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if db.Ready(check) != nil {
				http.Error(w, "database unavailable", 503)
				return
			}
		}
		w.WriteHeader(200)
	})
	client := &broker.Client{URL: c.BrokerURL, Token: c.Token, HTTP: &http.Client{Timeout: 6 * time.Second}}
	counts := &worker.Counters{}
	switch role {
	case "api":
		api.Register(mux, db)
	case "broker":
		broker.Register(mux, db, c.Token)
	}
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "pipeline_persisted_total %d\npipeline_duplicates_total %d\npipeline_processing_failures_total %d\n", counts.Persisted.Load(), counts.Duplicates.Load(), counts.Failures.Load())
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		fmt.Fprintf(w, "pipeline_heap_bytes %d\npipeline_goroutines %d\n", mem.HeapAlloc, runtime.NumGoroutine())
		if role == "collector" {
			fmt.Fprintln(w, "# TYPE pipeline_ingestion_seconds histogram")
			for i, b := range worker.LatencyBounds {
				fmt.Fprintf(w, "pipeline_ingestion_seconds_bucket{le=\"%g\"} %d\n", b, counts.LatencyBuckets[i].Load())
			}
			fmt.Fprintf(w, "pipeline_ingestion_seconds_bucket{le=\"+Inf\"} %d\npipeline_ingestion_seconds_count %d\npipeline_ingestion_seconds_sum %f\n", counts.LatencyCount.Load(), counts.LatencyCount.Load(), float64(counts.LatencyNanos.Load())/1e9)
		}
		if role == "broker" {
			check, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			v, err := db.Stats(check)
			if err == nil {
				fmt.Fprintf(w, "pipeline_queue_ready %d\npipeline_queue_leased %d\npipeline_queue_done %d\npipeline_queue_dead %d\npipeline_queue_oldest_seconds %f\n", v.Ready, v.Leased, v.Done, v.Dead, v.OldestSeconds)
			}
		}
	})
	server := &http.Server{Addr: c.Address, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	workErrors := make(chan error, 1)
	switch role {
	case "streamer":
		go func() {
			workErrors <- worker.Stream(ctx, client, worker.StreamConfig{Path: c.CSV, Producer: c.Producer + "-" + model.NewID(), Rate: c.Rate, Loops: c.Loops, RetryWindow: c.RetryWindow})
		}()
	case "collector":
		go func() { workErrors <- worker.Collect(ctx, client, db, c.Workers, counts) }()
	case "broker":
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					clean, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := db.Cleanup(clean); err != nil {
						slog.Warn("queue cleanup failed", "error", err)
					}
					cancel()
				}
			}
		}()
	}
	slog.Info("service started", "role", role, "address", c.Address)
	workDone := false
	select {
	case <-ctx.Done():
	case err = <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	case err = <-workErrors:
		workDone = true
		if errors.Is(err, context.Canceled) {
			err = nil
		}
	}
	draining.Store(true)
	stop()
	if !workDone && (role == "collector" || role == "streamer") {
		select {
		case workErr := <-workErrors:
			if workErr != nil && !errors.Is(workErr, context.Canceled) && err == nil {
				err = workErr
			}
		case <-time.After(25 * time.Second):
			return errors.New("worker drain timed out")
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if shutdownErr := server.Shutdown(shutdown); err == nil {
		err = shutdownErr
	}
	return err
}

package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/broker"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

type Consumer interface {
	Claim(context.Context) (*store.Delivery, error)
	Ack(context.Context, string, string) error
	Renew(context.Context, string, string) error
	Reject(context.Context, broker.Receipt) error
}
type Sink interface {
	Ping(context.Context) error
	Persist(context.Context, model.Event) (bool, error)
}
type Counters struct {
	Persisted, Duplicates, Failures atomic.Int64
	LatencyNanos, LatencyCount      atomic.Int64
	LatencyBuckets                  [8]atomic.Int64
}

var LatencyBounds = [8]float64{0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60}

func (c *Counters) ObserveLatency(since time.Time) {
	if since.IsZero() {
		return
	}
	d := time.Since(since)
	if d < 0 {
		d = 0
	}
	c.LatencyNanos.Add(int64(d))
	c.LatencyCount.Add(1)
	for i, b := range LatencyBounds {
		if d.Seconds() <= b {
			c.LatencyBuckets[i].Add(1)
		}
	}
}

func Collect(ctx context.Context, q Consumer, db Sink, workers int, counters *Counters) error {
	if workers < 1 || workers > 32 {
		return errors.New("workers must be between 1 and 32")
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); collect(ctx, q, db, counters) }()
	}
	wg.Wait()
	return nil
}

func collect(ctx context.Context, q Consumer, db Sink, counters *Counters) {
	failures := 0
	for ctx.Err() == nil {
		check, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := db.Ping(check)
		cancel()
		if err != nil {
			failures++
			slog.Warn("collector storage unavailable", "error", err)
			_ = Sleep(ctx, Backoff(failures))
			continue
		}
		d, err := q.Claim(ctx)
		if err != nil {
			failures++
			slog.Warn("claim failed", "error", err)
			_ = Sleep(ctx, Backoff(failures))
			continue
		}
		if d == nil {
			_ = Sleep(ctx, 250*time.Millisecond)
			continue
		}
		// Finish claimed work on shutdown, bounded below by the drain deadline.
		if process(q, db, d, counters) {
			failures = 0
		} else {
			failures++
			_ = Sleep(ctx, Backoff(failures))
		}
	}
}

func process(q Consumer, db Sink, d *store.Delivery, c *Counters) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopRenew:
				return
			case <-ticker.C:
				if err := q.Renew(ctx, d.Event.ID, d.Token); err != nil {
					slog.Warn("lease renewal failed", "event_id", d.Event.ID, "error", err)
					cancel()
					return
				}
			}
		}
	}()
	defer func() { close(stopRenew); <-renewDone }()
	_, _, validation := d.Event.Record.Parse()
	if validation != nil {
		c.Failures.Add(1)
		if err := q.Reject(ctx, broker.Receipt{ID: d.Event.ID, Token: d.Token, Permanent: true, Reason: validation.Error()}); err != nil {
			slog.Warn("dead-letter transition failed", "event_id", d.Event.ID, "error", err)
		}
		return true
	}
	duplicate, err := db.Persist(ctx, d.Event)
	if err != nil {
		c.Failures.Add(1)
		slog.Warn("persistence failed", "event_id", d.Event.ID, "error", err)
		permanent := errors.Is(err, store.ErrConflict)
		// Use a fresh bounded context if persistence consumed its deadline.
		nack, ncancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ncancel()
		if err = q.Reject(nack, broker.Receipt{ID: d.Event.ID, Token: d.Token, Permanent: permanent, Reason: "telemetry persistence failed", DelayMS: 1000}); err != nil {
			slog.Warn("requeue failed; lease will recover", "event_id", d.Event.ID, "error", err)
		}
		return false
	}
	if duplicate {
		c.Duplicates.Add(1)
	} else {
		c.Persisted.Add(1)
		c.ObserveLatency(d.EnqueuedAt)
	}
	// Retry lost ACK responses using the same receipt. Persistence is already safe.
	for attempt := 0; attempt < 3; attempt++ {
		if err = q.Ack(ctx, d.Event.ID, d.Token); err == nil {
			return true
		}
		var he *broker.HTTPError
		if errors.As(err, &he) && !he.Temporary() {
			break
		}
		if Sleep(ctx, Backoff(attempt)) != nil {
			break
		}
	}
	slog.Warn("acknowledgment failed; redelivery is safe", "event_id", d.Event.ID, "error", err)
	return false
}

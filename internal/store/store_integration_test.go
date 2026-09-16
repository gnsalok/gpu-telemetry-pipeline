//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
)

// These tests require a dedicated database. They never use DATABASE_URL.
func database(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL must point to a disposable test database")
	}
	s, err := Open(context.Background(), dsn, 12)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Pool.Close)
	ctx := context.Background()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, `TRUNCATE queue.messages,telemetry.observations,telemetry.gpus RESTART IDENTITY; UPDATE queue.capacity SET outstanding=0`); err != nil {
		t.Fatal(err)
	}
	return s
}
func event() model.Event {
	return model.Event{ID: model.NewID(), Producer: "integration", Loop: 1, Row: 1, Record: model.Record{UUID: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50", GPUIndex: "0", Host: "host", Device: "nvidia0", Model: "H100", Metric: "GPU_UTIL", Value: "0"}}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublishDedupCapacityAndConcurrentClaims(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	s.Capacity = 25
	var wg sync.WaitGroup
	ids := make(chan string, 25)
	for range 25 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := event()
			if _, err := s.Publish(ctx, e); err != nil {
				t.Error(err)
				return
			}
			ids <- e.ID
		}()
	}
	wg.Wait()
	close(ids)
	if _, err := s.Publish(ctx, event()); !errors.Is(err, ErrFull) {
		t.Fatalf("capacity: %v", err)
	}
	seen := sync.Map{}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				d, err := s.Claim(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if d == nil {
					return
				}
				if _, loaded := seen.LoadOrStore(d.Event.ID, true); loaded {
					t.Error("concurrent duplicate claim")
				}
				if err = s.Ack(ctx, d.Event.ID, d.Token); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	v, err := s.Stats(ctx)
	must(t, err)
	if v.Done != 25 || v.Ready != 0 || v.Leased != 0 {
		t.Fatalf("stats %+v", v)
	}
	e := event()
	duplicate, err := s.Publish(ctx, e)
	must(t, err)
	if duplicate {
		t.Fatal("new event duplicate")
	}
	duplicate, err = s.Publish(ctx, e)
	must(t, err)
	if !duplicate {
		t.Fatal("retry not deduplicated")
	}
	e.Record.Value = "1"
	if _, err = s.Publish(ctx, e); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestCrashAfterPersistAndStaleLease(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	e := event()
	_, err := s.Publish(ctx, e)
	must(t, err)
	first, err := s.Claim(ctx)
	must(t, err)
	duplicate, err := s.Persist(ctx, first.Event)
	must(t, err)
	if duplicate {
		t.Fatal("first insert duplicate")
	}
	before, err := s.Observations(ctx, Filter{GPU: e.Record.UUID, Limit: 10})
	must(t, err)
	// Simulate the collector dying without ACK and an expired delivery lease.
	_, err = s.Pool.Exec(ctx, `UPDATE queue.messages SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1`, e.ID)
	must(t, err)
	second, err := s.Claim(ctx)
	must(t, err)
	if second == nil || second.Token == first.Token {
		t.Fatal("lease not reassigned")
	}
	if err = s.Ack(ctx, e.ID, first.Token); !errors.Is(err, ErrLease) {
		t.Fatalf("stale ack: %v", err)
	}
	if err = s.Renew(ctx, e.ID, first.Token); !errors.Is(err, ErrLease) {
		t.Fatalf("stale renewal: %v", err)
	}
	duplicate, err = s.Persist(ctx, second.Event)
	must(t, err)
	if !duplicate {
		t.Fatal("redelivery inserted twice")
	}
	must(t, s.Ack(ctx, e.ID, second.Token))
	must(t, s.Ack(ctx, e.ID, second.Token))
	after, err := s.Observations(ctx, Filter{GPU: e.Record.UUID, Limit: 10})
	must(t, err)
	if len(after) != 1 || !after[0].Timestamp.Equal(before[0].Timestamp) {
		t.Fatal("redelivery changed history")
	}
	duplicate, err = s.Publish(ctx, e)
	must(t, err)
	if !duplicate {
		t.Fatal("completion tombstone missing")
	}
	// Same measurement, genuinely new observation.
	e.ID = model.NewID()
	_, err = s.Persist(ctx, e)
	must(t, err)
	after, err = s.Observations(ctx, Filter{GPU: e.Record.UUID, Limit: 10})
	must(t, err)
	if len(after) != 2 {
		t.Fatal("repeated measurement lost")
	}
}

func TestRetryDeadLettersAndRenewal(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	e := event()
	_, err := s.Publish(ctx, e)
	must(t, err)
	d, err := s.Claim(ctx)
	must(t, err)
	must(t, s.Renew(ctx, e.ID, d.Token))
	must(t, s.Reject(ctx, e.ID, d.Token, "database unavailable", false, time.Hour))
	if d, err = s.Claim(ctx); err != nil || d != nil {
		t.Fatalf("delayed retry returned early: %v", err)
	}
	_, err = s.Pool.Exec(ctx, `UPDATE queue.messages SET available_at=clock_timestamp()-interval '1 second'`)
	must(t, err)
	d, err = s.Claim(ctx)
	must(t, err)
	if d.Attempt != 1 {
		t.Fatal("dependency outage consumed attempts")
	}
	must(t, s.Reject(ctx, e.ID, d.Token, "invalid metric", true, 0))
	dead, err := s.Dead(ctx)
	must(t, err)
	if len(dead) != 1 || dead[0].Payload.ID != e.ID {
		t.Fatal("dead-letter payload missing")
	}
	must(t, s.Replay(ctx, e.ID))
	d, err = s.Claim(ctx)
	must(t, err)
	if d == nil || d.Attempt != 1 {
		t.Fatal("replay failed")
	}
	_, err = s.Pool.Exec(ctx, `UPDATE queue.messages SET attempts=5,lease_until=clock_timestamp()-interval '1 second'`)
	must(t, err)
	d, err = s.Claim(ctx)
	must(t, err)
	if d != nil {
		t.Fatal("exhausted delivery returned")
	}
	v, err := s.Stats(ctx)
	must(t, err)
	if v.Dead != 1 {
		t.Fatal("exhausted event not retained")
	}
}

func TestQueryBoundariesAndCleanup(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	e := event()
	_, err := s.Persist(ctx, e)
	must(t, err)
	items, err := s.Observations(ctx, Filter{GPU: e.Record.UUID, Limit: 10})
	must(t, err)
	stamp := items[0].Timestamp
	items, err = s.Observations(ctx, Filter{GPU: e.Record.UUID, Start: &stamp, End: &stamp, Limit: 10})
	must(t, err)
	if len(items) != 1 {
		t.Fatal("boundaries must be inclusive")
	}
	items, err = s.Observations(ctx, Filter{GPU: e.Record.UUID, AfterTime: &stamp, AfterID: e.ID, Limit: 10})
	must(t, err)
	if len(items) != 0 {
		t.Fatal("cursor repeated event")
	}
	if _, err = s.Observations(ctx, Filter{GPU: "unknown", Limit: 10}); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown GPU accepted")
	}
	gpus, err := s.GPUs(ctx, "", 10)
	must(t, err)
	if len(gpus) != 1 {
		t.Fatal("GPU missing")
	}
	_, err = s.Publish(ctx, e)
	must(t, err)
	d, err := s.Claim(ctx)
	must(t, err)
	must(t, s.Ack(ctx, e.ID, d.Token))
	_, err = s.Pool.Exec(ctx, `UPDATE queue.messages SET completed_at=clock_timestamp()-interval '25 hours'`)
	must(t, err)
	must(t, s.Cleanup(ctx))
	v, err := s.Stats(ctx)
	must(t, err)
	if v.Done != 0 {
		t.Fatal("expired tombstones retained")
	}
}

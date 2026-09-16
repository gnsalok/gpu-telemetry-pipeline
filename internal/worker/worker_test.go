package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/broker"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

type fakePublisher struct {
	events []model.Event
	fail   bool
}

func (f *fakePublisher) Publish(_ context.Context, e model.Event) error {
	f.events = append(f.events, e)
	if f.fail {
		f.fail = false
		return errors.New("response lost")
	}
	return nil
}

func TestCSVReplayAndRetryIdentity(t *testing.T) {
	p := &fakePublisher{fail: true}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := Stream(ctx, p, StreamConfig{Path: "../../data/telemetry.csv", Producer: "test", Rate: 1000000, Loops: 2, RetryWindow: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.events) != 4941 {
		t.Fatalf("events %d", len(p.events))
	}
	if p.events[0].ID != p.events[1].ID {
		t.Fatal("retry changed event identity")
	}
	seen := map[string]bool{}
	gpus := map[string]int{}
	for _, e := range p.events[1:] {
		if seen[e.ID] {
			t.Fatal("new observation reused an ID")
		}
		seen[e.ID] = true
		gpus[e.Record.UUID]++
	}
	if len(gpus) != 247 {
		t.Fatalf("GPUs %d", len(gpus))
	}
	for _, n := range gpus {
		if n != 20 {
			t.Fatalf("observations per GPU %d", n)
		}
	}
}
func TestCSVErrors(t *testing.T) {
	for name, content := range map[string]string{"missing": "uuid,value\nx,0\n", "empty": "", "duplicate": "uuid,uuid\nx,x\n"} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "data.csv")
			if err := os.WriteFile(p, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			err := Stream(context.Background(), &fakePublisher{}, StreamConfig{Path: p, Producer: "test", Rate: 10, Loops: 1, RetryWindow: time.Second})
			if err == nil {
				t.Fatal("expected CSV error")
			}
		})
	}
}

type permanentPublisher struct{ calls int }

func (p *permanentPublisher) Publish(context.Context, model.Event) error {
	p.calls++
	return &broker.HTTPError{Status: 409, Message: "conflicting identity"}
}
func TestPermanentPublishFailure(t *testing.T) {
	p := &permanentPublisher{}
	if publish(context.Background(), p, model.Event{}, time.Second) == nil || p.calls != 1 {
		t.Fatal("permanent error retried")
	}
}
func TestBackoffAndCancellation(t *testing.T) {
	for i := 0; i < 100; i++ {
		d := Backoff(i)
		if d <= 0 || d > 30*time.Second {
			t.Fatal(d)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) != context.Canceled {
		t.Fatal("sleep ignored cancellation")
	}
}

type fakeConsumer struct {
	ack, reject int
	permanent   bool
	ackErr      error
}

func (f *fakeConsumer) Claim(context.Context) (*store.Delivery, error) { return nil, nil }
func (f *fakeConsumer) Ack(context.Context, string, string) error      { f.ack++; return f.ackErr }
func (f *fakeConsumer) Renew(context.Context, string, string) error    { return nil }
func (f *fakeConsumer) Reject(_ context.Context, r broker.Receipt) error {
	f.reject++
	f.permanent = r.Permanent
	return nil
}

type fakeSink struct {
	calls     int
	duplicate bool
	err       error
}

func (f *fakeSink) Ping(context.Context) error { return nil }
func (f *fakeSink) Persist(context.Context, model.Event) (bool, error) {
	f.calls++
	return f.duplicate, f.err
}
func delivery() *store.Delivery {
	return &store.Delivery{Event: model.Event{ID: model.NewID(), Record: model.Record{UUID: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50", GPUIndex: "0", Host: "host", Device: "nvidia0", Model: "H100", Metric: "GPU_UTIL", Value: "0"}}, Token: model.NewID()}
}
func TestCollectorProcessing(t *testing.T) {
	for _, tc := range []struct {
		name               string
		invalid, duplicate bool
		sinkErr            error
		ack, reject        int
		permanent          bool
	}{{"success", false, false, nil, 1, 0, false}, {"redelivery", false, true, nil, 1, 0, false}, {"poison", true, false, nil, 0, 1, true}, {"database outage", false, false, errors.New("db down"), 0, 1, false}, {"identity conflict", false, false, store.ErrConflict, 0, 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			q := &fakeConsumer{}
			s := &fakeSink{duplicate: tc.duplicate, err: tc.sinkErr}
			d := delivery()
			if tc.invalid {
				d.Event.Record.Value = "NaN"
			}
			c := &Counters{}
			process(q, s, d, c)
			if q.ack != tc.ack || q.reject != tc.reject || q.permanent != tc.permanent {
				t.Fatalf("unexpected queue actions %+v", q)
			}
			if tc.invalid && s.calls != 0 {
				t.Fatal("poison persisted")
			}
			if tc.duplicate && c.Duplicates.Load() != 1 {
				t.Fatal("duplicate not counted")
			}
		})
	}
}

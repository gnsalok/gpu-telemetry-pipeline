package broker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

type fakeQueue struct {
	event   model.Event
	err     error
	receipt string
}

func (q *fakeQueue) Publish(_ context.Context, e model.Event) (bool, error) {
	q.event = e
	return false, q.err
}
func (q *fakeQueue) Claim(context.Context) (*store.Delivery, error) {
	if q.event.ID == "" {
		return nil, q.err
	}
	return &store.Delivery{Event: q.event, Token: model.NewID()}, q.err
}
func (q *fakeQueue) Ack(_ context.Context, id, token string) error { q.receipt = id; return q.err }
func (q *fakeQueue) Renew(context.Context, string, string) error   { return q.err }
func (q *fakeQueue) Reject(context.Context, string, string, string, bool, time.Duration) error {
	return q.err
}
func (q *fakeQueue) Stats(context.Context) (store.Stats, error) { return store.Stats{}, q.err }
func (q *fakeQueue) Dead(context.Context) ([]store.DeadLetter, error) {
	return []store.DeadLetter{}, q.err
}
func (q *fakeQueue) Replay(context.Context, string) error { return q.err }

func TestClientAndProtocol(t *testing.T) {
	q := &fakeQueue{}
	mux := http.NewServeMux()
	Register(mux, q, "secret")
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := &Client{URL: srv.URL, Token: "secret", HTTP: srv.Client()}
	ctx := context.Background()
	if d, err := c.Claim(ctx); d != nil || err != nil {
		t.Fatalf("empty claim: %v %v", d, err)
	}
	e := model.Event{ID: model.NewID(), Producer: "test"}
	if err := c.Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	d, err := c.Claim(ctx)
	if err != nil || d.Event.ID != e.ID {
		t.Fatal("delivery mismatch")
	}
	if err = c.Ack(ctx, e.ID, d.Token); err != nil || q.receipt != e.ID {
		t.Fatal("ACK failed")
	}
	if err = c.Renew(ctx, e.ID, d.Token); err != nil {
		t.Fatal(err)
	}
	if err = c.Reject(ctx, Receipt{ID: e.ID, Token: d.Token, Permanent: true}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		err       error
		status    int
		temporary bool
	}{{store.ErrFull, 429, true}, {store.ErrConflict, 409, false}, {store.ErrLease, 409, false}, {store.ErrNotFound, 404, false}, {errors.New("private database error"), 503, true}} {
		q.err = tc.err
		err = c.Publish(ctx, e)
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != tc.status || he.Temporary() != tc.temporary {
			t.Fatalf("error mapping: %v", err)
		}
		if tc.status == 503 && strings.Contains(err.Error(), "private database") {
			t.Fatal("internal details leaked")
		}
	}
	c.Token = "wrong"
	if err = c.Publish(ctx, e); err == nil {
		t.Fatal("unauthenticated publish accepted")
	}
}

func TestMalformedAndOversizedBodies(t *testing.T) {
	m := http.NewServeMux()
	Register(m, &fakeQueue{}, "")
	for _, body := range []string{`{}`, `{"unknown":true}`, `{} {}`, `{"producer_id":"` + strings.Repeat("x", 70*1024) + `"}`} {
		w := httptest.NewRecorder()
		m.ServeHTTP(w, httptest.NewRequest("POST", "/queue/v1/publish", strings.NewReader(body)))
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	for _, path := range []string{"/queue/v1/stats", "/queue/v1/dead"} {
		w := httptest.NewRecorder()
		m.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
}

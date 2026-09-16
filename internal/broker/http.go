// Package broker exposes the custom queue protocol over bounded JSON requests.
package broker

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

type Queue interface {
	Publish(context.Context, model.Event) (bool, error)
	Claim(context.Context) (*store.Delivery, error)
	Ack(context.Context, string, string) error
	Renew(context.Context, string, string) error
	Reject(context.Context, string, string, string, bool, time.Duration) error
	Stats(context.Context) (store.Stats, error)
	Dead(context.Context) ([]store.DeadLetter, error)
	Replay(context.Context, string) error
}

type Receipt struct {
	ID        string `json:"event_id"`
	Token     string `json:"lease_token"`
	Reason    string `json:"reason,omitempty"`
	Permanent bool   `json:"permanent,omitempty"`
	DelayMS   int64  `json:"delay_ms,omitempty"`
}

func Register(mux *http.ServeMux, q Queue, token string) {
	handle := func(pattern string, fn func(http.ResponseWriter, *http.Request)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
				http.Error(w, "unauthorized", 401)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			fn(w, r.WithContext(ctx))
		})
	}
	handle("POST /queue/v1/publish", func(w http.ResponseWriter, r *http.Request) {
		var e model.Event
		if !decode(w, r, &e) {
			return
		}
		if !model.ValidID(e.ID) || e.Producer == "" || len(e.Producer) > 256 {
			http.Error(w, "valid event_id and producer_id required", 400)
			return
		}
		duplicate, err := q.Publish(r.Context(), e)
		if fail(w, err) {
			return
		}
		write(w, map[string]bool{"duplicate": duplicate})
	})
	handle("POST /queue/v1/claim", func(w http.ResponseWriter, r *http.Request) {
		d, err := q.Claim(r.Context())
		if fail(w, err) {
			return
		}
		if d == nil {
			w.WriteHeader(204)
			return
		}
		write(w, d)
	})
	for _, action := range []string{"ack", "renew", "reject"} {
		handle("POST /queue/v1/"+action, func(w http.ResponseWriter, r *http.Request) {
			var v Receipt
			if !decode(w, r, &v) {
				return
			}
			if !model.ValidID(v.ID) || !model.ValidID(v.Token) || len(v.Reason) > 1024 || v.DelayMS < 0 || v.DelayMS > 30000 {
				http.Error(w, "invalid receipt", 400)
				return
			}
			var err error
			switch action {
			case "ack":
				err = q.Ack(r.Context(), v.ID, v.Token)
			case "renew":
				err = q.Renew(r.Context(), v.ID, v.Token)
			case "reject":
				err = q.Reject(r.Context(), v.ID, v.Token, v.Reason, v.Permanent, time.Duration(v.DelayMS)*time.Millisecond)
			}
			if !fail(w, err) {
				w.WriteHeader(204)
			}
		})
	}
	handle("GET /queue/v1/stats", func(w http.ResponseWriter, r *http.Request) {
		v, err := q.Stats(r.Context())
		if !fail(w, err) {
			write(w, v)
		}
	})
	handle("GET /queue/v1/dead", func(w http.ResponseWriter, r *http.Request) {
		v, err := q.Dead(r.Context())
		if !fail(w, err) {
			write(w, v)
		}
	})
	handle("POST /queue/v1/dead/{id}/replay", func(w http.ResponseWriter, r *http.Request) {
		if !fail(w, q.Replay(r.Context(), r.PathValue("id"))) {
			w.WriteHeader(204)
		}
	})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		http.Error(w, "invalid JSON or body exceeds 64 KiB", 400)
		return false
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "expected one JSON object", 400)
		return false
	}
	return true
}
func write(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("client disconnected", "error", err)
	}
}
func fail(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	code := 503
	message := "queue storage unavailable"
	switch {
	case errors.Is(err, store.ErrFull):
		code = 429
		message = err.Error()
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrLease):
		code = 409
		message = err.Error()
	case errors.Is(err, store.ErrNotFound):
		code = 404
		message = err.Error()
	default:
		slog.Error("queue request failed", "error", err)
	}
	http.Error(w, message, code)
	return true
}

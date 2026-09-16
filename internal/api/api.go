// Package api registers the read API and generates OpenAPI from the same types.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

type Reader interface {
	GPUs(context.Context, string, int) ([]model.GPU, error)
	Observations(context.Context, store.Filter) ([]model.Telemetry, error)
}

type GPUInput struct {
	Limit int    `query:"limit" default:"100" minimum:"1" maximum:"1000"`
	After string `query:"after" maxLength:"40" doc:"GPU UUID returned as next_cursor by the previous page"`
}
type GPUOutput struct {
	Body struct {
		Items []model.GPU `json:"items"`
		Next  string      `json:"next_cursor,omitempty"`
	}
}
type TelemetryInput struct {
	ID     string `path:"id" maxLength:"40"`
	Start  string `query:"start_time" maxLength:"64" doc:"Inclusive RFC3339 processing timestamp"`
	End    string `query:"end_time" maxLength:"64" doc:"Inclusive RFC3339 processing timestamp"`
	Limit  int    `query:"limit" default:"100" minimum:"1" maximum:"1000"`
	Cursor string `query:"cursor" maxLength:"1024"`
}
type TelemetryOutput struct {
	Body struct {
		Items []model.Telemetry `json:"items"`
		Next  string            `json:"next_cursor,omitempty"`
	}
}
type cursor struct {
	GPU   string    `json:"gpu"`
	Start string    `json:"start"`
	End   string    `json:"end"`
	Time  time.Time `json:"time"`
	ID    string    `json:"id"`
}

func Register(mux *http.ServeMux, db Reader) huma.API {
	cfg := huma.DefaultConfig("GPU Telemetry Pipeline", "1.0.0")
	cfg.Info.Description = "Persisted GPU telemetry. Timestamps are first committed collector processing times. Results are ordered by timestamp and event ID. Pagination is a live view, not a historical snapshot."
	api := humago.New(mux, cfg)
	huma.Register(api, huma.Operation{OperationID: "list-gpus", Method: http.MethodGet, Path: "/api/v1/gpus", Summary: "List GPUs with stored telemetry", Tags: []string{"Telemetry"}}, func(ctx context.Context, in *GPUInput) (*GPUOutput, error) {
		if in.After != "" && !model.ValidGPU(in.After) {
			return nil, huma.Error400BadRequest("invalid after GPU UUID")
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		items, err := db.GPUs(ctx, in.After, in.Limit+1)
		if err != nil {
			return nil, dbError(err)
		}
		out := &GPUOutput{}
		if len(items) > in.Limit {
			out.Body.Next = items[in.Limit-1].ID
			items = items[:in.Limit]
		}
		out.Body.Items = items
		return out, nil
	})
	huma.Register(api, huma.Operation{OperationID: "query-telemetry", Method: http.MethodGet, Path: "/api/v1/gpus/{id}/telemetry", Summary: "Query GPU observations in chronological order", Tags: []string{"Telemetry"}}, func(ctx context.Context, in *TelemetryInput) (*TelemetryOutput, error) {
		if !model.ValidGPU(in.ID) {
			return nil, huma.Error400BadRequest("invalid GPU UUID")
		}
		f := store.Filter{GPU: in.ID, Limit: in.Limit + 1}
		for _, v := range []struct {
			raw string
			dst **time.Time
		}{{in.Start, &f.Start}, {in.End, &f.End}} {
			if v.raw != "" {
				t, err := time.Parse(time.RFC3339Nano, v.raw)
				if err != nil {
					return nil, huma.Error400BadRequest("time filters must use RFC3339")
				}
				*v.dst = &t
			}
		}
		if f.Start != nil && f.End != nil && f.Start.After(*f.End) {
			return nil, huma.Error400BadRequest("start_time must not exceed end_time")
		}
		if in.Cursor != "" {
			var c cursor
			b, err := base64.RawURLEncoding.DecodeString(in.Cursor)
			if err != nil || json.Unmarshal(b, &c) != nil || c.GPU != in.ID || c.Start != in.Start || c.End != in.End || c.Time.IsZero() || !model.ValidID(c.ID) {
				return nil, huma.Error400BadRequest("invalid cursor or changed filters")
			}
			f.AfterTime = &c.Time
			f.AfterID = c.ID
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		items, err := db.Observations(ctx, f)
		if errors.Is(err, store.ErrNotFound) {
			return nil, huma.Error404NotFound("GPU not found")
		}
		if err != nil {
			return nil, dbError(err)
		}
		out := &TelemetryOutput{}
		if len(items) > in.Limit {
			last := items[in.Limit-1]
			b, _ := json.Marshal(cursor{in.ID, in.Start, in.End, last.Timestamp, last.EventID})
			out.Body.Next = base64.RawURLEncoding.EncodeToString(b)
			items = items[:in.Limit]
		}
		out.Body.Items = items
		return out, nil
	})
	return api
}

func dbError(err error) error {
	slog.Error("API database request failed", "error", err)
	return huma.Error503ServiceUnavailable("telemetry storage unavailable")
}

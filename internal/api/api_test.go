package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

const gpu = "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"

type reader struct {
	filter store.Filter
	items  []model.Telemetry
	err    error
}

func (r *reader) GPUs(context.Context, string, int) ([]model.GPU, error) {
	return []model.GPU{{ID: gpu}}, r.err
}
func (r *reader) Observations(_ context.Context, f store.Filter) ([]model.Telemetry, error) {
	r.filter = f
	return r.items, r.err
}
func request(t *testing.T, m *http.ServeMux, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}
func TestValidation(t *testing.T) {
	m := http.NewServeMux()
	Register(m, &reader{})
	for _, path := range []string{"/api/v1/gpus?limit=0", "/api/v1/gpus?limit=1001", "/api/v1/gpus?after=0", "/api/v1/gpus/0/telemetry", "/api/v1/gpus/" + gpu + "/telemetry?start_time=wrong", "/api/v1/gpus/" + gpu + "/telemetry?start_time=2026-01-02T00:00:00Z&end_time=2026-01-01T00:00:00Z", "/api/v1/gpus/" + gpu + "/telemetry?cursor=invalid"} {
		t.Run(path, func(t *testing.T) {
			w := request(t, m, path)
			if w.Code < 400 || w.Code >= 500 {
				t.Fatalf("status %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestPaginationAndFilters(t *testing.T) {
	r := &reader{items: []model.Telemetry{{EventID: model.NewID(), Timestamp: time.Now().UTC()}, {EventID: model.NewID(), Timestamp: time.Now().UTC()}}}
	m := http.NewServeMux()
	Register(m, r)
	path := "/api/v1/gpus/" + gpu + "/telemetry?limit=1&start_time=2026-01-01T00:00:00Z"
	w := request(t, m, path)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var page TelemetryOutput
	if err := json.Unmarshal(w.Body.Bytes(), &page.Body); err != nil {
		t.Fatal(err)
	}
	if len(page.Body.Items) != 1 || page.Body.Next == "" {
		t.Fatal("pagination missing")
	}
	w = request(t, m, path+"&cursor="+url.QueryEscape(page.Body.Next))
	if w.Code != 200 || r.filter.AfterID != r.items[0].EventID || r.filter.Start == nil {
		t.Fatal("cursor not applied")
	}
	w = request(t, m, "/api/v1/gpus/"+gpu+"/telemetry?cursor="+url.QueryEscape(page.Body.Next))
	if w.Code != 400 {
		t.Fatal("changed filter accepted")
	}
}
func TestNotFoundAndOpenAPI(t *testing.T) {
	m := http.NewServeMux()
	Register(m, &reader{err: store.ErrNotFound})
	if w := request(t, m, "/api/v1/gpus/"+gpu+"/telemetry"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	w := request(t, m, "/openapi.json")
	var spec map[string]any
	if json.Unmarshal(w.Body.Bytes(), &spec) != nil || spec["openapi"] != "3.1.0" {
		t.Fatal("missing generated OpenAPI")
	}
}

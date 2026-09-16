// Package worker implements cancellable CSV producers and telemetry consumers.
package worker

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/broker"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
)

type Publisher interface {
	Publish(context.Context, model.Event) error
}
type StreamConfig struct {
	Path, Producer string
	Rate           int
	Loops          int
	RetryWindow    time.Duration
}

var columns = []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "modelName", "Hostname", "container", "pod", "namespace", "value", "labels_raw"}

func Stream(ctx context.Context, p Publisher, c StreamConfig) error {
	if c.Rate < 1 || c.Rate > 1000000 || c.Loops < 0 || c.RetryWindow <= 0 || c.RetryWindow > time.Hour {
		return errors.New("invalid streamer configuration")
	}
	ticker := time.NewTicker(time.Second / time.Duration(c.Rate))
	defer ticker.Stop()
	for loop := 1; c.Loops == 0 || loop <= c.Loops; loop++ {
		if err := streamFile(ctx, p, c, ticker, loop); err != nil {
			return err
		}
	}
	return nil
}

func streamFile(ctx context.Context, p Publisher, c StreamConfig, ticker *time.Ticker, loop int) error {
	f, err := os.Open(c.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return err
	}
	index := map[string]int{}
	for i, h := range header {
		if _, ok := index[h]; ok {
			return fmt.Errorf("duplicate CSV column %q", h)
		}
		index[h] = i
	}
	for _, h := range columns {
		if _, ok := index[h]; !ok {
			return fmt.Errorf("missing CSV column %q", h)
		}
	}
	for row := 1; ; row++ {
		v, err := r.Read()
		if err == io.EOF {
			if row == 1 {
				return errors.New("CSV contains no data rows")
			}
			slog.Info("CSV pass completed", "loop", loop, "rows", row-1)
			return nil
		}
		if err != nil {
			return fmt.Errorf("CSV row %d: %w", row, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		get := func(k string) string { return v[index[k]] }
		e := model.Event{ID: model.NewID(), Producer: c.Producer, Loop: loop, Row: row, Record: model.Record{Timestamp: get("timestamp"), Metric: get("metric_name"), GPUIndex: get("gpu_id"), Device: get("device"), UUID: get("uuid"), Model: get("modelName"), Host: get("Hostname"), Container: get("container"), Pod: get("pod"), Namespace: get("namespace"), Value: get("value"), Labels: get("labels_raw")}}
		if err = publish(ctx, p, e, c.RetryWindow); err != nil {
			return fmt.Errorf("publish row %d event %s: %w", row, e.ID, err)
		}
	}
}

func publish(ctx context.Context, p Publisher, e model.Event, window time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	for attempt := 0; ; attempt++ {
		err := p.Publish(ctx, e)
		if err == nil {
			return nil
		}
		var he *broker.HTTPError
		if errors.As(err, &he) && !he.Temporary() {
			return err
		}
		slog.Warn("publish retry", "event_id", e.ID, "error", err, "attempt", attempt+1)
		if err = Sleep(ctx, Backoff(attempt)); err != nil {
			return err
		}
	}
}

func Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 7 {
		attempt = 7
	}
	base := 250 * time.Millisecond * time.Duration(1<<attempt)
	if base > 30*time.Second {
		base = 30 * time.Second
	}
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

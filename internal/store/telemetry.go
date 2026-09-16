package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
)

// Persist inserts one observation and its GPU atomically. Redelivery preserves
// the first committed timestamp. A conflicting reuse of an ID is not ignored.
func (s *Store) Persist(ctx context.Context, e model.Event) (bool, error) {
	g, value, err := e.Record.Parse()
	if err != nil {
		return false, err
	}
	_, hash, err := Encode(e)
	if err != nil {
		return false, err
	}
	b, err := json.Marshal(e.Record)
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO telemetry.gpus(uuid,host,gpu_index,device,model) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, g.ID, g.Host, g.Index, g.Device, g.Model)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO telemetry.observations(event_id,gpu_uuid,metric_name,value,source,payload_hash) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, e.ID, g.ID, e.Record.Metric, value, b, hash)
	if err != nil {
		return false, err
	}
	duplicate := tag.RowsAffected() == 0
	if duplicate {
		var old string
		if err = tx.QueryRow(ctx, `SELECT payload_hash FROM telemetry.observations WHERE event_id=$1`, e.ID).Scan(&old); err != nil {
			return false, err
		}
		if old != hash {
			return false, ErrConflict
		}
	}
	return duplicate, tx.Commit(ctx)
}

func (s *Store) GPUs(ctx context.Context, after string, limit int) ([]model.GPU, error) {
	rows, err := s.Pool.Query(ctx, `SELECT uuid,host,gpu_index,device,model FROM telemetry.gpus WHERE uuid>$1 ORDER BY uuid LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.GPU{}
	for rows.Next() {
		var g model.GPU
		if err = rows.Scan(&g.ID, &g.Host, &g.Index, &g.Device, &g.Model); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

type Filter struct {
	GPU       string
	Start     *time.Time
	End       *time.Time
	AfterTime *time.Time
	AfterID   string
	Limit     int
}

func (s *Store) Observations(ctx context.Context, f Filter) ([]model.Telemetry, error) {
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM telemetry.gpus WHERE uuid=$1)`, f.GPU).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.Pool.Query(ctx, `SELECT event_id,processed_at,gpu_uuid,metric_name,value,source FROM telemetry.observations
	 WHERE gpu_uuid=$1 AND ($2::timestamptz IS NULL OR processed_at >= $2) AND ($3::timestamptz IS NULL OR processed_at <= $3)
	 AND ($4::timestamptz IS NULL OR (processed_at,event_id)>($4,$5)) ORDER BY processed_at,event_id LIMIT $6`, f.GPU, f.Start, f.End, f.AfterTime, f.AfterID, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Telemetry{}
	for rows.Next() {
		var v model.Telemetry
		var b []byte
		if err = rows.Scan(&v.EventID, &v.Timestamp, &v.GPUUUID, &v.Metric, &v.Value, &b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &v.Source); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Package store provides durable queue and telemetry operations. Every queue
// transition is committed before its result is returned to a client.
package store

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var (
	ErrFull     = errors.New("queue capacity reached")
	ErrConflict = errors.New("event ID already has a different payload")
	ErrLease    = errors.New("lease expired or superseded")
	ErrNotFound = errors.New("not found")
)

type Store struct {
	Pool        *pgxpool.Pool
	Capacity    int
	Lease       time.Duration
	MaxAttempts int
}

func Open(ctx context.Context, dsn string, connections int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = connections
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: p, Capacity: 100000, Lease: 30 * time.Second, MaxAttempts: 5}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Serialize installation/upgrade jobs without holding application locks.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(742193)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

// Ready also verifies migration completion; database connectivity alone is not
// sufficient to advertise a usable broker or API during first installation.
func (s *Store) Ready(ctx context.Context) error {
	var ready bool
	err := s.Pool.QueryRow(ctx, `SELECT to_regclass('queue.messages') IS NOT NULL AND to_regclass('telemetry.observations') IS NOT NULL`).Scan(&ready)
	if err != nil {
		return err
	}
	if !ready {
		return errors.New("schema not installed")
	}
	return nil
}

func Encode(e model.Event) ([]byte, string, error) {
	b, err := json.Marshal(e)
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:]), err
}

// Publish is idempotent for the lifetime of the message or completion tombstone.
func (s *Store) Publish(ctx context.Context, e model.Event) (bool, error) {
	b, hash, err := Encode(e)
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO queue.messages(event_id,payload,payload_hash) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, e.ID, b, hash)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		var old string
		if err = tx.QueryRow(ctx, `SELECT payload_hash FROM queue.messages WHERE event_id=$1`, e.ID).Scan(&old); err != nil {
			return false, err
		}
		if old != hash {
			return false, ErrConflict
		}
		return true, tx.Commit(ctx)
	}
	// An atomic reservation prevents concurrent brokers from overfilling the queue.
	tag, err = tx.Exec(ctx, `UPDATE queue.capacity SET outstanding=outstanding+1 WHERE id=1 AND outstanding<$1`, s.Capacity)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, ErrFull
	}
	return false, tx.Commit(ctx)
}

type Delivery struct {
	Event      model.Event `json:"event"`
	Token      string      `json:"lease_token"`
	LeaseUntil time.Time   `json:"lease_until"`
	Attempt    int         `json:"attempt"`
	EnqueuedAt time.Time   `json:"enqueued_at"`
}

func (s *Store) Claim(ctx context.Context) (*Delivery, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Recover a bounded batch using the expiry index. Separating this from the
	// ready claim keeps a growing backlog off the claim's sort/filter hot path.
	_, err = tx.Exec(ctx, `UPDATE queue.messages SET
	 state=CASE WHEN attempts >= $1 THEN 'dead' ELSE 'ready' END,
	 last_error=CASE WHEN attempts >= $1 THEN 'delivery attempts exhausted' ELSE last_error END,
	 lease_until=NULL,available_at=now() WHERE event_id IN
	 (SELECT event_id FROM queue.messages WHERE state='leased' AND lease_until<=now() ORDER BY lease_until LIMIT 100 FOR UPDATE SKIP LOCKED)`, s.MaxAttempts)
	if err != nil {
		return nil, err
	}
	d := &Delivery{Token: model.NewID()}
	var payload []byte
	err = tx.QueryRow(ctx, `WITH candidate AS (
	 SELECT event_id FROM queue.messages WHERE state='ready' AND available_at<=now()
	 ORDER BY available_at,sequence LIMIT 1 FOR UPDATE SKIP LOCKED)
	 UPDATE queue.messages m SET state='leased',lease_token=$1,
	 lease_until=clock_timestamp()+($2 * interval '1 millisecond'),attempts=attempts+1
	 FROM candidate c WHERE m.event_id=c.event_id RETURNING m.payload,m.lease_until,m.attempts,m.created_at`, d.Token, s.Lease.Milliseconds()).Scan(&payload, &d.LeaseUntil, &d.Attempt, &d.EnqueuedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(payload, &d.Event); err != nil {
		return nil, err
	}
	return d, tx.Commit(ctx)
}

func (s *Store) Ack(ctx context.Context, id, token string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE queue.messages SET state='done',completed_at=clock_timestamp(),payload=NULL,lease_until=NULL
	 WHERE event_id=$1 AND state='leased' AND lease_token=$2 AND lease_until>clock_timestamp()`, id, token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var done bool
		err = tx.QueryRow(ctx, `SELECT state='done' AND lease_token=$2 FROM queue.messages WHERE event_id=$1`, id, token).Scan(&done)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !done {
			return ErrLease
		}
	} else if _, err = tx.Exec(ctx, `UPDATE queue.capacity SET outstanding=outstanding-1 WHERE id=1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Renew(ctx context.Context, id, token string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE queue.messages SET lease_until=clock_timestamp()+($3 * interval '1 millisecond')
	 WHERE event_id=$1 AND state='leased' AND lease_token=$2 AND lease_until>clock_timestamp()`, id, token, s.Lease.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLease
	}
	return nil
}

// Reject distinguishes permanent poison records from infrastructure failures.
func (s *Store) Reject(ctx context.Context, id, token, reason string, permanent bool, delay time.Duration) error {
	state := "ready"
	if permanent {
		state = "dead"
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE queue.messages SET state=$3,last_error=$4,lease_until=NULL,
	 available_at=clock_timestamp()+($5 * interval '1 millisecond'),
	 attempts=CASE WHEN $3='ready' THEN GREATEST(0,attempts-1) ELSE attempts END
	 WHERE event_id=$1 AND state='leased' AND lease_token=$2 AND lease_until>clock_timestamp()`, id, token, state, reason, delay.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLease
	}
	return nil
}

// Cleanup retains completed publication identities for at least 24 hours.
// Outstanding and dead-letter payloads are never automatically discarded.
func (s *Store) Cleanup(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM queue.messages WHERE event_id IN (SELECT event_id FROM queue.messages WHERE state='done' AND completed_at<now()-interval '24 hours' ORDER BY completed_at LIMIT 1000 FOR UPDATE SKIP LOCKED)`)
	return err
}

type Stats struct {
	Ready         int64   `json:"ready"`
	Leased        int64   `json:"leased"`
	Done          int64   `json:"done"`
	Dead          int64   `json:"dead"`
	OldestSeconds float64 `json:"oldest_seconds"`
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var v Stats
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='ready'),count(*) FILTER(WHERE state='leased'),count(*) FILTER(WHERE state='done'),count(*) FILTER(WHERE state='dead'),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at) FILTER(WHERE state IN ('ready','leased'))),0)::float8 FROM queue.messages`).Scan(&v.Ready, &v.Leased, &v.Done, &v.Dead, &v.OldestSeconds)
	return v, err
}

type DeadLetter struct {
	ID       string      `json:"event_id"`
	Reason   string      `json:"reason"`
	Attempts int         `json:"attempts"`
	Payload  model.Event `json:"payload"`
}

func (s *Store) Dead(ctx context.Context) ([]DeadLetter, error) {
	rows, err := s.Pool.Query(ctx, `SELECT event_id,last_error,attempts,payload FROM queue.messages WHERE state='dead' ORDER BY sequence LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DeadLetter{}
	for rows.Next() {
		var d DeadLetter
		var b []byte
		if err = rows.Scan(&d.ID, &d.Reason, &d.Attempts, &b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &d.Payload); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
func (s *Store) Replay(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE queue.messages SET state='ready',attempts=0,last_error='',lease_token='',lease_until=NULL,available_at=clock_timestamp() WHERE event_id=$1 AND state='dead'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

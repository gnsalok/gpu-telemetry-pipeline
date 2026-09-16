package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gnsalok/gpu-telemetry-pipeline/internal/model"
	"github.com/gnsalok/gpu-telemetry-pipeline/internal/store"
)

type Client struct {
	URL, Token string
	HTTP       *http.Client
}
type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string   { return fmt.Sprintf("broker HTTP %d: %s", e.Status, e.Message) }
func (e *HTTPError) Temporary() bool { return e.Status == 429 || e.Status >= 500 }

func (c *Client) request(ctx context.Context, path string, in, out any) (int, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return 0, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/queue/v1/"+path, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	r.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		r.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(r)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return resp.StatusCode, &HTTPError{resp.StatusCode, strings.TrimSpace(string(body))}
	}
	if out != nil && resp.StatusCode != 204 {
		if err = json.NewDecoder(io.LimitReader(resp.Body, 128*1024)).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}
func (c *Client) Publish(ctx context.Context, e model.Event) error {
	_, err := c.request(ctx, "publish", e, nil)
	return err
}
func (c *Client) Claim(ctx context.Context) (*store.Delivery, error) {
	var d store.Delivery
	code, err := c.request(ctx, "claim", nil, &d)
	if err != nil || code == 204 {
		return nil, err
	}
	return &d, nil
}
func (c *Client) Ack(ctx context.Context, id, token string) error {
	_, err := c.request(ctx, "ack", Receipt{ID: id, Token: token}, nil)
	return err
}
func (c *Client) Renew(ctx context.Context, id, token string) error {
	_, err := c.request(ctx, "renew", Receipt{ID: id, Token: token}, nil)
	return err
}
func (c *Client) Reject(ctx context.Context, v Receipt) error {
	_, err := c.request(ctx, "reject", v, nil)
	return err
}

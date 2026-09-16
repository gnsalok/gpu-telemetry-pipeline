// Package model defines the transport record and validated telemetry value.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Record preserves CSV values, including optional workload and source metadata.
type Record struct {
	Timestamp string `json:"source_timestamp"`
	Metric    string `json:"metric_name"`
	GPUIndex  string `json:"gpu_id"`
	Device    string `json:"device"`
	UUID      string `json:"uuid"`
	Model     string `json:"model"`
	Host      string `json:"host"`
	Container string `json:"container,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Value     string `json:"value"`
	Labels    string `json:"labels_raw"`
}

type Event struct {
	ID       string `json:"event_id"`
	Producer string `json:"producer_id"`
	Loop     int    `json:"loop"`
	Row      int    `json:"row"`
	Record   Record `json:"record"`
}

type GPU struct {
	ID     string `json:"id"`
	Host   string `json:"host"`
	Index  int    `json:"gpu_index"`
	Device string `json:"device"`
	Model  string `json:"model"`
}

type Telemetry struct {
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	GPUUUID   string    `json:"gpu_uuid"`
	Metric    string    `json:"metric_name"`
	Value     float64   `json:"value"`
	Source    Record    `json:"source"`
}

var gpuPattern = regexp.MustCompile(`^GPU-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func ValidGPU(id string) bool { return gpuPattern.MatchString(id) }

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func ValidID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && strings.ToLower(id) == id
}

func (r Record) Parse() (GPU, float64, error) {
	if !ValidGPU(r.UUID) {
		return GPU{}, 0, errors.New("invalid GPU UUID")
	}
	if strings.TrimSpace(r.Metric) == "" || strings.TrimSpace(r.Host) == "" || strings.TrimSpace(r.Device) == "" || strings.TrimSpace(r.Model) == "" {
		return GPU{}, 0, errors.New("metric, host, device and model are required")
	}
	i, err := strconv.Atoi(r.GPUIndex)
	if err != nil || i < 0 {
		return GPU{}, 0, errors.New("invalid GPU index")
	}
	v, err := strconv.ParseFloat(r.Value, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return GPU{}, 0, fmt.Errorf("invalid finite metric value: %q", r.Value)
	}
	return GPU{ID: r.UUID, Host: r.Host, Index: i, Device: r.Device, Model: r.Model}, v, nil
}

// Package config validates environment configuration before services start.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DSN, Address, BrokerURL, Token, CSV, Producer string
	Connections, Workers, Rate, Loops, Capacity   int
	RetryWindow                                   time.Duration
}

func Load() (Config, error) {
	c := Config{DSN: os.Getenv("DATABASE_URL"), Address: env("HTTP_ADDR", ":8080"), BrokerURL: env("BROKER_URL", "http://localhost:8081"), Token: os.Getenv("BROKER_TOKEN"), CSV: env("CSV_PATH", "data/telemetry.csv"), Producer: env("HOSTNAME", "local-streamer"), RetryWindow: time.Hour}
	for _, v := range []struct {
		k             string
		dst           *int
		def, min, max int
	}{{"DB_POOL_SIZE", &c.Connections, 4, 1, 32}, {"WORKERS", &c.Workers, 2, 1, 32}, {"RATE", &c.Rate, 100, 1, 1000000}, {"LOOPS", &c.Loops, 0, 0, 1000000}, {"QUEUE_CAPACITY", &c.Capacity, 100000, 1, 10000000}} {
		raw := env(v.k, strconv.Itoa(v.def))
		n, err := strconv.Atoi(raw)
		if err != nil || n < v.min || n > v.max {
			return c, fmt.Errorf("%s must be between %d and %d", v.k, v.min, v.max)
		}
		*v.dst = n
	}
	u, err := url.Parse(c.BrokerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return c, fmt.Errorf("BROKER_URL must be an HTTP(S) URL")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

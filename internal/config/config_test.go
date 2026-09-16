package config

import "testing"

func TestConfiguration(t *testing.T) {
	c, err := Load()
	if err != nil || c.Workers != 2 || c.Rate != 100 {
		t.Fatalf("defaults %+v %v", c, err)
	}
	for _, k := range []string{"RATE", "WORKERS", "DB_POOL_SIZE", "QUEUE_CAPACITY"} {
		t.Run(k, func(t *testing.T) {
			t.Setenv(k, "-1")
			if _, err := Load(); err == nil {
				t.Fatal("invalid value accepted")
			}
		})
	}
	t.Setenv("BROKER_URL", "file:///tmp/test")
	if _, err = Load(); err == nil {
		t.Fatal("invalid broker URL accepted")
	}
}

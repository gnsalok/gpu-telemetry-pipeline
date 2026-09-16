package model

import "testing"

func TestRecordValidation(t *testing.T) {
	valid := Record{UUID: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50", GPUIndex: "0", Host: "host", Device: "nvidia0", Model: "H100", Metric: "GPU_UTIL", Value: "0"}
	for _, value := range []string{"0", "689.457", "-1"} {
		r := valid
		r.Value = value
		if _, _, err := r.Parse(); err != nil {
			t.Fatalf("finite value %s: %v", value, err)
		}
	}
	for name, change := range map[string]func(*Record){"nan": func(r *Record) { r.Value = "NaN" }, "infinity": func(r *Record) { r.Value = "+Inf" }, "blank": func(r *Record) { r.Value = "" }, "uuid": func(r *Record) { r.UUID = "0" }, "index": func(r *Record) { r.GPUIndex = "-1" }, "metric": func(r *Record) { r.Metric = " " }, "host": func(r *Record) { r.Host = "" }} {
		t.Run(name, func(t *testing.T) {
			r := valid
			change(&r)
			if _, _, err := r.Parse(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
func TestIDs(t *testing.T) {
	a, b := NewID(), NewID()
	if a == b || !ValidID(a) || ValidID("xyz") || ValidID("aa") {
		t.Fatal("invalid identity behavior")
	}
}

package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDedupKey(t *testing.T) {
	if got := DedupKey(" evt_1 ", []byte("x")); got != "id:evt_1" {
		t.Errorf("header id: got %q", got)
	}
	a, b := DedupKey("", []byte(`{"a":1}`)), DedupKey("", []byte(`{"a":1}`))
	if a != b || !strings.HasPrefix(a, "sha256:") {
		t.Errorf("body hash should be stable: %q vs %q", a, b)
	}
	if DedupKey("", []byte(`{"a":2}`)) == a {
		t.Error("different bodies must not collide")
	}
	if got := DedupKey(strings.Repeat("x", 300), []byte("x")); !strings.HasPrefix(got, "sha256:") {
		t.Errorf("oversized id should fall back to the hash, got %q", got)
	}
}

func TestEventType(t *testing.T) {
	var p map[string]json.RawMessage
	json.Unmarshal([]byte(`{"type":"order.created","data":{"object":{"type":"invoice.paid"}},"n":3,"empty":""}`), &p)
	for path, want := range map[string]string{
		"type":             "order.created",
		"data.object.type": "invoice.paid",
		"missing":          "unknown",
		"n":                "unknown", // not a string
		"empty":            "unknown",
		"type.deeper":      "unknown",
	} {
		if got := EventType(p, path); got != want {
			t.Errorf("EventType(%q) = %q, want %q", path, got, want)
		}
	}
}

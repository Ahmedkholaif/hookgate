package signature

import (
	"errors"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"id":"evt_1","type":"invoice.paid"}`)
	hdr := Sign(body, now, "whsec_a")

	tests := []struct {
		name   string
		secret string
		header string
		body   []byte
		at     time.Time
		want   error
	}{
		{"valid", "whsec_a", hdr, body, now, nil},
		{"within tolerance", "whsec_a", hdr, body, now.Add(4 * time.Minute), nil},
		{"tampered body", "whsec_a", hdr, []byte(`{"id":"evt_1","type":"invoice.void"}`), now, ErrMismatch},
		{"wrong secret", "whsec_b", hdr, body, now, ErrMismatch},
		{"replayed later", "whsec_a", hdr, body, now.Add(10 * time.Minute), ErrExpired},
		{"from the future", "whsec_a", hdr, body, now.Add(-10 * time.Minute), ErrExpired},
		{"garbage", "whsec_a", "nonsense", body, now, ErrMalformed},
		{"no v1", "whsec_a", "t=1800000000", body, now, ErrMalformed},
		{"bad hex", "whsec_a", "t=1800000000,v1=zz", body, now, ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Verify(tt.secret, tt.header, tt.body, 5*time.Minute, tt.at); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestRotationAcceptsEitherSecret(t *testing.T) {
	now := time.Now()
	body := []byte("{}")
	hdr := Sign(body, now, "old", "new")
	for _, s := range []string{"old", "new"} {
		if err := Verify(s, hdr, body, time.Minute, now); err != nil {
			t.Fatalf("secret %q: %v", s, err)
		}
	}
}

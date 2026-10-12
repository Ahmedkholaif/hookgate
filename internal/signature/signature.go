// Package signature verifies (and, for tests and senders, creates) webhook
// signatures. It is the same scheme hookline signs with, so hookgate can
// ingest hookline deliveries directly.
//
// The header format is "t=<unix seconds>,v1=<hex HMAC-SHA256>", where the MAC
// covers "<t>.<raw body>". Binding the timestamp into the MAC lets receivers
// reject replays older than a tolerance window. A header may carry several v1
// values so that a secret can be rotated without dropping deliveries.
package signature

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Header is the default HTTP header that carries the signature. Sources can
// name a different one (hookline sends Hookline-Signature).
const Header = "Hookgate-Signature"

var (
	ErrMalformed = errors.New("signature: malformed header")
	ErrExpired   = errors.New("signature: timestamp outside tolerance")
	ErrMismatch  = errors.New("signature: no matching signature")
)

// Sign returns the header value for body signed with each secret at time ts.
func Sign(body []byte, ts time.Time, secrets ...string) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	var b strings.Builder
	b.WriteString("t=" + t)
	for _, s := range secrets {
		b.WriteString(",v1=" + hex.EncodeToString(mac(s, t, body)))
	}
	return b.String()
}

// Verify checks header against body. It succeeds if any v1 signature matches
// secret and the timestamp is within tolerance of now (in either direction).
func Verify(secret, header string, body []byte, tolerance time.Duration, now time.Time) error {
	var t string
	var sigs [][]byte
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrMalformed
		}
		switch k {
		case "t":
			t = v
		case "v1":
			sig, err := hex.DecodeString(v)
			if err != nil {
				return ErrMalformed
			}
			sigs = append(sigs, sig)
		}
	}
	unix, err := strconv.ParseInt(t, 10, 64)
	if err != nil || len(sigs) == 0 {
		return ErrMalformed
	}
	if d := now.Sub(time.Unix(unix, 0)); d > tolerance || d < -tolerance {
		return ErrExpired
	}
	want := mac(secret, t, body)
	for _, sig := range sigs {
		if hmac.Equal(sig, want) { // constant time
			return nil
		}
	}
	return ErrMismatch
}

func mac(secret, t string, body []byte) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(t))
	h.Write([]byte("."))
	h.Write(body)
	return h.Sum(nil)
}

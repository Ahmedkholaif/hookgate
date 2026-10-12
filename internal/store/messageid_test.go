package store

import "testing"

// hookgate-nest asserts the same vector, so both implementations derive
// identical message ids.
func TestMessageIDVector(t *testing.T) {
	const want = "4236585a-caa3-50c9-b45b-78fc4e7a4727"
	if got := MessageID("0b0f3f3e-6c43-4d4c-9a51-1f0d5b5c2a10", "billing"); got != want {
		t.Fatalf("MessageID = %s, want %s", got, want)
	}
}

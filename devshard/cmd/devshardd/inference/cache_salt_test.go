package inference

import (
	"encoding/json"
	"testing"
)

func TestWithCacheSalt(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)

	out := withCacheSalt(body, "sess-A")
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("output not valid json: %v", err)
	}
	salt, ok := m["cache_salt"].(string)
	if !ok || salt == "" {
		t.Fatalf("cache_salt not set, got %v", m["cache_salt"])
	}
	if m["model"] != "m" {
		t.Fatalf("original fields must be preserved; model=%v", m["model"])
	}

	// Deterministic per session (so a client's own follow-ups reuse its cache).
	if string(withCacheSalt(body, "sess-A")) != string(out) {
		t.Fatal("same session must yield the same salt")
	}

	// Different sessions get different salts (isolation — no shared KV blocks).
	var mb map[string]any
	if err := json.Unmarshal(withCacheSalt(body, "sess-B"), &mb); err != nil {
		t.Fatal(err)
	}
	if mb["cache_salt"] == m["cache_salt"] {
		t.Fatal("different sessions must get different cache_salt")
	}

	// Unparseable body passes through unchanged (best-effort, never breaks a request).
	bad := []byte(`{not json`)
	if string(withCacheSalt(bad, "s")) != string(bad) {
		t.Fatal("unparseable body must pass through unchanged")
	}
}

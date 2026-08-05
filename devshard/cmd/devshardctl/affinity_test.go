package main

import (
	"testing"
	"time"
)

func testAffinity(cfg affinityConfig) (*affinityTracker, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	t := newAffinityTracker(cfg)
	t.now = func() time.Time { return now }
	return t, &now
}

func alwaysMember(string) bool { return true }

func TestAffinityDisabledIsNoOp(t *testing.T) {
	cfg := defaultAffinityConfig() // Enabled=false
	tr, _ := testAffinity(cfg)
	tr.Record("sess", "hostA")
	if _, ok := tr.Pick("sess", alwaysMember); ok {
		t.Fatal("disabled tracker must never return a sticky host")
	}
}

func TestAffinitySticksThenReRandomisesOnRequestBound(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	cfg.MaxRequests = 3
	tr, _ := testAffinity(cfg)

	// First request of the session: no binding yet -> route naturally, record.
	if _, ok := tr.Pick("sess", alwaysMember); ok {
		t.Fatal("first Pick must miss")
	}
	tr.Record("sess", "hostA") // count=1

	// Requests 2 and 3 stick to hostA.
	for i := 2; i <= 3; i++ {
		got, ok := tr.Pick("sess", alwaysMember)
		if !ok || got != "hostA" {
			t.Fatalf("request %d: want sticky hostA, got %q ok=%v", i, got, ok)
		}
		tr.Record("sess", "hostA") // count=2, then 3 -> evict on the 3rd
	}

	// After MaxRequests (3) the binding is evicted -> re-randomise.
	if _, ok := tr.Pick("sess", alwaysMember); ok {
		t.Fatal("after the request bound the session must re-randomise")
	}
}

func TestAffinityExpiresOnTTL(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	cfg.MaxRequests = 100 // ensure TTL is the binding constraint
	cfg.TTL = time.Minute
	tr, now := testAffinity(cfg)

	tr.Record("sess", "hostA")
	if got, ok := tr.Pick("sess", alwaysMember); !ok || got != "hostA" {
		t.Fatalf("within TTL want hostA, got %q ok=%v", got, ok)
	}

	*now = now.Add(cfg.TTL) // exactly TTL later -> expired
	if _, ok := tr.Pick("sess", alwaysMember); ok {
		t.Fatal("binding must expire at TTL")
	}
}

func TestAffinityDropsHostThatLeftGroup(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	tr, _ := testAffinity(cfg)
	tr.Record("sess", "hostGone")

	onlyOthers := func(p string) bool { return p != "hostGone" }
	if _, ok := tr.Pick("sess", onlyOthers); ok {
		t.Fatal("a sticky host no longer in the group must be dropped")
	}
	// And the entry is gone, so a fresh natural landing rebinds cleanly.
	tr.Record("sess", "hostB")
	if got, ok := tr.Pick("sess", alwaysMember); !ok || got != "hostB" {
		t.Fatalf("want rebind to hostB, got %q ok=%v", got, ok)
	}
}

func TestAffinityRebindsOnFallbackToDifferentHost(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	cfg.MaxRequests = 5
	tr, _ := testAffinity(cfg)

	tr.Record("sess", "hostA") // count=1
	tr.Record("sess", "hostA") // count=2
	// Primary fell back to a different host (sticky was busy): rebind fresh.
	tr.Record("sess", "hostB")
	got, ok := tr.Pick("sess", alwaysMember)
	if !ok || got != "hostB" {
		t.Fatalf("want rebind to hostB, got %q ok=%v", got, ok)
	}
}

func TestAffinityKeyFromBody(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"prompt_cache_key wins", `{"prompt_cache_key":"conv-1","user":"u-9"}`, "conv-1"},
		{"user fallback", `{"user":"u-9"}`, "u-9"},
		{"prefer prompt_cache_key over user", `{"user":"u-9","prompt_cache_key":"conv-1"}`, "conv-1"},
		{"neither", `{"model":"m","messages":[]}`, ""},
		{"non-string prompt_cache_key skips to user", `{"prompt_cache_key":123,"user":"u-9"}`, "u-9"},
		{"non-string both", `{"prompt_cache_key":123,"user":{"id":"x"}}`, ""},
		{"whitespace trimmed to empty", `{"prompt_cache_key":"   "}`, ""},
		{"invalid json", `{not json`, ""},
		{"empty body", ``, ""},
	}
	for _, c := range cases {
		if got := affinityKeyFromBody([]byte(c.body)); got != c.want {
			t.Errorf("%s: affinityKeyFromBody(%q) = %q, want %q", c.name, c.body, got, c.want)
		}
	}
}

func TestAffinityMapIsBounded(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	cfg.MaxRequests = 100 // don't let the request bound evict during this test
	cfg.TTL = time.Hour
	cfg.MaxEntries = 100
	tr, _ := testAffinity(cfg)

	// A stream of distinct, never-repeated session keys (the leak scenario:
	// short conversations that are never Picked again).
	for i := 0; i < 10*cfg.MaxEntries; i++ {
		tr.Record(sessionKey(i), "hostA")
	}
	tr.mu.Lock()
	n := len(tr.byKey)
	tr.mu.Unlock()
	if n > cfg.MaxEntries {
		t.Fatalf("map must stay bounded by MaxEntries=%d, got %d", cfg.MaxEntries, n)
	}
}

func TestAffinitySweepReclaimsExpiredFirst(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	cfg.MaxRequests = 100
	cfg.TTL = time.Minute
	cfg.MaxEntries = 10
	tr, now := testAffinity(cfg)

	// Fill to the cap, then let them all expire.
	for i := 0; i < cfg.MaxEntries; i++ {
		tr.Record(sessionKey(i), "hostA")
	}
	*now = now.Add(cfg.TTL) // everything above is now expired

	// One more insert triggers a sweep that should reclaim the expired ones,
	// leaving just the fresh entry.
	tr.Record("fresh", "hostB")
	tr.mu.Lock()
	n := len(tr.byKey)
	tr.mu.Unlock()
	if n != 1 {
		t.Fatalf("sweep should reclaim expired entries first; want 1, got %d", n)
	}
}

func sessionKey(i int) string { return "sess-" + string(rune('a'+i%26)) + "-" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

func TestAffinityEmptyKeyIsNoOp(t *testing.T) {
	cfg := defaultAffinityConfig()
	cfg.Enabled = true
	tr, _ := testAffinity(cfg)
	tr.Record("", "hostA")
	if _, ok := tr.Pick("", alwaysMember); ok {
		t.Fatal("empty affinity key must never bind")
	}
}

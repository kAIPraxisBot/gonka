package broker

import (
	"testing"
	"time"
)

func testNodeAffinity(cfg nodeAffinityConfig) (*nodeSessionAffinity, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	a := newNodeSessionAffinity(cfg)
	a.now = func() time.Time { return now }
	return a, &now
}

func TestNodeAffinityStickThenReRandomiseOnRequestBound(t *testing.T) {
	cfg := nodeAffinityConfig{MaxRequests: 3, TTL: time.Hour, MaxEntries: 100}
	a, _ := testNodeAffinity(cfg)

	if _, ok := a.pick("sess"); ok {
		t.Fatal("first pick must miss")
	}
	a.record("sess", "nodeA") // count=1
	for i := 2; i <= 3; i++ {
		got, ok := a.pick("sess")
		if !ok || got != "nodeA" {
			t.Fatalf("req %d: want sticky nodeA, got %q ok=%v", i, got, ok)
		}
		a.record("sess", "nodeA") // count 2, then 3 -> evict
	}
	if _, ok := a.pick("sess"); ok {
		t.Fatal("after the request bound the session must re-randomise (load rebalance)")
	}
}

func TestNodeAffinityExpiresOnTTL(t *testing.T) {
	cfg := nodeAffinityConfig{MaxRequests: 1000, TTL: time.Minute, MaxEntries: 100}
	a, now := testNodeAffinity(cfg)
	a.record("sess", "nodeA")
	if got, ok := a.pick("sess"); !ok || got != "nodeA" {
		t.Fatalf("within TTL want nodeA, got %q ok=%v", got, ok)
	}
	*now = now.Add(cfg.TTL)
	if _, ok := a.pick("sess"); ok {
		t.Fatal("binding must expire at TTL")
	}
}

func TestNodeAffinityRebindsOnDifferentNode(t *testing.T) {
	cfg := nodeAffinityConfig{MaxRequests: 5, TTL: time.Hour, MaxEntries: 100}
	a, _ := testNodeAffinity(cfg)
	a.record("sess", "nodeA")
	a.record("sess", "nodeA")
	a.record("sess", "nodeB") // fell back to a different node -> rebind fresh
	got, ok := a.pick("sess")
	if !ok || got != "nodeB" {
		t.Fatalf("want rebind to nodeB, got %q ok=%v", got, ok)
	}
}

func TestNodeAffinityMapIsBounded(t *testing.T) {
	cfg := nodeAffinityConfig{MaxRequests: 1000, TTL: time.Hour, MaxEntries: 100}
	a, _ := testNodeAffinity(cfg)
	for i := 0; i < 10*cfg.MaxEntries; i++ {
		a.record(sessKey(i), "nodeA")
	}
	a.mu.Lock()
	n := len(a.byKey)
	a.mu.Unlock()
	if n > cfg.MaxEntries {
		t.Fatalf("map must stay bounded by MaxEntries=%d, got %d", cfg.MaxEntries, n)
	}
}

func TestNodeAffinityEmptySessionIsNoOp(t *testing.T) {
	cfg := nodeAffinityConfig{MaxRequests: 5, TTL: time.Hour, MaxEntries: 100}
	a, _ := testNodeAffinity(cfg)
	a.record("", "nodeA")
	if _, ok := a.pick(""); ok {
		t.Fatal("empty session id must never bind")
	}
}

func sessKey(i int) string {
	if i == 0 {
		return "s0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return "s" + string(b[pos:])
}

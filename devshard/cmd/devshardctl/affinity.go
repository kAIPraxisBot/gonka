package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

// affinityKeyFromBody reads the session key from the OpenAI-standard
// prompt_cache_key request field, falling back to user. Empty => no affinity.
func affinityKeyFromBody(body []byte) string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return ""
	}
	for _, name := range []string{"prompt_cache_key", "user"} {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		var s string
		if json.Unmarshal(raw, &s) == nil {
			if s = strings.TrimSpace(s); s != "" {
				return s
			}
		}
	}
	return ""
}

// Session affinity (hop 1): steer a session's primary attempt back to its recent
// participant for KV-cache reuse, bounded so no host is pinned indefinitely.
// Design, consensus and security analysis: proposals/kv-cache-affinity/README.md.

// affinityConfig tunes the bounded stickiness; OFF unless explicitly enabled.
type affinityConfig struct {
	Enabled     bool
	MaxRequests int           // stick for at most this many consecutive requests
	TTL         time.Duration // ...and at most this much wall-clock from first bind
	HoldTimeout time.Duration // how long the primary waits for the sticky host before falling back
	MaxEntries  int           // hard cap on live bindings (the untrusted session key is uncapped in length)
}

func defaultAffinityConfig() affinityConfig {
	return affinityConfig{
		Enabled:     false, // opt-in concept
		MaxRequests: 10,
		TTL:         2 * time.Minute,
		HoldTimeout: 750 * time.Millisecond,
		MaxEntries:  50_000,
	}
}

// affinityConfigFromEnv reads the tunables from the environment, keeping the
// conservative defaults for anything unset.
func affinityConfigFromEnv() affinityConfig {
	c := defaultAffinityConfig()
	if v := os.Getenv("DEVSHARD_AFFINITY_ENABLED"); v == "1" || v == "true" {
		c.Enabled = true
	}
	if n := readInt64Env("DEVSHARD_AFFINITY_MAX_REQUESTS", int64(c.MaxRequests)); n > 0 {
		c.MaxRequests = int(n)
	}
	if ms := readInt64Env("DEVSHARD_AFFINITY_TTL_MS", c.TTL.Milliseconds()); ms > 0 {
		c.TTL = time.Duration(ms) * time.Millisecond
	}
	if ms := readInt64Env("DEVSHARD_AFFINITY_HOLD_MS", c.HoldTimeout.Milliseconds()); ms > 0 {
		c.HoldTimeout = time.Duration(ms) * time.Millisecond
	}
	if n := readInt64Env("DEVSHARD_AFFINITY_MAX_ENTRIES", int64(c.MaxEntries)); n > 0 {
		c.MaxEntries = int(n)
	}
	return c
}

// affinityEntry is one session's sticky binding.
type affinityEntry struct {
	participant string
	count       int
	firstSeen   time.Time
}

// affinityTracker maps a session key to a bounded sticky participant.
type affinityTracker struct {
	cfg   affinityConfig
	mu    sync.Mutex
	byKey map[string]*affinityEntry
	now   func() time.Time // injectable for tests
}

func newAffinityTracker(cfg affinityConfig) *affinityTracker {
	return &affinityTracker{
		cfg:   cfg,
		byKey: make(map[string]*affinityEntry),
		now:   time.Now,
	}
}

func (t *affinityTracker) enabled() bool { return t != nil && t.cfg.Enabled }

// Pick returns the session's sticky participant if the binding is still live and
// still in the group; false => route naturally. isMember prunes a rotated-out host.
func (t *affinityTracker) Pick(key string, isMember func(participant string) bool) (string, bool) {
	if !t.enabled() || key == "" {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.byKey[key]
	if e == nil {
		return "", false
	}
	if t.expiredLocked(e) || (isMember != nil && !isMember(e.participant)) {
		delete(t.byKey, key)
		return "", false
	}
	return e.participant, true
}

// Record binds key->participant (or bumps its counter), evicting at the bound.
func (t *affinityTracker) Record(key, participant string) {
	if !t.enabled() || key == "" || participant == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.byKey[key]
	if e == nil || e.participant != participant || t.expiredLocked(e) {
		if e == nil && len(t.byKey) >= t.cfg.MaxEntries {
			t.sweepLocked()
		}
		t.byKey[key] = &affinityEntry{participant: participant, count: 1, firstSeen: t.now()}
		return
	}
	e.count++
	if e.count >= t.cfg.MaxRequests {
		delete(t.byKey, key)
	}
}

// sweepLocked drops expired entries, then arbitrary live ones down to 90% of
// the cap if still full. Caller holds t.mu; runs only at MaxEntries.
func (t *affinityTracker) sweepLocked() {
	for k, e := range t.byKey {
		if t.expiredLocked(e) {
			delete(t.byKey, k)
		}
	}
	if len(t.byKey) < t.cfg.MaxEntries {
		return
	}
	target := t.cfg.MaxEntries - t.cfg.MaxEntries/10
	for k := range t.byKey {
		if len(t.byKey) <= target {
			break
		}
		delete(t.byKey, k)
	}
}

// expiredLocked reports request-budget or TTL exhaustion. Caller holds t.mu.
func (t *affinityTracker) expiredLocked(e *affinityEntry) bool {
	if e.count >= t.cfg.MaxRequests {
		return true
	}
	return t.now().Sub(e.firstSeen) >= t.cfg.TTL
}

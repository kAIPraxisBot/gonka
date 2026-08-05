package broker

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// nodeSessionAffinity keeps a bounded session -> mlnode binding so a client
// session's follow-up requests reuse the same GPU's warm KV cache. Design and
// rationale: proposals/kv-cache-affinity/README.md.

type nodeAffinityConfig struct {
	MaxRequests int           // stick a session to a node for at most this many acquires
	TTL         time.Duration // ...and at most this much wall-clock from first bind
	MaxEntries  int           // hard cap on tracked sessions (bounded memory)
}

func defaultNodeAffinityConfig() nodeAffinityConfig {
	return nodeAffinityConfig{
		MaxRequests: 64,
		TTL:         10 * time.Minute,
		MaxEntries:  50_000,
	}
}

func nodeAffinityConfigFromEnv() nodeAffinityConfig {
	c := defaultNodeAffinityConfig()
	if v := os.Getenv("DAPI_MLNODE_AFFINITY_MAX_REQUESTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MaxRequests = n
		}
	}
	if v := os.Getenv("DAPI_MLNODE_AFFINITY_TTL_MS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			c.TTL = time.Duration(n) * time.Millisecond
		}
	}
	if v := os.Getenv("DAPI_MLNODE_AFFINITY_MAX_ENTRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MaxEntries = n
		}
	}
	return c
}

type nodeAffinityEntry struct {
	nodeID    string
	count     int
	firstSeen time.Time
}

// nodeSessionAffinity maps a session id to a bounded sticky mlnode. Concurrent-
// safe; its lock is independent of Broker.mu.
type nodeSessionAffinity struct {
	cfg   nodeAffinityConfig
	mu    sync.Mutex
	byKey map[string]*nodeAffinityEntry
	now   func() time.Time // injectable for tests
}

func newNodeSessionAffinity(cfg nodeAffinityConfig) *nodeSessionAffinity {
	return &nodeSessionAffinity{
		cfg:   cfg,
		byKey: make(map[string]*nodeAffinityEntry),
		now:   time.Now,
	}
}

// pick returns the session's sticky node if the binding is still live; false
// means no affinity (caller uses least-busy, then records where it landed).
func (a *nodeSessionAffinity) pick(sessionID string) (string, bool) {
	if a == nil || sessionID == "" {
		return "", false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.byKey[sessionID]
	if e == nil {
		return "", false
	}
	if a.expiredLocked(e) {
		delete(a.byKey, sessionID)
		return "", false
	}
	return e.nodeID, true
}

// record binds a session to its node (or bumps the counter), evicting at the
// request bound so load rebalances.
func (a *nodeSessionAffinity) record(sessionID, nodeID string) {
	if a == nil || sessionID == "" || nodeID == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.byKey[sessionID]
	if e == nil || e.nodeID != nodeID || a.expiredLocked(e) {
		if e == nil && len(a.byKey) >= a.cfg.MaxEntries {
			a.sweepLocked()
		}
		a.byKey[sessionID] = &nodeAffinityEntry{nodeID: nodeID, count: 1, firstSeen: a.now()}
		return
	}
	e.count++
	if e.count >= a.cfg.MaxRequests {
		delete(a.byKey, sessionID)
	}
}

func (a *nodeSessionAffinity) expiredLocked(e *nodeAffinityEntry) bool {
	if e.count >= a.cfg.MaxRequests {
		return true
	}
	return a.now().Sub(e.firstSeen) >= a.cfg.TTL
}

// sweepLocked reclaims expired entries, then -- if still at the cap -- arbitrary
// live ones down to 90%. Runs only when the map is at MaxEntries. Caller holds
// a.mu.
func (a *nodeSessionAffinity) sweepLocked() {
	for k, e := range a.byKey {
		if a.expiredLocked(e) {
			delete(a.byKey, k)
		}
	}
	if len(a.byKey) < a.cfg.MaxEntries {
		return
	}
	target := a.cfg.MaxEntries - a.cfg.MaxEntries/10
	for k := range a.byKey {
		if len(a.byKey) <= target {
			break
		}
		delete(a.byKey, k)
	}
}

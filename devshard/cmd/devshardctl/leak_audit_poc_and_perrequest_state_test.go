package main

import (
	"fmt"
	"testing"
	"time"
)

// TestLeakAudit_poc_and_perrequest_state drives the per-request / per-op state
// surfaces (participant limiter, response cache, hostperf PerfTracker,
// CapacityState escrow membership, PoC-mode preserved participants) many times
// with DISTINCT participants / escrows / nonces / request bodies and measures
// net goroutine growth plus heap growth.
//
// None of these surfaces spawn goroutines, so the goroutine delta is the
// primary signal for "per-request goroutine leak" (expected ~0). The heap
// delta surfaces unbounded map growth. The chatResponseCache is called out
// separately below because it has lazy-only TTL eviction and no size cap.
func TestLeakAudit_poc_and_perrequest_state(t *testing.T) {
	setPoCModeForTest(t, pocRequestModeRelaxed)
	setPoCPhaseState(true, "poc-generation")

	limiter := NewParticipantRequestLimiter(8, 60)
	cache := newChatResponseCache(time.Hour)
	perf := NewPerfTracker(nil)
	cap := NewCapacityState()

	n := 0
	op := func() {
		n++
		participant := fmt.Sprintf("participant-%d", n)
		escrow := fmt.Sprintf("escrow-%d", n)
		model := fmt.Sprintf("model-%d", n%4) // a few models, realistic cardinality

		// participant limiter: read path (untracked participant is allowed and
		// creates no state) + a quarantine that self-expires.
		_ = limiter.AllowRequest(participant, model)

		// hostperf: per-participant ring + context/tool maps.
		perf.Record(RequestSample{HostIdx: n % 4, ParticipantKey: participant, Responsive: true, SendTime: time.Now()})
		perf.RecordContextLimit(participant, uint64(4096+n))
		perf.RecordToolUnsupported(participant)

		// capacity state: per-escrow membership (replaced wholesale, but distinct
		// escrow ids accumulate keys).
		cap.SetEscrowMembership(escrow, map[string]int{participant: 1})

		// PoC preserved-participants: replaced wholesale each call.
		setPoCPreservedParticipantsByModel(map[string][]string{model: {participant}})

		// response cache: DISTINCT key per request body -> accumulates.
		body := []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":"req-%d"}]}`, n))
		key := chatCacheKey(model, body)
		cache.Set(key, cachedChatResponse{
			EscrowID: escrow,
			Body:     append([]byte(nil), body...),
		}, time.Now())
	}

	rep := detectGoroutineLeak(2, 200, op)
	t.Logf("combined per-request/per-op surface:\n%s", renderReport(rep))
	t.Logf("response cache entries after 202 distinct bodies: %d", len(cache.entries))
	t.Logf("perf hosts=%d contextLimits=%d toolUnsupported=%d",
		len(perf.hosts), len(perf.contextLimits), len(perf.toolUnsupported))
	t.Logf("capacity escrowMembership entries: %d", len(cap.escrowMembership))

	if rep.delta > 2 {
		t.Errorf("per-request goroutine leak: Δ%d goroutines after 200 iters\n%s", rep.delta, renderReport(rep))
	}
}

// TestLeakAudit_response_cache_unbounded isolates the chatResponseCache: it has
// no background sweeper and no size cap; eviction is purely lazy on Get of the
// SAME key. Distinct request bodies (the norm) accumulate forever until an
// identical body is re-requested after the TTL. This asserts the growth so the
// finding is reproducible.
func TestLeakAudit_response_cache_unbounded(t *testing.T) {
	cache := newChatResponseCache(time.Hour)
	n := 0
	op := func() {
		n++
		body := []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":"unique-%d"}]}`, n))
		key := chatCacheKey("model-x", body)
		cache.Set(key, cachedChatResponse{EscrowID: "e", Body: append([]byte(nil), body...)}, time.Now())
	}
	rep := detectGoroutineLeak(2, 500, op)
	t.Logf("response cache goroutine report:\n%s", renderReport(rep))
	t.Logf("response cache entries after ~502 distinct bodies: %d (no sweeper, no cap)", len(cache.entries))
	if len(cache.entries) < 500 {
		t.Errorf("expected cache to retain all distinct entries; got %d", len(cache.entries))
	}
}

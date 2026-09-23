package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"devshard/host"
	"devshard/types"
	"devshard/user"
)

var errBrokenVerifier = errors.New("broken escrow: verifier unavailable")

// brokenEscrowHost models a host of an escrow that "should be on the node but
// broke" (per Gleb's report): it acknowledges the request (receipt) but never
// produces output, so every inference stalls into an execution timeout. It
// also acts as a timeout verifier whose VerifyTimeout does NOT observe ctx
// cancellation — an unresponsive verifier host that holds the connection —
// which is the condition under which CollectTimeoutVotes' per-verifier fan-out
// cannot be reaped when the caller returns early.
type brokenEscrowHost struct {
	release <-chan struct{} // closed at test end so any parked goroutines exit
}

func (h brokenEscrowHost) Send(ctx context.Context, req host.HostRequest, stream io.Writer, receiptHandler func()) (*host.HostResponse, error) {
	if receiptHandler != nil {
		receiptHandler() // ack so the race treats the attempt as started, then stall
	}
	<-ctx.Done() // never stream output -> the internal execution-timeout fires
	return nil, ctx.Err()
}

func (h brokenEscrowHost) VerifyTimeout(ctx context.Context, inferenceID uint64, reason types.TimeoutReason, payload *host.InferencePayload, diffs []types.Diff) (bool, []byte, uint32, error) {
	// A verifier for a broken escrow yields no valid vote, but — like the real
	// HTTP transport (client.go passes ctx to the round-trip) — it honors ctx
	// cancellation and is bounded by a transport-level timeout rather than
	// hanging forever. So CollectTimeoutVotes' fan-out drains normally; the
	// leak under test is the redundancy-engine grace hold, not this.
	select {
	case <-ctx.Done():
		return false, nil, 0, ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return false, nil, 0, errBrokenVerifier
	case <-h.release:
		return false, nil, 0, nil
	}
}

// TestBrokenEscrowRequestGoroutineLeak drives repeated requests at a
// broken/stalling escrow and reports goroutine growth per creation site, so we
// can SEE where (and whether) the per-request goroutines pile up — the runtime
// evidence we'd otherwise read off a pprof dump. Logs the full report either
// way; fails if it reproduces a per-request leak.
func TestBrokenEscrowRequestGoroutineLeak(t *testing.T) {
	const numHosts = 8
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	clients := make([]user.HostClient, numHosts)
	for i := range clients {
		clients[i] = brokenEscrowHost{release: release}
	}
	env := setupTestProxyWithClients(t, clients)

	oneRequest := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer cancel()
		var buf bytes.Buffer
		_ = env.proxy.redundancy.RunInference(ctx, defaultParams(), &buf, nil)
	}

	const iters = 6
	rep := detectGoroutineLeak(1, iters, oneRequest)
	t.Logf("broken-escrow request path — %d requests:\n%s", iters, renderReport(rep))

	// Regression gate. BEFORE the waitForPendingLosers no-winner fast-cancel
	// fix this reproduced a per-request pile-up (Δ+58 for 6 requests, ~9-10
	// goroutines/request parked in monitorInflight + the drain waiters
	// watchInflightDone / waitForPendingLosers — held for the full
	// SecondaryWaitAfterWinner grace because the request had no winner). With
	// the fix, a no-winner request cancels its stalled attempts immediately, so
	// the per-request residue must stay well under one-set-per-host.
	if rep.delta >= numHosts {
		t.Errorf("goroutine pile-up NOT contained: Δ%+d after %d requests (~%d/request); see sites above",
			rep.delta, iters, rep.delta/iters)
	}
}

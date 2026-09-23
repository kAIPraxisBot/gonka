package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"devshard/host"
	"devshard/user"
)

// delayedWinnerClient acks the receipt immediately, waits `delay` (honoring
// ctx), then streams one content chunk and finishes with a valid HostResponse.
// The delay lets speculative escalations fan out several attempts BEFORE this
// host crowns itself the winner, so there are pending losers in flight when the
// winner settles. It is the only content-streamer, so it is the only host that
// can be crowned.
type delayedWinnerClient struct{ delay time.Duration }

func (c delayedWinnerClient) Send(ctx context.Context, req host.HostRequest, stream io.Writer, receiptHandler func()) (*host.HostResponse, error) {
	if receiptHandler != nil {
		receiptHandler()
	}
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if stream != nil {
		_, _ = io.WriteString(stream, `data: {"choices":[{"delta":{"content":"x"}}]}`+"\n\n")
	}
	return &host.HostResponse{
		Nonce:       req.Nonce,
		ConfirmedAt: time.Now().Unix(),
	}, nil
}

// stallingLoserClient models a realistic loser: it acks the receipt (so the
// race treats the attempt as started) and then blocks, HONORING ctx — exactly
// like the production HTTP transport (client.go passes ctx to the round-trip).
// It never streams content, so it can never be crowned; it stays "pending" and
// is only released when its per-attempt ctx is cancelled (which, once a winner
// is crowned, happens only at SecondaryWaitAfterWinner grace expiry) or when the
// test-wide release channel closes at cleanup. Because it obeys ctx, any
// goroutine pile-up it produces is NOT a fixture artifact.
type stallingLoserClient struct{ release <-chan struct{} }

func (c stallingLoserClient) Send(ctx context.Context, req host.HostRequest, stream io.Writer, receiptHandler func()) (*host.HostResponse, error) {
	if receiptHandler != nil {
		receiptHandler()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, context.Canceled
	}
}

// TestLeakAudit_winner_stalled_losers drives the "winner crowned + stalled
// losers" surface. One host streams content and finishes (winner); the rest ack
// then block on <-ctx.Done() (losers). When the winner settles, awaitRace hands
// the still-pending losers to a background finishRaceWhenPendingDone, which runs
// waitForPendingLosers with winnerNonce != 0 — the recent no-winner fast-cancel
// fix does NOT cover this branch, so it arms a SecondaryWaitAfterWinner timer
// and spawns a drain goroutine per pending loser, pinning them (plus each
// loser's SendOnly + monitorInflight + watchInflightDone goroutines) for the
// full grace.
//
// Phase 1 measures at the real 5-minute grace: the pinned goroutines outlive the
// harness's 8s settle, so the per-request pile-up is visible as net growth.
// Phase 2 lowers the grace below the settle window: the same goroutines drain
// once the grace expires, proving the hold is caused precisely by
// SecondaryWaitAfterWinner (a real, grace-bounded per-request pile-up under
// load) rather than an unbounded/genuinely-detached leak.
func TestLeakAudit_winner_stalled_losers(t *testing.T) {
	const numHosts = 4

	savedSpec := CurrentMaxSpeculativeAttempts()
	SetMaxSpeculativeAttempts(numHosts) // let escalation launch all hosts so the winner is always reached
	t.Cleanup(func() { SetMaxSpeculativeAttempts(savedSpec) })

	release := make(chan struct{})
	t.Cleanup(func() { close(release) }) // free any grace-pinned loser goroutines at test end

	newEnv := func() *testProxyEnv {
		clients := make([]user.HostClient, numHosts)
		clients[0] = delayedWinnerClient{delay: 300 * time.Millisecond}
		for i := 1; i < numHosts; i++ {
			clients[i] = stallingLoserClient{release: release}
		}
		return setupTestProxyWithClients(t, clients)
	}

	oneRequest := func(env *testProxyEnv) func() {
		return func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var buf bytes.Buffer
			_ = env.proxy.redundancy.RunInference(ctx, defaultParams(), &buf, nil)
		}
	}

	// ---- Phase 1: real 5-minute grace ----
	// Aggressive receipt/first-token timings so speculative escalation fans out
	// stalled losers quickly; grace = 5m (the production default) so pinned
	// loser goroutines outlive the harness's 8s settle.
	setSpeculativeTiming(t, 25*time.Millisecond, 40*time.Millisecond, 5*time.Millisecond, 5*time.Minute)
	envReal := newEnv()
	repReal := detectGoroutineLeak(1, 8, oneRequest(envReal))
	t.Logf("PHASE 1 — winner crowned + stalled losers, grace=5m, 8 requests:\n%s", renderReport(repReal))

	// ---- Phase 2: short grace, same everything else ----
	setSecondaryWaitAfterWinner(t, 400*time.Millisecond)
	envShort := newEnv()
	repShort := detectGoroutineLeak(1, 8, oneRequest(envShort))
	t.Logf("PHASE 2 — same surface, grace=400ms (< 8s settle), 8 requests:\n%s", renderReport(repShort))

	t.Logf("SUMMARY: grace=5m Δ=%+d (~%d/request) vs grace=400ms Δ=%+d — hold is caused by SecondaryWaitAfterWinner",
		repReal.delta, repReal.delta/8, repShort.delta)
}

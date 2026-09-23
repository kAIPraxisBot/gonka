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

// delayThenStallClient emits one content chunk after `delay` (so it can be a
// speculative loser rather than the winner) and then stalls, honoring ctx
// cancellation the whole time. This models a realistic host whose HTTP
// round-trip is bounded by the ctx the production transport passes through:
// when the proxy cancels (metaDrain after client disconnect), Send unwinds.
type delayThenStallClient struct {
	delay time.Duration
}

func (c delayThenStallClient) Send(ctx context.Context, _ host.HostRequest, stream io.Writer, receiptHandler func()) (*host.HostResponse, error) {
	if receiptHandler != nil {
		receiptHandler()
	}
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if stream != nil {
		_, _ = io.WriteString(stream, `data: {"choices":[{"delta":{"content":"loser"}}]}`+"\n\n")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// setMetaDrainTimeout shrinks the post-disconnect drain window so the reaping
// that production caps at 10s is observable inside the detector's ~8s settle.
func setMetaDrainTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := metaDrainTimeout
	metaDrainTimeout = d
	t.Cleanup(func() { metaDrainTimeout = prev })
}

// TestLeakAudit_client_disconnect drives the "client disconnects mid-flight
// while hosts stall" surface. Two hosts race: one emits content and finishes
// (winner), the other emits content late and then stalls (a speculative loser
// held by the finishRaceWhenPendingDone / waitForPendingLosers grace, default
// SecondaryWaitAfterWinner = 5 min). We then trigger the client-disconnect
// cancelFlag. The question: are the attempt / monitor / withMetaDrain / drain
// goroutines reaped within metaDrainTimeout, or do they pile up per request
// (and does the 5-min winner grace pin the stalled loser)?
func TestLeakAudit_client_disconnect(t *testing.T) {
	// Keep the winner grace at the production default (5 min) on purpose: the
	// disconnect path must reap the stalled loser via metaDrain regardless of
	// that grace. Only metaDrain is shortened so reaping is observable.
	setMetaDrainTimeout(t, 250*time.Millisecond)

	// The failed-loser timeout-consensus finalizer (finishRaceOutcome ->
	// HandleTimeout) sleeps until sendTime + RefusalTimeout + TimeoutBuffer on
	// a context.Background() by design (timeout votes must complete even after
	// the client goes away). Shrink TimeoutBuffer so that bounded deadline
	// (~1s + this) drains inside the detector's 8s settle, proving the
	// survivors are self-draining, not an unbounded per-request leak.
	prevBuf := user.TimeoutBuffer
	user.TimeoutBuffer = 200 * time.Millisecond
	t.Cleanup(func() { user.TimeoutBuffer = prevBuf })

	env := setupTestProxyWithClients(t, []user.HostClient{
		&streamContentThenReleaseClient{releaseCh: closedChan()}, // winner: streams content then finishes immediately (closedChan from escrow_checker_test.go)
		delayThenStallClient{delay: 25 * time.Millisecond},       // loser: streams content late, then stalls (honors ctx)
	})

	op := func() {
		// Force immediate parallel secondary so BOTH hosts start (Rule 1:
		// both marked unresponsive => Decide runs the secondary with no delay).
		for i := 0; i < 2; i++ {
			env.proxy.redundancy.perf.Record(RequestSample{HostIdx: i, Responsive: false})
		}

		flag := newCancelFlag()
		var buf bytes.Buffer
		done := make(chan error, 1)
		go func() {
			done <- env.proxy.redundancy.RunInference(context.Background(), defaultParams(), &buf, flag)
		}()

		// Let the winner stream content + finish and the loser start streaming
		// and enter its stall, then simulate the client dropping the socket.
		time.Sleep(60 * time.Millisecond)
		flag.Trigger()

		// RunInference returns as soon as the winner is done (handing the
		// stalled loser to the background finalizer). Wait for that return.
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("RunInference did not return after client disconnect")
		}
		// Give the background finalizer + metaDrain time to cancel and reap the
		// stalled loser (bounded by metaDrainTimeout, not the 5-min grace).
		time.Sleep(3 * metaDrainTimeout)
	}

	rep := detectGoroutineLeak(2, 8, op)
	t.Logf("%s", renderReport(rep))
}

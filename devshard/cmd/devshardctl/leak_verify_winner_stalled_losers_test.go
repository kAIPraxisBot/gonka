package main

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	"devshard/host"
)

// ackThenStallClient acks receipt then blocks on <-ctx.Done() without streaming
// any content (so it never crowns a winner) and honors ctx exactly like the
// production HTTP transport passing ctx into the round-trip.
type ackThenStallClient struct{}

func (ackThenStallClient) Send(ctx context.Context, req host.HostRequest, stream io.Writer, receiptHandler func()) (*host.HostResponse, error) {
	if receiptHandler != nil {
		receiptHandler()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// verifyForceParallel marks all hosts unresponsive so secondaries fire
// immediately (Decide Rule 1: primary_unresponsive), reproducing the
// winner-crowned-with-pending-losers surface.
func verifyForceParallel(env *testProxyEnv) {
	for i := range env.killables {
		env.proxy.redundancy.perf.Record(RequestSample{HostIdx: i, Responsive: false})
	}
}

// verifyWireWinnerAndLosers sets host[0] to stream content then error (crowns a
// winner, hands pending losers to the background finalizer) and every other
// host to stream content then block on <-ctx.Done() (a ctx-honoring stalled
// loser, exactly like the production HTTP transport).
func verifyWireWinnerAndLosers(env *testProxyEnv) {
	env.killables[0].inner = streamContentThenErrClient{}
	for i := 1; i < len(env.killables); i++ {
		env.killables[i].inner = ackThenStallClient{}
	}
}

func verifyDriveN(t *testing.T, useFlag bool, iters int) int {
	env := setupTestProxy(t, 2, nil, true)
	verifyForceParallel(env)
	verifyWireWinnerAndLosers(env)

	settle := func() {
		for i := 0; i < 8; i++ {
			runtime.GC()
			time.Sleep(150 * time.Millisecond)
		}
	}
	settle()
	before := runtime.NumGoroutine()
	for i := 0; i < iters; i++ {
		var buf bytes.Buffer
		var flag *cancelFlag
		if useFlag {
			flag = newCancelFlag()
		}
		_ = env.proxy.redundancy.RunInference(context.Background(), defaultParams(), &buf, flag)
		// Production: once ServeHTTP returns, net/http cancels r.Context(),
		// so watchClientCancel fires flag.Trigger(). Model that here.
		if flag != nil {
			flag.Trigger()
		}
	}
	settle()
	after := runtime.NumGoroutine()
	delta := after - before
	t.Logf("useFlag=%v iters=%d before=%d after=%d delta=%d", useFlag, iters, before, after, delta)
	return delta
}

// TestVerify_NilFlag reproduces the reported finding: nil client flag +
// SecondaryWaitAfterWinner grace pins loser goroutines for the whole grace.
func TestVerify_NilFlag(t *testing.T) {
	zeroReceiptTimeout(t)
	setSecondaryWaitAfterWinner(t, 20*time.Second)
	d6 := verifyDriveN(t, false, 6)
	d12 := verifyDriveN(t, false, 12)
	t.Logf("RESULT nil-flag grace=20s d6=%d d12=%d (expect held & scaling per-op)", d6, d12)
}

// TestVerify_ProdFlag models production: non-nil cancelFlag (proxy.go always
// passes one) triggered after RunInference returns, with the real
// metaDrainTimeout (shortened for test speed). Losers must drain via metaDrain,
// NOT the 30s grace -> delta ~0.
func TestVerify_ProdFlag(t *testing.T) {
	zeroReceiptTimeout(t)
	setSecondaryWaitAfterWinner(t, 30*time.Second) // same grace as nil-flag case
	saved := metaDrainTimeout
	metaDrainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { metaDrainTimeout = saved })
	delta := verifyDriveN(t, true, 8)
	t.Logf("RESULT prod-flag grace=30s metaDrain=200ms delta=%d (expect ~0: losers drain)", delta)
}

// TestVerify_Scale_ProdFlag checks the production path does NOT scale per-op
// (6 vs 12 iters): a real leak would grow, a bounded drain stays flat.
func TestVerify_Scale_ProdFlag(t *testing.T) {
	zeroReceiptTimeout(t)
	setSecondaryWaitAfterWinner(t, 30*time.Second)
	saved := metaDrainTimeout
	metaDrainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { metaDrainTimeout = saved })
	d6 := verifyDriveN(t, true, 6)
	d12 := verifyDriveN(t, true, 12)
	t.Logf("RESULT scale prod-flag d6=%d d12=%d (expect both ~0, no per-op growth)", d6, d12)
}

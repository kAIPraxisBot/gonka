package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRotationCreateLatchIsTTLBasedNotEpochScoped is the focused unit proof of the
// fix: a transient escrow-create failure (e.g. an RPC 503) suppresses re-creation
// only for rotationCreateRetryCooldown, then clears — WITHOUT the epoch changing.
// Before the fix the latch was keyed on the epoch and stayed set until the epoch
// boundary, so a single 503 disabled escrow (re)creation for the whole remaining
// epoch while retirement kept deactivating escrows → "no routable capacity".
func TestRotationCreateLatchIsTTLBasedNotEpochScoped(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	g := &Gateway{rotationClock: func() time.Time { return now }}
	const (
		model = "Qwen/Test"
		role  = rotationRoleTemp
		epoch = uint64(10)
	)

	g.recordRotationCreateFailure(model, role, epoch)
	require.True(t, g.rotationCreateFailed(model, role, epoch), "suppressed immediately after a 503")

	now = now.Add(rotationCreateRetryCooldown - time.Second)
	require.True(t, g.rotationCreateFailed(model, role, epoch), "still suppressed inside the cooldown window")

	// Cooldown elapsed — the epoch is UNCHANGED, yet suppression must lift.
	now = now.Add(2 * time.Second)
	require.False(t, g.rotationCreateFailed(model, role, epoch),
		"latch must expire within the same epoch (the fix), not persist to the epoch boundary")
}

// TestEnsureRotationEscrowsRetriesAfterCooldownSameEpoch drives the real gate
// (ensureRotationEscrows) with a mocked chain tx-client that returns a 503, then
// recovers. It asserts the create is suppressed during the cooldown and retried
// once the cooldown elapses — all within a single epoch.
func TestEnsureRotationEscrowsRetriesAfterCooldownSameEpoch(t *testing.T) {
	store, err := NewGatewayStore(filepath.Join(t.TempDir(), "gateway.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	settings := GatewaySettings{
		ChainREST:               "http://node:1317",
		PublicAPI:               "http://api:9000",
		DefaultModel:            "Qwen/Test",
		DefaultRequestMaxTokens: 1000,
		MaxConcurrentRequests:   2,
		EscrowRotation: EscrowRotationSettings{
			Enabled:           true,
			SettlementEnabled: false, // rotation without settlement — the reported case
			Models: []EscrowRotationModelSettings{{
				ModelID:       "Qwen/Test",
				TempCount:     4,
				TargetCount:   8,
				Amount:        1000,
				PrivateKeyEnv: "DEVSHARD_PRIVATE_KEY",
			}},
		},
	}.WithTuningDefaults()
	require.NoError(t, store.Initialize(settings, nil))

	model := normalizedEscrowRotationModels(settings)[0]
	const (
		role   = rotationRoleTemp
		epoch  = uint64(10)
		target = 4
	)

	now := time.Unix(1_700_000_000, 0)

	oldCreate := gatewayCreateRotationEscrow
	t.Cleanup(func() { gatewayCreateRotationEscrow = oldCreate })
	createAttempts := 0
	failCreates := true
	gatewayCreateRotationEscrow = func(*Gateway, context.Context, GatewaySettings, EscrowRotationModelSettings, string, uint64) (*CreateDevshardEscrowResult, error) {
		createAttempts++
		if failCreates {
			return nil, fmt.Errorf("503 Service Unavailable")
		}
		return &CreateDevshardEscrowResult{EscrowID: uint64(createAttempts)}, nil
	}

	g := &Gateway{store: store, rotationFailures: make(map[string]time.Time), rotationClock: func() time.Time { return now }}

	// Tick 1: the RPC returns 503 -> create fails, latch is set.
	_, err = g.ensureRotationEscrows(context.Background(), settings, model, role, epoch, target)
	require.Error(t, err)
	require.Equal(t, 1, createAttempts, "first tick attempts create once, then latches on the 503")

	// Tick 2, inside the cooldown: suppressed, no chain call made.
	now = now.Add(rotationCreateRetryCooldown / 2)
	_, err = g.ensureRotationEscrows(context.Background(), settings, model, role, epoch, target)
	require.ErrorIs(t, err, errEscrowRotationCreateSuppressed)
	require.Equal(t, 1, createAttempts, "inside cooldown the create is suppressed, no retry")

	// Tick 3, after the cooldown, RPC recovered — same epoch. Rotation must retry.
	failCreates = false
	now = now.Add(rotationCreateRetryCooldown + time.Second)
	res, err := g.ensureRotationEscrows(context.Background(), settings, model, role, epoch, target)
	require.NoError(t, err)
	require.Greater(t, createAttempts, 1, "after the cooldown, rotation retries within the same epoch (the fix)")
	require.Equal(t, target, res.CreatedCount, "recovery fills the target escrow count")
}

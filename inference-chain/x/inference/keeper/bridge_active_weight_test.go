package keeper

import (
	"testing"

	"github.com/cosmos/cosmos-sdk/x/group"
	"github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
)

func TestActiveEpochWeight(t *testing.T) {
	members := []*group.GroupMember{
		{Member: &group.Member{Address: "a", Weight: "10"}},
		{Member: &group.Member{Address: "b", Weight: "10"}},
		{Member: &group.Member{Address: "c", Weight: "20"}},
	}

	// a and b submitted seeds this epoch; c is in the group but inactive (empty signature).
	seeded := []*types.SeedSignature{
		{MemberAddress: "a", Signature: "sig"},
		{MemberAddress: "b", Signature: "sig"},
		{MemberAddress: "c", Signature: ""},
	}

	// Active weight is a+b = 20, not the full 40 — c's stranded 20 is excluded from the
	// denominator. This is the liveness-cliff fix: a+b (power 20) reach majority of 20
	// (needs 11) where under full-TotalWeight (needs 21) they would stall forever.
	require.Equal(t, int64(20), activeEpochWeight(members, seeded, 40))

	// No seeds recorded -> fall back to full total, so quorum is never broken.
	require.Equal(t, int64(40), activeEpochWeight(members, nil, 40))

	// All members active -> full sum.
	allSeeded := []*types.SeedSignature{
		{MemberAddress: "a", Signature: "s"},
		{MemberAddress: "b", Signature: "s"},
		{MemberAddress: "c", Signature: "s"},
	}
	require.Equal(t, int64(40), activeEpochWeight(members, allSeeded, 40))

	// A nil member entry is skipped safely (no panic); with no seeds -> fallback.
	require.Equal(t, int64(40), activeEpochWeight(append([]*group.GroupMember{{Member: nil}}, members...), nil, 40))
}

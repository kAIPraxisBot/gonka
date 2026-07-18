package apiconfig

import "testing"

func TestSanitizeConfig_StripsSecretsWithoutMutatingOriginal(t *testing.T) {
	original := Config{
		MLNodeKeyConfig: MLNodeKeyConfig{WorkerPublicKey: "pub", WorkerPrivateKey: "SECRET-priv"},
		CurrentSeed:     SeedInfo{Seed: 111},
		PreviousSeed:    SeedInfo{Seed: 222},
		UpcomingSeed:    SeedInfo{Seed: 333},
		CurrentHeight:   42,
	}

	sanitized := sanitizeConfig(original)

	if sanitized.MLNodeKeyConfig.WorkerPrivateKey != "" {
		t.Errorf("WorkerPrivateKey not stripped: %q", sanitized.MLNodeKeyConfig.WorkerPrivateKey)
	}
	if sanitized.CurrentSeed.Seed != 0 || sanitized.PreviousSeed.Seed != 0 || sanitized.UpcomingSeed.Seed != 0 {
		t.Errorf("seeds not stripped: %d/%d/%d",
			sanitized.CurrentSeed.Seed, sanitized.PreviousSeed.Seed, sanitized.UpcomingSeed.Seed)
	}
	// Non-secret fields are preserved.
	if sanitized.MLNodeKeyConfig.WorkerPublicKey != "pub" || sanitized.CurrentHeight != 42 {
		t.Errorf("non-secret fields altered: pubkey=%q height=%d",
			sanitized.MLNodeKeyConfig.WorkerPublicKey, sanitized.CurrentHeight)
	}
	// The original must be untouched (value-copy semantics — the leak fix relies
	// on this so sanitizing for the admin endpoint never wipes the live config).
	if original.MLNodeKeyConfig.WorkerPrivateKey != "SECRET-priv" || original.CurrentSeed.Seed != 111 {
		t.Error("sanitizeConfig mutated the original config")
	}
}

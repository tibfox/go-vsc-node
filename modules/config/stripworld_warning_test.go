package config

import (
	"errors"
	"testing"
)

// GV-H7 follow-up (fix/stripworld-warning): a stripWorld failure — e.g. EPERM
// when the node does not own a world-readable config file like a root-owned
// p2pConfig.json on a bind-mounted deployment — must NOT crash the node.
// Init() and Update() warn and continue instead of returning the error
// (pre-fix they returned it, which crash-looped the node at startup).
func TestStripWorldFailureIsNonFatal(t *testing.T) {
	orig := stripWorld
	t.Cleanup(func() { stripWorld = orig })
	stripWorld = func(string) error { return errors.New("simulated chmod EPERM") }

	type secret struct{ Seed string }
	dir := t.TempDir()

	// Update() writes a fresh config; the strip migration fails but the write
	// succeeded, so Update must return nil.
	c := New(secret{Seed: "default"}, &dir)
	if err := c.Update(func(s *secret) { s.Seed = "written" }); err != nil {
		t.Fatalf("Update returned error on stripWorld failure, want nil: %v", err)
	}

	// Init() reads the existing config; the strip migration fails but the
	// node must still boot, so Init must return nil and load the value.
	c2 := New(secret{}, &dir)
	if err := c2.Init(); err != nil {
		t.Fatalf("Init returned error on stripWorld failure, want nil: %v", err)
	}
	if got := c2.Get().Seed; got != "written" {
		t.Fatalf("Init loaded Seed=%q, want %q", got, "written")
	}

	// Sanity: with the real stripWorld restored, the happy path still works
	// (no regression to the GV-H7 strip itself).
	stripWorld = orig
	if err := New(secret{}, &dir).Init(); err != nil {
		t.Fatalf("Init with real stripWorld returned error: %v", err)
	}
}

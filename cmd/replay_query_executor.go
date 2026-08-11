package cmd

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	"github.com/dynatrace-oss/dtctl/pkg/config"
	pkgexec "github.com/dynatrace-oss/dtctl/pkg/exec"
	"github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// newReplayQueryExecutorFromConfig is deliberately limited to the Phase 4
// query, wait query, and query --live call sites. Phase 5 replaces this with a
// context-aware factory after the remaining direct-constructor audit.
func newReplayQueryExecutorFromConfig(cfg *config.Config, c *client.Client) (*pkgexec.DQLExecutor, error) {
	executor := NewDQLExecutorFromConfig(cfg, c)
	activation, err := replayActivationForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if !activation.Active {
		return executor, nil
	}

	ctx, err := cfg.CurrentContextObj()
	if err != nil {
		return nil, err
	}
	source, err := cfg.SourceIdentity()
	if err != nil {
		return nil, err
	}
	locator := session.ReplayLocator{
		ContextKey:          session.NewReplayContextKey(source, cfg.CurrentContext, ctx.Environment),
		ContextIdentityHash: session.ReplayContextIdentityHash(source, cfg.CurrentContext),
	}
	store := session.NewReplayStateStore(replayStateDirectory, replayClock)
	fallbackDisclosure := session.ReplayDisclosureFull
	fallbackProvenancePath := ""
	if state, stateErr := store.Status(locator); stateErr == nil {
		// A usable state snapshot owns disclosure routing even when the current
		// context has drifted or its provenance path has since become unusable.
		// The preparer performs sink preflight and emits the disclosure-safe
		// readiness result.
		fallbackDisclosure = state.Disclosure
		fallbackProvenancePath = state.ProvenancePath
	} else if ctx.Replay != nil {
		// With no usable state, derive only the disclosure route. Full replay
		// configuration validation is preparation work and, in restricted mode,
		// must not get ahead of the required sink preflight.
		fallbackDisclosure, fallbackProvenancePath, err = replayConfiguredRoute(ctx.Replay, locator)
		if err != nil {
			return nil, err
		}
	}
	preparer, err := pkgexec.NewReplayQueryPreparer(pkgexec.ReplayPreparerConfig{
		Store:                    store,
		Clock:                    replayClock,
		Locator:                  locator,
		ContextName:              cfg.CurrentContext,
		ExpectedContextInputHash: session.ReplayContextInputHash(ctx),
		ExpectedEnvironmentHash:  session.ReplayEnvironmentHash(ctx.Environment),
		EnvironmentID:            session.ReplayEnvironmentHash(ctx.Environment),
		ClientIdentity:           replayClientIdentity(c, ctx.TokenRef),
		FallbackDisclosure:       fallbackDisclosure,
		FallbackProvenancePath:   fallbackProvenancePath,
		SourcePolicy:             replay.Milestone1SourcePolicy(),
		SinkFactory: func(path string) session.ProvenanceSink {
			return session.NewFileProvenanceSink(path, replayStateDirectory)
		},
		WaitFunc: replayQueryWaitFunc,
	})
	if err != nil {
		return nil, err
	}
	return executor.WithQueryPreparer(preparer), nil
}

func replayClientIdentity(c *client.Client, tokenRef string) string {
	identity := "token-ref:" + tokenRef
	if c == nil {
		return identity
	}
	if subject, err := client.ExtractUserIDFromToken(c.Token()); err == nil && subject != "" {
		digest := sha256.Sum256([]byte(subject))
		return "principal-sha256:" + hex.EncodeToString(digest[:])
	}
	return identity
}

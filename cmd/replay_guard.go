package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// ReplayGuardError is the hard, context-derived command-boundary rejection.
// It is intentionally distinct from ProfileError: profiles shape discovery;
// this guard remains authoritative even when DTCTL_PROFILE widens the tree.
type ReplayGuardError struct {
	Command     string
	ContextName string
	Plugin      bool
	Restricted  bool
}

func (e *ReplayGuardError) Error() string {
	if e.Restricted {
		return "this command is not available in this context"
	}
	target := fmt.Sprintf("command %q", e.Command)
	if e.Plugin {
		target = fmt.Sprintf("plugin command %q", e.Command)
	}
	base := fmt.Sprintf("%s is not available while context %q is configured for replay", target, e.ContextName)
	if strings.HasPrefix(e.Command, "ctx ") {
		return base + "; leave with 'dtctl ctx <name>', select one invocation with '--context <name>', or set DTCTL_CONTEXT=<name>"
	}
	return base
}

func (e *ReplayGuardError) Suggestions() []string {
	return []string{
		"switch to an existing context with: dtctl ctx <name>",
		"select a context for one invocation with: dtctl --context <name> ...",
		"or set DTCTL_CONTEXT=<name> for the invocation",
	}
}

type replayGuardRecordingError struct{ detail error }

func (e *replayGuardRecordingError) Error() string {
	return "Required local recording is unavailable. The query was not executed."
}

func (e *replayGuardRecordingError) Unwrap() error { return e.detail }

type replayActivation struct {
	Active         bool
	ContextName    string
	Disclosure     string
	ProvenancePath string
	SessionID      string
}

func installReplayGuard(root *cobra.Command) {
	walkCommands(root, func(cmd *cobra.Command) {
		if cmd == root || (cmd.RunE == nil && cmd.Run == nil) {
			return
		}
		originalArgs := cmd.Args
		cmd.Args = func(c *cobra.Command, args []string) error {
			if err := guardReplayResolvedCommand(c, root); err != nil {
				return err
			}
			if originalArgs != nil {
				return originalArgs(c, args)
			}
			return nil
		}
		originalFlagError := cmd.FlagErrorFunc()
		cmd.SetFlagErrorFunc(func(c *cobra.Command, flagErr error) error {
			if err := guardReplayResolvedCommand(c, root); err != nil {
				return err
			}
			if originalFlagError != nil {
				return originalFlagError(c, flagErr)
			}
			return flagErr
		})
		originalRunE := cmd.RunE
		originalRun := cmd.Run
		cmd.RunE = func(c *cobra.Command, args []string) error {
			if err := guardReplayResolvedCommand(c, root); err != nil {
				return err
			}
			if originalRunE != nil {
				return originalRunE(c, args)
			}
			originalRun(c, args)
			return nil
		}
		cmd.Run = nil
	})
}

// guardReplayCommand derives the path from Cobra's resolved command object,
// never from raw argv. It runs before the selected handler, scope preflight,
// client construction, config mutation, or credential resolution.
func guardReplayResolvedCommand(cmd, root *cobra.Command) error {
	cfg, err := LoadConfig()
	if err != nil {
		if replayConfigErrorMustBlock() {
			return err
		}
		// No config exists, so there is no selected replay context. Commands that
		// do not require configuration (for example version) keep working.
		return nil
	}
	activation, err := replayActivationForConfig(cfg)
	if err != nil {
		return err
	}
	if !activation.Active {
		return nil
	}

	path := commandPathRelative(cmd, root)
	if !replayHardGuardAllows(path) {
		return routeReplayGuardFailure(activation, &ReplayGuardError{Command: path, ContextName: activation.ContextName})
	}
	return nil
}

func replayActivationForConfig(cfg *config.Config) (replayActivation, error) {
	ctx, err := cfg.CurrentContextObj()
	if err != nil {
		return replayActivation{}, nil
	}
	source, err := cfg.SourceIdentity()
	if err != nil {
		return replayActivation{}, err
	}
	locator := session.ReplayLocator{
		ContextKey:          session.NewReplayContextKey(source, cfg.CurrentContext, ctx.Environment),
		ContextIdentityHash: session.ReplayContextIdentityHash(source, cfg.CurrentContext),
	}
	configured := ctx.Replay != nil
	state, stateErr := session.NewReplayStateStore(replayStateDirectory, replayClock).Status(locator)
	switch {
	case stateErr == nil:
		return replayActivation{
			Active: configured || session.ReplayGuardActive(state), ContextName: cfg.CurrentContext,
			Disclosure: state.Disclosure, ProvenancePath: state.ProvenancePath, SessionID: state.SessionID,
		}, nil
	case errors.Is(stateErr, session.ErrReplaySessionNotFound):
		disclosure, path, routeErr := replayConfiguredRoute(ctx.Replay, locator)
		if routeErr != nil {
			return replayActivation{}, routeErr
		}
		return replayActivation{Active: configured, ContextName: cfg.CurrentContext, Disclosure: disclosure, ProvenancePath: path}, nil
	default:
		// A corrupt, unsafe, or unreadable state cannot be assumed inactive. Keep
		// lifecycle recovery commands available, but activate the hard boundary.
		disclosure, path, routeErr := replayConfiguredRoute(ctx.Replay, locator)
		if routeErr != nil {
			return replayActivation{}, routeErr
		}
		return replayActivation{Active: true, ContextName: cfg.CurrentContext, Disclosure: disclosure, ProvenancePath: path}, nil
	}
}

func replayConfiguredRoute(raw *config.ReplayConfig, locator session.ReplayLocator) (string, string, error) {
	disclosure := session.ReplayDisclosureFull
	if raw != nil && raw.Disclosure != "" {
		disclosure = raw.Disclosure
	}
	if disclosure != session.ReplayDisclosureFull && disclosure != session.ReplayDisclosureRestricted {
		return "", "", fmt.Errorf("unknown replay disclosure %q: use %q or %q", disclosure, session.ReplayDisclosureFull, session.ReplayDisclosureRestricted)
	}
	if disclosure != session.ReplayDisclosureRestricted {
		return disclosure, "", nil
	}
	path := ""
	if raw != nil {
		path = raw.ProvenancePath
	}
	if path == "" {
		path = filepath.Join(replayStateDirectory, string(locator.ContextKey)+".provenance.jsonl")
	}
	return disclosure, path, nil
}

func routeReplayGuardFailure(activation replayActivation, detail error) error {
	if activation.Disclosure != session.ReplayDisclosureRestricted {
		return detail
	}
	if activation.ProvenancePath == "" {
		return &replayGuardRecordingError{detail: fmt.Errorf("restricted command guard has no provenance path")}
	}
	sink := session.NewFileProvenanceSink(activation.ProvenancePath, replayStateDirectory)
	ctx := context.Background()
	if err := sink.Preflight(ctx); err != nil {
		return &replayGuardRecordingError{detail: err}
	}
	record := session.ReplayProvenanceRecord{
		SchemaVersion: session.ReplayProvenanceSchemaVersion,
		RecordedAt:    replayClock.Now().UTC(),
		Event:         "command_guard",
		SessionID:     activation.SessionID,
		Fields: map[string]any{
			"outcome": "blocked",
			"detail":  detail.Error(),
		},
	}
	switch value := detail.(type) {
	case *ReplayGuardError:
		record.Fields["command"] = value.Command
		value.Restricted = true
	}
	if err := sink.Append(ctx, record); err != nil {
		return &replayGuardRecordingError{detail: err}
	}
	return detail
}

// replayHardGuardAllows is exact for runnable leaves. Profile.Allows keeps its
// existing ancestor/prefix behavior for tree shaping, while the enforcement
// boundary does not automatically bless a future child command.
func replayHardGuardAllows(path string) bool {
	return config.ReplayProfileAllows(path)
}

func replayPluginDispatchGuard(leadingArgs, commandWords []string) error {
	cfg, err := loadConfigForReplayArgs(leadingArgs)
	if err != nil {
		if replayConfigErrorMustBlockForArgs(leadingArgs) {
			return err
		}
		return nil
	}
	activation, err := replayActivationForConfig(cfg)
	if err != nil || !activation.Active {
		return err
	}
	return routeReplayGuardFailure(activation, &ReplayGuardError{
		Command:     strings.Join(commandWords, " "),
		ContextName: activation.ContextName,
		Plugin:      true,
	})
}

func replayConfigErrorMustBlock() bool {
	args := []string(nil)
	if cfgFile != "" {
		args = []string{"--config", cfgFile}
	}
	return replayConfigErrorMustBlockForArgs(args)
}

func replayConfigErrorMustBlockForArgs(args []string) bool {
	if extractFlagValue(args, "config") != "" || os.Getenv(config.EnvConfig) != "" || config.FindLocalConfig() != "" {
		return true
	}
	_, err := os.Stat(config.DefaultConfigPath())
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

func replayShellAliasGuard(cfg *config.Config, args []string) error {
	if override := extractContextOverride(args); override != "" {
		cfg.CurrentContext = override
	} else if override := os.Getenv("DTCTL_CONTEXT"); override != "" {
		cfg.CurrentContext = override
	}
	activation, err := replayActivationForConfig(cfg)
	if err != nil || !activation.Active {
		return err
	}
	return routeReplayGuardFailure(activation, &ReplayGuardError{
		Command:     "shell alias",
		ContextName: activation.ContextName,
	})
}

func loadConfigForReplayArgs(args []string) (*config.Config, error) {
	var (
		cfg *config.Config
		err error
	)
	if path := extractFlagValue(args, "config"); path != "" {
		cfg, err = config.LoadFrom(path)
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		return nil, err
	}
	override := extractContextOverride(args)
	if override == "" {
		override = os.Getenv("DTCTL_CONTEXT")
	}
	if override != "" {
		cfg.CurrentContext = override
	}
	return cfg, nil
}

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

var (
	replayClock          session.Clock = session.SystemClock{}
	replayStateDirectory               = session.ReplayDir()
	replayQueryWaitFunc  func(context.Context, time.Duration) error
)

type replayInvocationContext struct {
	Config  *config.Config
	Context *config.Context
	Name    string
	Source  string
	Locator session.ReplayLocator
}

// ReplayStatusOutput is the stable management view printed by replay lifecycle
// commands. It contains clock and state metadata only, never credentials or
// tenant data.
type ReplayStatusOutput struct {
	ContextName          string   `json:"context_name" yaml:"context_name"`
	Status               string   `json:"status" yaml:"status"`
	SessionID            string   `json:"session_id,omitempty" yaml:"session_id,omitempty"`
	SessionStartedAt     string   `json:"session_started_at,omitempty" yaml:"session_started_at,omitempty"`
	HostNow              string   `json:"host_now" yaml:"host_now"`
	ClockMode            string   `json:"clock_mode,omitempty" yaml:"clock_mode,omitempty"`
	AnchorHost           string   `json:"anchor_host,omitempty" yaml:"anchor_host,omitempty"`
	AnchorVirtual        string   `json:"anchor_virtual,omitempty" yaml:"anchor_virtual,omitempty"`
	VirtualStart         string   `json:"virtual_start,omitempty" yaml:"virtual_start,omitempty"`
	VirtualNow           string   `json:"virtual_now,omitempty" yaml:"virtual_now,omitempty"`
	DataStart            string   `json:"data_start,omitempty" yaml:"data_start,omitempty"`
	DataEnd              string   `json:"data_end,omitempty" yaml:"data_end,omitempty"`
	VisibleEnd           string   `json:"visible_end,omitempty" yaml:"visible_end,omitempty"`
	Position             string   `json:"position,omitempty" yaml:"position,omitempty"`
	ConfigurationDrift   bool     `json:"configuration_drift" yaml:"configuration_drift"`
	StateKey             string   `json:"state_key" yaml:"state_key"`
	Disclosure           string   `json:"disclosure,omitempty" yaml:"disclosure,omitempty"`
	ProvenancePath       string   `json:"provenance_path,omitempty" yaml:"provenance_path,omitempty"`
	CompletedAt          string   `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
	StoppedAt            string   `json:"stopped_at,omitempty" yaml:"stopped_at,omitempty"`
	FinalVirtualNow      string   `json:"final_virtual_now,omitempty" yaml:"final_virtual_now,omitempty"`
	UnreadableStateFiles []string `json:"unreadable_state_files,omitempty" yaml:"unreadable_state_files,omitempty"`
}

func newReplayCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Manage a local historical replay session",
		Long: `Manage the local clock and state snapshot for a historical replay context.

Replay lifecycle operations read configuration and mutate only private local
state. They never contact the selected Dynatrace environment and remain usable
when the context safety level is readonly.

A replay block stored in the context keeps real-time DQL guarded before start
and after stop. A session started only from flags loses that configured signal
after stop; automation should store the complete replay block in the context.

Leave a replay context with 'dtctl ctx <name>', select another context for one
invocation with '--context <name>', or set DTCTL_CONTEXT=<name>. Switching away
does not stop the local replay session.`,
		Example: `  # Start from replay values stored in the selected context
  dtctl replay start --context historical-window

  # Move a manual clock and inspect the resulting snapshot
  dtctl replay advance 10m --context historical-window
  dtctl replay status --context historical-window -o json

  # End the local session without changing tenant data
  dtctl replay stop --context historical-window

  # Leave the replay context without stopping its session
  dtctl ctx production
  dtctl --context production query 'fetch logs'
  DTCTL_CONTEXT=production dtctl query 'fetch logs'`,
	}
	cmd.AddCommand(
		newReplayStartCommand(),
		newReplayAdvanceCommand(),
		newReplayStatusCommand(),
		newReplayStopCommand(),
	)
	return cmd
}

func newReplayStartCommand() *cobra.Command {
	var (
		dataStart    string
		dataEnd      string
		virtualStart string
		clockMode    string
		restart      bool
	)
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a local historical replay session",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateReplayOutputOptions(); err != nil {
				return err
			}
			inv, err := loadReplayInvocationContext()
			if err != nil {
				return err
			}
			if err := requireReplayStartPolicy(inv); err != nil {
				return err
			}

			overrides := session.ReplayConfigOverrides{}
			if cmd.Flags().Changed("data-start") {
				overrides.DataStart = &dataStart
			}
			if cmd.Flags().Changed("data-end") {
				overrides.DataEnd = &dataEnd
			}
			if cmd.Flags().Changed("virtual-start") {
				overrides.VirtualStart = &virtualStart
			}
			if cmd.Flags().Changed("clock-mode") {
				overrides.ClockMode = &clockMode
			}

			resolved, err := session.ResolveReplayConfig(inv.Context.Replay, overrides, replayStateDirectory, inv.Locator.ContextKey)
			if err != nil {
				return err
			}
			if resolved.Disclosure == session.ReplayDisclosureRestricted {
				sink := session.NewFileProvenanceSink(resolved.ProvenancePath, replayStateDirectory)
				if err := sink.Preflight(commandContext(cmd)); err != nil {
					return fmt.Errorf("replay provenance preflight failed: %w", err)
				}
			}

			store := session.NewReplayStateStore(replayStateDirectory, replayClock)
			state, err := store.Start(session.ReplayStartRequest{
				Locator:          inv.Locator,
				ContextName:      inv.Name,
				EnvironmentHash:  session.ReplayEnvironmentHash(inv.Context.Environment),
				ContextInputHash: session.ReplayContextInputHash(inv.Context),
				Config:           resolved,
				Restart:          restart,
			})
			if err != nil {
				return err
			}

			if err := printReplayStatus(cmd, replayStatusFromSession(state, state.SessionStartedAt, false)); err != nil {
				return err
			}
			if resolved.VirtualStart.Equal(resolved.DataStart) {
				if resolved.ClockMode == session.ReplayClockRealtime {
					output.FprintWarning(cmd.ErrOrStderr(), "the visible replay interval starts empty. Realtime mode reveals stored telemetry as the clock advances")
				} else {
					output.FprintWarning(cmd.ErrOrStderr(), "the visible replay interval starts empty. Manual mode requires 'dtctl replay advance'")
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dataStart, "data-start", "", "inclusive start of the stored replay interval (RFC 3339)")
	cmd.Flags().StringVar(&dataEnd, "data-end", "", "terminal boundary of the stored replay interval (RFC 3339)")
	cmd.Flags().StringVar(&virtualStart, "virtual-start", "", "initial virtual time (RFC 3339; defaults to data-start)")
	cmd.Flags().StringVar(&clockMode, "clock-mode", "", "virtual clock mode: realtime or manual")
	cmd.Flags().BoolVar(&restart, "restart", false, "replace any existing replay session for this context")
	return cmd
}

func newReplayAdvanceCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "advance <duration>",
		Short: "Advance the local replay clock by a fixed duration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			by, err := time.ParseDuration(args[0])
			if err != nil {
				return fmt.Errorf("invalid replay advance duration %q: use a positive fixed duration such as 30s, 10m, 2h, or 24h", args[0])
			}
			if by <= 0 {
				return fmt.Errorf("replay advance duration must be positive")
			}
			if err := validateReplayOutputOptions(); err != nil {
				return err
			}
			inv, err := loadReplayInvocationContext()
			if err != nil {
				return err
			}
			store := session.NewReplayStateStore(replayStateDirectory, replayClock)
			state, err := store.Advance(inv.Locator, by, session.ReplayContextInputHash(inv.Context))
			if errors.Is(err, session.ErrReplaySessionNotFound) {
				return noReplaySessionError(inv.Name)
			}
			if err != nil {
				return err
			}
			drifted := state.ContextInputHash != session.ReplayContextInputHash(inv.Context)
			return printReplayStatus(cmd, replayStatusFromSession(state, replayClock.Now().UTC(), drifted))
		},
	}
}

func newReplayStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the local replay session without a network call",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateReplayOutputOptions(); err != nil {
				return err
			}
			inv, err := loadReplayInvocationContext()
			if err != nil {
				return err
			}
			store := session.NewReplayStateStore(replayStateDirectory, replayClock)
			state, diagnostics, err := store.StatusWithDiagnostics(inv.Locator)
			if errors.Is(err, session.ErrReplaySessionNotFound) {
				now := replayClock.Now().UTC()
				return printReplayStatus(cmd, ReplayStatusOutput{
					ContextName:          inv.Name,
					Status:               session.ReplayStatusInactive,
					HostNow:              replayTimeString(now),
					ConfigurationDrift:   false,
					StateKey:             store.StateKey(inv.Locator.ContextKey),
					UnreadableStateFiles: diagnostics.UnreadableStateFiles,
				})
			}
			if err != nil {
				return err
			}
			// Observe after the store snapshot. If realtime crosses data_end
			// between the two reads, this later timestamp can only advance the
			// view to terminal-ready; it cannot produce terminal state paired
			// with an earlier, non-terminal virtual timestamp.
			now := replayClock.Now().UTC()
			drifted := state.ContextInputHash != session.ReplayContextInputHash(inv.Context)
			view := replayStatusFromSession(state, now, drifted)
			view.UnreadableStateFiles = diagnostics.UnreadableStateFiles
			return printReplayStatus(cmd, view)
		},
	}
}

func newReplayStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the local replay session",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateReplayOutputOptions(); err != nil {
				return err
			}
			inv, err := loadReplayInvocationContext()
			if err != nil {
				return err
			}
			store := session.NewReplayStateStore(replayStateDirectory, replayClock)
			state, err := store.Stop(inv.Locator)
			if errors.Is(err, session.ErrReplaySessionNotFound) {
				return noReplaySessionError(inv.Name)
			}
			if err != nil {
				return err
			}
			drifted := state.ContextInputHash != session.ReplayContextInputHash(inv.Context)
			return printReplayStatus(cmd, replayStatusFromSession(state, replayClock.Now().UTC(), drifted))
		},
	}
}

func loadReplayInvocationContext() (replayInvocationContext, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return replayInvocationContext{}, err
	}
	ctx, err := cfg.CurrentContextObj()
	if err != nil {
		return replayInvocationContext{}, err
	}
	source, err := cfg.SourceIdentity()
	if err != nil {
		return replayInvocationContext{}, err
	}
	name := cfg.CurrentContext
	return replayInvocationContext{
		Config:  cfg,
		Context: ctx,
		Name:    name,
		Source:  source,
		Locator: session.ReplayLocator{
			ContextKey:          session.NewReplayContextKey(source, name, ctx.Environment),
			ContextIdentityHash: session.ReplayContextIdentityHash(source, name),
		},
	}, nil
}

func requireReplayStartPolicy(inv replayInvocationContext) error {
	if inv.Context.GetEffectiveSafetyLevel() != config.SafetyLevelReadOnly {
		return fmt.Errorf("replay requires safety-level readonly for context %q because mutating commands still target the live tenant", inv.Name)
	}
	if strings.TrimSpace(inv.Context.Profile) != config.ProfileReplay {
		return fmt.Errorf("replay requires context %q to bind the reserved built-in profile %q", inv.Name, config.ProfileReplay)
	}
	profile, err := inv.Config.ResolveProfile()
	if err != nil {
		return err
	}
	if profile == nil || profile.Name != config.ProfileReplay {
		return fmt.Errorf("replay profile resolution was overridden for this invocation; unset %s or set it to %q", config.ProfileEnvVar, config.ProfileReplay)
	}
	return nil
}

func noReplaySessionError(contextName string) error {
	return fmt.Errorf("context %q has no replay session; run 'dtctl replay start --context %s'", contextName, contextName)
}

func replayStatusFromSession(state session.ReplaySession, hostNow time.Time, drifted bool) ReplayStatusOutput {
	virtualNow := session.VirtualNow(state, hostNow)
	visibleEnd := session.VisibleEnd(state, hostNow)
	status := state.Status
	if status == session.ReplayStatusActive && virtualNow.Equal(state.DataEnd) {
		status = session.ReplayStatusTerminalReady
	}
	if drifted && (status == session.ReplayStatusActive || status == session.ReplayStatusTerminalReady) {
		status = session.ReplayStatusDrifted
	}
	position := "inside-replay-interval"
	if virtualNow.Before(state.DataStart) {
		position = "before-replay-interval"
	} else if virtualNow.Equal(state.DataEnd) {
		position = "terminal-boundary"
	}
	view := ReplayStatusOutput{
		ContextName:        state.ContextName,
		Status:             status,
		SessionID:          state.SessionID,
		SessionStartedAt:   replayTimeString(state.SessionStartedAt),
		HostNow:            replayTimeString(hostNow),
		ClockMode:          state.ClockMode,
		AnchorHost:         replayTimeString(state.AnchorHost),
		AnchorVirtual:      replayTimeString(state.AnchorVirtual),
		VirtualStart:       replayTimeString(state.VirtualStart),
		VirtualNow:         replayTimeString(virtualNow),
		DataStart:          replayTimeString(state.DataStart),
		DataEnd:            replayTimeString(state.DataEnd),
		VisibleEnd:         replayTimeString(visibleEnd),
		Position:           position,
		ConfigurationDrift: drifted,
		StateKey:           string(state.ContextKey),
	}
	if state.Disclosure == session.ReplayDisclosureRestricted {
		view.Disclosure = state.Disclosure
		view.ProvenancePath = state.ProvenancePath
	}
	if state.CompletedAt != nil {
		view.CompletedAt = replayTimeString(*state.CompletedAt)
	}
	if state.StoppedAt != nil {
		view.StoppedAt = replayTimeString(*state.StoppedAt)
	}
	if state.FinalVirtualNow != nil {
		view.FinalVirtualNow = replayTimeString(*state.FinalVirtualNow)
	}
	return view
}

func printReplayStatus(cmd *cobra.Command, status ReplayStatusOutput) error {
	writer := cmd.OutOrStdout()
	if agentMode {
		ap := output.NewAgentPrinter(writer, &output.ResponseContext{Verb: "replay", Resource: "session"})
		ap.SetJQFilter(jqFilter)
		return ap.Print(status)
	}
	format := outputFormat
	if jqFilter != "" {
		format = output.NormalizeJQOutputFormat(format)
	}
	if plainMode && (format == "table" || format == "wide") {
		format = "json"
	}
	switch format {
	case "json", "yaml", "yml":
		printer := output.NewPrinterWithOpts(output.PrinterOptions{
			Format:   format,
			Writer:   writer,
			JQFilter: jqFilter,
		})
		return printer.Print(status)
	case "table", "wide", "":
		return printReplayStatusText(writer, status)
	default:
		return fmt.Errorf("unsupported output format %q for replay status; use table, json, or yaml", outputFormat)
	}
}

func validateReplayOutputOptions() error {
	if jqFilter != "" {
		if _, err := output.CompileJQ(jqFilter); err != nil {
			return err
		}
	}
	if agentMode {
		return nil
	}
	format := outputFormat
	if jqFilter != "" {
		format = output.NormalizeJQOutputFormat(format)
	}
	if plainMode && (format == "table" || format == "wide") {
		format = "json"
	}
	switch format {
	case "json", "yaml", "yml", "table", "wide", "":
		return nil
	default:
		return fmt.Errorf("unsupported output format %q for replay status; use table, json, or yaml", outputFormat)
	}
}

func printReplayStatusText(w io.Writer, status ReplayStatusOutput) error {
	const width = 23
	line := func(label, value string) {
		if value != "" {
			output.FprintDescribeKV(w, label+":", width, "%s", value)
		}
	}
	line("Context", status.ContextName)
	line("Status", status.Status)
	line("Session ID", status.SessionID)
	line("Session host start", status.SessionStartedAt)
	line("Current host time", status.HostNow)
	line("Clock mode", status.ClockMode)
	line("Host anchor", status.AnchorHost)
	line("Virtual anchor", status.AnchorVirtual)
	line("Virtual start", status.VirtualStart)
	line("Current virtual time", status.VirtualNow)
	line("Data start", status.DataStart)
	line("Data end", status.DataEnd)
	line("Visible end", status.VisibleEnd)
	line("Position", status.Position)
	line("Configuration drift", fmt.Sprintf("%t", status.ConfigurationDrift))
	line("State key", status.StateKey)
	line("Disclosure", status.Disclosure)
	line("Provenance path", status.ProvenancePath)
	line("Completed at", status.CompletedAt)
	line("Stopped at", status.StoppedAt)
	line("Final virtual time", status.FinalVirtualNow)
	for _, stateFile := range status.UnreadableStateFiles {
		line("Unreadable state file", stateFile)
	}
	return nil
}

func replayTimeString(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func commandContext(cmd *cobra.Command) context.Context {
	if cmd.Context() != nil {
		return cmd.Context()
	}
	return context.Background()
}

var replayCmd = newReplayCommand()

func init() {
	rootCmd.AddCommand(replayCmd)
}

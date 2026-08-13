package cmd

import (
	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// applyReplayDisclosureDiscovery resolves the selected context before Cobra
// renders help, catalogs, or completion. Restricted disclosure hides replay
// management verbs and the explain flag without disabling explicit lifecycle
// invocations; enforcement remains the hard guard's responsibility.
func applyReplayDisclosureDiscovery(root *cobra.Command, args []string) error {
	cfg, err := loadConfigForReplayArgs(args)
	if err != nil {
		// Match profile resolution: a command that needs config reports the real
		// load error later, while config-independent commands keep working.
		return nil
	}
	activation, err := replayActivationForConfig(cfg)
	if err != nil {
		return err
	}
	if !activation.Active || activation.Disclosure != session.ReplayDisclosureRestricted {
		return nil
	}

	for _, command := range root.Commands() {
		switch command.Name() {
		case "replay":
			command.Hidden = true
		case "query":
			if flag := command.Flags().Lookup("explain-replay"); flag != nil {
				flag.Hidden = true
			}
		}
	}
	return nil
}

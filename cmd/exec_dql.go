package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/pkg/exec"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// execDQLCmd executes a DQL query (DEPRECATED)
var execDQLCmd = &cobra.Command{
	Use:    "dql [query]",
	Short:  "Execute a DQL query (DEPRECATED: use 'dtctl query')",
	Hidden: true, // Hide from help output
	Long: `Execute a DQL query against Grail storage.

DEPRECATED: This command is deprecated. Use 'dtctl query' instead.
The 'dtctl query' command provides the same functionality with additional
features like template variables.

Examples:
  # Execute inline query (use 'dtctl query' instead)
  dtctl query "fetch logs | limit 10"

  # Execute from file (use 'dtctl query -f' instead)
  dtctl query -f query.dql

  # Output as JSON (use 'dtctl query -o json' instead)
  dtctl query "fetch logs" -o json
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Show deprecation warning
		output.PrintWarning("'dtctl exec dql' is deprecated. Use 'dtctl query' instead.")
		cfg, c, err := SetupClient()
		if err != nil {
			return err
		}

		executor, err := newDQLExecutorFromConfig(cfg, c)
		if err != nil {
			return err
		}

		queryFile, _ := cmd.Flags().GetString("file")

		if queryFile != "" {
			data, err := os.ReadFile(queryFile)
			if err != nil {
				return fmt.Errorf("failed to read file: %w", err)
			}
			return executor.ExecuteWithOptions(string(data), execDQLExecutionOptions(executor))
		}

		if len(args) == 0 {
			return fmt.Errorf("query string or --file is required")
		}

		query := args[0]
		return executor.ExecuteWithOptions(query, execDQLExecutionOptions(executor))
	},
}

func execDQLExecutionOptions(executor *exec.DQLExecutor) exec.DQLExecuteOptions {
	options := exec.DQLExecuteOptions{OutputFormat: outputFormat}
	disclosure, replayActive := executor.ReplayDisclosure(context.Background())
	if agentMode && replayActive && disclosure == session.ReplayDisclosureFull {
		options.AgentMode = true
		options.MetadataFields = []string{"all"}
	}
	return options
}

func init() {
	// DQL flags
	execDQLCmd.Flags().StringP("file", "f", "", "read query from file")
}

package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/suggest"
)

type replayExplainOutput struct {
	VirtualNow       string                      `json:"virtual_now" yaml:"virtual_now"`
	VirtualStart     string                      `json:"virtual_start" yaml:"virtual_start"`
	DataStart        string                      `json:"data_start" yaml:"data_start"`
	DataEnd          string                      `json:"data_end" yaml:"data_end"`
	VisibleStart     string                      `json:"visible_start" yaml:"visible_start"`
	VisibleEnd       string                      `json:"visible_end" yaml:"visible_end"`
	Locale           string                      `json:"locale,omitempty" yaml:"locale,omitempty"`
	Timezone         string                      `json:"timezone" yaml:"timezone"`
	Sources          []replayExplainSourceOutput `json:"sources" yaml:"sources"`
	Notices          []string                    `json:"notices,omitempty" yaml:"notices,omitempty"`
	EffectiveQuery   string                      `json:"effective_query" yaml:"effective_query"`
	CoverageVerified *bool                       `json:"coverage_verified,omitempty" yaml:"coverage_verified,omitempty"`
	CoverageMessage  string                      `json:"coverage_message,omitempty" yaml:"coverage_message,omitempty"`
}

type replayExplainSourceOutput struct {
	Ordinal              int                              `json:"ordinal" yaml:"ordinal"`
	Class                string                           `json:"class" yaml:"class"`
	Name                 string                           `json:"name" yaml:"name"`
	BoundaryPolicy       string                           `json:"boundary_policy" yaml:"boundary_policy"`
	RequestedFrom        string                           `json:"requested_from,omitempty" yaml:"requested_from,omitempty"`
	RequestedTo          string                           `json:"requested_to,omitempty" yaml:"requested_to,omitempty"`
	EffectiveFrom        string                           `json:"effective_from,omitempty" yaml:"effective_from,omitempty"`
	EffectiveTo          string                           `json:"effective_to,omitempty" yaml:"effective_to,omitempty"`
	Classification       string                           `json:"classification" yaml:"classification"`
	Proof                string                           `json:"proof" yaml:"proof"`
	DavisProblemsMapping *replayExplainDavisMappingOutput `json:"davis_problems_mapping,omitempty" yaml:"davis_problems_mapping,omitempty"`
}

type replayExplainDavisMappingOutput struct {
	Eligible               bool   `json:"eligible" yaml:"eligible"`
	OriginalView           string `json:"original_view" yaml:"original_view"`
	EffectiveSnapshotTable string `json:"effective_snapshot_table" yaml:"effective_snapshot_table"`
	LogicalF               string `json:"logical_f" yaml:"logical_f"`
	LogicalT               string `json:"logical_t" yaml:"logical_t"`
	PhysicalW              string `json:"physical_w" yaml:"physical_w"`
	PhysicalT              string `json:"physical_t" yaml:"physical_t"`
	WarmupClamped          bool   `json:"warmup_clamped" yaml:"warmup_clamped"`
}

func restrictedExplainFlagError() error {
	return &suggest.FlagError{Flag: "explain-replay", Message: "unknown flag --explain-replay"}
}

func printReplayExplanation(value replay.ExplainData) error {
	payload := replayExplainOutput{
		VirtualNow: replayExplainTime(value.Clock.VirtualNow), VirtualStart: replayExplainTime(value.Clock.VirtualStart),
		DataStart: replayExplainTime(value.Clock.ReplayInterval.Start), DataEnd: replayExplainTime(value.Clock.ReplayInterval.End),
		VisibleStart: replayExplainTime(value.Clock.VisibleInterval.Start), VisibleEnd: replayExplainTime(value.Clock.VisibleInterval.End),
		Locale: value.Clock.Locale, Timezone: value.Clock.Timezone, EffectiveQuery: value.EffectiveDQL,
		CoverageVerified: value.CoverageVerified, CoverageMessage: value.CoverageMessage,
	}
	for _, source := range value.Sources {
		item := replayExplainSourceOutput{
			Ordinal: source.Ordinal, Class: string(source.Class), Name: source.Name,
			BoundaryPolicy: string(source.BoundaryPolicy), Classification: string(source.Classification),
			Proof: source.Proof.Reason,
		}
		if source.Requested != nil {
			item.RequestedFrom = replayExplainTime(source.Requested.Range.Start)
			item.RequestedTo = replayExplainTime(source.Requested.Range.End)
		}
		if source.Effective != nil {
			item.EffectiveFrom = replayExplainTime(source.Effective.Start)
			item.EffectiveTo = replayExplainTime(source.Effective.End)
		}
		if mapping := source.DavisMapping; mapping != nil {
			item.DavisProblemsMapping = &replayExplainDavisMappingOutput{
				Eligible: true, OriginalView: mapping.Candidate.OriginalToken,
				EffectiveSnapshotTable: mapping.Candidate.SnapshotToken,
				LogicalF:               replayExplainTime(mapping.Logical.F), LogicalT: replayExplainTime(mapping.Logical.T),
				PhysicalW: replayExplainTime(mapping.Physical.W), PhysicalT: replayExplainTime(mapping.Physical.T),
				WarmupClamped: mapping.WarmupClamped,
			}
		}
		payload.Sources = append(payload.Sources, item)
	}
	for _, notice := range value.Notices {
		payload.Notices = append(payload.Notices, notice.Message)
	}
	if (outputFormat != "" && outputFormat != "table" && outputFormat != "wide") || agentMode {
		printer := NewPrinter()
		enrichAgent(printer, "query", "dql")
		return printer.Print(payload)
	}
	fmt.Fprintf(os.Stdout, "Virtual now: %s\n", payload.VirtualNow)
	fmt.Fprintf(os.Stdout, "Virtual start: %s\n", payload.VirtualStart)
	fmt.Fprintf(os.Stdout, "Replay interval: %s – %s\n", payload.DataStart, payload.DataEnd)
	fmt.Fprintf(os.Stdout, "Visible interval: %s – %s\n", payload.VisibleStart, payload.VisibleEnd)
	if payload.Locale != "" {
		fmt.Fprintf(os.Stdout, "Locale: %s\n", payload.Locale)
	}
	fmt.Fprintf(os.Stdout, "Timezone: %s\n", payload.Timezone)
	for _, source := range payload.Sources {
		fmt.Fprintf(os.Stdout, "Source %d: %s %s (%s; %s)\n", source.Ordinal, source.Class, source.Name, source.BoundaryPolicy, source.Classification)
		fmt.Fprintf(os.Stdout, "  Requested: %s – %s\n", source.RequestedFrom, source.RequestedTo)
		fmt.Fprintf(os.Stdout, "  Effective: %s – %s\n", source.EffectiveFrom, source.EffectiveTo)
		fmt.Fprintf(os.Stdout, "  Proof: %s\n", source.Proof)
		if mapping := source.DavisProblemsMapping; mapping != nil {
			fmt.Fprintf(os.Stdout, "  Davis problems mapping eligible: %t (%s -> %s)\n", mapping.Eligible, mapping.OriginalView, mapping.EffectiveSnapshotTable)
			fmt.Fprintf(os.Stdout, "  Logical Davis problems view interval [F,T): %s – %s\n", mapping.LogicalF, mapping.LogicalT)
			fmt.Fprintf(os.Stdout, "  Physical snapshot interval [W,T): %s – %s\n", mapping.PhysicalW, mapping.PhysicalT)
			fmt.Fprintf(os.Stdout, "  Warm-up clamped: %t\n", mapping.WarmupClamped)
		}
	}
	for _, notice := range payload.Notices {
		fmt.Fprintf(os.Stdout, "Notice: %s\n", notice)
	}
	if payload.CoverageVerified != nil {
		fmt.Fprintf(os.Stdout, "Coverage verified: %t\n", *payload.CoverageVerified)
	}
	if payload.CoverageMessage != "" {
		fmt.Fprintln(os.Stdout, payload.CoverageMessage)
	}
	fmt.Fprintf(os.Stdout, "Effective DQL:\n%s\n", payload.EffectiveQuery)
	return nil
}

func replayExplainTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/suggest"
)

type replayExplainOutput struct {
	VirtualNow     string                      `json:"virtual_now" yaml:"virtual_now"`
	VirtualStart   string                      `json:"virtual_start" yaml:"virtual_start"`
	DataStart      string                      `json:"data_start" yaml:"data_start"`
	DataEnd        string                      `json:"data_end" yaml:"data_end"`
	VisibleStart   string                      `json:"visible_start" yaml:"visible_start"`
	VisibleEnd     string                      `json:"visible_end" yaml:"visible_end"`
	Locale         string                      `json:"locale,omitempty" yaml:"locale,omitempty"`
	Timezone       string                      `json:"timezone" yaml:"timezone"`
	Sources        []replayExplainSourceOutput `json:"sources" yaml:"sources"`
	Notices        []string                    `json:"notices,omitempty" yaml:"notices,omitempty"`
	EffectiveQuery string                      `json:"effective_query" yaml:"effective_query"`
}

type replayExplainSourceOutput struct {
	Ordinal        int    `json:"ordinal" yaml:"ordinal"`
	Class          string `json:"class" yaml:"class"`
	Name           string `json:"name" yaml:"name"`
	BoundaryPolicy string `json:"boundary_policy" yaml:"boundary_policy"`
	RequestedFrom  string `json:"requested_from,omitempty" yaml:"requested_from,omitempty"`
	RequestedTo    string `json:"requested_to,omitempty" yaml:"requested_to,omitempty"`
	EffectiveFrom  string `json:"effective_from,omitempty" yaml:"effective_from,omitempty"`
	EffectiveTo    string `json:"effective_to,omitempty" yaml:"effective_to,omitempty"`
	Classification string `json:"classification" yaml:"classification"`
	Proof          string `json:"proof" yaml:"proof"`
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
	}
	for _, notice := range payload.Notices {
		fmt.Fprintf(os.Stdout, "Notice: %s\n", notice)
	}
	fmt.Fprintf(os.Stdout, "Effective DQL:\n%s\n", payload.EffectiveQuery)
	return nil
}

func replayExplainTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

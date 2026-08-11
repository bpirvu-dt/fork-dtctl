package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/dynatrace-oss/dtctl/sdk/urls"
)

const (
	// ReplayClockRealtime advances virtual time at the host-clock rate.
	ReplayClockRealtime = "realtime"
	// ReplayClockManual holds virtual time fixed until an explicit advance.
	ReplayClockManual = "manual"

	// ReplayDisclosureFull preserves the ordinary replay management output.
	ReplayDisclosureFull = "full"
	// ReplayDisclosureRestricted routes replay facts to private provenance.
	ReplayDisclosureRestricted = "restricted"
)

// ReplayConfig is the stable, user-authored replay block stored in a context.
// Timestamps remain strings so configuration round-trips preserve exact YAML.
type ReplayConfig struct {
	DataStart      string `yaml:"data_start" json:"data_start"`
	DataEnd        string `yaml:"data_end" json:"data_end"`
	VirtualStart   string `yaml:"virtual_start,omitempty" json:"virtual_start,omitempty"`
	ClockMode      string `yaml:"clock_mode,omitempty" json:"clock_mode,omitempty"`
	Disclosure     string `yaml:"disclosure,omitempty" json:"disclosure,omitempty"`
	ProvenancePath string `yaml:"provenance_path,omitempty" json:"provenance_path,omitempty"`
}

// ReplayValueSource records how a resolved session value was selected.
type ReplayValueSource string

const (
	ReplayValueFromFlag    ReplayValueSource = "flag"
	ReplayValueFromContext ReplayValueSource = "context"
	ReplayValueFromDefault ReplayValueSource = "default"
)

// ReplayValueSources records precedence decisions made by replay start.
type ReplayValueSources struct {
	DataStart      ReplayValueSource `json:"data_start" yaml:"data_start"`
	DataEnd        ReplayValueSource `json:"data_end" yaml:"data_end"`
	VirtualStart   ReplayValueSource `json:"virtual_start" yaml:"virtual_start"`
	ClockMode      ReplayValueSource `json:"clock_mode" yaml:"clock_mode"`
	Disclosure     ReplayValueSource `json:"disclosure" yaml:"disclosure"`
	ProvenancePath ReplayValueSource `json:"provenance_path" yaml:"provenance_path"`
}

// ReplayConfigOverrides contains only the start-command values explicitly set
// by flags. Nil means the corresponding context value retains precedence.
type ReplayConfigOverrides struct {
	DataStart    *string
	DataEnd      *string
	VirtualStart *string
	ClockMode    *string
}

// ResolvedReplayConfig is the validated UTC session configuration written to
// runtime state. Disclosure and provenance have no command-line overrides.
type ResolvedReplayConfig struct {
	DataStart      time.Time
	DataEnd        time.Time
	VirtualStart   time.Time
	ClockMode      string
	Disclosure     string
	ProvenancePath string
	ValueSources   ReplayValueSources
}

// ContextKey is a SHA-256 identity used in replay filenames. Its digest input
// includes config source, context name, and normalized environment URL.
type ContextKey string

// ReplayDir returns the private replay directory below the session state root.
func ReplayDir() string {
	return filepath.Join(StateDir(), "replay")
}

// NewReplayContextKey derives the opaque state-file key required by the replay
// contract. No identifying input is present in the returned filename-safe hash.
func NewReplayContextKey(configSource, contextName, environment string) ContextKey {
	return ContextKey(hashReplayValue(struct {
		ConfigSource string `json:"config_source"`
		ContextName  string `json:"context_name"`
		Environment  string `json:"environment"`
	}{configSource, contextName, NormalizeReplayEnvironment(environment)}))
}

// ReplayContextIdentityHash identifies one named context in one config source,
// independently of environment drift. Writer locks use this stable identity.
func ReplayContextIdentityHash(configSource, contextName string) string {
	return hashReplayValue(struct {
		ConfigSource string `json:"config_source"`
		ContextName  string `json:"context_name"`
	}{configSource, contextName})
}

// ReplayEnvironmentHash returns an opaque digest of the normalized target URL.
func ReplayEnvironmentHash(environment string) string {
	return hashReplayValue(NormalizeReplayEnvironment(environment))
}

// NormalizeReplayEnvironment canonicalizes the URL inputs used by replay state
// identity and drift checks. It builds on the session URL normalization while
// making scheme/host case and a trailing slash immaterial.
func NormalizeReplayEnvironment(environment string) string {
	normalized := urls.Normalize(strings.TrimSpace(environment))
	u, err := url.Parse(normalized)
	if err != nil || u.Host == "" {
		return strings.TrimRight(normalized, "/")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String()
}

// ResolveReplayConfig applies flag-over-context precedence, defaults omitted
// values, validates the complete configuration, and normalizes timestamps to
// UTC. replayDir is injectable so tests never touch the real state directory.
func ResolveReplayConfig(raw *ReplayConfig, overrides ReplayConfigOverrides, replayDir string, key ContextKey) (ResolvedReplayConfig, error) {
	if raw == nil {
		raw = &ReplayConfig{}
	}

	dataStartText, dataStartSource := resolveReplayValue(overrides.DataStart, raw.DataStart, "")
	dataEndText, dataEndSource := resolveReplayValue(overrides.DataEnd, raw.DataEnd, "")
	missing := make([]string, 0, 2)
	if dataStartText == "" {
		missing = append(missing, "data_start")
	}
	if dataEndText == "" {
		missing = append(missing, "data_end")
	}
	if len(missing) > 0 {
		return ResolvedReplayConfig{}, fmt.Errorf("replay configuration is incomplete: missing %s", strings.Join(missing, ", "))
	}

	dataStart, err := parseReplayTimestamp("data_start", dataStartText)
	if err != nil {
		return ResolvedReplayConfig{}, err
	}
	dataEnd, err := parseReplayTimestamp("data_end", dataEndText)
	if err != nil {
		return ResolvedReplayConfig{}, err
	}
	if !dataStart.Before(dataEnd) {
		return ResolvedReplayConfig{}, fmt.Errorf("replay data_start must be before data_end (resolved data_start=%s, data_end=%s)", formatReplayTime(dataStart), formatReplayTime(dataEnd))
	}

	virtualText, virtualSource := resolveReplayValue(overrides.VirtualStart, raw.VirtualStart, dataStartText)
	virtualStart, err := parseReplayTimestamp("virtual_start", virtualText)
	if err != nil {
		return ResolvedReplayConfig{}, err
	}
	if virtualStart.Before(dataStart) || virtualStart.After(dataEnd) {
		return ResolvedReplayConfig{}, fmt.Errorf("replay virtual_start must be within the replay interval (resolved data_start=%s, virtual_start=%s, data_end=%s)", formatReplayTime(dataStart), formatReplayTime(virtualStart), formatReplayTime(dataEnd))
	}

	clockMode, clockSource := resolveReplayValue(overrides.ClockMode, raw.ClockMode, ReplayClockRealtime)
	if clockMode != ReplayClockRealtime && clockMode != ReplayClockManual {
		return ResolvedReplayConfig{}, fmt.Errorf("unknown replay clock_mode %q: use %q or %q", clockMode, ReplayClockRealtime, ReplayClockManual)
	}

	disclosure, disclosureSource := resolveReplayValue(nil, raw.Disclosure, ReplayDisclosureFull)
	if disclosure != ReplayDisclosureFull && disclosure != ReplayDisclosureRestricted {
		return ResolvedReplayConfig{}, fmt.Errorf("unknown replay disclosure %q: use %q or %q", disclosure, ReplayDisclosureFull, ReplayDisclosureRestricted)
	}

	provenancePath := ""
	provenanceSource := ReplayValueFromDefault
	if disclosure == ReplayDisclosureRestricted {
		if raw.ProvenancePath != "" {
			provenancePath = raw.ProvenancePath
			provenanceSource = ReplayValueFromContext
		} else {
			absReplayDir, absErr := filepath.Abs(replayDir)
			if absErr != nil {
				return ResolvedReplayConfig{}, fmt.Errorf("resolve replay state directory: %w", absErr)
			}
			provenancePath = filepath.Join(filepath.Clean(absReplayDir), string(key)+".provenance.jsonl")
		}
		if err := ValidateReplayProvenancePath(provenancePath, raw.ProvenancePath == ""); err != nil {
			return ResolvedReplayConfig{}, err
		}
		provenancePath = filepath.Clean(provenancePath)
	}

	return ResolvedReplayConfig{
		DataStart:      dataStart.UTC(),
		DataEnd:        dataEnd.UTC(),
		VirtualStart:   virtualStart.UTC(),
		ClockMode:      clockMode,
		Disclosure:     disclosure,
		ProvenancePath: provenancePath,
		ValueSources: ReplayValueSources{
			DataStart:      dataStartSource,
			DataEnd:        dataEndSource,
			VirtualStart:   virtualSource,
			ClockMode:      clockSource,
			Disclosure:     disclosureSource,
			ProvenancePath: provenanceSource,
		},
	}, nil
}

func resolveReplayValue(override *string, contextValue, defaultValue string) (string, ReplayValueSource) {
	if override != nil {
		return *override, ReplayValueFromFlag
	}
	if contextValue != "" {
		return contextValue, ReplayValueFromContext
	}
	return defaultValue, ReplayValueFromDefault
}

func parseReplayTimestamp(field, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid replay %s %q: expected an absolute RFC 3339 timestamp with a timezone (for example, 2026-06-14T08:00:00Z)", field, value)
	}
	return parsed.UTC(), nil
}

func formatReplayTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// ReplayConfigHash covers the six resolved values that remain authoritative
// for the lifetime of a session.
func ReplayConfigHash(cfg ResolvedReplayConfig) string {
	return hashReplayValue(struct {
		DataStart      string `json:"data_start"`
		DataEnd        string `json:"data_end"`
		VirtualStart   string `json:"virtual_start"`
		ClockMode      string `json:"clock_mode"`
		Disclosure     string `json:"disclosure"`
		ProvenancePath string `json:"provenance_path"`
	}{
		formatReplayTime(cfg.DataStart),
		formatReplayTime(cfg.DataEnd),
		formatReplayTime(cfg.VirtualStart),
		cfg.ClockMode,
		cfg.Disclosure,
		cfg.ProvenancePath,
	})
}

// ReplayContextInputHash snapshots the current context inputs whose drift must
// block replay use. Token references and credentials are deliberately excluded.
func ReplayContextInputHash(ctx *Context) string {
	if ctx == nil {
		return hashReplayValue(nil)
	}
	return hashReplayValue(struct {
		Environment string        `json:"environment"`
		Replay      *ReplayConfig `json:"replay"`
		SafetyLevel string        `json:"safety_level"`
		Profile     string        `json:"profile"`
	}{
		NormalizeReplayEnvironment(ctx.Environment),
		ctx.Replay,
		ctx.GetEffectiveSafetyLevel().String(),
		ctx.Profile,
	})
}

func hashReplayValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal replay hash input: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

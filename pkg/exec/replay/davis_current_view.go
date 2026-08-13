package replay

import "fmt"

const davisProblemsWarmupCaveat = "When the visible window is short, the snapshot read may need to start before <visible-start> while the lifetime filter bounds stay at <visible-start> and <visible-end>; open problem snapshots have a documented six-hour refresh cadence, so retain at least six hours of warm-up."

func currentDavisView(table string, node *Node) (*DavisCurrentViewError, bool) {
	var snapshot, identity, pattern string
	switch table {
	case "dt.davis.problems":
		snapshot, identity = "dt.davis.problems.snapshots", "problem"
		pattern = fmt.Sprintf("fetch %s, from:<visible-start>, to:<visible-end> | sort timestamp desc | dedup event.id | filter event.start < <visible-end> and coalesce(event.end, <visible-end>) >= <visible-start>", snapshot)
	case "dt.davis.events":
		snapshot, identity = "dt.davis.events.snapshots", "event"
		// Event-view equivalence is unverified pending its own spike; do not
		// assume the problem lifetime fields or filter apply to events.
		pattern = fmt.Sprintf("fetch %s, from:<visible-start>, to:<visible-end> | sort timestamp desc | dedup event.id", snapshot)
	default:
		return nil, false
	}
	err := &DavisCurrentViewError{
		View:               table,
		SnapshotTable:      snapshot,
		IdentityKind:       identity,
		IdentityField:      "event.id",
		LatestPerIDPattern: pattern,
		Path:               node.Path,
	}
	if node.Span != nil {
		span := *node.Span
		err.Span = &span
	}
	return err, true
}

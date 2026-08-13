package replay

import "fmt"

func currentDavisView(table string, node *Node) (*DavisCurrentViewError, bool) {
	var snapshot, identity string
	switch table {
	case "dt.davis.problems":
		snapshot, identity = "dt.davis.problems.snapshots", "problem"
	case "dt.davis.events":
		snapshot, identity = "dt.davis.events.snapshots", "event"
	default:
		return nil, false
	}
	err := &DavisCurrentViewError{
		View:               table,
		SnapshotTable:      snapshot,
		IdentityKind:       identity,
		IdentityField:      "event.id",
		LatestPerIDPattern: fmt.Sprintf("fetch %s, from:<visible-start>, to:<visible-end> | sort timestamp desc | dedup event.id", snapshot),
		Path:               node.Path,
	}
	if node.Span != nil {
		span := *node.Span
		err.Span = &span
	}
	return err, true
}

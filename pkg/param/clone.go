package param

// CloneLogEntry returns a copy that does not share mutable command payloads
// with the source entry.
func CloneLogEntry(entry LogEntry) LogEntry {
	entry.Command = CloneCommand(entry.Command)
	return entry
}

// CloneLogEntries copies a log entry slice and each entry's mutable command
// payload.
func CloneLogEntries(entries []LogEntry) []LogEntry {
	if entries == nil {
		return nil
	}
	cloned := make([]LogEntry, len(entries))
	for i, entry := range entries {
		cloned[i] = CloneLogEntry(entry)
	}
	return cloned
}

// CloneSnapshot returns a copy that does not share mutable data bytes with the
// source snapshot.
func CloneSnapshot(snapshot *Snapshot) *Snapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	if snapshot.Data != nil {
		cloned.Data = append([]byte(nil), snapshot.Data...)
	}
	return &cloned
}

// CloneCommand copies command shapes used by the Raft log and state-machine
// adapters. Unknown value types are returned as-is because this package cannot
// infer their ownership semantics.
func CloneCommand(command any) any {
	switch cmd := command.(type) {
	case nil:
		return nil
	case []byte:
		if cmd == nil {
			return []byte(nil)
		}
		return append([]byte(nil), cmd...)
	case string:
		return cmd
	case KVCommand:
		return cmd
	case ConfigChangeCommand:
		return ConfigChangeCommand{NewPeerIDs: append([]int(nil), cmd.NewPeerIDs...)}
	case ClientCommand:
		return NewClientCommand(cmd.ClientID, cmd.SequenceNum, CloneCommand(cmd.Command))
	case NoopCommand:
		return cmd
	default:
		return command
	}
}

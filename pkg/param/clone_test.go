package param

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneLogEntryCopiesMutableCommandPayloads(t *testing.T) {
	t.Run("config change command", func(t *testing.T) {
		peers := []int{1, 2, 3}
		entry := LogEntry{
			Term:    1,
			Index:   2,
			Command: ConfigChangeCommand{NewPeerIDs: peers},
		}

		cloned := CloneLogEntry(entry)
		peers[0] = 99
		cloned.Command.(ConfigChangeCommand).NewPeerIDs[1] = 100

		assert.Equal(t, []int{99, 2, 3}, entry.Command.(ConfigChangeCommand).NewPeerIDs)
		assert.Equal(t, []int{1, 100, 3}, cloned.Command.(ConfigChangeCommand).NewPeerIDs)
	})

	t.Run("client command with bytes", func(t *testing.T) {
		payload := []byte("payload")
		entry := LogEntry{
			Term:    1,
			Index:   2,
			Command: NewClientCommand(7, 8, payload),
		}

		cloned := CloneLogEntry(entry)
		payload[0] = 'X'
		cloned.Command.(ClientCommand).Command.([]byte)[1] = 'Y'

		assert.Equal(t, []byte("Xayload"), entry.Command.(ClientCommand).Command)
		assert.Equal(t, []byte("pYyload"), cloned.Command.(ClientCommand).Command)
	})
}

package param

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpTypeUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		data string
		want OpType
	}{
		{name: "number", data: `2`, want: OpSet},
		{name: "string", data: `"delete"`, want: OpDelete},
		{name: "case insensitive string", data: `"GET"`, want: OpGet},
		{name: "unknown string", data: `"unknown"`, want: OpUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got OpType
			err := json.Unmarshal([]byte(tt.data), &got)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOpTypeUnmarshalJSONRejectsUnknownString(t *testing.T) {
	var got OpType
	err := json.Unmarshal([]byte(`"bad-op"`), &got)
	assert.Error(t, err)
}

package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/config"
	transportgrpc "github.com/xmh1011/go-kv/pkg/transport/grpc"
)

func TestDefaultElectionTimeoutCoversAppendEntriesTimeout(t *testing.T) {
	assert.Greater(t,
		config.DefaultElectionTimeout,
		transportgrpc.DefaultAppendEntriesTimeout,
		"followers must not start a new election before a healthy AppendEntries RPC can return")
}

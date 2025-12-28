package grpc

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/raft/api"
)

func TestGRPCTransport(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create two transports
	t1, err := NewTransport("127.0.0.1:0")
	assert.NoError(t, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	assert.NoError(t, err)
	defer t2.Close()

	// Mock Raft implementation for t1 and t2
	mockRaft1 := api.NewMockRaftService(ctrl)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := api.NewMockRaftService(ctrl)
	t2.RegisterRaft(mockRaft2)

	// Start transports
	assert.NoError(t, t1.Start())
	assert.NoError(t, t2.Start())

	// Setup peers
	peers := map[int]string{
		1: t1.Addr(),
		2: t2.Addr(),
	}
	t1.SetPeers(peers)
	t2.SetPeers(peers)

	// Test RequestVote
	t.Run("RequestVote", func(t *testing.T) {
		req := &param.RequestVoteArgs{
			Term:         1,
			CandidateID:  1,
			LastLogIndex: 10,
			LastLogTerm:  1,
			PreVote:      false,
		}
		resp := &param.RequestVoteReply{}

		mockRaft2.EXPECT().RequestVote(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = 1
				reply.VoteGranted = true
				return nil
			}).Times(1)

		err := t1.SendRequestVote("2", req, resp)
		assert.NoError(t, err)
		assert.True(t, resp.VoteGranted)
		assert.Equal(t, uint64(1), resp.Term)
	})

	// Test AppendEntries
	t.Run("AppendEntries", func(t *testing.T) {
		req := &param.AppendEntriesArgs{
			Term:     1,
			LeaderID: 1,
			Entries: []param.LogEntry{
				{Command: "cmd1", Term: 1, Index: 1},
			},
		}
		resp := &param.AppendEntriesReply{}

		mockRaft2.EXPECT().AppendEntries(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Success = true
				reply.Term = 1
				return nil
			}).Times(1)

		err := t1.SendAppendEntries("2", req, resp)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	})

	// Test ClientRequest
	t.Run("ClientRequest", func(t *testing.T) {
		req := &param.ClientArgs{
			ClientID:    100,
			SequenceNum: 1,
			Command:     "set key value",
		}
		resp := &param.ClientReply{}

		mockRaft2.EXPECT().ClientRequest(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.ClientArgs, reply *param.ClientReply) error {
				reply.Success = true
				reply.Result = "ok"
				return nil
			}).Times(1)

		err := t1.SendClientRequest("2", req, resp)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
		assert.Equal(t, "ok", resp.Result)
	})

	// Wait for async operations to complete if any
	time.Sleep(100 * time.Millisecond)
}

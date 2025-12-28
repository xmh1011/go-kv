package inmemory

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/raft/api"
)

func TestInMemoryTransport(t *testing.T) {
	t.Run("New, Connect, and Disconnect", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		trans := NewTransport("local")
		assert.NotNil(t, trans, "NewTransport should not return nil")
		assert.NotNil(t, trans.peers, "peers map should be initialized")
		assert.Empty(t, trans.peers, "peers map should be initially empty")

		mockPeer := api.NewMockRaftService(ctrl)
		trans.Connect("peer1", mockPeer)
		assert.Len(t, trans.peers, 1, "peers map should have 1 entry after connect")
		assert.Contains(t, trans.peers, "peer1", "peers map should contain the new peer")

		trans.Disconnect("peer1")
		assert.Empty(t, trans.peers, "peers map should be empty after disconnect")
	})

	t.Run("Send successful RPC calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		trans := NewTransport("local")
		mockPeer := api.NewMockRaftService(ctrl)
		trans.Connect("peer1", mockPeer)

		// --- Test SendRequestVote ---
		argsRV := &param.RequestVoteArgs{Term: 1, CandidateID: 10}
		replyRV := &param.RequestVoteReply{}
		mockPeer.EXPECT().RequestVote(gomock.Eq(argsRV), gomock.Any()).
			DoAndReturn(func(_ *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = 1
				reply.VoteGranted = true
				return nil
			}).Times(1)

		err := trans.SendRequestVote("peer1", argsRV, replyRV)
		assert.NoError(t, err, "SendRequestVote should not return an error on success")
		assert.Equal(t, uint64(1), replyRV.Term, "reply should be filled correctly for RequestVote")
		assert.True(t, replyRV.VoteGranted, "reply should be filled correctly for RequestVote")

		// --- Test SendAppendEntries ---
		argsAE := &param.AppendEntriesArgs{Term: 2}
		replyAE := &param.AppendEntriesReply{}
		mockPeer.EXPECT().AppendEntries(gomock.Eq(argsAE), gomock.Any()).
			DoAndReturn(func(_ *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 2
				reply.Success = true
				return nil
			}).Times(1)

		err = trans.SendAppendEntries("peer1", argsAE, replyAE)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), replyAE.Term)
		assert.True(t, replyAE.Success)

		// --- Test SendInstallSnapshot ---
		argsIS := &param.InstallSnapshotArgs{Term: 3}
		replyIS := &param.InstallSnapshotReply{}
		mockPeer.EXPECT().InstallSnapshot(gomock.Eq(argsIS), gomock.Any()).
			DoAndReturn(func(_ *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
				reply.Term = 3
				return nil
			}).Times(1)

		err = trans.SendInstallSnapshot("peer1", argsIS, replyIS)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), replyIS.Term)

		// --- Test SendClientRequest ---
		argsCR := &param.ClientArgs{Command: "SET"}
		replyCR := &param.ClientReply{}
		mockPeer.EXPECT().ClientRequest(gomock.Eq(argsCR), gomock.Any()).
			DoAndReturn(func(_ *param.ClientArgs, reply *param.ClientReply) error {
				reply.Success = true
				reply.Result = "OK"
				return nil
			}).Times(1)

		err = trans.SendClientRequest("peer1", argsCR, replyCR)
		assert.NoError(t, err)
		assert.True(t, replyCR.Success)
		assert.Equal(t, "OK", replyCR.Result)
	})

	t.Run("Send RPC to non-existent peer", func(t *testing.T) {
		trans := NewTransport("local")
		// No peers are connected
		err := trans.SendRequestVote("non-existent-peer", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.Error(t, err, "sending to a non-existent peer should return an error")
		assert.Contains(t, err.Error(), "could not connect to peer", "error message should be informative")
	})

	t.Run("Send RPC where peer returns an error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		trans := NewTransport("local")
		mockPeer := api.NewMockRaftService(ctrl)
		trans.Connect("peer1", mockPeer)

		expectedErr := errors.New("mock RPC failure")
		mockPeer.EXPECT().RequestVote(gomock.Any(), gomock.Any()).Return(expectedErr).Times(1)

		err := trans.SendRequestVote("peer1", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.Error(t, err, "transport should propagate the error from the peer")
		assert.Equal(t, expectedErr, err, "the returned error should be the one from the mock")
	})
}

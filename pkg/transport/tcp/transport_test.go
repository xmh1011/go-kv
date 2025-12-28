package tcp

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/raft/api"
)

func TestTCPTransport(t *testing.T) {

	t.Run("Successful end-to-end RPC call", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPeer := api.NewMockRaftService(ctrl)

		// --- 设置 Peer (RPC 服务器) ---
		// 期望 RequestVote 被调用一次
		mockPeer.EXPECT().RequestVote(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				assert.Equal(t, uint64(1), args.Term)
				assert.Equal(t, 10, args.CandidateID)
				reply.Term = 1
				reply.VoteGranted = true
				return nil
			}).Times(1)

		transPeer, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transPeer.Close()

		// 注册包装器
		transPeer.RegisterRaft(mockPeer)
		err = transPeer.Start()
		assert.NoError(t, err, "Server Start should not fail")

		peerAddr := transPeer.listener.Addr().String()

		// --- 设置 Local (RPC 客户端) ---
		transLocal, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transLocal.Close()

		transLocal.SetPeers(map[int]string{2: peerAddr})

		// --- 执行 RPC 调用 ---
		args := &param.RequestVoteArgs{Term: 1, CandidateID: 10}
		reply := &param.RequestVoteReply{}
		err = transLocal.SendRequestVote("2", args, reply)

		// --- 断言结果 ---
		assert.NoError(t, err, "RPC call should succeed")
		assert.Equal(t, uint64(1), reply.Term, "Reply term should match mock's response")
		assert.True(t, reply.VoteGranted, "Reply vote granted should match mock's response")
	})

	t.Run("Dial non-existent server", func(t *testing.T) {
		transLocal, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transLocal.Close()

		transLocal.SetPeers(map[int]string{3: "localhost:1"}) // 一个几乎肯定不可用的端口

		err = transLocal.SendRequestVote("3", &param.RequestVoteArgs{}, &param.RequestVoteReply{})

		assert.Error(t, err, "Should get an error when dialing a non-existent server")
		// The error can be "connection refused" or "timeout" depending on the OS and timing.
		// Checking for a generic error is more robust.
	})

	t.Run("Client connection caching", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockPeer := api.NewMockRaftService(ctrl)
		mockPeer.EXPECT().RequestVote(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		transPeer, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transPeer.Close()

		transPeer.RegisterRaft(mockPeer)
		err = transPeer.Start()
		assert.NoError(t, err)

		peerAddr := transPeer.listener.Addr().String()

		transLocal, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transLocal.Close()

		transLocal.SetPeers(map[int]string{2: peerAddr})

		err = transLocal.SendRequestVote("2", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.NoError(t, err)
		assert.Len(t, transLocal.peers, 1, "A client connection should be cached after the first call")

		err = transLocal.SendRequestVote("2", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.NoError(t, err)
		assert.Len(t, transLocal.peers, 1, "Connection cache size should not increase on subsequent calls")
	})

	t.Run("Handle server-side error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockPeer := api.NewMockRaftService(ctrl)
		expectedErr := errors.New("a deliberate error from peer")

		mockPeer.EXPECT().RequestVote(gomock.Any(), gomock.Any()).Return(expectedErr).Times(1)

		transPeer, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transPeer.Close()

		transPeer.RegisterRaft(mockPeer)
		err = transPeer.Start()
		assert.NoError(t, err)

		peerAddr := transPeer.listener.Addr().String()

		transLocal, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer transLocal.Close()

		transLocal.SetPeers(map[int]string{2: peerAddr})

		err = transLocal.SendRequestVote("2", &param.RequestVoteArgs{}, &param.RequestVoteReply{})

		assert.Error(t, err, "Client should receive an error if the server returns one")
		assert.Contains(t, err.Error(), expectedErr.Error(), "The propagated error should contain the original error message")
	})

	t.Run("Close listener", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		trans, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		trans.RegisterRaft(api.NewMockRaftService(ctrl))
		err = trans.Start()
		assert.NoError(t, err)
		addr := trans.listener.Addr().String()

		err = trans.Close()
		assert.NoError(t, err, "Close() should not return an error")

		time.Sleep(50 * time.Millisecond)

		trans2, err := NewTransport("localhost:0")
		assert.NoError(t, err)
		defer trans2.Close()

		trans2.SetPeers(map[int]string{2: addr})

		err = trans2.SendRequestVote("2", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.Error(t, err, "Should not be able to connect to a closed listener")
	})
}

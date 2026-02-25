package grpc

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/raft/api"
)

// TestGRPCTransport 基础功能测试
func TestGRPCTransport(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t1, err := NewTransport("127.0.0.1:0")
	assert.NoError(t, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	assert.NoError(t, err)
	defer t2.Close()

	mockRaft1 := api.NewMockRaftService(ctrl)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := api.NewMockRaftService(ctrl)
	t2.RegisterRaft(mockRaft2)

	assert.NoError(t, t1.Start())
	assert.NoError(t, t2.Start())

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

	time.Sleep(50 * time.Millisecond)
}

// TestInstallSnapshotStream 测试流式快照传输
func TestInstallSnapshotStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t1, err := NewTransport("127.0.0.1:0")
	require.NoError(t, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	require.NoError(t, err)
	defer t2.Close()

	mockRaft1 := api.NewMockRaftService(ctrl)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := api.NewMockRaftService(ctrl)
	t2.RegisterRaft(mockRaft2)

	require.NoError(t, t1.Start())
	require.NoError(t, t2.Start())

	peers := map[int]string{
		1: t1.Addr(),
		2: t2.Addr(),
	}
	t1.SetPeers(peers)
	t2.SetPeers(peers)

	t.Run("SmallSnapshot", func(t *testing.T) {
		snapshotData := make([]byte, 1024) // 1KB

		var callCount int
		var mu sync.Mutex

		mockRaft2.EXPECT().InstallSnapshot(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
				mu.Lock()
				defer mu.Unlock()
				callCount++
				assert.Equal(t, snapshotData, args.Data)
				assert.Equal(t, uint64(0), args.Offset)
				assert.True(t, args.Done)
				reply.Term = args.Term
				return nil
			}).Times(1)

		err := t1.SendInstallSnapshotStream("2", 1, 1, 100, 1, snapshotData)
		require.NoError(t, err)

		mu.Lock()
		assert.Equal(t, 1, callCount, "Should have called InstallSnapshot once")
		mu.Unlock()
	})

	t.Run("LargeSnapshot", func(t *testing.T) {
		snapshotData := make([]byte, 10*1024*1024) // 10MB

		var callCount int
		var mu sync.Mutex

		mockRaft2.EXPECT().InstallSnapshot(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
				mu.Lock()
				defer mu.Unlock()
				callCount++
				assert.LessOrEqual(t, args.Offset, uint64(len(snapshotData)))
				assert.NotEmpty(t, args.Data)
				// 最后一次调用应该是 Done=true
				if args.Done {
					assert.Equal(t, uint64(len(snapshotData)), args.Offset+uint64(len(args.Data)))
				}
				reply.Term = args.Term
				return nil
			}).MinTimes(1)

		start := time.Now()
		err := t1.SendInstallSnapshotStream("2", 1, 1, 100, 1, snapshotData)
		elapsed := time.Since(start)

		require.NoError(t, err)
		t.Logf("10MB snapshot transferred in %v", elapsed)

		mu.Lock()
		assert.Greater(t, callCount, 0, "Should have called InstallSnapshot at least once")
		mu.Unlock()
	})
}

// TestSendInstallSnapshot 测试通过接口发送快照（内部使用流式传输）
func TestSendInstallSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t1, err := NewTransport("127.0.0.1:0")
	require.NoError(t, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	require.NoError(t, err)
	defer t2.Close()

	mockRaft1 := api.NewMockRaftService(ctrl)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := api.NewMockRaftService(ctrl)
	t2.RegisterRaft(mockRaft2)

	require.NoError(t, t1.Start())
	require.NoError(t, t2.Start())

	peers := map[int]string{
		1: t1.Addr(),
		2: t2.Addr(),
	}
	t1.SetPeers(peers)
	t2.SetPeers(peers)

	t.Run("SendSnapshot", func(t *testing.T) {
		snapshotData := make([]byte, 5*1024*1024) // 5MB

		var callCount int
		var mu sync.Mutex

		mockRaft2.EXPECT().InstallSnapshot(gomock.Any(), gomock.Any()).
			DoAndReturn(func(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
				mu.Lock()
				defer mu.Unlock()
				callCount++
				assert.NotEmpty(t, args.Data)
				reply.Term = args.Term
				return nil
			}).MinTimes(1)

		req := &param.InstallSnapshotArgs{
			Term:              1,
			LeaderID:          1,
			LastIncludedIndex: 100,
			LastIncludedTerm:  1,
			Data:              snapshotData,
		}
		resp := &param.InstallSnapshotReply{}

		err := t1.SendInstallSnapshot("2", req, resp)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), resp.Term)

		mu.Lock()
		assert.Greater(t, callCount, 0, "Should have called InstallSnapshot at least once")
		mu.Unlock()
	})
}

// TestAppendEntriesTimeout 测试 AppendEntries 超时动态计算
func TestAppendEntriesTimeout(t *testing.T) {
	tests := []struct {
		name              string
		electionTimeout   time.Duration
		expectedTimeout   time.Duration
	}{
		{
			name:              "Default",
			electionTimeout:   0,
			expectedTimeout:   140 * time.Millisecond,
		},
		{
			name:              "200ms",
			electionTimeout:   200 * time.Millisecond,
			expectedTimeout:   140 * time.Millisecond, // 200ms * 0.7
		},
		{
			name:              "500ms",
			electionTimeout:   500 * time.Millisecond,
			expectedTimeout:   350 * time.Millisecond, // 500ms * 0.7
		},
		{
			name:              "1s",
			electionTimeout:   1 * time.Second,
			expectedTimeout:   700 * time.Millisecond, // 1s * 0.7
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trans, err := NewTransport("127.0.0.1:0")
			require.NoError(t, err)
			defer trans.Close()

			trans.SetElectionTimeout(tt.electionTimeout)
			assert.Equal(t, tt.expectedTimeout, trans.getAppendEntriesTimeout())
		})
	}
}

// TestTimeoutConstants 测试超时常量定义
func TestTimeoutConstants(t *testing.T) {
	assert.Equal(t, 300*time.Millisecond, DefaultRequestVoteTimeout)
	assert.Equal(t, 5*time.Second, DefaultClientRequestTimeout)
	assert.Equal(t, 10*time.Second, DefaultChunkSendTimeout)
	assert.Equal(t, float64(0.70), AppendEntriesTimeoutRatio)
}

// TestChunkSize 测试分块大小
func TestChunkSize(t *testing.T) {
	const chunkSize = 4 * 1024 * 1024 // 4MB

	tests := []struct {
		name     string
		size     int64
		expected int
	}{
		{"1byte", 1, 1},
		{"1KB", 1024, 1},
		{"4MB", 4 * 1024 * 1024, 1},
		{"4MB+1", 4*1024*1024 + 1, 2},
		{"10MB", 10 * 1024 * 1024, 3},
		{"16MB", 16 * 1024 * 1024, 4},
		{"100MB", 100 * 1024 * 1024, 25},
		{"1GB", 1024 * 1024 * 1024, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := (tt.size + chunkSize - 1) / chunkSize
			assert.Equal(t, tt.expected, int(chunks))
		})
	}
}

// BenchmarkSendInstallSnapshotStream 性能测试
func BenchmarkSendInstallSnapshotStream(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	t1, err := NewTransport("127.0.0.1:0")
	require.NoError(b, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	require.NoError(b, err)
	defer t2.Close()

	mockRaft1 := api.NewMockRaftService(ctrl)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := api.NewMockRaftService(ctrl)
	t2.RegisterRaft(mockRaft2)

	require.NoError(b, t1.Start())
	require.NoError(b, t2.Start())

	peers := map[int]string{
		1: t1.Addr(),
		2: t2.Addr(),
	}
	t1.SetPeers(peers)
	t2.SetPeers(peers)

	snapshotData := make([]byte, 1024*1024) // 1MB

	mockRaft2.EXPECT().InstallSnapshot(gomock.Any(), gomock.Any()).
		DoAndReturn(func(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
			reply.Term = args.Term
			return nil
		}).AnyTimes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = t1.SendInstallSnapshotStream("2", 1, 1, 100, 1, snapshotData)
	}
}

// BenchmarkSendInstallSnapshotStream_Large 大文件性能测试
func BenchmarkSendInstallSnapshotStream_Large(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	t1, err := NewTransport("127.0.0.1:0")
	require.NoError(b, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	require.NoError(b, err)
	defer t2.Close()

	mockRaft1 := api.NewMockRaftService(ctrl)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := api.NewMockRaftService(ctrl)
	t2.RegisterRaft(mockRaft2)

	require.NoError(b, t1.Start())
	require.NoError(b, t2.Start())

	peers := map[int]string{
		1: t1.Addr(),
		2: t2.Addr(),
	}
	t1.SetPeers(peers)
	t2.SetPeers(peers)

	snapshotData := make([]byte, 50*1024*1024) // 50MB

	mockRaft2.EXPECT().InstallSnapshot(gomock.Any(), gomock.Any()).
		DoAndReturn(func(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
			reply.Term = args.Term
			return nil
		}).AnyTimes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = t1.SendInstallSnapshotStream("2", 1, 1, 100, 1, snapshotData)
	}
}

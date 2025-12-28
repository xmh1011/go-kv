package raft

import (
	"encoding/json"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
)

// TestNewRaft_RecoveryState 测试 Raft 节点是否能从持久化存储中正确恢复状态。
func TestNewRaft_RecoveryState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)
	persistedState := param.HardState{CurrentTerm: 5, VotedFor: 2}
	mockStore.EXPECT().GetState().Return(persistedState, nil).Times(1)
	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)
	assert.Equal(t, persistedState.CurrentTerm, r.currentTerm, "recovered term should match")
	assert.Equal(t, int(persistedState.VotedFor), r.votedFor, "recovered votedFor should match")
}

func TestSubmit(t *testing.T) {
	type state struct {
		term  uint64
		state State
	}
	tests := []struct {
		name          string
		initialState  state
		command       string
		setupMocks    func(*storage.MockStorage, *transport.MockTransport, *storage.MockStateMachine, *sync.WaitGroup)
		expectedIndex uint64
		expectedTerm  uint64
		expectedOk    bool
	}{
		{
			name: "LeaderSuccess",
			initialState: state{
				term:  2,
				state: Leader,
			},
			command: "test-command",
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, wg *sync.WaitGroup) {
				lastLogIndex := uint64(5)
				gomock.InOrder(
					s.EXPECT().LastLogIndex().Return(lastLogIndex, nil).Times(1),
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
				)

				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				s.EXPECT().LastLogIndex().Return(lastLogIndex+1, nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).
					DoAndReturn(func(index uint64) (*param.LogEntry, error) {
						return &param.LogEntry{Term: 2, Index: index}, nil
					}).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

				wg.Add(2) // 2 peers
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						reply.Success = true
						reply.Term = args.Term
						wg.Done()
						return nil
					}).Times(2)
			},
			expectedIndex: 6,
			expectedTerm:  2,
			expectedOk:    true,
		},
		{
			name: "NotLeaderFail",
			initialState: state{
				term:  2,
				state: Follower,
			},
			command:       "test-command",
			setupMocks:    nil,
			expectedIndex: 0,
			expectedTerm:  0,
			expectedOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)
			peerIDs := []int{2, 3}

			// NewRaft init call
			if tt.initialState.state == Leader || tt.setupMocks != nil {
				mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
			}

			var wg sync.WaitGroup
			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM, &wg)
			}

			// For NotLeaderFail case, we can just create a simple struct if mocks are not needed,
			// but using NewRaft is safer to ensure consistent initialization.
			var r *Raft
			if tt.initialState.state == Follower && tt.setupMocks == nil {
				r = &Raft{state: Follower}
			} else {
				r = NewRaft(1, peerIDs, mockStore, mockSM, mockTrans, nil)
				r.currentTerm = tt.initialState.term
				r.state = tt.initialState.state
				if r.state == Leader {
					lastLogIndex := uint64(5)
					for _, peerID := range peerIDs {
						r.nextIndex[peerID] = lastLogIndex + 1
						r.matchIndex[peerID] = 0
					}
				}
			}

			index, term, ok := r.Submit(tt.command)

			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(t, tt.expectedIndex, index)
				assert.Equal(t, tt.expectedTerm, term)
			}
			wg.Wait()
		})
	}
}

func TestChangeConfig(t *testing.T) {
	type state struct {
		term             uint64
		state            State
		inJointConsensus bool
	}
	tests := []struct {
		name          string
		initialState  state
		newPeers      []int
		setupMocks    func(*storage.MockStorage, *transport.MockTransport, *storage.MockStateMachine, *sync.WaitGroup)
		expectedIndex uint64
		expectedTerm  uint64
		expectedOk    bool
	}{
		{
			name: "LeaderSuccess",
			initialState: state{
				term:             3,
				state:            Leader,
				inJointConsensus: false,
			},
			newPeers: []int{1, 2, 4, 5},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, wg *sync.WaitGroup) {
				lastLogIndex := uint64(10)
				gomock.InOrder(
					s.EXPECT().LastLogIndex().Return(lastLogIndex, nil).Times(1),
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
				)

				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				s.EXPECT().LastLogIndex().Return(lastLogIndex+1, nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).
					DoAndReturn(func(index uint64) (*param.LogEntry, error) {
						return &param.LogEntry{Term: 3, Index: index}, nil
					}).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

				// 4 unique peers: 2, 3, 4, 5 (1 is self)
				wg.Add(4)
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						reply.Success = true
						reply.Term = args.Term
						wg.Done()
						return nil
					}).Times(4)
			},
			expectedIndex: 11,
			expectedTerm:  3,
			expectedOk:    true,
		},
		{
			name: "FailWhileInJointConsensus",
			initialState: state{
				term:             3,
				state:            Leader,
				inJointConsensus: true,
			},
			newPeers:      []int{1, 2, 3},
			setupMocks:    nil,
			expectedIndex: 0,
			expectedTerm:  0,
			expectedOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)
			currentPeers := []int{2, 3}

			if tt.initialState.state == Leader && !tt.initialState.inJointConsensus {
				mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
			}

			var wg sync.WaitGroup
			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM, &wg)
			}

			var r *Raft
			if tt.initialState.inJointConsensus {
				r = &Raft{state: Leader, inJointConsensus: true}
			} else {
				r = NewRaft(1, currentPeers, mockStore, mockSM, mockTrans, nil)
				r.currentTerm = tt.initialState.term
				r.state = tt.initialState.state
				lastLogIndex := uint64(10)
				for _, peerID := range currentPeers {
					r.nextIndex[peerID] = lastLogIndex + 1
				}
				// Pre-populate nextIndex for new peers to simulate state
				for _, peerID := range tt.newPeers {
					if _, ok := r.nextIndex[peerID]; !ok {
						r.nextIndex[peerID] = lastLogIndex + 1
					}
				}
			}

			index, term, ok := r.ChangeConfig(tt.newPeers)

			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(t, tt.expectedIndex, index)
				assert.Equal(t, tt.expectedTerm, term)
				assert.True(t, r.inJointConsensus)
			}
			wg.Wait()
		})
	}
}

func TestClientRequest(t *testing.T) {
	type state struct {
		term           uint64
		state          State
		knownLeaderID  int
		clientSessions map[int64]int64
	}
	tests := []struct {
		name            string
		initialState    state
		args            *param.ClientArgs
		setupMocks      func(*storage.MockStorage, *transport.MockTransport, *storage.MockStateMachine, *Raft)
		expectedSuccess bool
		expectedResult  any
		expectedNotLdr  bool
		expectedLdrHint int
	}{
		{
			name: "LeaderProcessesCommand",
			initialState: state{
				term:  2,
				state: Leader,
			},
			args: &param.ClientArgs{ClientID: 123, SequenceNum: 1, Command: "test-command"},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, r *Raft) {
				gomock.InOrder(
					s.EXPECT().LastLogIndex().Return(uint64(5), nil).Times(1),
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
				)

				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{}, nil).AnyTimes()
				s.EXPECT().LastLogIndex().Return(uint64(6), nil).AnyTimes()

				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						reply.Success = true
						reply.Term = r.currentTerm
						return nil
					}).AnyTimes()

				sm.EXPECT().Apply(gomock.Any()).Return("success-result").AnyTimes()
			},
			expectedSuccess: true,
			expectedResult:  "success-result",
		},
		{
			name: "NotLeader",
			initialState: state{
				state:         Follower,
				knownLeaderID: 3,
			},
			args:            &param.ClientArgs{},
			setupMocks:      nil,
			expectedSuccess: false,
			expectedNotLdr:  true,
			expectedLdrHint: 3,
		},
		{
			name: "DuplicateRequest",
			initialState: state{
				state:          Leader,
				clientSessions: map[int64]int64{123: 5},
			},
			args:            &param.ClientArgs{ClientID: 123, SequenceNum: 5},
			setupMocks:      nil,
			expectedSuccess: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)
			commitChan := make(chan param.CommitEntry, 1)

			var r *Raft
			if tt.initialState.state == Leader && tt.setupMocks != nil {
				mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
				r = NewRaft(1, []int{2, 3}, mockStore, mockSM, mockTrans, commitChan)
				r.currentTerm = tt.initialState.term
				r.state = tt.initialState.state
				r.nextIndex[2] = 6
				r.nextIndex[3] = 6
			} else {
				r = &Raft{
					id:             1,
					state:          tt.initialState.state,
					knownLeaderID:  tt.initialState.knownLeaderID,
					clientSessions: tt.initialState.clientSessions,
					store:          mockStore,
					mu:             sync.Mutex{},
				}
			}

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM, r)
			}

			reply := &param.ClientReply{}

			// For async processing (LeaderProcessesCommand), we need to simulate apply notification
			if tt.name == "LeaderProcessesCommand" {
				requestDone := make(chan struct{})
				go func() {
					err := r.ClientRequest(tt.args, reply)
					assert.NoError(t, err)
					close(requestDone)
				}()

				time.Sleep(50 * time.Millisecond)
				r.mu.Lock()
				notifyChan, ok := r.notifyApply[6]
				r.mu.Unlock()
				if ok {
					notifyChan <- "success-result"
				}

				select {
				case <-requestDone:
				case <-time.After(2 * time.Second):
					t.Fatal("ClientRequest timed out")
				}
			} else {
				err := r.ClientRequest(tt.args, reply)
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectedSuccess, reply.Success)
			if tt.expectedResult != nil {
				assert.Equal(t, tt.expectedResult, reply.Result)
			}
			assert.Equal(t, tt.expectedNotLdr, reply.NotLeader)
			if tt.expectedNotLdr {
				assert.Equal(t, tt.expectedLdrHint, reply.LeaderHint)
			}
		})
	}
}

func TestHandleLinearizableRead(t *testing.T) {
	type state struct {
		term        uint64
		state       State
		commitIndex uint64
		lastApplied uint64
		knownLeader int
	}
	tests := []struct {
		name            string
		initialState    state
		cmd             param.KVCommand
		setupMocks      func(*storage.MockStorage, *transport.MockTransport, *storage.MockStateMachine, *Raft)
		triggerApply    bool // 是否需要模拟 applyLogs 唤醒
		expectedSuccess bool
		expectedResult  any
		expectedNotLdr  bool
		expectedLdrHint int
	}{
		{
			name: "Success",
			initialState: state{
				term:        1,
				state:       Leader,
				commitIndex: 10,
				lastApplied: 10,
			},
			cmd: param.KVCommand{Op: "get", Key: "testKey"},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, r *Raft) {
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 1, Index: 10}, nil).AnyTimes()
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						reply.Term = 1
						reply.Success = true
						return nil
					}).AnyTimes()
				sm.EXPECT().Get("testKey").Return("testValue", nil).Times(1)
			},
			expectedSuccess: true,
			expectedResult:  "testValue",
		},
		{
			name: "NotLeader",
			initialState: state{
				state:       Follower,
				knownLeader: 3,
			},
			cmd:             param.KVCommand{Op: "get", Key: "testKey"},
			setupMocks:      nil,
			expectedSuccess: false,
			expectedNotLdr:  true,
			expectedLdrHint: 3,
		},
		{
			name: "LeaseCheckFails",
			initialState: state{
				term:  1,
				state: Leader,
			},
			cmd: param.KVCommand{Op: "get", Key: "testKey"},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, r *Raft) {
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 1, Index: 0}, nil).AnyTimes()
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						return nil // Simulate timeout/partition (no success reply)
					}).AnyTimes()
			},
			expectedSuccess: false,
			expectedNotLdr:  true,
		},
		{
			name: "WaitsForApply",
			initialState: state{
				term:        1,
				state:       Leader,
				commitIndex: 10,
				lastApplied: 9, // Lagging behind
			},
			cmd: param.KVCommand{Op: "get", Key: "testKey"},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, r *Raft) {
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 1, Index: 10}, nil).AnyTimes()
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						reply.Term = 1
						reply.Success = true
						return nil
					}).AnyTimes()
				sm.EXPECT().Get("testKey").Return("testValue", nil).Times(1)
			},
			triggerApply:    true,
			expectedSuccess: true,
			expectedResult:  "testValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)

			var r *Raft
			if tt.initialState.state == Leader {
				mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
				r = NewRaft(1, []int{2, 3, 4, 5}, mockStore, mockSM, mockTrans, nil)
				r.currentTerm = tt.initialState.term
				r.state = tt.initialState.state
				r.commitIndex = tt.initialState.commitIndex
				r.lastApplied = tt.initialState.lastApplied
				// Initialize nextIndex to avoid panics
				for _, pid := range r.peerIDs {
					r.nextIndex[pid] = 11
				}
			} else {
				r = &Raft{
					state:         tt.initialState.state,
					knownLeaderID: tt.initialState.knownLeader,
					mu:            sync.Mutex{},
				}
			}

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM, r)
			}

			reply := &param.ClientReply{}
			readDone := make(chan struct{})

			go func() {
				err := r.handleLinearizableRead(tt.cmd, reply)
				assert.NoError(t, err)
				close(readDone)
			}()

			if tt.triggerApply {
				// Wait a bit to ensure it's blocked
				time.Sleep(10 * time.Millisecond)
				r.mu.Lock()
				r.lastApplied = 10
				r.lastAppliedCond.Broadcast()
				r.mu.Unlock()
			}

			select {
			case <-readDone:
			case <-time.After(200 * time.Millisecond):
				t.Fatal("Read operation timed out")
			}

			assert.Equal(t, tt.expectedSuccess, reply.Success)
			if tt.expectedResult != nil {
				assert.Equal(t, tt.expectedResult, reply.Result)
			}
			assert.Equal(t, tt.expectedNotLdr, reply.NotLeader)
			if tt.expectedNotLdr {
				assert.Equal(t, tt.expectedLdrHint, reply.LeaderHint)
			}
		})
	}
}

// TestClientRequest_ReadWriteBranching 测试 ClientRequest 是否能正确区分读写请求
func TestClientRequest_ReadWriteBranching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)

	// 这里必须保留带缓冲的 channel，防止死锁
	commitChan := make(chan param.CommitEntry, 10)

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, mockTrans, commitChan)
	defer r.Stop()

	r.state = Leader
	r.currentTerm = 1
	r.commitIndex = 1
	r.lastApplied = 1
	r.lastAck[2] = time.Now()
	r.lastAck[3] = time.Now()

	lastLogIndex := r.lastApplied
	for _, peerID := range r.peerIDs {
		r.nextIndex[peerID] = lastLogIndex + 1
		r.matchIndex[peerID] = 0
	}

	// 1. 测试“读”请求 (get)
	t.Run("ReadRequest", func(t *testing.T) {
		getCmd := newGetCommand(t, "key1")
		args := &param.ClientArgs{Command: getCmd}
		reply := &param.ClientReply{}

		// --- 关键修改：不要使用 gomock.Any() ---
		// confirmLeadership 会检查 prevLogIndex (即 index 1)
		// 如果使用 Any()，它会拦截后续 WriteRequest 中对 index 2 的调用！
		mockStore.EXPECT().GetEntry(uint64(1)).Return(&param.LogEntry{Term: 1, Index: 1}, nil).AnyTimes()

		mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 1
				reply.Success = true
				return nil
			}).AnyTimes()

		mockStore.EXPECT().LastLogIndex().Times(0)
		mockSM.EXPECT().Get("key1").Return("value1", nil).Times(1)

		err := r.ClientRequest(args, reply)
		assert.NoError(t, err)
		assert.True(t, reply.Success)
		assert.Equal(t, "value1", reply.Result)
	})

	// 2. 测试“写”请求 (set)
	t.Run("WriteRequest", func(t *testing.T) {
		setCmd := newSetCommand(t, "key1", "value1")
		args := &param.ClientArgs{ClientID: 123, SequenceNum: 1, Command: setCmd}
		reply := &param.ClientReply{}

		// 1. 期望同步调用
		callLastLog1 := mockStore.EXPECT().LastLogIndex().Return(uint64(1), nil).Times(1)
		callAppend := mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1).After(callLastLog1)

		// 2. 期望异步调用
		mockStore.EXPECT().LastLogIndex().Return(uint64(2), nil).AnyTimes().After(callAppend)
		mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()

		// 这里的期望现在可以正确匹配了，因为之前的 GetEntry(1) 不会拦截 GetEntry(2)
		mockStore.EXPECT().GetEntry(uint64(1)).Return(&param.LogEntry{Term: 1, Index: 1}, nil).AnyTimes()
		mockStore.EXPECT().GetEntry(uint64(2)).Return(&param.LogEntry{Command: setCmd, Term: 1, Index: 2}, nil).AnyTimes()

		mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

		mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Success = true
				reply.Term = r.currentTerm
				return nil
			}).AnyTimes()

		mockSM.EXPECT().Get(gomock.Any()).Times(0)

		// --- 运行测试 ---
		requestDone := make(chan struct{})
		go func() {
			err := r.ClientRequest(args, reply)
			assert.NoError(t, err)
			close(requestDone)
		}()

		// 等待 Raft 逻辑完成
		select {
		case <-requestDone:
			// success
		case <-time.After(1 * time.Second): // 稍微增加一点超时时间以防万一
			t.Fatal("ClientRequest goroutine did not finish. Deadlock.")
		}

		assert.True(t, reply.Success)
	})
}

// TestWaitForAppliedLog_Timeout 测试 waitForAppliedLog 函数的超时逻辑
func TestWaitForAppliedLog_Timeout(t *testing.T) {
	// --- Arrange ---
	r := &Raft{
		notifyApply: make(map[uint64]chan any),
		mu:          sync.Mutex{},
	}
	testIndex := uint64(10)
	testTimeout := 50 * time.Millisecond // 设置一个较短的超时时间用于测试

	// --- Act ---
	// 调用 waitForAppliedLog，但不向对应的 channel 发送任何通知
	startTime := time.Now()
	result, ok := r.waitForAppliedLog(testIndex, testTimeout)
	duration := time.Since(startTime)

	// --- Assert ---
	assert.False(t, ok, "Expected waitForAppliedLog to return false on timeout")
	assert.Nil(t, result, "Expected result to be nil on timeout")
	// 验证实际等待时间约等于我们设置的超时时间
	assert.GreaterOrEqual(t, duration, testTimeout, "Duration should be at least the timeout")
	assert.Less(t, duration, testTimeout*2, "Duration should not be excessively longer than the timeout") // 允许一些误差

	// 验证超时的 channel 是否已从 map 中移除，防止内存泄漏
	r.mu.Lock()
	_, exists := r.notifyApply[testIndex]
	r.mu.Unlock()
	assert.False(t, exists, "Notify channel for timed out index should be removed from the map")
}

// TestRandomizedElectionTimeout 验证随机超时是否落在 [T, 2T) 区间内。
func TestRandomizedElectionTimeout(t *testing.T) {
	// 创建一个 Raft 实例以访问其上的常量
	r := &Raft{
		electionTimeout: config.Conf.Raft.ElectionTimeout,
	}

	for i := 0; i < 100; i++ {
		timeout := r.randomizedElectionTimeout()
		assert.GreaterOrEqual(t, timeout, r.electionTimeout, "Timeout should be >= base electionTimeout")
		assert.Less(t, timeout, 2*r.electionTimeout, "Timeout should be < 2 * base electionTimeout")
	}
}

// TestRun_FollowerStartsElectionOnTimeout 测试 Follower 在超时后会启动选举。
func TestRun_FollowerStartsElectionOnTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	// 期望初始化调用
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)

	// 1. 将状态设为 Follower
	r.state = Follower
	r.currentTerm = 1
	// 2. 设置一个极短的、可预测的超时时间
	r.currentElectionTimeout = 5 * time.Millisecond
	r.electionResetEvent = time.Now()

	// 3. 期望：当选举超时时，Run() 循环会调用 startElection()。
	electionStartedChan := make(chan struct{})

	// --- Pre-Vote 阶段 ---
	// 1. 获取日志
	mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil)
	// (日志为空，不需要 GetEntry)

	// --- Real Vote 阶段 ---
	gomock.InOrder(
		// 2. 成为 Candidate，持久化状态
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 2, VotedFor: 1}).Return(nil).
			Do(func(any) {
				close(electionStartedChan) // 收到调用，发出信号
			}),
		// 3. startRealElection 还会获取日志信息
		mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil),
	)

	// 4. 选举启动后会广播投票请求 (Pre-Vote 和 Real Vote)
	mockTrans.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
			if args.PreVote {
				reply.Term = args.Term
				reply.VoteGranted = true
			}
			return nil
		}).AnyTimes()

	// 5. 启动 Run() 循环
	go r.Run()
	defer r.Stop() // 确保测试结束时停止

	// 6. 等待选举开始的信号
	select {
	case <-electionStartedChan:
		// 测试通过
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for election to start")
	}
}

// TestRun_LeaderDoesNotStartElection 测试 Leader 状态不会触发选举。
func TestRun_LeaderDoesNotStartElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)
	// 1. 将状态设为 Leader
	r.state = Leader
	// 2. 设置一个极短的超时
	r.currentElectionTimeout = 5 * time.Millisecond
	r.electionResetEvent = time.Now()

	// 3. 期望：SetState 永远不应该被调用（因为 Leader 不会开始选举）
	// 如果被调用，gomock 会自动失败测试
	mockStore.EXPECT().SetState(gomock.Any()).Return(nil).Times(0)

	// 4. 启动 Run() 循环
	go r.Run()
	defer r.Stop()

	// 5. 等待一段时间（超过选举超时）
	time.Sleep(20 * time.Millisecond)

	// 6. 验证状态仍然是 Leader
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	assert.Equal(t, Leader, state, "Leader state should not have changed")
}

// TestRun_StopShutsDownLoop 测试 Stop() 方法能正确关闭 Run() 循环。
func TestRun_StopShutsDownLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)

	// 启动 Run()
	go r.Run()

	// 立即调用 Stop()
	r.Stop()

	// 验证状态
	r.mu.Lock()
	assert.Equal(t, Dead, r.state, "State should be Dead after Stop()")
	r.mu.Unlock()

	// 验证 channel 是否关闭 (从已关闭的 channel 读取会立即返回)
	select {
	case <-r.shutdownChan:
		// 通道已按预期关闭
	default:
		t.Fatal("shutdownChan was not closed")
	}

	// 尝试再次 Stop (应该无操作)
	r.Stop()
}

// TestTimeoutResets 验证在所有必要情况下选举超时都会被重置。
func TestTimeoutResets(t *testing.T) {
	// helper function to create a raft instance for sub-tests
	newRaftForTest := func(t *testing.T) (*gomock.Controller, *storage.MockStorage, *Raft) {
		ctrl := gomock.NewController(t)
		mockStore := storage.NewMockStorage(ctrl)
		mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
		r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)
		return ctrl, mockStore, r
	}

	// 1. 测试收到心跳时 (handleTermAndHeartbeat)
	t.Run("OnHeartbeat", func(t *testing.T) {
		ctrl, _, r := newRaftForTest(t)
		defer ctrl.Finish()

		r.state = Follower
		r.currentTerm = 5
		r.currentElectionTimeout = 12345 // 设置一个已知的哨兵值

		// 模拟一个合法的心跳 RPC
		args := &param.AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 0} // 确保 PrevLogIndex 为 0 以匹配
		reply := &param.AppendEntriesReply{}

		// checkLogConsistency 会被调用，但由于 PrevLogIndex 为 0，它会直接返回 true
		err := r.AppendEntries(args, reply)
		assert.NoError(t, err, "AppendEntries should not return error")

		assert.True(t, reply.Success, "Heartbeat should have been accepted")
		assert.NotEqual(t, 12345, r.currentElectionTimeout, "Timeout should be reset on heartbeat")
	})

	// 2. 测试投票时 (grantVote)
	t.Run("OnGrantVote", func(t *testing.T) {
		ctrl, mockStore, r := newRaftForTest(t)
		defer ctrl.Finish()

		// 模拟 grantVote 所需的调用
		mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil)
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 5, VotedFor: 2}).Return(nil)

		r.state = Follower
		r.currentTerm = 5
		r.votedFor = -1                  // 确保可以投票
		r.currentElectionTimeout = 12345 // 哨兵值

		// 模拟一个合法的投票 RPC
		args := &param.RequestVoteArgs{Term: 5, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0}
		reply := &param.RequestVoteReply{}
		err := r.RequestVote(args, reply)
		assert.NoError(t, err, "RequestVote should not return error")

		assert.True(t, reply.VoteGranted, "Vote should have been granted")
		assert.NotEqual(t, 12345, r.currentElectionTimeout, "Timeout should be reset on grantVote")
	})

	// 3. 测试成为 Follower 时 (becomeFollower)
	t.Run("OnBecomeFollower", func(t *testing.T) {
		ctrl, mockStore, r := newRaftForTest(t)
		defer ctrl.Finish()

		// 模拟 becomeFollower 时的 SetState
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil)

		r.state = Candidate
		r.currentTerm = 5
		r.currentElectionTimeout = 12345 // 哨兵值

		// 手动调用 (模拟在 RPC 处理器中被调用)
		r.mu.Lock()
		err := r.becomeFollower(6)
		assert.NoError(t, err)
		r.mu.Unlock()

		assert.Equal(t, Follower, r.state)
		assert.Equal(t, uint64(6), r.currentTerm)
		assert.NotEqual(t, 12345, r.currentElectionTimeout, "Timeout should be reset on becomeFollower")
	})
}

// helper function 构造一个 "get" 命令
func newGetCommand(t *testing.T, key string) []byte {
	cmd := param.KVCommand{Op: "get", Key: key}
	b, err := json.Marshal(cmd)
	assert.NoError(t, err)
	return b
}

// helper function 构造一个 "set" 命令
func newSetCommand(t *testing.T, key, value string) []byte {
	cmd := param.KVCommand{Op: "set", Key: key, Value: value}
	b, err := json.Marshal(cmd)
	assert.NoError(t, err)
	return b
}

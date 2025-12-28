package raft

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
)

func TestStartElection(t *testing.T) {
	type state struct {
		term     uint64
		votedFor int
		state    State
	}

	tests := []struct {
		name         string
		initialState state
		peerIds      []int

		// setupMocks 用于设置 Mock 对象的期望行为。
		// stateChangeCh 用于从 Mock 回调中通知测试主线程（例如：“已进入 Candidate 状态”）。
		// wg 用于同步异步操作（例如等待心跳发送完成）。
		setupMocks func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, stateChangeCh chan string, wg *sync.WaitGroup)

		// afterStart 在 r.startElection() 启动后执行。
		// 用于模拟选举过程中的外部事件（例如：在选举中途收到 AppendEntries）。
		afterStart func(t *testing.T, r *Raft, stateChangeCh chan string)

		// verify 用于验证最终状态。
		verify func(t *testing.T, r *Raft, stateChangeCh chan string, wg *sync.WaitGroup)
	}{
		{
			name: "WinsElection",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			peerIds: []int{2, 3},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, stateChangeCh chan string, wg *sync.WaitGroup) {
				// 1. Pre-Vote 阶段
				s.EXPECT().LastLogIndex().Return(uint64(10), nil)
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

				// 2. 正式选举阶段
				gomock.InOrder(
					// 成为 Candidate
					s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
					// 获取日志
					s.EXPECT().LastLogIndex().Return(uint64(10), nil),
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
				)

				// Mock RPCs: Pre-Vote 和 RequestVote 都成功
				tr.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
						reply.Term = args.Term
						reply.VoteGranted = true
						return nil
					}).AnyTimes()

				// Mock for initLeaderState
				s.EXPECT().LastLogIndex().Return(uint64(10), nil)

				// Mock for Heartbeat
				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				s.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

				// 设置 WaitGroup 等待 2 个 Peer 的心跳
				wg.Add(2)
				var doneCounter int32
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
						reply.Success = true
						reply.Term = args.Term
						// 确保每个 peer 只 Done 一次，防止多次心跳导致 panic
						if atomic.AddInt32(&doneCounter, 1) <= 2 {
							wg.Done()
						}
						return nil
					}).AnyTimes()
			},
			verify: func(t *testing.T, r *Raft, stateChangeCh chan string, wg *sync.WaitGroup) {
				// 等待变为 Leader
				success := false
				for i := 0; i < 20; i++ {
					r.mu.Lock()
					if r.state == Leader {
						success = true
						r.mu.Unlock()
						break
					}
					r.mu.Unlock()
					time.Sleep(50 * time.Millisecond)
				}
				assert.True(t, success, "Node failed to become leader")

				// 等待心跳发送完成
				wg.Wait()
			},
		},
		{
			name: "LosesElection",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			peerIds: []int{2, 3},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, stateChangeCh chan string, wg *sync.WaitGroup) {
				// 1. Pre-Vote 阶段
				s.EXPECT().LastLogIndex().Return(uint64(10), nil)
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

				// 2. 正式选举阶段
				gomock.InOrder(
					// 成为 Candidate
					s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
					// 获取日志
					s.EXPECT().LastLogIndex().Return(uint64(10), nil),
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					// 选举超时，退回 Follower
					s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).
						Do(func(any) { stateChangeCh <- "reverted" }).Return(nil),
				)

				// Mock RPCs
				tr.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
						if args.PreVote {
							reply.Term = args.Term
							reply.VoteGranted = true
						} else {
							reply.Term = args.Term
							reply.VoteGranted = false
						}
						return nil
					}).AnyTimes()
			},
			verify: func(t *testing.T, r *Raft, stateChangeCh chan string, wg *sync.WaitGroup) {
				select {
				case msg := <-stateChangeCh:
					assert.Equal(t, "reverted", msg)
				case <-time.After(2 * time.Second):
					t.Fatal("timed out waiting for node to revert to follower")
				}

				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, Follower, r.state, "expected state to revert to Follower after timeout")
			},
		},
		{
			name: "StepsDownOnAppendEntries",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			peerIds: []int{2, 3},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, stateChangeCh chan string, wg *sync.WaitGroup) {
				// 1. Pre-Vote 阶段
				s.EXPECT().LastLogIndex().Return(uint64(10), nil)
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

				// 模拟选举开始
				gomock.InOrder(
					// 成为 Candidate，持久化状态
					s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Do(func(_ any) {
						stateChangeCh <- "candidate"
					}).Return(nil),
					// 获取日志信息
					s.EXPECT().LastLogIndex().Return(uint64(10), nil),
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					// 期望收到 AE 后，调用 becomeFollower 持久化新状态
					s.EXPECT().SetState(param.HardState{CurrentTerm: 7, VotedFor: math.MaxUint64}).Return(nil),
				)

				// 期望发出投票请求 (Pre-Vote 成功)
				tr.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
						if args.PreVote {
							reply.Term = args.Term
							reply.VoteGranted = true
						}
						return nil
					}).AnyTimes()
			},
			afterStart: func(t *testing.T, r *Raft, stateChangeCh chan string) {
				// 等待进入 Candidate 状态
				select {
				case msg := <-stateChangeCh:
					assert.Equal(t, "candidate", msg)
				case <-time.After(1 * time.Second):
					t.Fatal("timed out waiting for candidate state")
				}

				// 模拟收到来自更高任期 Leader 的 AppendEntries
				argsAE := &param.AppendEntriesArgs{Term: 7, LeaderID: 2, PrevLogIndex: 0}
				replyAE := &param.AppendEntriesReply{}

				err := r.AppendEntries(argsAE, replyAE)
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, r *Raft, stateChangeCh chan string, wg *sync.WaitGroup) {
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, Follower, r.state, "State should revert to Follower")
				assert.Equal(t, uint64(7), r.currentTerm, "Term should be updated to 7")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)

			// 通用初始化 Mock (NewRaft 会调用)
			mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

			// 用于同步的工具
			stateChangeCh := make(chan string, 10)
			var wg sync.WaitGroup

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM, stateChangeCh, &wg)
			}

			// 创建 Raft 实例
			commitChan := make(chan param.CommitEntry, 10)
			r := NewRaft(1, tt.peerIds, mockStore, mockSM, mockTrans, commitChan)
			defer r.Stop()

			// 设置初始状态
			r.state = tt.initialState.state
			r.currentTerm = tt.initialState.term
			r.votedFor = tt.initialState.votedFor

			// 启动选举
			go r.startElection()

			// 执行中间干预
			if tt.afterStart != nil {
				tt.afterStart(t, r, stateChangeCh)
			}

			// 验证结果
			if tt.verify != nil {
				tt.verify(t, r, stateChangeCh, &wg)
			}
		})
	}
}

func TestRequestVote(t *testing.T) {
	type state struct {
		term     uint64
		votedFor int
		state    State
	}

	tests := []struct {
		name          string
		initialState  state
		args          *param.RequestVoteArgs
		mockSetup     func(*storage.MockStorage)
		expectedReply *param.RequestVoteReply
		expectedState state
	}{
		{
			name: "GrantsVote",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			args: param.NewRequestVoteArgs(5, 1, uint64(10), 5, false),
			mockSetup: func(mockStore *storage.MockStorage) {
				// 期望 isLogUpToDate 会检查本地日志
				mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
				mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
				// 期望 grantVote 会持久化投票结果
				mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 5, VotedFor: 1}).Return(nil)
			},
			expectedReply: &param.RequestVoteReply{
				Term:        5,
				VoteGranted: true,
			},
			expectedState: state{
				term:     5,
				votedFor: 1,
				state:    Follower,
			},
		},
		{
			name: "RejectsForStaleTerm",
			initialState: state{
				term:     6,
				votedFor: -1,
				state:    Follower,
			},
			args: param.NewRequestVoteArgs(5, 1, uint64(10), 5, false),
			mockSetup: func(mockStore *storage.MockStorage) {
				// 不需要任何 mock，因为任期检查在访问存储之前
			},
			expectedReply: &param.RequestVoteReply{
				Term:        6,
				VoteGranted: false,
			},
			expectedState: state{
				term:     6,
				votedFor: -1,
				state:    Follower,
			},
		},
		{
			name: "RejectsForStaleLog",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			// 候选人日志只到索引 9，任期 5
			args: param.NewRequestVoteArgs(5, 1, uint64(9), 5, false),
			mockSetup: func(mockStore *storage.MockStorage) {
				// 期望 isLogUpToDate 会检查本地日志，我们让本地日志更新（索引 10，任期 5）
				mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
				mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
			},
			expectedReply: &param.RequestVoteReply{
				Term:        5,
				VoteGranted: false,
			},
			expectedState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
		},
		{
			name: "StepsDownOnHigherTerm",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Leader, // 假设当前是 Leader，收到更高任期请求
			},
			args: param.NewRequestVoteArgs(6, 1, uint64(10), 5, false),
			mockSetup: func(mockStore *storage.MockStorage) {
				gomock.InOrder(
					// 期望1: becomeFollower 会持久化新状态 (任期6, votedFor: -1)
					mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil),
					// 期望2: isLogUpToDate 检查日志（假设通过）
					mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
					mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					// 期望3: grantVote 会持久化投票结果 (任期6, votedFor: 1)
					mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
				)
			},
			expectedReply: &param.RequestVoteReply{
				Term:        6,
				VoteGranted: true,
			},
			expectedState: state{
				term:     6,
				votedFor: 1,
				state:    Follower,
			},
		},
		{
			name: "PreVote_RejectsForStaleTerm",
			initialState: state{
				term:     6,
				votedFor: -1,
				state:    Follower,
			},
			args: param.NewRequestVoteArgs(5, 1, uint64(10), 5, true), // PreVote, Term 5 < 6
			mockSetup: func(mockStore *storage.MockStorage) {
				// No mocks needed
			},
			expectedReply: &param.RequestVoteReply{
				Term:        6,
				VoteGranted: false,
			},
			expectedState: state{
				term:     6,
				votedFor: -1,
				state:    Follower,
			},
		},
		{
			name: "PreVote_RejectsForStaleLog",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			args: param.NewRequestVoteArgs(6, 1, uint64(9), 5, true), // PreVote, Term 6 > 5, but log stale
			mockSetup: func(mockStore *storage.MockStorage) {
				mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
				mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
			},
			expectedReply: &param.RequestVoteReply{
				Term:        5,
				VoteGranted: false,
			},
			expectedState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
		},
		{
			name: "PreVote_GrantsVote",
			initialState: state{
				term:     5,
				votedFor: -1,
				state:    Follower,
			},
			args: param.NewRequestVoteArgs(6, 1, uint64(10), 5, true), // PreVote, Term 6 > 5, log ok
			mockSetup: func(mockStore *storage.MockStorage) {
				mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
				mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
				// PreVote DOES NOT persist vote
			},
			expectedReply: &param.RequestVoteReply{
				Term:        5,
				VoteGranted: true,
			},
			expectedState: state{
				term:     5,
				votedFor: -1, // Should not change
				state:    Follower,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)

			if tt.mockSetup != nil {
				tt.mockSetup(mockStore)
			}

			r := &Raft{
				id:               2,
				currentTerm:      tt.initialState.term,
				votedFor:         tt.initialState.votedFor,
				state:            tt.initialState.state,
				store:            mockStore,
				mu:               sync.Mutex{},
				electionTimeout:  config.Conf.Raft.ElectionTimeout,
				heartbeatTimeout: config.Conf.Raft.HeartbeatTimeout,
			}

			reply := &param.RequestVoteReply{}
			err := r.RequestVote(tt.args, reply)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedReply.Term, reply.Term)
			assert.Equal(t, tt.expectedReply.VoteGranted, reply.VoteGranted)

			// 验证最终状态
			assert.Equal(t, tt.expectedState.term, r.currentTerm)
			assert.Equal(t, tt.expectedState.votedFor, r.votedFor)
			assert.Equal(t, tt.expectedState.state, r.state)
		})
	}
}

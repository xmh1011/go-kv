package raft

import (
	"errors"
	"math"
	"strconv"
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

func TestDetermineReplicationAction(t *testing.T) {
	type state struct {
		state     State
		nextIndex uint64
	}
	tests := []struct {
		name           string
		initialState   state
		setupMocks     func(*storage.MockStorage)
		expectedAction replicationAction
	}{
		{
			name: "ShouldSendSnapshot",
			initialState: state{
				state:     Leader,
				nextIndex: 5,
			},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().FirstLogIndex().Return(uint64(10), nil).Times(1)
			},
			expectedAction: actionSendSnapshot,
		},
		{
			name: "ShouldSendLogs",
			initialState: state{
				state:     Leader,
				nextIndex: 10,
			},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().FirstLogIndex().Return(uint64(5), nil).Times(1)
			},
			expectedAction: actionSendLogs,
		},
		{
			name: "NotLeader",
			initialState: state{
				state: Follower,
			},
			setupMocks:     nil,
			expectedAction: actionDoNothing,
		},
		{
			name: "GetFirstLogIndexFails",
			initialState: state{
				state:     Leader,
				nextIndex: 5,
			},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().FirstLogIndex().Return(uint64(0), errors.New("storage error")).Times(1)
			},
			expectedAction: actionSendLogs, // Fallback
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore)
			}

			r := &Raft{
				id:        1,
				state:     tt.initialState.state,
				store:     mockStore,
				nextIndex: map[int]uint64{2: tt.initialState.nextIndex},
			}

			action := r.determineReplicationAction(2)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestPrepareAppendEntriesArgs(t *testing.T) {
	type state struct {
		term        uint64
		nextIndex   uint64
		commitIndex uint64
	}
	tests := []struct {
		name          string
		initialState  state
		setupMocks    func(*storage.MockStorage)
		expectedError bool
		verifyArgs    func(*testing.T, *param.AppendEntriesArgs)
	}{
		{
			name: "Success",
			initialState: state{
				term:        5,
				nextIndex:   11,
				commitIndex: 10,
			},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
				s.EXPECT().LastLogIndex().Return(uint64(12), nil)
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11}, nil)
				s.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12}, nil)
			},
			expectedError: false,
			verifyArgs: func(t *testing.T, args *param.AppendEntriesArgs) {
				assert.Equal(t, uint64(5), args.Term)
				assert.Equal(t, uint64(10), args.PrevLogIndex)
				assert.Equal(t, uint64(5), args.PrevLogTerm)
				assert.Equal(t, 2, len(args.Entries))
				assert.Equal(t, uint64(10), args.LeaderCommit)
			},
		},
		{
			name: "GetLogTermFails",
			initialState: state{
				term:      5,
				nextIndex: 11,
			},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().GetEntry(uint64(10)).Return(nil, errors.New("read error"))
			},
			expectedError: true,
		},
		{
			name: "GetEntryFails",
			initialState: state{
				term:      5,
				nextIndex: 11,
			},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
				s.EXPECT().LastLogIndex().Return(uint64(12), nil)
				s.EXPECT().GetEntry(uint64(11)).Return(nil, errors.New("read error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore)
			}

			r := &Raft{
				id:          1,
				currentTerm: tt.initialState.term,
				commitIndex: tt.initialState.commitIndex,
				store:       mockStore,
				nextIndex:   map[int]uint64{2: tt.initialState.nextIndex},
			}

			args, err := r.prepareAppendEntriesArgs(2)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.verifyArgs(t, args)
			}
		})
	}
}

func TestUpdateCommitIndex(t *testing.T) {
	type state struct {
		term        uint64
		commitIndex uint64
		matchIndex  map[int]uint64
	}
	tests := []struct {
		name          string
		initialState  state
		setupMocks    func(*storage.MockStorage, *storage.MockStateMachine)
		expectedIndex uint64
	}{
		{
			name: "AdvancesCommitIndex",
			initialState: state{
				term:        5,
				commitIndex: 10,
				matchIndex:  map[int]uint64{1: 12, 2: 12, 3: 12}, // Majority at 12
			},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine) {
				s.EXPECT().LastLogIndex().Return(uint64(12), nil)
				s.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12}, nil).AnyTimes()
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11}, nil).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()
			},
			expectedIndex: 12,
		},
		{
			name: "NoMajority",
			initialState: state{
				term:        5,
				commitIndex: 10,
				matchIndex:  map[int]uint64{1: 12, 2: 10, 3: 10}, // Only 1 at 12
			},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine) {
				s.EXPECT().LastLogIndex().Return(uint64(12), nil)
			},
			expectedIndex: 10,
		},
		{
			name: "StaleTermLog",
			initialState: state{
				term:        6, // Current term 6
				commitIndex: 10,
				matchIndex:  map[int]uint64{1: 12, 2: 12, 3: 12},
			},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine) {
				s.EXPECT().LastLogIndex().Return(uint64(12), nil)
				s.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12}, nil)
			},
			expectedIndex: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockSM)
			}

			r := &Raft{
				id:           1,
				peerIDs:      []int{2, 3},
				currentTerm:  tt.initialState.term,
				commitIndex:  tt.initialState.commitIndex,
				matchIndex:   tt.initialState.matchIndex,
				store:        mockStore,
				lastApplied:  tt.initialState.commitIndex,
				stateMachine: mockSM,
				notifyApply:  make(map[uint64]chan any),
				mu:           sync.Mutex{},
			}
			r.lastAppliedCond = sync.NewCond(&r.mu)

			r.updateCommitIndex()
			time.Sleep(10 * time.Millisecond)
			assert.Equal(t, tt.expectedIndex, r.commitIndex)
		})
	}
}

func TestApplyLogs(t *testing.T) {
	tests := []struct {
		name            string
		commitIndex     uint64
		lastApplied     uint64
		snapshotThresh  int
		setupMocks      func(*storage.MockStorage, *storage.MockStateMachine, chan struct{})
		expectedApplied uint64
		expectSnapshot  bool
		verify          func(*testing.T, chan struct{})
	}{
		{
			name:           "AppliesEntries",
			commitIndex:    12,
			lastApplied:    10,
			snapshotThresh: -1, // Disabled
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11, Command: "cmd1"}, nil)
				s.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12, Command: "cmd2"}, nil)
				sm.EXPECT().Apply(gomock.Any()).Return("res1").Times(1)
				sm.EXPECT().Apply(gomock.Any()).Return("res2").Times(1)
			},
			expectedApplied: 12,
			expectSnapshot:  false,
		},
		{
			name:           "TriggersSnapshot",
			commitIndex:    11,
			lastApplied:    10,
			snapshotThresh: 100,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11, Command: "cmd1"}, nil)
				sm.EXPECT().Apply(gomock.Any()).Return("res1")
				s.EXPECT().LogSize().Return(101, nil)
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11}, nil)
				sm.EXPECT().GetSnapshot().Return([]byte("snap"), nil)
				s.EXPECT().SaveSnapshot(gomock.Any()).Return(nil)
				s.EXPECT().CompactLog(uint64(11)).Return(nil).Do(func(_ uint64) { close(done) })
			},
			expectedApplied: 11,
			expectSnapshot:  true,
			verify: func(t *testing.T, done chan struct{}) {
				select {
				case <-done:
				case <-time.After(1 * time.Second):
					t.Fatal("timeout waiting for snapshot async operations")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)
			done := make(chan struct{})

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockSM, done)
			}

			r := &Raft{
				id:                1,
				commitIndex:       tt.commitIndex,
				lastApplied:       tt.lastApplied,
				snapshotThreshold: tt.snapshotThresh,
				store:             mockStore,
				stateMachine:      mockSM,
				notifyApply:       make(map[uint64]chan any),
				mu:                sync.Mutex{},
			}
			r.lastAppliedCond = sync.NewCond(&r.mu)

			r.applyLogs()

			if tt.verify != nil {
				tt.verify(t, done)
			}

			r.mu.Lock()
			assert.Equal(t, tt.expectedApplied, r.lastApplied)
			if tt.expectSnapshot {
				// The flag is transient. After the async op is done, it's set to false.
				// The fact that `verify` passed (waited for done) is proof it was triggered.
				// So we don't assert on `isSnapshotting` here.
			}
			r.mu.Unlock()
		})
	}
}

func TestApplyConfigChange(t *testing.T) {
	type state struct {
		state       State
		inJoint     bool
		peerIDs     []int
		newPeerIDs  []int
		currentTerm uint64
	}
	tests := []struct {
		name         string
		initialState state
		cmd          param.ConfigChangeCommand
		setupMocks   func(*storage.MockStorage)
		verify       func(*testing.T, *Raft)
	}{
		{
			name: "EnterJointConsensus",
			initialState: state{
				state:   Leader,
				inJoint: false,
				peerIDs: []int{1, 2},
			},
			cmd: param.ConfigChangeCommand{NewPeerIDs: []int{1, 2, 3}},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().LastLogIndex().Return(uint64(10), nil).Times(2)
				s.EXPECT().AppendEntries(gomock.Any()).Return(nil)
			},
			verify: func(t *testing.T, r *Raft) {
				assert.True(t, r.inJointConsensus)
				assert.Equal(t, []int{1, 2, 3}, r.newPeerIDs)
			},
		},
		{
			name: "LeaveJointConsensus",
			initialState: state{
				state:      Leader,
				inJoint:    true,
				peerIDs:    []int{1, 2},
				newPeerIDs: []int{1, 2, 3},
			},
			cmd:        param.ConfigChangeCommand{NewPeerIDs: []int{1, 2, 3}},
			setupMocks: nil,
			verify: func(t *testing.T, r *Raft) {
				assert.False(t, r.inJointConsensus)
				assert.Equal(t, []int{1, 2, 3}, r.peerIDs)
				assert.Nil(t, r.newPeerIDs)
				assert.Equal(t, Leader, r.state)
			},
		},
		{
			name: "LeaderStepsDown",
			initialState: state{
				state:       Leader,
				inJoint:     true,
				peerIDs:     []int{1, 2},
				newPeerIDs:  []int{2, 3},
				currentTerm: 5,
			},
			cmd: param.ConfigChangeCommand{NewPeerIDs: []int{2, 3}},
			setupMocks: func(s *storage.MockStorage) {
				s.EXPECT().SetState(param.HardState{CurrentTerm: 5, VotedFor: math.MaxUint64}).Return(nil)
			},
			verify: func(t *testing.T, r *Raft) {
				assert.False(t, r.inJointConsensus)
				assert.Equal(t, Follower, r.state)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore)
			}

			r := &Raft{
				id:               1,
				state:            tt.initialState.state,
				inJointConsensus: tt.initialState.inJoint,
				peerIDs:          tt.initialState.peerIDs,
				newPeerIDs:       tt.initialState.newPeerIDs,
				currentTerm:      tt.initialState.currentTerm,
				store:            mockStore,
				nextIndex:        make(map[int]uint64),
				matchIndex:       make(map[int]uint64),
				mu:               sync.Mutex{},
				electionTimeout:  config.Conf.Raft.ElectionTimeout,
				heartbeatTimeout: config.Conf.Raft.HeartbeatTimeout,
			}

			r.applyConfigChange(tt.cmd, 10)

			if tt.verify != nil {
				tt.verify(t, r)
			}
		})
	}
}

func TestReplicateLogsToPeer(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(*storage.MockStorage, *transport.MockTransport, *storage.MockStateMachine, *Raft)
		verify     func(*testing.T, *Raft, chan param.CommitEntry)
	}{
		{
			name: "Success",
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, r *Raft) {
				peerID := 2
				r.nextIndex[peerID] = 11
				r.matchIndex[peerID] = 10
				r.commitIndex = 10
				r.lastApplied = 10

				gomock.InOrder(
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1),
					s.EXPECT().LastLogIndex().Return(uint64(11), nil).Times(1),
					s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "test", Term: 5, Index: 11}, nil).Times(1),
					tr.EXPECT().SendAppendEntries(strconv.Itoa(peerID), gomock.Any(), gomock.Any()).
						DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
							reply.Term = 5
							reply.Success = true
							return nil
						}).Times(1),
				)

				s.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11}, nil).AnyTimes()
				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()
			},
			verify: func(t *testing.T, r *Raft, commitChan chan param.CommitEntry) {
				select {
				case entry := <-commitChan:
					assert.Equal(t, uint64(11), entry.Index)
				case <-time.After(500 * time.Millisecond):
					t.Fatal("timed out waiting for log to be applied")
				}

				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, uint64(12), r.nextIndex[2])
				assert.Equal(t, uint64(11), r.matchIndex[2])
				assert.Equal(t, uint64(11), r.commitIndex)
			},
		},
		{
			name: "FollowerRejects",
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine, r *Raft) {
				peerID := 2
				r.nextIndex[peerID] = 11

				s.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).
					DoAndReturn(func(index uint64) (*param.LogEntry, error) {
						return &param.LogEntry{Term: 5, Index: index}, nil
					}).AnyTimes()
				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

				gomock.InOrder(
					tr.EXPECT().SendAppendEntries(strconv.Itoa(peerID), gomock.Any(), gomock.Any()).
						DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
							reply.Term = 5
							reply.Success = false
							reply.ConflictIndex = 8
							return nil
						}).Times(1),
					tr.EXPECT().SendAppendEntries(strconv.Itoa(peerID), gomock.Any(), gomock.Any()).
						DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
							reply.Term = 5
							reply.Success = true
							return nil
						}).AnyTimes(),
				)
			},
			verify: func(t *testing.T, r *Raft, commitChan chan param.CommitEntry) {
				time.Sleep(100 * time.Millisecond)
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, uint64(12), r.nextIndex[2])
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
			commitChan := make(chan param.CommitEntry, 1)

			mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
			r := NewRaft(1, []int{2, 3, 4}, mockStore, mockSM, mockTrans, commitChan) // Use 4 nodes to prevent accidental majority
			defer r.Stop()
			r.state = Leader
			r.currentTerm = 5

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM, r)
			}

			r.replicateLogsToPeer(2)

			if tt.verify != nil {
				tt.verify(t, r, commitChan)
			}
		})
	}
}

func TestAppendEntries(t *testing.T) {
	type state struct {
		term        uint64
		lastApplied uint64
	}
	tests := []struct {
		name            string
		initialState    state
		args            *param.AppendEntriesArgs
		setupMocks      func(*storage.MockStorage, *storage.MockStateMachine, *Raft)
		expectedSuccess bool
		expectedTerm    uint64
		expectedIndex   uint64 // for commit check
		verify          func(*testing.T, *Raft, *param.AppendEntriesReply, chan param.CommitEntry)
	}{
		{
			name:         "RejectStaleTerm",
			initialState: state{term: 5},
			args:         &param.AppendEntriesArgs{Term: 4},
			setupMocks:   nil,
			verify: func(t *testing.T, r *Raft, reply *param.AppendEntriesReply, _ chan param.CommitEntry) {
				assert.False(t, reply.Success)
				assert.Equal(t, uint64(5), reply.Term)
			},
		},
		{
			name:         "RejectInconsistentLog",
			initialState: state{term: 5},
			args:         &param.AppendEntriesArgs{Term: 5, PrevLogIndex: 10, PrevLogTerm: 4},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, r *Raft) {
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
			},
			verify: func(t *testing.T, r *Raft, reply *param.AppendEntriesReply, _ chan param.CommitEntry) {
				assert.False(t, reply.Success)
			},
		},
		{
			name:         "SuccessAppend",
			initialState: state{term: 5, lastApplied: 10},
			args: &param.AppendEntriesArgs{
				Term:         5,
				PrevLogIndex: 10,
				PrevLogTerm:  5,
				Entries:      []param.LogEntry{{Command: "cmd1", Term: 5, Index: 11}},
				LeaderCommit: 11,
			},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, r *Raft) {
				gomock.InOrder(
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					s.EXPECT().TruncateLog(uint64(11)).Return(nil),
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil),
					s.EXPECT().LastLogIndex().Return(uint64(11), nil),
				)
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "cmd1", Term: 5, Index: 11}, nil)
				sm.EXPECT().Apply(gomock.Any()).Return("success")
			},
			verify: func(t *testing.T, r *Raft, reply *param.AppendEntriesReply, commitChan chan param.CommitEntry) {
				assert.True(t, reply.Success)
				select {
				case entry := <-commitChan:
					assert.Equal(t, uint64(11), entry.Index)
				case <-time.After(100 * time.Millisecond):
					t.Fatal("timed out waiting for entry to be applied")
				}
				assert.Equal(t, uint64(11), r.commitIndex)
			},
		},
		{
			name:         "ConflictLongerLog",
			initialState: state{term: 5},
			args: &param.AppendEntriesArgs{
				Term:         5,
				LeaderID:     1,
				PrevLogIndex: 10,
				PrevLogTerm:  5,
				Entries:      []param.LogEntry{{Command: "cmd11", Term: 5, Index: 11}},
			},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, r *Raft) {
				gomock.InOrder(
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					s.EXPECT().TruncateLog(uint64(11)).Return(nil),
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil),
					s.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes(),
				)
			},
			verify: func(t *testing.T, r *Raft, reply *param.AppendEntriesReply, _ chan param.CommitEntry) {
				assert.True(t, reply.Success)
			},
		},
		{
			name:         "ConflictTermMismatch",
			initialState: state{term: 5},
			args: &param.AppendEntriesArgs{
				Term:         5,
				LeaderID:     1,
				PrevLogIndex: 10,
				PrevLogTerm:  4,
				Entries:      []param.LogEntry{{Command: "cmd11", Term: 5, Index: 11}},
			},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, r *Raft) {
				s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1)
			},
			verify: func(t *testing.T, r *Raft, reply *param.AppendEntriesReply, _ chan param.CommitEntry) {
				assert.False(t, reply.Success)
				assert.Equal(t, uint64(5), reply.Term)
				assert.Equal(t, uint64(5), reply.ConflictTerm)
				assert.Equal(t, uint64(10), reply.ConflictIndex)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)
			commitChan := make(chan param.CommitEntry, 1)

			r := &Raft{
				id:               2,
				currentTerm:      tt.initialState.term,
				store:            mockStore,
				stateMachine:     mockSM,
				commitChan:       commitChan,
				lastApplied:      tt.initialState.lastApplied,
				mu:               sync.Mutex{},
				electionTimeout:  config.Conf.Raft.ElectionTimeout,
				heartbeatTimeout: config.Conf.Raft.HeartbeatTimeout,
			}
			r.lastAppliedCond = sync.NewCond(&r.mu)

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockSM, r)
			}

			reply := &param.AppendEntriesReply{}
			err := r.AppendEntries(tt.args, reply)
			assert.NoError(t, err)

			if tt.verify != nil {
				tt.verify(t, r, reply, commitChan)
			}
		})
	}
}

func TestIsReplicatedByMajority(t *testing.T) {
	tests := []struct {
		name             string
		peerIDs          []int
		newPeerIDs       []int
		matchIndex       map[int]uint64
		inJointConsensus bool
		checkIndex       uint64
		expected         bool
	}{
		{
			name:    "SimpleMajorityMet",
			peerIDs: []int{2, 3, 4, 5},
			matchIndex: map[int]uint64{
				1: 10, 2: 10, 3: 10, 4: 9, 5: 9,
			},
			checkIndex: 10,
			expected:   true,
		},
		{
			name:    "SimpleMajorityNotMet",
			peerIDs: []int{2, 3, 4, 5},
			matchIndex: map[int]uint64{
				1: 10, 2: 10, 3: 9, 4: 9, 5: 9,
			},
			checkIndex: 10,
			expected:   false,
		},
		{
			name:             "JointConsensusMajorityMet",
			peerIDs:          []int{2, 3},
			newPeerIDs:       []int{3, 4, 5},
			inJointConsensus: true,
			matchIndex: map[int]uint64{
				1: 10, 2: 10,
				3: 10, 4: 10,
				5: 9,
			},
			checkIndex: 10,
			expected:   true,
		},
		{
			name:             "JointConsensusMajorityNotMet",
			peerIDs:          []int{2, 3},
			newPeerIDs:       []int{3, 4, 5},
			inJointConsensus: true,
			matchIndex: map[int]uint64{
				1: 10, 2: 10,
				3: 10, 4: 9, 5: 9,
			},
			checkIndex: 10,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Raft{
				id:               1,
				peerIDs:          tt.peerIDs,
				newPeerIDs:       tt.newPeerIDs,
				matchIndex:       tt.matchIndex,
				inJointConsensus: tt.inJointConsensus,
			}
			assert.Equal(t, tt.expected, r.isReplicatedByMajority(tt.checkIndex))
		})
	}
}

func TestProcessAppendEntriesReply(t *testing.T) {
	type state struct {
		term  uint64
		state State
	}
	tests := []struct {
		name         string
		initialState state
		reply        *param.AppendEntriesReply
		setupMocks   func(*storage.MockStorage, *transport.MockTransport, *storage.MockStateMachine)
		verify       func(*testing.T, *Raft, time.Time)
	}{
		{
			name:         "StepsDownOnHigherTerm",
			initialState: state{term: 5, state: Leader},
			reply:        &param.AppendEntriesReply{Term: 6, Success: false},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine) {
				s.EXPECT().GetState().Return(param.HardState{CurrentTerm: 5}, nil).Times(1)
				s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil).Times(1)
			},
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, Follower, r.state)
				assert.Equal(t, uint64(6), r.currentTerm)
				assert.Equal(t, -1, r.votedFor)
				assert.False(t, r.lastAck[2].After(pastTime))
			},
		},
		{
			name:         "UpdatesLastAckOnSuccess",
			initialState: state{term: 5, state: Leader},
			reply:        &param.AppendEntriesReply{Term: 5, Success: true},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine) {
				s.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
				s.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()
			},
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				assert.True(t, r.lastAck[2].After(pastTime))
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, uint64(11), r.nextIndex[2])
				assert.Equal(t, uint64(10), r.matchIndex[2])
			},
		},
		{
			name:         "UpdatesLastAckOnFailureMatchingTerm",
			initialState: state{term: 5, state: Leader},
			reply:        &param.AppendEntriesReply{Term: 5, Success: false},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport, sm *storage.MockStateMachine) {
				s.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
				s.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
				s.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
				tr.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				assert.True(t, r.lastAck[2].After(pastTime))
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
			commitChan := make(chan param.CommitEntry, 1)

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans, mockSM)
			}

			r := NewRaft(1, []int{2}, mockStore, mockSM, mockTrans, commitChan)
			defer r.Stop()
			r.state = tt.initialState.state
			r.currentTerm = tt.initialState.term

			pastTime := time.Now().Add(-1 * time.Second)
			r.lastAck[2] = pastTime

			args := &param.AppendEntriesArgs{PrevLogIndex: 9, Entries: []param.LogEntry{{Index: 10}}}

			r.mu.Lock()
			r.processAppendEntriesReply(2, args, tt.reply, tt.initialState.term)
			r.mu.Unlock()

			if tt.verify != nil {
				tt.verify(t, r, pastTime)
			}
		})
	}
}

func TestDispatchEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSM := storage.NewMockStateMachine(ctrl)

	t.Run("Apply normal command", func(t *testing.T) {
		notifyChan := make(chan any, 1)
		r := &Raft{
			stateMachine: mockSM,
			notifyApply:  map[uint64]chan any{10: notifyChan},
			mu:           sync.Mutex{},
		}
		r.lastAppliedCond = sync.NewCond(&r.mu)

		entry := param.LogEntry{Command: "test", Index: 10}

		mockSM.EXPECT().Apply(entry).Return("test_result")
		r.commitChan = make(chan param.CommitEntry, 1)

		r.dispatchEntries([]param.LogEntry{entry})

		select {
		case result := <-notifyChan:
			assert.Equal(t, "test_result", result)
		case <-time.After(50 * time.Millisecond):
			t.Fatal("timed out waiting for notification")
		}
	})

	t.Run("Apply config change to enter joint consensus", func(t *testing.T) {
		r := &Raft{inJointConsensus: false, mu: sync.Mutex{}}
		r.lastAppliedCond = sync.NewCond(&r.mu)

		cmd := param.ConfigChangeCommand{NewPeerIDs: []int{1, 2, 3}}
		entry := param.LogEntry{Command: cmd, Index: 10}

		r.dispatchEntries([]param.LogEntry{entry})

		assert.True(t, r.inJointConsensus, "should enter joint consensus")
		assert.Equal(t, cmd.NewPeerIDs, r.newPeerIDs)
	})
}

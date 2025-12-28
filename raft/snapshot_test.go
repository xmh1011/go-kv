package raft

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
)

func TestTakeSnapshot(t *testing.T) {
	type state struct {
		isSnapshotting bool
		lastApplied    uint64
	}
	tests := []struct {
		name             string
		initialState     state
		logSizeThreshold int
		setupMocks       func(*storage.MockStorage, *storage.MockStateMachine, chan struct{})
		verify           func(*testing.T, chan struct{})
	}{
		{
			name:             "Success",
			initialState:     state{lastApplied: 100},
			logSizeThreshold: 1000,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				gomock.InOrder(
					s.EXPECT().LogSize().Return(1001, nil),
					s.EXPECT().GetEntry(uint64(100)).Return(&param.LogEntry{Term: 5, Index: 100}, nil),
					sm.EXPECT().GetSnapshot().Return([]byte("data"), nil),
					s.EXPECT().SaveSnapshot(gomock.Any()).Return(nil),
					s.EXPECT().CompactLog(uint64(100)).Return(nil).Do(func(_ uint64) { close(done) }),
				)
			},
			verify: func(t *testing.T, done chan struct{}) {
				select {
				case <-done:
				case <-time.After(1 * time.Second):
					t.Fatal("timeout waiting for snapshot async operations")
				}
			},
		},
		{
			name:             "LogSizeBelowThreshold",
			initialState:     state{lastApplied: 100},
			logSizeThreshold: 1000,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				s.EXPECT().LogSize().Return(999, nil)
			},
			verify: nil,
		},
		{
			name:             "AlreadySnapshotting",
			initialState:     state{isSnapshotting: true},
			logSizeThreshold: 1000,
			setupMocks:       nil, // No calls expected
			verify:           nil,
		},
		{
			name:             "GetEntryFails",
			initialState:     state{lastApplied: 100},
			logSizeThreshold: 1000,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				gomock.InOrder(
					s.EXPECT().LogSize().Return(1001, nil),
					s.EXPECT().GetEntry(uint64(100)).Return(nil, errors.New("entry not found")),
				)
			},
			verify: nil,
		},
		{
			name:             "GetSnapshotFails",
			initialState:     state{lastApplied: 100},
			logSizeThreshold: 1000,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				gomock.InOrder(
					s.EXPECT().LogSize().Return(1001, nil),
					s.EXPECT().GetEntry(uint64(100)).Return(&param.LogEntry{Term: 5, Index: 100}, nil),
					sm.EXPECT().GetSnapshot().Return(nil, errors.New("state machine error")),
				)
			},
			verify: nil,
		},
		{
			name:             "FailsOnSaveError",
			initialState:     state{lastApplied: 100},
			logSizeThreshold: 1000,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				gomock.InOrder(
					s.EXPECT().LogSize().Return(1001, nil),
					s.EXPECT().GetEntry(uint64(100)).Return(&param.LogEntry{Term: 5, Index: 100}, nil),
					sm.EXPECT().GetSnapshot().Return([]byte("data"), nil),
					s.EXPECT().SaveSnapshot(gomock.Any()).Return(errors.New("disk is full")).Do(func(_ any) { close(done) }),
				)
			},
			verify: func(t *testing.T, done chan struct{}) {
				select {
				case <-done:
				case <-time.After(1 * time.Second):
					t.Fatal("timeout waiting for SaveSnapshot")
				}
			},
		},
		{
			name:             "FailsOnCompactError",
			initialState:     state{lastApplied: 100},
			logSizeThreshold: 1000,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				gomock.InOrder(
					s.EXPECT().LogSize().Return(1001, nil),
					s.EXPECT().GetEntry(uint64(100)).Return(&param.LogEntry{Term: 5, Index: 100}, nil),
					sm.EXPECT().GetSnapshot().Return([]byte("data"), nil),
					s.EXPECT().SaveSnapshot(gomock.Any()).Return(nil),
					s.EXPECT().CompactLog(uint64(100)).Return(errors.New("compaction failed")).Do(func(_ uint64) { close(done) }),
				)
			},
			verify: func(t *testing.T, done chan struct{}) {
				select {
				case <-done:
				case <-time.After(1 * time.Second):
					t.Fatal("timeout waiting for CompactLog")
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
			mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

			r := NewRaft(1, []int{2, 3}, mockStore, mockSM, nil, nil)
			r.lastApplied = tt.initialState.lastApplied
			r.isSnapshotting = tt.initialState.isSnapshotting
			done := make(chan struct{})

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockSM, done)
			}

			r.SetSnapshotThreshold(tt.logSizeThreshold)
			r.TakeSnapshot()

			if tt.verify != nil {
				tt.verify(t, done)
			}
		})
	}
}

func TestInstallSnapshot(t *testing.T) {
	type state struct {
		term        uint64
		lastApplied uint64
		commitIndex uint64
	}
	tests := []struct {
		name          string
		initialState  state
		args          *param.InstallSnapshotArgs
		setupMocks    func(*storage.MockStorage, *storage.MockStateMachine)
		expectedError error
		expectedState state
		expectedReply *param.InstallSnapshotReply
	}{
		{
			name: "Success",
			initialState: state{
				term:        5,
				lastApplied: 100,
				commitIndex: 100,
			},
			args: param.NewInstallSnapshotArgs(5, 1, 200, 4, []byte("data")),
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine) {
				gomock.InOrder(
					s.EXPECT().SaveSnapshot(gomock.Any()).Return(nil),
					s.EXPECT().CompactLog(uint64(200)).Return(nil),
					sm.EXPECT().ApplySnapshot([]byte("data")).Return(nil),
				)
			},
			expectedError: nil,
			expectedState: state{
				term:        5,
				lastApplied: 200,
				commitIndex: 200,
			},
			expectedReply: &param.InstallSnapshotReply{Term: 5},
		},
		{
			name: "StaleTerm",
			initialState: state{
				term: 6,
			},
			args:          &param.InstallSnapshotArgs{Term: 5},
			setupMocks:    nil,
			expectedError: nil,
			expectedState: state{term: 6},
			expectedReply: &param.InstallSnapshotReply{Term: 6},
		},
		{
			name: "FailsOnApplyError",
			initialState: state{
				term:        5,
				lastApplied: 50,
			},
			args: &param.InstallSnapshotArgs{Term: 5, LastIncludedIndex: 200, Data: []byte("corrupted")},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine) {
				gomock.InOrder(
					s.EXPECT().SaveSnapshot(gomock.Any()).Return(nil),
					s.EXPECT().CompactLog(uint64(200)).Return(nil),
					sm.EXPECT().ApplySnapshot([]byte("corrupted")).Return(errors.New("failed")),
				)
			},
			expectedError: errors.New("failed"),
			expectedState: state{
				term:        5,
				lastApplied: 50, // Should not update
			},
			expectedReply: &param.InstallSnapshotReply{Term: 5},
		},
		{
			name: "FailsOnPersistError",
			initialState: state{
				term:        5,
				lastApplied: 50,
			},
			args: &param.InstallSnapshotArgs{Term: 5, LastIncludedIndex: 200, Data: []byte("data")},
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine) {
				s.EXPECT().SaveSnapshot(gomock.Any()).Return(errors.New("disk full"))
			},
			expectedError: errors.New("disk full"),
			expectedState: state{
				term:        5,
				lastApplied: 50, // Should not update
			},
			expectedReply: &param.InstallSnapshotReply{Term: 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockSM := storage.NewMockStateMachine(ctrl)
			mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

			r := NewRaft(2, []int{1, 3}, mockStore, mockSM, nil, nil)
			r.currentTerm = tt.initialState.term
			r.lastApplied = tt.initialState.lastApplied
			r.commitIndex = tt.initialState.commitIndex

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockSM)
			}

			reply := &param.InstallSnapshotReply{}
			err := r.InstallSnapshot(tt.args, reply)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedReply != nil {
				assert.Equal(t, tt.expectedReply.Term, reply.Term)
			}

			assert.Equal(t, tt.expectedState.term, r.currentTerm)
			assert.Equal(t, tt.expectedState.lastApplied, r.lastApplied)
			if tt.name == "Success" {
				assert.Equal(t, tt.expectedState.commitIndex, r.commitIndex)
			}
		})
	}
}

func TestSendSnapshot(t *testing.T) {
	type state struct {
		term  uint64
		state State
	}
	tests := []struct {
		name          string
		initialState  state
		setupMocks    func(*storage.MockStorage, *transport.MockTransport)
		expectedState state
		expectedNext  uint64
		expectedMatch uint64
	}{
		{
			name:         "Success",
			initialState: state{term: 5, state: Leader},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport) {
				snapshot := param.NewSnapshot(150, 4, []byte("data"))
				s.EXPECT().ReadSnapshot().Return(snapshot, nil)
				tr.EXPECT().SendInstallSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
						reply.Term = 5
						return nil
					})
			},
			expectedState: state{term: 5, state: Leader},
			expectedNext:  151,
			expectedMatch: 150,
		},
		{
			name:         "StepsDownOnHigherTerm",
			initialState: state{term: 5, state: Leader},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport) {
				snapshot := param.NewSnapshot(150, 4, []byte("data"))
				s.EXPECT().ReadSnapshot().Return(snapshot, nil)
				tr.EXPECT().SendInstallSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(id string, args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
						reply.Term = 6
						return nil
					})
				s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil)
			},
			expectedState: state{term: 6, state: Follower},
			expectedNext:  0,
			expectedMatch: 0,
		},
		{
			name:         "ReadSnapshotFails",
			initialState: state{term: 5, state: Leader},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport) {
				s.EXPECT().ReadSnapshot().Return(nil, errors.New("read error"))
			},
			expectedState: state{term: 5, state: Leader},
			expectedNext:  0,
			expectedMatch: 0,
		},
		{
			name:         "SendRPCFails",
			initialState: state{term: 5, state: Leader},
			setupMocks: func(s *storage.MockStorage, tr *transport.MockTransport) {
				snapshot := param.NewSnapshot(150, 4, []byte("data"))
				s.EXPECT().ReadSnapshot().Return(snapshot, nil)
				tr.EXPECT().SendInstallSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("network error"))
			},
			expectedState: state{term: 5, state: Leader},
			expectedNext:  0,
			expectedMatch: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockStore.EXPECT().GetState().Return(param.HardState{CurrentTerm: tt.initialState.term}, nil).Times(1)

			r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)
			r.currentTerm = tt.initialState.term
			r.state = tt.initialState.state

			if tt.setupMocks != nil {
				tt.setupMocks(mockStore, mockTrans)
			}

			r.sendSnapshot(2)

			r.mu.Lock()
			defer r.mu.Unlock()
			assert.Equal(t, tt.expectedState.state, r.state)
			assert.Equal(t, tt.expectedState.term, r.currentTerm)
			if tt.expectedNext > 0 {
				assert.Equal(t, tt.expectedNext, r.nextIndex[2])
				assert.Equal(t, tt.expectedMatch, r.matchIndex[2])
			}
		})
	}
}

func TestProcessSnapshotReply(t *testing.T) {
	type state struct {
		term  uint64
		state State
	}
	tests := []struct {
		name          string
		initialState  state
		reply         *param.InstallSnapshotReply
		snapshotIndex uint64
		savedTerm     uint64
		verify        func(*testing.T, *Raft, time.Time)
	}{
		{
			name:          "Success",
			initialState:  state{term: 5, state: Leader},
			reply:         &param.InstallSnapshotReply{Term: 5},
			snapshotIndex: 100,
			savedTerm:     5,
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.True(t, r.lastAck[2].After(pastTime))
				assert.Equal(t, uint64(101), r.nextIndex[2])
				assert.Equal(t, uint64(100), r.matchIndex[2])
			},
		},
		{
			name:          "StaleReply_DifferentTerm",
			initialState:  state{term: 6, state: Leader}, // Current term is now 6
			reply:         &param.InstallSnapshotReply{Term: 5},
			snapshotIndex: 100,
			savedTerm:     5, // RPC was sent during term 5
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.False(t, r.lastAck[2].After(pastTime)) // Should not update
			},
		},
		{
			name:          "StaleReply_NotLeader",
			initialState:  state{term: 5, state: Follower}, // No longer leader
			reply:         &param.InstallSnapshotReply{Term: 5},
			snapshotIndex: 100,
			savedTerm:     5,
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.False(t, r.lastAck[2].After(pastTime)) // Should not update
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStore := storage.NewMockStorage(ctrl)
			mockTrans := transport.NewMockTransport(ctrl)
			mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
			commitChan := make(chan param.CommitEntry, 1)

			r := NewRaft(1, []int{2}, mockStore, nil, mockTrans, commitChan)
			r.state = tt.initialState.state
			r.currentTerm = tt.initialState.term

			pastTime := time.Now().Add(-1 * time.Second)
			r.lastAck[2] = pastTime

			r.processSnapshotReply(2, tt.reply, tt.snapshotIndex, tt.savedTerm)

			if tt.verify != nil {
				tt.verify(t, r, pastTime)
			}
		})
	}
}

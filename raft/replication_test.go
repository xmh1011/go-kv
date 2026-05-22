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
				store:     mockStore,
				nextIndex: map[int]uint64{2: tt.initialState.nextIndex},
			}
			r.setState(tt.initialState.state)

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
				// prepareAppendEntriesArgs now uses r.cachedLastLogIndex (set in struct)
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
				// prepareAppendEntriesArgs now uses r.cachedLastLogIndex (set in struct)
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
				id:                 1,
				currentTerm:        tt.initialState.term,
				commitIndex:        tt.initialState.commitIndex,
				cachedLastLogIndex: 12,
				store:              mockStore,
				nextIndex:          map[int]uint64{2: tt.initialState.nextIndex},
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

func TestPrepareAppendEntriesArgsRequestsSnapshotWhenNextIndexWasCompacted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	r := &Raft{
		id:                 1,
		currentTerm:        5,
		commitIndex:        10,
		cachedLastLogIndex: 12,
		store:              storage.NewMockStorage(ctrl),
		nextIndex:          map[int]uint64{2: 10},
		snapshot:           param.NewSnapshot(10, 4, []byte("snapshot")),
	}

	args, err := r.prepareAppendEntriesArgs(2)
	assert.Nil(t, args)
	assert.True(t, errors.Is(err, errPeerNeedsSnapshot))
}

func TestPrepareAppendEntriesArgsClampsNextIndexPastLocalTail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12}, nil).Times(1)

	r := &Raft{
		id:                 1,
		currentTerm:        5,
		commitIndex:        10,
		cachedLastLogIndex: 12,
		store:              mockStore,
		nextIndex:          map[int]uint64{2: 15},
	}

	args, err := r.prepareAppendEntriesArgs(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(13), r.nextIndex[2])
	assert.Equal(t, uint64(12), args.PrevLogIndex)
	assert.Empty(t, args.Entries)
}

func TestPrepareAppendEntriesArgsRequestsSnapshotWhenEntryWasCompactedInStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	gomock.InOrder(
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		mockStore.EXPECT().GetEntry(uint64(11)).Return(nil, nil),
		mockStore.EXPECT().ReadSnapshot().Return(param.NewSnapshot(11, 5, []byte("snapshot")), nil),
	)

	r := &Raft{
		id:                 1,
		currentTerm:        5,
		commitIndex:        10,
		cachedLastLogIndex: 12,
		store:              mockStore,
		nextIndex:          map[int]uint64{2: 11},
	}

	args, err := r.prepareAppendEntriesArgs(2)
	assert.Nil(t, args)
	assert.True(t, errors.Is(err, errPeerNeedsSnapshot))
	assert.NotNil(t, r.snapshot)
	assert.Equal(t, uint64(11), r.snapshot.LastIncludedIndex)
}

func TestPrepareAppendEntriesArgsRetriesWhenCachedTailIsStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	gomock.InOrder(
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		mockStore.EXPECT().GetEntry(uint64(11)).Return(nil, nil),
		mockStore.EXPECT().ReadSnapshot().Return(nil, nil),
		mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil),
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().ReadSnapshot().Return(nil, nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
	)

	r := &Raft{
		id:                 1,
		currentTerm:        5,
		commitIndex:        10,
		cachedLastLogIndex: 12,
		store:              mockStore,
		nextIndex:          map[int]uint64{2: 11},
	}

	args, err := r.prepareAppendEntriesArgs(2)
	assert.Nil(t, args)
	assert.True(t, errors.Is(err, errLocalLogUnavailable))
	assert.Equal(t, uint64(10), r.cachedLastLogIndex)
	assert.Equal(t, uint64(11), r.nextIndex[2])
}

func TestPrepareAppendEntriesArgsRewindsSparseLocalLogGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	gomock.InOrder(
		mockStore.EXPECT().GetEntry(uint64(11)).Return(nil, nil),
		mockStore.EXPECT().ReadSnapshot().Return(nil, nil),
		mockStore.EXPECT().ReadSnapshot().Return(nil, nil),
		mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil),
		mockStore.EXPECT().LastLogIndex().Return(uint64(12), nil),
		mockStore.EXPECT().ReadSnapshot().Return(nil, nil),
		mockStore.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12}, nil),
	)

	r := &Raft{
		id:                 1,
		currentTerm:        5,
		commitIndex:        10,
		cachedLastLogIndex: 12,
		store:              mockStore,
		nextIndex:          map[int]uint64{2: 12},
	}

	args, err := r.prepareAppendEntriesArgs(2)
	assert.Nil(t, args)
	assert.True(t, errors.Is(err, errLocalLogUnavailable))
	assert.Equal(t, uint64(10), r.cachedLastLogIndex)
	assert.Equal(t, uint64(11), r.nextIndex[2])
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
				// findMajorityCommitIndex now uses r.cachedLastLogIndex (set in struct)
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
				// findMajorityCommitIndex uses r.cachedLastLogIndex (set in struct)
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
				// findMajorityCommitIndex uses r.cachedLastLogIndex (set in struct)
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
				id:                 1,
				peerIDs:            []int{2, 3},
				currentTerm:        tt.initialState.term,
				commitIndex:        tt.initialState.commitIndex,
				cachedLastLogIndex: 12,
				matchIndex:         tt.initialState.matchIndex,
				store:              mockStore,
				lastApplied:        tt.initialState.commitIndex,
				stateMachine:       mockSM,
				notifyApply:        make(map[uint64][]chan any),
				mu:                 sync.Mutex{},
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
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11, Command: "cmd1"}, nil).AnyTimes()
				s.EXPECT().GetEntry(uint64(12)).Return(&param.LogEntry{Term: 5, Index: 12, Command: "cmd2"}, nil).AnyTimes()
				// fetchEntriesToApply now uses r.cachedLastLogIndex (set in struct)
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
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11, Command: "cmd1"}, nil).AnyTimes()
				// fetchEntriesToApply now uses r.cachedLastLogIndex (set in struct)
				sm.EXPECT().Apply(gomock.Any()).Return("res1").AnyTimes()
				s.EXPECT().LogSize().Return(101, nil).AnyTimes()
				sm.EXPECT().GetSnapshot().Return([]byte("snap"), nil).AnyTimes()
				s.EXPECT().SaveSnapshot(gomock.Any()).Return(nil).AnyTimes()
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
		{
			name:           "SkipsEntriesCoveredBySnapshot",
			commitIndex:    12,
			lastApplied:    10,
			snapshotThresh: -1,
			setupMocks: func(s *storage.MockStorage, sm *storage.MockStateMachine, done chan struct{}) {
				s.EXPECT().GetEntry(uint64(11)).Return(nil, nil).Times(1)
				s.EXPECT().ReadSnapshot().Return(param.NewSnapshot(12, 5, []byte("snap")), nil).Times(1)
			},
			expectedApplied: 12,
			expectSnapshot:  false,
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
				id:                 1,
				commitIndex:        tt.commitIndex,
				lastApplied:        tt.lastApplied,
				cachedLastLogIndex: tt.commitIndex,
				snapshotThreshold:  tt.snapshotThresh,
				store:              mockStore,
				stateMachine:       mockSM,
				notifyApply:        make(map[uint64][]chan any),
				mu:                 sync.Mutex{},
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

type blockingApplyStateMachine struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (sm *blockingApplyStateMachine) Apply(entry param.LogEntry) any {
	sm.startedOnce.Do(func() {
		close(sm.started)
	})
	<-sm.release
	return "applied"
}

func (sm *blockingApplyStateMachine) Get(key string) (string, error) {
	return "", nil
}

func (sm *blockingApplyStateMachine) GetSnapshot() ([]byte, error) {
	return nil, nil
}

func (sm *blockingApplyStateMachine) ApplySnapshot(snapshot []byte) error {
	return nil
}

type blockingSnapshotStateMachine struct {
	snapshotStarted     chan struct{}
	releaseSnapshot     chan struct{}
	snapshotStartedOnce sync.Once
}

func (sm *blockingSnapshotStateMachine) Apply(entry param.LogEntry) any {
	return "applied"
}

func (sm *blockingSnapshotStateMachine) Get(key string) (string, error) {
	return "", nil
}

func (sm *blockingSnapshotStateMachine) GetSnapshot() ([]byte, error) {
	sm.snapshotStartedOnce.Do(func() {
		close(sm.snapshotStarted)
	})
	<-sm.releaseSnapshot
	return []byte("snapshot"), nil
}

func (sm *blockingSnapshotStateMachine) ApplySnapshot(snapshot []byte) error {
	return nil
}

func TestApplyLogsAdvancesLastAppliedAfterStateMachineApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().
		GetEntry(uint64(1)).
		Return(&param.LogEntry{Term: 1, Index: 1, Command: "cmd"}, nil).
		Times(1)

	sm := &blockingApplyStateMachine{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	r := &Raft{
		id:                 1,
		commitIndex:        1,
		lastApplied:        0,
		cachedLastLogIndex: 1,
		snapshotThreshold:  -1,
		store:              mockStore,
		stateMachine:       sm,
		notifyApply:        make(map[uint64][]chan any),
	}
	r.lastAppliedCond = sync.NewCond(&r.mu)

	done := make(chan struct{})
	go func() {
		r.applyLogs()
		close(done)
	}()

	select {
	case <-sm.started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for state machine apply to start")
	}

	r.mu.Lock()
	assert.Equal(t, uint64(0), r.lastApplied, "lastApplied must not advance before Apply returns")
	r.mu.Unlock()

	close(sm.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for applyLogs to finish")
	}

	r.mu.Lock()
	assert.Equal(t, uint64(1), r.lastApplied)
	r.mu.Unlock()
}

func TestApplyLogsReleasesApplyMuBeforeSnapshotExport(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	entry := &param.LogEntry{Term: 1, Index: 1, Command: "cmd"}
	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetEntry(uint64(1)).Return(entry, nil).Times(2)
	mockStore.EXPECT().LogSize().Return(2, nil).Times(1)
	mockStore.EXPECT().SaveSnapshot(gomock.Any()).Return(nil).Times(1)
	mockStore.EXPECT().CompactLog(uint64(1)).Return(nil).Times(1)

	sm := &blockingSnapshotStateMachine{
		snapshotStarted: make(chan struct{}),
		releaseSnapshot: make(chan struct{}),
	}
	r := &Raft{
		id:                 1,
		commitIndex:        1,
		lastApplied:        0,
		cachedLastLogIndex: 1,
		snapshotThreshold:  1,
		store:              mockStore,
		stateMachine:       sm,
		notifyApply:        make(map[uint64][]chan any),
	}
	r.lastAppliedCond = sync.NewCond(&r.mu)

	applyDone := make(chan struct{})
	go func() {
		r.applyLogs()
		close(applyDone)
	}()

	select {
	case <-sm.snapshotStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot export to start")
	}

	applyMuAcquired := make(chan struct{})
	go func() {
		r.applyMu.Lock()
		r.applyMu.Unlock()
		close(applyMuAcquired)
	}()

	select {
	case <-applyMuAcquired:
	case <-time.After(time.Second):
		t.Fatal("applyMu stayed locked while snapshot export was blocked")
	}

	close(sm.releaseSnapshot)

	select {
	case <-applyDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for applyLogs to finish")
	}
	r.snapshotWG.Wait()
}

func TestProcessBatchCommitsSingleNodeEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	entry := param.LogEntry{Term: 2, Index: 1, Command: "cmd"}
	applied := make(chan struct{})

	mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1)
	mockStore.EXPECT().GetEntry(uint64(1)).Return(&entry, nil).Times(2)
	mockSM.EXPECT().
		Apply(entry).
		DoAndReturn(func(param.LogEntry) any {
			close(applied)
			return "ok"
		}).
		Times(1)

	r := &Raft{
		id:                 1,
		peerIDs:            []int{1},
		currentTerm:        2,
		cachedLastLogIndex: 0,
		snapshotThreshold:  -1,
		store:              mockStore,
		stateMachine:       mockSM,
		notifyApply:        make(map[uint64][]chan any),
		nextIndex:          make(map[int]uint64),
		matchIndex:         make(map[int]uint64),
	}
	r.setState(Leader)
	r.lastAppliedCond = sync.NewCond(&r.mu)

	resultCh := make(chan proposalResult, 1)
	r.processBatch([]proposalRequest{{command: "cmd", result: resultCh}})

	result := <-resultCh
	assert.True(t, result.ok)
	assert.Equal(t, uint64(1), result.index)

	r.mu.Lock()
	assert.Equal(t, uint64(1), r.commitIndex)
	r.mu.Unlock()

	select {
	case <-applied:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for single-node entry to apply")
	}

	assert.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.lastApplied == 1
	}, time.Second, 10*time.Millisecond)
}

func TestProcessBatchDeduplicatesPendingClientRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)

	wrapped := param.NewClientCommand(10, 1, []byte("cmd"))
	var storedEntry param.LogEntry

	mockStore.EXPECT().
		AppendEntries(gomock.Any()).
		DoAndReturn(func(entries []param.LogEntry) error {
			assert.Len(t, entries, 1)
			storedEntry = entries[0]
			assert.Equal(t, uint64(1), storedEntry.Index)
			assert.Equal(t, wrapped, storedEntry.Command)
			return nil
		}).
		Times(1)
	mockStore.EXPECT().GetEntry(uint64(1)).DoAndReturn(func(uint64) (*param.LogEntry, error) {
		return &storedEntry, nil
	}).Times(2)
	mockSM.EXPECT().Apply(gomock.Any()).Return("ok").Times(1)

	r := &Raft{
		id:                    1,
		peerIDs:               []int{1},
		currentTerm:           2,
		cachedLastLogIndex:    0,
		snapshotThreshold:     -1,
		store:                 mockStore,
		stateMachine:          mockSM,
		clientSessions:        make(map[int64]int64),
		pendingClientRequests: make(map[clientRequestKey]uint64),
		pendingLogClients:     make(map[uint64]clientRequestKey),
		notifyApply:           make(map[uint64][]chan any),
		nextIndex:             make(map[int]uint64),
		matchIndex:            make(map[int]uint64),
	}
	r.setState(Leader)
	r.lastAppliedCond = sync.NewCond(&r.mu)

	key := clientRequestKey{clientID: 10, sequenceNum: 1}
	firstResult := make(chan proposalResult, 1)
	secondResult := make(chan proposalResult, 1)
	r.processBatch([]proposalRequest{
		{command: wrapped, result: firstResult, clientKey: key, trackClient: true},
		{command: wrapped, result: secondResult, clientKey: key, trackClient: true},
	})

	first := <-firstResult
	second := <-secondResult
	assert.True(t, first.ok)
	assert.True(t, second.ok)
	assert.Equal(t, uint64(1), first.index)
	assert.Equal(t, uint64(1), second.index)

	assert.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.lastApplied == 1 && r.clientSessions[10] == 1 && len(r.pendingClientRequests) == 0
	}, time.Second, 10*time.Millisecond)
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
				// proposeNewConfigEntry uses r.cachedLastLogIndex (set in struct)
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
				assert.Equal(t, Leader, r.getState())
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
				assert.Equal(t, Follower, r.getState())
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
				id:                 1,
				inJointConsensus:   tt.initialState.inJoint,
				peerIDs:            tt.initialState.peerIDs,
				newPeerIDs:         tt.initialState.newPeerIDs,
				currentTerm:        tt.initialState.currentTerm,
				cachedLastLogIndex: 10,
				store:              mockStore,
				nextIndex:          make(map[int]uint64),
				matchIndex:         make(map[int]uint64),
				mu:                 sync.Mutex{},
				electionTimeout:    config.Conf.Raft.ElectionTimeout,
				heartbeatTimeout:   config.Conf.Raft.HeartbeatTimeout,
			}
			r.setState(tt.initialState.state)
			r.lastAppliedCond = sync.NewCond(&r.mu)

			r.applyConfigChange(tt.cmd, 10)

			if tt.verify != nil {
				tt.verify(t, r)
			}
		})
	}
}

func TestLeaderAppliesInitialConfigChangeAsJointEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().
		AppendEntries(gomock.Any()).
		DoAndReturn(func(entries []param.LogEntry) error {
			assert.Len(t, entries, 1)
			assert.Equal(t, uint64(11), entries[0].Index)
			return nil
		}).
		Times(1)

	r := &Raft{
		id:                 1,
		peerIDs:            []int{1, 2},
		newPeerIDs:         []int{1, 2, 3},
		inJointConsensus:   true,
		jointConfigIndex:   10,
		currentTerm:        5,
		cachedLastLogIndex: 10,
		store:              mockStore,
		nextIndex:          make(map[int]uint64),
		matchIndex:         make(map[int]uint64),
	}
	r.setState(Leader)
	r.lastAppliedCond = sync.NewCond(&r.mu)

	r.applyConfigChange(param.ConfigChangeCommand{NewPeerIDs: []int{1, 2, 3}}, 10)

	assert.True(t, r.inJointConsensus)
	assert.Equal(t, []int{1, 2}, r.peerIDs)
	assert.Equal(t, []int{1, 2, 3}, r.newPeerIDs)
	assert.Equal(t, uint64(10), r.jointConfigIndex)
	assert.Equal(t, uint64(11), r.cachedLastLogIndex)
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

				// Set cachedLastLogIndex for prepareAppendEntriesArgs
				r.cachedLastLogIndex = 11
				gomock.InOrder(
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1),
					// prepareAppendEntriesArgs uses r.cachedLastLogIndex now
					s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "test", Term: 5, Index: 11}, nil).Times(1),
					tr.EXPECT().SendAppendEntries(strconv.Itoa(peerID), gomock.Any(), gomock.Any()).
						DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
							reply.Term = 5
							reply.Success = true
							return nil
						}).Times(1),
				)

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

				// Set cachedLastLogIndex for prepareAppendEntriesArgs
				r.cachedLastLogIndex = 11
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
			mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil).Times(1)
			r := NewRaft(1, []int{2, 3, 4}, mockStore, mockSM, mockTrans, commitChan) // Use 4 nodes to prevent accidental majority
			defer r.Stop()
			r.setState(Leader)
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
					// checkLogConsistencyLockFree: 检查 PrevLogIndex=10
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					// findConflictAndPrepare: 检查 entry 11 是否已存在 → nil → 新条目
					s.EXPECT().GetEntry(uint64(11)).Return(nil, nil),
					// 无 TruncateLog（无冲突），直接 AppendEntries
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil),
				)
				// fetchEntriesToApply 读取条目以应用
				s.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "cmd1", Term: 5, Index: 11}, nil).AnyTimes()
				sm.EXPECT().Apply(gomock.Any()).Return("success").AnyTimes()
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
					// checkLogConsistencyLockFree: 检查 PrevLogIndex=10
					s.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
					// findConflictAndPrepare: 检查 entry 11 是否已存在 → nil → 新条目
					s.EXPECT().GetEntry(uint64(11)).Return(nil, nil),
					// 无 TruncateLog（无冲突），直接 AppendEntries
					s.EXPECT().AppendEntries(gomock.Any()).Return(nil),
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
				id:                 2,
				currentTerm:        tt.initialState.term,
				store:              mockStore,
				stateMachine:       mockSM,
				commitChan:         commitChan,
				lastApplied:        tt.initialState.lastApplied,
				cachedLastLogIndex: tt.initialState.lastApplied,
				mu:                 sync.Mutex{},
				electionTimeout:    config.Conf.Raft.ElectionTimeout,
				heartbeatTimeout:   config.Conf.Raft.HeartbeatTimeout,
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

func TestAppendEntriesRejectsIfTermChangesDuringDiskIO(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)

	r := &Raft{
		id:                 2,
		currentTerm:        5,
		store:              mockStore,
		stateMachine:       mockSM,
		commitChan:         make(chan param.CommitEntry, 1),
		cachedLastLogIndex: 10,
		electionTimeout:    config.Conf.Raft.ElectionTimeout,
		heartbeatTimeout:   config.Conf.Raft.HeartbeatTimeout,
	}
	r.setState(Follower)
	r.lastAppliedCond = sync.NewCond(&r.mu)

	args := &param.AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 10,
		PrevLogTerm:  5,
		Entries:      []param.LogEntry{{Command: "cmd11", Term: 5, Index: 11}},
		LeaderCommit: 11,
	}

	gomock.InOrder(
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		mockStore.EXPECT().GetEntry(uint64(11)).Return(nil, nil),
		mockStore.EXPECT().AppendEntries(gomock.Any()).DoAndReturn(func(entries []param.LogEntry) error {
			assert.Len(t, entries, 1)
			assert.Equal(t, uint64(11), entries[0].Index)
			r.mu.Lock()
			r.currentTerm = 6
			r.mu.Unlock()
			return nil
		}),
	)

	reply := &param.AppendEntriesReply{}
	err := r.AppendEntries(args, reply)

	assert.NoError(t, err)
	assert.False(t, reply.Success)
	assert.Equal(t, uint64(6), reply.Term)
	assert.Equal(t, uint64(11), r.cachedLastLogIndex)
	assert.Equal(t, uint64(0), r.commitIndex)
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
				s.EXPECT().LastLogIndex().Return(uint64(0), nil).Times(1)
				s.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil).Times(1)
			},
			verify: func(t *testing.T, r *Raft, pastTime time.Time) {
				r.mu.Lock()
				defer r.mu.Unlock()
				assert.Equal(t, Follower, r.getState())
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
				s.EXPECT().LastLogIndex().Return(uint64(10), nil).Times(1)
				s.EXPECT().ReadSnapshot().Return(nil, nil).AnyTimes()
				s.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
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
				s.EXPECT().LastLogIndex().Return(uint64(10), nil).Times(1)
				s.EXPECT().ReadSnapshot().Return(nil, nil).AnyTimes()
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
			r.setState(tt.initialState.state)
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

func TestSuccessfulAppendEntriesReplyDoesNotRegressPeerProgress(t *testing.T) {
	r := &Raft{
		id:                 1,
		currentTerm:        5,
		peerIDs:            []int{1, 2, 3, 4, 5},
		nextIndex:          map[int]uint64{2: 12},
		matchIndex:         map[int]uint64{2: 11},
		lastAck:            make(map[int]time.Time),
		cachedLastLogIndex: 11,
	}
	r.setState(Leader)

	args := &param.AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 9,
		PrevLogTerm:  5,
	}
	reply := &param.AppendEntriesReply{Term: 5, Success: true}

	r.processAppendEntriesReply(2, args, reply, 5)

	assert.Equal(t, uint64(12), r.nextIndex[2])
	assert.Equal(t, uint64(11), r.matchIndex[2])
}

func TestSuccessfulAppendEntriesRequestsNextBatchWhenPeerStillBehind(t *testing.T) {
	r := &Raft{
		id:                 1,
		peerIDs:            []int{1, 2, 3, 4, 5},
		nextIndex:          map[int]uint64{2: 10},
		matchIndex:         map[int]uint64{2: 9},
		cachedLastLogIndex: 50,
	}
	r.setState(Leader)

	entries := make([]param.LogEntry, MaxEntriesPerAppendEntries)
	for i := range entries {
		entries[i] = param.LogEntry{Index: uint64(10 + i), Term: 1}
	}
	args := &param.AppendEntriesArgs{
		PrevLogIndex: 9,
		Entries:      entries,
	}

	shouldContinue := r.handleSuccessfulAppendEntries(2, args)

	assert.True(t, shouldContinue)
	assert.Equal(t, uint64(42), r.nextIndex[2])
	assert.Equal(t, uint64(41), r.matchIndex[2])
}

func TestSuccessfulAppendEntriesDoesNotRequestNextBatchWhenPeerCaughtUp(t *testing.T) {
	r := &Raft{
		id:                 1,
		peerIDs:            []int{1, 2, 3, 4, 5},
		nextIndex:          map[int]uint64{2: 10},
		matchIndex:         map[int]uint64{2: 9},
		cachedLastLogIndex: 41,
	}
	r.setState(Leader)

	entries := make([]param.LogEntry, MaxEntriesPerAppendEntries)
	for i := range entries {
		entries[i] = param.LogEntry{Index: uint64(10 + i), Term: 1}
	}
	args := &param.AppendEntriesArgs{
		PrevLogIndex: 9,
		Entries:      entries,
	}

	shouldContinue := r.handleSuccessfulAppendEntries(2, args)

	assert.False(t, shouldContinue)
	assert.Equal(t, uint64(42), r.nextIndex[2])
	assert.Equal(t, uint64(41), r.matchIndex[2])
}

func TestFailedAppendEntriesReplyDoesNotBacktrackBelowMatchIndex(t *testing.T) {
	r := &Raft{
		id:         1,
		nextIndex:  map[int]uint64{2: 12},
		matchIndex: map[int]uint64{2: 11},
	}
	r.setState(Follower)

	r.handleFailedAppendEntries(2, &param.AppendEntriesReply{
		Success:       false,
		ConflictIndex: 5,
	})

	assert.Equal(t, uint64(12), r.nextIndex[2])
}

func TestDispatchEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSM := storage.NewMockStateMachine(ctrl)

	t.Run("Apply normal command", func(t *testing.T) {
		notifyChan := make(chan any, 1)
		r := &Raft{
			stateMachine: mockSM,
			notifyApply:  map[uint64][]chan any{10: []chan any{notifyChan}},
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

func TestDispatchEntriesSkipsDuplicateClientCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSM := storage.NewMockStateMachine(ctrl)

	notifyChan := make(chan any, 1)
	clientKey := clientRequestKey{clientID: 7, sequenceNum: 3}
	r := &Raft{
		stateMachine:          mockSM,
		clientSessions:        map[int64]int64{7: 3},
		notifyApply:           map[uint64][]chan any{10: []chan any{notifyChan}},
		pendingLogClients:     map[uint64]clientRequestKey{10: clientKey},
		pendingClientRequests: map[clientRequestKey]uint64{clientKey: 10},
		mu:                    sync.Mutex{},
		commitChan:            make(chan param.CommitEntry, 1),
	}
	r.lastAppliedCond = sync.NewCond(&r.mu)

	entry := param.LogEntry{
		Command: param.NewClientCommand(7, 3, []byte(`{"op":2,"key":"k","value":"v"}`)),
		Index:   10,
	}

	r.dispatchEntries([]param.LogEntry{entry})

	select {
	case <-notifyChan:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for duplicate notification")
	}
	assert.Equal(t, uint64(10), r.lastApplied)
	assert.Empty(t, r.pendingLogClients)
	assert.Empty(t, r.pendingClientRequests)
	assert.Empty(t, r.commitChan)
}

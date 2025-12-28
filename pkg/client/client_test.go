package client

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/transport"
)

// setup a helper function to create a client with a mock transport layer for each test.
func setup(t *testing.T) (*gomock.Controller, *transport.MockTransport, *Client) {
	ctrl := gomock.NewController(t)
	mockTrans := transport.NewMockTransport(ctrl)

	servers := map[int]string{
		1: "localhost:8001",
		2: "localhost:8002",
		3: "localhost:8003",
	}

	client := NewClient(servers, mockTrans)
	// For predictability in tests, let's set a fixed client ID.
	client.clientID = 12345
	return ctrl, mockTrans, client
}

func TestNewClient(t *testing.T) {
	ctrl, _, client := setup(t)
	defer ctrl.Finish()

	assert.NotNil(t, client)
	assert.NotZero(t, client.clientID)
	assert.Equal(t, int64(0), client.sequenceNum)
	assert.Equal(t, 0, client.leaderHint)
	assert.NotNil(t, client.servers)
	assert.NotNil(t, client.trans)
}

func TestSelectTargetNode(t *testing.T) {
	_, _, client := setup(t)

	// Case 1: No leader hint, should return the first available server
	targetID := client.selectTargetNode()
	// The order of map iteration is not guaranteed, but we know it must be one of the keys.
	assert.Contains(t, client.servers, targetID)

	// Case 2: With a leader hint, should return the leader hint
	client.leaderHint = 2
	targetID = client.selectTargetNode()
	assert.Equal(t, 2, targetID)
}

func TestDecideNextAction(t *testing.T) {
	_, _, client := setup(t)

	testCases := []struct {
		name               string
		reply              *param.ClientReply
		err                error
		expectedAction     clientAction
		expectedLeaderHint int
	}{
		{
			name:               "Network Error",
			reply:              &param.ClientReply{},
			err:                errors.New("connection refused"),
			expectedAction:     actionRetry,
			expectedLeaderHint: 0, // Should reset leader hint on network error
		},
		{
			name: "Not Leader Reply",
			reply: &param.ClientReply{
				NotLeader:  true,
				LeaderHint: 3,
			},
			err:                nil,
			expectedAction:     actionRetry,
			expectedLeaderHint: 3, // Should update leader hint
		},
		{
			name: "Success Reply",
			reply: &param.ClientReply{
				Success: true,
				Result:  "OK",
			},
			err:                nil,
			expectedAction:     actionSuccess,
			expectedLeaderHint: 1, // 期望 leaderHint 保持为 1
		},
		{
			name: "Leader Process Failure Reply",
			reply: &param.ClientReply{
				Success: false,
			},
			err:                nil,
			expectedAction:     actionRetry,
			expectedLeaderHint: 0, // Should reset leader hint
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset leader hint before each test case
			client.leaderHint = 1
			_, action := client.decideNextAction(1, tc.reply, tc.err)

			assert.Equal(t, tc.expectedAction, action)
			assert.Equal(t, tc.expectedLeaderHint, client.leaderHint)
		})
	}
}

func TestSendCommand(t *testing.T) {
	t.Run("Success on first try", func(t *testing.T) {
		ctrl, mockTrans, client := setup(t)
		defer ctrl.Finish()

		command := "get key"
		expectedResult := "value"

		// Expect one call to SendClientRequest
		mockTrans.EXPECT().
			SendClientRequest(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(nodeID string, args *param.ClientArgs, reply *param.ClientReply) error {
				// Simulate a successful reply from the leader
				reply.Success = true
				reply.Result = expectedResult
				assert.Equal(t, command, args.Command)
				assert.Equal(t, client.clientID, args.ClientID)
				return nil
			})

		result, ok := client.SendCommand(command)
		assert.True(t, ok)
		assert.Equal(t, expectedResult, result)
		assert.Equal(t, int64(1), client.sequenceNum)
	})

	t.Run("Success after not-leader reply", func(t *testing.T) {
		ctrl, mockTrans, client := setup(t)
		defer ctrl.Finish()

		command := "set key value"
		expectedResult := "OK"

		// Use gomock.InOrder to ensure the calls happen in sequence
		gomock.InOrder(
			// First call goes to a random node (e.g., node 1)
			mockTrans.EXPECT().
				SendClientRequest("1", gomock.Any(), gomock.Any()).
				DoAndReturn(func(nodeID string, args *param.ClientArgs, reply *param.ClientReply) error {
					reply.NotLeader = true
					reply.LeaderHint = 2 // Hint that node 2 is the leader
					return nil
				}),

			// Second call should go to the hinted leader (node 2)
			mockTrans.EXPECT().
				SendClientRequest("2", gomock.Any(), gomock.Any()).
				DoAndReturn(func(nodeID string, args *param.ClientArgs, reply *param.ClientReply) error {
					reply.Success = true
					reply.Result = expectedResult
					return nil
				}),
		)

		// Set an initial target so the first call is predictable
		client.leaderHint = 1
		result, ok := client.SendCommand(command)

		assert.True(t, ok)
		assert.Equal(t, expectedResult, result)
		assert.Equal(t, 2, client.leaderHint, "Leader hint should be updated to the correct leader")
	})

	t.Run("Command times out", func(t *testing.T) {
		ctrl, mockTrans, client := setup(t)
		defer ctrl.Finish()

		// Expect SendClientRequest to be called, but simulate it taking too long
		mockTrans.EXPECT().
			SendClientRequest(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(nodeID string, args *param.ClientArgs, reply *param.ClientReply) error {
				// Sleep for longer than the client's timeout
				time.Sleep(6 * time.Second)
				return nil
			}).AnyTimes() // Use AnyTimes because the test might exit before the call finishes

		result, ok := client.SendCommand("some command")

		assert.False(t, ok)
		assert.Nil(t, result)
	})
}

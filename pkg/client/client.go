package client

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"time"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/transport"
)

// clientAction 定义了客户端在处理完一次 RPC 响应后应采取的下一步动作。
type clientAction int

const (
	actionSuccess clientAction = iota // 动作：成功，可以返回结果
	actionFail                        // 动作：失败，应终止操作
	actionRetry                       // 动作：重试，应继续循环
)

// Client 封装了与 Raft 集群交互的逻辑。
type Client struct {
	clientID    int64               // 客户端的唯一ID
	sequenceNum int64               // 当前请求的序列号
	servers     map[int]string      // 集群中所有节点的 ID -> 地址映射
	leaderHint  int                 // 当前已知的 Leader ID
	trans       transport.Transport // 用于网络通信的传输层
}

// NewClient 创建一个新的客户端实例。
func NewClient(servers map[int]string, trans transport.Transport) *Client {
	// 生成一个随机的64位整数作为客户端ID。
	randID, _ := rand.Int(rand.Reader, big.NewInt(int64(^uint64(0)>>1)))
	return &Client{
		clientID:    randID.Int64(),
		sequenceNum: 0,
		servers:     servers,
		leaderHint:  0, // 初始时不知道谁是 Leader
		trans:       trans,
	}
}

// SendCommand 向 Raft 集群发送一个命令。
func (c *Client) SendCommand(command any) (any, bool) {
	opTimeout := time.After(5 * time.Second)
	c.sequenceNum++
	request := param.NewClientArgs(c.clientID, c.sequenceNum, command)

	for {
		select {
		case <-opTimeout:
			log.Warnf("[Client] Command (seq:%d) timed out after 5 seconds.", c.sequenceNum)
			return nil, false
		default:
			result, action := c.attemptOnce(request)

			// 使用我们定义的常量进行比较，而不是字符串。
			switch action {
			case actionSuccess:
				return result, true
			case actionFail:
				return nil, false
			case actionRetry:
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	}
}

// attemptOnce 负责执行单次向集群发送命令的尝试。
func (c *Client) attemptOnce(request *param.ClientArgs) (any, clientAction) {
	targetNodeID := c.selectTargetNode()
	log.Infof("[Client] Sending command (seq:%d) to node %d", c.sequenceNum, targetNodeID)

	reply := &param.ClientReply{}
	err := c.trans.SendClientRequest(strconv.Itoa(targetNodeID), request, reply)

	return c.decideNextAction(targetNodeID, reply, err)
}

// selectTargetNode 负责根据当前已知的 Leader 信息选择一个发送请求的目标节点。
func (c *Client) selectTargetNode() int {
	if c.leaderHint != 0 {
		return c.leaderHint
	}
	for id := range c.servers {
		return id
	}
	return 0
}

// decideNextAction 封装了所有处理 RPC 响应的决策逻辑。
// 它现在返回我们定义的 clientAction 类型，而不是字符串。
func (c *Client) decideNextAction(targetNodeID int, reply *param.ClientReply, err error) (result any, action clientAction) {
	if err != nil {
		log.Warnf("[Client] Error sending request to node %d: %v. Retrying...", targetNodeID, err)
		c.leaderHint = 0
		return nil, actionRetry
	}

	if reply.NotLeader {
		log.Infof("[Client] Node %d is not leader. New leader hint: %d. Retrying...", targetNodeID, reply.LeaderHint)
		c.leaderHint = reply.LeaderHint
		return nil, actionRetry
	}

	if reply.Success {
		log.Infof("[Client] Command (seq:%d) successfully processed.", c.sequenceNum)
		return reply.Result, actionSuccess
	}

	log.Warnf("[Client] Command (seq:%d) failed to be processed by leader. Retrying...", c.sequenceNum)
	c.leaderHint = 0
	return nil, actionRetry
}

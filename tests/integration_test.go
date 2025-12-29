package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/raft"
)

// cluster 封装了测试集群的组件
type cluster struct {
	nodes         []*raft.Raft
	transports    []transport.Transport
	stateMachines []storage.StateMachine
	storages      []storage.Storage
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
	transportType string
	storageType   string
	dataDir       string
}

// newClusterWithConfig 是创建测试集群的核心工厂函数。
// totalNodes: 物理启动的节点总数（例如 5 个，即使初始集群只配置了 3 个）。
// initialConfigSize: 初始 Raft 配置中的节点数量（例如 3，意味着节点 1-3 在配置中，4-5 是游离节点）。
// transportType: 指定传输层实现 ("tcp", "grpc")。
// storageType: 指定存储层实现 ("inmemory", "simplefile", "lsm")。
func newClusterWithConfig(t *testing.T, totalNodes int, initialConfigSize int, transportType string, storageType string) *cluster {
	c := &cluster{
		nodes:         make([]*raft.Raft, totalNodes),
		transports:    make([]transport.Transport, totalNodes),
		stateMachines: make([]storage.StateMachine, totalNodes),
		storages:      make([]storage.Storage, totalNodes),
		commitChans:   make([]chan param.CommitEntry, totalNodes),
		peerMap:       make(map[int]string),
		transportType: transportType,
		storageType:   storageType,
		dataDir:       t.TempDir(), // 自动清理的临时目录
	}

	// 1. 初始化传输层 (Transport)
	// 为每个节点分配地址并创建 Transport 实例。
	for i := 0; i < totalNodes; i++ {
		id := i + 1

		// 默认使用 TCP/GRPC 的随机端口模式
		addr := "127.0.0.1:0"

		// 统一调用，逻辑更清晰
		trans, err := transport.NewTransport(transportType, addr)
		if err != nil {
			t.Fatalf("failed to create transport for node %d: %v", id, err)
		}

		c.transports[i] = trans
		c.peerMap[id] = trans.Addr() // 记录分配到的实际地址
	}

	// 2. 构造 Raft 初始配置
	// 只有前 initialConfigSize 个节点被包含在初始的 peer 列表中。
	initialPeerIDs := make([]int, 0)
	for i := 0; i < initialConfigSize; i++ {
		initialPeerIDs = append(initialPeerIDs, i+1)
	}

	// 3. 初始化并启动 Raft 节点
	for i := 0; i < totalNodes; i++ {
		id := i + 1

		// 创建存储层和状态机
		store, sm, err := storage.NewStorage(storageType, c.dataDir, id)
		if err != nil {
			t.Fatalf("failed to create storage for node %d: %v", id, err)
		}

		c.storages[i] = store
		c.stateMachines[i] = sm
		c.commitChans[i] = make(chan param.CommitEntry, 100)

		// 启动一个后台协程持续读取 commitChan，防止阻塞
		// 因为在测试中是通过 c.stateMachines 直接验证数据的，不需要通过 channel 接收通知。
		go func(ch chan param.CommitEntry) {
			for range ch {
				// 丢弃数据，仅为了防止阻塞
			}
		}(c.commitChans[i])

		// 配置 Transport 的 Peer 列表（物理连接层需要知道所有节点的地址）
		c.transports[i].SetPeers(c.peerMap)

		// 创建 Raft 实例
		// 注意：不在 initialPeerIDs 中的节点（如 Node 4, 5）启动时会认为自己不在集群配置中。
		rf := raft.NewRaft(id, initialPeerIDs, store, sm, c.transports[i], c.commitChans[i])
		c.nodes[i] = rf

		// 将 Raft 实例注册到 Transport，以便处理入站 RPC 请求
		c.transports[i].RegisterRaft(rf)

		// 启动 Transport 监听
		if err := c.transports[i].Start(); err != nil {
			t.Fatalf("failed to start transport for node %d: %v", id, err)
		}

		// 启动 Raft 主循环
		go rf.Run()
	}

	return c
}

// shutdown 优雅地关闭集群中的所有节点和传输层。
func (c *cluster) shutdown() {
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i] != nil {
			c.nodes[i].Stop()
		}
		if c.transports[i] != nil {
			_ = c.transports[i].Close()
		}
		if c.storages[i] != nil {
			_ = c.storages[i].Close()
		}
		if c.stateMachines[i] != nil {
			// 如果状态机有 Close 方法，也应该调用
			// 这里假设 StateMachine 接口没有 Close，或者具体实现不需要显式关闭
			// 如果 lsm.StateMachineAdapter 有 Close，需要在这里处理
			if closer, ok := c.stateMachines[i].(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
	}
}

// getLeader 轮询集群状态，直到找到一个稳定的 Leader。
// 这个函数会发送探测请求来确认 Leader 的真实性（避免选出过期的 Leader）。
func (c *cluster) getLeader(t *testing.T) *raft.Raft {
	probeCmd := param.KVCommand{Op: param.OpGet, Key: "probe-key"}
	probeCmdBytes, _ := json.Marshal(probeCmd)

	// 最多重试 40 次，每次间隔 200ms，总共等待 8 秒
	for i := 0; i < 40; i++ {
		time.Sleep(200 * time.Millisecond)
		for _, node := range c.nodes {
			if node.IsStopped() {
				continue
			}

			// 发送一个只读请求来探测
			args := &param.ClientArgs{
				ClientID:    100,
				SequenceNum: int64(i),
				Command:     probeCmdBytes,
			}
			reply := &param.ClientReply{}

			// 忽略错误，只关注 reply.NotLeader 和 Success
			_ = node.ClientRequest(args, reply)

			if !reply.NotLeader {
				// 如果请求成功，或者虽然 key 不存在但已通过 ReadIndex 检查，则认为是 Leader
				if reply.Success {
					return node
				}
				if errMsg, ok := reply.Result.(string); ok && errMsg == "key not found" {
					return node
				}
			}
		}
	}
	t.Fatal("Cluster failed to elect a leader within timeout")
	return nil
}

// restartNode 模拟节点崩溃并恢复的过程。
// 它会停止旧节点，使用相同的存储和地址启动一个新节点。
func (c *cluster) restartNode(t *testing.T, nodeIndex int) {
	oldNode := c.nodes[nodeIndex]
	id := oldNode.ID()

	// 1. 停止旧节点 (模拟 Crash)
	oldNode.Stop()
	_ = c.transports[nodeIndex].Close()
	// 关闭存储，模拟进程退出
	_ = c.storages[nodeIndex].Close()
	if closer, ok := c.stateMachines[nodeIndex].(interface{ Close() error }); ok {
		_ = closer.Close()
	}

	// 2. 创建新的 Transport (模拟进程重启，端口重新绑定)
	// 关键点：必须复用之前的地址，否则其他节点找不到它
	prevAddr := c.peerMap[id]
	newTrans, err := transport.NewTransport(c.transportType, prevAddr)
	if err != nil {
		t.Fatalf("failed to recreate transport for node %d: %v", id, err)
	}
	c.transports[nodeIndex] = newTrans

	// 3. 恢复存储状态
	var store storage.Storage
	var sm storage.StateMachine

	if c.storageType == storage.SimpleFileStorage || c.storageType == storage.LSMStorage {
		// 文件存储/LSM：重新加载磁盘文件
		store, sm, err = storage.NewStorage(c.storageType, c.dataDir, id)
		if err != nil {
			t.Fatalf("failed to reload storage: %v", err)
		}
		c.storages[nodeIndex] = store
		c.stateMachines[nodeIndex] = sm
	} else {
		// 内存存储：直接复用旧对象来模拟持久化
		// 注意：在真实场景中，内存存储重启后数据会丢失，这里是为了测试 Raft 逻辑
		// 如果要测试非持久化存储重启丢数据，需要重新 NewStorage
		store = c.storages[nodeIndex]
		sm = c.stateMachines[nodeIndex]
	}

	// 重新构建 peerIDs 列表 (包含所有节点)
	peerIDs := make([]int, 0)
	for k := range c.peerMap {
		peerIDs = append(peerIDs, k)
	}

	// 4. 创建并启动新 Raft 实例
	newRaft := raft.NewRaft(id, peerIDs, store, sm, newTrans, c.commitChans[nodeIndex])
	c.nodes[nodeIndex] = newRaft

	newTrans.SetPeers(c.peerMap)
	newTrans.RegisterRaft(newRaft)
	if err := newTrans.Start(); err != nil {
		t.Fatalf("failed to start transport for node %d: %v", id, err)
	}

	go newRaft.Run()
	t.Logf("Node %d restarted.", id)
}

// TestCluster_ElectionAndReplication 测试基本的选举和日志复制功能。
// 验证集群能否选出 Leader，以及 Leader 写入的数据能否复制到所有 Follower。
func TestCluster_ElectionAndReplication(t *testing.T) {
	// 参数化测试：覆盖所有传输和存储组合
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				t.Logf("Leader elected: Node %d", leader.ID())

				// 发送写请求
				key := "test-key"
				value := "test-value"
				cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
				cmdBytes, _ := json.Marshal(cmd)

				clientArgs := &param.ClientArgs{ClientID: 999, SequenceNum: 1, Command: cmdBytes}
				clientReply := &param.ClientReply{}

				err := leader.ClientRequest(clientArgs, clientReply)
				assert.NoError(t, err)
				assert.True(t, clientReply.Success)

				// 验证所有节点数据一致性
				time.Sleep(1 * time.Second)
				for i := 0; i < 3; i++ {
					val, err := c.stateMachines[i].Get(key)
					assert.NoError(t, err)
					assert.Equal(t, value, val)
				}
			})
		}
	}
}

// TestCluster_LeaderFailover 测试 Leader 宕机后的故障转移能力。
// 验证旧 Leader 挂掉后，集群能选出新 Leader 并不丢失数据。
func TestCluster_LeaderFailover(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				// 1. 写入第一条数据
				oldLeader := c.getLeader(t)
				t.Logf("Original Leader: Node %d", oldLeader.ID())

				cmd1, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "k1", Value: "v1"})
				reply1 := &param.ClientReply{}
				err := oldLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd1}, reply1)
				assert.NoError(t, err)
				assert.True(t, reply1.Success)

				// 2. 模拟 Leader 宕机
				t.Logf("Stopping Leader Node %d...", oldLeader.ID())
				oldLeader.Stop()
				for i, node := range c.nodes {
					if node == oldLeader {
						_ = c.transports[i].Close() // 关闭网络连接
						break
					}
				}

				// 3. 等待新 Leader 产生
				t.Log("Waiting for new leader...")
				time.Sleep(2 * time.Second)

				newLeader := c.getLeader(t)
				t.Logf("New Leader: Node %d", newLeader.ID())
				assert.NotEqual(t, oldLeader.ID(), newLeader.ID())

				// 4. 向新 Leader 写入数据
				cmd2, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "k2", Value: "v2"})
				reply2 := &param.ClientReply{}
				err = newLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply2)
				assert.NoError(t, err)
				assert.True(t, reply2.Success)

				// 5. 验证数据一致性（旧数据还在，新数据已写入）
				time.Sleep(1 * time.Second)
				for i, node := range c.nodes {
					if node.ID() == oldLeader.ID() {
						continue
					}
					val1, err1 := c.stateMachines[i].Get("k1")
					assert.NoError(t, err1)
					assert.Equal(t, "v1", val1)

					val2, err2 := c.stateMachines[i].Get("k2")
					assert.NoError(t, err2)
					assert.Equal(t, "v2", val2)
				}
			})
		}
	}
}

// TestCluster_NetworkPartition 测试网络分区场景。
// 场景：Leader 被隔离到少数派分区，多数派分区选出新 Leader。
func TestCluster_NetworkPartition(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				leaderID := leader.ID()
				t.Logf("Leader: Node %d", leaderID)

				// 1. 制造分区：隔离当前的 Leader
				partitionedNodeID := leaderID
				t.Logf("Isolating Node %d...", partitionedNodeID)

				for i, node := range c.nodes {
					if node.ID() == partitionedNodeID {
						// Leader 无法联系任何人
						c.transports[i].SetPeers(make(map[int]string))
					} else {
						// 其他人移除 Leader
						newPeers := make(map[int]string)
						for id, addr := range c.peerMap {
							if id != partitionedNodeID {
								newPeers[id] = addr
							}
						}
						c.transports[i].SetPeers(newPeers)
					}
				}

				// 2. 等待多数派分区选出新 Leader
				t.Log("Waiting for new leader in majority partition...")
				time.Sleep(5 * time.Second)

				var newLeader *raft.Raft
				var majorityNodes []*raft.Raft
				for _, node := range c.nodes {
					if node.ID() != partitionedNodeID {
						majorityNodes = append(majorityNodes, node)
					}
				}

				// 在多数派中寻找新 Leader
				probeCmd := param.KVCommand{Op: param.OpGet, Key: "probe-key"}
				probeCmdBytes, _ := json.Marshal(probeCmd)
				foundLeader := false
				for i := 0; i < 20 && !foundLeader; i++ {
					time.Sleep(200 * time.Millisecond)
					for _, node := range majorityNodes {
						reply := &param.ClientReply{}
						_ = node.ClientRequest(&param.ClientArgs{Command: probeCmdBytes}, reply)
						if !reply.NotLeader {
							if reply.Success {
								newLeader = node
								foundLeader = true
								break
							}
							// "key not found" 也意味着它是 Leader 并成功查询了状态机
							if errMsg, ok := reply.Result.(string); ok && errMsg == "key not found" {
								newLeader = node
								foundLeader = true
								break
							}
						}
					}
				}

				if newLeader == nil {
					t.Fatal("Majority partition failed to elect a new leader")
				}

				assert.NotEqual(t, partitionedNodeID, newLeader.ID())
				t.Logf("New Leader in majority partition: Node %d", newLeader.ID())

				// 3. 验证新 Leader 可写
				cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "partition-key", Value: "val"})
				reply := &param.ClientReply{}
				_ = newLeader.ClientRequest(&param.ClientArgs{Command: cmd}, reply)
				assert.True(t, reply.Success)

				// 4. 恢复分区 (Healing)
				t.Log("Healing partition...")
				for i := 0; i < 3; i++ {
					c.transports[i].SetPeers(c.peerMap)
				}

				// 5. 验证旧 Leader 同步了数据
				time.Sleep(3 * time.Second)
				for i, node := range c.nodes {
					if node.ID() == partitionedNodeID {
						val, err := c.stateMachines[i].Get("partition-key")
						assert.NoError(t, err)
						assert.Equal(t, "val", val)
					}
				}
			})
		}
	}
}

// TestCluster_ConcurrentClientRequests 测试高并发客户端请求。
// 验证 Raft 在压力下不会出现数据竞争或死锁。
func TestCluster_ConcurrentClientRequests(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				t.Logf("Leader: Node %d", leader.ID())

				const concurrentRequests = 50
				var wg sync.WaitGroup
				wg.Add(concurrentRequests)

				// 并发启动 50 个协程发送请求
				for i := 0; i < concurrentRequests; i++ {
					go func(seq int) {
						defer wg.Done()
						key := fmt.Sprintf("concurrent-key-%d", seq)
						value := fmt.Sprintf("value-%d", seq)
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
						cmdBytes, _ := json.Marshal(cmd)

						args := &param.ClientArgs{
							ClientID:    int64(100 + seq),
							SequenceNum: 1,
							Command:     cmdBytes,
						}
						reply := &param.ClientReply{}
						err := leader.ClientRequest(args, reply)
						assert.NoError(t, err)
						assert.True(t, reply.Success)
					}(i)
				}

				wg.Wait()

				t.Log("Verifying data consistency after concurrent requests...")
				time.Sleep(2 * time.Second)

				// 验证所有 Key 都正确写入
				for i := 0; i < concurrentRequests; i++ {
					key := fmt.Sprintf("concurrent-key-%d", i)
					expectedValue := fmt.Sprintf("value-%d", i)

					// 检查 Leader
					leaderVal, err := c.stateMachines[leader.ID()-1].Get(key)
					assert.NoError(t, err)
					assert.Equal(t, expectedValue, leaderVal)

					// 检查 Followers
					for j := 0; j < 3; j++ {
						if c.nodes[j].ID() == leader.ID() {
							continue
						}
						followerVal, err := c.stateMachines[j].Get(key)
						assert.NoError(t, err)
						assert.Equal(t, expectedValue, followerVal)
					}
				}
			})
		}
	}
}

// TestCluster_LogReplication 测试大量连续日志复制和混合并发写入。
func TestCluster_LogReplication(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				t.Logf("Leader elected: Node %d", leader.ID())

				// 1. 顺序写入 50 条日志
				logCount := 50
				t.Logf("Writing %d logs sequentially...", logCount)
				for i := 0; i < logCount; i++ {
					key := fmt.Sprintf("seq-key-%d", i)
					value := fmt.Sprintf("seq-val-%d", i)
					cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: value})

					reply := &param.ClientReply{}
					err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, reply)
					assert.NoError(t, err)
					assert.True(t, reply.Success)
				}

				// 2. 并发写入 20 条日志
				t.Log("Writing 20 logs concurrently...")
				var wg sync.WaitGroup
				concurrency := 20
				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						key := fmt.Sprintf("conc-key-%d", idx)
						value := fmt.Sprintf("conc-val-%d", idx)
						cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: value})

						reply := &param.ClientReply{}
						err := leader.ClientRequest(&param.ClientArgs{ClientID: int64(100 + idx), SequenceNum: 1, Command: cmd}, reply)
						assert.NoError(t, err)
						assert.True(t, reply.Success)
					}(i)
				}
				wg.Wait()

				// 3. 验证最终一致性
				t.Log("Verifying data consistency...")
				time.Sleep(2 * time.Second)

				for i := 0; i < 3; i++ {
					for j := 0; j < logCount; j++ {
						key := fmt.Sprintf("seq-key-%d", j)
						val, err := c.stateMachines[i].Get(key)
						assert.NoError(t, err)
						assert.Equal(t, fmt.Sprintf("seq-val-%d", j), val)
					}
					for j := 0; j < concurrency; j++ {
						key := fmt.Sprintf("conc-key-%d", j)
						val, err := c.stateMachines[i].Get(key)
						assert.NoError(t, err)
						assert.Equal(t, fmt.Sprintf("conc-val-%d", j), val)
					}
				}
			})
		}
	}
}

// TestCluster_TakeSnapshot 测试手动触发快照。
// 验证快照操作不会导致数据丢失，且后续写入依然正常。
func TestCluster_TakeSnapshot(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				t.Logf("Leader elected: Node %d", leader.ID())

				// 1. 写入足够多的数据
				logCount := 30
				t.Logf("Writing %d logs...", logCount)
				for i := 0; i < logCount; i++ {
					key := fmt.Sprintf("snap-key-%d", i)
					value := fmt.Sprintf("snap-val-%d", i)
					cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: value})

					reply := &param.ClientReply{}
					err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, reply)
					assert.NoError(t, err)
					assert.True(t, reply.Success)
				}

				time.Sleep(1 * time.Second)

				// 2. 触发快照 (压缩日志)
				t.Log("Taking snapshot on Leader...")
				leader.TakeSnapshot()

				// 3. 验证数据依然存在
				for i := 0; i < logCount; i++ {
					key := fmt.Sprintf("snap-key-%d", i)
					val, err := c.stateMachines[leader.ID()-1].Get(key)
					assert.NoError(t, err)
					assert.Equal(t, fmt.Sprintf("snap-val-%d", i), val)
				}

				// 4. 验证后续写入
				newKey := "post-snap-key"
				newValue := "post-snap-value"
				cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: newKey, Value: newValue})
				reply := &param.ClientReply{}
				err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 100, Command: cmd}, reply)
				assert.NoError(t, err)
				assert.True(t, reply.Success)

				time.Sleep(500 * time.Millisecond)
				val, err := c.stateMachines[leader.ID()-1].Get(newKey)
				assert.Equal(t, newValue, val)
			})
		}
	}
}

// TestCluster_InstallSnapshot 测试 InstallSnapshot RPC。
// 场景：Follower 落后太多，Leader 已删除了需要的日志，只能发送快照进行同步。
func TestCluster_InstallSnapshot(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				leaderID := leader.ID()
				t.Logf("Leader: Node %d", leaderID)

				// 1. 隔离一个 Follower
				var follower *raft.Raft
				for _, node := range c.nodes {
					if node.ID() != leaderID {
						follower = node
						break
					}
				}
				if follower == nil {
					t.Fatal("Could not find a follower node")
				}
				t.Logf("Isolating Follower: Node %d", follower.ID())

				// 切断网络
				c.transports[follower.ID()-1].SetPeers(make(map[int]string))
				newPeers := make(map[int]string)
				for id, addr := range c.peerMap {
					if id != follower.ID() {
						newPeers[id] = addr
					}
				}
				for i, node := range c.nodes {
					if node.ID() != follower.ID() {
						c.transports[i].SetPeers(newPeers)
					}
				}

				// 2. Leader 写入数据并快照 (这会删除旧日志)
				snapLogCount := 20
				t.Logf("Writing %d logs to be snapshotted...", snapLogCount)
				for i := 0; i < snapLogCount; i++ {
					key := fmt.Sprintf("k-%d", i)
					cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: "v"})
					err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, &param.ClientReply{})
					assert.NoError(t, err)
				}

				t.Log("Leader taking snapshot...")
				leader.TakeSnapshot()

				// 3. 继续写入新数据 (Follower 需要先装快照再装这部分日志)
				extraLogCount := 10
				t.Logf("Writing %d extra logs after snapshot...", extraLogCount)
				for i := snapLogCount; i < snapLogCount+extraLogCount; i++ {
					key := fmt.Sprintf("k-%d", i)
					cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: "v"})
					err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, &param.ClientReply{})
					assert.NoError(t, err)
				}

				// 4. 恢复 Follower 连接
				t.Logf("Reconnecting Follower Node %d...", follower.ID())
				for i := 0; i < 3; i++ {
					c.transports[i].SetPeers(c.peerMap)
				}

				// 5. 等待同步
				t.Log("Waiting for Follower to catch up via Snapshot + Logs...")
				time.Sleep(3 * time.Second)

				// 6. 验证数据 (包括快照中的数据和之后日志的数据)
				val, err := c.stateMachines[follower.ID()-1].Get("k-0")
				assert.NoError(t, err)
				assert.Equal(t, "v", val)

				lastLogKey := fmt.Sprintf("k-%d", snapLogCount+extraLogCount-1)
				val, err = c.stateMachines[follower.ID()-1].Get(lastLogKey)
				assert.NoError(t, err)
				assert.Equal(t, "v", val)

				t.Log("InstallSnapshot test passed!")
			})
		}
	}
}

// TestCluster_Persistence_Restart 测试持久化存储。
// 验证节点重启后能否从磁盘/存储恢复状态，不丢失数据。
func TestCluster_Persistence_Restart(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				leaderID := leader.ID()
				t.Logf("Leader: Node %d", leaderID)

				// 1. 写入数据
				key := "persist-key"
				value := "persist-value"
				cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: value})
				reply := &param.ClientReply{}
				err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, reply)
				assert.NoError(t, err)
				assert.True(t, reply.Success)

				time.Sleep(1 * time.Second)

				// 2. 崩溃并重启 Leader
				t.Logf("Crashing Leader Node %d...", leaderID)
				c.restartNode(t, leaderID-1)

				time.Sleep(2 * time.Second)

				newLeader := c.getLeader(t)
				t.Logf("Cluster stabilized, new leader: %d", newLeader.ID())

				// 3. 验证数据恢复
				val, err := c.stateMachines[leaderID-1].Get(key)
				assert.NoError(t, err)
				assert.Equal(t, value, val, "Restarted node lost its state machine data!")

				lastIndex, _ := c.storages[leaderID-1].LastLogIndex()
				assert.Greater(t, lastIndex, uint64(0), "Restarted node lost its Raft logs!")

				// 4. 验证重启后依然能同步新数据
				key2 := "persist-key-2"
				cmd2, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key2, Value: "v2"})
				err = newLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply)
				assert.NoError(t, err)
				assert.True(t, reply.Success)

				time.Sleep(1 * time.Second)

				val2, err := c.stateMachines[leaderID-1].Get(key2)
				assert.NoError(t, err)
				assert.Equal(t, "v2", val2)
			})
		}
	}
}

// TestCluster_UnreliableNetwork_Churn 混沌测试
func TestCluster_UnreliableNetwork_Churn(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				// 使用 5 节点集群
				c := newClusterWithConfig(t, 5, 5, tr, st)
				defer c.shutdown()

				// 1. 启动持续写入的协程
				stopCh := make(chan struct{})
				var wg sync.WaitGroup
				wg.Add(1)

				go func() {
					defer wg.Done()
					i := 0
					for {
						select {
						case <-stopCh:
							return
						default:
							var leader *raft.Raft
							for _, n := range c.nodes {
								if !n.IsStopped() && n.State() == raft.Leader {
									leader = n
									break
								}
							}

							if leader != nil {
								key := fmt.Sprintf("churn-%d", i)
								cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: "v"})

								// 异步执行 ClientRequest，防止阻塞循环
								// 使用 goroutine 来发送请求，这样即使卡住也不会影响 stopCh 的响应
								go func(l *raft.Raft, clientID int64, seqNum int64, command []byte) {
									// 我们不关心单个请求的成功与否，只关心最终一致性
									_ = l.ClientRequest(param.NewClientArgs(clientID, seqNum, command), &param.ClientReply{})
								}(leader, 1, int64(i), cmd)
							}
							i++
							time.Sleep(50 * time.Millisecond)
						}
					}
				}()

				// 2. 启动 Chaos Monkey：随机断网
				testDuration := 10 * time.Second
				start := time.Now()

				t.Log("Starting network churn for 10 seconds...")
				for time.Since(start) < testDuration {
					target := rand.Intn(5)
					// 断网
					c.transports[target].SetPeers(make(map[int]string))

					time.Sleep(time.Duration(rand.Intn(300)+100) * time.Millisecond)

					// 恢复
					c.transports[target].SetPeers(c.peerMap)

					time.Sleep(time.Duration(rand.Intn(300)+100) * time.Millisecond)
				}

				// 3. 停止干扰，发送停止信号
				close(stopCh)
				// 等待写入协程退出（因为现在写入是异步发起的，wg.Wait() 会很快返回）
				wg.Wait()

				t.Log("Network churn stopped. Waiting for convergence...")
				// 确保所有网络恢复
				for i := 0; i < 5; i++ {
					c.transports[i].SetPeers(c.peerMap)
				}

				// 写入一个 Barrier 数据确保集群恢复可用
				// 我们需要重试几次，因为选主可能需要一点时间
				var finalLeader *raft.Raft
				barrierSuccess := false
				for retry := 0; retry < 10; retry++ {
					time.Sleep(1 * time.Second)
					finalLeader = c.getLeader(t)
					if finalLeader == nil {
						continue
					}
					barrierCmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "barrier", Value: "final"})
					// 这里用带超时的 context 模拟（如果你的 ClientRequest 支持 Context）
					// 或者简单地在一个短命 goroutine 里跑
					done := make(chan error, 1)
					go func() {
						done <- finalLeader.ClientRequest(&param.ClientArgs{ClientID: 9999, SequenceNum: 1, Command: barrierCmd}, &param.ClientReply{})
					}()

					select {
					case err := <-done:
						if err == nil {
							barrierSuccess = true
						}
					case <-time.After(1 * time.Second):
						// timeout
					}

					if barrierSuccess {
						t.Logf("Barrier entry committed on Leader %d", finalLeader.ID())
						break
					}
				}

				if !barrierSuccess {
					t.Fatal("Cluster failed to recover and commit barrier entry after churn")
				}

				// 4. 验证最终一致性
				time.Sleep(2 * time.Second)

				leader := c.getLeader(t)
				expectedLastIndex, _ := leader.Storage().LastLogIndex()
				t.Logf("Convergence Target: Leader LastLogIndex = %d", expectedLastIndex)

				for i := 0; i < 5; i++ {
					node := c.nodes[i]
					nodeID := node.ID()

					// 检查函数
					checkNode := func() error {
						idx, err := node.Storage().LastLogIndex()
						if err != nil {
							return fmt.Errorf("failed to get log index: %v", err)
						}
						if idx != expectedLastIndex {
							return fmt.Errorf("log index mismatch: expected %d, got %d", expectedLastIndex, idx)
						}

						// 验证状态机
						// 注意：由于我们在混沌期间是“盲写”的，我们不能确定 churn-0 是否被提交。
						// 但我们知道 barrier key 一定被提交了。
						val, err := c.stateMachines[i].Get("barrier")
						if err != nil || val != "final" {
							return fmt.Errorf("state machine mismatch for barrier: got %v, %v", val, err)
						}
						return nil
					}

					var lastErr error
					success := false
					for retry := 0; retry < 50; retry++ {
						if err := checkNode(); err == nil {
							success = true
							break
						} else {
							lastErr = err
							time.Sleep(100 * time.Millisecond)
						}
					}

					if !success {
						t.Fatalf("Node %d failed to converge after timeout. Last error: %v", nodeID, lastErr)
					}
				}

				t.Log("Chaos test passed: All nodes converged.")
			})
		}
	}
}

// TestCluster_MembershipChange 测试集群成员变更 (Configuration Change)。
// 验证动态添加和移除节点的功能。
func TestCluster_MembershipChange(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				// 启动 5 个物理节点，但初始 Raft 配置只包含 [1, 2, 3]
				c := newClusterWithConfig(t, 5, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				t.Logf("Initial Leader: Node %d (Config: [1, 2, 3])", leader.ID())

				// 1. 验证基本读写
				cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "k1", Value: "v1"})
				err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, &param.ClientReply{})
				assert.NoError(t, err)

				// 2. 动态添加 Node 4
				newPeersAdd := []int{1, 2, 3, 4}
				t.Log("Changing config: Adding Node 4 -> [1, 2, 3, 4]")

				configCmdAdd := param.NewConfigChangeCommand(newPeersAdd)
				reply := &param.ClientReply{}
				err = leader.ClientRequest(&param.ClientArgs{ClientID: 0, SequenceNum: 0, Command: configCmdAdd}, reply)
				assert.NoError(t, err)
				assert.True(t, reply.Success, "Config change (add node) failed")

				// 等待配置变更完成
				time.Sleep(3 * time.Second)

				// 3. 验证新节点 Node 4 已经同步数据
				val, err := c.stateMachines[3].Get("k1") // Index 3 is Node 4
				assert.NoError(t, err)
				assert.Equal(t, "v1", val)

				// 4. 验证新集群可写入
				leader = c.getLeader(t) // 重新获取 Leader，以防万一
				cmd2, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "k2", Value: "v2"})
				reply2 := &param.ClientReply{}
				err = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply2)
				assert.NoError(t, err)
				assert.True(t, reply2.Success)

				time.Sleep(1 * time.Second)
				val2, _ := c.stateMachines[3].Get("k2")
				assert.Equal(t, "v2", val2)

				// 5. 动态移除 Node 2
				newPeersRemove := []int{1, 3, 4}
				t.Log("Changing config: Removing Node 2 -> [1, 3, 4]")

				configCmdRemove := param.NewConfigChangeCommand(newPeersRemove)
				leader = c.getLeader(t) // 再次重新获取 Leader
				reply3 := &param.ClientReply{}
				err = leader.ClientRequest(&param.ClientArgs{ClientID: 0, SequenceNum: 0, Command: configCmdRemove}, reply3)
				assert.NoError(t, err)

				time.Sleep(2 * time.Second)

				// 停止被移除的节点
				c.nodes[1].Stop()
				_ = c.transports[1].Close()

				// 6. 验证移除后集群仍可写入
				leader = c.getLeader(t)
				cmd3, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "k3", Value: "v3"})
				reply4 := &param.ClientReply{}
				err = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 3, Command: cmd3}, reply4)
				assert.NoError(t, err)
				assert.True(t, reply4.Success)

				// 7. 验证最终一致性（使用重试机制等待 Follower 同步）
				targetNodes := []int{0, 2, 3} // Node 1, 3, 4
				for _, idx := range targetNodes {
					nodeID := idx + 1
					var val string
					var err error

					success := false
					for i := 0; i < 50; i++ {
						val, err = c.stateMachines[idx].Get("k3")
						if err == nil && val == "v3" {
							success = true
							break
						}
						time.Sleep(100 * time.Millisecond)
					}

					if !success {
						t.Fatalf("Node %d failed to receive new data 'v3'. Last error: %v, Last Val: %s", nodeID, err, val)
					}
					t.Logf("Node %d successfully synced 'v3'", nodeID)
				}
			})
		}
	}
}

// TestCluster_FullClusterRestart 测试全集群崩溃恢复 (Disaster Recovery)。
// 模拟整个数据中心断电重启。
func TestCluster_FullClusterRestart(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				// 必须使用持久化存储
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)

				// 写入关键数据
				cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "critical", Value: "data"})
				err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, &param.ClientReply{})
				assert.NoError(t, err)
				time.Sleep(1 * time.Second)

				// 模拟全集群崩溃
				t.Log("Simulating full cluster crash...")
				for i := 0; i < 3; i++ {
					c.nodes[i].Stop()
					_ = c.transports[i].Close()
				}

				// 重启所有节点
				t.Log("Restarting all nodes...")
				for i := 0; i < 3; i++ {
					id := i + 1
					prevAddr := c.peerMap[id]

					var newTrans transport.Transport
					newTrans, _ = transport.NewTransport(c.transportType, prevAddr)
					c.transports[i] = newTrans

					// 重新加载存储
					store, sm, err := storage.NewStorage(c.storageType, c.dataDir, id)
					if err != nil {
						t.Fatalf("failed to reload storage: %v", err)
					}

					c.storages[i] = store
					c.stateMachines[i] = sm

					peerIDs := []int{1, 2, 3}

					newRaft := raft.NewRaft(id, peerIDs, store, sm, newTrans, c.commitChans[i])
					c.nodes[i] = newRaft

					newTrans.SetPeers(c.peerMap)
					newTrans.RegisterRaft(newRaft)
					if err := newTrans.Start(); err != nil {
						t.Fatalf("failed to start transport for node %d: %v", id, err)
					}
					go newRaft.Run()
				}

				t.Log("Waiting for cluster to recover...")
				time.Sleep(3 * time.Second)

				newLeader := c.getLeader(t)
				t.Logf("Cluster recovered. New Leader: %d", newLeader.ID())

				// 验证数据恢复
				val, err := c.stateMachines[newLeader.ID()-1].Get("critical")
				assert.NoError(t, err)
				assert.Equal(t, "data", val, "Data should persist after full cluster restart")

				// 验证集群恢复服务
				cmd2, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "new-data", Value: "after-crash"})
				reply := &param.ClientReply{}
				err = newLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply)
				assert.NoError(t, err)
				assert.True(t, reply.Success)
			})
		}
	}
}

// TestCluster_StaleRead 测试陈旧读 (Stale Read) 防护。
// 验证被隔离的旧 Leader 无法服务读请求。
func TestCluster_StaleRead(t *testing.T) {
	transports := []string{transport.TcpTransport, transport.GrpcTransport}
	storages := []string{storage.InmemoryStorage, storage.SimpleFileStorage, storage.LSMStorage}

	for _, tr := range transports {
		for _, st := range storages {
			t.Run(fmt.Sprintf("%s_%s", tr, st), func(t *testing.T) {
				c := newClusterWithConfig(t, 3, 3, tr, st)
				defer c.shutdown()

				leader := c.getLeader(t)
				leaderID := leader.ID()
				t.Logf("Original Leader: %d", leaderID)

				cmd, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "k", Value: "v"})
				err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, &param.ClientReply{})
				assert.NoError(t, err)

				t.Log("Isolating old leader...")
				c.transports[leaderID-1].SetPeers(make(map[int]string))

				newPeers := make(map[int]string)
				for id, addr := range c.peerMap {
					if id != leaderID {
						newPeers[id] = addr
					}
				}
				for i := 0; i < 3; i++ {
					if c.nodes[i].ID() != leaderID {
						c.transports[i].SetPeers(newPeers)
					}
				}

				time.Sleep(3 * time.Second)

				t.Log("Sending read request to isolated (old) leader...")

				readCmd, _ := json.Marshal(param.KVCommand{Op: param.OpGet, Key: "k"})
				reply := &param.ClientReply{}

				// 异步发送请求，并设置超时
				done := make(chan bool)
				go func() {
					// 这个请求应该要么失败，要么被 ReadIndex 阻塞住
					err := leader.ClientRequest(&param.ClientArgs{ClientID: 2, SequenceNum: 1, Command: readCmd}, reply)
					if err == nil {
						done <- true
					}
				}()

				select {
				case <-done:
					// 如果请求返回了，必须确保它没有成功（Success=false）或者返回了 NotLeader
					assert.False(t, reply.Success, "Old leader should not respond successfully to read requests.")
				case <-time.After(2 * time.Second):
					t.Log("Pass: Old leader timed out (likely stuck in ReadIndex check).")
				}
			})
		}
	}
}

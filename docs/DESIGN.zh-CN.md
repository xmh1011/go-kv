# go-kv 系统设计

English version: [DESIGN.md](DESIGN.md)

本文按模块解释整个 `go-kv` 系统。它面向第一次接触分布式存储的新读者，因此会先解释请求如何流动，再逐个拆解模块。

## 1. 系统目标

`go-kv` 提供一个复制式键值存储。用户可以向集群 leader 写入 key，之后用线性一致语义从 leader 读取这个 key。

系统要解决四个问题：

1. **复制**：每个已提交写入都必须复制到足够多节点。
2. **容错**：少数节点故障时，集群仍能继续服务。
3. **持久化**：已提交数据和 Raft 元数据需要在进程重启后恢复。
4. **压缩**：日志和磁盘数据不能无限增长。

## 2. 主要模块

| 模块 | 路径 | 职责 |
|---|---|---|
| 服务端入口 | `cmd/server` | 加载配置，创建存储和传输，启动 Raft。 |
| 客户端入口 | `cmd/client` | 解析 CLI 命令并发送到集群。 |
| 客户端库 | `pkg/client` | 重试请求，跟随 leader hint，分配客户端序列号。 |
| 配置 | `pkg/config` | 将 YAML 和环境变量加载到 `config.Conf`。 |
| 日志 | `pkg/log` | 提供分级日志，默认保持低噪音。 |
| 共享参数 | `pkg/param` | 定义命令、日志条目、快照和 RPC 消息。 |
| 传输层 | `pkg/transport` | 抽象 gRPC、TCP、内存 RPC。 |
| Raft | `raft` | 负责选举、日志复制、提交、应用、读、快照。 |
| 存储抽象 | `pkg/storage` | 定义 Raft 稳定存储和状态机接口。 |
| LSM 引擎 | `engine/lsm` | 提供 WAL、MemTable、SSTable、布隆过滤器、Compaction。 |
| 测试 | `tests` | 运行真实多节点集群和性能场景。 |

## 3. 写请求流程

一次写请求会走完整 Raft 路径：

```text
kv-client set k v
        |
        v
pkg/client 发送 ClientRequest
        |
        v
leader raft.ClientRequest
        |
        v
包装成 ClientCommand(ClientID, SequenceNum, payload)
        |
        v
追加 LogEntry 到稳定存储
        |
        v
通过 AppendEntries 复制给 follower
        |
        v
多数派确认该 entry
        |
        v
leader 推进 commitIndex
        |
        v
apply loop 将命令应用到状态机
        |
        v
等待中的客户端收到 apply 结果
```

对新手来说，最重要的一点是：写请求不是 leader 收到就完成。它必须被 Raft 提交，并且本地状态机已经应用后，才算真正完成。

## 4. 读请求流程

读请求不能返回旧数据。`go-kv` 使用 ReadIndex 或 lease 确认保护读请求：

```text
kv-client get k
        |
        v
leader raft.ClientRequest
        |
        v
确认当前节点仍是 leader
        |
        v
记录 readIndex = 当前 commitIndex
        |
        v
等待 lastApplied >= readIndex
        |
        v
stateMachine.Get(k)
```

也就是说，leader 必须先证明自己仍有效，再确认状态机已经应用到读请求开始时的提交点。

## 5. 服务端生命周期

`cmd/server/main.go` 负责把运行时组件组装起来：

1. 通过 `pkg/config` 加载 YAML。
2. 初始化日志。
3. 根据 `raft.peers` 构造 peer map。
4. 通过 `storage.NewStorage` 创建存储。
5. 通过 `transport.NewTransport` 创建传输层。
6. 创建 `raft.Raft` 节点。
7. 将 Raft 节点注册到传输层。
8. 启动传输监听和 Raft 主循环。
9. 从 `commitChan` 消费提交通知。
10. 收到进程信号后停止 Raft、传输层和存储。

服务端不会从 `commitChan` 再次应用日志。Raft 模块在发送提交通知前已经完成应用。该 channel 主要用于观察和避免内部提交通知阻塞。

## 6. 客户端模块

`cmd/client` 会把用户命令转换为 `param.KVCommand`：

```go
type KVCommand struct {
    Op    OpType `json:"op"`
    Key   string `json:"key"`
    Value string `json:"value"`
}
```

`pkg/client` 负责面向集群的行为：

- 选择目标节点；
- 通过传输层发送请求；
- 处理 `NotLeader` 和 leader hint；
- 对临时失败进行重试；
- 附加稳定 client ID 和递增 sequence number。

sequence number 很重要，因为客户端可能在超时后重试。Raft 日志里可以出现同一个逻辑请求的重复条目，但状态机必须只应用一次。

## 7. 配置与日志

配置由 `pkg/config` 中的 Viper 加载。

代码中有默认值，YAML 可以覆盖默认值。环境变量使用 `GO_KV_` 前缀，并把点号转换为下划线：

```bash
GO_KV_LOG_LEVEL=debug
GO_KV_RAFT_READ_INDEX_MODE=heartbeat
```

默认日志级别是 `warn`。心跳进度、复制进度、Compaction 进度等高频运行信息通常应该保持在 `debug`。`warn` 和 `error` 应该用于需要关注的问题。

## 8. 传输层模块

Raft 不直接调用 gRPC 或 TCP，而是依赖接口：

```go
type Transport interface {
    Addr() string
    SetPeers(peers map[int]string)
    RegisterRaft(raftInstance api.RaftService)
    Start() error
    Close() error
    SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error
    SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error
    SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error
    SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error
}
```

这样可以让 Raft 逻辑不依赖具体网络协议。测试可以使用内存传输，本地集群默认使用 gRPC。

## 9. Raft 模块

Raft 负责复制日志。主要文件：

- `raft/raft.go`：节点状态、客户端请求、批处理、ReadIndex 辅助逻辑；
- `raft/election.go`：PreVote 和 RequestVote；
- `raft/replication.go`：AppendEntries、复制进度、提交与应用；
- `raft/snapshot.go`：本地快照和 InstallSnapshot RPC。

最重要的状态变量：

- `currentTerm` 和 `votedFor`：持久化选举状态；
- `commitIndex`：已知被提交的最高日志索引；
- `lastApplied`：已经应用到状态机的最高日志索引；
- `nextIndex[peer]`：下一次要发给某个 follower 的日志索引；
- `matchIndex[peer]`：某个 follower 已确认复制的最高日志索引。

Raft 通过 `storage.Storage` 接口持久化数据。它不关心实现是内存、文件还是 LSM。

## 10. 存储抽象

`pkg/storage/storage.go` 定义两个关键接口：

- `Storage`：持久化 Raft 元数据、日志条目和快照。
- `StateMachine`：应用已提交命令的业务状态机。

当 `raft.engine = "lsm"` 时，每个节点会创建两个独立的 LSM 数据库：

```text
data/node-1/
├── lsm_raftlog/        # Raft HardState、日志条目、快照
└── lsm_statemachine/   # 用户键值数据
```

这样可以避免共识元数据和用户数据混在一起，也让快照和恢复逻辑更容易理解。

## 11. LSM 引擎模块

LSM 引擎位于 `engine/lsm`。

写入路径：

```text
Database.Put/Delete
        |
        v
MemTable.Insert 先写 WAL
        |
        v
插入跳表
        |
        v
MemTable 满后提升为 immutable MemTable
        |
        v
flush immutable MemTable 到 Level-0 SSTable
        |
        v
调度后台 compaction 将数据移动到更低层级
```

读取路径从新到旧：

1. 活跃 MemTable；
2. immutable MemTable，从新到旧；
3. Level-0 SSTable，从新到旧；
4. Level-1 及更深层，通过稀疏索引和布隆过滤器定位。

删除使用 tombstone 表示。tombstone 必须遮蔽旧值，直到 compaction 能证明旧值不会再出现。

Flush 和 compaction 的职责是分开的。前台写路径只需要发布一个持久化的 Level-0
SSTable，并在发布成功后删除 immutable memtable 的 WAL。更大的 SSTable merge
通过合并调度的后台 compaction worker 执行，这样 LSM compaction 不会进入 Raft
apply 关键路径。

## 12. 持久化与恢复

Raft 需要从进程崩溃中恢复。恢复依赖三层数据：

1. **Raft HardState**：当前任期、投票对象、提交索引。
2. **Raft 日志条目**：通过选择的存储后端持久化。
3. **状态机快照/数据**：键值状态机的当前数据。

LSM-backed Raft 日志使用带 magic header 的紧凑二进制格式。这避免了每次追加/读取日志都走 gob 反射路径。

恢复边界必须非常明确：

- Raft 会在启动 apply loop 前恢复持久化的 `commitIndex`，这样重启前已经
  durable 且 committed 的 entry 会在节点恢复后继续被 apply。
- MemTable recovery 只重放命名为 `{id}.wal` 的已提交 WAL 文件。WAL 目录里的
  临时文件、目录和无关文件会被忽略。
- SSTable recovery 只加载已提交的 `.sst` 文件，忽略未提交的临时文件，并移除
  没有可恢复数据的旧空 SSTable。
- SSTable 发布先在同目录写临时文件，sync 并 close 后 rename 成最终 `.sst`
  名称，然后再发布内存 metadata。读者应该看到旧 catalog 或新 catalog，
  不能看到半写入文件。
- MemTable 和 SSTable ID 分配是每个 manager 实例本地的。恢复另一个数据库
  不能重置仍在运行的数据库 ID。

## 13. 快照

快照用于限制 Raft 日志增长。

安全顺序是：

1. Raft 发现持久化日志大小超过阈值。
2. Raft 捕获 `lastApplied` 和该索引的 term。
3. 状态机导出这个已应用状态对应的快照。
4. Raft 持久化快照。
5. Raft 压缩快照覆盖的日志。
6. 落后 follower 可以通过 InstallSnapshot 接收快照。

关键安全规则：Raft 不能压缩尚未被持久化状态机快照覆盖的日志。

## 14. 测试策略

仓库有多个层级的测试：

- 包级单元测试，覆盖数据结构和边界条件；
- 使用 mock storage/transport 的 Raft 测试；
- 使用真实 LSM 文件的存储测试；
- 真实集群集成测试；
- 端到端性能测试；
- 包含重启、快照和一致性检查的长时间端到端测试。

日常修改建议运行：

```bash
make test
make integration-test
```

涉及 storage/Raft 的修改还应运行：

```bash
go test -race ./pkg/storage/lsm ./raft ./engine/lsm/... ./pkg/storage/... ./pkg/param
```

生产式验证运行：

```bash
make long-test
```

## 15. 设计不变量

读代码和修改代码时，下面这些不变量非常重要：

- `lastApplied` 只能在状态机真正应用对应命令之后推进。
- 读请求必须等待 `lastApplied >= readIndex`。
- election 和 ReadIndex timeout 必须覆盖健康传输 RPC 的超时预算。
- `(ClientID, SequenceNum)` 标识的客户端命令最多应用一次。
- pending client request 不能仅因为某次 leader 侧 apply 等待超时就被删除；
  重试需要重新绑定到原始 log index。
- follower 落后到已压缩日志之前时，应该发送快照，而不是继续补缺失日志。
- 正在 flush 的 immutable memtable 必须保持可搜索，直到 SSTable 安全发布。
- tombstone 必须遮蔽旧值，直到 compaction 可以安全丢弃它。
- 从读者视角看，SSTable 元数据更新必须是原子的。
- LSM flush 可以在前台发布 Level-0 SSTable，但 compaction 必须在 Raft apply
  前台路径之外运行。
- WAL recovery 必须忽略非提交目录项；但如果某个已提交 `{id}.wal` 文件内容
  损坏，仍然必须失败。
- Raft `Stop()` 必须等待 in-flight apply、snapshot、AppendEntries 和
  state-machine storage 临界区结束后，调用方才能关闭或重新打开存储。

保持这些不变量比优化某一条局部路径更重要。

## 16. 实现细节阅读地图

本文是总览设计文档，故意保持在系统层面。排查核心存储或共识问题时，应继续阅读实现级设计文档：

| 主题 | 文档 | 重点内容 |
|---|---|---|
| Raft 状态归属 | [RAFT.zh-CN.md](RAFT.zh-CN.md) | 哪些字段是论文状态，哪些字段是实现防线，以及每个字段由哪把锁保护。 |
| AppendEntries 和 apply | [RAFT.zh-CN.md](RAFT.zh-CN.md) | Follower AppendEntries 分阶段、leader 复制进度、committed entry apply 流程、ReadIndex 等待规则。 |
| Snapshot compaction | [RAFT.zh-CN.md](RAFT.zh-CN.md) | 本地快照和 InstallSnapshot 的精确锁顺序。 |
| SSTable 文件格式 | [LSM.zh-CN.md](LSM.zh-CN.md) | 物理文件顺序、footer 布局、DataBlock 懒加载、index/value 配对。 |
| LSM 文件目录 | [LSM.zh-CN.md](LSM.zh-CN.md) | Level 0 顺序、稀疏索引、compaction 元数据和恢复之间的关系。 |
| LSM 中的 Raft 日志 | [LSM.zh-CN.md](LSM.zh-CN.md) | `meta:*` key、固定宽度 `log:*` key 和 `GLG1` 二进制日志格式。 |
| 运行系统 | [USAGE.zh-CN.md](USAGE.zh-CN.md) | 构建、本地集群、CLI、配置、测试和 release tag。 |
| Go SDK 使用 | [SDK.zh-CN.md](SDK.zh-CN.md) | 应用侧包、client 调用、命令类型、transport 设置和重试语义。 |
| 长测证据 | [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md) | 最新 10 分钟 E2E 结果、一致性门禁和复现命令。 |
| 故障复盘 | [BUG_FIX_RETROSPECTIVE.zh-CN.md](BUG_FIX_RETROSPECTIVE.zh-CN.md) | 最新内核修复的根因、Raft/LSM 原理和经验。 |

如果端到端测试暴露一致性 bug，应先按这张地图定位问题边界，再改代码。严重问题通常来自 Raft 复制进度、snapshot compaction 和 LSM 可见性之间的边界被破坏。

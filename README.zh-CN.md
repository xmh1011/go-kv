# go-kv

`go-kv` 是一个用 Go 编写的分布式、容错键值存储系统。它使用 Raft 共识算法在多个节点之间复制命令，并使用 LSM-tree 存储引擎高效持久化用户数据和 Raft 日志数据。

English documentation: [README.md](README.md)

## 这个项目实现了什么

`go-kv` 的代码规模适合学习，但包含真实分布式数据库中常见的核心模块：

- 支持 leader 选举、日志复制、快照、ReadIndex 读和联合共识成员变更的 Raft 集群。
- 可插拔存储层，包含内存、简单文件和 LSM 后端。
- 自研 LSM-tree 引擎，包含 WAL、MemTable、Immutable MemTable、SSTable、布隆过滤器、稀疏索引和 Compaction。
- 可插拔传输层，包含 gRPC、TCP 和内存传输。
- 使用真实 gRPC 与 LSM 存储的多节点端到端测试。

## 文档地图

如果你是第一次阅读代码，建议先看总设计文档。

| 主题 | 英文 | 中文 |
|---|---|---|
| 总体系统设计 | [docs/DESIGN.md](docs/DESIGN.md) | [docs/DESIGN.zh-CN.md](docs/DESIGN.zh-CN.md) |
| Raft 设计 | [docs/RAFT.md](docs/RAFT.md) | [docs/RAFT.zh-CN.md](docs/RAFT.zh-CN.md) |
| LSM-tree 设计 | [docs/LSM.md](docs/LSM.md) | [docs/LSM.zh-CN.md](docs/LSM.zh-CN.md) |
| 使用指南 | [docs/USAGE.md](docs/USAGE.md) | [docs/USAGE.zh-CN.md](docs/USAGE.zh-CN.md) |
| Go SDK 接口文档 | [docs/SDK.md](docs/SDK.md) | [docs/SDK.zh-CN.md](docs/SDK.zh-CN.md) |
| 性能报告 | [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | [docs/PERFORMANCE.zh-CN.md](docs/PERFORMANCE.zh-CN.md) |
| Bug 修复复盘 | [docs/BUG_FIX_RETROSPECTIVE.md](docs/BUG_FIX_RETROSPECTIVE.md) | [docs/BUG_FIX_RETROSPECTIVE.zh-CN.md](docs/BUG_FIX_RETROSPECTIVE.zh-CN.md) |

## 架构概览

系统主要分为五层：

```text
CLI client / server
        |
        v
客户端命令与配置层
        |
        v
Raft 共识模块
        |
        +--------------------+
        |                    |
        v                    v
Raft 稳定存储          状态机
        |                    |
        +---------+----------+
                  v
              LSM 引擎
```

服务端接收客户端命令，Raft leader 将写命令复制到多数派节点，提交后的命令会应用到键值状态机。读请求通过 ReadIndex 或 lease 确认 leader 仍然有效，从而避免读到旧数据。

## 仓库结构

```text
go-kv/
├── cmd/
│   ├── client/             # CLI 客户端入口
│   └── server/             # 服务端入口和生命周期管理
├── conf/                   # 本地集群示例 YAML 配置
├── docs/                   # 设计文档和性能报告
├── engine/lsm/             # 自研 LSM-tree 存储引擎
│   ├── database/           # 数据库门面
│   ├── kv/                 # key/value 编码与 tombstone
│   ├── memtable/           # 活跃与不可变 MemTable
│   ├── sstable/            # SSTable、布隆过滤器、Compaction
│   └── wal/                # 预写日志
├── pkg/
│   ├── client/             # 带重试的客户端逻辑
│   ├── config/             # 基于 Viper 的配置加载
│   ├── log/                # 日志封装
│   ├── param/              # RPC 与命令共享类型
│   ├── storage/            # Raft 存储与状态机接口
│   └── transport/          # gRPC、TCP、内存传输
├── raft/                   # Raft 选举、复制、快照逻辑
├── scripts/                # 辅助脚本
└── tests/                  # 集成、端到端、基准和长时间测试
```

## 环境要求

- 推荐 Go 1.25 或更高版本。
- 使用 `make` 运行标准构建和测试命令。
- 只有在需要重新生成 gRPC 代码时，才需要 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`。

## 构建

```bash
make build
```

该命令会生成：

- `kv-server`
- `kv-client`

## 启动本地三节点集群

启动默认本地集群：

```bash
make cluster
```

该命令会使用下面三个配置文件启动三个服务端进程：

- `conf/config-1.yaml`
- `conf/config-2.yaml`
- `conf/config-3.yaml`

停止集群：

```bash
make stop-cluster
```

清理生成的二进制、日志、测试产物和本地数据：

```bash
make clean
```

## 使用 CLI 客户端

客户端默认从 `conf/config.yaml` 读取集群节点列表。

写入 key：

```bash
./kv-client set mykey "hello world"
```

读取 key：

```bash
./kv-client get mykey
```

删除 key：

```bash
./kv-client delete mykey
```

指定其他配置文件：

```bash
./kv-client --config conf/config-1.yaml get mykey
```

## 配置

主要配置分为三块：

```yaml
log:
  level: "warn"

raft:
  id: 1
  transport: "grpc"
  engine: "lsm"
  data_dir: "./data"
  heartbeat_timeout: 50ms
  election_timeout: 200ms
  snapshot_threshold: 8192
  read_index_mode: "lease"
  peers:
    - id: 1
      address: "127.0.0.1:8001"

lsm:
  max_mem_table_size: 2097152
  max_sstable_size: 2097152
```

环境变量可以覆盖配置项。例如：

```bash
GO_KV_LOG_LEVEL=debug make e2e-test
```

默认日志级别是 `warn`。排查 Raft 选举、复制、ReadIndex、LSM flush、compaction 或传输细节时可以切到 `debug`。

## 测试命令

运行 `tests/` 目录之外的单元测试：

```bash
make test
```

运行集成测试：

```bash
make integration-test
```

运行标准端到端性能测试：

```bash
make e2e-test
```

运行长时间端到端测试：

```bash
make long-test
```

运行基准测试：

```bash
make bench-test
```

## 存储模式

Raft 存储工厂支持三种模式：

| 模式 | 用途 |
|---|---|
| `inmemory` | 快速测试，进程退出后数据丢失。 |
| `simplefile` | 使用本地文件的小型持久化测试后端。 |
| `lsm` | 主要的生产式后端，使用自研 LSM 引擎。 |

当 `raft.engine` 为 `lsm` 时，每个节点会有两个独立的 LSM 数据库：

- 一个用于 Raft 元数据、日志条目和快照；
- 一个用于键值状态机。

这样可以让共识元数据和用户数据互不干扰。

## 传输模式

| 模式 | 用途 |
|---|---|
| `grpc` | 默认跨进程传输。 |
| `tcp` | 备用 RPC 传输。 |
| `inmemory` | 仅测试使用，不能跨进程。 |

独立 CLI 客户端不能使用 `inmemory`，因为它和服务端运行在不同进程中。

## 关键一致性保证

- 写请求只有在 leader 追加日志、复制到多数派、提交并应用到状态机后才返回成功。
- 读请求由 leader 处理，并通过 ReadIndex 或 lease 确认保护。
- 客户端写重试会携带 `(ClientID, SequenceNum)`，避免同一个状态机命令重复应用。
- 快照只有在状态机快照持久化后才压缩 Raft 日志。
- LSM 删除使用 tombstone，避免旧值在 compaction 或重启后复活。

## 推荐阅读顺序

如果你是新读者，建议按这个顺序阅读：

1. [总体系统设计](docs/DESIGN.zh-CN.md)
2. [Raft 设计](docs/RAFT.zh-CN.md)
3. [LSM-tree 设计](docs/LSM.zh-CN.md)
4. [使用指南](docs/USAGE.zh-CN.md)
5. [Go SDK 接口文档](docs/SDK.zh-CN.md)
6. [性能报告](docs/PERFORMANCE.zh-CN.md)
7. [Bug 修复复盘](docs/BUG_FIX_RETROSPECTIVE.zh-CN.md)
8. `tests/integration_test.go` 和 `tests/long_running_e2e_test.go`

测试通常是理解不同模块如何协作的最好入口。

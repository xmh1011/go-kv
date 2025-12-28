# go-kv: A Distributed Fault-Tolerant Key-Value Store

`go-kv` 是一个使用 Go 语言实现的、基于 Raft 共识算法的分布式、容错的键值存储系统。它内置了一个高性能的、基于 LSM 树的存储引擎，旨在提供高可用的数据存储服务。

## 核心特性

- **分布式共识**: 基于 [Raft](https://raft.github.io/) 算法实现数据复制和强一致性。
- **容错能力**: 在一个 N 个节点的集群中，可以容忍 `(N-1)/2` 个节点的故障。
- **高性能存储**: 内置基于 **LSM 树 (Log-Structured Merge-Tree)** 的存储引擎，优化写入性能。
- **可插拔后端**: 支持多种存储和网络传输后端，便于测试和扩展。
  - **存储层**: In-Memory, Simple File (JSON), LSM-Tree。
  - **网络层**: TCP, gRPC。
- **线性一致性读**: 通过 `ReadIndex` 协议确保读操作不会读到陈旧的数据。
- **动态成员变更**: 支持在集群运行时动态地增加或移除节点，无需停机。
- **日志压缩**: 通过快照机制定期压缩 Raft 日志，防止日志无限增长。

## 架构设计

`go-kv` 的架构分为清晰的几层：客户端、服务器、Raft 共识模块、存储层和网络传输层。

![Architecture Diagram](https://user-images.githubusercontent.com/1089339/209834158-c33a5b2e-7d2d-4b0d-8b0d-8e41c4a5f9e9.png)

### 1. Raft 共识模块 (`raft/`)

Raft 模块是系统的核心，负责保证数据在多个节点间的一致性。

- **状态**: 完整实现了 Raft 的三种状态：`Follower`, `Candidate`, `Leader`。
- **领导者选举**:
  - 使用随机化的选举超时来避免选举冲突（Split Vote）。
  - 实现了 **Pre-Vote** 阶段：在发起正式选举前，候选人会先进行一轮“预投票”，询问其他节点是否愿意为它投票。这可以防止一个网络隔离后又恢复的节点用一个旧的、但更高的任期号来干扰当前稳定的集群。
- **日志复制**:
  - Leader 接收客户端请求，将其作为日志条目（Log Entry）复制到所有 Follower。
  - 使用 `nextIndex` 和 `matchIndex` 数组来跟踪每个 Follower 的日志同步进度，并在需要时高效地修复不一致的日志。
- **安全性保证**:
  - **Leader Append-Only**: 只有 Leader 能追加日志。
  - **Log Matching Property**: 如果不同节点上的两条日志拥有相同的索引和任期号，那么它们之前的所有日志都完全相同。
  - **Leader Completeness**: 新当选的 Leader 必须包含所有已提交的日志。
  - **State Machine Safety**: 只有被多数派节点确认“提交”的日志，才能被应用到状态机。
- **线性一致性读 (Linearizable Reads)**:
  - 为了防止 Leader 从其本地状态机读到可能已经“过时”的数据（例如，在一个网络分区刚刚恢复的瞬间），系统实现了 `ReadIndex` 协议。
  - 当 Leader 收到一个读请求时，它会先向集群多数派确认自己的 Leader 地位仍然有效，然后等待本地状态机至少应用到当前已知的 `commitIndex`，最后才执行读操作。
- **动态成员变更**:
  - 采用 Raft 论文中描述的“联合共识”（Joint Consensus）方法，支持在不停止服务的情况下动态增删节点。
  - 变更过程分为两个阶段：首先提交一个包含新旧配置 `(C-old,new)` 的日志，当该日志被提交后，再提交一个只包含新配置 `(C-new)` 的日志，从而安全地完成过渡。

### 2. LSM 树存储引擎 (`engine/lsm/`)

为了实现高写入性能，`go-kv` 实现了一个 LSM 树存储引擎。其核心思想是将离散的、随机的写操作转化为批量的、顺序的写操作。

- **核心组件**:
  - **MemTable**: 一个在内存中的、有序的数据结构（本项目中为跳表）。所有新的写请求首先被写入这里。
  - **Write-Ahead Log (WAL)**: 所有写操作在写入 MemTable 之前，会先以追加的方式写入磁盘上的 WAL 文件。这确保了即使服务器崩溃，内存中的数据也能通过回放 WAL 来恢复。
  - **Immutable MemTable**: 当 MemTable 的大小达到阈值，它会变为一个只读的 Immutable MemTable，同时一个新的 MemTable 会被创建以服务新的写请求。后台线程会将这个 Immutable MemTable 的内容刷写到磁盘。
  - **SSTables (Sorted String Tables)**: Immutable MemTable 的数据被刷写到磁盘后，形成一个 SSTable 文件。SSTable 内部按 Key 排序且不可修改。
  - **分层结构 (Levels)**: SSTable 被组织在多个层级中（Level 0, Level 1, ...）。
    - **Level 0**: 直接由 MemTable 刷写而来，因此 Level 0 的 SSTable 文件之间可能存在重叠的键范围。
    - **Level 1+**: 在这些层级中，SSTable 文件之间的键范围保证互不重叠，这极大地优化了查找效率。
- **读写流程**:
  - **写 (Put/Delete)**:
    1.  记录到 WAL。
    2.  插入/更新到 MemTable。删除操作会写入一个特殊的“墓碑”标记。
  - **读 (Get)**:
    1.  从 MemTable 中查找。
    2.  如果未找到，从 Immutable MemTable 中查找。
    3.  如果仍未找到，从 Level 0 的 SSTable 中（按从新到旧的顺序）查找。
    4.  最后，从 Level 1 及更高层级中查找。由于 L1+ 层文件不重叠，可以快速定位到可能包含该 Key 的文件。
    5.  **布隆过滤器 (Bloom Filter)** 被用于快速判断一个 Key 是否 *可能* 存在于某个 SSTable 中，从而避免大量无效的磁盘 I/O。
- **合并 (Compaction)**:
  - 一个后台进程会持续地进行合并操作，这是 LSM 树的核心维护机制。
  - 它会选择某个层级的一个或多个 SSTable，与下一层级中与之键范围重叠的 SSTable 进行合并，生成新的 SSTable 文件。
  - 这个过程会物理地删除被覆盖的旧数据和被标记为“墓碑”的数据，从而回收磁盘空间，并优化未来的读取性能。

## 快速开始

### 1. 环境准备

- Go 1.21 或更高版本

### 2. 构建

```bash
# 构建服务端和客户端二进制文件
make build
```

### 3. 运行集群

这是最便捷的启动一个三节点集群的方式。

```bash
# 启动集群 (后台运行)
make cluster
```

```bash
# 停止并清理集群
make stop-cluster
```

### 4. 使用客户端

客户端通过 `conf/config.yaml` 文件来发现集群节点。

- **设置键值对**:
  ```bash
  ./kv-client set mykey "hello world"
  ```
  > ✅ Success! Result: <nil>

- **获取键值**:
  ```bash
  ./kv-client get mykey
  ```
  > ✅ Success! Value: hello world

- **删除键**:
  ```bash
  ./kv-client delete mykey
  ```
  > ✅ Success! Result: <nil>

- **再次获取 (验证删除)**:
  ```bash
  ./kv-client get mykey
  ```
  > ✅ Success! Result: key not found

### 5. 其他 Makefile 命令

- **运行所有测试**:
  ```bash
  make test
  ```

- **清理构建产物和数据**:
  ```bash
  make clean
  ```

## 项目结构

```
go-kv/
├── .github/            # GitHub 相关配置 (例如 CI/CD workflows)
├── cmd/                # 应用程序入口
│   ├── client/         # 客户端 CLI 工具
│   │   └── main.go
│   └── server/         # go-kv 服务端
│       └── main.go
├── conf/               # 配置文件目录
│   └── config.yaml     # 主配置文件
├── engine/lsm/         # LSM 树存储引擎核心实现
│   ├── database/       # 数据库顶层封装 (组合各组件)
│   ├── memtable/       # 内存表 (MemTable) 实现
│   │   └── skiplist/   # 跳表数据结构
│   ├── sstable/        # SSTable (有序字符串表) 相关实现
│   │   ├── block/      # SSTable 的数据块/索引块等
│   │   └── bloom/      # 布隆过滤器实现
│   └── wal/            # 预写日志 (Write-Ahead Log)
├── pkg/                # 可复用的公共包
│   ├── client/         # 客户端 API 逻辑
│   ├── config/         # YAML 配置加载与解析
│   ├── log/            # 日志系统封装 (基于 zap)
│   ├── param/          # RPC 参数和核心数据结构 (如 LogEntry, KVCommand)
│   ├── storage/        # 存储层抽象与实现
│   │   ├── inmemory/   # 内存存储 (用于测试)
│   │   ├── lsm/        # LSM 树存储适配器
│   │   ├── simplefile/ # 简单文件存储 (用于测试)
│   │   └── storage.go  # 存储层核心接口 (Storage, StateMachine)
│   ├── transport/      # 网络传输层抽象与实现
│   │   ├── grpc/       # gRPC 传输实现
│   │   ├── inmemory/   # 内存传输 (用于测试)
│   │   ├── tcp/        # TCP 传输实现
│   │   └── transport.go # 传输层核心接口 (Transport)
│   └── utils/          # 通用工具函数
├── raft/               # Raft 共识模块核心实现
│   ├── election.go     # 选举逻辑 (RequestVote, Pre-Vote)
│   ├── replication.go  # 日志复制逻辑 (AppendEntries)
│   ├── snapshot.go     # 日志快照与压缩逻辑
│   └── raft.go         # Raft 主结构和状态机
├── tests/              # 集成测试
│   └── integration_test.go
├── Makefile            # 项目构建、测试、清理等脚本
└── README.md           # 项目说明文档
```

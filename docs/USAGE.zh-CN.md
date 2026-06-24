# 使用指南

English version: [USAGE.md](USAGE.md)

本文介绍如何构建、运行、测试和发布 `go-kv`。

## 1. 环境要求

- Go 1.25 或更高版本。
- `make`。
- 只有重新生成 gRPC protobuf 文件时才需要 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`。

安装依赖并校验模块依赖：

```bash
make deps
```

## 2. 构建

构建服务端和客户端：

```bash
make build
```

生成文件：

- `kv-server`
- `kv-client`

## 3. 启动本地集群

启动默认三节点集群：

```bash
make cluster
```

该命令使用：

- `conf/config-1.yaml`
- `conf/config-2.yaml`
- `conf/config-3.yaml`

每个文件设置不同的 `raft.id`，但共享同一份 peer 列表。默认传输层是 gRPC，默认存储引擎是 LSM。

`make cluster` 会等到 `8001`、`8002`、`8003` 三个端口都开始监听后才报告成功。
每个节点的日志写入 `raft-node-N.log`，早期进程输出写入 `raft-node-N.out`。

运行完整的本地启动和 CLI smoke 路径：

```bash
make cluster-smoke
```

停止集群：

```bash
make stop-cluster
```

清理本地数据、日志、二进制和测试产物：

```bash
make clean
```

## 4. 使用 CLI 客户端

客户端默认从 `conf/config.yaml` 读取 peer 列表，也可以通过 `--config` 指定配置文件。

写入 key：

```bash
./kv-client set user:1 "alice"
```

读取 key：

```bash
./kv-client get user:1
```

删除 key：

```bash
./kv-client delete user:1
```

指定配置：

```bash
./kv-client --config conf/config-1.yaml get user:1
```

客户端会在临时传输错误时重试，并根据 Raft 的 `NotLeader` 响应跟随 leader hint。每个请求都会携带稳定的 `ClientID` 和递增的 `SequenceNum`，因此同一个逻辑请求即使重试，也最多被状态机应用一次。

## 5. 配置

关键字段：

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
  peers:
    - id: 1
      address: "127.0.0.1:8001"

lsm:
  max_mem_table_size: 2097152
  max_sstable_size: 2097152
```

`raft.id` 必须能在 `raft.peers` 中找到。做本地多进程测试时，每个服务端进程应该使用不同配置文件，并设置不同的 `raft.id`。

环境变量可以覆盖配置。例如：

```bash
GO_KV_LOG_LEVEL=debug ./kv-server --config conf/config-1.yaml
```

普通运行建议使用 `warn`。排查 Raft 选举、复制、ReadIndex、快照、LSM flush 或 LSM compaction 时再切到 `debug`。

## 6. 存储模式

| 模式 | 用途 |
|---|---|
| `inmemory` | 仅用于快速测试，进程退出后数据丢失。 |
| `simplefile` | 小型持久化测试后端。 |
| `lsm` | 主要的生产式后端。 |

当 `raft.engine` 是 `lsm` 时，每个节点会创建两个独立 LSM 数据库：

```text
data/node-1/
├── lsm_raftlog/
└── lsm_statemachine/
```

这样可以把共识元数据和用户 key/value 数据分开。

## 7. 测试命令

运行 `tests/` 包之外的单元测试并生成覆盖率：

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

运行 benchmark：

```bash
make bench-test
```

日常 PR 检查：

```bash
GO_KV_LOG_LEVEL=warn go test -short ./... -count=1 -timeout=15m
make test
```

修改 Raft 或 LSM 正确性逻辑时，先跑触发问题的单个长时间 E2E，再跑全量长时间 E2E 回归：

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' \
  -count=1

GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' \
  -count=1
```

## 8. 排障入口

| 现象 | 优先检查 |
|---|---|
| 客户端命令失败 | 确认集群有 leader，客户端配置包含全部 peer。 |
| 持续 `NotLeader` | 检查是否频繁选举，或 leader hint 是否过期。 |
| 读超时 | 检查 ReadIndex 模式，以及 `lastApplied` 是否追上。 |
| Apply 超时 | 检查 leader commit 进度、follower 复制和 LSM 写入延迟。 |
| 快照或 compaction 警告 | 判断是已恢复的 stale metadata，还是实际文件损坏。 |
| CI 没有 Codecov 评论 | 确认 `CODECOV_TOKEN` 已配置，且 `coverage.txt` 非空。 |

## 9. 发布流程

发布流水线会在推送 `v*.*.*` 标签时发布 GitHub Release。也可以在 GitHub
Actions 页面为一个已经存在的版本标签手动触发发布。Release tag 必须使用
`v0.1.0` 或 `v0.1.0-rc.1` 这种语义化版本格式，workflow 会在构建前验证 tag 已存在。

创建 release tag：

```bash
git switch main
git pull --ff-only origin main
GO_KV_LOG_LEVEL=warn make test
git tag v0.1.0
git push origin v0.1.0
```

如果要手动发布已有标签，在 GitHub Actions 页面运行 `Release` workflow，并设置：

- `version`：要发布的已有标签，例如 `v0.1.0`。
- `prerelease`：是否把 GitHub Release 标记为预发布版本。

流水线会运行 short tests，构建多平台 server/client 二进制，生成每个平台的 checksum，
再生成全量 `SHA256SUMS.txt` manifest，并把产物附加到 GitHub Release。

Release 产物按平台命名：

```text
kv-server-linux-amd64
kv-client-linux-amd64
kv-server-darwin-arm64
kv-client-darwin-arm64
kv-server-windows-amd64.exe
kv-client-windows-amd64.exe
go-kv-<goos>-<goarch>.sha256
SHA256SUMS.txt
```

下载 release 后，先校验 checksum：

```bash
sha256sum -c SHA256SUMS.txt
```

作为 Go module 使用时，固定同一个 tag：

```bash
go get github.com/xmh1011/go-kv@v0.1.0
```

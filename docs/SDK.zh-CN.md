# Go SDK 接口文档

English version: [SDK.md](SDK.md)

本文说明应用代码如何通过当前 Go SDK 访问正在运行的 `go-kv` 集群。SDK 面很小：应用通常只需要使用 `pkg/client`、`pkg/param` 和 `pkg/transport`。

在 `v1.0.0` 之前，公开 API 仍可能调整。`v0.x` 版本可以使用，但应视为仍在演进。

## 1. 包地图

| 包 | Import path | 应用侧用途 |
|---|---|---|
| Client SDK | `github.com/xmh1011/go-kv/pkg/client` | 创建带重试的客户端，向集群发送命令。 |
| 命令类型 | `github.com/xmh1011/go-kv/pkg/param` | 构造 key-value 命令，理解客户端响应。 |
| 传输工厂 | `github.com/xmh1011/go-kv/pkg/transport` | 创建 gRPC 或 TCP client transport。 |
| 配置加载 | `github.com/xmh1011/go-kv/pkg/config` | 可选，用于读取 YAML 配置文件。 |

`raft`、`pkg/storage`、`engine/lsm` 等底层包是核心实现包。它们适合测试和嵌入式实验，但不是推荐的应用 SDK 入口。

## 2. 快速开始

先启动本地集群：

```bash
make build
make cluster
```

然后在 Go 代码里访问集群：

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"

    kvclient "github.com/xmh1011/go-kv/pkg/client"
    "github.com/xmh1011/go-kv/pkg/param"
    "github.com/xmh1011/go-kv/pkg/transport"
)

func main() {
    peers := map[int]string{
        1: "127.0.0.1:8001",
        2: "127.0.0.1:8002",
        3: "127.0.0.1:8003",
    }

    trans, err := transport.NewClientTransport("127.0.0.1:0", transport.GrpcTransport)
    if err != nil {
        log.Fatal(err)
    }
    defer trans.Close()
    trans.SetPeers(peers)

    c := kvclient.NewClient(peers, trans)

    if ok := set(c, "user:1", "alice"); !ok {
        log.Fatal("set failed")
    }

    value, ok := get(c, "user:1")
    if !ok {
        log.Fatal("get failed")
    }
    fmt.Println(value)

    if ok := del(c, "user:1"); !ok {
        log.Fatal("delete failed")
    }
}

func set(c *kvclient.Client, key, value string) bool {
    cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
    payload, _ := json.Marshal(cmd)
    _, ok := c.SendCommand(payload)
    return ok
}

func get(c *kvclient.Client, key string) (string, bool) {
    cmd := param.KVCommand{Op: param.OpGet, Key: key}
    payload, _ := json.Marshal(cmd)
    result, ok := c.SendCommand(payload)
    if !ok {
        return "", false
    }
    value, ok := result.(string)
    return value, ok
}

func del(c *kvclient.Client, key string) bool {
    cmd := param.KVCommand{Op: param.OpDelete, Key: key}
    payload, _ := json.Marshal(cmd)
    _, ok := c.SendCommand(payload)
    return ok
}
```

## 3. Client API

### `client.NewClient`

```go
func NewClient(servers map[int]string, trans transport.Transport) *Client
```

创建一个知道全部集群节点、并通过指定 transport 发送请求的客户端。

参数：

| 名称 | 含义 |
|---|---|
| `servers` | Raft node ID 到网络地址的映射，ID 必须和服务端配置一致。 |
| `trans` | `transport.NewClientTransport` 返回的 client transport。 |

行为：

- 生成随机 client ID；
- 初始没有 leader hint；
- 先发送到某个已知节点，之后跟随 Raft 返回的 leader hint。

### `Client.SendCommand`

```go
func (c *Client) SendCommand(command any) (any, bool)
```

向集群发送一个逻辑命令。

行为：

- 每个逻辑命令递增一次 client sequence number；
- 将命令包装成 `param.ClientArgs`；
- 对临时网络错误和 `NotLeader` 响应进行重试；
- 根据 `LeaderHint` 跟随当前 leader；
- 5 秒内没有完成则返回 `false`。

返回值：

| 返回值 | 含义 |
|---|---|
| `result any` | 状态机返回值。`get` 成功时返回 string；`set` 和 `delete` 通常返回 nil。 |
| `ok bool` | 命令是否在客户端超时前成功完成。 |

当前 key-value 状态机要求命令 payload 是 JSON 编码后的 `param.KVCommand`，类型为 `[]byte`。

## 4. Command API

### `param.KVCommand`

```go
type KVCommand struct {
    Op    OpType `json:"op"`
    Key   string `json:"key"`
    Value string `json:"value"`
}
```

支持操作：

| 操作 | 常量 | 必填字段 | 结果 |
|---|---|---|---|
| Get | `param.OpGet` | `Key` | 成功时返回 string value |
| Set | `param.OpSet` | `Key`, `Value` | 成功时通常返回 nil |
| Delete | `param.OpDelete` | `Key` | 成功时通常返回 nil |

`OpType` 可以从 JSON 整数解码，也可以从 `"get"`、`"set"`、`"delete"` 等字符串解码。

### `param.ClientArgs`

```go
type ClientArgs struct {
    ClientID    int64
    SequenceNum int64
    Command     any
}
```

大多数应用不应该直接构造 `ClientArgs`。建议使用 `client.Client.SendCommand`，由 SDK 负责分配稳定请求身份并支持重试去重。

### `param.ClientReply`

```go
type ClientReply struct {
    Success    bool
    Result     any
    NotLeader  bool
    LeaderHint int
}
```

高层 client 会在内部消费这个响应。直接调用 RPC 的用户可以根据 `NotLeader` 和 `LeaderHint` 重试到当前 leader。

## 5. Transport API

### `transport.NewClientTransport`

```go
func NewClientTransport(clientAddr, transportType string) (Transport, error)
```

创建客户端侧 transport。

支持类型：

| 常量 | 值 | 说明 |
|---|---|---|
| `transport.GrpcTransport` | `grpc` | 推荐默认值，适合本地集群和跨进程客户端。 |
| `transport.TcpTransport` | `tcp` | 备用 RPC transport。 |
| `transport.InMemoryTransport` | `inmemory` | 不支持独立客户端跨进程使用。 |

如果希望系统自动选择本地端口，可以使用 `"127.0.0.1:0"` 作为 client address。

创建 transport 后需要设置 peer map：

```go
trans.SetPeers(peers)
```

peer map 必须和服务端配置文件中的 node ID 和地址一致。

## 6. 从配置加载 Peers

应用可以直接构造 peer map，也可以使用 `pkg/config` 读取 `conf/config.yaml`。

```go
if err := config.Init("./conf/config.yaml"); err != nil {
    log.Fatal(err)
}

cfg := config.GetConfig()
peers := make(map[int]string, len(cfg.Raft.Peers))
for _, peer := range cfg.Raft.Peers {
    peers[peer.ID] = peer.Address
}
```

嵌入式 SDK 使用场景中，直接传 peer map 往往更简单；如果应用和 CLI 共用配置文件，则可以使用配置加载方式。

## 7. 错误和重试语义

高层 client 在 5 秒内无法完成命令时返回 `(nil, false)`。

常见原因：

| 原因 | Client 行为 |
|---|---|
| 目标节点不是 leader | 使用 `LeaderHint` 重试。 |
| 传输错误 | 清空 leader hint，重试其他已知节点。 |
| Leader 返回失败 | 清空 leader hint，直到超时前持续重试。 |
| 命令超时 | 返回 `ok=false`。 |

当前 client 不返回 typed error。需要精确失败原因的应用，可以直接调用底层 transport API 并检查 `param.ClientReply`，或在 SDK 外层封装自己的 timeout/error 模型。

## 8. 一致性说明

- 写请求只有在 Raft 提交并且 leader 状态机应用后才会返回成功。
- 读请求走 Raft leader 路径，并由 ReadIndex 或 lease confirmation 保护。
- 写重试通过 `(ClientID, SequenceNum)` 请求身份去重。
- 读取不存在的 key 是应用层 miss。当前高层 client 会把它暴露为服务端路径上的未成功命令结果。

## 9. API 稳定性

对于 `v0.x` 版本：

- `pkg/client.Client` 和 `param.KVCommand` 是推荐应用入口；
- Raft 内部、存储适配器和生成的 protobuf 类型可能随着实现演进而调整；
- 除非是在仓库内写测试、benchmark 或扩展，优先使用本文档里的高层 SDK 路径。

# Go SDK Interface Reference

Chinese version: [SDK.zh-CN.md](SDK.zh-CN.md)

This document describes the current Go SDK surface for application code that
wants to talk to a running `go-kv` cluster. The SDK is intentionally small:
applications normally use `pkg/client`, `pkg/param`, and `pkg/transport`.

The public API is not guaranteed to be stable before a `v1.0.0` release. Treat
the `v0.x` API as usable but still evolving.

## 1. Package Map

| Package | Import path | Use from applications |
|---|---|---|
| Client SDK | `github.com/xmh1011/go-kv/pkg/client` | Create a retrying client and send commands to the cluster. |
| Command types | `github.com/xmh1011/go-kv/pkg/param` | Build key-value commands and inspect client replies. |
| Transport factory | `github.com/xmh1011/go-kv/pkg/transport` | Create gRPC or TCP client transport. |
| Config loader | `github.com/xmh1011/go-kv/pkg/config` | Optional helper for loading YAML config files. |

Lower-level packages such as `raft`, `pkg/storage`, and `engine/lsm` are core
implementation packages. They are useful for tests and embedded experiments, but
they are not the preferred application SDK.

## 2. Quick Start

Start a local cluster first:

```bash
make build
make cluster
```

Then call the cluster from Go:

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

Creates a client that knows every cluster peer and can send requests through the
provided transport.

Parameters:

| Name | Meaning |
|---|---|
| `servers` | Map from Raft node ID to network address. The IDs must match the server configs. |
| `trans` | A client transport returned by `transport.NewClientTransport`. |

Behavior:

- Generates a random client ID.
- Starts with no leader hint.
- Sends to a known server first, then follows leader hints returned by Raft.

### `Client.SendCommand`

```go
func (c *Client) SendCommand(command any) (any, bool)
```

Sends one logical command to the cluster.

Behavior:

- Increments the client sequence number once per logical command.
- Wraps the command in `param.ClientArgs`.
- Retries transient transport errors and `NotLeader` replies.
- Follows `LeaderHint` when the server reports that it is not the leader.
- Gives up after 5 seconds and returns `false`.

Return values:

| Return | Meaning |
|---|---|
| `result any` | Command result from the state machine. `get` returns a string on success. `set` and `delete` normally return nil. |
| `ok bool` | True if the command completed successfully before the client timeout. |

The current key-value state machine expects the command payload to be a JSON
encoded `param.KVCommand` stored as `[]byte`.

## 4. Command API

### `param.KVCommand`

```go
type KVCommand struct {
    Op    OpType `json:"op"`
    Key   string `json:"key"`
    Value string `json:"value"`
}
```

Supported operations:

| Operation | Constant | Required fields | Result |
|---|---|---|---|
| Get | `param.OpGet` | `Key` | string value on success |
| Set | `param.OpSet` | `Key`, `Value` | nil on success |
| Delete | `param.OpDelete` | `Key` | nil on success |

`OpType` can be decoded from either JSON integer values or strings such as
`"get"`, `"set"`, and `"delete"`.

### `param.ClientArgs`

```go
type ClientArgs struct {
    ClientID    int64
    SequenceNum int64
    Command     any
}
```

Most applications should not construct `ClientArgs` directly. Use
`client.Client.SendCommand`, which assigns stable request identity for retry
deduplication.

### `param.ClientReply`

```go
type ClientReply struct {
    Success    bool
    Result     any
    NotLeader  bool
    LeaderHint int
}
```

The high-level client consumes this reply internally. Direct RPC users can use
`NotLeader` and `LeaderHint` to retry on the current leader.

## 5. Transport API

### `transport.NewClientTransport`

```go
func NewClientTransport(clientAddr, transportType string) (Transport, error)
```

Creates a client-side transport.

Supported transport types:

| Constant | Value | Notes |
|---|---|---|
| `transport.GrpcTransport` | `grpc` | Recommended default for local clusters and cross-process clients. |
| `transport.TcpTransport` | `tcp` | Alternative RPC transport. |
| `transport.InMemoryTransport` | `inmemory` | Not supported for standalone clients. |

Use `"127.0.0.1:0"` as the client address when the OS should pick an available
local port.

After creating the transport, call:

```go
trans.SetPeers(peers)
```

The peer map must contain the same node IDs and addresses used by server
configuration files.

## 6. Loading Peers From Config

Applications can either build the peer map directly or load `conf/config.yaml`
with `pkg/config`.

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

Direct peer maps are often simpler for embedded SDK usage. Config loading is
useful when the application shares the same config file as the CLI.

## 7. Error And Retry Semantics

The high-level client returns `(nil, false)` when a command does not complete
within 5 seconds.

Common causes:

| Cause | Client behavior |
|---|---|
| Target node is not leader | Retry using `LeaderHint`. |
| Transport error | Clear leader hint and retry another known node. |
| Leader returns failure | Clear leader hint and retry until timeout. |
| Command timeout | Return `ok=false`. |

The client currently does not return a typed error. Applications that need exact
failure reasons should call the lower-level transport API and inspect
`param.ClientReply`, or wrap the SDK with their own timeout and error model.

## 8. Consistency Notes

- Successful writes are acknowledged only after Raft commits the command and the
  leader applies it to the state machine.
- Reads use the Raft leader path and are protected by ReadIndex or lease
  confirmation.
- Write retries are deduplicated through `(ClientID, SequenceNum)` request
  identity.
- A `get` for a missing key is an application-level miss. The current high-level
  client exposes it as an unsuccessful command result from the server path.

## 9. API Stability

For `v0.x` releases:

- `pkg/client.Client` and `param.KVCommand` are the intended application entry
  points.
- Raft internals, storage adapters, and generated protobuf types may change as
  the implementation evolves.
- Prefer the documented high-level SDK path unless you are writing tests,
  benchmarks, or extensions inside this repository.

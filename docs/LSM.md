# LSM 树存储引擎详解

本文档详细介绍 `go-kv` 项目中 LSM 树 (Log-Structured Merge-Tree) 存储引擎的设计与实现。

> 相关源码：`engine/lsm/` 目录

---

## 目录

- [1. 概述与设计哲学](#1-概述与设计哲学)
- [2. 整体架构](#2-整体架构)
- [3. 数据模型](#3-数据模型)
- [4. 跳表 MemTable](#4-跳表-memtable)
- [5. MemTable 管理器](#5-memtable-管理器)
- [6. WAL 预写日志](#6-wal-预写日志)
- [7. SSTable 文件格式](#7-sstable-文件格式)
- [8. 布隆过滤器](#8-布隆过滤器)
- [9. SSTable 管理器与搜索](#9-sstable-管理器与搜索)
- [10. Compaction 策略](#10-compaction-策略)
- [11. Raft 存储适配器](#11-raft-存储适配器)
- [12. 配置参数](#12-配置参数)

---

## 1. 概述与设计哲学

### 1.1 为什么选择 LSM 树

`go-kv` 是一个基于 Raft 共识协议的分布式 KV 存储系统。Raft 日志复制的工作模式天然就是**追加写入**：每个节点不断接收 Leader 发来的日志条目，将其持久化后应用到状态机。这种写入模式与 LSM 树的设计理念高度契合。

LSM 树（Log-Structured Merge-Tree）由 Patrick O'Neil 等人在 1996 年的论文 *"The Log-Structured Merge-Tree"* 中提出。其核心思想是：**将离散的随机写转化为批量的顺序写**。所有写入操作首先进入内存中的有序数据结构（MemTable），积累到一定大小后批量刷写到磁盘上的有序文件（SSTable）。这使得 LSM 树在写密集型场景中远优于传统的 B+ 树。

### 1.2 核心权衡：三角困境

存储引擎设计中存在一个经典的三角困境（RUM Conjecture）。LSM 树在三个维度上做出了明确的取舍：

| 维度 | LSM 树 | B+ 树 |
|------|--------|-------|
| **写放大 (Write Amplification)** | 较高 — 数据在 Compaction 中被反复读写 | 较低 — 就地更新 |
| **读放大 (Read Amplification)** | 较高 — 可能需要搜索多层 | 较低 — O(log n) 一次定位 |
| **空间放大 (Space Amplification)** | 中等 — 旧版本数据暂存直到 Compaction | 较低 — 就地更新 |
| **写入吞吐** | **极高** — 顺序 I/O | 较低 — 随机 I/O |

`go-kv` 选择 LSM 树正是因为 Raft 场景的写入压力远大于读取压力：
- **写路径**：每个 Raft 日志条目都必须持久化到存储层（`AppendEntries`），集群的所有节点都要执行
- **读路径**：只有 Leader 执行状态机读取（`Get`），且通过布隆过滤器和稀疏索引大幅减少了读放大

### 1.3 设计目标

1. **高写入吞吐** — 内存缓冲 + 顺序写盘，最大化磁盘带宽利用率
2. **崩溃安全** — 每次写操作先写 WAL，确保持久性语义
3. **可预测的读延迟** — 布隆过滤器 + 分层稀疏索引，避免全量扫描
4. **后台维护透明** — Compaction 在后台异步执行，不阻塞前台读写
5. **简洁实现** — 不追求极致优化，而是在正确性和可理解性之间取得平衡

---

## 2. 整体架构

### 2.1 组件层次

```
┌──────────────────────────────────────────────────────────────────┐
│                        Database                                   │
│                    database/database.go                           │
│                                                                   │
│   ┌─────────────────────────┐   ┌──────────────────────────────┐ │
│   │    MemTable Manager     │   │      SSTable Manager         │ │
│   │    memtable/manager.go  │   │      sstable/manager.go      │ │
│   │                         │   │                              │ │
│   │  ┌───────────────────┐  │   │  Level 0: [sst][sst]        │ │
│   │  │  Active MemTable  │  │   │  Level 1: [sst][sst][sst]   │ │
│   │  │  (跳表 + WAL)     │  │   │  Level 2: [sst]...[sst]     │ │
│   │  └───────────────────┘  │   │  ...                         │ │
│   │                         │   │  Level 6: [sst]...[sst]      │ │
│   │  ┌───────────────────┐  │   │                              │ │
│   │  │  IMemTable 队列   │  │   │  ┌────────────────────────┐  │ │
│   │  │  [imem][imem]...  │  │   │  │    Compaction Engine   │  │ │
│   │  └───────────────────┘  │   │  │    sstable/compaction   │  │ │
│   └─────────────────────────┘   │  └────────────────────────┘  │ │
│                                  └──────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 写入路径

一次写操作的完整流程：

```
Put(key, value)
  │
  ├── 1. MemTable Manager 获取写锁 (mu.Lock)
  │
  ├── 2. 活跃 MemTable 能容纳?
  │     │
  │     ├── YES → 写 WAL → 插入跳表 → 释放锁 → 完成
  │     │
  │     └── NO → promote():
  │           ├── IMemTable 队列已满? → 驱逐最老的 IMemTable
  │           │                          └── 刷写到 SSTable (Level 0)
  │           ├── 冻结活跃 MemTable → 加入 IMemTable 队列
  │           ├── 创建新的活跃 MemTable (新 ID + 新 WAL)
  │           └── 写入新 MemTable → 释放锁 → 完成
  │
  └── 3. SSTable Manager 检查 Level 0 文件数
        └── 超过阈值? → 触发 Compaction
```

### 2.3 读取路径

一次读操作按照数据新鲜度从高到低搜索：

```
Get(key)
  │
  ├── 1. 搜索活跃 MemTable (跳表 O(log n))
  │     └── 找到 → 返回（含墓碑检查）
  │
  ├── 2. 搜索 IMemTable 队列（从新到旧）
  │     └── 找到 → 返回（含墓碑检查）
  │
  ├── 3. 搜索 Level 0 SSTable（从新到旧，可能重叠）
  │     ├── 布隆过滤器快速排除
  │     ├── IndexBlock 二分查找定位
  │     └── DataBlock 懒加载读取值
  │
  ├── 4. 搜索 Level 1~6 SSTable（稀疏索引二分查找）
  │     ├── sort.Search 定位候选 SSTable
  │     ├── 布隆过滤器快速排除
  │     ├── IndexBlock 二分查找定位
  │     └── DataBlock 懒加载读取值
  │
  └── 5. 所有层都未找到 → 返回 "key not found"
```

### 2.4 目录结构

```
{db_path}/
├── wal/                    # WAL 文件目录
│   ├── 1.wal               # MemTable 1 的 WAL
│   ├── 2.wal               # MemTable 2 的 WAL
│   └── ...
└── sst/                    # SSTable 文件目录
    ├── 0-level/            # Level 0
    │   ├── 10.sst
    │   └── 11.sst
    ├── 1-level/            # Level 1
    │   ├── 5.sst
    │   ├── 6.sst
    │   └── 7.sst
    ├── 2-level/            # Level 2
    │   └── ...
    └── 6-level/            # Level 6 (最大层)
        └── ...
```

---

## 3. 数据模型

> 源码：`engine/lsm/kv/kv.go`

### 3.1 KV 编码格式

`go-kv` 使用简单的**长度前缀编码**（Length-Prefixed Encoding），所有整数采用 **little-endian** 字节序：

```
┌──────────────────────────────────────────────────────────────┐
│                     KeyValuePair 编码                         │
├──────────┬──────────┬────────────┬────────────┤             │
│ KeyLen   │ KeyData  │ ValueLen   │ ValueData  │             │
│ (4 bytes)│ (变长)   │ (4 bytes)  │ (变长)     │             │
│ uint32 LE│          │ uint32 LE  │            │             │
└──────────┴──────────┴────────────┴────────────┘             │
```

**类型定义**：

```go
type Key string          // 键 — 字符串类型
type Value []byte        // 值 — 字节切片

type KeyValuePair struct {
    Key   Key
    Value Value
}
```

**编码优化**：`EncodeTo` 预分配一个完整的 buffer（`4 + len(key) + 4 + len(value)` 字节），通过单次 `io.Write` 调用写出，避免多次系统调用。

**长度限制**：

| 字段 | 最大长度 |
|------|---------|
| Key  | 1 MB (`1 << 20`) |
| Value | 1 GB (`1 << 30`) |

### 3.2 删除墓碑机制

LSM 树不直接删除数据，而是写入一条特殊的**墓碑标记**（Tombstone）：

```go
const deletedValueStr = "～DELETED～"      // 使用全角波浪线避免与正常数据冲突
var DeletedValue = Value(deletedValueStr)
```

**墓碑的生命周期**：

1. **写入**：`Delete(key)` 调用会插入 `KeyValuePair{Key: key, Value: DeletedValue}`
2. **传播**：墓碑随着 Compaction 向下层传播
3. **清理**：只有在 **最大层（Level 6）** 的 Compaction 中，墓碑才会被物理删除

为什么不能提前清理？因为低层的墓碑可能"遮蔽"了高层中同一 key 的旧值。如果提前删除墓碑，那个旧值就会"复活"。只有在最大层，我们确信没有更高层存在该 key 的旧版本，才能安全删除。

### 3.3 大小估算

```go
func (p *KeyValuePair) EstimateSize() int {
    return 4 + len(p.Key) + 4 + len(p.Value) + 8
    //     ^KeyLen  ^Key    ^ValLen ^Value     ^Offset（索引中的偏移量）
}
```

这个估算用于 MemTable 和 SSTable Builder 的容量判断。额外的 8 字节对应 IndexBlock 中为该条目存储的值偏移量。

---

## 4. 跳表 MemTable

> 源码：`engine/lsm/memtable/skiplist/skiplist.go`

### 4.1 为什么选择跳表

MemTable 需要一个支持高效插入和有序遍历的内存数据结构。常见的选择有红黑树和跳表。`go-kv` 选择跳表，原因如下：

| 特性 | 跳表 | 红黑树 |
|------|------|--------|
| 插入/查找 | O(log n) 期望 | O(log n) 最坏 |
| 实现复杂度 | **简单** — 数组 + 随机层高 | 复杂 — 旋转 + 染色 |
| 并发友好 | 好 — 可细粒度锁/无锁 | 差 — 旋转影响多个节点 |
| 有序遍历 | 天然支持（底层链表） | 需要中序遍历 |
| 缓存友好性 | 中等 | 较差（指针跳转多） |

实际上，LevelDB、RocksDB、HBase 等主流 LSM 实现都选择了跳表。

### 4.2 数据结构

```go
const (
    maxLevel = 32      // 最大层数
    pFactor  = 0.25    // 升层概率
)

type Node struct {
    Pair    kv.KeyValuePair
    Forward []*Node          // Forward[i] 指向第 i 层的下一个节点
}

type SkipList struct {
    Head  *Node              // 哨兵头节点，Forward 长度为 maxLevel
    Level int                // 当前实际使用的最高层数
}
```

**层高参数分析**：

- `pFactor = 0.25` 意味着每个节点有 25% 的概率被提升到更高层
- 期望层高：`1 / (1 - p) = 1.33`
- 对于 N 个元素，期望最高层数：`log₄(N)`
- 即使有 100 万个元素，期望最高层也只有 `log₄(1,000,000) ≈ 10` 层
- `maxLevel = 32` 可以支持 `4³² ≈ 1.8 × 10¹⁹` 个元素，远超实际需要

### 4.3 随机层高生成

```go
func randomLevel() int {
    lv := 1
    for lv < maxLevel && rand.Float64() < pFactor {
        lv++
    }
    return lv
}
```

生成的层高服从**几何分布**：

| 层高 | 概率 |
|------|------|
| 1 | 75.0% |
| 2 | 18.75% |
| 3 | 4.69% |
| 4 | 1.17% |
| 5 | 0.29% |
| ≥6 | 0.07% |

### 4.4 核心操作

**Search(key)**：从最高层开始，在每一层沿 Forward 指针前进，直到找到目标或确认不存在。

```
Level 3:  Head ──────────────────────────────────► [K]
Level 2:  Head ──────────► [D] ──────────────────► [K]
Level 1:  Head ────► [B] ─► [D] ────► [G] ──────► [K]
Level 0:  Head ► [A] ► [B] ► [D] ► [E] ► [G] ► [H] ► [K]
                                          ^
                                     Search("G")
```

搜索过程：Level 3 到达 K（太大），下降 → Level 2 到达 D（太小），前进到 K（太大），下降 → Level 1 到达 G，命中。

**Add(pair)**：如果 key 已存在则**就地更新**值（upsert 语义），否则生成随机层高并插入。在搜索过程中记录每一层的"前驱节点"（update 数组），用于更新 Forward 指针。

**Delete(key)**：逻辑删除 — 将值设置为 `DeletedValue`（墓碑标记），然后从各层的链表中物理移除该节点。如果最高层变空，则递减 `Level`。

### 4.5 MemTable 封装

> 源码：`engine/lsm/memtable/memtable.go`

```go
type MemTable struct {
    id          uint64              // 唯一标识
    entries     *skiplist.SkipList  // 底层跳表
    wal         *wal.WAL            // 关联的 WAL 文件
    sizeInBytes uint64              // 近似大小（字节）
    maxSize     uint64              // 容量阈值（默认 2 MB）
}
```

**Insert 流程**：

1. 先写 WAL（`wal.Append(pair)`）— 确保持久性
2. 再插入跳表（`entries.Add(pair)`）— 更新内存索引
3. 累加 `sizeInBytes`

**容量判断**：`CanInsert(pair)` 检查 `ApproximateSize() + pair.EstimateSize() <= maxSize`。当 MemTable 写满时，由 Manager 触发 promote 流程。

**崩溃恢复**：`RecoverFromWAL` 从 WAL 文件名中提取 ID，然后回放 WAL 中的所有 KV 对到跳表中，重建内存状态。

---

## 5. MemTable 管理器

> 源码：`engine/lsm/memtable/manager.go`

### 5.1 设计概述

MemTable Manager 管理活跃 MemTable 和 IMemTable 队列的完整生命周期。它是连接内存层和磁盘层的枢纽：上游接收写入请求，下游驱逐老化的 IMemTable 供 SSTable Manager 刷写。

```go
type Manager struct {
    mu                 sync.RWMutex    // 读写锁
    walPath            string          // WAL 目录
    Mem                *MemTable       // 活跃 MemTable（可写）
    IMems              []*IMemTable    // 不可变 MemTable 队列
    maxIMemTableCount  int             // 队列容量上限（默认 10）
}
```

### 5.2 并发模型

Manager 使用 `sync.RWMutex` 实现读写分离：

- **写操作**（Insert/Delete）：获取写锁 `mu.Lock()`，保证同一时刻只有一个写入者
- **读操作**（Search）：获取读锁 `mu.RLock()`，多个读者可以并发执行

这意味着读操作不会阻塞其他读操作，只有在写入触发 promote（涉及内存结构变更）时才会短暂阻塞读取。

### 5.3 Promote 机制

当活跃 MemTable 写满时，Manager 执行 promote：

```
promote() 触发条件: !Mem.CanInsert(pair)
  │
  ├── 1. 冻结当前 MemTable → 创建 IMemTable（零拷贝，直接转移跳表引用）
  │
  ├── 2. IMemTable 队列是否已满？（len(IMems) >= maxIMemTableCount）
  │     │
  │     ├── YES → 驱逐 IMems[0]（最老的），返回给调用方刷写到 SSTable
  │     │
  │     └── NO → 无需驱逐
  │
  ├── 3. 将新 IMemTable 追加到队列尾部
  │
  └── 4. 创建新的活跃 MemTable（新 ID + 新 WAL 文件）
```

**零拷贝冻结**：`IMemTable` 直接引用原 `MemTable` 的跳表指针和 WAL 句柄，不复制任何数据。冻结后原 MemTable 不再接收写入。

### 5.4 搜索顺序

```go
func (m *Manager) Search(key kv.Key) (*kv.KeyValuePair, bool, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    // 1. 搜索活跃 MemTable
    if pair, found := m.Mem.Search(key); found { return pair, true, nil }

    // 2. 从新到旧搜索 IMemTable 队列
    for i := len(m.IMems) - 1; i >= 0; i-- {
        if pair, found := m.IMems[i].Search(key); found { return pair, true, nil }
    }

    return nil, false, nil
}
```

从新到旧的搜索顺序确保了**最新版本优先**——如果同一个 key 在活跃 MemTable 和 IMemTable 中都存在，活跃 MemTable 中的版本一定是最新的。

### 5.5 ID 生成

Manager 使用 `atomic.Uint64` 全局计数器为每个 MemTable 分配递增 ID。这个 ID 同时用作 WAL 文件名和 SSTable 文件名的一部分。

### 5.6 崩溃恢复

```
Recover()
  │
  ├── 读取 WAL 目录下所有文件
  ├── 按 ID 排序（升序）
  ├── 除最后一个外 → 恢复为 IMemTable（只读，不需要 WAL 句柄）
  ├── 最后一个 → 恢复为活跃 MemTable（打开 WAL 续写）
  └── 如果 IMemTable 数量超过上限 → 只保留最新的 maxIMemTableCount 个
```

---

## 6. WAL 预写日志

> 源码：`engine/lsm/wal/wal.go`

### 6.1 设计原则

WAL（Write-Ahead Log）是 LSM 引擎崩溃安全性的基石。核心原则很简单：**数据在写入内存之前，必须先持久化到磁盘**。这样即使进程崩溃，也能通过回放 WAL 恢复内存中的 MemTable。

### 6.2 每 MemTable 独立 WAL

`go-kv` 采用**每个 MemTable 对应一个独立 WAL 文件**的设计（文件名为 `{id}.wal`）。相比单一全局 WAL 的优势：

| 特性 | 独立 WAL | 全局 WAL |
|------|---------|---------|
| WAL 清理 | 简单 — IMemTable 刷写后直接删除对应 WAL | 复杂 — 需要 checkpoint 机制 |
| 恢复复杂度 | 低 — 每个 WAL 独立恢复 | 高 — 需要分离不同 MemTable 的数据 |
| 并发写入 | 无需锁 — 每个 MemTable 单线程写 | 需要锁 — 多 MemTable 竞争 |
| 磁盘空间 | 略高 — 多个文件句柄 | 略低 — 单个文件 |

### 6.3 实现细节

```go
type WAL struct {
    file *os.File
    buf  *bufio.Writer    // 32 KB 缓冲
    path string
}
```

**缓冲写入**：WAL 使用 32 KB 的 `bufio.Writer` 缓冲。这在批量小写入场景下显著减少了系统调用次数——多个小 KV 对会在缓冲区中积累，直到显式 Flush。

### 6.4 持久性语义

WAL 提供三个层次的持久性保证：

| 操作 | 语义 | 实现 |
|------|------|------|
| `Append(pair)` | 编码 KV → 缓冲区 → Flush 到 OS | `pair.EncodeTo(buf)` + `buf.Flush()` |
| `Flush()` | 缓冲区内容写入 OS 页缓存 | `buf.Flush()` |
| `Sync()` | 强制刷盘（fsync） | `buf.Flush()` + `file.Sync()` |

`go-kv` 在每次 `Append` 后执行 `Flush()`，将数据从用户态缓冲推送到 OS 页缓存。这在大多数场景下已经足够安全——只有在 OS 崩溃（而非进程崩溃）时才会丢失数据。如果需要更强的持久性保证，可以改为每次 `Append` 后调用 `Sync()`，代价是更高的写入延迟。

### 6.5 崩溃恢复流程

```
WAL Recovery(fileName, walPath, callback)
  │
  ├── 打开 WAL 文件 (O_RDWR | O_APPEND)
  ├── 读取全部内容到内存 (io.ReadAll)
  ├── 循环解码 KV 对：
  │     ├── 解码 KeyLen (4 bytes LE) + KeyData
  │     ├── 解码 ValueLen (4 bytes LE) + ValueData
  │     └── 调用 callback(KeyValuePair)  // 插入跳表
  ├── 遇到 EOF 或解码错误 → 停止（容忍尾部不完整记录）
  └── 返回新的 WAL 句柄（指向同一文件，准备续写）
```

WAL 恢复容忍尾部的不完整记录——如果最后一条记录在写入过程中进程崩溃，导致只写了一部分，解码时会遇到 EOF 或长度不匹配，此时直接停止恢复。这条不完整的记录会被忽略，这是安全的，因为它从未被确认写入成功。

---

## 7. SSTable 文件格式

> 源码：`engine/lsm/sstable/sstable.go`、`engine/lsm/sstable/block/`

### 7.1 整体布局

SSTable（Sorted String Table）是 LSM 树在磁盘上的持久化格式。每个 SSTable 文件包含一组按 key 排序的 KV 对，一旦写入便不可修改（immutable）。

```
┌────────────────────────────────────────────────────────┐
│                      SSTable 文件                       │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Header                                          │  │
│  │  ┌────────────────┬────────────────┐             │  │
│  │  │ MinKey (变长)  │ MaxKey (变长)  │             │  │
│  │  │ 4B len + data  │ 4B len + data  │             │  │
│  │  └────────────────┴────────────────┘             │  │
│  ├──────────────────────────────────────────────────┤  │
│  │  FilterBlock (布隆过滤器)                         │  │
│  │  ┌──────────┬──────────────────────┐             │  │
│  │  │ Size (8B)│ Bloom Filter Data    │             │  │
│  │  └──────────┴──────────────────────┘             │  │
│  ├──────────────────────────────────────────────────┤  │
│  │  DataBlock (值数据)                               │  │
│  │  ┌──────────────┬──────────────┬─────┐           │  │
│  │  │ Value₁       │ Value₂       │ ... │           │  │
│  │  │ 4B len + data│ 4B len + data│     │           │  │
│  │  └──────────────┴──────────────┴─────┘           │  │
│  ├──────────────────────────────────────────────────┤  │
│  │  IndexBlock (键索引)                              │  │
│  │  ┌─────────────────────┬────────────────────┐    │  │
│  │  │ Entry₁              │ Entry₂             │... │  │
│  │  │ 4B kLen + key + 8B  │ 4B kLen + key + 8B│    │  │
│  │  │              offset │              offset│    │  │
│  │  └─────────────────────┴────────────────────┘    │  │
│  ├──────────────────────────────────────────────────┤  │
│  │  Footer (固定 32 字节)                            │  │
│  │  ┌────────────────────┬─────────────────────┐    │  │
│  │  │ DataHandle  (16B)  │ IndexHandle  (16B)  │    │  │
│  │  │ Offset(8) Size(8)  │ Offset(8) Size(8)  │    │  │
│  │  └────────────────────┴─────────────────────┘    │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

### 7.2 各 Block 详解

#### Header

Header 位于文件开头，记录该 SSTable 包含的键范围：

```go
type Header struct {
    MinKey kv.Key    // 最小键
    MaxKey kv.Key    // 最大键
}
```

用于快速判断一个目标 key 是否可能在该 SSTable 的范围内，无需打开文件查看索引。

#### FilterBlock

紧随 Header，存储序列化的布隆过滤器。编码格式：

```
┌─────────────────┬──────────────────────┐
│ DataLen (8 bytes)│ Bloom Filter Binary   │
│ uint64 LE        │ (MarshalBinary)       │
└─────────────────┴──────────────────────┘
```

#### DataBlock

存储所有值（Value），按照 key 的排序顺序排列。每个值独立编码：

```
┌────────────────┬────────────────┐
│ ValueLen (4B)  │ Value Data     │
│ uint32 LE      │ (变长)         │
├────────────────┼────────────────┤
│ ValueLen (4B)  │ Value Data     │
│ uint32 LE      │ (变长)         │
├────────────────┼────────────────┤
│ ...            │ ...            │
└────────────────┴────────────────┘
```

DataBlock 在查询时**不会被完整加载到内存**。通过 IndexBlock 中记录的偏移量，可以 seek 到目标值的位置，只读取需要的那一条记录（懒加载）。

#### IndexBlock

为每个 key 存储一个索引条目，指向该 key 对应 value 在 DataBlock 中的偏移位置：

```go
type IndexEntry struct {
    Key    kv.Key
    Offset int64     // Value 在 DataBlock 中的字节偏移
}
```

编码格式：`4B KeyLen + Key Data + 8B Offset (LE)`

IndexBlock **在 SSTable 打开时完整加载到内存**，支持二分查找。

#### Footer

固定 32 字节，位于文件末尾，记录 DataBlock 和 IndexBlock 的位置：

```go
const FooterSize = 32   // 2 × Handle (16 bytes each)

type Handle struct {
    Offset uint64    // Block 在文件中的起始偏移
    Size   uint64    // Block 的字节长度
}

type Footer struct {
    DataHandle  Handle
    IndexHandle Handle
}
```

Footer 的固定大小使得加载 SSTable 时可以直接 seek 到 `fileSize - 32` 读取，然后通过 Handle 定位其他 Block。

### 7.3 SSTable 加载流程

```
Open SSTable file
  │
  ├── 1. 从文件开头解码 Header（MinKey + MaxKey）
  ├── 2. 解码 FilterBlock（布隆过滤器）
  ├── 3. Seek 到 fileSize - 32，解码 Footer
  ├── 4. 使用 Footer.IndexHandle 定位并解码 IndexBlock
  └── 5. DataBlock 不加载 — 需要时按 offset 懒加载
```

### 7.4 SSTable 查询流程

```
SSTable.Search(key)
  │
  ├── 1. Header 范围检查: MinKey <= key <= MaxKey?
  │     └── NO → 返回 nil
  │
  ├── 2. 布隆过滤器: MayContain(key)?
  │     └── NO → 返回 nil（key 一定不存在）
  │
  ├── 3. IndexBlock 二分查找: 定位 key 的 IndexEntry
  │     └── 未找到 → 返回 nil
  │
  └── 4. 懒加载: 按 IndexEntry.Offset seek 到 DataBlock 中读取 Value
        └── 返回 KeyValuePair
```

---

## 8. 布隆过滤器

> 源码：`engine/lsm/sstable/bloom/bloom.go`、`engine/lsm/sstable/bloom/murmur.go`

### 8.1 作用

布隆过滤器是一种概率型数据结构，用于快速判断一个元素是否**可能**存在于集合中。它有两种回答：

- **"可能存在"** — 需要进一步查找确认（可能是假阳性）
- **"一定不存在"** — 100% 确定不存在（零假阴性）

在 LSM 引擎中，布隆过滤器的价值在于：**避免无效的磁盘 I/O**。当查询一个 key 时，如果布隆过滤器说"一定不存在"，就可以跳过该 SSTable 的 IndexBlock 查找和 DataBlock 读取。考虑到 Level 0 可能有多个重叠的 SSTable，且低层 SSTable 可能很多，布隆过滤器可以过滤掉绝大多数无效查询。

### 8.2 参数配置

```go
const (
    defaultBloomFilterM = 1_600_000    // 位数组大小 m = 1,600,000 bits（~200 KB）
    defaultBloomFilterK = 16           // 哈希函数数量 k = 16
)
```

### 8.3 假阳性率估算

布隆过滤器的假阳性率公式：

```
FPR ≈ (1 - e^(-kn/m))^k
```

其中 `m` = 位数组大小，`k` = 哈希函数数量，`n` = 元素数量。

以 `go-kv` 的默认参数 `m = 1,600,000`、`k = 16` 为例：

| 元素数量 n | 假阳性率 |
|-----------|---------|
| 1,000 | ≈ 10⁻¹⁷ （几乎为零） |
| 10,000 | ≈ 10⁻¹² |
| 50,000 | ≈ 0.0003% |
| 100,000 | ≈ 1.3% |

对于默认 2 MB 的 SSTable，以平均 100 字节/条目计算，大约存储 20,000 条记录，此时假阳性率极低。

### 8.4 哈希函数：MurmurHash3 128-bit

`go-kv` 使用 MurmurHash3 的 128-bit 变体作为布隆过滤器的哈希函数。MurmurHash3 的特点是：

- **速度快**：面向软件实现优化，无需 AES-NI 等硬件支持
- **分布均匀**：通过雪崩效应确保输出的每一位都受输入的每一位影响
- **零分配**：`go-kv` 的实现使用 `unsafe.Pointer` 直接操作内存，无堆分配

**双重哈希扩展**：128-bit 哈希输出被扩展为 256-bit（4 个 64-bit 值），然后通过 Kirsch-Mitzenmacker 双重哈希技巧从 4 个基础哈希值推导出 `k` 个位置：

```go
func (f *Filter) location(h [4]uint64, i uint) uint {
    return uint(location(h, i) % uint64(f.arraySize))
}
```

这避免了为每个位置调用一次完整的哈希函数，只需一次哈希计算即可生成所有 `k` 个位置。

### 8.5 序列化

布隆过滤器与 SSTable 一起持久化。FilterBlock 的编码包含：
- `arraySize`（uint64 大端）+ `hashNum`（uint64 大端）+ bitset 二进制数据

底层位数组使用 `bits-and-blooms/bitset` 库，支持高效的二进制序列化。

---

## 9. SSTable 管理器与搜索

> 源码：`engine/lsm/sstable/manager.go`

### 9.1 分层管理

SSTable Manager 维护所有 SSTable 的元数据和分层索引：

```go
type Manager struct {
    mu              sync.RWMutex
    sstPath         string                // SSTable 根目录
    levels          [][]*SSTable          // 每层的 SSTable 列表（按 ID 降序）
    fileIndex       map[string]*SSTable   // 文件路径 → SSTable 快速查找
    totalMap        map[int][]string      // 每层的文件路径列表
    sparseIndexes   [][]*SSTable          // Level 1+ 的稀疏索引（按 MinKey 排序）
    compactionCond  *sync.Cond            // Compaction 协调
    compactingLevels map[int]bool         // 每层的 Compaction 状态
    minSSTableLevel int                   // 默认 0
    maxSSTableLevel int                   // 默认 6
    levelSizeBase   int                   // 默认 2
}
```

### 9.2 Level 0 特殊处理

Level 0 是由 MemTable 直接刷写而来的 SSTable。由于 MemTable 之间可能存在时间重叠（一个 MemTable 正在写入时，另一个已经被刷写），Level 0 的 SSTable **键范围可能重叠**。

因此，Level 0 的搜索必须**线性扫描所有 SSTable**（从新到旧）：

```
Level 0 Search(key):
  for each sst in level0 (newest first):
    if sst.MayContain(key):     // Header 范围检查 + 布隆过滤器
      result = sst.Search(key)  // IndexBlock 二分查找
      if found: return result
  return nil
```

### 9.3 Level 1+ 稀疏索引

从 Level 1 开始，每层的 SSTable **键范围保证互不重叠**（这是 Compaction 保证的）。`go-kv` 利用这个特性构建**稀疏索引**——一个按 MinKey 排序的 SSTable 数组：

```
Level 1 稀疏索引:
  [SST_A: MinKey="a", MaxKey="d"]
  [SST_B: MinKey="e", MaxKey="h"]
  [SST_C: MinKey="k", MaxKey="n"]

Search("f"):
  sort.Search → 定位到 SST_B（最后一个 MinKey <= "f" 的 SSTable）
  SST_B.MayContain("f") → true
  SST_B.Search("f") → 找到
```

使用 `sort.Search`（二分查找）定位候选 SSTable，时间复杂度从线性扫描的 O(n) 降低到 O(log n)。

### 9.4 每层容量限制

每层允许的最大 SSTable 文件数量按指数增长：

```
maxFileNumsInLevel(level) = levelSizeBase ^ (level + 1)
```

默认 `levelSizeBase = 2`：

| 层级 | 最大文件数 | 最大总容量（按 2 MB/SSTable） |
|------|----------|------------------------------|
| Level 0 | 2 | 4 MB |
| Level 1 | 4 | 8 MB |
| Level 2 | 8 | 16 MB |
| Level 3 | 16 | 32 MB |
| Level 4 | 32 | 64 MB |
| Level 5 | 64 | 128 MB |
| Level 6 | 128 | 256 MB |
| **总计** | **254** | **~508 MB** |

### 9.5 Compaction 协调

SSTable Manager 使用 `sync.Cond` 协调搜索和 Compaction 的并发：

- 搜索某层时，如果该层正在 Compaction，搜索线程会**等待** Compaction 完成
- Compaction 完成后通过 `Broadcast()` 唤醒所有等待的搜索线程

这确保了搜索不会读到 Compaction 过程中的中间状态（如 SSTable 文件被删除但新文件尚未注册）。

### 9.6 SSTable 创建流程

```
CreateNewSSTable(imem *IMemTable)
  │
  ├── 1. Build: 遍历 IMemTable，构建 SSTable（Header、Filter、Data、Index、Footer）
  ├── 2. Write: 写入 Level 0 目录
  ├── 3. Clean: 删除 IMemTable 关联的 WAL 文件
  ├── 4. Register: 加入 Manager 的内存索引
  └── 5. Compact: 检查 Level 0 是否超限 → 触发 Compaction
```

---

## 10. Compaction 策略

> 源码：`engine/lsm/sstable/compaction.go`、`engine/lsm/sstable/merge.go`

### 10.1 为什么需要 Compaction

随着写入持续进行，SSTable 文件会不断积累。如果不进行 Compaction：
- **读放大恶化**：需要搜索的 SSTable 越来越多
- **空间放大恶化**：同一 key 的多个版本同时占用磁盘
- **墓碑堆积**：已删除的 key 无法被物理回收

Compaction 通过合并多个 SSTable 来解决这些问题。

### 10.2 Leveled Compaction

`go-kv` 采用 **Leveled Compaction** 策略（与 LevelDB/RocksDB 的默认策略相同）。核心思想是：

1. 当某一层的文件数超过阈值时，选择该层的部分文件
2. 找到下一层中与其键范围重叠的文件
3. 合并所有文件，写入下一层
4. 删除原始文件

### 10.3 Level 0 Compaction

Level 0 较特殊，因为其 SSTable 之间可能键范围重叠。Level 0 Compaction 是**同步执行**的，且合并**所有** Level 0 文件：

```
Level 0 Compaction:
  │
  ├── 1. 等待 Level 0 当前 Compaction 完成
  ├── 2. 标记 Level 0 正在 Compaction
  ├── 3. 加载所有 Level 0 SSTable 的 KV 对到内存
  ├── 4. 计算 Level 0 全体的键范围 [globalMin, globalMax]
  ├── 5. 在 Level 1 中找到所有与此范围重叠的 SSTable
  ├── 6. 加载重叠的 Level 1 KV 对
  ├── 7. 合并排序 + 去重 + 墓碑处理 → 写入新的 Level 1 SSTable
  ├── 8. 删除所有旧的 Level 0 + 重叠的 Level 1 文件
  ├── 9. 更新内存索引和稀疏索引
  └── 10. 如果 Level 1 也超限 → 触发 Level 1 Compaction
```

### 10.4 Level 1+ Compaction

Level 1 及更高层的 Compaction 只选择**溢出部分**的文件（超过阈值的最老文件）：

```
Level N Compaction (N >= 1):
  │
  ├── 1. 选择溢出文件: files = levels[N][limit:]（最老的超限文件）
  ├── 2. 加载溢出文件的 KV 对
  ├── 3. 在 Level N+1 中找到重叠的 SSTable
  ├── 4. 合并 → 写入 Level N+1
  ├── 5. 删除旧文件
  └── 6. 如果 Level N+1 也超限 → 递归 Compact
```

### 10.5 K-way 堆排序合并

> 源码：`engine/lsm/sstable/merge.go`

Compaction 的核心是将多个有序 KV 集合合并为一个。`go-kv` 使用**最小堆**（`container/heap`）实现 K-way 合并排序：

```go
type minHeap []*KVEntry

// 按 key 字符串排序
func (h minHeap) Less(i, j int) bool {
    return string(h[i].pair.Key) < string(h[j].pair.Key)
}
```

**合并过程**：

```
CompactAndMergeKVs(kvs, targetLevel)
  │
  ├── 1. 将所有 KV 对推入最小堆
  │
  ├── 2. 循环弹出最小 key:
  │     ├── 与上一个写入的 key 相同? → 跳过（去重，保留最新版本）
  │     ├── 是墓碑 && targetLevel == MaxSSTableLevel? → 丢弃（物理删除）
  │     ├── 是墓碑 && targetLevel < MaxSSTableLevel? → 保留（继续传播）
  │     └── 正常 KV → 写入 SSTable Builder
  │
  ├── 3. Builder 满了? → 刷写为新 SSTable，创建新 Builder
  │
  └── 4. 返回所有生成的 SSTable
```

**去重规则**：由于 KV 对的输入顺序保证了同一 key 的最新版本先于旧版本（来自更高层或更新 ID 的 SSTable），第一个出现的版本就是最新版本，后续的都可以安全跳过。

**墓碑清理**：只在**最大层（Level 6）**的 Compaction 中物理删除墓碑。在非最大层，墓碑必须保留以遮蔽更高层可能存在的旧值。

### 10.6 异步 Compaction

Level 1+ 的 Compaction 在独立的 goroutine 中异步执行，不阻塞前台的读写操作：

```go
func (m *Manager) asyncCompactLevel(level int) {
    go func() {
        m.mu.Lock()
        // 等待该层当前 Compaction 完成
        for m.compactingLevels[level] {
            m.compactionCond.Wait()
        }
        m.compactingLevels[level] = true
        m.mu.Unlock()

        // 执行 Compaction...

        m.mu.Lock()
        m.compactingLevels[level] = false
        m.compactionCond.Broadcast()
        m.mu.Unlock()
    }()
}
```

---

## 11. Raft 存储适配器

> 源码：`pkg/storage/lsm/storage.go`、`pkg/storage/lsm/state_machine.go`

### 11.1 设计动机

Raft 模块需要一个满足 `Storage` 接口的持久化后端来存储日志条目、HardState 和快照。LSM 存储适配器将这些 Raft 概念映射到 LSM 的 KV 操作上，复用已有的存储引擎而不需要另外实现持久化逻辑。

### 11.2 Key Schema

所有 Raft 数据通过特定的 key 前缀组织：

| Key 模式 | 含义 | 值编码 |
|---------|------|--------|
| `meta:hard_state` | Raft HardState | 24B: Term(8) + VotedFor(8) + CommitIndex(8)，大端 |
| `meta:log_meta` | 日志元数据 | 24B: FirstIndex(8) + LastIndex(8) + LogSize(8)，大端 |
| `meta:snapshot` | 快照数据 | Gob 编码的 `param.Snapshot` |
| `log:00000000000000000042` | 日志条目（索引 42） | 二进制编码（见下） |

**日志 key 格式**：`log:` 前缀 + 20 位零填充十进制索引。固定宽度确保了 LSM 中的字典序与日志索引的自然序一致，这对范围扫描和 Compaction 的正确性至关重要。

### 11.3 日志条目编码

日志条目使用紧凑的二进制格式（替代了早期的全量 gob 编码）：

```
┌──────────┬──────────┬──────────┬──────────────────┐
│ Term     │ Index    │ CmdLen   │ Command (Gob)    │
│ (8 bytes)│ (8 bytes)│ (4 bytes)│ (变长)           │
│ big-end  │ big-end  │ big-end  │                  │
└──────────┴──────────┴──────────┴──────────────────┘
```

Term 和 Index 使用固定 8 字节大端编码，Command 字段仍使用 gob（因为 `Command` 类型是 `any`，可能是 `KVCommand`、`ConfigChangeCommand` 或 `[]byte`）。这种混合编码消除了 gob 的 struct 包络开销，同时保持了 Command 类型的灵活性。

### 11.4 元数据缓存

为了避免频繁的磁盘查询，适配器将 `firstIndex`、`lastIndex`、`logSize` 缓存在内存中，并在每次 `AppendEntries`、`TruncateLog`、`CompactLog` 操作后同步更新。三个值通过单次 `db.Put("meta:log_meta", ...)` 批量写入，减少 I/O 次数。

### 11.5 关键操作

**AppendEntries**：批量编码所有条目，逐个 `db.Put`，最后一次性更新 `log_meta`。

**TruncateLog(fromIndex)**：从 `fromIndex` 到 `lastIndex` 逐个 `db.Delete`，更新元数据。用于 Follower 截断与 Leader 冲突的日志。

**CompactLog(upToIndex)**：从 `firstIndex` 到 `upToIndex` 逐个 `db.Delete`，更新 `firstIndex`。用于快照后压缩已被快照覆盖的旧日志。

### 11.6 状态机适配器

`StateMachineAdapter` 将 Raft 的 `Apply` 调用转化为 LSM 的 `Put`/`Delete` 操作：

```go
func (sm *StateMachineAdapter) Apply(entry param.LogEntry) any {
    cmd := entry.Command.([]byte)
    var kvCmd param.KVCommand
    json.Unmarshal(cmd, &kvCmd)

    switch kvCmd.Op {
    case param.OpSet:
        sm.db.Put(kv.Key(kvCmd.Key), kv.Value(kvCmd.Value))
    case param.OpDelete:
        sm.db.Delete(kv.Key(kvCmd.Key))
    }
}
```

**快照操作**：

- `GetSnapshot()`：强制 flush 所有 MemTable，然后读取所有 SSTable 文件内容，JSON 序列化为 `map[string][]byte`
- `ApplySnapshot(data)`：关闭 DB，清空数据目录，写入快照文件，重新初始化 Manager

---

## 12. 配置参数

> 源码：`pkg/config/config.go`

### 12.1 LSM 配置项

| 参数 | 配置路径 | 默认值 | 说明 |
|------|---------|--------|------|
| `MaxMemTableSize` | `lsm.max_mem_table_size` | **2,097,152 (2 MB)** | MemTable 达到此大小后触发 promote |
| `MaxSSTableSize` | `lsm.max_sstable_size` | **2,097,152 (2 MB)** | 单个 SSTable 的最大大小，Compaction 时如果超过会分裂 |
| `MaxIMemTableCount` | `lsm.max_imem_table_count` | **10** | IMemTable 队列的最大长度，超过后驱逐最老的 |
| `MinSSTableLevel` | `lsm.min_sstable_level` | **0** | SSTable 的起始层级 |
| `MaxSSTableLevel` | `lsm.max_sstable_level` | **6** | SSTable 的最大层级（共 7 层） |
| `LevelSizeBase` | `lsm.level_size_base` | **2** | 每层容量的指数底数 |

### 12.2 调优建议

**写入优先场景**（如 Raft 日志存储）：
- 增大 `MaxMemTableSize`（如 4 MB）：减少 promote 和 flush 频率
- 增大 `MaxIMemTableCount`（如 20）：允许更多 IMemTable 缓冲，减少 SSTable 创建的反压
- 代价：更高的内存占用，崩溃恢复时 WAL 回放更慢

**读取优先场景**：
- 减小 `MaxMemTableSize`（如 1 MB）：更频繁地 flush，减少内存中的搜索层数
- 减小 `LevelSizeBase`：更积极的 Compaction，减少每层的 SSTable 数量
- 代价：更高的写放大

**大数据集**：
- 增大 `MaxSSTableLevel`：支持更多数据层级
- 增大 `LevelSizeBase`：每层容纳更多文件
- 代价：更高的读放大（更多层需要搜索）

### 12.3 内部常量

以下是硬编码在源码中的常量，不可通过配置文件修改：

| 常量 | 值 | 位置 | 说明 |
|------|------|------|------|
| 跳表最大层数 | 32 | `skiplist.go` | 支持 4³² 个元素 |
| 跳表升层概率 | 0.25 | `skiplist.go` | 平衡插入和查找性能 |
| WAL 缓冲区 | 32 KB | `wal.go` | bufio 写缓冲 |
| 布隆过滤器位数 | 1,600,000 | `bloom.go` | ~200 KB 每个过滤器 |
| 布隆过滤器哈希数 | 16 | `bloom.go` | 与位数组大小匹配 |
| Footer 大小 | 32 bytes | `footer.go` | 固定大小便于定位 |
| KV Key 最大长度 | 1 MB | `kv.go` | 防止异常大 key |
| KV Value 最大长度 | 1 GB | `kv.go` | 防止异常大 value |

---

## 附录：常用术语对照

| 术语 | 全称 | 含义 |
|------|------|------|
| LSM | Log-Structured Merge-Tree | 日志结构合并树 |
| WAL | Write-Ahead Log | 预写日志 |
| SSTable | Sorted String Table | 有序字符串表 |
| MemTable | Memory Table | 内存表 |
| IMemTable | Immutable MemTable | 不可变内存表 |
| Compaction | — | 合并压缩 |
| Tombstone | — | 墓碑（删除标记） |
| Bloom Filter | — | 布隆过滤器 |
| FPR | False Positive Rate | 假阳性率 |

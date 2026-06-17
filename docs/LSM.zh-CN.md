# LSM-tree 存储设计

English version: [LSM.md](LSM.md)

本文解释 `go-kv` 使用的 LSM-tree 引擎。它面向刚接触存储引擎的读者，因此会同时解释数据结构和每个模块存在的原因。

相关源码：

- `engine/lsm/database`
- `engine/lsm/kv`
- `engine/lsm/memtable`
- `engine/lsm/sstable`
- `engine/lsm/wal`
- `pkg/storage/lsm`

## 1. 为什么使用 LSM-tree

Raft 会产生写密集工作负载。每个已提交操作都会成为日志条目，每个节点都必须持久化这些日志。LSM-tree 很适合这种场景，因为它把大量小随机写转换成顺序写。

核心思想很简单：

```text
写 WAL
        |
        v
写内存有序表
        |
        v
内存表满后 flush 成磁盘有序文件
        |
        v
后台合并磁盘文件
```

LevelDB 和 RocksDB 等系统都使用类似设计。

## 2. 模块布局

| 模块 | 路径 | 职责 |
|---|---|---|
| 数据库门面 | `engine/lsm/database` | 对外提供 `Put`、`Get`、`Delete`、恢复和 flush 协调。 |
| KV 编码 | `engine/lsm/kv` | Key/value 表示、二进制编码、tombstone。 |
| WAL | `engine/lsm/wal` | 活跃 memtable 的追加式恢复日志。 |
| MemTable | `engine/lsm/memtable` | 基于跳表的内存有序数据。 |
| SSTable | `engine/lsm/sstable` | 不可变有序文件、索引、布隆过滤器、compaction。 |
| Raft 适配器 | `pkg/storage/lsm` | 将 LSM 用作 Raft 稳定存储和状态机存储。 |

## 3. 数据库目录布局

每个 LSM 数据库拥有一个目录：

```text
lsm_database/
├── wal/
│   ├── 1.wal
│   └── 2.wal
└── sst/
    ├── 0-level/
    │   ├── 10.sst
    │   └── 11.sst
    ├── 1-level/
    │   └── 7.sst
    └── 6-level/
```

对于 LSM-backed Raft 节点，会有两个数据库：

```text
data/node-1/
├── lsm_raftlog/
└── lsm_statemachine/
```

这样可以避免 Raft 元数据和用户键值数据混在一起。

## 4. 数据模型

基础记录是：

```go
type Key string
type Value []byte

type KeyValuePair struct {
    Key   Key
    Value Value
}
```

记录使用长度前缀编码：

```text
KeyLen | KeyBytes | ValueLen | ValueBytes
```

这个格式简单、确定，并且容易从磁盘恢复。

## 5. 删除和 Tombstone

LSM-tree 不会立刻移除旧值。删除会写入一个特殊 tombstone：

```go
KeyValuePair{Key: key, Value: kv.DeletedValue}
```

为什么重要：

```text
Level 0: delete(k)      最新
Level 2: k = old-value  更旧
```

如果 tombstone 太早消失，`old-value` 会重新可见。因此引擎会保留 tombstone，直到 compaction 到达可以证明旧版本不再存在的层级。

## 6. 写路径

`Put` 和 `Delete` 的写路径：

```text
Database.Put/Delete
        |
        v
MemTableManager.Insert/Delete
        |
        v
活跃 MemTable 先写 WAL
        |
        v
插入跳表
        |
        v
如果活跃 MemTable 满：
        将它提升为 immutable MemTable
        创建新的活跃 MemTable
        调度旧 immutable MemTable flush
```

WAL 写入发生在跳表更新之前。如果进程在 WAL 写入后崩溃，恢复时可以重放 WAL 重建 memtable。

## 7. MemTable 和 Immutable MemTable

活跃 MemTable 接收写入，内部使用跳表保持 key 有序，并支持低成本范围遍历。

当活跃 MemTable 过大：

1. 它变为 immutable memtable；
2. 新的活跃 MemTable 开始接收写入；
3. flush worker 将某个 immutable memtable 写成 Level 0 SSTable。

关键不变量：

```text
正在 flush 的 immutable memtable 必须保持可搜索，直到 SSTable 发布成功。
```

否则读请求可能错过已经离开活跃 memtable、但还没进入 SSTable 元数据的数据。

## 8. WAL

WAL 是某个 memtable 的追加式文件。每次 mutation 在插入跳表前先写 WAL。

恢复流程：

1. 按 ID 顺序读取 WAL 文件。
2. 较旧 WAL 文件恢复成 immutable memtable。
3. 最新 WAL 文件恢复成活跃 memtable。
4. Tombstone 像普通记录一样重放。

WAL 保护尚未 flush 到 SSTable 的最近写入。

## 9. SSTable 格式

SSTable 是不可变且按 key 排序的文件。当前实现中，一个 SSTable 文件包含一个逻辑数据区和一个索引区。实际落盘顺序是：

```text
+---------+--------------+-------------+-------------+--------+
| Header  | FilterBlock  | Data values | IndexBlock  | Footer |
+---------+--------------+-------------+-------------+--------+
```

一个容易踩坑的细节是：`DataBlock` 只保存 value。`IndexBlock` 保存排序后的 key 以及 value 在数据区中的偏移。恢复 key/value pair 时，是把 `IndexBlock[i].Key` 和 `DataBlock[i]` 配对。

布隆过滤器快速回答：

```text
这个 key 一定不在这里
```

或者：

```text
这个 key 可能在这里
```

布隆过滤器可能误判存在，但不会误判不存在。只要布隆过滤器返回“一定不存在”，就可以跳过该 SSTable 的数据区读取。

## 10. 读路径

读请求从新到旧搜索：

```text
Database.Get(key)
        |
        v
活跃 MemTable
        |
        v
immutable MemTable，从新到旧
        |
        v
Level 0 SSTable，从新到旧
        |
        v
Level 1..N SSTable，通过稀疏索引定位
```

Level 0 文件来自 memtable flush，key 范围可能重叠，因此必须按从新到旧搜索。

Level 1 及更深层由 compaction 生成，同一层内应该不重叠，因此可以更快定位候选文件。

## 11. Compaction

Compaction 将某一层 SSTable 合并到下一层。

它的作用：

- 移除被覆盖的旧版本；
- 最终移除 tombstone；
- 降低读放大；
- 控制每层文件大小。

合并规则：

```text
同一个 key，最新记录获胜。
```

如果最新记录是最大层的 tombstone，引擎可以在它遮蔽所有旧值后丢弃它。

从读者视角看，SSTable 元数据更新必须是原子的。读者应该看到旧文件集合或新文件集合，而不是半更新状态。

Compaction 还要区分 stale metadata 和真实文件损坏。如果内存目录引用的文件已经不存在，manager 会剪掉这个过期元数据并继续。如果文件存在但无法解码，compaction 仍然返回错误。这样既保持对真实损坏的严格性，又允许文件目录中的 stale entry 自恢复。

## 12. Raft 日志存储适配器

`pkg/storage/lsm/storage.go` 基于 LSM 实现 `storage.Storage`。

它存储：

- hard state；
- Raft 日志条目；
- 日志元数据（`firstIndex`、`lastIndex`、`logSize`）；
- 快照。

Raft 日志条目使用紧凑二进制格式：

```text
GLG1 | term | index | command_length | encoded_command
```

支持的命令类型通过显式 tag 编码：

- nil command；
- byte slice；
- string；
- KV command；
- config-change command；
- client command wrapper。

未知旧 gob 日志数据不会被当成兼容 Raft 日志数据。缺少或无效 magic 会快速失败。

## 13. 状态机适配器

`pkg/storage/lsm/state_machine.go` 实现键值状态机。

Apply 流程：

```text
Raft 已提交 LogEntry
        |
        v
如果存在 ClientCommand 则解包
        |
        v
解码 KVCommand
        |
        v
Database.Put/Delete
```

读流程：

```text
Raft ReadIndex 确认安全
        |
        v
StateMachine.Get(key)
        |
        v
Database.Get(key)
```

状态机快照编码为 LSM 文件的二进制归档。归档中使用长度前缀保存文件名和原始字节。

Snapshot 导出会在持有 SSTable manager read lock 时打开所有准备复制的 SSTable 文件，之后从这些已打开的 fd 读取数据。这可以抵抗并发 compaction：即使 compaction 随后删除了目录项，已打开 fd 仍然指向 snapshot 选择的文件内容。

## 14. 恢复

恢复分层进行：

1. 从 WAL 恢复 memtable。
2. 从磁盘恢复 SSTable 元数据。
3. 恢复 Raft hard state 和日志元数据。
4. 从 LSM 目录或 Raft 快照恢复状态机。

重要恢复规则：

- Level 0 SSTable 恢复后必须按从新到旧搜索。
- Tombstone 必须像普通记录一样恢复和 flush。
- 日志元数据必须隐藏已 compact 或已 truncate 的索引。
- 重新追加的日志条目必须替换该索引旧值。

## 15. 配置

重要 LSM 配置：

| 配置 | 含义 |
|---|---|
| `lsm.max_mem_table_size` | 活跃 memtable 提升前的大小。 |
| `lsm.max_sstable_size` | SSTable builder flush 的目标大小。 |
| `lsm.max_imem_table_count` | immutable memtable 触发 flush 压力的数量。 |
| `lsm.min_sstable_level` | 最低 SSTable 层级，通常是 0。 |
| `lsm.max_sstable_level` | 最深 compaction 层级。 |
| `lsm.level_size_base` | 层级容量增长因子。 |

较小值可以让测试快速触发 flush 和 compaction。较大值可以减少后台工作，但会在 compaction 前使用更多内存和磁盘。

## 16. 具体 SSTable 落盘布局

必须理解真实文件布局，因为很多存储 bug 都来自把元数据和载荷数据混在一起理解。

```text
offset 0
|
v
Header
FilterBlock
Data values
IndexBlock
Footer                         文件末尾固定 32 字节
```

主要结构：

| 部分 | 代码 | 保存内容 | 作用 |
|---|---|---|---|
| `Header` | `engine/lsm/sstable/block/header.go` | 最小 key、最大 key | 快速排除不可能命中的文件，也用于稀疏索引排序。 |
| `FilterBlock` | `engine/lsm/sstable/bloom` | 布隆过滤器 bit | key 一定不存在时跳过后续读取。 |
| `DataBlock` | `engine/lsm/sstable/block/data.go` | 只有 value 字节 | 紧凑保存实际值。 |
| `IndexBlock` | `engine/lsm/sstable/block/index.go` | 排序 key 和 value offset | 重建 key/value pair，并支持 seek。 |
| `Footer` | `engine/lsm/sstable/block/footer.go` | `DataHandle` 和 `IndexHandle` | 从文件尾部定位数据区和索引区。 |

`Footer` 固定 32 字节：

```text
DataHandle.Offset  uint64 little-endian
DataHandle.Size    uint64 little-endian
IndexHandle.Offset uint64 little-endian
IndexHandle.Size   uint64 little-endian
```

`SSTable.DecodeFrom` 只加载元数据：header、filter、footer 和 index。它不会加载 value。value 通过 `DecodeDataBlock` 懒加载：先 seek 到 `Footer.DataHandle.Offset`，然后只解码 `Footer.DataHandle.Size` 指定的字节数。

这里有一条关键规则：

```go
t.DataBlock = block.NewDataBlock()
```

必须在解码 data values 之前执行。否则重复读取同一个 SSTable 时，新的解码结果会追加到旧的内存 slice 里，最终导致 `DataBlock` 和 `IndexBlock` 长度不一致。

## 17. 内存中的 SSTable 元数据

`engine/lsm/sstable.Manager` 是内存文件目录的唯一 owner。它的状态由 `Manager.mu` 保护。

| 字段 | 含义 |
|---|---|
| `levels [][]*SSTable` | 按层保存 SSTable。Level 0 必须从新到旧搜索。 |
| `fileIndex map[string]*SSTable` | 通过文件路径快速找到 SSTable。 |
| `totalMap map[int][]string` | 按层保存文件路径。 |
| `sparseIndexes [][]*SSTable` | Level 1 及更深层按最小 key 排序的稀疏索引。 |
| `compactingLevels map[int]bool` | 正在 compaction 的层级。 |

一次 flush 只有在 SSTable 元数据发布之后，才算对读请求可见：

```text
immutable memtable
        |
        v
BuildSSTableFromIMemTable
        |
        v
EncodeTo(file)
        |
        v
addTable(sst) 在 Manager.mu 下发布元数据
        |
        v
imem.Clean() 成功后才删除旧 WAL
```

因此，正在 flush 的 immutable memtable 必须保持可搜索，直到 SSTable 编码并发布成功。如果在 `addTable` 前就让它不可见，读请求可能漏掉已经离开活跃 memtable、但还没进入 SSTable 元数据的 key。

恢复也有顺序要求。Level 0 文件范围可能重叠，必须先查最新文件。`Recover` 先按 ID 升序遍历文件，再调用会插入队首的 `addTable`，这样恢复后的 Level 0 仍然是从新到旧。

## 18. LSM 中的 Raft 日志 keyspace

Raft 存储适配器把共识元数据和日志条目作为普通 LSM key 存储。keyspace 很小，并且是显式的。

| Key | Value 格式 |
|---|---|
| `meta:hard_state` | 24 字节：`currentTerm`、`votedFor`、`commitIndex`，都是 big-endian uint64。 |
| `meta:log_meta` | 24 字节：`firstIndex`、`lastIndex`、`logSize`，都是 big-endian uint64。 |
| `meta:snapshot` | gob 编码的 `param.Snapshot`。 |
| `log:00000000000000000001` | 二进制 Raft 日志条目。 |

日志 key 是固定宽度字符串：

```text
"log:" + 20 位十进制索引，不足补 0
```

这样可以让字符串字典序等价于数字顺序。Raft 日志条目的格式是：

```text
4 bytes   magic "GLG1"
8 bytes   term, big-endian
8 bytes   index, big-endian
4 bytes   command length, big-endian
N bytes   tagged command payload
```

命令 payload 使用 1 字节 tag 区分 nil、bytes、string、KV command、配置变更和 client command wrapper。日志 decoder 必须看到 `GLG1` magic；未知旧 gob 日志不会被当作兼容日志处理。Raft 快照仍然使用 gob，因为它是 `meta:snapshot` 下的独立 value。

适配器用自己的 `StorageAdapter.mu` 缓存 `firstIndex`、`lastIndex` 和 `logSize`。这三个值定义逻辑日志窗口：

```text
firstIndex <= 可见日志索引 <= lastIndex
```

`GetEntry` 对窗口外索引返回 nil，即使底层 LSM 里还残留旧物理 key。`AppendEntries`、`TruncateLog`、`CompactLog` 必须同时维护物理 LSM 操作和这个逻辑窗口。

## 19. 容易出错的边界和防线

这个项目里最难的 LSM bug 往往不在 skiplist 或文件编码本身，而在模块边界：

| 边界 | 失败模式 | 防线 |
|---|---|---|
| Memtable flush | immutable memtable 正在写盘时 key 短暂消失。 | flush 中的 immutable memtable 保持可搜索，直到 SSTable 发布。 |
| Level 0 恢复 | 重启后旧表遮蔽新数据。 | 恢复后仍保持 Level 0 从新到旧查询。 |
| Tombstone compaction | 已删除 key 从旧层重新出现。 | 只有确定不存在更旧版本时才丢弃 tombstone。 |
| SSTable 懒加载 | 重复读取追加重复 value。 | 每次 decode 前 reset `DataBlock`。 |
| Raft truncate/reappend | 同一 index 上旧日志 payload 和新日志 payload 混用。 | 重写已有 log key 时先扣除旧大小，再写新值。 |
| Raft compaction | apply loop 读取已被压缩的 committed entry。 | Raft 必须通过覆盖该 index 的 snapshot 跳过；没有覆盖 snapshot 时才应明显失败。 |
| Compaction 目录清理 | 元数据引用已经删除的文件。 | 缺失文件可以剪掉元数据，但存在且损坏的文件仍然是硬错误。 |

这些防线在 review 中必须被当作正确性要求，而不是性能细节。

## 20. 修改 LSM 时的不变量

修改 LSM 代码时必须保持：

- WAL append 发生在活跃 memtable 修改之前。
- 正在 flush 的 immutable memtable 保持可搜索，直到 flush 成功。
- Level 0 查询顺序是从新到旧。
- Tombstone 必须遮蔽旧值，直到可以安全丢弃。
- SSTable decode 在填充可复用结构前必须 reset。
- Compaction 元数据更新受 manager lock 保护。
- 只有确认物理 SSTable 文件不存在时，才能剪掉 missing-file 元数据；存在但损坏的文件仍然必须报错。
- Raft 日志读取必须遵守逻辑 `[firstIndex, lastIndex]` 窗口。
- 快照导出和应用不能与状态机写入并发冲突。

大多数存储 bug 都是这些不变量之一被破坏。

近期具体故障和修复过程见 [BUG_FIX_RETROSPECTIVE.zh-CN.md](BUG_FIX_RETROSPECTIVE.zh-CN.md)。

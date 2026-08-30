# Solana Snapshot 解析过程详解

本文说明本项目如何解析 Solana snapshot，以及为什么当前 ETL 使用 `updated_slot` 作为账户版本、使用 tombstone 表达 SPL Token 账户删除。当前输出目标为 ClickHouse，本文重点补充 ClickHouse 增量入库语义。

这里需要先区分两件事：

1. snapshot 中的 AppendVec 是账户的物理存储文件；
2. AccountsDb index 才是 Solana 判断某个 pubkey 当前有效版本的逻辑依据。

因此，snapshot 不是交易日志，也不是可以依靠 AppendVec 文件顺序重放的历史记录。它是 Agave 在某个 bank slot 上整理出来的 canonical checkpoint。

核心代码：

- `src/append_vec.rs`：本项目解析 AppendVec 二进制布局；
- `src/archived.rs`、`src/unpacked.rs`：读取 tar archive 或已解压 snapshot；
- `src/bin/solana-snapshot-etl/clickhouse.rs`：解析账户并写入 ClickHouse；
- `agave/runtime/src/accounts_background_service.rs`：官方 full/incremental snapshot 生成前的 flush、clean、shrink 流程；
- `agave/snapshots/src/archive.rs`：官方归档时过滤 obsolete 账户和 tombstone 的流程；
- `agave/runtime/src/serde_snapshot.rs`：官方 snapshot manifest 和 AccountsDb 字段；
- `agave/accounts-db/src/accounts_db.rs`：官方 AccountsDb index、duplicate 和 obsolete 处理。

---

## 1. Full 和 incremental snapshot 的官方语义

### 1.1 Full snapshot

Agave 在生成 full snapshot 前大致执行：

~~~text
set_latest_full_snapshot_slot(S)
  -> force_flush_accounts_cache()
  -> clean_accounts()
  -> shrink_ancient_slots()
  -> shrink_candidate_slots()
  -> get_snapshot_storages(None)
  -> serialize + archive
~~~

可参见 [accounts_background_service.rs](../agave/runtime/src/accounts_background_service.rs) 和 [bank.rs](../agave/runtime/src/bank.rs)。

`get_snapshot_storages(None)` 获取当前 bank slot 之前仍有账户的 storage。由于此前已经执行 clean 和 shrink，归档内容不是原始 AppendVec 历史，而是经过 AccountsDb 整理后的状态。

full archive 通过 `AccountStorageReader` 排除：

- 已标记为 obsolete 的旧账户记录；
- tombstone 记录。

因此，full snapshot 可以作为完整基线直接重建账户表，但它不会携带“某个旧账户何时被删除”的历史 tombstone。

### 1.2 Incremental snapshot

incremental snapshot 的 base slot 是最近一次 full snapshot 的 slot，而不是上一个 incremental snapshot 的 slot：

~~~text
full:              S_full
incremental:       [S_full, S_incremental]
~~~

官方调用的是：

~~~rust
bank.get_snapshot_storages(Some(full_snapshot_slot))
~~~

也就是选取 `storage.slot() > S_full` 的 storage。后一个增量包仍可能包含 base 之后旧范围内的 storage：

~~~text
[S_full, 1200]
[S_full, 1400]
~~~

第二个包不是简单的“前一个包再加 1201～1400 的文件”。旧 slot 的 storage 可能在中间经历 shrink，因而文件名、AppendVec ID、长度、offset 和物理内容都可能变化。

incremental archive 与 full archive 的关键区别是：

~~~rust
Full        -> TombstonesFilter::Exclude
Incremental -> TombstonesFilter::Include
~~~

这是因为 full 已经是完整状态，而 incremental 必须把 full base 中需要删除的账户传播下去。

### 1.3 官方如何加载 full + incremental

官方加载时先解压 full，再解压 incremental，然后把两组 storage 合并，最后重新构建 AccountsDb index。incremental 的 bank fields 代表最新 bank 状态，但账户 storage 是 full 和 incremental 的并集。

加载 archive 时，Agave 还可能重新映射 AppendVec ID，以避免 full 与 incremental 的文件 ID 冲突。这说明 AppendVec ID 不是跨 snapshot 稳定的版本号。

---

## 2. AccountsDb 如何决定账户的逻辑版本

### 2.1 一个 slot 只有一个 storage

官方 snapshot manifest 的 `SlotAccountStorageEntries` 在真实 snapshot 中每个 slot 只有一个 storage entry。加载器如果发现同一 slot 有多个 storage，会直接报错。

所以“同一个 slot 下多个不同 AppendVec ID 如何排序”不是正常的 canonical snapshot 场景。物理运行时可能在 shrink 过程中暂时同时存在旧、新 storage，但 snapshot 导出完成后只保留一个有效 storage。

### 2.2 同一个 pubkey 的多个版本

在 AccountsDb index 中，同一个 pubkey 可以暂时拥有多个不同 slot 的索引项：

~~~text
pubkey -> [(slot_1, location_1), (slot_2, location_2), ...]
~~~

官方最终取最大 slot 作为当前版本，较低 slot 的项标记为 duplicate/obsolete。官方代码在启动时明确按最高 slot 保留账户，其余版本用于计算 duplicate 数据并随后清理。

因此，逻辑版本关系是：

~~~text
(pubkey, 最大 updated_slot) = 当前有效状态
~~~

不是：

~~~text
(pubkey, append_vec_id, account.offset) = 当前有效状态
~~~

正常导出的 canonical archive 通常已经过滤掉旧版本，所以实际 archive 中每个 pubkey 通常只出现一条非 tombstone 记录。AccountsDb 之所以仍然支持跨 slot duplicate，是为了启动重建和兼容尚未清理的存储。

同一 pubkey、同一 slot 的多个有效版本不应出现在 canonical archive 中。若遇到这种情况，应视为异常，不能用 AppendVec ID 或 offset 声称恢复了真实交易顺序。

### 2.3 shrink 为什么会重写旧 slot 文件

shrink 会创建一个新的 storage，但新 storage 保持原来的 slot：

~~~text
old: (slot, old_append_vec_id, old_offset)
new: (同一个 slot, new_append_vec_id, new_offset)
~~~

AccountsDb index 只把同一账户的物理位置替换成新的 `(store_id, offset)`。所以后续 snapshot 中旧 slot 文件内容发生变化，并不代表旧 slot 又发生了链上交易；通常只是物理压缩和重组。

### 2.4 AppendVec 遍历顺序没有业务含义

归档时官方 `AccountStoragesOrderer` 会按照文件大小交错排列 storage，以优化 I/O。构建 index 时还会随机并发扫描 storage。

因此以下顺序都不能当作交易顺序：

- tar 中 AppendVec 文件出现的顺序；
- AppendVec 文件 ID 的大小；
- 账户在 AppendVec 内的 byte offset；
- `append_vec_id` 和 `account.offset` 的字典序。

当前 snapshot wire 中原先名为 `write_version` 的字段已经是 unused/兼容字段，通常为 0。它不能用于恢复 snapshot 内的同 slot 写入顺序。

---

## 3. AppendVec 的二进制布局

本项目的底层解析仍然从 AppendVec 的固定内存布局开始。一个账户记录大致是：

~~~text
[ StoredMeta ][ AccountMeta ][ Hash ][ account_data ][ padding ]
~~~

关键结构位于 `src/append_vec.rs`：

~~~rust
pub struct StoredMeta {
    pub write_version: StoredMetaWriteVersion,
    pub data_len: u64,
    pub pubkey: Pubkey,
}
~~~

~~~rust
#[repr(C)]
pub struct AccountMeta {
    pub lamports: u64,
    pub rent_epoch: Epoch,
    pub owner: Pubkey,
    pub executable: bool,
}
~~~

读取一个账户时，解析器依次读取 `StoredMeta`、`AccountMeta`、账户 hash 和 `data_len` 字节的 data，并将下一个 offset 向上对齐到 8 字节边界。

这里的 `offset` 只是文件内物理地址，用于读取数据；它不是版本字段。

---

## 4. 从通用账户到 Token 账户

解析出 `StoredAccountMeta` 后，代码根据 `account.account_meta.owner` 分流：

~~~text
owner == SPL Token Program
    -> spl_token::state::Account/Mint::unpack

owner == Token-2022 Program
    -> spl_token_2022::state::Account/Mint::unpack

owner == Metaplex Metadata Program
    -> Borsh deserialize Metadata
~~~

SPL Token 和 Token-2022 的账户 data 不是通用 AppendVec 元数据，而是各自程序定义的二进制结构。必须先通过对应 crate 的 `unpack` 解析，再写入 ClickHouse 的业务字段。

同理，Metaplex metadata 账户要按照其 Borsh 结构反序列化，不能把 data bytes 当成 SPL Token 数据。

---

## 5. CloseAccount 和 tombstone

### 5.1 为什么关闭后看不到原来的 token 字段

AccountsDb 会把 zero-lamport 账户规范化为 canonical empty account，通常表现为：

~~~text
lamports = 0
data_len = 0
owner = Pubkey::default()
executable = false
~~~

因此 CloseAccount 归档记录中已经没有原来的 mint、token owner、close authority 等信息。仅靠这个空账户无法重新 unpack 出 SPL Token Account。

它只能作为删除候选。导入器记录 empty account 的 pubkey 和 AppendVec 所属 slot，在本组
`hot_token_account_state FINAL` 按 pubkey 查找旧的 live 行。只有找到了 hot Token Account，
才写入一条更高 `updated_slot`、`is_deleted=1` 的 tombstone，并复用旧行的 `mint`/`owner`。
普通 canonical empty account 不进入 L2，因此不会积累空 mint/owner 的无意义删除行。

### 5.2 Full 和 incremental 对 tombstone 的区别

- full snapshot 排除 tombstone；
- incremental snapshot 保留 tombstone。

所以从 full 开始重建时，已关闭的 token account 不需要插入删除行；它本来就不应出现在基线中。

但在已有数据库上持续消费 incremental 时，必须处理 tombstone，否则旧的 live token 行会一直保留，导致 holder 和 token 分布统计错误。

### 5.3 ClickHouse 表的最终版本设计

`hot_token_account_state` 是唯一的 Token Account 状态表；解析器仅对本组冻结 hot mint
直接写入该表，不再使用 `raw_token_account` 中转：

~~~sql
is_deleted   UInt8 DEFAULT 0
updated_slot UInt64
ENGINE = ReplacingMergeTree(updated_slot, is_deleted)
ORDER BY pubkey
~~~

其中：

- `is_deleted = 0`：正常 token account；
- `is_deleted = 1`：CloseAccount tombstone；
- `updated_slot`：账户版本所属的链上 slot；
- `append_vec_id`、`account_offset`、`final_version`：不再作为业务字段或版本字段。

强一致查询仍应使用：

~~~sql
SELECT *
FROM solana.hot_token_account_state FINAL
WHERE is_deleted = 0;
~~~

完整 DDL 以 [clickhouse_schema.md](clickhouse_schema.md) 为准。

---

## 6. 增量 ETL 的截止 slot 和文件过滤

监听模式不再接收人工指定的截止 slot。默认启动时查询
`solana.raw_account` 的 `max(updated_slot)`，回退 1000（最小为 0）作为恢复 slot；回退值可通过
`--resume-slot-rewind` 配置；每次只处理：

~~~text
append_vec.slot() > resume_slot
~~~

传入 `--bootstrap` 时，恢复 slot 固定为 0，且必须先成功导入一个 full snapshot，之后才允许处理
incremental snapshot。这样新库一定以完整基线开始；不传该参数时则使用数据库水位线回退后的结果，
让回退范围内的 slot 数据可重复写入以覆盖可能的边界遗漏。

首次启动时如果找不到能够推进恢复 slot 的合适快照，程序直接报错退出；如果最早的增量包
`base_slot` 高于恢复 slot 且没有可桥接的 full snapshot，也视为 slot 断档。首次成功入库后，
后续扫描暂时找不到新文件时才按轮询间隔等待。

这样做的依据不是“旧文件不会变化”，而是：

1. 旧 slot 文件即使变化，通常只是 shrink 的物理重组；
2. 账户逻辑版本仍由 slot 决定；
3. 已经处理过的 slot 不需要因为 AppendVec ID/offset 改变而再次写入；
4. 后续 incremental 通常是从同一个 full base 累积到更高 slot 的 storage 集合。

这里的“没有空洞”指没有漏掉需要处理的 storage slot 范围，不是要求每个链上 slot 都有一个文件。很多 slot 没有账户写入，本来就不会产生 AppendVec。

这个优化有一个前提：此前的 incremental 必须已经被正确处理。如果某个 CloseAccount tombstone 在旧 incremental 中漏掉，后续 snapshot 的 shrink 重组不会可靠地补充这次逻辑删除。

此外，不能在保留旧数据库状态的情况下直接用一个新的 full snapshot 跨过未处理的 incremental，因为 full 会排除 tombstone。新的 full 更适合作为重新建立基线的起点。

---

### 6.1 Full 的 ClickHouse 快速入库路径

由于 Agave 在归档 full snapshot 时使用 `TombstonesFilter::Exclude`，full archive
不会包含需要传播的 tombstone。项目因此按 snapshot 类型选择入库路径：

- **full**：直接把 AppendVec 中的 canonical 账户写入 `raw_account`；只有本组冻结 hot mint
  的 Token Account、Mint 与 metadata 写入 L2 或 hot-only raw 表；不建立关闭账户候选集合；
- **incremental**：收集 canonical empty 账户，按 pubkey 去重后在 L2 做 point lookup。只有
  已存在的 hot Token Account 才追加携带原 mint/owner 的 `is_deleted=1` tombstone；不做 raw
  历史扫描。

这不改变版本语义：`hot_token_account_state` 仍使用
`ReplacingMergeTree(updated_slot, is_deleted)`，后续增量仍可覆盖 full 写入的行。对于已经有
旧数据、且中间漏掉了 incremental 的数据库，不能把新的 full 当作删除历史的补丁；full 本身
不携带这些历史 tombstone，应该从新的 full 重新建立基线，再连续应用之后的 incremental。

## 7. 完整链路总结

~~~text
snapshot archive
  -> snapshot manifest / AccountsDbFields
  -> accounts/<slot>.<append_vec_id>
  -> AppendVec 二进制记录
  -> StoredMeta + AccountMeta + hash + data
  -> owner 分流
     -> SPL Token / Token-2022 unpack
     -> Metaplex Borsh deserialize
  -> ClickHouse raw_account
  -> frozen-hot only: raw_token_mint / raw_token_metadata / hot_token_account_state
~~~

版本和删除语义则是：

~~~text
业务版本：updated_slot
物理位置：append_vec_id、account.offset（仅审计，不排序）
删除版本：updated_slot 更高的 is_deleted=1 tombstone
~~~

最终结论：

1. snapshot 是官方整理后的 canonical 状态，不是 AppendVec 日志；
2. 同一 pubkey 的逻辑新旧关系由 slot 决定；
3. 同一 slot 的多个物理文件或多个记录不能用物理顺序解释，canonical archive 中也不应出现；
4. `write_version`、AppendVec ID 和 offset 都不能作为链上版本；
5. full 用来建立完整基线，incremental 用来传播 full 之后的账户变化和删除；
6. SPL Token 删除必须用 `updated_slot + is_deleted` tombstone 表达；删除行只在同组 L2 已有
   旧 hot 行时写入，因此保留该行的 mint/owner；
7. `raw_token_account` 已移除，ClickHouse 的 Token Account 当前态只由 L2 表维护。

# Solana Snapshot 解析过程详解

本文以这个仓库的实现为基础，说明 snapshot 是如何被读取、解析、映射成 SQLite 表的。重点解释：

- snapshot 里每个账户是怎么布局的；
- 代码中用什么结构体、偏移量和对齐规则从二进制数据里取字段；
- 这些字段最终如何落到 SQLite 的 `account`、`token_account`、`token_mint`、`token_multisig` 和 `token_metadata` 表。

核心代码入口：

- `src/append_vec.rs`：定义 `AppendVec`、`StoredMeta`、`AccountMeta` 与 account 的内存布局；
- `src/solana.rs`：定义 `DeserializableVersionedBank` 与 `AccountsDbFields<T>`，负责从 snapshot manifest 读取 bank 结构；
- `src/bin/solana-snapshot-etl/sqlite.rs`：负责把解析出的账户数据落库。

---

## 1. 解析入口：snapshot manifest 与 bank

Solana snapshot 并不是简单的 “账户列表 JSON” 或 “某个表 dump”。它本质上是一个经过序列化的 Solana runtime state，里面包含：

- 当前 bank / slot / epoch 信息；
- accounts DB 的索引结构；
- 每个 append vec 中存放的所有账户数据。

这个项目在读取 snapshot 时会先定位 snapshot manifest 文件，然后用 bincode 反序列化为 `DeserializableVersionedBank`：

```rust
let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
```

`DeserializableVersionedBank` 里有一组关键字段：

- `slot`: 当前 snapshot 对应的 slot；
- `epoch`: 当前 epoch；
- `block_height`: 区块高度；
- `accounts_data_len`: snapshot 中 accounts 数据段的大致长度；
- `epoch_stakes`：epoch stake 信息；
- `hash`、`parent_hash`、`parent_slot`：链上下文。

另外还有一个非常关键的结构：

```rust
pub struct AccountsDbFields<T>(
    pub HashMap<Slot, Vec<T>>,
    pub StoredMetaWriteVersion,
    pub Slot,
    pub BankHashInfo,
    pub Vec<Slot>,
    pub Vec<(Slot, Hash)>,
);
```

它表示：

- `HashMap<Slot, Vec<T>>`：每个 slot 对应一组账户数据；
- `StoredMetaWriteVersion`：账户写版本；
- `Slot`：当前 slot；
- `BankHashInfo`：bank hash；
- 后面两个字段：最近根 slot 和其 hash 列表。

也就是说，真正的账户不是直接从这个 “manifest” 文件里抽取，而是从 `AccountsDbFields` 里对应的 `Vec<T>` 继续找出各个 append vec，再去解析 append vec 中的账户记录。

---

## 2. snapshot 中账户的真实存储结构：AppendVec

Solana 的 account 存储使用的是 `AppendVec`，这是一个“追加型连续内存块”。它的特点是：

- 每个账户在文件中按顺序追加；
- 读取时用 mmap 直接映射到内存；
- 账户记录由多个 metadata 段拼接而成；
- 每个账户还可能对齐到 64 字节边界，避免非对齐访问造成崩溃。

代码中的关键定义是：

```rust
pub struct StoredMeta {
    pub write_version: StoredMetaWriteVersion,
    pub data_len: u64,
    pub pubkey: Pubkey,
}
```

```rust
#[repr(C)]
pub struct AccountMeta {
    pub lamports: u64,
    pub rent_epoch: Epoch,
    pub owner: Pubkey,
    pub executable: bool,
}
```

并且 `StoredAccountMeta` 把它们组合起来：

```rust
pub struct StoredAccountMeta<'a> {
    pub meta: &'a StoredMeta,
    pub account_meta: &'a AccountMeta,
    pub data: &'a [u8],
    pub offset: usize,
    pub stored_size: usize,
    pub hash: &'a Hash,
}
```

这表示一个账户在 append vec 中的布局大致是：

```text
[ StoredMeta ][ AccountMeta ][ Hash ][ account_data ]
```

其中：

- `StoredMeta`：写版本 + 数据长度 + pubkey；
- `AccountMeta`：lamports + rent_epoch + owner + executable；
- `Hash`：当前账户的 hash；
- `account_data`：账户原始数据 bytes，长度为 `meta.data_len`；

---

## 3. 偏移量和 64 字节对齐：为什么要这么做

这一段是 Solana snapshot 解析最关键的底层细节。

在 `append_vec.rs` 中，偏移计算非常精确：

```rust
pub const ALIGN_BOUNDARY_OFFSET: usize = mem::size_of::<u64>();
macro_rules! u64_align {
    ($addr: expr) => {
        ($addr + (ALIGN_BOUNDARY_OFFSET - 1)) & !(ALIGN_BOUNDARY_OFFSET - 1)
    };
}
```

也就是说，所有记录在内存中按 8 字节边界对齐。`get_slice` 会在读数据时检查边界：

```rust
fn get_slice(&self, offset: usize, size: usize) -> Option<(&[u8], usize)> {
    let (next, overflow) = offset.overflowing_add(size);
    if overflow || next > self.len() {
        return None;
    }
    let data = &self.map[offset..next];
    let next = u64_align!(next);
    Some((unsafe { ... }, next))
}
```

`get_account` 的关键逻辑是：

```rust
let (meta, next): (&StoredMeta, _) = self.get_type(offset)?;
let (account_meta, next): (&AccountMeta, _) = self.get_type(next)?;
let (hash, next): (&Hash, _) = self.get_type(next)?;
let (data, next) = self.get_slice(next, meta.data_len as usize)?;
let stored_size = next - offset;
```

从这个过程可以看出，真正的解析规则是：

1. 先从当前 `offset` 取出 `StoredMeta`；
2. 再按 8 字节对齐后的偏移读取 `AccountMeta`；
3. 再读取 `Hash`；
4. 再用 `meta.data_len` 读取账户数据；
5. 最终得到一个 `StoredAccountMeta`。

这说明：snapshot 中的每个账户并不是一个“独立对象”，而是一个“连续消息块”，其中字段之间通过前后偏移 + 对齐来恢复。

---

## 4. 账户字段的含义：从 snapshot 到 SQLite `account` 表

在 `src/bin/solana-snapshot-etl/sqlite.rs` 中，本项目会把每个账户写进 SQLite 的 `account` 表：

```sql
CREATE TABLE account  (
    pubkey BLOB(32) NOT NULL PRIMARY KEY,
    data_len INTEGER(8) NOT NULL,
    owner BLOB(32) NOT NULL,
    lamports INTEGER(8) NOT NULL,
    executable INTEGER(1) NOT NULL,
    rent_epoch INTEGER(8) NOT NULL
);
```

插入代码是：

```rust
self.db.prepare_cached(
    "INSERT OR REPLACE INTO account (pubkey, data_len, owner, lamports, executable, rent_epoch)
     VALUES (?, ?, ?, ?, ?, ?);",
)?;
```

对应的来源如下：

- `pubkey`：来自 `account.meta.pubkey`；也就是 `StoredMeta.pubkey`；
- `data_len`：来自 `account.meta.data_len`；
- `owner`：来自 `account.account_meta.owner`；
- `lamports`：来自 `account.account_meta.lamports`；
- `executable`：来自 `account.account_meta.executable`；
- `rent_epoch`：来自 `account.account_meta.rent_epoch`；

这张表是所有账户的“总索引表”。本质上它不是抽象概念，而是把账户基础元数据直接从 `StoredMeta + AccountMeta` 拍平成数据库字段。

---

## 5. 交易需要的 Token 账户：从二进制数据中 unpack 出结构体

Solana 里有两类 token 账户：

- SPL Token（`spl_token::id()`）
- Token-2022（`TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`）

`insert_account` 会判断 owner：

```rust
if account.account_meta.owner == spl_token::id() {
    self.insert_token(account)?;
} else if account.account_meta.owner == *token_2022_program_id() {
    self.insert_token_2022(account)?;
}
```

然后根据账户数据长度判断具体是哪种结构：

- `spl_token::state::Account::LEN`
- `spl_token::state::Mint::LEN`
- `spl_token::state::Multisig::LEN`

并调用：

```rust
spl_token::state::Account::unpack(account.data)
```

这一步是关键：账户数据 bytes 并不是 SQL 字段，而是符合 SPL Token 定义的二进制结构体，代码以 `unpack` 的方式把 bytes 还原成 Rust 结构体。之后再写入 SQLite。

---

## 6. `token_account` 表字段含义

创建表：

```sql
CREATE TABLE token_account (
    pubkey BLOB(32) NOT NULL PRIMARY KEY,
    mint BLOB(32) NOT NULL,
    owner BLOB(32) NOT NULL,
    amount INTEGER(8) NOT NULL,
    delegate BLOB(32),
    state INTEGER(1) NOT NULL,
    is_native INTEGER(8),
    delegated_amount INTEGER(8) NOT NULL,
    close_authority BLOB(32)
);
```

对应的 `spl_token::state::Account` 字段映射：

- `pubkey`：账户 pubkey，来自 `account.meta.pubkey`；
- `mint`：`token_account.mint`，对应 token mint 地址；
- `owner`：`token_account.owner`，对应 token account 的 owner；
- `amount`：`token_account.amount`，当前余额；
- `delegate`：`token_account.delegate`，可选受托地址；
- `state`：`token_account.state`，账户状态，例如 Initialized/Uninitialized 等；
- `is_native`：`token_account.is_native`；
- `delegated_amount`：`token_account.delegated_amount`；
- `close_authority`：`token_account.close_authority`；

从“二进制 bytes”到 SQLite 的这一步，本质上是：

```text
account.data (SPL Token Account binary) -> Account::unpack(...) -> token_account struct -> SQLite row
```

这里字段没有从文件直接硬编码“偏移”，而是依赖 SPL Token 定义中的固定布局；`unpack` 会按其规范从 bytes 中还原出结构。

---

## 7. `token_mint` 表字段含义

创建表：

```sql
CREATE TABLE token_mint (
    pubkey BLOB(32) NOT NULL PRIMARY KEY,
    mint_authority BLOB(32) NULL,
    supply INTEGER(8) NOT NULL,
    decimals INTEGER(2) NOT NULL,
    is_initialized BOOL NOT NULL,
    freeze_authority BLOB(32) NULL
);
```

`mint` 结构来自 `spl_token::state::Mint`：

- `pubkey`：mint 账户的 pubkey；
- `mint_authority`：mint 权限地址；
- `supply`：当前总供应量；
- `decimals`：精度；
- `is_initialized`：初始化状态；
- `freeze_authority`：冻结权限地址。

同样通过：

```rust
spl_token::state::Mint::unpack(account.data)
```

恢复出结构体后写入 SQLite。

---

## 8. `token_multisig` 表字段含义

创建表：

```sql
CREATE TABLE token_multisig (
    pubkey BLOB(32) NOT NULL,
    signer BLOB(32) NOT NULL,
    m INTEGER(2) NOT NULL,
    n INTEGER(2) NOT NULL,
    PRIMARY KEY (pubkey, signer)
);
```

这个表不是一个单行记录，而是多签账户会展开成多个签名者行。

代码是：

```rust
for signer in &token_multisig.signers[..token_multisig.n as usize] {
    token_multisig_insert.insert(params![
        account.meta.pubkey.as_ref(),
        signer.as_ref(),
        token_multisig.m,
        token_multisig.n
    ])?;
}
```

也就是：

- `pubkey`：多签账户地址；
- `signer`：每个 signer 的公钥；
- `m`：门槛值；
- `n`：总签名人数。

这里是把一个多签 binary 结构展开成“签名者维度”的行记录。

---

## 9. `token_metadata` 表字段含义

创建表：

```sql
CREATE TABLE token_metadata (
    pubkey BLOB(32) NOT NULL,
    mint BLOB(32) NOT NULL,
    name TEXT(32) NOT NULL,
    symbol TEXT(10) NOT NULL,
    uri TEXT(200) NOT NULL,
    seller_fee_basis_points INTEGER(4) NOT NULL,
    primary_sale_happened INTEGER(1) NOT NULL,
    is_mutable INTEGER(1) NOT NULL,
    edition_nonce INTEGER(2) NULL,
    collection_verified INTEGER(1) NULL,
    collection_key BLOB(32) NULL
);
```

这一层不是 SPL Token 结构，而是 Metaplex Metadata 程序账户。

代码中先读出这个账户的 “类型标识”：

```rust
let account_key = match mpl_metadata::AccountKey::deserialize(&mut data_peek) {
    Ok(v) => v,
    Err(_) => return Ok(()),
};
```

然后检查：

```rust
match account_key {
    mpl_metadata::AccountKey::MetadataV1 => {
        let meta_v1 = mpl_metadata::Metadata::deserialize(&mut data_peek)?;
        let meta_v1_1 = mpl_metadata::MetadataExt::deserialize(&mut data_peek).ok();
        let meta_v1_2 = meta_v1_1
            .as_ref()
            .and_then(|_| mpl_metadata::MetadataExtV1_2::deserialize(&mut data_peek).ok());
        ...
    }
}
```

这里体现的是另一种解析方式：

- 不是按 `AccountMeta`/`StoredMeta` 结构去找字段；
- 而是按 Metaplex Metadata 的 Borsh 定义，把 bytes 逐段 deserialize 成多个结构体；
- `Metadata` 负责基础字段；
- `MetadataExt` 和 `MetadataExtV1_2` 负责可选扩展字段；
- collection 信息也会额外 unpack 出来。

字段对应关系：

- `pubkey`：metadata account 地址；
- `mint`：NFT / token mint 地址；
- `name`：`meta_v1.data.name`；
- `symbol`：`meta_v1.data.symbol`；
- `uri`：`meta_v1.data.uri`；
- `seller_fee_basis_points`：销售手续费；
- `primary_sale_happened`：是否已完成首次销售；
- `is_mutable`：是否可变；
- `edition_nonce`：edition nonce（可选）；
- `collection_verified`：collection 是否校验有效；
- `collection_key`：collection 的 key（可选）。

这一步是通过 Borsh 反序列化直接恢复结构体，不再依赖 `AppendVec` 中的通用账户元数据。

---

## 10. 完整链路总结：从二进制到 SQLite

一条账户数据从 snapshot 进入数据库，整体过程可概括为：

```text
snapshot file
  -> snapshot manifest
  -> DeserializableVersionedBank / AccountsDbFields
  -> AppendVec
  -> StoredMeta + AccountMeta + Hash + account data
  -> StoredAccountMeta
  -> account.owner 决定分支
     -> SPL Token or Token-2022: unpack() -> token_account / token_mint / token_multisig
     -> MPL Metadata: Borsh deserialize() -> token_metadata
  -> SQLite 表
```

其中最关键的底层事实是：

1. account 的真实布局在 `AppendVec` 中，由 `StoredMeta`、`AccountMeta`、`Hash` 和 `data` 连续存储；
2. 访问时通过 `offset` + `size_of` + 8 字节对齐来恢复字段；
3. 解析后，字段映射到 SQLite 表；
4. 特定程序账户（token、metadata）再进一步通过 `unpack` 或 `deserialize` 解析其业务结构。

---

## 11. 一个具体例子：Token Account 的解析路径

例如，某个账户的 `owner` 是 SPL Token Program：

```rust
account.account_meta.owner == spl_token::id()
```

那么它会被送进：

```rust
spl_token::state::Account::unpack(account.data)
```

如果 unpack 成功，就得到类似这样的结构：

```rust
struct Account {
    mint: Pubkey,
    owner: Pubkey,
    amount: u64,
    delegate: COption<Pubkey>,
    state: AccountState,
    is_native: COption<u64>,
    delegated_amount: u64,
    close_authority: COption<Pubkey>,
}
```

再按字段映射写入：

```sql
INSERT INTO token_account (
    pubkey, mint, owner, amount, delegate, state,
    is_native, delegated_amount, close_authority
) VALUES (...)
```

这个过程不是“读某个固定 byte 偏移”，而是用 SPL Token 自己定义的结构体布局，严格从 bytes 中恢复出各字段，然后写表。

---

## 12. 结论

这个项目的 snapshot 解析并不是黑盒式“直接遍历文件”，而是建立了一条非常清晰的链路：

- 先从 snapshot manifest 确认 bank / slot / epoch；
- 再从 `AccountsDbFields` 里面定位 append vec；
- 再由 `StoredMeta`、`AccountMeta`、Hash 和 account data 的固定布局恢复每个账户；
- 再按 owner 程序分流，调用 `unpack` 或 Borsh `deserialize` 恢复业务字段；
- 最后落库到 SQLite 中。

这个设计的优点在于：

- 能直接读取 Solana 原生内存布局；
- 适配各种 account 程序；
- 跟 Solana 运行时的索引与存储格式保持一致；
- 可以通过偏移 + 类型 + 对齐规则高性能解析大规模 snapshot。

如果需要进一步深入，可以继续阅读：

- `src/append_vec.rs`：AppendVec / StoredMeta / AccountMeta 的底层布局；
- `src/solana.rs`：snapshot manifest 的 bincode 结构；
- `src/bin/solana-snapshot-etl/sqlite.rs`：字段落库和程序账户解析逻辑。

-- 本文件假定 ClickHouse 已配置两个 storage policy：
--   hot_active_policy  -> 无后缀 active 组所在磁盘
--   hot_backup_policy  -> _bak staging 组所在磁盘
-- 请按实际部署替换名称。active/_bak 不应使用同一块数据盘。

CREATE DATABASE IF NOT EXISTS solana;

-- ============================================================
-- 全局配置表（不属于 active/_bak 任一数据组）
-- ============================================================

CREATE TABLE solana.hot_token
(
    mint    String,
    enable  UInt8 DEFAULT 1,
    version UInt64 DEFAULT 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY mint
COMMENT '全局 hot mint 配置；修改一条 mint 时必须递增 version';

CREATE VIEW solana.hot_token_enabled AS
SELECT mint, version
FROM solana.hot_token FINAL
WHERE enable = 1;

-- ============================================================
-- active 组：raw_account、hot-only mint/metadata、L2、L3
-- ============================================================

CREATE TABLE solana.raw_account
(
    pubkey       String,
    owner        LowCardinality(String),
    lamports     UInt64,
    data_len     UInt64,
    executable   Bool,
    updated_slot UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (owner, pubkey)
SETTINGS storage_policy = 'hot_active_policy'
COMMENT '账户元信息和 watcher watermark 来源；不保存 account data';

-- 只保存本组冻结 hot mint 的 Mint 账户。它不再是全链 token mint 历史表。
CREATE TABLE solana.raw_token_mint
(
    mint             String,
    mint_authority   Nullable(String),
    supply           UInt64,
    decimals         UInt8,
    is_initialized   Bool,
    freeze_authority Nullable(String),
    updated_slot     UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
SETTINGS storage_policy = 'hot_active_policy';

-- 只保存本组冻结 hot mint 的 Metaplex metadata。
CREATE TABLE solana.raw_token_metadata
(
    mint                    String,
    name                    String,
    symbol                  String,
    uri                     String,
    update_authority        LowCardinality(String),
    is_mutable              Bool,
    token_standard          Nullable(UInt8),
    seller_fee_basis_points UInt16,
    creators                Array(String),
    updated_slot            UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
SETTINGS storage_policy = 'hot_active_policy';

-- L2 是唯一的 hot Token Account 状态来源。解析器直接写入本表，不再有
-- raw_token_account 中转层。CloseAccount tombstone 从本表按 pubkey 恢复
-- mint/owner 后写回，因此删除行也保留该 pair。
CREATE TABLE solana.hot_token_account_state
(
    pubkey           String,
    mint             String,
    owner            String,
    amount           UInt64,
    delegate         Nullable(String),
    delegated_amount UInt64,
    state            Enum8('uninitialized' = 0, 'initialized' = 1, 'frozen' = 2),
    close_authority  Nullable(String),
    is_deleted       UInt8,
    updated_slot     UInt64
)
ENGINE = ReplacingMergeTree(updated_slot, is_deleted)
ORDER BY pubkey
SETTINGS storage_policy = 'hot_active_policy', deduplicate_merge_projection_mode = 'rebuild',index_granularity = 512
COMMENT '冻结 hot mint 的 Token Account 最新态；is_deleted=1 为 CloseAccount 删除版本';

-- L3 增量按 (mint, owner) 聚合。该 projection 按 pair 排序，避免每次
-- 从按 pubkey 排序的 L2 扫描全部 Token Account。pubkey/version 字段保留，
-- 让增量可在已裁剪的 pair 范围内以 argMax 选取每个账户的最新版本；不要
-- 在这条热点路径上使用 FINAL，否则 ClickHouse 会回退为主表全扫描。
ALTER TABLE solana.hot_token_account_state
    ADD PROJECTION IF NOT EXISTS proj_by_pair
    (
        SELECT pubkey, mint, owner, amount, state, is_deleted, updated_slot
        ORDER BY (mint, owner, pubkey)
    );

CREATE TABLE solana.hot_wallet_token_balance
(
    mint         String,
    owner        String,
    amount_raw   UInt64,
    updated_slot UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (mint, owner)
SETTINGS storage_policy = 'hot_active_policy', deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'L3 钱包 × hot mint 余额；增量仅重算受影响 pair';

ALTER TABLE solana.hot_wallet_token_balance
    ADD PROJECTION IF NOT EXISTS proj_by_mint_amount
    (
        SELECT mint, owner, amount_raw, updated_slot
        ORDER BY (mint, amount_raw, owner)
    );

ALTER TABLE solana.hot_wallet_token_balance
    ADD PROJECTION IF NOT EXISTS proj_by_owner
    (
        SELECT mint, owner, amount_raw, updated_slot
        ORDER BY (owner, mint)
    );

CREATE TABLE solana.hot_token_info
(
    mint                  String,
    decimals              UInt8,
    supply_raw            UInt64,
    name                  String,
    symbol                String,
    uri                   String,
    token_standard        Nullable(UInt8),
    mint_updated_slot     UInt64,
    metadata_updated_slot UInt64,
    updated_slot          UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
SETTINGS storage_policy = 'hot_active_policy'
COMMENT '全量阶段由 frozen filter 与本组 hot-only mint/metadata 构建；本组增量期间保持不变';

-- ============================================================
-- staging / backup 组。字段、引擎、排序键必须与 active 对应表一致；
-- 唯一不同是 storage_policy。
-- ============================================================

CREATE TABLE solana.raw_account_bak
(
    pubkey       String,
    owner        LowCardinality(String),
    lamports     UInt64,
    data_len     UInt64,
    executable   Bool,
    updated_slot UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (owner, pubkey)
SETTINGS storage_policy = 'hot_backup_policy';

CREATE TABLE solana.raw_token_mint_bak
(
    mint             String,
    mint_authority   Nullable(String),
    supply           UInt64,
    decimals         UInt8,
    is_initialized   Bool,
    freeze_authority Nullable(String),
    updated_slot     UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
SETTINGS storage_policy = 'hot_backup_policy';

CREATE TABLE solana.raw_token_metadata_bak
(
    mint                    String,
    name                    String,
    symbol                  String,
    uri                     String,
    update_authority        LowCardinality(String),
    is_mutable              Bool,
    token_standard          Nullable(UInt8),
    seller_fee_basis_points UInt16,
    creators                Array(String),
    updated_slot            UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
SETTINGS storage_policy = 'hot_backup_policy';

CREATE TABLE solana.hot_token_account_state_bak
(
    pubkey           String,
    mint             String,
    owner            String,
    amount           UInt64,
    delegate         Nullable(String),
    delegated_amount UInt64,
    state            Enum8('uninitialized' = 0, 'initialized' = 1, 'frozen' = 2),
    close_authority  Nullable(String),
    is_deleted       UInt8,
    updated_slot     UInt64
)
ENGINE = ReplacingMergeTree(updated_slot, is_deleted)
ORDER BY pubkey
SETTINGS storage_policy = 'hot_backup_policy', deduplicate_merge_projection_mode = 'rebuild', index_granularity = 512;

ALTER TABLE solana.hot_token_account_state_bak
    ADD PROJECTION IF NOT EXISTS proj_by_pair
    (
        SELECT pubkey, mint, owner, amount, state, is_deleted, updated_slot
        ORDER BY (mint, owner, pubkey)
    );

CREATE TABLE solana.hot_wallet_token_balance_bak
(
    mint         String,
    owner        String,
    amount_raw   UInt64,
    updated_slot UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (mint, owner)
SETTINGS storage_policy = 'hot_backup_policy', deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE solana.hot_wallet_token_balance_bak
    ADD PROJECTION IF NOT EXISTS proj_by_mint_amount
    (
        SELECT mint, owner, amount_raw, updated_slot
        ORDER BY (mint, amount_raw, owner)
    );

ALTER TABLE solana.hot_wallet_token_balance_bak
    ADD PROJECTION IF NOT EXISTS proj_by_owner
    (
        SELECT mint, owner, amount_raw, updated_slot
        ORDER BY (owner, mint)
    );

CREATE TABLE solana.hot_token_info_bak
(
    mint                  String,
    decimals              UInt8,
    supply_raw            UInt64,
    name                  String,
    symbol                String,
    uri                   String,
    token_standard        Nullable(UInt8),
    mint_updated_slot     UInt64,
    metadata_updated_slot UInt64,
    updated_slot          UInt64
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
SETTINGS storage_policy = 'hot_backup_policy';

-- ============================================================
-- 从旧 raw_token_account 架构升级
-- ============================================================
-- 先停止所有旧/新 watcher。以下 ALTER 让现有 L2 接受新直写行；随后必须
-- 用 --bootstrap 导入一次 full snapshot，建立 filter、清理非-hot mint/meta
-- 并以新规则重建 active L2/L3。不要以普通续传模式跳过这一步。

ALTER TABLE solana.hot_token_account_state
    ADD COLUMN IF NOT EXISTS delegate Nullable(String) AFTER amount;
ALTER TABLE solana.hot_token_account_state
    ADD COLUMN IF NOT EXISTS delegated_amount UInt64 DEFAULT 0 AFTER delegate;
ALTER TABLE solana.hot_token_account_state
    ADD COLUMN IF NOT EXISTS close_authority Nullable(String) AFTER state;

ALTER TABLE solana.hot_token_account_state_bak
    ADD COLUMN IF NOT EXISTS delegate Nullable(String) AFTER amount;
ALTER TABLE solana.hot_token_account_state_bak
    ADD COLUMN IF NOT EXISTS delegated_amount UInt64 DEFAULT 0 AFTER delegate;
ALTER TABLE solana.hot_token_account_state_bak
    ADD COLUMN IF NOT EXISTS close_authority Nullable(String) AFTER state;

-- 新的 bootstrap 成功并确认不再运行旧程序时，删除原 L1 中转表以释放其磁盘空间：
-- DROP TABLE solana.raw_token_account SETTINGS max_table_size_to_drop = 0;
-- DROP TABLE solana.raw_token_account_bak SETTINGS max_table_size_to_drop = 0;
-- 新版本已稳定运行、且不再需要旧二进制回退时，也可删除废弃的控制/filter 表：
-- DROP TABLE solana.hot_token_filter SETTINGS max_table_size_to_drop = 0;
-- DROP TABLE solana.hot_token_filter_bak SETTINGS max_table_size_to_drop = 0;
-- DROP TABLE solana.hot_index_control SETTINGS max_table_size_to_drop = 0;

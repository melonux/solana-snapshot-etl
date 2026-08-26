-- ========================================
-- 0. raw_account: 原始账户快照表（未解析的原始元信息）
-- ========================================
CREATE TABLE solana.raw_account
(
    pubkey       String              COMMENT '账户自身的地址（base58），唯一标识一个账户',
    owner        LowCardinality(String) COMMENT '拥有该账户的 Program 地址，决定 data 字段应如何解析，如 Token Program、System Program 等；取值集合有限，适合字典编码',
    lamports     UInt64              COMMENT '账户余额，单位为 lamports（1 SOL = 1e9 lamports）',
    data_len     UInt64              COMMENT '账户 data 字段的字节长度；本表不存储 data 内容本身，仅记录长度，用于快速判断账户类型/是否为空账户',
    executable   Bool                COMMENT '是否为可执行账户（即链上程序本身），普通钱包/数据账户此值为 false',
    updated_slot UInt64              COMMENT '本条快照数据采集时对应的 slot 高度，用于版本去重'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (owner, pubkey)
COMMENT 'L0: 原始账户快照表，一行对应链上一个账户的元信息（不含 data 内容），是 raw_token_account / raw_token_mint 等 L1 解析表的数据来源';


-- ========================================
-- 1. raw_token_account: SPL Token 账户（余额表）
-- ========================================
CREATE TABLE solana.raw_token_account
(
    pubkey            String              COMMENT 'token account 自身的地址，唯一标识一条持仓记录',
    mint              String              COMMENT '这个持仓对应的 token 地址（关联 raw_token_mint.mint）；distinct 值随 token 数量增长可能达千万级，不适合字典编码',
    owner             String              COMMENT '这个 token account 归属的钱包地址（谁能签名转出余额）；本质是任意用户钱包，不适合字典编码',
    amount            UInt64              COMMENT '余额，最小单位整数，需配合 raw_token_mint.decimals 换算成人类可读数量',
    delegate           Nullable(String)    COMMENT '被授权可代为转出的地址，没有授权则为空，类似 ERC20 approve 里的 spender；虽然常见于协议托管场景，但不保证低基数，暂不加字典编码',
    delegated_amount  UInt64 DEFAULT 0    COMMENT '被授权可转出的最大数量，没有授权则为 0',
    state             Enum8('uninitialized' = 0, 'initialized' = 1, 'frozen' = 2)
                                          COMMENT '账户状态：uninitialized 未初始化（无效数据），initialized 正常可用，frozen 已被冻结；Enum8 本身已是紧凑编码，无需再套 LowCardinality',
    close_authority   Nullable(String)    COMMENT '有权限关闭此账户并收回 rent 押金的地址，没有设置则为空；常与 owner 相同，不适合字典编码',
    is_deleted        UInt8 DEFAULT 0   COMMENT '是否已通过 CloseAccount 关闭；UInt8 中 0 表示存在、1 表示删除；删除版本的 mint/owner 可为空，当前持仓查询必须过滤 0',
    updated_slot      UInt64            COMMENT '账户在 AccountsDb 中发生此版本写入的链上 slot（AppendVec 所属 slot）'
)
ENGINE = ReplacingMergeTree(updated_slot, is_deleted)
ORDER BY pubkey
COMMENT 'L1: SPL Token 账户余额快照表，一行对应一个 token account 的最新状态；pubkey 是稳定版本键，支持用 CloseAccount tombstone 覆盖旧持仓';

ALTER TABLE solana.raw_token_account
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- 按 mint 查询 holder / Top-N 的 Projection
ALTER TABLE solana.raw_token_account
    ADD PROJECTION proj_by_mint_owner
    (
        SELECT *
        ORDER BY (mint, owner, pubkey)
    );

-- 反查用的 Projection：按 owner 优先排序，加速"某地址持有哪些 token"的查询
ALTER TABLE solana.raw_token_account
    ADD PROJECTION proj_by_owner
    (
        SELECT *
        ORDER BY (owner, mint, pubkey)
    );

ALTER TABLE solana.raw_token_account MATERIALIZE PROJECTION proj_by_owner;
ALTER TABLE solana.raw_token_account MATERIALIZE PROJECTION proj_by_mint_owner;

-- 当前有效 token account 的查询模板。ReplacingMergeTree 的物理 merge 是异步的，
-- 因此对强一致当前态查询必须使用 FINAL，并排除 CloseAccount 写入的 tombstone。
SELECT *
FROM solana.raw_token_account FINAL
WHERE is_deleted = 0;

-- updated_slot 是唯一的业务版本字段。官方 canonical snapshot 不会保留同一
-- pubkey 在同一 slot 的多个有效版本，因此不需要用 AppendVec 的物理位置推断顺序。
-- is_deleted 作为 ReplacingMergeTree 的第二个参数，UInt8=1 表示删除版本；引擎会在
-- 版本比较和后台合并时按删除标记处理，查询时仍建议显式使用 FINAL 并过滤 is_deleted=0。

-- 迁移说明：旧版表使用 ORDER BY (mint, owner, pubkey)，不能原地改为以
-- pubkey 去重，也无法从已经丢失的历史 CloseAccount 反推出 tombstone。
-- 已部署旧版时，先保留旧表，再按本文件的 DDL 创建新的 raw_token_account：
--
-- RENAME TABLE solana.raw_token_account TO solana.raw_token_account_v1_backup;
--
-- 然后从一个新的全量 snapshot 重建，并连续消费该全量 snapshot 之后的
-- incremental snapshots。验证无误后再删除 _v1_backup；不要把新的全量
-- snapshot 直接追加到旧表。
-- 当前版本移除了 append_vec_id/account_offset/final_version。旧表的字段布局和引擎定义
-- 不兼容，应新建表并从全量 snapshot 重新导入。


-- ========================================
-- 2. raw_token_mint: SPL Token 的 Mint 账户（供应量/权限表）
-- ========================================
CREATE TABLE solana.raw_token_mint
(
    mint              String              COMMENT 'token 地址，唯一标识一个 token',
    mint_authority    Nullable(String)    COMMENT '有权限增发新 token 的地址，若已被永久放弃增发权限则为空；常为创建者个人钱包，不适合字典编码',
    supply            UInt64              COMMENT '当前总供应量，最小单位整数，需配合 decimals 换算',
    decimals          UInt8               COMMENT '小数位数，决定 amount/supply 如何换算成人类可读数量',
    is_initialized    Bool                COMMENT '该 mint 账户是否已正常初始化',
    freeze_authority  Nullable(String)    COMMENT '有权限冻结某个持币账户的地址，若已放弃该权限则为空；常与 mint_authority 相同，不适合字典编码',
    updated_slot      UInt64              COMMENT '本条快照数据采集时对应的 slot 高度，用于版本去重'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
COMMENT 'L1: SPL Token Mint 账户快照表，记录每个 token 的供应量与增发/冻结权限';


-- ========================================
-- 3. raw_token_metadata: Metaplex Token Metadata 账户（展示信息表）
-- ========================================
CREATE TABLE solana.raw_token_metadata
(
    mint                     String              COMMENT 'token 地址，关联 raw_token_mint.mint',
    name                     String              COMMENT 'token 名称，如 "USD Coin"',
    symbol                   String              COMMENT 'token 代号，如 "USDC"',
    uri                      String              COMMENT '指向链下 JSON 文件的链接，里面包含 logo 图片地址、描述等详细信息',
    update_authority         LowCardinality(String) COMMENT '有权限修改这些展示信息的地址；大量 token 由 launchpad（如 pump.fun）批量创建并共用同一个程序控制的地址，实际 distinct 值远小于总行数，适合字典编码',
    is_mutable               Bool                COMMENT '这些展示信息以后是否还能被修改',
    token_standard           Nullable(UInt8)     COMMENT 'Metaplex TokenStandard 的 Borsh 枚举值：0=NonFungible，1=FungibleAsset，2=Fungible，3=NonFungibleEdition，4=ProgrammableNonFungible，5=ProgrammableNonFungibleEdition；旧 metadata 未设置时为 NULL',
    seller_fee_basis_points  UInt16 DEFAULT 0    COMMENT '版税比例（万分之一为单位），主要用于 NFT，普通 token 一般为 0',
    creators                 Array(String) DEFAULT []
                                                  COMMENT '创作者地址列表，主要用于 NFT 分成场景，普通 token 一般为空；地址随创作者个人变化，不适合字典编码',
    updated_slot             UInt64              COMMENT '本条快照数据采集时对应的 slot 高度，用于版本去重'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
COMMENT 'L1: Metaplex Token Metadata 账户快照表，记录每个 token 的名称/图标等展示信息，并非所有 token 都存在对应记录';

-- 已有表只增加 schema 时可执行：
-- ALTER TABLE solana.raw_token_metadata
--     ADD COLUMN IF NOT EXISTS token_standard Nullable(UInt8) AFTER is_mutable;
-- 这只会让历史行显示 NULL。若需要补齐历史 token_standard，须从新的全量
-- snapshot 重建/重灌 raw_token_metadata，因为 L1 表不保留 metadata 原始 bytes。

-- ========================================
-- 0. raw_account: 当前服役组原始账户快照表（未解析的原始元信息）
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
-- 1. raw_token_account: 当前服役组 SPL Token 账户（余额表）
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

-- ========================================
-- 2. raw_token_mint: 当前服役组 SPL Token Mint 账户（供应量/权限表）
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
-- 3. raw_token_metadata: 当前服役组 Metaplex Token Metadata 账户（展示信息表）
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


-- ========================================
-- 4. hot_token: 外部查询的热点 token 集合
-- ========================================
-- import_hot_token 只写入 mint 列；rank、request_count 等 CSV 统计列不会入库。
CREATE TABLE solana.hot_token
(
    mint    String COMMENT 'Token mint 地址',
    enable  UInt8 DEFAULT 1 COMMENT '1=启用，0=禁用',
    version UInt64 DEFAULT 1 COMMENT '同一 mint 的递增版本号'
)
ENGINE = ReplacingMergeTree(version)
ORDER BY mint
COMMENT '热点 Token 集合及其启用状态';
-- 修改时，必须增加 version 编号。

-- 创建一个只返回启用 Token 的视图，后续二层 Materialized View 都引用它
CREATE VIEW solana.hot_token_enabled AS
SELECT mint, version
FROM solana.hot_token FINAL
WHERE enable = 1;



-- ========================================
-- 5. hot_index_control: 表组切换控制
-- ========================================
CREATE TABLE solana.hot_index_control
(
    control_key       LowCardinality(String)
                       COMMENT '控制项名称，固定为 default',

    active_group      UInt8
                       COMMENT '最近一次切换后的逻辑组编号：1 或 2；仅供编排与审计，查询不依赖此字段路由',

    generation        UInt64
                       COMMENT '全局递增代数，每次切换必须递增',

    ready_slot        UInt64
                       COMMENT '当前 active group 已完成的快照 slot',

    hot_token_version UInt64
                       COMMENT '本组构建时使用的 hot_token 版本',

    updated_at        DateTime64(3, 'UTC')
                       DEFAULT now64(3)
                       COMMENT '控制记录更新时间'
)
ENGINE = ReplacingMergeTree(generation)
ORDER BY control_key
COMMENT '热门 Token 二层索引的内部编排与审计表；不参与查询路由，查询始终访问无后缀 active 表';

-- generation 必须全局严格递增；
-- ready_slot 只有在目标组完成全量、增量和聚合刷新后才能写入；
-- 编排程序查询控制表时必须使用 FINAL；
-- EXCHANGE TABLES 才是 active/_bak 的实际切换动作，控制表记录在切换成功后追加；
-- 查询服务固定访问无后缀的 active 表，不读取 active_group 决定表名；
-- 控制记录写入失败不影响已经完成的表交换，后续可通过校验后补写审计记录；


-- ============================================================
-- hot_token_account_state
-- L2 状态明细：只保存启用热门 Token 的 Token Account 最新状态。
--
-- 用途：
-- 1. 作为 raw_token_account 的热门币筛选子集；
-- 2. 保留 token-account 粒度的版本与删除状态；
-- 3. 为 hot_wallet_token_balance 的钱包维度聚合提供正确输入。
-- ============================================================
CREATE TABLE solana.hot_token_account_state
(
    pubkey       String
                 COMMENT 'SPL Token Account 地址；一个账户对应一个 mint 和一个钱包 owner，是本表的业务主键',

    mint         String
                 COMMENT 'Token mint 地址；仅存当前 hot_token 中 enable=1 的普通 Token',

    owner        String
                 COMMENT 'Token Account 的实际持有人钱包地址；同一钱包可通过多个 Token Account 持有同一种 Token',

    amount       UInt64
                 COMMENT '该 Token Account 的余额，最小单位整数；展示时需结合 hot_token_info.decimals 换算',

    state        Enum8(
                     'uninitialized' = 0,
                     'initialized'   = 1,
                     'frozen'        = 2
                 )
                 COMMENT 'SPL Token Account 状态；initialized 和 frozen 都是有效持仓，uninitialized 不应进入钱包余额聚合',

    is_deleted   UInt8
                 COMMENT '删除版本标志：0=当前有效账户版本，1=CloseAccount tombstone；tombstone 的 mint/owner/amount 可为中性值',

    updated_slot UInt64
                 COMMENT '账户版本所属的 Solana slot；ReplacingMergeTree 按此字段选择同一 pubkey 的最新版本'
)
ENGINE = ReplacingMergeTree(updated_slot, is_deleted)
ORDER BY pubkey
COMMENT 'L2：热门 Token 的逐 Token-Account 当前态表；支持用 tombstone 覆盖已关闭账户';

-- ============================================================
-- hot_wallet_token_balance
-- L3 服务聚合：每个 钱包 × 热门 Token 仅一行的当前持仓。
--
-- 用途：
-- 1. 快速查询某钱包持有的所有热门普通 Token；
-- 2. 快速查询某一个热门 Token 的 Top-N holder；
-- 3. 避免服务层每次请求扫描并聚合多个 Token Account。
-- ============================================================
CREATE TABLE solana.hot_wallet_token_balance
(
    mint         String
                 COMMENT 'Token mint 地址；与 owner 共同唯一标识一条钱包级持仓',

    owner        String
                 COMMENT '钱包地址；一个钱包针对同一 mint 的多个 Token Account 已在 amount_raw 中汇总',

    amount_raw   UInt64
                 COMMENT '钱包对该 mint 的总持仓，最小单位整数；等于所有有效 Token Account 的 amount 之和',

    updated_slot UInt64
                 COMMENT '该 (mint, owner) 聚合余额的版本 slot；ReplacingMergeTree 按此字段保留同一键的最新版本'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (mint, owner)
COMMENT 'L3：热门 Token 的钱包级当前余额表；同一 (mint, owner) 通过 updated_slot 追加新版本，查询时按需使用 FINAL/argMax 得到最新余额';

-- ReplacingMergeTree 合并时重建 Projection；默认 throw 会禁止 ADD PROJECTION。
-- Projection 创建完成后仍须保留该设置，否则后续去重 Merge 可能失败或丢弃 Projection。
ALTER TABLE solana.hot_wallet_token_balance
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- 按 mint 过滤后可供 Top-N 查询使用；Projection 定义使用升序，查询时再 DESC 排序。
ALTER TABLE solana.hot_wallet_token_balance
    ADD PROJECTION IF NOT EXISTS proj_by_mint_amount
    (
        SELECT mint, owner, amount_raw, updated_slot
        ORDER BY (mint, amount_raw, owner)
    );

-- 反查某钱包持有哪些热门 Token。
ALTER TABLE solana.hot_wallet_token_balance
    ADD PROJECTION IF NOT EXISTS proj_by_owner
    (
        SELECT mint, owner, amount_raw, updated_slot
        ORDER BY (owner, mint)
    );

-- 空表无需 MATERIALIZE；已有数据时分别执行：
-- ALTER TABLE solana.hot_wallet_token_balance MATERIALIZE PROJECTION proj_by_mint_amount;
-- ALTER TABLE solana.hot_wallet_token_balance MATERIALIZE PROJECTION proj_by_owner;


-- ============================================================
-- hot_token_info
-- L2 展示信息：启用热门 Token 的 mint 和 metadata 当前信息。
--
-- 用途：
-- 1. 为钱包资产列表、Token holder 榜单提供名称、symbol、小数位等展示字段；
-- 2. 避免下游服务再 JOIN 数亿级 raw_token_mint/raw_token_metadata；
-- 3. 与同组 hot_wallet_token_balance 联合使用，保证切换时数据代际一致。
-- ============================================================

CREATE TABLE solana.hot_token_info
(
    mint             String COMMENT 'Token mint 地址；本表业务主键，仅存启用的热门 Token',
    decimals         UInt8 COMMENT '小数位数；amount_raw / 10^decimals 为展示数量',
    supply_raw       UInt64 COMMENT '当前总供应量，最小单位整数',
    name             String COMMENT 'Token 名称；无 Metaplex metadata 时为空字符串',
    symbol           String COMMENT 'Token 符号；无 Metaplex metadata 时为空字符串',
    uri              String COMMENT 'Metaplex metadata URI；无 metadata 时为空字符串',
    token_standard   Nullable(UInt8) COMMENT 'Metaplex TokenStandard；无 metadata 或未设置时为 NULL',
    mint_updated_slot UInt64 COMMENT 'raw_token_mint 当前版本的更新 slot',
    metadata_updated_slot UInt64 COMMENT 'raw_token_metadata 当前版本的更新 slot；无 metadata 时为 0',
    updated_slot     UInt64 COMMENT '本行派生版本；等于 mint_updated_slot 与 metadata_updated_slot 的较大值'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
COMMENT 'L2：热门 Token 的 mint 和 metadata 当前展示信息表，为下游展示避免扫描 raw 表';

-- ============================================================
-- 双路部署说明
-- ============================================================
-- 双路切换要求 raw 层和二层派生表都各有两组：不带后缀的 active 组，
-- 以及带 `_bak` 后缀的 staging/备用组。本文件前面的 DDL 创建 active 组；
-- 使用下面的语句创建空的备用组。CREATE TABLE ... AS 会复制字段、引擎和
-- 排序键定义，但不会复制数据。

CREATE TABLE solana.raw_account_bak AS solana.raw_account;
CREATE TABLE solana.raw_token_mint_bak AS solana.raw_token_mint;
CREATE TABLE solana.raw_token_account_bak AS solana.raw_token_account;
CREATE TABLE solana.raw_token_metadata_bak AS solana.raw_token_metadata;
CREATE TABLE solana.hot_token_info_bak AS solana.hot_token_info;
CREATE TABLE solana.hot_wallet_token_balance_bak AS solana.hot_wallet_token_balance;
CREATE TABLE solana.hot_token_account_state_bak AS solana.hot_token_account_state;

-- 为确保两组定义一致，备用表也显式补齐设置和 Projection（IF NOT EXISTS 可安全重复执行）：
ALTER TABLE solana.hot_wallet_token_balance_bak
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE solana.hot_wallet_token_balance_bak
    ADD PROJECTION IF NOT EXISTS proj_by_mint_amount
    (SELECT mint, owner, amount_raw, updated_slot ORDER BY (mint, amount_raw, owner));
ALTER TABLE solana.hot_wallet_token_balance_bak
    ADD PROJECTION IF NOT EXISTS proj_by_owner
    (SELECT mint, owner, amount_raw, updated_slot ORDER BY (owner, mint));

-- 查询服务始终访问不带后缀的 active 表。后续切换使用 EXCHANGE TABLES，
-- 每对交换原子，但多对表按顺序交换；详细的写入屏障、交换顺序和短暂跨表
-- 不一致窗口见 hot-index-double-buffer.md。

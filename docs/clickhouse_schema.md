本文件记录了 clickhouse 上已创建的表格的 schema

```
-- ========================================
-- 0. account: 原始账户快照表（未解析的原始元信息）
-- ========================================
CREATE TABLE solana.account
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
    updated_slot      UInt64              COMMENT '本条快照数据采集时对应的 slot 高度，用于版本去重'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY (mint, owner, pubkey)
COMMENT 'L1: SPL Token 账户余额快照表，一行对应链上一个 token account';

ALTER TABLE solana.raw_token_account
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- 反查用的 Projection：按 owner 优先排序，加速"某地址持有哪些 token"的查询
ALTER TABLE solana.raw_token_account
    ADD PROJECTION proj_by_owner
    (
        SELECT *
        ORDER BY (owner, mint, pubkey)
    );

ALTER TABLE solana.raw_token_account MATERIALIZE PROJECTION proj_by_owner;


-- ========================================
-- 2. raw_token_mint: SPL Token 的 Mint 账户（供应量/权限表）
-- ========================================
CREATE TABLE solana.raw_token_mint
(
    mint              String              COMMENT 'token 地址，唯一标识一个 token',
    supply            UInt64              COMMENT '当前总供应量，最小单位整数，需配合 decimals 换算',
    decimals          UInt8               COMMENT '小数位数，决定 amount/supply 如何换算成人类可读数量',
    is_initialized    Bool                COMMENT '该 mint 账户是否已正常初始化',
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
    is_mutable               Bool                COMMENT '这些展示信息以后是否还能被修改',
    updated_slot             UInt64              COMMENT '本条快照数据采集时对应的 slot 高度，用于版本去重'
)
ENGINE = ReplacingMergeTree(updated_slot)
ORDER BY mint
COMMENT 'L1: Metaplex Token Metadata 账户快照表，记录每个 token 的名称/图标等展示信息，并非所有 token 都存在对应记录';
```
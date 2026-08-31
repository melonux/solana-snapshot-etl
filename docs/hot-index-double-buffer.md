# Hot 索引双缓冲

## 目标

新 full snapshot 的冷加载、Merge 和聚合不能阻塞正在服务的增量路径。ETL 因此维护一组无后缀的 active 表和一组 `_bak` staging 表；对外查询永远使用无后缀表名。

```text
active                                      staging
raw_account                 <->            raw_account_bak
raw_token_mint              <->            raw_token_mint_bak
raw_token_metadata          <->            raw_token_metadata_bak
hot_token_filter            <->            hot_token_filter_bak
hot_token_account_state     <->            hot_token_account_state_bak
hot_token_info              <->            hot_token_info_bak
hot_wallet_token_balance    <->            hot_wallet_token_balance_bak
```

`hot_token`、`hot_token_enabled` 和 `hot_index_control` 是全局配置/审计对象，不参与交换。

## 表职责

- `raw_account`：所有账户的轻量元信息；它提供 watcher 的 `max(updated_slot)` watermark。
- `raw_token_mint`、`raw_token_metadata`：只保存本表组冻结 hot mint 的解析结果，用于 full 阶段构建 `hot_token_info`。
- `hot_token_filter`：本表组的冻结 mint 集合。
- `hot_token_account_state`：L2，唯一的 hot Token Account 状态表。解析器直接写入；没有 `raw_token_account` 中转表。
- `hot_wallet_token_balance`：L3，按 `(mint, owner)` 的聚合余额。
- `hot_token_info`：L2 展示缓存，仅在 full 阶段构建，直到下一次组切换前保持不变。

## 冻结 hot mint 集合

`hot_token_enabled` 是可变的全局配置，但一个 active generation 的历史不能因配置更新而重新解释：

1. 构建一个 full generation 时，程序将 `hot_token_enabled` snapshot 到目标组的 `hot_token_filter`；同时加载为进程内 `HashSet`。
2. 该组的 SPL Token、Token-2022 Mint 和 Metaplex metadata 解析均只保留集合内 mint。
3. 该组的所有 incremental 都复用同一集合。active 增量不会查询 `hot_token_enabled`，也不会因全局配置变化重建 L2/L3。
4. 新 full 的 staging 组得到自己的新集合；交换表名后，它才成为新的 active 集合。

因此 active/staging 在同一 slot 使用不同 hot 集合是正常且预期的。切换前只验证 staging 自身：filter 非空，且 `hot_token_info_bak` 的行数与 filter 相同；不比较两组的 L2/L3 行数或余额总量。

## 全量与增量路径

### Full

1. 停止目标组七张表的后台 Merge，清空目标组。
2. snapshot 全局 hot mint 到该组 filter。
3. 解析 archive：所有账户元信息进入 `raw_account`；只有 frozen hot mint 的 mint、metadata 和 Token Account 进入对应表。
4. Token Account 直接写入 `hot_token_account_state`，带 `delegate`、`delegated_amount` 和 `close_authority`。
5. 从 L2 构建整个 `hot_wallet_token_balance`；从 filter 和本组 hot-only raw mint/metadata 构建 `hot_token_info`。
6. 重启 Merge，并等待前后间隔 5 分钟的检测中该组每张表的活动 parts 数量保持不变，再接受它的增量。

Full archive 排除了历史 tombstone，因此它是新的当前态基线，不做 CloseAccount 候选扫描。

### Incremental

1. 各组独立使用自己的 `slot` watermark 选择/过滤 archive；active 不受 staging 水位影响。
2. 解析到 live Token Account 时，只有 mint 在本组 frozen filter 中才向 L2 写入一条版本，并记录其 `(mint, owner)`。
3. canonical empty account 仅作为 CloseAccount 候选。程序按 pubkey 从同组 L2 `FINAL` 查询旧的 live mint/owner；找得到才追加一条带该 pair 的 tombstone。普通零 lamport 账户不会写入 L2。
4. 对本批 live 行和 tombstone 的去重 `(mint, owner)` 集合，查询 L2 `FINAL` 聚合该 pair 的所有当前 Token Account，并向 L3 写入覆盖版本。未受影响 pair 不扫描、不重建。
5. `hot_token_info` 不在 incremental 中重建。

所以耗时不再与 `raw_token_account` 历史大小相关；典型增量的成本是解析 archive、少量 L2 pubkey lookup，以及受影响 wallet/mint pair 在 L2 中的聚合。一个 pair 若有很多 Token Account，仍需读取该 pair 的全部当前 L2 行，这是获得正确余额所必需的边界。

## 水位与切换

active 与 staging 各自维护自己的 slot：

```text
active slot  ── active eligible incremental ──> active slot'
staging slot ─ staging eligible incremental ─> staging slot'
```

新 full 到来时，后台线程从 slot 0 构建 `_bak`；active 继续消费自己的增量。staging 完成后依序追赶其适用增量。两组达到同一 slot、staging 无队列任务时，程序：

1. 执行 staging 自检；
2. 依序 `EXCHANGE TABLES` 交换七对表；
3. 将 staging 的 frozen mint 集合替换为 active 的进程内集合；
4. 向 `hot_index_control` 写审计记录；
5. 保留旧 `_bak` 五分钟，然后 `TRUNCATE` 以供下一轮 full 使用。

`EXCHANGE TABLES` 的多对交换不是全局单事务。短窗口中的跨表查询可能读到不同代际；单表查询和只读 L3 的 Top-N 查询不受影响。

## 查询

当前态需要考虑 ReplacingMergeTree 尚未后台合并的版本：

```sql
-- 某个 hot mint 的 holder Top-N
SELECT owner, amount_raw
FROM solana.hot_wallet_token_balance FINAL
WHERE mint = '...'
  AND amount_raw > 0
ORDER BY amount_raw DESC
LIMIT 100;

-- 某钱包的 hot 资产
SELECT mint, amount_raw
FROM solana.hot_wallet_token_balance FINAL
WHERE owner = '...'
  AND amount_raw > 0
ORDER BY mint;
```

Projection `proj_by_mint_amount` 与 `proj_by_owner` 适合非 `FINAL` 的低延迟近实时读；严格当前态读使用 `FINAL` 或下游自己的 `argMax` 语义。

## 升级

从有 `raw_token_account` 的版本升级时，先按 [clickhouse_schema.md](clickhouse_schema.md) 增加 L2 的三个字段并创建两张 filter 表。然后用 `--bootstrap` 导入一次 full snapshot；成功后确认旧进程已停，再删除 `raw_token_account` 和 `_bak` 以回收空间。普通 watcher 续传不能替代这次 bootstrap，因为旧组没有新架构所需的冻结 filter。

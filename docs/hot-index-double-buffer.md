# Hot 索引双缓冲

## 目标

新 full snapshot 的冷加载、Merge 和聚合不能阻塞正在服务的增量路径。ETL 因此维护一组无后缀的 active 表和一组 `_bak` staging 表；对外查询永远使用无后缀表名。

```text
active                                      staging
raw_account                 <->            raw_account_bak
raw_token_mint              <->            raw_token_mint_bak
raw_token_metadata          <->            raw_token_metadata_bak
hot_token_account_state     <->            hot_token_account_state_bak
hot_token_info              <->            hot_token_info_bak
hot_wallet_token_balance    <->            hot_wallet_token_balance_bak
```

`hot_token` 和 `hot_token_enabled` 是全局配置对象，不参与交换。

## 表职责

- `raw_account`：所有账户的轻量元信息；它提供 watcher 的 `max(updated_slot)` watermark。
- `raw_token_mint`、`raw_token_metadata`：只保存本表组冻结 hot mint 的解析结果，用于 full 阶段构建 `hot_token_info`。
- `hot_token_account_state`：L2，唯一的 hot Token Account 状态表。解析器直接写入；没有 `raw_token_account` 中转表。
- `hot_wallet_token_balance`：L3，按 `(mint, owner)` 的聚合余额。
- `hot_token_info`：L2 展示缓存，仅在 full 阶段构建，直到下一次组切换前保持不变。

## 冻结 hot mint 集合

`hot_token_enabled` 是可变的全局配置，但一个 active generation 的历史不能因配置更新而重新解释：

1. 构建一个 full generation 时，程序将 `hot_token_enabled` snapshot 为进程内 `HashSet`，并原子写入工作目录的 `solana-snapshot-etl-hot-mints-*.txt` 文件。
2. 该组的 SPL Token、Token-2022 Mint 和 Metaplex metadata 解析均只保留集合内 mint。
3. 该组的所有 incremental 都复用同一集合。active 增量不会查询 `hot_token_enabled`，也不会因全局配置变化重建 L2/L3。
4. 新 full 的 staging 组得到自己的新集合；交换表名后，它才成为新的 active 集合。

因此 active/staging 在同一 slot 使用不同 hot 集合是正常且预期的。切换前只验证 staging 自身：内存集合非空，且 `hot_token_info_bak` 的行数与该集合大小相同；不比较两组的 L2/L3 行数或余额总量。

## 全量与增量路径

### Full

1. 停止目标组六张表的后台 Merge，清空目标组。
2. snapshot 全局 hot mint 到内存集合和本地状态文件。
3. 解析 archive：所有账户元信息进入 `raw_account`；只有 frozen hot mint 的 mint、metadata 和 Token Account 进入对应表。
4. Token Account 直接写入 `hot_token_account_state`，带 `delegate`、`delegated_amount` 和 `close_authority`。
5. 从 L2 构建整个 `hot_wallet_token_balance`；按内存集合分批查询本组 hot-only raw mint/metadata，构建 `hot_token_info`。
6. 重启 Merge，并等待前后间隔 2 分钟的检测中该组每张表的活动 parts 数量保持不变，再接受它的增量。

Full archive 排除了历史 tombstone，因此它是新的当前态基线，不做 CloseAccount 候选扫描。

### Incremental

1. 各组独立使用自己的 `slot` watermark 选择/过滤 archive；active 不受 staging 水位影响。
2. 解析到 live Token Account 时，只有 mint 在本组 frozen `HashSet` 中才向 L2 写入一条版本，并记录其 `(mint, owner)`。
3. canonical empty account 仅作为 CloseAccount 候选。程序按 pubkey 从同组 L2 `FINAL` 查询旧的 live mint/owner；找得到才追加一条带该 pair 的 tombstone。普通零 lamport 账户不会写入 L2。
4. 对本批 live 行和 tombstone 的去重 `(mint, owner)` 集合，查询 L2 `FINAL` 聚合该 pair 的所有当前 Token Account，并向 L3 写入覆盖版本。未受影响 pair 不扫描、不重建。
5. `hot_token_info` 不在 incremental 中重建。

所以耗时不再与 `raw_token_account` 历史大小相关；典型增量的成本是解析 archive、少量 L2 pubkey lookup，以及受影响 wallet/mint pair 在 L2 中的聚合。一个 pair 若有很多 Token Account，仍需读取该 pair 的全部当前 L2 行，这是获得正确余额所必需的边界。

## 代际、水位与切换

每一路同时维护两个不同的 slot：

- `full_slot`：该路绑定的全量快照代际。全量开始时确定，直到该物理组被下一次 full 清空并重建前都不改变。
- `max_slot`：该路最后一次成功提交的全量或增量的最大 slot。每次该路成功完成导入后推进。

active 与 staging 各自维护自己的 `max_slot`；二者在切换时不必相等：

```text
active slot  ── active eligible incremental ──> active slot'
staging slot ─ staging eligible incremental ─> staging slot'
```

首次 cold start 时，active 先绑定并灌入一个 full，staging 为 `disabled`。后续发现的“新 full”按 `new_full_slot > active.full_slot` 判断，而**不是**与 `active.max_slot` 比较：active 即使已经通过增量推进得更远，仍可把该 full 的新尾段当作增量处理；staging 则绑定该 full 并从 slot 0 冷启动。

staging 完成 full 和首个适用增量后，active 不再派发新的增量；待此前 active 导入已提交，程序：

1. 执行 staging 自检；
2. 依序 `EXCHANGE TABLES` 交换六对表；
3. 将 staging 的 frozen mint 集合替换为 active 的进程内集合；
4. 立即把 JSON 中的 staging generation 提升为 active，并把旧 active 记为 `disabled` staging。

旧 `_bak` 不按时间自动清空；它保留到下一次更高 `full_slot` 的 full 到来，届时才清空并灌入新 staging full。

工作目录中的 `solana-snapshot-etl-state.json`（v3）为 active/staging 分别记录 `phase`、`full_snapshot`、`max_slot`、冻结 mint 文件和正在执行的增量。`phase` 的含义如下；其中「已提交」指相应 INSERT 和派生索引刷新均已成功返回，并已把该结果写入 JSON。

| `phase` | 可出现在哪一路 | 已经提交的状态 | 当前允许/禁止的工作 | 停止后再次启动 |
| --- | --- | --- | --- | --- |
| `disabled` | 通常是 staging；bootstrap 前的 staging，以及切换后的旧 active 都在此状态 | 对一个从未使用的 staging，`full_snapshot`、`max_slot`、mint 文件均为空。切换后的旧 active 则会保留其原有的 full、`max_slot` 和 mint 文件，便于审计，但它已经不再是工作代际。 | 不给此路派发增量。旧 `_bak` 表不清理、不更新；只有发现比 `active.full_slot` 更新的 full 时，才会清空此物理组并把它转为 `full_loading`。 | 保持禁用；不会因为重启而误把旧 `_bak` 当作 staging 继续更新。 |
| `waiting_for_full` | 仅 active | `--bootstrap` 已原子重置 journal；尚未选定任何 full，故没有有效 `full_snapshot` 或 `max_slot`。 | 只扫描并选择可用的 full；不会从旧 active 水位继续增量，也不会写 staging。 | 继续等待/选择 full。选定后先写入 `full_loading`，再进行清表和导入。 |
| `full_loading` | active 或 staging | 已固定写入这一路要使用的 `full_snapshot`（路径与不可变的 `full_slot`）；full 数据和 `max_slot` 还不能视为完成。 | 该物理组正在或即将停止 Merge、清空并导入此 full；这一路不接收增量。另一条 active 路若处于正常服务状态，仍可独立做增量。 | 使用 JSON 中**同一个** full archive 重做该路 full；不会改选较新的 full，也不会以未完成的数据开始增量。 |
| `full_merging` | active 或 staging | full 原始数据、L2/L3 派生索引和冻结 mint 文件均已提交；`max_slot` 已置为该 `full_slot`。Merge 已恢复，但尚未通过稳定屏障。 | 只等待该组六张表的活动 parts 数连续两次、间隔 2 分钟保持不变；在此之前禁止向该路派发增量。 | 只重新启动/继续等待 Merge 稳定，**绝不重清表或重灌 full**。稳定后转为 `ready`。 |
| `ready` | active 或 staging | 已绑定 full 且已经完成 full 与此前所有记录的增量；`max_slot` 是该路最后成功导入的 slot。 | 可以选择一个 `base_slot <= max_slot < slot` 的增量。active 按自己的水位继续服务；staging 的第一个成功增量会触发切换准备。 | 读取冻结 mint 文件，以保存的 `max_slot` 继续选择下一份适用增量；不重放已经完成的工作。 |
| `incremental_loading` | active 或 staging | 此前 full 及所有早先增量已经提交；JSON 已在本次增量开始**之前**记录 `inflight_incremental`，但该增量尚未被标记为完成，因此 `max_slot` 仍是上一个成功水位。 | 该路不再接收另一份增量。active 的写入是同步的；staging 的首增量在后台执行，期间 active 可完成手头已经开始的工作，但不会在 staging 首增量完成后再获派发新任务。 | 按 JSON 记录的同一份 archive 重做这次增量；成功后更新 `max_slot` 并转入 `ready`（active）或 `cutover_pending`（staging）。重复写入依靠按 slot 的版本语义保持结果正确。 |
| `cutover_pending` | 同时出现在 active 和 staging | staging 的 full 和**第一份适用增量**已提交，且 staging `max_slot` 已更新；active 此前正在进行的增量也已经提交，之后不再给 active 派新任务。切换前还会将两组六张表的 UUID 写入 `cutover` checkpoint。 | 不再向任何一路派发增量。先做 staging 自检（冻结 mint 集合非空，且 `hot_token_info_bak` 行数等于该集合大小），随后逐对 `EXCHANGE TABLES`。不要求 `staging.max_slot == active.max_slot`。 | 重做自检并根据 UUID checkpoint 仅交换尚未交换的表对；已交换的表对不会被换回。交换完成后立即将 staging 提升为 `active/ready`，并将旧 active 写成 `staging/disabled`。 |

`--bootstrap` 一启动就原子重置为“active 等待/灌入 full、staging disabled”；非 `--bootstrap` 重启会从该状态继续，而不会重新读取可变的 `hot_token_enabled`。

`EXCHANGE TABLES` 的多对交换不是全局单事务。短窗口中的跨表查询可能读到不同代际；单表查询和只读 L3 的 Top-N 查询不受影响。切换前 JSON 会保存六对表的 ClickHouse UUID；若进程在任一对交换后中断，重启会检查 UUID，只交换尚未完成的对，绝不会把已交换的对换回去。

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

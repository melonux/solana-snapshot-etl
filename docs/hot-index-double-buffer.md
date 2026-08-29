# 热门 Token 二层索引的双路切换设计

本文说明热门 Token 查询索引在全量快照周期中的双路（双缓冲、Blue-Green）运行方式。它覆盖 raw 层、热门 Token 派生层、聚合层和查询路由，不改变快照解析本身的账户版本语义。

## 1. 业务背景

快照导入链路大致每 12 小时经历一次完整周期：

```text
全量快照 [0, S0]
    -> 增量快照 [S0, S1]
    -> 增量快照 [S1, S2]
    -> ...
    -> 增快照量 [S7, S8]   不存在增量快照 [S8, S9]  
    -> 新全量快照 [0, S9]
```

新一轮全量快照虽然给出了 `S9` 时刻的账户最终形态，但它不会携带上一轮全量之后每个账户的变更历史。例如，最后一个增量覆盖 `[1000, 1100]`，下一轮全量覆盖 `[0, 1200]`；全量中没有上一轮增量中的 CloseAccount 历史信息。

因此，如果永远在同一批表上追加全量和增量：

- 关闭账户的 tombstone 可能无法在下一轮全量中重现；
- 重复写入、历史残留和边界重放会逐步积累；
- 统计结果会出现小的、可接受的偏差。

本系统接受这段时间内的微小误差，但希望误差不会跨多个全量周期累积。解决办法是每次新全量到达时，使用另一组表从零重建，完成后整体切换。

## 2. 表组拓扑

raw 层也必须双路。否则备用组无法从新全量快照独立获得完整基线。

```text
raw_account                 raw_account_bak
raw_token_account           raw_token_account_bak
raw_token_mint              raw_token_mint_bak
raw_token_metadata          raw_token_metadata_bak
       |                            |
       v                            v
hot_token_account_state     hot_token_account_state_bak
hot_token_info              hot_token_info_bak
hot_wallet_token_balance    hot_wallet_token_balance_bak
       |                            |
       +------------+---------------+
                    v
          hot_index_control
                    |
                    v
              编排与审计
```

只保留一份的表：

```text
hot_token
hot_token_enabled
hot_index_control
```

其中：

- 无后缀 raw 表和 `_bak` raw 表是两个相互独立的快照状态库；
- `hot_token_account_state` 和 `hot_token_account_state_bak` 保存热门 Token 的 Token Account 粒度当前态；
- `hot_wallet_token_balance` 和 `hot_wallet_token_balance_bak` 保存钱包 × mint 的聚合余额，直接服务两类查询；
- `hot_token_info` 和 `hot_token_info_bak` 保存同一组的 decimals、供应量和 metadata 展示信息；
- `hot_index_control` 供编排器记录构建进度、切换代数和审计信息。

```text
不带后缀       当前服役组（active）
带 _bak 后缀   当前备用组（staging/backup）
```

查询服务永远访问 `hot_wallet_token_balance` 和 `hot_token_info`，不需要先读取控制表再拼接表名。组切换时只交换两组表名。

## 3. 一组表的职责

### 3.1 raw 层

每一组 raw 表都完整接收对应的全量和增量快照。`raw_token_account` 与 `raw_token_account_bak` 继续使用：

```sql
ENGINE = ReplacingMergeTree(updated_slot, is_deleted)
ORDER BY pubkey
```

同一 `pubkey` 的最新版本由 `updated_slot` 决定，CloseAccount 由更高 slot 的 `is_deleted = 1` tombstone 表示。强一致当前态查询需要 `FINAL` 并过滤 `is_deleted = 0`。

全量归档不包含历史 tombstone；增量归档包含 tombstone。新组从全量冷启动后，再连续应用其后的增量，删除状态只需要在本次全量周期内正确传播。全量归档本身已是当前态，因此全量导入后的 L2 回填直接扫描 raw 表，不执行 `FINAL`；后续增量只将变化版本和 tombstone 追加到 L2，由 L2 自身的 `ReplacingMergeTree` 负责版本覆盖。

### 3.2 热门 Token 状态层

`hot_token_account_state` 和 `hot_token_account_state_bak` 分别从同名组的 `raw_token_account` 与 `raw_token_account_bak` 筛选热门 Token，按 `pubkey` 保留版本和 tombstone。它们是聚合层的唯一输入，避免服务查询时扫描近十亿行 raw 数据。

即使 tombstone 的 `mint`、`owner` 为空，也不能按 mint 过滤掉删除版本；删除版本必须通过 `pubkey` 覆盖旧的 live 版本。由于每轮全量会清空并重建 staging 组，600 万级 tombstone 的存储和 merge 成本是可接受的。

### 3.3 钱包聚合层

`hot_wallet_token_balance` 与 `hot_wallet_token_balance_bak` 分别对对应的状态层执行：

```text
过滤 FINAL 后的有效账户
    -> 按 (mint, owner) 汇总 amount
    -> 丢弃 amount=0 的结果
```

表按 `(mint, owner)` 保存 ReplacingMergeTree 版本；`proj_by_mint_amount` 按
`(mint, amount_raw, owner)` 组织数据，`proj_by_owner` 按 `(owner, mint)` 组织数据。
Projection 定义使用升序，Top-N 查询时再对 `amount_raw DESC` 排序。由于余额表会
追加同一 `(mint, owner)` 的新版本，精确读取使用限定范围的 `FINAL` 或 `argMax`；
不要求强实时的查询可以直接读取，后台 Merge 收敛后自然得到最新版本。`amount_raw`
始终保留最小单位整数，展示时再结合 `hot_token_info.decimals` 或
`hot_token_info_bak.decimals` 换算。

全量阶段的 L3 聚合也直接读取刚建立的 L2 当前态，不做 `FINAL`。聚合使用 `max_bytes_before_external_group_by` / `max_bytes_before_external_sort` 的 1 GiB 阈值：高基数的 `(mint, owner)` 中间状态会溢写到 ClickHouse 临时磁盘，而不是占满服务器内存。因此部署时必须为 ClickHouse 的临时目录预留空间。

## 4. 生命周期

### 4.1 初次冷启动

初次部署时先构建无后缀 active 组：

1. 暂停 active 组七张 raw+hot 表的后台 Merge；
2. 清空并确认无后缀 raw 表和二层表为空；
3. 以 `resume_slot = 0` 导入一个可用的全量快照；
4. 构建并刷新 active 组的热门 Token 状态、钱包余额和 Token 信息；
5. 确认全量和二层刷新全部成功后，恢复 active 组七张 raw+hot 表的后台 Merge；如果失败则保持暂停并退出，等待清理后重新冷启动；
6. 等待 active 组七张 raw+hot 表的每个 partition 活跃分片数均小于 20；在此 Merge 收敛窗口内不派发 active 后续增量 INSERT；
7. 按全量之后的增量顺序写入 active 组；
8. 创建空的 `_bak` 组；
9. 将 `hot_index_control` 写成当前切换代数并记录 `ready_slot`，用于内部审计。

### 4.1.1 全量冷启动期间的 Merge 控制

全量快照导入是追加写入最密集的阶段。`ReplacingMergeTree`/`MergeTree` 的后台 Merge 会同时消耗 CPU 和磁盘 IO，并可能让其他较稀疏的 HTTP RowBinary INSERT 流超过 ClickHouse 的接收空闲超时。因此，编排器只在全量冷启动期间暂停目标组的七张 raw+hot 表：

```sql
SYSTEM STOP MERGES solana.raw_account;
SYSTEM STOP MERGES solana.raw_token_account;
SYSTEM STOP MERGES solana.raw_token_mint;
SYSTEM STOP MERGES solana.raw_token_metadata;
SYSTEM STOP MERGES solana.hot_token_account_state;
SYSTEM STOP MERGES solana.hot_token_info;
SYSTEM STOP MERGES solana.hot_wallet_token_balance;
```

全量及 hot 表刷新成功后，对 active 组执行对应的七条 `SYSTEM START MERGES`；下面以 `_bak` 为例，active 组只需去掉 `_bak` 后缀：

新一轮全量构建 `_bak` 时只暂停 `_bak` 表；无后缀 active 组继续 Merge 并对外服务：

```sql
SYSTEM STOP MERGES solana.raw_account_bak;
SYSTEM STOP MERGES solana.raw_token_account_bak;
SYSTEM STOP MERGES solana.raw_token_mint_bak;
SYSTEM STOP MERGES solana.raw_token_metadata_bak;
SYSTEM STOP MERGES solana.hot_token_account_state_bak;
SYSTEM STOP MERGES solana.hot_token_info_bak;
SYSTEM STOP MERGES solana.hot_wallet_token_balance_bak;
```

只有在全量导入和该组二层刷新全部成功、即将进入增量阶段时，才恢复后台 Merge：

```sql
SYSTEM START MERGES solana.raw_account_bak;
SYSTEM START MERGES solana.raw_token_account_bak;
SYSTEM START MERGES solana.raw_token_mint_bak;
SYSTEM START MERGES solana.raw_token_metadata_bak;
SYSTEM START MERGES solana.hot_token_account_state_bak;
SYSTEM START MERGES solana.hot_token_info_bak;
SYSTEM START MERGES solana.hot_wallet_token_balance_bak;
```

如果 bootstrap 的解析、ClickHouse 导入、重置或二层刷新失败，则不执行 `SYSTEM START MERGES`，程序直接失败退出；该组数据必须清理后重新冷启动。如果失败发生在已经服役 active 的情况下（即 `_bak` staging），则只告警并清理 `_bak`，保持 active 继续服务，在主循环中重试全量，不影响 active 的增量更新。

暂停 Merge 不会中断已经运行的 Merge，只阻止新的后台 Merge；INSERT 自身仍然需要压缩、写盘和创建 part。暂停期间会累积 parts，因此只能用于短期冷启动，不能在正常增量阶段长期保持。ClickHouse 的 `parts_to_delay_insert` / `parts_to_throw_insert` 阈值仍然有效，必须持续监控：

```sql
SELECT table, count() AS active_parts, sum(rows) AS rows
FROM system.parts
WHERE database = 'solana' AND active = 1
GROUP BY table
ORDER BY active_parts DESC;
```

当前程序在每个全量路径开始前自动执行 `SYSTEM STOP MERGES`，覆盖该组七张 raw+hot 表；只有全量及二层刷新成功后才执行 `SYSTEM START MERGES`，并在日志中记录组名和操作结果。恢复后不会立刻开始下一份增量：程序会对目标组七张表分别执行与下例等价的 `system.parts` 查询，要求**每个 partition** 的 `parts_count < 20` 后才解除该组增量写入屏障。这样让全量导入形成的 raw 和 hot Merge 高峰先释放 IO，避免下一批 HTTP RowBinary INSERT 因磁盘排队而超时；它不是 `OPTIMIZE FINAL`，剩余少量 Merge 仍由后台自然完成。`hot_token_enabled` 与 `hot_index_control` 是全局控制表，不属于任一组，不参与此暂停/恢复。

```sql
SELECT
    partition,
    count() AS parts_count,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS total_size
FROM system.parts
WHERE database = 'solana'
  AND table = 'raw_token_account_bak'
  AND active = 1
GROUP BY partition
ORDER BY parts_count DESC, partition;
```

检查间隔为 10 秒，未收敛时每 30 秒输出一次 INFO 进度。若 `system.parts` 查询暂时失败，已成功的全量数据不会被清理；程序保持 Merge 运行并重试该检查。bootstrap 失败路径保持 Merge 暂停并退出；staging 的全量/二层刷新失败路径保持 `_bak` Merge 暂停、清理后重试。全量 raw 是规范当前态，L2 回填和 mint/metadata 信息回填均不使用 `FINAL`；增量路径在需要读取 L2 当前态作钱包聚合时才使用 `FINAL`，并将聚合中间结果限制为可溢写到临时磁盘。

该控制只覆盖全量冷启动。正常增量阶段不主动暂停或恢复 Merge；增量写入失败时沿用原有的失败即停止和按 active 最大 slot 重启续传策略。

### 4.1.2. raw 已完成、hot 刷新失败时的修复

全量 raw 写入和 hot 派生刷新是两个阶段。若日志显示 raw 已提交、但旧版本在 `ReplacingSorted`/`FINAL` 或构建 Join 右侧哈希表时因 ClickHouse 内存上限（`Code: 241 MEMORY_LIMIT_EXCEEDED`）失败，不需要再次解析或导入快照。使用一次性修复动作：

```shell
solana-snapshot-etl --clickhouse-rebuild-hot
# 仓库中的辅助脚本也支持：
./run.sh --clickhouse-rebuild-hot
```

该动作只读取现有无后缀 active raw 表，先暂停 active 组七张 raw+hot 表的后台 Merge，再重建三张 active hot 表；不会读取快照、不会清空或重灌 raw，也不会触碰 `_bak`。全量基线的 L2 回填不使用 `FINAL`；`hot_token_info` 按每批 10,000 个、按 mint 排序的连续范围构建，且只在全量/修复时建立，正常增量保持不变；余额聚合允许在 1 GiB 后溢写临时磁盘。三张表全部成功后才恢复 active 组 Merge。若重建再次失败，程序退出并保持 Merge 暂停，便于先调整 ClickHouse 内存、临时磁盘或并发设置后重试；不能把不完整的 hot 结果当作成功状态继续追加增量。

修复成功后，正常 watcher 应在不带 `--bootstrap` 的情况下启动，以 active raw 最大 slot 续传增量。仓库 `run.sh` 默认就是续传模式；只有首次建立空库时才显式追加 `./run.sh --bootstrap`。

### 4.2 正常增量阶段

当前由无后缀表对外服务。每个增量快照成功导入后：

- 更新 `raw_*`；
- 更新 `hot_token_account_state` 和 `hot_wallet_token_balance`；
- 保留本组已有的 `hot_token_info`（该表只在本组全量冷启动时重建）；
- 推进 active 组的数据水位。

此时 `_bak` 表可以保持为空或保存上一个周期的旧数据，但不接收增量，也不参与线上查询。

### 4.3 新全量到达

当发现新的全量快照时，使用带 `_bak` 后缀的表作为 staging：

1. 确认 `_bak` 表当前没有被查询服务使用；
2. 暂停 `_bak` 组七张 raw+hot 表的后台 Merge；
3. 清空所有 raw 和二层 `_bak` 表；
4. 用 `resume_slot = 0` 将新全量完整导入 `_bak` 组；
5. 完成该全量及二层刷新且确认成功后恢复 `_bak` 七张 raw+hot 表的后台 Merge；等待每个 partition 的活跃分片数均小于 20；失败时保持暂停，由主循环清理后重试，不影响 active；
6. 记录该全量的 slot 作为 staging 基线；
7. 继续把基线之后的增量应用到 `_bak` 组。

在 `_bak` 重建期间，无后缀 active 表继续对外服务，并由 watcher 主线程持续消费 active 路径的增量；`_bak` 全量导入、hot 刷新和 Merge 收敛在独立后台任务中进行。后台任务完成后，staging 才从自己的全量 slot 开始追赶增量。两条路径按各自水位处理同一批增量，互不扇出写入；因此 `_bak` 的冷启动和 Merge 高峰不会阻塞 active 增量更新。

两组都必须按相同的快照顺序推进，并记录各自最后成功的 `ready_slot`。如果 staging 导入、解析或二层刷新失败，只保留 active 组继续服务；程序给出警告，保持 `_bak` raw Merge 暂停，清理 `_bak` 七张表后自动重试该全量，不退出主循环。若清理本身失败，下一轮重试时会再次尝试清理。

### 4.4 追平与切换

`_bak` 组满足以下条件后才允许切换：

- 已成功导入新全量；
- 已应用到与 active 组相同的最新增量 slot；
- `hot_token_account_state_bak` 已完成必要 merge/刷新；
- `hot_wallet_token_balance_bak` 已刷新完成，`hot_token_info_bak` 已在全量阶段建立；
- 抽样校验结果满足预期。

切换前先建立一个短暂的写入屏障：

1. 停止向 active 和 `_bak` 两条路径派发新的快照；
2. 等待两路已经开始的 ClickHouse INSERT 全部完成；
3. 记下两组共同达到的 slot，确认 staging 组已追平到这个 slot。

然后使用一个 `EXCHANGE TABLES` 语句交换 active 和 staging 的全部表名。每一对表的交换都是原子的，不需要临时名称：

```sql
EXCHANGE TABLES
    solana.raw_account AND solana.raw_account_bak,
    solana.raw_token_account AND solana.raw_token_account_bak,
    solana.raw_token_mint AND solana.raw_token_mint_bak,
    solana.raw_token_metadata AND solana.raw_token_metadata_bak,
    solana.hot_token_account_state AND solana.hot_token_account_state_bak,
    solana.hot_token_info AND solana.hot_token_info_bak,
    solana.hot_wallet_token_balance AND solana.hot_wallet_token_balance_bak;
```

同一条语句中的多对交换会按对顺序处理，而不是把七对表作为一个全局原子事务。因此，切换的极短窗口中，跨表查询可能读到不同代际的组合。将 `hot_token_info` 放在 `hot_wallet_token_balance` 前交换：主要业务表 `hot_wallet_token_balance` 切换时，新的 Token 信息已经就绪；Top-N 查询只读取余额表，不受此窗口影响。

如果业务要求一次 JOIN 中的余额和展示信息也必须严格来自同一代际，则仅靠多对 `EXCHANGE TABLES` 不够，需要增加 generation 过滤、将展示字段写入余额表，或继续使用单一控制表/视图路由。当前方案接受这个极短的展示信息不一致窗口，换取查询端不需要动态选择表名。

`hot_index_control` 仍建议保留，但职责改为内部编排和审计：记录构建代数、`ready_slot`、快照文件名和 `hot_token_version`。它不再是查询服务的路由依赖；切换成功后再追加一条记录用于审计。如果控制表写入失败，不影响已经完成的表名切换，但编排程序应通过表名和 slot 校验后补写记录。

交换成功后，恢复从不带后缀的 active 表写入后续增量。原来的 active 组现在带 `_bak` 后缀，进入 5 分钟回滚窗口；窗口结束后再清空它。写入屏障通常只需覆盖当前 INSERT 的收尾时间，不需要停止查询服务。

### 4.5 退役旧组

切换完成后，旧组变成下一轮 staging 组。不要立即删除：

1. 先确认查询服务已经通过不带后缀的表名看到新组数据；
2. 保留旧组 5 分钟，作为回滚安全窗口；
3. 5 分钟窗口结束后，使用带 `max_table_size_to_drop = 0` 的 `TRUNCATE TABLE` 清空 `_bak`；
4. 下一次新全量到达时，重新使用已清空的 `_bak` 组。

回滚只需在 5 分钟窗口内再次执行相同的 `EXCHANGE TABLES`。窗口结束后才允许清空 `_bak`，因此超时后不再依赖旧组进行回滚。

清空旧组时必须显式关闭大表删除限制；对七张表分别执行：

```sql
TRUNCATE TABLE solana.raw_account_bak SETTINGS max_table_size_to_drop = 0;
TRUNCATE TABLE solana.raw_token_account_bak SETTINGS max_table_size_to_drop = 0;
TRUNCATE TABLE solana.raw_token_mint_bak SETTINGS max_table_size_to_drop = 0;
TRUNCATE TABLE solana.raw_token_metadata_bak SETTINGS max_table_size_to_drop = 0;
TRUNCATE TABLE solana.hot_token_account_state_bak SETTINGS max_table_size_to_drop = 0;
TRUNCATE TABLE solana.hot_token_info_bak SETTINGS max_table_size_to_drop = 0;
TRUNCATE TABLE solana.hot_wallet_token_balance_bak SETTINGS max_table_size_to_drop = 0;
```

## 5. `hot_token` 一致性

`hot_token` 是单份外部配置表，同一 mint 的变更通过递增 `version` 表示。构建一个 staging 组时，需要固定本轮使用的热门 Token 集合，否则构建期间新增或禁用 Token 会让同组表出现代际混合。

当前实现默认不阻塞 `hot_token` 写入，而是在每个快照刷新前计算启用集合指纹；指纹变化时会对该组的 L2 状态执行一次全量回填，再重建 L3。因此配置变更最终会自动生效。若要求一次全量构建期间严格固定集合，仍可在外部配置层冻结 `hot_token` 写入。

有两种实现方式：

1. 在 staging 构建期间暂时冻结 `hot_token` 变更；
2. 在构建开始时把 `hot_token_enabled FINAL` 的结果固化为本轮配置快照，所有筛选和 backfill 都使用该快照。

无论采用哪种方式，都要把配置版本写入 `hot_index_control.hot_token_version`。当前 `hot_token.version` 是按 mint 递增的行版本；如果需要严格的全局配置代数，应由配置管理程序额外维护一个全局 generation，不能仅依赖任意一行的 version。

## 6. 查询路由

查询服务固定访问不带后缀的当前表，不需要查询 `hot_index_control`，也不需要拼接表名：

```text
hot_wallet_token_balance
hot_token_info
```

换名操作对查询服务透明。服务只需在连接或定期健康检查时读取 `hot_index_control`，用于展示当前 generation 和 ready_slot，不把它作为业务查询的前置步骤。

钱包资产查询：

```sql
SELECT b.mint, b.amount_raw, i.decimals, i.symbol, i.name
FROM solana.hot_wallet_token_balance AS b
LEFT ANY JOIN solana.hot_token_info AS i USING (mint)
WHERE b.owner = {wallet}
ORDER BY b.mint;
```

指定 Token 的 Top-N：

```sql
SELECT owner, amount_raw
FROM solana.hot_wallet_token_balance
WHERE mint = {mint}
ORDER BY amount_raw DESC, owner
LIMIT {n};
```

如果需要离线校验备用组，才直接访问带 `_bak` 后缀的表；线上业务查询始终使用不带后缀的表。

## 7. 运行约束与校验

- 两组 raw 和二层表必须使用同一套 schema、引擎和 Projection 定义。
- 全量冷启动期间只暂停目标组七张 raw+hot 表的后台 Merge；active 服役组在 staging 构建期间继续 Merge。
- 只有冷启动和二层刷新全部成功后才恢复目标组 raw 表的 Merge；恢复后必须等待四张 raw 表每个 partition 的活跃分片数均小于 20，才派发下一份增量；bootstrap 失败时保持暂停并退出，staging 失败时保持 `_bak` 暂停、清理后重试；暂停期间监控 `system.parts`，防止触发 parts 延迟/拒绝阈值。
- 清空操作只能针对非 active 的 `_bak` 组执行，并且必须在 5 分钟回滚窗口结束后使用 `TRUNCATE TABLE ... SETTINGS max_table_size_to_drop = 0`。
- 全量重建必须从 slot 0 开始，不能沿用 active 组的 raw 水位。
- 增量文件要按 base slot 和 ending slot 校验顺序；两组不得跨越未处理的 slot gap。
- `EXCHANGE TABLES` 切换必须先暂停两条增量路径的新快照派发，并等待两组未完成的 INSERT 全部提交。
- 切换前比较两组的最新成功 slot、热门 Token 数、钱包余额总量和 Top-N 抽样结果。
- 记录 `generation`、`ready_slot`、快照文件名和 `hot_token_version`，便于审计与回滚。
- 旧组清空前必须保留 5 分钟可回滚窗口；发生 staging 失败时 active 组不受影响。

该设计允许 active 组在一个全量周期内存在微小误差，但通过下一轮 staging 全量重建把误差、重复版本和历史残留限制在单个周期内，不会无限累积。

## 8. 与现有 ETL 的衔接

ETL 现在已经按本设计实现双路表组。运行时通过 `TableGroup::Active` 和
`TableGroup::Backup` 生成稳定表名与 `_bak` 表名，所有 raw 写入、tombstone
写入及二层刷新都携带同一个逻辑组参数。核心行为如下：

1. 冷启动时清空 active 组，从 `resume_slot = 0` 导入全量；新一轮全量到达后清空 `_bak` 并从 slot 0 构建 staging；
2. 正常增量只导入 active。staging 存在时，每个增量分别打开两次 loader，按各自水位独立写入 active 与 `_bak`；
3. 全量 raw 导入后建立对应组的 `hot_token_account_state*`、`hot_wallet_token_balance*` 和 `hot_token_info*`；正常增量只筛选变更 slot 更新前两者，`hot_token_info*` 作为静态展示缓存保留到该组退役；L3 两张服务表先在临时克隆表中构建，再用 `EXCHANGE TABLES` 换入，避免 active 查询看到半成品；
4. 两组 slot 追平后暂停当前派发，比较三张派生表的行数及余额总量，依次执行七对 `EXCHANGE TABLES`，更新 `hot_index_control` 审计记录，并保留旧组 5 分钟；
5. 安全窗口结束后按 `TRUNCATE TABLE ... SETTINGS max_table_size_to_drop = 0` 清空 `_bak`，等待下一轮全量。
6. 每轮全量冷启动时，只暂停正在接收全量的目标组七张 raw+hot 表的后台 Merge；只有全量及二层刷新成功后才恢复，并等待目标组每张表每个 partition 的活跃分片数都小于 20，才恢复该组的增量派发。bootstrap 失败则保持暂停并退出；staging 失败则保持 `_bak` 暂停、清理后重试。active 组在 staging 冷启动及 `_bak` Merge 收敛期间不暂停，继续对外服务并消费增量。

程序启动时仍会先校验两组表、字段、引擎和 Projection；校验失败不会读取快照。

## 9. 程序启动前的 schema 校验

使用 ClickHouse 输出时，程序在读取快照、建立 AppendVec loader 或启动导入 worker 之前执行一次 schema 校验。校验失败会立即退出，不会产生部分导入。

校验内容包括：

- active 与 `_bak` 两组的 17 张表是否存在；
- 每张表的 ClickHouse engine 是否符合预期；
- raw、热门 Token、控制表和三张二层表的必需字段及字段类型；
- `hot_wallet_token_balance` 与 `hot_wallet_token_balance_bak` 是否均为
  `ReplacingMergeTree(updated_slot)`、排序键 `(mint, owner)`，并存在
  `proj_by_mint_amount`（`mint, amount_raw, owner`）和 `proj_by_owner`
  （`owner, mint`）两个 Projection；两张表还必须设置
  `deduplicate_merge_projection_mode = 'rebuild'`，以保证后续去重 Merge
  不会报错或丢失 Projection。

校验使用 `system.tables`、`system.columns` 和 `system.projections`，不会扫描业务表数据。它只验证结构，不验证两组的 slot 是否追平；slot 追平属于 staging 构建和切换前检查。

因此，新增或修改表结构后，应先在两组表上完成 DDL 和 Projection，再启动 ETL。错误信息会列出所有缺失或不匹配项，修复后重新启动即可。

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

全量归档不包含历史 tombstone；增量归档包含 tombstone。新组从全量冷启动后，再连续应用其后的增量，删除状态只需要在本次全量周期内正确传播。

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

表的排序键 `(mint, amount_raw DESC, owner)` 服务指定 Token 的 Top-N；owner projection 服务指定钱包的资产列表。`amount_raw` 始终保留最小单位整数，展示时再结合 `hot_token_info.decimals` 或 `hot_token_info_bak.decimals` 换算。

## 4. 生命周期

### 4.1 初次冷启动

初次部署时先构建无后缀 active 组：

1. 清空并确认无后缀 raw 表和二层表为空；
2. 以 `resume_slot = 0` 导入一个可用的全量快照；
3. 按全量之后的增量顺序写入 active 组；
4. 构建并刷新 active 组的热门 Token 状态、钱包余额和 Token 信息；
5. 创建空的 `_bak` 组；
6. 将 `hot_index_control` 写成当前切换代数并记录 `ready_slot`，用于内部审计。

### 4.2 正常增量阶段

当前由无后缀表对外服务。每个增量快照成功导入后：

- 更新 `raw_*`；
- 更新或刷新无后缀二层表；
- 推进 active 组的数据水位。

此时 `_bak` 表可以保持为空或保存上一个周期的旧数据，但不接收增量，也不参与线上查询。

### 4.3 新全量到达

当发现新的全量快照时，使用带 `_bak` 后缀的表作为 staging：

1. 确认 `_bak` 表当前没有被查询服务使用；
2. 清空所有 raw 和二层 `_bak` 表；
3. 用 `resume_slot = 0` 将新全量完整导入 `_bak` 组；
4. 记录该全量的 slot 作为 staging 基线；
5. 继续把基线之后的增量应用到 `_bak` 组。

在 `_bak` 重建期间，无后缀 active 表不能停服，继续接收新增量；同时由另一条独立的 staging 消费路径把全量之后的增量写入 `_bak`。两条路径互不扇出写入，各自维护自己的快照水位。

两组都必须按相同的快照顺序推进，并记录各自最后成功的 `ready_slot`。如果 staging 导入失败，只保留 active 组继续服务，修复后清理 staging 组再重试。

### 4.4 追平与切换

`_bak` 组满足以下条件后才允许切换：

- 已成功导入新全量；
- 已应用到与 active 组相同的最新增量 slot；
- `hot_token_account_state_bak` 已完成必要 merge/刷新；
- `hot_wallet_token_balance_bak` 和 `hot_token_info_bak` 已刷新完成；
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
- 清空操作只能针对非 active 的 `_bak` 组执行，并且必须在 5 分钟回滚窗口结束后使用 `TRUNCATE TABLE ... SETTINGS max_table_size_to_drop = 0`。
- 全量重建必须从 slot 0 开始，不能沿用 active 组的 raw 水位。
- 增量文件要按 base slot 和 ending slot 校验顺序；两组不得跨越未处理的 slot gap。
- `EXCHANGE TABLES` 切换必须先暂停两条增量路径的新快照派发，并等待两组未完成的 INSERT 全部提交。
- 切换前比较两组的最新成功 slot、热门 Token 数、钱包余额总量和 Top-N 抽样结果。
- 记录 `generation`、`ready_slot`、快照文件名和 `hot_token_version`，便于审计与回滚。
- 旧组清空前必须保留 5 分钟可回滚窗口；发生 staging 失败时 active 组不受影响。

该设计允许 active 组在一个全量周期内存在微小误差，但通过下一轮 staging 全量重建把误差、重复版本和历史残留限制在单个周期内，不会无限累积。

## 8. 与现有 ETL 的衔接

当前 ETL 代码中的 ClickHouse 表名常量仍使用无后缀的单路名称（例如 `raw_account`、`raw_token_account`）。完成数据库双路建表后，还需要完成以下改造，才能上线本设计：

1. 将输出目标抽象为逻辑组 `active | staging`，由它生成四张 raw 表和三张二层表的固定名称；
2. 冷启动 staging 组时强制使用 `resume_slot = 0`，不能读取 active 组的 raw 水位；
3. 增量阶段由 active 和 staging 两条独立消费者路径负责：正常阶段只运行 active 路径；新全量到达后才启动 `_bak` 路径，并从其全量基线之后开始消费增量；
4. 每组独立记录最后成功的 snapshot slot，只有两组追平后才更新 `hot_index_control`；
5. 所有依赖 raw 表的查询、Materialized View、Projection 和诊断 SQL 都要绑定到同一个逻辑组，不能跨组 JOIN；换名后应确认依赖仍指向对应物理表 UUID。

在改造完成前，不要直接启用双路表名并恢复导入；否则旧版 ETL 只会写入无后缀 active 表，无法从 slot 0 构建 `_bak`，也无法让两条路径追平。

## 9. 程序启动前的 schema 校验

使用 ClickHouse 输出时，程序在读取快照、建立 AppendVec loader 或启动导入 worker 之前执行一次 schema 校验。校验失败会立即退出，不会产生部分导入。

校验内容包括：

- active 与 `_bak` 两组的 17 张表是否存在；
- 每张表的 ClickHouse engine 是否符合预期；
- raw、热门 Token、控制表和三张二层表的必需字段及字段类型；
- `hot_wallet_token_balance` 与 `hot_wallet_token_balance_bak` 是否都存在 `proj_by_owner` Projection。

校验使用 `system.tables`、`system.columns` 和 `system.projections`，不会扫描业务表数据。它只验证结构，不验证两组的 slot 是否追平；slot 追平属于 staging 构建和切换前检查。

因此，新增或修改表结构后，应先在两组表上完成 DDL 和 Projection，再启动 ETL。错误信息会列出所有缺失或不匹配项，修复后重新启动即可。

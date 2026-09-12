# Changelog

## v1.16.1 (2026-09-12, sqlite-backend 分支)

### refactor: KiraOS 去 residual 品牌——模块更名 kira_memory_import + kv 键迁移

- 模块与用例更名：`kiraos_import.py` → `kira_memory_import.py`、
  `tests/test_kiraos_import.py` → `tests/test_kira_memory_import.py`
  （`run_kiraos_import` → `run_kira_memory_import`、`parse_kiraos_session`
  → `parse_kira_memory_session` 等，全库 kiraos 字样清零）。
- API 路由同步：`/memory/kiraos/import` → `/memory/kira_memory/import`
  （GET 预览 + POST 执行；前端页面同仓发布，两侧一并更新）。
- **kv 标记键迁移**：`kiraos_import_last_run` → `kira_memory_import_last_run`。
  旧键在已部署实例上有落库数据，插件启动时
  `migrate_legacy_import_kv` 一次性把旧键值搬到新键并删除旧行（幂等、
  新键优先、失败不阻断启动）——总览 kv 卡为通用 dump，不迁移会残留
  裸旧键条目。迁移用例 2 项（carry-over / legacy dropped）。
- alias 行 `source` 标签 `kiraos_import` → `kira_memory_import`
  （存量部署该值为 0 行，无兼容问题）。
- README 的 KiraOS_Plugin 外链按 v1.16.0 口径移除（数据源实为宿主
  KiraAI 的 data/memory，写入者=宿主 session_manager）。

## v1.16.0 (2026-09-12, sqlite-backend 分支)

### fix(migrate): --force 唯一键冲突跳过 + 维度校验去 1024 硬编码

- --force 合并语义补全：目标非空时 id 整体偏移 + 引用重映射之外，唯一键
  冲突的源行（summary/fact document_id、alias 三键、边结构键）改为跳过并
  保留目标现状（此前直接撞 UNIQUE 约束中途崩溃、留下半提交数据）；跳过行
  的 id 映射到目标已有行，cluster.source_fact_ids / 边 backfill|cid 证据键 /
  kv id 键的重映射据此保持可解析；marker 记录 per-table skipped 数，校验段
  对账（目标增量 = 源行数 - 跳过数）。
- 迁移修复顺带发现的存量 bug：PG 可空的 supersede_reason 直透 NULL 撞
  SQLite NOT NULL 约束（原工具遇 superseded 边即崩）——补默认值兜底。
- 维度校验不再钉死 1024：改为源↔目标同序采样行的维度比对 + 非空向量行数
  一致性（任意 embedding 模型维度均适用；pack 失败丢向量会在校验暴露）。
- migrate_backend 抽出可测入口 _run_migration；tests/test_sqlite_backend.py
  新增 13 项迁移 checks（FakePG 桩 + 真 SQLite 目标：跳过/偏移/混合重映射/
  kv 重映射/marker 守卫/4 维向量校验）。
- P3-2 落地：kernel.search 的 embed_task 守卫（检索中止路径取消飞行中的
  向量化请求，杜绝孤儿任务 "never retrieved" 告警）+ EmbeddingService.
  _validate 对非 list 形态 fail-open（封掉 len() TypeError 这唯一抛出面）；
  新增 TestRecallEmbedGuard / test_34d 两项用例。
- 注释/docstring 语言：本批引入的注释与 docstring 统一英文（新文件
  tests/_harness.py、tests/test_recall.py 的注释/docstring 全英文，断言
  失败消息沿用既有中文惯例；既有文件仅本次新增/修改的块用英文，从
  test_review_fixes.py 搬入的既有中文注释保持原样）。
- WebUI 显示文案：「KiraOS 数据迁入」→「Kira 记忆数据迁入」（总览 kv 卡片
  label + 迁入面板标题），面板说明中 KiraOS_Plugin → KiraAI（仅显示文案，
  kv 键名与后端语义不变）。

### refactor(review): 测试套件去重合并 + 死代码清理（review 批）

- 测试基建收敛：新增 `tests/_harness.py`（宿主桩 + 装载器），替换此前
  7 份近似拷贝的 core.* 桩样板（~600 行）；主套件 test_noriflow_memory
  保持自包含（会被 test_config_web spec-exec）。
- 按域合并：test_recall_quality + test_hybrid_rollout + test_recall_hints
  → `tests/test_recall.py`（46 用例）；test_review_fixes + test_encode_backfill
  → 并入 test_noriflow_memory.py（193 用例）。修复类改动自此扩展对应
  模块的既有 TestCase，不再另起回归文件。
- 过期测试修剪：RecallHints 旁路移除的 hasattr 负存在断言、
  make_recall_hints 签名锁（与 test_entity_edge 形态学习用例重复）。
- 死代码删除：db/sqlite._pairs_cond（零引用）、envelope.render_reply_ref_block
  （零引用；reply_refs 形参保留）、memory_kernel.entity_hint_keys（仅测试
  引用，复合键投影由 main 内联完成，保留会致词典二次匹配）。
- README 测试一节同步新布局。

## v1.15.5 (2026-09-12, sqlite-backend 分支)

### feat(webui): 注入语料页「立即执行补编码」按钮

- 合并 agent 新增手动触发面：周期间等待改为可唤醒事件（kick()），手动
  触发的周期与周期任务共用同一循环，绝不并发执行；stop() 清事件防幽灵
  成功；运行期暴露 cycle_running / last_cycle 快照。
- 新端点 ×2：POST /memory/encode/backfill（409=周期执行中/预热未结束，
  503=合并管线未装配）、GET /memory/encode/backfill/status（待补编码
  数量，计数上限 1000；上轮周期完成时间与统计）。
- 注入语料页顶部新增触发条：显示待补编码条数与上轮周期结果，周期进行中
  按钮禁用并加速轮询；后端未升级（端点 404）时按钮保持禁用并提示。
- 测试：tests/test_encode_backfill.py 新增 4 项（kick 守卫 ×3 + 真循环
  唤醒/快照/停止清理）。

## v1.15.4 (2026-09-12, sqlite-backend 分支)

### fix(webui): 总览 kv 标记卡片化（后端迁移 / KiraOS 迁入）

_backend_migration_done / kiraos_import_last_run 此前不在 KV_META 白名单
内，总览按原样渲染 raw JSON。补齐友好卡片（迁移来源 + 行数 + 本地时间；
迁入摘要/事实/别名计数），未知键仍按原样兜底展示（不静默消失）。

## v1.15.3 (2026-09-12, sqlite-backend 分支)

### fix(schema)+docs: 存储后端下拉 + 布尔开关归一 + 注释英文化

- storage_backend 改为 string+options（宿主 enum 类型已废弃；StringField
  自带 options 下发与默认值纠正）。
- 4 个 boolean 配置项归一为 switch（宿主加载器三种写法等价映射，纯命名
  归一，行为不变）。
- db 包全部模块 + kiraos_import.py + migrate_backend.py 注释/docstring
  全量英文化（AST 剥离比对验证零代码改动）。

## v1.15.2 (2026-09-12, sqlite-backend 分支)

### fix: SQLite 后端 review 修复 ×3（promote 画像列污染 / decay 参与者门控 / BM25 欠填）

代码 review（059025f..943b672）确认的两个真 bug + 一处与 PG 的行为差异，
共用同一根因：raw sqlite3.Row 的 JSON TEXT 列被当作 Python 数组消费。

- **promote_pass 画像行 related_user_ids 污染**：raw 行的 JSON TEXT 喂给
  `_json_list` 被按字符迭代，晋档落库形如 `["[", "\"", "u", "2", "\"", "]"]`
  （字符数组）。修复为 JSON TEXT → JSON TEXT 直接透传（与 create/replace
  路径同型）。该列当前无读取方（画像注入只取 statement），属潜伏污染；
  启动自修复见下。
- **decay_pass 活跃度门控丢失参与者键组**：活动对推导迭代 raw 的
  `participants` JSON TEXT 字符串，参与者派生键组全部静默丢弃（仅剩触发者
  user_id），仅以参与者身份活跃的用户簇被错误冻结不衰减（PG 语义：
  `unnest(participants)`）。修复为先 `_json_array_of` 再迭代。
- **BM25 路欠填**：旧实现先取 FTS 全库 top-limit 再与 where 相交，where
  选择性强时（会话隔离/黑名单）top-limit 可能全被排除，BM25 路为空
  （PG 版 where 在 LIMIT 前生效）。改写为 FTS5 影子表 JOIN 主表，
  MATCH + 同 where + bm25 序 + LIMIT 一次完成。
- **启动自修复**：`apply_migrations` 末尾新增 `_repair_profile_related_ids`
  ——按「可解析为非空列表 + 元素全单字符 + 拼接回原串可解析为 JSON 数组」
  签名重写坏行（合法行含多字符 uid、空数组均不命中，无误伤、幂等、
  随每次启动执行）。已部署实例的 SQLite 库重启后自愈。

- 测试：test_sqlite_backend.py 新增 8 项回归（BM25 where 填充、promote
  关联列 TEXT 形态、decay 参与者门控、自修复命中/负样本/幂等），85/85；
  主套件 162/162 + 其余子套件全绿。

## v1.15.1 (2026-09-12, sqlite-backend 分支)

### fix: KiraOS 迁入卡片「扫描失败：(children || []) is not iterable」

迁入卡片 `line()` 助手的加粗分支把单个元素直接传给了 `el()` 的
children 参数（该参数按数组迭代），预览渲染第一行即抛 TypeError，被
捕获后显示为扫描失败——后端 GET /memory/kiraos/import 实际正常。
修复为统一传数组；无后端改动。

## v1.15.0 (2026-09-12, sqlite-backend 分支)

### feat: KiraOS_Plugin 记忆数据无损迁入（双后端方案 P4）

维护页设置栏新增「KiraOS 数据迁入」卡片：预览源目录计数 → 弹窗二次确认 →
只读迁入 KiraOS_Plugin 的存量记忆（默认源 = 宿主 `data/memory`，可改路径）。

- **映射**（`kiraos_import.py`，全部只经 MemoryBackend 接口写入，双后端通用）：
  - `chat_memory.json` 按 chunk 组装为信封行文本（有 sender 带归属属性，
    无 sender 的消息转储不伪造归属；正文过防伪装清洗；内嵌
    `[Sep 10 2026 18:44]` 时间戳解析为 occurred_at，兜底文件 mtime），
    以 `summarized=false` 入摘要表，由补编码遍 LLM 提炼为摘要/事实/边；
    宿主系统通知（`Notice [... user_id: system_*]` 伪账号签名，定时任务
    喂送等）不迁入——对长期记忆无价值（用户决策），带真实 user_id 的
    消息转储/平台事件不受影响；用户侧内容全部被滤掉的批次（只剩 bot
    空报文回复）整体跳过，不白烧补编码预算；
  - TOML 事实/洞察（entities + global/self）→ 原始事实表（importance≥7
    → high 置信，其余 medium；群实体归属群 ID；global 域归属 bot，
    bot UID 不可得则跳过并计数）；`archive/`、`global/skills`、
    `skills/`、`memory_index.db`、`core.txt` 按语义不迁入；
  - `profile.json` → 别名行（name/nickname/aliases，source=kiraos_import）。
- **幂等去重**：摘要/事实 document_id 与现网管线同式（kernel 同源公式）、
  别名唯一键 upsert——重复执行或与现网提炼重复的内容命中同键静默跳过；
  实插量按三表行数差实测；`_memory_local_kv` 留最近一次迁入结果标记
  （维护页回显）。
- **命名空间对齐**：KiraOS session/entity 的 adapter 前缀剥类型段后即
  宿主 platform + 裸 session_id，与现网 `participants` 复合键同构，
  导入数据直接进入召回/画像注入面。
- **安全边界**：源目录零写入（测试断言迁入前后逐文件字节 + mtime 不变）；
  误指目录（无 chat_memory.json 且无 entities/）422 拒绝；迁入并发 409。
- **测试**：新增 `tests/test_kiraos_import.py`（48 项：会话映射/信封组装/
  幂等键公式/群与 bot 归属/别名去重/跳过规则/幂等重跑/源只读性）。

## v1.14.1 (2026-09-12, sqlite-backend 分支)

### fix: 维护页在 SQLite 后端下全页 500（Internal Server Error）

`main._pool_or_503()` 返回裸连接池而非 backend 对象——webui_store 双后端
SQL 按 `backend.dialect` 派发方言，池 shim 缺该属性被兜底成 postgres，
SQLite 后端下维护页所有请求把 PG 方言 SQL（`::text` 等）打进 SQLite，
报 `sqlite3.OperationalError: unrecognized token: ":"`。修复为返回
backend；harness 补方言派发契约回归锁（`webui_dialect_dispatch_contract`）。

## v1.14.0 (2026-09-12, sqlite-backend 分支)

### feat: 双存储后端——新增 SQLite 后端（与 PostgreSQL 二选一）

同一份 kernel / 合并 agent / 提示词 / WebUI 逻辑，新增 SQLite 存储后端，
个人部署不再需要维护 PostgreSQL + pgvector 实例（设计定稿：
nori-core `docs/plans/noriflow-dual-storage-backend-plan.md`，P0-P2/P3 落地）。

- **配置**：新增 `storage_backend`（postgres | sqlite | auto；auto=配置了
  dsn 用 postgres，否则 sqlite——现有部署行为零变化）与 `sqlite_path`
  （空 = 插件数据目录/memory.sqlite3）。均为启动期只读，运行中不可切换
  （切后端 = 换库，跨后端迁移走迁移工具，本期不做在线互转）。
- **db 拆包（P0，零行为变化）**：`db.py` → `db/` 包（base 共享纯函数 +
  MemoryBackend 接口契约 / postgres 原样迁入 / sqlite 新实现 / factory
  工厂 / _sqlite_pool 连接 shim）；`db/__init__` 重导出原名，全部上层
  调用方 `from .db import X` 零改动。
- **SQLite 后端（P1）**：
  - `migrations_sqlite/001_init.sql` 合并式初始化（等价 PG 001-010 终态，
    版本序列两侧共享，后续 schema 变更成对新增）；
  - 类型映射：TIMESTAMPTZ→固定格式 UTC TEXT（字典序=时间序）、TEXT[]→
    JSON TEXT（json_each 查询）、vector→float32 BLOB（维度不匹配按缺失
    处理，换 embedding 模型无需重建表）；
  - 向量检索不引入 sqlite-vec：SQL 圈定行集 + Python/numpy 暴力余弦 +
    FTS5 影子表 BM25 路，`_rrf_fuse` 双后端共享融合；个人规模（≤10⁵ 行）
    可用，天花板声明见方案 §5.3；
  - 写事务统一 `BEGIN IMMEDIATE`；数组语义（ANY/&&/array_append/unnest）
    按方案 §5.2 改写；`REGEXP` 由 connect 注册 Python 实现（占位名守卫
    仍引用单源常量 PLACEHOLDER_NAME_SQL）；
  - `_SingleConnPool` shim：acquire 面与 asyncpg 池一致（同任务可重入），
    execute 返回 asyncpg 风格状态串（乐观锁/计数解析依赖）。
- **WebUI 双后端（P2）**：`webui_store.py` 保留共享层（行装配/校验/常量），
  方言分歧 SQL 拆到 `webui_store_pg.py` / `webui_store_sqlite.py`（函数
  一一对应，按 `backend.dialect` 派发）；维护页设置新增存储后端分组，
  dsn 非空守卫仅在 postgres（或 auto 且现值有 dsn）时生效。
- **测试**：新增 `tests/test_sqlite_backend.py` 真库集成 harness（76 项：
  四条收敛管线 / recall 双路 / alias / edge / WebUI 全函数 / 重开持久化），
  集成测试首次无需 PG 服务；既有桩测试全绿（PG 路为纯搬迁零回归）。
- **依赖**：requirements 新增 `aiosqlite`。

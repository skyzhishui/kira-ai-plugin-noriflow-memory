# Changelog

## Unreleased

- 统一 PostgreSQL/SQLite 跨会话摘要的应用时钟边界，并钳制未来时间摘要，避免时钟偏差导致召回不一致。
- 兼容保存含历史跨字段冲突的配置；主动修改冲突字段时仍严格校验。
- 防止簇/摘要弹窗重复提交；回填状态连续获取失败后保持操作按钮禁用，需重新探测后恢复。

## v1.18.9 (2026-11-05)

- 修复 raw-only 用户列表的最高分为空时显示为 `null`；无画像分数现在显示为 `—`。

## v1.18.8 (2026-11-05)

PR #9 CodeRabbit 第三轮评审修复：20 条行内反馈、1 条 body-only 反馈，以及上一轮确认遗漏的 3 项。

### fix: 后端与数据一致性

- naming 阈值校验改为 `naming_promote_threshold * decay_factor >= demote_threshold`，与实际衰减顺序一致；同步更新 schema 提示；
- 多账号画像去重排序纳入分组最高分，PG/SQLite 保持一致；
- 用户 scope 查询补齐 `session_id + turn_id`；跨会话 memo 键加入 platform、session type，并保护执行中的轮次不被容量清理；
- 用户列表纳入只有原始事实、尚未成簇的用户；关系图数据补齐 `superseded_at`；
- 补充配置、memo、画像排序、raw-only 用户和关系图字段回归测试。

### fix: WebUI 与可访问性

- 大图斥力改为空间网格近似，避免 stride 采样遗漏节点；SVG 卸载后停止残留 rAF；簇列表增加请求序号守卫；
- 弹窗增加焦点陷阱、背景 inert、关闭后焦点恢复；画像行、关系行、图节点和边支持键盘操作；图谱平移改用 `getScreenCTM().inverse()`；
- 修复簇修正保存时 status 被旧值回写、分类筛选缺少 preference/commitment、评分提示阈值写死、图标按钮缺少 aria-label 等问题；
- 图谱加载失败显示重试入口，补编码轮询首次基线与连续失败恢复，使用服务端 `Date` 消除 finished_at 时钟偏差，并显示边降级时间；
- 支持运行中动态切换 reduced-motion；修复卡片/横幅/弹窗多层背景语法和关系图命中线样式；
- raw-only 用户显示“未成簇”，用户列表标题改为“有记忆用户”。

Docstring Coverage 门禁在本地无对应配置/工具，无法复现；本轮新增公共接口未引入无意义文档占位，保留现状并记录待 CI 反馈。

## v1.18.7 (2026-11-05)

PR #9 CodeRabbit 第二轮评审（16 行内 + 2 follow-up + 4 nitpick）修复：后端 7 项、WebUI JS 9 项、
CSS 可访问性 2 项、测试补强 2 条 nitpick；2 条迁移类 nitpick 评估后不改（014 CONCURRENTLY 需改
迁移执行器且有 INVALID 索引风险，015 为部署期测量建议非代码缺陷）。

### fix: 后端（评审二轮）

- **清空 naming_promote_threshold 恢复派生**：可选字段空值此前按省略键保存，旧显式值永续；提交
  null 时派生值被写死、之后刻度变化不再跟随。现 `prepare_save` 对 null 持久化 null 本身，加载时
  重新从 score_start_high 懒派生（前端配合：default 为 null 的 float 字段空输入提交 null）；
- **新增装配校验 `naming_promote_threshold >= demote_threshold`**：低于降级线的 naming 簇晋档后
  会在下一次衰减遍被立即降级删除（画像反复抖动、称呼承诺静默丢失）。显式路径自报
  (naming_promote_threshold, demote_threshold)、派生路径自报 (score_start_high, demote_threshold)，
  回退必收敛；注意：现网已存在该组合的显式配置保存将被拒、启动期回退重置相关键；
- **rerank_candidates 缺省恢复 50**：候选池先 SQL 截断再近重复去重，20 条在近重复密集时去重后
  不足 top_k；config/main 兜底/schema/README 四处同步（惰性向量取回保证扩池不增传输开销）；
- **insert_persona_fact_raw 双后端持久化前钳制未来 occurred_at**（与工具写入路径同款不可信时间
  守卫），document_id/evidence_key 保留原始时间（幂等键不随钳制漂移）；
- **跨会话注入 memo 按轮次隔离**：键改 (session_id, turn_id)（宿主 event_id），同会话并发轮不再
  互相清空/覆盖排除集合；新增 TTL 清理 + 64 条硬上限防按轮次键无限增长；空 turn_id 保留旧会话级
  语义（旧宿主/测试兼容）；
- **多账号画像先去重后截断**：`fetch_profile_sections_multi`（PG/SQLite 对齐）改为 SQL 内先按
  (category, statement) 去重（非空/最新 occurred_at 优先）再 row_number 截断，跨账号相同
  statement 不再挤占 per_section_limit 名额。

### fix: WebUI（评审二轮）

- 弹窗关闭竞态：旧保存请求的 close 回调不再清空已替换的新弹窗（仅本弹窗仍挂载时清空）；
- **主导航 tab 改为 `<button>`（Major）**：Tab/Enter/Space 键盘可达，active 项 aria-current="page"，
  切换后焦点保持在当前 tab；CSS 补 UA reset 与 :focus-visible 焦点环，渲染盒模型不变；
- **按钮对比度达标 WCAG AA**：`.btn` 渐变端点局部加深（亮色 #0d6d9c→#6a4ed6 白字，暗色保留发光
  渐变翻转为墨色文字，与 active tab 星标同构），danger 变体同步修复，全部主题×端点组合 ≥4.5:1
  （全局 --sky/--violet 未动）；
- 原始事实/注入语料列表筛选与翻页请求加序号守卫，先发后至不再覆盖最新结果；
- 回填轮询连续 5 次失败放弃时恢复按钮可用并隐藏进度条，不再永久卡死；
- 注入语料页补编码周期结束自动刷新摘要列表（running→idle 跃迁 + finished_at 变化双通道检测），
  空闲时以 10s 续轮询覆盖后台自发的合并周期（切页/断连自动停止）；
- Kira 记忆迁入增加源路径版本守卫：改路径即清空旧预览并禁用「开始迁入」，迟到的扫描/迁入响应
  不得回写新路径的界面状态；
- 关系图谱首次状态轮询不再吞掉快照之后完成的关系回填（completedAfterSnapshot 判定，自终止无
  循环重载）；
- 可选 float 字段清空提交 null 恢复派生（见后端第一条）。

### test: 评审二轮 nitpick 补强

- test_recall_lazy_embeddings：启用 rerank stub 使 use_rerank=True、断言候选池 limit==20、stub
  按 limit 截断贴近真实后端；
- 新增 DM 跨会话注入 hook 端到端用例：启用 cross_session_summary_enabled，断言注入块存在且 memo
  登记与注入一致（此前测试不经过 hook，登记调用被删/调换不会被发现）。

全量 14 测试套件通过；web 改动以 node --check + 手工验收步骤覆盖。

## v1.18.6 (2026-11-04)

PR #9 CodeRabbit 评审轮（2 Major + 7 Minor）9 项修复：维护页控件类型、跨会话查询边界与排除范围、截断判定、naming 提示词口径、WebUI 四项。

### fix: 评审确认的缺陷（9 项）

- **维护页 Optional 字段控件误判**（Major）：`_field_type` 现解包 `Optional[X]`/`X | None`
  （含嵌套与 PEP 604）后按内层类型分类，`naming_promote_threshold`（`Optional[float]`）不再
  落入 "list" 兜底被渲染成多行文本——宿主无该键时表单回存提交 `[]` 必被 422 拒绝的问题随之
  消失；`_literal_options` 与 `_numeric_bounds` 的 int 判定同步解包。全 88 字段程序化审计
  仅此 1 个分类变化，无误伤；
- **未来时间戳摘要注入**：`fetch_recent_cross_session_summaries`（PG/SQLite 行为对齐）无条件加
  `occurred_at <= now()` 上界（与 max_age_days 是否启用无关），未来发生的摘要不再按 DESC 排最前
  占据 top_k 名额；
- **跨会话排除裸 session_id 误伤**：排除当前会话改用 platform + 会话类型复合条件（类型经
  group_id 判别：gm→`group_id <> ''`、dm→`group_id = ''`），同适配器下私聊与群聊裸 ID 相同时
  群聊摘要不再被误排除；调用链 main.py → memory_kernel → 双后端透传
  current_platform/current_session_type，未传类型的旧调用保留裸 ID 排除回退；
- **截断判定误杀完整摘要**：`_looks_truncated` 收紧为强信号（逗号族/开括号收尾、空锚点），多行
  摘要末字为 CJK 汉字不再判截断，语义完整但末行无句末标点的摘要不再被重试后降级原文单通道
  （`_CJK_IDEOGRAPH_RE` 死代码一并移除）；
- **naming 提示词规则自相矛盾**：分级 b 与示例第 2/3 条统一——接受迹象只认本人的实际应答，
  "未纠正/沉默"不再构成接受迹象，杜绝无应答的本人被提取为称呼偏好并晋档入画像；
- **图谱点击必清空选择**（Major）：setPointerCapture 把 pointerup 事件目标重定向到 SVG 自身，
  closest 命中判定失效——pointerdown 记录手势起始目标供 pointerup 使用；pointercancel 手势
  取消不再改动选中状态；
- **画像卡竞态与删除残留**：画像请求加单调序号丢弃过期响应，删除用户仅清空正展示该用户的卡片，
  删除后才返回的在途响应不再回填已删用户画像；
- **回填空闲分支只轮询一次**：空闲时每 5s 续轮询（切页/断连自动停止），其他标签页启动或完成的
  回填不再被本页漏检（与注释声称的等待行为一致）；
- **设置页整数项静默截断**：`parseInt` 会把 "1e3"/"1.5" 截成 1 后无提示热生效，现改 `Number` +
  严格整数校验，非法输入 toast 报错并中止保存（float 分支行为不变）。

全量 14 测试套件通过；web 四项无自动化覆盖（node --check + 手工路径推演验收）。

## v1.18.5 (2026-11-04)

存量中文注释/docstring 全面英文化（历史清理批）。

### docs: 存量注释英文化

- v1.18.4 评审批仅覆盖 1.18.0→1.18.3 增量行；本批清理此前全部存量：
  54 个文件、+3377/-2036 行
- 38 个 Python 文件 ~1740 中文注释/docstring 行全部译为英文；16 个
  SQL 迁移文件 106 行 `--` 注释译为英文（DDL 剥注释后与原版逐字节一致，
  迁移执行器无 checksum 校验）
- 「只动注释」由 token 级校验机械化兜底：全部改动文件的非 docstring
  字符串字面量与代码 token 流与改动前逐字节一致（日志/断言消息/UI
  文案/提示词/桩数据零改动）
- 9 处字符集数据样例有意保留：守卫/清洗 regex 的匹配对象本身（逗号族、
  开括号族、【话题】锚点、"查ID"、单字名"阿"/"好"、"未知"占位词）
- 术语与 v1.18.4 增量批统一（promote/profile/recall/rolling rollout
  fill-back/circuit breaker/dedup/idempotency key 等）；引用标记
  （nori #NN、PR、commit hash、迁移号、评审轮次）原样保留
- 全量 14 测试套件通过（纯注释变更，行为零变化）

## v1.18.4 (2026-11-03)

v1.18.0→v1.18.3 增量的自主评审批（缺陷/注释合规/测试精简/冗余/一致性五维）。

### fix: 评审确认的三项缺陷

- **跨会话条目 session_name 死字段**（D1）：DB 层从不返回该列，条目恒渲染
  「（未命名会话）」白耗预算——条目收敛为三行（session_id/时间/内容），
  移除死常量与死分支；
- **MemoryBackend ABC 契约缺口**（D3）：补 `fetch_recent_cross_session_
  summaries` / `fetch_embeddings_by_ids` 两条 @abstractmethod 声明（其余
  53 个 fetch_* 均在案，唯二漏声明；缺方法时被 fail-open 静默掩盖）；
- **naming 派生下界守卫**（D4）：`score_start_high<=0`（字段级合法）时派生
  0 绕过 `gt=0`、任何 naming 提取建簇即晋档——改为拒绝并要求显式设置
  （自报键 `("naming_promote_threshold","score_start_high")` 回退后必然
  收敛），新增 test_16d 双分支；
- **评审 D2 复核证伪**：懒向量首块失败续取在所有可达状态不可达
  （`lazy_embeddings = not is_per_user`；非 per-user 路径 keep_bound=top_k
  < chunk 且缺向量行必 kept）——原「每轮至多一次取回失败」不变式成立，
  成立依据固化进注释（不加死防御代码）。

### refactor: 冗余收敛（R1/R2）

- 细粒度相对时间标注下沉 `time_labels.relative_time_precise`（该模块自述
  使命即避免实现漂移），main.py 删除平行实现；
- `_cross_session_parse_ts` 删除，复用 `db.base.sqlite_parse_ts`
  （`db/__init__` 已再出口；naive 分支实际不可达，严格解析足够）。

### docs: 新增注释英文化（175 块）

v1.18.0→1.18.3 增量新增注释/docstring 违反工作区规则（All new code
comments must be written in English）——全部译为英文（web 62 + Python
113），术语统一（promote/profile/recall/rolling rollout fill-back/
circuit breaker/dedup 等），引用标记保留；有意保留 7 行字符集数据样例
（截断守卫的逗号族/开括号族/【话题】锚点字面量，即 regex 匹配对象本身）。
既有历史中文注释（范围外）不在本批处理。

### test: 精简与加固（B1-B5）

- B1：装配器预算用例动态构造——钉死「截断后放弃」「截断保留」两分支
  （原硬编码 260 与 120 同走 room<=0 分支，截断分支零覆盖），补中间态
  memo 契约（次条预算裁剪不登记）；
- B3：fail_open 补熔断计数断言（原断言对失败路径不敏感）；
- B4/B5：懒向量两用例合并、31b/31c 合并为 `test_31b_profile_fetch_sql_
  shape`（一次捕获三方法×两后端，断言零丢失并补齐 SQLite 形式）；
- B2：重复 `db.close()` 删除；test_16d 新增。

### docs: 注释与代码一致性修正（A2-A7）

「每轮两次调用」措辞修正、残量阈值注明含省略号、单行截断口径补开括号族、
migration 015 头注更新（014 已落地）、encode() 降级枚举补截断路径、
`_cfg_float` 注解 `float | None`、fetch_embeddings_by_ids 传输量估算随
rerank 缺省 50→20 更新。

## v1.18.3 (2026-11-03)

同步 nori 上游 #34：用户画像「近期动态」栏改时间优先排序——窗口首键
`COALESCE(cluster.occurred_at, updated_at) DESC`（occurred_at 为最近提及
时间，合并 GREATEST 推进），其余分栏保持 `score DESC -> updated_at DESC
-> id ASC` 旧序（CASE 首键恒 NULL 被跳过，确定性契约不变），早期高分
recent 条目不再挤占每栏 5 条限量、把新近动态顶出画像。随带：双后端
（PG/SQLite）单键与 multi 四处查询同构修改；`write_fact` 未来时间戳
钳制到 now 且幂等键（evidence_key / document_id 前缀）保留原始入参
日期（钳后 now 跨日漂移会击穿同日去重，上游评审二轮结论）；维护页
簇/事实筛选下拉与画像栏卡片补 preference/commitment；修正
test_31b owner-only SQL 断言取错位（原取 args 元组，恒真空转）。存量
数据核查：本部署 SQLite 库 1399 簇 future occurred_at = 0，无需迁移。

## v1.18.2 (2026-11-02)

同步 nori 上游（nori_plugin_noriflow_memory）四项：跨会话近期摘要注入（#31）、
称呼偏好低门槛入库（#32）、编码摘要半句截断防护（#26）、记忆工具准则单面注入
收敛（#29）；维护页换装「记忆星图 / Memory Atlas」改版（#32，去品牌化）。

### feat(cross-session): 跨会话近期摘要注入（同步 nori #31 + migration 014）

- 私聊回合前置当前用户在**其他会话**的近期已编码摘要（时间线置于滚动补回/
  召回之前）：补充"这个人最近在别处聊了什么"的背景，补齐跨会话记忆面；
  仅 `session_type != gm` 注入，`cross_session_summary_enabled` 缺省**关闭**；
- 新增 `migrations/014_cross_session_time_index.sql` + `migrations_sqlite/`
  同构版（部分索引 `idx_mcs_cross_session_time`，SQLite 支持部分索引、布尔
  列 0/1 真值语义一致）——编号 014 正是 1.18.1 迁移头注预留的坑位；
- 双后端新增 `fetch_recent_cross_session_summaries`：PG `participants &&`
  数组重叠 + `make_interval` 陈旧窗口；SQLite 走 `json_each` EXISTS 同语义、
  陈旧窗口按写侧 `sqlite_format_ts` 同构 TEXT 截止值比较；
- kernel 新增跨会话注入去重 memo（与滚动补回同构的 TTL/容量语义）：入口
  **无条件清空**本轮 memo（任何提前返回不残留上轮排除项——残留项会在 TTL
  窗口内错误排除 recall/检索结果）；共享检索排除通道升级为
  `_injected_excluded_ids = 滚动补回 ∪ 跨会话`，recall 与 memory_search
  同轮排除已注入行，防双通道重复暴露；
- **防重契约**：memo 只登记实际呈现给模型的行（前言计入整块预算、超限丢
  旧条、首条超限截断且正文残留 < 10 字符整条放弃）——被预算裁剪的行保持
  可检索，否则本轮既看不到又召不回，形成召回盲区；
- 身份归并：宿主暂无跨平台身份服务，按单复合键（platform:uid）回退（与
  nori 侧身份未命中时的回退同构）；会话名暂无目录服务，统一「（未命名
  会话）」；
- 新增四配置键（enabled/top_k/max_chars/max_age_days），config.py / main.py
  兜底 / schema.json / config_web / README 五处口径一致。

### feat(naming): 称呼偏好低门槛入库（同步 nori #32 + 96214f4）

- 新增 `naming_promote_threshold`（缺省 3.0，需 ≤ `score_start_high`）：
  naming 维度按专属低阈值晋档——本人明确称呼要求（"叫我X""别叫我Y"，
  confidence=high 起步分 3.0）**单次即可入画像**，不再需要 4-5 次证据累积；
- **建簇即时晋档**（nori 96214f4 同款，双后端）：`apply_fact_merge` create
  路径起步分已达维度阈值（recent/naming 各自低阈值，其余通用）时建簇即
  `profiled` 并**同事务写画像行**，不再依赖晋档遍——堵住同周期衰减遍先把
  分数打到阈值之下的时序窗口（run_cycle 顺序：事实处置 → 衰减 → 晋档）；
  replace 路径与晋档遍的维度阈值 CASE 同步加 naming 臂；
- 新增 `naming_prompt_enabled`（缺省开）：编码提示词附加
  `prompts/memory_encode_naming.prompt` 判定节——证据分级 a-f：仅记录本人
  希望被如何称呼/明确反感的称呼（a 档）；他人称呼习惯须有本人接受迹象才可
  提取且最高 medium（b 档，bot 行仅作语境）；自称/对 bot 的叫法/纯关系
  身份称谓（"主人""爸爸"类）/推断一律不提取；模板缺失自动降级跳过并告警；
- 维度阈值经 `apply_fact_merge` / `promote_pass` 全链路下渗（主动写入、
  合并处置、晋档遍三处调用点），PG 与 SQLite 行为逐字对齐；`naming_promote_threshold`
  纳入 `_promote_thresholds_within_cap` 装配期校验与 `CROSS_FIELD_VALIDATED_KEYS`
  （> `score_cap` 即拒——单次晋档与晋档遍两路都够不到阈值，称呼偏好会静默
  进不了画像）。**缺省派生**（评审修收：避免升级级联连坐）——不设/留空时从
  `score_start_high` 派生：「本人明确表态（high 证据起步分 = score_start_high）
  单次晋档」承诺在任何评分刻度下按构造成立（缺省刻度即 3.0），缩放刻度的既有
  部署不设新键时派生值天然合法，**不会**触发跨字段回退把合法的显式评分键连坐
  复位；显式设置时按本值生效（高于 score_start_high 属合法保守形态，退化为
  二次证据累积，不硬拒）；

### fix(encoder): 编码摘要半句截断防护（同步 nori #26）

- 上游 LLM 在 `response_format=json_object` 引导解码下会**间歇性提前终止**：
  摘要停在半句、JSON 由上游自行闭合、`finish_reason` 仍为 stop（无截断
  信号）——半句 JSON 合法，解析层无从察觉，照单写入即成为可召回的残缺
  记忆（该轮 facts 全丢）。Kira 侧同款 learner 链路同样暴露；
- 新增 `_looks_truncated`（反向白名单口径）：多行摘要末字为汉字/逗号族/
  开括号判截断；单行纯锚点形态合法，仅空锚点或逗号族收尾判截断；ASCII
  字母/数字/半角 ~/emoji/句末标点收尾一律不判（nori 生产 190 条疑似中旧
  白名单口径曾误杀 4 条完整摘要）；
- 疑似截断**重试一次**，仍截断则降级原文单通道（summarized=false 不参与
  召回，由合并 agent 补编码遍重编码）——半句摘要绝不作为完整记忆写入；
  encode 主流程重构为 `_encode_once`（渲染→调用→解析→装配）+ 重试外环，
  硬失败降级语义不变。

### chore(tools): 记忆工具准则单面注入收敛（同步 nori #29）

- `:tools` 准则块撤下逐工具路由复述（"memory_lookup 查…memory_write 仅
  在…"六件套 + memory_search 参数形态）——各工具 description 已是准确
  唯一口径，准则复述会随实现漂移（nori #29"幻影能力"教训）；准则保留
  跨工具行为守则（不重复检索已注入内容/不假装记得/不提工具名/跨会话时间
  与当事人/答应记住自然应下）并显式声明"以工具自身说明为准"；
- 新增锁定测试：准则块不得出现任何工具名（防回潮）。

### feat(web): 维护页换装「记忆星图 / Memory Atlas」（同步 nori #32 改版）

- 维护页从单文件朴素版换装 nori 侧星图改版（模块化 `memory.js` /
  `memory.css` / `ambient.js` + 星系视觉），API base 适配 Kira 路由
  （`/api/plugin/kira-ai-plugin-noriflow-memory/memory/...`），认证改为
  宿主同源 Cookie、主题跟随宿主 plugin-bridge `data-theme`；
- **去品牌化**：移除 nori 人像立绘/头像与全部品牌文案，拟人语气（喵~）
  泛化为中性文案，标题改为「记忆星图 · MEMORY ATLAS」；
- 原 Kira 专有功能块（Kira 记忆迁入 / 宿主库迁入 / 手动补编码，对应 6
  条端点）移植进新页设置区，无功能丢失；页面引用的 17 个唯一端点全部
  在 `@register.api` 注册表内核对。

### 核对结论（未改动项）

- nori #23（retain 写入门控——仅 bot 参与轮次写入）：Kira 架构性已满足
  ——`session_memory_updated` 仅由 IM finalize 阶段（bot 回复落库后）发射，
  未参与轮次永不触发编码；历史窗口仍含未参与消息作上下文，语义一致；
- nori #20（memory_search 关系边归并）：Kira 已包含——工具 description
  明确"查某成员时 fact 结果一并返回该成员的确切人际关系边"，无独立
  category=relation 维度；
- nori #30（计划任务履约观测）：平台依赖（订阅 nori 调度插件
  `task_triggered` 事件，Kira 无对应事件源），暂缓。

### 测试

- 新增 `tests/test_cross_session_naming.py`（9 用例）：PG/SQLite 查询形状
  与行为、memo 排除接线、装配器预算语义、naming 即时晋档/晋档遍维度臂、
  截断判定矩阵、重试-降级链路、判定节开关、准则无工具名锁定；
- `test_sqlite_backend.py`：迁移列表补 014；建簇即时晋档后晋档遍期望归零；
- 全量 14 套件本地全绿（Python 3.12，CI 同款逐脚本直跑）。

## v1.18.1 (2026-11-02)

### perf(recall): BM25 物化 tsv 生成列 + 去重向量按 id 惰性取回

- **BM25 物化（P1）**：新增 `migrations/015_bm25_materialize.sql`（仅 PostgreSQL，
  编号跳过 014 预留 nori-core 侧 `014_cross_session_time_index` 移植位）——
  `memory_chat_summary` 增加 stored 生成列 `tsv = to_tsvector('simple', search_text)`
  （to_tsvector(regconfig, text) 为 immutable，PG12+ 可用于生成列），BM25 腿的
  过滤与 `ts_rank` 排序均改读该列，表达式 GIN 索引 `idx_mcs_fts` 退役、由
  `GIN(tsv)`（`idx_mcs_tsv`）接管。实测 13,515 行语料：BM25 腿 ~160ms → ~30ms
  （原 ORDER BY `ts_rank(to_tsvector(...))` 对每条命中行重复分词 ~400 字正文，
  高选择率下 planner 弃 GIN 走 Parallel Seq Scan；拆分测量 GIN 过滤单独 24ms、
  预截断 token 后 2.4ms——~135ms 为重复分词）。回滚兼容：旧代码（表达式形态）
  在无 `idx_mcs_fts` 时退化为 Seq Scan，变慢但结果正确；SQLite 路径不涉及；
- **去重向量惰性取回（P2，按需分页）**：召回检索 SELECT 不再随行携带向量（asyncpg
  未注册 pgvector codec 时每行 ~18KB `'[...]'` 文本，两路 × rerank_candidates 约
  1.8MB 传输 + JSON 解析）——去重循环只为其**实际消费**的行按主键批量补取向量：
  首块 = `top_k * 4`（至少 16 行）启动前取回，循环越过已加载边界时按需续取下一块
  （被丢行不占 `top_k` 名额，长近重复段会把循环推过头块——窗外近重复必须继续比对
  去重，不得静默保留）。PG/SQLite 双后端新增 `fetch_embeddings_by_ids`（PG 走
  `id = ANY($1)` 精确批量，SQLite 走 `IN` 占位符批查，均跳过 NULL embedding 行）；
  健康（早停）路径仍只付一次批量取回。取回失败 fail-open（经共享 `_guarded_call`，
  失败计入熔断），缺向量行沿用"不比对、永不误删"语义；旧版 DB 实现/测试桩无该
  方法时自动保持旧行为（检索行自带向量），跨版本兼容；
- **`rerank_candidates` 缺省 50→20**：`config.py` 模型缺省、`main.py` 宿主配置
  兜底、`schema.json` 维护页展示与 README 配置表四处口径一致。已显式配置该键的
  部署不受影响（宿主配置优先于模型缺省），仅新装/缺键部署生效。
- **评审收尾（neko 建议，PR #19）**：
  - BM25 腿迁移前兜底——`tsv` 列缺失（schema 被手动回滚等 migration 015 未生效
    场景）时精确捕获 `asyncpg.UndefinedColumnError` 并**闩锁**回退 to_tsvector
    表达式形态（结果正确、仅变慢；重启重跑迁移后恢复），不再让 `UndefinedColumn`
    打挂整条召回与熔断；闩锁避免每轮召回都付一次失败查询，健康路径零额外开销；
  - 去重向量惰性取回纳入熔断口径——`fetch_embeddings_by_ids` 改经共享
    `_guarded_call` 调用（失败计入熔断、与检索腿同口径）。熔断按**连续失败**
    计数：检索腿每轮成功即清零计数，惰性取回单独失败最多累计 1 次、不会独自
    打开熔断（只有检索腿自身连续失败——真库不可用——才会熔断短路召回，属预期
    行为）；单轮取回失败 fail-open 退回"缺向量不比对"语义，对外行为不变。
  - 评审测试桩修正——`_NoTsvConn` 按 SQL 内容分流向量腿/表达式 BM25 腿/tsv
    探测（原实现不区分，向量腿被喂入 BM25 桩行、融合断言空转通过）；断言
    加强为向量腿空结果下命中行只能来自表达式回退腿。
  - 去重窗口缺口（neko 第二轮 review，重要）——原惰性实现只为头部窗
    （`top_k*4`，至少 16 行）取向量，而被丢行不占 `top_k` 名额、去重循环可越过
    窗口：窗外近重复因无向量被静默保留（相对旧全量比对行为是质量回归）。改为
    按循环实际消费**分页补取**（新增 `_load_dedup_vectors`：首块启动前取，
    越界即续取下一块并继续比对），窗外照常去重；健康（早停）路径仍只付一次
    批量取回，最坏情况（候选池全重复）续取次数以池大小为界，仍远小于旧行为的
    全量向量传输 + JSON 解析。
  - 分页熔断累计（neko 第五轮 review，成立）——分页续取使"单轮最多 1 次取回
    失败"的前提失效：同轮多块连续失败（中间无检索腿成功清零）可在单轮内累到
    `failure_threshold`（缺省 5）打开共享熔断，下一轮召回被整体短路为空。
    修复：任一分页块取回失败即停止本轮后续分页（`dedup_fetch_dead` 闩锁，
    `_load_dedup_vectors` 返回成败），单轮计入熔断的取回失败恒 ≤1 次；未取到
    的行保持"缺向量不比对"语义，且不再重复无效探测（每轮一次告警）。
  - 用例注册遗漏（neko 第五轮 review，成立）——`test_recall.py` 的 `main()`
    漏注册 `test_bm25_tsv_missing_falls_back_and_latches`，直跑方式下零执行
    （重蹈上文 runner 空转覆辙）；注册后真实运行立即揪出测试桩路由错误——
    两腿 SELECT 列都含余弦距离表达式 `embedding <=>`，原按该片段分流会把
    BM25 查询误判为向量腿，改按 FTS 算子判别（先判表达式形态、再判 tsv
    探测）后用例方才真实通过。
  - `tests/test_recall_lazy_embeddings.py` 补上缺失的 `__main__` runner——该文件
    是唯一没有 runner 的测试套件，CI 逐脚本直跑方式下实际零执行（退出码恒 0），
    此前"全绿"对该文件为空转；补齐后惰性/兼容/迁移守卫用例首次真正运行并通过，
    分页续取用例即在其中。
  - **migration 015 锁与重写评估（neko 第三轮 review，结论：无代码改动）**：
    `ADD COLUMN ... GENERATED ALWAYS ... STORED` 确会一次性整表重写并持
    ACCESS EXCLUSIVE 锁（13.5k 行毫秒级；数百万行历史表会拉长启动窗口），
    但：执行器按脚本**单事务**包裹（`apply_migrations`），DROP/CREATE 索引
    原子、中途失败全回滚，不存在"无 FTS 索引窗口"；语句受池级
    `db_command_timeout`（缺省 60s）约束，超时干净回滚、记忆功能关闭并在
    下次启动重试，不会半途无限持锁。`CREATE INDEX CONCURRENTLY` 无法在
    事务内运行（需改造执行器），"普通列 + 触发器 + 分批回填"会引入永久写
    放大（写路径性能回退），均不采纳。大表部署可在升级前**手工预执行**本
    迁移（全幂等：三处 `IF NOT EXISTS`；旧代码不读 `tsv`，仅 BM25 腿短暂
    退化为 Seq Scan，见迁移头注），避开启动重写窗口。

### feat(config): 画像评分缺省值对齐长周期陪伴场景（promote 10→6 / demote 3→2 / decay 0.8→0.9）

- 缺省值改动：`promote_threshold` 10→6、`demote_threshold` 3→2、`decay_factor` 0.8→0.9
  （`config.py` 模型缺省、`main.py` 宿主配置兜底、`schema.json` 维护页展示三处口径一致）；
- 依据（线上 seki 平台画像数据）：晋升阈值原与 `score_cap`（10）相等，簇需 4-5 次独立
  确认才能入画像——实测 1390 个簇中仅 59 个（4.3%）达到 3 次确认，26 个 3-5 次确认的
  高质量事实（人设/偏好/互动边界）长期卡在 6-10 分区间进不了画像；降为 6 后这批事实
  可直接晋档（两次独立 high 抽取或三次 medium 抽取即达线）；
- 迟滞带同步收窄但仍留有余量：`promote - demote = 4 > score_start_high(3)`，保证降档簇
  （分数 < demote）一条 high 证据无法回档、需 2 条，避免"降级→单条证据→再晋档"翻转；
- **模型级强制校验**：新增 `model_validator(mode="after")`
  `_hysteresis_band_guards_repromote`——当 `promote_threshold - demote_threshold` 小于
  单条证据最大加分 `max(score_start_high, score_start_medium)` 时直接抛错，非法组合在
  配置装配期即炸出（维护页保存走同一模型，非法值返回 422 并带原因），不再依赖
  description 文字约束；同时给 `demote_threshold` 补 `gt=0`（0 会让画像永不掉出）。
  判定阈值取 **小于** 而非"不大于"：降档判定是严格 `score < demote_threshold`，降档簇
  分数上界为 `demote - ε`，故 band 恰等于单条证据加分时仍安全（`demote-ε+加分 <
  promote`），band 2 才真正允许一条证据回档（已核 5/3 → 5.999 ≥ 5 可回档）。
  校验按配置的证据加分换算而非写死 3 分，评分刻度整体缩放的部署同样成立；推论：
  `demote_threshold > 0` + band ≥ 加分 ⇒ `promote_threshold` 必然大于单条证据加分
  （缺省 6 > 3），低于该值的部署在装配期被拒并回退安全缺省值；
- `decay_factor` 0.8→0.9：半衰期 = `ln(0.5)/ln(factor)` 个衰减周期——按仓库缺省周期
  `decay_interval_days=14` 折算，0.8 约 3.11 周期（43 天 / 1.4 个月）、0.9 约 6.58 周期
  （92 天 / 3.1 个月）；按线上部署的 30 天周期折算则分别为 93 天（3.1 个月）与
  197 天（6.6 个月）。线上实测（30 天周期、0.8）：仅跑过 1 次衰减就有 27/51 已晋档事实
  跌破晋升线、15 个进入降档危险区；改 0.9 后显著放缓自然遗忘；
- **跨字段校验失败的降级范围收窄**：跨字段校验
  （model_validator）的 pydantic 错误 loc 为空，原逐项回退定位不到键会退化为
  **整份回退默认值**（用户其余配置一并被清）；且跨字段键表若按并集回退，任一组
  非法会把另一组**合法的显式配置**也清掉。现校验器经 `CrossFieldConfigError`
  **自报管辖键**（pydantic 把原始异常放入 `errors()[i]["ctx"]["error"]`），
  兜底层据此**只回退真正非法的那组**（如池边界非法不牵连显式调好的画像阈值），
  其余用户配置保留、告警点名被回退的键；pydantic 对多个 model_validator 只报
  第一个失败，两组同时非法时逐轮回退（键集单调收缩必然终止）；自报键缺失时
  防御性退回 `CROSS_FIELD_VALIDATED_KEYS` 整表回退；
- **起步分非负 + 倒挂显式守卫**：`score_start_high` /
  `score_start_medium` 补 `ge=0`——负起步分会让迟滞带校验的"单条证据最大加分"
  变负、校验形同虚设，可配出 `promote=1 / demote=5` 的倒挂组合（迟滞带语义失效，
  低分晋档高分降档互相矛盾）；同时 band 校验器前置显式 `promote > demote` 守卫，
  关掉 `ge=0` 下仅剩的 `promote == demote` 零迟滞漏网组合。另修测试断言歧义：
  `test_09/10/12` 原断言 `pool_max == 8`（入参值），但分组回退是整组复位、
  `pool_max` 应取缺省——原入参 8 恰与缺省 8 相同而"碰巧"通过，现改入参为 6
  （≠ 缺省）使断言无歧义；
- **晋升阈值不得超过分数封顶**：新增
  `_promote_thresholds_within_cap`——`promote_threshold` / `recent_promote_threshold`
  大于 `score_cap` 时装配期抛错（相等允许：分数经 `min(cap, ...)` 可达封顶，
  旧缺省 promote=10==cap=10 即此形态；浮点比较带 1e-9 容差）。此前该组合可从
  维护页直接配出且无任何拦截——簇分数被 cap 封顶后永远达不到阈值，**晋升被
  静默关闭**；跨字段回退只复位 band 组、遗留小刻度 score_cap 的级联场景同样
  中招（回退后 promote 复位 6.0 而 cap 留 1.5）。回退组联动：该校验器自报
  键 (score_cap, promote_threshold, recent_promote_threshold)，band 组复位后
  若与遗留 cap 组合非法，兜底循环第二轮自动复位 cap 组，终态刻度自洽；
- **迟滞带比较带浮点容差 + 兜底告警保留异常堆栈**：
  band 比较改为 `band + 1e-9 < max_inc`——二进制浮点减法有表示误差（实测
  `6.1 - 3.1 == 2.9999999999999996`），数学上 band 恰等于加分（相等亦安全）的
  组合不得因表示误差被误拒（维护页 422 / 启动回退均受影响）；真非法组合
  （band=2.0 远超容差）仍被拒。`_try_build_config` 终端告警的 `exc_info` 显式
  传捕获的异常——重写后告警位于 except 块外，`sys.exc_info()` 已清空（轮次
  耗尽路径更无活动异常），不带会丢掉定位配置构造失败所需的堆栈；
- 破坏性提示：已显式写入宿主配置且**迟滞带组合合法**的部署（如旧缺省 10/3、
  6/2）不受影响，宿主配置优先于模型缺省；显式配置组合非法（band < 单条证据最大
  加分，如 5/3）的部署在装配期被拒——维护页保存时 422 拒绝、启动装配期仅回退
  涉及的评分键（其余配置保留）并打告警；仅新装或配置缺键的部署采用新缺省；
  降档阈值下移后画像保留期变长（低分事实清理节奏放缓），且 recent 维度的过期
  降档不清零分数，其单证据回档路径与本组阈值无关。

### docs: 文档与配置 schema 对齐 v1.18.0（无功能变更）

- README 配置表补齐 v1.18.0 三个键：`retain_interval_group` / `retain_interval_private`
  （攒批触发间隔，1=逐轮）与 `flush_idle_s`（空闲 flush 延迟 + 巡检周期换算 +
  `memory_retain_pending` 恢复备份），升级者据此可知私聊写入延迟默认由逐轮变为 5 轮攒批；
- README「本地测试」补 `tests/test_memory_retain_batch.py`（v1.18.0 主交付 95 例），
  `test_memory_tools_sqlite.py` 的口径改为中性的「记忆工具」，避免与正文档「六件」冲突；
- README 适用平台改回 `KiraAI >= 3.0.0rc1`（与 manifest `core_version` 一致，
  `>=3.0.0` 会拒绝 rc.1 宿主）；依赖说明补条件依赖 `tomli`（Python < 3.11）与 `tzdata`（Windows）；
- `schema.json` 补 `recall_time_label_mode`（string / relative|absolute|both，默认 both）——
  此前仅插件维护页可编辑，宿主插件配置页缺失该项；
- CHANGELOG v1.18.0 尾注「SQLite 迁移脚本 112/112」改为「SQLite 后端套件 112 checks 全绿」
  （112 为 `tests/test_sqlite_backend.py` 的 check 计数，非迁移脚本数）。

## v1.18.0 (2026-10-20)

### feat: 私聊 retain 攒批合并编码 + 注入/画像耗时观测

同步 nori 仓 PR #25（feat(memory): 私聊 retain 攒批合并编码 + 注入/画像耗时观测下沉），
按 KiraAI 插件架构适配实现（nori 原实现基于 nori_core 对象模型与仅 PG 部署假设，
kira 侧按「回合完成信号 + 滚动行缓存水位线 + PG/SQLite 双后端」重写）：

- **攒批门控**：新增 `retain_interval_group`（默认 1=群聊逐轮，行为零变化）/
  `retain_interval_private`（默认 5）/ `flush_idle_s`（默认 1800）三配置键，
  运行时活读、热切换即时生效；
- **批次合并**：私聊 N 轮合并为单批次编码——合并批次 = 各轮批次行按时间序
  拼接，历史窗口取攒批前的快照（等价 nori 的 exclude_ids 回退机制，
  保证攒批轮次不进历史窗口）；
- **双持久化**：内存 pending 为主真源，`memory_retain_pending` 表（迁移 013，
  PG/SQLite 双方言）为重启恢复备份——每轮 UPSERT「该会话全部未编码数据」的
  合并快照（含编码在途批次），崩溃恢复不丢在途内容；
- **失败回座**：编码失败不丢批次（合并回内存 + 重写表行），待下一触发点重试；
  攒批路径不回滚水位线（真源已移交攒批缓存/表行，逐轮路径的回滚语义不变）；
- **重启恢复与空闲巡检**：读表回灌轮数延续（回灌不提前编码，续攒到间隔才落库）；
  巡检协程按 `max(5s, min(300s, flush_idle_s))` 周期对超龄批次强制编码，防末批滞留；
- **生命周期联动**：会话逐出（LRU）时调度该会话攒批编码，不滞留；停机
  （terminate）强制编码未满批；巡检协程挂插件 start/stop 配对；
- **数据一致性收口**：同一会话的回合在别名 I/O 前冻结快照、合并按内容
  锚点定序（并发信号不再乱序/重复进编码输入）；编码成功后删行失败交由
  巡检重试；PG JSONB 的 pending 快照写入前清理 U+0000（恢复副本丢弃该
  字符，内存编码仍用原文）；
- **用户擦除联动**：WebUI 删除用户记忆前先脱敏/清除该用户的 pending 与
  在途批次并取消编码任务（群聊保留其他成员的行），同时重写/删除
  `memory_retain_pending` 备份行，避免删除后旧内容被重新编码；
- **后端迁移**：`migrate_backend.py` 一并迁移 `memory_retain_pending`
  快照（JSONB → JSON TEXT），同会话快照冲突时拒绝覆盖（即使 `--force`）；
- **耗时观测**：`build_injection_text` / `build_profile_text` 增加计时包装
  （DEBUG 落盘；超过 500ms 升级 WARNING 作慢查询哨兵，与上游 nori 口径一致）。

测试：新增 `tests/test_memory_retain_batch.py`（95 例：门控等价性/批次内容
正确性/表行生命周期/失败回座/水位线语义/恢复续攒/巡检/逐出联动/停机收口/
SQLite 真实往返 + 并发/乱序定序/用户擦除/脏数据防御回归）；全量 485 passed
（基线 390 + 新增 95），SQLite 后端套件 112 checks 全绿。

## v1.17.3 (2026-10-14)

### fix: 全仓修复批——数据损坏防御 / LLM 契约 / 周期健壮性 / 向量召回 / 一致性 + 键公式单源化

全仓分阶段修复批，测试 371 → 383 全绿：

- **数据损坏**：`json_utils` 正则修复改为分阶段——先直接 `json.loads`
  透传合法 JSON（含 `, }`、`: True` 等诱饵序列的字符串值原样保留），失败才
  正则修复重试，json_repair 兜底；
- **LLM 契约**：簇裁决与边审计部分覆盖时不再把未裁决项当
  无关（杜绝重复簇 create）、水位不推进过未覆盖边；`true == 1` 布尔 ID
  拒绝（adjudicate 与边审计双处）；
- **周期健壮性**：合并周期六个 DB 阶段各自 try/except 喂
  熔断 + 跳过续跑，`_save_skip_state` 进 finally；decay/lifecycle 时间戳
  kv 写失败不中断不双重衰减；重编码预留探针尊重毒丸跳过名单；
- **向量与召回**：近重去重维度不匹配跳过比对（换
  embedding 模型后不再假命中丢行）；bot 排除双形态覆盖；召回 token 预算
  计入时间标签；滚动补回改两阶段 memo（build 暂存、prompt 装配成功才
  commit，宿主丢弃装配时不再双通道不可见）；写侧去重命中带告警；
- **一致性**：WebUI 摘要编辑的 SQLite FTS5 影子表同步并入同事务；
  `search_fact_clusters` 全空白 user_ids 双后端统一 fail-closed（杜绝
  跨用户暴露）；
- **main.py**：enabled_tools None 守卫；
  bot_platform 幽灵属性接线（捕获→传播→消费三点链路）；LRU 兜底取最近
  活跃会话；匿名消息键改单调计数器（地址复用不再静默丢失）；记忆删除
  DB 故障与不存在三态分文案；变量遮蔽与 config 端点防御式解引用；
- **单源化收敛**：幂等键 `summary_document_id`/`evidence_key`、画像投影
  SQL 模板、LIKE 转义、`resolve_bot_forms` 提为共享 helper，全仓内联
  拷贝清零（字节级兼容由原锁定测试背书）；
- **杂项**：SQLite 连接池关闭竞态、单字 CJK token 保留、FTS 空白行排除
  出补算窗口、`_ilike` 未导入 NameError（PG 后端关键词搜索必崩）随 LIKE
  去重顺带修复、recall 日志自动建目录、死代码清理（`_drain_reinforcement`
  等）、docstring 幽灵队列/单后端声明等 11 处文档失真修正；
- **测试**：测试基建幂等化（同进程双模块实例致 assertRaises 断裂），
  新增 12 个锁定测试（含独立复核补齐的维度跳过 / rollout no-commit /
  fail-closed 空白 user_ids）。

### feat: fact 检索附带成员关系边

- 移除 `memory_search` 的 `category=relation` 过滤维度，关系边随 `target=fact`
  按成员自动返回，并以【事实】/【关系】分节呈现；
- 复用现有关系行渲染、边时间限定、复合键查询与规范名查询能力；关系查询、别名
  查询和单边渲染均按既有契约降级，事实查询数据库不可用时返回统一暂不可用提示；
- 保持事实行格式与版本号不变，补充工具 guidance、schema 和回归覆盖。
- 关系查询故障显式提示“关系边暂不可查询”，保留已返回事实；本人跨平台事实检索
  附加关系查询的会话平台限定，避免将查询失败或范围限制误解为没有关系。

## v1.17.2 (2026-10-13)

### fix: 迁入预览改为按需触发（消除切设置栏的全量加载）

- 维护页两张迁入卡片（文件源 / 宿主数据库）不再在渲染时自动扫描——此前
  每次切到设置栏 tab 都会触发一次文件源全量读取与一次宿主会话全量加载
  （`ctx.session_mgr.get_sessions(include_content=True)` = 全表 + 全正文），
  对长期运行、会话持续增长的部署造成无谓延迟与内存峰值；
- 扫描与迁入改为用户显式点击触发的一次性动作；宿主库「从宿主数据库迁入」
  按钮初始即可点（导入接口自带加载、不依赖预览，省去一次无谓扫描）；
- 导入完成后不再自动重扫，改用本次返回 stats 就地更新提示，使每次操作
  仅发生一次全量加载；
- 仅改插件侧 `web/index.html`；迁入 / 预览 API 行为不变。

## v1.17.1 (2026-10-12)

### fix: KiraAI v3.0.0 兼容（配置落盘 / 存量迁入 / 声明校准）

对齐 KiraAI v3.0.0-rc.1：

- **配置落盘修复**：维护页设置栏保存延迟 import 的 `PLUGIN_CONFIG_DIR`
  由已删除的 `core.plugin.plugin_registry` 改到 `core.plugin.manager`——
  此前该 import 抛错被兜底 except 吞掉，配置只热生效、不落盘，宿主重启
  回退旧值；
- **存量迁入新增宿主数据库源**：v3 起 KiraAI 会话入库（`chat_sessions`）、
  不再更新 `data/memory/chat_memory.json`，文件式迁入无法覆盖升级后新
  产生的记忆；新增「宿主数据库迁入」（经 `ctx.session_mgr.get_sessions`
  读取，复用同一 chunk→summary 管线与幂等键，仅迁会话摘要），维护页新增
  对应卡片；文件迁入保留为 v2 遗留存量通道。内部抽取
  `build_chat_summary_rows` 供两源共用（消除重复的 chunk 组装逻辑）；
- 两种迁入来源独立保存最近迁入记录，预览按来源读取；文件迁入覆盖旧共享
  标记前保留其中的宿主库历史，预览读取不写入数据库；
- **core_version 声明**：`>=2.31.0` → `>=3.0.0rc1`（`>=3.0.0` 会拒绝当前
  rc.1 预发布版本）；
- 宿主 `plugin_configs` 内存同步加存在性防御：宿主结构变化时降级为仅
  落盘 + 告警，而非抛 AttributeError 中断保存。

### docs: README 口径修正 + 同源描述同步

- mermaid「LLM 记忆工具」节点与功能简介补齐六件套口径
  （search/write/remove + profile/lookup/correct，此前分别只列 3/5 件）；
- `tool_scope_locked` 配置行「慎开」→「慎关」（风险在关闭侧：关闭才
  解锁显式参数跨会话/跨用户语义），并按实现改写为全工具准确描述——
  search/write 忽略显式 session_id/user_id（write 另含 platform）、
  remove 无作用域入参删除限定触发作用域（无法推导时拒绝）、
  profile/lookup/correct 与指名检索恒钉死触发者不受开关影响；
  schema.json WebUI hint 同步（含「全部记忆工具忽略」过度表述修正）；
- 双后端方案指引由本仓不存在的 `docs/plans/` 改指本仓 CHANGELOG；
- 本地测试清单补 test_time_label.py 与 test_fix_batch_20260912.py
  两行；桩基建描述改双文件口径（_harness.py + plugin_env.py：主套件
  与回归批依赖后者，真库/真文件套件自带桩）——_harness.py 头部
  「主套件自包含」过时注释同步修正。

## v1.17.0 (2026-10-07, main 分支)

### feat: 画像 8 维度扩展对齐 nori（preference/commitment + 生命周期）

补齐本版本工具对齐时遗留的画像呈现面与编码面——此前 memory_write
可写 preference/commitment，但画像渲染六栏静默丢弃这两类行、编码器
值域也不含它们（memory_profile 工具描述的 8 项口径与实现不符）：

- **画像栏位**：persona_service `_SECTION_ORDER` 扩为 8 维——
  preference（喜好偏好，序列化并入 established_notes）与 commitment
  （约定承诺，并入 memory_points）插于已知事实与互动偏好之间；
  contracts.PersonProfile 补 memory_points 字段；
- **「其他名称」栏**（对齐 nori）：alias 层历史名变体（last_seen
  降序，最多 5 个），置于基本信息之后；排除当前称呼（块抬头名先取
  再拼装）；无变体整栏省略（不写「暂无」占位）；db 双后端新增
  fetch_alias_variants（占位名守卫过滤，PG ANY / sqlite IN 同构）；
- **commitment 生命周期**：decay_pass 新增 commitment 维度过期分支
  ——起算点 COALESCE(last_evidence_at, occurred_at)（簇合并推进最近
  确认时间，持续被提起的约定持续续期；承诺日期在陈述文本内非结构化
  列），超窗（commitment_expire_days，默认 60 天）降 pending_uncertain
  出画像（簇体保留可复活）；与 recent 同为日历语义不受缺席冻结门控；
  统计键新增 expired_commitment（双后端同构）；
- **编码器**：category 值域六选一 → 八选一（prompt 定义 +
  _VALID_CATEGORIES + 结构化输出 enum 三处同步），编码链路开始产出
  preference/commitment 事实；
- **配置/维护页**：commitment_expire_days 配置项（schema.json +
  config_web 评分状态机分组 + _build_config 接线，反射测试锁定）；
- **测试**：test_noriflow 新增八栏渲染 / 其他名称排除当前称呼 / 栏
  序用例；test_sqlite_backend 新增 commitment 过期真库回归（降级出
  画像 + 簇体存活）+ migrations_applied 断言补 012（存量遗漏）；
  10 套 direct-run 全绿。

### feat(tools+lifecycle): 对齐 nori 侧工具五件套 + 摘要生命周期（归档/强化）

**主动工具对齐**（语义与 nori 侧 v0.10.8 同源，handler 形态适配 KiraAI）：

- `memory_search` 升级双目标：summary=对话摘要（原管线不变）+ **fact=
  事实簇语义检索**（结论级，簇表 cosine + 维度过滤 + name 归属收窄）；
  `category=relation` 按成员名字查关系边（复用 fetch_active_edges）。
- `memory_write` 语义升级为**确定性事实直写**（对齐 nori）：raw 插入后
  立即 `apply_fact_merge` 成簇（create/replace），不等合并 agent 周期；
  六维 category + confidence + `replaces_cluster_id` 原子替换；幂等闭环
  （document_id 粒度 + 乐观锁 skipped）；**replace 目标归属校验**
  （fetch_cluster_owner，跨用户替代拒绝——nori PR#4 安全修复同款）。
  旧「写摘要行」语义由 memory_remove/维护页承担。
- 新增 `memory_profile`：按名字查成员画像（画像块外兜底；实体命中
  match 机制解析名字，重名歧义返回候选不猜）。
- 新增 `memory_lookup`：维护性深查——归档摘要（include_archived）+
  失效/被替代/待定簇（include_inactive），带状态标注供恢复定位。
- 新增 `memory_correct`：记忆状态调整（簇 drop/dispute/reactivate、
  摘要 reactivate），reason 必填审计留痕；归属不符与不存在同款输出
  （不泄露他人簇存在性）；replaced 簇指向继任簇给精确提示。
- **记忆工具准则注入**（`:tools` prompt 段，对齐 nori guidance）：防
  播报措辞（查到的记忆当亲历自然叙述 / 答应记住自然应下）+ 工具分工；
  本轮至少一件记忆工具可用时注入。
- 门控：`enabled_tools` 管六件全集（search/write/remove + 新三件）；
  `memory_tools_enabled`（默认开）总开关管新三件；白名单/作用域锁定
  机制全量适用（search fact 通道同样钉死触发者作用域）。

**db 层新增**（base 抽象 + postgres/sqlite 双实现，PG 版自 nori 平移）：

`upsert_persona_fact_raw_for_apply`（DO UPDATE RETURNING id）、
`search_fact_clusters`（sqlite 版复刻 FTS 检索的 Python cosine 模式）、
`fetch_cluster_owner`、`cluster_status_op`（drop 清投影/dispute 打标/
reactivate 白名单）、`restore_summary`（会话归属校验）、
`archive_stale_summaries`、`reinforce_summaries`；kernel 新增
`write_fact`（直写簇路径）与 `_fire_reinforcement`（召回命中强化
fire-and-forget）。

**摘要生命周期**（对齐 nori 011 迁移 + v0.10.7 机制，默认关）：

- 迁移 011（sqlite 方言，版本序列与 PG 共享）/ 011（PG）：`archived/archived_at/last_recall_at/
  recall_count` 四列 + partial index；检索 SQL 结构性排除归档行
  （双后端同款，含兜底 where 分支）。
- 归档遍（挂合并 agent 周期，kv 门控）：超龄（最低保留期 + 3×半衰期）
  且强化窗口内无召回命中 → archived；keyset 分页 + 两段式复查（候选
  数判满批 / UPDATE 时复查判据收敛召回并发竞态）；**首遍延后至强化
  窗口后**（存量行强化预积累，防升级瞬间误归档高频记忆）。
- 召回访问强化：最终注入集异步刷新 last_recall_at/recall_count
  （`summary_lifecycle_reinforce_on_recall` 默认开，不受总开关门控——
  预累积信号防首遍误判）。
- 配置组 `summary_lifecycle_*` 六项 + 维护页 schema「摘要生命周期」分组。

**测试**：新增 `tests/test_memory_tools_sqlite.py`（真 SQLite 集成
28 checks：直写链/状态机三动作与归属校验/簇检索过滤/lifecycle 判据
边界/keyset 分批/检索排除归档）；`test_noriflow_memory.py` 的 write
用例适配新语义；`migrations_applied` 断言更新 [1, 2]；9 套 direct-run
全绿。

### feat(recall): 混合分层时间标注——recall_time_label_mode 形态开关（PR #4）

对齐 nori 侧 PR#7（feat/recall-time-label-and-review-fixes）：

- 三态形态开关 `recall_time_label_mode`（relative / absolute / both，
  默认 both）；absolute 分层精度——7 天内带时分（近事可辨批内时序），
  更久只到日期，跨年带年份
- 将来时间戳守卫按完整时刻比较（同日内未来时刻一并拦截）
- 跨年近事（days<7 且跨年）绝对部分带年份，不被时分短路丢掉
- Literal 白名单装配期拒绝非法值；热切换直赋未知值按 relative 兜底
  落告警（`_relative_time_label` 按注入记忆逐条执行，模块级按值去重
  防日志洪泛）
- 新增 tests/test_time_label.py 10 用例；既有标注断言经 _make_kernel
  显式 relative 锁旧形态

### feat: 实体关系边复合键口径补齐（PR #5）

- entity_edge.py 三函数 bot 判定复合键化（形态×session_platform）：
  select_relation_edges（bot 端点误判/场景 C 锚）、
  relation_statement_line（渲染层裸 uid 撞号把真人渲染成 bot 昵称）、
  neighbor_profile_uids（裸 uid 排除漏注入真人画像）；
  session_platform 必填 kwarg，漏传 TypeError 不静默退回裸判定
- db 层：fetch_alias_names_by_owner owners 三态契约（None=全表原
  语义；空列表/过滤后空=短路返回 {} 不落全表）；fetch_active_edges
  非空字符串入参 fail fast（空串保持旧容忍）
- migrations/ + migrations_sqlite/ 012：复合键读路径表达式部分索引
  （postgres ANY 下推验证；sqlite EXPLAIN QUERY PLAN USING INDEX 验证）
- 测试：跨平台 bot 撞号三面用例（select/statement_line/neighbor）、
  owners 三态 db 层契约用例、fail fast 用例

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
- kernel.search 的 embed_task 守卫（检索中止路径取消飞行中的
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

### refactor: 测试套件去重合并 + 死代码清理

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

### fix: SQLite 后端三处修复（promote 画像列污染 / decay 参与者门控 / BM25 欠填）

两处真 bug + 一处与 PG 的行为差异，共用同一根因：raw sqlite3.Row 的 JSON TEXT 列被当作 Python 数组消费。

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

### feat: KiraOS_Plugin 记忆数据无损迁入

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
个人部署不再需要维护 PostgreSQL + pgvector 实例。

- **配置**：新增 `storage_backend`（postgres | sqlite | auto；auto=配置了
  dsn 用 postgres，否则 sqlite——现有部署行为零变化）与 `sqlite_path`
  （空 = 插件数据目录/memory.sqlite3）。均为启动期只读，运行中不可切换
  （切后端 = 换库，跨后端迁移走迁移工具，本期不做在线互转）。
- **db 拆包（零行为变化）**：`db.py` → `db/` 包（base 共享纯函数 +
  MemoryBackend 接口契约 / postgres 原样迁入 / sqlite 新实现 / factory
  工厂 / _sqlite_pool 连接 shim）；`db/__init__` 重导出原名，全部上层
  调用方 `from .db import X` 零改动。
- **SQLite 后端**：
  - `migrations_sqlite/001_init.sql` 合并式初始化（等价 PG 001-010 终态，
    版本序列两侧共享，后续 schema 变更成对新增）；
  - 类型映射：TIMESTAMPTZ→固定格式 UTC TEXT（字典序=时间序）、TEXT[]→
    JSON TEXT（json_each 查询）、vector→float32 BLOB（维度不匹配按缺失
    处理，换 embedding 模型无需重建表）；
  - 向量检索不引入 sqlite-vec：SQL 圈定行集 + Python/numpy 暴力余弦 +
    FTS5 影子表 BM25 路，`_rrf_fuse` 双后端共享融合；个人规模（≤10⁵ 行）
    可用，更大规模建议 PostgreSQL 后端；
  - 写事务统一 `BEGIN IMMEDIATE`；数组语义（ANY/&&/array_append/unnest）
    按 SQLite 方言（json_each 等）等价改写；`REGEXP` 由 connect 注册 Python 实现（占位名守卫
    仍引用单源常量 PLACEHOLDER_NAME_SQL）；
  - `_SingleConnPool` shim：acquire 面与 asyncpg 池一致（同任务可重入），
    execute 返回 asyncpg 风格状态串（乐观锁/计数解析依赖）。
- **WebUI 双后端**：`webui_store.py` 保留共享层（行装配/校验/常量），
  方言分歧 SQL 拆到 `webui_store_pg.py` / `webui_store_sqlite.py`（函数
  一一对应，按 `backend.dialect` 派发）；维护页设置新增存储后端分组，
  dsn 非空守卫仅在 postgres（或 auto 且现值有 dsn）时生效。
- **测试**：新增 `tests/test_sqlite_backend.py` 真库集成 harness（76 项：
  四条收敛管线 / recall 双路 / alias / edge / WebUI 全函数 / 重开持久化），
  集成测试首次无需 PG 服务；既有桩测试全绿（PG 路为纯搬迁零回归）。
- **依赖**：requirements 新增 `aiosqlite`。

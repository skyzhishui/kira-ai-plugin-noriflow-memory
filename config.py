"""本地记忆插件配置模型（pydantic）。

对应插件 config.toml 顶层字段（enabled 除外——那是插件管理器语义）。
M2（写入链路）只消费连接池 / embedding / 补算 / 熔断字段；
recall、合并 agent、评分状态机字段随 M3-M5 里程碑接入。
"""

from pydantic import BaseModel, Field, model_validator


class LocalMemoryConfig(BaseModel):
    """本地记忆后端（PostgreSQL + pgvector / SQLite 双后端）运行时配置。"""

    storage_backend: str = Field(
        default="auto",
        description='存储后端：postgres | sqlite | auto（auto=配置了 dsn 用 postgres，'
        "否则 sqlite）。启动期只读，运行中不可切换（切后端=换库，须走迁移工具）",
    )
    sqlite_path: str = Field(
        default="",
        description="SQLite 库文件路径（storage_backend=sqlite 时生效；"
        "空 = 插件数据目录/memory.sqlite3）",
    )
    dsn: str = Field(default="", description="PostgreSQL 连接串（空表示未配置；storage_backend=auto 时留空即选 sqlite 后端）")
    pool_min: int = Field(default=2, ge=1, description="连接池最小连接数")
    pool_max: int = Field(default=8, ge=1, description="连接池最大连接数")
    db_command_timeout: int = Field(
        default=60,
        ge=0,
        description="DB 语句级超时（秒，0=不启用）：防单条挂起查询永久占用池槽位饿死记忆子系统",
    )

    embedding_model: str = Field(
        default="",
        description="embedding 模型覆盖，格式 provider_id:model_id（留空用宿主默认 embedding；"
        "解析失败回退默认并告警）",
    )
    embedding_dims: int = Field(
        default=1024,
        ge=1,
        description="向量维度（须与迁移 DDL 的 vector(1024) 一致，启动时校验；≥1 防 0 值"
        "让插件重启后拒绝装配）",
    )
    encode_input_max_chars: int = Field(
        default=30000,
        ge=0,
        description="编码输入字符上限（0=不截断）：超长输入截断保底，防编码必败降级成补编码毒丸行",
    )

    backfill_interval_seconds: float = Field(
        default=300.0, gt=0, description="embedding 补算任务周期（秒）"
    )
    backfill_batch_size: int = Field(
        default=64, ge=1, description="补算任务单表单批扫描行数"
    )

    # ---- M4：合并 agent ----
    merge_interval_hours: float = Field(default=6.0, gt=0, description="合并 agent 执行周期（小时）")
    merge_batch_size: int = Field(default=200, ge=1, description="归一化遍单批拉取的 raw 事实数")
    candidate_top_k: int = Field(default=20, ge=1, description="候选簇向量检索 top-K")
    llm_budget_per_cycle: int = Field(
        default=200, ge=0, description="每周期 LLM 裁定调用预算（超出顺延下周期）"
    )

    # ---- M4/M5：评分状态机 ----
    score_start_high: int = Field(default=3, description="high 置信事实起步分")
    score_start_medium: int = Field(default=2, description="medium 置信事实起步分")
    score_cap: float = Field(
        default=10.0,
        gt=0,
        description="分数封顶（须为正：0 会把簇分数钉死 0、晋升停摆）",
    )
    promote_threshold: float = Field(
        default=10.0,
        gt=0,
        description="进入画像阈值（须为正：0 会让全部簇立即晋升）",
    )
    demote_threshold: float = Field(default=3.0, description="画像降级阈值（迟滞下界）")
    decay_factor: float = Field(default=0.8, gt=0, le=1, description="每衰减周期分数系数")
    decay_interval_days: int = Field(default=14, ge=1, description="衰减周期（天），兼缺席冻结窗口粒度")
    recent_expire_days: int = Field(default=30, ge=1, description="recent 维度过期天数")
    recent_promote_threshold: float = Field(default=4.0, description="recent 维度进画像阈值")
    decay_requires_activity: bool = Field(
        default=True,
        description="衰减门控：仅衰减/降级/死亡本周期内出现过的用户的簇（缺席冻结画像）",
    )
    sticky_evidence_count: int = Field(
        default=4,
        ge=0,
        description="证据地板阈值：证据数达此值的簇衰减不低于画像降级阈值（0=禁用）",
    )
    pending_dead_days: int = Field(
        default=90,
        ge=1,
        description="pending_uncertain 死亡窗口（天）：降级/过期后无新证据超此天数才置 dead，与衰减周期解耦",
    )
    anchor_profile_size: int = Field(
        default=5,
        ge=0,
        description="画像锚定条数：每维度按注入排序前 N 条豁免分数衰减与降级，被新确认事实顶替掉出前 N 名才恢复自然衰减（0=禁用；应与注入每栏条数一致）",
    )

    # ---- M3：recall ----
    recall_top_k: int = Field(default=5, ge=1, description="召回条数上限")
    recall_relevance_threshold: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="召回相关度阈值（rerank 分数或 cosine 相似度低于此值过滤，0=不过滤，对齐现有配置）",
    )
    recall_max_tokens: int = Field(
        default=2048,
        ge=128,
        description="注入文本 token 预算上限（按字符数保守估算截断，CJK 近似 1 字符=1 token）",
    )
    recall_time_decay_enabled: bool = Field(
        default=False,
        description="recall 排序是否叠加时间衰减：score × 2^(-年龄/半衰期) 后重排（仅改排序，不过滤；occurred_at 缺失不降权）",
    )
    recall_time_decay_half_life_days: float = Field(
        default=90.0,
        gt=0,
        description="时间衰减半衰期（天）：距今天数达到该值的记忆排序分减半，越小越偏好近期",
    )
    # KiraAI 版新增：摘要记忆 recall 会话隔离（true=仅召回本会话摘要，
    # false=跨会话召回）；画像不设隔离（画像按用户跨会话聚合）
    summary_recall_session_scoped: bool = Field(
        default=True,
        description="摘要记忆 recall 是否会话隔离（true 仅召回本会话，默认隔离）",
    )
    max_persona_profiles: int = Field(
        default=3, ge=1, description="群聊单轮注入画像人数上限",
    )
    topic_blacklist: list[str] = Field(
        default_factory=list,
        description="话题黑名单：包含这些关键词的记忆在召回候选层（SQL）结构性排除，"
        "不占 top_k 名额；注入层保留二次防线兜漏网",
    )
    rerank_enabled: bool = Field(default=True, description="是否启用重排序（需 model_config [rerank]）")
    rerank_candidates: int = Field(default=50, ge=1, description="进入重排序的向量候选数")

    # ---- 召回质量优化（扩选/近时排除/去重/时间标注，同步自上游 nori 版） ----
    recall_expansion_enabled: bool = Field(
        default=True,
        description="召回扩选：把会话最近 N 批摘要的参与者并入用户过滤，"
        "让『问及未在场成员』的场景可命中其参与过的同会话摘要；"
        "扩展命中恒钉死当前会话（结构性防跨会话泄漏，独立于跨会话开关）",
    )
    recall_expansion_recent_batches: int = Field(
        default=20,
        ge=1,
        description="扩选参与者取样的最近摘要批次数（按 occurred_at 倒序取 participants）",
    )
    recall_exclude_history_window: bool = Field(
        default=True,
        description="近时排除：最近 K 批摘要（K=宿主可见窗口块数 max_memory_length，"
        "每轮恰好一批）不参与召回——这些内容已在宿主 history 窗口内可见，"
        "不再占用召回名额",
    )
    recall_hint_enabled: bool = Field(
        default=True,
        description="实体命中匹配总开关：窗口词典 + 持久别名层的名字匹配，"
        "命中喂画像候选（含私聊提及他人）、P3 关系注入节点匹配，"
        "与问及他人召回键组（实体键并入主路检索，跨会话/会话隔离由"
        " summary_recall_session_scoped/recall_cross_session 管）。"
        "v1.7.1 起不再驱动召回候选拕取旁路（旁路已整体移除——引用原文由"
        "适配层并入 recall query 走主路）",
    )
    # ---- P1：持久实体别名层（窗口词典之后的持久命中来源） ----
    alias_enabled: bool = Field(
        default=True,
        description="持久实体别名层：memory_entity_alias 表（变体拆分后的历史名字流）"
        "作为窗口词典之后的持久命中来源——提起久未发言/历史改名成员可命中其"
        " uid，喂给召回实体路与画像候选；重名歧义时跳过（确定性优先）",
    )
    alias_variant_cap: int = Field(
        default=8,
        ge=1,
        description="每用户保留的名字变体上限（按 last_seen 倒序截断，抑制状态播报式名片的变体风暴）",
    )
    alias_stopwords: list[str] = Field(
        default_factory=list,
        description="别名停用词：与常用词撞车的名字（如有人昵称叫『谢谢』），命中也不注入",
    )
    # ---- P2：结构化关系提取落库（只写不查，灰度审计期） ----
    relation_extract_enabled: bool = Field(
        default=False,
        description="P2 关系提取总开关：编码提示词附加 relations 扩展节，产出"
        "结构化三元组零 LLM 合并落 memory_entity_edge（bot 可作端点；人-人边"
        "插入即 active，bot 边 pending）。开启后进入只写不查审计期（P3 注入"
        "由 relation_inject_enabled 另行控制）",
    )
    relation_bot_edge_min_evidence: int = Field(
        default=2,
        ge=1,
        description="bot 端点边激活门槛：evidence_count（含本次证据，按 {session}|{date} 去重）"
        "达到该值才 pending→active（防玩笑/幻觉自我强化；人-人边单次声明即激活）",
    )
    # ---- P3：边注入（场景 A 节点+边 / C bot 边 AT/引用门槛） ----
    relation_inject_enabled: bool = Field(
        default=False,
        description="P3 边注入总开关：实体命中节点且边 label 词形出现在输入（场景 A）、"
        "或 AT/引用 bot 且 bot 边 label 命中（场景 C）时，注入尾部追加『# 相关人物关系』"
        "小节（陈述行为主体 + 未命中节点的对端画像）。与提取开关分离，审计期后开启",
    )
    relation_inject_max_neighbors: int = Field(
        default=2,
        ge=1,
        description="场景 A 每命中节点的邻居边上限（按 evidence_count/last_seen 倒序取）",
    )
    relation_inject_max_profiles: int = Field(
        default=2,
        ge=0,
        description="边注入的对端画像人数上限（独立于宿主画像预算；0=只出陈述行）",
    )
    relation_inject_max_lines: int = Field(
        default=3,
        ge=1,
        description="每轮关系陈述行总上限",
    )
    relation_label_stopwords: list[str] = Field(
        default_factory=lambda: ["朋友", "认识", "熟人", "网友"],
        description="泛关系词停用表：写侧拦截（命中 label 的边不落库、已有边"
        "不再刷新）+ 读侧不触发边扩展双生效（高频口语词误触发面大，如"
        "『姐姐我告诉你』式非关系用法防不住的兜底层；按场景人工维护，"
        "非称谓词形另由提示词规则与 parse_relation 端点名守卫拦）",
    )
    relation_audit_enabled: bool = Field(
        default=False,
        description="Relation-edge semantic audit pass (hooked into the merge "
        "agent cycle): an LLM incrementally audits active/pending edges by id "
        "watermark; edges judged as interaction descriptions, address "
        "compounds, statement/endpoint mismatch or direction conflicts are "
        "set to superseded (tombstone, not physically deleted, manually "
        "recoverable). unsure/failed batches defer without advancing the "
        "watermark; a batch failing 3 consecutive cycles is skipped to keep "
        "the pipeline moving (manual full re-audit = clear the watermark kv).",
    )
    relation_audit_batch_size: int = Field(
        default=20,
        ge=1,
        description="Edges per semantic-audit batch (one LLM call per batch; "
        "calls per cycle are capped separately and overflow defers)",
    )
    dedup_similarity_threshold: float = Field(
        default=0.85,
        ge=0.0,
        le=1.0,
        description="近重复去重阈值：最终序中与已保留条目 embedding cosine ≥ 此值的丢弃（0=关闭）；"
        "用于抑制相邻轮次摘要近重复占满 top_k",
    )
    write_dedup_enabled: bool = Field(
        default=True,
        description="写入侧近重去重：retain 编码摘要落库前与同会话最近"
        " write_dedup_window 批摘要比对，cosine ≥ write_dedup_threshold 的跳过"
        "写入（抑制历史上下文泄漏导致的同事件重复摘要；facts/relations 通道"
        "不受影响，自带幂等键去重）",
    )
    write_dedup_window: int = Field(
        default=8,
        ge=1,
        description="写入侧近重去重的比对窗口（同会话最近批次数）",
    )
    write_dedup_threshold: float = Field(
        default=0.85,
        ge=0.0,
        le=1.0,
        description="写入侧近重去重阈值：与窗口内摘要 cosine ≥ 此值跳过写入（0=关闭）",
    )
    recall_time_label_enabled: bool = Field(
        default=True,
        description="注入记忆追加相对时间标注（今天/昨天/N天前/约N周前/约N个月前，按本地时区换算）",
    )
    timezone: str = Field(
        default="",
        description="本地时区名（如 Asia/Shanghai；空=优先宿主 locale.TZ，再回退服务器本地时区）。"
        "相对时间标注与窗口时间换算以此为准",
    )
    recall_log_enabled: bool = Field(
        default=False,
        description="召回评估日志开关：启用后每次检索与注入各落一条 JSONL（fail-open，不影响召回）",
    )
    recall_log_path: str = Field(
        default="",
        description="召回日志文件路径（空=插件数据目录/memory_recall_log.jsonl）",
    )

    # ---- 混合检索（方案 A：写侧 bigram 分词列 + tsvector GIN） ----
    hybrid_search_enabled: bool = Field(
        default=True,
        description="向量+BM25 混合检索：两路候选 RRF 融合（稀有条目——人名/游戏名/"
        "黑话——经 BM25 路进入候选池）；关闭=纯向量（内部基准：bigram "
        "tsvector 稀有探针覆盖率 100%/0.6ms）",
    )
    hybrid_rrf_k: int = Field(
        default=60, ge=1, description="RRF 融合常数 k（越大两路排名越平权）"
    )

    # ---- 滚动补回：宿主窗口外最近批次摘要注入 ----
    recent_rollout_enabled: bool = Field(
        default=True,
        description="滚动补回：注入最近 N 批滚动出宿主可见窗口的同会话摘要"
        "（跳过窗口内 K 批，与近时排除同源）；recall 同轮排除这些行防重复注入",
    )
    recent_rollout_batches: int = Field(
        default=3, ge=1, description="滚动补回取的最近批次数"
    )
    recent_rollout_max_chars: int = Field(
        default=1500, ge=100, description="滚动补回块字符预算（超限按批丢弃旧批，单批超限截断）"
    )

    # ---- 熔断 ----
    failure_threshold: int = Field(
        default=5,
        ge=1,
        description="DB 熔断阈值（连续失败次数；≥1 防 0 值首次失败即熔断）",
    )
    recovery_seconds: float = Field(default=60.0, gt=0, description="DB 熔断恢复时间（秒）")

    @model_validator(mode="after")
    def _pool_bounds_consistent(self) -> "LocalMemoryConfig":
        """pool_min must not exceed pool_max (pool creation would fail)."""
        if self.pool_min > self.pool_max:
            raise ValueError("pool_min 不能大于 pool_max")
        return self

    # ---- 主动工具安全 ----
    tool_scope_locked: bool = Field(
        default=True,
        description="工具作用域锁定：开启时 memory_search/write/remove 忽略 LLM 显式传入的 "
        "session_id/user_id，钉死为触发会话/触发者（memory_remove 只能删触发作用域内的行）；"
        "关闭时恢复显式参数语义（白名单命中的调用方可跨会话/跨用户操作）",
    )

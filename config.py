"""Local memory plugin config model (pydantic).

Corresponds to the plugin config.toml top-level fields (except enabled,
which is plugin-manager semantics). M2 (write path) only consumes the
connection pool / embedding / backfill / breaker fields; recall, merge
agent, and scoring-state-machine fields land with M3-M5 milestones.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

# Config keys involved in cross-field validation (model_validator): the
# defensive upper bound of the fallback reset scope. Precise fallback
# relies on each validator raising CrossFieldConfigError and self-
# reporting the keys it governs (main._try_build_config resets only the
# actually-illegal group, never touching explicit configs of other legal
# combinations); the whole-table fallback kicks in only when a cross-
# field error carries no self-reported keys (a future validator
# forgetting to pass keys). Field-level constraints (gt/ge etc.) do not
# go through this table -- pydantic errors carry loc and can be
# pinpointed per field.
CROSS_FIELD_VALIDATED_KEYS: tuple = (
    "pool_min", "pool_max",
    "score_cap", "promote_threshold", "demote_threshold",
    "recent_promote_threshold", "naming_promote_threshold",
    "score_start_high", "score_start_medium",
)


class CrossFieldConfigError(ValueError):
    """Cross-field validation failure: keys declares the config keys this
    validator governs, which must be reset to defaults together.

    pydantic places this exception instance verbatim into
    ValidationError.errors()[i]["ctx"]["error"]; the fallback layer uses
    it to pinpoint exactly which key group to reset.
    """

    def __init__(self, message: str, keys) -> None:
        super().__init__(message)
        self.keys = tuple(keys)


class LocalMemoryConfig(BaseModel):
    """Runtime config for the local memory backend (PostgreSQL + pgvector / SQLite dual backend)."""

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
    retain_interval_group: int = Field(
        default=1, ge=1, description="群聊 retain 触发间隔（回合数）"
    )
    retain_interval_private: int = Field(
        default=5, ge=1, description="私聊 retain 触发间隔（回合数）"
    )
    flush_idle_s: float = Field(
        default=1800.0, gt=0, description="会话空闲 flush 延迟（秒）"
    )

    backfill_interval_seconds: float = Field(
        default=300.0, gt=0, description="embedding 补算任务周期（秒）"
    )
    backfill_batch_size: int = Field(
        default=64, ge=1, description="补算任务单表单批扫描行数"
    )

    # ---- M4: merge agent ----
    merge_interval_hours: float = Field(default=6.0, gt=0, description="合并 agent 执行周期（小时）")
    merge_batch_size: int = Field(default=200, ge=1, description="归一化遍单批拉取的 raw 事实数")
    candidate_top_k: int = Field(default=20, ge=1, description="候选簇向量检索 top-K")
    llm_budget_per_cycle: int = Field(
        default=200, ge=0, description="每周期 LLM 裁定调用预算（超出顺延下周期）"
    )

    # ---- M4/M5: scoring state machine ----
    score_start_high: int = Field(
        default=3, ge=0,
        description="high 置信事实起步分（非负：负数会让迟滞带校验的"
        "单条证据加分失真，配出 promote/demote 倒挂的非法组合）",
    )
    score_start_medium: int = Field(
        default=2, ge=0,
        description="medium 置信事实起步分（非负，理由同 score_start_high）",
    )
    score_cap: float = Field(
        default=10.0,
        gt=0,
        description="分数封顶（须为正：0 会把簇分数钉死 0、晋升停摆）",
    )
    promote_threshold: float = Field(
        default=6.0,
        gt=0,
        description="进入画像阈值（须为正：0 会让全部簇立即晋升）；"
        "迟滞带 = promote_threshold - demote_threshold，不得小于单条证据最大加分"
        "（max(score_start_high, score_start_medium)；相等亦安全——降档判定为严格小于），"
        "否则降档簇一条证据即可回档（默认 6-2=4 ≥ 3；装配期强制校验）",
    )
    demote_threshold: float = Field(
        default=2.0,
        gt=0,
        description="画像降级阈值（迟滞下界；须为正：0 会让画像永不掉出）；"
        "与 promote_threshold 的差值不得小于单条证据最大加分"
        "（max(score_start_high, score_start_medium)），否则降档簇一条证据即可回档",
    )
    decay_factor: float = Field(default=0.9, gt=0, le=1, description="每衰减周期分数系数")
    decay_interval_days: int = Field(default=14, ge=1, description="衰减周期（天），兼缺席冻结窗口粒度")
    recent_expire_days: int = Field(default=30, ge=1, description="recent 维度过期天数")
    recent_promote_threshold: float = Field(default=4.0, description="recent 维度进画像阈值")
    naming_promote_threshold: Optional[float] = Field(
        default=None,
        gt=0,
        description="naming（称呼偏好）维度进画像阈值：缺省（None/不设）从"
        " score_start_high 派生——本人明确称呼要求（confidence=high，起步分"
        "即 score_start_high）单次即可晋档入画像，该承诺在任何评分刻度下按"
        "构造成立（缺省刻度下即 3.0）；显式设置时按本值生效（高于 "
        "score_start_high 则退化为需二次证据累积，属合法保守配置），且不得"
        "小于 demote_threshold（否则晋档后下一轮衰减即被降级）、不得大于 "
        "score_cap（均装配期校验）",
    )
    naming_prompt_enabled: bool = Field(
        default=True,
        description="naming 附加编码段开关（只控制编码提示词扩展节附加与否，"
        "对齐 relation_extract_enabled 模式；关闭后编码提示词不含该段，"
        "naming 提取退回基础规则行为）",
    )
    commitment_expire_days: int = Field(
        default=60,
        ge=1,
        description="commitment 维度过期天数：最近一次被确认后超过此天数无新证据，"
        "降 pending_uncertain 出画像（簇体保留可复活；起算点取 "
        "COALESCE(last_evidence_at, occurred_at)——持续被提起的约定可持续续期）",
    )
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

    # ---- M3: recall ----
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
    # KiraAI-edition addition: summary-memory recall session isolation
    # (true=recall only this session's summaries, false=recall across
    # sessions); profiles have no isolation (profiles aggregate per user
    # across sessions)
    summary_recall_session_scoped: bool = Field(
        default=True,
        description="摘要记忆 recall 是否会话隔离（true 仅召回本会话，默认隔离）",
    )
    # ---- Summary lifecycle (archived state + access reinforcement;
    # aligned with the nori-side 011 same-named config group) ----
    summary_lifecycle_enabled: bool = Field(
        default=False,
        description="摘要生命周期总开关：开启后合并 agent 周期执行归档遍"
        "（written_at 超过归档时限且强化窗口内无召回命中的行置 archived，"
        "结构性退出召回；原文保留、按 id 可读，memory_lookup 可查、"
        "memory_correct(reactivate) 可恢复）。关闭后已归档行保持归档态"
        "（不回流）",
    )
    summary_lifecycle_grace_days: int = Field(
        default=30,
        ge=0,
        description="最低保留期（天）：written_at 距今不足此值的行永不归档",
    )
    summary_lifecycle_half_life_days: float = Field(
        default=90.0,
        gt=0,
        description="生命周期半衰期（天）：归档时限 = 最低保留期 + 3×半衰期"
        "（衰减权重降至 12.5% 以下，即『陈年且长期无人问津』）",
    )
    summary_lifecycle_reinforce_window_days: int = Field(
        default=90,
        ge=1,
        description="访问强化窗口（天）：最近一次被召回命中在窗口内的行豁免归档"
        "（一次召回续命一个窗口）",
    )
    summary_lifecycle_interval_days: int = Field(
        default=7,
        ge=1,
        description="归档遍执行周期（天）：上次执行时间 kv 持久化，重启不丢；"
        "首次运行只打时间戳不归档（首遍延后至强化窗口后）",
    )
    summary_lifecycle_reinforce_on_recall: bool = Field(
        default=True,
        description="召回访问强化：召回最终注入集的行异步刷新 last_recall_at "
        "与 recall_count（fire-and-forget，失败仅告警）。默认开启且不受"
        " summary_lifecycle_enabled 门控——预累积强化信号，避免开启生命"
        "周期后首遍判据因无 last_recall_at 把存量高频记忆误归档",
    )
    # ---- Active memory tools (aligned with the nori-side five-tool set) ----
    memory_tools_enabled: bool = Field(
        default=True,
        description="主动记忆维护工具总开关：memory_profile / "
        "memory_lookup / memory_correct 新三件（关闭则三件不注册）；"
        "既有 memory_search/write/remove 不受本开关管理，"
        "细粒度禁用走 enabled_tools",
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

    # ---- Recall quality tuning (expansion / recent-range exclusion /
    # dedup / time labeling, synced from upstream nori) ----
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
    # ---- P1: persistent entity alias layer (persistent hit source after
    # the window dictionary) ----
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
    # ---- P2: structured relation extraction storage (write-only, no
    # query; grayscale audit period) ----
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
    # ---- P3: edge injection (scenario A node+edge / scenario C bot-edge
    # AT/reference gate) ----
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
        description="注入记忆追加时间标注（recall 主路与滚动补回共用，按本地时区换算）",
    )
    recall_time_label_mode: Literal["relative", "absolute", "both"] = Field(
        default="both",
        description="时间标注形态：relative=仅相对（今天/N天前）；"
        "absolute=仅绝对（9月28日 14:30，跨年带年份）；"
        "both=相对+绝对并列（3天前 · 9月28日 14:30）。绝对部分分层精度："
        "7 天内带时分，更久只到日期——occurred_at 是批次落库时间，"
        "远期事件时刻本就近似，精确到分钟反而误导时序推理",
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

    # ---- Hybrid search (plan A: write-side bigram tokenization column +
    # tsvector GIN) ----
    hybrid_search_enabled: bool = Field(
        default=True,
        description="向量+BM25 混合检索：两路候选 RRF 融合（稀有条目——人名/游戏名/"
        "黑话——经 BM25 路进入候选池）；关闭=纯向量（内部基准：bigram "
        "tsvector 稀有探针覆盖率 100%/0.6ms）",
    )
    hybrid_rrf_k: int = Field(
        default=60, ge=1, description="RRF 融合常数 k（越大两路排名越平权）"
    )

    # ---- Rolling rollout fill-back: inject summaries of recent batches
    # that rolled out of the host window ----
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
    cross_session_summary_enabled: bool = Field(
        default=False,
        description="跨会话近期摘要：私聊回合前置当前用户在其他会话的近期"
        "摘要（仅 dm 注入；recall 同轮排除已注入行防重复暴露）",
    )
    cross_session_summary_top_k: int = Field(
        default=2, ge=1, description="跨会话摘要候选条数"
    )
    cross_session_summary_max_chars: int = Field(
        default=1500, ge=100, description="跨会话摘要整块字符预算（含前言；超限丢弃旧条，首条超限截断）"
    )
    cross_session_summary_max_age_days: int = Field(
        default=30, ge=0, description="跨会话摘要最大陈旧天数（0=不限）"
    )

    # ---- Circuit breaker ----
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
            raise CrossFieldConfigError(
                "pool_min 不能大于 pool_max", ("pool_min", "pool_max")
            )
        return self

    @model_validator(mode="after")
    def _hysteresis_band_guards_repromote(self) -> "LocalMemoryConfig":
        """Hysteresis band validation: when a demoted cluster can climb
        back with one piece of evidence, the profile flips between
        "demote -> one piece of evidence -> promote again".

        Demotion is a strict less-than (`score < demote_threshold`), so a
        demoted cluster's score ceiling is `demote_threshold - ε`; on
        revival the score accumulates via `min(cap, score + start_score)`,
        and re-promotion requires `score >= promote_threshold`. Hence when
        `band = promote - demote >= the max single-evidence increment`,
        `demote - ε + increment < promote` always holds (band exactly
        equal to the increment is also safe), so one piece of evidence
        cannot climb back; when band is smaller than the increment (e.g.
        5/3), a demoted cluster + one high evidence suffices.

        Illegal combinations raise at config assembly time (the
        maintenance page save goes through the same model; illegal values
        return 422), no longer relying on description text alone.

        Float comparisons carry a 1e-9 tolerance: binary float subtraction
        has representation error (e.g. 6.1 - 3.1 = 2.9999999999999996), so
        a combination where band mathematically equals the increment
        (equality is safe) must not be wrongly rejected over that error.
        """
        max_inc = max(self.score_start_high, self.score_start_medium)
        band = self.promote_threshold - self.demote_threshold
        if self.promote_threshold <= self.demote_threshold:
            # Explicit inversion guard: ge=0 already blocks negative start
            # scores; this closes the remaining promote==demote case (zero
            # hysteresis) and any inverted combination leaking in through
            # other paths
            raise CrossFieldConfigError(
                f"promote_threshold ({self.promote_threshold}) 必须大于 "
                f"demote_threshold ({self.demote_threshold})，迟滞带不得为零或负",
                ("promote_threshold", "demote_threshold"),
            )
        if band + 1e-9 < max_inc:
            raise CrossFieldConfigError(
                f"promote_threshold - demote_threshold ({band}) 不得小于单条证据最大加分"
                f" ({max_inc} = max(score_start_high, score_start_medium))，"
                "否则降档簇一条证据即可回档，画像会在降级与晋档之间反复翻转",
                (
                    "promote_threshold", "demote_threshold",
                    "score_start_high", "score_start_medium",
                ),
            )
        return self

    @model_validator(mode="after")
    def _resolve_naming_threshold_default(self) -> "LocalMemoryConfig":
        """Derive the naming threshold default: None -> score_start_high.

        A derived default keeps the promise "one explicit naming statement
        (high evidence, start score = score_start_high) promotes in a single
        step" true by construction at any score scale, and avoids the
        upgrade cascade: scaled legacy deployments (score_start_high already
        scaled) stay valid without setting the new key, so no cross-field
        fallback wipes their legal explicit scoring keys. Explicit values
        win as-is (including conservative forms above score_start_high).
        Defined before _promote_thresholds_within_cap so the cap check sees
        the resolved value.

        Derivation lower bound (review D4): score_start_high <= 0 is a legal
        field value (ge=0), but deriving 0 would bypass this field's gt=0 --
        every naming extraction (0-score starts included) would promote into
        the profile at cluster creation. Reject and demand an explicit
        naming threshold; the self-reported key fallback (both keys reset:
        3.0 / None -> derived 3.0) always converges.

        Demotion-floor guard (review): the decay pass multiplies the score
        by decay_factor (<= 1) and then demotes every profiled cluster below
        demote_threshold. A naming threshold below demote_threshold therefore
        promotes naming clusters that the very next decay pass demotes again
        (profile churn, and a naming promise that silently drops out). The
        guard self-reports the keys it governs: the explicit pair
        (naming_promote_threshold, demote_threshold) or, when the threshold
        is derived, (score_start_high, demote_threshold) -- resetting those
        always converges (default score_start_high 3 > default demote 2).
        """
        derived = self.naming_promote_threshold is None
        if derived:
            if self.score_start_high <= 0:
                raise CrossFieldConfigError(
                    f"score_start_high ({self.score_start_high}) <= 0 时无法派生"
                    " naming_promote_threshold（派生值 0 会绕过 gt=0，任何 naming"
                    " 提取建簇即晋档入画像）——请显式设置 naming_promote_threshold",
                    ("naming_promote_threshold", "score_start_high"),
                )
            self.naming_promote_threshold = float(self.score_start_high)
        if self.naming_promote_threshold + 1e-9 < self.demote_threshold:
            raise CrossFieldConfigError(
                f"naming_promote_threshold ({self.naming_promote_threshold}) 不得小于 "
                f"demote_threshold ({self.demote_threshold})：衰减遍先按 "
                f"decay_factor 衰减再按 demote_threshold 降级，低于降级线的 "
                "naming 簇晋档后会在下一轮衰减遍被立即降级删除（画像反复抖动，"
                "称呼承诺静默丢失）",
                (
                    ("demote_threshold", "score_start_high")
                    if derived
                    else ("naming_promote_threshold", "demote_threshold")
                ),
            )
        return self

    @model_validator(mode="after")
    def _promote_thresholds_within_cap(self) -> "LocalMemoryConfig":
        """Promotion thresholds must not exceed the score cap; otherwise
        cluster scores are capped by score_cap and never reach the
        threshold, silently disabling promotion (previously the
        maintenance page could configure this combination with no
        interception at all -- including the cascade where a cross-field
        fallback resets only the band group and leaves a legacy
        small-scale score_cap).

        The naming dimension follows the same rule (included since the
        v1.18.2 port): with naming_promote_threshold > score_cap, neither
        the single-statement promote nor the promotion pass can reach the
        threshold, and naming preferences silently never enter the
        profile. Note only the cap upper bound is validated: the
        "<= score_start_high" in the description is an effectiveness
        precondition of the single-step promote promise, not a legality
        constraint (exceeding it merely degrades to two-step evidence
        accumulation, a legal conservative config), so it is not
        hard-rejected here.

        Equality is allowed: the score can reach the cap via
        min(score_cap, ...) (the old default promote=10 == score_cap=10 is
        exactly this form). Float comparisons carry a 1e-9 tolerance, same
        as the hysteresis band check.
        """
        cap = self.score_cap
        if (
            self.promote_threshold > cap + 1e-9
            or self.recent_promote_threshold > cap + 1e-9
            or self.naming_promote_threshold > cap + 1e-9
        ):
            raise CrossFieldConfigError(
                f"promote_threshold ({self.promote_threshold}) / "
                f"recent_promote_threshold ({self.recent_promote_threshold}) / "
                f"naming_promote_threshold ({self.naming_promote_threshold}) "
                f"不得大于 score_cap ({cap})：簇分数被 score_cap 封顶后永远达不到"
                "晋升阈值，晋升会被静默关闭",
                (
                    "score_cap",
                    "promote_threshold",
                    "recent_promote_threshold",
                    "naming_promote_threshold",
                ),
            )
        return self

    # ---- Active tool safety ----
    tool_scope_locked: bool = Field(
        default=True,
        description="工具作用域锁定：开启时 memory_search/write/remove 忽略 LLM 显式传入的 "
        "session_id/user_id，钉死为触发会话/触发者（memory_remove 只能删触发作用域内的行）；"
        "关闭时恢复显式参数语义（白名单命中的调用方可跨会话/跨用户操作）",
    )

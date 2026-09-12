"""存量关系回填：已确认事实簇 -> memory_entity_edge（一次性维护任务）。

P2 关系提取只对增量编码批次生效——历史已确认事实（active/profiled 簇）
里沉淀的关系需要一次回填才能进边表。本模块经维护页手动触发
（关系图谱栏"从已确认事实回填"），不走常规管线。

数据流（零 LLM 信任原则，逐条防御）：
1. 源：fetch_confirmed_relation_sources（active+profiled 簇，id 升序）；
2. 目录：fetch_alias_directory 全表 -> 纯函数 _build_directory 消歧
   （同名多 uid 跳过，确定性优先）；bot 昵称显式入目录 (bot)；
3. 批次：每批 cluster_batch_size 条；批内陈述子串命中目录名的名字
   才进该批词典（提示词有界）；无可解析名字的批次跳过 LLM 直达 0 条；
4. LLM：prompts/fact_relations.prompt 单次调用出 relations 数组；
5. 校验：parse_relation（label 2-8 字/statement<=200/confidence 值域）
   + cluster_id 必须引用输入行 + subject 必须等于该簇归属 uid +
   object 必须在该批词典 uid 集 + label 词形必须出现在源陈述里；
   端点占位名（未知/uid 兜底形）以目录规范名顶替；
6. 落库：db.upsert_entity_edge，evidence_key = backfill|{cluster_id}
   （重跑幂等不涨计数）；occurred_at 取簇 occurred_at；bot 边照常
   pending + min_evidence 门槛（回填不算双证据，激活仍需线上复现）。

失败语义：单批失败（LLM 不可用/输出不可解析）计数并即停——失败批与
其后的簇水位不推进，下次重跑自动补；全量重跑（force_full）忽略水位。
计数语义：回填行 count_on_conflict=False——命中已有边的冲突路径只刷新
不计分（防线上提取 +1 后回填回声再 +1、防回填回声凑满 bot 边双证据
门槛），新行插入仍计 1；重跑数据侧零副作用。
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Optional

from .alias_store import is_placeholder_name
from .db import MemoryDatabase
from .entity_edge import edge_has_bot_endpoint, parse_relation

try:  # nori-core 宿主
    from nori_core.models_client.prompt import PromptLoader
    from nori_core.utils.json_utils import safe_parse_llm_json
except ImportError:  # kira 宿主（vendor 副本同 API）
    from .prompt_loader import PromptLoader
    from .json_utils import safe_parse_llm_json

try:  # kira host: route through the host logging manager (std logging
    # records are filtered out by the host and never reach data/log.log —
    # backfill failures would be invisible).
    from core.logging_manager import get_logger as _get_logger

    logger = _get_logger("noriflow_memory.relation_backfill", "cyan")
except ImportError:  # kira 宿主（模块与上游 nori 版保持同构）
    import logging

    logger = logging.getLogger(__name__)

_RELATIONS_PROMPT_NAME = "fact_relations"
DEFAULT_BATCH_SIZE = 40
# 水位 kv 键：已成功回填到的最大簇 id——增量重跑只取其后的新簇，
# 不再对已回填簇重复调 LLM（数据侧另有 count_on_conflict=False 双保险）
_WATERMARK_KEY = "relation_backfill_watermark"
# 复活簇回填待办 kv 键（JSON id 数组，容量上限防无界增长）：merge 复活 /
# 维护页手工复活的簇通常已在水位之下，增量扫描永远取不到——登记待办
# 后每轮回填按 id 精取补提取（backfill|{id} evidence_key 幂等，重复无害）
_BACKFILL_PENDING_KEY = "relation_backfill_pending_ids"
_BACKFILL_PENDING_MAX = 500


def _kv_pool(db_or_pool):
    """MemoryDatabase 或裸 pool -> 裸 pool（标记函数两侧调用方通吃）。"""
    return db_or_pool.pool if hasattr(db_or_pool, "pool") else db_or_pool


async def _kv_get(db_or_pool, key: str):
    """kv 读取：MemoryDatabase.get_kv 优先，裸 pool 走 SQL。"""
    if hasattr(db_or_pool, "get_kv"):
        return await db_or_pool.get_kv(key)
    pool = _kv_pool(db_or_pool)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT value FROM _memory_local_kv WHERE key = $1", key
        )
        return row["value"] if row else None


async def _kv_set(db_or_pool, key: str, value: str) -> None:
    """kv 写入：MemoryDatabase.set_kv 优先，裸 pool 走 SQL。"""
    if hasattr(db_or_pool, "set_kv"):
        await db_or_pool.set_kv(key, value)
        return
    pool = _kv_pool(db_or_pool)
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO _memory_local_kv (key, value) VALUES ($1, $2) "
            "ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = now()",
            key,
            value,
        )


async def mark_backfill_pending(db_or_pool, cluster_ids: list[int]) -> int:
    """登记复活簇的回填待办（append 语义，容量上限内保序去重）。

    Args:
        db_or_pool: MemoryDatabase / asyncpg 连接池 / 具备 get_kv+set_kv 的桩。
        cluster_ids: 复活簇 id 列表。

    Returns:
        本次新登记的簇数。
    """
    ids = sorted({int(i) for i in cluster_ids or [] if int(i) > 0})
    if not ids:
        return 0
    raw = await _kv_get(db_or_pool, _BACKFILL_PENDING_KEY)
    pending: list[int] = []
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                pending = [int(i) for i in parsed if str(i).isdigit()]
        except Exception:
            pending = []
    known = set(pending)
    fresh = [i for i in ids if i not in known]
    if not fresh:
        return 0
    merged = (pending + fresh)[-_BACKFILL_PENDING_MAX:]
    await _kv_set(db_or_pool, _BACKFILL_PENDING_KEY, json.dumps(merged))
    return len(fresh)


async def take_backfill_pending(db_or_pool) -> list[int]:
    """取走回填待办（读后清空；失败保留待办下轮重试）。

    Returns:
        待办簇 id 列表（空列表 = 无待办或读取失败）。
    """
    try:
        raw = await _kv_get(db_or_pool, _BACKFILL_PENDING_KEY)
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            ids = (
                [int(i) for i in parsed if str(i).isdigit()]
                if isinstance(parsed, list)
                else []
            )
        except Exception:
            ids = []
        if ids:
            await _kv_set(db_or_pool, _BACKFILL_PENDING_KEY, "[]")
        return ids
    except Exception:
        logger.warning("回填待办读取失败（保留待办，下轮重试）", exc_info=True)
        return []


@dataclass
class _DirectoryEntry:
    """目录条目：名字 -> (platform, uid)。platform 空串 = 通配（bot）。"""

    platform: str
    uid: str


def _chunked(items: list, size: int) -> list[list]:
    """Split a list into consecutive chunks of at most ``size`` items."""
    return [items[i : i + size] for i in range(0, len(items), size)]


def build_directory(
    alias_rows: list[tuple[str, str, str]],
    bot_user_id: str = "",
    bot_nickname: str = "",
) -> dict[str, _DirectoryEntry]:
    """名字 -> 目录条目（歧义名字整名跳过，与 alias_store.match 同裁决）。

    Args:
        alias_rows: (platform, uid, name) 全表行。
        bot_user_id: bot 平台 uid（非空时以通配 platform 入目录）。
        bot_nickname: bot 昵称（目录键）。

    Returns:
        name -> _DirectoryEntry；同名字多 uid 的名字不出现（确定性优先）。
    """
    grouped: dict[str, list[tuple[str, str]]] = {}
    for platform, uid, name in alias_rows or []:
        if not name or not uid:
            continue
        grouped.setdefault(name, [])
        if (platform, uid) not in grouped[name]:
            grouped[name].append((platform, uid))
    directory: dict[str, _DirectoryEntry] = {}
    for name, pairs in grouped.items():
        if len(pairs) == 1:
            platform, uid = pairs[0]
            directory[name] = _DirectoryEntry(platform=platform, uid=uid)
    bot = (bot_user_id or "").strip()
    if bot and (bot_nickname or "").strip():
        directory[(bot_nickname or "").strip()] = _DirectoryEntry(platform="", uid=bot)
    return directory


class RelationBackfill:
    """存量关系回填任务（维护页手动触发，可重跑幂等）。"""

    def __init__(
        self,
        *,
        llm_call: Callable[[str, str], Awaitable[str]],
        db: MemoryDatabase,
        config,
        bot_user_id: str = "",
        bot_nickname: str = "",
        prompt_dir: Optional[Path] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        bot_forms_provider: Optional[Callable[[], list[str]]] = None,
    ) -> None:
        """装配回填器。

        Args:
            llm_call: LLM 出口 (system_prompt, user_prompt) -> 原始输出
                文本——两侧宿主出口不同（上游 nori 版 / kira
                run_structured），由装配层闭包包装，本模块保持两侧字节
                级一致。
            db: 记忆库连接池。
            config: 插件配置（relation_bot_edge_min_evidence）。
            bot_user_id: bot 平台 uid（bot 边判定 + 目录 bot 条目）。
            bot_nickname: bot 昵称。
            prompt_dir: 提示词目录；None 用本插件 prompts/。
            batch_size: 每批簇数。
            bot_forms_provider: bot uid 全形态集合读取器（kernel.
                _bot_uid_forms_all 语义——bot 端点判定与 retain 通道
                对齐；None 退化为单形态 bot_user_id）。
        """
        self._llm_call = llm_call
        self._db = db
        self._config = config
        self._bot_user_id = (bot_user_id or "").strip()
        self._bot_nickname = (bot_nickname or "").strip()
        self._bot_forms_provider = bot_forms_provider
        self._loader = PromptLoader(prompt_dir or (Path(__file__).resolve().parent / "prompts"))
        self._batch_size = max(1, int(batch_size))

    def _bot_forms(self) -> list[str]:
        """Bot uid 全形态集合（provider 优先；退化为单形态 uid）。"""
        if self._bot_forms_provider is not None:
            try:
                forms = [f for f in (self._bot_forms_provider() or []) if f]
            except Exception:
                forms = []
            if forms:
                return forms
        return [self._bot_user_id] if self._bot_user_id else []

    async def run(
        self,
        progress: Optional[Callable[[dict], None]] = None,
        force_full: bool = False,
    ) -> dict:
        """执行回填（id 窗口分页逐批；单批失败即停，余量顺延下次重跑）。

        水位增量：非全量模式从 kv 水位（relation_backfill_watermark）
        之后的新簇开始；每批成功后水位推进到该批最大簇 id，失败即停——
        失败批与其后的簇保持未推进，下次重跑自动补。全量模式忽略水位
        （簇陈述修正后想重新提取时用；数据侧 count_on_conflict=False
        保证重跑只刷新不计分）。源查询按 batch_size 窗口分页推进（大库
        全量不再一次性载入内存）。复活簇待办（merge/维护页复活、水位
        之下的簇）在每轮开头取走并按 id 精取，置于增量批次之前补提取。

        Args:
            progress: 每批一次的进度回调（收 {done,total,relations}）。
            force_full: True 忽略水位全量重跑。

        Returns:
            汇总 dict：clusters_total/batches_total/batches_failed/
            relations_written/relations_discarded/mode。
        """
        after_id = 0
        mode = "full"
        if not force_full:
            try:
                raw = await self._db.get_kv(_WATERMARK_KEY)
                after_id = max(0, int(raw or 0))
                mode = "incremental"
            except Exception:
                logger.warning("回填水位读取失败（退化为全量）", exc_info=True)
                after_id, mode = 0, "full"
        # 复活簇待办：水位之下的复活簇（merge/手工复活）按 id 精取
        revived_pending = await take_backfill_pending(self._db)
        revived = []
        if revived_pending:
            try:
                revived = await self._db.fetch_confirmed_relation_sources_by_ids(
                    revived_pending
                )
            except Exception:
                logger.warning("复活簇待办精取失败（待办已清，可全量重跑补）",
                               exc_info=True)
                revived = []
        bot = self._bot_user_id
        try:
            remaining = await self._db.count_confirmed_relation_sources(
                after_id, exclude_user_id=bot
            )
        except TypeError:
            # Older stubs without the exclude param (test doubles)
            remaining = await self._db.count_confirmed_relation_sources(after_id)
        except Exception:
            remaining = 0
        directory = build_directory(
            await self._db.fetch_alias_directory(), bot, self._bot_nickname
        )
        # uid -> 规范名（写侧守卫源）：LLM 输出"未知"/uid 兜底形时用
        # 目录名顶替（bot 通配条目不作规范名源）；未命中留空由 upsert
        # 语句保旧名
        names_by_owner: dict[tuple[str, str], str] = {}
        for name, entry in directory.items():
            if not entry.platform:
                continue
            names_by_owner.setdefault((entry.platform, entry.uid), name)
        summary = {
            "clusters_total": len(revived) + remaining,
            "batches_total": (
                len(_chunked(revived, self._batch_size))
                + -(-remaining // self._batch_size)
            ),
            "batches_failed": 0,
            "relations_written": 0,
            "relations_discarded": 0,
            "mode": mode,
        }
        done = 0

        async def _process(batch: list[dict]) -> None:
            nonlocal done
            try:
                written, discarded = await self._run_batch(
                    batch, directory, names_by_owner
                )
                summary["relations_written"] += written
                summary["relations_discarded"] += discarded
            except Exception:
                summary["batches_failed"] += 1
                logger.warning(
                    "回填批次失败（水位止步于批内最大簇 id 之前，重跑可补）",
                    exc_info=True,
                )
                raise
            done += 1
            if progress is not None:
                progress(
                    {
                        "done": done,
                        "total": summary["batches_total"],
                        "relations": summary["relations_written"],
                    }
                )

        stopped = False
        # 复活簇批（待办批；水位不动——这些簇本就在水位之下）
        for batch in _chunked(revived, self._batch_size):
            try:
                await _process(batch)
            except Exception:
                stopped = True
                break
        # 增量/全量批：id 窗口分页，每批成功即推水位
        if not stopped:
            while True:
                try:
                    chunk = await self._db.fetch_confirmed_relation_sources(
                        after_id, limit=self._batch_size
                    )
                except Exception:
                    summary["batches_failed"] += 1
                    logger.warning("回填源窗口拉取失败（重跑可补）", exc_info=True)
                    break
                if not chunk:
                    break
                fetched = len(chunk)
                after_id = max(int(c["id"]) for c in chunk)
                if bot:
                    chunk = [c for c in chunk if str(c["user_id"]) != bot]
                try:
                    await _process(chunk)
                except Exception:
                    break
                try:
                    await self._db.set_kv(_WATERMARK_KEY, str(after_id))
                except Exception:
                    logger.warning("回填水位写入失败（下轮可能重复该批）",
                                   exc_info=True)
                if fetched < self._batch_size:
                    break
        logger.info(
            "关系回填完成[%s]: 簇 %d / 批 %d（失败 %d）/ 边 %d 条（弃 %d）",
            summary["mode"],
            summary["clusters_total"], summary["batches_total"],
            summary["batches_failed"], summary["relations_written"],
            summary["relations_discarded"],
        )
        return summary

    # ------------------------------------------------------------------

    async def _run_batch(
        self,
        batch: list[dict],
        directory: dict[str, _DirectoryEntry],
        names_by_owner: dict[tuple[str, str], str],
    ) -> tuple[int, int]:
        """单批：名字预筛 -> LLM 提取 -> 校验 -> 落库。

        Returns:
            (写入边数, 校验丢弃数)。
        """
        by_id = {int(c["id"]): c for c in batch}
        # 批内词典：陈述子串命中的目录名（含 bot）——提示词有界
        names: list[str] = []
        statements = [str(c["canonical_statement"] or "") for c in batch]
        for name in directory:
            if any(name in s for s in statements):
                names.append(name)
        if not names:
            return 0, 0
        batch_entries = {n: directory[n] for n in sorted(names)}
        valid_uids = {e.uid for e in batch_entries.values()}

        system_prompt = self._loader.render(
            _RELATIONS_PROMPT_NAME,
            bot_nickname=self._bot_nickname or "本AI",
            bot_user_id=self._bot_user_id or "未知",
        )
        user_prompt = self._render_user_prompt(batch, batch_entries)
        raw = await self._llm_call(system_prompt, user_prompt)
        parsed = safe_parse_llm_json(raw)
        items = parsed.get("relations") if isinstance(parsed, dict) else None
        if not isinstance(items, list):
            raise ValueError("回填输出缺少 relations 数组")

        rows: list[dict] = []
        discarded = 0
        seen_struct_keys: set[tuple[str, str, str, str]] = set()
        for item in items:
            row = self._to_row(item, by_id, valid_uids, batch_entries, names_by_owner)
            if row is None:
                discarded += 1
                continue
            key = (row["platform"], row["subject_uid"], row["object_uid"], row["relation_label"])
            if key in seen_struct_keys:
                # 同批重复结构键：留首条（evidence_key 相同，落库也只会幂等）
                discarded += 1
                continue
            seen_struct_keys.add(key)
            rows.append(row)
        if rows:
            await self._db.upsert_entity_edge(
                rows, label_stopwords=self._config.relation_label_stopwords
            )
        return len(rows), discarded

    def _render_user_prompt(
        self, batch: list[dict], entries: dict[str, _DirectoryEntry]
    ) -> str:
        """用户提示词：事实行 + 名字→uid 词典。"""
        lines = ["【用户事实（每行一条，#簇ID [platform] uid=归属uid：陈述）】"]
        for c in batch:
            stmt = str(c["canonical_statement"] or "").replace("\n", " ")
            lines.append(f"#{c['id']} [{c['platform'] or '-'}] uid={c['user_id']}：{stmt}")
        lines.append("")
        lines.append("【名字→uid 词典（object_user_id 只能从这里取）】")
        for name, entry in entries.items():
            tag = " (bot)" if entry.uid == self._bot_user_id and not entry.platform else ""
            lines.append(f"{name}={entry.platform or '-'}:{entry.uid}{tag}")
        return "\n".join(lines)

    def _to_row(
        self,
        item: object,
        by_id: dict[int, dict],
        valid_uids: set[str],
        entries: dict[str, _DirectoryEntry],
        names_by_owner: dict[tuple[str, str], str],
    ) -> Optional[dict]:
        """LLM 元素 -> 边行；任一防御不通过返回 None。

        校验链：cluster_id 引用输入行 -> parse_relation 结构 ->
        subject == 簇归属 uid -> object ∈ 批词典 uid -> 平台一致
        （bot 通配）-> label 词形出现在源陈述。端点占位名（未知/
        uid 兜底形）用目录规范名顶替，未命中留空（upsert 保旧名）。
        """
        if not isinstance(item, dict):
            return None
        try:
            cluster_id = int(item.get("cluster_id"))
        except (TypeError, ValueError):
            return None
        cluster = by_id.get(cluster_id)
        if cluster is None:
            return None
        rel = parse_relation(item)
        if rel is None:
            return None
        owner = str(cluster["user_id"])
        if rel.subject_user_id != owner:
            return None
        if rel.object_user_id not in valid_uids:
            return None
        platform = str(cluster["platform"] or "")
        obj_entry = self._find_entry(entries, rel.object_user_id)
        if obj_entry is None:
            return None
        if obj_entry.platform and obj_entry.platform != platform:
            return None
        stmt = str(cluster["canonical_statement"] or "")
        if rel.label not in stmt:
            return None
        subject_name = rel.subject_display_name
        if is_placeholder_name(subject_name):
            subject_name = names_by_owner.get((platform, rel.subject_user_id), "")
        obj_name = rel.object_display_name
        if is_placeholder_name(obj_name):
            obj_name = names_by_owner.get((platform, rel.object_user_id)) or (
                next((n for n, e in entries.items() if e is obj_entry), "")
            )
        return {
            "platform": platform,
            "subject_uid": rel.subject_user_id,
            "object_uid": rel.object_user_id,
            "subject_name": subject_name,
            "object_name": obj_name,
            "relation_label": rel.label,
            "statement": rel.statement,
            "confidence": rel.confidence,
            "occurred_at": cluster["occurred_at"] or datetime.now(),
            "evidence_key": f"backfill|{cluster_id}",
            # 回填通道专用：命中已有边只刷新不计分（见模块 docstring 与
            # db.upsert_entity_edge——线上提取/补编码不传，默认照常计分）
            "count_on_conflict": False,
            "is_bot_edge": edge_has_bot_endpoint(
                rel.subject_user_id, rel.object_user_id, self._bot_forms()
            ),
            "min_evidence": self._config.relation_bot_edge_min_evidence,
        }

    @staticmethod
    def _find_entry(
        entries: dict[str, _DirectoryEntry], uid: str
    ) -> Optional[_DirectoryEntry]:
        for entry in entries.values():
            if entry.uid == uid:
                return entry
        return None


class BackfillController:
    """回填任务控制器：状态挂长期对象（插件实例），非 per-request API 对象。

    webui 插件按请求解析 get_web_ui（MemoryWebApi 每次新建），运行中的
    asyncio 任务与进度状态必须驻留更外层——本控制器由 main.py 装配并
    随插件生命周期存续，API 层只透传 start/status。
    """

    def __init__(self, factory: Callable[[], Optional[RelationBackfill]]) -> None:
        """装配控制器。

        Args:
            factory: 惰性构造 RelationBackfill（task_router 未装配时
                返回 None——由插件装配层决定可用性）。
        """
        self._factory = factory
        self._task: Optional[asyncio.Task] = None
        self.state: dict = self._fresh_state()

    @staticmethod
    def _fresh_state() -> dict:
        return {
            "running": False,
            "started_at": "",
            "finished_at": "",
            "done": 0,
            "total": 0,
            "relations": 0,
            "failed_batches": 0,
            "mode": "",
            "error": "",
        }

    def available(self) -> bool:
        """回填是否可用（factory 产出的 runner 非 None）。"""
        try:
            return self._factory() is not None
        except Exception:
            logger.warning("回填 runner 构造失败", exc_info=True)
            return False

    def start(self, full: bool = False) -> tuple[bool, str]:
        """启动回填任务；已在运行/不可用时拒绝。

        Args:
            full: True 忽略水位全量重跑（增量失败/簇陈述修正后补提取）。

        Returns:
            (是否已启动, 消息)。
        """
        if self.state.get("running"):
            return False, "回填已在运行中"
        runner = self._factory()
        if runner is None:
            return False, "task_router 未装配，回填不可用"
        state = self._fresh_state()
        state["running"] = True
        state["started_at"] = datetime.now().astimezone().isoformat()
        state["mode"] = "full" if full else "incremental"

        def _progress(p: dict) -> None:
            state["done"] = p.get("done", state["done"])
            state["total"] = p.get("total", state["total"])
            state["relations"] = p.get("relations", state["relations"])

        async def _job() -> None:
            try:
                summary = await runner.run(progress=_progress, force_full=full)
                state["failed_batches"] = summary.get("batches_failed", 0)
                state["mode"] = summary.get("mode", state["mode"])
                state["error"] = ""
            except Exception as exc:
                logger.warning("关系回填任务异常终止", exc_info=True)
                state["error"] = str(exc)
            finally:
                state["running"] = False
                state["finished_at"] = datetime.now().astimezone().isoformat()
                self._task = None

        self.state = state
        self._task = asyncio.create_task(_job())
        return True, "回填已启动"

    async def stop(self) -> None:
        """插件停机清理：取消运行中的回填任务。"""
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self.state = self._fresh_state()

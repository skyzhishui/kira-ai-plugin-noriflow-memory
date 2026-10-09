"""Legacy relation backfill: confirmed fact clusters -> memory_entity_edge (one-off maintenance task).

P2 relation extraction only applies to incremental encoding batches;
relations sedimented in historical confirmed facts (active/profiled
clusters) need a one-time backfill before entering the edge table. This
module is manually triggered from the maintenance page ("backfill from
confirmed facts" in the relation-graph section) and does not run on the
regular pipeline.

Data flow (zero-LLM-trust principle, defended per entry):
1. source: fetch_confirmed_relation_sources (active+profiled clusters, id ascending);
2. directory: fetch_alias_directory full table -> pure function
   _build_directory disambiguates (same-name multi-uid skipped,
   determinism first); the bot nickname explicitly enters the directory (bot);
3. batches: cluster_batch_size entries per batch; only names whose
   substrings hit a batch's statement enter that batch's dictionary
   (bounded prompt); batches with no resolvable name skip the LLM and
   return 0 directly;
4. LLM: one prompts/fact_relations.prompt call producing the relations array;
5. validation: parse_relation (label 2-8 chars/statement<=200/confidence
   domain) + cluster_id must reference an input row + subject must equal
   that cluster's owning uid + object must be inside the batch-dictionary
   uid set + the label form must appear in the source statement; endpoint
   placeholder names (unknown/uid-fallback forms) replaced by the
   directory canonical name;
6. persist: db.upsert_entity_edge with evidence_key = backfill|{cluster_id}
   (rerun idempotent, no count inflation); occurred_at taken from the
   cluster occurred_at; bot edges still go pending + min_evidence threshold
   (backfill does not count as double evidence; activation still needs
   online reproduction).

Failure semantics: a batch failure (LLM unavailable/unparseable output)
counts and stops immediately; the failed batch and clusters after it keep
the watermark unadvanced and are auto-recovered on the next rerun; full
rerun (force_full) ignores the watermark. Counting semantics: backfill rows
use count_on_conflict=False, a conflict path hitting an existing edge only
refreshes without scoring (prevents online extraction +1 then backfill echo
+1, and backfill echo from reaching the bot-edge double-evidence threshold);
new-row inserts still count 1; reruns have zero side effects on the data.
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
    """MemoryDatabase or bare pool -> bare pool (works for callers on either side of the seam)."""
    return db_or_pool.pool if hasattr(db_or_pool, "pool") else db_or_pool


async def _kv_get(db_or_pool, key: str):
    """kv read: MemoryDatabase.get_kv preferred, bare pool goes through SQL."""
    if hasattr(db_or_pool, "get_kv"):
        return await db_or_pool.get_kv(key)
    pool = _kv_pool(db_or_pool)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT value FROM _memory_local_kv WHERE key = $1", key
        )
        return row["value"] if row else None


async def _kv_set(db_or_pool, key: str, value: str) -> None:
    """kv write: MemoryDatabase.set_kv preferred, bare pool goes through SQL."""
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
    """Register revived clusters as backfill todos (append semantics; dedup, order-preserved within the capacity cap).

    Args:
        db_or_pool: MemoryDatabase / asyncpg pool / a stub with get_kv+set_kv.
        cluster_ids: Revived cluster id list.

    Returns:
        Number of clusters newly registered this call.
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
    """Take the backfill todos (read then clear; on failure keep them for the next round).

    Returns:
        Todo cluster id list (empty list = no todos or read failed).
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
    """Directory entry: name -> (platform, uid). Empty platform = wildcard (bot)."""

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
    """Build a name -> directory-entry map (ambiguous names dropped entirely, same ruling as alias_store.match).

    Args:
        alias_rows: Full-table (platform, uid, name) rows.
        bot_user_id: Bot platform uid (enters the directory with a wildcard
            platform when non-empty).
        bot_nickname: Bot nickname (directory key).

    Returns:
        name -> _DirectoryEntry; names resolving to multiple uids do not appear
        (determinism first).
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
    """Legacy relation backfill task (manually triggered from the maintenance page, rerunnable idempotently)."""

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
        """Assemble the backfiller.

        Args:
            llm_call: LLM exit (system_prompt, user_prompt) -> raw output
                text: the two hosts' exits differ (upstream nori / kira
                run_structured), wrapped by the assembly-layer closure so this
                module stays byte-identical across both sides.
            db: Memory store connection pool.
            config: Plugin config (relation_bot_edge_min_evidence).
            bot_user_id: Bot platform uid (bot-edge ruling + directory bot entry).
            bot_nickname: Bot nickname.
            prompt_dir: Prompt directory; None uses this plugin's prompts/.
            batch_size: Number of clusters per batch.
            bot_forms_provider: Reader for the bot uid's full-form set
                (kernel._bot_uid_forms_all semantics, bot-endpoint ruling
                aligned with the retain channel; None degrades to the
                single form bot_user_id).
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
        """Bot uid full-form set (provider preferred; degrades to single-form uid)."""
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
        """Run the backfill (id-window paging per batch; a single-batch failure stops, the remainder defers to the next run).

        Watermark incremental: in non-full mode start from clusters after kv
        watermark (relation_backfill_watermark); on each successful batch the
        watermark advances to that batch's max cluster id; a failure stops
        there, the failed batch and clusters after it stay unadvanced and are
        auto-recovered on the next run. Full mode ignores the watermark (used
        when cluster statements were corrected and re-extraction is wanted;
        count_on_conflict=False guarantees a rerun only refreshes without
        scoring). The source query pages by batch_size window (a large-table
        full run no longer loads everything into memory). Revived-cluster
        todos (merge/maintenance-page revival, clusters below the watermark)
        are taken at the start of each round and fetched precisely by id,
        placed before incremental batches for re-extraction; clusters that
        fail precise-fetch/batch-processing are re-registered as todos
        (incremental retry next round).

        Args:
            progress: Per-batch progress callback (receives {done,total,relations}).
            force_full: True ignores the watermark for a full rerun.

        Returns:
            Summary dict: clusters_total/batches_total/batches_failed/
            relations_written/relations_discarded/mode.
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
        # 复活簇待办：水位之下的复活簇（merge/手工复活）按 id 精取。
        # take 即清空——失败面（精取失败/批处理失败）须重新登记回待办，
        # 否则水位之下的簇在增量模式永远失去重试机会（只能全量重跑）
        revived_pending = await take_backfill_pending(self._db)
        revived = []
        revived_retry: set[int] = set()
        if revived_pending:
            try:
                revived = await self._db.fetch_confirmed_relation_sources_by_ids(
                    revived_pending
                )
            except Exception:
                logger.warning("复活簇待办精取失败（本批重新登记待办）",
                               exc_info=True)
                await self._requeue_backfill_pending(revived_pending)
                revived = []
            else:
                revived_retry = {int(c["id"]) for c in revived}
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
            revived_retry -= {int(c["id"]) for c in batch}
        if revived_retry:
            # 中途失败的复活批：重新登记待办，下轮增量重试
            await self._requeue_backfill_pending(sorted(revived_retry))
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

    async def _requeue_backfill_pending(self, cluster_ids: list[int]) -> None:
        """Re-register failed revived-cluster todos (fail-open: a registration failure only logs; a full rerun covers it)."""
        try:
            await mark_backfill_pending(self._db, cluster_ids)
        except Exception:
            logger.warning(
                "复活簇待办重登记失败（cluster_ids=%s，可全量重跑补）",
                cluster_ids[:10],
                exc_info=True,
            )

    async def _run_batch(
        self,
        batch: list[dict],
        directory: dict[str, _DirectoryEntry],
        names_by_owner: dict[tuple[str, str], str],
    ) -> tuple[int, int]:
        """Single batch: name pre-filter -> LLM extraction -> validation -> persist.

        Returns:
            (edges written, validation-discarded count).
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
        """User prompt: fact rows + name->uid dictionary."""
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
        """LLM element -> edge row; return None when any defense fails.

        Validation chain: cluster_id references an input row -> parse_relation
        structure -> subject == the cluster's owning uid -> object inside the
        batch-dictionary uid set -> platform consistent (bot wildcard), label
        form appears in the source statement. Endpoint placeholder names
        (unknown/uid-fallback forms) are replaced with the directory canonical
        name; unmatched ones stay empty (upsert keeps the old name).
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
    """Backfill task controller: state lives on a long-lived object (the plugin instance), not a per-request API object.

    The webui plugin parses get_web_ui per request (MemoryWebApi is built
    each time); running asyncio tasks and progress state must dwell at an
    outer layer. This controller is assembled by main.py and lives with the
    plugin lifecycle; the API layer only passes through start/status.
    """

    def __init__(self, factory: Callable[[], Optional[RelationBackfill]]) -> None:
        """Assemble the controller.

        Args:
            factory: Lazily constructs RelationBackfill (returns None when
                task_router is not assembled, availability decided by the
                plugin assembly layer).
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
        """Whether backfill is available (the runner produced by the factory is non-None)."""
        try:
            return self._factory() is not None
        except Exception:
            logger.warning("回填 runner 构造失败", exc_info=True)
            return False

    def start(self, full: bool = False) -> tuple[bool, str]:
        """Start the backfill task; reject when already running/unavailable.

        Args:
            full: True ignores the watermark for a full rerun (incremental
                failure or re-extraction after cluster statement correction).

        Returns:
            (started or not, message).
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
        """Plugin-shutdown cleanup: cancel a running backfill task."""
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self.state = self._fresh_state()

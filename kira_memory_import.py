"""Lossless import of KiraAI host memory data (dual-backend plan P4,
2026-09-12).

Source surfaces (all opened read-only; the source tree is never written):
- ``chat_memory.json``: ``{session: {title, description, memory:
  [chunk][msg]}}``, each chunk -> a memory_chat_summary row
  (kind=chat_summary, summarized=false, embedding=NULL) which the merge
  agent's encode catch-up pass distills via LLM into summaries + facts +
  relation edges (same path as the host pipeline's degraded raw text,
  docs/plans/noriflow-dual-storage-backend-plan.md §11); host system
  notices (the ``Notice [... user_id: system_*]`` signature, scheduled-task
  feeds etc.) are not imported — no long-term-memory value (user decision
  2026-09-12); real message dumps are unaffected;
- ``entities/{user,group}_<quoted_id>/{facts,reflections}/*.toml`` and
  ``global/{facts,global/self/{facts,reflections}}/*.toml``:
  -> memory_persona_fact_raw rows (extracted_flag=0) that enter
  profiles/recall after the merge agent's normalization pass dedups them
  into clusters;
- ``entities/*/profile.json``: name/nickname/aliases -> memory_entity_alias
  rows (source="kira_memory_import").

Idempotency (safe to re-run): every write reuses the live pipeline's
idempotency keys —
- summary document_id = ``{session_id}-{md5(content)[:12]}`` (same formula
  as kernel, memory_kernel.py ingest);
- fact document_id = ``fact_document_id(uids, session_id, occurred_at)
  -{md5(statement)[:12]}`` (same formula as kernel);
- alias rows upsert on the unique key (platform, user_id, name).
Re-imported or doubly-distilled content hits the same keys and is silently
skipped by the database — no duplicate rows; import statistics are measured
by row-count deltas on the three tables (no reliance on return values).

Explicitly not imported (counted and explained in the result): ``archive/``
(memories KiraAI's forgetting cycle already deleted — reviving them
violates that semantics), ``global/skills`` and ``skills/`` (skill
subsystem, not memory), ``memory_index.db`` (runtime index, not the source
of truth), ``core.txt`` (prompt).

Namespace mapping: KiraAI session/entity ids carry an adapter prefix
(``"seki:dm:9900000002"`` / entity ``"seki:9900000002"``) — the adapter
segment is the host platform, and after stripping the dm/gm type segment
the ids match the live memory_chat_summary.session_id (bare id) and
participants ("platform:uid" composite keys), so recall/profile injection
connects seamlessly. Group-entity facts use the group id as the owning uid
(the noriflow fact table is modeled per-user and group-level knowledge has
no dedicated carrier yet; the rows are stored and visible but not yet
covered by injection, counted honestly in the result). Global facts without
an entity belong to the bot itself (platform/uid injected by the caller;
when unavailable the domain is skipped and counted — better missing than a
fabricated ownership).
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover - Python 3.10 takes the tomli branch
    import tomli as tomllib

from .db.base import fact_document_id
from .envelope import format_history_message

# kv marker key (_memory_local_kv): JSON snapshot of the latest import result
KIRA_MEMORY_IMPORT_KV_KEY = "kira_memory_import_last_run"

# Session ids look like "{adapter}:{dm|gm}:{bare_id}"; non-session types
# such as sm never map to memory entities (same whitelist as KiraAI
# memory_manager._parse_entity_from_session)
_SESSION_RE = re.compile(r"^(?P<platform>[^:]+):(?P<stype>dm|gm):(?P<sid>.+)$")

# Entity directory names "{type}_{urlquoted_id}" (inverse of
# memory_paths._id_to_path_segment)
_ENTITY_DIR_RE = re.compile(r"^(?P<etype>user|group|channel)_(?P<quoted>.+)$")

# In-body timestamp (the format the host feeds KiraAI history in):
# "[Sep 10 2026 18:44 Thu]"
_CONTENT_TS_RE = re.compile(
    r"\[([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?\s+[A-Z][a-z]{2}\]"
)
_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# Host system-notice signature (the Notice variant of KiraAI's built-in
# kira-ai plugin _format_user_message):
# "[Sep 10 2026 11:00 Thu] Notice [user_id: system_qzone_task] | ...".
# Every host message fed to KiraAI carries "Notice"/"[message_id:" wrapping —
# the discriminating feature of a system notice is a system_* pseudo account
# (scheduled tasks etc.) as user_id inside the Notice brackets; real messages
# (group-chat dumps included) carry a real user_id. Such content has no
# long-term-memory value and is not imported (user decision, 2026-09-12);
# the signature is only matched at the head of the body so that quotations
# inside the body never hit.
_SYSTEM_NOTICE_RE = re.compile(r"Notice\s*\[[^\]]*user_id:\s*system[\w-]*")

# Fact statements land in the ownership dimension (the profile's "known
# facts" section); reflections (higher-order insights) are equally
# distilled conclusions and also land in stable — the text is not modified
# with a type prefix, keeping idempotent alignment with the live pipeline
_IMPORT_CATEGORY = "stable"

_TOML_ENTITY_DIRS = ("entities",)
_TOML_GLOBAL_DIRS = (("global/facts", ""), ("global/self/facts", "self"),
                     ("global/self/reflections", "self"))


def parse_kira_memory_session(session: str) -> Optional[dict]:
    """Parse a KiraAI session id into live-namespace fields.

    Returns:
        {"platform", "session_id", "group_id", "user_id"}; private chats
        have session_id=user_id=bare uid with an empty group_id, group
        chats have session_id=group_id=bare gid. Non-session types or
        malformed ids return None.
    """
    m = _SESSION_RE.match(str(session or ""))
    if not m:
        return None
    platform, sid = m.group("platform"), m.group("sid")
    if m.group("stype") == "dm":
        return {"platform": platform, "session_id": sid,
                "group_id": "", "user_id": sid}
    return {"platform": platform, "session_id": sid,
            "group_id": sid, "user_id": ""}


def parse_content_timestamp(content: str) -> Optional[datetime]:
    """Parse the "[Sep 10 2026 18:44 Thu]" timestamp embedded at the head
    of a message body.

    KiraAI message dicts carry no separate time field (main.py builds
    chunks with role/content/sender_id/sender_name only); the time lives in
    the body the host fed in. The result is a naive local datetime (produced
    by the host's local clock); unparseable bodies return None.
    """
    m = _CONTENT_TS_RE.search(str(content or "")[:64])
    if not m:
        return None
    month = _MONTHS.get(m.group(1))
    if month is None:
        return None
    try:
        return datetime(
            int(m.group(3)), month, int(m.group(2)),
            int(m.group(4)), int(m.group(5)), int(m.group(6) or 0),
        )
    except ValueError:
        return None


def is_system_notice(msg: dict) -> bool:
    """Whether this is a host system notice (no sender metadata + the
    Notice/system_* signature at the head of the body).

    Only scheduled-task feeds from system pseudo accounts (system_qzone_task
    etc.) hit; Notices carrying a real user_id (platform events like group
    membership changes) and plain message dumps never count.
    """
    if not isinstance(msg, dict) or msg.get("role") != "user":
        return False
    if str(msg.get("sender_id") or "").strip():
        return False
    content = str(msg.get("content") or "")
    return bool(_SYSTEM_NOTICE_RE.search(content[:200]))


def compose_chunk_content(chunk: list, bot_nickname: str = "") -> str:
    """Assemble a KiraAI chunk (message list) into envelope line text (the
    encode-input line format).

    User messages with a sender carry uid/name attributes (the ownership
    evidence for facts); senderless dumps carry no ownership attributes
    (uid evidence must not be fabricated); assistant lines carry
    self="true". Host system notices (is_system_notice) are not imported.
    Bodies go through format_history_message's anti-spoofing scrub first
    (embedded "<msg" payload fragments get neutralized). Empty messages are
    skipped.
    """
    lines: list[str] = []
    for msg in chunk if isinstance(chunk, list) else []:
        if not isinstance(msg, dict):
            continue
        if is_system_notice(msg):
            continue
        content = str(msg.get("content") or "").strip()
        if not content:
            continue
        role = str(msg.get("role") or "")
        ts = parse_content_timestamp(content)
        if role == "assistant":
            lines.append(format_history_message(
                bot_nickname or "Bot", content, timestamp=ts, is_self_message=True,
            ))
        elif role == "user":
            sender_id = str(msg.get("sender_id") or "").strip()
            sender_name = str(msg.get("sender_name") or "").strip()
            if sender_id:
                lines.append(format_history_message(
                    sender_name or sender_id, content, timestamp=ts, user_id=sender_id,
                ))
            else:
                lines.append(format_history_message("", content, timestamp=ts))
        # Other roles (system etc.) never enter memory
    return "\n".join(lines)


def chunk_occurred_at(chunk: list, fallback: datetime) -> datetime:
    """Chunk occurrence time: the latest parseable embedded timestamp in
    the bodies, falling back to fallback."""
    stamps = [
        ts for ts in (
            parse_content_timestamp(str(m.get("content") or ""))
            for m in chunk if isinstance(m, dict)
        ) if ts is not None
    ]
    return max(stamps) if stamps else fallback


def scan_source(source_path: str | Path) -> dict:
    """Scan the source directory read-only and return preview counts (no
    writes at all; feeds the confirmation dialog)."""
    source = Path(source_path)
    out: dict[str, Any] = {
        "source_path": str(source),
        "exists": source.is_dir(),
        "sessions": 0, "chunks": 0, "messages": 0,
        "toml_files": 0, "profiles": 0, "archive_files": 0,
        "parse_errors": 0,
    }
    if not out["exists"]:
        return out

    chat_path = source / "chat_memory.json"
    if chat_path.is_file():
        try:
            data = json.loads(chat_path.read_text(encoding="utf-8"))
        except Exception:
            out["parse_errors"] += 1
            data = {}
        for sess, body in data.items() if isinstance(data, dict) else []:
            if not isinstance(body, dict) or not parse_kira_memory_session(sess):
                continue
            mem = body.get("memory")
            if not isinstance(mem, list):
                continue
            out["sessions"] += 1
            out["chunks"] += len(mem)
            out["messages"] += sum(
                len(c) for c in mem if isinstance(c, list)
            )

    for toml_path, _ns, _p, _u in _iter_toml_files(source):
        out["toml_files"] += 1

    for profile_path in _iter_profile_files(source):
        out["profiles"] += 1

    archive = source / "archive"
    if archive.is_dir():
        out["archive_files"] = sum(
            1 for p in archive.rglob("*") if p.is_file()
        )
    return out


def _iter_profile_files(source: Path):
    """Scan entities/*/profile.json (read-only)."""
    entities = source / "entities"
    if not entities.is_dir():
        return
    for entry in sorted(entities.iterdir()):
        profile = entry / "profile.json"
        if entry.is_dir() and profile.is_file():
            yield profile


def _iter_toml_files(source: Path):
    """Scan importable TOML memory files.

    Yields:
        (path, namespace, platform, owner_uid); namespace is "entity"
        (entity domain) or "bot" (global domain, owned by the bot). Files
        whose entity-domain directory name fails to parse count as parse
        errors and are skipped.
    """
    seen: set[Path] = set()

    for base in _TOML_ENTITY_DIRS:
        root = source / base
        if not root.is_dir():
            continue
        for entry in sorted(root.iterdir()):
            if not entry.is_dir():
                continue
            m = _ENTITY_DIR_RE.match(entry.name)
            if not m:
                continue
            owner = unquote(m.group("quoted"))
            platform, _, uid = owner.partition(":")
            etype = m.group("etype")
            if etype == "channel":
                continue  # KiraAI channel entities have no ownership semantics here
            for folder in ("facts", "reflections"):
                folder_dir = entry / folder
                if not folder_dir.is_dir():
                    continue
                for f in sorted(folder_dir.glob("*.toml")):
                    if f not in seen:
                        seen.add(f)
                        yield f, "entity", platform, uid

    for rel, _ns in _TOML_GLOBAL_DIRS:
        root = source / rel
        if not root.is_dir():
            continue
        for f in sorted(root.glob("*.toml")):
            if f not in seen:
                seen.add(f)
                yield f, "bot", "", ""


def _load_toml(path: Path) -> Optional[dict]:
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except Exception:
        return None


def _to_local_naive(value: datetime) -> datetime:
    """Normalize to a naive local datetime (same policy as
    kernel._to_local for the document_id date segment: naive is taken as
    local, aware converts to local)."""
    if value.tzinfo is None:
        return value
    return value.astimezone().replace(tzinfo=None)


def _toml_occurred_at(data: dict, path: Path) -> datetime:
    """TOML memory occurrence time: [meta].timestamp (epoch seconds) >
    [source].time (ISO) > file mtime. Normalized to naive local."""
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    try:
        raw_ts = meta.get("timestamp")
        if raw_ts:
            return _to_local_naive(
                datetime.fromtimestamp(float(raw_ts), tz=timezone.utc)
            )
    except (TypeError, ValueError):
        pass
    src = data.get("source") if isinstance(data.get("source"), dict) else {}
    raw_time = src.get("time")
    if isinstance(raw_time, datetime):
        return _to_local_naive(raw_time)
    if isinstance(raw_time, str):
        try:
            return _to_local_naive(datetime.fromisoformat(raw_time))
        except ValueError:
            pass
    return datetime.fromtimestamp(path.stat().st_mtime)


def _toml_fact_row(
    data: dict, path: Path, *, platform: str, user_id: str,
    display_name: str, session_id: str, group_id: str,
) -> Optional[dict]:
    """TOML memory -> insert_persona_fact_raw argument set (same
    idempotency-key formula as kernel)."""
    statement = str(data.get("text") or "").strip()
    if not statement:
        return None
    try:
        importance = max(1, min(10, int(data.get("importance", 5))))
    except (TypeError, ValueError):
        importance = 5
    occurred_at = _toml_occurred_at(data, path)
    content_hash = hashlib.md5(statement.encode("utf-8")).hexdigest()[:12]
    document_id = (
        f"{fact_document_id([user_id], session_id, occurred_at)}-{content_hash}"
    )
    return {
        "document_id": document_id,
        "platform": platform,
        "user_id": user_id,
        "related_user_ids": [],
        "display_name": display_name,
        "category": _IMPORT_CATEGORY,
        "statement": statement,
        "confidence": "high" if importance >= 7 else "medium",
        "session_id": session_id,
        "group_id": group_id,
        "evidence_key": f"{session_id}|{occurred_at.strftime('%Y-%m-%d')}",
        "occurred_at": occurred_at,
        "embedding": None,
    }


async def run_kira_memory_import(
    db,
    *,
    source_path: str | Path,
    bot_platform: str = "",
    bot_uid: str = "",
    bot_nickname: str = "",
) -> dict:
    """Run the import (source read-only; target written entirely through
    idempotency keys). Returns a stats dict.

    Args:
        db: MemoryBackend (already connected; backend-agnostic — only
            interface methods and the pool's execute/fetchval surface).
        source_path: KiraAI memory data root (containing
            chat_memory.json/entities/...).
        bot_platform / bot_uid: owning bot for global-domain facts (the
            domain is skipped when unavailable).
        bot_nickname: display name for envelope assistant lines.

    Raises:
        FileNotFoundError: source directory missing.
        ValueError: source directory has neither chat_memory.json nor
            entities/ (not a KiraAI data root — guards against a wrong
            path).
    """
    source = Path(source_path)
    if not source.is_dir():
        raise FileNotFoundError(f"KiraAI 数据目录不存在: {source}")
    if not (source / "chat_memory.json").is_file() and not (source / "entities").is_dir():
        raise ValueError(
            f"{source} 下没有 chat_memory.json 或 entities/，不像 KiraAI 记忆数据根"
        )

    stats: dict[str, Any] = {
        "source_path": str(source),
        "sessions": 0, "chunks_total": 0,
        "system_notice_msgs_skipped": 0, "chunks_bot_only_skipped": 0,
        "summary_rows": 0, "summary_skipped": 0,
        "facts_total": 0, "fact_rows": 0, "facts_skipped": 0,
        "aliases_upserted": 0, "profiles_read": 0,
        "global_facts_skipped_no_bot": 0,
        "parse_errors": 0, "errors": [],
    }

    counts_before = await _table_counts(db)

    # ---- 1. chat_memory.json -> summary table (summarized=false, pending
    # encode catch-up) ----
    chat_rows: list[dict] = []
    session_platforms: list[str] = []
    chat_path = source / "chat_memory.json"
    if chat_path.is_file():
        try:
            chat_data = json.loads(chat_path.read_text(encoding="utf-8"))
        except Exception as exc:
            stats["parse_errors"] += 1
            stats["errors"].append(f"chat_memory.json 解析失败: {exc}")
            chat_data = {}
        file_mtime = _to_local_naive(
            datetime.fromtimestamp(chat_path.stat().st_mtime)
        )
        if isinstance(chat_data, dict):
            for sess, body in chat_data.items():
                parsed = parse_kira_memory_session(sess)
                if not parsed or not isinstance(body, dict):
                    continue
                mem = body.get("memory")
                if not isinstance(mem, list):
                    continue
                stats["sessions"] += 1
                session_platforms.append(parsed["platform"])
                platform = parsed["platform"]
                session_id = parsed["session_id"]
                for chunk in mem:
                    if not isinstance(chunk, list):
                        continue
                    stats["chunks_total"] += 1
                    stats["system_notice_msgs_skipped"] += sum(
                        1 for m in chunk if is_system_notice(m)
                    )
                    # Batches whose user-side content is entirely system
                    # notices (or absent) are not imported: rows reduced to
                    # bot replies (often empty-payload placeholders) hold no
                    # long-term-memory value, and skipping them saves the
                    # encode catch-up budget (a natural extension of the
                    # system-notice filter, user decision 2026-09-12)
                    if not any(
                        isinstance(m, dict)
                        and m.get("role") == "user"
                        and not is_system_notice(m)
                        and str(m.get("content") or "").strip()
                        for m in chunk
                    ):
                        stats["chunks_bot_only_skipped"] += 1
                        continue
                    content = compose_chunk_content(chunk, bot_nickname)
                    if not content:
                        continue
                    occurred_at = chunk_occurred_at(chunk, file_mtime)
                    # Trigger: private chat = the peer user; group chat = the
                    # first sender known inside the chunk (KiraAI chunks
                    # often carry no sender metadata — leaving it empty
                    # follows the bot_self-row precedent; never fabricate)
                    user_id = parsed["user_id"]
                    participants: list[str] = []
                    if not user_id:
                        for msg in chunk:
                            if (
                                isinstance(msg, dict)
                                and msg.get("role") == "user"
                                and str(msg.get("sender_id") or "").strip()
                            ):
                                user_id = str(msg["sender_id"]).strip()
                                break
                    if user_id:
                        participants.append(f"{platform}:{user_id}")
                    for msg in chunk:
                        if not isinstance(msg, dict) or msg.get("role") != "user":
                            continue
                        sid = str(msg.get("sender_id") or "").strip()
                        if sid:
                            key = f"{platform}:{sid}"
                            if key not in participants:
                                participants.append(key)
                    chat_rows.append({
                        "document_id": (
                            f"{session_id}-"
                            f"{hashlib.md5(content.encode('utf-8')).hexdigest()[:12]}"
                        ),
                        "kind": "chat_summary",
                        "platform": platform,
                        "session_id": session_id,
                        "group_id": parsed["group_id"],
                        "user_id": user_id,
                        "participants": participants,
                        "content": content,
                        "occurred_at": occurred_at,
                        "embedding": None,
                        "summarized": False,
                    })

    # ---- 2. TOML memories -> fact table (extracted_flag=0, pending
    # normalization) ----
    # display_name comes from the entity profile's name/nickname (read
    # separately after the TOML scan)
    display_names = _profile_display_names(source, stats)
    fact_rows: list[dict] = []
    alias_rows: list[dict] = []
    for path, namespace, platform, uid in _iter_toml_files(source):
        data = _load_toml(path)
        if data is None:
            stats["parse_errors"] += 1
            stats["errors"].append(f"TOML 解析失败: {path.name}")
            continue
        if namespace == "bot":
            # When the bot's platform is not injected, infer it from the
            # source session prefixes (one deployment's adapter prefix is
            # the host platform, e.g. "seki:dm:..." -> "seki")
            inferred = _infer_platform(session_platforms)
            eff_platform = bot_platform or inferred
            if not eff_platform or not bot_uid:
                stats["global_facts_skipped_no_bot"] += 1
                continue
            platform, uid = eff_platform, bot_uid
        if not platform or not uid:
            stats["global_facts_skipped_no_bot"] += 1
            continue
        stats["facts_total"] += 1
        # Fact source session: TOML [source].session (a KiraAI session id)
        # with the type segment stripped; sessions without a source record
        # empty (document_id/evidence_key degrade to global keys, still
        # idempotent)
        src = data.get("source") if isinstance(data.get("source"), dict) else {}
        parsed_src = parse_kira_memory_session(str(src.get("session") or ""))
        session_id = parsed_src["session_id"] if parsed_src else ""
        group_id = parsed_src["group_id"] if parsed_src else ""
        row = _toml_fact_row(
            data, path, platform=platform, user_id=uid,
            display_name=display_names.get((platform, uid), ""),
            session_id=session_id, group_id=group_id,
        )
        if row is not None:
            fact_rows.append(row)

    # ---- 3. profile.json -> alias rows ----
    for profile_path in _iter_profile_files(source):
        try:
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
        except Exception:
            stats["parse_errors"] += 1
            continue
        if not isinstance(profile, dict):
            continue
        stats["profiles_read"] += 1
        m = _ENTITY_DIR_RE.match(profile_path.parent.name)
        owner = unquote(m.group("quoted")) if m else ""
        platform, _, uid = owner.partition(":")
        if not platform or not uid:
            continue
        try:
            last_seen = _to_local_naive(datetime.fromtimestamp(
                float(profile.get("last_interaction") or 0), tz=timezone.utc,
            )) if profile.get("last_interaction") else datetime.now().replace(tzinfo=None)
        except (TypeError, ValueError):
            last_seen = datetime.now().replace(tzinfo=None)
        names: list[str] = []
        for key in ("name", "nickname"):
            value = str(profile.get(key) or "").strip()
            if value and value not in names:
                names.append(value)
        for value in profile.get("aliases") or []:
            value = str(value or "").strip()
            if value and value not in names:
                names.append(value)
        for name in names:
            alias_rows.append({
                "platform": platform, "user_id": uid, "name": name,
                "last_seen": last_seen, "source": "kira_memory_import",
            })
    stats["aliases_upserted"] = len(alias_rows)

    # ---- 4. writes (idempotent-key conflicts silently skipped by the DB) ----
    for row in chat_rows:
        await db.insert_chat_summary(**row)
    for row in fact_rows:
        await db.insert_persona_fact_raw(**row)
    if alias_rows:
        await db.alias_upsert(alias_rows)

    counts_after = await _table_counts(db)
    stats["summary_rows"] = max(
        0, counts_after["summary"] - counts_before["summary"]
    )
    stats["summary_skipped"] = max(0, len(chat_rows) - stats["summary_rows"])
    stats["fact_rows"] = max(0, counts_after["facts"] - counts_before["facts"])
    stats["facts_skipped"] = max(0, len(fact_rows) - stats["fact_rows"])

    # ---- 5. kv marker (admin page shows the latest import result) ----
    marker = {
        "finished_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source_path": str(source),
        **{k: v for k, v in stats.items() if k != "errors"},
    }
    try:
        await db.set_kv(KIRA_MEMORY_IMPORT_KV_KEY, json.dumps(marker, ensure_ascii=False))
    except Exception as exc:
        stats["errors"].append(f"kv 标记写入失败: {exc}")

    return stats


async def _table_counts(db) -> dict[str, int]:
    """Row counts of the three target tables (idempotent keys silently
    skip => actually-inserted rows are measured by deltas)."""
    out = {"summary": 0, "facts": 0, "aliases": 0}
    pool = db.pool
    async with pool.acquire() as conn:
        out["summary"] = int(
            await conn.fetchval("SELECT count(*) FROM memory_chat_summary") or 0
        )
        out["facts"] = int(
            await conn.fetchval("SELECT count(*) FROM memory_persona_fact_raw") or 0
        )
        out["aliases"] = int(
            await conn.fetchval("SELECT count(*) FROM memory_entity_alias") or 0
        )
    return out


async def read_last_run(db) -> Optional[dict]:
    """Read the kv marker (shown on the admin page; None when absent)."""
    try:
        raw = await db.get_kv(KIRA_MEMORY_IMPORT_KV_KEY)
    except Exception:
        return None
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


# Pre-v1.16.1 marker key (module was kiraos_import.py; kept only for the
# one-time carry-over below — never written by current code)
_LEGACY_KV_KEY = "kiraos_import_last_run"


async def migrate_legacy_import_kv(db) -> None:
    """One-time rename carry-over: move the pre-v1.16.1 import marker to the
    renamed key so existing deployments keep their last-run record in the
    overview card and import panel. Idempotent; the new key always wins, and
    the legacy row is dropped after the copy (the overview kv dump is generic,
    so a leftover legacy row would render as an unlabeled raw item)."""
    if await db.get_kv(KIRA_MEMORY_IMPORT_KV_KEY) is not None:
        return
    legacy = await db.get_kv(_LEGACY_KV_KEY)
    if legacy is None:
        return
    await db.set_kv(KIRA_MEMORY_IMPORT_KV_KEY, legacy)
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM _memory_local_kv WHERE key = $1", _LEGACY_KV_KEY
        )


def _infer_platform(session_platforms: list[str]) -> str:
    """Pick the most frequent platform prefix among source sessions (bot
    ownership-platform inference)."""
    counts: dict[str, int] = {}
    for p in session_platforms:
        counts[p] = counts.get(p, 0) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _profile_display_names(source: Path, stats: dict) -> dict[tuple[str, str], str]:
    """(platform, uid) -> profile display name (display_name source for
    TOML fact rows)."""
    out: dict[tuple[str, str], str] = {}
    for profile_path in _iter_profile_files(source):
        try:
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
        except Exception:
            stats["parse_errors"] += 1
            continue
        if not isinstance(profile, dict):
            continue
        m = _ENTITY_DIR_RE.match(profile_path.parent.name)
        if not m:
            continue
        owner = unquote(m.group("quoted"))
        platform, _, uid = owner.partition(":")
        name = str(profile.get("name") or "").strip() or str(
            profile.get("nickname") or ""
        ).strip()
        if platform and uid and name:
            out[(platform, uid)] = name
    return out


def default_source_path(plugin_data_dir: str | Path) -> str:
    """Default source path inference: host data root/memory (two levels
    above plugin_data/<pid>).

    KiraAI host's memory root = the host's data/memory (memory_paths
    default + the set_data_root(get_data_path()/memory) injection);
    measured on the reference deployment as /data/KiraAI/data/memory. Falls back
    to ./data/memory when inference fails.
    """
    d = Path(plugin_data_dir)
    if d.parent.name == "plugin_data":
        return str(d.parent.parent / "memory")
    return str(Path("data") / "memory")

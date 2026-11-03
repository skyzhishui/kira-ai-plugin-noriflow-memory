"""Self-contained stub tests for kira-ai-plugin-noriflow-memory.

Does not depend on the real KiraAI core or PostgreSQL: core.* is injected as
stub modules, asyncpg uses the real package, and DB interactions are handled
by FakeDB/FakePool stand-ins. Run:

    python tests/test_noriflow_memory.py

Coverage: config fault tolerance / envelope format / encoder parsing / merge-verdict decisions / kernel dual-channel
write with bot fact filtering / persona assembly and blacklist / the three core tools / enabled_tools gating /
retain orchestration (turn signal, watermark incremental, failure rollback) / auto-disabling simple_memory /
maintenance API validation/degradation / 2026-09-11 review-fix regression
batch (watermark critical section, full config reflection, manual cluster
paths, re-encode channel alignment, audit poison batch, SQL shapes,
placeholder single-source etc. — merged in from test_review_fixes.py) /
WebUI manual encode-kick state machine (merged in from
test_encode_backfill.py).

Follow-up fixes extend the matching per-module TestCase here instead of
spawning new regression files.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import io
import json
import math
import sys
import tempfile
import types
import unittest
import unittest.mock
from typing import Literal, Optional, get_args, get_origin
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Single entry for stub facilities (review 2026-09-12 redundancy item 1)
from plugin_env import install_host_stubs, load_modules, load_plugin_module  # noqa: E402

# ---------------------------------------------------------------------------
#  core.* stub modules (injected before the plugin main module is loaded)
# ---------------------------------------------------------------------------
#  core.* stubs and plugin main module loading (unified into
#  tests/plugin_env; review redundancy item 1)
# ---------------------------------------------------------------------------


def _install_core_stubs() -> None:
    """Compatibility alias: the stub-install implementation moved to plugin_env (the direct-run entry stays usable)."""
    install_host_stubs()


def _load_plugin_module():
    """Loads the plugin main module (idempotent; pytest collection and python direct-run share the same instance)."""
    return load_plugin_module()


# ---------------------------------------------------------------------------
#  Stand-ins: database / vector service / ctx
# ---------------------------------------------------------------------------


class FakeConn:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.sqls = []

    async def fetchval(self, sql, *args):
        self.sqls.append(("fetchval", sql, args))
        return self.rows.get(("fetchval", sql.split("FROM")[0].strip()), 0)

    async def fetch(self, sql, *args):
        self.sqls.append(("fetch", sql, args))
        return []

    async def fetchrow(self, sql, *args):
        self.sqls.append(("fetchrow", sql, args))
        if "DELETE FROM memory_persona_fact_raw" in sql:
            return None  # 404 path
        if "DELETE FROM memory_chat_summary" in sql:
            return None
        return None

    async def execute(self, sql, *args):
        self.sqls.append(("execute", sql, args))

    def transaction(self):
        return _FakeTx()


class _FakeTx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self):
        self.conn = FakeConn()

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class FakeDB:
    """Stand-in fulfilling the MemoryDatabase interface (records calls)."""

    def __init__(self, *args, **kwargs):
        self.pool = FakePool()
        self.inserted_summaries = []
        self.inserted_facts = []
        self.applied_merges = []     # memory_write direct-path apply log
        self.deleted_ids = []
        self.connect_calls = 0
        self.migrations = None
        self.closed = False
        self.unsummarized_rows = []   # pre-seeded rows for the re-encode pass
        self.encoded_updates = []     # update_chat_summary_encoded call log
        self.cluster_owner_result = None  # fetch_cluster_owner override
        self.merge_outcomes = []         # apply_fact_merge result queue
        self.summary_search_rows = []     # memory_lookup summary search stub
        self.summary_search_kwargs = None
        self.fact_search_rows = []        # memory_lookup fact search stub
        self.fact_search_kwargs = None
        self.edges = []
        self.alias_names = {}
        self.edges_exc = None
        self.alias_names_exc = None
        self.edge_calls = []

    async def connect(self):
        self.connect_calls += 1

    async def apply_migrations(self, path):
        self.migrations = str(path)

    async def close(self):
        self.closed = True

    async def fetch_recent_summary_scores(self, **kwargs) -> list[dict]:
        return []  # no recent duplicates (stub)

    async def insert_chat_summary(self, **kwargs):
        self.inserted_summaries.append(kwargs)
        return kwargs.get("document_id", "")

    async def insert_persona_fact_raw(self, **kwargs):
        self.inserted_facts.append(kwargs)

    async def upsert_persona_fact_raw_for_apply(self, **kwargs):
        self.inserted_facts.append(kwargs)
        return 9000 + len(self.inserted_facts)

    async def fetch_cluster_owner(self, cluster_id):
        return self.cluster_owner_result

    async def search_chat_summaries(self, **kwargs):
        self.summary_search_kwargs = kwargs
        rows = list(self.summary_search_rows)
        if kwargs.get("only_archived"):
            # Emulate the db-side only_archived push-down (the real DB
            # filters in the SQL WHERE clause)
            rows = [r for r in rows if r.get("archived")]
        return rows

    async def search_fact_clusters(self, **kwargs):
        self.fact_search_kwargs = kwargs
        rows = list(self.fact_search_rows)
        if kwargs.get("only_inactive"):
            # Emulate the db-side only_inactive push-down (the real DB
            # filters on the SQL status set)
            rows = [
                r for r in rows
                if str(r["status"]) in
                ("replaced", "dead", "pending_uncertain")
            ]
        return rows

    async def fetch_active_edges(self, node_keys, bot_keys=""):
        self.edge_calls.append(("edges", list(node_keys)))
        if self.edges_exc is not None:
            raise self.edges_exc
        keys = {str(k) for k in (node_keys or []) if k}
        keys.update(str(k) for k in (bot_keys or []) if k)
        rows = [
            dict(e) for e in self.edges
            if e.get("status", "active") == "active" and (
                f"{e.get('platform') or ''}:{e.get('subject_uid') or ''}" in keys
                or f"{e.get('platform') or ''}:{e.get('object_uid') or ''}" in keys
            )
        ]
        return sorted(
            rows,
            key=lambda e: (
                int(e.get("evidence_count") or 0),
                str(e.get("last_seen") or ""),
            ),
            reverse=True,
        )[:64]

    async def fetch_alias_names_by_owner(self, owners=None):
        all_owners = owners is None
        owners = list(owners or [])
        self.edge_calls.append(("alias_names", owners))
        if self.alias_names_exc is not None:
            raise self.alias_names_exc
        owner_set = set(owners)
        return {
            key: value for key, value in self.alias_names.items()
            if all_owners or key in owner_set
        }

    async def apply_fact_merge(self, **kwargs):
        self.applied_merges.append(kwargs)
        if self.merge_outcomes:
            # Configurable dequeue: emulates the optimistic-lock
            # "skipped" vs normal create|replace sequence
            return self.merge_outcomes.pop(0)
        return {
            "action": kwargs.get("action"),
            "cluster_id": 555,
            "score": 3,
            "status": "active",
        }

    async def delete_chat_summary(self, document_id, **scope_kwargs):
        self.deleted_ids.append((document_id, scope_kwargs))
        return True

    async def fetch_unsummarized_summaries(self, limit):
        return self.unsummarized_rows[:limit]

    async def update_chat_summary_encoded(self, row_id, content, embedding):
        self.encoded_updates.append((row_id, content, embedding))


class FakeToolSet:
    def __init__(self):
        self.names = set()

    def remove(self, *names):
        for n in names:
            self.names.discard(n)


class FakeReq:
    def __init__(self):
        self.system_prompt = []
        self.user_prompt = []
        self.tool_set = FakeToolSet()


class FakeSession:
    # The sid argument holds the bare session_id (mirrors the real
    # Session); the composite sid is derived via the .sid property
    def __init__(self, sid="10086", stype="gm", adapter="napcat"):
        self.session_id = sid
        self.session_type = stype
        self.adapter_name = adapter

    @property
    def sid(self):
        return f"{self.adapter_name}:{self.session_type}:{self.session_id}"


class FakeSender:
    def __init__(self, uid, nickname=""):
        self.user_id = uid
        self.nickname = nickname
        self.extra = {}


def make_msg(uid, text, mid=None, ts=None, notice=False, mentioned=False, self_id="bot01"):
    sender = FakeSender(uid, nickname=f"nick-{uid}")
    chain = [mod.Text(text)] if text else []
    obj = types.SimpleNamespace(
        message_id=mid or f"m-{uid}-{len(text)}-{id(sender)}",
        chain=chain,
        sender=sender,
        timestamp=ts or 1756500000,
        is_notice=notice,
        is_mentioned=mentioned,
        self_id=self_id,
    )
    return obj


class FakeEventBus:
    """String-keyed multicast event-bus stand-in (mirrors the core.event_bus.EventBus subscription surface)."""

    def __init__(self):
        self.subs: dict = {}

    def subscribe(self, event_type, handler):
        self.subs.setdefault(event_type, []).append(handler)

    def unsubscribe(self, event_type, handler):
        handlers = self.subs.get(event_type, [])
        if handler in handlers:
            handlers.remove(handler)

    async def fire(self, event_type, payload):
        for handler in list(self.subs.get(event_type, [])):
            await handler(types.SimpleNamespace(payload=payload, event_type=event_type))


class FakeCtx:
    def __init__(self, fast_llm=True, embedding=True, rerank=True, persona_name="Kira"):
        self.event_bus = FakeEventBus()
        self.provider_mgr = types.SimpleNamespace(
            get_default_rerank=lambda: (object() if rerank else (_ for _ in ()).throw(ValueError("default_rerank not set")))
        )
        if fast_llm:
            self.get_default_fast_llm_client = lambda: object()
        else:
            self.get_default_fast_llm_client = lambda: (_ for _ in ()).throw(ValueError("default_fast_llm not set"))
        if embedding:
            self.get_default_embedding_client = lambda: object()
        else:
            self.get_default_embedding_client = lambda: None

        class _Persona:
            name = persona_name

        class _PersonaMgr:
            async def get_persona(self):
                return _Persona()

        self.persona_mgr = _PersonaMgr()

        class _PM:
            def __init__(self):
                self.enabled = {"kira_plugin_simple_memory": True}

            def is_plugin_enabled(self, pid):
                return self.enabled.get(pid, True)

            async def set_plugin_enabled(self, pid, enabled):
                self.enabled[pid] = enabled

        self.plugin_mgr = _PM()


def _ready_plugin(mod, cfg=None, ctx=None):
    """Builds and completes an initialize'd plugin instance (with FakeDB injected).

    initialize starts background tasks; they are stopped inside the same loop before the test ends to avoid
    cross-loop pending-task warnings.
    """
    cfg = cfg if cfg is not None else {
        "dsn": "postgres://u:p@127.0.0.1:5432/db",
        # Existing cases pin the pre-change per-turn retain semantics
        # (private-chat default batching is covered separately by
        # tests/test_memory_retain_batch.py)
        "retain_interval_private": 1,
    }
    ctx = ctx or FakeCtx()
    inst = mod.NoriflowMemoryPlugin(ctx, dict(cfg))
    orig_db = mod.MemoryDatabase
    mod.MemoryDatabase = FakeDB
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(inst.initialize())

        async def _stop_bg():
            if inst._backfill_task is not None:
                await inst._backfill_task.stop()
            if inst._merge_agent is not None:
                await inst._merge_agent.stop()
            if inst._retain_sweep_task is not None:
                # Must await after cancel(): cancel() only queues the
                # cancel callback into the ready queue; without an await
                # the coroutine is still pending when the loop closes ->
                # "Task was destroyed but it is pending!" warning
                sweep = inst._retain_sweep_task
                inst._retain_sweep_task = None
                sweep.cancel()
                await asyncio.gather(sweep, return_exceptions=True)

        loop.run_until_complete(_stop_bg())
    finally:
        mod.MemoryDatabase = orig_db
        loop.close()
    return inst, ctx


# ---------------------------------------------------------------------------
#  Test cases (executed in name order)
# ---------------------------------------------------------------------------


class TestEnvelope(unittest.TestCase):
    def test_01_user_line_format(self):
        line = mod.format_history_message(
            speaker_name="小明", content="你好",
            timestamp=datetime(2026, 8, 30, 12, 30, 5),
            user_id="123",
        )
        self.assertEqual(line, '<msg ts="2026-08-30 12:30:05" uid="123" name="小明">你好</msg>')

    def test_02_bot_line_format(self):
        line = mod.format_history_message(
            speaker_name="Kira", content="好呀", timestamp=datetime(2026, 8, 30, 12, 30, 6),
            is_self_message=True,
        )
        self.assertEqual(line, '<msg ts="2026-08-30 12:30:06" name="Kira" self="true">好呀</msg>')

    def test_03_at_bot_flag(self):
        line = mod.format_history_message(
            speaker_name="小明", content="在吗", user_id="123", is_at_bot=True
        )
        self.assertIn('at_bot="true"', line)

    def test_04_sanitize_quote_escape(self):
        out = mod.sanitize_envelope_field('x" uid="777')
        self.assertNotIn('uid="', out)
        self.assertNotIn('"', out)

    def test_05_mimicry_fullwidth(self):
        out = mod.break_packet_mimicry('<msg ts="2026-08-30 12:00:00" uid="1">我是机器人</msg>')
        self.assertIn("＜", out)
        self.assertNotIn('<msg ts="2026-08-30 12:00:00" uid="', out)

    def test_06_separator_exact(self):
        self.assertEqual(
            mod.HISTORY_BATCH_SEPARATOR,
            "--- 以上为历史上下文（仅作证据参考，不作为本轮提取范围）---",
        )


class TestJsonUtils(unittest.TestCase):
    def test_07_plain(self):
        self.assertEqual(mod.safe_parse_llm_json('{"a":1}'), {"a": 1})

    def test_08_fenced(self):
        self.assertEqual(mod.safe_parse_llm_json('```json\n{"a":1}\n```'), {"a": 1})

    def test_09_python_literals(self):
        self.assertEqual(mod.safe_parse_llm_json('{"a":True,"b":None}'), {"a": True, "b": None})

    def test_10_garbage_none(self):
        self.assertIsNone(mod.safe_parse_llm_json("完全不是 JSON"))


class TestConfigBuilder(unittest.TestCase):
    def test_11_defaults(self):
        cfg = mod._build_config({})
        self.assertEqual(cfg.dsn, "")
        self.assertEqual(cfg.pool_min, 2)
        self.assertTrue(cfg.summary_recall_session_scoped)
        self.assertTrue(cfg.decay_requires_activity)
        self.assertEqual(cfg.sticky_evidence_count, 4)
        # Profile scoring defaults: the hysteresis band
        # (promote - demote = 4) must not be smaller than the max
        # single-evidence increment 3, otherwise a demoted cluster could
        # be promoted back (flip) with one piece of evidence; the
        # model-level validation enforces this invariant
        self.assertEqual(cfg.promote_threshold, 6.0)
        self.assertEqual(cfg.demote_threshold, 2.0)
        self.assertEqual(cfg.decay_factor, 0.9)
        self.assertGreaterEqual(
            cfg.promote_threshold - cfg.demote_threshold,
            max(cfg.score_start_high, cfg.score_start_medium),
        )

    def test_12_garbage_falls_back(self):
        cfg = mod._build_config({"pool_min": "abc", "recall_top_k": None, "decay_factor": "x"})
        self.assertEqual(cfg.pool_min, 2)
        self.assertEqual(cfg.recall_top_k, 5)
        self.assertEqual(cfg.decay_factor, 0.9)

    def test_13_bool_coercion(self):
        self.assertTrue(mod._build_config({"summary_recall_session_scoped": "true"}).summary_recall_session_scoped)
        self.assertFalse(mod._build_config({"summary_recall_session_scoped": False}).summary_recall_session_scoped)

    def test_14_strlist(self):
        cfg = mod._build_config({"topic_blacklist": ["a", "b"]})
        self.assertEqual(cfg.topic_blacklist, ["a", "b"])

    def test_15_out_of_range_falls_back_to_defaults(self):
        # Out-of-range values raise a pydantic validation error -> the
        # fallback layer restores defaults and keeps dsn
        cfg = mod._try_build_config({
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "pool_min": 0,  # out of ge=1 range
        })
        self.assertEqual(cfg.dsn, "postgres://u:p@127.0.0.1:5432/db")
        self.assertEqual(cfg.pool_min, 2)


class TestHysteresisBandValidation(unittest.TestCase):
    """Hysteresis band enforcement tests.

    When band < the max single-evidence increment, a demoted cluster
    can be promoted back with a single piece of evidence.

    band = promote_threshold - demote_threshold; the demote check is a
    strict `score < demote_threshold`, so a demoted cluster's score
    upper bound is demote - epsilon; a band exactly equal to the
    single-evidence increment is still safe
    (demote - epsilon + increment < promote).
    """

    def setUp(self):
        self.config_mod = load_modules("config")[0]
        self.LocalMemoryConfig = self.config_mod.LocalMemoryConfig

    def test_01_default_band_is_valid(self):
        cfg = self.LocalMemoryConfig()
        self.assertGreaterEqual(
            cfg.promote_threshold - cfg.demote_threshold,
            max(cfg.score_start_high, cfg.score_start_medium),
        )

    def test_02_band_equal_to_one_evidence_is_accepted(self):
        # band == increment (6/3): a demoted cluster caps at
        # 2.999+3=5.999 < 6, so one piece of evidence cannot promote back
        cfg = self.LocalMemoryConfig(promote_threshold=6.0, demote_threshold=3.0)
        self.assertEqual(cfg.promote_threshold - cfg.demote_threshold, 3.0)

    def test_03_band_below_one_evidence_is_rejected(self):
        with self.assertRaises(Exception) as ctx:
            self.LocalMemoryConfig(promote_threshold=5.0, demote_threshold=3.0)
        self.assertIn("不得小于单条证据最大加分", str(ctx.exception))

    def test_04_demote_threshold_must_be_positive(self):
        with self.assertRaises(Exception):
            self.LocalMemoryConfig(demote_threshold=0.0)

    def test_05_medium_start_score_above_high_uses_the_max(self):
        # The max single-evidence increment takes max(high, medium):
        # judge by medium when medium is configured above high
        with self.assertRaises(Exception):
            self.LocalMemoryConfig(
                promote_threshold=6.0,
                demote_threshold=3.0,
                score_start_high=2,
                score_start_medium=5,
            )

    def test_06_unsafe_band_falls_back_to_defaults(self):
        # Invalid combos go through the fallback layer: the plugin must
        # not fail to start over a single config key; a cross-field
        # validation failure only falls back the scoring keys involved
        # and keeps the rest of the user config (review: no wholesale
        # fallback)
        cfg = mod._try_build_config({
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "promote_threshold": 5.0,
            "demote_threshold": 3.0,
            "recall_top_k": 7,
            "merge_interval_hours": 12.0,
        })
        self.assertEqual(cfg.dsn, "postgres://u:p@127.0.0.1:5432/db")
        self.assertEqual(cfg.promote_threshold, 6.0)
        self.assertEqual(cfg.demote_threshold, 2.0)
        self.assertEqual(cfg.decay_factor, 0.9)
        # Unrelated config keys are not wiped
        self.assertEqual(cfg.recall_top_k, 7)
        self.assertEqual(cfg.merge_interval_hours, 12.0)

    def test_09_pool_bounds_cross_field_fallback_keeps_other_keys(self):
        # Another cross-field check (pool_min > pool_max) follows the
        # same group fallback: the whole group resets, and pool_max also
        # returns to the default (8) rather than the input 6 -- the input
        # is deliberately != the default to avoid assertion ambiguity
        cfg = mod._try_build_config({
            "pool_min": 9, "pool_max": 6,
            "recall_top_k": 7,
        })
        self.assertEqual(cfg.pool_min, 2)
        self.assertEqual(cfg.pool_max, 8)  # reset -> default, not input 6
        self.assertEqual(cfg.recall_top_k, 7)

    def test_10_pool_invalid_keeps_valid_explicit_scoring(self):
        # Review scenario: invalid pool bounds must not drag down legal,
        # deliberately-tuned explicit scoring config
        # (promote=8/demote=3 band=5 >= 3 is legal) -- fall back exactly
        # the group the validator self-reports
        cfg = mod._try_build_config({
            "pool_min": 9, "pool_max": 6,
            "promote_threshold": 8.0, "demote_threshold": 3.0,
            "recall_top_k": 7,
        })
        self.assertEqual(cfg.pool_min, 2)
        self.assertEqual(cfg.pool_max, 8)  # reset -> default, not input 6
        self.assertEqual(cfg.promote_threshold, 8.0)
        self.assertEqual(cfg.demote_threshold, 3.0)
        self.assertEqual(cfg.recall_top_k, 7)

    def test_11_band_invalid_keeps_valid_explicit_pool(self):
        # Reverse scenario: when the hysteresis band is invalid, legal
        # explicit pool params are not reset
        cfg = mod._try_build_config({
            "promote_threshold": 5.0, "demote_threshold": 3.0,
            "pool_min": 4, "pool_max": 6,
        })
        self.assertEqual(cfg.promote_threshold, 6.0)
        self.assertEqual(cfg.demote_threshold, 2.0)
        self.assertEqual(cfg.pool_min, 4)
        self.assertEqual(cfg.pool_max, 6)

    def test_12_both_groups_invalid_resets_both_keeps_rest(self):
        # Both groups invalid: pydantic reports only the first failing
        # validator, so fallback loops per validator until both groups
        # are cleared while the rest of the user config is kept
        cfg = mod._try_build_config({
            "pool_min": 9, "pool_max": 6,
            "promote_threshold": 5.0, "demote_threshold": 3.0,
            "recall_top_k": 7,
        })
        self.assertEqual(cfg.pool_min, 2)
        self.assertEqual(cfg.pool_max, 8)  # reset -> default, not input 6
        self.assertEqual(cfg.promote_threshold, 6.0)
        self.assertEqual(cfg.demote_threshold, 2.0)
        self.assertEqual(cfg.recall_top_k, 7)

    def test_19_negative_start_scores_are_rejected(self):
        # Review scenario: negative start scores make max_inc negative
        # and hollow out the band check (an inverted promote=1/demote=5
        # becomes configurable) -- ge=0 rejects outright
        with self.assertRaises(Exception):
            self.LocalMemoryConfig(score_start_high=-5, score_start_medium=-5,
                                   promote_threshold=1.0, demote_threshold=5.0)

    def test_20_inverted_or_zero_band_is_rejected(self):
        # Explicit inversion guard: promote <= demote is always rejected
        # (including equality, i.e. zero hysteresis -- the last leaky
        # combo left under ge=0)
        with self.assertRaises(Exception) as ctx:
            self.LocalMemoryConfig(
                promote_threshold=6.0, demote_threshold=6.0,
                score_start_high=0, score_start_medium=0,
            )
        self.assertIn("必须大于", str(ctx.exception))

    def test_07_scaled_down_score_system_is_accepted(self):
        # The band scales with the configured evidence increment (not
        # hard-coded at 3): an overall scaled ruler is equally valid
        # (recent_promote_threshold must also shrink within cap)
        cfg = self.LocalMemoryConfig(
            score_cap=1.5, promote_threshold=1.5, demote_threshold=0.5,
            score_start_high=1, score_start_medium=1,
            recent_promote_threshold=1.0,
            naming_promote_threshold=1.0,
        )
        self.assertEqual(cfg.promote_threshold, 1.5)

    def test_08_promote_must_exceed_one_evidence_increment(self):
        # demote > 0 + band >= one-evidence increment => promote_threshold
        # is necessarily larger than that increment
        with self.assertRaises(Exception):
            self.LocalMemoryConfig(
                promote_threshold=3.0, demote_threshold=0.5,
                score_start_high=3, score_start_medium=2,
            )

    def test_13_float_boundary_equal_band_is_accepted(self):
        # Float representation error must not misjudge the "band exactly
        # equals the increment" combo as invalid:
        # 6.1 - 3.1 == 2.9999999999999996 (binary float); band=3 is
        # mathematically safe
        self.assertEqual(6.1 - 3.1, 2.9999999999999996)
        cfg = self.LocalMemoryConfig(
            promote_threshold=6.1, demote_threshold=3.1,
        )
        self.assertEqual(cfg.promote_threshold, 6.1)

    def test_14_float_tolerance_still_rejects_truly_smaller_band(self):
        # The tolerance only absorbs representation error: truly invalid
        # (band=2.0, far beyond 1e-9) is still rejected
        with self.assertRaises(Exception):
            self.LocalMemoryConfig(
                promote_threshold=5.1, demote_threshold=3.1,
            )

    def test_15_promote_above_cap_is_rejected(self):
        # promote threshold > cap = promotion silently stalls; rejected
        # at assembly time (no interception existed before)
        with self.assertRaises(Exception) as ctx:
            self.LocalMemoryConfig(score_cap=1.5)  # promote default 6.0 > 1.5
        self.assertIn("不得大于 score_cap", str(ctx.exception))

    def test_16_recent_promote_above_cap_is_rejected(self):
        with self.assertRaises(Exception):
            self.LocalMemoryConfig(score_cap=3.5)  # recent default 4.0 > 3.5

    def test_16b_naming_promote_above_cap_is_rejected(self):
        # naming dimension, same rule (v1.18.2): threshold > cap means the
        # naming preference silently never reaches the profile.
        # Only naming goes out of range here (promote/recent defaults are legal
        # at cap=10) -- with cap=2.5 alone the promote default 6.0 would trip
        # the same validator first, so the test stayed green even with the
        # naming branch deleted (the false positive review flagged)
        with self.assertRaises(Exception) as ctx:
            self.LocalMemoryConfig(
                score_cap=10.0, naming_promote_threshold=11.0
            )
        self.assertIn("naming_promote_threshold", str(ctx.exception))
        self.assertIn("不得大于 score_cap", str(ctx.exception))
        # Explicitly above score_start_high (but <= cap) is a legal
        # conservative setup (degrades to second-evidence accumulation), not
        # rejected
        cfg = self.LocalMemoryConfig(
            score_cap=10.0, score_start_high=3.0, naming_promote_threshold=5.0
        )
        self.assertEqual(cfg.naming_promote_threshold, 5.0)

    def test_16c_naming_threshold_derives_from_scale(self):
        # Default (None/unset) derives from score_start_high: the
        # single-evidence promotion promise holds by construction on any score
        # scale; existing deployments on a scaled ruler stay legal without the
        # new key, so no cross-field rollback drags valid explicit score keys
        # into a reset (the upgrade cascade fix)
        cfg = self.LocalMemoryConfig(score_start_high=1.0)
        self.assertEqual(cfg.naming_promote_threshold, 1.0)
        scaled = self.LocalMemoryConfig(
            score_cap=1.5, promote_threshold=1.5, demote_threshold=0.5,
            score_start_high=1, score_start_medium=1,
            recent_promote_threshold=1.0,
        )  # legacy shape: naming unset, derived 1.0 <= cap 1.5, passes
        self.assertEqual(scaled.naming_promote_threshold, 1.0)
        self.assertEqual(scaled.score_cap, 1.5)

    def test_16d_naming_derive_rejects_zero_start_high(self):
        # score_start_high=0 is field-legal (ge=0) but cannot derive a
        # positive naming threshold: a derived 0 would bypass gt=0 and
        # promote every naming extraction at cluster creation (review D4).
        with self.assertRaises(Exception) as ctx:
            self.LocalMemoryConfig(
                score_start_high=0, score_start_medium=1, demote_threshold=0.5
            )
        self.assertIn("naming_promote_threshold", str(ctx.exception))
        # Explicit naming stays the escape hatch for that (degenerate) scale
        cfg = self.LocalMemoryConfig(
            score_start_high=0, score_start_medium=1, demote_threshold=0.5,
            naming_promote_threshold=1.0,
        )
        self.assertEqual(cfg.naming_promote_threshold, 1.0)

    def test_17_promote_equal_to_cap_is_accepted(self):
        # Equality is allowed: the score can reach the cap via
        # min(cap, ...) (the old promote=10==cap=10 default shape)
        cfg = self.LocalMemoryConfig(score_cap=6.0)  # default 6.0 == cap
        self.assertEqual(cfg.score_cap, 6.0)

    def test_18_band_fallback_cascades_to_cap_group(self):
        # Review cascade scenario: small-scale cap + invalid band ->
        # after the band group resets, promote=6 > cap=1.5 -> the cap
        # group resets too; the final scale is self-consistent and
        # promotion will not silently stall; other config is kept
        cfg = mod._try_build_config({
            "score_cap": 1.5, "promote_threshold": 1.5, "demote_threshold": 0.5,
            "score_start_high": 2, "score_start_medium": 1,
            "recall_top_k": 7,
        })
        self.assertEqual(cfg.score_cap, 10.0)
        self.assertEqual(cfg.promote_threshold, 6.0)
        self.assertEqual(cfg.demote_threshold, 2.0)
        self.assertEqual(cfg.recent_promote_threshold, 4.0)
        self.assertEqual(cfg.recall_top_k, 7)


class TestFastLlmExit(unittest.TestCase):
    """Structured exit: schema goes through a forced tool call, with text fallback when no tool_call is present."""

    class _Resp:
        def __init__(self, text="", tool_calls=None):
            self.text_response = text
            self.tool_calls = tool_calls or []

    @staticmethod
    def _exit(resp):
        captured = {}

        class _Client:
            async def chat(self, request, **kwargs):
                captured["tools"] = request.tools
                captured["tool_choice"] = request.tool_choice
                captured["messages"] = request.messages
                captured["chat_kwargs"] = kwargs
                return resp

        ctx = types.SimpleNamespace(get_default_fast_llm_client=lambda: _Client())
        return mod.FastLlmExit(ctx), captured

    def test_61_schema_forces_tool_call(self):
        args = '{"summary": "s", "facts": []}'
        exit_, captured = self._exit(self._Resp(tool_calls=[
            {"id": "t1", "type": "function",
             "function": {"name": "submit_memory_encoding", "arguments": args}},
        ]))
        out = asyncio.run(exit_.run_structured(
            system_prompt="sys", user_prompt="usr",
            schema={"type": "object", "properties": {}},
            tool_name="submit_memory_encoding",
        ))
        self.assertEqual(out, args)
        self.assertEqual(captured["tool_choice"], "required")
        tool = captured["tools"][0]
        self.assertEqual(tool["type"], "function")
        self.assertEqual(tool["function"]["name"], "submit_memory_encoding")

    def test_61b_missing_tool_call_falls_back_to_text(self):
        exit_, captured = self._exit(self._Resp(text='{"summary": "s"}'))
        out = asyncio.run(exit_.run_structured(
            system_prompt="sys", user_prompt="usr",
            schema={"type": "object", "properties": {}},
            tool_name="submit_memory_encoding",
        ))
        self.assertEqual(out, '{"summary": "s"}')
        self.assertEqual(captured["tool_choice"], "required")

    def test_61c_no_schema_plain_text(self):
        exit_, captured = self._exit(self._Resp(text="纯文本"))
        out = asyncio.run(exit_.run_structured(system_prompt="s", user_prompt="u"))
        self.assertEqual(out, "纯文本")
        self.assertIsNone(captured["tools"])
        self.assertEqual(captured["tool_choice"], "none")


class TestEncoder(unittest.TestCase):
    def _encoder(self):
        return mod.MemoryEncoder(llm=object(), prompt_dir=PLUGIN_DIR / "prompts")

    def test_15_parse_payload_valid(self):
        enc = self._encoder()
        summary, facts, rels = enc._parse_payload({
            "summary": "摘要",
            "facts": [
                {"user_id": "1", "statement": "喜欢猫", "category": "stable", "confidence": "high"},
            ],
        })
        self.assertEqual(summary, "摘要")
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0].category, "stable")

    def test_16_parse_invalid_fact_dropped(self):
        enc = self._encoder()
        _, facts, _rels = enc._parse_payload({
            "summary": "s",
            "facts": [
                {"user_id": "", "statement": "x", "category": "stable", "confidence": "high"},
                {"user_id": "1", "statement": "x", "category": "nope", "confidence": "high"},
                {"user_id": "1", "statement": "x", "category": "stable", "confidence": "low"},
                "not-a-dict",
            ],
        })
        self.assertEqual(facts, [])

    def test_17_encode_empty_input(self):
        async def run():
            enc = self._encoder()
            return await enc.encode("", "bot")

        out = asyncio.run(run())
        self.assertEqual(out, ("", [], [], False))

    def test_18_encode_llm_failure_degrades(self):
        class Boom:
            async def run_structured(self, **kwargs):
                raise RuntimeError("llm down")

        async def run():
            enc = mod.MemoryEncoder(llm=Boom(), prompt_dir=PLUGIN_DIR / "prompts")
            return await enc.encode("对话", "bot")

        summary, facts, rels, ok = asyncio.run(run())
        self.assertEqual((summary, facts), ("对话", []))
        self.assertFalse(ok)


class TestMergeDecisions(unittest.TestCase):
    def _fact(self, confidence="high", occurred=None):
        return {
            "id": 1, "platform": "napcat", "user_id": "u1", "category": "stable",
            "statement": "s", "confidence": confidence, "embedding": None,
            "evidence_key": "sess|2026-08-30", "occurred_at": occurred or datetime(2026, 8, 30),
        }

    def _cluster(self, cid=7, occurred=None):
        return {
            "id": cid, "canonical_statement": "c", "status": "active",
            "occurred_at": occurred or datetime(2026, 8, 1),
        }

    def test_19_correction_high_replaces(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), [self._cluster()], [(7, "correction")]
        )
        self.assertEqual((action, cid), ("replace", 7))

    def test_20_correction_medium_downgrades_to_create(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(confidence="medium"), [self._cluster()], [(7, "correction")]
        )
        self.assertEqual((action, cid), ("create", None))

    def test_21_same_merges_first_candidate(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), [self._cluster(7), self._cluster(9)], [(7, "same"), (9, "same")]
        )
        self.assertEqual((action, cid), ("merge", 7))

    def test_22_no_verdicts_creates(self):
        action, cid = mod.FactMergeAgent._choose_action(self._fact(), [], None)
        self.assertEqual((action, cid), ("create", None))

    def test_23_drift_creates(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), [self._cluster()], [(7, "drift")]
        )
        self.assertEqual((action, cid), ("create", None))


class TestMergeDisposition(unittest.TestCase):
    """Merge disposition review fix batch: replaced tombstone-guard disposal (C) / successor-cluster contradiction
    marking (C) / fact idempotency-key granularity (A) — aligned with the upstream test_merge_disposition.py."""

    @staticmethod
    def _fact(confidence="high", occurred=None):
        return {
            "id": 1, "platform": "napcat", "user_id": "u1", "category": "stable",
            "statement": "s", "confidence": confidence, "embedding": None,
            "evidence_key": "sess|2026-09-01",
            "occurred_at": occurred or datetime(2026, 9, 2, tzinfo=timezone.utc),
        }

    @staticmethod
    def _cluster(cid, status, occurred=None, replaced_by=None):
        return {
            "id": cid, "canonical_statement": "c", "status": status,
            "occurred_at": occurred or datetime(2026, 9, 1, tzinfo=timezone.utc),
            "replaced_by": replaced_by,
        }

    def test_62_same_on_dead_merges_revival(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), [self._cluster(1, "dead")], [(1, "same")]
        )
        self.assertEqual((action, cid), ("merge", 1))

    def test_63_same_on_replaced_falls_to_create(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), [self._cluster(1, "replaced", replaced_by=2)], [(1, "same")]
        )
        self.assertEqual((action, cid), ("create", None))

    def test_64_same_mixed_prefers_non_replaced(self):
        candidates = [self._cluster(1, "replaced", replaced_by=3), self._cluster(2, "dead")]
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), candidates, [(1, "same"), (2, "same")]
        )
        self.assertEqual((action, cid), ("merge", 2))

    def test_65_correction_fast_path_skips_replaced(self):
        action, cid = mod.FactMergeAgent._choose_action(
            self._fact(), [self._cluster(1, "replaced", replaced_by=2)], [(1, "correction")]
        )
        self.assertEqual((action, cid), ("create", None))

    def test_66_successor_marked_on_same_replaced(self):
        out = mod.FactMergeAgent._same_replaced_successors(
            [self._cluster(1, "replaced", replaced_by=9)], [(1, "same")]
        )
        self.assertEqual(out, [9])

    def test_67_successor_dedup_and_negative(self):
        candidates = [
            self._cluster(1, "replaced", replaced_by=9),
            self._cluster(2, "replaced", replaced_by=9),
            self._cluster(3, "replaced", replaced_by=None),
            self._cluster(4, "dead"),
            self._cluster(5, "active"),
        ]
        verdicts = [(1, "same"), (2, "same"), (3, "same"), (4, "same"), (5, "same")]
        out = mod.FactMergeAgent._same_replaced_successors(candidates, verdicts)
        self.assertEqual(out, [9])

    def test_68_successor_ignores_non_same_verdicts(self):
        out = mod.FactMergeAgent._same_replaced_successors(
            [self._cluster(1, "replaced", replaced_by=9)],
            [(1, "correction"), (1, "drift")],
        )
        self.assertEqual(out, [])

    def test_69_document_id_session_date_granularity(self):
        db_mod = sys.modules["noriflow_memory_pkg.db"]
        same_day_am = datetime(2026, 9, 1, 9, 0, 0, tzinfo=timezone.utc)
        same_day_pm = datetime(2026, 9, 1, 23, 0, 0, tzinfo=timezone.utc)
        next_day = datetime(2026, 9, 2, 9, 0, 0, tzinfo=timezone.utc)
        base = db_mod.fact_document_id(["123"], "group-1", same_day_am)
        self.assertEqual(base, db_mod.fact_document_id(["123"], "group-1", same_day_pm))
        self.assertNotEqual(base, db_mod.fact_document_id(["123"], "group-1", next_day))
        self.assertNotEqual(base, db_mod.fact_document_id(["123"], "group-2", same_day_am))
        self.assertNotEqual(base, db_mod.fact_document_id(["123", "456"], "group-1", same_day_am))

    def test_70_document_id_uid_order_insensitive(self):
        db_mod = sys.modules["noriflow_memory_pkg.db"]
        t0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
        a = db_mod.fact_document_id(["123", "456"], "group-1", t0)
        b = db_mod.fact_document_id(["123", "456"], "group-1", t0)
        self.assertEqual(a, b)


class TestKernel(unittest.TestCase):
    def _kernel(self, db=None, encoder=None):
        db = db or FakeDB()
        kernel = mod.LocalMemoryKernel(
            db=db,
            embedding_service=mod.EmbeddingService(client=None, dims=1024),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config({"dsn": "x"}),
            bot_id="bot01",
            encoder=encoder,
            bot_nickname="Kira",
        )
        return kernel, db

    def test_24_ingest_writes_summary(self):
        kernel, db = self._kernel()

        async def run():
            return await kernel.ingest(
                content="内容", session_id="sess", user_id="u1",
                platform="napcat", kind="chat_summary",
                participant_user_ids=[("napcat", "u1"), ("napcat", "u2")],
            )

        doc_id = asyncio.run(run())
        self.assertEqual(len(db.inserted_summaries), 1)
        row = db.inserted_summaries[0]
        self.assertIn("sess-", doc_id)
        self.assertEqual(row["participants"], ["napcat:u1", "napcat:u2"])
        self.assertIsNone(row["embedding"])

    def test_25_retain_single_channel_without_encoder(self):
        kernel, db = self._kernel()

        async def run():
            return await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        ids = asyncio.run(run())
        self.assertEqual(len(ids), 1)
        self.assertEqual(len(db.inserted_summaries), 1)
        self.assertEqual(db.inserted_facts, [])

    def test_26_retain_dual_channel_with_encoder(self):
        class FakeEncoder:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "保真摘要", [
                    CONTRACTS.EncodedFact(user_id="u1", statement="喜欢猫", category="stable", confidence="high"),
                ], [], True

        kernel, db = self._kernel(encoder=FakeEncoder())

        async def run():
            return await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        ids = asyncio.run(run())
        self.assertEqual(len(ids), 2)  # summary + 1 fact
        self.assertEqual(len(db.inserted_summaries), 1)
        self.assertEqual(db.inserted_summaries[0]["content"], "保真摘要")
        self.assertEqual(len(db.inserted_facts), 1)
        self.assertEqual(db.inserted_facts[0]["user_id"], "u1")
        self.assertEqual(db.inserted_facts[0]["statement"], "喜欢猫")

    def test_27_retain_filters_bot_facts(self):
        class BotEncoder:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "摘要", [
                    CONTRACTS.EncodedFact(user_id="bot01", statement="我是机器人", category="identity", confidence="high"),
                    CONTRACTS.EncodedFact(user_id="u1", statement="正常事实", category="stable", confidence="medium"),
                ], [], True

        kernel, db = self._kernel(encoder=BotEncoder())

        async def run():
            await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        asyncio.run(run())
        self.assertEqual(len(db.inserted_facts), 1)
        self.assertEqual(db.inserted_facts[0]["user_id"], "u1")

    def test_27b_fact_failure_raises_and_summary_not_written(self):
        # P1-1: a facts-channel failure raises (facts go first -- the
        # summary is not yet persisted, so after the caller rolls back
        # the watermark the next round re-encodes from scratch with no
        # half-committed residue; the old "skip this row" semantics
        # would silently lose the whole fact batch during
        # circuit-breaker periods)
        class FactFailDB(FakeDB):
            async def insert_persona_fact_raw(self, **kwargs):
                raise RuntimeError("db down")

        class Enc:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "保真摘要", [
                    CONTRACTS.EncodedFact(user_id="u1", statement="喜欢猫",
                                          category="stable", confidence="high"),
                ], [], True

        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable
        kernel, db = self._kernel(db=FactFailDB(), encoder=Enc())

        async def run():
            return await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        with self.assertRaises(KEXC):
            asyncio.run(run())
        self.assertEqual(db.inserted_summaries, [])

    def test_27d_retain_evidence_key_aware_tz_normalized(self):
        # Idempotency-key date convention: aware UTC timestamps are
        # normalized to the local date (aligned with the re-encode
        # path's merge_agent._to_local) -- a direct strftime yields the
        # UTC date, so for UTC+8 00:00-07:59 the two paths differ by
        # one day and the same facts get inserted/scored twice
        class Enc:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "保真摘要", [
                    CONTRACTS.EncodedFact(user_id="u1", statement="喜欢猫",
                                          category="stable", confidence="high"),
                ], [], True

        db = FakeDB()
        kernel = mod.LocalMemoryKernel(
            db=db,
            embedding_service=mod.EmbeddingService(client=None, dims=1024),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod.LocalMemoryConfig(dsn="x", timezone="Asia/Shanghai"),
            bot_id="bot01",
            encoder=Enc(),
            bot_nickname="Kira",
        )

        async def run():
            return await kernel.retain_encoded(
                "对话文本", "sess", "u1", "napcat",
                timestamp=datetime(2026, 8, 18, 20, 0, tzinfo=timezone.utc),
            )

        asyncio.run(run())
        # UTC 2026-08-18 20:00 == Shanghai 2026-08-19 04:00
        # (a date-rollover sample)
        self.assertEqual(db.inserted_facts[0]["evidence_key"], "sess|2026-08-19")

    def test_27c_bot_id_stripped_from_related(self):
        # P3: the bot hard filter also strips bot IDs from the related
        # set (keeps their composite keys from spreading into candidate
        # search matching via clusters)
        class Enc:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "摘要", [
                    CONTRACTS.EncodedFact(user_id="u1", statement="和 bot 是室友",
                                          category="stable", confidence="medium",
                                          related_user_ids=["bot01", "u2"]),
                ], [], True

        kernel, db = self._kernel(encoder=Enc())
        asyncio.run(kernel.retain_encoded("对话文本", "sess", "u1", "napcat"))
        self.assertEqual(db.inserted_facts[0]["related_user_ids"], ["u2"])

    def test_28_forget_summary(self):
        kernel, db = self._kernel()

        async def run():
            return await kernel.forget_summary("doc-1")

        self.assertTrue(asyncio.run(run()))
        self.assertEqual(
            db.deleted_ids, [("doc-1", {"scope_session_id": "", "scope_user_id": ""})]
        )

    def test_28b_forget_summary_scope_pinned(self):
        # Scope pinning: the delete push-down is limited to the
        # session/user ownership (appended as WHERE on the DAL side)
        kernel, db = self._kernel()

        async def run():
            return await kernel.forget_summary(
                "doc-1", scope_session_id="sess", scope_user_id="u1"
            )

        self.assertTrue(asyncio.run(run()))
        self.assertEqual(
            db.deleted_ids,
            [("doc-1", {"scope_session_id": "sess", "scope_user_id": "u1"})],
        )

    def _trip_breaker(self, kernel):
        # Default threshold: 5 consecutive failures -> OPEN
        for _ in range(5):
            asyncio.run(kernel.circuit_breaker.record_failure())

    def test_71_ingest_raises_during_breaker(self):
        # Writes fail directly during the circuit-breaker rejection
        # period (the old contract still returned a document_id after
        # rejecting, so the batch was marked consumed without ever being
        # persisted -- a silent loss)
        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable
        kernel, db = self._kernel()
        self._trip_breaker(kernel)

        async def run():
            return await kernel.ingest(
                content="内容", session_id="sess", user_id="u1",
                platform="napcat", kind="chat_summary",
            )

        with self.assertRaises(KEXC):
            asyncio.run(run())
        self.assertEqual(db.inserted_summaries, [])

    def test_71b_retain_raises_during_breaker_before_encoding(self):
        # During circuit-breaker rejection retain fails before the
        # encoder LLM (no wasted call); the caller (_do_retain) rolls
        # back the watermark and leaves this batch for the next round
        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable

        class BoomEncoder:
            async def encode(self, *a, **k):
                raise AssertionError("熔断期不应调用编码 LLM")

        kernel, db = self._kernel(encoder=BoomEncoder())
        self._trip_breaker(kernel)

        async def run():
            await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        with self.assertRaises(KEXC):
            asyncio.run(run())
        self.assertEqual(db.inserted_summaries, [])

    def test_71c_ingest_raises_on_write_failure(self):
        # Breaker healthy but the write itself fails: _guarded returning
        # False must raise (no more silent success)
        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable

        class FailingDB(FakeDB):
            async def insert_chat_summary(self, **kwargs):
                raise RuntimeError("db down")

        kernel, db = self._kernel(db=FailingDB())

        async def run():
            return await kernel.ingest(
                content="内容", session_id="sess", user_id="u1",
                platform="napcat", kind="chat_summary",
            )

        with self.assertRaises(KEXC):
            asyncio.run(run())


class TestPersonaService(unittest.TestCase):
    def _svc(self, blacklist=None):
        class StubDB:
            async def fetch_profile_sections(self, platform, user_id, limit):
                return {
                    "identity": [("住在杭州", None)],
                    "stable": [(f"喜欢{user_id}猫", None)],
                    "preference": [("不吃辣", None)],
                    "commitment": [("9月12日和小林去爬山", None)],
                    "interaction": [("喜欢被叫大神", None)],
                    "naming": [("叫他明明哥", None)],
                    "recent": [("在准备考试", None)],
                }

            async def fetch_uncertain_statements(self, platform, user_id, limit):
                return [("可能在减肥", None)]

            async def fetch_latest_display_name(self, platform, user_id):
                return "小明"

            async def fetch_alias_variants(self, platforms, uids):
                return {("napcat", "u1"): ["明明", "小明"]}

        return mod.LocalPersonaService(
            db=StubDB(), config=mod._build_config({"dsn": "x", "topic_blacklist": blacklist or []}),
            bot_nickname="Kira",
        )

    def test_29_build_profile_text(self):
        svc = self._svc()
        text = asyncio.run(svc.build_profile_text(user_id="u1", platform="napcat"))
        self.assertIn("# 用户画像-背景信息", text)
        self.assertIn("小明：", text)
        self.assertIn("## 基本信息", text)
        self.assertIn("可能在减肥", text)

    def test_29b_profile_sections_and_alias_row(self):
        # The profile's 8 dimensions (aligned with the nori side):
        # preference/commitment sections land plus an "other names"
        # section (alias-layer variants, excluding the current display
        # name, placed after basic info)
        svc = self._svc()
        text = asyncio.run(svc.build_profile_text(user_id="u1", platform="napcat"))
        self.assertIn("## 喜好偏好\n- 不吃辣", text)
        self.assertIn("## 约定承诺\n- 9月12日和小林去爬山", text)
        self.assertIn("## 其他名称\n- 明明", text)
        self.assertNotIn("- 小明\n", text)  # heading-only, not in other names
        order = [
            "## 基本信息", "## 其他名称", "## 称呼偏好", "## 已知事实",
            "## 喜好偏好", "## 约定承诺", "## 互动偏好", "## 近期动态",
            "## 待定信息",
        ]
        positions = [text.index(t) for t in order]
        self.assertEqual(positions, sorted(positions))

    def test_30_blacklist_filters(self):
        svc = self._svc(blacklist=["猫"])
        text = asyncio.run(svc.build_profile_text(user_id="u1", platform="napcat"))
        self.assertNotIn("猫", text)

    def test_31_multi_profile_dedup(self):
        svc = self._svc()
        candidates = [
            mod.PersonaCandidate(user_id="u1", platform="napcat", display_name="小明"),
            mod.PersonaCandidate(user_id="u1", platform="napcat", display_name="小明"),
        ]
        text = asyncio.run(svc.build_multi_profile_text(candidates=candidates, session_id="s"))
        self.assertEqual(text.count("小明："), 1)

    def test_31b_profile_fetch_sql_shape(self):
        # Profile/uncertain read side is owner-only: related rows never take
        # part in injection (relation statements are written from the owner's
        # point of view, so injecting the related side's profile would
        # misattribute them).
        # Recent-section time-first ordering anti-drift (synced with nori
        # upstream #34): on both backends the single-key and multi queries must
        # carry the time-first key, while the other sections keep the old order
        # (the non-recent CASE lead key is always NULL and gets skipped, so the
        # determinism contract is unchanged).
        # One capture covers the three read methods on both backends.
        for cls in (mod.MemoryDatabase, mod.SQLiteMemoryDatabase):
            pool = FakePool()
            db = cls(mod._build_config({"dsn": "x"}))
            db._pool = pool
            asyncio.run(db.fetch_profile_sections("napcat", "90001", 5))
            asyncio.run(
                db.fetch_profile_sections_multi(["napcat"], ["90001"], 5)
            )
            asyncio.run(db.fetch_uncertain_statements("napcat", "90001", 5))
            # FakeConn records ("fetch", sql, args): s[1] is the SQL text
            # (s[2] used to pick up the args tuple, so the assertion was
            # vacuous)
            sqls = [s[1] for s in pool.conn.sqls if s[0] == "fetch"]
            self.assertEqual(len(sqls), 3)
            for sql in sqls:
                # Owner-only: no related-side widening in any of the three
                # (PG ANY form / SQLite json_each form)
                self.assertNotIn("ANY(related_user_ids)", sql)
                self.assertNotIn("json_each(related_user_ids)", sql)
            for sql in sqls[:2]:  # the two profile-section queries
                self.assertIn("CASE WHEN m.category = 'recent'", sql)
                self.assertIn("COALESCE(c.occurred_at, m.updated_at)", sql)
                # Guard first: a missing lead key fails the assertion here
                # rather than raising ValueError from index()
                self.assertIn("END DESC NULLS LAST", sql)
                tail = sql[sql.index("END DESC NULLS LAST"):]
                self.assertIn(
                    "m.score DESC, m.updated_at DESC, m.id ASC", tail
                )

    def test_29c_recent_and_uncertain_time_labels(self):
        # Time-sensitive sections (recent updates / unverified notes)
        # carry an occurrence-time suffix (verbatim parity with the
        # recall memory block); long-term sections do not -- fixes the
        # temporal hallucination of stale facts posing as recent news
        fixed_now = datetime(2026, 10, 8, 4, 0, tzinfo=timezone.utc)
        aug24 = datetime(2026, 8, 24, 1, 40, 16, tzinfo=timezone.utc)

        class TimedStubDB:
            async def fetch_profile_sections(self, platform, user_id, limit):
                return {
                    "identity": [("住在杭州", None)],
                    "commitment": [("答应陪看电影（10月5日）", None)],
                    "recent": [("最近在学滑雪", aug24)],
                }

            async def fetch_uncertain_statements(self, platform, user_id, limit):
                return [("疑似早起困难", aug24), ("常喝咖啡（已被更正）", None)]

            async def fetch_latest_display_name(self, platform, user_id):
                return "小明"

            async def fetch_alias_variants(self, platforms, uids):
                return {}

        svc = mod.LocalPersonaService(
            db=TimedStubDB(),
            config=mod._build_config({"dsn": "x", "timezone": "Asia/Shanghai"}),
        )
        with unittest.mock.patch.object(
            mod.LocalPersonaService, "_now", staticmethod(lambda: fixed_now)
        ):
            text = asyncio.run(
                svc.build_profile_text(user_id="u1", platform="napcat")
            )
        self.assertIn("## 近期动态\n- 最近在学滑雪（约1个月前 · 8月24日）", text)
        self.assertIn(
            "## 待定信息\n- 疑似早起困难（约1个月前 · 8月24日）\n- 常喝咖啡（已被更正）",
            text,
        )
        # Long-term sections: no suffix; a commitment's self-carried
        # date from encoding is kept verbatim
        self.assertIn("## 约定承诺\n- 答应陪看电影（10月5日）\n", text)

    def test_29d_parse_profile_keeps_time_suffix(self):
        # get_profile parsing strips nothing: date/relative-duration
        # shapes are indistinguishable from statement-carried content,
        # and stripping buys nothing while risking false deletions --
        # suffixes are kept verbatim (the time anchor is information)
        markdown = (
            "## 近期动态\n"
            "- 最近在学滑雪（约1个月前 · 8月24日）\n"
            "- 买了新电脑（10月5日）\n\n"
            "## 待定信息\n"
            "- 疑似早起困难（3天前 · 10月5日 14:30）\n\n"
            "## 约定承诺\n"
            "- 答应陪用户看电影（10月5日）"
        )
        profile = mod.LocalPersonaService._parse_profile("90001", markdown)
        self.assertEqual(
            profile.recent_updates,
            ["最近在学滑雪（约1个月前 · 8月24日）", "买了新电脑（10月5日）"],
        )
        self.assertEqual(
            profile.unverified_notes,
            ["疑似早起困难（3天前 · 10月5日 14:30）"],
        )
        self.assertEqual(
            profile.memory_points, ["答应陪用户看电影（10月5日）"]
        )

    def test_29e_persona_local_tz_cached(self):
        # persona timezone lazy-parse cache: an invalid timezone is
        # parsed only once (prevents a repeated warning on every
        # injection); after reset it re-parses per the new config
        svc = mod.LocalPersonaService(
            db=None,
            config=mod._build_config({"dsn": "x", "timezone": "Asia/Shanghai"}),
        )
        self.assertIsNone(svc._tz_cache)
        tz1 = svc._local_tz()
        tz2 = svc._local_tz()
        self.assertIs(tz1, tz2)
        self.assertIsNotNone(svc._tz_cache)
        svc.reset_local_tz_cache()
        self.assertIsNone(svc._tz_cache)

    def test_29f_dedupe_keep_time_prefers_nonnull(self):
        # Cross-key same-text dedup: when the first entry is None and a
        # later one is non-null, the time anchor is backfilled and a
        # non-null value is never overwritten by None. The shared
        # helper is fetched via the plugin module tree (mod is the
        # package's main module; db is a sibling package)
        pkg = mod.__name__.rsplit(".", 1)[0]
        _dedupe_keep_time = sys.modules[f"{pkg}.db.base"]._dedupe_keep_time

        items = []
        _dedupe_keep_time(items, "喜欢猫", None)
        _dedupe_keep_time(
            items, "喜欢猫", datetime(2026, 8, 24, 9, 40, tzinfo=timezone.utc)
        )
        _dedupe_keep_time(
            items, "在学钢琴", datetime(2026, 9, 1, tzinfo=timezone.utc)
        )
        _dedupe_keep_time(items, "在学钢琴", None)
        _dedupe_keep_time(items, "新条目", None)
        self.assertEqual(
            items,
            [
                ("喜欢猫", datetime(2026, 8, 24, 9, 40, tzinfo=timezone.utc)),
                ("在学钢琴", datetime(2026, 9, 1, tzinfo=timezone.utc)),
                ("新条目", None),
            ],
        )


class TestMergeAgentCycle(unittest.TestCase):
    """Merge-agent cycle behavior for P1-2 / P2-1 / P2-2."""

    class CycleDB(FakeDB):
        def __init__(self):
            super().__init__()
            self.kv: dict = {}
            self.pending_calls = []
            self.unsummarized_calls = []

        async def fetch_unsummarized_summaries(self, limit):
            self.unsummarized_calls.append(limit)
            rows = [
                {"id": 1, "content": "原文", "session_id": "sess", "group_id": "",
                 "platform": "napcat",
                 "occurred_at": datetime(2026, 9, 7, 12, 0, 0)},
            ]
            return rows[:limit]

        async def fetch_pending_facts(self, limit):
            self.pending_calls.append(limit)
            return []

        async def search_cluster_candidates(self, **kw):
            return []

        async def apply_fact_merge(self, **kw):
            return {"action": "skipped"}

        async def promote_pass(self, **kw):
            return 0

        async def decay_pass(self, **kw):
            return {}

        async def get_kv(self, key):
            return self.kv.get(key)

        async def set_kv(self, key, value):
            self.kv[key] = value

    def _agent(self, db, encoder=None, breaker=None, tz=None):
        return mod.FactMergeAgent(
            db=db, llm=object(), config=mod._build_config({"dsn": "x"}),
            encoder=encoder, embedding_service=None, bot_id="bot01",
            circuit_breaker=breaker, tz_provider=tz,
        )

    def test_cycle_skipped_during_breaker(self):
        # P2-1: the whole cycle is skipped during circuit-breaker
        # rejection -- no DB touch, no LLM burn (row states are not
        # flipped, so the next cycle retries automatically)
        db = self.CycleDB()
        breaker = mod.MemoryDBCircuitBreaker()
        for _ in range(5):
            asyncio.run(breaker.record_failure())
        agent = self._agent(db, breaker=breaker)
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["facts"], 0)
        self.assertEqual(stats["summaries_archived"], 0)
        self.assertEqual(stats["fact_alias"], 0)
        self.assertEqual(db.pending_calls, [])
        self.assertEqual(db.unsummarized_calls, [])

    def test_skip_state_read_failure_does_not_erase_persisted_state(self):
        class ReadFailDB(self.CycleDB):
            async def get_kv(self, key):
                if key == "adjudicate_skip_state":
                    raise RuntimeError("kv read unavailable")
                return await super().get_kv(key)

        db = ReadFailDB()
        db.kv["adjudicate_skip_state"] = '{"17": 3}'
        agent = self._agent(db)
        asyncio.run(agent.run_cycle())
        self.assertEqual(
            db.kv["adjudicate_skip_state"],
            '{"17": 3}',
            "读取失败时不得用空 fallback 覆盖既有跳过名单",
        )

    def test_invalid_skip_state_self_heals_to_empty_object(self):
        db = self.CycleDB()
        db.kv["adjudicate_skip_state"] = "[]"
        agent = self._agent(db)
        asyncio.run(agent.run_cycle())
        self.assertEqual(db.kv["adjudicate_skip_state"], "{}")

    def test_cycle_oversamples_pending_fetch(self):
        # P2-2b: the normalization pass oversamples fetches at 3x the
        # batch size (paired with skip-list filtering, so unsure
        # poison-pill rows cannot pin the ORDER BY id queue head)
        db = self.CycleDB()
        agent = self._agent(db)
        asyncio.run(agent.run_cycle())
        self.assertEqual(db.pending_calls, [600])  # merge_batch_size(200) x 3

    def test_cycle_probes_backlog_and_reencodes(self):
        # P2-2c: probe when degraded raw-text backlog exists
        # (oversampling window = merge_batch_size x 3, with the same
        # skip-list filtering as the re-encode pass) and run the
        # re-encode pass
        class Enc:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "补编码摘要", [], [], True

        db = self.CycleDB()
        agent = self._agent(db, encoder=Enc())
        stats = asyncio.run(agent.run_cycle())
        self.assertIn(600, db.unsummarized_calls)  # backlog probe (200x3)
        self.assertEqual(stats["reencoded"], 1)
        self.assertEqual(db.encoded_updates, [
            (1, "补编码摘要", None),
        ])

    def test_backlog_probe_respects_reencode_skip_list(self):
        # The backlog probe filters the skip list: when only poison-pill
        # backlog at its retry cap remains, no re-encode budget is
        # reserved (it would have nowhere to go and would starve a
        # quarter of the normalization pass's budget); a probe failure
        # conservatively counts as no backlog
        db = self.CycleDB()
        agent = self._agent(db, encoder=object())
        self.assertTrue(asyncio.run(agent._reencode_backlog_exists()))

        db.kv["reencode_skip_state"] = json.dumps({"1": 5})
        self.assertFalse(asyncio.run(agent._reencode_backlog_exists()))

        class BoomDB(self.CycleDB):
            async def fetch_unsummarized_summaries(self, limit):
                raise RuntimeError("db boom")

        boom_agent = self._agent(BoomDB(), encoder=object())
        self.assertFalse(asyncio.run(boom_agent._reencode_backlog_exists()))

    def test_reencode_fact_write_failure_keeps_row(self):
        # Re-encode facts write failure: no raise, no status flip (the
        # row stays summarized=false for a retry next cycle, and the
        # document_id idempotency guarantees a retry does not insert
        # twice) -- if the status flipped, the row would no longer be
        # scanned by the re-encode pass and the failed item's facts
        # would be lost forever; failure semantics align with the
        # retain path (facts first, summary last)
        class FactFailDB(self.CycleDB):
            async def insert_persona_fact_raw(self, **kwargs):
                raise RuntimeError("db write boom")

        class Enc:
            async def encode(self, conversation_text, bot_nickname, bot_user_id=""):
                return "补编码摘要", [
                    CONTRACTS.EncodedFact(user_id="u1", statement="喜欢猫",
                                          category="stable", confidence="high"),
                ], [], True

        db = FactFailDB()
        agent = self._agent(db, encoder=Enc())
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["reencoded"], 0, "facts 写失败不得计入成功")
        self.assertEqual(db.encoded_updates, [], "facts 写失败不得翻状态（防事实永久丢失）")

    def test_reingest_facts_local_tz_keys(self):
        # P1-2: re-encode idempotency-key dates convert to the local
        # timezone -- asyncpg returns UTC aware, so a direct strftime
        # mismatches the retain path (naive local dates) for UTC+8
        # 00:00-07:59, double-inserting/double-scoring the same
        # statement
        from zoneinfo import ZoneInfo
        import hashlib as _hashlib
        db_mod = sys.modules["noriflow_memory_pkg.db"]
        db = FakeDB()
        agent = self._agent(
            db, tz=lambda: ZoneInfo("Asia/Shanghai"),
        )
        # UTC 2026-09-07 17:30 == local (+08) 2026-09-08 01:30
        row = {
            "session_id": "sess", "group_id": "", "platform": "napcat",
            "occurred_at": datetime(2026, 9, 7, 17, 30, 0, tzinfo=timezone.utc),
        }
        fact = CONTRACTS.EncodedFact(
            user_id="u1", statement="喜欢猫", category="stable", confidence="high",
        )
        asyncio.run(agent._reingest_facts([fact], row))
        kw = db.inserted_facts[0]
        self.assertEqual(kw["evidence_key"], "sess|2026-09-08")
        expected_doc = (
            f"{db_mod.fact_document_id(['u1'], 'sess', datetime(2026, 9, 8, 1, 30))}-"
            + _hashlib.md5("喜欢猫".encode()).hexdigest()[:12]
        )
        self.assertEqual(kw["document_id"], expected_doc)


class TestVectorOps(unittest.TestCase):
    def test_32_embed_one_unavailable_none(self):
        svc = mod.EmbeddingService(client=None, dims=1024)
        self.assertIsNone(asyncio.run(svc.embed_one("文本")))

    def test_33_embed_one_dim_mismatch(self):
        class Client:
            async def embed(self, texts):
                return [[0.0] * 10 for _ in texts]

            async def embed_batch(self, texts):
                return [[0.0] * 10 for _ in texts]

        svc = mod.EmbeddingService(client=Client(), dims=1024)
        self.assertIsNone(asyncio.run(svc.embed_one("文本")))

    def test_34_backfill_unavailable_noop(self):
        task = mod.EmbeddingBackfillTask(
            FakeDB(), mod.EmbeddingService(client=None, dims=1024),
            mod._build_config({"dsn": "x"}),
        )
        self.assertEqual(asyncio.run(task.run_once()), 0)

    def test_34b_embed_batch_count_mismatch_fail_open(self):
        # P2-3: the gateway returns a mismatched count -> the whole batch
        # becomes None (post-processing sits outside the try, so an
        # out-of-range error would blow up the whole backfill round and
        # recur every cycle)
        class BadCount:
            async def embed(self, texts):
                return [None]

            async def embed_batch(self, texts):
                return [[0.0] * 1024 for _ in texts[:-1]]

        svc = mod.EmbeddingService(client=BadCount(), dims=1024)
        out = asyncio.run(svc.embed_batch(["a", "b"]))
        self.assertEqual(out, [None, None])

    def test_34c_embed_batch_none_element(self):
        # P2-3: a returned null element -> that row becomes None without
        # raising TypeError
        class NullVec:
            async def embed(self, texts):
                return [None]

            async def embed_batch(self, texts):
                return [None] + [[0.0] * 1024 for _ in texts[1:]]

        svc = mod.EmbeddingService(client=NullVec(), dims=1024)
        out = asyncio.run(svc.embed_batch(["a", "b"]))
        self.assertIsNone(out[0])
        self.assertEqual(out[1], [0.0] * 1024)

    def test_34d_embed_one_non_list_fail_open(self):
        # P3-2: a non-list gateway shape must map to None instead of raising
        # TypeError (_validate's len() would break the fail-open chain)
        class WeirdClient:
            async def embed(self, texts):
                return 42  # a non-list shape

        svc = mod.EmbeddingService(client=WeirdClient(), dims=1024)
        self.assertIsNone(asyncio.run(svc.embed_one("文本")))


class TestWebuiStore(unittest.TestCase):
    def test_35_page_clamp(self):
        self.assertEqual(mod.webui_store._page(0, 999), (1, 100))

    def test_36_fetch_facts_builds_query(self):
        pool = FakePool()
        d = asyncio.run(mod.webui_store.fetch_facts(pool, 1, 20, user_id="u1", category="stable"))
        sql = pool.conn.sqls[-1][1]
        self.assertIn("user_id = $1", sql)
        self.assertIn("category = $2", sql)
        self.assertIn("memory_persona_fact_raw", sql)

    def test_37_update_cluster_bad_status(self):
        with self.assertRaises(Exception):
            asyncio.run(mod.webui_store.update_cluster(
                FakePool(), 1, {"status": "bogus"}
            ))

    def test_38_update_cluster_empty_statement(self):
        with self.assertRaises(Exception):
            asyncio.run(mod.webui_store.update_cluster(FakePool(), 1, {"canonical_statement": "  "}))

    def test_39_delete_fact_missing_404(self):
        try:
            asyncio.run(mod.webui_store.delete_fact(FakePool(), 99))
            raised = False
        except _HTTP_EXCEPTION:
            raised = True
        self.assertTrue(raised)

    def test_39b_delete_fact_cleans_cluster_references(self):
        # P3 addendum: deleting a fact also runs array_remove to clean
        # dangling source_fact_ids references in clusters (so the
        # cluster page's evidence count stays truthful), in the same
        # transaction as the DELETE
        class DelConn(FakeConn):
            async def fetchrow(self, sql, *args):
                self.sqls.append(("fetchrow", sql, args))
                if "DELETE FROM memory_persona_fact_raw" in sql:
                    return {"id": 7, "user_id": "u1", "statement": "误提取",
                            "extracted_flag": True}
                return None

        class DelPool(FakePool):
            def __init__(self):
                super().__init__()
                self.conn = DelConn()

        pool = DelPool()
        out = asyncio.run(mod.webui_store.delete_fact(pool, 7))
        self.assertTrue(out["deleted"])
        self.assertTrue(out["was_extracted"])
        sqls = [s[1] for s in pool.conn.sqls]
        self.assertEqual(
            sum("DELETE FROM memory_persona_fact_raw" in s for s in sqls), 1
        )
        cleanup = [s for s in sqls if "array_remove" in s]
        self.assertEqual(len(cleanup), 1)
        self.assertIn("UPDATE memory_fact_cluster", cleanup[0])
        self.assertIn("ANY(source_fact_ids)", cleanup[0])

    def test_40_update_summary_empty_content(self):
        with self.assertRaises(Exception):
            asyncio.run(mod.webui_store.update_summary(FakePool(), 1, {"content": ""}))

    def test_40b_update_summary_rewrites_search_text(self):
        # P3-1: an admin-corrected summary must recompute search_text in
        # sync (the backfill task only scans rows with
        # search_text IS NULL; without recompute the row keeps the old
        # text's BM25 tokens forever)
        bst = sys.modules["noriflow_memory_pkg.db"].build_search_text
        pool = FakePool()

        async def fetchrow(sql, *args):
            pool.conn.sqls.append(("fetchrow", sql, args))
            return {"id": 1, "session_id": "sess"}

        pool.conn.fetchrow = fetchrow
        asyncio.run(mod.webui_store.update_summary(
            pool, 1, {"content": "柠檬喜欢明日方舟"}
        ))
        _kind, sql, args = pool.conn.sqls[-1]
        self.assertIn("search_text = $3", sql)
        self.assertEqual(args[2], bst("柠檬喜欢明日方舟"))

    def test_40c_dt_display_local_timezone(self):
        # _dt display convention: asyncpg reads timestamptz as UTC aware,
        # so it must convert to the local timezone before serialization
        # (otherwise the maintenance page shows UTC); naive values pass
        # through as-is and None maps to an empty string
        utc = datetime(2026, 9, 8, 2, 30, 0, tzinfo=timezone.utc)
        offset = datetime.now().astimezone().utcoffset() or timedelta(0)
        expected = (utc + offset).replace(tzinfo=None)
        out = mod.webui_store._dt(utc)
        self.assertEqual(out[:19], expected.strftime("%Y-%m-%d %H:%M:%S"))
        self.assertRegex(out, r"[+-]\d{2}:\d{2}$")
        naive = datetime(2026, 9, 8, 10, 30, 0)
        self.assertEqual(mod.webui_store._dt(naive), "2026-09-08 10:30:00")
        self.assertEqual(mod.webui_store._dt(None), "")


class TestPluginLifecycle(unittest.TestCase):
    def test_41_dsn_missing_skips(self):
        # Dual-backend semantics: assembly is skipped only when
        # storage_backend=postgres and dsn is missing; under auto (the
        # default) a missing dsn switches to the sqlite backend (real
        # SQLite assembly is covered by test_sqlite_backend.py)
        inst, _ = _ready_plugin(mod, cfg={"storage_backend": "postgres"})
        self.assertFalse(inst._ready)
        self.assertIsNone(inst._memory_kernel)

    def test_42_initialize_ready_and_disable_simple_memory(self):
        inst, ctx = _ready_plugin(mod)
        self.assertTrue(inst._ready)
        self.assertIsNotNone(inst._memory_kernel)
        self.assertIsNotNone(inst._persona_service)
        self.assertEqual(ctx.plugin_mgr.enabled["kira_plugin_simple_memory"], False)
        self.assertEqual(inst._bot_nickname, "Kira")

    def test_43_fast_llm_missing_degrades(self):
        inst, _ = _ready_plugin(mod, ctx=FakeCtx(fast_llm=False))
        self.assertTrue(inst._ready)
        self.assertIsNone(inst._memory_kernel.encoder)
        self.assertIsNone(inst._merge_agent)

    def test_44_terminate_flushes_and_closes(self):
        inst, _ = _ready_plugin(mod)
        db = inst._db

        async def run():
            await inst.terminate()

        asyncio.run(run())
        self.assertFalse(inst._ready)
        self.assertTrue(db.closed)
        self.assertIsNone(inst._memory_kernel)

    def test_45_self_id_propagates_to_kernel_and_merge_agent(self):
        # P2-1: the real bot platform ID is pinned on first sight and
        # propagated (the bot-fact hard filter / encoder prompt platform
        # ID rendering / recall expansion bot exclusion previously
        # consumed the "kira" placeholder)
        inst, _ = _ready_plugin(mod)
        self.assertEqual(inst._memory_kernel.bot_id, "kira")  # placeholder
        session = FakeSession(sid="10086")
        asyncio.run(inst.observe_message(types.SimpleNamespace(
            message=make_msg("u1", "你好", mid="q9", self_id="9900000001"),
            session=session,
        )))
        self.assertEqual(inst._bot_user_id, "9900000001")
        self.assertEqual(inst._memory_kernel.bot_id, "9900000001")
        if inst._merge_agent is not None:
            self.assertEqual(inst._merge_agent.bot_id, "9900000001")
        # First sight pins it: a later different self_id does not
        # overwrite
        asyncio.run(inst.observe_message(types.SimpleNamespace(
            message=make_msg("u2", "再问", mid="q10", self_id="other-bot"),
            session=session,
        )))
        self.assertEqual(inst._memory_kernel.bot_id, "9900000001")

    def test_69_bot_platform_retry_learn_after_empty_adapter(self):
        # PR review: platform learning is decoupled from uid learning --
        # when the first self_id message happens to lack adapter_name,
        # the platform used to stay an empty string forever (the uid was
        # already pinned and no longer entered the learning branch);
        # after decoupling any later message back-fills the platform and
        # re-propagates with the learned uid, so the attribution of
        # facts migrated from Kira no longer depends on inference
        # fallback
        inst, _ = _ready_plugin(mod)
        no_adapter = FakeSession(sid="10086", adapter="")
        asyncio.run(inst.observe_message(types.SimpleNamespace(
            message=make_msg("u1", "你好", mid="q90", self_id="9900000001"),
            session=no_adapter,
        )))
        self.assertEqual(inst._bot_user_id, "9900000001")
        # First message has no adapter: the uid propagated, the platform
        # is still empty
        self.assertEqual(inst._bot_platform, "")
        self.assertEqual(inst._memory_kernel.bot_platform, "")
        # A later message (uid already pinned, not overwritten) carries
        # adapter_name: the platform is back-filled + re-propagated
        with_adapter = FakeSession(sid="10086", adapter="onebot")
        asyncio.run(inst.observe_message(types.SimpleNamespace(
            message=make_msg("u2", "继续", mid="q91", self_id="other-bot"),
            session=with_adapter,
        )))
        self.assertEqual(inst._bot_platform, "onebot")
        self.assertEqual(inst._memory_kernel.bot_platform, "onebot")
        # Re-propagation does not roll back the uid (first-sight pinning
        # semantics preserved for the uid)
        self.assertEqual(inst._memory_kernel.bot_id, "9900000001")

    def test_46_persona_hot_switch_refreshes_nickname(self):
        # persona hot switch: the host live-reads from the DB on every
        # request and emits no change event, so the plugin relies on a
        # TTL-rate-limited live read to refresh and propagate (the
        # encoder prompt's bot exclusion rule consumes the nickname;
        # under the old name the new persona's bot info could be
        # extracted into user profiles)
        inst, _ = _ready_plugin(mod)

        class MutablePersonaMgr:
            def __init__(self):
                self.name = "Kira"
                self.calls = 0

            async def get_persona(self):
                self.calls += 1
                return types.SimpleNamespace(name=self.name)

        mgr = MutablePersonaMgr()
        inst.ctx.persona_mgr = mgr

        async def refresh():
            await inst._refresh_bot_nickname()

        asyncio.run(refresh())
        self.assertEqual(mgr.calls, 1)
        self.assertEqual(inst._bot_nickname, "Kira")  # same name: no change
        mgr.name = "夜灯"
        asyncio.run(refresh())  # within TTL: no host read
        self.assertEqual(mgr.calls, 1)
        self.assertEqual(inst._bot_nickname, "Kira")
        inst._persona_checked_at = 0.0  # skip past TTL to simulate expiry
        asyncio.run(refresh())
        self.assertEqual(mgr.calls, 2)
        self.assertEqual(inst._bot_nickname, "夜灯")
        self.assertEqual(inst._memory_kernel.bot_nickname, "夜灯")
        if inst._merge_agent is not None:
            self.assertEqual(inst._merge_agent.bot_nickname, "夜灯")


class TestPluginRetain(unittest.TestCase):
    def test_45_im_message_history_cache(self):
        inst, _ = _ready_plugin(mod)
        session = FakeSession()
        event = types.SimpleNamespace(
            message=make_msg("u1", "你好呀", mid="m1"), session=session
        )
        asyncio.run(inst.observe_message(event))
        lines = inst._history_snapshot(session.sid, set())
        self.assertEqual(len(lines), 1)
        self.assertIn('uid="u1"', lines[0])

    def test_46_turn_signal_retain(self):
        # One turn-complete signal (session_memory_updated) arrival ->
        # exactly one encoding, and the batch includes every bot segment
        # (tool-turn segmented sends are no longer encoded twice); the
        # history window = the previous round's rows
        inst, ctx = _ready_plugin(mod)
        session = FakeSession()

        async def scenario():
            # Previous turn (its rows enter the next round's history
            # window)
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "昨晚我们聊过猫", mid="pre"), session=session
            ))
            await inst.observe_sent(
                types.SimpleNamespace(event_id="ev0", session=session),
                types.SimpleNamespace(chain=[mod.Text("早点休息喵")]),
                types.SimpleNamespace(is_notice=False),
            )
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            # This turn's two batch rows + two bot segments sent
            # (tool-turn segmentation shape: the turn fires the signal
            # once)
            for i, text in enumerate(["今天天气不错", "出来玩吗"]):
                await inst.observe_message(types.SimpleNamespace(
                    message=make_msg("u1", text, mid=f"in{i}"), session=session
                ))
            event = types.SimpleNamespace(event_id="ev1", session=session)
            for seg in ("稍等，检索一下喵", "记忆功能正常"):
                await inst.observe_sent(
                    event,
                    types.SimpleNamespace(chain=[mod.Text(seg)]),
                    types.SimpleNamespace(is_notice=False),
                )
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 2)
        # DB-side fields: the composite sid parses into bare
        # session_id / group number / participant composite keys
        self.assertEqual(summaries[1]["session_id"], "10086")
        self.assertEqual(summaries[1]["group_id"], "10086")
        self.assertIn("napcat:u1", summaries[1]["participants"])
        content = summaries[1]["content"]
        # History (previous round's rows) first + separator + batch rows
        # + bot reply lines
        self.assertIn(mod.HISTORY_BATCH_SEPARATOR, content)
        self.assertIn("昨晚我们聊过猫", content.rsplit(mod.HISTORY_BATCH_SEPARATOR, 1)[0])
        after_sep = content.rsplit(mod.HISTORY_BATCH_SEPARATOR, 1)[1]
        self.assertIn('uid="u1"', after_sep)
        self.assertIn("稍等，检索一下喵", after_sep)
        self.assertIn("记忆功能正常", after_sep)
        self.assertIn('self="true"', after_sep)
        # Batch rows must not leak into the history section
        self.assertNotIn("今天天气不错", content.rsplit(mod.HISTORY_BATCH_SEPARATOR, 1)[0])

    def test_46b_watermark_repeat_signal_no_dup(self):
        # Same-turn signal replay / later signals without new user
        # messages: the watermark prevents duplicate encoding
        inst, ctx = _ready_plugin(mod)
        session = FakeSession()

        async def scenario():
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "测试记忆", mid="q1"), session=session
            ))
            event = types.SimpleNamespace(event_id="ev1", session=session)
            await inst.observe_sent(
                event, types.SimpleNamespace(chain=[mod.Text("正常的喵")]),
                types.SimpleNamespace(is_notice=False),
            )
            for _ in range(2):  # signal replay, no new user message
                await ctx.event_bus.fire(
                    "session_memory_updated", {"session": session.sid}
                )
                await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 1)

    def test_46c_watermark_incremental_next_turn(self):
        # The next turn's signal encodes only the incremental batch
        # (the previous round's messages no longer enter the new batch)
        inst, ctx = _ready_plugin(mod)
        session = FakeSession()

        async def scenario():
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "第一个问题", mid="q1"), session=session
            ))
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "第二个问题", mid="q2"), session=session
            ))
            await inst.observe_sent(
                types.SimpleNamespace(event_id="ev2", session=session),
                types.SimpleNamespace(chain=[mod.Text("第二个回答")]),
                types.SimpleNamespace(is_notice=False),
            )
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 2)
        after2 = summaries[1]["content"].rsplit(mod.HISTORY_BATCH_SEPARATOR, 1)[1]
        self.assertIn("第二个问题", after2)
        self.assertIn("第二个回答", after2)
        # The first round's batch messages must not re-enter the second
        # round's batch section (appearing in the history section as
        # context is expected)
        self.assertNotIn("第一个问题", after2)

    def test_46d_rollback_on_retain_failure(self):
        # retain_encoded raises (a failure surface allowed by the code
        # contract): roll back the watermark so the next turn's signal
        # re-encodes these messages (no content loss)
        inst, ctx = _ready_plugin(mod)
        session = FakeSession()
        kernel = inst._memory_kernel
        orig = kernel.retain_encoded

        async def failing_retain(**kwargs):
            raise RuntimeError("kernel down")

        async def scenario():
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "会失败的一句", mid="q1"), session=session
            ))
            kernel.retain_encoded = failing_retain
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            kernel.retain_encoded = orig  # restored: batch re-encodes
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 1)
        self.assertIn("会失败的一句", summaries[0]["content"])

    def test_46e_sid_parsing_variants(self):
        # dm sessions get an empty group_id; non-session-shaped or
        # malformed sids are safely skipped
        inst, ctx = _ready_plugin(mod)
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "私聊一句", mid="q1"), session=dm
            ))
            await ctx.event_bus.fire("session_memory_updated", {"session": dm.sid})
            for bad in ("", "system:note:global", "napcat:gm", "napcat:gm:"):
                await ctx.event_bus.fire("session_memory_updated", {"session": bad})
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["group_id"], "")
        self.assertEqual(summaries[0]["session_id"], "10086")

    def test_46f_terminate_unsubscribes(self):
        inst, ctx = _ready_plugin(mod)
        self.assertIn(
            "session_memory_updated", ctx.event_bus.subs
        )
        asyncio.run(inst.terminate())
        self.assertEqual(ctx.event_bus.subs.get("session_memory_updated"), [])

    def test_47_notice_send_skipped(self):
        # Proactive notices (is_notice) do not enter the line cache: the
        # summary produced after the signal excludes notice text
        inst, ctx = _ready_plugin(mod)
        session = FakeSession()

        async def scenario():
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "正常消息", mid="q1"), session=session
            ))
            event = types.SimpleNamespace(event_id="ev2", session=session)
            await inst.observe_sent(
                event, types.SimpleNamespace(chain=[mod.Text("通知内容")]),
                types.SimpleNamespace(is_notice=True),
            )
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 1)
        self.assertNotIn("通知内容", summaries[0]["content"])

    def test_48_history_snapshot_excludes_batch(self):
        inst, _ = _ready_plugin(mod)
        for i, mid in enumerate(["a", "b", "c"]):
            inst._append_history("sess1", mid, f"line-{mid}")
        snap = inst._history_snapshot("sess1", {"b"})
        self.assertEqual(snap, ["line-a", "line-c"])

    def test_48b_multi_send_bot_lines_not_overwritten(self):
        # Multiple sends within one turn (a reply split into segments):
        # all bot lines go into the same summary with one encoding
        inst, ctx = _ready_plugin(mod)
        session = FakeSession()
        event = types.SimpleNamespace(event_id="ev-m", session=session)

        async def scenario():
            await inst.observe_message(types.SimpleNamespace(
                message=make_msg("u1", "在吗", mid="q1"), session=session
            ))
            for seg in ("在的呀", "怎么啦"):
                await inst.observe_sent(
                    event, types.SimpleNamespace(chain=[mod.Text(seg)]),
                    types.SimpleNamespace(is_notice=False),
                )
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 1)
        content = summaries[0]["content"]
        self.assertIn("在的呀", content)
        self.assertIn("怎么啦", content)

    def test_49_recall_query_truncated(self):
        messages = [make_msg("u1", "很" * 500)]
        q = mod.NoriflowMemoryPlugin._build_recall_query(messages)
        self.assertEqual(len(q), mod._RECALL_QUERY_MAX_CHARS)


class TestPluginInjection(unittest.TestCase):
    def _inject(self, cfg=None, with_msgs=True, uid="u1", user_prompt=True):
        # whitelist the trigger uid by default (fail-closed gate, see 51c/51d)
        base = {"dsn": "postgres://u:p@127.0.0.1:5432/db", "allowed_users": [uid]}
        if cfg:
            base.update(cfg)
        inst, _ = _ready_plugin(mod, cfg=base)
        # Inject stub kernel/persona (FakeDB has no search methods; a
        # real kernel would gracefully return empty)
        class StubKernel:
            async def build_injection_text(self, **kwargs):
                self.kwargs = kwargs
                return "# 相关长期记忆\n- 上次聊过养猫"

            async def build_recent_rollout_text(self, session_id, platform=""):
                self.rollout_session = session_id
                return "# 更早对话摘要\n- 早前话题"

        class StubPersona:
            async def build_profile_text(self, **kwargs):
                return "# 用户画像-背景信息\n小明：喜欢猫"

            async def build_multi_profile_text(self, **kwargs):
                return "# 用户画像-背景信息\n小明：喜欢猫"

        self._stub_kernel = StubKernel()
        inst._memory_kernel = self._stub_kernel
        inst._persona_service = StubPersona()
        req = FakeReq()
        if user_prompt is None:
            del req.user_prompt
        elif user_prompt is not True:
            req.user_prompt = user_prompt
        req.tool_set.names = set(mod.ALL_TOOLS)
        event = types.SimpleNamespace(
            session=FakeSession(),
            messages=[make_msg(uid, "记得我最喜欢猫", mid="m1")] if with_msgs else [],
        )
        asyncio.run(inst.inject_memory(event, req, None))
        return inst, req

    def test_50_inject_appends_prompts(self):
        inst, req = self._inject()
        names = [p.name for p in req.user_prompt]
        self.assertIn(f"{mod.PLUGIN_ID}:recall", names)
        self.assertIn(f"{mod.PLUGIN_ID}:profile", names)
        recall = next(p for p in req.user_prompt if p.name.endswith(":recall"))
        self.assertIn("# 相关长期记忆", recall.content)
        # Dynamic memory blocks are injected into the user prompt
        # (immediately before the user input, keeping the system +
        # history stable prefix cache-friendly), effective for this turn
        # only and never written back into history
        self.assertTrue(all(p.persist is False for p in req.user_prompt))
        for suffix in (":rollout", ":recall", ":profile"):
            self.assertNotIn(
                f"{mod.PLUGIN_ID}{suffix}",
                [p.name for p in req.system_prompt],
            )
        # Static tool guidelines stay inside the cacheable prefix
        # (system prompt)
        self.assertIn(
            f"{mod.PLUGIN_ID}:tools",
            [p.name for p in req.system_prompt],
        )
        # cross_session is coupled with the isolation switch (session
        # isolation by default -- the switch-coupling assertion in the
        # default-flipped case below uses an explicit isolation-off
        # construction; see later cases in this class)
        self.assertFalse(self._stub_kernel.kwargs["cross_session"])
        # whitelisted trigger user: ready + default config keeps all tools
        self.assertEqual(req.tool_set.names, set(mod.ALL_TOOLS))

    def test_51_tools_gated_by_config(self):
        inst, req = self._inject(cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "enabled_tools": ["memory_search"],
        })
        self.assertEqual(req.tool_set.names, {"memory_search"})

    def test_51b_enabled_tools_empty_disables_all(self):
        # An explicit empty list = disable all (the schema's
        # "deselect means disable" semantics)
        inst, req = self._inject(cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "enabled_tools": [],
        })
        self.assertEqual(req.tool_set.names, set())

    def test_51c_tools_removed_for_non_whitelisted_trigger(self):
        # non-whitelisted trigger user: all memory tools removed for the
        # whole turn regardless of enabled_tools
        inst, req = self._inject(cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "enabled_tools": ["memory_search"],
            "allowed_users": ["someone-else"],
        })
        self.assertEqual(req.tool_set.names, set())

    def test_51d_tools_removed_when_whitelist_empty(self):
        # fail-closed: empty allowed_users removes the tools for everyone
        inst, req = self._inject(cfg={"dsn": "postgres://u:p@127.0.0.1:5432/db",
                                      "allowed_users": []})
        self.assertEqual(req.tool_set.names, set())

    def test_52_no_messages_no_prompts(self):
        inst, req = self._inject(with_msgs=False)
        self.assertEqual(req.user_prompt, [])
        self.assertEqual(req.system_prompt, [])
        # identity-less batch -> whitelist fail-closed removes the tools
        # (the "ready keeps all tools enabled" assertion moved to test_50)
        self.assertEqual(req.tool_set.names, set())

    def test_52b_session_scoped_switch(self):
        # Isolation-switch flipped side: explicitly off (false) ->
        # cross_session=True (the on-path assertion for the default
        # isolation -> cross_session=False lives in test_50)
        inst, req = self._inject(cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "summary_recall_session_scoped": False,
        })
        self.assertTrue(self._stub_kernel.kwargs["cross_session"])

    def test_52c_rollout_prompt_prepended_before_recall(self):
        # The rolling rollout fill-back block is injected as a separate
        # Prompt, and its timeline precedes the recall block
        inst, req = self._inject()
        names = [p.name for p in req.user_prompt]
        self.assertIn(f"{mod.PLUGIN_ID}:rollout", names)
        self.assertLess(
            names.index(f"{mod.PLUGIN_ID}:rollout"),
            names.index(f"{mod.PLUGIN_ID}:recall"),
            "滚动补回块应先于 recall 块（时间线旧→新）",
        )
        rollout = next(
            p for p in req.user_prompt if p.name.endswith(":rollout")
        )
        self.assertIn("# 更早对话摘要", rollout.content)
        self.assertEqual(self._stub_kernel.rollout_session, "10086")

    def test_52d_rollout_disabled_by_config(self):
        inst, req = self._inject(cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "recent_rollout_enabled": False,
        })
        names = [p.name for p in req.user_prompt]
        self.assertNotIn(f"{mod.PLUGIN_ID}:rollout", names)
        self.assertFalse(hasattr(self._stub_kernel, "rollout_session"))

    def test_52e_fallback_to_system_without_user_prompt(self):
        # Older host without the user_prompt attribute: fall back to the
        # system prompt, injection is not lost
        inst, req = self._inject(user_prompt=None)
        names = [p.name for p in req.system_prompt]
        self.assertIn(f"{mod.PLUGIN_ID}:rollout", names)
        self.assertIn(f"{mod.PLUGIN_ID}:recall", names)
        self.assertIn(f"{mod.PLUGIN_ID}:profile", names)
        self.assertFalse(hasattr(req, "user_prompt"))

    def test_52f_fallback_to_system_when_user_prompt_immutable(self):
        # user_prompt is not a list (immutable type): same fallback,
        # without raising TypeError
        inst, req = self._inject(user_prompt=())
        names = [p.name for p in req.system_prompt]
        self.assertIn(f"{mod.PLUGIN_ID}:recall", names)
        self.assertIn(f"{mod.PLUGIN_ID}:profile", names)


class TestPluginTools(unittest.TestCase):
    # default whitelist holds the trigger user 9900000002 (matches the
    # sender in _tool_event); denial-path tests override allowed_users
    ALLOWED_UID = "9900000002"

    def _inst(self, extra_cfg=None):
        cfg = {"dsn": "postgres://u:p@127.0.0.1:5432/db",
               "allowed_users": [self.ALLOWED_UID]}
        if extra_cfg:
            cfg.update(extra_cfg)
        return _ready_plugin(mod, cfg=cfg)[0]

    def _tool_event(self, uid=None):
        # framework dispatches tools as func(event, **args); the event carries
        # the triggering session context
        return types.SimpleNamespace(
            session=FakeSession(sid="napcat:dm:9900000002", stype="dm"),
            message=make_msg(uid or self.ALLOWED_UID, "上次我们聊了什么？", mid="m-tool"),
        )

    def _bare_event(self):
        # passes the whitelist (first non-notice batch sender is listed) but
        # carries no session context; replaces the old event=None calls that
        # fail-closed now rejects unconditionally
        return types.SimpleNamespace(
            messages=[make_msg(self.ALLOWED_UID, "上次我们聊了什么？", mid="m-bare")]
        )

    def _scope_kernel(self, captured):
        class ScopeKernel:
            async def search(self, **kwargs):
                captured.update(kwargs)
                return []
        return ScopeKernel()

    def test_53_search_requires_scope(self):
        inst = self._inst()
        # whitelisted trigger user but no session context and no explicit
        # scope -> the tool asks for explicit session_id/user_id
        out = asyncio.run(inst.memory_search(self._bare_event(), query="猫"))
        self.assertIn("session_id 或 user_id", out)

    def test_53b_search_framework_dispatch_signature(self):
        # Reproduces the production framework dispatch func(event,
        # **args): the old signature (no event parameter) would raise
        # "got multiple values for argument 'query'"
        inst = self._inst()
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", top_k=10))
        self.assertTrue(isinstance(out, str))

    def test_53c_search_scope_falls_back_to_event(self):
        # When the LLM passes no session_id/user_id, fall back to the
        # triggering event (session scope)
        inst = self._inst()
        captured = {}
        inst._memory_kernel = self._scope_kernel(captured)
        asyncio.run(inst.memory_search(self._tool_event(), query="猫"))
        self.assertEqual(captured.get("scope"), "session")
        self.assertEqual(captured.get("session_id"), "napcat:dm:9900000002")
        self.assertEqual(captured.get("user_id"), "9900000002")

    def test_53d_explicit_user_scope_not_overridden(self):
        # Lock off: the LLM passes only user_id explicitly -> keep the
        # cross-session user scope, do not override with the event (the
        # explicit-argument semantics of tool_scope_locked=false)
        inst = self._inst(extra_cfg={"tool_scope_locked": False})
        captured = {}
        inst._memory_kernel = self._scope_kernel(captured)
        asyncio.run(inst.memory_search(self._tool_event(), query="猫", user_id="u9"))
        self.assertEqual(captured.get("scope"), "user")
        self.assertEqual(captured.get("user_id"), "u9")

    def test_53d2_explicit_scope_overridden_when_locked(self):
        # Lock on (default): explicit session_id/user_id are ignored and
        # pinned to the triggering session/trigger user -- prevents a
        # whitelist-passing caller from searching other people's memory
        # sideways via arguments
        inst = self._inst()
        captured = {}
        inst._memory_kernel = self._scope_kernel(captured)
        asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", user_id="u9", session_id="other-sess",
        ))
        self.assertEqual(captured.get("scope"), "session")
        self.assertEqual(captured.get("session_id"), "napcat:dm:9900000002")
        self.assertEqual(captured.get("user_id"), "9900000002")

    def test_53e_search_denied_for_non_whitelisted_user(self):
        # code-level gate: non-whitelisted trigger user -> rejected before
        # the kernel is ever touched
        inst = self._inst()
        captured = {}
        inst._memory_kernel = self._scope_kernel(captured)
        out = asyncio.run(inst.memory_search(self._tool_event(uid="999"), query="猫"))
        self.assertIn("权限不足", out)
        self.assertIn("allowed_users", out)
        self.assertEqual(captured, {})

    def test_53f_denied_without_whitelist_config(self):
        # fail-closed: missing/empty allowed_users rejects everyone
        inst, _ = _ready_plugin(mod, cfg={"dsn": "postgres://u:p@127.0.0.1:5432/db"})
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫"))
        self.assertIn("权限不足", out)
        out = asyncio.run(inst.memory_write(self._tool_event(), text="记住"))
        self.assertIn("权限不足", out)
        out = asyncio.run(inst.memory_remove(self._tool_event(), document_id="d1"))
        self.assertIn("权限不足", out)

    def test_53g_batch_event_trigger_user(self):
        # real dispatch passes a batch event (messages only): the trigger
        # user is the first non-notice sender
        inst = self._inst()
        captured = {}
        inst._memory_kernel = self._scope_kernel(captured)
        event = types.SimpleNamespace(
            session=FakeSession(sid="napcat:gm:10086"),
            messages=[
                make_msg("sys", "撤回了一条消息", notice=True),
                make_msg(self.ALLOWED_UID, "上次聊了什么？", mid="m-b1"),
            ],
        )
        out = asyncio.run(inst.memory_search(event, query="猫"))
        self.assertNotIn("权限不足", out)
        self.assertEqual(captured.get("session_id"), "napcat:gm:10086")

    def test_53h_platform_prefixed_whitelist_entry(self):
        # entries support <platform>:<user_id>: identical uids on different
        # platforms do not cross-match
        inst = self._inst(extra_cfg={"allowed_users": ["napcat:9900000002"]})
        self.assertIsNone(inst._whitelist_denial(self._tool_event()))
        inst2 = self._inst(extra_cfg={"allowed_users": ["telegram:9900000002"]})
        denial = inst2._whitelist_denial(self._tool_event())
        self.assertIsNotNone(denial)
        self.assertIn("权限不足", denial)

    def test_53i_trigger_user_derivation(self):
        f = mod.NoriflowMemoryPlugin._trigger_user
        # batch: first non-notice sender + session platform
        ev = types.SimpleNamespace(
            session=FakeSession(sid="napcat:gm:1"),
            messages=[make_msg("s0", "n", notice=True), make_msg("u1", "hi")],
        )
        self.assertEqual(f(ev), ("u1", "napcat"))
        # single-message event: message.sender
        ev2 = types.SimpleNamespace(message=make_msg("u2", "hi"))
        self.assertEqual(f(ev2), ("u2", ""))
        # nothing to derive -> ("", "")
        self.assertEqual(f(None), ("", ""))

    def test_53j_session_whitelist_full_sid_entry(self):
        # full sid entry allows any member of that session even when the
        # trigger user is not in allowed_users
        inst = self._inst(extra_cfg={
            "allowed_users": [], "allowed_sessions": ["napcat:dm:9900000002"],
        })
        self.assertIsNone(inst._whitelist_denial(self._tool_event(uid="someone-else")))

    def test_53k_session_whitelist_bare_and_platform_entries(self):
        # bare session id and <platform>:<session_id> both match a
        # napcat:gm:10086 session
        ev = types.SimpleNamespace(
            session=FakeSession(sid="napcat:gm:10086"),
            messages=[make_msg("u7", "hi")],
        )
        for entry in ("10086", "napcat:10086", "napcat:gm:10086"):
            inst = self._inst(extra_cfg={
                "allowed_users": [], "allowed_sessions": [entry],
            })
            self.assertIsNone(
                inst._whitelist_denial(ev),
                f"entry {entry!r} should match napcat:gm:10086",
            )

    def test_53l_session_whitelist_no_cross_platform_match(self):
        # a bare session id matches across platforms only by explicit
        # platform entry; <platform>:<id> never matches another platform
        inst = self._inst(extra_cfg={
            "allowed_users": [], "allowed_sessions": ["telegram:10086"],
        })
        denial = inst._whitelist_denial(types.SimpleNamespace(
            session=FakeSession(sid="napcat:gm:10086"),
            messages=[make_msg("u7", "hi")],
        ))
        self.assertIsNotNone(denial)
        self.assertIn("权限不足", denial)

    def test_53m_session_whitelist_denied_other_session(self):
        # configured session whitelist + unmatched session -> denied (even
        # with a user whitelist empty)
        inst = self._inst(extra_cfg={
            "allowed_users": [], "allowed_sessions": ["napcat:gm:99999"],
        })
        captured = {}
        inst._memory_kernel = self._scope_kernel(captured)
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫"))
        self.assertIn("权限不足", out)
        self.assertEqual(captured, {})

    def test_53n_session_whitelist_either_dimension_allows(self):
        # user hit OR session hit allows; user miss + session hit passes
        inst = self._inst(extra_cfg={"allowed_sessions": ["napcat:dm:9900000002"]})
        # default allowed_users holds ALLOWED_UID too; uid=888 relies on
        # the session whitelist alone
        self.assertIsNone(inst._whitelist_denial(self._tool_event(uid="888")))

    def test_53o_both_whitelists_empty_fail_closed(self):
        # both lists empty/missing: everyone denied (fail-closed)
        inst, _ = _ready_plugin(mod, cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "allowed_users": [], "allowed_sessions": [],
        })
        self.assertIsNotNone(inst._whitelist_denial(self._tool_event()))

    def test_53p_session_whitelist_without_session_context(self):
        # session whitelist configured but event carries no session: only
        # the user whitelist can still allow (here it does via allowed_users)
        inst = self._inst(extra_cfg={"allowed_sessions": ["napcat:gm:10086"]})
        self.assertIsNone(inst._whitelist_denial(self._bare_event()))
        inst2 = self._inst(extra_cfg={
            "allowed_users": [], "allowed_sessions": ["napcat:gm:10086"],
        })
        # no session context + empty user whitelist -> denied
        self.assertIsNotNone(inst2._whitelist_denial(self._bare_event()))

    def test_53q_tools_removed_for_unlisted_session(self):
        # per-turn tool_set removal also honors the session whitelist:
        # non-listed session strips all memory tools even though the
        # trigger user stays out of allowed_users
        inst, _ = _ready_plugin(mod, cfg={
            "dsn": "postgres://u:p@127.0.0.1:5432/db",
            "allowed_users": [],
            "allowed_sessions": ["napcat:gm:99999"],
        })
        req = FakeReq()
        req.tool_set.names = set(mod.ALL_TOOLS)
        event = types.SimpleNamespace(
            session=FakeSession(sid="napcat:gm:10086"),
            messages=[make_msg("u7", "上次聊了什么？", mid="m-x")],
        )
        asyncio.run(inst.inject_memory(event, req, None))
        self.assertEqual(req.tool_set.names, set())

    def test_53r_name_tools_resolve_against_composite_session_sid(self):
        # EntityDirectory is keyed by Session.sid (platform:type:id), while
        # _event_scope exposes the host's bare session_id. Name-based tools
        # must pass the composite key or every profile/fact lookup misses.
        inst = self._inst()
        calls = []

        class NameKernel:
            async def entity_hint_entries(self, sid, text):
                calls.append((sid, text))
                if sid == "napcat:gm:10086":
                    return [("小李", "napcat", "u9")]
                return []

        class Persona:
            async def build_profile_text(self, uid, *, session_id, platform):
                return f"profile:{platform}:{uid}:{session_id}"

        inst._memory_kernel = NameKernel()
        inst._persona_service = Persona()
        event = types.SimpleNamespace(
            session=FakeSession(sid="10086", stype="gm"),
            message=make_msg(self.ALLOWED_UID, "小李是谁"),
        )

        out = asyncio.run(inst.memory_profile(event, name="小李"))

        self.assertEqual(out, "profile:napcat:u9:10086")
        self.assertEqual(calls, [("napcat:gm:10086", "小李")])

    def test_54_search_with_session(self):
        inst = self._inst()
        # The stub db has no search methods: kernel.search goes through
        # db.search_chat_summaries (FakeDB lacks it -> exception)
        out = asyncio.run(inst.memory_search(self._bare_event(), query="猫", session_id="s"))
        # FakeDB lacks the method -> the tool returns failure text
        # instead of raising
        self.assertTrue(isinstance(out, str))

    def test_53j_search_cross_session_follows_config(self):
        # Fix A: the tool path's cross-session posture follows
        # summary_recall_session_scoped (search previously hard-defaulted
        # cross_session=False, so the config only affected the injection
        # path)
        captured = {}
        inst = self._inst(extra_cfg={"summary_recall_session_scoped": False})
        inst._memory_kernel = self._scope_kernel(captured)
        asyncio.run(inst.memory_search(self._tool_event(), query="猫"))
        self.assertTrue(captured.get("cross_session"))
        captured2 = {}
        inst2 = self._inst()  # default summary_recall_session_scoped=True
        inst2._memory_kernel = self._scope_kernel(captured2)
        asyncio.run(inst2.memory_search(self._tool_event(), query="猫"))
        self.assertFalse(captured2.get("cross_session"))

    def test_53k_entity_keys_passed_to_search(self):
        # Tool-path B: query entity hits are merged into the search key
        # groups (gated by recall_hint_enabled)
        captured = {}

        class EntKernel:
            async def entity_hint_entries(self, sid, text):
                return [("小李", "seki", "9900000003")]

            async def search(self, **kwargs):
                captured.update(kwargs)
                return []

        inst = self._inst()
        inst._memory_kernel = EntKernel()
        asyncio.run(inst.memory_search(self._tool_event(), query="小李说过什么"))
        self.assertEqual(captured.get("entity_user_keys"), ["seki:9900000003"])
        self.assertEqual(captured.get("entity_user_ids"), ["9900000003"])

    def test_53l_entity_match_skipped_when_hint_disabled(self):
        # recall_hint_enabled=False: no entity matching, key groups stay
        # empty (None)
        captured = {}

        class EntKernel:
            async def entity_hint_entries(self, sid, text):
                raise AssertionError("recall_hint_enabled=False 不应触发实体匹配")

            async def search(self, **kwargs):
                captured.update(kwargs)
                return []

        inst = self._inst(extra_cfg={"recall_hint_enabled": False})
        inst._memory_kernel = EntKernel()
        asyncio.run(inst.memory_search(self._tool_event(), query="小李说过什么"))
        self.assertIsNone(captured.get("entity_user_keys"))
        self.assertIsNone(captured.get("entity_user_ids"))

    def test_55_write_returns_id(self):
        inst = self._inst()
        # tool-scope lock pins ownership to the triggering event; the event
        # must carry a session (bare events are rejected symmetrically with
        # memory_remove -- see test_55e). v2 semantics: deterministic
        # facts are written directly (nori-aligned)
        out = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            session_id="s", user_id="u1", platform="napcat",
        ))
        self.assertIn("已记住（簇 555", out)
        self.assertEqual(len(inst._db.inserted_facts), 1)
        self.assertEqual(
            inst._db.applied_merges[0].get("action"), "create"
        )

    def test_55d_write_reports_failure_during_breaker(self):
        # P2-2 side effect: during circuit-breaker rejection
        # memory_write reports the write failure (the old contract
        # faked success)
        inst = self._inst()
        for _ in range(5):
            asyncio.run(inst._memory_kernel.circuit_breaker.record_failure())
        out = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            session_id="s", user_id="u1", platform="napcat",
        ))
        self.assertTrue(out.startswith("写入失败"), out)
        self.assertEqual(inst._db.inserted_facts, [])

    def test_55e_write_rejects_when_scope_undervable(self):
        # P3 symmetry: with scope lock on and no session/user derivable
        # from the trigger event -> reject explicitly (the old behavior
        # silently wrote a row with empty attribution, asymmetric with
        # memory_remove)
        inst = self._inst()
        out = asyncio.run(inst.memory_write(
            self._bare_event(), text="他最爱猫", category="preference",
            session_id="s", user_id="u1", platform="napcat",
        ))
        self.assertIn("拒绝写入", out)
        self.assertEqual(inst._db.inserted_facts, [])

    def test_55f_write_rejects_non_integer_replaces(self):
        # The LLM passes the cluster id as a non-numeric string (the
        # schema says integer but cannot stop it): it must return a
        # readable error instead of letting ValueError blow through the
        # handler
        inst = self._inst()
        out = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            replaces_cluster_id="旧簇",
        ))
        self.assertIn("必须是整数", out)
        self.assertEqual(inst._db.inserted_facts, [])

    def test_55g_write_replace_owner_opaque(self):
        # Existence-leak regression lock: "not found" and "owned by
        # someone else" for the replace target must produce identical
        # output -- cluster ids are auto-incremented and enumerable, so
        # two different messages would be a existence oracle
        inst = self._inst()
        inst._db.cluster_owner_result = None
        out_a = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            replaces_cluster_id=123,
        ))
        inst._db.cluster_owner_result = ("napcat", "someone-else")
        out_b = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            replaces_cluster_id=123,
        ))
        self.assertEqual(out_a, out_b)
        self.assertIn("不存在或不属于当前用户", out_a)
        self.assertEqual(inst._db.inserted_facts, [])

    def test_55b_write_scope_falls_back_to_event(self):
        # The LLM passes only text: session/user/platform are filled in
        # from the triggering event
        inst = self._inst()
        out = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
        ))
        self.assertIn("已记住（簇 555", out)
        row = inst._db.inserted_facts[0]
        self.assertEqual(row.get("session_id"), "napcat:dm:9900000002")
        self.assertEqual(row.get("platform"), "napcat")

    def test_55c_write_denied_for_non_whitelisted_user(self):
        # a denied call must not write anything
        inst = self._inst()
        out = asyncio.run(inst.memory_write(
            self._tool_event(uid="999"), text="他最爱猫", category="preference",
        ))
        self.assertIn("权限不足", out)
        # The write path now uses raw upsert + apply_fact_merge, so
        # inserted_summaries stays empty (the old assertion is obsolete);
        # the double-empty assertion aligns with the inserted_facts
        # convention of 55d/55e
        self.assertEqual(inst._db.inserted_facts, [])
        self.assertEqual(inst._db.applied_merges, [])

    def _embed_kernel(self):
        # memory_lookup only uses kernel.embedding_service.embed_one
        class EmbSvc:
            async def embed_one(self, text):
                return [1.0]

        class Kernel:
            embedding_service = EmbSvc()

        return Kernel()

    def _fact_kernel(self, entries=None, qualifier=None):
        class EmbSvc:
            async def embed_one(self, text):
                return [1.0]

        class Kernel:
            embedding_service = EmbSvc()

            async def entity_hint_entries(self, sid, text):
                return list(entries or [])

            def _edge_time_qualifier(self, value):
                if qualifier is not None:
                    return qualifier
                return ""

        return Kernel()

    def _relation_edge(self, subject="u2", object_="u3", platform="napcat"):
        return {
            "platform": platform,
            "subject_uid": subject,
            "object_uid": object_,
            "subject_name": "旧名2",
            "object_name": "旧名3",
            "relation_label": "朋友",
            "last_seen": datetime(2026, 10, 13, 12, 0),
            "evidence_count": 2,
        }

    def test_56c_search_fact_attaches_relations_for_named_member(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel([("小李", "qq", "u2")])
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "qq", "user_id": "u2", "score": 0.9,
            "canonical_statement": "喜欢猫", "category": "preference",
            "status": "active", "evidence_count": 2,
        }]
        inst._db.edges = [self._relation_edge(platform="qq")]
        inst._db.alias_names = {("qq", "u2"): "小李", ("qq", "u3"): "小王"}
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact", name="小李",
        ))
        self.assertIn("【事实】", out)
        self.assertIn("【关系】", out)
        self.assertIn("小李(u2)的朋友是小王(u3)", out)
        self.assertEqual(inst._db.edge_calls[0], ("edges", ["qq:u2"]))
        self.assertEqual(inst._db.edge_calls[1], ("alias_names", [("qq", "u2"), ("qq", "u3")]))

    def test_56d_search_fact_without_name_attaches_triggerer_relations(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, object_="u3")]
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact",
        ))
        self.assertIn("【关系】", out)
        self.assertEqual(inst._db.edge_calls[0], ("edges", ["napcat:9900000002"]))

    def test_56e_search_fact_with_only_relations_returns_relation_section(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, object_="u3")]
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertTrue(out.startswith("【关系】"))
        self.assertNotIn("没有找到相关事实", out)

    def test_56f_search_fact_relation_lookup_fail_open(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "napcat", "user_id": self.ALLOWED_UID,
            "score": 0.9, "canonical_statement": "喜欢猫",
            "category": "preference", "status": "active", "evidence_count": 1,
        }]
        inst._db.edges_exc = RuntimeError("edge unavailable")
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("【事实】", out)
        self.assertIn("【关系】", out)
        self.assertIn("关系边暂不可查询", out)

    def test_56g_search_fact_alias_failure_keeps_snapshot_names(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel([("小李", "napcat", "u2")])
        inst._db.edges = [self._relation_edge()]
        inst._db.alias_names_exc = RuntimeError("aliases unavailable")
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact", name="小李",
        ))
        self.assertIn("旧名2(u2)的朋友是旧名3(u3)", out)

    def test_56h_search_fact_reuses_kernel_edge_time_qualifier(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel(qualifier="截至10月13日")
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID)]
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("截至10月13日", out)

    def test_56aa_format_edge_time_uses_passed_kernel(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel(qualifier="self-kernel")
        passed_kernel = self._fact_kernel(qualifier="passed-kernel")
        self.assertEqual(
            inst._format_edge_time(passed_kernel, datetime.now()),
            "passed-kernel",
        )

    def test_56i_search_fact_malformed_edge_time_keeps_fact_results(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "napcat", "user_id": self.ALLOWED_UID,
            "score": 0.9, "canonical_statement": "喜欢猫",
            "category": "preference", "status": "active", "evidence_count": 1,
        }]
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID)]
        inst._memory_kernel._edge_time_qualifier = lambda value: (_ for _ in ()).throw(AttributeError("bad"))
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("【事实】", out)
        self.assertIn("【关系】", out)
        self.assertNotIn("截至", out)

    def test_56j_search_category_relation_is_rejected(self):
        inst = self._inst()
        for target in ("fact", "summary"):
            out = asyncio.run(inst.memory_search(self._tool_event(), query="关系", target=target, category="relation"))
            self.assertIn("category 只能是", out)
            self.assertNotIn("relation", out)

    def test_56k_search_category_enum_has_no_relation(self):
        tool = next(t for t in mod.register.tools if t["name"] == "memory_search")
        enum = tool["params"]["properties"]["category"]["enum"]
        self.assertNotIn("relation", enum)
        self.assertEqual(set(enum), set(mod._ACTIVE_CATEGORIES))
        self.assertNotIn("query", tool["params"].get("required", []))

    def test_56l_search_fact_skips_relation_header_when_all_edges_fail_rendering(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID)]
        with unittest.mock.patch.object(mod, "relation_statement_line", side_effect=RuntimeError("bad edge")):
            out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertNotIn("【关系】", out)

    def test_56m_search_fact_limits_relation_edges_to_top_k(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, object_=f"u{i}") for i in range(3)]
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact", top_k=2))
        self.assertEqual(out.count("- 旧名2(9900000002)的朋友是"), 2)

    def test_56n_search_fact_relation_db_unavailable_fails_open(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "napcat", "user_id": self.ALLOWED_UID,
            "score": 0.9, "canonical_statement": "喜欢猫",
            "category": "preference", "status": "active", "evidence_count": 1,
        }]
        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable
        inst._db.edges_exc = KEXC("db down")
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("【事实】", out)
        self.assertNotIn("记忆检索暂时不可用", out)
        self.assertIn("【关系】", out)
        self.assertIn("关系边暂不可查询", out)

    def test_56n2_search_fact_alias_db_unavailable_keeps_snapshot_edges(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID)]
        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable
        inst._db.alias_names_exc = KEXC("db down")
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("【关系】", out)
        self.assertIn("旧名2(9900000002)", out)

    def test_56o_search_fact_with_no_facts_or_relations_reports_combined_empty(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertEqual(out, "没有找到相关事实或关系。")

    def test_56p_search_fact_recovers_platform_for_explicit_scope(self):
        inst = self._inst(extra_cfg={"tool_scope_locked": False})
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "napcat", "user_id": self.ALLOWED_UID,
            "score": 0.9, "canonical_statement": "喜欢猫",
            "category": "preference", "status": "active", "evidence_count": 1,
        }]
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID)]
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact",
            session_id="other-session", user_id=self.ALLOWED_UID,
        ))
        self.assertIn("【关系】", out)
        self.assertEqual(inst._db.edge_calls[0], ("edges", ["napcat:" + self.ALLOWED_UID]))
        self.assertEqual(inst._db.fact_search_kwargs["platform"], "")

    def test_56q_search_fact_relation_anchor_does_not_use_last_fact_row_owner(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [
            {"id": 1, "platform": "napcat", "user_id": self.ALLOWED_UID,
             "score": 0.9, "canonical_statement": "事实1", "category": "stable",
             "status": "active", "evidence_count": 1},
            {"id": 2, "platform": "napcat", "user_id": "other",
             "score": 0.8, "canonical_statement": "事实2", "category": "stable",
             "status": "active", "evidence_count": 1},
        ]
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID)]
        asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertEqual(inst._db.edge_calls[0], ("edges", ["napcat:" + self.ALLOWED_UID]))

    def test_56r_search_fact_named_member_allows_empty_query(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel([("小李", "qq", "u2")])
        inst._db.edges = [self._relation_edge(platform="qq")]
        out = asyncio.run(inst.memory_search(
            self._tool_event(), target="fact", name="小李",
        ))
        self.assertIn("【关系】", out)

    def test_56s_search_fact_renders_bot_endpoint_with_bot_nickname(self):
        inst = self._inst()
        inst._bot_user_id = "bot01"
        inst._bot_nickname = "Kira"
        inst._memory_kernel = self._fact_kernel()
        inst._db.edges = [self._relation_edge(subject="bot01", object_=self.ALLOWED_UID)]
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("Kira(bot01)", out)

    def test_56z_search_fact_bot_kernel_fallback_is_preserved(self):
        inst = self._inst()
        inst._bot_user_id = ""
        inst._bot_nickname = "Kira"
        kernel = self._fact_kernel()
        kernel.bot_id = "kernel-bot"
        kernel._bot_uid_forms_all = lambda explicit: [explicit] if explicit else []
        inst._memory_kernel = kernel
        inst._db.edges = [self._relation_edge(subject="kernel-bot", object_=self.ALLOWED_UID)]
        out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
        self.assertIn("Kira(kernel-bot)", out)

    def test_56t_search_fact_platform_recovery_without_session(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "qq", "user_id": self.ALLOWED_UID,
            "score": 1, "canonical_statement": "喜欢猫", "category": "stable",
            "status": "active", "evidence_count": 1,
        }]
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, platform="qq")]
        out = asyncio.run(inst.memory_search(self._bare_event(), query="猫", target="fact"))
        self.assertIn("【事实】", out)
        self.assertIn("【关系】", out)
        self.assertEqual(inst._db.edge_calls[0], ("edges", ["qq:" + self.ALLOWED_UID]))

    def test_56u_search_fact_uses_session_platform_and_marks_missing_context(self):
        inst = self._inst(extra_cfg={"tool_scope_locked": False})
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": i, "platform": p, "user_id": self.ALLOWED_UID,
            "score": 1, "canonical_statement": "喜欢猫", "category": "stable",
            "status": "active", "evidence_count": 1,
        } for i, p in enumerate(("qq", "telegram"))]
        inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, platform="napcat")]
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact", user_id=self.ALLOWED_UID,
        ))
        self.assertIn("【关系】", out)
        self.assertEqual(inst._db.edge_calls[0], ("edges", ["napcat:" + self.ALLOWED_UID]))

        bare = self._inst(extra_cfg={"tool_scope_locked": False})
        bare._memory_kernel = self._fact_kernel()
        bare._db.fact_search_rows = []
        out = asyncio.run(bare.memory_search(self._bare_event(), query="猫", target="fact"))
        self.assertIn("平台", out)
        self.assertIn("暂不可", out)
        self.assertEqual(bare._db.edge_calls, [])

    def test_56ab_search_fact_empty_facts_relation_error_is_not_empty_result(self):
        KEXC = sys.modules["noriflow_memory_pkg.memory_kernel"].MemoryDBUnavailable
        for error in (RuntimeError("edge read failed"), KEXC("db down")):
            with self.subTest(error=type(error).__name__):
                inst = self._inst()
                inst._memory_kernel = self._fact_kernel()
                inst._db.edges_exc = error
                out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact"))
                self.assertIn("关系边暂不可查询", out)
                self.assertNotIn("没有找到相关事实或关系", out)

    def test_56ac_search_fact_cross_platform_rows_mark_relation_scope(self):
        for has_edge in (False, True):
            with self.subTest(has_edge=has_edge):
                inst = self._inst(extra_cfg={"tool_scope_locked": False})
                inst._memory_kernel = self._fact_kernel()
                inst._db.fact_search_rows = [{
                    "id": 1, "platform": "qq", "user_id": self.ALLOWED_UID,
                    "score": 1, "canonical_statement": "喜欢猫", "category": "stable",
                    "status": "active", "evidence_count": 1,
                }]
                inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, platform="qq")]
                if has_edge:
                    inst._db.edges.append(self._relation_edge(subject=self.ALLOWED_UID))
                out = asyncio.run(inst.memory_search(
                    self._tool_event(), query="猫", target="fact", user_id=self.ALLOWED_UID,
                ))
                self.assertIn("关系边仅限当前会话平台 napcat", out)
                self.assertIn("未查询事实涉及的其他平台：qq", out)
                self.assertIn("【事实】", out)
                self.assertEqual(inst._db.edge_calls[0], ("edges", ["napcat:" + self.ALLOWED_UID]))
                self.assertEqual(inst._db.fact_search_kwargs["platform"], "")

    def test_56ad_search_fact_multiple_platforms_without_session_are_marked(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": i, "platform": p, "user_id": self.ALLOWED_UID,
            "score": 1, "canonical_statement": "喜欢猫", "category": "stable",
            "status": "active", "evidence_count": 1,
        } for i, p in enumerate(("qq", "telegram"))]
        out = asyncio.run(inst.memory_search(self._bare_event(), query="猫", target="fact"))
        self.assertIn("【事实】", out)
        self.assertIn("关系边暂不可查询：缺少平台上下文", out)
        self.assertEqual(inst._db.edge_calls, [])

    def test_56af_search_fact_no_rows_without_session_marks_empty_and_context(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel()
        out = asyncio.run(inst.memory_search(self._bare_event(), query="猫", target="fact"))
        self.assertNotIn("没有找到相关事实或关系", out)
        self.assertIn("关系边暂不可查询：缺少平台上下文", out)
        self.assertEqual(inst._db.edge_calls, [])

    def test_56ag_relation_failure_does_not_append_platform_scope_note(self):
        inst = self._inst(extra_cfg={"tool_scope_locked": False})
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": 1, "platform": "qq", "user_id": self.ALLOWED_UID,
            "score": 1, "canonical_statement": "喜欢猫", "category": "stable",
            "status": "active", "evidence_count": 1,
        }]
        inst._db.edges_exc = RuntimeError("edge read failed")
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact", user_id=self.ALLOWED_UID,
        ))
        self.assertIn("【事实】", out)
        self.assertIn("关系边暂不可查询", out)
        self.assertNotIn("关系边仅限当前会话平台", out)
        self.assertNotIn("未查询事实涉及的其他平台", out)

    def test_56ae_search_fact_other_uid_multiple_platforms_are_marked(self):
        inst = self._inst(extra_cfg={"tool_scope_locked": False})
        inst._memory_kernel = self._fact_kernel()
        inst._db.fact_search_rows = [{
            "id": i, "platform": p, "user_id": "other",
            "score": 1, "canonical_statement": "喜欢猫", "category": "stable",
            "status": "active", "evidence_count": 1,
        } for i, p in enumerate(("qq", "telegram"))]
        out = asyncio.run(inst.memory_search(
            self._tool_event(), query="猫", target="fact", user_id="other",
        ))
        self.assertIn("【事实】", out)
        self.assertNotIn("没有找到相关事实或关系", out)
        self.assertIn("关系边暂不可查询：缺少平台上下文", out)
        self.assertEqual(inst._db.edge_calls, [])

    def test_56v_relation_fake_obeys_empty_scope_and_recent_edge_order(self):
        db = FakeDB()
        db.edges = [self._relation_edge(), self._relation_edge(object_="u4")]
        db.edges[1]["last_seen"] += timedelta(days=1)
        self.assertEqual(asyncio.run(db.fetch_active_edges([])), [])
        self.assertEqual(asyncio.run(db.fetch_active_edges(["napcat:u2"]))[0]["object_uid"], "u4")
        db.alias_names = {("napcat", "u2"): "小李"}
        self.assertEqual(asyncio.run(db.fetch_alias_names_by_owner([])), {})
        self.assertEqual(asyncio.run(db.fetch_alias_names_by_owner(None)), db.alias_names)

    def test_56w_search_fact_bot_uid_on_other_platform_keeps_member_name(self):
        inst = self._inst()
        inst._bot_user_id = "bot01"
        inst._bot_nickname = "Kira"
        inst._memory_kernel = self._fact_kernel([("小李", "qq", "u2")])
        inst._db.edges = [self._relation_edge(object_="bot01", platform="qq")]
        out = asyncio.run(inst.memory_search(self._tool_event(), target="fact", name="小李"))
        self.assertIn("旧名3(bot01)", out)
        self.assertNotIn("Kira(bot01)", out)

    def test_56x_search_fact_relation_top_k_clamping(self):
        for top_k, count in ((-1, 1), (0, 5), (None, 5), ("bad", 5), (100, 10)):
            with self.subTest(top_k=top_k):
                inst = self._inst()
                inst._memory_kernel = self._fact_kernel()
                inst._db.edges = [self._relation_edge(subject=self.ALLOWED_UID, object_=str(i)) for i in range(12)]
                out = asyncio.run(inst.memory_search(self._tool_event(), query="猫", target="fact", top_k=top_k))
                self.assertEqual(out.count("- "), count)

    def test_56y_search_fact_one_alias_update_and_owner_dedup(self):
        inst = self._inst()
        inst._memory_kernel = self._fact_kernel([("小李", "qq", "u2")])
        inst._db.edges = [self._relation_edge(platform="qq"), self._relation_edge(platform="qq")]
        inst._db.alias_names = {("qq", "u2"): "新名2"}
        out = asyncio.run(inst.memory_search(self._tool_event(), target="fact", name="小李"))
        self.assertIn("新名2(u2)的朋友是旧名3(u3)", out)
        self.assertEqual(inst._db.edge_calls[1], ("alias_names", [("qq", "u2"), ("qq", "u3")]))

    def test_55h_write_replaces_same_day_revert(self):
        # Same-day revert regression lock: when the original statement
        # was written today (its row consumed, optimistic lock skipped),
        # a correction with replaces must land a new row under a fresh
        # idempotency key to complete the replacement, and must not
        # report "recorded" for an unexecuted correction -- the wrong
        # statement would otherwise remain in effect in the DB
        inst = self._inst()
        inst._db.cluster_owner_result = ("napcat", self.ALLOWED_UID)
        inst._db.merge_outcomes = [
            {"action": "skipped"},
            {"action": "replace", "cluster_id": 777, "score": 3,
             "status": "active"},
        ]
        out = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            replaces_cluster_id=123,
        ))
        self.assertIn("已更正", out)
        # Two rows land (original idempotency key + correction scope
        # key), keys differ, both merges are replace
        self.assertEqual(len(inst._db.inserted_facts), 2)
        doc_ids = [r["document_id"] for r in inst._db.inserted_facts]
        self.assertNotEqual(doc_ids[0], doc_ids[1])
        self.assertTrue(
            all(m["action"] == "replace" for m in inst._db.applied_merges)
        )

    def test_55i_write_replaces_repeat_correction_idempotent(self):
        # The same correction repeated within the day: after the
        # fresh-key retry still returns skipped, report honestly that
        # the correction already ran today; a plain duplicate write
        # (without replaces) keeps the "already recorded" wording
        inst = self._inst()
        inst._db.cluster_owner_result = ("napcat", self.ALLOWED_UID)
        inst._db.merge_outcomes = [{"action": "skipped"}, {"action": "skipped"}]
        out = asyncio.run(inst.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
            replaces_cluster_id=123,
        ))
        self.assertIn("更正今天已经执行过", out)
        self.assertEqual(len(inst._db.inserted_facts), 2)
        self.assertEqual(len(inst._db.applied_merges), 2)
        inst2 = self._inst()
        inst2._db.merge_outcomes = [{"action": "skipped"}]
        out = asyncio.run(inst2.memory_write(
            self._tool_event(), text="他最爱猫", category="preference",
        ))
        self.assertIn("已经记录过", out)
        self.assertNotIn("更正", out)
        self.assertEqual(len(inst2._db.inserted_facts), 1)

    def test_56_lookup_summary_only_archived(self):
        # Contract regression lock: the tool description promises to
        # search only inactive memories -- live summaries must not
        # appear in the results (unlabeled live rows would be misread by
        # the model under the "invalidated retrieval" semantics). The
        # filter is pushed down to the db side via only_archived, and
        # the tool layer takes top_k directly without enlarging the pool
        inst = self._inst()
        inst._memory_kernel = self._embed_kernel()
        inst._db.summary_search_rows = [
            {"id": 1, "document_id": "d1", "archived": False,
             "relevance": 0.9, "content": "生效摘要"},
            {"id": 2, "document_id": "d2", "archived": True,
             "relevance": 0.5, "content": "归档摘要"},
        ]
        out = asyncio.run(inst.memory_lookup(self._tool_event(), query="猫"))
        self.assertIn("[id:2]", out)
        self.assertIn("[已归档]", out)
        self.assertNotIn("[id:1]", out)
        self.assertNotIn("生效摘要", out)
        # Push-down assertion: the real DB filters in SQL + limit takes
        # top_k directly (default 5)
        self.assertTrue(inst._db.summary_search_kwargs["only_archived"])
        self.assertEqual(inst._db.summary_search_kwargs["limit"], 5)
        # With only live rows, report "nothing found" per the contract
        inst._db.summary_search_rows = [
            {"id": 1, "document_id": "d1", "archived": False,
             "relevance": 0.9, "content": "生效摘要"},
        ]
        out = asyncio.run(inst.memory_lookup(self._tool_event(), query="猫"))
        self.assertIn("没有找到已归档", out)

    def test_56b_lookup_fact_only_inactive(self):
        # Contract regression lock: the fact branch keeps only
        # replaced/dead/pending_uncertain via only_inactive on the SQL
        # side -- active/profiled live clusters never enter the
        # candidate window; a fixed-size pool + caller-side filtering
        # would squeeze the target rows out when live clusters exceed
        # the pool (false negative)
        inst = self._inst()
        inst._memory_kernel = self._embed_kernel()
        inst._db.fact_search_rows = [
            {"id": 11, "platform": "qq", "user_id": "9900000002",
             "status": "active", "canonical_statement": "生效簇",
             "category": "preference", "evidence_count": 2},
            {"id": 12, "platform": "qq", "user_id": "9900000002",
             "status": "dead", "canonical_statement": "失效簇",
             "category": "preference", "evidence_count": 1},
            {"id": 13, "platform": "qq", "user_id": "9900000002",
             "status": "pending_uncertain",
             "canonical_statement": "待定簇", "category": "preference",
             "evidence_count": 1},
        ]
        out = asyncio.run(
            inst.memory_lookup(self._tool_event(), query="猫", target="fact")
        )
        self.assertIn("[簇12][已失效]", out)
        self.assertIn("[簇13][待定]", out)
        # Ownership labeling: consumed rows must carry the owner uid
        # (prevents fact subject drift)
        self.assertIn("（9900000002）", out)
        self.assertNotIn("[簇11]", out)
        self.assertNotIn("生效簇", out)
        # Push-down assertion: SQL-side filtering + limit takes top_k
        # directly
        self.assertTrue(inst._db.fact_search_kwargs["only_inactive"])
        self.assertEqual(inst._db.fact_search_kwargs["limit"], 5)

    def test_56_remove(self):
        # Lock on (default): the delete push-down is limited to the
        # triggering session/user ownership
        inst = self._inst()
        out = asyncio.run(inst.memory_remove(self._tool_event(), document_id="doc-9"))
        self.assertEqual(out, "已删除")
        self.assertEqual(
            inst._db.deleted_ids,
            [("doc-9", {"scope_session_id": "napcat:dm:9900000002",
                        "scope_user_id": "9900000002"})],
        )

    def test_56a_remove_bare_event_rejected_when_locked(self):
        # Lock on but the event has no derivable scope (bare batch
        # event): reject the delete (fail-closed)
        inst = self._inst()
        out = asyncio.run(inst.memory_remove(self._bare_event(), document_id="doc-9"))
        self.assertIn("拒绝删除", out)
        self.assertEqual(inst._db.deleted_ids, [])

    def test_56a2_remove_bare_event_allowed_when_unlocked(self):
        # Lock off: with no derivable scope, delete without scoping per
        # the old behavior
        inst = self._inst(extra_cfg={"tool_scope_locked": False})
        out = asyncio.run(inst.memory_remove(self._bare_event(), document_id="doc-9"))
        self.assertEqual(out, "已删除")
        self.assertEqual(
            inst._db.deleted_ids,
            [("doc-9", {"scope_session_id": "", "scope_user_id": ""})],
        )

    def test_56b_remove_denied_for_non_whitelisted_user(self):
        inst = self._inst()
        out = asyncio.run(inst.memory_remove(self._tool_event(uid="999"), document_id="doc-9"))
        self.assertIn("权限不足", out)
        self.assertEqual(inst._db.deleted_ids, [])

    def test_57_tool_registration_records(self):
        inst = self._inst()
        reg = sys.modules["core.plugin"].register
        names = {t["name"] for t in reg.tools}
        self.assertIn("memory_search", names)
        self.assertIn("memory_write", names)
        self.assertIn("memory_remove", names)
        pages = [p["route"] for p in reg.pages]
        # Pages mount on a non-empty subpath: mounting on the root path
        # leaves residue after the host Mount rstrips (hot-reload 404)
        self.assertEqual(pages, ["/dashboard"])
        api_paths = {a["path"] for a in reg.apis}
        self.assertIn("/memory/overview", api_paths)
        self.assertIn("/memory/profile/{platform}/{user_id}", api_paths)


class TestHelpers(unittest.TestCase):
    def test_58_chain_text_text_only(self):
        chain = [mod.Text("hello"), object(), mod.Text(" world")]
        self.assertEqual(mod.NoriflowMemoryPlugin._chain_text(chain), "hello world")

    def test_59_to_datetime_seconds_and_ms(self):
        d1 = mod.NoriflowMemoryPlugin._to_datetime(1756500000)
        d2 = mod.NoriflowMemoryPlugin._to_datetime(1756500000000)
        self.assertEqual(int(d1.timestamp()), 1756500000)
        self.assertEqual(int(d2.timestamp()), 1756500000)

    def test_60_persona_candidates_exclude_bot(self):
        inst = _ready_plugin(mod)[0]
        inst._bot_user_id = "bot01"
        msgs = [make_msg("u1", "a"), make_msg("u1", "b"), make_msg("bot01", "c")]
        cands = inst._persona_candidates(msgs, "napcat")
        self.assertEqual([c.user_id for c in cands], ["u1"])

    def test_60b_batch_participants_carry_platform(self):
        # Incremental batch participant dedup preserves order (group
        # profile completeness); the trigger user = first speaker
        inst, ctx = _ready_plugin(mod)
        inst._bot_user_id = "bot01"
        session = FakeSession()

        async def scenario():
            for uid, text in (("u1", "a"), ("u2", "b"), ("bot01", "c")):
                await inst.observe_message(types.SimpleNamespace(
                    message=make_msg(uid, text, mid=f"m-{uid}"), session=session
                ))
            await ctx.event_bus.fire(
                "session_memory_updated", {"session": session.sid}
            )
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["participants"], ["napcat:u1", "napcat:u2"])
        self.assertEqual(summaries[0]["user_id"], "u1")


# ---------------------------------------------------------------------------
class TestSummaryState(unittest.TestCase):
    """summarized encoding-state chain: degraded flag + re-encode pass (003_summary_state)."""

    def _unsummarized_row(self):
        return {
            "id": 7,
            "session_id": "sess-1",
            "group_id": "g1",
            "platform": "napcat",
            "participants": ["napcat:u1"],
            "user_id": "u1",
            "content": "对话原文",
            "occurred_at": datetime(2026, 8, 30, 23, 31, 9),
        }

    def _kernel(self, db=None, encoder=None):
        db = db or FakeDB()
        kernel = mod.LocalMemoryKernel(
            db=db,
            embedding_service=mod.EmbeddingService(client=None, dims=1024),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config({"dsn": "x"}),
            bot_id="kira",
            encoder=encoder,
            bot_nickname="Kira",
        )
        return kernel, db

    def _agent(self, db, encoder):
        return mod.FactMergeAgent(
            db=db, llm=object(), config=mod._build_config({"dsn": "x"}),
            encoder=encoder, embedding_service=mod.EmbeddingService(client=None, dims=1024),
            bot_nickname="Kira", bot_id="kira",
        )

    def test_29_encode_success_ok_true(self):
        seen = {}

        class Llm:
            async def run_structured(self, **kwargs):
                seen.update(kwargs)
                return json.dumps({"summary": "摘要", "facts": [
                    {"user_id": "u1", "statement": "喜欢猫", "category": "stable", "confidence": "high"}]},
                    ensure_ascii=False)

        async def run():
            enc = mod.MemoryEncoder(llm=Llm(), prompt_dir=PLUGIN_DIR / "prompts")
            return await enc.encode("对话", "Kira", "kira")

        summary, facts, rels, ok = asyncio.run(run())
        self.assertTrue(ok)
        self.assertEqual(summary, "摘要")
        self.assertEqual(len(facts), 1)
        # The encoder passes the result schema and the submit tool name
        # to the structured exit
        self.assertIsNotNone(seen.get("schema"))
        self.assertEqual(seen.get("tool_name"), "submit_memory_encoding")

    def test_30_retain_degraded_marks_false(self):
        class DegradedEncoder:
            async def encode(self, text, nickname, bot_user_id=""):
                return text, [], [], False

        kernel, db = self._kernel(encoder=DegradedEncoder())

        async def run():
            await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        asyncio.run(run())
        row = db.inserted_summaries[0]
        self.assertFalse(row["summarized"])
        self.assertEqual(row["content"], "对话文本")

    def test_31_retain_encoded_marks_true(self):
        class OkEncoder:
            async def encode(self, text, nickname, bot_user_id=""):
                return "保真摘要", [], [], True

        kernel, db = self._kernel(encoder=OkEncoder())

        async def run():
            await kernel.retain_encoded("对话文本", "sess", "u1", "napcat")

        asyncio.run(run())
        self.assertTrue(db.inserted_summaries[0]["summarized"])

    def test_32_ingest_default_true(self):
        kernel, db = self._kernel()

        async def run():
            await kernel.ingest("通知", session_id="sess", kind="bot_self")

        asyncio.run(run())
        self.assertTrue(db.inserted_summaries[0]["summarized"])

    def test_33_reencode_without_encoder_or_budget(self):
        db = FakeDB()
        db.unsummarized_rows = [self._unsummarized_row()]
        agent = self._agent(db, encoder=None)
        self.assertEqual(asyncio.run(agent._reencode_pass(10)), 0)
        agent = self._agent(db, encoder=object())
        self.assertEqual(asyncio.run(agent._reencode_pass(0)), 0)

    def test_34_reencode_fail_streak_breaks(self):
        calls = []

        class DegradedEncoder:
            async def encode(self, text, nickname, bot_user_id=""):
                calls.append(text)
                return text, [], [], False

        db = FakeDB()
        db.unsummarized_rows = [self._unsummarized_row() for _ in range(5)]
        agent = self._agent(db, DegradedEncoder())
        done = asyncio.run(agent._reencode_pass(50))
        self.assertEqual(done, 0)
        self.assertEqual(len(calls), 3)  # break after 3 straight degradations
        self.assertEqual(db.encoded_updates, [])

    def test_35_reencode_success_updates_and_ingests_facts(self):
        class OkEncoder:
            async def encode(self, text, nickname, bot_user_id=""):
                return "重编码摘要", [
                    CONTRACTS.EncodedFact(user_id="u1", statement="喜欢猫", category="stable", confidence="high"),
                    CONTRACTS.EncodedFact(user_id="kira", statement="bot 事实", category="recent", confidence="medium"),
                ], [], True

        db = FakeDB()
        db.unsummarized_rows = [self._unsummarized_row()]
        agent = self._agent(db, OkEncoder())
        done = asyncio.run(agent._reencode_pass(50))

        self.assertEqual(done, 1)
        row_id, content, emb = db.encoded_updates[0]
        self.assertEqual((row_id, content), (7, "重编码摘要"))
        # The bot's own facts are hard-filtered, so only 1 row lands;
        # evidence_key reuses the original row's occurred_at
        self.assertEqual(len(db.inserted_facts), 1)
        fact = db.inserted_facts[0]
        self.assertEqual(fact["user_id"], "u1")
        self.assertEqual(fact["evidence_key"], "sess-1|2026-08-30")
        self.assertEqual(fact["occurred_at"], self._unsummarized_row()["occurred_at"])

    def test_36_search_sql_fallback_filters_unsummarized(self):
        # The wide-recall path (empty conds, e.g. scope=session without
        # a session_id) needs the same fallback WHERE exclusion of
        # unencoded rows
        db = mod.MemoryDatabase(mod._build_config({"dsn": "x"}))
        db._pool = FakePool()

        async def run():
            await db.search_chat_summaries(query_vec=[0.1] * 8, limit=5, scope="session", session_id="")

        asyncio.run(run())
        sqls = [s for _, s, _ in db.pool.conn.sqls]
        self.assertTrue(
            any("summarized OR kind = 'bot_self'" in s for s in sqls),
            f"兜底 where 缺少 summarized 过滤: {sqls}",
        )


class TestRecallTimeDecay(unittest.TestCase):
    """Recall time decay (recall_time_decay_enabled / half_life switches)."""

    _NOW = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)

    def _row(self, doc_id, relevance, occurred_at, seq=[iter(range(1, 10000))]):
        return {
            "id": next(seq[0]),
            "document_id": doc_id,
            "kind": "chat_summary",
            "session_id": "s1",
            "user_id": "u1",
            "content": f"内容-{doc_id}",
            "occurred_at": occurred_at,
            "relevance": relevance,
        }

    def _kernel(self, rows, **cfg):
        class SearchDB:
            async def search_chat_summaries(self, **kwargs):
                return list(rows)

        class StubEmbedding:
            async def embed_one(self, text):
                return [0.1]

            async def embed_batch(self, texts):
                return [[0.1] for _ in texts]

        raw = {"dsn": "x"}
        raw.update(cfg)
        kernel = mod.LocalMemoryKernel(
            db=SearchDB(),
            embedding_service=StubEmbedding(),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config(raw),
            bot_id="kira",
        )
        kernel._now = staticmethod(lambda: self._NOW)
        return kernel

    def test_37_decay_disabled_by_default(self):
        rows = [
            self._row("old-high", 0.75, self._NOW - timedelta(days=120)),
            self._row("new-low", 0.60, self._NOW - timedelta(days=1)),
        ]
        items = asyncio.run(self._kernel(rows).search(query="猫", session_id="s1"))
        self.assertEqual([i.id for i in items], ["old-high", "new-low"])
        self.assertAlmostEqual(items[0].score, 0.75)

    def test_38_decay_flips_order_by_age(self):
        rows = [
            self._row("old-high", 0.75, self._NOW - timedelta(days=120)),
            self._row("new-low", 0.60, self._NOW - timedelta(days=1)),
        ]
        kernel = self._kernel(rows, recall_time_decay_enabled=True,
                              recall_time_decay_half_life_days=90.0)
        items = asyncio.run(kernel.search(query="猫", session_id="s1"))
        self.assertEqual([i.id for i in items], ["new-low", "old-high"])
        self.assertAlmostEqual(items[1].score, 0.75 * (2.0 ** (-120.0 / 90.0)))

    def test_39_half_life_maths(self):
        rows = [self._row("half", 0.80, self._NOW - timedelta(days=1))]
        kernel = self._kernel(rows, recall_time_decay_enabled=True,
                              recall_time_decay_half_life_days=1.0)
        items = asyncio.run(kernel.search(query="猫", session_id="s1"))
        self.assertAlmostEqual(items[0].score, 0.40)

    def test_40_missing_occurred_at_not_penalized(self):
        rows = [
            self._row("old", 0.75, self._NOW - timedelta(days=400)),
            self._row("unknown", 0.70, None),
        ]
        kernel = self._kernel(rows, recall_time_decay_enabled=True)
        items = asyncio.run(kernel.search(query="猫", session_id="s1"))
        self.assertEqual(items[0].id, "unknown")
        self.assertAlmostEqual(items[0].score, 0.70)

    def test_41_decay_reorders_but_never_filters(self):
        rows = [
            self._row("a", 0.75, self._NOW - timedelta(days=400)),
            self._row("b", 0.50, self._NOW - timedelta(days=2)),
        ]
        kernel = self._kernel(rows, recall_time_decay_enabled=True,
                              recall_relevance_threshold=0.3)
        items = asyncio.run(kernel.search(query="猫", session_id="s1"))
        self.assertEqual({i.id for i in items}, {"a", "b"})
        self.assertEqual(items[0].id, "b")


class TestRecallEmbedGuard(unittest.TestCase):
    """recall embed_task guard: when the search aborts mid-flight the
    vectorization task must be cancelled (an orphaned task's unretrieved
    exception triggers "never retrieved" warning noise)."""

    def test_derive_failure_cancels_inflight_embed(self):
        class SlowEmbedding:
            def __init__(self):
                self.cancelled = False

            async def embed_one(self, text):
                try:
                    await asyncio.sleep(30)  # simulate an in-flight HTTP call
                    return [0.1]
                except asyncio.CancelledError:
                    self.cancelled = True
                    raise

        emb = SlowEmbedding()
        kernel = mod.LocalMemoryKernel(
            db=FakeDB(),
            embedding_service=emb,
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config({"dsn": "x"}),
            bot_id="kira",
        )

        async def boom(*args, **kwargs):
            await asyncio.sleep(0)  # yield one tick so embed_task starts
            raise RuntimeError("扩选查询失败（测试注入）")

        kernel._derive_expanded_users = boom

        async def run():
            with self.assertRaises(RuntimeError):
                await kernel.search(query="猫", session_id="s1")
            # inside the loop: assert the in-flight task got cancelled
            # (no orphaned task left behind)
            pending = [
                t for t in asyncio.all_tasks()
                if t is not asyncio.current_task()
            ]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self.assertTrue(emb.cancelled, "向量化任务未被取消")

        asyncio.run(run())


class TestEntityRecall(unittest.TestCase):
    """Asking-about-others recall (entity key groups): SQL landing points and kernel key-group passing."""

    def _db_sql(self, **kw):
        db = mod.MemoryDatabase(mod._build_config({"dsn": "x"}))
        db._pool = FakePool()
        base = dict(
            query_vec=[0.1] * 8, limit=5, scope="session",
            session_id="s1", platform="seki",
            user_keys=["seki:me"], user_ids=["me"],
        )
        base.update(kw)
        asyncio.run(db.search_chat_summaries(**base))
        return " ".join(s for _, s, _ in db.pool.conn.sqls)

    def test_entity_keys_join_primary_block_when_cross_session(self):
        # Cross-session open: entity keys merge into the primary key
        # group (same pool as the speaker, OR-ed), with no
        # session-pinned block
        sql = self._db_sql(
            cross_session=True,
            entity_user_keys=["seki:9900000003"], entity_user_ids=["9900000003"],
        )
        self.assertEqual(sql.count("participants &&"), 2)
        self.assertEqual(sql.count("user_id = ANY("), 2)
        # No "session-pinned" shaped block appears (the shape unique to
        # the isolated landing point)
        self.assertNotIn("AND summarized AND session_id =", sql)

    def test_entity_keys_session_pinned_when_isolated(self):
        # Session isolation: entity keys pin to the current session
        # isomorphic with the expansion group; the primary key group
        # keeps only the speaker
        sql = self._db_sql(
            cross_session=False,
            entity_user_keys=["seki:9900000003"], entity_user_ids=["9900000003"],
        )
        self.assertEqual(sql.count("participants &&"), 2)
        self.assertIn("AND summarized AND session_id =", sql)

    def test_entity_keys_alone_still_form_blocks(self):
        # With no speaker filter (e.g. planner passes no user), entity
        # keys form their own group
        sql = self._db_sql(
            cross_session=True, user_keys=None, user_ids=None,
            entity_user_keys=["seki:9900000003"], entity_user_ids=["9900000003"],
        )
        self.assertEqual(sql.count("participants &&"), 1)
        self.assertEqual(sql.count("user_id = ANY("), 1)

    def test_no_entity_keys_unchanged_shape(self):
        # No entity keys: the SQL matches the existing shape (no third
        # block)
        sql = self._db_sql(cross_session=True)
        self.assertEqual(sql.count("participants &&"), 1)
        sql2 = self._db_sql(cross_session=False)
        self.assertNotIn("AND summarized AND session_id =", sql2)

    def _cap_kernel(self, captured, rows=None):
        class CapDB:
            async def search_chat_summaries(self, **kwargs):
                captured.update(kwargs)
                return list(rows or [])

        class StubEmbedding:
            async def embed_one(self, text):
                return [0.1]

            async def embed_batch(self, texts):
                return [[0.1] for _ in texts]

        kernel = mod.LocalMemoryKernel(
            db=CapDB(),
            embedding_service=StubEmbedding(),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config({"dsn": "x"}),
            bot_id="kira",
        )
        return kernel

    def test_build_injection_consumes_hints_entity_keys(self):
        # Injection path: hints.entity_user_keys pass through verbatim
        # (composite keys + derived bare uids)
        captured = {}
        kernel = self._cap_kernel(captured)
        RecallHints = sys.modules[mod.LocalMemoryKernel.__module__].RecallHints
        hints = RecallHints(entity_user_keys=["seki:9900000003"])
        asyncio.run(kernel.build_injection_text(
            query="小李说过什么", session_id="s1", hints=hints,
        ))
        self.assertEqual(captured.get("entity_user_keys"), ["seki:9900000003"])
        self.assertEqual(captured.get("entity_user_ids"), ["9900000003"])

    def test_build_injection_fallback_matches_query(self):
        # planner fallback: when hints carry no keys, self-match the
        # query (window-dictionary path)
        captured = {}
        kernel = self._cap_kernel(captured)

        class Dir:
            async def match(self, sid, text):
                return [("小李", "seki", "9900000003")]

        kernel._entity_directory = Dir()
        asyncio.run(kernel.build_injection_text(query="小李说过什么", session_id="s1"))
        self.assertEqual(captured.get("entity_user_keys"), ["seki:9900000003"])
        self.assertEqual(captured.get("entity_user_ids"), ["9900000003"])

    def test_build_injection_no_fallback_when_disabled(self):
        # recall_hint_enabled=False: no fallback matching
        captured = {}
        kernel = self._cap_kernel(captured)
        kernel.config = mod._build_config({"dsn": "x", "recall_hint_enabled": False})

        class Dir:
            async def match(self, sid, text):
                raise AssertionError("recall_hint_enabled=False 不应触发匹配")

        kernel._entity_directory = Dir()
        asyncio.run(kernel.build_injection_text(query="小李说过什么", session_id="s1"))
        self.assertIsNone(captured.get("entity_user_keys"))


class TestWriteDedup(unittest.TestCase):
    """Write-side near-duplicate dedup (the three write_dedup_* keys + ingest apply_write_dedup gating)."""

    class DedupDB:
        def __init__(self, scores=None, fail_fetch=False):
            self.scores = scores or []
            self.fail_fetch = fail_fetch
            self.fetch_kwargs = None
            self.inserted = []

        async def fetch_recent_summary_scores(self, **kwargs):
            if self.fail_fetch:
                raise RuntimeError("去重查询失败（测试注入）")
            self.fetch_kwargs = kwargs
            return self.scores

        async def insert_chat_summary(self, **kwargs):
            self.inserted.append(kwargs)
            return kwargs["document_id"]

    class StubEmbedding:
        async def embed_one(self, text):
            return [0.1]

        async def embed_batch(self, texts):
            return [[0.1] for _ in texts]

    class StubEncoder:
        async def encode(self, text, nickname, bot_user_id=""):
            return "编码摘要", [], [], True

    def _kernel(self, scores=None, fail_fetch=False, **cfg):
        raw = {"dsn": "x"}
        raw.update(cfg)
        db = self.DedupDB(scores=scores, fail_fetch=fail_fetch)
        kernel = mod.LocalMemoryKernel(
            db=db,
            embedding_service=self.StubEmbedding(),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config(raw),
            bot_id="kira",
            encoder=self.StubEncoder(),
        )
        return kernel, db

    def _retain(self, kernel):
        return asyncio.run(kernel.retain_encoded(
            conversation_text='<msg ts="2026-09-12 10:00:00" uid="u1" name="小白">你好</msg>',
            session_id="s1", user_id="u1", platform="qq",
        ))

    def test_60_near_dup_summary_skipped(self):
        kernel, db = self._kernel(scores=[{"id": 1, "content": "旧", "score": 0.92}])
        doc_ids = self._retain(kernel)
        self.assertEqual(db.inserted, [])  # summary write skipped
        self.assertEqual(len(doc_ids), 1)  # still returns an idempotent doc id
        self.assertEqual(db.fetch_kwargs["limit"], 8)
        self.assertEqual(db.fetch_kwargs["session_id"], "s1")

    def test_61_below_threshold_inserted(self):
        kernel, db = self._kernel(scores=[{"id": 1, "content": "旧", "score": 0.60}])
        self._retain(kernel)
        self.assertEqual(len(db.inserted), 1)
        self.assertEqual(db.inserted[0]["content"], "编码摘要")

    def test_62_disabled_inserted(self):
        kernel, db = self._kernel(
            scores=[{"id": 1, "content": "旧", "score": 0.95}],
            write_dedup_enabled=False,
        )
        self._retain(kernel)
        self.assertEqual(len(db.inserted), 1)

    def test_63_threshold_zero_disables(self):
        kernel, db = self._kernel(
            scores=[{"id": 1, "content": "旧", "score": 0.95}],
            write_dedup_threshold=0.0,
        )
        self._retain(kernel)
        self.assertEqual(len(db.inserted), 1)

    def test_64_fetch_failure_fail_open(self):
        kernel, db = self._kernel(fail_fetch=True)
        self._retain(kernel)  # must not raise
        self.assertEqual(len(db.inserted), 1)

    def test_65_window_config_passed(self):
        kernel, db = self._kernel(
            scores=[{"id": 1, "content": "旧", "score": 0.10}],
            write_dedup_window=3,
        )
        self._retain(kernel)
        self.assertEqual(db.fetch_kwargs["limit"], 3)

    def test_66_tool_write_not_deduped(self):
        # apply_write_dedup defaults to False: explicit tool writes do
        # not participate in dedup
        kernel, db = self._kernel(scores=[{"id": 1, "content": "旧", "score": 0.95}])
        asyncio.run(kernel.ingest(
            content="记住我爱猫", session_id="s1", user_id="u1", platform="qq",
            kind="chat_summary",
        ))
        self.assertEqual(len(db.inserted), 1)

    def test_67_degraded_raw_not_deduped(self):
        # Degraded raw text (summarized=False) does not participate in
        # dedup; it is left for the re-encode pass
        class BoomEncoder:
            async def encode(self, text, nickname, bot_user_id=""):
                raise RuntimeError("编码失败")

        db = self.DedupDB(scores=[{"id": 1, "content": "旧", "score": 0.95}])
        kernel = mod.LocalMemoryKernel(
            db=db,
            embedding_service=self.StubEmbedding(),
            circuit_breaker=mod.MemoryDBCircuitBreaker(),
            config=mod._build_config({"dsn": "x"}),
            bot_id="kira",
            encoder=BoomEncoder(),
        )
        asyncio.run(kernel.retain_encoded(
            conversation_text='<msg ts="2026-09-12 10:00:00" uid="u1" name="小白">你好</msg>',
            session_id="s1", user_id="u1", platform="qq",
        ))
        self.assertEqual(len(db.inserted), 1)
        self.assertFalse(db.inserted[0]["summarized"])

    def test_68_recall_dedup_default_lowered(self):
        # Recall-side near-duplicate dedup default threshold 0.9 -> 0.85
        self.assertEqual(
            mod._build_config({}).dedup_similarity_threshold, 0.85
        )


# ---------------------------------------------------------------------------
#  Merged from test_review_fixes.py (2026-09-11 review-fix regression batch)
# ---------------------------------------------------------------------------


def _msg(uid, text, mid, session):
    return types.SimpleNamespace(
        message=make_msg(uid, text, mid=mid), session=session
    )


class _CaptureConn:
    """Capture connection dispatching by SQL target (cluster txn / edges / kv)."""

    def __init__(self, cluster_row=None, edges=None):
        self.cluster_row = dict(cluster_row or {})
        self.edges = list(edges or [])
        self.executed: list[str] = []
        self.superseded_args = None

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def transaction(self):
        return _Tx(self)

    async def fetchrow(self, sql, *args):
        if "FOR UPDATE" in sql:
            return self.cluster_row
        raise AssertionError("unexpected fetchrow: " + sql[:80])

    async def fetch(self, sql, *args):
        if "evidence_keys" in sql and "memory_entity_edge" in sql:
            return self.edges
        raise AssertionError("unexpected fetch: " + sql[:80])

    async def execute(self, sql, *args):
        self.executed.append(sql)
        if "memory_entity_edge" in sql and "superseded" in sql:
            self.superseded_args = args


class _Tx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _KvPool(_CaptureConn):
    """update_cluster backend stub (postgres dialect) + kv (backfill-todo
    consumption).

    After the webui_store dual-backend split, update_cluster dispatches via
    the backend; this stub fills in the MemoryBackend tombstone-propagation
    method surface (delegates to the PG implementation re-exported by the db
    package; SQL capture behavior unchanged from before the split).
    """

    dialect = "postgres"

    def __init__(self, cluster_row=None, edges=None):
        super().__init__(cluster_row, edges)
        self.kv: dict = {}

    async def get_kv(self, key):
        return self.kv.get(key)

    async def set_kv(self, key, value):
        self.kv[key] = value

    async def supersede_backfill_edges_of(self, conn, cluster_ids):
        return await _DB.supersede_backfill_edges_of(conn, cluster_ids)


class _SqlPool:
    """String-level SQL capture pool (db-layer shape assertions)."""

    def __init__(self, fetch_rows=None, fetchval=None):
        self.queries: list = []
        self._fetch_rows = fetch_rows if fetch_rows is not None else []
        self._fetchval = fetchval

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def fetch(self, sql, *args):
        self.queries.append((sql, args))
        return self._fetch_rows

    async def fetchval(self, sql, *args):
        self.queries.append((sql, args))
        return self._fetchval


def _db_with_pool(pool):
    db = _DB.MemoryDatabase.__new__(_DB.MemoryDatabase)
    db._pool = pool
    return db


class _AgentDB:
    def __init__(self):
        self.edge_kwargs: dict = {}
        self.inserted_facts: list[dict] = []
        self.kv: dict = {}
        self.merges: list[dict] = []

    async def get_kv(self, key):
        return self.kv.get(key)

    async def set_kv(self, key, value):
        self.kv[key] = value

    async def upsert_entity_edge(self, rows, **kwargs):
        self.edge_kwargs = {"rows": rows, "kwargs": kwargs}

    async def insert_persona_fact_raw(self, **kwargs):
        self.inserted_facts.append(kwargs)

    async def fetch_unsummarized_summaries(self, limit):
        return []

    async def fetch_pending_facts(self, limit):
        return [{
            "id": 1, "document_id": "d", "platform": "qq", "user_id": "u1",
            "related_user_ids": [], "display_name": "", "category": "stable",
            "statement": "我爱猫", "confidence": "high", "session_id": "s1",
            "group_id": "", "evidence_key": "s1|2026-09-11",
            "occurred_at": datetime(2026, 9, 11, tzinfo=timezone.utc),
            "embedding": None,
        }]

    async def search_cluster_candidates(self, **kwargs):
        return [{
            "id": 9, "canonical_statement": "我爱狗", "status": "active",
            "score": 3.0, "evidence_count": 1, "evidence_keys": [],
            "last_evidence_at": None, "occurred_at": datetime(2026, 9, 1),
            "replaced_by": None,
        }]

    async def apply_fact_merge(self, **kwargs):
        self.merges.append(kwargs)
        return {"action": "merge", "cluster_id": kwargs.get("cluster_id"),
                "score": 3.0, "status": "active"}

    async def promote_pass(self, **kwargs):
        return 0

    async def relation_integrity_report(self, **kwargs):
        return {}


class TestWatermarkAtomicity(unittest.TestCase):
    def test_concurrent_signal_during_alias_upsert_no_double_take(self):
        """While signal A is suspended on the alias-upsert DB I/O, signal B
        must not take the same batch.

        Before the fix: consumed.update happened after the await, so B took
        the same unconsumed delta -> double encoding + duplicate summary
        rows. After: the watermark advances synchronously before the first
        await.
        """
        inst, _ = _ready_plugin(mod)
        session = FakeSession(stype="dm", sid="9900000002")

        async def scenario():
            await inst.observe_message(_msg("u1", "第一条消息", "m1", session))
            gate = asyncio.Event()

            async def gated_alias(user_delta):
                await gate.wait()  # emulate slow db_command_timeout I/O

            inst._alias_upsert_batch = gated_alias
            retains: list[set] = []

            async def fake_retain(state, sid="", delta_keys=None, durable=True):
                retains.append(set(delta_keys or ()))

            inst._do_retain = fake_retain
            evt = types.SimpleNamespace(payload={"session": session.sid})
            ta = asyncio.create_task(inst._on_session_memory_updated(evt))
            await asyncio.sleep(0.02)
            # A already advanced the watermark synchronously (suspended
            # on the alias upsert at this moment)
            self.assertIn("m1", inst._consumed.get(session.sid, set()))
            tb = asyncio.create_task(inst._on_session_memory_updated(evt))
            await asyncio.sleep(0.02)
            gate.set()
            await asyncio.gather(ta, tb)
            await asyncio.gather(*inst._retain_tasks)
            return retains

        retains = asyncio.run(scenario())
        self.assertEqual(retains, [{"m1"}], "同批增量只可被取走一次")


class TestBuildConfigReflection(unittest.TestCase):
    def test_every_field_wired(self):
        """Every LocalMemoryConfig field must be wired from raw by
        _build_config.

        Probe values (different from the defaults, within field bounds) are
        injected field by field — a missed field (like recall_hint_enabled
        before the fix) silently falls back to its default and gets caught
        here.
        """
        raw: dict = {}
        expected: dict = {}
        for name, f in mod.LocalMemoryConfig.model_fields.items():
            default = (
                f.default_factory() if f.default_factory is not None else f.default
            )
            ann = f.annotation
            # Optional[float] is unwrapped and probed as float; for a None
            # default (derived field) use the value derived at the default
            # scale as the probe baseline (so the probe differs from it)
            if ann == Optional[float]:
                ann = float
                if default is None:
                    default = 3.0
            lo = hi = None
            ge = gt = le = None
            for m in f.metadata:
                ge = getattr(m, "ge", None) if ge is None else ge
                gt = getattr(m, "gt", None) if gt is None else gt
                le = getattr(m, "le", None) if le is None else le
            if ge is not None or gt is not None:
                lo = ge if ge is not None else gt
            if le is not None:
                hi = le
            if ann is bool:
                value = not default
            elif ann in (int, float):
                base = default if isinstance(default, (int, float)) else 0
                value = base + (1.5 if ann is float else 1)
                if hi is not None:
                    value = min(value, hi)
                if lo is not None:
                    floor = lo + (1 if (ann is int and gt is not None) else 0)
                    value = max(value, floor)
                if ann is float:
                    value = float(value)
            elif ann is str or ann == "str":
                value = f"probe-{name}"
            elif get_origin(ann) is Literal:
                # Literal whitelist field: take the first non-default
                # legal value to verify wiring pass-through -- if the
                # probe value equals the default (recall_time_label_mode
                # defaults to "both", exactly the whitelist's last item),
                # a missed _build_config wire falls back to the same
                # default, making raw/expected/actual all equal and the
                # assertion vacuous; strings outside the whitelist get
                # normalized back by _cfg_time_label_mode at assembly,
                # so the probe must come from inside the whitelist
                non_default = [a for a in get_args(ann) if a != default]
                value = (non_default or list(get_args(ann)))[0]
            else:  # list[str]
                value = ["probe"]
            raw[name] = value
            expected[name] = value
        built = mod._build_config(raw)
        for name, value in expected.items():
            self.assertEqual(
                getattr(built, name), value,
                f"字段 {name} 未被 _build_config 接线（回落默认值）",
            )


class TestUpdateClusterPaths(unittest.TestCase):
    def _row(self, **kw):
        base = {
            "id": 12, "platform": "qq", "user_id": "u1", "category": "stable",
            "canonical_statement": "旧陈述", "score": 5.0,
            "status": "replaced", "replaced_by": 99, "demoted_at": None,
            "related_user_ids": [],
        }
        base.update(kw)
        return base

    def test_replaced_same_status_submission_passes(self):
        """P1-3: a replaced cluster "keep current status" submission (statement/score only) must pass."""
        conn = _KvPool(cluster_row=self._row())
        out = asyncio.run(mod.webui_store.update_cluster(
            conn, 12,
            {"canonical_statement": "更正后的陈述", "status": "replaced", "score": 2.5},
        ))
        self.assertTrue(out["updated"])
        self.assertEqual(out["status"], "replaced")
        self.assertEqual(out["edges_superseded"], 0)

    def test_replaced_transition_out_rejected(self):
        conn = _KvPool(cluster_row=self._row())
        with self.assertRaises(Exception) as cm:
            asyncio.run(mod.webui_store.update_cluster(
                conn, 12, {"status": "active"},
            ))
        self.assertIn("不可手工迁出", str(cm.exception))

    def test_normal_cluster_same_status_is_noop_status(self):
        conn = _KvPool(cluster_row=self._row(status="active", replaced_by=None))
        out = asyncio.run(mod.webui_store.update_cluster(
            conn, 12, {"status": "active", "score": 6.0},
        ))
        self.assertEqual(out["status"], "active")

    def test_manual_kill_retires_backfill_edges(self):
        """P2-10: manually setting dead retires pure backfill-source edges."""
        conn = _KvPool(
            cluster_row=self._row(status="active", replaced_by=None),
            edges=[{"id": 7, "evidence_keys": ["backfill|12"]}],
        )
        out = asyncio.run(mod.webui_store.update_cluster(conn, 12, {"status": "dead"}))
        self.assertEqual(out["edges_superseded"], 1)
        self.assertEqual(conn.superseded_args, ([7],))

    def test_manual_revive_registers_backfill_pending(self):
        """P2-9: manually reviving a pending/dead cluster registers a backfill todo."""
        conn = _KvPool(cluster_row=self._row(status="dead", replaced_by=None))
        asyncio.run(mod.webui_store.update_cluster(conn, 12, {"status": "active"}))
        self.assertEqual(
            json.loads(conn.kv.get("relation_backfill_pending_ids", "[]")),
            [12],
        )


class TestReingestChannelAlignment(unittest.TestCase):
    def _agent(self, db):
        return mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(), config=mod.LocalMemoryConfig(),
            encoder=None, embedding_service=None,
            bot_nickname="Kira", bot_id="kira",
            bot_forms_provider=lambda: ["kira", "9900000004"],
            alias_name_resolver=lambda platform, uid: (
                "Kira" if uid == "9900000004" else ""
            ),
        )

    def test_reingest_relations_write_side_invariants(self):
        """P2-1: stopwords pass-through + bot-form-set matching + placeholder-name guard override."""
        db = _AgentDB()
        agent = self._agent(db)
        row = {
            "id": 5, "session_id": "s1", "platform": "qq",
            "occurred_at": datetime(2026, 9, 11, tzinfo=timezone.utc),
        }
        rel = _EDGE.EncodedRelation(
            subject_user_id="9900000004", object_user_id="u2",
            label="姐姐", statement="Kira说小张是她的主人",
            subject_display_name="未知", object_display_name="",
        )
        asyncio.run(agent._reingest_relations([rel], row))
        captured = db.edge_kwargs
        self.assertEqual(
            captured["kwargs"].get("label_stopwords"),
            mod.LocalMemoryConfig().relation_label_stopwords,
            "补编码关系通道必须传 label_stopwords",
        )
        out = captured["rows"][0]
        self.assertEqual(out["subject_name"], "Kira", "占位名用别名顶替")
        self.assertEqual(out["object_name"], "", "未命中留空由 upsert 保旧名")
        self.assertTrue(out["is_bot_edge"], "bot 形态集合（provider 桥接）命中")

    def test_reingest_facts_strip_bot_from_related(self):
        """P2-2: related_user_ids strips bot as well (aligned with the retain path)."""
        db = _AgentDB()
        agent = self._agent(db)
        agent.bot_id = "kira"
        row = {
            "id": 5, "session_id": "s1", "platform": "qq", "group_id": "",
            "occurred_at": datetime(2026, 9, 11, tzinfo=timezone.utc),
        }
        facts = [
            CONTRACTS.EncodedFact(user_id="u1", statement="爱猫", display_name="",
                                  category="stable", confidence="high",
                                  related_user_ids=["u2", "kira"]),
            CONTRACTS.EncodedFact(user_id="kira", statement="bot 自身", display_name="",
                                  category="stable", confidence="high",
                                  related_user_ids=[]),
        ]
        asyncio.run(agent._reingest_facts(facts, row))
        self.assertEqual(len(db.inserted_facts), 1, "bot 主体事实整条丢弃")
        self.assertEqual(
            db.inserted_facts[0]["related_user_ids"], ["u2"],
            "related 集合同步剔除 bot",
        )


class TestRelationAuditPoisonBatch(unittest.TestCase):
    def _agent(self, db):
        agent = mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(relation_audit_enabled=True),
            encoder=None,
        )
        return agent

    def test_poison_batch_skipped_after_consecutive_failures(self):
        db = _AgentDB()
        db.fetch_edges = None
        ts = datetime(2026, 9, 11, tzinfo=timezone.utc)
        edges = [
            {"id": 10, "updated_at": ts}, {"id": 20, "updated_at": ts},
        ]

        async def fetch_edges(after_id, limit):
            return edges

        db.fetch_edges_for_audit = fetch_edges
        agent = self._agent(db)

        class UnsureAuditor:
            async def audit(self, edges):
                return {"edges": [], "unsure": True}

        agent._relation_auditor = UnsureAuditor()
        for _ in range(2):
            asyncio.run(agent._relation_audit_pass())
        self.assertIsNone(db.kv.get("relation_audit_edge_id"))
        # Third same-watermark batch failure -> skip the batch and
        # advance the watermark (edges after it are no longer pinned)
        asyncio.run(agent._relation_audit_pass())
        self.assertEqual(db.kv.get("relation_audit_edge_id"), "20")

    def test_normal_supersede_uses_optimistic_lock_and_reasons(self):
        db = _AgentDB()
        ts = datetime(2026, 9, 11, tzinfo=timezone.utc)
        edges = [
            {"id": 10, "updated_at": ts, "subject_uid": "a", "object_uid": "b",
             "subject_name": "", "object_name": "", "relation_label": "姐姐",
             "statement": "s", "status": "active", "confidence": "high",
             "evidence_count": 2, "last_seen": ts},
        ]
        served = {"n": 0}

        async def fetch_edges(after_id, limit):
            served["n"] += 1
            return edges if served["n"] == 1 else []

        db.fetch_edges_for_audit = fetch_edges
        captured: dict = {}

        async def supersede(edge_ids, **kwargs):
            captured["ids"] = edge_ids
            captured["expected"] = kwargs.get("expected_updated_at")
            captured["reasons"] = kwargs.get("reasons")
            return 1

        db.supersede_edges = supersede
        agent = self._agent(db)

        class BadAuditor:
            async def audit(self, edges):
                return {"edges": [(10, "bad", "互动描述")], "unsure": False}

        agent._relation_auditor = BadAuditor()
        audited, superseded = asyncio.run(agent._relation_audit_pass())
        self.assertEqual((audited, superseded), (1, 1))
        self.assertEqual(captured["ids"], [10])
        self.assertEqual(captured["expected"], {10: ts})
        self.assertEqual(captured["reasons"], {10: "互动描述"})
        self.assertEqual(db.kv.get("relation_audit_edge_id"), "10")

    def test_partial_coverage_never_advances_watermark_then_poison_skips(self):
        # M2: 2 edges submitted but only 1 verdict returned -- the
        # watermark must not advance past uncovered edges (under a
        # monotonic watermark, advancing = permanent audit misses); with
        # 3 consecutive partial-coverage batches at the same watermark,
        # poison-pill batch skipping kicks in; on skip, audited counts
        # only covered edges and no demotion is applied to the
        # partially-covered batch
        db = _AgentDB()
        ts = datetime(2026, 9, 11, tzinfo=timezone.utc)
        edges = [
            {"id": 10, "updated_at": ts}, {"id": 20, "updated_at": ts},
        ]

        async def fetch_edges(after_id, limit):
            return edges

        db.fetch_edges_for_audit = fetch_edges
        supersede_calls: list = []

        async def supersede(edge_ids, **kwargs):
            supersede_calls.append(edge_ids)
            return len(edge_ids)

        db.supersede_edges = supersede
        agent = self._agent(db)

        class PartialAuditor:
            async def audit(self, edges):
                return {"edges": [(10, "bad", "互动描述")], "unsure": False}

        agent._relation_auditor = PartialAuditor()
        for _ in range(2):
            asyncio.run(agent._relation_audit_pass())
        self.assertIsNone(
            db.kv.get("relation_audit_edge_id"),
            "覆盖不全不得推进水位（未覆盖边将永久漏审）",
        )
        self.assertEqual(supersede_calls, [], "覆盖不全的批不得降级任何边")
        # Third same-watermark partial coverage -> poison-pill batch
        # skip advances the watermark; audited counts only covered edges
        audited, superseded = asyncio.run(agent._relation_audit_pass())
        self.assertEqual(db.kv.get("relation_audit_edge_id"), "20")
        self.assertEqual(audited, 1)
        self.assertEqual(superseded, 0)


class TestEmptyVerdicts(unittest.TestCase):
    def test_all_invalid_verdicts_defer_without_create(self):
        db = _AgentDB()

        class EmptyAdjudicator:
            async def adjudicate(self, fact, candidates):
                return {"verdicts": [], "unsure": False}

        agent = mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(llm_budget_per_cycle=5),
            encoder=None,
        )
        agent._adjudicator = EmptyAdjudicator()
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["skipped_unsure"], 1)
        self.assertEqual(stats["created"], 0)
        self.assertEqual(db.merges, [], "空 verdicts 不得盲目建簇")

    def test_partial_coverage_verdicts_defer_without_create(self):
        # M1: 2 candidates but only 1 covered -- an uncovered candidate
        # is treated as unrelated by _choose_action and would take the
        # create path, duplicating cluster creation; partial coverage
        # must defer the whole fact as unsure (the no-blind-cluster-
        # creation invariant)
        db = _AgentDB()

        async def two_candidates(**kwargs):
            return [
                {"id": 1, "canonical_statement": "我爱猫", "status": "active",
                 "score": 3.0, "evidence_count": 1, "evidence_keys": [],
                 "last_evidence_at": None,
                 "occurred_at": datetime(2026, 9, 1), "replaced_by": None},
                {"id": 9, "canonical_statement": "我爱狗", "status": "active",
                 "score": 3.0, "evidence_count": 1, "evidence_keys": [],
                 "last_evidence_at": None,
                 "occurred_at": datetime(2026, 9, 1), "replaced_by": None},
            ]

        db.search_cluster_candidates = two_candidates

        class PartialAdjudicator:
            async def adjudicate(self, fact, candidates):
                return {"verdicts": [(1, "same")], "unsure": False}

        agent = mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(llm_budget_per_cycle=5),
            encoder=None,
        )
        agent._adjudicator = PartialAdjudicator()
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["skipped_unsure"], 1)
        self.assertEqual(stats["created"], 0)
        self.assertEqual(db.merges, [], "部分覆盖不得处置（含 create 路径）")


class TestCycleStageIsolation(unittest.TestCase):
    """Cycle stage isolation for M4.

    A DB read failure in the normalization pass must stay isolated:
    feed the circuit-breaker channel, keep the remaining passes
    running, and persist the skip list on every exit path (the old
    behavior aborted the whole cycle via _run raising).
    """

    def _agent(self, db):
        agent = mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(llm_budget_per_cycle=5),
            encoder=None,
        )
        fails: list[str] = []

        async def record_failure():
            fails.append("fail")

        agent._record_db_failure = record_failure  # type: ignore[method-assign]
        return agent, fails

    def test_fact_fetch_failure_runs_remaining_stages(self):
        db = _AgentDB()

        async def boom(limit):
            raise RuntimeError("db read boom")

        db.fetch_pending_facts = boom
        promote_calls: list = []

        async def promote(**kw):
            promote_calls.append(kw)
            return 0

        db.promote_pass = promote
        agent, fails = self._agent(db)
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["facts"], 0)
        self.assertTrue(fails, "读失败须喂熔断通道（与处置失败同机制）")
        self.assertEqual(len(promote_calls), 1, "晋档遍不受归一化遍失败影响")
        for key in ("promoted", "reencoded", "edges_audited",
                    "edges_superseded", "summaries_archived", "fact_alias"):
            self.assertIn(key, stats)
        self.assertIn(
            "adjudicate_skip_state", db.kv,
            "任何退出路径都要持久化跳过名单（try/finally）",
        )

    def test_candidate_search_failure_defers_fact_only(self):
        db = _AgentDB()

        async def boom(**kw):
            raise RuntimeError("hnsw boom")

        db.search_cluster_candidates = boom
        agent, fails = self._agent(db)
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["facts"], 0)
        self.assertEqual(db.merges, [], "检索失败的本条顺延，不得盲处置")
        self.assertTrue(fails)
        self.assertEqual(stats["llm_calls"], 0)


class TestIdValidationBoolGuard(unittest.TestCase):
    """Bool/float id validation guard for L1.

    A JSON true deserializes to True, and True == 1 slips through
    `in valid_ids` -- bool and non-int ids must be counted in dropped
    as invalid entries.
    """

    def _llm(self, payload):
        class Llm:
            async def run_structured(self, **kwargs):
                return json.dumps(payload, ensure_ascii=False)

        return Llm()

    def test_adjudicate_bool_cluster_id_dropped(self):
        ma = sys.modules["noriflow_memory_pkg.merge_agent"]
        adj = ma.FactAdjudicator(
            llm=self._llm({
                "verdicts": [
                    {"cluster_id": True, "verdict": "same"},  # True == 1
                    {"cluster_id": 1, "verdict": "same"},
                ],
                "unsure": False,
            }),
            prompt_dir=PLUGIN_DIR / "prompts",
        )
        fact = {"statement": "我爱猫", "category": "stable", "confidence": "high"}
        candidates = [{"id": 1, "canonical_statement": "我爱猫",
                       "status": "active"}]
        result = asyncio.run(adj.adjudicate(fact, candidates))
        self.assertEqual(result["verdicts"], [(1, "same")])
        self.assertEqual(result["dropped"], 1)

    def test_adjudicate_float_cluster_id_normalized(self):
        # PR review: the strict isinstance(int) check wrongly killed
        # float ids -- an occasional {"cluster_id": 1.0} from the model
        # would be dropped, leaving verdicts under-covered and the fact
        # deferred forever; normalize back to candidate 1 by the
        # baseline int() width (bool interception unchanged).
        # Follow-up review: non-integral floats (1.9) must be blanked
        # and dropped -- int() would silently truncate 1.9 into 1 and
        # mis-match the wrong cluster
        ma = sys.modules["noriflow_memory_pkg.merge_agent"]
        adj = ma.FactAdjudicator(
            llm=self._llm({
                "verdicts": [
                    {"cluster_id": 1.0, "verdict": "same"},
                    {"cluster_id": "2", "verdict": "same"},  # same-width norm
                    {"cluster_id": 1.9, "verdict": "same"},  # non-int: dropped
                ],
                "unsure": False,
            }),
            prompt_dir=PLUGIN_DIR / "prompts",
        )
        fact = {"statement": "我爱猫", "category": "stable", "confidence": "high"}
        candidates = [
            {"id": 1, "canonical_statement": "我爱猫", "status": "active"},
            {"id": 2, "canonical_statement": "我养猫", "status": "active"},
        ]
        result = asyncio.run(adj.adjudicate(fact, candidates))
        self.assertEqual(result["verdicts"], [(1, "same"), (2, "same")])
        self.assertEqual(result["dropped"], 1)

    def test_audit_float_edge_id_normalized(self):
        # Same as the PR review above: the relation-edge audit's strict
        # isinstance(int) check must return to int() width
        # normalization -- float/numeric-string ids land on the real
        # edge id (bool interception unchanged)
        ma = sys.modules["noriflow_memory_pkg.merge_agent"]
        auditor = ma.RelationAuditor(
            llm=self._llm({
                "edges": [{"id": 10.0, "verdict": "bad", "reason": "互动描述"}],
                "unsure": False,
            }),
            prompt_dir=PLUGIN_DIR / "prompts",
        )
        edges = [
            {"id": 10, "subject_name": "", "subject_uid": "a",
             "object_name": "", "object_uid": "b", "relation_label": "姐姐",
             "statement": "s", "status": "active", "evidence_count": 2},
        ]
        result = asyncio.run(auditor.audit(edges))
        self.assertEqual(result["edges"], [(10, "bad", "互动描述")])

    def test_audit_bool_edge_id_dropped(self):
        ma = sys.modules["noriflow_memory_pkg.merge_agent"]
        auditor = ma.RelationAuditor(
            llm=self._llm({
                "edges": [
                    {"id": True, "verdict": "ok"},  # True == 1
                    {"id": 10, "verdict": "bad", "reason": "互动描述"},
                ],
                "unsure": False,
            }),
            prompt_dir=PLUGIN_DIR / "prompts",
        )
        edges = [
            {"id": 1, "subject_name": "", "subject_uid": "a",
             "object_name": "", "object_uid": "b", "relation_label": "姐姐",
             "statement": "s", "status": "active", "evidence_count": 2},
            {"id": 10, "subject_name": "", "subject_uid": "a",
             "object_name": "", "object_uid": "b", "relation_label": "姐姐",
             "statement": "s", "status": "active", "evidence_count": 2},
        ]
        result = asyncio.run(auditor.audit(edges))
        self.assertEqual(
            result["edges"], [(10, "bad", "互动描述")]
        )
        self.assertEqual(len(result["edges"]), 1)


class TestTimestampKvWriteIsolation(unittest.TestCase):
    """Timestamp KV write isolation for L2.

    A persist failure of the post-decay/archive timestamp must not
    abort the cycle (the in-memory guardrail is set first, so only a
    restart could re-run it early) nor allow a second execution in the
    same process.
    """

    def test_decay_kv_failure_never_aborts_or_double_decays(self):
        db = _AgentDB()
        decay_calls: list = []

        async def set_kv(key, value):
            if key == "last_decay_at":
                raise RuntimeError("kv write boom")
            db.kv[key] = value

        async def decay_pass(**kw):
            decay_calls.append(kw)
            return {}

        db.set_kv = set_kv
        db.decay_pass = decay_pass
        # Pre-seed an expired timestamp: skip the first-run init branch
        # and go straight into decay
        db.kv["last_decay_at"] = (
            datetime.now(timezone.utc) - timedelta(days=90)
        ).isoformat()
        agent = mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(),
            encoder=None,
        )
        asyncio.run(agent._maybe_decay())
        self.assertEqual(len(decay_calls), 1)
        self.assertIsNotNone(agent._last_decay_at, "内存护栏须先于 set_kv 设置")
        # No second decay within the same process
        asyncio.run(agent._maybe_decay())
        self.assertEqual(len(decay_calls), 1)

    def test_summary_lifecycle_kv_failure_still_returns_total(self):
        db = _AgentDB()

        async def set_kv(key, value):
            if key == "last_summary_lifecycle_at":
                raise RuntimeError("kv write boom")
            db.kv[key] = value

        async def archive(**kw):
            return 5

        db.set_kv = set_kv
        db.archive_stale_summaries = archive
        db.kv["last_summary_lifecycle_at"] = (
            datetime.now(timezone.utc) - timedelta(days=90)
        ).isoformat()
        agent = mod.FactMergeAgent(
            db=db, llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(summary_lifecycle_enabled=True),
            encoder=None,
        )
        total = asyncio.run(agent._maybe_summary_lifecycle())
        self.assertEqual(total, 5)
        self.assertIsNotNone(agent._last_summary_lifecycle_at)


class TestConfigConstraints(unittest.TestCase):
    def test_zero_breakers_rejected(self):
        import pydantic

        for kwargs in (
            {"score_cap": 0}, {"promote_threshold": 0},
            {"failure_threshold": 0}, {"embedding_dims": 0},
            {"pool_min": 9, "pool_max": 8},
        ):
            with self.assertRaises(pydantic.ValidationError, msg=str(kwargs)):
                mod.LocalMemoryConfig(**kwargs)

    def test_positive_values_accepted(self):
        # With the score scale scaled as a whole (start_score/promote/
        # demote in proportion) it should still pass: the band scales
        # with the configured evidence increment, not hard-coded at 3
        cfg = mod.LocalMemoryConfig(
            score_cap=1.5, promote_threshold=1.5, demote_threshold=0.5,
            score_start_high=1, score_start_medium=1,
            recent_promote_threshold=1.0,
            naming_promote_threshold=1.0,
            failure_threshold=1, embedding_dims=1024,
        )
        self.assertEqual(cfg.score_cap, 1.5)


class TestPersistSemantics(unittest.TestCase):
    def test_merge_direction_memory_wins(self):
        """A stale on-disk value after a failed persist must not roll back the in-memory truth on the next unrelated save."""
        cw = importlib.import_module("noriflow_memory_pkg.config_web")
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            (d / "p.json").write_text(
                json.dumps({"A": 0, "Z": 9}), encoding="utf-8"
            )
            cw.persist_host_config(d, "p", {"B": 5}, {"A": 1})
            merged = json.loads((d / "p.json").read_text(encoding="utf-8"))
            self.assertEqual(merged["A"], 1, "内存真相源胜出（陈旧磁盘回滚）")
            self.assertEqual(merged["B"], 5)
            self.assertEqual(merged["Z"], 9, "磁盘独有键保留")
            self.assertFalse(
                list(d.glob("*.tmp")), "原子写不得残留临时文件"
            )

    def test_changed_persisted_with_validated_types(self):
        cw = importlib.import_module("noriflow_memory_pkg.config_web")
        result = cw.prepare_save({"score_cap": "7"}, {})
        self.assertEqual(result["changed"]["score_cap"], 7.0)
        self.assertIsInstance(result["changed"]["score_cap"], float)

    def test_mask_sentinel_collision_honored(self):
        cw = importlib.import_module("noriflow_memory_pkg.config_web")
        mask = cw._MASK
        # Normal case: mask = unchanged -> restore the current value
        out = cw.unmask_values({"dsn": mask}, {"dsn": "postgres://x"})
        self.assertEqual(out["dsn"], "postgres://x")
        # Collision: the real value happens to be the mask string ->
        # the submitted value is honored (setting that value is allowed)
        out2 = cw.unmask_values({"dsn": mask}, {"dsn": mask})
        self.assertEqual(out2["dsn"], mask)

    def test_numeric_bounds_exposed(self):
        cw = importlib.import_module("noriflow_memory_pkg.config_web")
        schema = cw.config_schema()
        # score_cap is float + gt=0 (exclusive bound): min must be the
        # closest representable float above 0 (nextafter), so a UI
        # submit of that value is not rejected server-side with a 422
        # (submitting 0 would be)
        self.assertEqual(schema["score_cap"].get("min"), math.nextafter(0, math.inf))
        self.assertGreater(schema["failure_threshold"].get("min", 0), 0)
        self.assertEqual(schema["embedding_dims"].get("min"), 1)


class TestDbSqlShapes(unittest.TestCase):
    def test_existing_edge_keys_excludes_superseded(self):
        pool = _SqlPool(fetch_rows=[])
        db = _db_with_pool(pool)
        asyncio.run(db._fetch_existing_edge_keys(pool, [{
            "platform": "qq", "subject_uid": "a", "object_uid": "b",
            "relation_label": "姐姐",
        }]))
        sql = pool.queries[0][0]
        self.assertIn("e.status <> 'superseded'", sql)

    def test_supersede_edges_counts_returning_rows(self):
        pool = _SqlPool(fetch_rows=[{"id": 1}, {"id": 2}])
        db = _db_with_pool(pool)
        count = asyncio.run(db.supersede_edges([1, 2, 3]))
        self.assertEqual(count, 2, "RETURNING 行计数（fetchval 恒 0 的旧病）")
        self.assertIn("RETURNING e.id", pool.queries[0][0])

        ts = datetime(2026, 9, 11, tzinfo=timezone.utc)
        pool2 = _SqlPool(fetch_rows=[{"id": 1}])
        db2 = _db_with_pool(pool2)
        asyncio.run(db2.supersede_edges(
            [1], expected_updated_at={1: ts}, reasons={1: "互动描述"},
        ))
        sql2 = pool2.queries[0][0]
        self.assertIn("e.updated_at = v.ts", sql2, "乐观锁守卫")

    def test_merge_dedup_replay_keeps_state(self):
        """P3-f: an evidence_key dedup hit (pure replay) must not change status or clear flags."""
        src = inspect.getsource(mod.MemoryDatabase.apply_fact_merge)
        self.assertIn("WHEN $1 = ANY(c.evidence_keys) THEN c.status", src)
        self.assertIn(
            "WHEN $1 = ANY(c.evidence_keys)\n"
            "                                              THEN c.demoted_at",
            src,
        )


class TestBackfillPendingMarker(unittest.TestCase):
    def test_mark_take_dedupe(self):
        holder = types.SimpleNamespace(kv={})

        async def get_kv(key):
            return holder.kv.get(key)

        async def set_kv(key, value):
            holder.kv[key] = value

        holder.get_kv = get_kv
        holder.set_kv = set_kv
        self.assertEqual(asyncio.run(_BACKFILL.mark_backfill_pending(holder, [3, 5])), 2)
        self.assertEqual(asyncio.run(_BACKFILL.mark_backfill_pending(holder, [5, 7])), 1)
        self.assertEqual(
            json.loads(holder.kv[_BACKFILL._BACKFILL_PENDING_KEY]), [3, 5, 7]
        )
        taken = asyncio.run(_BACKFILL.take_backfill_pending(holder))
        self.assertEqual(taken, [3, 5, 7])
        self.assertEqual(asyncio.run(_BACKFILL.take_backfill_pending(holder)), [])


class TestSupersedeBackfillEdgesOf(unittest.TestCase):
    def test_only_pure_backfill_edges_of_dead_clusters(self):
        conn = _CaptureConn(edges=[
            {"id": 1, "evidence_keys": ["backfill|5"]},        # dead source
            {"id": 2, "evidence_keys": ["backfill|5", "s|2026-09-01"]}, # mixed
            {"id": 3, "evidence_keys": ["backfill|6"]},        # source alive
        ])
        count = asyncio.run(_DB.supersede_backfill_edges_of(conn, [5]))
        self.assertEqual(count, 1)
        self.assertEqual(conn.superseded_args, ([1],))


class TestPlaceholderSingleSource(unittest.TestCase):
    def test_python_and_sql_predicates_agree(self):
        corpus = [
            ("未知", True), ("未知用户", True), ("未知用户3", True),
            ("用户12", True), ("123", True),
            ("unknown", True), ("UNDEFINED", True),
            ("小张", False), ("未知旅人", False), ("u123", False),
        ]
        import re as _re
        sql_re = _re.compile(_ALIAS.PLACEHOLDER_NAME_SQL, _re.IGNORECASE)
        for name, expected in corpus:
            self.assertIs(_ALIAS.is_placeholder_name(name), expected, name)
            self.assertEqual(
                bool(sql_re.match(name)), expected,
                f"SQL 副本与 Python 判定漂移: {name}",
            )

    def test_db_sources_embed_shared_constant(self):
        # After the db package split the SQL bodies live in db/postgres.py
        # (and sqlite.py) — the placeholder-name regex literal must not be
        # inlined in any backend SQL (root cause of the three-way drift)
        sources = []
        for sub in ("postgres", "sqlite"):
            try:
                sources.append(
                    inspect.getsource(
                        importlib.import_module(f"noriflow_memory_pkg.db.{sub}")
                    )
                )
            except (ImportError, ModuleNotFoundError):
                pass  # sqlite backend is optional (skipped when aiosqlite is missing)
        for src in sources:
            self.assertIn("PLACEHOLDER_NAME_SQL", src)
            self.assertNotIn(
                "^(unknown|undefined|用户[0-9]+", src,
                "db 层不得再内联占位名正则字面量（三处漂移的病根）",
            )


class TestStatementTruncation(unittest.TestCase):
    def test_fact_statement_truncated(self):
        fact = mod.MemoryEncoder._parse_fact({
            "user_id": "u1", "statement": "长" * 500,
            "category": "stable", "confidence": "high",
        })
        self.assertIsNotNone(fact)
        self.assertLessEqual(len(fact.statement), 200)


class TestTimezoneCacheReset(unittest.TestCase):
    def test_reset_picks_up_new_timezone(self):
        kernel = mod.LocalMemoryKernel(
            db=FakeDB(),
            embedding_service=types.SimpleNamespace(),
            circuit_breaker=None,
            config=mod.LocalMemoryConfig(),
            bot_id="kira",
        )
        from zoneinfo import ZoneInfo

        first = kernel._local_tz()
        self.assertIs(kernel._local_tz(), first, "缓存命中")
        kernel.config.timezone = "Asia/Tokyo"
        self.assertIs(
            kernel._local_tz(), first, "未重置时沿用旧时区（修复前的行为）"
        )
        kernel.reset_local_tz_cache()
        self.assertEqual(kernel._local_tz(), ZoneInfo("Asia/Tokyo"))


# ---------------------------------------------------------------------------
#  Merged from test_encode_backfill.py (WebUI manual encode-kick state machine, v1.15.5)
# ---------------------------------------------------------------------------


class TestEncodeKickGuards(unittest.TestCase):
    def _agent(self):
        return mod.FactMergeAgent(
            db=types.SimpleNamespace(), llm=types.SimpleNamespace(),
            config=mod.LocalMemoryConfig(),
            encoder=None, embedding_service=None,
        )

    def test_kick_before_start_returns_false(self):
        """No inter-cycle event exists before start(): kick must refuse."""
        agent = self._agent()
        self.assertFalse(agent.kick())
        self.assertFalse(agent.cycle_running)
        self.assertIsNone(agent.last_cycle)

    def test_kick_while_cycle_running_returns_false(self):
        """A running cycle must not be kicked (the loop is the only runner)."""
        agent = self._agent()
        agent._in_cycle = True
        agent._kick_event = asyncio.Event()
        try:
            self.assertFalse(agent.kick())
        finally:
            agent._in_cycle = False

    def test_kick_with_event_sets_it(self):
        """Idle loop with a live event: kick resolves it and returns True."""
        agent = self._agent()
        event = asyncio.Event()
        agent._kick_event = event
        self.assertTrue(agent.kick())
        self.assertTrue(event.is_set())


class TestEncodeKickLoop(unittest.TestCase):
    def test_kick_wakes_loop_and_records_last_cycle(self):
        """The real _run loop: a kicked cycle finishes early, snapshot lands."""
        async def main():
            old_delay = _MERGE._INITIAL_DELAY_SECONDS
            _MERGE._INITIAL_DELAY_SECONDS = 0.01
            try:
                agent = mod.FactMergeAgent(
                    db=types.SimpleNamespace(), llm=types.SimpleNamespace(),
                    config=mod.LocalMemoryConfig(),
                    encoder=None, embedding_service=None,
                )
                counter = {"n": 0}

                async def fake_cycle():
                    counter["n"] += 1
                    return {"reencoded": counter["n"]}

                agent.run_cycle = fake_cycle  # type: ignore[method-assign]
                agent.start()
                await asyncio.sleep(0.25)
                self.assertIsNotNone(agent.last_cycle, "first cycle snapshot missing")
                self.assertFalse(agent.cycle_running)
                first = agent.last_cycle["stats"]["reencoded"]
                self.assertTrue(agent.kick(), "idle loop must accept a kick")
                await asyncio.sleep(0.25)
                second = agent.last_cycle["stats"]["reencoded"]
                self.assertGreater(second, first, "kicked cycle did not run")
                self.assertFalse(agent.cycle_running)
                await agent.stop()
                self.assertFalse(agent.kick(), "post-stop kick must refuse")
            finally:
                _MERGE._INITIAL_DELAY_SECONDS = old_delay

        asyncio.run(main())


mod = None
_HTTP_EXCEPTION = None


def _setup() -> None:
    global mod, _HTTP_EXCEPTION, CONTRACTS
    mod = _load_plugin_module()
    import importlib as _il
    globals()['CONTRACTS'] = _il.import_module('noriflow_memory_pkg.contracts')
    globals()['_DB'] = _il.import_module('noriflow_memory_pkg.db')
    globals()['_MERGE'] = _il.import_module('noriflow_memory_pkg.merge_agent')
    globals()['_BACKFILL'] = _il.import_module('noriflow_memory_pkg.relation_backfill')
    globals()['_ALIAS'] = _il.import_module('noriflow_memory_pkg.alias_store')
    globals()['_EDGE'] = _il.import_module('noriflow_memory_pkg.entity_edge')
    _ju = _il.import_module('noriflow_memory_pkg.json_utils')
    mod.safe_parse_llm_json = _ju.safe_parse_llm_json
    _env = _il.import_module('noriflow_memory_pkg.envelope')
    for _n in ('format_history_message', 'sanitize_envelope_field', 'break_packet_mimicry', 'HISTORY_BATCH_SEPARATOR'):
        setattr(mod, _n, getattr(_env, _n))
    try:
        from fastapi import HTTPException

        _HTTP_EXCEPTION = HTTPException
    except ImportError:
        _HTTP_EXCEPTION = Exception


# Assembled at import time (pytest collection path); calling main()
# for a direct run is idempotent
_setup()


def main() -> int:
    _setup()
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0)
    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        result = runner.run(suite)
    for _, tb in getattr(result, "errors", []) + getattr(result, "failures", []):
        print(tb.rstrip())
        print()
    total = result.testsRun
    failed = len(result.failures) + len(result.errors)
    print(f"{total - failed}/{total} tests passed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

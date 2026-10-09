"""KiraAI client adaptation layer: wraps the KiraAI provider system into this plugin's internal interface.

Internal plugin modules (kernel/vector_ops/encoder/merge_agent) keep the
established interface shapes (embed/embed_batch, rerank returns (index,
score) pairs, run_structured structured-text exit); this module adapts
one-way:

- KiraEmbeddingClient: EmbeddingModelClient.embed(texts) batch interface
  -> single embed + batch embed_batch;
- KiraRerankClient: RerankModelClient.rerank -> list[RerankResult]
  -> list of (index, score) pairs (consumed by kernel via dict(ranked));
- FastLlmExit: ctx default fast LLM -> run_structured(system, user, schema)
  -> str (structured output goes through native KiraAI tool calling).

Unconfigured models (default_embedding/default_rerank raise ValueError)
are caught by main.py at assembly with try/except falling to None -> the
corresponding plugin feature degrades (in-row vectors set to NULL awaiting
backfill / pure-vector ordering), consistent with the graceful degradation
semantics.
"""

from __future__ import annotations

from core.logging_manager import get_logger

from typing import Optional

logger = get_logger("noriflow_memory.clients", "cyan")


class KiraEmbeddingClient:
    """Embedding adapter (single-call interface wrapping a batch backend)."""

    def __init__(self, client) -> None:
        # KiraAI EmbeddingModelClient：embed(texts: list[str]) -> list[list[float]]
        self._client = client

    async def embed(self, text: str) -> list[float]:
        vectors = await self._client.embed([text])
        return vectors[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return await self._client.embed(list(texts))

    async def close(self) -> None:
        """Host-managed client needs no closing (interface-compatible with the upstream nori client)."""


class KiraRerankClient:
    """Rerank adapter (RerankResult -> (index, score) pairs)."""

    def __init__(self, client) -> None:
        self._client = client

    async def rerank(
        self, query: str, documents: list[str], top_k: Optional[int] = None
    ) -> list[tuple[int, float]]:
        results = await self._client.rerank(query, documents, top_n=top_k)
        return [(int(r.index), float(r.score)) for r in results]

    async def close(self) -> None:
        """Host-managed client needs no closing (interface-compatible with the upstream nori client)."""


class FastLlmExit:
    """LLM exit for background structured tasks (single fast-model call).

    Uses the ctx default fast LLM (encoding/arbitration are both background
    tasks; the fast tier is enough). When a schema is passed, structured
    output goes through native KiraAI tool calling: tool_choice="required"
    forces the model to call the single submit tool, whose tool_call
    arguments are the result JSON matching the schema (the generic
    equivalent of response_format json_object).

    Fallback: when a particular model/gateway returns no tool_call, fall
    back to text output, handled by the strong prompt instruction plus the
    caller's safe_parse_llm_json lenient parsing (fail-open degradation).
    """

    def __init__(self, ctx) -> None:
        self._ctx = ctx
        self._warned_no_tool_call = False

    async def run_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        schema: Optional[dict] = None,
        tool_name: str = "submit_result",
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        # 延迟解析客户端：initialize 时 provider 可能尚未就绪（如热重载序）
        client = self._ctx.get_default_fast_llm_client()
        if client is None:
            raise RuntimeError("fast LLM 未配置（default_fast_llm）")
        from core.provider import LLMRequest

        extra: dict = {}
        if schema:
            # 结构化出口：强制调用唯一的提交工具，arguments 即结果 JSON
            extra["tools"] = [{
                "type": "function",
                "function": {
                    "name": tool_name,
                    "description": "提交本次任务的结构化结果（JSON 对象，字段遵循参数 schema）",
                    "parameters": schema,
                },
            }]
            extra["tool_choice"] = "required"
        request = LLMRequest(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            **extra,
        )
        response = await client.chat(request, temperature=temperature, max_tokens=max_tokens)
        if schema:
            for call in response.tool_calls or []:
                fn = (call or {}).get("function") or {}
                if fn.get("name") == tool_name and fn.get("arguments"):
                    return fn["arguments"]
            # 无 tool_call：走提示词 JSON 兜底（一次性告警，避免刷日志）
            if (response.text_response or "").strip() and not self._warned_no_tool_call:
                self._warned_no_tool_call = True
                logger.warning(
                    "fast LLM 未按 tool_choice=required 返回 %s 调用，回退提示词 JSON 解析",
                    tool_name,
                )
        return response.text_response

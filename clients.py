"""KiraAI 客户端适配层：把 KiraAI provider 体系包装成本插件内部接口。

插件内部模块（kernel/vector_ops/encoder/merge_agent）沿用既定接口形态
（embed/embed_batch、rerank 返回 (index, score) 对、run_structured
结构化文本出口），本模块做单向适配：

- KiraEmbeddingClient：EmbeddingModelClient.embed(texts) 批量接口
  -> embed 单条 + embed_batch 批量；
- KiraRerankClient：RerankModelClient.rerank -> list[RerankResult]
  -> (index, score) 对列表（kernel 以 dict(ranked) 消费）；
- FastLlmExit：ctx 默认 fast LLM -> run_structured(system, user, schema)
  -> str（结构化输出走 KiraAI 原生 tool calling）。

未配置的模型（default_embedding/default_rerank 抛 ValueError）由 main.py
在装配时 try/except 落 None -> 插件内部对应功能降级（行内向量置 NULL 待
补算 / 纯向量序），与 graceful degradation 语义一致。
"""

from __future__ import annotations

from core.logging_manager import get_logger

from typing import Optional

logger = get_logger("noriflow_memory.clients", "cyan")


class KiraEmbeddingClient:
    """embedding 适配器（单条接口封装批量底层）。"""

    def __init__(self, client) -> None:
        # KiraAI EmbeddingModelClient：embed(texts: list[str]) -> list[list[float]]
        self._client = client

    async def embed(self, text: str) -> list[float]:
        vectors = await self._client.embed([text])
        return vectors[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return await self._client.embed(list(texts))

    async def close(self) -> None:
        """宿主托管客户端无需关闭（接口兼容上游 nori 版客户端）。"""


class KiraRerankClient:
    """重排序适配器（RerankResult -> (index, score) 对）。"""

    def __init__(self, client) -> None:
        self._client = client

    async def rerank(
        self, query: str, documents: list[str], top_k: Optional[int] = None
    ) -> list[tuple[int, float]]:
        results = await self._client.rerank(query, documents, top_n=top_k)
        return [(int(r.index), float(r.score)) for r in results]

    async def close(self) -> None:
        """宿主托管客户端无需关闭（接口兼容上游 nori 版客户端）。"""


class FastLlmExit:
    """后台结构化任务的 LLM 出口（快模型单次调用）。

    使用 ctx 默认 fast LLM（编码/裁定均为后台任务，快速档足够）。
    传入 schema 时结构化输出走 KiraAI 原生 tool calling：以
    tool_choice="required" 强制模型调用唯一的提交工具，tool_call 的
    arguments 即符合 schema 的结果 JSON（response_format json_object
    的通用等价物）。

    兜底：个别模型/网关不返回 tool_call 时回退文本输出，由提示词强
    指令 + 调用方 safe_parse_llm_json 容错解析承接（fail-open 降级）。
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

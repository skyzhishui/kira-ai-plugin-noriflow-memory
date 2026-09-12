"""Prompt 模板加载器（自 nori-core 同名模块 vendor 的精简版）。

从插件 prompts/ 目录读取 .prompt 文件，用安全的正则替换渲染 {var}
占位符：{{/}} 转义为字面花括号（支持模板内 JSON 示例），缺失变量保留
占位原样。mtime 缓存——提示词文件热更即时生效。
"""

from __future__ import annotations

import re
from pathlib import Path


def render_template(template: str, **kwargs: object) -> str:
    """渲染 {var} 占位符模板，{{/}} 转义为字面花括号。

    转义先于占位符替换（哨兵保护）：占位符值原样插入，其中的花括号
    不被二次转义。
    """
    escaped = template.replace("{{", "\x00").replace("}}", "\x01")
    rendered = re.sub(
        r"\{(\w+)\}",
        lambda m: str(kwargs.get(m.group(1), m.group(0))),
        escaped,
    )
    return rendered.replace("\x00", "{").replace("\x01", "}")


class PromptLoader:
    """Prompt 模板加载器。从 prompts/ 目录读取 .prompt 文件并渲染。"""

    def __init__(self, prompts_dir: str | Path = "prompts") -> None:
        self.prompts_dir = Path(prompts_dir)
        # name -> (mtime, template)：mtime 变化即失效重读
        self._cache: dict[str, tuple[float, str]] = {}

    def load(self, name: str) -> str:
        """加载模板（不含 .prompt 后缀）。缓存复用，文件 mtime 变化时失效。"""
        file_path = self.prompts_dir / f"{name}.prompt"
        try:
            mtime = file_path.stat().st_mtime
        except FileNotFoundError:
            cached = self._cache.get(name)
            if cached is not None:
                return cached[1]
            raise
        cached = self._cache.get(name)
        if cached is not None and cached[0] == mtime:
            return cached[1]
        template = file_path.read_text(encoding="utf-8")
        self._cache[name] = (mtime, template)
        return template

    def render(self, name: str, **kwargs: object) -> str:
        """加载并渲染模板（缺失变量保留占位符原样）。"""
        return render_template(self.load(name), **kwargs)

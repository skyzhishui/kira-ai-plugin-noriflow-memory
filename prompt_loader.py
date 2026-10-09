"""Prompt template loader (trimmed version vendored from the nori-core same-name module).

Reads .prompt files from the plugin prompts/ directory and renders {var}
placeholders with safe regex substitution: {{/}} escapes to literal braces
(supports JSON examples inside templates); missing variables keep the
placeholder as-is. mtime cache, prompt files hot-reload on change.
"""

from __future__ import annotations

import re
from pathlib import Path


def render_template(template: str, **kwargs: object) -> str:
    """Render a {var} placeholder template; {{/}} escapes to literal braces.

    Escaping happens before placeholder substitution (sentinel protection):
    placeholder values are inserted verbatim, and their braces are not
    escaped a second time.
    """
    escaped = template.replace("{{", "\x00").replace("}}", "\x01")
    rendered = re.sub(
        r"\{(\w+)\}",
        lambda m: str(kwargs.get(m.group(1), m.group(0))),
        escaped,
    )
    return rendered.replace("\x00", "{").replace("\x01", "}")


class PromptLoader:
    """Prompt template loader. Reads .prompt files from prompts/ and renders."""

    def __init__(self, prompts_dir: str | Path = "prompts") -> None:
        self.prompts_dir = Path(prompts_dir)
        # name -> (mtime, template)：mtime 变化即失效重读
        self._cache: dict[str, tuple[float, str]] = {}

    def load(self, name: str) -> str:
        """Load a template (without the .prompt suffix). Cache-reused; invalidated when the file mtime changes."""
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
        """Load and render a template (missing variables keep the placeholder as-is)."""
        return render_template(self.load(name), **kwargs)

"""Shared direct-run test infrastructure (host stubs + module loader).

2026-09-12 review cleanup: every direct-run test file used to carry its own
copy of the core.* host stubs and the importlib loader (7 near-identical
copies, ~600 lines of boilerplate); they are now consolidated here.

- install_core_stubs(): core.* / fastapi host stubs (a superset of the
  historical per-file variants — includes register.tag and the fastapi
  fallback; harmless for files that never touch those surfaces);
- load_module(name): loader handling both the db package directory and
  single-file modules (stubs installed first, always).

Convention: test files load the plugin submodules they need through
load_module before use; main.py is always loaded last (its import chain
pulls in the other submodules via the package stub).

Note: test_noriflow_memory.py (the unittest main suite) stays
self-contained — it is spec-exec'd under a separate module name by
test_config_web.py and does not import this module.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent


def install_core_stubs() -> None:
    """Install the core.* host stubs (idempotent: skipped when the package
    stub already exists)."""
    if "noriflow_memory_pkg" in sys.modules:
        return

    def get_logger(*args, **kwargs):
        import logging

        return logging.getLogger("stub")

    core = types.ModuleType("core")
    logging_mod = types.ModuleType("core.logging_manager")
    logging_mod.get_logger = get_logger
    sys.modules["core"] = core
    sys.modules["core.logging_manager"] = logging_mod

    plugin_mod = types.ModuleType("core.plugin")

    class Priority:
        LOW = -50
        MEDIUM = 0
        HIGH = 50

    class BasePlugin:
        def __init__(self, ctx, cfg):
            self.ctx = ctx
            self.plugin_cfg = cfg

    class _On:
        def __getattr__(self, name):
            def deco(*args, **kwargs):
                def wrap(func):
                    return func

                return wrap

            return deco

    class _Register:
        def tool(self, name, description, params):
            def wrap(func):
                return func

            return wrap

        def tag(self, *args, **kwargs):
            def wrap(func):
                return func

            return wrap

        def page(self, route, menu=None):
            def wrap(func):
                return func

            return wrap

        def api(self, method, path, auth=True, **kwargs):
            def wrap(func):
                return func

            return wrap

    plugin_mod.BasePlugin = BasePlugin
    plugin_mod.Priority = Priority
    plugin_mod.on = _On()
    plugin_mod.register = _Register()
    plugin_mod.PluginPage = type(
        "PluginPage", (), {"from_folder": staticmethod(lambda path: ("folder", path))}
    )
    plugin_mod.PageMenu = type("PageMenu", (), {"__init__": lambda self, **kw: None})
    plugin_mod.logger = get_logger()
    sys.modules["core.plugin"] = plugin_mod

    chat_mod = types.ModuleType("core.chat")
    elements_mod = types.ModuleType("core.chat.message_elements")

    class Text:
        def __init__(self, text=""):
            self.text = text

    class At:
        def __init__(self, pid="", nickname=None):
            self.pid = str(pid)
            self.nickname = nickname

    class Reply:
        def __init__(self, message_id="", message_content=None, chain=None):
            self.message_id = str(message_id)
            self.message_content = message_content
            self.chain = chain

    elements_mod.Text = Text
    elements_mod.At = At
    elements_mod.Reply = Reply
    sys.modules["core.chat"] = chat_mod
    sys.modules["core.chat.message_elements"] = elements_mod

    prompt_mod = types.ModuleType("core.prompt_manager")

    class Prompt:
        def __init__(self, content="", name="", source="", persist=True):
            self.content = content

    prompt_mod.Prompt = Prompt
    sys.modules["core.prompt_manager"] = prompt_mod

    if sys.modules.get("fastapi") is None:
        # Leave the real package alone if it is already imported; otherwise
        # install a stub (this shadows an installed-but-unimported fastapi
        # for the rest of the process — the plugin only needs
        # HTTPException/Body, so that is fine for tests)
        fastapi_stub = types.ModuleType("fastapi")

        class HTTPException(Exception):
            def __init__(self, status_code=400, detail=""):
                self.status_code = status_code
                self.detail = detail
                super().__init__(detail)

        def Body(*args, **kwargs):
            return None

        fastapi_stub.HTTPException = HTTPException
        fastapi_stub.Body = Body
        sys.modules["fastapi"] = fastapi_stub

    pkg = types.ModuleType("noriflow_memory_pkg")
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules["noriflow_memory_pkg"] = pkg


def load_module(mod_name: str):
    """Load a plugin submodule (db is a package directory, everything else a
    single file; host stubs installed first)."""
    install_core_stubs()
    target = PLUGIN_DIR / mod_name
    if target.is_dir():
        path = target / "__init__.py"
        kwargs = {"submodule_search_locations": [str(target)]}
    else:
        path = PLUGIN_DIR / f"{mod_name}.py"
        kwargs = {}
    spec = importlib.util.spec_from_file_location(
        f"noriflow_memory_pkg.{mod_name}", path, **kwargs
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"noriflow_memory_pkg.{mod_name}"] = mod
    spec.loader.exec_module(mod)
    return mod

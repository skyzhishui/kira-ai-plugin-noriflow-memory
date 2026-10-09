"""Shared test bootstrap: core.* host stubs + plugin package loader (works both under pytest and python direct-run).

Previously the 7 test files each copied ~100-140 lines of stub facilities
(review 2026-09-12 redundancy item 1); this module unifies them into a
single implementation:

- install_host_stubs(): idempotently injects core.* stubs (logging/plugin/chat/prompt/
  provider; fallback stub when fastapi is missing). The core.plugin on/register
  stubs record decoration calls (test_noriflow_memory lifecycle/subscription
  assertions depend on that; the other files do not consume the records, harmless).
- load_modules(*names): ensures noriflow_memory_pkg is ready, then imports
  by name and returns a tuple of modules — dependency modules resolve via
  normal import machinery through pkg.__path__, and sys.modules dedup keeps
  a single instance across files.

Importing this module relies on tests/ being on sys.path: pytest (auto-inserts
a non-package test dir) and direct-run (script dir is sys.path[0]) both satisfy
that; each test file also inserts explicitly as a fallback.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
PKG_NAME = "noriflow_memory_pkg"


def install_host_stubs() -> None:
    """Idempotently injects core.* host stubs (no-op when already installed)."""
    if "core.plugin" in sys.modules:
        return

    core = types.ModuleType("core")
    sys.modules["core"] = core

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
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            def deco(*args, **kwargs):
                def wrap(func):
                    self.calls.append((name, args, kwargs, func.__name__))
                    return func

                return wrap

            return deco

    class _Register:
        def __init__(self):
            self.tools = []
            self.pages = []
            self.apis = []

        def tool(self, name, description, params):
            def wrap(func):
                if not any(t["name"] == name for t in self.tools):
                    self.tools.append({"name": name, "description": description,
                                       "params": params, "func": func.__name__})
                return func

            return wrap

        def page(self, route, menu=None):
            def wrap(func):
                if not any(p["route"] == route for p in self.pages):
                    self.pages.append({"route": route, "menu": menu,
                                       "func": func.__name__})
                return func

            return wrap

        def api(self, method, path, auth=True, **kwargs):
            def wrap(func):
                if not any(a["method"] == method and a["path"] == path
                           for a in self.apis):
                    self.apis.append({"method": method, "path": path,
                                      "auth": auth, "func": func.__name__})
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

    def get_logger(*args, **kwargs):
        import logging

        return logging.getLogger("stub")

    plugin_mod.logger = get_logger()
    sys.modules["core.plugin"] = plugin_mod

    logging_mgr = types.ModuleType("core.logging_manager")
    logging_mgr.get_logger = get_logger
    sys.modules["core.logging_manager"] = logging_mgr

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
        # 对齐宿主签名：persist/end 是注入与持久化断言的依据
        def __init__(self, content="", name="", source="", persist=True,
                     end=None, **kwargs):
            self.content = content
            self.name = name
            self.source = source
            self.persist = persist
            self.end = end

    prompt_mod.Prompt = Prompt
    sys.modules["core.prompt_manager"] = prompt_mod

    provider_mod = types.ModuleType("core.provider")

    class LLMRequest:
        # 桩：仅承接 clients.FastLlmExit 用到的构造参数与 tool_choice 推导
        def __init__(self, messages=None, tools=None, tool_funcs=None,
                     tool_set=None, tool_choice=None):
            self.messages = messages or []
            self.tools = tools
            self.tool_choice = tool_choice or ("auto" if tools else "none")

    provider_mod.LLMRequest = LLMRequest
    sys.modules["core.provider"] = provider_mod

    if "fastapi" not in sys.modules:
        try:
            import fastapi  # noqa: F401  真包优先（HTTPException 断言语义一致）
        except ImportError:
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


def load_modules(*names: str):
    """Loads plugin modules by name (returns a tuple; repeated loads dedup via sys.modules)."""
    install_host_stubs()
    if PKG_NAME not in sys.modules:
        pkg = types.ModuleType(PKG_NAME)
        pkg.__path__ = [str(PLUGIN_DIR)]
        sys.modules[PKG_NAME] = pkg
    return tuple(importlib.import_module(f"{PKG_NAME}.{n}") for n in names)


def load_plugin_module():
    """Loads the plugin main module (with all dependencies; idempotent, returns the same instance)."""
    return load_modules("main")[0]

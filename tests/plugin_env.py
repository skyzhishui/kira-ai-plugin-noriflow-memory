"""共享测试引导：core.* 宿主桩 + 插件包装载（pytest 与 python 直跑两用）。

此前 7 个测试文件各自复制 ~100-140 行桩设施（review 2026-09-12 冗余项 1），
本模块统一为单一实现：

- install_host_stubs(): 幂等注入 core.* 桩（logging/plugin/chat/prompt/
  provider；fastapi 未安装时的兜底桩）。core.plugin 的 on/register 桩
  记录装饰调用（test_noriflow_memory 的生命周期/订阅断言依赖；其余文件
  不消费记录，仅无害）。
- load_modules(*names): 确保 noriflow_memory_pkg 就绪后按名 import，
  返回模块元组——依赖模块经 pkg.__path__ 由常规导入机制解析，
  sys.modules 去重保证跨文件单实例。

导入本模块依赖 tests/ 在 sys.path：pytest（非包测试目录自动插入）与
直跑（脚本目录即 sys.path[0]）均满足；各测试文件显式 insert 兜底。
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
PKG_NAME = "noriflow_memory_pkg"


def install_host_stubs() -> None:
    """幂等注入 core.* 宿主桩（已安装时跳过）。"""
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
    """按名加载插件模块（返回元组；重复加载经 sys.modules 去重）。"""
    install_host_stubs()
    if PKG_NAME not in sys.modules:
        pkg = types.ModuleType(PKG_NAME)
        pkg.__path__ = [str(PLUGIN_DIR)]
        sys.modules[PKG_NAME] = pkg
    return tuple(importlib.import_module(f"{PKG_NAME}.{n}") for n in names)


def load_plugin_module():
    """加载插件主模块（连带全依赖；幂等，返回同一实例）。"""
    return load_modules("main")[0]

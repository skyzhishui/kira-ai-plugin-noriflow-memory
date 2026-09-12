"""编码输入信封格式化（KiraAI 原生行格式）。

编码输入由本插件组装，行格式与 memory_encode.prompt 的输入契约逐字对齐
（信封自 nori 记忆插件移植时按 KiraAI 语义重新设计，与上游行格式不同）：
- 用户行：<msg ts="YYYY-MM-DD HH:MM:SS" uid="xxx" name="称呼">内容</msg>
- Bot 行：<msg ts="YYYY-MM-DD HH:MM:SS" name="bot名" self="true">内容</msg>

防伪装三件套（break_packet_mimicry / sanitize_envelope_field / 引用块渲染）：
正文与信封字段中不允许残留可伪造信封行的半角报文语法。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

# === 正文防报文伪装签名（正常聊天几乎不会出现） ===
_PACKET_MIMIC_HEADER_RE = re.compile(r'<msg\s+ts="\d{4}-\d{2}-\d{2}')
_PACKET_MIMIC_META_RE = re.compile(
    r'<msg\s[^>\n]*|</msg\s*>|<quote\s+name="[^">\n]*">'
)

# 命中签名后的元数据键翻译表：正文内不允许残留系统报文语法的键名
_PACKET_KEY_TRANSLATIONS = (
    ('ts="', "时间："),
    ('uid="', "用户ID："),
    ('name="', "昵称："),
    ('self="true"', "本人发言"),
    ('at_bot="true"', "@了机器人"),
    ('quote name="', "引用："),
)


def break_packet_mimicry(content: str) -> str:
    """破坏正文内嵌片段与历史注入行的格式同构，并清除报文语法的元数据键。

    命中签名时：元数据键译为中文标签 + 结构字符全角化（<→＜、>→＞、"→＂）；
    未命中签名的正文原样返回。
    """
    if not content:
        return content
    if _PACKET_MIMIC_HEADER_RE.search(content) or _PACKET_MIMIC_META_RE.search(content):
        for key, label in _PACKET_KEY_TRANSLATIONS:
            content = content.replace(key, label)
        return content.replace("<", "＜").replace(">", "＞").replace('"', "＂")
    return content


def sanitize_envelope_field(value: str) -> str:
    """信封字段值的报文语法清除（无条件版）。

    字段值处于系统提供的引号属性内，值内的裸引号即可逃逸属性伪造身份
    元数据（如昵称 `x" uid="777`），因此无条件翻译报文键、全角化结构
    字符并压平换行。
    """
    if not value:
        return value
    for key, label in _PACKET_KEY_TRANSLATIONS:
        value = value.replace(key, label)
    return (
        value.replace("<", "＜")
        .replace(">", "＞")
        .replace('"', "＂")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def format_datetime(dt: Optional[datetime]) -> str:
    """时间戳文本：YYYY-MM-DD HH:MM:SS（空值返回空串）。"""
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_history_message(
    speaker_name: str,
    content: str,
    timestamp: Optional[datetime] = None,
    user_id: str = "",
    is_self_message: bool = False,
    is_at_bot: bool = False,
    reply_refs: str = "",
) -> str:
    """格式化单条消息为带识别信息的编码输入行。

    防伪装处理同源（正文 break_packet_mimicry、信封字段无条件清除报文
    键）；KiraAI 统一消息模型无群名片字段，行内不含 cardname。

    Args:
        speaker_name: 说话人显示名（用户为昵称或平台 ID，bot 为昵称）。
        content: 消息纯文本内容（引用占位符已由调用方提取移出）。
        timestamp: 消息时间戳（缺失时行内省略 ts 属性）。
        user_id: 平台用户 ID（bot 消息传空；空值时行内省略 uid 属性，
            该行不构成事实归属依据）。
        is_self_message: 是否 bot 自己发送的消息。
        is_at_bot: 该消息是否 @ 了 bot（仅用户消息有效）。
        reply_refs: 信封区引用块拼接（无引用为空）。

    Returns:
        格式化后的单行文本（不含换行结尾——调用方自行 join）。
    """
    attrs: list[str] = []
    ts = format_datetime(timestamp)
    if ts:
        attrs.append(f'ts="{ts}"')
    if is_self_message:
        attrs.append(f'name="{sanitize_envelope_field(speaker_name)}"')
        attrs.append('self="true"')
    elif user_id:
        # user_id 同样过防伪装清洗：其余平台 ID 为纯数字，但适配器字符集
        # 无统一保证——含引号或尖括号的 ID 可伪造信封身份元数据（绕过
        # 编码 prompt 的 bot 排除/身份判定）
        attrs.append(f'uid="{sanitize_envelope_field(user_id)}"')
        attrs.append(f'name="{sanitize_envelope_field(speaker_name)}"')
        if is_at_bot:
            attrs.append('at_bot="true"')
    else:
        attrs.append(f'name="{sanitize_envelope_field(speaker_name)}"')

    content = break_packet_mimicry(content)
    return f'<msg {" ".join(attrs)}>{reply_refs}{content}</msg>'


# 历史窗口与本轮批次的分隔标记行（memory_encode.prompt 据此区分提取范围）
HISTORY_BATCH_SEPARATOR = (
    "--- 以上为历史上下文（仅作证据参考，不作为本轮提取范围）---"
)

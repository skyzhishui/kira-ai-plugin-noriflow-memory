"""Encoding input envelope formatting (KiraAI native line format).

The encoding input is assembled by this plugin; the line format is
verbatim-aligned with the input contract of memory_encode.prompt (the
envelope was redesigned for KiraAI semantics when ported from the nori
memory plugin; it differs from the upstream line format):
- User line: <msg ts="YYYY-MM-DD HH:MM:SS" uid="xxx" name="user_name">content</msg>
- Bot line: <msg ts="YYYY-MM-DD HH:MM:SS" name="bot_name" self="true">content</msg>

Anti-forgery triple (break_packet_mimicry / sanitize_envelope_field /
quote-block rendering): no half-width message-packet syntax that could forge
an envelope line may remain in the body or envelope fields.
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
    """Break the structural likeness between embedded body fragments and historical injection lines, and scrub message-syntax metadata keys.

    On signature match: metadata keys are translated to Chinese labels and
    structural characters are full-width-ized (<, >, "); body without a
    signature match is returned unchanged.
    """
    if not content:
        return content
    if _PACKET_MIMIC_HEADER_RE.search(content) or _PACKET_MIMIC_META_RE.search(content):
        for key, label in _PACKET_KEY_TRANSLATIONS:
            content = content.replace(key, label)
        return content.replace("<", "＜").replace(">", "＞").replace('"', "＂")
    return content


def sanitize_envelope_field(value: str) -> str:
    """Unconditional packet-syntax scrub of envelope field values.

    Field values sit inside system-provided quoted attributes; a bare quote
    inside the value can escape the attribute and forge identity metadata
    (e.g. nickname `x" uid="777`), so packet keys are unconditionally
    translated, structural characters full-width-ized, and newlines flattened.
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
    """Timestamp text: YYYY-MM-DD HH:MM:SS (empty string for None)."""
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
    """Format a single message as an encoding input line with identifying info.

    Anti-forgery handling shares the same source (break_packet_mimicry on
    the body, unconditional packet-key scrub on envelope fields); the KiraAI
    unified message model has no group-card-name field, so the line carries
    no cardname.

    Args:
        speaker_name: Speaker display name (nickname or platform id for
            users, nickname for the bot).
        content: Plain text body of the message (quote placeholders already
            extracted by the caller).
        timestamp: Message timestamp (the ts attribute is omitted when missing).
        user_id: Platform user id (empty for bot messages; when empty the
            uid attribute is omitted and the line does not constitute a fact
            attribution basis).
        is_self_message: Whether the bot sent the message itself.
        is_at_bot: Whether the message @-mentioned the bot (only valid for user messages).
        reply_refs: Quote block concatenated into the envelope area (empty when no quote).

    Returns:
        Formatted single-line text (no trailing newline, the caller joins).
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

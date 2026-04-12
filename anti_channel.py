# -*- coding: utf-8 -*-
"""
anti_channel.py — Защита от спама от имени каналов
Подключение в bot.py:
    import anti_channel
    # В main(): anti_channel.init(bot, dp, ADMIN_IDS, LOG_CHANNEL_ID)
"""
import logging
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ChatPermissions

log = logging.getLogger(__name__)

_bot: Optional[Bot] = None
_admin_ids: set = set()
_log_channel: int = 0

# Чаты где включена защита: {cid: bool}
_enabled: dict = {}

# Чаты где каналы в whitelist: {cid: set(channel_id)}
_whitelist: dict = {}


def init(bot: Bot, dp: Dispatcher, admin_ids, log_channel_id: int):
    """Инициализация модуля антиканал."""
    global _bot, _admin_ids, _log_channel
    _bot = bot
    _admin_ids = set(admin_ids) if admin_ids else set()
    _log_channel = log_channel_id

    @dp.message(F.sender_chat)
    async def on_channel_message(message: Message):
        """Обрабатывает сообщения от имени каналов."""
        cid = message.chat.id

        # Если модуль выключен для чата — пропускаем
        if not _enabled.get(cid, True):
            return

        sender_chat = message.sender_chat
        if not sender_chat:
            return

        # Если это сам чат пишет от своего имени — разрешаем
        if sender_chat.id == cid:
            return

        # Проверяем whitelist
        if sender_chat.id in _whitelist.get(cid, set()):
            return

        try:
            await message.delete()
        except Exception as e:
            log.warning(f"anti_channel: не удалось удалить сообщение: {e}")
            return

        try:
            await _bot.ban_chat_sender_chat(cid, sender_chat.id)
        except Exception as e:
            log.warning(f"anti_channel: не удалось забанить канал {sender_chat.id}: {e}")

        channel_title = sender_chat.title or str(sender_chat.id)
        log.info(f"anti_channel: заблокирован канал «{channel_title}» в чате {cid}")

        if _log_channel:
            try:
                await _bot.send_message(
                    _log_channel,
                    f"🚫 <b>Антиканал</b>\n"
                    f"💬 Чат: <code>{cid}</code>\n"
                    f"📢 Канал: <b>{channel_title}</b> (<code>{sender_chat.id}</code>)\n"
                    f"🔨 Действие: удалено сообщение + бан канала",
                    parse_mode="HTML"
                )
            except Exception:
                pass

    log.info("anti_channel: модуль инициализирован")


def enable(cid: int):
    """Включить защиту для чата."""
    _enabled[cid] = True


def disable(cid: int):
    """Выключить защиту для чата."""
    _enabled[cid] = False


def is_enabled(cid: int) -> bool:
    return _enabled.get(cid, True)


def add_whitelist(cid: int, channel_id: int):
    """Добавить канал в whitelist."""
    if cid not in _whitelist:
        _whitelist[cid] = set()
    _whitelist[cid].add(channel_id)


def remove_whitelist(cid: int, channel_id: int):
    """Убрать канал из whitelist."""
    if cid in _whitelist:
        _whitelist[cid].discard(channel_id)

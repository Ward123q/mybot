# -*- coding: utf-8 -*-
"""
chat_settings.py — Система настроек для каждого чата
Подключение: import chat_settings as cs
"""
import json
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

_bot = None

def set_bot(bot):
    global _bot
    _bot = bot


def db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_tables():
    conn = db()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS chat_settings (
        cid         INTEGER PRIMARY KEY,
        settings    TEXT    DEFAULT '{}'
    )""")
    conn.commit()
    conn.close()
    log.info("✅ chat_settings таблица создана")


# ══════════════════════════════════════════
#  ДЕФОЛТНЫЕ НАСТРОЙКИ
# ══════════════════════════════════════════

DEFAULT_SETTINGS = {
    # ⏰ Расписание
    "schedule_enabled":   False,
    "close_time":         "00:00",   # время закрытия чата
    "open_time":          "08:00",   # время открытия
    "quiet_start":        "23:00",   # тихий час начало
    "quiet_end":          "07:00",   # тихий час конец
    "quiet_enabled":      False,

    # 🛡 Модерация
    "max_warns":          3,
    "warn_expiry_days":   30,
    "mute_duration":      60,        # минут по умолчанию
    "auto_mute_newcomers":False,
    "newcomer_mute_hours":1,
    "auto_kick_inactive": False,
    "inactive_days":      30,
    "ban_on_max_warns":   True,

    # 👋 Приветствие
    "welcome_enabled":    True,
    "welcome_text":       "👋 Добро пожаловать, {name}!",
    "welcome_delete_prev":True,
    "welcome_delete_mins":5,
    "verify_enabled":     False,
    "verify_question":    "Сколько будет 2+2?",
    "verify_answer":      "4",
    "verify_timeout_mins":5,

    # 🤖 Антиспам
    "antispam_enabled":   True,
    "antimat_enabled":    True,
    "flood_msgs":         10,        # сообщений в минуту
    "flood_action":       "mute",    # mute/warn/kick
    "antilink_enabled":   False,
    "antisticker_enabled":False,
    "anticaps_enabled":   True,
    "caps_percent":       70,        # % заглавных для срабатывания

    # 🎮 Функции
    "xp_enabled":         True,
    "rep_enabled":        True,
    "games_enabled":      True,
    "economy_enabled":    True,
    "polls_enabled":      True,
    "anon_enabled":       True,
    "clans_enabled":      True,

    # 🏆 Экономика
    "xp_per_msg":         5,
    "rep_cooldown_hours": 1,
    "daily_bonus":        50,
    "newcomer_bonus":     100,

    # 📢 Авто-объявления
    "announce_enabled":   False,
    "announce_text":      "",
    "announce_interval":  100,       # каждые N сообщений
    "rules_remind_enabled":False,
    "rules_remind_interval": 200,
}


def get_settings(cid: int) -> dict:
    conn = db()
    row = conn.execute(
        "SELECT settings FROM chat_settings WHERE cid=?", (cid,)
    ).fetchone()
    conn.close()

    settings = dict(DEFAULT_SETTINGS)
    if row and row["settings"]:
        try:
            saved = json.loads(row["settings"])
            settings.update(saved)
        except:
            pass
    return settings


def save_settings(cid: int, settings: dict):
    conn = db()
    conn.execute(
        "INSERT INTO chat_settings (cid, settings) VALUES (?,?) "
        "ON CONFLICT(cid) DO UPDATE SET settings=?",
        (cid, json.dumps(settings), json.dumps(settings))
    )
    conn.commit()
    conn.close()


def update_setting(cid: int, key: str, value):
    settings = get_settings(cid)
    settings[key] = value
    save_settings(cid, settings)


# ══════════════════════════════════════════
#  РАСПИСАНИЕ ЧАТА
# ══════════════════════════════════════════

async def schedule_loop():
    """Фоновый цикл — проверяет расписание каждую минуту"""
    while True:
        try:
            await _check_schedules()
        except Exception as e:
            log.error(f"schedule_loop ошибка: {e}")
        await asyncio.sleep(60)


async def _check_schedules():
    if not _bot:
        return

    conn = db()
    chats = conn.execute("SELECT cid, settings FROM chat_settings").fetchall()
    conn.close()

    now = datetime.now()
    current_time = now.strftime("%H:%M")

    for row in chats:
        try:
            settings = json.loads(row["settings"]) if row["settings"] else {}

            # Расписание открытия/закрытия
            if settings.get("schedule_enabled"):
                close_time = settings.get("close_time", "00:00")
                open_time  = settings.get("open_time", "08:00")

                if current_time == close_time:
                    try:
                        from aiogram.types import ChatPermissions
                        await _bot.set_chat_permissions(
                            row["cid"],
                            ChatPermissions(can_send_messages=False)
                        )
                        await _bot.send_message(
                            row["cid"],
                            f"🔒 <b>Чат закрыт</b>\n"
                            f"⏰ Откроется в {open_time}",
                            parse_mode="HTML"
                        )
                    except:
                        pass

                elif current_time == open_time:
                    try:
                        from aiogram.types import ChatPermissions
                        await _bot.set_chat_permissions(
                            row["cid"],
                            ChatPermissions(
                                can_send_messages=True,
                                can_send_media_messages=True,
                                can_send_polls=True,
                                can_send_other_messages=True,
                                can_add_web_page_previews=True
                            )
                        )
                        await _bot.send_message(
                            row["cid"],
                            f"🔓 <b>Чат открыт!</b>\n"
                            f"🌅 Доброе утро!",
                            parse_mode="HTML"
                        )
                    except:
                        pass

            # Тихий час
            if settings.get("quiet_enabled"):
                quiet_start = settings.get("quiet_start", "23:00")
                quiet_end   = settings.get("quiet_end", "07:00")

                if current_time == quiet_start:
                    try:
                        from aiogram.types import ChatPermissions
                        await _bot.set_chat_permissions(
                            row["cid"],
                            ChatPermissions(can_send_messages=False)
                        )
                        await _bot.send_message(
                            row["cid"],
                            f"🌙 <b>Тихий час</b>\n"
                            f"Чат в режиме только чтение до {quiet_end}",
                            parse_mode="HTML"
                        )
                    except:
                        pass
                elif current_time == quiet_end:
                    try:
                        from aiogram.types import ChatPermissions
                        await _bot.set_chat_permissions(
                            row["cid"],
                            ChatPermissions(
                                can_send_messages=True,
                                can_send_media_messages=True,
                                can_send_polls=True,
                                can_send_other_messages=True
                            )
                        )
                        await _bot.send_message(
                            row["cid"],
                            f"☀️ <b>Тихий час закончился!</b>",
                            parse_mode="HTML"
                        )
                    except:
                        pass

        except:
            pass


# ══════════════════════════════════════════
#  СЧЁТЧИК СООБЩЕНИЙ (для авто-объявлений)
# ══════════════════════════════════════════

_msg_counters: dict = {}  # {cid: count}


async def on_message(cid: int):
    """Вызывается при каждом сообщении в чате"""
    if not _bot:
        return

    _msg_counters[cid] = _msg_counters.get(cid, 0) + 1
    count = _msg_counters[cid]

    settings = get_settings(cid)

    # Авто-объявление
    if settings.get("announce_enabled") and settings.get("announce_text"):
        interval = settings.get("announce_interval", 100)
        if count % interval == 0:
            try:
                await _bot.send_message(
                    cid,
                    f"📢 {settings['announce_text']}",
                    parse_mode="HTML"
                )
            except:
                pass

    # Напоминание правил
    if settings.get("rules_remind_enabled"):
        interval2 = settings.get("rules_remind_interval", 200)
        if count % interval2 == 0:
            try:
                await _bot.send_message(
                    cid,
                    "📜 <b>Напоминание:</b> соблюдайте правила чата! /rules",
                    parse_mode="HTML"
                )
            except:
                pass

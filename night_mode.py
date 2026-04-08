# -*- coding: utf-8 -*-
"""
night_mode.py — Автоматический ночной режим
Настраивается через дашборд, применяется по расписанию.

Подключение в bot.py:
    import night_mode
    # В main(): await night_mode.init(bot)
    # В StatsMiddleware / обработчике сообщений:
    #   await night_mode.filter_message(message)
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot
from aiogram.types import Message, ChatPermissions

log = logging.getLogger(__name__)

_bot: Optional[Bot] = None

# ══════════════════════════════════════════
#  НАСТРОЙКИ ПО УМОЛЧАНИЮ
# ══════════════════════════════════════════

DEFAULT_CFG = {
    # Включён ли ночной режим
    "enabled":              False,
    # Время начала (HH:MM)
    "start_time":           "23:00",
    # Время конца (HH:MM)
    "end_time":             "07:00",
    # Только-чтение (полный запрет на сообщения)
    "read_only":            False,
    # Запретить медиа (фото, видео, гифки)
    "block_media":          True,
    # Запретить стикеры
    "block_stickers":       True,
    # Запретить голосовые и кружки
    "block_voice":          True,
    # Запретить файлы
    "block_files":          False,
    # Запретить форварды
    "block_forwards":       True,
    # Слоумод (секунд, 0 = выкл)
    "slowmode":             30,
    # Уведомить чат при включении/выключении
    "notify":               True,
    # Текст уведомления при включении
    "msg_start":            "🌙 <b>Ночной режим</b> — с {start} до {end}\nМедиа и стикеры ограничены.",
    # Текст уведомления при выключении
    "msg_end":              "☀️ <b>Доброе утро!</b> Ночной режим снят.",
    # Удалять нарушения автоматически
    "delete_violations":    True,
    # Отправлять предупреждение нарушителю
    "warn_violators":       False,
}

# Состояние: {cid: {"active": bool, "activated_at": float}}
_state: dict = {}

# Конфиги: {cid: dict}
_cfg_cache: dict = {}


def _db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_tables():
    conn = _db()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS night_mode_cfg (
        cid     INTEGER PRIMARY KEY,
        data    TEXT DEFAULT '{}'
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS night_mode_log (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        cid     INTEGER,
        event   TEXT,
        ts      INTEGER DEFAULT (strftime('%s','now'))
    )
    """)
    conn.commit()
    conn.close()


def get_cfg(cid: int) -> dict:
    if cid in _cfg_cache:
        return _cfg_cache[cid]
    conn = _db()
    row = conn.execute("SELECT data FROM night_mode_cfg WHERE cid=?", (cid,)).fetchone()
    conn.close()
    cfg = dict(DEFAULT_CFG)
    if row:
        try:
            cfg.update(json.loads(row["data"]))
        except:
            pass
    _cfg_cache[cid] = cfg
    return cfg


def save_cfg(cid: int, cfg: dict):
    _cfg_cache[cid] = cfg
    conn = _db()
    conn.execute(
        "INSERT INTO night_mode_cfg (cid, data) VALUES (?,?) "
        "ON CONFLICT(cid) DO UPDATE SET data=?",
        (cid, json.dumps(cfg, ensure_ascii=False),
         json.dumps(cfg, ensure_ascii=False))
    )
    conn.commit()
    conn.close()


async def init(bot: Bot):
    global _bot
    _bot = bot
    _init_tables()
    asyncio.create_task(_night_mode_loop())
    log.info("✅ night_mode.py инициализирован")


# ══════════════════════════════════════════
#  ОСНОВНОЙ ЦИКл — проверка каждую минуту
# ══════════════════════════════════════════

async def _night_mode_loop():
    while True:
        try:
            await _check_all_chats()
        except Exception as e:
            log.error(f"night_mode_loop ошибка: {e}")
        await asyncio.sleep(60)


async def _check_all_chats():
    conn = _db()
    rows = conn.execute("SELECT cid, data FROM night_mode_cfg").fetchall()
    conn.close()

    now = datetime.now()
    current_time = now.strftime("%H:%M")

    for row in rows:
        try:
            cfg = dict(DEFAULT_CFG)
            cfg.update(json.loads(row["data"] or "{}"))
            cid = row["cid"]

            if not cfg.get("enabled"):
                continue

            start = cfg.get("start_time", "23:00")
            end   = cfg.get("end_time",   "07:00")
            is_currently_night = _is_night_time(current_time, start, end)
            was_active = _state.get(cid, {}).get("active", False)

            if is_currently_night and not was_active:
                await _activate(cid, cfg)
            elif not is_currently_night and was_active:
                await _deactivate(cid, cfg)

        except Exception as e:
            log.error(f"Ошибка night_mode для {row['cid']}: {e}")


def _is_night_time(current: str, start: str, end: str) -> bool:
    """Проверяет, является ли текущее время ночным (с учётом перехода через полночь)"""
    def to_min(t):
        h, m = map(int, t.split(":"))
        return h * 60 + m

    c = to_min(current)
    s = to_min(start)
    e = to_min(end)

    if s > e:
        # Ночной период переходит через полночь (напр. 23:00 → 07:00)
        return c >= s or c < e
    else:
        return s <= c < e


async def _activate(cid: int, cfg: dict):
    """Включает ночной режим"""
    _state[cid] = {"active": True, "activated_at": time.time()}

    perms = _build_perms(cfg, night_active=True)
    try:
        await _bot.set_chat_permissions(cid, perms)
    except Exception as e:
        log.warning(f"Не смог установить права ночного режима в {cid}: {e}")

    if cfg.get("slowmode", 0) > 0:
        try:
            await _bot.set_chat_slow_mode_delay(cid, cfg["slowmode"])
        except:
            pass

    if cfg.get("notify", True):
        start = cfg.get("start_time", "23:00")
        end   = cfg.get("end_time",   "07:00")
        text  = cfg.get("msg_start", DEFAULT_CFG["msg_start"])
        text  = text.replace("{start}", start).replace("{end}", end)
        try:
            await _bot.send_message(cid, text, parse_mode="HTML")
        except:
            pass

    # Лог
    conn = _db()
    conn.execute("INSERT INTO night_mode_log (cid, event) VALUES (?,?)", (cid, "activate"))
    conn.commit()
    conn.close()
    log.info(f"🌙 Ночной режим ВКЛЮЧЁН в чате {cid}")


async def _deactivate(cid: int, cfg: dict):
    """Выключает ночной режим"""
    _state[cid] = {"active": False, "activated_at": None}

    # Восстанавливаем полные права
    full_perms = ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_change_info=False,
        can_invite_users=True,
        can_pin_messages=False,
    )
    try:
        await _bot.set_chat_permissions(cid, full_perms)
    except Exception as e:
        log.warning(f"Не смог снять права в {cid}: {e}")

    # Убираем слоумод
    try:
        await _bot.set_chat_slow_mode_delay(cid, 0)
    except:
        pass

    if cfg.get("notify", True):
        text = cfg.get("msg_end", DEFAULT_CFG["msg_end"])
        try:
            await _bot.send_message(cid, text, parse_mode="HTML")
        except:
            pass

    conn = _db()
    conn.execute("INSERT INTO night_mode_log (cid, event) VALUES (?,?)", (cid, "deactivate"))
    conn.commit()
    conn.close()
    log.info(f"☀️ Ночной режим ВЫКЛЮЧЕН в чате {cid}")


def _build_perms(cfg: dict, night_active: bool) -> ChatPermissions:
    """Строит объект прав в зависимости от настроек ночного режима"""
    if not night_active:
        return ChatPermissions(
            can_send_messages=True,
            can_send_media_messages=True,
            can_send_polls=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True,
        )

    if cfg.get("read_only"):
        return ChatPermissions(can_send_messages=False)

    block_media    = cfg.get("block_media", True)
    block_stickers = cfg.get("block_stickers", True)
    block_voice    = cfg.get("block_voice", True)

    return ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=not block_media,
        can_send_polls=True,
        can_send_other_messages=not block_stickers,
        can_add_web_page_previews=not block_media,
    )


# ══════════════════════════════════════════
#  ФИЛЬТРАЦИЯ СООБЩЕНИЙ В РЕАЛЬНОМ ВРЕМЕНИ
# ══════════════════════════════════════════

async def filter_message(message: Message) -> bool:
    """
    Проверяет сообщение на нарушение ночного режима.
    Возвращает True если сообщение нарушает правила и было удалено.
    Вызывать из обработчика сообщений.
    """
    if not message.from_user:
        return False
    cid = message.chat.id
    if not _state.get(cid, {}).get("active", False):
        return False

    cfg = get_cfg(cid)
    if not cfg.get("enabled"):
        return False

    violated = False
    reason = ""

    # Проверяем тип контента
    if cfg.get("block_media") and (message.photo or message.video or message.animation):
        violated = True
        reason = "медиа"
    elif cfg.get("block_stickers") and (message.sticker or message.video_sticker):
        violated = True
        reason = "стикеры"
    elif cfg.get("block_voice") and (message.voice or message.video_note or message.audio):
        violated = True
        reason = "голосовые"
    elif cfg.get("block_files") and message.document:
        violated = True
        reason = "файлы"
    elif cfg.get("block_forwards") and message.forward_from:
        violated = True
        reason = "форварды"

    if violated and cfg.get("delete_violations"):
        try:
            await message.delete()
        except:
            pass

        if cfg.get("warn_violators"):
            try:
                sent = await message.answer(
                    f"🌙 <b>Ночной режим</b>\n"
                    f"{message.from_user.full_name}, {reason} ограничены до утра.",
                    parse_mode="HTML"
                )
                asyncio.create_task(_auto_delete(sent, 10))
            except:
                pass
        return True

    return False


async def _auto_delete(msg, delay: int):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass


# ══════════════════════════════════════════
#  РУЧНОЕ УПРАВЛЕНИЕ
# ══════════════════════════════════════════

async def force_activate(cid: int):
    """Принудительно включить ночной режим"""
    cfg = get_cfg(cid)
    await _activate(cid, cfg)


async def force_deactivate(cid: int):
    """Принудительно выключить ночной режим"""
    cfg = get_cfg(cid)
    await _deactivate(cid, cfg)


def is_active(cid: int) -> bool:
    return _state.get(cid, {}).get("active", False)


# ══════════════════════════════════════════
#  API ДЛЯ ДАШБОРДА
# ══════════════════════════════════════════

def get_all_configs() -> list:
    """Все конфиги ночного режима для дашборда"""
    conn = _db()
    rows = conn.execute("SELECT cid, data FROM night_mode_cfg").fetchall()
    conn.close()
    result = []
    for r in rows:
        try:
            cfg = dict(DEFAULT_CFG)
            cfg.update(json.loads(r["data"] or "{}"))
            cfg["cid"] = r["cid"]
            cfg["is_active"] = _state.get(r["cid"], {}).get("active", False)
            result.append(cfg)
        except:
            pass
    return result


def get_chat_config(cid: int) -> dict:
    cfg = get_cfg(cid)
    cfg["cid"] = cid
    cfg["is_active"] = is_active(cid)
    return cfg


def update_config(cid: int, data: dict):
    cfg = get_cfg(cid)
    # Булевы поля
    bool_fields = ["enabled", "read_only", "block_media", "block_stickers",
                   "block_voice", "block_files", "block_forwards",
                   "notify", "delete_violations", "warn_violators"]
    for k in bool_fields:
        if k in data:
            cfg[k] = bool(data[k])
    # Строки
    for k in ["start_time", "end_time", "msg_start", "msg_end"]:
        if k in data and data[k]:
            cfg[k] = str(data[k])
    # Числа
    if "slowmode" in data:
        try:
            cfg["slowmode"] = int(data["slowmode"])
        except:
            pass
    save_cfg(cid, cfg)
    return cfg


def get_log(cid: int, limit: int = 20) -> list:
    conn = _db()
    rows = conn.execute(
        "SELECT event, ts FROM night_mode_log WHERE cid=? ORDER BY ts DESC LIMIT ?",
        (cid, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

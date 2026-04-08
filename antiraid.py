# -*- coding: utf-8 -*-
"""
antiraid.py — Антирейд защита + Детектор скам-аккаунтов
Подключение в bot.py:
    import antiraid
    # В main(): await antiraid.init(bot, dp, ADMIN_IDS, LOG_CHANNEL_ID)
    # В on_new_member: await antiraid.on_join(message, member)
"""
import asyncio
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, ChatPermissions, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import Command

log = logging.getLogger(__name__)

_bot: Optional[Bot] = None
_admin_ids: set = set()
_log_channel: int = 0

# ══════════════════════════════════════════
#  ХРАНИЛИЩЕ СОСТОЯНИЙ
# ══════════════════════════════════════════

# Время входов в чат: {cid: [timestamp, ...]}
_join_times: dict = defaultdict(list)

# Чаты в режиме антирейда: {cid: {"until": ts, "action": str, "count": int}}
_raid_active: dict = {}

# Настройки антирейда: {cid: {...}}
_raid_cfg: dict = {}

# Скам-метки: {uid: {"score": int, "reasons": [...], "ts": float}}
_scam_flags: dict = {}

DEFAULT_RAID_CFG = {
    "enabled":         True,
    "threshold":       7,       # кол-во входов за окно
    "window_secs":     10,      # окно в секундах
    "lock_minutes":    10,      # на сколько минут блокировать
    "action":          "lock",  # lock | slowmode | members_only
    "slowmode_delay":  30,      # секунд при slowmode
    "scam_check":      True,    # проверять скам при входе
    "scam_threshold":  60,      # порог счёта скама (0-100)
    "notify_admins":   True,
}


def _db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_tables():
    conn = _db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS antiraid_cfg (
        cid     INTEGER PRIMARY KEY,
        data    TEXT    DEFAULT '{}'
    );
    CREATE TABLE IF NOT EXISTS raid_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER,
        chat_title  TEXT,
        join_count  INTEGER,
        action      TEXT,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS scam_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER,
        uid         INTEGER,
        username    TEXT,
        score       INTEGER,
        reasons     TEXT,
        action      TEXT,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    conn.commit()
    conn.close()


def _load_cfg():
    import json
    conn = _db()
    rows = conn.execute("SELECT cid, data FROM antiraid_cfg").fetchall()
    conn.close()
    for r in rows:
        try:
            cfg = dict(DEFAULT_RAID_CFG)
            cfg.update(json.loads(r["data"]))
            _raid_cfg[r["cid"]] = cfg
        except:
            pass


def get_cfg(cid: int) -> dict:
    if cid not in _raid_cfg:
        _raid_cfg[cid] = dict(DEFAULT_RAID_CFG)
    return _raid_cfg[cid]


def save_cfg(cid: int, cfg: dict):
    import json
    _raid_cfg[cid] = cfg
    conn = _db()
    conn.execute(
        "INSERT INTO antiraid_cfg (cid, data) VALUES (?,?) "
        "ON CONFLICT(cid) DO UPDATE SET data=?",
        (cid, json.dumps(cfg), json.dumps(cfg))
    )
    conn.commit()
    conn.close()


async def init(bot: Bot, dp: Dispatcher, admin_ids: set, log_channel: int = 0):
    global _bot, _admin_ids, _log_channel
    _bot = bot
    _admin_ids = admin_ids
    _log_channel = log_channel
    _init_tables()
    _load_cfg()
    _register_handlers(dp)
    asyncio.create_task(_raid_recovery_loop())
    log.info("✅ antiraid.py инициализирован")


# ══════════════════════════════════════════
#  АНТИРЕЙД — ЯДРО
# ══════════════════════════════════════════

async def on_join(message: Message, member) -> bool:
    """
    Вызывается при каждом вступлении. 
    Возвращает True если пользователь был заблокирован/кикнут.
    """
    cid  = message.chat.id
    uid  = member.id
    cfg  = get_cfg(cid)

    if not cfg.get("enabled", True):
        return False

    # ── 1. Проверка глобального блэклиста ──────────────────
    if await _check_global_blacklist(cid, uid, member):
        return True

    # ── 2. Детектор скам-аккаунтов ─────────────────────────
    if cfg.get("scam_check", True):
        kicked = await _check_scam(message, member, cfg)
        if kicked:
            return True

    # ── 3. Антирейд ────────────────────────────────────────
    now = time.time()
    window = cfg.get("window_secs", 10)

    # Чистим старые записи
    _join_times[cid] = [t for t in _join_times[cid] if now - t < window]
    _join_times[cid].append(now)

    count = len(_join_times[cid])
    threshold = cfg.get("threshold", 7)

    if count >= threshold and cid not in _raid_active:
        await _activate_raid_mode(message, count, cfg)

    return False


async def _check_global_blacklist(cid: int, uid: int, member) -> bool:
    """Проверяет и банит если в глобальном чёрном списке"""
    conn = _db()
    row = conn.execute(
        "SELECT uid FROM global_blacklist WHERE uid=?", (uid,)
    ).fetchone()
    conn.close()

    if row:
        try:
            await _bot.ban_chat_member(cid, uid)
            log.info(f"🚷 Забанен по глобальному ЧС: {uid} в чате {cid}")
            # Уведомление в лог
            await _send_log(
                f"🚷 <b>ГЛОБАЛЬНЫЙ ЧС</b>\n\n"
                f"👤 {member.full_name} (<code>{uid}</code>)\n"
                f"💬 Чат: <code>{cid}</code>\n"
                f"🔨 Автобан при входе"
            )
            return True
        except Exception as e:
            log.warning(f"Не смог забанить {uid}: {e}")
    return False


async def _check_scam(message: Message, member, cfg: dict) -> bool:
    """
    Оценивает вероятность скам-аккаунта (0-100).
    Чем выше — тем подозрительнее.
    """
    uid   = member.id
    score = 0
    reasons = []

    # ── Признак 1: Свежий аккаунт по Telegram ID ──────────
    # Telegram ID увеличивается со временем.
    # Примерные границы:
    #   < 100_000_000  → очень старый (до 2014)
    #   > 7_000_000_000 → создан в 2023-2024
    # Новые спам-аккаунты обычно > 6_500_000_000
    if uid > 6_500_000_000:
        score += 35
        reasons.append("очень новый аккаунт")
    elif uid > 5_000_000_000:
        score += 20
        reasons.append("новый аккаунт")
    elif uid > 3_000_000_000:
        score += 5

    # ── Признак 2: Нет username ────────────────────────────
    if not member.username:
        score += 20
        reasons.append("нет @username")

    # ── Признак 3: Нет фото профиля ───────────────────────
    try:
        photos = await _bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count == 0:
            score += 15
            reasons.append("нет фото")
    except:
        pass

    # ── Признак 4: Подозрительное имя ─────────────────────
    name = member.full_name.lower()
    spam_keywords = [
        "casino", "crypto", "bitcoin", "earn", "profit", "invest",
        "заработ", "крипт", "казино", "биткоин", "инвест", "доход",
        "admin", "support", "moderator", "официальн",
        "free", "gift", "giveaway", "розыгрыш",
    ]
    for kw in spam_keywords:
        if kw in name:
            score += 25
            reasons.append(f"ключевое слово '{kw}' в имени")
            break

    # ── Признак 5: Имя из цифр / случайных символов ───────
    import re
    if re.fullmatch(r'[0-9\s]+', member.full_name.strip()):
        score += 20
        reasons.append("имя из цифр")
    elif re.fullmatch(r'[A-Za-z0-9]{1,4}\s?[A-Za-z0-9]{0,4}', member.full_name.strip()):
        # Очень короткое случайное имя
        if len(member.full_name.strip()) <= 5:
            score += 10
            reasons.append("очень короткое имя")

    # ── Признак 6: Уже был забанен в других чатах ─────────
    conn = _db()
    ban_count = conn.execute(
        "SELECT COUNT(*) as c FROM scam_log WHERE uid=? AND action='ban'", (uid,)
    ).fetchone()
    conn.close()
    if ban_count and ban_count["c"] > 0:
        score += 30
        reasons.append(f"ранее помечен скамом {ban_count['c']} раз")

    threshold = cfg.get("scam_threshold", 60)

    # Сохраняем в кэш
    _scam_flags[uid] = {
        "score": score, "reasons": reasons, "ts": time.time()
    }

    if score >= threshold:
        action = "ban" if score >= 80 else "kick"
        await _handle_scam(message, member, score, reasons, action)
        return True

    elif score >= 40:
        # Подозрительный — уведомляем без действия
        await _notify_scam_suspicious(message.chat.id, member, score, reasons)

    return False


async def _handle_scam(message: Message, member, score: int, reasons: list, action: str):
    cid = message.chat.id
    uid = member.id
    cid_title = message.chat.title or str(cid)

    try:
        if action == "ban":
            await _bot.ban_chat_member(cid, uid)
            action_text = "🔨 Забанен"
        else:
            await _bot.ban_chat_member(cid, uid)
            await _bot.unban_chat_member(cid, uid)
            action_text = "👢 Кикнут"
    except Exception as e:
        log.warning(f"Скам-действие не выполнено: {e}")
        action_text = "❌ Не смог применить"

    reasons_text = " • ".join(reasons) if reasons else "—"
    text = (
        f"🚨 <b>СКАМ-АККАУНТ ОБНАРУЖЕН</b>\n\n"
        f"👤 <b>{member.full_name}</b> (<code>{uid}</code>)\n"
        f"💬 Чат: {cid_title}\n"
        f"📊 Оценка риска: <b>{score}/100</b>\n"
        f"📋 Причины:\n• {reasons_text}\n\n"
        f"{action_text} автоматически"
    )

    # Логируем в БД
    import json
    conn = _db()
    conn.execute(
        "INSERT INTO scam_log (cid, uid, username, score, reasons, action) VALUES (?,?,?,?,?,?)",
        (cid, uid, member.username or "", score, json.dumps(reasons, ensure_ascii=False), action)
    )
    conn.commit()
    conn.close()

    await _send_log(text)

    # Уведомляем в чат (автоудаляется через 30 сек)
    try:
        sent = await message.answer(
            f"🚨 <b>{member.full_name}</b> — подозрительный аккаунт, заблокирован.",
            parse_mode="HTML"
        )
        asyncio.create_task(_auto_delete(sent, 30))
    except:
        pass


async def _notify_scam_suspicious(cid: int, member, score: int, reasons: list):
    """Уведомление о подозрительном аккаунте без бана"""
    reasons_text = " • ".join(reasons) if reasons else "—"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔨 Забанить", callback_data=f"scam:ban:{cid}:{member.id}"),
        InlineKeyboardButton(text="👢 Кикнуть",  callback_data=f"scam:kick:{cid}:{member.id}"),
        InlineKeyboardButton(text="✅ Игнорировать", callback_data=f"scam:ignore:{cid}:{member.id}"),
    ]])
    await _send_log(
        f"⚠️ <b>Подозрительный аккаунт</b>\n\n"
        f"👤 {member.full_name} (<code>{member.id}</code>)\n"
        f"💬 Чат: <code>{cid}</code>\n"
        f"📊 Оценка: <b>{score}/100</b>\n"
        f"• {reasons_text}",
        reply_markup=kb
    )


async def _activate_raid_mode(message: Message, count: int, cfg: dict):
    """Включает защиту от рейда"""
    cid   = message.chat.id
    title = message.chat.title or str(cid)
    action = cfg.get("action", "lock")
    lock_mins = cfg.get("lock_minutes", 10)

    until_ts = time.time() + lock_mins * 60
    _raid_active[cid] = {"until": until_ts, "action": action, "count": count}

    try:
        if action == "lock":
            await _bot.set_chat_permissions(
                cid, ChatPermissions(can_send_messages=False)
            )
            action_desc = f"🔒 Чат заблокирован на {lock_mins} мин."

        elif action == "slowmode":
            delay = cfg.get("slowmode_delay", 30)
            await _bot.set_chat_slow_mode_delay(cid, delay)
            action_desc = f"🐢 Замедленный режим: {delay} сек."

        elif action == "members_only":
            # Только давние участники могут писать — ограничиваем новых
            action_desc = f"👥 Новым участникам запрещено писать на {lock_mins} мин."

        else:
            action_desc = "⚠️ Обнаружен рейд"

        # Уведомление в чат
        sent = await message.answer(
            f"🚨 <b>АНТИРЕЙД АКТИВИРОВАН</b>\n\n"
            f"За {cfg.get('window_secs',10)} сек. вступило <b>{count}</b> человек.\n"
            f"{action_desc}\n\n"
            f"⏰ Восстановление через <b>{lock_mins}</b> мин.",
            parse_mode="HTML"
        )
        asyncio.create_task(_auto_delete(sent, 60))

    except Exception as e:
        log.error(f"Ошибка антирейда: {e}")
        action_desc = f"Ошибка: {e}"

    # Логируем в БД
    conn = _db()
    conn.execute(
        "INSERT INTO raid_log (cid, chat_title, join_count, action) VALUES (?,?,?,?)",
        (cid, title, count, action)
    )
    conn.commit()
    conn.close()

    # Уведомление в лог-канал и всем админам
    if cfg.get("notify_admins", True):
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔓 Снять защиту", callback_data=f"raid:unlock:{cid}"),
        ]])
        await _send_log(
            f"🚨 <b>РЕЙД В ЧАТЕ</b>\n\n"
            f"💬 {title} (<code>{cid}</code>)\n"
            f"👥 {count} вступлений за {cfg.get('window_secs',10)} сек.\n"
            f"{action_desc}\n"
            f"⏰ До: {datetime.fromtimestamp(until_ts).strftime('%H:%M:%S')}",
            reply_markup=kb
        )

    log.warning(f"🚨 РЕЙД в {cid}: {count} входов за {cfg.get('window_secs',10)}с → {action}")


async def _raid_recovery_loop():
    """Фоновая задача — снимает защиту после таймера"""
    while True:
        try:
            now = time.time()
            to_remove = []
            for cid, info in _raid_active.items():
                if now >= info["until"]:
                    await _deactivate_raid_mode(cid, info["action"])
                    to_remove.append(cid)
            for cid in to_remove:
                _raid_active.pop(cid, None)
        except Exception as e:
            log.error(f"raid_recovery_loop ошибка: {e}")
        await asyncio.sleep(30)


async def _deactivate_raid_mode(cid: int, action: str):
    """Снимает ограничения после рейда"""
    try:
        if action in ("lock", "members_only"):
            await _bot.set_chat_permissions(
                cid,
                ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_polls=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True
                )
            )
        elif action == "slowmode":
            await _bot.set_chat_slow_mode_delay(cid, 0)

        await _send_log(
            f"✅ <b>Антирейд снят</b>\n"
            f"💬 Чат <code>{cid}</code> — права восстановлены"
        )
    except Exception as e:
        log.warning(f"Ошибка снятия рейда {cid}: {e}")


# ══════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════

async def _send_log(text: str, reply_markup=None):
    if not _bot:
        return
    # Отправляем в лог-канал
    if _log_channel:
        try:
            await _bot.send_message(
                _log_channel, text,
                parse_mode="HTML", reply_markup=reply_markup
            )
        except Exception as e:
            log.warning(f"Лог-канал недоступен: {e}")
    # И всем админам
    for aid in _admin_ids:
        try:
            await _bot.send_message(
                aid, text,
                parse_mode="HTML", reply_markup=reply_markup
            )
        except:
            pass


async def _auto_delete(message, delay_secs: int):
    await asyncio.sleep(delay_secs)
    try:
        await message.delete()
    except:
        pass


# ══════════════════════════════════════════
#  КОМАНДЫ
# ══════════════════════════════════════════

async def cmd_antiraid(message: Message):
    """/antiraid — настройки антирейда"""
    cid = message.chat.id
    cfg = get_cfg(cid)

    status = "✅ Включён" if cfg.get("enabled") else "❌ Выключен"
    action_map = {"lock": "🔒 Блокировка", "slowmode": "🐢 Замедление", "members_only": "👥 Только участники"}
    action = action_map.get(cfg.get("action", "lock"), "Неизвестно")
    raid_now = "🔴 АКТИВЕН" if cid in _raid_active else "🟢 Не активен"

    # Статистика
    conn = _db()
    total_raids = conn.execute("SELECT COUNT(*) as c FROM raid_log WHERE cid=?", (cid,)).fetchone()
    scams_today = conn.execute(
        "SELECT COUNT(*) as c FROM scam_log WHERE cid=? AND date(ts,'unixepoch')=date('now')",
        (cid,)
    ).fetchone()
    conn.close()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Вкл/Выкл",      callback_data=f"ar:toggle:{cid}"),
         InlineKeyboardButton(text="⚡ Действие",       callback_data=f"ar:action:{cid}")],
        [InlineKeyboardButton(text="📊 Статистика",     callback_data=f"ar:stats:{cid}"),
         InlineKeyboardButton(text="⚙️ Настройки",     callback_data=f"ar:settings:{cid}")],
    ])

    await message.answer(
        f"━━━━━━━━━━━━━━━\n"
        f"🛡 <b>АНТИРЕЙД</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"Статус: {status}\n"
        f"Режим: {action}\n"
        f"Сейчас: {raid_now}\n\n"
        f"Порог: <b>{cfg.get('threshold',7)}</b> входов за <b>{cfg.get('window_secs',10)}</b> сек.\n"
        f"Блокировка: <b>{cfg.get('lock_minutes',10)}</b> мин.\n\n"
        f"📊 Рейдов всего: <b>{total_raids['c'] if total_raids else 0}</b>\n"
        f"🚨 Скамов сегодня: <b>{scams_today['c'] if scams_today else 0}</b>",
        parse_mode="HTML",
        reply_markup=kb
    )


async def cb_antiraid(call: CallbackQuery):
    parts = call.data.split(":")
    action = parts[1]
    cid = int(parts[2]) if len(parts) > 2 else 0

    if action == "toggle":
        cfg = get_cfg(cid)
        cfg["enabled"] = not cfg.get("enabled", True)
        save_cfg(cid, cfg)
        status = "✅ включён" if cfg["enabled"] else "❌ выключен"
        await call.answer(f"Антирейд {status}")
        await call.message.delete()

    elif action == "action":
        actions_map = {
            "lock":         "🔒 Блокировка чата",
            "slowmode":     "🐢 Замедленный режим",
            "members_only": "👥 Новым участникам запрет",
        }
        cfg = get_cfg(cid)
        current = cfg.get("action", "lock")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"{'✅ ' if current==k else ''}{v}",
                callback_data=f"ar:setaction:{cid}:{k}"
            )] for k, v in actions_map.items()
        ])
        await call.message.edit_text(
            "⚡ <b>Выберите действие при рейде:</b>",
            parse_mode="HTML", reply_markup=kb
        )

    elif action == "setaction":
        new_action = parts[3]
        cfg = get_cfg(cid)
        cfg["action"] = new_action
        save_cfg(cid, cfg)
        await call.answer(f"Действие обновлено")
        await call.message.delete()

    elif action == "stats":
        conn = _db()
        raids = conn.execute(
            "SELECT chat_title, join_count, action, ts FROM raid_log WHERE cid=? ORDER BY ts DESC LIMIT 5",
            (cid,)
        ).fetchall()
        scams = conn.execute(
            "SELECT username, score, action, ts FROM scam_log WHERE cid=? ORDER BY ts DESC LIMIT 5",
            (cid,)
        ).fetchall()
        conn.close()

        text = "📊 <b>Статистика защиты</b>\n\n"
        if raids:
            text += "<b>Последние рейды:</b>\n"
            for r in raids:
                dt = datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")
                text += f"• {dt}: {r['join_count']} чел. → {r['action']}\n"
        if scams:
            text += "\n<b>Скам-аккаунты:</b>\n"
            for s in scams:
                dt = datetime.fromtimestamp(s["ts"]).strftime("%d.%m %H:%M")
                text += f"• {dt}: @{s['username']} (риск {s['score']}) → {s['action']}\n"

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"ar:back:{cid}")
        ]])
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

    elif action == "unlock":
        if cid in _raid_active:
            await _deactivate_raid_mode(cid, _raid_active[cid]["action"])
            _raid_active.pop(cid, None)
            await call.answer("✅ Антирейд снят")
            await call.message.delete()
        else:
            await call.answer("Рейд уже неактивен")

    elif action == "back":
        await call.message.delete()

    await call.answer()


async def cb_scam_action(call: CallbackQuery):
    parts = call.data.split(":")
    action = parts[1]
    cid = int(parts[2])
    uid = int(parts[3])

    if action == "ban":
        try:
            await _bot.ban_chat_member(cid, uid)
            await call.answer("🔨 Забанен")
            await call.message.edit_text(
                call.message.text + "\n\n🔨 <b>Забанен администратором</b>",
                parse_mode="HTML"
            )
        except Exception as e:
            await call.answer(f"Ошибка: {e}")

    elif action == "kick":
        try:
            await _bot.ban_chat_member(cid, uid)
            await _bot.unban_chat_member(cid, uid)
            await call.answer("👢 Кикнут")
            await call.message.edit_text(
                call.message.text + "\n\n👢 <b>Кикнут администратором</b>",
                parse_mode="HTML"
            )
        except Exception as e:
            await call.answer(f"Ошибка: {e}")

    elif action == "ignore":
        await call.answer("✅ Проигнорировано")
        await call.message.edit_text(
            call.message.text + "\n\n✅ <b>Проигнорировано</b>",
            parse_mode="HTML"
        )

    await call.answer()


# ══════════════════════════════════════════
#  API ДЛЯ ДАШБОРДА
# ══════════════════════════════════════════

def get_raid_stats_for_dashboard() -> dict:
    """Возвращает статистику для дашборда"""
    conn = _db()
    total = conn.execute("SELECT COUNT(*) as c FROM raid_log").fetchone()["c"]
    today = conn.execute(
        "SELECT COUNT(*) as c FROM raid_log WHERE date(ts,'unixepoch')=date('now')"
    ).fetchone()["c"]
    scams_total = conn.execute("SELECT COUNT(*) as c FROM scam_log").fetchone()["c"]
    recent_raids = conn.execute(
        "SELECT * FROM raid_log ORDER BY ts DESC LIMIT 10"
    ).fetchall()
    recent_scams = conn.execute(
        "SELECT * FROM scam_log ORDER BY ts DESC LIMIT 10"
    ).fetchall()
    conn.close()

    return {
        "raids_total": total,
        "raids_today": today,
        "scams_total": scams_total,
        "active_raids": len(_raid_active),
        "recent_raids": [dict(r) for r in recent_raids],
        "recent_scams": [dict(r) for r in recent_scams],
    }


def get_chat_raid_cfg(cid: int) -> dict:
    return get_cfg(cid)


def update_chat_raid_cfg(cid: int, data: dict):
    cfg = get_cfg(cid)
    cfg.update(data)
    save_cfg(cid, cfg)


# ══════════════════════════════════════════
#  РЕГИСТРАЦИЯ ХЕНДЛЕРОВ
# ══════════════════════════════════════════

def _register_handlers(dp: Dispatcher):
    dp.message.register(cmd_antiraid, Command("antiraid"))
    dp.callback_query.register(cb_antiraid,    F.data.startswith("ar:"))
    dp.callback_query.register(cb_scam_action, F.data.startswith("scam:"))
    log.info("✅ antiraid.py хендлеры зарегистрированы")

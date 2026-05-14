# -*- coding: utf-8 -*-
"""
admin_guardian.py — 🛡️ Admin Guardian
═════════════════════════════════════════════════════════════════
Модуль защиты от взлома/злоупотребления админов:

  • 🚨 ДЕТЕКТОР АНОМАЛИЙ — алерт при 20+ банах/минуту, массовых
    киках, удалениях, опасных изменениях настроек и т.п.
  • ↩️ /undo @admin N — откат последних N действий админа (через
    кнопку подтверждения с показом списка действий)
  • 🧊 /freeze @admin — заморозка админа (с выбором: только команды
    бота / полное снятие админки в Telegram)
  • 🧊 /unfreeze @admin — разморозка
  • 📋 /frozen — список замороженных
  • 📜 /audit — последние audit-события

Все действия логируются в audit-канал (shared.log_channel_id).
Команды доступны ТОЛЬКО владельцу (shared.owner_id).

Подключение в bot.py:
    import admin_guardian as ag
    ...
    await ag.init(bot, dp)

В существующих командах ban/mute/warn/kick вызови:
    await ag.record_action(admin_id, "ban", target_id, cid,
                           reason="...", extra={...})
    await ag.check_rate_alert(admin_id, cid)

В check_admin / require_admin добавь проверку заморозки:
    if ag.is_frozen(admin_id):
        return False
"""

import asyncio
import json
import logging
import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
)

import shared

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════

DB_PATH = "admin_guardian.db"

# Пороги для детектора аномалий (действий/минуту)
THRESHOLDS = {
    "ban":    20,   # 20 банов за минуту → ALERT
    "kick":   15,
    "mute":   25,
    "warn":   30,
    "delete": 50,   # 50 удалений сообщений → ALERT
    "unban":  15,
}

# Окно усреднения (секунды)
WINDOW_SEC = 60

# Сколько действий хранить на админа для undo
MAX_HISTORY = 100

# TTL подтверждений undo (секунды)
CONFIRM_TTL = 120

# Cooldown между алертами на одного админа (секунды)
ALERT_COOLDOWN = 300

# Анти-спам: повторный алерт по тому же типу не чаще чем раз в N сек
_last_alert: Dict[str, float] = {}

# Pending undo подтверждения {token: {...}}
_pending_undo: Dict[str, dict] = {}

# Глобальные
_bot: Optional[Bot] = None


# ══════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════════

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_tables():
    conn = _db()
    c = conn.cursor()

    # История действий админов (для /undo)
    c.execute("""
        CREATE TABLE IF NOT EXISTS admin_actions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id    INTEGER NOT NULL,
            admin_name  TEXT,
            action      TEXT NOT NULL,        -- ban|kick|mute|warn|delete|unban|unmute
            target_id   INTEGER,
            target_name TEXT,
            cid         INTEGER,
            reason      TEXT,
            extra       TEXT,                 -- JSON с доп. данными (mute_minutes и т.п.)
            ts          REAL NOT NULL,
            undone      INTEGER DEFAULT 0     -- 1 если действие уже откачено
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_aa_admin_ts ON admin_actions(admin_id, ts DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_aa_ts ON admin_actions(ts DESC)")

    # Замороженные админы
    c.execute("""
        CREATE TABLE IF NOT EXISTS frozen_admins (
            admin_id    INTEGER PRIMARY KEY,
            admin_name  TEXT,
            mode        TEXT NOT NULL,         -- 'bot' (только команды бота) | 'tg' (снято в TG)
            frozen_by   INTEGER,
            reason      TEXT,
            ts          REAL NOT NULL,
            affected_chats TEXT                -- JSON list of cid для mode='tg' чтобы можно было restore
        )
    """)

    # Журнал аудита (все действия Guardian-а)
    c.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event       TEXT NOT NULL,
            actor_id    INTEGER,
            actor_name  TEXT,
            target_id   INTEGER,
            target_name TEXT,
            details     TEXT,                  -- JSON
            ts          REAL NOT NULL
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC)")

    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════
#  ПУБЛИЧНЫЙ API — вызывается из bot.py / других модулей
# ══════════════════════════════════════════════════════════════

async def record_action(
    admin_id: int,
    action: str,
    *,
    admin_name: str = "",
    target_id: int = 0,
    target_name: str = "",
    cid: int = 0,
    reason: str = "",
    extra: Optional[dict] = None,
):
    """
    Зарегистрировать действие админа. После этого его можно откатить
    через /undo. Также запускает проверку rate-аномалий.

    Пример:
        await ag.record_action(
            message.from_user.id, "ban",
            admin_name=message.from_user.full_name,
            target_id=target.id, target_name=target.full_name,
            cid=message.chat.id, reason=reason,
            extra={"chat_title": message.chat.title}
        )
    """
    try:
        conn = _db()
        conn.execute(
            """INSERT INTO admin_actions
               (admin_id, admin_name, action, target_id, target_name,
                cid, reason, extra, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (admin_id, admin_name, action, target_id, target_name,
             cid, reason, json.dumps(extra or {}, ensure_ascii=False), time.time())
        )
        # Чистим хвост > MAX_HISTORY на админа
        conn.execute("""
            DELETE FROM admin_actions
            WHERE id IN (
                SELECT id FROM admin_actions
                WHERE admin_id = ? AND undone = 0
                ORDER BY ts DESC
                LIMIT -1 OFFSET ?
            )
        """, (admin_id, MAX_HISTORY))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"record_action: {e}")
        return

    # Проверяем rate
    await check_rate_alert(admin_id, action, admin_name=admin_name, cid=cid)


def is_frozen(admin_id: int) -> bool:
    """Проверить, заморожен ли админ. Используй в require_admin/check_admin."""
    try:
        conn = _db()
        row = conn.execute(
            "SELECT 1 FROM frozen_admins WHERE admin_id=?", (admin_id,)
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


def get_freeze_info(admin_id: int) -> Optional[dict]:
    try:
        conn = _db()
        row = conn.execute(
            "SELECT * FROM frozen_admins WHERE admin_id=?", (admin_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  ДЕТЕКТОР АНОМАЛИЙ (RATE-BASED)
# ══════════════════════════════════════════════════════════════

async def check_rate_alert(admin_id: int, action: str, *,
                           admin_name: str = "", cid: int = 0):
    """Проверяет частоту действий и поднимает алерт если превышен порог."""
    threshold = THRESHOLDS.get(action)
    if not threshold:
        return

    cutoff = time.time() - WINDOW_SEC
    try:
        conn = _db()
        row = conn.execute(
            """SELECT COUNT(*) FROM admin_actions
               WHERE admin_id=? AND action=? AND ts >= ?""",
            (admin_id, action, cutoff)
        ).fetchone()
        conn.close()
        count = row[0] if row else 0
    except Exception as e:
        log.warning(f"check_rate: {e}")
        return

    if count < threshold:
        return

    # Cooldown
    key = f"{admin_id}:{action}"
    now = time.time()
    if now - _last_alert.get(key, 0) < ALERT_COOLDOWN:
        return
    _last_alert[key] = now

    await _raise_anomaly_alert(admin_id, admin_name, action, count, threshold, cid)


async def _raise_anomaly_alert(admin_id: int, admin_name: str,
                               action: str, count: int, threshold: int,
                               cid: int = 0):
    """Отправляет алерт владельцу + в audit + в dashboard."""
    icon = {
        "ban":    "🔨", "kick":   "👞", "mute":   "🔇",
        "warn":   "⚠️", "delete": "🗑️", "unban":  "🔓",
    }.get(action, "❗")

    txt = (
        f"━━━━━━━━━━━━━━━\n"
        f"🚨 <b>АНОМАЛИЯ АДМИНА</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"{icon} <b>Действие:</b> {action}\n"
        f"📊 <b>Частота:</b> {count} за {WINDOW_SEC}с (порог: {threshold})\n"
        f"👤 <b>Админ:</b> {admin_name or '—'} (<code>{admin_id}</code>)\n"
        f"🕐 <b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        f"⚙️ Используй /freeze <code>{admin_id}</code> для блокировки\n"
        f"↩️ Используй /undo <code>{admin_id}</code> N для отката"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧊 Заморозить", callback_data=f"ag_freeze_quick:{admin_id}"),
            InlineKeyboardButton(text="↩️ Откат 10",  callback_data=f"ag_undo_quick:{admin_id}:10"),
        ],
        [InlineKeyboardButton(text="📋 Подробнее", callback_data=f"ag_details:{admin_id}")],
    ])

    # Owner DM (всем владельцам)
    if _bot:
        # Собираем множество владельцев
        owners = set()
        try:
            import sys
            main = sys.modules.get("__main__") or sys.modules.get("bot")
            if main and hasattr(main, "OWNERS"):
                owners = set(main.OWNERS)
        except Exception:
            pass
        if not owners and shared.owner_id:
            owners = {shared.owner_id}

        for ow_id in owners:
            try:
                await _bot.send_message(ow_id, txt, parse_mode="HTML", reply_markup=kb)
            except Exception as e:
                log.warning(f"alert owner DM ({ow_id}): {e}")

    # Лог-канал
    try:
        await shared.send_log(txt)
    except Exception:
        pass

    # Dashboard alert
    try:
        shared.add_alert(
            "danger",
            "🚨 Аномалия админа",
            f"{admin_name or admin_id}: {count} × {action} за {WINDOW_SEC}с",
            cid=cid, uid=admin_id
        )
    except Exception:
        pass

    # Audit
    _audit("ANOMALY_DETECTED",
           actor_id=admin_id, actor_name=admin_name,
           details={"action": action, "count": count, "threshold": threshold, "cid": cid})


# ══════════════════════════════════════════════════════════════
#  AUDIT LOG
# ══════════════════════════════════════════════════════════════

def _audit(event: str, *, actor_id: int = 0, actor_name: str = "",
           target_id: int = 0, target_name: str = "",
           details: Optional[dict] = None):
    try:
        conn = _db()
        conn.execute(
            """INSERT INTO audit_log
               (event, actor_id, actor_name, target_id, target_name, details, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (event, actor_id, actor_name, target_id, target_name,
             json.dumps(details or {}, ensure_ascii=False), time.time())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"_audit: {e}")


async def _audit_log_to_channel(event: str, text: str):
    """Шлёт audit-запись в лог-канал."""
    try:
        await shared.send_log(
            f"📜 <b>AUDIT</b> · <code>{event}</code>\n\n{text}"
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════

def _is_owner(uid: int) -> bool:
    """Проверка: является ли юзер владельцем.

    Поддерживает множество владельцев (если в bot.py определена переменная OWNERS)
    или одиночного владельца (shared.owner_id).
    """
    # Пробуем достать множество владельцев из основного модуля
    try:
        import sys
        main = sys.modules.get("__main__") or sys.modules.get("bot")
        if main and hasattr(main, "OWNERS"):
            return uid in main.OWNERS
    except Exception:
        pass
    # Фолбэк: одиночный владелец из shared
    return uid == shared.owner_id


async def _owner_only(message: Message) -> bool:
    if not _is_owner(message.from_user.id):
        try:
            await message.reply("⛔ Только владелец бота может использовать эту команду.")
        except Exception:
            pass
        return False
    return True


def _parse_target(message: Message, command: CommandObject) -> Optional[int]:
    """
    Достаёт target_uid из команды:
      • реплай на сообщение
      • первый аргумент: ID или @username
    Возвращает int uid или None если @username (нужен лукап).
    """
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id

    if not command.args:
        return None

    arg = command.args.split()[0].strip()
    if arg.startswith("@"):
        return None  # @username не резолвим без хранилища
    try:
        return int(arg)
    except ValueError:
        return None


def _fmt_action(row: sqlite3.Row) -> str:
    icon = {
        "ban":    "🔨", "kick":   "👞", "mute":   "🔇",
        "warn":   "⚠️", "delete": "🗑️", "unban":  "🔓", "unmute": "🔊",
    }.get(row["action"], "•")
    ago = int(time.time() - row["ts"])
    if ago < 60:
        ago_s = f"{ago}с"
    elif ago < 3600:
        ago_s = f"{ago // 60}м"
    else:
        ago_s = f"{ago // 3600}ч"
    target = row["target_name"] or f"<code>{row['target_id']}</code>" or "—"
    return f"{icon} <b>{row['action']}</b> → {target} <i>({ago_s} назад)</i>"


# ══════════════════════════════════════════════════════════════
#  /undo
# ══════════════════════════════════════════════════════════════

async def cmd_undo(message: Message, command: CommandObject):
    """
    /undo <admin_id|reply> [N]   — откатить последние N (по умолч. 10) действий админа.
    Шаг 1: показывает список и просит подтверждения.
    """
    if not await _owner_only(message):
        return

    admin_id = _parse_target(message, command)
    if not admin_id:
        await message.reply(
            "⚙️ Использование:\n"
            "<code>/undo &lt;admin_id&gt; [N]</code> или реплай на сообщение\n\n"
            "Пример: <code>/undo 12345 10</code>",
            parse_mode="HTML"
        )
        return

    # Сколько откатывать
    n = 10
    if command.args:
        parts = command.args.split()
        if len(parts) >= 2:
            try:
                n = max(1, min(int(parts[1]), MAX_HISTORY))
            except ValueError:
                pass

    # Достаём последние N действий
    conn = _db()
    rows = conn.execute(
        """SELECT * FROM admin_actions
           WHERE admin_id=? AND undone=0
           ORDER BY ts DESC LIMIT ?""",
        (admin_id, n)
    ).fetchall()
    conn.close()

    if not rows:
        await message.reply(
            f"📭 Нет действий для отката у админа <code>{admin_id}</code>.",
            parse_mode="HTML"
        )
        return

    # Подготавливаем preview
    preview = "\n".join(_fmt_action(r) for r in rows[:15])
    if len(rows) > 15:
        preview += f"\n<i>… и ещё {len(rows) - 15}</i>"

    admin_name = rows[0]["admin_name"] or f"ID {admin_id}"

    token = f"{admin_id}_{int(time.time())}"
    _pending_undo[token] = {
        "admin_id": admin_id,
        "admin_name": admin_name,
        "action_ids": [r["id"] for r in rows],
        "requested_by": message.from_user.id,
        "ts": time.time(),
    }

    txt = (
        f"↩️ <b>Подтверждение отката</b>\n\n"
        f"👤 Админ: <b>{admin_name}</b> (<code>{admin_id}</code>)\n"
        f"📊 Действий к откату: <b>{len(rows)}</b>\n\n"
        f"<b>Будет отменено:</b>\n{preview}\n\n"
        f"⚠️ Действие необратимо. Подтвердить?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить откат", callback_data=f"ag_undo_ok:{token}"),
            InlineKeyboardButton(text="❌ Отмена",           callback_data=f"ag_undo_no:{token}"),
        ]
    ])
    await message.reply(txt, parse_mode="HTML", reply_markup=kb)

    _audit("UNDO_REQUESTED",
           actor_id=message.from_user.id,
           actor_name=message.from_user.full_name,
           target_id=admin_id, target_name=admin_name,
           details={"count": len(rows)})


async def cb_undo_confirm(call: CallbackQuery):
    data = call.data.split(":", 1)[1]
    pending = _pending_undo.pop(data, None)
    if not pending:
        await call.answer("⏱ Истёк срок или уже подтверждено", show_alert=True)
        return
    if call.from_user.id != pending["requested_by"]:
        await call.answer("⛔ Только инициатор может подтвердить", show_alert=True)
        return
    if not _is_owner(call.from_user.id):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    await call.answer("🔄 Откатываю…")

    # Выполняем откат
    result = await _perform_undo(pending["action_ids"])

    txt = (
        f"✅ <b>Откат выполнен</b>\n\n"
        f"👤 Админ: <b>{pending['admin_name']}</b> (<code>{pending['admin_id']}</code>)\n"
        f"📊 Запрошено: {len(pending['action_ids'])}\n"
        f"✓ Успешно: <b>{result['ok']}</b>\n"
        f"✗ Ошибок: <b>{result['fail']}</b>\n\n"
        f"<b>Детали:</b>\n{result['summary']}"
    )
    try:
        await call.message.edit_text(txt, parse_mode="HTML")
    except Exception:
        pass

    _audit("UNDO_EXECUTED",
           actor_id=call.from_user.id,
           actor_name=call.from_user.full_name,
           target_id=pending["admin_id"], target_name=pending["admin_name"],
           details={"ok": result["ok"], "fail": result["fail"]})
    await _audit_log_to_channel("UNDO_EXECUTED", txt)


async def cb_undo_cancel(call: CallbackQuery):
    data = call.data.split(":", 1)[1]
    _pending_undo.pop(data, None)
    try:
        await call.message.edit_text("❌ Откат отменён.")
    except Exception:
        pass
    await call.answer()


async def cb_undo_quick(call: CallbackQuery):
    """Быстрый откат прямо из алерта (10 действий, с подтверждением)."""
    if not _is_owner(call.from_user.id):
        await call.answer("⛔ Только владелец", show_alert=True)
        return
    _, admin_id_s, n_s = call.data.split(":")
    admin_id = int(admin_id_s)
    n = int(n_s)

    conn = _db()
    rows = conn.execute(
        """SELECT * FROM admin_actions
           WHERE admin_id=? AND undone=0
           ORDER BY ts DESC LIMIT ?""", (admin_id, n)
    ).fetchall()
    conn.close()

    if not rows:
        await call.answer("Нет действий для отката", show_alert=True)
        return

    admin_name = rows[0]["admin_name"] or f"ID {admin_id}"
    token = f"{admin_id}_{int(time.time())}"
    _pending_undo[token] = {
        "admin_id": admin_id, "admin_name": admin_name,
        "action_ids": [r["id"] for r in rows],
        "requested_by": call.from_user.id, "ts": time.time(),
    }

    preview = "\n".join(_fmt_action(r) for r in rows[:10])
    txt = (
        f"↩️ <b>Подтверждение быстрого отката</b>\n\n"
        f"👤 {admin_name}\n"
        f"📊 Откатить: {len(rows)} действий\n\n{preview}\n\n"
        f"Подтвердить?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да",  callback_data=f"ag_undo_ok:{token}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"ag_undo_no:{token}"),
        ]
    ])
    try:
        await call.message.answer(txt, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await call.answer()


async def _perform_undo(action_ids: List[int]) -> dict:
    """Откатывает указанные действия. Возвращает {ok, fail, summary}."""
    ok = 0
    fail = 0
    summary_lines = []

    conn = _db()
    rows = conn.execute(
        f"SELECT * FROM admin_actions WHERE id IN ({','.join('?' * len(action_ids))})",
        action_ids
    ).fetchall()
    conn.close()

    for r in rows:
        action = r["action"]
        cid = r["cid"]
        tid = r["target_id"]
        tname = r["target_name"] or str(tid)
        success = False
        err = ""

        try:
            if action == "ban":
                # Разбанить
                await _bot.unban_chat_member(cid, tid, only_if_banned=True)
                success = True

            elif action == "kick":
                # Kick = ban+unban; восстановить нельзя (юзер не возвращается),
                # но снимем "вечный" статус, если был
                await _bot.unban_chat_member(cid, tid, only_if_banned=True)
                success = True
                summary_lines.append("ℹ️ Кик: юзера придётся пригласить повторно")

            elif action == "mute":
                # Снять мут — даём базовые права
                from aiogram.types import ChatPermissions
                await _bot.restrict_chat_member(
                    cid, tid,
                    permissions=ChatPermissions(
                        can_send_messages=True,
                        can_send_media_messages=True,
                        can_send_polls=True,
                        can_send_other_messages=True,
                        can_add_web_page_previews=True,
                    )
                )
                success = True

            elif action == "warn":
                # Снимаем один варн из БД проекта
                try:
                    import database as db
                    if hasattr(db, "remove_warn"):
                        db.remove_warn(cid, tid)
                    elif hasattr(db, "_conn"):
                        c = db._conn()
                        c.execute(
                            """DELETE FROM warns WHERE rowid IN (
                                 SELECT rowid FROM warns
                                 WHERE cid=? AND uid=?
                                 ORDER BY rowid DESC LIMIT 1
                               )""", (cid, tid)
                        )
                        c.commit(); c.close()
                    success = True
                except Exception as e:
                    err = f"warn DB: {e}"

            elif action == "unban":
                # Откат разбана = повторный бан
                await _bot.ban_chat_member(cid, tid)
                success = True

            elif action == "unmute":
                from aiogram.types import ChatPermissions
                await _bot.restrict_chat_member(
                    cid, tid,
                    permissions=ChatPermissions(can_send_messages=False)
                )
                success = True

            elif action == "delete":
                # Восстановить удалённое сообщение невозможно
                err = "удалённое сообщение восстановить нельзя"

            else:
                err = f"неизвестный action: {action}"

        except Exception as e:
            err = str(e)

        if success:
            ok += 1
            summary_lines.append(f"✓ {action} → {tname}")
            # Помечаем как undone
            try:
                conn = _db()
                conn.execute("UPDATE admin_actions SET undone=1 WHERE id=?", (r["id"],))
                conn.commit(); conn.close()
            except Exception:
                pass
        else:
            fail += 1
            summary_lines.append(f"✗ {action} → {tname}: {err}")

    summary = "\n".join(summary_lines[:20])
    if len(summary_lines) > 20:
        summary += f"\n<i>… и ещё {len(summary_lines) - 20}</i>"

    return {"ok": ok, "fail": fail, "summary": summary or "—"}


# ══════════════════════════════════════════════════════════════
#  /freeze /unfreeze
# ══════════════════════════════════════════════════════════════

async def cmd_freeze(message: Message, command: CommandObject):
    """
    /freeze <admin_id|reply> [reason]
    → показывает выбор режима: 'только бот' или 'снять админку в TG'
    """
    if not await _owner_only(message):
        return

    admin_id = _parse_target(message, command)
    if not admin_id:
        await message.reply(
            "⚙️ Использование:\n"
            "<code>/freeze &lt;admin_id&gt; [причина]</code> или реплай\n\n"
            "Пример: <code>/freeze 12345 угнан акк</code>",
            parse_mode="HTML"
        )
        return

    # Достать имя из истории действий или из реплая
    admin_name = ""
    if message.reply_to_message and message.reply_to_message.from_user:
        admin_name = message.reply_to_message.from_user.full_name

    if not admin_name:
        conn = _db()
        row = conn.execute(
            "SELECT admin_name FROM admin_actions WHERE admin_id=? AND admin_name!='' ORDER BY ts DESC LIMIT 1",
            (admin_id,)
        ).fetchone()
        conn.close()
        if row:
            admin_name = row["admin_name"]

    # Парсим причину
    reason = ""
    if command.args:
        parts = command.args.split(maxsplit=1)
        reason = parts[1] if len(parts) > 1 and not parts[0].startswith("@") and not parts[0].lstrip("-").isdigit() else (parts[1] if len(parts) > 1 else "")
        # Если первый аргумент не ID — значит реплай + причина
        if message.reply_to_message and command.args:
            reason = command.args

    if is_frozen(admin_id):
        info = get_freeze_info(admin_id)
        await message.reply(
            f"🧊 Админ уже заморожен.\n"
            f"Режим: <code>{info.get('mode')}</code>\n"
            f"Причина: {info.get('reason') or '—'}",
            parse_mode="HTML"
        )
        return

    token = f"{admin_id}_{int(time.time())}"
    _pending_undo[token] = {  # переиспользуем тот же кэш
        "kind": "freeze",
        "admin_id": admin_id,
        "admin_name": admin_name or f"ID {admin_id}",
        "reason": reason,
        "requested_by": message.from_user.id,
        "ts": time.time(),
    }

    txt = (
        f"🧊 <b>Заморозка админа</b>\n\n"
        f"👤 {admin_name or 'ID ' + str(admin_id)} (<code>{admin_id}</code>)\n"
        f"📝 Причина: {reason or '—'}\n\n"
        f"<b>Выбери режим:</b>\n\n"
        f"🤖 <b>Только бот</b> — игнорировать его команды боту, "
        f"но админка в Telegram остаётся\n\n"
        f"🚫 <b>Снять в Telegram</b> — полностью снять админ-права "
        f"во всех чатах бота (обратимо через /unfreeze)"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤖 Только бот",       callback_data=f"ag_fr_mode:bot:{token}"),
            InlineKeyboardButton(text="🚫 Снять в Telegram", callback_data=f"ag_fr_mode:tg:{token}"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"ag_fr_cancel:{token}")],
    ])
    await message.reply(txt, parse_mode="HTML", reply_markup=kb)


async def cb_freeze_mode(call: CallbackQuery):
    if not _is_owner(call.from_user.id):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    parts = call.data.split(":")
    mode = parts[1]   # bot | tg
    token = parts[2]
    pending = _pending_undo.pop(token, None)
    if not pending or pending.get("kind") != "freeze":
        await call.answer("⏱ Истёк срок", show_alert=True)
        return

    admin_id = pending["admin_id"]
    admin_name = pending["admin_name"]
    reason = pending["reason"]
    affected = []

    if mode == "tg":
        # Пытаемся снять админку во всех известных чатах
        try:
            import database as db
            if hasattr(db, "get_known_chats"):
                chats = db.get_known_chats()
            else:
                chats = []
        except Exception:
            chats = []

        for cid in chats:
            try:
                # Не каждый бот может снимать админку (надо быть creator),
                # но попытка через promote с пустыми правами
                await _bot.promote_chat_member(
                    cid, admin_id,
                    can_manage_chat=False,
                    can_delete_messages=False,
                    can_manage_video_chats=False,
                    can_restrict_members=False,
                    can_promote_members=False,
                    can_change_info=False,
                    can_invite_users=False,
                    can_pin_messages=False,
                )
                affected.append(cid)
            except Exception as e:
                log.warning(f"freeze tg cid={cid}: {e}")

    # Сохраняем в БД
    conn = _db()
    conn.execute(
        """INSERT OR REPLACE INTO frozen_admins
           (admin_id, admin_name, mode, frozen_by, reason, ts, affected_chats)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (admin_id, admin_name, mode, call.from_user.id, reason, time.time(),
         json.dumps(affected, ensure_ascii=False))
    )
    conn.commit()
    conn.close()

    mode_label = "🤖 Только бот" if mode == "bot" else "🚫 Снято в Telegram"
    extra = ""
    if mode == "tg":
        extra = f"\n📍 Затронуто чатов: <b>{len(affected)}</b>"

    txt = (
        f"✅ <b>Админ заморожен</b>\n\n"
        f"👤 {admin_name} (<code>{admin_id}</code>)\n"
        f"⚙️ Режим: {mode_label}\n"
        f"📝 Причина: {reason or '—'}"
        f"{extra}\n\n"
        f"Для размораживания: /unfreeze <code>{admin_id}</code>"
    )
    try:
        await call.message.edit_text(txt, parse_mode="HTML")
    except Exception:
        pass
    await call.answer("🧊 Заморожено")

    _audit("FREEZE",
           actor_id=call.from_user.id, actor_name=call.from_user.full_name,
           target_id=admin_id, target_name=admin_name,
           details={"mode": mode, "reason": reason, "affected_chats": affected})
    await _audit_log_to_channel("FREEZE", txt)


async def cb_freeze_cancel(call: CallbackQuery):
    token = call.data.split(":", 1)[1]
    _pending_undo.pop(token, None)
    try:
        await call.message.edit_text("❌ Заморозка отменена.")
    except Exception:
        pass
    await call.answer()


async def cb_freeze_quick(call: CallbackQuery):
    """Быстрая заморозка из алерта аномалии."""
    if not _is_owner(call.from_user.id):
        await call.answer("⛔ Только владелец", show_alert=True)
        return
    admin_id = int(call.data.split(":", 1)[1])

    conn = _db()
    row = conn.execute(
        "SELECT admin_name FROM admin_actions WHERE admin_id=? ORDER BY ts DESC LIMIT 1",
        (admin_id,)
    ).fetchone()
    conn.close()
    admin_name = (row["admin_name"] if row else "") or f"ID {admin_id}"

    token = f"{admin_id}_{int(time.time())}"
    _pending_undo[token] = {
        "kind": "freeze", "admin_id": admin_id, "admin_name": admin_name,
        "reason": "Автоматически из алерта аномалии",
        "requested_by": call.from_user.id, "ts": time.time(),
    }
    txt = (
        f"🧊 <b>Заморозка из алерта</b>\n\n"
        f"👤 {admin_name}\n\nВыбери режим:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤖 Только бот",       callback_data=f"ag_fr_mode:bot:{token}"),
            InlineKeyboardButton(text="🚫 Снять в Telegram", callback_data=f"ag_fr_mode:tg:{token}"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"ag_fr_cancel:{token}")],
    ])
    try:
        await call.message.answer(txt, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await call.answer()


async def cmd_unfreeze(message: Message, command: CommandObject):
    if not await _owner_only(message):
        return

    admin_id = _parse_target(message, command)
    if not admin_id:
        await message.reply("⚙️ /unfreeze <code>&lt;admin_id&gt;</code>", parse_mode="HTML")
        return

    info = get_freeze_info(admin_id)
    if not info:
        await message.reply("❄️ Этот админ не заморожен.")
        return

    conn = _db()
    conn.execute("DELETE FROM frozen_admins WHERE admin_id=?", (admin_id,))
    conn.commit()
    conn.close()

    txt = (
        f"✅ <b>Админ разморожен</b>\n\n"
        f"👤 {info['admin_name']} (<code>{admin_id}</code>)\n\n"
        f"⚠️ Если режим был <code>tg</code> — права в чатах придётся выдать вручную."
    )
    await message.reply(txt, parse_mode="HTML")

    _audit("UNFREEZE",
           actor_id=message.from_user.id, actor_name=message.from_user.full_name,
           target_id=admin_id, target_name=info["admin_name"])
    await _audit_log_to_channel("UNFREEZE", txt)


async def cmd_frozen(message: Message):
    if not await _owner_only(message):
        return
    conn = _db()
    rows = conn.execute("SELECT * FROM frozen_admins ORDER BY ts DESC").fetchall()
    conn.close()

    if not rows:
        await message.reply("📭 Замороженных админов нет.")
        return

    lines = ["🧊 <b>Замороженные админы:</b>\n"]
    for r in rows:
        ago = int(time.time() - r["ts"])
        if ago < 3600:
            ago_s = f"{ago // 60}м"
        elif ago < 86400:
            ago_s = f"{ago // 3600}ч"
        else:
            ago_s = f"{ago // 86400}д"
        mode_icon = "🤖" if r["mode"] == "bot" else "🚫"
        lines.append(
            f"{mode_icon} <b>{r['admin_name']}</b> (<code>{r['admin_id']}</code>)\n"
            f"    └ {ago_s} назад · {r['reason'] or '—'}"
        )
    await message.reply("\n".join(lines), parse_mode="HTML")


# ══════════════════════════════════════════════════════════════
#  /audit /details
# ══════════════════════════════════════════════════════════════

async def cmd_audit(message: Message, command: CommandObject):
    if not await _owner_only(message):
        return
    n = 20
    if command.args:
        try:
            n = max(1, min(int(command.args.split()[0]), 100))
        except ValueError:
            pass

    conn = _db()
    rows = conn.execute("SELECT * FROM audit_log ORDER BY ts DESC LIMIT ?", (n,)).fetchall()
    conn.close()

    if not rows:
        await message.reply("📭 Журнал аудита пуст.")
        return

    lines = [f"📜 <b>Audit-лог</b> (последние {len(rows)}):\n"]
    for r in rows:
        ago = int(time.time() - r["ts"])
        if ago < 60:    ago_s = f"{ago}с"
        elif ago < 3600: ago_s = f"{ago // 60}м"
        else:            ago_s = f"{ago // 3600}ч"
        actor = r["actor_name"] or f"ID {r['actor_id']}" or "—"
        target = r["target_name"] or (f"ID {r['target_id']}" if r["target_id"] else "")
        line = f"<code>{r['event']:18}</code> · {actor}"
        if target:
            line += f" → {target}"
        line += f" <i>({ago_s})</i>"
        lines.append(line)

    await message.reply("\n".join(lines), parse_mode="HTML")


async def cb_details(call: CallbackQuery):
    if not _is_owner(call.from_user.id):
        await call.answer("⛔ Только владелец", show_alert=True)
        return
    admin_id = int(call.data.split(":", 1)[1])

    conn = _db()
    rows = conn.execute(
        """SELECT action, COUNT(*) as cnt
           FROM admin_actions
           WHERE admin_id=? AND ts >= ?
           GROUP BY action ORDER BY cnt DESC""",
        (admin_id, time.time() - 3600)
    ).fetchall()
    last = conn.execute(
        """SELECT * FROM admin_actions
           WHERE admin_id=? ORDER BY ts DESC LIMIT 10""", (admin_id,)
    ).fetchall()
    conn.close()

    txt = f"📋 <b>Детали по админу</b> <code>{admin_id}</code>\n\n"
    txt += "<b>За последний час:</b>\n"
    if rows:
        for r in rows:
            txt += f"  • {r['action']}: <b>{r['cnt']}</b>\n"
    else:
        txt += "  —\n"
    txt += "\n<b>Последние 10 действий:</b>\n"
    txt += "\n".join(_fmt_action(r) for r in last) if last else "  —"

    try:
        await call.message.answer(txt, parse_mode="HTML")
    except Exception:
        pass
    await call.answer()


# ══════════════════════════════════════════════════════════════
#  ИНИЦИАЛИЗАЦИЯ
# ══════════════════════════════════════════════════════════════

def _register_handlers(dp: Dispatcher):
    # Основные команды (с префиксом /g чтобы не конфликтовать с другими хэндлерами)
    dp.message.register(cmd_undo,     Command("undo", "gundo"))
    dp.message.register(cmd_freeze,   Command("freeze", "gfreeze"))
    dp.message.register(cmd_unfreeze, Command("unfreeze", "gunfreeze"))
    dp.message.register(cmd_frozen,   Command("frozen", "gfrozen"))
    # /audit уже занят в bot.py под "аудит чата" — используем /gaudit
    dp.message.register(cmd_audit,    Command("gaudit"))

    dp.callback_query.register(cb_undo_confirm, F.data.startswith("ag_undo_ok:"))
    dp.callback_query.register(cb_undo_cancel,  F.data.startswith("ag_undo_no:"))
    dp.callback_query.register(cb_undo_quick,   F.data.startswith("ag_undo_quick:"))
    dp.callback_query.register(cb_freeze_mode,  F.data.startswith("ag_fr_mode:"))
    dp.callback_query.register(cb_freeze_cancel,F.data.startswith("ag_fr_cancel:"))
    dp.callback_query.register(cb_freeze_quick, F.data.startswith("ag_freeze_quick:"))
    dp.callback_query.register(cb_details,      F.data.startswith("ag_details:"))


async def _cleanup_loop():
    """Раз в минуту чистим истёкшие pending-токены."""
    while True:
        try:
            now = time.time()
            expired = [t for t, p in _pending_undo.items() if now - p["ts"] > CONFIRM_TTL]
            for t in expired:
                _pending_undo.pop(t, None)
        except Exception:
            pass
        await asyncio.sleep(60)


async def init(bot: Bot, dp: Dispatcher):
    """Инициализация модуля. Вызывай из bot.py после shared.init()."""
    global _bot
    _bot = bot
    _init_tables()
    _register_handlers(dp)
    asyncio.create_task(_cleanup_loop())
    log.info("✅ admin_guardian.py инициализирован")

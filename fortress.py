# -*- coding: utf-8 -*-
"""
fortress.py — Многоуровневая система активной защиты
═════════════════════════════════════════════════════

Модули:
  1. 🍯 HoneypotSystem    — скрытые ловушки для спамеров и ботов
                           (невидимые команды, фейковые инвайты, приманки)
  2. 🔒 QuarantineZone   — автоматическая карантинная зона
                           (изоляция, проверки, ступенчатое освобождение)
  3. 🚨 IncidentManager  — жизненный цикл инцидентов безопасности
                           (автоэскалация, таймлайн, постмортем)
  4. ⏱  RateLimiter      — многоуровневый rate-limiting на уровне бота
                           (токен-бакет, скользящее окно, адаптивный порог)
  5. 🔍 ForensicsEngine  — криминалистика: восстановление удалённых данных,
                           timeline событий, экспорт для расследований
  6. 🛡  ActiveDefense    — активная защита: автоответ на атаки,
                           decoy-сообщения, канареечные токены

Подключение в bot.py:
    import fortress
    # В main():
    await fortress.init(bot, dp)
    # В StatsMiddleware:
    if await fortress.gate(message):
        return   # сообщение заблокировано
    # При любом удалении сообщения:
    await fortress.forensics.record_deletion(message, reason)
    # В on_new_member:
    await fortress.quarantine.check_new_member(message, member)

Команды:
    /honeypot [on|off|status]
    /quarantine [list|release uid|stats]
    /incident [list|show id|close id]
    /ratelimit [status|reset uid|config]
    /forensics [timeline uid|export cid|deleted]
    /defense [status|lockdown|unlock]
"""

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import random
import re
import sqlite3
import string
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery, ChatPermissions,
    InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

log = logging.getLogger(__name__)

_bot: Optional[Bot] = None
_admin_ids: Set[int] = set()
_log_channel: int = 0

DB_FILE = "skinvault.db"


# ══════════════════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_tables():
    conn = _db()
    conn.executescript("""
    -- ═══════════════════════════════════════════
    --  HONEYPOT
    -- ═══════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS ft_honeypots (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER NOT NULL,
        kind        TEXT    NOT NULL,   -- 'command','link','keyword','invite'
        trigger     TEXT    NOT NULL,   -- что триггерит ловушку
        action      TEXT    DEFAULT 'ban',
        hits        INTEGER DEFAULT 0,
        active      INTEGER DEFAULT 1,
        created_at  INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS ft_honeypot_hits (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        honeypot_id INTEGER NOT NULL,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        text        TEXT    DEFAULT '',
        action_taken TEXT   DEFAULT '',
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );

    -- ═══════════════════════════════════════════
    --  КАРАНТИН
    -- ═══════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS ft_quarantine (
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        reason      TEXT    DEFAULT '',
        level       INTEGER DEFAULT 1,   -- 1=мут, 2=изоляция, 3=полный карантин
        expires_at  INTEGER DEFAULT 0,   -- 0 = бессрочно
        released_by INTEGER DEFAULT 0,
        checks_done INTEGER DEFAULT 0,
        created_at  INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (uid, cid)
    );
    CREATE TABLE IF NOT EXISTS ft_quarantine_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        event       TEXT    NOT NULL,
        details     TEXT    DEFAULT '',
        by_uid      INTEGER DEFAULT 0,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );

    -- ═══════════════════════════════════════════
    --  ИНЦИДЕНТЫ
    -- ═══════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS ft_incidents (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER NOT NULL,
        kind        TEXT    NOT NULL,
        severity    INTEGER DEFAULT 1,   -- 1=low 2=med 3=high 4=critical
        status      TEXT    DEFAULT 'open',
        title       TEXT    NOT NULL,
        description TEXT    DEFAULT '',
        affected_uids TEXT  DEFAULT '[]',
        actions_taken TEXT  DEFAULT '[]',
        assigned_to INTEGER DEFAULT 0,
        opened_at   INTEGER DEFAULT (strftime('%s','now')),
        updated_at  INTEGER DEFAULT (strftime('%s','now')),
        closed_at   INTEGER DEFAULT 0,
        postmortem  TEXT    DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS ft_incident_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        incident_id INTEGER NOT NULL,
        event_type  TEXT    NOT NULL,
        description TEXT    DEFAULT '',
        by_uid      INTEGER DEFAULT 0,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );

    -- ═══════════════════════════════════════════
    --  RATE LIMITING
    -- ═══════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS ft_rate_violations (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        rule        TEXT    NOT NULL,
        count       INTEGER DEFAULT 1,
        window_start INTEGER NOT NULL,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS ft_rate_config (
        cid         INTEGER NOT NULL,
        rule        TEXT    NOT NULL,
        limit_count INTEGER NOT NULL,
        window_secs INTEGER NOT NULL,
        action      TEXT    DEFAULT 'warn',
        PRIMARY KEY (cid, rule)
    );

    -- ═══════════════════════════════════════════
    --  FORENSICS
    -- ═══════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS ft_message_archive (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        msg_id      INTEGER NOT NULL,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        text        TEXT    DEFAULT '',
        media_type  TEXT    DEFAULT '',
        entities    TEXT    DEFAULT '[]',
        ts          INTEGER NOT NULL,
        archived_at INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_ft_archive_uid
        ON ft_message_archive(uid, cid);
    CREATE TABLE IF NOT EXISTS ft_deletions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        msg_id      INTEGER NOT NULL,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        text        TEXT    DEFAULT '',
        reason      TEXT    DEFAULT '',
        deleted_by  INTEGER DEFAULT 0,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_ft_deletions_uid
        ON ft_deletions(uid, cid);
    CREATE TABLE IF NOT EXISTS ft_events_timeline (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        event_type  TEXT    NOT NULL,
        payload     TEXT    DEFAULT '{}',
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_ft_timeline_uid
        ON ft_events_timeline(uid, cid, ts);

    -- ═══════════════════════════════════════════
    --  ACTIVE DEFENSE
    -- ═══════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS ft_lockdowns (
        cid         INTEGER PRIMARY KEY,
        active      INTEGER DEFAULT 0,
        level       INTEGER DEFAULT 1,   -- 1=slowmode 2=members_only 3=closed
        reason      TEXT    DEFAULT '',
        auto        INTEGER DEFAULT 0,   -- автоматический локдаун
        started_at  INTEGER DEFAULT 0,
        expires_at  INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ft_canary_tokens (
        token       TEXT    PRIMARY KEY,
        label       TEXT    NOT NULL,
        cid         INTEGER NOT NULL,
        created_by  INTEGER NOT NULL,
        triggered   INTEGER DEFAULT 0,
        triggered_by INTEGER DEFAULT 0,
        triggered_at INTEGER DEFAULT 0,
        created_at  INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS ft_defense_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER NOT NULL,
        action      TEXT    NOT NULL,
        reason      TEXT    DEFAULT '',
        by_uid      INTEGER DEFAULT 0,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    conn.commit()
    conn.close()
    log.info("✅ fortress: таблицы инициализированы")


# ══════════════════════════════════════════════════════════════════════════
#  1. 🍯 HONEYPOT SYSTEM
# ══════════════════════════════════════════════════════════════════════════

# Встроенные ловушки — невидимые команды которые не анонсированы нигде
_BUILTIN_HONEYPOTS = [
    # Команды которые только боты пробуют
    "/admin", "/admins", "/op", "/owner", "/promote",
    "/addadmin", "/setowner", "/getadmin",
    # Типичные спам-команды
    "/earn", "/money", "/free", "/prize", "/winner",
    "/click", "/subscribe", "/follow",
    # Разведывательные
    "/members", "/userlist", "/getusers", "/dumpusers",
]

# Паттерны приманок в тексте
_HONEYPOT_PATTERNS = [
    r'\+7\s*\(?\d{3}\)?\s*\d{3}[-\s]?\d{2}[-\s]?\d{2}',  # фейковый телефон
    r'@honeyadmin\d+',   # фейковый admin-аккаунт
]

# RAM-кэш последних срабатываний {uid: ts}
_honeypot_recent_hits: Dict[int, float] = {}
# Кулдаун уведомлений по uid (не спамить при повторных хитах)
_HONEYPOT_NOTIFY_COOLDOWN = 300  # 5 минут


def _get_honeypots(cid: int) -> List[dict]:
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM ft_honeypots WHERE cid=? AND active=1", (cid,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _add_honeypot(cid: int, kind: str, trigger: str, action: str = "ban") -> int:
    conn = _db()
    cur = conn.execute(
        "INSERT INTO ft_honeypots(cid,kind,trigger,action) VALUES(?,?,?,?)",
        (cid, kind, trigger.lower().strip(), action)
    )
    hid = cur.lastrowid
    conn.commit()
    conn.close()
    return hid


async def _honeypot_triggered(honeypot: dict, message: Message):
    """Обрабатывает срабатывание ловушки"""
    uid = message.from_user.id
    cid = message.chat.id
    now = time.time()

    # Кулдаун — не спамим уведомлениями
    last_hit = _honeypot_recent_hits.get(uid, 0)
    notify = (now - last_hit) > _HONEYPOT_NOTIFY_COOLDOWN
    _honeypot_recent_hits[uid] = now

    # Удаляем сообщение немедленно
    try:
        await message.delete()
    except Exception:
        pass

    action = honeypot.get("action", "ban")
    action_taken = ""

    try:
        if action == "ban":
            await _bot.ban_chat_member(cid, uid)
            action_taken = "🔨 Забанен"
        elif action == "kick":
            await _bot.ban_chat_member(cid, uid)
            await _bot.unban_chat_member(cid, uid)
            action_taken = "👢 Кикнут"
        elif action == "mute":
            until = datetime.now() + timedelta(hours=1)
            await _bot.restrict_chat_member(
                cid, uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until
            )
            action_taken = "🔇 Замьючен на 1ч"
        elif action == "quarantine":
            await quarantine.add(uid, cid, f"Honeypot: {honeypot['trigger']}", level=2)
            action_taken = "🔒 Карантин"
        else:
            action_taken = "📝 Logged"
    except Exception as e:
        action_taken = f"❌ Ошибка: {e}"

    # Пишем в БД
    conn = _db()
    conn.execute(
        "INSERT INTO ft_honeypot_hits(honeypot_id,uid,cid,text,action_taken) VALUES(?,?,?,?,?)",
        (honeypot["id"], uid, cid,
         (message.text or "")[:200], action_taken)
    )
    conn.execute(
        "UPDATE ft_honeypots SET hits=hits+1 WHERE id=?",
        (honeypot["id"],)
    )
    conn.commit()
    conn.close()

    # Пишем в forensics timeline
    forensics.record_event(uid, cid, "honeypot_hit", {
        "trigger": honeypot["trigger"],
        "kind": honeypot["kind"],
        "action": action_taken,
    })

    if notify:
        name = message.from_user.full_name
        await _send_log(
            f"🍯 <b>Honeypot сработал!</b>\n\n"
            f"👤 {name} (<code>{uid}</code>)\n"
            f"💬 {message.chat.title}\n"
            f"🎯 Триггер: <code>{honeypot['trigger']}</code> ({honeypot['kind']})\n"
            f"⚡ Действие: {action_taken}",
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Правильно",
                    callback_data=f"ft:hp_ok:{uid}:{cid}"
                ),
                InlineKeyboardButton(
                    text="↩️ Отменить",
                    callback_data=f"ft:hp_undo:{uid}:{cid}:{action}"
                ),
            ]])
        )

    # Автоматически создаём инцидент при повторных хитах
    conn = _db()
    total_hits = conn.execute(
        "SELECT SUM(hits) as s FROM ft_honeypots WHERE cid=?", (cid,)
    ).fetchone()["s"] or 0
    conn.close()

    if total_hits % 10 == 0:  # каждые 10 хитов — новый инцидент
        await incident_manager.create(
            cid=cid,
            kind="honeypot_mass_hit",
            severity=3,
            title=f"Массовые срабатывания honeypot ({total_hits} хитов)",
            description=f"Последний: {name} ({uid}), триггер: {honeypot['trigger']}",
            affected_uids=[uid],
        )


async def check_honeypot(message: Message) -> bool:
    """
    Проверяет сообщение на попадание в ловушку.
    Возвращает True если сообщение обработано honeypot-ом.
    """
    if not message.from_user:
        return False
    if message.chat.type not in ("group", "supergroup"):
        return False

    cid = message.chat.id
    text = (message.text or message.caption or "").strip().lower()

    # 1. Встроенные honeypot-команды
    if text:
        for cmd in _BUILTIN_HONEYPOTS:
            if text.startswith(cmd):
                # Проверяем — это публичная команда бота? Если нет — ловушка
                await _honeypot_triggered(
                    {"id": 0, "trigger": cmd, "kind": "builtin_command",
                     "action": "mute"},
                    message
                )
                return True

    # 2. Кастомные ловушки чата
    honeypots = _get_honeypots(cid)
    for hp in honeypots:
        trigger = hp["trigger"].lower()
        kind = hp["kind"]

        if kind == "command" and text.startswith(trigger):
            await _honeypot_triggered(hp, message)
            return True
        elif kind == "keyword" and trigger in text:
            await _honeypot_triggered(hp, message)
            return True
        elif kind == "link":
            if trigger in text:
                await _honeypot_triggered(hp, message)
                return True
        elif kind == "pattern":
            try:
                if re.search(trigger, text):
                    await _honeypot_triggered(hp, message)
                    return True
            except re.error:
                pass

    return False


def get_honeypot_stats(cid: int) -> dict:
    conn = _db()
    total = conn.execute(
        "SELECT COUNT(*) as c FROM ft_honeypot_hits WHERE cid=?", (cid,)
    ).fetchone()["c"]
    today = conn.execute(
        "SELECT COUNT(*) as c FROM ft_honeypot_hits WHERE cid=? "
        "AND date(ts,'unixepoch')=date('now')", (cid,)
    ).fetchone()["c"]
    top = conn.execute(
        "SELECT h.trigger, COUNT(hits.id) as cnt "
        "FROM ft_honeypots h "
        "LEFT JOIN ft_honeypot_hits hits ON h.id=hits.honeypot_id "
        "WHERE h.cid=? GROUP BY h.id ORDER BY cnt DESC LIMIT 5",
        (cid,)
    ).fetchall()
    conn.close()
    return {
        "total_hits": total,
        "today_hits": today,
        "top_triggers": [{"trigger": r["trigger"], "hits": r["cnt"]} for r in top],
    }


# ══════════════════════════════════════════════════════════════════════════
#  2. 🔒 QUARANTINE ZONE
# ══════════════════════════════════════════════════════════════════════════

class QuarantineZone:
    """
    Карантинная зона — ступенчатая изоляция пользователей.

    Уровни:
      1 = Мягкий мут (нельзя медиа/стикеры)
      2 = Строгий мут (нельзя ничего кроме текста)
      3 = Полная изоляция (нельзя ничего)
    """

    LEVELS = {
        1: {
            "name": "Мягкий карантин",
            "perms": ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
            "duration_hours": 1,
            "emoji": "🟡",
        },
        2: {
            "name": "Строгий карантин",
            "perms": ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
            "duration_hours": 6,
            "emoji": "🟠",
        },
        3: {
            "name": "Полная изоляция",
            "perms": ChatPermissions(
                can_send_messages=False,
            ),
            "duration_hours": 24,
            "emoji": "🔴",
        },
    }

    async def add(self, uid: int, cid: int, reason: str,
                  level: int = 1, duration_hours: Optional[int] = None,
                  by_uid: int = 0):
        """Помещает пользователя в карантин"""
        level = max(1, min(3, level))
        level_info = self.LEVELS[level]
        hours = duration_hours or level_info["duration_hours"]
        expires_at = int(time.time()) + hours * 3600

        # Проверяем — уже в карантине?
        conn = _db()
        existing = conn.execute(
            "SELECT level FROM ft_quarantine WHERE uid=? AND cid=?",
            (uid, cid)
        ).fetchone()
        conn.close()

        if existing and existing["level"] >= level:
            # Уже на более высоком уровне — эскалируем
            level = min(3, existing["level"] + 1)
            level_info = self.LEVELS[level]

        conn = _db()
        conn.execute(
            "INSERT INTO ft_quarantine(uid,cid,reason,level,expires_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(uid,cid) DO UPDATE SET level=excluded.level, "
            "reason=excluded.reason, expires_at=excluded.expires_at",
            (uid, cid, reason[:300], level, expires_at)
        )
        conn.execute(
            "INSERT INTO ft_quarantine_log(uid,cid,event,details,by_uid) VALUES(?,?,?,?,?)",
            (uid, cid, "quarantine_add",
             f"level={level}, reason={reason[:100]}, expires={expires_at}", by_uid)
        )
        conn.commit()
        conn.close()

        # Применяем ограничения
        try:
            until = datetime.now() + timedelta(hours=hours)
            await _bot.restrict_chat_member(
                cid, uid,
                permissions=level_info["perms"],
                until_date=until
            )
        except Exception as e:
            log.warning(f"quarantine restrict error uid={uid}: {e}")

        forensics.record_event(uid, cid, "quarantine_add", {
            "level": level, "reason": reason, "hours": hours,
        })

        emoji = level_info["emoji"]
        log.info(f"[QUARANTINE] {emoji} uid={uid} level={level} cid={cid}")

        await _send_log(
            f"{emoji} <b>Карантин — {level_info['name']}</b>\n\n"
            f"👤 <code>{uid}</code>\n"
            f"💬 чат <code>{cid}</code>\n"
            f"📋 {reason[:200]}\n"
            f"⏱ На {hours} часов",
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Освободить",
                    callback_data=f"ft:qr_release:{uid}:{cid}"
                ),
                InlineKeyboardButton(
                    text="⬆️ Эскалировать",
                    callback_data=f"ft:qr_escalate:{uid}:{cid}"
                ),
                InlineKeyboardButton(
                    text="🔨 Забанить",
                    callback_data=f"ft:qr_ban:{uid}:{cid}"
                ),
            ]])
        )

    async def release(self, uid: int, cid: int, by_uid: int = 0):
        """Освобождает из карантина"""
        conn = _db()
        conn.execute(
            "DELETE FROM ft_quarantine WHERE uid=? AND cid=?", (uid, cid)
        )
        conn.execute(
            "INSERT INTO ft_quarantine_log(uid,cid,event,by_uid) VALUES(?,?,?,?)",
            (uid, cid, "quarantine_release", by_uid)
        )
        conn.commit()
        conn.close()

        try:
            # Восстанавливаем базовые права
            await _bot.restrict_chat_member(
                cid, uid,
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                )
            )
        except Exception as e:
            log.warning(f"quarantine release error: {e}")

        forensics.record_event(uid, cid, "quarantine_release", {"by": by_uid})

    async def escalate(self, uid: int, cid: int, by_uid: int = 0):
        """Повышает уровень карантина"""
        conn = _db()
        row = conn.execute(
            "SELECT level, reason FROM ft_quarantine WHERE uid=? AND cid=?",
            (uid, cid)
        ).fetchone()
        conn.close()

        if not row:
            await self.add(uid, cid, "Эскалация без причины", level=2, by_uid=by_uid)
            return

        new_level = min(3, row["level"] + 1)
        await self.add(uid, cid, row["reason"], level=new_level, by_uid=by_uid)

    def get_list(self, cid: int) -> List[dict]:
        conn = _db()
        rows = conn.execute(
            "SELECT * FROM ft_quarantine WHERE cid=? ORDER BY level DESC",
            (cid,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def is_quarantined(self, uid: int, cid: int) -> Optional[dict]:
        conn = _db()
        row = conn.execute(
            "SELECT * FROM ft_quarantine WHERE uid=? AND cid=?",
            (uid, cid)
        ).fetchone()
        conn.close()
        if not row:
            return None
        now = int(time.time())
        if row["expires_at"] and row["expires_at"] < now:
            return None  # истёк
        return dict(row)

    async def check_new_member(self, message: Message, member) -> bool:
        """Автоматически проверяет новых участников"""
        uid = member.id
        cid = message.chat.id

        # Если уже в карантине — применяем снова
        status = self.is_quarantined(uid, cid)
        if status:
            level_info = self.LEVELS[status["level"]]
            try:
                await _bot.restrict_chat_member(
                    cid, uid, permissions=level_info["perms"]
                )
            except Exception:
                pass
            return True

        return False

    async def _auto_expire_loop(self):
        """Каждые 15 минут снимает истёкший карантин"""
        while True:
            await asyncio.sleep(900)
            try:
                now = int(time.time())
                conn = _db()
                expired = conn.execute(
                    "SELECT uid, cid FROM ft_quarantine "
                    "WHERE expires_at > 0 AND expires_at < ?", (now,)
                ).fetchall()
                conn.close()

                for row in expired:
                    await self.release(row["uid"], row["cid"], by_uid=0)
                    log.info(f"[QUARANTINE] Авто-снятие uid={row['uid']}")
            except Exception as e:
                log.error(f"quarantine auto_expire: {e}")


quarantine = QuarantineZone()


# ══════════════════════════════════════════════════════════════════════════
#  3. 🚨 INCIDENT MANAGER
# ══════════════════════════════════════════════════════════════════════════

SEVERITY_LABELS = {
    1: ("🔵", "LOW"),
    2: ("🟡", "MEDIUM"),
    3: ("🟠", "HIGH"),
    4: ("🔴", "CRITICAL"),
}

# Правила автоэскалации инцидентов
_ESCALATION_RULES = [
    # (kind, severity_threshold, escalate_to)
    ("honeypot_mass_hit", 3, 4),
    ("raid_detected", 3, 4),
    ("mass_ban", 2, 3),
]

# Антиспам для создания инцидентов {(cid, kind): last_ts}
_incident_cooldowns: Dict[Tuple[int, str], float] = {}
_INCIDENT_COOLDOWN = 300  # 5 минут между инцидентами одного типа


class IncidentManager:
    async def create(
        self,
        cid: int,
        kind: str,
        severity: int,
        title: str,
        description: str = "",
        affected_uids: Optional[List[int]] = None,
    ) -> int:
        """Создаёт инцидент. Возвращает ID."""
        # Кулдаун
        key = (cid, kind)
        now = time.time()
        if now - _incident_cooldowns.get(key, 0) < _INCIDENT_COOLDOWN:
            return -1
        _incident_cooldowns[key] = now

        conn = _db()
        cur = conn.execute(
            "INSERT INTO ft_incidents(cid,kind,severity,title,description,affected_uids) "
            "VALUES(?,?,?,?,?,?)",
            (cid, kind, severity, title[:200], description[:1000],
             json.dumps(affected_uids or []))
        )
        iid = cur.lastrowid
        conn.commit()
        conn.close()

        self._add_event(iid, "created",
                        f"severity={severity}, kind={kind}")

        emoji, label = SEVERITY_LABELS.get(severity, ("⚪", "UNKNOWN"))

        await _send_log(
            f"{emoji} <b>Инцидент #{iid} — {label}</b>\n\n"
            f"📋 <b>{title}</b>\n"
            f"💬 чат <code>{cid}</code>\n"
            f"{description[:300]}\n\n"
            + (f"👥 Затронуто: {', '.join(f'<code>{u}</code>' for u in (affected_uids or [])[:5])}"
               if affected_uids else ""),
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Закрыть",
                    callback_data=f"ft:inc_close:{iid}"
                ),
                InlineKeyboardButton(
                    text="⬆️ Эскалировать",
                    callback_data=f"ft:inc_escalate:{iid}"
                ),
                InlineKeyboardButton(
                    text="🔒 Локдаун чата",
                    callback_data=f"ft:inc_lockdown:{iid}:{cid}"
                ),
            ]])
        )

        # Проверяем правила автоэскалации
        for rule_kind, rule_sev, escalate_to in _ESCALATION_RULES:
            if kind == rule_kind and severity >= rule_sev:
                await self.escalate(iid, escalate_to)

        return iid

    def _add_event(self, incident_id: int, event_type: str,
                   description: str = "", by_uid: int = 0):
        conn = _db()
        conn.execute(
            "INSERT INTO ft_incident_events(incident_id,event_type,description,by_uid) "
            "VALUES(?,?,?,?)",
            (incident_id, event_type, description[:500], by_uid)
        )
        conn.execute(
            "UPDATE ft_incidents SET updated_at=strftime('%s','now') WHERE id=?",
            (incident_id,)
        )
        conn.commit()
        conn.close()

    async def escalate(self, incident_id: int, new_severity: Optional[int] = None):
        conn = _db()
        row = conn.execute(
            "SELECT severity, cid FROM ft_incidents WHERE id=?",
            (incident_id,)
        ).fetchone()
        conn.close()

        if not row:
            return

        new_sev = new_severity or min(4, row["severity"] + 1)
        conn = _db()
        conn.execute(
            "UPDATE ft_incidents SET severity=?, updated_at=strftime('%s','now') WHERE id=?",
            (new_sev, incident_id)
        )
        conn.commit()
        conn.close()

        self._add_event(incident_id, "escalated",
                        f"severity: {row['severity']} → {new_sev}")

        emoji, label = SEVERITY_LABELS.get(new_sev, ("⚪", "?"))
        await _send_log(
            f"{emoji} <b>Инцидент #{incident_id} эскалирован → {label}</b>"
        )

    async def close(self, incident_id: int, by_uid: int = 0,
                    postmortem: str = ""):
        conn = _db()
        conn.execute(
            "UPDATE ft_incidents SET status='closed', closed_at=strftime('%s','now'), "
            "postmortem=? WHERE id=?",
            (postmortem[:2000], incident_id)
        )
        conn.commit()
        conn.close()
        self._add_event(incident_id, "closed",
                        f"by={by_uid}, postmortem={bool(postmortem)}", by_uid)

    def get_open(self, cid: Optional[int] = None) -> List[dict]:
        conn = _db()
        if cid:
            rows = conn.execute(
                "SELECT * FROM ft_incidents WHERE status='open' AND cid=? "
                "ORDER BY severity DESC, opened_at DESC LIMIT 20",
                (cid,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ft_incidents WHERE status='open' "
                "ORDER BY severity DESC, opened_at DESC LIMIT 50"
            ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["affected_uids"] = json.loads(d.get("affected_uids") or "[]")
                d["actions_taken"] = json.loads(d.get("actions_taken") or "[]")
            except Exception:
                pass
            result.append(d)
        return result

    def get_timeline(self, incident_id: int) -> List[dict]:
        conn = _db()
        rows = conn.execute(
            "SELECT * FROM ft_incident_events WHERE incident_id=? ORDER BY ts ASC",
            (incident_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]


incident_manager = IncidentManager()


# ══════════════════════════════════════════════════════════════════════════
#  4. ⏱ RATE LIMITER
# ══════════════════════════════════════════════════════════════════════════

# Встроенные правила rate-limit
DEFAULT_RATE_RULES = {
    "messages":     {"limit": 20,  "window": 60,   "action": "mute_5min"},
    "media":        {"limit": 5,   "window": 60,   "action": "delete"},
    "mentions":     {"limit": 5,   "window": 60,   "action": "warn"},
    "commands":     {"limit": 10,  "window": 60,   "action": "mute_1min"},
    "forwards":     {"limit": 3,   "window": 60,   "action": "delete"},
    "stickers":     {"limit": 10,  "window": 60,   "action": "delete"},
    "new_accounts": {"limit": 5,   "window": 300,  "action": "quarantine"},
}

# Токен-бакет кэш: {(uid, cid, rule): (tokens, last_refill_ts)}
_token_buckets: Dict[Tuple[int, int, str], Tuple[float, float]] = {}
# Скользящее окно: {(uid, cid, rule): deque[ts]}
_sliding_windows: Dict[Tuple[int, int, str], deque] = defaultdict(
    lambda: deque(maxlen=500)
)
# Адаптивные пороги: {(uid, cid): violations_count}
_adaptive_penalties: Dict[Tuple[int, int], int] = defaultdict(int)


class RateLimiter:
    def _get_config(self, cid: int, rule: str) -> dict:
        conn = _db()
        row = conn.execute(
            "SELECT * FROM ft_rate_config WHERE cid=? AND rule=?",
            (cid, rule)
        ).fetchone()
        conn.close()
        if row:
            return {"limit": row["limit_count"], "window": row["window_secs"],
                    "action": row["action"]}
        return DEFAULT_RATE_RULES.get(rule, {"limit": 30, "window": 60,
                                              "action": "warn"})

    def check_sliding_window(self, uid: int, cid: int, rule: str) -> bool:
        """
        Проверяет скользящее окно.
        Возвращает True если лимит превышен.
        """
        cfg = self._get_config(cid, rule)
        limit = cfg["limit"]
        window = cfg["window"]

        key = (uid, cid, rule)
        now = time.time()
        w = _sliding_windows[key]

        # Чистим старые события
        while w and w[0] < now - window:
            w.popleft()

        if len(w) >= limit:
            return True  # лимит превышен

        w.append(now)
        return False

    def get_adaptive_limit(self, uid: int, cid: int, base_limit: int) -> int:
        """
        Адаптивный лимит — снижается с каждым нарушением.
        5 нарушений → лимит ×0.5, 10 нарушений → лимит ×0.25
        """
        violations = _adaptive_penalties.get((uid, cid), 0)
        if violations >= 10:
            return max(1, base_limit // 4)
        elif violations >= 5:
            return max(1, base_limit // 2)
        return base_limit

    async def handle_violation(self, uid: int, cid: int, rule: str,
                                message: Message):
        """Обрабатывает нарушение rate-limit"""
        cfg = self._get_config(cid, rule)
        action = cfg["action"]

        _adaptive_penalties[(uid, cid)] = _adaptive_penalties.get((uid, cid), 0) + 1
        violations = _adaptive_penalties[(uid, cid)]

        # Адаптивная эскалация при повторных нарушениях
        if violations >= 5 and action in ("warn", "delete"):
            action = "mute_5min"
        if violations >= 10:
            action = "mute_1hour"

        # Записываем нарушение
        conn = _db()
        window_start = int(time.time()) - cfg["window"]
        conn.execute(
            "INSERT INTO ft_rate_violations(uid,cid,rule,count,window_start) "
            "VALUES(?,?,?,?,?)",
            (uid, cid, rule, violations, window_start)
        )
        conn.commit()
        conn.close()

        forensics.record_event(uid, cid, "rate_limit_violation", {
            "rule": rule, "violations": violations, "action": action,
        })

        try:
            if action == "delete":
                await message.delete()
            elif action == "warn":
                await message.delete()
                sent = await _bot.send_message(
                    cid,
                    f"⚠️ {message.from_user.mention_html()}, "
                    f"медленнее ({rule}).",
                    parse_mode="HTML"
                )
                asyncio.create_task(_auto_delete(sent, 5))
            elif action.startswith("mute_"):
                await message.delete()
                duration_map = {
                    "mute_1min": 1, "mute_5min": 5,
                    "mute_1hour": 60, "mute_24h": 1440,
                }
                mins = duration_map.get(action, 5)
                until = datetime.now() + timedelta(minutes=mins)
                await _bot.restrict_chat_member(
                    cid, uid,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=until
                )
                sent = await _bot.send_message(
                    cid,
                    f"🔇 {message.from_user.mention_html()} "
                    f"замьючен на {mins} мин. (rate limit: {rule})",
                    parse_mode="HTML"
                )
                asyncio.create_task(_auto_delete(sent, 10))
            elif action == "quarantine":
                await quarantine.add(uid, cid,
                                     f"Rate limit: {rule}", level=1)
        except Exception as e:
            log.warning(f"rate_limit action error: {e}")

    async def check(self, message: Message) -> bool:
        """
        Проверяет все применимые правила для сообщения.
        Возвращает True если сообщение заблокировано.
        """
        if not message.from_user:
            return False
        if message.chat.type not in ("group", "supergroup"):
            return False

        uid = message.from_user.id
        cid = message.chat.id

        # Пропускаем администраторов
        try:
            member = await _bot.get_chat_member(cid, uid)
            if member.status in ("administrator", "creator"):
                return False
        except Exception:
            pass

        checks = [
            ("messages", True),
            ("media", bool(message.photo or message.video or
                           message.document or message.audio)),
            ("stickers", bool(message.sticker)),
            ("forwards", bool(message.forward_origin)),
            ("commands", bool(message.text and message.text.startswith("/"))),
            ("mentions", len(message.entities or []) > 0 and
             any(e.type == "mention" for e in (message.entities or []))),
        ]

        for rule, applies in checks:
            if not applies:
                continue
            if self.check_sliding_window(uid, cid, rule):
                await self.handle_violation(uid, cid, rule, message)
                return True

        return False

    def reset_user(self, uid: int, cid: int):
        """Сбрасывает счётчики пользователя"""
        keys_to_del = [k for k in _sliding_windows if k[0] == uid and k[1] == cid]
        for k in keys_to_del:
            del _sliding_windows[k]
        _adaptive_penalties.pop((uid, cid), None)

    def get_status(self, uid: int, cid: int) -> dict:
        """Возвращает текущий статус rate-limit пользователя"""
        violations = _adaptive_penalties.get((uid, cid), 0)
        windows = {}
        for rule in DEFAULT_RATE_RULES:
            key = (uid, cid, rule)
            if key in _sliding_windows:
                windows[rule] = len(_sliding_windows[key])
        return {"violations": violations, "current_counts": windows}


rate_limiter = RateLimiter()


# ══════════════════════════════════════════════════════════════════════════
#  5. 🔍 FORENSICS ENGINE
# ══════════════════════════════════════════════════════════════════════════

class ForensicsEngine:
    """Криминалистический движок — сохраняет и восстанавливает данные"""

    # Размер буфера сообщений в памяти перед сбросом в БД
    _BUFFER_SIZE = 50
    _msg_buffer: List[dict] = []

    def archive_message(self, message: Message):
        """Архивирует сообщение (вызывается для каждого входящего)"""
        if not message.from_user:
            return

        entry = {
            "msg_id": message.message_id,
            "uid": message.from_user.id,
            "cid": message.chat.id,
            "text": (message.text or message.caption or "")[:1000],
            "media_type": self._get_media_type(message),
            "entities": json.dumps(
                [{"type": e.type, "offset": e.offset, "length": e.length}
                 for e in (message.entities or [])]
            ),
            "ts": int(message.date.timestamp()) if message.date else int(time.time()),
        }
        self._msg_buffer.append(entry)

        if len(self._msg_buffer) >= self._BUFFER_SIZE:
            self._flush_buffer()

    def _flush_buffer(self):
        if not self._msg_buffer:
            return
        try:
            conn = _db()
            conn.executemany(
                "INSERT OR IGNORE INTO ft_message_archive"
                "(msg_id,uid,cid,text,media_type,entities,ts) "
                "VALUES(:msg_id,:uid,:cid,:text,:media_type,:entities,:ts)",
                self._msg_buffer
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"forensics flush: {e}")
        finally:
            self._msg_buffer.clear()

    @staticmethod
    def _get_media_type(message: Message) -> str:
        if message.photo:
            return "photo"
        if message.video:
            return "video"
        if message.document:
            return "document"
        if message.audio:
            return "audio"
        if message.sticker:
            return "sticker"
        if message.voice:
            return "voice"
        if message.animation:
            return "animation"
        return "text"

    def record_deletion(self, message: Message, reason: str,
                        deleted_by: int = 0):
        """Записывает факт удаления сообщения"""
        if not message.from_user:
            return
        try:
            conn = _db()
            conn.execute(
                "INSERT INTO ft_deletions(msg_id,uid,cid,text,reason,deleted_by) "
                "VALUES(?,?,?,?,?,?)",
                (
                    message.message_id,
                    message.from_user.id,
                    message.chat.id,
                    (message.text or message.caption or "")[:500],
                    reason[:200],
                    deleted_by,
                )
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug(f"record_deletion: {e}")

    def record_event(self, uid: int, cid: int, event_type: str,
                     payload: Optional[dict] = None):
        """Записывает событие в таймлайн"""
        try:
            conn = _db()
            conn.execute(
                "INSERT INTO ft_events_timeline(uid,cid,event_type,payload) "
                "VALUES(?,?,?,?)",
                (uid, cid, event_type, json.dumps(payload or {}))
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug(f"record_event: {e}")

    def get_timeline(self, uid: int, cid: int,
                     limit: int = 50) -> List[dict]:
        """Возвращает таймлайн событий пользователя"""
        self._flush_buffer()
        conn = _db()
        rows = conn.execute(
            "SELECT * FROM ft_events_timeline "
            "WHERE uid=? AND cid=? ORDER BY ts DESC LIMIT ?",
            (uid, cid, limit)
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["payload"] = json.loads(d.get("payload") or "{}")
            except Exception:
                pass
            result.append(d)
        return result

    def get_deleted_messages(self, uid: int, cid: int,
                             limit: int = 50) -> List[dict]:
        """Возвращает удалённые сообщения пользователя"""
        conn = _db()
        rows = conn.execute(
            "SELECT * FROM ft_deletions WHERE uid=? AND cid=? "
            "ORDER BY ts DESC LIMIT ?",
            (uid, cid, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_archived_messages(self, uid: int, cid: int,
                               limit: int = 100) -> List[dict]:
        """Возвращает архив сообщений пользователя"""
        self._flush_buffer()
        conn = _db()
        rows = conn.execute(
            "SELECT * FROM ft_message_archive WHERE uid=? AND cid=? "
            "ORDER BY ts DESC LIMIT ?",
            (uid, cid, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def export_user_report(self, uid: int, cid: int) -> str:
        """
        Генерирует полный forensics-отчёт по пользователю в текстовом формате.
        Используется для расследований и передачи модераторам.
        """
        self._flush_buffer()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Собираем данные
        timeline = self.get_timeline(uid, cid, limit=100)
        deleted = self.get_deleted_messages(uid, cid, limit=30)
        archived = self.get_archived_messages(uid, cid, limit=50)

        # Считаем статистику
        event_counts: Dict[str, int] = {}
        for ev in timeline:
            et = ev.get("event_type", "unknown")
            event_counts[et] = event_counts.get(et, 0) + 1

        lines = [
            f"╔══════════════════════════════════════╗",
            f"║  FORENSICS REPORT  ║  {now}",
            f"╚══════════════════════════════════════╝",
            f"",
            f"TARGET: uid={uid}  cid={cid}",
            f"",
            f"══ СТАТИСТИКА СОБЫТИЙ ══",
        ]
        for et, count in sorted(event_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {et}: {count}")

        lines += [
            f"",
            f"══ УДАЛЁННЫЕ СООБЩЕНИЯ ({len(deleted)}) ══",
        ]
        for d in deleted[:10]:
            ts = datetime.fromtimestamp(d["ts"]).strftime("%m/%d %H:%M")
            lines.append(
                f"  [{ts}] {d['reason']}: {d['text'][:80]}"
            )

        lines += [
            f"",
            f"══ ТАЙМЛАЙН СОБЫТИЙ (последние {len(timeline)}) ══",
        ]
        for ev in timeline[:20]:
            ts = datetime.fromtimestamp(ev["ts"]).strftime("%m/%d %H:%M")
            payload = ev.get("payload", {})
            payload_str = ", ".join(
                f"{k}={v}" for k, v in list(payload.items())[:3]
            )
            lines.append(f"  [{ts}] {ev['event_type']}: {payload_str}")

        lines += [
            f"",
            f"══ АРХИВ СООБЩЕНИЙ (последние {len(archived)}) ══",
        ]
        for msg in archived[:10]:
            ts = datetime.fromtimestamp(msg["ts"]).strftime("%m/%d %H:%M")
            media = f"[{msg['media_type']}] " if msg["media_type"] != "text" else ""
            lines.append(f"  [{ts}] {media}{msg['text'][:80]}")

        lines.append(f"\n══ КОНЕЦ ОТЧЁТА ══")
        return "\n".join(lines)

    async def cleanup_old_records(self, days: int = 90):
        """Удаляет записи старше N дней"""
        cutoff = int(time.time()) - days * 86400
        conn = _db()
        conn.execute(
            "DELETE FROM ft_message_archive WHERE archived_at < ?", (cutoff,)
        )
        conn.execute(
            "DELETE FROM ft_events_timeline WHERE ts < ?", (cutoff,)
        )
        deleted_count = conn.execute(
            "SELECT changes() as c"
        ).fetchone()["c"]
        conn.commit()
        conn.close()
        log.info(f"[FORENSICS] Очищено {deleted_count} старых записей (>{days} дней)")


forensics = ForensicsEngine()


# ══════════════════════════════════════════════════════════════════════════
#  6. 🛡 ACTIVE DEFENSE
# ══════════════════════════════════════════════════════════════════════════

# Canary token формат
_CANARY_ALPHABET = string.ascii_letters + string.digits
_CANARY_LENGTH = 32


class ActiveDefense:
    """
    Активная защита чата.

    Функции:
    - Локдаун чата (3 уровня)
    - Canary tokens (детекция утечек)
    - Decoy-сообщения (приманки внутри чата)
    - Автоматическая реакция на признаки атаки
    """

    def generate_canary(self, cid: int, created_by: int, label: str) -> str:
        """Генерирует уникальный canary-токен"""
        token = "".join(random.choices(_CANARY_ALPHABET, k=_CANARY_LENGTH))
        conn = _db()
        conn.execute(
            "INSERT INTO ft_canary_tokens(token,label,cid,created_by) VALUES(?,?,?,?)",
            (token, label[:100], cid, created_by)
        )
        conn.commit()
        conn.close()
        return token

    async def check_canary(self, text: str, uid: int, cid: int) -> bool:
        """Проверяет наличие canary-токена в тексте"""
        if len(text) < _CANARY_LENGTH:
            return False

        # Ищем токены в тексте
        conn = _db()
        tokens = conn.execute(
            "SELECT * FROM ft_canary_tokens WHERE triggered=0"
        ).fetchall()
        conn.close()

        for tok in tokens:
            if tok["token"] in text:
                # Токен сработал!
                conn = _db()
                conn.execute(
                    "UPDATE ft_canary_tokens SET triggered=1, triggered_by=?, "
                    "triggered_at=strftime('%s','now') WHERE token=?",
                    (uid, tok["token"])
                )
                conn.commit()
                conn.close()

                await _send_log(
                    f"🐦 <b>CANARY TOKEN TRIGGERED!</b>\n\n"
                    f"🏷 Метка: <b>{tok['label']}</b>\n"
                    f"👤 Обнаружен у: <code>{uid}</code>\n"
                    f"💬 Чат: <code>{cid}</code>\n"
                    f"🎯 Источник токена: чат <code>{tok['cid']}</code>\n\n"
                    f"<i>Возможна утечка информации из чата!</i>"
                )

                await incident_manager.create(
                    cid=tok["cid"],
                    kind="canary_triggered",
                    severity=4,
                    title=f"Canary token '{tok['label']}' сработал",
                    description=f"Токен обнаружен у uid={uid} в чате {cid}",
                    affected_uids=[uid],
                )
                return True
        return False

    async def lockdown(self, cid: int, level: int = 1,
                       reason: str = "", by_uid: int = 0,
                       auto: bool = False, duration_hours: int = 0):
        """
        Вводит локдаун чата.

        Уровни:
          1 = slowmode (30 сек между сообщениями)
          2 = только участники (запрет новым входить)
          3 = полное закрытие чата (нельзя писать никому)
        """
        expires_at = int(time.time()) + duration_hours * 3600 if duration_hours else 0

        conn = _db()
        conn.execute(
            "INSERT INTO ft_lockdowns(cid,active,level,reason,auto,started_at,expires_at) "
            "VALUES(?,1,?,?,?,strftime('%s','now'),?) "
            "ON CONFLICT(cid) DO UPDATE SET active=1,level=excluded.level,"
            "reason=excluded.reason,auto=excluded.auto,expires_at=excluded.expires_at",
            (cid, level, reason[:300], 1 if auto else 0, expires_at)
        )
        conn.execute(
            "INSERT INTO ft_defense_log(cid,action,reason,by_uid) VALUES(?,?,?,?)",
            (cid, f"lockdown_level_{level}", reason[:300], by_uid)
        )
        conn.commit()
        conn.close()

        forensics.record_event(0, cid, "lockdown_start",
                               {"level": level, "auto": auto, "reason": reason})

        try:
            if level == 1:
                # Slowmode 30 секунд
                await _bot.set_chat_slow_mode_delay(cid, 30)
                action_desc = "Включён slowmode (30 сек)"
            elif level == 2:
                # Запрет новых участников + slowmode
                await _bot.set_chat_slow_mode_delay(cid, 60)
                action_desc = "Замедление + ограничения"
            elif level >= 3:
                # Полное закрытие — никто не может писать
                await _bot.set_chat_permissions(
                    cid,
                    ChatPermissions(can_send_messages=False)
                )
                action_desc = "Чат полностью закрыт"
        except Exception as e:
            action_desc = f"Ошибка: {e}"

        lvl_labels = {1: "🟡 SLOWMODE", 2: "🟠 ОГРАНИЧЕНИЯ", 3: "🔴 ЗАКРЫТ"}
        auto_str = " (авто)" if auto else ""

        await _send_log(
            f"🔒 <b>Локдаун чата {lvl_labels.get(level, '?')}{auto_str}</b>\n\n"
            f"💬 <code>{cid}</code>\n"
            f"📋 {reason[:200]}\n"
            f"⚡ {action_desc}"
            + (f"\n⏱ На {duration_hours}ч" if duration_hours else ""),
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔓 Снять локдаун",
                    callback_data=f"ft:ld_unlock:{cid}"
                ),
                InlineKeyboardButton(
                    text="⬆️ Уровень выше",
                    callback_data=f"ft:ld_escalate:{cid}"
                ),
            ]])
        )

    async def unlock(self, cid: int, by_uid: int = 0):
        """Снимает локдаун"""
        conn = _db()
        conn.execute(
            "UPDATE ft_lockdowns SET active=0 WHERE cid=?", (cid,)
        )
        conn.execute(
            "INSERT INTO ft_defense_log(cid,action,by_uid) VALUES(?,?,?)",
            (cid, "lockdown_removed", by_uid)
        )
        conn.commit()
        conn.close()

        try:
            await _bot.set_chat_slow_mode_delay(cid, 0)
            await _bot.set_chat_permissions(
                cid,
                ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                    can_invite_users=True,
                )
            )
        except Exception as e:
            log.warning(f"unlock error: {e}")

        forensics.record_event(0, cid, "lockdown_end", {"by": by_uid})
        await _send_log(f"🔓 <b>Локдаун снят</b> — чат <code>{cid}</code>")

    def is_locked(self, cid: int) -> Optional[dict]:
        conn = _db()
        row = conn.execute(
            "SELECT * FROM ft_lockdowns WHERE cid=? AND active=1", (cid,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        return dict(row)

    async def _auto_lockdown_monitor(self):
        """Мониторинг авто-триггеров для локдауна"""
        while True:
            await asyncio.sleep(60)
            try:
                # Проверяем истёкшие локдауны
                now = int(time.time())
                conn = _db()
                expired = conn.execute(
                    "SELECT cid FROM ft_lockdowns "
                    "WHERE active=1 AND expires_at>0 AND expires_at<?",
                    (now,)
                ).fetchall()
                conn.close()

                for row in expired:
                    await self.unlock(row["cid"], by_uid=0)
                    log.info(f"[DEFENSE] Авто-снятие локдауна cid={row['cid']}")
            except Exception as e:
                log.error(f"auto_lockdown_monitor: {e}")


active_defense = ActiveDefense()


# ══════════════════════════════════════════════════════════════════════════
#  ГЛАВНЫЙ GATE — вызывается для каждого сообщения
# ══════════════════════════════════════════════════════════════════════════

async def gate(message: Message) -> bool:
    """
    Единая точка входа для всех проверок fortress.
    Вызывается из StatsMiddleware перед обработкой сообщения.
    Возвращает True если сообщение заблокировано и не должно обрабатываться.
    """
    if not message.from_user:
        return False
    if message.chat.type not in ("group", "supergroup"):
        return False

    uid = message.from_user.id
    cid = message.chat.id

    # Пропускаем администраторов
    try:
        m = await _bot.get_chat_member(cid, uid)
        if m.status in ("administrator", "creator"):
            forensics.archive_message(message)
            return False
    except Exception:
        pass

    # Архивируем для forensics
    forensics.archive_message(message)

    # 1. Canary check
    text = message.text or message.caption or ""
    if text and len(text) >= _CANARY_LENGTH:
        await active_defense.check_canary(text, uid, cid)

    # 2. Honeypot check
    if await check_honeypot(message):
        return True

    # 3. Rate limiting
    if await rate_limiter.check(message):
        return True

    # 4. Карантин — проверяем сообщения от пользователей в карантине
    qstatus = quarantine.is_quarantined(uid, cid)
    if qstatus:
        level = qstatus.get("level", 1)
        if level >= 3:
            # Полный карантин — удаляем все сообщения
            try:
                await message.delete()
            except Exception:
                pass
            return True

    return False


# ══════════════════════════════════════════════════════════════════════════
#  КОМАНДЫ БОТА
# ══════════════════════════════════════════════════════════════════════════

async def cmd_honeypot(message: Message):
    """/honeypot [on|off|add kind trigger|list|stats]"""
    cid = message.chat.id
    args = (message.text or "").split()[1:]

    if not args or args[0] == "stats":
        stats = get_honeypot_stats(cid)
        hps = _get_honeypots(cid)
        hp_list = "\n".join(
            f"  • [{hp['kind']}] <code>{hp['trigger']}</code> → {hp['action']} "
            f"({hp['hits']} хитов)"
            for hp in hps[:10]
        ) or "  нет ловушек"
        await message.answer(
            f"🍯 <b>Honeypot System</b>\n\n"
            f"Хитов сегодня: <b>{stats['today_hits']}</b>\n"
            f"Всего хитов: <b>{stats['total_hits']}</b>\n\n"
            f"<b>Активные ловушки:</b>\n{hp_list}\n\n"
            f"/honeypot add command /secretcmd ban\n"
            f"/honeypot add keyword 'купить подписку' quarantine\n"
            f"/honeypot add pattern 'https?://t\\.me/\\+' mute",
            parse_mode="HTML"
        )
        return

    if args[0] == "add" and len(args) >= 3:
        kind = args[1]
        if kind not in ("command", "keyword", "link", "pattern", "invite"):
            await message.answer("⚠️ Типы: command, keyword, link, pattern, invite")
            return
        trigger = args[2]
        action = args[3] if len(args) > 3 else "ban"
        if action not in ("ban", "kick", "mute", "quarantine", "log"):
            await message.answer("⚠️ Действия: ban, kick, mute, quarantine, log")
            return
        hid = _add_honeypot(cid, kind, trigger, action)
        await message.answer(
            f"🍯 Ловушка #{hid} добавлена\n"
            f"Тип: <b>{kind}</b>\n"
            f"Триггер: <code>{trigger}</code>\n"
            f"Действие: <b>{action}</b>",
            parse_mode="HTML"
        )

    elif args[0] == "list":
        hps = _get_honeypots(cid)
        if not hps:
            await message.answer("🍯 Ловушек нет")
            return
        text = "🍯 <b>Ловушки чата:</b>\n\n"
        for hp in hps:
            text += (f"#{hp['id']} [{hp['kind']}] "
                     f"<code>{hp['trigger']}</code> → {hp['action']} "
                     f"({hp['hits']} хитов)\n")
        await message.answer(text, parse_mode="HTML")
    else:
        await message.answer(
            "🍯 /honeypot stats|list|add [kind] [trigger] [action]"
        )


async def cmd_quarantine(message: Message):
    """/quarantine [list|release uid|stats]"""
    cid = message.chat.id
    args = (message.text or "").split()[1:]

    if not args or args[0] == "list":
        qlist = quarantine.get_list(cid)
        if not qlist:
            await message.answer("🔒 Карантин пуст")
            return
        text = f"🔒 <b>Карантин ({len(qlist)} чел.):</b>\n\n"
        for q in qlist[:15]:
            lvl = quarantine.LEVELS.get(q["level"], {})
            emoji = lvl.get("emoji", "❓")
            exp = datetime.fromtimestamp(q["expires_at"]).strftime("%H:%M") \
                if q["expires_at"] else "∞"
            text += (f"{emoji} <code>{q['uid']}</code> — "
                     f"ур.{q['level']} до {exp}\n"
                     f"  📋 {q['reason'][:60]}\n")
        await message.answer(text, parse_mode="HTML")

    elif args[0] == "release" and len(args) >= 2:
        try:
            uid = int(args[1])
            await quarantine.release(uid, cid, by_uid=message.from_user.id)
            await message.answer(f"✅ <code>{uid}</code> освобождён из карантина",
                                 parse_mode="HTML")
        except ValueError:
            await message.answer("⚠️ /quarantine release [uid]")

    elif args[0] == "add" and len(args) >= 2:
        try:
            uid = int(args[1])
            level = int(args[2]) if len(args) > 2 else 1
            reason = " ".join(args[3:]) or "Ручной карантин"
            await quarantine.add(uid, cid, reason, level=level,
                                 by_uid=message.from_user.id)
            await message.answer(
                f"🔒 <code>{uid}</code> помещён в карантин (ур.{level})",
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("⚠️ /quarantine add [uid] [level 1-3] [reason]")
    else:
        await message.answer(
            "🔒 /quarantine list | release [uid] | add [uid] [level] [reason]"
        )


async def cmd_incident(message: Message):
    """/incident [list|show id|close id [postmortem]]"""
    cid = message.chat.id
    args = (message.text or "").split()[1:]

    if not args or args[0] == "list":
        incidents = incident_manager.get_open(cid)
        if not incidents:
            await message.answer("✅ Открытых инцидентов нет")
            return
        text = f"🚨 <b>Инциденты ({len(incidents)}):</b>\n\n"
        for inc in incidents[:10]:
            emoji, label = SEVERITY_LABELS.get(inc["severity"], ("⚪", "?"))
            dt = datetime.fromtimestamp(inc["opened_at"]).strftime("%d.%m %H:%M")
            text += (f"{emoji} <b>#{inc['id']}</b> [{label}] {inc['title'][:60]}\n"
                     f"   {dt} | {inc['kind']}\n\n")
        await message.answer(text, parse_mode="HTML")

    elif args[0] == "show" and len(args) >= 2:
        try:
            iid = int(args[1])
            conn = _db()
            inc = conn.execute(
                "SELECT * FROM ft_incidents WHERE id=?", (iid,)
            ).fetchone()
            conn.close()
            if not inc:
                await message.answer(f"❌ Инцидент #{iid} не найден")
                return
            inc = dict(inc)
            timeline = incident_manager.get_timeline(iid)
            emoji, label = SEVERITY_LABELS.get(inc["severity"], ("⚪", "?"))
            tl_text = "\n".join(
                f"  [{datetime.fromtimestamp(e['ts']).strftime('%H:%M')}] "
                f"{e['event_type']}: {e['description'][:60]}"
                for e in timeline[-5:]
            )
            await message.answer(
                f"{emoji} <b>Инцидент #{iid} — {label}</b>\n\n"
                f"<b>{inc['title']}</b>\n"
                f"Статус: {inc['status']}\n"
                f"Тип: {inc['kind']}\n\n"
                f"{inc['description'][:300]}\n\n"
                f"<b>Последние события:</b>\n{tl_text}",
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("⚠️ /incident show [id]")

    elif args[0] == "close" and len(args) >= 2:
        try:
            iid = int(args[1])
            postmortem = " ".join(args[2:])
            await incident_manager.close(iid, by_uid=message.from_user.id,
                                          postmortem=postmortem)
            await message.answer(f"✅ Инцидент #{iid} закрыт")
        except ValueError:
            await message.answer("⚠️ /incident close [id] [postmortem]")
    else:
        await message.answer("🚨 /incident list | show [id] | close [id] [postmortem]")


async def cmd_ratelimit(message: Message):
    """/ratelimit [status|reset uid|config rule limit window]"""
    cid = message.chat.id
    args = (message.text or "").split()[1:]

    if not args or args[0] == "status":
        target = message.reply_to_message.from_user.id \
            if message.reply_to_message else message.from_user.id
        status = rate_limiter.get_status(target, cid)
        counts_text = "\n".join(
            f"  {rule}: {count}"
            for rule, count in status["current_counts"].items()
        ) or "  нет активных"
        await message.answer(
            f"⏱ <b>Rate Limit — <code>{target}</code></b>\n\n"
            f"Нарушений всего: <b>{status['violations']}</b>\n\n"
            f"Текущие счётчики:\n{counts_text}",
            parse_mode="HTML"
        )

    elif args[0] == "reset" and len(args) >= 2:
        try:
            uid = int(args[1])
            rate_limiter.reset_user(uid, cid)
            await message.answer(f"✅ Счётчики <code>{uid}</code> сброшены",
                                 parse_mode="HTML")
        except ValueError:
            await message.answer("⚠️ /ratelimit reset [uid]")

    elif args[0] == "config" and len(args) >= 4:
        rule = args[1]
        try:
            limit = int(args[2])
            window = int(args[3])
            action = args[4] if len(args) > 4 else "warn"
            conn = _db()
            conn.execute(
                "INSERT INTO ft_rate_config(cid,rule,limit_count,window_secs,action) "
                "VALUES(?,?,?,?,?) ON CONFLICT(cid,rule) DO UPDATE SET "
                "limit_count=excluded.limit_count, window_secs=excluded.window_secs, "
                "action=excluded.action",
                (cid, rule, limit, window, action)
            )
            conn.commit()
            conn.close()
            await message.answer(
                f"✅ Rate limit настроен\n"
                f"Правило: <b>{rule}</b>\n"
                f"Лимит: {limit} за {window}с → {action}",
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer(
                "⚠️ /ratelimit config [rule] [limit] [window_secs] [action]"
            )
    else:
        rules_text = "\n".join(
            f"  {r}: {v['limit']}/{v['window']}s → {v['action']}"
            for r, v in DEFAULT_RATE_RULES.items()
        )
        await message.answer(
            f"⏱ <b>Rate Limiter</b>\n\n"
            f"Правила по умолчанию:\n{rules_text}\n\n"
            f"/ratelimit status [@user]\n"
            f"/ratelimit reset [uid]\n"
            f"/ratelimit config [rule] [limit] [window] [action]",
            parse_mode="HTML"
        )


async def cmd_forensics(message: Message):
    """/forensics [timeline uid|export uid|deleted uid|canary label]"""
    cid = message.chat.id
    args = (message.text or "").split()[1:]

    if not args:
        await message.answer(
            "🔍 <b>Forensics Engine</b>\n\n"
            "/forensics timeline [uid] — таймлайн событий\n"
            "/forensics deleted [uid] — удалённые сообщения\n"
            "/forensics export [uid] — полный отчёт\n"
            "/forensics canary [label] — создать canary-токен",
            parse_mode="HTML"
        )
        return

    target = message.reply_to_message.from_user.id \
        if message.reply_to_message else None
    if not target and len(args) >= 2:
        try:
            target = int(args[1])
        except ValueError:
            pass

    if args[0] == "timeline":
        if not target:
            await message.answer("⚠️ Укажи uid или ответь на сообщение")
            return
        timeline = forensics.get_timeline(target, cid, limit=20)
        if not timeline:
            await message.answer(f"🔍 Нет событий для <code>{target}</code>",
                                 parse_mode="HTML")
            return
        text = f"🔍 <b>Timeline <code>{target}</code> ({len(timeline)}):</b>\n\n"
        for ev in timeline:
            ts = datetime.fromtimestamp(ev["ts"]).strftime("%d.%m %H:%M")
            payload = ev.get("payload", {})
            p_str = ", ".join(f"{k}={v}" for k, v in list(payload.items())[:2])
            text += f"  [{ts}] {ev['event_type']}: {p_str}\n"
        await message.answer(text, parse_mode="HTML")

    elif args[0] == "deleted":
        if not target:
            await message.answer("⚠️ Укажи uid или ответь на сообщение")
            return
        deleted = forensics.get_deleted_messages(target, cid, limit=20)
        if not deleted:
            await message.answer(f"🔍 Нет удалённых для <code>{target}</code>",
                                 parse_mode="HTML")
            return
        text = f"🗑 <b>Удалённые (<code>{target}</code>):</b>\n\n"
        for d in deleted:
            ts = datetime.fromtimestamp(d["ts"]).strftime("%d.%m %H:%M")
            text += f"[{ts}] <i>{d['reason']}</i>: {d['text'][:80]}\n"
        await message.answer(text, parse_mode="HTML")

    elif args[0] == "export":
        if not target:
            await message.answer("⚠️ Укажи uid или ответь на сообщение")
            return
        await message.answer("⏳ Генерирую отчёт...")
        report = forensics.export_user_report(target, cid)
        # Отправляем как файл
        import io
        buf = io.BytesIO(report.encode("utf-8"))
        buf.name = f"forensics_{target}_{int(time.time())}.txt"
        await _bot.send_document(
            message.chat.id,
            buf,
            caption=f"🔍 Forensics Report — uid {target}"
        )

    elif args[0] == "canary":
        label = " ".join(args[1:]) or f"canary_{int(time.time())}"
        token = active_defense.generate_canary(cid, message.from_user.id, label)
        await message.answer(
            f"🐦 <b>Canary Token создан</b>\n\n"
            f"Метка: {label}\n"
            f"Токен: <code>{token}</code>\n\n"
            f"<i>Вставьте этот токен в секретный документ или сообщение. "
            f"Если он появится в чате — получите мгновенный алерт.</i>",
            parse_mode="HTML"
        )
    else:
        await message.answer("⚠️ /forensics timeline|deleted|export|canary")


async def cmd_defense(message: Message):
    """/defense [status|lockdown level|unlock|canary list]"""
    cid = message.chat.id
    args = (message.text or "").split()[1:]

    if not args or args[0] == "status":
        lock = active_defense.is_locked(cid)
        if lock:
            lvl_labels = {1: "🟡 SLOWMODE", 2: "🟠 ОГРАНИЧЕНИЯ", 3: "🔴 ЗАКРЫТ"}
            started = datetime.fromtimestamp(lock["started_at"]).strftime("%d.%m %H:%M")
            status_text = (
                f"⚠️ Активный локдаун: {lvl_labels.get(lock['level'], '?')}\n"
                f"С: {started}\n"
                f"Причина: {lock['reason'][:100]}"
            )
        else:
            status_text = "✅ Локдауна нет"

        conn = _db()
        canary_count = conn.execute(
            "SELECT COUNT(*) as c FROM ft_canary_tokens WHERE cid=? AND triggered=0",
            (cid,)
        ).fetchone()["c"]
        triggered = conn.execute(
            "SELECT COUNT(*) as c FROM ft_canary_tokens WHERE cid=? AND triggered=1",
            (cid,)
        ).fetchone()["c"]
        conn.close()

        await message.answer(
            f"🛡 <b>Active Defense</b>\n\n"
            f"{status_text}\n\n"
            f"🐦 Canary tokens: {canary_count} активных, {triggered} сработавших\n\n"
            f"/defense lockdown [1|2|3] [reason] — локдаун\n"
            f"/defense unlock — снять локдаун",
            parse_mode="HTML"
        )

    elif args[0] == "lockdown":
        level = int(args[1]) if len(args) > 1 and args[1].isdigit() else 1
        reason = " ".join(args[2:]) or "Ручной локдаун"
        await active_defense.lockdown(
            cid, level=level, reason=reason,
            by_uid=message.from_user.id
        )
        await message.answer(f"🔒 Локдаун уровня {level} включён")

    elif args[0] == "unlock":
        await active_defense.unlock(cid, by_uid=message.from_user.id)
        await message.answer("🔓 Локдаун снят")
    else:
        await message.answer("🛡 /defense status | lockdown [1-3] | unlock")


# ══════════════════════════════════════════════════════════════════════════
#  CALLBACK HANDLERS
# ══════════════════════════════════════════════════════════════════════════

async def cb_fortress(call: CallbackQuery):
    """Обработчик всех callback'ов fortress"""
    parts = call.data.split(":")
    action = parts[1]

    if action == "hp_ok":
        uid, cid = int(parts[2]), int(parts[3])
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer("✅ Действие подтверждено")

    elif action == "hp_undo":
        uid, cid, orig_action = int(parts[2]), int(parts[3]), parts[4]
        try:
            if orig_action == "ban":
                await _bot.unban_chat_member(cid, uid)
            elif orig_action == "mute":
                await _bot.restrict_chat_member(
                    cid, uid,
                    permissions=ChatPermissions(
                        can_send_messages=True,
                        can_send_media_messages=True,
                        can_send_other_messages=True,
                    )
                )
            await call.answer(f"↩️ Действие отменено для {uid}")
        except Exception as e:
            await call.answer(f"Ошибка: {e}")
        await call.message.edit_reply_markup(reply_markup=None)

    elif action == "qr_release":
        uid, cid = int(parts[2]), int(parts[3])
        await quarantine.release(uid, cid, by_uid=call.from_user.id)
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer(f"✅ {uid} освобождён из карантина")

    elif action == "qr_escalate":
        uid, cid = int(parts[2]), int(parts[3])
        await quarantine.escalate(uid, cid, by_uid=call.from_user.id)
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer(f"⬆️ Уровень карантина повышен")

    elif action == "qr_ban":
        uid, cid = int(parts[2]), int(parts[3])
        try:
            await _bot.ban_chat_member(cid, uid)
            await quarantine.release(uid, cid, by_uid=call.from_user.id)
            await call.answer(f"🔨 {uid} забанен")
        except Exception as e:
            await call.answer(f"Ошибка: {e}")
        await call.message.edit_reply_markup(reply_markup=None)

    elif action == "inc_close":
        iid = int(parts[2])
        await incident_manager.close(iid, by_uid=call.from_user.id)
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer(f"✅ Инцидент #{iid} закрыт")

    elif action == "inc_escalate":
        iid = int(parts[2])
        await incident_manager.escalate(iid)
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer(f"⬆️ Инцидент #{iid} эскалирован")

    elif action == "inc_lockdown":
        iid, cid = int(parts[2]), int(parts[3])
        await active_defense.lockdown(
            cid, level=2,
            reason=f"Инцидент #{iid}",
            by_uid=call.from_user.id
        )
        await call.answer(f"🔒 Локдаун чата включён")

    elif action == "ld_unlock":
        cid = int(parts[2])
        await active_defense.unlock(cid, by_uid=call.from_user.id)
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer("🔓 Локдаун снят")

    elif action == "ld_escalate":
        cid = int(parts[2])
        lock = active_defense.is_locked(cid)
        new_level = min(3, (lock["level"] + 1) if lock else 2)
        await active_defense.lockdown(
            cid, level=new_level,
            reason="Эскалация",
            by_uid=call.from_user.id
        )
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer(f"⬆️ Локдаун → уровень {new_level}")

    else:
        await call.answer()


# ══════════════════════════════════════════════════════════════════════════
#  DASHBOARD API
# ══════════════════════════════════════════════════════════════════════════

def dashboard_get_incidents(cid: Optional[int] = None,
                             status: str = "open") -> List[dict]:
    return incident_manager.get_open(cid)


def dashboard_get_quarantine(cid: int) -> List[dict]:
    return quarantine.get_list(cid)


def dashboard_get_honeypot_stats(cid: int) -> dict:
    return get_honeypot_stats(cid)


def dashboard_get_rate_violations(cid: int, limit: int = 100) -> List[dict]:
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM ft_rate_violations WHERE cid=? ORDER BY ts DESC LIMIT ?",
        (cid, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def dashboard_get_lockdown_status(cid: int) -> Optional[dict]:
    return active_defense.is_locked(cid)


def dashboard_get_defense_log(cid: int, limit: int = 50) -> List[dict]:
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM ft_defense_log WHERE cid=? ORDER BY ts DESC LIMIT ?",
        (cid, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def dashboard_get_overview() -> dict:
    """Сводная статистика для дашборда"""
    conn = _db()
    incidents_open = conn.execute(
        "SELECT COUNT(*) as c FROM ft_incidents WHERE status='open'"
    ).fetchone()["c"]
    incidents_critical = conn.execute(
        "SELECT COUNT(*) as c FROM ft_incidents WHERE status='open' AND severity=4"
    ).fetchone()["c"]
    quarantine_total = conn.execute(
        "SELECT COUNT(*) as c FROM ft_quarantine"
    ).fetchone()["c"]
    hp_hits_today = conn.execute(
        "SELECT COUNT(*) as c FROM ft_honeypot_hits "
        "WHERE date(ts,'unixepoch')=date('now')"
    ).fetchone()["c"]
    rate_violations_today = conn.execute(
        "SELECT COUNT(*) as c FROM ft_rate_violations "
        "WHERE date(ts,'unixepoch')=date('now')"
    ).fetchone()["c"]
    lockdowns_active = conn.execute(
        "SELECT COUNT(*) as c FROM ft_lockdowns WHERE active=1"
    ).fetchone()["c"]
    canary_triggered = conn.execute(
        "SELECT COUNT(*) as c FROM ft_canary_tokens WHERE triggered=1"
    ).fetchone()["c"]
    conn.close()

    return {
        "incidents_open": incidents_open,
        "incidents_critical": incidents_critical,
        "quarantine_count": quarantine_total,
        "honeypot_hits_today": hp_hits_today,
        "rate_violations_today": rate_violations_today,
        "lockdowns_active": lockdowns_active,
        "canary_triggered": canary_triggered,
    }


# ══════════════════════════════════════════════════════════════════════════
#  ФОНОВЫЕ ЗАДАЧИ
# ══════════════════════════════════════════════════════════════════════════

async def _background_forensics_cleanup():
    """Каждую неделю чистит старые forensics-записи"""
    while True:
        await asyncio.sleep(7 * 24 * 3600)
        try:
            await forensics.cleanup_old_records(days=90)
        except Exception as e:
            log.error(f"forensics_cleanup: {e}")


async def _background_flush():
    """Каждые 30 секунд сбрасывает буфер forensics"""
    while True:
        await asyncio.sleep(30)
        try:
            forensics._flush_buffer()
        except Exception as e:
            log.error(f"forensics_flush: {e}")


# ══════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ
# ══════════════════════════════════════════════════════════════════════════

async def _send_log(text: str, kb: Optional[InlineKeyboardMarkup] = None):
    if not _bot:
        return
    if _log_channel:
        try:
            await _bot.send_message(
                _log_channel, text, parse_mode="HTML", reply_markup=kb
            )
        except Exception as e:
            log.warning(f"[FORTRESS] log_channel: {e}")
    for aid in _admin_ids:
        try:
            await _bot.send_message(aid, text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass


async def _auto_delete(msg: Message, delay: int):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass


def _register_handlers(dp: Dispatcher):
    dp.message.register(cmd_honeypot,   Command("honeypot"))
    dp.message.register(cmd_quarantine, Command("quarantine"))
    dp.message.register(cmd_incident,   Command("incident"))
    dp.message.register(cmd_ratelimit,  Command("ratelimit"))
    dp.message.register(cmd_forensics,  Command("forensics"))
    dp.message.register(cmd_defense,    Command("defense"))
    dp.callback_query.register(cb_fortress, F.data.startswith("ft:"))
    log.info("✅ fortress.py хендлеры зарегистрированы")


# ══════════════════════════════════════════════════════════════════════════
#  ИНИЦИАЛИЗАЦИЯ
# ══════════════════════════════════════════════════════════════════════════

async def init(bot: Bot, dp: Dispatcher,
               admin_ids: Optional[Set[int]] = None,
               log_channel: int = 0):
    """
    Инициализация модуля fortress.

    В bot.py:
        import fortress
        await fortress.init(bot, dp, admin_ids=ADMIN_IDS, log_channel=LOG_CHANNEL)

    В StatsMiddleware (перед основной логикой):
        if await fortress.gate(message):
            return

    При удалении сообщений:
        fortress.forensics.record_deletion(message, reason="antispam")

    При входе пользователя:
        await fortress.quarantine.check_new_member(message, member)
    """
    global _bot, _admin_ids, _log_channel
    _bot = bot
    _admin_ids = admin_ids or set()
    _log_channel = log_channel

    _init_tables()
    _register_handlers(dp)

    asyncio.create_task(quarantine._auto_expire_loop())
    asyncio.create_task(active_defense._auto_lockdown_monitor())
    asyncio.create_task(_background_forensics_cleanup())
    asyncio.create_task(_background_flush())

    log.info("✅ fortress.py инициализирован — 6 подсистем активны")

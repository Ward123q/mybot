# -*- coding: utf-8 -*-
"""
security_features.py — Расширенные фичи защиты:
  1. 📸 Проверка аватара при входе
  2. 🔗 Расширенный антилинк с белым списком доменов
  3. 🌍 Глобальные защитные фичи (синхронизация банов, авто-очистка, вотчлист)

Подключение в bot.py:
    import security_features as sf
    # В main():
    await sf.init(bot, dp, ADMIN_IDS, LOG_CHANNEL_ID)
    # В on_new_member (после антирейда):
    await sf.check_new_member(message, member)
    # В StatsMiddleware (в конце, перед return):
    if await sf.check_message(event):
        return
"""
import asyncio
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, ChatPermissions,
    InlineKeyboardMarkup, InlineKeyboardButton
)

log = logging.getLogger(__name__)

_bot: Optional[Bot]  = None
_admin_ids: set      = set()
_log_channel: int    = 0


# ══════════════════════════════════════════════════════════════
#  БД
# ══════════════════════════════════════════════════════════════

def _db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_tables():
    conn = _db()
    conn.executescript("""
    -- Настройки проверки аватара
    CREATE TABLE IF NOT EXISTS avatar_check_cfg (
        cid      INTEGER PRIMARY KEY,
        enabled  INTEGER DEFAULT 0,
        action   TEXT    DEFAULT 'kick',
        mute_mins INTEGER DEFAULT 0
    );
    -- Настройки антилинк
    CREATE TABLE IF NOT EXISTS antilink_cfg (
        cid         INTEGER PRIMARY KEY,
        enabled     INTEGER DEFAULT 0,
        whitelist   TEXT    DEFAULT '[]',
        action      TEXT    DEFAULT 'delete',
        warn_on_del INTEGER DEFAULT 1,
        allow_admins INTEGER DEFAULT 1,
        block_tg_invites INTEGER DEFAULT 1,
        block_masked INTEGER DEFAULT 1
    );
    -- Глобальный вотчлист (наблюдение без бана)
    CREATE TABLE IF NOT EXISTS watchlist (
        uid      INTEGER PRIMARY KEY,
        reason   TEXT,
        added_by TEXT,
        ts       INTEGER DEFAULT (strftime('%s','now'))
    );
    -- Лог нарушений антилинк
    CREATE TABLE IF NOT EXISTS antilink_log (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        cid      INTEGER,
        uid      INTEGER,
        username TEXT,
        url      TEXT,
        action   TEXT,
        ts       INTEGER DEFAULT (strftime('%s','now'))
    );
    -- Лог проверок аватара
    CREATE TABLE IF NOT EXISTS avatar_check_log (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        cid      INTEGER,
        uid      INTEGER,
        name     TEXT,
        action   TEXT,
        ts       INTEGER DEFAULT (strftime('%s','now'))
    );
    -- Синхронизация банов между чатами {uid: [cid1, cid2]}
    CREATE TABLE IF NOT EXISTS cross_ban_sync (
        cid      INTEGER PRIMARY KEY,
        enabled  INTEGER DEFAULT 0
    );
    """)
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════
#  ИНИЦИАЛИЗАЦИЯ
# ══════════════════════════════════════════════════════════════

async def init(bot: Bot, dp: Dispatcher, admin_ids: set, log_channel: int = 0):
    global _bot, _admin_ids, _log_channel
    _bot       = bot
    _admin_ids = admin_ids
    _log_channel = log_channel
    _init_tables()
    _register_handlers(dp)
    asyncio.create_task(_watchlist_notify_loop())
    log.info("✅ security_features.py инициализирован")


# ══════════════════════════════════════════════════════════════
#  1. 📸 ПРОВЕРКА АВАТАРА ПРИ ВХОДЕ
# ══════════════════════════════════════════════════════════════

def _avatar_cfg(cid: int) -> dict:
    conn = _db()
    row = conn.execute(
        "SELECT * FROM avatar_check_cfg WHERE cid=?", (cid,)
    ).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"enabled": False, "action": "kick", "mute_mins": 0}


def _avatar_save(cid: int, cfg: dict):
    conn = _db()
    conn.execute(
        "INSERT INTO avatar_check_cfg (cid,enabled,action,mute_mins) VALUES (?,?,?,?) "
        "ON CONFLICT(cid) DO UPDATE SET enabled=excluded.enabled, "
        "action=excluded.action, mute_mins=excluded.mute_mins",
        (cid, 1 if cfg.get("enabled") else 0,
         cfg.get("action", "kick"), cfg.get("mute_mins", 0))
    )
    conn.commit()
    conn.close()


async def check_new_member(message: Message, member) -> bool:
    """
    Вызывается при каждом входе. True = пользователь обработан (кик/мут).
    Выполняет: проверку аватара + вотчлист.
    """
    cid = message.chat.id
    uid = member.id

    # ── Проверка вотчлиста ─────────────────────────────────────
    conn = _db()
    wl = conn.execute("SELECT reason FROM watchlist WHERE uid=?", (uid,)).fetchone()
    conn.close()
    if wl:
        await _send_log(
            f"👁 <b>ВОТЧЛИСТ — вступил в чат</b>\n\n"
            f"👤 {member.full_name} (<code>{uid}</code>)\n"
            f"💬 {message.chat.title}\n"
            f"📋 Причина слежки: {wl['reason']}",
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔨 Забанить", callback_data=f"sf:wl_ban:{cid}:{uid}"),
                InlineKeyboardButton(text="✅ Снять с наблюдения", callback_data=f"sf:wl_remove:{uid}"),
            ]])
        )

    # ── Проверка аватара ───────────────────────────────────────
    cfg = _avatar_cfg(cid)
    if not cfg.get("enabled"):
        return False

    try:
        photos = await _bot.get_user_profile_photos(uid, limit=1)
        has_avatar = photos.total_count > 0
    except Exception as e:
        log.warning(f"Не смог получить фото {uid}: {e}")
        return False

    if has_avatar:
        return False  # всё хорошо

    # Нет аватара — применяем действие
    action   = cfg.get("action", "kick")
    mute_mins = cfg.get("mute_mins", 0)
    name     = member.full_name
    cid_title = message.chat.title or str(cid)

    action_text = ""
    kicked = False

    try:
        if action == "kick":
            await _bot.ban_chat_member(cid, uid)
            await _bot.unban_chat_member(cid, uid)
            action_text = "👢 Кикнут"
            kicked = True

        elif action == "ban":
            await _bot.ban_chat_member(cid, uid)
            action_text = "🔨 Забанен"
            kicked = True

        elif action == "mute":
            mins = mute_mins or 60
            until = datetime.now() + timedelta(minutes=mins)
            await _bot.restrict_chat_member(
                cid, uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until
            )
            action_text = f"🔇 Замьючен на {mins} мин."

        elif action == "warn":
            action_text = "⚠️ Предупреждён"

    except Exception as e:
        action_text = f"❌ Ошибка: {e}"

    # Уведомление в чат
    kb_chat = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"sf:av_approve:{cid}:{uid}"),
        InlineKeyboardButton(text="🔨 Забанить", callback_data=f"sf:av_ban:{cid}:{uid}"),
    ]]) if not kicked else None

    try:
        sent = await message.answer(
            f"📸 <b>{name}</b> — нет аватара.\n{action_text}.",
            parse_mode="HTML",
            reply_markup=kb_chat
        )
        asyncio.create_task(_auto_delete(sent, 60))
    except:
        pass

    # Лог в БД
    conn = _db()
    conn.execute(
        "INSERT INTO avatar_check_log (cid,uid,name,action) VALUES (?,?,?,?)",
        (cid, uid, name, action)
    )
    conn.commit()
    conn.close()

    # Лог-канал
    await _send_log(
        f"📸 <b>Нет аватара</b>\n\n"
        f"👤 {name} (<code>{uid}</code>)\n"
        f"💬 {cid_title}\n"
        f"{action_text}"
    )

    return kicked


# ══════════════════════════════════════════════════════════════
#  2. 🔗 РАСШИРЕННЫЙ АНТИЛИНК
# ══════════════════════════════════════════════════════════════

# Кулдаун предупреждений: {cid:uid: ts}
_antilink_warn_cd: dict = {}

# Стандартный белый список
DEFAULT_WHITELIST = [
    "t.me", "telegram.me", "telegram.org",
    "youtube.com", "youtu.be",
    "wikipedia.org",
    "github.com",
]

# Паттерны для определения ссылок (включая замаскированные)
_URL_PATTERN = re.compile(
    r'(?:https?://|www\.|t\.me/|tg://)'
    r'(?:[a-zA-Z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
    re.IGNORECASE
)

# Паттерны Telegram-инвайтов
_TG_INVITE_PATTERN = re.compile(
    r'(?:t\.me/(?:joinchat/|\+)|telegram\.(?:me|dog)/joinchat/|tg://join\?invite=)'
    r'[a-zA-Z0-9_\-]+',
    re.IGNORECASE
)

# Unicode-спуфинг (похожие символы для обхода)
_UNICODE_REPLACEMENTS = {
    'а': 'a', 'е': 'e', 'о': 'o', 'р': 'p', 'с': 'c',
    'у': 'y', 'х': 'x', 'ё': 'e', 'і': 'i', 'ї': 'i',
    'ℂ': 'c', 'ℍ': 'h', 'ℕ': 'n', 'ℙ': 'p', 'ℚ': 'q',
    '⒞': 'c', '①': '1', '②': '2', '③': '3',
}


def _antilink_cfg(cid: int) -> dict:
    conn = _db()
    row = conn.execute("SELECT * FROM antilink_cfg WHERE cid=?", (cid,)).fetchone()
    conn.close()
    if row:
        d = dict(row)
        try:
            d["whitelist"] = json.loads(d.get("whitelist") or "[]")
        except:
            d["whitelist"] = []
        return d
    return {
        "enabled": False,
        "whitelist": list(DEFAULT_WHITELIST),
        "action": "delete",
        "warn_on_del": True,
        "allow_admins": True,
        "block_tg_invites": True,
        "block_masked": True,
    }


def _antilink_save(cid: int, cfg: dict):
    wl = cfg.get("whitelist", [])
    conn = _db()
    conn.execute(
        "INSERT INTO antilink_cfg (cid,enabled,whitelist,action,warn_on_del,"
        "allow_admins,block_tg_invites,block_masked) VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT(cid) DO UPDATE SET enabled=excluded.enabled, "
        "whitelist=excluded.whitelist, action=excluded.action, "
        "warn_on_del=excluded.warn_on_del, allow_admins=excluded.allow_admins, "
        "block_tg_invites=excluded.block_tg_invites, block_masked=excluded.block_masked",
        (cid, 1 if cfg.get("enabled") else 0, json.dumps(wl),
         cfg.get("action", "delete"),
         1 if cfg.get("warn_on_del", True) else 0,
         1 if cfg.get("allow_admins", True) else 0,
         1 if cfg.get("block_tg_invites", True) else 0,
         1 if cfg.get("block_masked", True) else 0)
    )
    conn.commit()
    conn.close()


def _normalize_url(text: str) -> str:
    """Нормализует текст для обхода unicode-спуфинга"""
    result = text.lower()
    for char, replacement in _UNICODE_REPLACEMENTS.items():
        result = result.replace(char, replacement)
    # Убираем нулевые пробелы и soft hyphen
    result = re.sub(r'[\u200b\u200c\u200d\u00ad\u2060]', '', result)
    return result


def _extract_domain(url: str) -> str:
    """Извлекает домен из URL"""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Убираем www.
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return url.lower()


def _is_whitelisted(url: str, whitelist: list) -> bool:
    """Проверяет домен на белый список"""
    domain = _extract_domain(url)
    for allowed in whitelist:
        allowed = allowed.lower().strip()
        if domain == allowed or domain.endswith('.' + allowed):
            return True
    return False


def _find_urls_in_message(message: Message) -> list:
    """Находит все ссылки в сообщении включая entity-ссылки"""
    urls = []

    # 1. Явные ссылки в тексте
    text = message.text or message.caption or ""
    if text:
        normalized = _normalize_url(text)
        for match in _URL_PATTERN.finditer(normalized):
            urls.append(match.group())

    # 2. Entity-ссылки (кнопки, встроенные URL)
    entities = (message.entities or []) + (message.caption_entities or [])
    for entity in entities:
        if entity.type == "url":
            url_text = text[entity.offset: entity.offset + entity.length]
            urls.append(url_text)
        elif entity.type == "text_link" and entity.url:
            urls.append(entity.url)

    # 3. Инлайн-кнопки с URL (reply_markup)
    if message.reply_markup:
        try:
            for row in message.reply_markup.inline_keyboard:
                for btn in row:
                    if btn.url:
                        urls.append(btn.url)
        except:
            pass

    return list(set(urls))  # убираем дубли


async def check_message(message: Message) -> bool:
    """
    Вызывается из StatsMiddleware для каждого сообщения.
    True = сообщение нарушает правила и было удалено.
    """
    if not message.from_user:
        return False
    if message.chat.type not in ("group", "supergroup"):
        return False

    cid = message.chat.id
    uid = message.from_user.id
    cfg = _antilink_cfg(cid)

    if not cfg.get("enabled"):
        return False

    # Пропускаем администраторов
    if cfg.get("allow_admins", True):
        try:
            member = await _bot.get_chat_member(cid, uid)
            if member.status in ("administrator", "creator"):
                return False
        except:
            pass

    # Ищем ссылки
    urls = _find_urls_in_message(message)
    if not urls:
        return False

    whitelist  = cfg.get("whitelist", DEFAULT_WHITELIST)
    blocked_url = None
    is_tg_invite = False

    for url in urls:
        # Telegram-инвайты — отдельная проверка
        if cfg.get("block_tg_invites", True):
            if _TG_INVITE_PATTERN.search(url):
                blocked_url  = url
                is_tg_invite = True
                break

        # Проверка белого списка
        if not _is_whitelisted(url, whitelist):
            # Дополнительно: замаскированные ссылки
            if cfg.get("block_masked", True):
                norm = _normalize_url(url)
                if any(kw in norm for kw in ['joinchat', 'invite', '+', 'bit.ly', 'tinyurl', 'goo.gl']):
                    blocked_url = url
                    break
            blocked_url = url
            break

    if not blocked_url:
        return False

    # Применяем действие
    action  = cfg.get("action", "delete")
    name    = message.from_user.full_name
    deleted = False

    try:
        await message.delete()
        deleted = True
    except:
        pass

    warn_text = ""
    if action in ("warn", "delete") and cfg.get("warn_on_del", True):
        # Кулдаун предупреждений — не спамим
        cd_key = f"{cid}:{uid}"
        now    = time.time()
        if now - _antilink_warn_cd.get(cd_key, 0) > 30:
            _antilink_warn_cd[cd_key] = now
            link_type = "инвайт-ссылка Telegram" if is_tg_invite else "ссылка"
            try:
                sent = await _bot.send_message(
                    cid,
                    f"🔗 {message.from_user.mention_html()}, "
                    f"{link_type} запрещена в этом чате.",
                    parse_mode="HTML"
                )
                asyncio.create_task(_auto_delete(sent, 10))
            except:
                pass

    elif action == "mute":
        try:
            await _bot.restrict_chat_member(
                cid, uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=datetime.now() + timedelta(minutes=30)
            )
            warn_text = "🔇 Замьючен на 30 мин."
            sent = await _bot.send_message(
                cid,
                f"🔗 {message.from_user.mention_html()} — {warn_text} (спам ссылок)",
                parse_mode="HTML"
            )
            asyncio.create_task(_auto_delete(sent, 15))
        except:
            pass

    elif action == "kick":
        try:
            await _bot.ban_chat_member(cid, uid)
            await _bot.unban_chat_member(cid, uid)
            warn_text = "👢 Кикнут"
        except:
            pass

    elif action == "ban":
        try:
            await _bot.ban_chat_member(cid, uid)
            warn_text = "🔨 Забанен"
        except:
            pass

    # Лог в БД
    domain_short = _extract_domain(blocked_url)[:50]
    conn = _db()
    conn.execute(
        "INSERT INTO antilink_log (cid,uid,username,url,action) VALUES (?,?,?,?,?)",
        (cid, uid, message.from_user.username or "", domain_short, action)
    )
    conn.commit()
    conn.close()

    return deleted


# ══════════════════════════════════════════════════════════════
#  3. 🌍 ГЛОБАЛЬНЫЕ ЗАЩИТНЫЕ ФИЧИ
# ══════════════════════════════════════════════════════════════

# ── 3a. Синхронизация банов между чатами ──────────────────────

async def sync_ban_to_chats(banned_uid: int, by_cid: int, reason: str = ""):
    """
    Когда кого-то банят в одном чате — баним во всех чатах
    с включённой синхронизацией.
    """
    conn = _db()
    sync_chats = conn.execute(
        "SELECT cid FROM cross_ban_sync WHERE enabled=1 AND cid!=?", (by_cid,)
    ).fetchall()
    conn.close()

    if not sync_chats:
        return

    banned_count = 0
    for row in sync_chats:
        target_cid = row["cid"]
        try:
            await _bot.ban_chat_member(target_cid, banned_uid)
            banned_count += 1
            await asyncio.sleep(0.1)  # не флудим API
        except:
            pass

    if banned_count > 0:
        await _send_log(
            f"🔗 <b>Кросс-бан синхронизирован</b>\n\n"
            f"👤 <code>{banned_uid}</code>\n"
            f"💬 Распространён на {banned_count} чатов\n"
            f"📋 Причина: {reason or '—'}"
        )


def enable_cross_ban(cid: int, enable: bool):
    conn = _db()
    conn.execute(
        "INSERT INTO cross_ban_sync (cid,enabled) VALUES (?,?) "
        "ON CONFLICT(cid) DO UPDATE SET enabled=excluded.enabled",
        (cid, 1 if enable else 0)
    )
    conn.commit()
    conn.close()


def is_cross_ban_enabled(cid: int) -> bool:
    conn = _db()
    row = conn.execute("SELECT enabled FROM cross_ban_sync WHERE cid=?", (cid,)).fetchone()
    conn.close()
    return bool(row["enabled"]) if row else False


# ── 3b. Вотчлист ──────────────────────────────────────────────

def watchlist_add(uid: int, reason: str, added_by: str):
    conn = _db()
    conn.execute(
        "INSERT INTO watchlist (uid,reason,added_by) VALUES (?,?,?) "
        "ON CONFLICT(uid) DO UPDATE SET reason=excluded.reason",
        (uid, reason, added_by)
    )
    conn.commit()
    conn.close()


def watchlist_remove(uid: int):
    conn = _db()
    conn.execute("DELETE FROM watchlist WHERE uid=?", (uid,))
    conn.commit()
    conn.close()


def watchlist_check(uid: int) -> Optional[dict]:
    conn = _db()
    row = conn.execute("SELECT * FROM watchlist WHERE uid=?", (uid,)).fetchone()
    conn.close()
    return dict(row) if row else None


def watchlist_all() -> list:
    conn = _db()
    rows = conn.execute("SELECT * FROM watchlist ORDER BY ts DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── 3c. Авто-очистка неактивных ───────────────────────────────

async def auto_cleanup_inactive(cid: int, days: int = 30, dry_run: bool = False) -> list:
    """
    Находит пользователей без сообщений за N дней.
    dry_run=True — только список, без кика.
    """
    conn = _db()
    cutoff = int(time.time()) - days * 86400
    # Берём uid из chat_stats, у которых нет активности в last_seen
    rows = conn.execute(
        "SELECT DISTINCT uid FROM chat_stats WHERE cid=?", (cid,)
    ).fetchall()
    conn.close()

    inactive = []
    for row in rows:
        uid = row["uid"]
        # Проверяем last_seen
        conn = _db()
        ls = conn.execute(
            "SELECT ts FROM last_seen WHERE cid=? AND uid=?", (cid, uid)
        ).fetchone()
        conn.close()

        if ls and ls["ts"] > cutoff:
            continue  # активен
        if not ls:
            continue  # нет данных, не трогаем

        try:
            member = await _bot.get_chat_member(cid, uid)
            if member.status in ("administrator", "creator", "kicked", "left"):
                continue
        except:
            continue

        inactive.append(uid)

    if not dry_run:
        kicked = 0
        for uid in inactive:
            try:
                await _bot.ban_chat_member(cid, uid)
                await _bot.unban_chat_member(cid, uid)
                kicked += 1
                await asyncio.sleep(0.05)
            except:
                pass
        await _send_log(
            f"🧹 <b>Авто-очистка чата</b>\n\n"
            f"💬 <code>{cid}</code>\n"
            f"👥 Удалено неактивных за {days} дней: <b>{kicked}</b>"
        )

    return inactive


# ── 3d. Фоновый мониторинг вотчлиста ─────────────────────────

_watchlist_last_notify: dict = {}  # {uid: ts}


async def _watchlist_notify_loop():
    """Каждые 6 часов присылает дайджест по вотчлисту"""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            wl = watchlist_all()
            if wl:
                text = f"👁 <b>ВОТЧЛИСТ — {len(wl)} чел. под наблюдением</b>\n\n"
                for entry in wl[:10]:
                    dt = datetime.fromtimestamp(entry["ts"]).strftime("%d.%m.%Y")
                    text += f"• <code>{entry['uid']}</code> — {entry['reason']} (с {dt})\n"
                if len(wl) > 10:
                    text += f"\n...и ещё {len(wl)-10}"
                await _send_log(text)
        except Exception as e:
            log.error(f"watchlist_notify_loop: {e}")


# ══════════════════════════════════════════════════════════════
#  КОМАНДЫ
# ══════════════════════════════════════════════════════════════

async def cmd_avatarcheck(message: Message):
    """/avatarcheck [on|off|action kick|ban|mute|warn]"""
    from aiogram.filters import CommandObject
    cid  = message.chat.id
    text = message.text or ""
    args = text.split()[1:] if len(text.split()) > 1 else []
    cfg  = _avatar_cfg(cid)

    if not args:
        st = "✅ включена" if cfg.get("enabled") else "❌ выключена"
        await message.answer(
            f"📸 <b>Проверка аватара</b>\n\n"
            f"Статус: {st}\n"
            f"Действие: <b>{cfg.get('action','kick')}</b>\n\n"
            f"/avatarcheck on — включить\n"
            f"/avatarcheck off — выключить\n"
            f"/avatarcheck action kick|ban|mute|warn",
            parse_mode="HTML"
        )
        return

    if args[0] == "on":
        cfg["enabled"] = True
        _avatar_save(cid, cfg)
        await message.answer("📸 Проверка аватара <b>включена</b>", parse_mode="HTML")
    elif args[0] == "off":
        cfg["enabled"] = False
        _avatar_save(cid, cfg)
        await message.answer("📸 Проверка аватара <b>выключена</b>", parse_mode="HTML")
    elif args[0] == "action" and len(args) > 1 and args[1] in ("kick", "ban", "mute", "warn"):
        cfg["action"] = args[1]
        _avatar_save(cid, cfg)
        await message.answer(f"📸 Действие: <b>{args[1]}</b>", parse_mode="HTML")
    else:
        await message.answer("⚠️ /avatarcheck on|off|action kick|ban|mute|warn")


async def cmd_antilink(message: Message):
    """/antilink [on|off|whitelist add|remove DOMAIN|action|status]"""
    cid  = message.chat.id
    text = message.text or ""
    args = text.split()[1:] if len(text.split()) > 1 else []
    cfg  = _antilink_cfg(cid)

    if not args:
        st = "✅ включён" if cfg.get("enabled") else "❌ выключен"
        wl = cfg.get("whitelist", [])
        wl_text = ", ".join(wl[:5]) + ("..." if len(wl) > 5 else "") if wl else "—"
        await message.answer(
            f"🔗 <b>Антилинк</b>\n\n"
            f"Статус: {st}\n"
            f"Действие: <b>{cfg.get('action','delete')}</b>\n"
            f"Белый список ({len(wl)}): {wl_text}\n\n"
            f"<b>Команды:</b>\n"
            f"/antilink on|off\n"
            f"/antilink action delete|warn|mute|kick|ban\n"
            f"/antilink whitelist add example.com\n"
            f"/antilink whitelist remove example.com\n"
            f"/antilink whitelist list",
            parse_mode="HTML"
        )
        return

    if args[0] == "on":
        cfg["enabled"] = True
        _antilink_save(cid, cfg)
        await message.answer("🔗 Антилинк <b>включён</b>", parse_mode="HTML")

    elif args[0] == "off":
        cfg["enabled"] = False
        _antilink_save(cid, cfg)
        await message.answer("🔗 Антилинк <b>выключен</b>", parse_mode="HTML")

    elif args[0] == "action" and len(args) > 1:
        act = args[1].lower()
        if act not in ("delete", "warn", "mute", "kick", "ban"):
            await message.answer("⚠️ Действие: delete | warn | mute | kick | ban"); return
        cfg["action"] = act
        _antilink_save(cid, cfg)
        await message.answer(f"🔗 Действие: <b>{act}</b>", parse_mode="HTML")

    elif args[0] == "whitelist":
        if len(args) < 2:
            await message.answer("⚠️ /antilink whitelist add|remove|list [domain]"); return
        sub = args[1].lower()
        wl  = cfg.get("whitelist", [])

        if sub == "list":
            wl_text = "\n".join(f"• {d}" for d in wl) if wl else "Белый список пуст"
            await message.answer(f"🔗 <b>Белый список ({len(wl)}):</b>\n{wl_text}", parse_mode="HTML")

        elif sub == "add" and len(args) > 2:
            domain = args[2].lower().strip()
            if domain.startswith(("http://", "https://")):
                domain = _extract_domain(domain)
            if domain not in wl:
                wl.append(domain)
                cfg["whitelist"] = wl
                _antilink_save(cid, cfg)
                await message.answer(f"✅ Добавлен: <code>{domain}</code>", parse_mode="HTML")
            else:
                await message.answer(f"ℹ️ Уже в списке: <code>{domain}</code>", parse_mode="HTML")

        elif sub == "remove" and len(args) > 2:
            domain = args[2].lower().strip()
            if domain in wl:
                wl.remove(domain)
                cfg["whitelist"] = wl
                _antilink_save(cid, cfg)
                await message.answer(f"🗑 Удалён: <code>{domain}</code>", parse_mode="HTML")
            else:
                await message.answer(f"❌ Не найден: <code>{domain}</code>", parse_mode="HTML")

        elif sub == "reset":
            cfg["whitelist"] = list(DEFAULT_WHITELIST)
            _antilink_save(cid, cfg)
            await message.answer("♻️ Белый список сброшен к стандартному")

        else:
            await message.answer("⚠️ /antilink whitelist add|remove|list|reset [domain]")
    else:
        await message.answer("⚠️ /antilink on|off|action|whitelist")


async def cmd_watchlist(message: Message):
    """/watch [add ID причина | remove ID | list]"""
    cid  = message.chat.id
    text = message.text or ""
    args = text.split()[1:] if len(text.split()) > 1 else []

    if not args or args[0] == "list":
        wl = watchlist_all()
        if not wl:
            await message.answer("👁 Вотчлист пуст"); return
        lines = "\n".join(
            f"• <code>{e['uid']}</code> — {e['reason'][:40]}"
            for e in wl[:20]
        )
        await message.answer(
            f"👁 <b>Вотчлист ({len(wl)}):</b>\n{lines}",
            parse_mode="HTML"
        )

    elif args[0] == "add" and len(args) >= 2:
        try:
            uid    = int(args[1])
            reason = " ".join(args[2:]) or "Без причины"
            by     = message.from_user.full_name
            watchlist_add(uid, reason, by)
            await message.answer(
                f"👁 <code>{uid}</code> добавлен в вотчлист\n📋 {reason}",
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("⚠️ /watch add [ID] [причина]")

    elif args[0] == "remove" and len(args) >= 2:
        try:
            uid = int(args[1])
            watchlist_remove(uid)
            await message.answer(f"✅ <code>{uid}</code> снят с наблюдения", parse_mode="HTML")
        except ValueError:
            await message.answer("⚠️ /watch remove [ID]")

    else:
        await message.answer("⚠️ /watch list | /watch add ID причина | /watch remove ID")


async def cmd_crossban(message: Message):
    """/crossban on|off — синхронизация банов"""
    cid  = message.chat.id
    text = message.text or ""
    args = text.split()[1:]

    if not args:
        enabled = is_cross_ban_enabled(cid)
        st = "✅ включена" if enabled else "❌ выключена"
        await message.answer(
            f"🔗 <b>Кросс-бан синхронизация</b>: {st}\n\n"
            f"Баны в этом чате будут применяться\nво всех чатах с включённой синхронизацией.\n\n"
            f"/crossban on|off",
            parse_mode="HTML"
        )
        return

    if args[0] == "on":
        enable_cross_ban(cid, True)
        await message.answer("🔗 Кросс-бан синхронизация <b>включена</b>", parse_mode="HTML")
    elif args[0] == "off":
        enable_cross_ban(cid, False)
        await message.answer("🔗 Кросс-бан синхронизация <b>выключена</b>", parse_mode="HTML")


# ══════════════════════════════════════════════════════════════
#  CALLBACK ОБРАБОТЧИКИ
# ══════════════════════════════════════════════════════════════

async def cb_sf(call: CallbackQuery):
    parts  = call.data.split(":")
    action = parts[1]

    if action == "av_approve":
        cid, uid = int(parts[2]), int(parts[3])
        await call.message.edit_text(
            call.message.text + "\n\n✅ <b>Одобрен администратором</b>",
            parse_mode="HTML"
        )
        await call.answer("✅ Одобрен")

    elif action == "av_ban":
        cid, uid = int(parts[2]), int(parts[3])
        try:
            await _bot.ban_chat_member(cid, uid)
            await call.message.edit_text(
                call.message.text + "\n\n🔨 <b>Забанен администратором</b>",
                parse_mode="HTML"
            )
            await call.answer("🔨 Забанен")
        except Exception as e:
            await call.answer(f"Ошибка: {e}")

    elif action == "wl_ban":
        cid, uid = int(parts[2]), int(parts[3])
        try:
            await _bot.ban_chat_member(cid, uid)
            await call.message.edit_text(
                call.message.text + "\n\n🔨 <b>Забанен</b>",
                parse_mode="HTML"
            )
            await call.answer("🔨 Забанен")
        except Exception as e:
            await call.answer(f"Ошибка: {e}")

    elif action == "wl_remove":
        uid = int(parts[2])
        watchlist_remove(uid)
        await call.message.edit_text(
            call.message.text + "\n\n✅ <b>Снят с наблюдения</b>",
            parse_mode="HTML"
        )
        await call.answer("✅ Снят с вотчлиста")

    await call.answer()


# ══════════════════════════════════════════════════════════════
#  API ДЛЯ ДАШБОРДА
# ══════════════════════════════════════════════════════════════

def get_antilink_cfg(cid: int) -> dict:
    return _antilink_cfg(cid)


def update_antilink_cfg(cid: int, data: dict):
    cfg = _antilink_cfg(cid)
    for k in ("enabled", "warn_on_del", "allow_admins", "block_tg_invites", "block_masked"):
        if k in data:
            cfg[k] = bool(data[k])
    if "action" in data:
        cfg["action"] = data["action"]
    if "whitelist" in data:
        cfg["whitelist"] = data["whitelist"]
    _antilink_save(cid, cfg)
    return cfg


def get_avatar_cfg(cid: int) -> dict:
    return _avatar_cfg(cid)


def update_avatar_cfg(cid: int, data: dict):
    cfg = _avatar_cfg(cid)
    if "enabled" in data:
        cfg["enabled"] = bool(data["enabled"])
    if "action" in data:
        cfg["action"] = data["action"]
    if "mute_mins" in data:
        try:
            cfg["mute_mins"] = int(data["mute_mins"])
        except:
            pass
    _avatar_save(cid, cfg)
    return cfg


def get_antilink_stats(cid: int = None) -> dict:
    conn = _db()
    if cid:
        total = conn.execute(
            "SELECT COUNT(*) as c FROM antilink_log WHERE cid=?", (cid,)
        ).fetchone()["c"]
        today = conn.execute(
            "SELECT COUNT(*) as c FROM antilink_log WHERE cid=? AND date(ts,'unixepoch')=date('now')",
            (cid,)
        ).fetchone()["c"]
    else:
        total = conn.execute("SELECT COUNT(*) as c FROM antilink_log").fetchone()["c"]
        today = conn.execute(
            "SELECT COUNT(*) as c FROM antilink_log WHERE date(ts,'unixepoch')=date('now')"
        ).fetchone()["c"]
    recent = conn.execute(
        "SELECT * FROM antilink_log ORDER BY ts DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return {
        "total": total,
        "today": today,
        "recent": [dict(r) for r in recent]
    }


# ══════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ
# ══════════════════════════════════════════════════════════════

async def _send_log(text: str, kb=None):
    if not _bot:
        return
    if _log_channel:
        try:
            await _bot.send_message(_log_channel, text, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            log.warning(f"Лог-канал: {e}")
    for aid in _admin_ids:
        try:
            await _bot.send_message(aid, text, parse_mode="HTML", reply_markup=kb)
        except:
            pass


async def _auto_delete(msg, delay: int):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass


def _register_handlers(dp: Dispatcher):
    dp.message.register(cmd_avatarcheck, Command("avatarcheck"))
    dp.message.register(cmd_antilink,    Command("antilink"))
    dp.message.register(cmd_watchlist,   Command("watch"))
    dp.message.register(cmd_crossban,    Command("crossban"))
    dp.callback_query.register(cb_sf,    F.data.startswith("sf:"))
    log.info("✅ security_features.py хендлеры зарегистрированы")

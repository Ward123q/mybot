# -*- coding: utf-8 -*-
"""
dashboard.py — Глобально улучшенная веб-панель управления
Приоритеты 1-5 + Система администрации с 10 рангами
Только владелец (ID: 7823802800) может выдавать/забирать роли
"""
import os
import json
import traceback
import asyncio
import logging
import time
import secrets
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from aiohttp import web
import database as db
import shared
try:
    import antiraid
    import night_mode
    import security_features as sf
    _SF_LOADED = True
except ImportError:
    _SF_LOADED = False

log = logging.getLogger(__name__)

DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "changeme123")
OWNER_TG_ID = 7823802800  # Единственный владелец
OWNER_RANK   = 15           # Ранг владельца

# ── Дежурства ──────────────────────────────────────────────────
_duty_active: dict  = {}   # {uid: {uid,name,rank,rank_name,start,end,hours,actions}}
_duty_history: list = []   # [{uid,name,start,end,duration_mins,actions_count}]

def start_duty(tg_uid: int, name: str, rank: int, hours: float = 8.0, rank_name: str = ""):
    _duty_active[tg_uid] = {
        "uid": tg_uid, "name": name, "rank": rank,
        "rank_name": rank_name, "start": time.time(),
        "end": time.time() + hours * 3600,
        "hours": hours, "actions": 0,
    }

def end_duty(tg_uid: int):
    d = _duty_active.pop(tg_uid, None)
    if d:
        _duty_history.insert(0, {
            "uid": tg_uid, "name": d["name"],
            "start": d["start"], "end": time.time(),
            "duration_mins": int((time.time() - d["start"]) / 60),
            "actions_count": d.get("actions", 0),
        })
        if len(_duty_history) > 300:
            _duty_history.pop()

def get_duty_status(tg_uid: int):
    d = _duty_active.get(tg_uid)
    if not d:
        return None
    if time.time() >= d["end"]:
        end_duty(tg_uid)
        return None
    elapsed   = int((time.time() - d["start"]) / 60)
    remaining = int((d["end"] - time.time()) / 60)
    return {**d, "elapsed_mins": elapsed, "remaining_mins": remaining,
            "pct": min(100, int(elapsed / max(d["hours"] * 60, 1) * 100))}

def get_all_on_duty() -> list:
    return [s for uid in list(_duty_active) if (s := get_duty_status(uid))]

def _increment_duty_actions(tg_uid: int):
    if tg_uid in _duty_active:
        _duty_active[tg_uid]["actions"] = _duty_active[tg_uid].get("actions", 0) + 1

def _get_mod_stats(name: str, uid: int) -> dict:
    try:
        conn = db.get_conn()
        bans  = conn.execute("SELECT COUNT(*) FROM mod_history WHERE by_name=? AND action LIKE '%Бан%'", (name,)).fetchone()[0] or 0
        warns = conn.execute("SELECT COUNT(*) FROM mod_history WHERE by_name=? AND action LIKE '%Варн%'", (name,)).fetchone()[0] or 0
        mutes = conn.execute("SELECT COUNT(*) FROM mod_history WHERE by_name=? AND action LIKE '%Мут%'", (name,)).fetchone()[0] or 0
        tkt   = conn.execute("SELECT COUNT(*) FROM dashboard_admin_log WHERE tg_uid=? AND action='TICKET_CLOSE'", (uid,)).fetchone()[0] or 0
        logins= conn.execute("SELECT COUNT(*) FROM dashboard_admin_log WHERE tg_uid=? AND action='LOGIN'", (uid,)).fetchone()[0] or 0
        conn.close()
        return {"bans":bans,"warns":warns,"mutes":mutes,"tickets":tkt,"total":bans+warns+mutes,"logins":logins}
    except:
        return {"bans":0,"warns":0,"mutes":0,"tickets":0,"total":0,"logins":0}


_bot = None
_admin_ids = set()
_bot_instance = None   # алиас, обновляется в set_bot

def set_bot(bot, admin_ids: set):
    global _bot, _admin_ids, _bot_instance
    _bot = bot
    _bot_instance = bot   # синхронизируем оба имени
    _admin_ids = admin_ids

# ══════════════════════════════════════════
#  СИСТЕМА АДМИНИСТРАЦИИ — 15 РАНГОВ
# ══════════════════════════════════════════

RANK_TIERS = {
    "junior": {"label": "🔵 Младший состав",  "color": "#607d8b"},
    "mid":    {"label": "🟢 Средний состав",   "color": "#42a5f5"},
    "senior": {"label": "🟡 Старший состав",   "color": "#ab47bc"},
    "high":   {"label": "🟠 Высший состав",    "color": "#ec407a"},
    "top":    {"label": "🔴 Топ состав",       "color": "#ff7043"},
    "owner":  {"label": "⭐ Владелец",         "color": "#ffd700"},
}

DASHBOARD_RANKS = {
    1:  {"name":"👁 Наблюдатель",       "color":"#607d8b","tier":"junior",
         "perms":["view_overview","view_chats","view_users"],
         "desc":"Только просмотр основной статистики"},
    2:  {"name":"📋 Репортёр",          "color":"#78909c","tier":"junior",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts"],
         "desc":"Просмотр репортов и алертов"},
    3:  {"name":"🎫 Поддержка",         "color":"#26a69a","tier":"junior",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets"],
         "desc":"Работа с тикетами и просмотр репортов"},
    4:  {"name":"🛡 Юниор-Мод",        "color":"#42a5f5","tier":"junior",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation"],
         "desc":"Закрытие тикетов, обработка репортов"},
    5:  {"name":"⚔️ Мод",              "color":"#66bb6a","tier":"mid",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted"],
         "desc":"Мут и варн пользователей, медиа-лог"},
    6:  {"name":"⚡ Старший Мод",       "color":"#ffa726","tier":"mid",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy"],
         "desc":"Бан/разбан пользователей, экономика"},
    7:  {"name":"🔱 Хед-Мод",           "color":"#ef5350","tier":"mid",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings"],
         "desc":"Управление плагинами, настройки просмотра"},
    8:  {"name":"🌀 Куратор",           "color":"#29b6f6","tier":"senior",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles"],
         "desc":"Управление дежурствами, профили модов"},
    9:  {"name":"💎 Администратор",     "color":"#ab47bc","tier":"senior",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles",
                  "edit_settings","broadcast","manage_chat_settings"],
         "desc":"Полное управление настройками и рассылки"},
    10: {"name":"🔥 Старший Адм",       "color":"#ff7043","tier":"senior",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles",
                  "edit_settings","broadcast","manage_chat_settings",
                  "manage_admins_view","manage_junior_admins"],
         "desc":"Управление младшим составом (1-7)"},
    11: {"name":"⭐ Гл. Администратор", "color":"#ec407a","tier":"high",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles",
                  "edit_settings","broadcast","manage_chat_settings",
                  "manage_admins_view","manage_junior_admins",
                  "manage_senior_admins","view_audit_log"],
         "desc":"Контроль над составом ≤ 10"},
    12: {"name":"💠 Куратор Сервера",   "color":"#00e5ff","tier":"high",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles",
                  "edit_settings","broadcast","manage_chat_settings",
                  "manage_admins_view","manage_junior_admins",
                  "manage_senior_admins","view_audit_log",
                  "server_settings","manage_all_chats"],
         "desc":"Глобальные настройки сервера"},
    13: {"name":"🌙 Зам. Владельца",    "color":"#b39ddb","tier":"top",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles",
                  "edit_settings","broadcast","manage_chat_settings",
                  "manage_admins_view","manage_junior_admins",
                  "manage_senior_admins","view_audit_log",
                  "server_settings","manage_all_chats","manage_admins_all"],
         "desc":"Заместитель — полный доступ кроме выдачи 14-15"},
    14: {"name":"👑 Со-Владелец",       "color":"#ff8f00","tier":"top",
         "perms":["view_overview","view_chats","view_users","view_reports","view_alerts",
                  "view_tickets","reply_tickets","close_tickets","handle_reports","view_moderation",
                  "mute_users","warn_users","view_media","view_deleted",
                  "ban_users","unban_users","view_economy","manage_plugins","view_settings",
                  "manage_duty","view_mod_profiles",
                  "edit_settings","broadcast","manage_chat_settings",
                  "manage_admins_view","manage_junior_admins",
                  "manage_senior_admins","view_audit_log",
                  "server_settings","manage_all_chats",
                  "manage_admins_all","co_owner_actions"],
         "desc":"Со-Владелец — управление всей администрацией"},
    15: {"name":"🌟 Владелец",          "color":"#ffd700","tier":"owner",
         "perms":["ALL"],
         "desc":"Полный доступ ко всему. Только вы."},
}

# Таблица администраторов дашборда в памяти + SQLite
# {session_token: {"uid": tg_uid, "name": str, "rank": int, "login_time": float, "ip": str}}
_dashboard_sessions: dict = {}
# Трекер сессий {ip: {"last_seen": float, "current": str, "pages": int}}
_active_sessions: dict = {}


def _init_admin_db():
    """Создаёт таблицу dashboard_admins если нет"""
    try:
        conn = db.get_conn()
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS dashboard_admins (
            tg_uid    INTEGER PRIMARY KEY,
            name      TEXT,
            rank      INTEGER DEFAULT 1,
            granted_by INTEGER,
            granted_at TEXT DEFAULT (datetime('now')),
            last_login TEXT
        );
        CREATE TABLE IF NOT EXISTS dashboard_admin_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_uid     INTEGER,
            action     TEXT,
            details    TEXT,
            ip         TEXT,
            ts         TEXT DEFAULT (datetime('now'))
        );
        """)
        # Владелец всегда ранг 10
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_admins (tg_uid, name, rank, granted_by) VALUES (?,?,?,?)",
            (OWNER_TG_ID, "Владелец", OWNER_RANK, OWNER_TG_ID)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"_init_admin_db: {e}")


def _get_admin(tg_uid: int) -> dict | None:
    try:
        conn = db.get_conn()
        row = conn.execute(
            "SELECT * FROM dashboard_admins WHERE tg_uid=?", (tg_uid,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except:
        return None


def _get_all_admins() -> list:
    try:
        conn = db.get_conn()
        rows = conn.execute(
            "SELECT * FROM dashboard_admins ORDER BY rank DESC"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except:
        return []


def _grant_admin(tg_uid: int, name: str, rank: int, granted_by: int):
    try:
        conn = db.get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_admins (tg_uid, name, rank, granted_by) VALUES (?,?,?,?)",
            (tg_uid, name, rank, granted_by)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"_grant_admin: {e}")


def _revoke_admin(tg_uid: int):
    try:
        conn = db.get_conn()
        conn.execute("DELETE FROM dashboard_admins WHERE tg_uid=?", (tg_uid,))
        conn.commit()
        conn.close()
    except:
        pass


def _log_admin_db(tg_uid: int, action: str, details: str, ip: str = ""):
    try:
        conn = db.get_conn()
        conn.execute(
            "INSERT INTO dashboard_admin_log (tg_uid, action, details, ip) VALUES (?,?,?,?)",
            (tg_uid, action, details, ip)
        )
        conn.commit()
        conn.close()
    except:
        pass
    shared.log_admin_action(f"{action}: {details}")


def _has_perm(session_token: str, perm: str) -> bool:
    sess = _dashboard_sessions.get(session_token)
    if not sess:
        return False
    rank = sess.get("rank", 0)
    if rank >= OWNER_RANK:
        return True
    rank_perms = DASHBOARD_RANKS.get(rank, {}).get("perms", [])
    return "ALL" in rank_perms or perm in rank_perms


def _get_session(request) -> dict | None:
    token = request.cookies.get("dsess_token")
    if not token:
        return None
    sess = _dashboard_sessions.get(token)
    if sess and _SESSION_IP_LOCK:
        current_ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()
        session_ip = sess.get("ip", "")
        if session_ip and current_ip and session_ip != current_ip:
            log.warning(f"[IP_WATCHDOG] IP change {session_ip}→{current_ip} uid={sess.get('uid')}")
            _dashboard_sessions.pop(token, None)
            try:
                asyncio.ensure_future(_brute_notify_owner(f"IP change {session_ip}→{current_ip}", 0))
            except:
                pass
            return None
    return sess


def _track_session(request, path=""):
    ip = request.headers.get("X-Forwarded-For", request.remote or "unknown").split(",")[0].strip()
    now = time.time()
    if ip not in _active_sessions:
        _active_sessions[ip] = {"ip": ip, "last_seen": now, "pages": 0, "current": path}
    _active_sessions[ip].update({"last_seen": now, "current": path or str(request.rel_url)})
    _active_sessions[ip]["pages"] = _active_sessions[ip].get("pages", 0) + 1
    for k in list(_active_sessions.keys()):
        if now - _active_sessions[k]["last_seen"] > 600:
            del _active_sessions[k]


def _get_active_sessions() -> list:
    now = time.time()
    return [s for s in _active_sessions.values() if now - s["last_seen"] < 600]


# ══════════════════════════════════════════
#  2FA
# ══════════════════════════════════════════
_2fa_pending: dict = {}

def _gen_2fa_code() -> str:
    return str(secrets.randbelow(900000) + 100000)

async def _send_2fa_code(tg_uid: int, code: str) -> bool:
    if not _bot:
        return False
    try:
        await _bot.send_message(
            tg_uid,
            f"━━━━━━━━━━━━━━━\n"
            f"🔐 <b>КОД ВХОДА В ДАШБОРД</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"🔑 Код: <code>{code}</code>\n\n"
            f"⏳ Действителен 5 минут.\n"
            f"Если это не ты — смени токен!",
            parse_mode="HTML"
        )
        return True
    except:
        return False

# ══════════════════════════════════════════
#  AUTH DECORATOR
# ══════════════════════════════════════════

def require_auth(perm: str = "view_overview"):
    def decorator(handler):
        @wraps(handler)
        async def wrapper(request: web.Request):
            _track_session(request)
            sess = _get_session(request)
            if not sess:
                raise web.HTTPFound("/dashboard/login")
            if not _has_perm(request.cookies.get("dsess_token"), perm):
                return web.Response(
                    text=page(navbar(sess) + no_access_html(perm)),
                    content_type="text/html"
                )
            return await handler(request)
        return wrapper
    return decorator


def no_access_html(perm: str) -> str:
    return f"""
    <div class="container" style="padding-top:80px;text-align:center;">
        <div style="font-size:64px;margin-bottom:20px;">🚫</div>
        <h2 style="color:var(--text);margin-bottom:12px;">Нет доступа</h2>
        <p style="color:var(--text2);">Требуется право: <code>{perm}</code></p>
        <p style="color:var(--text2);margin-top:8px;">Обратитесь к владельцу для повышения ранга.</p>
        <a href="/dashboard" class="btn btn-primary" style="margin-top:24px;">← На главную</a>
    </div>"""

# ══════════════════════════════════════════
#  CSS / HTML ШАБЛОНЫ
# ══════════════════════════════════════════

HTML_BASE = """<!DOCTYPE html>
<html lang="ru" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>CHAT GUARD — Dashboard</title>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  /* ═══════════════════════════════════════════════════════════════
     CHAT GUARD DASHBOARD v4 — OBSIDIAN TERMINAL
     Шрифты: Outfit (UI) + Space Mono (code/mono)
     Тема: глубокий тёмно-синий космос, неоново-изумрудный акцент
  ═══════════════════════════════════════════════════════════════ */
  @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Space+Mono:ital,wght@0,400;0,700;1,400&display=swap');

  :root {
    /* Фоны */
    --bg0:  #060810;
    --bg1:  #080b14;
    --bg2:  #0c1020;
    --bg3:  #111827;
    --bg4:  #161d2e;
    --bg5:  #1c2538;

    /* Границы */
    --br0:  rgba(255,255,255,.04);
    --br1:  rgba(255,255,255,.07);
    --br2:  rgba(255,255,255,.12);

    /* Текст */
    --t1:   #f0f4ff;
    --t2:   #7b8cad;
    --t3:   #4a5568;

    /* Акценты */
    --acc:  #5865f2;
    --acc2: #4752c4;
    --acc-g:rgba(88,101,242,.15);
    --acc-s:rgba(88,101,242,.06);
    --blue: #3b82f6;
    --blue-g:rgba(59,130,246,.12);
    --pur:  #a78bfa;
    --pur-g:rgba(167,139,250,.12);
    --red:  #f23f42;
    --red-g:rgba(242,63,66,.15);
    --ylw:  #f0b132;
    --ylw-g:rgba(240,177,50,.15);
    --cyan: #22d3ee;
    --green:#23a55a;
    --green-g:rgba(35,165,90,.15);

    /* Размеры */
    --sw:   240px;
    --r:    10px;
    --r2:   14px;
    --r3:   20px;

    /* Тени */
    --sh0:  0 2px 8px rgba(0,0,0,.4);
    --sh1:  0 8px 32px rgba(0,0,0,.5);
    --sh2:  0 20px 60px rgba(0,0,0,.6);
  }

  /* ── RESET & BASE ─────────────────────── */
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    font-family: 'Outfit', sans-serif;
    background: var(--bg0);
    color: var(--t1);
    min-height: 100vh;
    display: flex;
    overflow-x: hidden;
    /* Сетка-фон */
    background-image:
      radial-gradient(ellipse 80% 50% at 50% -10%, rgba(88,101,242,.06) 0%, transparent 60%),
      linear-gradient(rgba(255,255,255,.015) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255,255,255,.015) 1px, transparent 1px);
    background-size: 100% 100%, 40px 40px, 40px 40px;
  }
  [data-theme="light"] body, [data-theme="light"].body {
    background-image:
      radial-gradient(ellipse 80% 50% at 50% -10%, rgba(14,160,106,.04) 0%, transparent 60%),
      linear-gradient(rgba(0,0,0,.02) 1px, transparent 1px),
      linear-gradient(90deg, rgba(0,0,0,.02) 1px, transparent 1px);
    background-size: 100% 100%, 40px 40px, 40px 40px;
  }
  ::selection { background: rgba(88,101,242,.35); color: #fff; }
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: var(--bg1); }
  ::-webkit-scrollbar-thumb { background: var(--bg5); border-radius: 10px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--acc2); }

  /* ── SIDEBAR ─────────────────────────── */
  .sidebar {
    width: var(--sw);
    background: rgba(8,11,20,.95);
    border-right: 1px solid var(--br1);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0; left: 0; bottom: 0;
    z-index: 200;
    transition: transform .3s cubic-bezier(.4,0,.2,1);
  }
  .sidebar::after {
    content: '';
    position: absolute;
    top: 0; right: 0; bottom: 0;
    width: 1px;
    background: linear-gradient(180deg,
      transparent 0%, var(--acc) 30%, var(--pur) 70%, transparent 100%);
    opacity: .3;
  }
  .sidebar-brand {
    padding: 20px 18px 16px;
    border-bottom: 1px solid var(--br0);
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .sidebar-brand .logo {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, var(--acc), var(--pur));
    border-radius: var(--r);
    display: flex; align-items: center; justify-content: center;
    font-size: 16px; font-weight: 900; color: #000;
    box-shadow: 0 0 20px rgba(0,229,160,.3);
    flex-shrink: 0;
  }
  .sidebar-brand .brand-text {
    font-size: 13px; font-weight: 800; letter-spacing: 2px;
    color: var(--t1); font-family: 'Space Mono', monospace;
    text-transform: uppercase;
  }
  .sidebar-brand .brand-sub {
    font-size: 9px; color: var(--acc); letter-spacing: 1px;
    text-transform: uppercase; font-weight: 600;
  }
  .sidebar-user {
    padding: 12px 18px;
    border-bottom: 1px solid var(--br0);
    font-size: 12px;
  }
  .sidebar-user .u-name { font-weight: 700; color: var(--t1); font-size: 13px; }
  .sidebar-user .u-rank {
    display: inline-block; padding: 2px 8px; border-radius: 20px;
    font-size: 10px; font-weight: 600; margin-top: 4px;
    background: var(--acc-g); color: var(--acc);
    border: 1px solid rgba(0,229,160,.2);
  }
  .sidebar-nav { flex: 1; overflow-y: auto; padding: 6px 0; }
  .nav-section {
    font-size: 9px; color: var(--t3); letter-spacing: 2px;
    padding: 14px 18px 5px; text-transform: uppercase; font-weight: 700;
    font-family: 'Space Mono', monospace;
  }
  .nav-link {
    display: flex; align-items: center; gap: 10px;
    padding: 8px 18px;
    color: var(--t2); text-decoration: none;
    font-size: 13px; font-weight: 500;
    border-left: 2px solid transparent;
    transition: all .15s;
    position: relative;
    margin: 1px 8px; border-radius: 8px; border-left: none;
  }
  .nav-link:hover {
    background: var(--br0); color: var(--t1);
  }
  .nav-link.active {
    background: var(--acc-g);
    color: var(--acc);
    font-weight: 600;
  }
  .nav-link.active::before {
    content: '';
    position: absolute; left: 0; top: 20%; bottom: 20%;
    width: 3px; border-radius: 0 3px 3px 0;
    background: var(--acc);
    box-shadow: 0 0 8px var(--acc);
  }
  .nav-link .nav-icon { font-size: 14px; width: 18px; text-align: center; }
  .nav-badge {
    margin-left: auto; background: var(--red);
    color: #fff; font-size: 9px; font-weight: 700;
    padding: 1px 6px; border-radius: 20px; min-width: 16px; text-align: center;
  }
  .sidebar-footer {
    padding: 12px 18px;
    border-top: 1px solid var(--br0);
    display: flex; justify-content: space-between; align-items: center;
  }
  .sidebar-toggle {
    display: none; position: fixed; top: 14px; left: 14px; z-index: 300;
    background: rgba(8,11,20,.9); border: 1px solid var(--br2);
    border-radius: var(--r); padding: 7px 11px; cursor: pointer; color: var(--t1);
    font-size: 18px; backdrop-filter: blur(10px);
    box-shadow: var(--sh0);
  }

  /* ── MAIN ────────────────────────────── */
  .main {
    margin-left: var(--sw);
    flex: 1; min-height: 100vh;
    display: flex; flex-direction: column;
  }
  .topbar {
    background: rgba(8,11,20,.85);
    border-bottom: 1px solid var(--br1);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    padding: 0 24px;
    height: 56px;
    display: flex; align-items: center; justify-content: space-between;
    position: sticky; top: 0; z-index: 100;
  }
  .topbar-left { display: flex; align-items: center; gap: 12px; }
  .topbar-right { display: flex; align-items: center; gap: 8px; }
  .topbar-title { font-size: 14px; font-weight: 600; color: var(--t2); }

  /* Поиск */
  .search-global { position: relative; }
  .search-global input {
    background: var(--bg3); border: 1px solid var(--br1);
    border-radius: var(--r); padding: 7px 12px 7px 34px;
    color: var(--t1); font-size: 12px; width: 220px;
    font-family: 'Outfit', sans-serif;
    transition: all .2s;
  }
  .search-global input:focus {
    outline: none; border-color: var(--acc);
    width: 280px; box-shadow: 0 0 0 3px var(--acc-s);
  }
  .search-global .search-icon {
    position: absolute; left: 10px; top: 50%; transform: translateY(-50%);
    color: var(--t3); font-size: 13px;
  }
  #search-results {
    position: absolute; top: calc(100% + 8px); left: 0; right: 0;
    background: var(--bg3); border: 1px solid var(--br2);
    border-radius: var(--r2); box-shadow: var(--sh2);
    display: none; z-index: 999; max-height: 320px; overflow-y: auto;
  }
  #search-results .sr-item {
    padding: 10px 14px; display: flex; align-items: center; gap: 10px;
    cursor: pointer; transition: background .1s; font-size: 13px;
  }
  #search-results .sr-item:hover { background: var(--bg4); }

  .container { padding: 24px; flex: 1; max-width: 1600px; }
  .page-title {
    font-size: 20px; font-weight: 800; margin-bottom: 24px;
    display: flex; align-items: center; gap: 12px;
    font-family: 'Space Mono', monospace; letter-spacing: -0.5px;
  }

  /* ── CARDS ──────────────────────────── */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 12px; margin-bottom: 24px;
  }
  .card {
    background: var(--bg2);
    border-radius: var(--r2); padding: 18px 16px;
    border: 1px solid var(--br1);
    position: relative; overflow: hidden;
    transition: all .2s;
    cursor: default;
  }
  .card::before {
    content: ''; position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, var(--acc), var(--pur));
    opacity: 0; transition: opacity .2s;
  }
  .card:hover { border-color: var(--br2); transform: translateY(-2px); box-shadow: var(--sh1); }
  .card:hover::before { opacity: 1; }
  .card-icon { font-size: 22px; margin-bottom: 10px; display: block; }
  .card-value { font-size: 26px; font-weight: 800; line-height: 1;
    font-family: 'Space Mono', monospace; }
  .card-label { font-size: 11px; color: var(--t2); margin-top: 6px;
    text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
  .card-value[data-live] { transition: color .3s; }

  /* ── SECTION ────────────────────────── */
  .section {
    background: var(--bg2);
    border-radius: var(--r2);
    border: 1px solid var(--br1);
    margin-bottom: 16px;
    overflow: hidden;
    transition: border-color .2s;
  }
  .section:hover { border-color: var(--br2); }
  .section-header {
    padding: 14px 18px;
    font-size: 12px; font-weight: 700; color: var(--t2);
    border-bottom: 1px solid var(--br0);
    display: flex; align-items: center; gap: 8px;
    text-transform: uppercase; letter-spacing: 1.5px;
    background: rgba(255,255,255,.02);
  }
  .section-body { padding: 16px 18px; }

  /* ── GRID ───────────────────────────── */
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }

  /* ── BUTTONS ────────────────────────── */
  .btn {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 8px 16px; border-radius: var(--r);
    font-size: 13px; font-weight: 600;
    border: 1px solid transparent; cursor: pointer;
    font-family: 'Outfit', sans-serif;
    transition: all .15s;
    text-decoration: none; white-space: nowrap;
    touch-action: manipulation;
  }
  .btn:hover { transform: translateY(-1px); opacity: .9; }
  .btn:active { transform: translateY(0) scale(.98); }
  .btn-primary  {
    background: var(--acc); color: #fff; border-color: var(--acc);
    box-shadow: 0 0 16px rgba(88,101,242,.25);
  }
  .btn-primary:hover { box-shadow: 0 0 24px rgba(88,101,242,.4); }
  .btn-danger   { background: var(--red-g); color: var(--red); border-color: rgba(248,113,113,.25); }
  .btn-success  { background: var(--acc-g); color: var(--acc); border-color: rgba(0,229,160,.2); }
  .btn-warn     { background: var(--ylw-g); color: var(--ylw); border-color: rgba(251,191,36,.2); }
  .btn-outline  { background: transparent; border-color: var(--br2); color: var(--t2); }
  .btn-outline:hover { color: var(--t1); border-color: var(--acc); }
  .btn-ghost    { background: var(--bg4); border-color: var(--br1); color: var(--t2); }
  .btn-ghost:hover { color: var(--t1); }
  .btn-sm { padding: 5px 10px; font-size: 12px; border-radius: 8px; }
  .btn-xs { padding: 3px 8px; font-size: 11px; border-radius: 6px; }

  /* ── FORMS ──────────────────────────── */
  .form-group { margin-bottom: 16px; }
  .form-group label {
    display: block; margin-bottom: 6px;
    font-size: 11px; color: var(--t2); font-weight: 700;
    letter-spacing: 1px; text-transform: uppercase;
  }
  .form-control {
    width: 100%; padding: 10px 14px;
    background: var(--bg3); border: 1px solid var(--br1);
    border-radius: var(--r); color: var(--t1); font-size: 13px;
    font-family: 'Outfit', sans-serif;
    transition: all .2s;
  }
  .form-control:focus {
    outline: none; border-color: var(--acc);
    box-shadow: 0 0 0 3px var(--acc-s);
  }
  .form-control:hover { border-color: var(--br2); }
  select.form-control option { background: var(--bg3); }
  textarea.form-control { resize: vertical; min-height: 80px; }

  /* ── TABLE ──────────────────────────── */
  table { width: 100%; border-collapse: collapse; }
  th {
    padding: 10px 14px; text-align: left;
    font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
    text-transform: uppercase; color: var(--t3);
    background: rgba(255,255,255,.02);
    border-bottom: 1px solid var(--br1);
    font-family: 'Space Mono', monospace;
  }
  td {
    padding: 11px 14px; font-size: 13px;
    border-bottom: 1px solid var(--br0);
    color: var(--t1);
    transition: background .1s;
  }
  tr:hover td { background: rgba(255,255,255,.02); }
  tr:last-child td { border-bottom: none; }
  code { font-family: 'Space Mono', monospace; font-size: 12px;
    background: var(--bg4); padding: 2px 6px; border-radius: 5px; color: var(--acc); }

  /* ── BADGE ──────────────────────────── */
  .badge {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 2px 8px; border-radius: 20px;
    font-size: 11px; font-weight: 700;
    font-family: 'Space Mono', monospace;
  }
  .badge-open     { background: rgba(251,191,36,.15); color: var(--ylw); border: 1px solid rgba(251,191,36,.2); }
  .badge-progress { background: var(--blue-g); color: var(--blue); border: 1px solid rgba(59,130,246,.2); }
  .badge-closed   { background: var(--acc-g); color: var(--acc); border: 1px solid rgba(0,229,160,.2); }
  .badge-danger   { background: var(--red-g); color: var(--red); border: 1px solid rgba(248,113,113,.2); }
  .badge-accent   { background: var(--pur-g); color: var(--pur); border: 1px solid rgba(167,139,250,.2); }

  /* ── TOGGLE ─────────────────────────── */
  .toggle-wrap {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 0; border-bottom: 1px solid var(--br0);
  }
  .toggle-wrap:last-child { border-bottom: none; }
  .toggle-info b { display: block; font-size: 13px; color: var(--t1); font-weight: 600; }
  .toggle-info span { font-size: 12px; color: var(--t2); }
  .toggle-switch { position: relative; display: inline-block; width: 40px; height: 22px; flex-shrink: 0; }
  .toggle-switch input { opacity: 0; width: 0; height: 0; }
  .toggle-slider {
    position: absolute; cursor: pointer; inset: 0;
    background: var(--bg5); border-radius: 22px;
    border: 1px solid var(--br2); transition: all .2s;
  }
  .toggle-slider::before {
    content: ''; position: absolute;
    width: 16px; height: 16px; left: 2px; bottom: 2px;
    background: var(--t3); border-radius: 50%; transition: all .2s;
  }
  input:checked + .toggle-slider { background: var(--acc-g); border-color: rgba(0,229,160,.4); }
  input:checked + .toggle-slider::before {
    transform: translateX(18px); background: var(--acc);
    box-shadow: 0 0 8px rgba(0,229,160,.5);
  }

  /* ── MODAL ──────────────────────────── */
  .modal-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,.7);
    display: none; align-items: center; justify-content: center;
    z-index: 500; backdrop-filter: blur(4px);
    padding: 20px;
  }
  .modal-overlay.active { display: flex; }
  .modal {
    background: var(--bg3); border: 1px solid var(--br2);
    border-radius: var(--r3); padding: 28px;
    max-width: 500px; width: 100%; max-height: 90vh; overflow-y: auto;
    box-shadow: var(--sh2);
    animation: modalIn .2s cubic-bezier(.34,1.56,.64,1);
  }
  @keyframes modalIn { from { transform: scale(.95); opacity: 0; } to { transform: scale(1); opacity: 1; } }
  .modal-title { font-size: 16px; font-weight: 800; margin-bottom: 20px;
    font-family: 'Space Mono', monospace; }
  .modal-footer { display: flex; gap: 8px; justify-content: flex-end; margin-top: 20px; }

  /* ── ALERT / EMPTY STATE ────────────── */
  .alert { padding: 12px 16px; border-radius: var(--r); margin-bottom: 14px; font-size: 13px; }
  .alert-success { background: var(--acc-g); color: var(--acc); border: 1px solid rgba(0,229,160,.2); }
  .alert-danger  { background: var(--red-g); color: var(--red); border: 1px solid rgba(248,113,113,.2); }
  .alert-info    { background: var(--blue-g); color: var(--blue); border: 1px solid rgba(59,130,246,.2); }
  .empty-state {
    text-align: center; padding: 40px 20px;
    color: var(--t3); font-size: 14px;
  }

  /* ── TOAST ──────────────────────────── */
  #toast-container {
    position: fixed; bottom: 24px; right: 24px;
    display: flex; flex-direction: column; gap: 8px;
    z-index: 9999;
  }
  .toast {
    display: flex; align-items: center; gap: 10px;
    padding: 12px 18px; border-radius: var(--r2);
    background: var(--bg4); border: 1px solid var(--br2);
    box-shadow: var(--sh1); font-size: 13px; font-weight: 500;
    animation: toastIn .3s ease;
    max-width: 320px;
  }
  .toast.success { border-color: rgba(0,229,160,.3); }
  .toast.danger  { border-color: rgba(248,113,113,.3); }
  @keyframes toastIn { from { transform: translateX(120%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
  @keyframes toastOut { to { transform: translateX(120%); opacity: 0; } }

  /* ── RANK CARD ──────────────────────── */
  .rank-card {
    display: flex; align-items: center; gap: 12px;
    padding: 10px 14px; border-radius: var(--r);
    background: var(--bg3); border: 1px solid var(--br1);
    margin-bottom: 6px; transition: all .15s;
  }
  .rank-card:hover { border-color: var(--br2); background: var(--bg4); }
  .rank-badge {
    min-width: 32px; height: 32px; border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 12px; font-weight: 800; flex-shrink: 0;
    font-family: 'Space Mono', monospace;
  }
  .rank-name { font-size: 13px; font-weight: 700; }
  .rank-desc { font-size: 11px; color: var(--t2); margin-top: 2px; }

  /* ── DUTY CARD ──────────────────────── */
  .duty-card {
    background: var(--bg3); border: 1px solid var(--br1);
    border-radius: var(--r2); padding: 16px;
    transition: all .2s;
  }
  .duty-card:hover { border-color: var(--acc); box-shadow: 0 0 16px var(--acc-s); }

  /* ── PROGRESS ───────────────────────── */
  .progress-bar {
    height: 6px; background: var(--bg5); border-radius: 6px; overflow: hidden;
  }
  .progress-fill {
    height: 100%; border-radius: 6px;
    background: linear-gradient(90deg, var(--acc), var(--pur));
    transition: width .5s cubic-bezier(.4,0,.2,1);
    box-shadow: 0 0 8px rgba(0,229,160,.4);
  }

  /* ── BUBBLE CHAT ────────────────────── */
  .bubble-wrap { display: flex; flex-direction: column; gap: 12px; }
  .bubble {
    display: flex; gap: 10px; max-width: 85%;
  }
  .bubble.own { flex-direction: row-reverse; align-self: flex-end; }
  .bubble-body {
    background: var(--bg4); border: 1px solid var(--br1);
    padding: 10px 14px; border-radius: 14px; font-size: 13px;
    line-height: 1.5;
  }
  .bubble.own .bubble-body {
    background: var(--acc-g); border-color: rgba(0,229,160,.2);
  }

  /* ── GLOBAL SEARCH OVERLAY ──────────── */
  #qs-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,.7);
    z-index: 600; display: none; padding-top: 80px;
    justify-content: center;
    backdrop-filter: blur(8px);
  }
  #qs-overlay.open { display: flex; }
  #qs-box {
    background: var(--bg3); border: 1px solid var(--br2);
    border-radius: var(--r3); width: 560px; max-width: 95vw;
    box-shadow: var(--sh2); overflow: hidden;
    animation: modalIn .2s cubic-bezier(.34,1.56,.64,1);
  }
  #qs-input {
    width: 100%; padding: 18px 22px;
    background: transparent; border: none; border-bottom: 1px solid var(--br1);
    color: var(--t1); font-size: 16px; font-family: 'Outfit', sans-serif;
  }
  #qs-input:focus { outline: none; }
  #qs-results .qsr {
    padding: 12px 20px; cursor: pointer; font-size: 13px;
    display: flex; gap: 10px; align-items: center;
    transition: background .1s;
  }
  #qs-results .qsr:hover { background: var(--bg4); }

  /* ── MOBILE SIDEBAR OVERLAY ─────────── */
  .sidebar-overlay {
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,.6); z-index: 199;
    backdrop-filter: blur(4px);
  }
  .sidebar-overlay.active { display: block; }

  /* ── MOBILE BOTTOM NAV ──────────────── */
  #mobile-bottom-nav {
    display: none;
    position: fixed; bottom: 0; left: 0; right: 0;
    background: rgba(8,11,20,.95); border-top: 1px solid var(--br1);
    padding: 8px 0 env(safe-area-inset-bottom, 8px);
    z-index: 150; justify-content: space-around; align-items: center;
    backdrop-filter: blur(16px);
  }
  #mobile-bottom-nav a {
    display: flex; flex-direction: column; align-items: center;
    gap: 3px; color: var(--t3); text-decoration: none;
    font-size: 10px; padding: 4px 8px; border-radius: 8px;
    transition: color .15s; -webkit-tap-highlight-color: transparent;
  }
  #mobile-bottom-nav a.active, #mobile-bottom-nav a:hover { color: var(--acc); }
  #mobile-bottom-nav .nav-icon { font-size: 20px; }

  /* ── ADAPTIVE ───────────────────────── */
  @media (max-width: 900px) {
    .sidebar {
      transform: translateX(-100%);
      position: fixed; top: 0; left: 0; height: 100vh;
      z-index: 200; box-shadow: 4px 0 32px rgba(0,0,0,.8);
    }
    .sidebar.open { transform: translateX(0); }
    .main { margin-left: 0 !important; }
    .sidebar-toggle { display: flex !important; align-items: center; justify-content: center; }
    .grid-2, .grid-3 { grid-template-columns: 1fr !important; }
    .cards { grid-template-columns: 1fr 1fr; }
    .search-global { display: none; }
    .container { padding: 16px; }
    .page-title { font-size: 16px; margin-bottom: 16px; }
    table { display: block; overflow-x: auto; -webkit-overflow-scrolling: touch; }
    .modal { width: 95vw !important; max-height: 90vh; overflow-y: auto; padding: 20px; }
    #cc-grid { display: flex !important; flex-direction: column !important; height: auto !important; }
    #cc-left, #cc-right { width: 100% !important; height: 280px; border-right: none !important; border-left: none !important; border-bottom: 1px solid var(--br1); }
    #cc-center { min-height: 400px; }
    #mobile-bottom-nav { display: flex; }
    .main { padding-bottom: 70px; }
  }
  @media (max-width: 600px) {
    .cards { grid-template-columns: 1fr; }
    .topbar { padding: 0 12px; }
    .topbar-title { font-size: 13px; }
    .page-title { font-size: 14px; }
    .cards[style*="grid-template-columns:repeat(4"] { grid-template-columns: 1fr 1fr !important; }
  }
  @media (max-width: 400px) {
    .container { padding: 10px; }
    .cards { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
{body}

<!-- Toast Container -->
<div id="toast-container"></div>

<!-- Quick Search Overlay -->
<div id="qs-overlay" onclick="if(event.target===this)closeQS()">
  <div id="qs-box">
    <input id="qs-input" placeholder="🔍 Поиск по ID, тикетам, пользователям..." oninput="doQS(this.value)" onkeydown="handleQSKey(event)">
    <div id="qs-results"></div>
  </div>
</div>

<script>
// ─── Theme ───────────────────────────────
(function(){
  var t = localStorage.getItem('cg_theme') || 'dark';
  document.documentElement.setAttribute('data-theme', t);
})();
function toggleTheme(){
  var t = document.documentElement.getAttribute('data-theme');
  var n = t==='dark'?'light':'dark';
  document.documentElement.setAttribute('data-theme', n);
  localStorage.setItem('cg_theme', n);
}

// ─── Sidebar ─────────────────────────────
function toggleSidebar(){
  var sb = document.querySelector('.sidebar');
  var ov = document.getElementById('sb-overlay');
  var open = sb.classList.toggle('open');
  if(ov) ov.classList.toggle('active', open);
  document.body.style.overflow = open ? 'hidden' : '';
}
function closeSidebar(){
  document.querySelector('.sidebar').classList.remove('open');
  var ov = document.getElementById('sb-overlay');
  if(ov) ov.classList.remove('active');
  document.body.style.overflow = '';
}

// ─── Toast ───────────────────────────────
function showToast(msg, type='info', dur=4000){
  var icons = {success:'✅',danger:'❌',info:'ℹ️',warn:'⚠️'};
  var t = document.createElement('div');
  t.className = 'toast ' + type;
  t.innerHTML = '<span style="font-size:16px;">' + (icons[type]||'📢') + '</span><span>' + msg + '</span>';
  document.getElementById('toast-container').appendChild(t);
  setTimeout(function(){
    t.style.animation = 'toastOut .3s ease forwards';
    setTimeout(function(){ t.remove(); }, 300);
  }, dur);
}

// ─── Modal ───────────────────────────────
function openModal(id){ document.getElementById(id).classList.add('active'); }
function closeModal(id){ document.getElementById(id).classList.remove('active'); }
document.addEventListener('keydown', function(e){
  if(e.key==='Escape') document.querySelectorAll('.modal-overlay.active').forEach(function(m){ m.classList.remove('active'); });
  if((e.ctrlKey||e.metaKey) && e.key==='k'){ e.preventDefault(); openQS(); }
});
function toggleTheme(){
  var h=document.documentElement,c=h.getAttribute('data-theme');
  var n=c==='dark'?'light':'dark';
  h.setAttribute('data-theme',n);
  localStorage.setItem('dtheme',n);
  var b=document.getElementById('theme-btn');
  if(b)b.textContent=n==='dark'?'🌙':'☀️';
}
(function(){
  var s=localStorage.getItem('dtheme');
  if(s){document.documentElement.setAttribute('data-theme',s);
  var b=document.getElementById('theme-btn');if(b)b.textContent=s==='dark'?'🌙':'☀️';}
})();

// ─── Quick Search ─────────────────────────
function openQS(){ document.getElementById('qs-overlay').classList.add('open'); document.getElementById('qs-input').focus(); }
function closeQS(){ document.getElementById('qs-overlay').classList.remove('open'); document.getElementById('qs-results').innerHTML=''; document.getElementById('qs-input').value=''; }
var _qsTimer=null;
function doQS(q){
  clearTimeout(_qsTimer);
  if(!q.trim()){ document.getElementById('qs-results').innerHTML=''; return; }
  _qsTimer = setTimeout(function(){
    fetch('/api/search?q='+encodeURIComponent(q))
      .then(function(r){return r.json();})
      .then(function(d){
        var html = '';
        (d.users||[]).forEach(function(u){
          html += '<div class="qsr" onclick="window.location=\'/dashboard/users/'+u.uid+'\'"><span>👤</span><span>'+u.name+' <code>'+u.uid+'</code></span></div>';
        });
        (d.tickets||[]).forEach(function(t){
          html += '<div class="qsr" onclick="window.location=\'/dashboard/tickets/'+t.id+'\'"><span>🎫</span><span>#'+t.id+' '+t.subject+'</span></div>';
        });
        (d.chats||[]).forEach(function(c){
          html += '<div class="qsr" onclick="window.location=\'/dashboard/chats/'+c.cid+'\'"><span>💬</span><span>'+c.title+' <code>'+c.cid+'</code></span></div>';
        });
        if(!html) html = '<div class="qsr" style="color:var(--text2);">Ничего не найдено</div>';
        document.getElementById('qs-results').innerHTML = html;
      }).catch(function(){});
  }, 250);
}
function handleQSKey(e){
  if(e.key==='Escape') closeQS();
  if(e.key==='Enter'){
    var v = document.getElementById('qs-input').value.trim();
    if(/^\\d+$/.test(v)) window.location='/dashboard/users/'+v;
  }
}

// Закрываем sidebar при навигации на мобиле
document.addEventListener('DOMContentLoaded', function(){
  document.querySelectorAll('.nav-link').forEach(function(link){
    link.addEventListener('click', function(){
      if(window.innerWidth <= 900) closeSidebar();
    });
  });
  // Свайп влево закрывает sidebar
  var ts = 0;
  document.addEventListener('touchstart', function(e){ ts = e.touches[0].clientX; }, {passive:true});
  document.addEventListener('touchend', function(e){
    var dx = e.changedTouches[0].clientX - ts;
    if(dx < -50 && document.querySelector('.sidebar.open')) closeSidebar();
  }, {passive:true});
});

// ─── Live Stats (SSE) ─────────────────────
(function(){
  try {
    var es = new EventSource('/dashboard/events');
    es.onmessage = function(e){
      if(e.data==='connected') return;
      try {
        var d = JSON.parse(e.data);
        if(d.type==='new_ticket'){
          showToast('🎫 Новый тикет #'+d.id+' от '+d.user, 'info', 8000);
          var el = document.querySelector('[data-live="tickets_open"]');
          if(el) el.textContent = parseInt(el.textContent||0)+1;
        }
        if(d.type==='alert'){
          showToast('🚨 '+d.title, 'warn', 6000);
          var el2 = document.querySelector('[data-live="alerts"]');
          if(el2) el2.textContent = parseInt(el2.textContent||0)+1;
        }
        if(d.type==='stats'){
          Object.keys(d).forEach(function(k){
            var el3 = document.querySelector('[data-live="'+k+'"]');
            if(el3) el3.textContent = d[k];
          });
        }
      } catch(err){}
    };
  } catch(err){}
})();

// ─── Auto-refresh stats every 15s ─────────
setInterval(function(){
  fetch('/api/live').then(function(r){return r.json();}).then(function(d){
    if(d.online!==undefined){
      var el=document.querySelector('[data-live="online"]');
      if(el) el.textContent=d.online;
    }
    if(d.messages!==undefined){
      var el=document.querySelector('[data-live="messages"]');
      if(el) el.textContent=(d.messages||0).toLocaleString();
    }
  }).catch(function(){});
}, 15000);

// ─── AJAX Actions (ban/mute/warn) ─────────
function modAction(action, uid, cid, reason, callback){
  if(!confirm('Подтвердить: '+action+' для пользователя '+uid+'?')) return;
  fetch('/api/modaction', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({action:action, uid:uid, cid:cid, reason:reason||'Нарушение правил'})
  }).then(function(r){return r.json();}).then(function(d){
    showToast(d.ok ? '✅ '+d.message : '❌ '+d.error, d.ok?'success':'danger');
    if(d.ok && callback) callback(d);
  }).catch(function(){ showToast('❌ Ошибка соединения','danger'); });
}
</script>
</body>
</html>"""


def page(body: str) -> str:
    return HTML_BASE.replace("{body}", body)


def navbar(sess: dict | None = None, active: str = "") -> str:
    if not sess:
        return ""
    rank = sess.get("rank", 1)
    rank_info = DASHBOARD_RANKS.get(rank, DASHBOARD_RANKS[1])
    uname = sess.get("name", "Аноним")
    rank_color = rank_info["color"]

    def link(key, url, icon, label, perm="view_overview", badge=""):
        token = sess.get("_token", "")
        if not _has_perm(token, perm):
            return ""
        a_cls = "nav-link active" if active == key else "nav-link"
        bdg = f'<span class="nav-badge">{badge}</span>' if badge else ""
        return f'<a href="{url}" class="{a_cls}"><span class="nav-icon">{icon}</span>{label}{bdg}</a>'

    # Считаем открытые тикеты для бейджа
    try:
        from database import ticket_stats_all
        import asyncio as _aio
        ts = {"open": 0}
    except:
        ts = {"open": 0}

    nav = f"""
    <button class="sidebar-toggle" onclick="toggleSidebar()" aria-label="Меню" style="font-size:20px;line-height:1;">☰</button>
    <div class="sidebar-overlay" id="sb-overlay" onclick="closeSidebar()"></div>
    <div class="sidebar">
      <div class="sidebar-brand">
        <div class="logo">⚡</div>
        <div>
          <div class="brand-text">CHAT GUARD</div>
          <div class="brand-sub">Dashboard v2.0</div>
        </div>
      </div>
      <div class="sidebar-user">
        <div class="u-name">👤 {uname}</div>
        <div class="u-rank" style="background:rgba({_hex_to_rgb(rank_color)},.15);color:{rank_color};">
          {rank_info['name']}
        </div>
      </div>
      <nav class="sidebar-nav">
        <div class="nav-section">Обзор</div>
        {link("overview", "/dashboard", "📊", "Главная", "view_overview")}
        {link("chats", "/dashboard/chats", "💬", "Чаты", "view_chats")}
        {link("users", "/dashboard/users", "👥", "Пользователи", "view_users")}

        <div class="nav-section">Работа</div>
        {link("tickets", "/dashboard/tickets", "🎫", "Тикеты", "view_tickets")}
        {link("reports", "/dashboard/reports", "🚨", "Репорты", "view_reports")}
        {link("moderation", "/dashboard/moderation", "🛡", "Модерация", "view_moderation")}
        {link("alerts", "/dashboard/alerts", "🔴", "Алерты", "view_alerts")}

        <div class="nav-section">Данные</div>
        {link("media", "/dashboard/media", "🎬", "Медиа-лог", "view_media")}
        {link("deleted", "/dashboard/deleted", "🗑", "Удалённые", "view_deleted")}
        {link("economy", "/dashboard/economy", "💰", "Экономика", "view_economy")}
        {link("analytics", "/dashboard/analytics", "📈", "Аналитика", "view_overview")}

        <div class="nav-section">Управление</div>
        {link("plugins", "/dashboard/plugins", "🧩", "Плагины", "manage_plugins")}
        {link("broadcast", "/dashboard/broadcast", "📢", "Рассылка", "broadcast")}
        {link("chat_settings", "/dashboard/chat_settings", "⚙️", "Настройки чатов", "manage_chat_settings")}
        {link("security", "/dashboard/security", "🛡", "Защита", "view_moderation")}
        {link("admins", "/dashboard/admins", "👑", "Администраторы", "view_overview") if rank >= OWNER_RANK else ""}
        {f'<a href="/dashboard/owner" class="nav-link{"  active" if active=="owner" else ""}">'
         f'<span class="nav-icon">🌟</span><span>Панель владельца</span></a>'
         if rank >= OWNER_RANK else ""}
        {link("settings", "/dashboard/settings", "🔧", "Настройки", "view_settings")}

        <div class="nav-section">Инструменты</div>
        {link("activity_map", "/dashboard/activity_map", "🗺", "Карта активности", "view_overview")}
        {link("achievements", "/dashboard/achievements", "🎯", "Достижения", "view_overview")}
        {link("team_chat",    "/dashboard/team_chat",    "💬", "Чат команды", "view_overview")}
        {link("threats",      "/dashboard/threats",      "🔭", "Разведка угроз", "view_alerts")}
        {link("appeals",      "/dashboard/appeals",      "⚖️", "Апелляции", "view_reports")}
        {link("msg_search",   "/dashboard/msg_search",   "🔍", "Поиск сообщений", "view_deleted")}
        {link("wiki",         "/dashboard/wiki",         "📚", "Wiki команды",     "view_overview")}
        {link("incidents",    "/dashboard/incidents",    "🚨", "Инциденты",        "view_overview")}
        {link("economy",       "/dashboard/economy",       "💰", "Экономика",        "view_overview")}
        {link("command_center","/dashboard/command_center","🎮", "Command Center",  "view_overview")}
        {link("automations",  "/dashboard/automations",  "⚡", "Автоправила",      "view_overview")}
        {link("reports_cfg",  "/dashboard/reports_cfg",  "📊", "Авто-отчёты",      "view_overview")}
        {link("bot_control",  "/dashboard/bot_control",  "🤖", "Управление ботом", "view_overview")}
        {link("voice",        "/dashboard/voice",        "🎙", "Голосовые",        "view_overview")}
        {link("themes",       "/dashboard/themes",       "🎨", "Темы", "view_overview")}
      </nav>
      <div class="sidebar-footer">
        <button onclick="toggleTheme()" class="btn btn-ghost btn-xs">🌙/☀️</button>
        <button onclick="openQS()" class="btn btn-ghost btn-xs" title="Ctrl+K">🔍</button>
        <a href="/dashboard/logout" class="btn btn-xs" style="background:rgba(239,68,68,.1);color:var(--danger);">Выход</a>
      </div>
    </div>
    <div class="main">
    <div class="topbar">
      <div class="topbar-left">
        <button class="btn btn-ghost btn-xs" onclick="toggleSidebar()" style="display:none" id="mob-menu">☰</button>
      </div>
      <div class="topbar-right">
        <button onclick="openQS()" class="btn btn-ghost btn-sm">🔍 Поиск <kbd style="font-size:10px;opacity:.5;">Ctrl+K</kbd></button>
        <span style="font-size:12px;color:var(--text2);">👤 {uname}</span>
      </div>
    </div>
    """
    return nav


def _hex_to_rgb(hex_color: str) -> str:
    h = hex_color.lstrip('#')
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"{r},{g},{b}"


def close_main() -> str:
    return """</div>
<nav id="mobile-bottom-nav">
  <a href="/dashboard"><span style="font-size:20px;">📊</span><span>Обзор</span></a>
  <a href="/dashboard/chats"><span style="font-size:20px;">💬</span><span>Чаты</span></a>
  <a href="/dashboard/command_center"><span style="font-size:20px;">🎮</span><span>Команды</span></a>
  <a href="/dashboard/tickets"><span style="font-size:20px;">🎫</span><span>Тикеты</span></a>
  <a href="#" onclick="toggleSidebar();return false;"><span style="font-size:20px;">☰</span><span>Меню</span></a>
</nav>
<script>
(function(){{
  var path=window.location.pathname;
  document.querySelectorAll('#mobile-bottom-nav a').forEach(function(a){{
    if(a.href && path===new URL(a.href,location).pathname) a.style.color='var(--accent)';
  }});
}})();
</script>"""


# ══════════════════════════════════════════
#  LOGIN / AUTH
# ══════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════
#  🔒 ЗАЩИТА СЕССИЙ — БРУТФОРС + IP WATCHDOG
# ══════════════════════════════════════════════════════════════════

_login_attempts: dict = {}   # {ip: [timestamps]}
_ip_blocked:    dict = {}   # {ip: unblock_timestamp}
_BRUTE_MAX       = 5        # попыток
_BRUTE_WINDOW    = 300      # секунд (5 мин)
_BRUTE_BLOCK     = 900      # блок на 15 мин
_SESSION_IP_LOCK = True     # force-logout при смене IP

def _brute_check(ip: str) -> bool:
    """True = IP заблокирован."""
    now = time.time()
    unblock = _ip_blocked.get(ip, 0)
    if now < unblock:
        return True
    return False

def _brute_register_fail(ip: str):
    """Фиксирует неудачную попытку. Блокирует при превышении лимита."""
    now = time.time()
    attempts = [t for t in _login_attempts.get(ip, []) if now - t < _BRUTE_WINDOW]
    attempts.append(now)
    _login_attempts[ip] = attempts
    if len(attempts) >= _BRUTE_MAX:
        _ip_blocked[ip] = now + _BRUTE_BLOCK
        log.warning(f"[BRUTE] IP {ip} заблокирован на {_BRUTE_BLOCK//60} мин")
        # Попытка уведомить владельца
        try:
            asyncio.ensure_future(_brute_notify_owner(ip, len(attempts)))
        except:
            pass

def _brute_register_success(ip: str):
    """Сбрасывает счётчик при успехе."""
    _login_attempts.pop(ip, None)
    _ip_blocked.pop(ip, None)

async def _brute_notify_owner(ip: str, count: int):
    """Шлёт алерт владельцу в Telegram."""
    if not _bot:
        return
    try:
        await _bot.send_message(
            OWNER_TG_ID,
            f"🚨 <b>ALERT: Брутфорс атака!</b>\n\n"
            f"🌐 IP: <code>{ip}</code>\n"
            f"🔑 Попыток: <b>{count}</b>\n"
            f"⏰ Заблокирован на 15 минут\n\n"
            f"<i>Dashboard Security Monitor</i>",
            parse_mode="HTML"
        )
    except:
        pass

def _session_ip_check(request, sess: dict | None) -> bool:
    """True = сессия валидна по IP."""
    if not sess or not _SESSION_IP_LOCK:
        return True
    current_ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()
    session_ip = sess.get("ip", "")
    if session_ip and current_ip and current_ip != session_ip:
        log.warning(f"[SESSION] IP сменился: {session_ip} → {current_ip} для uid={sess.get('uid')}")
        return False
    return True

def _brute_remaining(ip: str) -> int:
    """Возвращает секунд до разблокировки."""
    return max(0, int(_ip_blocked.get(ip, 0) - time.time()))

async def handle_login(request: web.Request):
    error = ""
    ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()

    # Брутфорс проверка
    if _brute_check(ip):
        remaining = _brute_remaining(ip)
        body = f"""
        <div class="login-bg">
          <div class="login-box">
            <div class="login-logo">🚫</div>
            <div class="login-title">Доступ заблокирован</div>
            <div class="login-sub" style="color:var(--danger);">
              Слишком много неудачных попыток.<br>
              Попробуйте через <b>{remaining // 60} мин {remaining % 60} сек</b>.
            </div>
            <p style="text-align:center;margin-top:16px;font-size:12px;color:var(--text2);">
              IP: {ip}
            </p>
          </div>
        </div>"""
        return web.Response(text=HTML_BASE.replace("{body}", body), content_type="text/html", status=429)

    if request.method == "POST":
        data = await request.post()
        token = data.get("token", "")
        code = data.get("code", "")
        sess_id = data.get("sess_id", "")
        tg_uid_str = data.get("tg_uid", "")

        # Step 2: проверяем 2FA код
        if sess_id and code and tg_uid_str:
            pending = _2fa_pending.get(sess_id)
            try:
                tg_uid = int(tg_uid_str)
            except:
                tg_uid = 0
            if pending and pending["code"] == code.strip() and time.time() < pending["expires"]:
                admin = _get_admin(tg_uid)
                if admin:
                    _2fa_pending.pop(sess_id, None)
                    new_sess = secrets.token_hex(24)
                    ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()
                    _dashboard_sessions[new_sess] = {
                        "uid": tg_uid,
                        "name": admin["name"],
                        "rank": admin["rank"],
                        "login_time": time.time(),
                        "ip": ip,
                        "_token": new_sess,
                    }
                    _brute_register_success(ip)
                    _log_admin_db(tg_uid, "LOGIN", f"Успешный вход с {ip}", ip)
                    # Обновляем last_login
                    try:
                        conn = db.get_conn()
                        conn.execute("UPDATE dashboard_admins SET last_login=datetime('now') WHERE tg_uid=?", (tg_uid,))
                        conn.commit()
                        conn.close()
                    except:
                        pass
                    response = web.HTTPFound("/dashboard")
                    response.set_cookie("dsess_token", new_sess, max_age=86400 * 7, httponly=True, samesite="Lax")
                    raise response
                else:
                    error = "❌ Ваш Telegram ID не добавлен в систему. Обратитесь к владельцу."
            else:
                _2fa_pending.pop(sess_id, None)
                error = "❌ Неверный или истёкший код"

            # Показываем форму кода снова или ошибку
            body = f"""
            <div class="login-bg">
              <div class="login-box">
                <div class="login-logo">🔐</div>
                <div class="login-title">Код подтверждения</div>
                <div class="login-sub">Введите 6-значный код из Telegram</div>
                <form method="POST">
                  <input type="hidden" name="sess_id" value="{sess_id}">
                  <input type="hidden" name="tg_uid" value="{tg_uid_str}">
                  <div class="form-group">
                    <label>Код из Telegram</label>
                    <input class="form-control" type="text" name="code" placeholder="123456"
                      maxlength="6" autofocus style="font-size:22px;text-align:center;letter-spacing:8px;">
                  </div>
                  <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;">✅ Подтвердить</button>
                </form>
                {"" if not error else f'<p style="color:var(--danger);text-align:center;margin-top:12px;font-size:13px;">{error}</p>'}
                <p style="text-align:center;margin-top:16px;"><a href="/dashboard/login" style="color:var(--text2);font-size:12px;">← Назад</a></p>
              </div>
            </div>"""
            return web.Response(text=HTML_BASE.replace("{body}", body), content_type="text/html")

        # Step 1: проверяем токен + tg_uid
        if token != DASHBOARD_TOKEN and token:
            _brute_register_fail(ip)
            error = "❌ Неверный токен"
        if token == DASHBOARD_TOKEN and tg_uid_str:
            try:
                tg_uid = int(tg_uid_str)
            except:
                error = "❌ Неверный Telegram ID"
                tg_uid = 0

            if tg_uid:
                admin = _get_admin(tg_uid)
                if not admin:
                    _brute_register_fail(ip)
                    error = "❌ Ваш Telegram ID не добавлен в систему. Обратитесь к владельцу."
                else:
                    code_val = _gen_2fa_code()
                    sess_id = secrets.token_hex(16)
                    _2fa_pending[sess_id] = {"code": code_val, "expires": time.time() + 300, "uid": tg_uid}
                    sent = await _send_2fa_code(tg_uid, code_val)
                    if not sent and tg_uid == OWNER_TG_ID:
                        # Владелец — пропускаем 2FA если бот не отвечает
                        new_sess = secrets.token_hex(24)
                        ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()
                        _dashboard_sessions[new_sess] = {
                            "uid": tg_uid, "name": admin["name"],
                            "rank": admin["rank"], "login_time": time.time(), "ip": ip, "_token": new_sess,
                        }
                        response = web.HTTPFound("/dashboard")
                        response.set_cookie("dsess_token", new_sess, max_age=86400 * 7, httponly=True, samesite="Lax")
                        raise response

                    body = f"""
                    <div class="login-bg">
                      <div class="login-box">
                        <div class="login-logo">🔐</div>
                        <div class="login-title">Код подтверждения</div>
                        <div class="login-sub" style="color:var(--success);">✅ Код отправлен в Telegram</div>
                        <form method="POST" style="margin-top:20px;">
                          <input type="hidden" name="sess_id" value="{sess_id}">
                          <input type="hidden" name="tg_uid" value="{tg_uid}">
                          <div class="form-group">
                            <label>6-значный код</label>
                            <input class="form-control" type="text" name="code" placeholder="123456"
                              maxlength="6" autofocus style="font-size:22px;text-align:center;letter-spacing:8px;">
                          </div>
                          <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;">✅ Войти</button>
                        </form>
                        <p style="text-align:center;margin-top:16px;"><a href="/dashboard/login" style="color:var(--text2);font-size:12px;">← Назад</a></p>
                      </div>
                    </div>"""
                    return web.Response(text=HTML_BASE.replace("{body}", body), content_type="text/html")
        elif token and token != DASHBOARD_TOKEN:
            error = "❌ Неверный токен"

    body = f"""
    <div class="login-bg">
      <div class="login-box">
        <div class="login-logo">⚡</div>
        <div class="login-title">CHAT GUARD</div>
        <div class="login-sub">Панель управления v2.0</div>
        <form method="POST">
          <div class="form-group">
            <label>Ваш Telegram ID</label>
            <input class="form-control" type="number" name="tg_uid" placeholder="123456789" required autofocus>
            <div style="font-size:11px;color:var(--text2);margin-top:4px;">Должен быть добавлен владельцем</div>
          </div>
          <div class="form-group">
            <label>Токен доступа</label>
            <input class="form-control" type="password" name="token" placeholder="Введи токен...">
          </div>
          <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;font-size:15px;">🔐 Войти</button>
        </form>
        {"" if not error else f'<p style="color:var(--danger);text-align:center;margin-top:14px;font-size:13px;">{error}</p>'}
        <div style="margin-top:24px;padding-top:20px;border-top:1px solid var(--border);text-align:center;">
          <p style="font-size:12px;color:var(--text2);">Нет доступа? Напишите владельцу боту.</p>
        </div>
      </div>
    </div>"""
    return web.Response(text=HTML_BASE.replace("{body}", body), content_type="text/html")


async def handle_logout(request: web.Request):
    token = request.cookies.get("dsess_token")
    if token and token in _dashboard_sessions:
        sess = _dashboard_sessions.pop(token)
        _log_admin_db(sess.get("uid", 0), "LOGOUT", "Выход из дашборда")
    response = web.HTTPFound("/dashboard/login")
    response.del_cookie("dsess_token")
    raise response


# ══════════════════════════════════════════
#  ГЛАВНАЯ СТРАНИЦА
# ══════════════════════════════════════════

async def handle_overview(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    chats = [dict(r) for r in await db.get_all_chats()]
    ticket_stats = await db.ticket_stats_all()

    conn = db.get_conn()
    total_users = conn.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats").fetchone()[0] or 0
    total_msgs  = conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats").fetchone()[0] or 0
    total_bans  = conn.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    total_warns = conn.execute("SELECT COALESCE(SUM(count),0) FROM warnings").fetchone()[0] or 0

    recent_acts, top_mods = [], []
    try:
        act_rows = conn.execute(
            "SELECT action, reason, by_name, created_at FROM mod_history ORDER BY created_at DESC LIMIT 10"
        ).fetchall()
        recent_acts = [dict(r) for r in act_rows]
        mod_rows = conn.execute(
            "SELECT by_name, COUNT(*) as cnt FROM mod_history GROUP BY by_name ORDER BY cnt DESC LIMIT 6"
        ).fetchall()
        top_mods = [dict(r) for r in mod_rows]
    except:
        pass

    # Today's actions count
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_acts = conn.execute(
            "SELECT COUNT(*) FROM mod_history WHERE created_at LIKE ?", (today_str + "%",)
        ).fetchone()[0] or 0
    except:
        today_acts = 0

    conn.close()

    online_count = shared.get_online_count()
    alerts_count = len(shared.alerts)

    # ── Stat cards ──────────────────────────────────────────────────────────
    def stat_card(icon, label, value, sub="", color="var(--acc)", live_key=""):
        live = f' data-live="{live_key}"' if live_key else ""
        return f"""
        <div class="card" style="--card-accent:{color};">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div>
              <div style="font-size:11px;color:var(--t2);font-weight:700;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px;">{label}</div>
              <div style="font-size:28px;font-weight:900;font-family:'Space Mono',monospace;color:{color};line-height:1;"{live}>{value}</div>
              {f'<div style="font-size:11px;color:var(--t3);margin-top:6px;">{sub}</div>' if sub else ''}
            </div>
            <div style="font-size:28px;opacity:.4;">{icon}</div>
          </div>
          <div style="margin-top:12px;height:2px;background:var(--br1);border-radius:2px;overflow:hidden;">
            <div style="height:100%;width:60%;background:{color};border-radius:2px;opacity:.5;"></div>
          </div>
        </div>"""

    cards_html = f"""
    <div class="cards" style="grid-template-columns:repeat(4,1fr);">
      {stat_card("💬", "Чатов", len(chats), "подключено", "var(--acc)")}
      {stat_card("👥", "Участников", f"{total_users:,}", "уникальных", "var(--blue)", "messages")}
      {stat_card("🟢", "Онлайн", online_count, "за 5 минут", "var(--green)", "online")}
      {stat_card("📨", "Сообщений", f"{total_msgs:,}", "всего", "var(--pur)")}
    </div>
    <div class="cards" style="grid-template-columns:repeat(4,1fr);margin-top:0;">
      {stat_card("🔨", "Банов", total_bans, "активных", "var(--red)")}
      {stat_card("⚡", "Варнов", total_warns, "активных", "var(--ylw)")}
      {stat_card("🎫", "Тикетов", ticket_stats['open'], f"из {ticket_stats['total']} всего", "var(--cyan)", "tickets_open")}
      {stat_card("🚨", "Алертов", alerts_count, "требуют внимания", "var(--red)", "alerts")}
    </div>"""

    # ── Top chats ────────────────────────────────────────────────────────────
    chat_rows_html = ""
    for i, c in enumerate(chats[:8]):
        cid = c["cid"]
        title = c.get("title") or str(cid)
        cc = db.get_conn()
        msgs  = cc.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        users = cc.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        bans  = cc.execute("SELECT COUNT(*) FROM ban_list WHERE cid=?", (cid,)).fetchone()[0] or 0
        cc.close()
        num_badge = f'<span style="display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;background:var(--bg4);border-radius:6px;font-size:10px;font-weight:800;color:var(--t3);font-family:Space Mono,monospace;">{i+1}</span>'
        chat_rows_html += f"""
        <tr>
          <td>{num_badge} <a href="/dashboard/chats/{cid}" style="color:var(--t1);font-weight:600;text-decoration:none;">{title[:28]}</a></td>
          <td><span style="font-family:Space Mono,monospace;color:var(--blue);">{users:,}</span></td>
          <td><span style="font-family:Space Mono,monospace;color:var(--pur);">{msgs:,}</span></td>
          <td><span style="font-family:Space Mono,monospace;color:var(--red);">{bans}</span></td>
          <td><a href="/dashboard/chats/{cid}" class="btn btn-xs btn-ghost">Открыть →</a></td>
        </tr>"""

    # ── Recent actions ────────────────────────────────────────────────────────
    def act_color(act):
        if "Бан" in act: return "var(--red)"
        if "Мут" in act: return "var(--ylw)"
        if "Варн" in act: return "var(--ylw)"
        if "Разбан" in act or "Размут" in act: return "var(--green)"
        return "var(--t2)"

    act_rows_html = ""
    for r in recent_acts:
        dt  = str(r.get("created_at", ""))[:16].replace("T", " ")
        act = r.get("action", "—")
        by  = r.get("by_name", "—")
        reason = (r.get("reason") or "—")[:28]
        color = act_color(act)
        act_rows_html += f"""
        <tr>
          <td style="font-family:Space Mono,monospace;font-size:10px;color:var(--t3);white-space:nowrap;">{dt}</td>
          <td><span style="color:{color};font-weight:700;font-size:12px;">{act}</span></td>
          <td style="color:var(--t2);font-size:12px;">{reason}</td>
          <td style="font-size:12px;color:var(--t2);">{by}</td>
        </tr>"""

    # ── Top mods ─────────────────────────────────────────────────────────────
    max_cnt = top_mods[0]["cnt"] if top_mods else 1
    mod_rows_html = ""
    for i, r in enumerate(top_mods):
        pct = int(r["cnt"] / max_cnt * 100)
        medal = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"][i] if i < 6 else f"{i+1}."
        mod_rows_html += f"""
        <tr>
          <td style="font-size:14px;">{medal}</td>
          <td style="font-weight:600;">👮 {r['by_name']}</td>
          <td>
            <div style="display:flex;align-items:center;gap:8px;">
              <div style="flex:1;height:4px;background:var(--bg4);border-radius:4px;overflow:hidden;">
                <div style="height:100%;width:{pct}%;background:var(--acc);border-radius:4px;"></div>
              </div>
              <span style="font-family:Space Mono,monospace;font-size:12px;color:var(--acc);min-width:30px;text-align:right;">{r['cnt']}</span>
            </div>
          </td>
        </tr>"""

    # ── Active sessions ────────────────────────────────────────────────────────
    sessions_html = ""
    for s in _get_active_sessions():
        ago = int((time.time() - s["last_seen"]) // 60)
        sessions_html += f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:10px 18px;border-bottom:1px solid var(--br0);">
          <div style="display:flex;align-items:center;gap:10px;">
            <span style="width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;
                         box-shadow:0 0 6px var(--green);"></span>
            <code style="font-size:12px;">{s['ip']}</code>
            <span style="font-size:11px;color:var(--t3);">{s.get('current','')[:40]}</span>
          </div>
          <span style="font-size:11px;color:var(--t3);">{ago}м назад · {s.get('pages',0)} стр.</span>
        </div>"""

    # ── Quick actions row ─────────────────────────────────────────────────────
    quick_actions = f"""
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px;">
      <a href="/dashboard/tickets" class="btn btn-outline btn-sm">🎫 Тикеты
        {f'<span style="background:var(--red);color:#fff;border-radius:20px;padding:0 6px;font-size:10px;">{ticket_stats["open"]}</span>' if ticket_stats["open"] else ""}
      </a>
      <a href="/dashboard/reports" class="btn btn-outline btn-sm">🚨 Репорты</a>
      <a href="/dashboard/alerts" class="btn btn-outline btn-sm">🔴 Алерты
        {f'<span style="background:var(--red);color:#fff;border-radius:20px;padding:0 6px;font-size:10px;">{alerts_count}</span>' if alerts_count else ""}
      </a>
      <a href="/dashboard/moderation" class="btn btn-outline btn-sm">🛡 Модерация</a>
      <a href="/dashboard/broadcast" class="btn btn-outline btn-sm">📢 Рассылка</a>
      <a href="/dashboard/command_center" class="btn btn-primary btn-sm">🎮 Command Center</a>
    </div>"""

    # ── Status bar ────────────────────────────────────────────────────────────
    status_bar = f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:20px;">
      </div>
      """

    alerts_section = card("🚨 Алерты и мониторинг", f"""
      {tog("alerts_enabled","Алерты включены","Детектор спама и флуда","⚡")}
      {tog("alert_on_mass_ban","Алерт при массовом бане","5+ банов за 1 минуту = тревога","🔴")}
      {tog("alert_on_raid","Алерт при рейде","Массовый вход новых аккаунтов","⚠️")}
      {tog("alert_on_new_member","Алерт на нового участника","Каждый вход = запись в алертах","👤")}
      {num_input("spam_threshold","Порог спама",s.get("spam_threshold",10),5,60,"сообщений/мин")}
      {num_input("flood_threshold","Порог флуда",s.get("flood_threshold",15),5,100,"сообщений/мин")}
    """)

    logs_section = card("📋 Логирование событий", f"""
      {tog("log_bans","Логировать баны","","")}
      {tog("log_mutes","Логировать муты","","")}
      {tog("log_warns","Логировать варны","","")}
      {tog("log_kicks","Логировать кики","","")}
      {tog("log_joins","Логировать входы","Может быть много записей","👋")}
      {tog("log_leaves","Логировать выходы","","")}
      {tog("log_edited_messages","Логировать редактирование","Сохранять исходный текст","✏️")}
      {tog("log_deleted_messages","Логировать удалённые","Сохранять удалённые сообщения","🗑")}
      {tog("media_log_enabled","Медиа-лог","Логировать фото/видео/файлы","🖼")}
    """)

    display_section = card("🎨 Интерфейс дашборда", f"""
      {tog("show_user_ids","Показывать ID пользователей","","")}
      {tog("auto_refresh","Авто-обновление каждые 30с","","")}
      {tog("compact_view","Компактный вид","Меньше отступов, больше данных","📦")}
      {tog("show_avatars","Показывать аватары","","")}
      {num_input("items_per_page","Записей на странице",s.get("items_per_page",20),5,100,"")}
    """)

    body = navbar(sess, "settings") + f"""
    <div class="container">
      <div class="page-title">🔧 Настройки</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
        <div>
{alerts_section}
          <div style="padding:8px 16px 4px;font-size:13px;color:var(--text2);">
              Всего заблокировано каналов: <b style="color:var(--text)">{total_blocked}</b>
            </div>
            <table>
              <thead><tr><th>Чат ID</th><th>Последний блок</th><th>Всего</th></tr></thead>
              <tbody>{blocks_html}</tbody>
            </table>
          </div>
          <div class="section" style="margin-bottom:16px;">
            <div class="section-header">📥 Экспорт данных</div>
            <div style="padding:16px;display:flex;flex-direction:column;gap:8px;">
              <a href="/dashboard/export/stats" class="btn btn-outline">📊 Статистика (.csv)</a>
              <a href="/dashboard/export/bans" class="btn btn-outline">🔨 Список банов (.csv)</a>
              <a href="/dashboard/export/modhistory" class="btn btn-outline">📋 История модерации (.csv)</a>
            </div>
          </div>
        </div>
        <div>
          {logs_section}
          {display_section}
          <div class="section">
            <div class="section-header">📜 Лог действий администраторов</div>
            <table>
              <thead><tr><th>Время</th><th>Действие</th></tr></thead>
              <tbody>{log_html}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    {'<script>setTimeout(function(){location.reload()},30000)</script>' if s.get("auto_refresh") else ""}
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

async def handle_settings(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    s = shared.dashboard_settings

    if request.method == "POST":
        data = await request.post()
        bool_keys = [
            "alerts_enabled","media_log_enabled","show_user_ids","auto_refresh",
            "alert_on_mass_ban","alert_on_raid","alert_on_new_member",
            "log_mutes","log_bans","log_warns","log_kicks",
            "log_joins","log_leaves","log_edited_messages","log_deleted_messages",
            "dark_theme","compact_view","show_avatars",
        ]
        for k in bool_keys:
            s[k] = data.get(k) == "1"
        s["spam_threshold"]  = int(data.get("spam_threshold", 10))
        s["flood_threshold"] = int(data.get("flood_threshold", 15))
        s["items_per_page"]  = int(data.get("items_per_page", 20))
        _log_admin_db(sess.get("uid",0) if sess else 0, "SETTINGS", "Настройки дашборда обновлены")
        raise web.HTTPFound("/dashboard/settings")

    def tog(key, label, desc="", icon=""):
        val = s.get(key, True)
        desc_html = f'<small style="color:var(--t2);display:block;margin-top:2px;">{desc}</small>' if desc else ""
        checked = "checked" if val else ""
        return (f'<label style="display:flex;align-items:flex-start;justify-content:space-between;'                f'padding:12px 0;border-bottom:1px solid var(--br0);cursor:pointer;gap:12px;">'                f'<div><span style="font-weight:500;">{icon} {label}</span>{desc_html}</div>'                f'<input type="checkbox" name="{key}" value="1" {checked} '                f'style="width:18px;height:18px;cursor:pointer;accent-color:var(--acc);flex-shrink:0;"></label>')

    def num_input(key, label, val, mn, mx, desc=""):
        desc_html = f'<small style="color:var(--t2)">{desc}</small>' if desc else ""
        return (f'<div style="padding:12px 0;border-bottom:1px solid var(--br0);">'                f'<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;">'                f'<div><span style="font-weight:500;">{label}</span>{desc_html}</div>'                f'<input type="number" name="{key}" value="{val}" min="{mn}" max="{mx}" '                f'style="width:80px;padding:6px 10px;border-radius:8px;border:1px solid var(--br1);'                f'background:var(--bg3);color:var(--t1);font-size:14px;text-align:center;"></div></div>')

    log_html = "".join(
        f"<tr><td style='font-size:11px;color:var(--t2);white-space:nowrap'>{r['time']}</td>"
        f"<td style='font-size:13px'>{r['action']}</td></tr>"
        for r in shared.admin_action_log[:30]
    ) or "<tr><td colspan='2' class='empty-state'>Нет действий</td></tr>"

    def card(title, content):
        return (f'<div class="section" style="margin-bottom:16px;">'                f'<div class="section-header">{title}</div>'                f'<div style="padding:0 20px 8px;">'                f'<form method="POST">{content}'                f'<button class="btn btn-primary" type="submit" style="width:100%;margin-top:16px;">💾 Сохранить</button>'                f'</form></div></div>')

    alerts_section = card("🚨 Алерты и мониторинг",
        tog("alerts_enabled","Алерты включены","Детектор спама и флуда","⚡") +
        tog("alert_on_mass_ban","Алерт при массовом бане","5+ банов за минуту","🔴") +
        tog("alert_on_raid","Алерт при рейде","Массовый вход новых аккаунтов","⚠️") +
        tog("alert_on_new_member","Алерт на нового участника","","👤") +
        num_input("spam_threshold","Порог спама",s.get("spam_threshold",10),5,60,"сообщ/мин") +
        num_input("flood_threshold","Порог флуда",s.get("flood_threshold",15),5,100,"сообщ/мин")
    )

    logs_section = card("📋 Логирование событий",
        tog("log_bans","Логировать баны") +
        tog("log_mutes","Логировать муты") +
        tog("log_warns","Логировать варны") +
        tog("log_kicks","Логировать кики") +
        tog("log_joins","Логировать входы","","👋") +
        tog("log_leaves","Логировать выходы") +
        tog("log_edited_messages","Логировать редактирование","","✏️") +
        tog("log_deleted_messages","Логировать удалённые","","🗑") +
        tog("media_log_enabled","Медиа-лог","Фото/видео/файлы","🖼")
    )

    display_section = card("🎨 Интерфейс дашборда",
        tog("show_user_ids","Показывать ID пользователей") +
        tog("auto_refresh","Авто-обновление каждые 30с") +
        tog("compact_view","Компактный вид","","📦") +
        tog("show_avatars","Показывать аватары") +
        num_input("items_per_page","Записей на странице",s.get("items_per_page",20),5,100,"")
    )

    body = navbar(sess, "settings") + f"""
    <div class="container">
      <div class="page-title">🔧 Настройки</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
        <div>
          {alerts_section}
          {logs_section}
          <div class="section" style="margin-bottom:16px;">
            <div class="section-header">📥 Экспорт данных</div>
            <div style="padding:16px;display:flex;flex-direction:column;gap:8px;">
              <a href="/dashboard/export/stats" class="btn btn-outline">📊 Статистика (.csv)</a>
              <a href="/dashboard/export/bans" class="btn btn-outline">🔨 Список банов (.csv)</a>
              <a href="/dashboard/export/modhistory" class="btn btn-outline">📋 История модерации (.csv)</a>
            </div>
          </div>
        </div>
        <div>
          {display_section}
          <div class="section">
            <div class="section-header">📜 Лог действий администраторов</div>
            <table>
              <thead><tr><th>Время</th><th>Действие</th></tr></thead>
              <tbody>{log_html}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    {'<script>setTimeout(function(){location.reload()},30000)</script>' if s.get("auto_refresh") else ""}
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")


handle_settings = require_auth("view_settings")(handle_settings)



# ══════════════════════════════════════════
#  API ENDPOINTS
# ══════════════════════════════════════════

async def api_live(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"error": "Unauthorized"}, status=401)
    import time as _t
    now = _t.time()
    online = [{"uid": uid, "name": d["name"]} for uid, d in shared.online_users.items() if now - d["ts"] < 300]
    conn = db.get_conn()
    total_msgs = conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats").fetchone()[0] or 0
    total_bans = conn.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    conn.close()
    t_stats = await db.ticket_stats_all()
    return web.json_response({
        "online": len(online), "online_users": online[:10],
        "messages": total_msgs, "bans": total_bans,
        "tickets": t_stats, "alerts": len(shared.alerts),
        "tickets_open": t_stats["open"],
    })


async def api_hourly(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({}, status=401)
    # Суммируем по всем чатам
    try:
        conn = db.get_conn()
        rows = [dict(_r) for _r in [dict(_r) for _r in conn.execute("SELECT hour, SUM(count) as total FROM hourly_stats GROUP BY hour").fetchall()]]
        conn.close()
        result = {r["hour"]: r["total"] for r in rows}
    except:
        result = {}
    return web.json_response(result)


async def api_search(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"users":[],"tickets":[],"chats":[]})

    q = request.rel_url.query.get("q", "").strip().lower()
    if not q:
        return web.json_response({"users":[],"tickets":[],"chats":[]})

    conn = db.get_conn()
    users = []
    tickets = []
    chats_res = []

    try:
        if q.isdigit():
            rows = [dict(_r) for _r in conn.execute("SELECT DISTINCT uid FROM chat_stats WHERE uid=? LIMIT 5", (int(q),)).fetchall()]
        else:
            rows = []
        for r in rows:
            uid = r["uid"]
            name = shared.online_users.get(uid, {}).get("name") or f"User {uid}"
            users.append({"uid": uid, "name": name})
    except:
        pass

    try:
        trows = [dict(_r) for _r in conn.execute("SELECT id, subject FROM tickets WHERE subject LIKE ? OR CAST(id AS TEXT)=? LIMIT 5", (f"%{q}%", q)).fetchall()]
        tickets = [{"id": r["id"], "subject": r["subject"]} for r in trows]
    except:
        pass

    try:
        crows = [dict(_r) for _r in conn.execute("SELECT cid, title FROM known_chats WHERE LOWER(title) LIKE ? OR CAST(cid AS TEXT)=? LIMIT 5", (f"%{q}%", q)).fetchall()]
        chats_res = [{"cid": r["cid"], "title": r["title"]} for r in crows]
    except:
        pass

    conn.close()
    return web.json_response({"users": users, "tickets": tickets, "chats": chats_res})


async def api_modaction(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)

    data = await request.json()
    action = data.get("action")
    uid = int(data.get("uid", 0))
    cid = int(data.get("cid", 0))
    reason = data.get("reason", "Нарушение правил")
    sess = _dashboard_sessions[token]

    if not _bot or not uid:
        return web.json_response({"ok": False, "error": "Бот не запущен или нет UID"})

    perm_map = {"ban": "ban_users", "unban": "ban_users", "mute": "mute_users", "warn": "warn_users"}
    required = perm_map.get(action)
    if required and not _has_perm(token, required):
        return web.json_response({"ok": False, "error": "Нет прав"})

    try:
        if action == "ban":
            await _bot.ban_chat_member(cid, uid)
            msg = f"Пользователь {uid} забанен в чате {cid}"
        elif action == "unban":
            await _bot.unban_chat_member(cid, uid, only_if_banned=True)
            msg = f"Пользователь {uid} разбанен в чате {cid}"
        elif action == "mute":
            from aiogram.types import ChatPermissions
            await _bot.restrict_chat_member(cid, uid, ChatPermissions(can_send_messages=False), until_date=timedelta(hours=1))
            msg = f"Пользователь {uid} замучен на 1ч в чате {cid}"
        elif action == "warn":
            msg = f"Предупреждение выдано {uid}"
        else:
            return web.json_response({"ok": False, "error": "Неизвестное действие"})

        _log_admin_db(sess.get("uid", 0), f"ACTION_{action.upper()}", f"{msg}. Причина: {reason}", request.remote or "")
        return web.json_response({"ok": True, "message": msg})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})


async def api_stats(request: web.Request):
    token = request.headers.get("X-Token") or request.rel_url.query.get("token")
    if token != DASHBOARD_TOKEN:
        return web.json_response({"error": "Unauthorized"}, status=401)
    conn = db.get_conn()
    total_users = conn.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats").fetchone()[0] or 0
    total_msgs = conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats").fetchone()[0] or 0
    total_bans = conn.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    conn.close()
    t_stats = await db.ticket_stats_all()
    return web.json_response({"chats": len(await db.get_all_chats()), "users": total_users, "messages": total_msgs, "bans": total_bans, "tickets": t_stats})


# ══════════════════════════════════════════
#  ЭКСПОРТ CSV
# ══════════════════════════════════════════

async def handle_export(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not _has_perm(token, "view_settings"):
        raise web.HTTPFound("/dashboard")
    export_type = request.match_info.get("type", "stats")
    conn = db.get_conn()
    if export_type == "stats":
        rows = [dict(_r) for _r in conn.execute("SELECT k.title, cs.uid, cs.msg_count, COALESCE(r.score,0) as rep, COALESCE(w.count,0) as warns FROM chat_stats cs LEFT JOIN known_chats k ON cs.cid=k.cid LEFT JOIN reputation r ON cs.cid=r.cid AND cs.uid=r.uid LEFT JOIN warnings w ON cs.cid=w.cid AND cs.uid=w.uid ORDER BY cs.msg_count DESC LIMIT 1000").fetchall()]
        conn.close()
        csv = "Чат,UserID,Сообщений,Репутация,Варны\n" + "".join(f"{r['title'] or ''},{r['uid']},{r['msg_count']},{r['rep']},{r['warns']}\n" for r in rows)
    elif export_type == "bans":
        rows = [dict(_r) for _r in conn.execute("SELECT k.title, b.uid FROM ban_list b LEFT JOIN known_chats k ON b.cid=k.cid").fetchall()]
        conn.close()
        csv = "Чат,UserID\n" + "".join(f"{r['title'] or ''},{r['uid']}\n" for r in rows)
    elif export_type == "modhistory":
        rows = [dict(_r) for _r in conn.execute("SELECT k.title, m.uid, m.action, m.reason, m.by_name, m.created_at FROM mod_history m LEFT JOIN known_chats k ON m.cid=k.cid ORDER BY m.created_at DESC LIMIT 2000").fetchall()]
        conn.close()
        csv = "Чат,UserID,Действие,Причина,Кто,Дата\n" + "".join(f"{r['title'] or ''},{r['uid']},{r['action']},{r['reason'] or ''},{r['by_name']},{str(r['created_at'])[:16]}\n" for r in rows)
    else:
        conn.close()
        raise web.HTTPNotFound()
    return web.Response(text=csv, content_type="text/csv", headers={"Content-Disposition": f"attachment; filename={export_type}_{datetime.now().strftime('%d%m%Y')}.csv"})


# ══════════════════════════════════════════
#  SSE
# ══════════════════════════════════════════

async def handle_sse(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        raise web.HTTPUnauthorized()
    response = web.StreamResponse()
    response.headers.update({"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    await response.prepare(request)
    queue = asyncio.Queue()
    shared.sse_clients.append(queue)
    try:
        await response.write(b"data: connected\n\n")
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=30)
                await response.write(f"data: {msg}\n\n".encode())
            except asyncio.TimeoutError:
                await response.write(b": ping\n\n")
    except:
        pass
    finally:
        try:
            shared.sse_clients.remove(queue)
        except:
            pass
    return response


async def handle_health(request: web.Request):
    return web.Response(text="OK")


# ══════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════



# ══════════════════════════════════════════
#  ПРОФИЛЬ МОДЕРАТОРА
# ══════════════════════════════════════════


def _mod_activity_json(uid: int) -> str:
    """Возвращает JSON с активностью мода за 30 дней для Chart.js."""
    import json
    from datetime import datetime, timedelta
    labels = []
    values = []
    try:
        conn = db.get_conn()
        for i in range(29, -1, -1):
            day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            day_label = (datetime.now() - timedelta(days=i)).strftime("%d.%m")
            cnt = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE by_name IN "
                "(SELECT name FROM dashboard_admins WHERE tg_uid=?) "
                "AND date(created_at)=?", (uid, day)
            ).fetchone()[0] or 0
            labels.append(day_label)
            values.append(cnt)
        conn.close()
    except:
        labels = [str(i) for i in range(30)]
        values = [0] * 30
    return json.dumps({"labels": labels, "values": values})


def _mod_rank_progress_html(uid: int, rank: int, stats: dict) -> str:
    """Прогресс-бар до следующего ранга с порогами."""
    # Пороги для повышения ранга (накопленные действия)
    RANK_THRESHOLDS = {
        1: 10, 2: 25, 3: 50, 4: 100, 5: 200,
        6: 350, 7: 500, 8: 750, 9: 1000, 10: 1500,
        11: 2000, 12: 3000, 13: 5000, 14: 8000,
    }
    if rank >= OWNER_RANK:
        return '<div style="color:var(--gold);font-weight:700;">⭐ Максимальный ранг достигнут</div>'

    next_rank   = rank + 1
    threshold   = RANK_THRESHOLDS.get(rank, 9999)
    total_acts  = stats.get("bans", 0) + stats.get("warns", 0) + stats.get("mutes", 0) + stats.get("tickets", 0)
    pct         = min(100, int(total_acts / threshold * 100)) if threshold else 100
    next_name   = DASHBOARD_RANKS.get(next_rank, {}).get("name", "—")
    next_color  = DASHBOARD_RANKS.get(next_rank, {}).get("color", "#00e5a0")
    rgb         = _hex_to_rgb(next_color)
    remaining   = max(0, threshold - total_acts)

    metrics = [
        ("🔨", "Банов",  stats.get("bans",0),    "#ef4444"),
        ("⚡", "Варнов", stats.get("warns",0),   "#f59e0b"),
        ("🔇", "Мутов",  stats.get("mutes",0),   "#8b5cf6"),
        ("🎫", "Тикетов",stats.get("tickets",0), "var(--acc)"),
    ]
    metrics_html = "".join(
        f'<div style="text-align:center;">'
        f'<div style="font-size:18px;font-weight:700;color:{c};">{v}</div>'
        f'<div style="font-size:11px;color:var(--text2);">{icon} {label}</div>'
        f'</div>'
        for icon, label, v, c in metrics
    )

    return (
        f'<div style="margin-bottom:12px;display:flex;justify-content:space-between;font-size:13px;">'
        f'<span>Текущий: <b>{DASHBOARD_RANKS.get(rank,{{}}).get("name","—")}</b></span>'
        f'<span>Цель: <b style="color:{next_color};">{next_name}</b></span>'
        f'</div>'
        f'<div style="background:var(--bg3);border-radius:8px;height:14px;overflow:hidden;margin-bottom:8px;">'
        f'<div style="height:100%;width:{pct}%;background:linear-gradient(90deg,rgba({rgb},.6),{next_color});'
        f'border-radius:8px;transition:width .5s;"></div>'
        f'</div>'
        f'<div style="display:flex;justify-content:space-between;font-size:12px;color:var(--text2);margin-bottom:16px;">'
        f'<span>{total_acts} / {threshold} действий</span>'
        f'<span>{pct}% · осталось {remaining}</span>'
        f'</div>'
        f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;">'
        f'{metrics_html}'
        f'</div>'
    )

async def handle_mod_profile(request: web.Request):
    """Страница профиля конкретного модератора."""
    sess = _get_session(request)
    _track_session(request)
    if not sess:
        raise web.HTTPFound("/dashboard/login")

    target_uid = int(request.match_info.get("uid", 0))
    admin = _get_admin(target_uid)
    if not admin:
        raise web.HTTPFound("/dashboard/admins")

    rank     = admin["rank"]
    ri       = DASHBOARD_RANKS.get(rank, DASHBOARD_RANKS[1])
    stats    = _get_mod_stats(admin["name"], target_uid)
    duty_st  = get_duty_status(target_uid)
    earned   = _mod_achievements.get(target_uid, {})

    # История дежурств
    my_hist  = [d for d in _duty_history if d["uid"] == target_uid][:10]

    try:
        conn = db.get_conn()
        recent_acts = [dict(r) for r in conn.execute(
            "SELECT action,reason,created_at FROM mod_history WHERE by_name=? ORDER BY created_at DESC LIMIT 20",
            (admin["name"],)
        ).fetchall()]
        conn.close()
    except:
        recent_acts = []

    acts_html = "".join(
        f"<tr>"
        f"<td style='font-size:11px;color:var(--text2);'>{str(r['created_at'])[:16]}</td>"
        f"<td style='font-weight:600;color:{'var(--danger)' if 'Бан' in str(r['action']) else 'var(--warn)'};'>{r['action']}</td>"
        f"<td style='color:var(--text2);font-size:12px;'>{(str(r.get('reason') or '—'))[:40]}</td>"
        f"</tr>"
        for r in recent_acts
    ) or "<tr><td colspan='3' class='empty-state'>Нет действий</td></tr>"

    hist_html = "".join(
        f"<tr>"
        f"<td style='font-size:11px;color:var(--text2);'>{datetime.fromtimestamp(d['start']).strftime('%d.%m %H:%M')}</td>"
        f"<td>{d['duration_mins']} мин</td>"
        f"<td style='color:var(--accent);'>{d['actions_count']}</td>"
        f"</tr>"
        for d in my_hist
    ) or "<tr><td colspan='3' class='empty-state'>Нет</td></tr>"

    badges_html = "".join(
        f'<span title="{ACHIEVEMENTS_DEF[k][1]}: {ACHIEVEMENTS_DEF[k][2]}" style="font-size:26px;cursor:default;">{ACHIEVEMENTS_DEF[k][0]}</span>'
        for k in earned if k in ACHIEVEMENTS_DEF
    ) or '<span style="color:var(--text2);font-size:13px;">Нет достижений</span>'

    rgb = _hex_to_rgb(ri["color"])

    duty_widget = ""
    if duty_st:
        rh = duty_st["remaining_mins"] // 60; rm = duty_st["remaining_mins"] % 60
        duty_widget = (
            f'<div style="margin-top:12px;padding:10px 14px;background:rgba(34,197,94,.08);'
            f'border-radius:8px;border:1px solid rgba(34,197,94,.2);">'
            f'<div style="font-size:13px;color:var(--success);font-weight:700;">🟢 На дежурстве</div>'
            f'<div style="font-size:12px;color:var(--text2);margin-top:4px;">'
            f'Осталось: {rh}ч {rm}м | Действий: {duty_st.get("actions",0)}</div>'
            f'<div style="height:4px;background:var(--bg4);border-radius:2px;margin-top:8px;">'
            f'<div style="height:4px;width:{duty_st["pct"]}%;background:var(--success);border-radius:2px;"></div></div>'
            f'</div>'
        )

    body = navbar(sess, "admins") + f"""
    <div class="container">
      <div class="page-title">
        👤 {admin['name']}
        <a href="/dashboard/admins" class="btn btn-ghost btn-sm" style="margin-left:auto;">← Назад</a>
      </div>

      <div class="grid-2" style="margin-bottom:24px;">
        <div class="section">
          <div class="section-body">
            <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;">
              <div style="width:56px;height:56px;border-radius:14px;
                          background:rgba({rgb},.2);border:2px solid {ri['color']};
                          display:flex;align-items:center;justify-content:center;font-size:24px;">
                {ri['name'][0]}
              </div>
              <div>
                <div style="font-size:18px;font-weight:800;">{admin['name']}</div>
                <span class="badge" style="background:rgba({rgb},.15);color:{ri['color']};margin-top:6px;display:inline-block;">
                  {rank} · {ri['name']}
                </span>
              </div>
            </div>
            <div style="font-size:13px;line-height:2.2;border-top:1px solid var(--border);padding-top:12px;">
              <div><span style="color:var(--text2);">TG ID:</span> <code>{target_uid}</code></div>
              <div><span style="color:var(--text2);">Уровень:</span> <b style="color:{ri['color']};text-transform:uppercase;">{ri['tier']}</b></div>
              <div><span style="color:var(--text2);">Права:</span> {len(ri['perms'])} {"(все)" if "ALL" in ri['perms'] else ""}</div>
              <div><span style="color:var(--text2);">Последний вход:</span> {str(admin.get('last_login') or '—')[:16]}</div>
              <div><span style="color:var(--text2);">Добавлен:</span> {str(admin.get('granted_at') or '—')[:16]}</div>
              <div><span style="color:var(--text2);">Входов в систему:</span> {stats['logins']}</div>
            </div>
            {duty_widget}
          </div>
        </div>

        <div>
          <div class="cards" style="grid-template-columns:repeat(2,1fr);margin-bottom:16px;">
            <div class="card"><div class="card-icon">🔨</div><div class="card-label">Банов</div>
              <div class="card-value" style="color:var(--danger);">{stats['bans']}</div></div>
            <div class="card"><div class="card-icon">⚡</div><div class="card-label">Варнов</div>
              <div class="card-value" style="color:var(--warn);">{stats['warns']}</div></div>
            <div class="card"><div class="card-icon">🔇</div><div class="card-label">Мутов</div>
              <div class="card-value" style="color:var(--purple);">{stats['mutes']}</div></div>
            <div class="card"><div class="card-icon">🎫</div><div class="card-label">Тикетов</div>
              <div class="card-value" style="color:var(--accent);">{stats['tickets']}</div></div>
          </div>
          <div class="section">
            <div class="section-header">🏅 Достижения ({len(earned)})</div>
            <div style="padding:16px;display:flex;gap:8px;flex-wrap:wrap;">{badges_html}</div>
          </div>
        </div>
      </div>

      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">📈 Активность за 30 дней</div>
        <div style="padding:16px;height:200px;position:relative;">
          <canvas id="modActivityChart"></canvas>
        </div>
      </div>

      <div class="grid-2">
        <div class="section">
          <div class="section-header">📋 Последние 20 действий</div>
          <table>
            <thead><tr><th>Время</th><th>Действие</th><th>Причина</th></tr></thead>
            <tbody>{acts_html}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">⏰ История дежурств</div>
          <table>
            <thead><tr><th>Начало</th><th>Длит.</th><th>Действий</th></tr></thead>
            <tbody>{hist_html}</tbody>
          </table>
        </div>
      </div>

      <div class="section">
        <div class="section-header">🎯 Прогресс до ранга {DASHBOARD_RANKS.get(rank+1, {{}}).get('name', '🌟 Максимум')}</div>
        <div style="padding:16px;">
          {_mod_rank_progress_html(target_uid, rank, stats)}
        </div>
      </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
    <script>
    (function() {{
      var ctx = document.getElementById("modActivityChart");
      if (!ctx) return;
      var data = {_mod_activity_json(target_uid)};
      new Chart(ctx, {{
        type: "line",
        data: {{
          labels: data.labels,
          datasets: [{{
            label: "Действий",
            data: data.values,
            borderColor: "#00e5a0",
            backgroundColor: "rgba(99,102,241,0.1)",
            borderWidth: 2,
            pointRadius: 3,
            fill: true,
            tension: 0.4
          }}]
        }},
        options: {{
          responsive: true, maintainAspectRatio: false,
          plugins: {{ legend: {{ display: false }} }},
          scales: {{
            x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }},
            y: {{ beginAtZero: true, ticks: {{ stepSize: 1, font: {{ size: 10 }} }} }}
          }}
        }}
      }});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_mod_profile = require_auth("view_overview")(handle_mod_profile)


# ══════════════════════════════════════════
#  📚 ВНУТРЕННЯЯ WIKI
# ══════════════════════════════════════════
#
#  Хранилище: SQLite таблица wiki_pages
#  История:   SQLite таблица wiki_history
#  Поиск:     LIKE по title + body
#  Markdown:  рендер через marked.js (CDN)
#
# ══════════════════════════════════════════

def _wiki_init_db():
    """Создаём таблицы wiki если нет."""
    try:
        conn = db.get_conn()
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS wiki_pages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            slug       TEXT    UNIQUE NOT NULL,
            title      TEXT    NOT NULL,
            body       TEXT    DEFAULT '',
            category   TEXT    DEFAULT 'Общее',
            author_id  INTEGER DEFAULT 0,
            author     TEXT    DEFAULT '',
            created_at TEXT    DEFAULT (datetime('now')),
            updated_at TEXT    DEFAULT (datetime('now')),
            views      INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS wiki_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id    INTEGER NOT NULL,
            title      TEXT,
            body       TEXT,
            author_id  INTEGER DEFAULT 0,
            author     TEXT    DEFAULT '',
            changed_at TEXT    DEFAULT (datetime('now')),
            diff_size  INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_wiki_slug    ON wiki_pages(slug);
        CREATE INDEX IF NOT EXISTS idx_wiki_history ON wiki_history(page_id);
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"_wiki_init_db: {e}")


def _wiki_slug(title: str) -> str:
    """Генерирует slug из заголовка."""
    import re
    s = title.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    s = s.strip("-")
    return s[:80] or "page"


def _wiki_get_page(slug: str) -> dict | None:
    try:
        conn = db.get_conn()
        row = conn.execute("SELECT * FROM wiki_pages WHERE slug=?", (slug,)).fetchone()
        conn.close()
        return dict(row) if row else None
    except:
        return None


def _wiki_get_all(search: str = "", category: str = "") -> list:
    try:
        conn = db.get_conn()
        q = "SELECT id,slug,title,category,author,updated_at,views FROM wiki_pages WHERE 1=1"
        params = []
        if search:
            q += " AND (title LIKE ? OR body LIKE ?)"
            params += [f"%{search}%", f"%{search}%"]
        if category:
            q += " AND category=?"
            params.append(category)
        q += " ORDER BY updated_at DESC"
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
        conn.close()
        return rows
    except:
        return []


def _wiki_get_categories() -> list:
    try:
        conn = db.get_conn()
        rows = conn.execute(
            "SELECT category, COUNT(*) as cnt FROM wiki_pages GROUP BY category ORDER BY cnt DESC"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except:
        return []


def _wiki_save_page(slug: str, title: str, body: str, category: str,
                    author_id: int, author: str) -> int:
    """Создаёт или обновляет страницу. Возвращает id."""
    try:
        conn = db.get_conn()
        existing = conn.execute(
            "SELECT id, title, body FROM wiki_pages WHERE slug=?", (slug,)
        ).fetchone()

        if existing:
            page_id  = existing["id"]
            old_body = existing["body"] or ""
            diff_size = abs(len(body) - len(old_body))
            # Сохраняем в историю
            conn.execute(
                "INSERT INTO wiki_history (page_id,title,body,author_id,author,diff_size) VALUES (?,?,?,?,?,?)",
                (page_id, existing["title"], old_body, author_id, author, diff_size)
            )
            conn.execute(
                "UPDATE wiki_pages SET title=?,body=?,category=?,author_id=?,author=?,updated_at=datetime('now') WHERE id=?",
                (title, body, category, author_id, author, page_id)
            )
        else:
            conn.execute(
                "INSERT INTO wiki_pages (slug,title,body,category,author_id,author) VALUES (?,?,?,?,?,?)",
                (slug, title, body, category, author_id, author)
            )
            page_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.commit()
        conn.close()
        return page_id
    except Exception as e:
        log.error(f"_wiki_save_page: {e}")
        return 0


def _wiki_delete_page(slug: str):
    try:
        conn = db.get_conn()
        row = conn.execute("SELECT id FROM wiki_pages WHERE slug=?", (slug,)).fetchone()
        if row:
            conn.execute("DELETE FROM wiki_history WHERE page_id=?", (row["id"],))
            conn.execute("DELETE FROM wiki_pages WHERE slug=?", (slug,))
            conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"_wiki_delete_page: {e}")


def _wiki_get_history(page_id: int, limit: int = 20) -> list:
    try:
        conn = db.get_conn()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM wiki_history WHERE page_id=? ORDER BY changed_at DESC LIMIT ?",
            (page_id, limit)
        ).fetchall()]
        conn.close()
        return rows
    except:
        return []


def _wiki_increment_views(slug: str):
    try:
        conn = db.get_conn()
        conn.execute("UPDATE wiki_pages SET views=views+1 WHERE slug=?", (slug,))
        conn.commit()
        conn.close()
    except:
        pass


# ── CSS для wiki-страниц (markdown preview) ──────────────────────
WIKI_MARKDOWN_CSS = """
<style id="wiki-md-css">
.md-body { font-size:14px; line-height:1.75; color:var(--text); }
.md-body h1 { font-size:22px; font-weight:800; margin:0 0 16px; padding-bottom:8px; border-bottom:1px solid var(--border); }
.md-body h2 { font-size:18px; font-weight:700; margin:24px 0 10px; }
.md-body h3 { font-size:15px; font-weight:700; margin:18px 0 8px; }
.md-body p  { margin:0 0 12px; }
.md-body ul, .md-body ol { margin:0 0 12px; padding-left:20px; }
.md-body li { margin-bottom:4px; }
.md-body blockquote { border-left:3px solid var(--accent); margin:12px 0; padding:8px 14px; background:var(--accent-glow); border-radius:0 8px 8px 0; color:var(--text2); }
.md-body code { background:var(--bg3); padding:2px 6px; border-radius:4px; font-family:monospace; font-size:12px; color:var(--accent); }
.md-body pre  { background:var(--bg3); padding:14px; border-radius:10px; overflow-x:auto; margin:0 0 12px; }
.md-body pre code { background:none; padding:0; color:var(--text3); }
.md-body table { width:100%; border-collapse:collapse; margin:0 0 12px; font-size:13px; }
.md-body th { background:var(--bg3); padding:8px 12px; text-align:left; font-weight:700; font-size:11px; text-transform:uppercase; letter-spacing:.5px; }
.md-body td { padding:8px 12px; border-bottom:1px solid var(--border); }
.md-body hr { border:none; border-top:1px solid var(--border); margin:20px 0; }
.md-body a  { color:var(--accent); text-decoration:none; }
.md-body a:hover { text-decoration:underline; }
.md-body img { max-width:100%; border-radius:8px; }
.md-body strong { font-weight:700; }
.md-body em { font-style:italic; }
</style>
"""

WIKI_CATEGORIES = ["Общее", "Правила", "Инструкции", "Прецеденты", "Технические", "Шаблоны"]


# ── Список страниц ───────────────────────────────────────────────

async def handle_wiki_list(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _wiki_init_db()

    search   = request.rel_url.query.get("q", "").strip()
    cat_flt  = request.rel_url.query.get("cat", "")
    pages    = _wiki_get_all(search, cat_flt)
    cats     = _wiki_get_categories()

    # Статистика
    total_pages = len(_wiki_get_all())
    try:
        conn = db.get_conn()
        total_edits = conn.execute("SELECT COUNT(*) FROM wiki_history").fetchone()[0] or 0
        conn.close()
    except:
        total_edits = 0

    # Табы категорий
    cat_tabs = f'<a href="/dashboard/wiki" class="btn btn-sm {"btn-primary" if not cat_flt else "btn-ghost"}" style="margin-right:4px;">Все ({total_pages})</a>'
    for c in cats:
        active = "btn-primary" if cat_flt == c["category"] else "btn-ghost"
        cat_tabs += f'<a href="/dashboard/wiki?cat={c["category"]}" class="btn btn-sm {active}" style="margin-right:4px;">{c["category"]} ({c["cnt"]})</a>'

    # Карточки страниц
    if pages:
        cards_html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;">'
        for p in pages:
            upd = str(p.get("updated_at",""))[:16].replace("T"," ")
            cards_html += (
                f'<div class="card" style="cursor:pointer;" onclick="window.location=\'/dashboard/wiki/{p["slug"]}\'">'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px;">'
                f'<span class="badge badge-accent" style="font-size:10px;">{p["category"]}</span>'
                f'<span style="font-size:11px;color:var(--text2);">👁 {p["views"]}</span>'
                f'</div>'
                f'<div style="font-weight:700;font-size:14px;margin-bottom:4px;">{p["title"]}</div>'
                f'<div style="font-size:12px;color:var(--text2);">✏️ {p["author"] or "—"} · {upd}</div>'
                f'</div>'
            )
        cards_html += "</div>"
    else:
        cards_html = f'<div class="empty-state">{"Ничего не найдено по запросу «" + search + "»" if search else "Wiki пустая — создай первую страницу!"}</div>'

    body = navbar(sess, "wiki") + WIKI_MARKDOWN_CSS + f"""
    <div class="container">
      <div class="page-title">📚 Внутренняя Wiki
        <a href="/dashboard/wiki/new" class="btn btn-primary btn-sm" style="margin-left:auto;">✏️ Новая страница</a>
      </div>

      <div class="cards" style="grid-template-columns:repeat(3,1fr);margin-bottom:24px;">
        <div class="card"><div class="card-icon">📄</div><div class="card-label">Страниц</div>
          <div class="card-value">{total_pages}</div></div>
        <div class="card"><div class="card-icon">✏️</div><div class="card-label">Правок</div>
          <div class="card-value">{total_edits}</div></div>
        <div class="card"><div class="card-icon">🗂</div><div class="card-label">Категорий</div>
          <div class="card-value">{len(cats)}</div></div>
      </div>

      <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px;flex-wrap:wrap;">
        <form method="GET" style="display:flex;gap:8px;flex:1;min-width:200px;">
          {"<input type='hidden' name='cat' value='" + cat_flt + "'>" if cat_flt else ""}
          <input class="form-control" name="q" value="{search}"
            placeholder="🔍 Поиск по заголовку и содержимому..." style="flex:1;">
          <button class="btn btn-primary btn-sm" type="submit">Найти</button>
          {"<a href='/dashboard/wiki' class='btn btn-ghost btn-sm'>✕</a>" if search or cat_flt else ""}
        </form>
      </div>

      <div style="margin-bottom:20px;display:flex;flex-wrap:wrap;gap:4px;">
        {cat_tabs}
      </div>

      {cards_html}
    </div>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_wiki_list = require_auth("view_overview")(handle_wiki_list)


# ── Просмотр страницы ────────────────────────────────────────────

async def handle_wiki_view(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _wiki_init_db()

    slug = request.match_info.get("slug", "")
    if slug == "new":
        raise web.HTTPFound("/dashboard/wiki/new/edit")

    p = _wiki_get_page(slug)
    if not p:
        raise web.HTTPFound("/dashboard/wiki")

    _wiki_increment_views(slug)
    history = _wiki_get_history(p["id"], limit=5)
    can_edit = _has_perm(request.cookies.get("dsess_token",""), "view_overview")

    hist_html = ""
    for h in history:
        dt  = str(h.get("changed_at",""))[:16]
        dsz = h.get("diff_size", 0)
        sign = f'+{dsz}' if dsz >= 0 else str(dsz)
        hist_html += (
            f'<div style="padding:8px 0;border-bottom:1px solid var(--border);font-size:12px;">'
            f'<div style="display:flex;justify-content:space-between;">'
            f'<span style="font-weight:600;">{h.get("author","—")}</span>'
            f'<span style="color:var(--text2);">{dt}</span>'
            f'</div>'
            f'<div style="color:var(--text2);margin-top:2px;">'
            f'<span style="color:{"var(--success)" if dsz>0 else "var(--danger)"};">{sign} симв</span>'
            f'</div>'
            f'</div>'
        )
    if not hist_html:
        hist_html = '<div style="color:var(--text2);font-size:12px;padding:8px 0;">Нет правок</div>'

    upd = str(p.get("updated_at",""))[:16].replace("T"," ")
    crt = str(p.get("created_at",""))[:16].replace("T"," ")

    edit_btn = f'<a href="/dashboard/wiki/{slug}/edit" class="btn btn-primary btn-sm">✏️ Редактировать</a>' if can_edit else ""
    delete_form = ""
    if can_edit:
        delete_form = (
            f'<form method="POST" action="/dashboard/wiki/{slug}/delete" style="display:inline;">'
            f'<button class="btn btn-sm" style="background:rgba(239,68,68,.1);color:var(--danger);" '
            f'onclick="return confirm(\'Удалить страницу?\')">🗑 Удалить</button>'
            f'</form>'
        )

    body = navbar(sess, "wiki") + WIKI_MARKDOWN_CSS + f"""
    <div class="container">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:24px;flex-wrap:wrap;">
        <a href="/dashboard/wiki" class="btn btn-ghost btn-sm">← Wiki</a>
        <span class="badge badge-accent">{p["category"]}</span>
        <span style="font-size:12px;color:var(--text2);margin-left:auto;">👁 {p["views"]} просмотров</span>
        {edit_btn}
        {delete_form}
      </div>

      <div class="grid-2" style="gap:24px;align-items:start;">
        <div style="grid-column:1/2;">
          <div class="section">
            <div class="section-body">
              <div class="md-body" id="md-render">
                <p style="color:var(--text2);">Загрузка...</p>
              </div>
            </div>
          </div>
        </div>

        <div style="grid-column:2/3;">
          <div class="section" style="margin-bottom:16px;">
            <div class="section-header">ℹ️ Информация</div>
            <div class="section-body" style="font-size:13px;line-height:2.2;">
              <div><span style="color:var(--text2);">Создана:</span> {crt}</div>
              <div><span style="color:var(--text2);">Обновлена:</span> {upd}</div>
              <div><span style="color:var(--text2);">Автор:</span> {p.get("author","—")}</div>
              <div><span style="color:var(--text2);">Slug:</span> <code>{slug}</code></div>
            </div>
          </div>

          <div class="section">
            <div class="section-header">
              📜 История правок
              <a href="/dashboard/wiki/{slug}/history" style="font-size:12px;color:var(--accent);font-weight:400;">Все →</a>
            </div>
            <div style="padding:0 16px;">{hist_html}</div>
          </div>
        </div>
      </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/marked/9.1.6/marked.min.js"></script>
    <script>
    var RAW = {json.dumps(p.get("body",""))};
    document.addEventListener("DOMContentLoaded", function() {{
      var el = document.getElementById("md-render");
      if (el && typeof marked !== "undefined") {{
        el.innerHTML = marked.parse(RAW);
      }} else if (el) {{
        el.innerHTML = "<pre>" + RAW.replace(/</g,"&lt;") + "</pre>";
      }}
    }});
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_wiki_view = require_auth("view_overview")(handle_wiki_view)


# ── Редактор страницы ────────────────────────────────────────────

async def handle_wiki_edit(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _wiki_init_db()

    slug      = request.match_info.get("slug", "new")
    is_new    = (slug == "new")
    p         = None if is_new else _wiki_get_page(slug)
    result    = ""

    if request.method == "POST":
        data     = await request.post()
        title    = (data.get("title","") or "").strip()
        body_md  = data.get("body","") or ""
        category = data.get("category","Общее")
        new_slug = _wiki_slug(title) if is_new else slug

        if not title:
            result = '<div style="color:var(--danger);margin-bottom:12px;">⚠️ Заголовок обязателен</div>'
        else:
            author_id = sess.get("uid", 0) if sess else 0
            author    = sess.get("name", "Аноним") if sess else "Аноним"
            _wiki_save_page(new_slug, title, body_md, category, author_id, author)
            _log_admin_db(author_id, "WIKI_EDIT", f"{'Создана' if is_new else 'Изменена'}: {title}")
            raise web.HTTPFound(f"/dashboard/wiki/{new_slug}")

    # Данные для формы
    f_title    = p["title"]    if p else ""
    f_body     = p["body"]     if p else ""
    f_category = p["category"] if p else "Общее"

    cat_opts = "".join(
        f'<option value="{c}" {"selected" if c == f_category else ""}>{c}</option>'
        for c in WIKI_CATEGORIES
    )

    page_title = "Новая страница" if is_new else f'Редактировать: {p["title"] if p else slug}'
    back_url   = "/dashboard/wiki" if is_new else f"/dashboard/wiki/{slug}"

    body = navbar(sess, "wiki") + WIKI_MARKDOWN_CSS + f"""
    <div class="container">
      <div class="page-title">
        {"✏️ " + page_title}
        <a href="{back_url}" class="btn btn-ghost btn-sm" style="margin-left:auto;">← Назад</a>
      </div>
      {result}

      <form method="POST" id="wikiForm">
        <div class="grid-2" style="gap:20px;align-items:start;">

          <div>
            <div class="form-group">
              <label>Заголовок *</label>
              <input class="form-control" type="text" name="title"
                value="{f_title}" placeholder="Название страницы..." required
                oninput="updatePreviewTitle(this.value)" style="font-size:15px;font-weight:600;">
            </div>
            <div class="form-group">
              <label>Категория</label>
              <select class="form-control" name="category">{cat_opts}</select>
            </div>
            <div class="form-group">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
                <label style="margin:0;">Содержимое (Markdown)</label>
                <div style="display:flex;gap:6px;">
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('**', '**')"><b>B</b></button>
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('*', '*')"><i>I</i></button>
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('`', '`')">code</button>
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('\\n```\\n', '\\n```\\n')">```</button>
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('## ', '')">H2</button>
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('- ', '')">list</button>
                  <button type="button" class="btn btn-ghost btn-xs" onclick="insertMd('> ', '')">quote</button>
                </div>
              </div>
              <textarea class="form-control" name="body" id="wikiBody" rows="22"
                placeholder="# Заголовок&#10;&#10;Текст страницы..."
                oninput="updatePreview(this.value)"
                style="font-family:monospace;font-size:13px;resize:vertical;">{f_body}</textarea>
              <div style="font-size:11px;color:var(--text2);margin-top:4px;">
                Поддерживается Markdown: **жирный**, *курсив*, `код`, ## заголовки, - списки, > цитаты, | таблицы
              </div>
            </div>
            <div style="display:flex;gap:10px;">
              <button class="btn btn-primary" type="submit" style="flex:1;padding:12px;">
                💾 {'Создать' if is_new else 'Сохранить'}
              </button>
              <a href="{back_url}" class="btn btn-ghost" style="padding:12px 20px;">Отмена</a>
            </div>
          </div>

          <div>
            <div class="section" style="position:sticky;top:80px;">
              <div class="section-header">
                👁 Предпросмотр
                <span id="charCount" style="font-size:11px;color:var(--text2);font-weight:400;"></span>
              </div>
              <div class="section-body">
                <h1 id="previewTitle" style="font-size:18px;font-weight:800;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border);">{f_title or "Заголовок"}</h1>
                <div class="md-body" id="mdPreview" style="min-height:200px;"></div>
              </div>
            </div>
          </div>
        </div>
      </form>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/marked/9.1.6/marked.min.js"></script>
    <script>
    var editor = document.getElementById("wikiBody");
    var preview = document.getElementById("mdPreview");

    function updatePreview(val) {{
      if (typeof marked !== "undefined" && preview) {{
        preview.innerHTML = marked.parse(val || "");
      }}
      var cc = document.getElementById("charCount");
      if (cc) cc.textContent = (val||"").length + " симв";
    }}
    function updatePreviewTitle(val) {{
      var el = document.getElementById("previewTitle");
      if (el) el.textContent = val || "Заголовок";
    }}
    function insertMd(before, after) {{
      var start = editor.selectionStart, end = editor.selectionEnd;
      var sel = editor.value.substring(start, end);
      var replacement = before + sel + after;
      editor.value = editor.value.substring(0, start) + replacement + editor.value.substring(end);
      editor.selectionStart = start + before.length;
      editor.selectionEnd   = start + before.length + sel.length;
      editor.focus();
      updatePreview(editor.value);
    }}
    document.addEventListener("DOMContentLoaded", function() {{
      updatePreview(editor ? editor.value : "");
    }});
    // Ctrl+S → сохранить
    document.addEventListener("keydown", function(e) {{
      if ((e.ctrlKey || e.metaKey) && e.key === "s") {{
        e.preventDefault();
        document.getElementById("wikiForm").submit();
      }}
    }});
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_wiki_edit = require_auth("view_overview")(handle_wiki_edit)


# ── Удаление страницы ────────────────────────────────────────────

async def handle_wiki_delete(request: web.Request):
    token = request.cookies.get("dsess_token","")
    if not _has_perm(token, "view_overview"):
        raise web.HTTPFound("/dashboard/wiki")
    sess = _get_session(request)
    slug = request.match_info.get("slug","")
    if slug:
        _wiki_delete_page(slug)
        _log_admin_db(sess.get("uid",0) if sess else 0, "WIKI_DELETE", f"Удалена: {slug}")
    raise web.HTTPFound("/dashboard/wiki")


# ── История правок конкретной страницы ───────────────────────────

async def handle_wiki_history(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _wiki_init_db()

    slug = request.match_info.get("slug","")
    p    = _wiki_get_page(slug)
    if not p:
        raise web.HTTPFound("/dashboard/wiki")

    history = _wiki_get_history(p["id"], limit=50)

    rows_html = ""
    for i, h in enumerate(history):
        dt   = str(h.get("changed_at",""))[:16].replace("T"," ")
        dsz  = h.get("diff_size",0)
        sign = f"+{dsz}" if dsz >= 0 else str(dsz)
        color = "var(--success)" if dsz > 0 else "var(--danger)"
        # Превью старого текста
        old_preview = (h.get("body","") or "")[:120].replace("<","&lt;")
        rows_html += (
            f'<div style="padding:16px;border-bottom:1px solid var(--border);">'
            f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">'
            f'<div style="width:32px;height:32px;border-radius:50%;background:var(--accent-glow);'
            f'display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:var(--accent);">'
            f'{len(history)-i}</div>'
            f'<div style="flex:1;">'
            f'<div style="font-weight:700;font-size:13px;">{h.get("author","—")}</div>'
            f'<div style="font-size:11px;color:var(--text2);">{dt}</div>'
            f'</div>'
            f'<span style="font-size:12px;font-weight:700;color:{color};font-family:Space Mono,monospace;">{sign} симв</span>'
            f'</div>'
            f'<div style="font-size:12px;color:var(--text2);background:var(--bg3);padding:8px 12px;'
            f'border-radius:6px;font-family:Space Mono,monospace;white-space:pre-wrap;word-break:break-all;">'
            f'{old_preview}{"..." if len(h.get("body","") or "") > 120 else ""}'
            f'</div>'
            f'</div>'
        )
    if not rows_html:
        rows_html = '<div class="empty-state">История правок пуста</div>'

    body = navbar(sess, "wiki") + f"""
    <div class="container">
      <div class="page-title">
        📜 История: {p["title"]}
        <a href="/dashboard/wiki/{slug}" class="btn btn-ghost btn-sm" style="margin-left:auto;">← Назад</a>
        <a href="/dashboard/wiki/{slug}/edit" class="btn btn-primary btn-sm">✏️ Редактировать</a>
      </div>

      <div class="section">
        <div class="section-header">
          Всего правок: {len(history)}
          <span style="font-size:12px;color:var(--text2);font-weight:400;">последние 50</span>
        </div>
        {rows_html}
      </div>
    </div>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_wiki_history = require_auth("view_overview")(handle_wiki_history)


# ── API: быстрый поиск для quick search ─────────────────────────

async def api_wiki_search(request: web.Request):
    token = request.cookies.get("dsess_token","")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"results":[]})
    q = request.rel_url.query.get("q","").strip()
    if len(q) < 2:
        return web.json_response({"results":[]})
    pages = _wiki_get_all(search=q)
    return web.json_response({
        "results": [{"slug": p["slug"], "title": p["title"], "category": p["category"]}
                    for p in pages[:8]]
    })



# ══════════════════════════════════════════════════════════════════
#  🎙 ГОЛОСОВЫЕ КОМАНДЫ
# ══════════════════════════════════════════════════════════════════
# Команды отправляются боту через HTTP → бот исполняет в Telegram
# Поддерживаемые фразы (NLU на ключевых словах, без внешних API):
#   "забань [имя/id]", "замути [имя] на [время]", "сними мут с [имя]",
#   "выдай варн [имя]", "разбань [имя]", "статус чата", "топ активных",
#   "заблокируй чат", "разблокируй чат", "отправь объявление [текст]"

_VOICE_PATTERNS = [
    # (ключевые слова, action, описание)
    (["забань", "бан", "ban"],                     "ban",          "🔨 Бан пользователя"),
    (["замути", "мут", "заглуши"],                 "mute",         "🔇 Мут пользователя"),
    (["сними мут", "размути", "unmute"],           "unmute",       "🔊 Снять мут"),
    (["варн", "предупреди", "warn"],               "warn",         "⚡ Варн"),
    (["разбань", "unban"],                         "unban",        "🕊 Разбан"),
    (["заблокируй чат", "lock", "локдаун"],        "lockdown",     "🔒 Локдаун"),
    (["разблокируй чат", "unlock", "открой чат"],  "unlock",       "🔓 Открыть чат"),
    (["статус", "status", "состояние"],            "status",       "📊 Статус"),
    (["топ активных", "топ юзеров", "топ"],        "top_active",   "🏆 Топ активных"),
    (["объявление", "объяви", "анонс", "announce"],"announce",     "📢 Объявление"),
    (["кик", "kick", "выгони"],                    "kick",         "👟 Кик"),
    (["очисти чат", "clear", "удали сообщения"],   "clear_chat",   "🧹 Очистка"),
    (["статистика", "стат чата", "аналитика"],     "chat_stats",   "📈 Статистика"),
    (["рассылка", "broadcast"],                    "broadcast",    "📡 Рассылка"),
]

def _voice_parse(text: str) -> dict:
    """Разбирает голосовую команду, возвращает {action, target, arg}."""
    t = text.lower().strip()
    result = {"action": None, "target": None, "arg": "", "raw": text}

    for keywords, action, _ in _VOICE_PATTERNS:
        if any(kw in t for kw in keywords):
            result["action"] = action
            break

    if not result["action"]:
        return result

    # Ищем ID (числовой или @username)
    import re
    id_m = re.search(r"id[:\s]*(\d+)", t)
    at_m = re.search(r"@(\w+)", t)
    num_m = re.search(r"\b(\d{5,12})\b", t)
    if id_m:
        result["target"] = id_m.group(1)
    elif at_m:
        result["target"] = "@" + at_m.group(1)
    elif num_m:
        result["target"] = num_m.group(1)

    # Время мута
    time_m = re.search(r"на (\d+)\s*(мин|ч|час|д|день)", t)
    if time_m:
        n, unit = int(time_m.group(1)), time_m.group(2)
        if "мин" in unit:   result["arg"] = f"{n}m"
        elif "ч" in unit or "час" in unit: result["arg"] = f"{n}h"
        elif "д" in unit or "ден" in unit: result["arg"] = f"{n}d"

    # Текст объявления
    if result["action"] == "announce":
        for kw in ["объявление", "объяви", "анонс", "announce"]:
            idx = t.find(kw)
            if idx != -1:
                after = text[idx + len(kw):].strip(" :–-")
                if after:
                    result["arg"] = after
                break

    return result

def _voice_history_init():
    try:
        conn = db.get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS voice_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER, admin_name TEXT,
            raw_text TEXT, action TEXT, target TEXT, arg TEXT,
            status TEXT, result TEXT,
            created_at TEXT DEFAULT (datetime('now')))""")
        conn.commit(); conn.close()
    except: pass

async def handle_voice_cmd(request: web.Request):
    import traceback as _voice_tb
    try:
        return await _handle_voice_cmd_inner(request)
    except Exception as _voice_err:
        return web.Response(
            text=f"<pre style='color:red;padding:20px'>VOICE 500:\n{_voice_tb.format_exc()}</pre>",
            content_type="text/html", status=500)

async def _handle_voice_cmd_inner(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _voice_history_init()
    result_html = ""

    action = None; target = None; arg = ""
    if request.method == "POST":
        data = await request.post()
        raw_text = (data.get("voice_text") or "").strip()
        chat_id_str = data.get("chat_id") or ""

        if not raw_text:
            result_html = '<div style="color:var(--danger);padding:10px;">⚠️ Пустая команда</div>'
        else:
            parsed = _voice_parse(raw_text)
            action = parsed["action"]
            target = parsed["target"]
            arg    = parsed["arg"]
            admin_id   = sess.get("uid", 0) if sess else 0
            admin_name = sess.get("name", "?") if sess else "?"

            status = "ok"; res_text = ""
            bot_inst = _bot_instance

            if not action:
                status = "error"
                res_text = f"❓ Команда не распознана: «{raw_text}»"
            elif not bot_inst:
                status = "error"
                res_text = "🤖 Бот не подключён к дашборду"
            else:
                try:
                    from aiogram.types import ChatPermissions
                    from datetime import timedelta
                    chat_id = int(chat_id_str) if chat_id_str else 0

                    if action == "status":
                        res_text = "📊 Команда статус выполнена — см. Overview"
                    elif action == "top_active":
                        res_text = "🏆 Топ активных — см. раздел Аналитика"
                    elif action == "lockdown" and chat_id:
                        await bot_inst.set_chat_permissions(chat_id,
                            ChatPermissions(can_send_messages=False))
                        res_text = f"🔒 Чат {chat_id} заблокирован"
                    elif action == "unlock" and chat_id:
                        await bot_inst.set_chat_permissions(chat_id,
                            ChatPermissions(can_send_messages=True,
                                can_send_media_messages=True, can_send_polls=True,
                                can_send_other_messages=True, can_add_web_page_previews=True))
                        res_text = f"🔓 Чат {chat_id} разблокирован"
                    elif action == "announce" and chat_id and arg:
                        await bot_inst.send_message(chat_id,
                            f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{arg}\n\n— Администрация",
                            parse_mode="HTML")
                        res_text = f"📢 Объявление отправлено в чат {chat_id}"
                    elif action in ("ban","mute","unmute","warn","unban","kick") and target and chat_id:
                        target_id = int(target) if target.isdigit() else 0
                        if target_id:
                            if action == "ban":
                                await bot_inst.ban_chat_member(chat_id, target_id)
                                res_text = f"🔨 Бан ID{target_id}"
                            elif action == "mute":
                                mins = 60
                                if arg:
                                    import re
                                    m = re.match(r"(\d+)([mhd])", arg)
                                    if m:
                                        n = int(m.group(1))
                                        u = m.group(2)
                                        mins = n if u=="m" else (n*60 if u=="h" else n*1440)
                                await bot_inst.restrict_chat_member(chat_id, target_id,
                                    ChatPermissions(can_send_messages=False),
                                    until_date=timedelta(minutes=mins))
                                res_text = f"🔇 Мут ID{target_id} на {mins} мин"
                            elif action == "unmute":
                                await bot_inst.restrict_chat_member(chat_id, target_id,
                                    ChatPermissions(can_send_messages=True,
                                        can_send_media_messages=True, can_send_polls=True,
                                        can_send_other_messages=True, can_add_web_page_previews=True))
                                res_text = f"🔊 Мут снят ID{target_id}"
                            elif action == "unban":
                                await bot_inst.unban_chat_member(chat_id, target_id, only_if_banned=True)
                                res_text = f"🕊 Разбан ID{target_id}"
                            elif action == "kick":
                                await bot_inst.ban_chat_member(chat_id, target_id)
                                await bot_inst.unban_chat_member(chat_id, target_id)
                                res_text = f"👟 Кик ID{target_id}"
                            elif action == "warn":
                                res_text = f"⚡ Варн — реализуется через бот. ID{target_id}"
                        else:
                            status = "warn"
                            res_text = f"⚠️ Не удалось определить ID цели: {target}"
                    elif action == "clear_chat" and chat_id:
                        res_text = "🧹 Очистка — используй панель модерации"
                    elif action == "broadcast":
                        res_text = "📡 Рассылка — используй страницу Broadcast"
                    else:
                        status = "warn"
                        res_text = f"⚠️ Действие «{action}» требует уточнения (чат/цель)"

                    # Логируем
                    conn = db.get_conn()
                    conn.execute("""INSERT INTO voice_history
                        (admin_id,admin_name,raw_text,action,target,arg,status,result)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        (admin_id, admin_name, raw_text, action or "", target or "", arg or "", status, res_text))
                    conn.commit(); conn.close()
                    _log_admin_db(admin_id, "VOICE_CMD", f"{action}: {res_text}")

                except Exception as e:
                    import traceback as _tvc
                    status = "error"
                    res_text = f"❌ Ошибка: {e} | {_tvc.format_exc()[:200]}"

            color = "var(--success)" if status=="ok" else ("var(--warning)" if status=="warn" else "var(--danger)")
            icon  = "✅" if status=="ok" else ("⚠️" if status=="warn" else "❌")
            result_html = (
                f'<div style="background:var(--bg3);border-left:3px solid {color};'
                f'padding:12px 16px;border-radius:0 8px 8px 0;margin-bottom:20px;">'
                f'<div style="font-weight:700;margin-bottom:4px;">{icon} {res_text}</div>'
                f'<div style="font-size:12px;color:var(--text2);">Распознано: {action or "—"}'
                f' | Цель: {target or "—"} | Аргумент: {arg or "—"}</div>'
                f'</div>'
            )

    # Получаем историю
    history_rows = []
    try:
        conn = db.get_conn()
        history_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM voice_history ORDER BY created_at DESC LIMIT 30"
        ).fetchall()]
        conn.close()
    except: pass

    # Список чатов для выбора
    chats = []
    try:
        chats = [dict(r) for r in await db.get_all_chats()]
    except: pass
    chat_opts = "".join(f'<option value="{c["cid"]}">{c["title"][:30]}</option>' for c in chats)

    # Паттерны подсказок
    patterns_html = ""
    for keywords, action, desc in _VOICE_PATTERNS:
        example = keywords[0]
        onclick_val = f"document.getElementById(\'voiceText\').value=\'{example} @username\'"
        patterns_html += (
            f'<div style="display:flex;align-items:center;gap:8px;padding:6px 0;'
            f'border-bottom:1px solid var(--border);cursor:pointer;" '
            f'onclick="{onclick_val}">'
            f'<span style="font-size:13px;flex:1;">{desc}</span>'
            f'<code style="font-size:11px;background:var(--bg3);padding:2px 6px;border-radius:4px;">'
            f'{example}</code>'
            f'</div>'
        )

    hist_html = ""
    for h in history_rows:
        color = "#22c55e" if h.get("status")=="ok" else ("#f59e0b" if h.get("status")=="warn" else "#ef4444")
        hist_html += (
            f'<div style="padding:8px 0;border-bottom:1px solid var(--border);">'
            f'<div style="display:flex;gap:8px;align-items:center;">'
            f'<span style="width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0;"></span>'
            f'<span style="font-size:12px;flex:1;">{h.get("raw_text","")[:60]}</span>'
            f'<span style="font-size:11px;color:var(--text2);">{str(h.get("created_at",""))[:16]}</span>'
            f'</div>'
            f'<div style="font-size:11px;color:var(--text2);padding-left:16px;margin-top:2px;">'
            f'{h.get("result","")[:80]}</div>'
            f'</div>'
        )
    if not hist_html:
        hist_html = '<div style="color:var(--text2);font-size:13px;padding:12px 0;">Команд ещё не было</div>'

    body = navbar(sess, "voice") + f"""
    <div class="container">
      <div class="page-title">🎙 Голосовые команды</div>
      {result_html}
      <div class="grid-2" style="gap:24px;align-items:start;">

        <div>
          <div class="section">
            <div class="section-header">🎤 Ввод команды</div>
            <div class="section-body">
              <form method="POST">
                <div class="form-group">
                  <label>Чат</label>
                  <select class="form-control" name="chat_id">
                    <option value="">— выбери чат —</option>
                    {chat_opts}
                  </select>
                </div>
                <div class="form-group">
                  <label>Команда текстом (или нажми микрофон)</label>
                  <div style="display:flex;gap:8px;">
                    <input class="form-control" type="text" name="voice_text" id="voiceText"
                      placeholder="Например: замути @user на 30 мин" style="flex:1;">
                    <button type="button" class="btn btn-primary" id="micBtn"
                      onclick="startVoice()" title="Говори">🎤</button>
                  </div>
                  <div id="voiceStatus" style="font-size:12px;color:var(--text2);margin-top:6px;"></div>
                </div>
                <button class="btn btn-primary" type="submit" style="width:100%;">
                  ▶ Выполнить
                </button>
              </form>
            </div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">📜 История команд</div>
            <div style="padding:0 16px 8px;">{hist_html}</div>
          </div>
        </div>

        <div>
          <div class="section">
            <div class="section-header">📋 Доступные команды</div>
            <div style="padding:0 16px 8px;font-size:12px;color:var(--text2);padding-top:8px;">
              Нажми на строку — вставит пример в поле ввода
            </div>
            <div style="padding:0 16px 8px;">{patterns_html}</div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">💡 Примеры фраз</div>
            <div style="padding:12px 16px;font-size:12px;line-height:2;">
              <code>забань 123456789</code><br>
              <code>замути @username на 1 час</code><br>
              <code>сними мут с 123456789</code><br>
              <code>заблокируй чат</code><br>
              <code>объявление Внимание всем!</code><br>
              <code>статус</code>
            </div>
          </div>
        </div>
      </div>
    </div>

    <script>
    var recognition = null;
    function startVoice() {{
      var btn = document.getElementById("micBtn");
      var status = document.getElementById("voiceStatus");
      if (!("webkitSpeechRecognition" in window) && !("SpeechRecognition" in window)) {{
        status.textContent = "⚠️ Браузер не поддерживает голосовой ввод (используй Chrome)";
        return;
      }}
      if (recognition) {{ recognition.stop(); recognition = null; btn.textContent = "🎤"; return; }}
      var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
      recognition = new SR();
      recognition.lang = "ru-RU";
      recognition.continuous = false;
      recognition.interimResults = false;
      recognition.onstart = function() {{
        btn.textContent = "⏹";
        btn.style.background = "#ef4444";
        status.textContent = "🔴 Говорите...";
      }};
      recognition.onresult = function(e) {{
        var text = e.results[0][0].transcript;
        document.getElementById("voiceText").value = text;
        status.textContent = "✅ Распознано: " + text;
        btn.textContent = "🎤"; btn.style.background = "";
        recognition = null;
      }};
      recognition.onerror = function(e) {{
        status.textContent = "❌ Ошибка: " + e.error;
        btn.textContent = "🎤"; btn.style.background = "";
        recognition = null;
      }};
      recognition.onend = function() {{
        btn.textContent = "🎤"; btn.style.background = "";
        recognition = null;
      }};
      recognition.start();
    }}
    </script>
    """ + close_main()
    try:
        rendered = page(body)
    except Exception as _ve:
        import traceback as _tb
        return web.Response(text=f"<pre style='color:red'>VOICE ERROR:\n{_tb.format_exc()}</pre>",
                            content_type="text/html")
    return web.Response(text=rendered, content_type="text/html")

handle_voice_cmd = require_auth("view_overview")(handle_voice_cmd)


# ══════════════════════════════════════════════════════════════════
#  📊 АВТО-ОТЧЁТЫ В КАНАЛ
# ══════════════════════════════════════════════════════════════════

def _reports_cfg_init():
    try:
        conn = db.get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS auto_reports_cfg (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            channel_title TEXT DEFAULT '',
            schedule TEXT DEFAULT 'daily',
            hour INTEGER DEFAULT 21,
            report_type TEXT DEFAULT 'full',
            enabled INTEGER DEFAULT 1,
            last_sent TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS auto_reports_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT, report_type TEXT,
            status TEXT, error TEXT,
            sent_at TEXT DEFAULT (datetime('now')))""")
        conn.commit(); conn.close()
    except: pass

async def _generate_report_text(report_type: str = "full") -> str:
    """Генерирует HTML-отчёт для отправки в Telegram."""
    from datetime import datetime, timedelta
    now = datetime.now()
    today = now.strftime("%d.%m.%Y")

    lines = [
        f"📊 <b>АВТО-ОТЧЁТ</b> — {today} {now.strftime('%H:%M')}",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    try:
        conn = db.get_conn()

        # Статистика по чатам
        chats = await db.get_all_chats()
        lines.append(f"\n💬 <b>Чатов:</b> {len(chats)}")

        # Тикеты
        try:
            t_open = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
            t_prog = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='in_progress'").fetchone()[0]
            lines.append(f"🎫 <b>Тикеты:</b> {t_open} открытых, {t_prog} в работе")
        except: pass

        # Модерация за сегодня
        try:
            actions_today = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE date(created_at)=date('now')"
            ).fetchone()[0]
            lines.append(f"⚔️ <b>Действий сегодня:</b> {actions_today}")
        except: pass

        # Топ модераторов
        if report_type in ("full", "mods"):
            try:
                top_mods = conn.execute(
                    "SELECT by_name, COUNT(*) as cnt FROM mod_history "
                    "WHERE date(created_at)=date('now') "
                    "GROUP BY by_name ORDER BY cnt DESC LIMIT 5"
                ).fetchall()
                if top_mods:
                    lines.append("\n🏆 <b>Топ модераторов сегодня:</b>")
                    for i, m in enumerate(top_mods, 1):
                        lines.append(f"  {i}. {m['by_name']} — {m['cnt']} действий")
            except: pass

        # Алерты
        if report_type in ("full", "security"):
            try:
                alerts_today = conn.execute(
                    "SELECT COUNT(*) FROM dashboard_admin_log WHERE action LIKE '%ALERT%' AND date(created_at)=date('now')"
                ).fetchone()[0]
                lines.append(f"\n🚨 <b>Алертов сегодня:</b> {alerts_today}")
            except: pass

        # Варны и баны из mod_history
        try:
            bans = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE action LIKE '%Бан%' AND date(created_at)=date('now')"
            ).fetchone()[0]
            warns = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE action LIKE '%Варн%' AND date(created_at)=date('now')"
            ).fetchone()[0]
            lines.append(f"\n⚡ <b>Варнов:</b> {warns} | 🔨 <b>Банов:</b> {bans}")
        except: pass

        conn.close()
    except Exception as e:
        lines.append(f"\n⚠️ Частичная ошибка: {e}")

    lines.append("\n━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🤖 <i>Chat Guard Dashboard</i>")
    return "\n".join(lines)

async def _send_report_to_channel(channel_id: str, report_type: str = "full"):
    """Отправляет отчёт в канал через бота."""
    bot_inst = _bot_instance
    if not bot_inst:
        return False, "Бот не подключён"
    try:
        text = await _generate_report_text(report_type)
        await bot_inst.send_message(int(channel_id), text, parse_mode="HTML")
        # Лог
        try:
            conn = db.get_conn()
            conn.execute("INSERT INTO auto_reports_log (channel_id,report_type,status) VALUES (?,?,?)",
                         (channel_id, report_type, "ok"))
            conn.commit(); conn.close()
        except: pass
        return True, "Отчёт отправлен"
    except Exception as e:
        try:
            conn = db.get_conn()
            conn.execute("INSERT INTO auto_reports_log (channel_id,report_type,status,error) VALUES (?,?,?,?)",
                         (channel_id, report_type, "error", str(e)))
            conn.commit(); conn.close()
        except: pass
        return False, str(e)

async def handle_reports_cfg(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _reports_cfg_init()
    result_html = ""

    if request.method == "POST":
        data = await request.post()
        action = data.get("action","")

        if action == "add":
            channel_id = (data.get("channel_id") or "").strip()
            channel_title = (data.get("channel_title") or "").strip()
            schedule = data.get("schedule","daily")
            hour = int(data.get("hour","21") or 21)
            report_type = data.get("report_type","full")

            if not channel_id:
                result_html = '<div class="alert alert-danger">⚠️ Укажи ID канала</div>'
            else:
                try:
                    conn = db.get_conn()
                    conn.execute("""INSERT INTO auto_reports_cfg
                        (channel_id,channel_title,schedule,hour,report_type,enabled)
                        VALUES (?,?,?,?,?,1)""",
                        (channel_id, channel_title, schedule, hour, report_type))
                    conn.commit(); conn.close()
                    _log_admin_db(sess.get("uid",0) if sess else 0, "REPORTS_CFG_ADD", f"Канал: {channel_id}")
                    result_html = f'<div class="alert alert-success">✅ Канал {channel_id} добавлен</div>'
                except Exception as e:
                    result_html = f'<div class="alert alert-danger">❌ {e}</div>'

        elif action == "toggle":
            cfg_id = data.get("cfg_id","")
            try:
                conn = db.get_conn()
                row = conn.execute("SELECT enabled FROM auto_reports_cfg WHERE id=?", (cfg_id,)).fetchone()
                if row:
                    new_val = 0 if row["enabled"] else 1
                    conn.execute("UPDATE auto_reports_cfg SET enabled=? WHERE id=?", (new_val, cfg_id))
                    conn.commit()
                conn.close()
            except: pass

        elif action == "delete":
            cfg_id = data.get("cfg_id","")
            try:
                conn = db.get_conn()
                conn.execute("DELETE FROM auto_reports_cfg WHERE id=?", (cfg_id,))
                conn.commit(); conn.close()
            except: pass

    # Загружаем конфиги
    cfgs = []
    try:
        conn = db.get_conn()
        cfgs = [dict(r) for r in conn.execute(
            "SELECT * FROM auto_reports_cfg ORDER BY id DESC"
        ).fetchall()]
        conn.close()
    except: pass

    # Последние логи
    logs = []
    try:
        conn = db.get_conn()
        logs = [dict(r) for r in conn.execute(
            "SELECT * FROM auto_reports_log ORDER BY sent_at DESC LIMIT 20"
        ).fetchall()]
        conn.close()
    except: pass

    sched_map = {"daily":"Ежедневно","weekly":"Еженедельно","hourly":"Каждый час"}
    type_map  = {"full":"Полный","mods":"Модераторы","security":"Безопасность","tickets":"Тикеты"}

    cfgs_html = ""
    for c in cfgs:
        enabled = c.get("enabled",1)
        status_color = "var(--success)" if enabled else "var(--text2)"
        status_text  = "🟢 Активен" if enabled else "⚫ Выключен"
        _cid_val   = c.get("channel_id","")
        _rtype_val = c.get("report_type","full")
        _cid_int   = c["id"]
        cfgs_html += "".join([
            '<div style="display:flex;align-items:center;gap:12px;padding:12px 0;',
            'border-bottom:1px solid var(--border);">',
            '<div style="flex:1;">',
            f'<div style="font-weight:600;">{c.get("channel_title") or "Канал"} ',
            f'<code style="font-size:11px;">({_cid_val})</code></div>',
            f'<div style="font-size:12px;color:var(--text2);">',
            f'{sched_map.get(c.get("schedule",""),"")} в {c.get("hour","21")}:00 · ',
            f'{type_map.get(c.get("report_type",""),"")}</div>',
            '</div>',
            f'<span style="color:{status_color};font-size:12px;">{status_text}</span>',
            f'<form method="POST" style="display:inline;">',
            f'<input type="hidden" name="action" value="toggle">',
            f'<input type="hidden" name="cfg_id" value="{_cid_int}">',
            '<button class="btn btn-xs btn-ghost" type="submit">⏯</button>',
            '</form>',
            '<form method="POST" style="display:inline;">',
            '<input type="hidden" name="action" value="delete">',
            f'<input type="hidden" name="cfg_id" value="{_cid_int}">',
            '<button class="btn btn-xs" style="color:var(--danger);" ',
            'onclick="return confirm(\'Удалить?\')" type="submit">\U0001f5d1 Удалить</button>',
            '</form>',
            f'<button class="btn btn-xs btn-primary" ',
            f'onclick="sendNow({_cid_int}, \'{_cid_val}\', \'{_rtype_val}\')">' + '▶ Сейчас</button>',
            '</div>',
        ])
    if not cfgs_html:
        cfgs_html = '<div class="empty-state">Нет настроенных каналов</div>'

    log_html = ""
    for l in logs:
        ok = l.get("status") == "ok"
        icon = "✅" if ok else "❌"
        err_txt = (" — " + l["error"][:40]) if not ok and l.get("error") else ""
        log_html += (
            f'<div style="font-size:12px;padding:5px 0;border-bottom:1px solid var(--border);">'
            f'{icon} {l.get("channel_id","")[:15]} · {type_map.get(l.get("report_type",""),"")} · '
            f'{str(l.get("sent_at",""))[:16]}'
            f'{err_txt}'
        )
    if not log_html:
        log_html = '<div style="color:var(--text2);font-size:12px;padding:8px 0;">Логов нет</div>'

    hour_opts = "".join(f'<option value="{h}">{h}:00</option>' for h in range(0,24))

    body = navbar(sess, "reports_cfg") + f"""
    <div class="container">
      <div class="page-title">📊 Авто-отчёты в канал</div>
      {result_html}

      <div class="grid-2" style="gap:24px;align-items:start;">
        <div>
          <div class="section">
            <div class="section-header">➕ Добавить канал</div>
            <div class="section-body">
              <form method="POST">
                <input type="hidden" name="action" value="add">
                <div class="form-group">
                  <label>ID канала * <span style="font-size:11px;color:var(--text2);">(отрицательное число)</span></label>
                  <input class="form-control" name="channel_id" placeholder="-1001234567890" required>
                  <div style="font-size:11px;color:var(--text2);margin-top:4px;">
                    Чтобы узнать ID: добавь бота в канал и напиши /start
                  </div>
                </div>
                <div class="form-group">
                  <label>Название канала</label>
                  <input class="form-control" name="channel_title" placeholder="Лог-канал">
                </div>
                <div class="form-group">
                  <label>Расписание</label>
                  <select class="form-control" name="schedule">
                    <option value="daily">Ежедневно</option>
                    <option value="weekly">Еженедельно (понедельник)</option>
                    <option value="hourly">Каждый час</option>
                  </select>
                </div>
                <div class="form-group">
                  <label>Час отправки (для daily/weekly)</label>
                  <select class="form-control" name="hour">{hour_opts}</select>
                </div>
                <div class="form-group">
                  <label>Тип отчёта</label>
                  <select class="form-control" name="report_type">
                    <option value="full">Полный (всё)</option>
                    <option value="mods">Модераторы</option>
                    <option value="security">Безопасность</option>
                    <option value="tickets">Тикеты</option>
                  </select>
                </div>
                <button class="btn btn-primary" type="submit" style="width:100%;">➕ Добавить</button>
              </form>
            </div>
          </div>
        </div>

        <div>
          <div class="section">
            <div class="section-header">📋 Настроенные каналы</div>
            <div style="padding:0 16px 8px;">{cfgs_html}</div>
          </div>
          <div class="section" style="margin-top:20px;">
            <div class="section-header">📝 Лог отправок</div>
            <div style="padding:0 16px 8px;">{log_html}</div>
          </div>
        </div>
      </div>
    </div>
    <script>
    function sendNow(cfgId, channelId, reportType) {{
      if (!confirm('Отправить отчёт прямо сейчас?')) return;
      fetch('/api/reports_cfg/send_now', {{
        method: 'POST',
        headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{channel_id: channelId, report_type: reportType}})
      }}).then(r=>r.json()).then(d=>{{
        alert(d.ok ? '✅ ' + d.msg : '❌ ' + d.msg);
      }});
    }}
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_reports_cfg = require_auth("view_overview")(handle_reports_cfg)

async def api_reports_send_now(request: web.Request):
    token = request.cookies.get("dsess_token","")
    if not _has_perm(token, "view_overview"):
        return web.json_response({"ok": False, "msg": "Нет доступа"})
    try:
        data = await request.json()
        channel_id  = data.get("channel_id","")
        report_type = data.get("report_type","full")
        ok, msg = await _send_report_to_channel(channel_id, report_type)
        return web.json_response({"ok": ok, "msg": msg})
    except Exception as e:
        return web.json_response({"ok": False, "msg": str(e)})

async def _auto_reports_loop():
    """Фоновая задача — проверяет расписание и отправляет отчёты."""
    import asyncio as _aio
    from datetime import datetime
    while True:
        await _aio.sleep(60)
        try:
            now = datetime.now()
            conn = db.get_conn()
            cfgs = [dict(r) for r in conn.execute(
                "SELECT * FROM auto_reports_cfg WHERE enabled=1"
            ).fetchall()]
            conn.close()
            for c in cfgs:
                schedule = c.get("schedule","daily")
                hour     = c.get("hour", 21)
                last     = c.get("last_sent","")
                today    = now.strftime("%Y-%m-%d")
                channel  = c.get("channel_id","")
                rtype    = c.get("report_type","full")
                should_send = False
                if schedule == "hourly":
                    should_send = not last.startswith(now.strftime("%Y-%m-%d %H"))
                elif schedule == "daily":
                    should_send = now.hour == hour and not last.startswith(today)
                elif schedule == "weekly":
                    should_send = now.weekday() == 0 and now.hour == hour and not last.startswith(today)
                if should_send and channel:
                    ok, _ = await _send_report_to_channel(channel, rtype)
                    if ok:
                        conn2 = db.get_conn()
                        conn2.execute("UPDATE auto_reports_cfg SET last_sent=? WHERE id=?",
                                      (now.strftime("%Y-%m-%d %H:%M"), c["id"]))
                        conn2.commit(); conn2.close()
        except: pass


# ══════════════════════════════════════════════════════════════════
#  ⚡ КОНСТРУКТОР АВТОПРАВИЛ
# ══════════════════════════════════════════════════════════════════
#
#  ЕСЛИ [триггер] И [условие] ТО [действие]
#  Примеры:
#    ЕСЛИ warn_count >= 3 ТО mute 60m
#    ЕСЛИ новый участник + содержит ссылку ТО mute 24h + warn
#    ЕСЛИ flood >= 10 msg/min ТО mute 5m
#    ЕСЛИ score < -50 ТО warn + notify_owner

_AUTOMATION_TRIGGERS = {
    "warn_count":     "⚡ Количество варнов",
    "msg_per_min":    "💬 Сообщений в минуту (флуд)",
    "new_member":     "👤 Новый участник",
    "link_in_msg":    "🔗 Ссылка в сообщении",
    "reputation":     "⭐ Репутация пользователя",
    "night_message":  "🌙 Сообщение ночью (23:00–07:00)",
    "ban_count":      "🔨 Количество банов",
    "keyword":        "🔤 Ключевое слово в сообщении",
    "join_age_days":  "📅 Аккаунт моложе N дней",
    "no_avatar":      "👤 Нет аватарки",
}

_AUTOMATION_CONDITIONS = {
    ">=": "≥ больше или равно",
    "<=": "≤ меньше или равно",
    ">":  "> строго больше",
    "<":  "< строго меньше",
    "==": "= равно",
    "contains": "содержит (текст)",
    "true": "= да (без значения)",
}

_AUTOMATION_ACTIONS = {
    "mute_5m":    "🔇 Мут на 5 минут",
    "mute_1h":    "🔇 Мут на 1 час",
    "mute_24h":   "🔇 Мут на 24 часа",
    "warn":       "⚡ Выдать варн",
    "ban":        "🔨 Забанить",
    "kick":       "👟 Кикнуть",
    "delete_msg": "🗑 Удалить сообщение",
    "notify_mods":"📣 Уведомить модов в ЛС",
    "notify_owner":"👑 Уведомить владельца",
    "add_to_watch":"👁 Добавить в список наблюдения",
    "lockdown":   "🔒 Локдаун чата",
    "send_rules": "📋 Отправить правила в ЛС",
}

def _automations_init():
    try:
        conn = db.get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS automation_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            chat_id TEXT DEFAULT 'all',
            trigger TEXT NOT NULL,
            condition_op TEXT DEFAULT '>=',
            condition_val TEXT DEFAULT '3',
            action TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            fires INTEGER DEFAULT 0,
            created_by TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS automation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER, rule_name TEXT,
            chat_id TEXT, uid TEXT, result TEXT,
            fired_at TEXT DEFAULT (datetime('now')))""")
        conn.commit(); conn.close()
    except: pass

async def handle_automations(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _automations_init()
    result_html = ""

    if request.method == "POST":
        data = await request.post()
        action = data.get("action","")

        if action == "create":
            name   = (data.get("name") or "Правило").strip()
            chat_id= (data.get("chat_id") or "all").strip()
            trig   = data.get("trigger","warn_count")
            cond_op= data.get("condition_op",">=")
            cond_val = (data.get("condition_val") or "3").strip()
            act    = data.get("act","warn")
            by     = (sess.get("name","?") if sess else "?")
            try:
                conn = db.get_conn()
                conn.execute("""INSERT INTO automation_rules
                    (name,chat_id,trigger,condition_op,condition_val,action,enabled,created_by)
                    VALUES (?,?,?,?,?,?,1,?)""",
                    (name, chat_id, trig, cond_op, cond_val, act, by))
                conn.commit(); conn.close()
                _log_admin_db(sess.get("uid",0) if sess else 0, "AUTOMATION_CREATE", f"{name}")
                result_html = f'<div class="alert alert-success">✅ Правило «{name}» создано</div>'
            except Exception as e:
                result_html = f'<div class="alert alert-danger">❌ {e}</div>'

        elif action == "toggle":
            rule_id = data.get("rule_id","")
            try:
                conn = db.get_conn()
                row = conn.execute("SELECT enabled FROM automation_rules WHERE id=?", (rule_id,)).fetchone()
                if row:
                    conn.execute("UPDATE automation_rules SET enabled=? WHERE id=?",
                                 (0 if row["enabled"] else 1, rule_id))
                    conn.commit()
                conn.close()
            except: pass

        elif action == "delete":
            rule_id = data.get("rule_id","")
            try:
                conn = db.get_conn()
                conn.execute("DELETE FROM automation_rules WHERE id=?", (rule_id,))
                conn.commit(); conn.close()
            except: pass

    # Загружаем правила
    rules = []
    try:
        conn = db.get_conn()
        rules = [dict(r) for r in conn.execute(
            "SELECT * FROM automation_rules ORDER BY id DESC"
        ).fetchall()]
        conn.close()
    except: pass

    # Лог срабатываний
    auto_log = []
    try:
        conn = db.get_conn()
        auto_log = [dict(r) for r in conn.execute(
            "SELECT * FROM automation_log ORDER BY fired_at DESC LIMIT 30"
        ).fetchall()]
        conn.close()
    except: pass

    # Строим таблицу правил
    rules_html = ""
    for r in rules:
        enabled = r.get("enabled",1)
        trig_label = _AUTOMATION_TRIGGERS.get(r.get("trigger",""), r.get("trigger",""))
        act_label  = _AUTOMATION_ACTIONS.get(r.get("action",""), r.get("action",""))
        cond_label = _AUTOMATION_CONDITIONS.get(r.get("condition_op",""), r.get("condition_op",""))
        status_color = "#22c55e" if enabled else "#6b7280"
        rules_html += (
            f'<div style="padding:14px 0;border-bottom:1px solid var(--border);">'
            f'<div style="display:flex;align-items:flex-start;gap:12px;">'
            f'<div style="width:10px;height:10px;border-radius:50%;background:{status_color};'
            f'margin-top:4px;flex-shrink:0;"></div>'
            f'<div style="flex:1;">'
            f'<div style="font-weight:600;font-size:13px;">{r.get("name","?")}</div>'
            f'<div style="font-size:12px;color:var(--text2);margin-top:3px;">'
            f'ЕСЛИ <b>{trig_label}</b> {cond_label} <b>{r.get("condition_val","")}</b> '
            f'→ <b>{act_label}</b>'
            f'</div>'
            f'<div style="font-size:11px;color:var(--text2);margin-top:2px;">'
            f'Чат: {r.get("chat_id","all")} · Сработало: {r.get("fires",0)} раз · {r.get("created_by","")}'
            f'</div>'
            f'</div>'
            f'<div style="display:flex;gap:6px;flex-shrink:0;">'
            f'<form method="POST" style="display:inline;">'
            f'<input type="hidden" name="action" value="toggle">'
            f'<input type="hidden" name="rule_id" value="{r["id"]}">'
            f'<button class="btn btn-xs btn-ghost" type="submit" title="Вкл/выкл">⏯</button>'
            f'</form>'
            f'<form method="POST" style="display:inline;">'
            f'<input type="hidden" name="action" value="delete">'
            f'<input type="hidden" name="rule_id" value="{r["id"]}">'
            f'<button class="btn btn-xs" style="color:var(--danger);" '
            f'onclick="return confirm(\'Удалить правило?\')" type="submit">🗑</button>'
            f'</form>'
            f'</div>'
            f'</div>'
            f'</div>'
        )
    if not rules_html:
        rules_html = '<div class="empty-state">Правил нет — создай первое</div>'

    log_html = ""
    for l in auto_log:
        log_html += (
            f'<div style="font-size:12px;padding:4px 0;border-bottom:1px solid var(--border);">'
            f'⚡ <b>{l.get("rule_name","?")}</b> · uid={l.get("uid","?")} · '
            f'{str(l.get("fired_at",""))[:16]}'
            f'</div>'
        )
    if not log_html:
        log_html = '<div style="color:var(--text2);font-size:12px;padding:8px 0;">Срабатываний нет</div>'

    # Опции для селектов
    trig_opts = "".join(f'<option value="{k}">{v}</option>' for k,v in _AUTOMATION_TRIGGERS.items())
    cond_opts = "".join(f'<option value="{k}">{v}</option>' for k,v in _AUTOMATION_CONDITIONS.items())
    act_opts  = "".join(f'<option value="{k}">{v}</option>' for k,v in _AUTOMATION_ACTIONS.items())

    # Шаблоны быстрого создания
    templates = [
        ("Авто-мут флудеров",    "msg_per_min", ">=", "10",  "mute_5m"),
        ("Варн за 3 нарушения",  "warn_count",  ">=",  "3",  "notify_mods"),
        ("Кик новичков со ссылками", "new_member","true","", "kick"),
        ("Мут ночью",            "night_message","true","",  "mute_1h"),
        ("Наблюдение за новичками", "join_age_days","<=","7","add_to_watch"),
    ]
    tmpl_html = ""
    for t_name, t_trig, t_op, t_val, t_act in templates:
        _tmpl_onclick = f"fillTemplate('{t_name}','{t_trig}','{t_op}','{t_val}','{t_act}')"
        tmpl_html += (
            f'<button class="btn btn-ghost btn-sm" style="margin:3px;" '
            f'onclick="{_tmpl_onclick}">'
            f'{t_name}</button>'
        )

    body = navbar(sess, "automations") + f"""
      <div class="page-title">⚡ Конструктор автоправил</div>
      {result_html}

      <div class="grid-2" style="gap:24px;align-items:start;">
        <div>
          <div class="section">
            <div class="section-header">➕ Создать правило</div>
            <div class="section-body">
              <div style="margin-bottom:12px;">
                <div style="font-size:12px;color:var(--text2);margin-bottom:6px;">Шаблоны:</div>
                {tmpl_html}
              </div>
              <form method="POST" id="automForm">
                <input type="hidden" name="action" value="create">
                <div class="form-group">
                  <label>Название правила</label>
                  <input class="form-control" name="name" id="rName" placeholder="Мой авторуль" required>
                </div>
                <div class="form-group">
                  <label>Применять в чате</label>
                  <input class="form-control" name="chat_id" value="all" placeholder="all или ID чата">
                  <div style="font-size:11px;color:var(--text2);margin-top:3px;">all = все чаты</div>
                </div>
                <div style="background:var(--bg3);padding:14px;border-radius:10px;margin-bottom:12px;">
                  <div style="font-size:11px;color:var(--text2);font-weight:700;margin-bottom:10px;">ЕСЛИ...</div>
                  <div class="form-group" style="margin-bottom:8px;">
                    <select class="form-control" name="trigger" id="rTrig">{trig_opts}</select>
                  </div>
                  <div style="display:flex;gap:8px;">
                    <select class="form-control" name="condition_op" id="rOp" style="flex:1;">{cond_opts}</select>
                    <input class="form-control" name="condition_val" id="rVal" style="flex:1;" placeholder="3">
                  </div>
                </div>
                <div style="background:var(--bg3);padding:14px;border-radius:10px;margin-bottom:16px;">
                  <div style="font-size:11px;color:var(--text2);font-weight:700;margin-bottom:10px;">ТО...</div>
                  <select class="form-control" name="act" id="rAct">{act_opts}</select>
                </div>
                <button class="btn btn-primary" type="submit" style="width:100%;">
                  ⚡ Создать правило
                </button>
              </form>
            </div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">📝 Лог срабатываний</div>
            <div style="padding:0 16px 8px;">{log_html}</div>
          </div>
        </div>

        <div>
          <div class="section">
            <div class="section-header">📋 Активные правила ({len(rules)})</div>
            <div style="padding:0 16px 8px;">{rules_html}</div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">ℹ️ Как работает</div>
            <div style="padding:12px 16px;font-size:12px;line-height:1.8;color:var(--text2);">
              Правила проверяются ботом при каждом событии.<br>
              Для активации нужно добавить вызов <code>check_automations()</code> в <code>bot.py</code>.<br><br>
              <b>Пример в bot.py:</b><br>
              <code style="font-size:11px;background:var(--bg3);padding:4px 8px;border-radius:4px;display:block;margin-top:6px;">
              from dashboard import check_automations<br>
              await check_automations(cid, uid, "warn_count", warnings[cid][uid])
              </code>
            </div>
          </div>
        </div>
      </div>
    </div>
    <script>
    function fillTemplate(name, trig, op, val, act) {{
      document.getElementById("rName").value = name;
      document.getElementById("rTrig").value = trig;
      document.getElementById("rOp").value = op;
      document.getElementById("rVal").value = val;
      document.getElementById("rAct").value = act;
    }}
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_automations = require_auth("view_overview")(handle_automations)

async def api_automations_delete(request: web.Request):
    token = request.cookies.get("dsess_token","")
    if not _has_perm(token, "view_overview"):
        return web.json_response({"ok": False})
    data = await request.json()
    rule_id = data.get("id","")
    try:
        conn = db.get_conn()
        conn.execute("DELETE FROM automation_rules WHERE id=?", (rule_id,))
        conn.commit(); conn.close()
        return web.json_response({"ok": True})
    except Exception as e:
        return web.json_response({"ok": False, "msg": str(e)})

async def check_automations(chat_id: int, uid: int, trigger: str, value) -> list:
    """Проверяет правила и возвращает список действий для выполнения.
    Вызывать из bot.py: actions = await dashboard.check_automations(cid, uid, 'warn_count', count)
    """
    _automations_init()
    results = []
    try:
        conn = db.get_conn()
        rules = [dict(r) for r in conn.execute(
            "SELECT * FROM automation_rules WHERE enabled=1 AND trigger=? AND (chat_id='all' OR chat_id=?)",
            (trigger, str(chat_id))
        ).fetchall()]
        conn.close()
        for r in rules:
            op  = r.get("condition_op",">=")
            val_str = r.get("condition_val","0")
            try:
                val_num = float(val_str)
                v       = float(value)
                match = (
                    (op == ">=" and v >= val_num) or
                    (op == "<=" and v <= val_num) or
                    (op == ">"  and v  > val_num) or
                    (op == "<"  and v  < val_num) or
                    (op == "==" and v == val_num) or
                    (op == "contains" and str(val_str).lower() in str(value).lower()) or
                    (op == "true")
                )
            except:
                match = (op == "true") or (str(val_str).lower() in str(value).lower())
            if match:
                results.append({"rule_id": r["id"], "rule_name": r["name"], "action": r["action"]})
                # Лог
                conn2 = db.get_conn()
                conn2.execute("INSERT INTO automation_log (rule_id,rule_name,chat_id,uid,result) VALUES (?,?,?,?,?)",
                              (r["id"], r["name"], str(chat_id), str(uid), r["action"]))
                conn2.execute("UPDATE automation_rules SET fires=fires+1 WHERE id=?", (r["id"],))
                conn2.commit(); conn2.close()
    except: pass
    return results


# ══════════════════════════════════════════════════════════════════
#  🤖 ПОЛНОЕ УПРАВЛЕНИЕ БОТОМ — СИНХРОНИЗАЦИЯ
# ══════════════════════════════════════════════════════════════════
#
#  Страница /dashboard/bot_control — единый центр управления:
#  - Статус бота (онлайн/оффлайн, аптайм, чаты)
#  - Прямые команды всем чатам или конкретному
#  - Управление настройками бота (антимат, автокик, slowmode)
#  - Массовые действия (рассылка, SOS, разлокдаун)
#  - Просмотр активных мутов/банов
#  - Синхронизация данных бот ↔ дашборд

async def handle_bot_control(request: web.Request):
    import traceback as _bot_tb
    try:
     return await _handle_bot_control_inner(request)
    except Exception as _bot_err:
        return web.Response(
            text=f"<pre style='color:red;padding:20px'>BOT_CONTROL 500:\n{_bot_tb.format_exc()}</pre>",
            content_type="text/html", status=500)

async def _handle_bot_control_inner(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    result_html = ""

    if request.method == "POST":
        data = await request.post()
        action = data.get("action","")
        chat_id_str = data.get("chat_id","")
        bot_inst = _bot_instance
        sess_uid = sess.get("uid",0) if sess else 0

        try:
            chat_id = int(chat_id_str) if chat_id_str else 0
        except:
            chat_id = 0

        if not bot_inst:
            result_html = '<div class="alert alert-danger">🤖 Бот не подключён к дашборду</div>'
        else:
            try:
                from aiogram.types import ChatPermissions
                
                msg = ""
                _adm_name = sess.get("name","Администратор") if sess else "Администратор"

                async def _notify(cid, text):
                    """Отправляет уведомление в чат от имени администрации."""
                    try:
                        n = await bot_inst.send_message(cid,
                            f"<b>🛡 Администрация</b> | {text}",
                            parse_mode="HTML")
                        # Автоудаление через 30 сек
                        import asyncio as _al
                        async def _del():
                            await _al.sleep(30)
                            try: await bot_inst.delete_message(cid, n.message_id)
                            except: pass
                        _al.create_task(_del())
                    except: pass

                if action == "lockdown_all":
                    locked = 0
                    chats = await db.get_all_chats()
                    for c in chats:
                        try:
                            await bot_inst.set_chat_permissions(c["cid"],
                                ChatPermissions(can_send_messages=False))
                            locked += 1
                        except: pass
                    msg = f"🔒 Локдаун применён к {locked} чатам"
                    _log_admin_db(sess_uid, "BOT_LOCKDOWN_ALL", msg)
                    chats2 = await db.get_all_chats()
                    for c2 in chats2:
                        await _notify(c2["cid"], f"🔒 Чат заблокирован администратором <b>{_adm_name}</b>")

                elif action == "unlock_all":
                    unlocked = 0
                    chats = await db.get_all_chats()
                    for c in chats:
                        try:
                            await bot_inst.set_chat_permissions(c["cid"],
                                ChatPermissions(can_send_messages=True,
                                    can_send_media_messages=True, can_send_polls=True,
                                    can_send_other_messages=True, can_add_web_page_previews=True))
                            unlocked += 1
                        except: pass
                    msg = f"🔓 Разлокдаун {unlocked} чатов"
                    _log_admin_db(sess_uid, "BOT_UNLOCK_ALL", msg)
                    chats3 = await db.get_all_chats()
                    for c3 in chats3:
                        await _notify(c3["cid"], f"🔓 Чат разблокирован администратором <b>{_adm_name}</b>")

                elif action == "broadcast" and data.get("broadcast_text"):
                    text = data.get("broadcast_text","").strip()
                    sent = 0
                    chats = await db.get_all_chats()
                    for c in chats:
                        try:
                            await bot_inst.send_message(c["cid"],
                                f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{text}\n\n— Администрация",
                                parse_mode="HTML")
                            sent += 1
                            import asyncio
                            await asyncio.sleep(0.05)
                        except: pass
                    msg = f"📢 Рассылка: {sent} чатов"
                    _log_admin_db(sess_uid, "BOT_BROADCAST", text[:50])

                elif action == "send_to_chat" and chat_id and data.get("msg_text"):
                    text = data.get("msg_text","").strip()
                    await bot_inst.send_message(chat_id, text, parse_mode="HTML")
                    msg = f"✉️ Сообщение отправлено в {chat_id}"
                    _log_admin_db(sess_uid, "BOT_SEND_MSG", f"→{chat_id}: {text[:40]}")

                elif action == "lockdown_chat" and chat_id:
                    await bot_inst.set_chat_permissions(chat_id,
                        ChatPermissions(can_send_messages=False))
                    msg = f"🔒 Чат {chat_id} заблокирован"
                    _log_admin_db(sess_uid, "BOT_LOCK_CHAT", str(chat_id))
                    await _notify(chat_id, f"🔒 Чат заблокирован администратором <b>{_adm_name}</b>")

                elif action == "unlock_chat" and chat_id:
                    await bot_inst.set_chat_permissions(chat_id,
                        ChatPermissions(can_send_messages=True,
                            can_send_media_messages=True, can_send_polls=True,
                            can_send_other_messages=True, can_add_web_page_previews=True))
                    msg = f"🔓 Чат {chat_id} разблокирован"
                    _log_admin_db(sess_uid, "BOT_UNLOCK_CHAT", str(chat_id))
                    await _notify(chat_id, f"🔓 Чат открыт администратором <b>{_adm_name}</b>")

                elif action == "slowmode" and chat_id:
                    delay = int(data.get("slowmode_val","30") or 30)
                    _bot_token = os.getenv("BOT_TOKEN","")
                    import aiohttp as _ah
                    async with _ah.ClientSession() as _s:
                        _r = await _s.post(
                            f"https://api.telegram.org/bot{_bot_token}/setChatSlowModeDelay",
                            json={"chat_id": chat_id, "slow_mode_delay": delay}
                        )
                        _rd = await _r.json()
                    if not _rd.get("ok"):
                        raise Exception(_rd.get("description","Telegram error"))
                    msg = f"🐢 Slowmode {delay}с в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_SLOWMODE", f"{chat_id}: {delay}s")
                    await _notify(chat_id, f"🐢 Slowmode {'включён: ' + str(delay) + 'с' if delay else 'отключён'} — <b>{_adm_name}</b>")

                elif action == "pin_msg" and chat_id and data.get("msg_id"):
                    msg_id = int(data.get("msg_id",0))
                    await bot_inst.pin_chat_message(chat_id, msg_id)
                    msg = f"📌 Сообщение {msg_id} закреплено в {chat_id}"

                elif action == "unpin_all" and chat_id:
                    await bot_inst.unpin_all_chat_messages(chat_id)
                    msg = f"📌 Все сообщения откреплены в {chat_id}"

                elif action == "ban_user" and chat_id and data.get("target_uid"):
                    target_uid = int(data.get("target_uid",0))
                    reason = data.get("ban_reason","Нарушение правил")
                    await bot_inst.ban_chat_member(chat_id, target_uid)
                    msg = f"🔨 Бан ID{target_uid} в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_BAN", f"{chat_id}:{target_uid} {reason}")
                    await _notify(chat_id, f"🔨 Пользователь <code>{target_uid}</code> заблокирован — <b>{_adm_name}</b>\nПричина: {reason}")

                elif action == "unban_user" and chat_id and data.get("target_uid"):
                    target_uid = int(data.get("target_uid",0))
                    await bot_inst.unban_chat_member(chat_id, target_uid, only_if_banned=True)
                    msg = f"🕊 Разбан ID{target_uid} в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_UNBAN", f"{chat_id}:{target_uid}")
                    await _notify(chat_id, f"🕊 Пользователь <code>{target_uid}</code> разблокирован — <b>{_adm_name}</b>")

                elif action == "mute_user" and chat_id and data.get("target_uid"):
                    from datetime import timedelta
                    target_uid = int(data.get("target_uid",0))
                    mins = int(data.get("mute_mins","60") or 60)
                    await bot_inst.restrict_chat_member(chat_id, target_uid,
                        ChatPermissions(can_send_messages=False),
                        until_date=timedelta(minutes=mins))
                    msg = f"🔇 Мут ID{target_uid} на {mins}мин в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_MUTE", f"{chat_id}:{target_uid} {mins}m")
                    await _notify(chat_id, f"🔇 Пользователь <code>{target_uid}</code> замучен на <b>{mins} мин</b> — {_adm_name}")

                elif action == "kick_user" and chat_id and data.get("target_uid"):
                    target_uid = int(data.get("target_uid",0))
                    await bot_inst.ban_chat_member(chat_id, target_uid)
                    await bot_inst.unban_chat_member(chat_id, target_uid)
                    msg = f"👟 Кик ID{target_uid} из чата {chat_id}"
                    _log_admin_db(sess_uid, "BOT_KICK", f"{chat_id}:{target_uid}")
                    await _notify(chat_id, f"👟 Пользователь <code>{target_uid}</code> исключён — <b>{_adm_name}</b>")

                elif action == "set_title" and chat_id and data.get("new_title"):
                    title = data.get("new_title","").strip()
                    await bot_inst.set_chat_title(chat_id, title)
                    msg = f"✏️ Название чата {chat_id} изменено на «{title}»"
                    await _notify(chat_id, f"✏️ Название чата изменено на <b>{title}</b> — {_adm_name}")

                elif action == "set_description" and chat_id and data.get("new_desc"):
                    desc = data.get("new_desc","").strip()
                    await bot_inst.set_chat_description(chat_id, desc)
                    msg = f"📝 Описание чата {chat_id} обновлено"

                elif action == "bot_commands_set":
                    from aiogram.types import BotCommand
                    commands = [
                        BotCommand(command="start",   description="Начало работы"),
                        BotCommand(command="help",    description="Помощь"),
                        BotCommand(command="profile", description="Мой профиль"),
                        BotCommand(command="top",     description="Топ активных"),
                        BotCommand(command="daily",   description="Ежедневный бонус"),
                        BotCommand(command="rules",   description="Правила чата"),
                        BotCommand(command="report",  description="Пожаловаться"),
                        BotCommand(command="ticket",  description="Открыть тикет"),
                    ]
                    await bot_inst.set_my_commands(commands=commands)
                    msg = "✅ Команды бота обновлены"

                elif action == "sync_chats":
                    # Синхронизируем список чатов из shared в БД
                    synced = 0
                    try:
                        from shared import online_users
                        # Если есть known_chats в боте — синхронизируем
                        conn = db.get_conn()
                        rows = conn.execute("SELECT cid,title FROM known_chats").fetchall()
                        for r in rows:
                            conn.execute("INSERT OR REPLACE INTO chats (cid,title) VALUES (?,?)",
                                        (r[0], r[1]))
                            synced += 1
                        conn.commit(); conn.close()
                    except: pass
                    msg = f"🔄 Синхронизировано {synced} чатов"
                else:
                    msg = "⚠️ Действие не выполнено — проверь параметры"

                color = "var(--success)" if not msg.startswith("⚠️") else "var(--warning)"
                result_html = (
                    f'<div style="background:var(--bg3);border-left:3px solid {color};'
                    f'padding:12px 16px;border-radius:0 8px 8px 0;margin-bottom:20px;">'
                    f'{msg}</div>'
                )
            except Exception as e:
                import traceback as _tbc
                result_html = (
                    f'<div class="alert alert-danger" style="white-space:pre-wrap;">'
                    f'❌ {e}\n\n{_tbc.format_exc()}</div>'
                )

    # Статус бота
    bot_inst = _bot_instance
    bot_info_html = ""
    if bot_inst:
        try:
            me = await bot_inst.get_me()
            bot_info_html = (
                f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">'
                f'<div style="width:48px;height:48px;border-radius:50%;background:var(--accent-glow);'
                f'display:flex;align-items:center;justify-content:center;font-size:22px;">🤖</div>'
                f'<div>'
                f'<div style="font-weight:700;">{me.full_name}</div>'
                f'<div style="font-size:13px;color:var(--text2);">@{me.username} · ID {me.id}</div>'
                f'<div style="font-size:12px;color:var(--success);margin-top:2px;">🟢 Онлайн</div>'
                f'</div>'
                f'</div>'
            )
        except:
            bot_info_html = '<div style="color:var(--danger);">❌ Не удалось получить данные бота</div>'
    else:
        bot_info_html = (
            '<div style="color:var(--danger);">'
            '❌ Бот не подключён. Убедись что вызван <code>dashboard.set_bot(bot, ADMIN_IDS)</code>'
            '</div>'
        )

    # Список чатов
    chats = []
    try:
        chats = [dict(r) for r in await db.get_all_chats()]
    except: pass
    chat_opts = "".join(f'<option value="{c["cid"]}">{c["title"][:30]} ({c["cid"]})</option>' for c in chats)

    # Последние действия
    recent_actions = []
    try:
        conn = db.get_conn()
        recent_actions = [dict(r) for r in conn.execute(
            "SELECT action,details,created_at FROM dashboard_admin_log "
            "WHERE action LIKE 'BOT_%' ORDER BY created_at DESC LIMIT 20"
        ).fetchall()]
        conn.close()
    except: pass
    actions_html = ""
    for a in recent_actions:
        actions_html += (
            f'<div style="padding:7px 0;border-bottom:1px solid var(--border);font-size:12px;">'
            f'<span style="font-weight:600;">{a.get("action","").replace("BOT_","")}</span> '
            f'— {(a.get("details","") or "")[:60]} '
            f'<span style="color:var(--text2);">{str(a.get("created_at",""))[:16]}</span>'
            f'</div>'
        )
    if not actions_html:
        actions_html = '<div style="color:var(--text2);font-size:12px;padding:8px 0;">Нет истории</div>'

    body = navbar(sess, "bot_control") + f"""
    <div class="container">
      <div class="page-title">🤖 Управление ботом</div>
      {result_html}

      <div class="cards" style="grid-template-columns:repeat(4,1fr);margin-bottom:24px;">
        <div class="card"><div class="card-icon">🤖</div><div class="card-label">Статус</div>
          <div class="card-value" style="color:{'var(--success)' if bot_inst else 'var(--danger)'};">
          {'Онлайн' if bot_inst else 'Офлайн'}</div></div>
        <div class="card"><div class="card-icon">💬</div><div class="card-label">Чатов</div>
          <div class="card-value">{len(chats)}</div></div>
        <div class="card"><div class="card-icon">⚡</div><div class="card-label">Действий</div>
          <div class="card-value">{len(recent_actions)}</div></div>
        <div class="card"><div class="card-icon">🔄</div><div class="card-label">Синхронизация</div>
          <div class="card-value" style="font-size:12px;">Активна</div></div>
      </div>

      <div class="grid-2" style="gap:24px;align-items:start;">
        <div>
          <div class="section">
            <div class="section-header">🤖 Информация о боте</div>
            <div style="padding:12px 16px;">{bot_info_html}</div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">🌍 Массовые действия</div>
            <div class="section-body">
              <div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px;">
                <form method="POST" style="display:inline;">
                  <input type="hidden" name="action" value="lockdown_all">
                  <button class="btn btn-sm" style="background:rgba(239,68,68,.1);color:var(--danger);"
                    onclick="return confirm('Заблокировать ВСЕ чаты?')">🔒 Локдаун всех</button>
                </form>
                <form method="POST" style="display:inline;">
                  <input type="hidden" name="action" value="unlock_all">
                  <button class="btn btn-sm btn-ghost"
                    onclick="return confirm('Разблокировать все чаты?')">🔓 Разлокдаун</button>
                </form>
                <form method="POST" style="display:inline;">
                  <input type="hidden" name="action" value="bot_commands_set">
                  <button class="btn btn-sm btn-ghost">⚙️ Обновить команды</button>
                </form>
                <form method="POST" style="display:inline;">
                  <input type="hidden" name="action" value="sync_chats">
                  <button class="btn btn-sm btn-ghost">🔄 Синхронизация</button>
                </form>
              </div>

              <form method="POST">
                <input type="hidden" name="action" value="broadcast">
                <label>Рассылка во все чаты</label>
                <textarea class="form-control" name="broadcast_text" rows="3"
                  placeholder="Текст объявления..." style="margin-bottom:8px;"></textarea>
                <button class="btn btn-primary btn-sm" type="submit"
                  onclick="return confirm('Отправить во все чаты?')">📢 Рассылка</button>
              </form>
            </div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">📝 История действий</div>
            <div style="padding:0 16px 8px;">{actions_html}</div>
          </div>
        </div>

        <div>
          <div class="section">
            <div class="section-header">🎯 Действия в конкретном чате</div>
            <div class="section-body">
              <div class="form-group">
                <label>Выбери чат</label>
                <select class="form-control" id="globalChat" onchange="setChat(this.value)">
                  <option value="">— выбери чат —</option>
                  {chat_opts}
                </select>
              </div>

              <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:16px;">
                <button class="btn btn-sm btn-ghost" onclick="submitAction('lockdown_chat')">🔒 Локдаун</button>
                <button class="btn btn-sm btn-ghost" onclick="submitAction('unlock_chat')">🔓 Разлок</button>
                <button class="btn btn-sm btn-ghost" onclick="submitAction('unpin_all')">📌 Открепить всё</button>
              </div>

              <form method="POST" id="chatActionForm">
                <input type="hidden" name="chat_id" id="fChatId">
                <input type="hidden" name="action" id="fAction">

                <div class="form-group">
                  <label>Slowmode (секунд)</label>
                  <div style="display:flex;gap:8px;">
                    <input class="form-control" name="slowmode_val" type="number" value="30" style="flex:1;">
                    <button class="btn btn-primary btn-sm" type="button"
                      onclick="submitAction('slowmode')">Применить</button>
                  </div>
                </div>

                <div class="form-group">
                  <label>Отправить сообщение в чат</label>
                  <textarea class="form-control" name="msg_text" rows="2"
                    placeholder="HTML поддерживается..."></textarea>
                  <button class="btn btn-primary btn-sm" type="button" style="margin-top:6px;"
                    onclick="submitAction('send_to_chat')">✉️ Отправить</button>
                </div>
              </form>
            </div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">👤 Действия над пользователем</div>
            <div class="section-body">
              <form method="POST">
                <input type="hidden" name="chat_id" id="userChatId">
                <div class="form-group">
                  <label>Чат</label>
                  <select class="form-control" name="chat_id" id="userChat">
                    <option value="">— выбери чат —</option>
                    {chat_opts}
                  </select>
                </div>
                <div class="form-group">
                  <label>Telegram ID пользователя</label>
                  <input class="form-control" name="target_uid" type="number" placeholder="123456789">
                </div>
                <div class="form-group">
                  <label>Минуты мута</label>
                  <input class="form-control" name="mute_mins" type="number" value="60">
                </div>
                <div class="form-group">
                  <label>Причина бана</label>
                  <input class="form-control" name="ban_reason" value="Нарушение правил">
                </div>
                <div style="display:flex;flex-wrap:wrap;gap:6px;">
                  <button class="btn btn-sm" style="background:rgba(239,68,68,.1);color:var(--danger);"
                    name="action" value="ban_user" type="submit"
                    onclick="return confirm('Забанить?')">🔨 Бан</button>
                  <button class="btn btn-sm btn-ghost" name="action" value="unban_user" type="submit">🕊 Разбан</button>
                  <button class="btn btn-sm btn-ghost" name="action" value="mute_user" type="submit">🔇 Мут</button>
                  <button class="btn btn-sm btn-ghost" name="action" value="kick_user" type="submit"
                    onclick="return confirm('Кикнуть?')">👟 Кик</button>
                </div>
              </form>
            </div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">✏️ Настройки чата</div>
            <div class="section-body">
              <form method="POST">
                <div class="form-group">
                  <label>Чат</label>
                  <select class="form-control" name="chat_id">
                    <option value="">— выбери чат —</option>
                    {chat_opts}
                  </select>
                </div>
                <div class="form-group">
                  <label>Новое название</label>
                  <div style="display:flex;gap:8px;">
                    <input class="form-control" name="new_title" placeholder="Название чата" style="flex:1;">
                    <button class="btn btn-primary btn-sm" name="action" value="set_title" type="submit">✓</button>
                  </div>
                </div>
                <div class="form-group">
                  <label>Описание</label>
                  <div style="display:flex;gap:8px;">
                    <input class="form-control" name="new_desc" placeholder="Описание чата" style="flex:1;">
                    <button class="btn btn-primary btn-sm" name="action" value="set_description" type="submit">✓</button>
                  </div>
                </div>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>

    <script>
    function setChat(val) {{
      document.getElementById("fChatId").value = val;
    }}
    function submitAction(action) {{
      var chatVal = document.getElementById("globalChat").value;
      if (!chatVal && action !== "lockdown_all" && action !== "unlock_all" &&
          action !== "broadcast" && action !== "bot_commands_set" && action !== "sync_chats") {{
        alert("Выбери чат!");
        return;
      }}
      document.getElementById("fChatId").value = chatVal;
      document.getElementById("fAction").value = action;
      document.getElementById("chatActionForm").submit();
    }}
    </script>
    """ + close_main()
    try:
        rendered = page(body)
    except Exception as _be:
        import traceback as _tb
        return web.Response(text=f"<pre style='color:red'>BOT_CTRL ERROR:\n{_tb.format_exc()}</pre>",
                            content_type="text/html")
    return web.Response(text=rendered, content_type="text/html")

handle_bot_control = require_auth("view_overview")(handle_bot_control)

async def api_bot_action(request: web.Request):
    """API для быстрых действий ботом (используется JS)."""
    token = request.cookies.get("dsess_token","")
    if not _has_perm(token, "view_overview"):
        return web.json_response({"ok": False, "msg": "Нет доступа"})
    bot_inst = _bot_instance
    if not bot_inst:
        return web.json_response({"ok": False, "msg": "Бот не подключён"})
    try:
        data = await request.json()
        action   = data.get("action","")
        chat_id  = int(data.get("chat_id",0))
        target   = int(data.get("target",0))
        arg      = data.get("arg","")

        from aiogram.types import ChatPermissions
        from datetime import timedelta

        if action == "send_message" and chat_id and arg:
            await bot_inst.send_message(chat_id, arg, parse_mode="HTML")
            return web.json_response({"ok": True, "msg": "Отправлено"})
        elif action == "lock" and chat_id:
            await bot_inst.set_chat_permissions(chat_id, ChatPermissions(can_send_messages=False))
            return web.json_response({"ok": True, "msg": f"Чат {chat_id} заблокирован"})
        elif action == "unlock" and chat_id:
            await bot_inst.set_chat_permissions(chat_id,
                ChatPermissions(can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
            return web.json_response({"ok": True, "msg": f"Чат {chat_id} разблокирован"})
        elif action == "ban" and chat_id and target:
            await bot_inst.ban_chat_member(chat_id, target)
            return web.json_response({"ok": True, "msg": f"Забанен ID{target}"})
        elif action == "unban" and chat_id and target:
            await bot_inst.unban_chat_member(chat_id, target, only_if_banned=True)
            return web.json_response({"ok": True, "msg": f"Разбанен ID{target}"})
        elif action == "mute" and chat_id and target:
            mins = int(arg) if arg and str(arg).isdigit() else 60
            await bot_inst.restrict_chat_member(chat_id, target,
                ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            return web.json_response({"ok": True, "msg": f"Мут ID{target} на {mins}м"})
        elif action == "kick" and chat_id and target:
            await bot_inst.ban_chat_member(chat_id, target)
            await bot_inst.unban_chat_member(chat_id, target)
            return web.json_response({"ok": True, "msg": f"Кикнут ID{target}"})
        else:
            return web.json_response({"ok": False, "msg": f"Неизвестное действие: {action}"})
    except Exception as e:
        return web.json_response({"ok": False, "msg": str(e)})

async def api_bot_status(request: web.Request):
    """Возвращает текущий статус бота."""
    token = request.cookies.get("dsess_token","")
    if not _has_perm(token, "view_overview"):
        return web.json_response({"online": False})
    bot_inst = _bot_instance
    if not bot_inst:
        return web.json_response({"online": False, "name": "—", "username": "—"})
    try:
        me = await bot_inst.get_me()
        chats = await db.get_all_chats()
        return web.json_response({
            "online": True,
            "name": me.full_name,
            "username": me.username,
            "id": me.id,
            "chats_count": len(chats),
        })
    except Exception as e:
        return web.json_response({"online": False, "error": str(e)})

async def api_bot_chats(request: web.Request):
    """Список чатов бота."""
    token = request.cookies.get("dsess_token","")
    if not _has_perm(token, "view_overview"):
        return web.json_response({"chats": []})
    try:
        chats = [dict(r) for r in await db.get_all_chats()]
        return web.json_response({"chats": chats})
    except Exception as e:
        return web.json_response({"chats": [], "error": str(e)})

# Запускаем фоновые задачи авто-отчётов
import asyncio as _asyncio_bg
_auto_reports_task = None

def _start_background_tasks():
    """Вызывается из start_dashboard для запуска фоновых задач."""
    global _auto_reports_task
    try:
        loop = _asyncio_bg.get_event_loop()
        _auto_reports_task = loop.create_task(_auto_reports_loop())
    except: pass



# ══════════════════════════════════════════════════════════════════
#  🚨 СИСТЕМА ИНЦИДЕНТОВ
# ══════════════════════════════════════════════════════════════════

def _incidents_init():
    try:
        conn = db.get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS incidents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT    NOT NULL,
            description TEXT    DEFAULT '',
            severity    TEXT    DEFAULT 'low',
            status      TEXT    DEFAULT 'open',
            assigned_to TEXT    DEFAULT '',
            assigned_uid INTEGER DEFAULT 0,
            created_by  TEXT    DEFAULT '',
            created_uid INTEGER DEFAULT 0,
            postmortem  TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT (datetime('now')),
            updated_at  TEXT    DEFAULT (datetime('now')),
            resolved_at TEXT    DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS incident_timeline (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id INTEGER NOT NULL,
            author      TEXT    DEFAULT '',
            author_uid  INTEGER DEFAULT 0,
            note        TEXT    NOT NULL,
            created_at  TEXT    DEFAULT (datetime('now')))""")
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f"_incidents_init: {e}")

_INCIDENT_SEVERITY = {
    "low":      ("🟢", "Низкий",    "#22c55e"),
    "medium":   ("🟡", "Средний",   "#f59e0b"),
    "high":     ("🔴", "Высокий",   "#ef4444"),
    "critical": ("💀", "Критичный", "#dc2626"),
}
_INCIDENT_STATUS = {
    "open":          ("🔴", "Открыт"),
    "investigating": ("🟡", "Расследование"),
    "resolved":      ("✅", "Решён"),
    "closed":        ("⚫", "Закрыт"),
}

async def handle_incidents(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    _incidents_init()
    result_html = ""

    if request.method == "POST":
        data    = await request.post()
        action  = data.get("action", "")
        inc_id  = int(data.get("inc_id", 0) or 0)
        uid     = sess.get("uid", 0) if sess else 0
        uname   = sess.get("name", "?") if sess else "?"

        if action == "create":
            title    = (data.get("title") or "").strip()
            desc     = (data.get("description") or "").strip()
            severity = data.get("severity", "low")
            assigned = (data.get("assigned") or "").strip()
            if title:
                conn = db.get_conn()
                conn.execute(
                    "INSERT INTO incidents (title,description,severity,assigned_to,created_by,created_uid) VALUES (?,?,?,?,?,?)",
                    (title, desc, severity, assigned, uname, uid)
                )
                new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                # Первая запись в таймлайн
                conn.execute(
                    "INSERT INTO incident_timeline (incident_id,author,author_uid,note) VALUES (?,?,?,?)",
                    (new_id, uname, uid, f"Инцидент создан. Severity: {severity}. {desc[:100]}")
                )
                conn.commit(); conn.close()
                _log_admin_db(uid, "INCIDENT_CREATE", f"#{new_id}: {title}")
                # Алерт если критичный
                if severity == "critical" and _bot:
                    try:
                        import asyncio as _aio
                        _aio.ensure_future(_bot.send_message(
                            OWNER_TG_ID,
                            f"💀 <b>КРИТИЧНЫЙ ИНЦИДЕНТ #{new_id}</b>\n\n"
                            f"<b>{title}</b>\n{desc[:200]}\n\n"
                            f"👤 Создал: {uname}\n"
                            f"🔗 /dashboard/incidents",
                            parse_mode="HTML"
                        ))
                    except: pass
                result_html = f'<div class="alert alert-success">✅ Инцидент #{new_id} создан</div>'

        elif action == "add_note" and inc_id:
            note = (data.get("note") or "").strip()
            if note:
                conn = db.get_conn()
                conn.execute(
                    "INSERT INTO incident_timeline (incident_id,author,author_uid,note) VALUES (?,?,?,?)",
                    (inc_id, uname, uid, note)
                )
                conn.execute("UPDATE incidents SET updated_at=datetime('now') WHERE id=?", (inc_id,))
                conn.commit(); conn.close()
                _log_admin_db(uid, "INCIDENT_NOTE", f"#{inc_id}: {note[:50]}")

        elif action == "update_status" and inc_id:
            new_status = data.get("new_status", "")
            postmortem = (data.get("postmortem") or "").strip()
            if new_status in _INCIDENT_STATUS:
                conn = db.get_conn()
                resolved_at = "datetime('now')" if new_status == "resolved" else "NULL"
                conn.execute(
                    f"UPDATE incidents SET status=?,updated_at=datetime('now')"
                    f"{',resolved_at=datetime('+chr(39)+'now'+chr(39)+')' if new_status=='resolved' else ''}"
                    f"{',postmortem=?' if postmortem else ''} WHERE id=?",
                    ([new_status] + ([postmortem] if postmortem else []) + [inc_id])
                )
                conn.execute(
                    "INSERT INTO incident_timeline (incident_id,author,author_uid,note) VALUES (?,?,?,?)",
                    (inc_id, uname, uid, f"Статус изменён → {_INCIDENT_STATUS[new_status][1]}" + (f". Post-mortem: {postmortem[:100]}" if postmortem else ""))
                )
                conn.commit(); conn.close()
                _log_admin_db(uid, "INCIDENT_STATUS", f"#{inc_id}→{new_status}")
                # Алерт при решении
                if new_status == "resolved" and _bot:
                    try:
                        import asyncio as _aio
                        inc = db.get_conn().execute("SELECT title,severity FROM incidents WHERE id=?", (inc_id,)).fetchone()
                        if inc:
                            _aio.ensure_future(_bot.send_message(
                                OWNER_TG_ID,
                                f"✅ <b>Инцидент #{inc_id} решён</b>\n<b>{inc['title']}</b>\n👤 {uname}",
                                parse_mode="HTML"
                            ))
                    except: pass

        elif action == "delete" and inc_id:
            conn = db.get_conn()
            conn.execute("DELETE FROM incident_timeline WHERE incident_id=?", (inc_id,))
            conn.execute("DELETE FROM incidents WHERE id=?", (inc_id,))
            conn.commit(); conn.close()
            _log_admin_db(uid, "INCIDENT_DELETE", f"#{inc_id}")

    # Фильтр
    status_filter = request.rel_url.query.get("status", "open")
    detail_id     = int(request.rel_url.query.get("id", 0) or 0)

    conn = db.get_conn()
    if detail_id:
        # Детальный вид инцидента
        inc = conn.execute("SELECT * FROM incidents WHERE id=?", (detail_id,)).fetchone()
        timeline = [dict(r) for r in conn.execute(
            "SELECT * FROM incident_timeline WHERE incident_id=? ORDER BY created_at ASC",
            (detail_id,)
        ).fetchall()]
        conn.close()

        if not inc:
            raise web.HTTPFound("/dashboard/incidents")

        inc = dict(inc)
        sev_icon, sev_label, sev_color = _INCIDENT_SEVERITY.get(inc["severity"], ("🟢","—","#22c55e"))
        st_icon, st_label = _INCIDENT_STATUS.get(inc["status"], ("🔴","Открыт"))

        tl_html = ""
        for t in timeline:
            tl_html += (
                f'<div style="display:flex;gap:12px;padding:10px 0;border-bottom:1px solid var(--border);">'
                f'<div style="width:32px;height:32px;border-radius:50%;background:var(--bg3);'
                f'display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;'
                f'color:var(--accent);flex-shrink:0;">{t["author"][:1].upper()}</div>'
                f'<div style="flex:1;">'
                f'<div style="font-size:12px;font-weight:600;">{t["author"]} '
                f'<span style="color:var(--text2);font-weight:400;">{str(t["created_at"])[:16]}</span></div>'
                f'<div style="font-size:13px;margin-top:3px;">{t["note"]}</div>'
                f'</div></div>'
            )

        status_opts = "".join(
            f'<option value="{k}" {"selected" if k==inc["status"] else ""}>{v[0]} {v[1]}</option>'
            for k, v in _INCIDENT_STATUS.items()
        )

        body = navbar(sess, "incidents") + f"""
    <div class="container">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:24px;flex-wrap:wrap;">
        <a href="/dashboard/incidents" class="btn btn-ghost btn-sm">← Все инциденты</a>
        <span style="font-size:22px;font-weight:800;">#{inc["id"]} {inc["title"]}</span>
        <span class="badge" style="background:rgba({_hex_to_rgb(sev_color)},.15);color:{sev_color};">{sev_icon} {sev_label}</span>
        <span style="color:var(--text2);font-size:13px;">{st_icon} {st_label}</span>
        <span style="margin-left:auto;font-size:12px;color:var(--text2);">
          Создан: {str(inc["created_at"])[:16]} · {inc["created_by"]}
        </span>
      </div>

      {result_html}

      <div class="grid-2" style="gap:24px;align-items:start;">
        <div>
          <div class="section">
            <div class="section-header">📋 Описание</div>
            <div style="padding:16px;font-size:14px;line-height:1.7;">{inc["description"] or "—"}</div>
          </div>
          {"<div class='section' style='margin-top:20px;'><div class='section-header'>📝 Post-mortem</div><div style='padding:16px;font-size:14px;'>" + inc["postmortem"] + "</div></div>" if inc.get("postmortem") else ""}
          <div class="section" style="margin-top:20px;">
            <div class="section-header">⚙️ Управление</div>
            <div class="section-body">
              <form method="POST">
                <input type="hidden" name="action" value="update_status">
                <input type="hidden" name="inc_id" value="{inc["id"]}">
                <div class="form-group">
                  <label>Изменить статус</label>
                  <select class="form-control" name="new_status">{status_opts}</select>
                </div>
                <div class="form-group">
                  <label>Post-mortem (при решении)</label>
                  <textarea class="form-control" name="postmortem" rows="3"
                    placeholder="Что произошло, почему, как предотвратить...">{inc.get("postmortem","")}</textarea>
                </div>
                <button class="btn btn-primary" type="submit" style="width:100%;">💾 Сохранить</button>
              </form>
            </div>
          </div>
        </div>

        <div>
          <div class="section">
            <div class="section-header">📜 Таймлайн ({len(timeline)} записей)</div>
            <div style="padding:0 16px 8px;">{tl_html or "<div class='empty-state'>Нет записей</div>"}</div>
          </div>
          <div class="section" style="margin-top:20px;">
            <div class="section-header">➕ Добавить запись</div>
            <div class="section-body">
              <form method="POST">
                <input type="hidden" name="action" value="add_note">
                <input type="hidden" name="inc_id" value="{inc["id"]}">
                <textarea class="form-control" name="note" rows="3"
                  placeholder="Что происходит, что предпринято..." required></textarea>
                <button class="btn btn-primary" style="margin-top:8px;width:100%;" type="submit">➕ Добавить</button>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
    """ + close_main()
        return web.Response(text=page(body), content_type="text/html")

    # Список инцидентов
    if status_filter == "all":
        incidents = [dict(r) for r in conn.execute(
            "SELECT * FROM incidents ORDER BY created_at DESC"
        ).fetchall()]
    else:
        incidents = [dict(r) for r in conn.execute(
            "SELECT * FROM incidents WHERE status=? ORDER BY created_at DESC",
            (status_filter,)
        ).fetchall()]

    counts = {}
    for s in _INCIDENT_STATUS.keys():
        counts[s] = conn.execute("SELECT COUNT(*) FROM incidents WHERE status=?", (s,)).fetchone()[0]
    counts["all"] = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    conn.close()

    tabs_html = ""
    for s, (icon, label) in _INCIDENT_STATUS.items():
        active = "btn-primary" if status_filter == s else "btn-ghost"
        tabs_html += f'<a href="?status={s}" class="btn btn-sm {active}" style="margin-right:4px;">{icon} {label} ({counts.get(s,0)})</a>'
    tabs_html += f'<a href="?status=all" class="btn btn-sm {"btn-primary" if status_filter=="all" else "btn-ghost"}" style="margin-right:4px;">📋 Все ({counts["all"]})</a>'

    rows_html = ""
    for inc in incidents:
        sev_icon, sev_label, sev_color = _INCIDENT_SEVERITY.get(inc["severity"], ("🟢","—","#22c55e"))
        st_icon, st_label = _INCIDENT_STATUS.get(inc["status"], ("🔴","Открыт"))
        _inc_url = "/dashboard/incidents?id=" + str(inc["id"])
        _inc_desc = ("<div style=\"font-size:12px;color:var(--text2);margin-top:6px;\">" + inc["description"][:100] + "...</div>") if inc.get("description") else ""
        rows_html += (
            f"<div style='padding:16px;border-bottom:1px solid var(--border);cursor:pointer;' data-url='{_inc_url}' onclick='window.location=this.dataset.url'>"
            f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
            f"<div style='padding:16px;border-bottom:1px solid var(--border);cursor:pointer;'" 
            "onclick=\"location.href='" + _inc_url + "'\">"
            f'background:rgba({_hex_to_rgb(sev_color)},.15);color:{sev_color};">{sev_icon} {sev_label}</span>'
            f'<span style="font-size:11px;color:var(--text2);">{st_icon} {st_label}</span>'
            f'<span style="margin-left:auto;font-size:11px;color:var(--text2);">'
            f'{str(inc["created_at"])[:16]} · {inc["created_by"]}</span>'
            f'</div>'
            f'{_inc_desc}'
        )
    if not rows_html:
        rows_html = '<div class="empty-state">Инцидентов нет</div>'

    sev_opts = "".join(
        f'<option value="{k}">{v[0]} {v[1]}</option>'
        for k, v in _INCIDENT_SEVERITY.items()
    )

    # Получаем список модов для назначения
    admins = _get_all_admins()
    admin_opts = "".join(f'<option value="{a["name"]}">{a["name"]} (ранг {a["rank"]})</option>' for a in admins)

    body = navbar(sess, "incidents") + f"""
    <div class="container">
      <div class="page-title">🚨 Система инцидентов
        <span style="font-size:12px;color:var(--danger);font-weight:600;margin-left:auto;">
          {counts.get("open",0)} открытых · {counts.get("investigating",0)} расследуется
        </span>
      </div>

      {result_html}

      <div style="margin-bottom:20px;display:flex;gap:4px;flex-wrap:wrap;">{tabs_html}</div>

      <div class="grid-2" style="gap:24px;align-items:start;">
        <div>
          <div class="section">
            <div class="section-header">📋 Инциденты ({len(incidents)})</div>
            {rows_html}
          </div>
        </div>

        <div>
          <div class="section">
            <div class="section-header">➕ Новый инцидент</div>
            <div class="section-body">
              <form method="POST">
                <input type="hidden" name="action" value="create">
                <div class="form-group">
                  <label>Заголовок *</label>
                  <input class="form-control" name="title" placeholder="Что произошло?" required>
                </div>
                <div class="form-group">
                  <label>Описание</label>
                  <textarea class="form-control" name="description" rows="3"
                    placeholder="Детали инцидента..."></textarea>
                </div>
                <div class="form-group">
                  <label>Серьёзность</label>
                  <select class="form-control" name="severity">{sev_opts}</select>
                </div>
                <div class="form-group">
                  <label>Ответственный</label>
                  <select class="form-control" name="assigned">
                    <option value="">— не назначен —</option>
                    {admin_opts}
                  </select>
                </div>
                <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;">
                  🚨 Создать инцидент
                </button>
              </form>
            </div>
          </div>

          <div class="section" style="margin-top:20px;">
            <div class="section-header">ℹ️ Severity уровни</div>
            <div style="padding:12px 16px;">
              {"".join(f'<div style="padding:5px 0;font-size:13px;">{icon} <b style="color:{color};">{label}</b></div>' for k,(icon,label,color) in _INCIDENT_SEVERITY.items())}
            </div>
          </div>
        </div>
      </div>
    </div>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_incidents = require_auth("view_overview")(handle_incidents)


# ══════════════════════════════════════════════════════════════════
#  🎮 LIVE COMMAND CENTER
# ══════════════════════════════════════════════════════════════════
#  Страница /dashboard/command_center
#  - Live-лента событий (SSE): сообщения, варны, баны, жалобы
#  - Мини-терминал: любые команды боту прямо в браузере
#  - Карточки всех чатов с live-счётчиками
#  - Быстрые действия по любому юзеру (ID/username)
#  - Глобальный оверлей статуса: онлайн, алерты, дежурные
#  - История команд в сессии

_cc_event_log: list = []   # последние 200 событий для новых клиентов
_CC_MAX_EVENTS = 200

def _cc_push_event(etype: str, text: str, chat: str = "", uid: int = 0):
    """Добавляет событие в лог и рассылает по SSE."""
    import json, time as _t, datetime as _dt
    event = {
        "type":  etype,
        "text":  text,
        "chat":  chat,
        "uid":   uid,
        "ts":    _t.time(),
        "time":  _dt.datetime.now().strftime("%H:%M:%S"),
    }
    _cc_event_log.append(event)
    if len(_cc_event_log) > _CC_MAX_EVENTS:
        _cc_event_log.pop(0)
    # Рассылаем через shared SSE
    try:
        payload = json.dumps(event, ensure_ascii=False)
        for q in list(shared.sse_clients):
            try:
                q.put_nowait(f"cc:{payload}")
            except:
                pass
    except:
        pass


async def handle_command_center(request: web.Request):
    import json, time as _t, traceback as _tb
    try:
        return await _handle_command_center_inner(request)
    except Exception as e:
        return web.Response(
            text=f"<pre style='color:red;padding:20px'>COMMAND CENTER ERROR:\n{_tb.format_exc()}</pre>",
            content_type="text/html"
        )


async def _handle_command_center_inner(request: web.Request):
    import json, time as _t
    sess = _get_session(request)
    _track_session(request)

    result = {"ok": False, "msg": ""}

    # ── POST: выполнить команду ──────────────────────────────────
    if request.method == "POST":
        data    = await request.post()
        cmd     = (data.get("cmd") or "").strip()
        chat_id = int(data.get("chat_id") or 0)
        target  = (data.get("target") or "").strip()
        arg     = (data.get("arg") or "").strip()
        uid_s   = sess.get("uid", 0) if sess else 0
        uname   = sess.get("name", "?") if sess else "?"

        if not _bot:
            result = {"ok": False, "msg": "🤖 Бот не подключён"}
        elif not chat_id and cmd not in ("status", "broadcast", "sos_on", "sos_off"):
            result = {"ok": False, "msg": "⚠️ Выбери чат"}
        else:
            try:
                from aiogram.types import ChatPermissions
                from datetime import timedelta

                # Парсим target → int uid если возможно
                target_uid = 0
                if target:
                    try:
                        target_uid = int(target)
                    except:
                        pass

                # ── Команды ─────────────────────────────────────
                if cmd == "lock":
                    await _bot.set_chat_permissions(chat_id, ChatPermissions(can_send_messages=False))
                    msg = f"🔒 Чат {chat_id} заблокирован"

                elif cmd == "unlock":
                    await _bot.set_chat_permissions(chat_id, ChatPermissions(
                        can_send_messages=True, can_send_media_messages=True,
                        can_send_polls=True, can_send_other_messages=True,
                        can_add_web_page_previews=True))
                    msg = f"🔓 Чат {chat_id} разблокирован"

                elif cmd == "announce":
                    if not arg:
                        result = {"ok": False, "msg": "⚠️ Введи текст объявления"}
                        raise ValueError("no arg")
                    await _bot.send_message(chat_id,
                        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{arg}\n\n— Администрация",
                        parse_mode="HTML")
                    msg = f"📢 Объявление отправлено в {chat_id}"

                elif cmd == "ban" and target_uid:
                    await _bot.ban_chat_member(chat_id, target_uid)
                    msg = f"🔨 Бан ID{target_uid} в чате {chat_id}"

                elif cmd == "unban" and target_uid:
                    await _bot.unban_chat_member(chat_id, target_uid, only_if_banned=True)
                    msg = f"🕊 Разбан ID{target_uid}"

                elif cmd == "mute" and target_uid:
                    mins = int(arg) if arg.isdigit() else 60
                    await _bot.restrict_chat_member(chat_id, target_uid,
                        ChatPermissions(can_send_messages=False),
                        until_date=timedelta(minutes=mins))
                    msg = f"🔇 Мут ID{target_uid} на {mins}мин"

                elif cmd == "unmute" and target_uid:
                    await _bot.restrict_chat_member(chat_id, target_uid,
                        ChatPermissions(can_send_messages=True,
                            can_send_media_messages=True, can_send_polls=True,
                            can_send_other_messages=True, can_add_web_page_previews=True))
                    msg = f"🔊 Размут ID{target_uid}"

                elif cmd == "kick" and target_uid:
                    await _bot.ban_chat_member(chat_id, target_uid)
                    await _bot.unban_chat_member(chat_id, target_uid)
                    msg = f"👟 Кик ID{target_uid}"

                elif cmd == "pin" and arg:
                    # arg = message_id
                    try:
                        await _bot.pin_chat_message(chat_id, int(arg))
                        msg = f"📌 Сообщение {arg} закреплено"
                    except:
                        msg = "❌ Не удалось закрепить"

                elif cmd == "slowmode":
                    delay = int(arg) if arg.isdigit() else 30
                    _bot_token2 = os.getenv("BOT_TOKEN","")
                    import aiohttp as _ah2
                    async with _ah2.ClientSession() as _s2:
                        _r2 = await _s2.post(
                            f"https://api.telegram.org/bot{_bot_token2}/setChatSlowModeDelay",
                            json={"chat_id": chat_id, "slow_mode_delay": delay}
                        )
                        _rd2 = await _r2.json()
                    if not _rd2.get("ok"):
                        raise Exception(_rd2.get("description","Telegram error"))
                    msg = f"🐢 Slowmode {delay}с в чате {chat_id}"

                elif cmd == "send" and arg:
                    await _bot.send_message(chat_id, arg, parse_mode="HTML")
                    msg = f"✉️ Сообщение отправлено"

                elif cmd == "sos_on":
                    chats_all = await db.get_all_chats()
                    locked = 0
                    for c in chats_all:
                        try:
                            await _bot.set_chat_permissions(c["cid"],
                                ChatPermissions(can_send_messages=False))
                            locked += 1
                        except: pass
                    msg = f"🚨 SOS: {locked} чатов заблокировано"

                elif cmd == "sos_off":
                    chats_all = await db.get_all_chats()
                    unlocked = 0
                    for c in chats_all:
                        try:
                            await _bot.set_chat_permissions(c["cid"],
                                ChatPermissions(can_send_messages=True,
                                    can_send_media_messages=True, can_send_polls=True,
                                    can_send_other_messages=True,
                                    can_add_web_page_previews=True))
                            unlocked += 1
                        except: pass
                    msg = f"✅ Разлокдаун: {unlocked} чатов"

                elif cmd == "broadcast":
                    if not arg:
                        result = {"ok": False, "msg": "⚠️ Введи текст"}
                        raise ValueError("no arg")
                    chats_all = await db.get_all_chats()
                    sent = 0
                    for c in chats_all:
                        try:
                            await _bot.send_message(c["cid"],
                                f"📢 <b>Сообщение от администрации</b>\n\n{arg}",
                                parse_mode="HTML")
                            sent += 1
                            import asyncio as _aio
                            await _aio.sleep(0.05)
                        except: pass
                    msg = f"📡 Рассылка: {sent} чатов"

                elif cmd == "status":
                    me = await _bot.get_me()
                    chats_all = await db.get_all_chats()
                    online = shared.get_online_count()
                    msg = (f"🤖 @{me.username} · "
                           f"💬 {len(chats_all)} чатов · "
                           f"👥 {online} онлайн · "
                           f"🚨 {len(shared.alerts)} алертов")
                else:
                    result = {"ok": False, "msg": f"❓ Неизвестная команда: {cmd}"}
                    raise ValueError("unknown cmd")

                result = {"ok": True, "msg": msg}
                _cc_push_event("cmd", f"[{uname}] {cmd}: {msg}", str(chat_id), uid_s)
                _log_admin_db(uid_s, f"CC_{cmd.upper()}", msg)

            except ValueError:
                pass
            except Exception as e:
                result = {"ok": False, "msg": f"❌ {e}"}
                _cc_push_event("error", f"[{uname}] Ошибка {cmd}: {e}")

        # JSON ответ для AJAX
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return web.json_response(result)

    # ── GET: рендер страницы ─────────────────────────────────────
    chats = []
    try:
        chats = [dict(r) for r in await db.get_all_chats()]
    except: pass

    # Статистика по каждому чату
    chat_stats_map = {}
    try:
        conn = db.get_conn()
        for c in chats:
            cid = c["cid"]
            warns = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE cid=? AND action LIKE '%Варн%' AND date(created_at)=date('now')",
                (cid,)).fetchone()[0] or 0
            bans = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE cid=? AND action LIKE '%Бан%' AND date(created_at)=date('now')",
                (cid,)).fetchone()[0] or 0
            msgs_today = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE cid=? AND date(created_at)=date('now')",
                (cid,)).fetchone()[0] or 0
            chat_stats_map[cid] = {"warns": warns, "bans": bans, "actions": msgs_today}
        conn.close()
    except: pass

    # Онлайн модераторы
    on_duty_list = get_all_on_duty()
    online_count = shared.get_online_count()
    alerts_count = len(shared.alerts)
    open_tickets = 0
    try:
        conn = db.get_conn()
        open_tickets = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
        conn.close()
    except: pass

    # Последние 50 событий для initial load
    recent_events = _cc_event_log[-50:][::-1]

    # Строим карточки чатов
    chat_cards_html = ""
    for c in chats[:20]:
        cid    = c["cid"]
        title  = (c.get("title") or str(cid))[:22]
        st     = chat_stats_map.get(cid, {})
        warns  = st.get("warns", 0)
        bans   = st.get("bans", 0)
        heat   = "🔥" if warns + bans > 5 else ("⚡" if warns + bans > 2 else "✅")
        chat_cards_html += (
            f'<div class="cc-chat-card" data-cid="{cid}" onclick="selectChat({cid},\'{title}\')">'
            f'<div style="font-weight:600;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'
            f'{heat} {title}</div>'
            f'<div style="font-size:10px;color:var(--text2);margin-top:4px;display:flex;gap:8px;">'
            f'<span>⚡{warns}в</span><span>🔨{bans}б</span>'
            f'</div></div>'
        )
    if not chat_cards_html:
        chat_cards_html = '<div style="color:var(--text2);font-size:12px;padding:12px;">Чатов нет</div>'

    # История команд из лога
    cmd_history_html = ""
    for ev in recent_events[:30]:
        color = {"cmd": "var(--accent)", "error": "var(--danger)", "warn": "var(--warn)"}.get(ev.get("type"), "var(--text2)")
        cmd_history_html += (
            f'<div style="padding:5px 0;border-bottom:1px solid var(--border);font-size:11px;">'
            f'<span style="color:var(--text2);">{ev.get("time","")}</span> '
            f'<span style="color:{color};">{ev.get("text","")[:80]}</span>'
            f'</div>'
        )
    if not cmd_history_html:
        cmd_history_html = '<div style="color:var(--text2);font-size:12px;padding:8px 0;">Команд ещё не было</div>'

    # Дежурные
    duty_html = ""
    for d in on_duty_list[:5]:
        ri = DASHBOARD_RANKS.get(d.get("rank", 1), DASHBOARD_RANKS[1])
        duty_html += (
            f'<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--border);">'
            f'<div style="width:8px;height:8px;border-radius:50%;background:var(--success);flex-shrink:0;"></div>'
            f'<div style="flex:1;font-size:12px;font-weight:600;">{d.get("name","?")}</div>'
            f'<div style="font-size:10px;color:{ri["color"]};">{ri["name"]}</div>'
            f'</div>'
        )
    if not duty_html:
        duty_html = '<div style="color:var(--text2);font-size:12px;padding:8px 0;">Никто не дежурит</div>'

    # Чаты для select
    chat_opts = "".join(
        f'<option value="{c["cid"]}">{(c.get("title") or str(c["cid"]))[:30]} ({c["cid"]})</option>'
        for c in chats
    )

    result_html = ""
    if result.get("msg"):
        color = "var(--success)" if result["ok"] else "var(--danger)"
        result_html = (
            f'<div style="background:var(--bg3);border-left:3px solid {color};'
            f'padding:10px 14px;border-radius:0 8px 8px 0;margin-bottom:16px;font-size:13px;">'
            f'{result["msg"]}</div>'
        )

    body = navbar(sess, "command_center") + f"""
    <div style="padding:0;max-width:100%;margin:0 auto;">

      <!-- ── Статус-бар ── -->
      <div style="background:var(--bg2);border-bottom:1px solid var(--border);
                  padding:10px 24px;display:flex;gap:24px;align-items:center;flex-wrap:wrap;">
        <div style="display:flex;align-items:center;gap:8px;">
          <div style="width:10px;height:10px;border-radius:50%;
               background:{'#22c55e' if _bot else '#ef4444'};"></div>
          <span style="font-size:13px;font-weight:600;">
            {'🟢 Бот онлайн' if _bot else '🔴 Бот офлайн'}
          </span>
        </div>
        <div style="font-size:13px;">👥 <b id="cc-online">{online_count}</b> онлайн</div>
        <div style="font-size:13px;">💬 <b>{len(chats)}</b> чатов</div>
        <div style="font-size:13px;">🎫 <b id="cc-tickets">{open_tickets}</b> тикетов</div>
        <div style="font-size:13px;{'color:var(--danger);font-weight:700;' if alerts_count else ''}">
          🚨 <b id="cc-alerts">{alerts_count}</b> алертов
        </div>
        <div style="margin-left:auto;display:flex;gap:8px;">
          <form method="POST" style="display:inline;">
            <input type="hidden" name="cmd" value="sos_on">
            <button class="btn btn-sm" style="background:rgba(239,68,68,.15);color:var(--danger);"
              onclick="return confirm('🚨 Заблокировать ВСЕ чаты?')">🚨 SOS ВКЛ</button>
          </form>
          <form method="POST" style="display:inline;">
            <input type="hidden" name="cmd" value="sos_off">
            <button class="btn btn-sm btn-ghost"
              onclick="return confirm('Разблокировать все чаты?')">✅ SOS ВЫКЛ</button>
          </form>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:260px 1fr 300px;gap:0;height:calc(100vh - 120px);overflow:hidden;" id="cc-grid">

        <!-- ── Левая колонка: Чаты ── -->
        <div id="cc-left" style="border-right:1px solid var(--border);overflow-y:auto;padding:0;">
          <div style="padding:12px 16px;font-size:11px;font-weight:700;color:var(--text2);
               text-transform:uppercase;letter-spacing:.1em;border-bottom:1px solid var(--border);
               position:sticky;top:0;background:var(--bg1);">
            💬 Чаты ({len(chats)})
            <input id="chatSearch" placeholder="Поиск..." style="display:block;width:100%;margin-top:6px;
              padding:5px 8px;border-radius:6px;border:1px solid var(--border);background:var(--bg3);
              color:var(--text1);font-size:11px;" oninput="filterChats(this.value)">
          </div>
          <div id="chatList" style="padding:8px;">
            {chat_cards_html}
          </div>
        </div>

        <!-- ── Центр: терминал команд ── -->
        <div style="display:flex;flex-direction:column;overflow:hidden;">

          <!-- Выбранный чат + статус -->
          <div style="padding:12px 20px;border-bottom:1px solid var(--border);
               background:var(--bg2);display:flex;gap:12px;align-items:center;">
            <div style="flex:1;">
              <span style="font-size:12px;color:var(--text2);">Активный чат:</span>
              <span id="activeChatName" style="font-size:14px;font-weight:700;margin-left:8px;">
                — не выбран —
              </span>
            </div>
            <select id="chatSelect" class="form-control" style="max-width:240px;font-size:12px;"
              onchange="selectChatById(this.value)">
              <option value="">— выбери чат —</option>
              {chat_opts}
            </select>
          </div>

          {result_html}

          <!-- Кнопки быстрых команд -->
          <div style="padding:12px 20px;border-bottom:1px solid var(--border);
               display:flex;gap:6px;flex-wrap:wrap;background:var(--bg2);">
            <button class="cc-quick-btn" onclick="runCmd('lock')" 
              style="background:rgba(239,68,68,.12);color:#ef4444;">🔒 Локдаун</button>
            <button class="cc-quick-btn" onclick="runCmd('unlock')"
              style="background:rgba(34,197,94,.12);color:#22c55e;">🔓 Открыть</button>
            <button class="cc-quick-btn" onclick="runCmdPrompt('ban','ID пользователя для бана:')"
              style="background:rgba(239,68,68,.08);color:#ef4444;">🔨 Бан</button>
            <button class="cc-quick-btn" onclick="runCmdPrompt('unban','ID для разбана:')"
              style="background:rgba(34,197,94,.08);color:#22c55e;">🕊 Разбан</button>
            <button class="cc-quick-btn" onclick="runCmdMute()"
              style="background:rgba(139,92,246,.12);color:#8b5cf6;">🔇 Мут</button>
            <button class="cc-quick-btn" onclick="runCmdPrompt('unmute','ID для размута:')"
              style="background:rgba(139,92,246,.08);color:#8b5cf6;">🔊 Размут</button>
            <button class="cc-quick-btn" onclick="runCmdPrompt('kick','ID для кика:')"
              style="background:rgba(245,158,11,.12);color:#f59e0b;">👟 Кик</button>
            <button class="cc-quick-btn" onclick="runCmdAnnounce()"
              style="background:rgba(99,102,241,.12);color:#6366f1;">📢 Анонс</button>
            <button class="cc-quick-btn" onclick="runCmdSlowmode()"
              style="background:rgba(6,182,212,.12);color:#06b6d4;">🐢 Slowmode</button>
            <button class="cc-quick-btn" onclick="runCmd('status')"
              style="background:rgba(107,114,128,.15);color:var(--text2);">📊 Статус</button>
          </div>

          <!-- Ввод произвольной команды -->
          <div style="padding:12px 20px;border-bottom:1px solid var(--border);background:var(--bg2);">
            <div style="display:flex;gap:8px;align-items:center;">
              <div style="color:var(--accent);font-family:monospace;font-size:14px;flex-shrink:0;">❯</div>
              <input id="termInput" class="form-control" placeholder="cmd [target] [arg]  — напр: ban 123456  или  announce Внимание!"
                style="font-family:monospace;font-size:12px;flex:1;"
                onkeydown="if(event.key==='Enter')sendTermCmd()">
              <button class="btn btn-primary btn-sm" onclick="sendTermCmd()" style="flex-shrink:0;">▶</button>
            </div>
            <div style="font-size:10px;color:var(--text2);margin-top:5px;line-height:1.8;">
              Команды: <code>lock</code> <code>unlock</code> <code>ban ID</code> <code>mute ID мин</code>
              <code>kick ID</code> <code>announce текст</code> <code>send текст</code>
              <code>slowmode N</code> <code>broadcast текст</code> <code>status</code>
            </div>
          </div>

          <!-- Лента ответов терминала -->
          <div id="termOutput" style="flex:1;overflow-y:auto;padding:12px 20px;
               font-family:Space Mono,monospace;font-size:12px;background:var(--bg1);">
            <div style="color:var(--text2);">Command Center готов. Выбери чат и отправляй команды.</div>
          </div>
        </div>

        <!-- ── Правая колонка: лента событий + дежурные ── -->
        <div id="cc-right" style="border-left:1px solid var(--border);overflow-y:auto;display:flex;flex-direction:column;">

          <!-- Дежурные -->
          <div style="padding:12px 16px;border-bottom:1px solid var(--border);">
            <div style="font-size:11px;font-weight:700;color:var(--text2);
                 text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;">
              🟢 На дежурстве ({len(on_duty_list)})
            </div>
            {duty_html}
          </div>

          <!-- Live лента событий -->
          <div style="flex:1;overflow-y:auto;">
            <div style="padding:10px 16px;font-size:11px;font-weight:700;color:var(--text2);
                 text-transform:uppercase;letter-spacing:.1em;
                 border-bottom:1px solid var(--border);position:sticky;top:0;
                 background:var(--bg1);display:flex;justify-content:space-between;align-items:center;">
              <span>📡 Live события</span>
              <span id="liveStatus" style="font-size:9px;color:var(--text2);">●</span>
            </div>
            <div id="eventFeed" style="padding:0 12px;">
              {cmd_history_html}
            </div>
          </div>
        </div>
      </div>
    </div>

    <style>
    .cc-chat-card {{
      padding:10px 12px;border-radius:8px;cursor:pointer;margin-bottom:4px;
      border:1px solid transparent;transition:all .15s;
    }}
    .cc-chat-card:hover {{ background:var(--bg3);border-color:var(--border); }}
    .cc-chat-card.active {{ background:rgba(99,102,241,.15);border-color:var(--accent); }}
    .cc-quick-btn {{
      padding:5px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;
      border:none;transition:opacity .15s;
    }}
    .cc-quick-btn:hover {{ opacity:.8; }}
    #termOutput {{ scrollbar-width:thin; }}
    </style>

    <script>
    var _activeCid = 0;
    var _activeName = '';
    var _cmdHistory = [];
    var _historyIdx = -1;

    function selectChat(cid, name) {{
      _activeCid = cid;
      _activeName = name;
      document.getElementById('activeChatName').textContent = name + ' (' + cid + ')';
      document.getElementById('chatSelect').value = cid;
      document.querySelectorAll('.cc-chat-card').forEach(function(c) {{
        c.classList.toggle('active', c.dataset.cid == cid);
      }});
      appendOutput('✅ Выбран чат: ' + name + ' (' + cid + ')', 'var(--accent)');
    }}
    function selectChatById(cid) {{
      var opt = document.querySelector('#chatSelect option[value="'+cid+'"]');
      if (opt) selectChat(cid, opt.textContent.split(' (')[0]);
    }}
    function filterChats(q) {{
      q = q.toLowerCase();
      document.querySelectorAll('.cc-chat-card').forEach(function(c) {{
        c.style.display = c.textContent.toLowerCase().includes(q) ? '' : 'none';
      }});
    }}

    function appendOutput(text, color) {{
      color = color || 'var(--text1)';
      var d = document.getElementById('termOutput');
      var now = new Date().toTimeString().slice(0,8);
      var div = document.createElement('div');
      div.style.cssText = 'padding:3px 0;border-bottom:1px solid var(--border);';
      div.innerHTML = '<span style="color:var(--text2);">' + now + '</span> '
        + '<span style="color:' + color + ';">' + text + '</span>';
      d.prepend(div);
    }}

    function submitCmd(cmd, target, arg) {{
      var fd = new FormData();
      fd.append('cmd', cmd);
      fd.append('chat_id', _activeCid);
      fd.append('target', target || '');
      fd.append('arg', arg || '');
      appendOutput('❯ ' + cmd + (target?' '+target:'') + (arg?' '+arg:''), 'var(--accent)');
      fetch(window.location.href, {{
        method: 'POST',
        headers: {{'X-Requested-With': 'XMLHttpRequest'}},
        body: fd
      }}).then(function(r){{return r.json();}}).then(function(d){{
        appendOutput(d.msg, d.ok ? 'var(--success)' : 'var(--danger)');
      }}).catch(function(e){{
        appendOutput('❌ Сетевая ошибка: ' + e, 'var(--danger)');
      }});
    }}

    function runCmd(cmd) {{
      if (!_activeCid && !['status','broadcast','sos_on','sos_off'].includes(cmd)) {{
        appendOutput('⚠️ Выбери чат!', 'var(--warn)'); return;
      }}
      submitCmd(cmd, '', '');
    }}
    function runCmdPrompt(cmd, label) {{
      if (!_activeCid) {{ appendOutput('⚠️ Выбери чат!', 'var(--warn)'); return; }}
      var v = prompt(label); if (!v) return;
      submitCmd(cmd, v, '');
    }}
    function runCmdMute() {{
      if (!_activeCid) {{ appendOutput('⚠️ Выбери чат!', 'var(--warn)'); return; }}
      var uid = prompt('ID для мута:'); if (!uid) return;
      var mins = prompt('Минут (Enter = 60):') || '60';
      submitCmd('mute', uid, mins);
    }}
    function runCmdAnnounce() {{
      if (!_activeCid) {{ appendOutput('⚠️ Выбери чат!', 'var(--warn)'); return; }}
      var text = prompt('Текст объявления:'); if (!text) return;
      submitCmd('announce', '', text);
    }}
    function runCmdSlowmode() {{
      if (!_activeCid) {{ appendOutput('⚠️ Выбери чат!', 'var(--warn)'); return; }}
      var sec = prompt('Задержка (секунд, 0=выкл):') || '30';
      submitCmd('slowmode', '', sec);
    }}

    function sendTermCmd() {{
      var input = document.getElementById('termInput');
      var raw = input.value.trim();
      if (!raw) return;
      _cmdHistory.unshift(raw); _historyIdx = -1;
      var parts = raw.split(/\\s+/);
      var cmd = parts[0];
      var target = '';
      var arg = '';
      // Парсим: cmd [target/число] [остаток]
      var cmdsWithTarget = ['ban','unban','mute','unmute','kick'];
      if (cmdsWithTarget.includes(cmd) && parts.length >= 2) {{
        target = parts[1];
        arg = parts.slice(2).join(' ');
      }} else {{
        arg = parts.slice(1).join(' ');
      }}
      if (!_activeCid && !['status','broadcast','sos_on','sos_off'].includes(cmd)) {{
        appendOutput('⚠️ Выбери чат слева!', 'var(--warn)');
        return;
      }}
      submitCmd(cmd, target, arg);
      input.value = '';
    }}

    // Стрелки вверх/вниз для истории команд
    document.getElementById('termInput').addEventListener('keydown', function(e) {{
      if (e.key === 'ArrowUp') {{
        _historyIdx = Math.min(_historyIdx + 1, _cmdHistory.length - 1);
        if (_cmdHistory[_historyIdx]) this.value = _cmdHistory[_historyIdx];
        e.preventDefault();
      }} else if (e.key === 'ArrowDown') {{
        _historyIdx = Math.max(_historyIdx - 1, -1);
        this.value = _historyIdx >= 0 ? _cmdHistory[_historyIdx] : '';
        e.preventDefault();
      }}
    }});

    // SSE лента событий
    (function() {{
      var feed = document.getElementById('eventFeed');
      var status = document.getElementById('liveStatus');
      function connect() {{
        var es = new EventSource('/dashboard/sse');
        es.onopen = function() {{
          status.style.color = '#22c55e';
          status.title = 'Live';
        }};
        es.onmessage = function(e) {{
          if (!e.data || e.data === 'connected') return;
          if (!e.data.startsWith('cc:')) return;
          try {{
            var ev = JSON.parse(e.data.slice(3));
            var colors = {{cmd:'var(--accent)',error:'var(--danger)',warn:'var(--warn)',ban:'#ef4444',mute:'#8b5cf6',report:'#f59e0b'}};
            var color = colors[ev.type] || 'var(--text2)';
            var div = document.createElement('div');
            div.style.cssText = 'padding:5px 0;border-bottom:1px solid var(--border);font-size:11px;animation:fadeIn .3s;';
            div.innerHTML = '<span style="color:var(--text2);">' + ev.time + '</span> '
              + (ev.chat ? '<span style="font-size:10px;background:var(--bg3);padding:1px 5px;border-radius:4px;margin:0 4px;">'+ ev.chat.slice(-10) +'</span> ' : '')
              + '<span style="color:' + color + ';">' + ev.text + '</span>';
            feed.prepend(div);
            // Ограничим 100 записей
            while (feed.children.length > 100) feed.removeChild(feed.lastChild);
          }} catch(e) {{}}
        }};
        es.onerror = function() {{
          status.style.color = '#ef4444';
          status.title = 'Disconnected';
          setTimeout(connect, 3000);
        }};
      }}
      connect();
    }})();

    // Обновление счётчиков каждые 10 сек
    setInterval(function() {{
      fetch('/api/live').then(function(r){{return r.json();}}).then(function(d){{
        if (d.online !== undefined) document.getElementById('cc-online').textContent = d.online;
        if (d.alerts !== undefined) document.getElementById('cc-alerts').textContent = d.alerts;
      }}).catch(function(){{}});
    }}, 10000);
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")


handle_command_center = require_auth("view_overview")(handle_command_center)



# ══════════════════════════════════════════════════════════════════
#  📱 TELEGRAM MINI APP — API
# ══════════════════════════════════════════════════════════════════
import hashlib, hmac as _hmac, urllib.parse as _uparse
import json as _json

_mini_tokens: dict = {}   # {token: {uid, name, rank, ts}}


def _verify_init_data(init_data: str) -> dict | None:
    """Верифицирует initData от Telegram WebApp через HMAC-SHA256."""
    bot_token = os.getenv("BOT_TOKEN", "")
    if not bot_token:
        return None
    try:
        # Парсим init_data
        parsed = dict(_uparse.parse_qsl(init_data, keep_blank_values=True))
        check_hash = parsed.pop("hash", "")
        # Строка для проверки — отсортированные key=value через newline
        data_check = "\n".join(
            f"{k}={v}" for k, v in sorted(parsed.items())
        )
        # Секретный ключ = HMAC-SHA256("WebAppData", bot_token)
        secret = _hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        # Вычисляем hash
        expected = _hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not _hmac.compare_digest(expected, check_hash):
            return None
        # Проверяем не устарели ли данные (10 минут)
        auth_date = int(parsed.get("auth_date", 0))
        if time.time() - auth_date > 600:
            return None
        # Возвращаем данные пользователя
        user_json = parsed.get("user", "{}")
        return _json.loads(user_json)
    except Exception as e:
        log.warning(f"[MINI] verify_init_data error: {e}")
        return None


async def api_mini_auth(request: web.Request):
    """POST /api/mini/auth — авторизация Mini App через initData."""
    try:
        body = await request.json()
        init_data = body.get("initData", "")
    except Exception:
        return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    if not init_data:
        return web.json_response({"ok": False, "error": "No initData"}, status=400)

    # Верифицируем
    user = _verify_init_data(init_data)
    if not user:
        return web.json_response({"ok": False, "error": "Invalid initData"}, status=403)

    uid = user.get("id")
    if not uid:
        return web.json_response({"ok": False, "error": "No user id"}, status=403)

    # Проверяем что пользователь есть в администрации
    admin = _get_admin(uid)
    if not admin:
        return web.json_response({"ok": False, "error": "Not an admin"}, status=403)

    # Генерируем токен
    token = secrets.token_hex(32)
    _mini_tokens[token] = {
        "uid":   uid,
        "name":  admin["name"],
        "rank":  admin["rank"],
        "ts":    time.time(),
    }
    # Чистим старые токены (> 24ч)
    cutoff = time.time() - 86400
    for k in [k for k, v in _mini_tokens.items() if v["ts"] < cutoff]:
        del _mini_tokens[k]

    ri = DASHBOARD_RANKS.get(admin["rank"], DASHBOARD_RANKS[1])
    return web.json_response({
        "ok":    True,
        "token": token,
        "user":  {"uid": uid, "name": admin["name"],
                  "rank": admin["rank"], "rank_name": ri["name"]},
    })


def _mini_auth(request) -> dict | None:
    """Проверяет X-Mini-Token заголовок."""
    token = request.headers.get("X-Mini-Token", "")
    if not token:
        return None
    sess = _mini_tokens.get(token)
    if not sess:
        return None
    # Токен живёт 24ч
    if time.time() - sess["ts"] > 86400:
        del _mini_tokens[token]
        return None
    return sess


async def api_mini_stats(request: web.Request):
    """GET /api/mini/stats — основная статистика."""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    try:
        online_count  = shared.get_online_count()
        alerts_count  = len(shared.alerts)
        on_duty_list  = get_all_on_duty()

        conn = db.get_conn()
        bans_total = conn.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
        tickets_open = 0
        try:
            tickets_open = conn.execute(
                "SELECT COUNT(*) FROM tickets WHERE status='open'"
            ).fetchone()[0] or 0
        except Exception:
            pass
        conn.close()

        # Последние события из лог-таблицы
        events = []
        try:
            conn2 = db.get_conn()
            rows = conn2.execute(
                "SELECT action, details, created_at FROM dashboard_admin_log "
                "ORDER BY created_at DESC LIMIT 10"
            ).fetchall()
            conn2.close()
            icon_map = {
                "BAN": "🔨", "WARN": "⚡", "MUTE": "🔇", "KICK": "👟",
                "LOGIN": "🔑", "GRANT": "👑", "REVOKE": "❌",
                "TICKET": "🎫", "INCIDENT": "🚨",
            }
            for r in rows:
                act = str(r["action"] or "")
                icon = next((v for k, v in icon_map.items() if k in act.upper()), "📋")
                events.append({
                    "icon": icon,
                    "text": (r["details"] or act)[:60],
                    "time": str(r["created_at"] or "")[:16],
                })
        except Exception:
            pass

        duty = [
            {"name": d.get("name", "?"),
             "rank_name": DASHBOARD_RANKS.get(d.get("rank", 1), DASHBOARD_RANKS[1])["name"]}
            for d in on_duty_list[:5]
        ]

        return web.json_response({
            "ok":          True,
            "online":      online_count,
            "alerts":      alerts_count,
            "tickets_open": tickets_open,
            "bans":        bans_total,
            "duty":        duty,
            "events":      events,
        })
    except Exception as e:
        log.error(f"[MINI] stats error: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_mini_tickets(request: web.Request):
    """GET /api/mini/tickets?status=open"""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    status = request.rel_url.query.get("status", "open")
    try:
        conn = db.get_conn()
        if status == "all":
            rows = conn.execute(
                "SELECT * FROM tickets ORDER BY created_at DESC LIMIT 50"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM tickets WHERE status=? ORDER BY created_at DESC LIMIT 50",
                (status,)
            ).fetchall()
        conn.close()
        tickets = [dict(r) for r in rows]
        return web.json_response({"ok": True, "tickets": tickets})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_mini_ticket_detail(request: web.Request):
    """GET /api/mini/ticket/{id}"""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    tid = int(request.match_info.get("id", 0))
    try:
        conn = db.get_conn()
        row = conn.execute("SELECT * FROM tickets WHERE id=?", (tid,)).fetchone()
        conn.close()
        if not row:
            return web.json_response({"ok": False, "error": "Not found"}, status=404)
        return web.json_response({"ok": True, "ticket": dict(row)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_mini_ticket_close(request: web.Request):
    """POST /api/mini/ticket/{id}/close"""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    tid = int(request.match_info.get("id", 0))
    try:
        conn = db.get_conn()
        conn.execute(
            "UPDATE tickets SET status='closed' WHERE id=?", (tid,)
        )
        conn.commit(); conn.close()
        _log_admin_db(sess["uid"], "TICKET_CLOSE_MINI", f"#{tid}")
        return web.json_response({"ok": True, "msg": f"Тикет #{tid} закрыт"})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_mini_chats(request: web.Request):
    """GET /api/mini/chats — список чатов."""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    try:
        chats = [dict(r) for r in await db.get_all_chats()]
        return web.json_response({"ok": True, "chats": chats[:30]})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_mini_action(request: web.Request):
    """POST /api/mini/action — действие над пользователем/чатом."""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    if not _bot:
        return web.json_response({"ok": False, "msg": "Бот не подключён"})

    try:
        body    = await request.json()
        action  = body.get("action", "")
        uid     = int(body.get("user_id", 0) or 0)
        cid     = int(body.get("chat_id", 0) or 0)
        arg     = str(body.get("arg", "") or "")
    except Exception:
        return web.json_response({"ok": False, "msg": "Invalid JSON"}, status=400)

    try:
        from aiogram.types import ChatPermissions
        from datetime import timedelta as _td

        msg = ""

        if action == "ban" and uid and cid:
            await _bot.ban_chat_member(cid, uid)
            msg = f"🔨 Бан ID{uid}"
            _log_admin_db(sess["uid"], "MINI_BAN", f"{cid}:{uid}")

        elif action == "unban" and uid and cid:
            await _bot.unban_chat_member(cid, uid, only_if_banned=True)
            msg = f"🕊 Разбан ID{uid}"
            _log_admin_db(sess["uid"], "MINI_UNBAN", f"{cid}:{uid}")

        elif action == "kick" and uid and cid:
            await _bot.ban_chat_member(cid, uid)
            await _bot.unban_chat_member(cid, uid)
            msg = f"👟 Кик ID{uid}"
            _log_admin_db(sess["uid"], "MINI_KICK", f"{cid}:{uid}")

        elif action == "mute" and uid and cid:
            mins = int(arg) if arg.isdigit() else 60
            await _bot.restrict_chat_member(cid, uid,
                ChatPermissions(can_send_messages=False),
                until_date=_td(minutes=mins))
            msg = f"🔇 Мут ID{uid} на {mins}мин"
            _log_admin_db(sess["uid"], "MINI_MUTE", f"{cid}:{uid} {mins}m")

        elif action == "unmute" and uid and cid:
            await _bot.restrict_chat_member(cid, uid,
                ChatPermissions(can_send_messages=True,
                    can_send_media_messages=True, can_send_polls=True,
                    can_send_other_messages=True, can_add_web_page_previews=True))
            msg = f"🔊 Размут ID{uid}"
            _log_admin_db(sess["uid"], "MINI_UNMUTE", f"{cid}:{uid}")

        elif action == "warn" and uid:
            msg = f"⚡ Варн ID{uid} (через бот-команду)"
            _log_admin_db(sess["uid"], "MINI_WARN", f"uid={uid}")

        elif action == "send_message" and cid and arg:
            await _bot.send_message(cid, arg, parse_mode="HTML")
            msg = f"✉️ Сообщение отправлено"
            _log_admin_db(sess["uid"], "MINI_MSG", f"→{cid}: {arg[:40]}")

        elif action == "lock" and cid:
            await _bot.set_chat_permissions(cid,
                ChatPermissions(can_send_messages=False))
            msg = f"🔒 Чат {cid} заблокирован"
            _log_admin_db(sess["uid"], "MINI_LOCK", str(cid))

        elif action == "unlock" and cid:
            await _bot.set_chat_permissions(cid,
                ChatPermissions(can_send_messages=True,
                    can_send_media_messages=True, can_send_polls=True,
                    can_send_other_messages=True, can_add_web_page_previews=True))
            msg = f"🔓 Чат {cid} разблокирован"
            _log_admin_db(sess["uid"], "MINI_UNLOCK", str(cid))

        else:
            return web.json_response({"ok": False, "msg": f"Неизвестное действие: {action}"})

        return web.json_response({"ok": True, "msg": msg})

    except Exception as e:
        log.error(f"[MINI] action error: {e}")
        return web.json_response({"ok": False, "msg": str(e)}, status=500)


async def api_mini_me(request: web.Request):
    """GET /api/mini/me — профиль текущего мода."""
    sess = _mini_auth(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)

    uid   = sess["uid"]
    admin = _get_admin(uid)
    if not admin:
        return web.json_response({"ok": False, "error": "Not found"}, status=404)

    ri    = DASHBOARD_RANKS.get(admin["rank"], DASHBOARD_RANKS[1])
    stats = _get_mod_stats(admin["name"], uid)

    recent = []
    try:
        conn = db.get_conn()
        rows = conn.execute(
            "SELECT action, created_at FROM mod_history WHERE by_id=? "
            "ORDER BY created_at DESC LIMIT 10",
            (uid,)
        ).fetchall()
        conn.close()
        recent = [{"action": r["action"], "created_at": str(r["created_at"])} for r in rows]
    except Exception:
        pass

    return web.json_response({
        "ok":    True,
        "admin": {
            "name":      admin["name"],
            "rank":      admin["rank"],
            "rank_name": ri["name"],
        },
        "stats":  stats,
        "recent": recent,
    })


async def handle_mini_app(request: web.Request):
    """GET /mini — отдаёт mini_app.html."""
    import os as _os
    html_path = _os.path.join(_os.path.dirname(__file__), "mini_app.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        return web.Response(text=html, content_type="text/html")
    except FileNotFoundError:
        return web.Response(text="<h1>mini_app.html не найден</h1>", content_type="text/html", status=404)


# owner_features.py — вставляется в dashboard.py перед start_dashboard()

import secrets as _sec
import hashlib as _hl
import gzip as _gz
import shutil as _sh
import os as _os_bk

# ══════════════════════════════════════════════════════════════════
#  🗄 ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ
# ══════════════════════════════════════════════════════════════════
def _owner_init_tables():
    try:
        conn = db.get_conn()
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS vip_users (
            uid INTEGER PRIMARY KEY,
            username TEXT, granted_by INTEGER,
            tier TEXT DEFAULT 'standard',
            expires_at TEXT, granted_at TEXT DEFAULT (datetime('now')),
            note TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS network_bans (
            uid INTEGER PRIMARY KEY,
            reason TEXT, banned_by INTEGER,
            banned_at TEXT DEFAULT (datetime('now')),
            chat_origin INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS backup_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT, size_bytes INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            status TEXT DEFAULT 'ok', note TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS mod_2fa_settings (
            uid INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 1,
            actions TEXT DEFAULT 'ban,kick,revoke'
        );
        CREATE TABLE IF NOT EXISTS push_settings (
            uid INTEGER PRIMARY KEY,
            new_ticket INTEGER DEFAULT 1,
            new_alert INTEGER DEFAULT 1,
            new_report INTEGER DEFAULT 1,
            new_appeal INTEGER DEFAULT 1,
            quiet_start TEXT DEFAULT '23:00',
            quiet_end TEXT DEFAULT '08:00'
        );
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"[OWNER] init tables: {e}")


# ══════════════════════════════════════════════════════════════════
#  ⭐ VIP СИСТЕМА
# ══════════════════════════════════════════════════════════════════
def _vip_get_all() -> list:
    try:
        conn = db.get_conn()
        rows = conn.execute("SELECT * FROM vip_users ORDER BY expires_at DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []

def _vip_grant(uid: int, username: str, tier: str, days: int, granted_by: int, note: str = ""):
    import datetime as _dt
    exp = (_dt.datetime.now() + _dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = db.get_conn()
    conn.execute("""INSERT INTO vip_users (uid,username,granted_by,tier,expires_at,note)
        VALUES (?,?,?,?,?,?) ON CONFLICT(uid) DO UPDATE SET
        tier=excluded.tier, expires_at=excluded.expires_at,
        granted_by=excluded.granted_by, note=excluded.note""",
        (uid, username, granted_by, tier, exp, note))
    conn.commit(); conn.close()

def _vip_revoke(uid: int):
    conn = db.get_conn()
    conn.execute("DELETE FROM vip_users WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def _vip_is_active(uid: int) -> bool:
    try:
        conn = db.get_conn()
        row = conn.execute(
            "SELECT expires_at FROM vip_users WHERE uid=? AND expires_at > datetime('now')",
            (uid,)).fetchone()
        conn.close()
        return row is not None
    except: return False


# ══════════════════════════════════════════════════════════════════
#  🌐 СЕТЬ ЧАТОВ — ГЛОБАЛЬНЫЙ БАН
# ══════════════════════════════════════════════════════════════════
def _netban_get_all() -> list:
    try:
        conn = db.get_conn()
        rows = conn.execute("SELECT * FROM network_bans ORDER BY banned_at DESC LIMIT 100").fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []

def _netban_is_banned(uid: int) -> bool:
    try:
        conn = db.get_conn()
        row = conn.execute("SELECT 1 FROM network_bans WHERE uid=?", (uid,)).fetchone()
        conn.close()
        return row is not None
    except: return False

async def _netban_apply(uid: int, reason: str, banned_by: int, chat_origin: int = 0):
    """Баним юзера во всех чатах сети."""
    conn = db.get_conn()
    conn.execute("""INSERT INTO network_bans (uid,reason,banned_by,chat_origin)
        VALUES (?,?,?,?) ON CONFLICT(uid) DO UPDATE SET
        reason=excluded.reason, banned_by=excluded.banned_by""",
        (uid, reason, banned_by, chat_origin))
    conn.commit(); conn.close()
    if _bot:
        chats = await db.get_all_chats()
        banned = 0
        for c in chats:
            try:
                await _bot.ban_chat_member(c["cid"], uid)
                banned += 1
            except: pass
        return banned
    return 0

async def _netban_remove(uid: int):
    """Разбаниваем во всех чатах."""
    conn = db.get_conn()
    conn.execute("DELETE FROM network_bans WHERE uid=?", (uid,))
    conn.commit(); conn.close()
    if _bot:
        chats = await db.get_all_chats()
        for c in chats:
            try: await _bot.unban_chat_member(c["cid"], uid, only_if_banned=True)
            except: pass


# ══════════════════════════════════════════════════════════════════
#  💾 АВТО-БЭКАП
# ══════════════════════════════════════════════════════════════════
async def _do_backup(manual: bool = False) -> dict:
    """Создаёт gzip бэкап БД и шлёт в лог-канал."""
    import datetime as _dt, io as _io
    try:
        db_path = _os_bk.path.abspath("skinvault.db")
        if not _os_bk.path.exists(db_path):
            return {"ok": False, "error": "БД не найдена"}

        ts    = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"backup_{ts}{'_manual' if manual else ''}.db.gz"
        size  = _os_bk.path.getsize(db_path)

        # Сжимаем в памяти
        buf = _io.BytesIO()
        with open(db_path, "rb") as f_in, _gz.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(f_in.read())
        gz_data = buf.getvalue()
        gz_size = len(gz_data)

        # Шлём в лог-канал
        if _bot:
            from aiogram.types import BufferedInputFile
            caption = (
                f"💾 <b>{'Ручной' if manual else 'Авто'}-бэкап</b>\n\n"
                f"📅 {_dt.datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                f"📦 Размер: {size//1024} КБ → {gz_size//1024} КБ (gzip)\n"
                f"🗄 Файл: <code>{fname}</code>"
            )
            await _bot.send_document(
                LOG_CHANNEL_ID,
                document=BufferedInputFile(gz_data, filename=fname),
                caption=caption, parse_mode="HTML"
            )

        # Логируем
        conn = db.get_conn()
        conn.execute("INSERT INTO backup_log (filename,size_bytes,note) VALUES (?,?,?)",
                     (fname, gz_size, "manual" if manual else "auto"))
        conn.commit(); conn.close()

        return {"ok": True, "filename": fname, "size": gz_size}
    except Exception as e:
        log.error(f"[BACKUP] error: {e}")
        return {"ok": False, "error": str(e)}


async def _backup_loop():
    """Авто-бэкап каждые 6 часов."""
    import asyncio as _aio
    await _aio.sleep(30)  # ждём запуска бота
    while True:
        await _do_backup()
        await _aio.sleep(6 * 3600)


# ══════════════════════════════════════════════════════════════════
#  🔐 2FA ДЛЯ КРИТИЧЕСКИХ ДЕЙСТВИЙ
# ══════════════════════════════════════════════════════════════════
_2fa_action_pending: dict = {}  # {uid: {action, code, expires, data}}

def _2fa_is_required(uid: int, action: str) -> bool:
    """Проверяет нужна ли 2FA для данного действия."""
    try:
        conn = db.get_conn()
        row = conn.execute("SELECT enabled, actions FROM mod_2fa_settings WHERE uid=?",
                           (uid,)).fetchone()
        conn.close()
        if not row:
            # По умолчанию — включена для всех
            return action in ("ban", "kick", "revoke_admin", "update_rank", "global_ban")
        if not row["enabled"]:
            return False
        actions = (row["actions"] or "").split(",")
        return action in actions
    except: return False

async def _2fa_send_code(uid: int, action: str, action_data: dict = None) -> str:
    """Генерирует и отправляет код 2FA."""
    import random as _r
    code = str(_r.randint(100000, 999999))
    _2fa_action_pending[uid] = {
        "code":    code,
        "action":  action,
        "data":    action_data or {},
        "expires": time.time() + 300,
    }
    if _bot:
        try:
            await _bot.send_message(uid,
                f"🔐 <b>Подтверждение действия</b>\n\n"
                f"Действие: <b>{action}</b>\n"
                f"Код подтверждения:\n\n"
                f"<code>{code}</code>\n\n"
                f"⏰ Действителен 5 минут.\n"
                f"Введи на странице дашборда.",
                parse_mode="HTML")
        except Exception as e:
            log.warning(f"[2FA] send error: {e}")
    return code

def _2fa_verify(uid: int, code: str) -> bool:
    """Проверяет код 2FA."""
    pending = _2fa_action_pending.get(uid)
    if not pending:
        return False
    if time.time() > pending["expires"]:
        _2fa_action_pending.pop(uid, None)
        return False
    if pending["code"] == code.strip():
        _2fa_action_pending.pop(uid, None)
        return True
    return False


# ══════════════════════════════════════════════════════════════════
#  🔔 PUSH-УВЕДОМЛЕНИЯ МОДЕРАТОРАМ
# ══════════════════════════════════════════════════════════════════
def _push_get_settings(uid: int) -> dict:
    try:
        conn = db.get_conn()
        row = conn.execute("SELECT * FROM push_settings WHERE uid=?", (uid,)).fetchone()
        conn.close()
        if row: return dict(row)
    except: pass
    return {"uid": uid, "new_ticket": 1, "new_alert": 1,
            "new_report": 1, "new_appeal": 1,
            "quiet_start": "23:00", "quiet_end": "08:00"}

def _push_save_settings(uid: int, s: dict):
    try:
        conn = db.get_conn()
        conn.execute("""INSERT INTO push_settings
            (uid,new_ticket,new_alert,new_report,new_appeal,quiet_start,quiet_end)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(uid) DO UPDATE SET
            new_ticket=excluded.new_ticket, new_alert=excluded.new_alert,
            new_report=excluded.new_report, new_appeal=excluded.new_appeal,
            quiet_start=excluded.quiet_start, quiet_end=excluded.quiet_end""",
            (uid, s.get("new_ticket",1), s.get("new_alert",1),
             s.get("new_report",1), s.get("new_appeal",1),
             s.get("quiet_start","23:00"), s.get("quiet_end","08:00")))
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f"[PUSH] save: {e}")

def _push_is_quiet(uid: int) -> bool:
    """Тихий час — не беспокоить."""
    import datetime as _dt
    try:
        s = _push_get_settings(uid)
        now = _dt.datetime.now().strftime("%H:%M")
        qs, qe = s.get("quiet_start","23:00"), s.get("quiet_end","08:00")
        if qs <= qe:
            return qs <= now <= qe
        else:  # переход через полночь
            return now >= qs or now <= qe
    except: return False

async def push_notify(event_type: str, text: str, kb=None):
    """Рассылает push-уведомление всем модам с включённым типом события."""
    if not _bot: return
    admins = _get_all_admins()
    for adm in admins:
        uid = adm["tg_uid"]
        if uid == OWNER_TG_ID: continue
        if _push_is_quiet(uid): continue
        s = _push_get_settings(uid)
        if not s.get(event_type, 1): continue
        try:
            await _bot.send_message(uid, text, parse_mode="HTML",
                                     reply_markup=kb)
        except: pass


# ══════════════════════════════════════════════════════════════════
#  👑 СТРАНИЦА ВЛАДЕЛЬЦА
# ══════════════════════════════════════════════════════════════════
async def handle_owner_panel(request: web.Request):
    sess = _get_session(request)
    if not sess or sess.get("rank") < OWNER_RANK:
        raise web.HTTPFound("/dashboard")
    _track_session(request)

    result_msg = ""
    result_ok  = True

    if request.method == "POST":
        data   = await request.post()
        action = data.get("action","")
        uid_s  = sess.get("uid", OWNER_TG_ID)

        # ── VIP ──────────────────────────────────────
        if action == "vip_grant":
            try:
                vip_uid  = int(data.get("vip_uid",0))
                vip_user = data.get("vip_username","").strip()
                vip_tier = data.get("vip_tier","standard")
                vip_days = int(data.get("vip_days",30))
                vip_note = data.get("vip_note","").strip()
                _vip_grant(vip_uid, vip_user, vip_tier, vip_days, uid_s, vip_note)
                _log_admin_db(uid_s, "VIP_GRANT", f"{vip_uid} {vip_tier} {vip_days}d")
                result_msg = f"✅ VIP выдан: {vip_user} ({vip_uid}) на {vip_days} дней"
                if _bot:
                    try:
                        await _bot.send_message(vip_uid,
                            f"⭐ <b>Поздравляем!</b>\n\n"
                            f"Вам выдан VIP-статус <b>{vip_tier}</b> на <b>{vip_days} дней</b>.\n"
                            f"{('📝 ' + vip_note) if vip_note else ''}",
                            parse_mode="HTML")
                    except: pass
            except Exception as e:
                result_msg = f"❌ {e}"; result_ok = False

        elif action == "vip_revoke":
            vip_uid = int(data.get("vip_uid",0))
            _vip_revoke(vip_uid)
            _log_admin_db(uid_s, "VIP_REVOKE", str(vip_uid))
            result_msg = f"✅ VIP отозван: {vip_uid}"

        # ── Сеть чатов — глобальный бан ──────────────
        elif action == "netban_add":
            try:
                nb_uid    = int(data.get("nb_uid",0))
                nb_reason = data.get("nb_reason","Нарушение").strip()
                banned    = await _netban_apply(nb_uid, nb_reason, uid_s)
                _log_admin_db(uid_s, "NETBAN_ADD", f"{nb_uid}: {nb_reason}")
                result_msg = f"✅ Глобальный бан: {nb_uid} ({banned} чатов)"
                await push_notify("new_alert",
                    f"🌐 <b>Глобальный бан</b>\n"
                    f"ID: <code>{nb_uid}</code>\nПричина: {nb_reason}")
            except Exception as e:
                result_msg = f"❌ {e}"; result_ok = False

        elif action == "netban_remove":
            nb_uid = int(data.get("nb_uid",0))
            await _netban_remove(nb_uid)
            _log_admin_db(uid_s, "NETBAN_REMOVE", str(nb_uid))
            result_msg = f"✅ Глобальный бан снят: {nb_uid}"

        # ── Бэкап ─────────────────────────────────────
        elif action == "backup_now":
            res = await _do_backup(manual=True)
            if res["ok"]:
                result_msg = f"✅ Бэкап создан: {res['filename']} ({res['size']//1024} КБ)"
            else:
                result_msg = f"❌ Ошибка: {res['error']}"; result_ok = False

        # ── Завершить все сессии ──────────────────────
        elif action == "kill_all_sessions":
            my_token = request.cookies.get("dsess_token","")
            killed = 0
            for k in list(_dashboard_sessions.keys()):
                if k != my_token:
                    del _dashboard_sessions[k]
                    killed += 1
            _log_admin_db(uid_s, "KILL_SESSIONS", f"Killed {killed}")
            result_msg = f"✅ Завершено {killed} сессий"

        # ── Push настройки владельца ──────────────────
        elif action == "save_push":
            target_uid = int(data.get("push_uid", uid_s))
            _push_save_settings(target_uid, {
                "new_ticket": 1 if data.get("p_ticket") else 0,
                "new_alert":  1 if data.get("p_alert")  else 0,
                "new_report": 1 if data.get("p_report") else 0,
                "new_appeal": 1 if data.get("p_appeal") else 0,
                "quiet_start": data.get("quiet_start","23:00"),
                "quiet_end":   data.get("quiet_end","08:00"),
            })
            result_msg = "✅ Настройки push сохранены"

        raise web.HTTPFound("/dashboard/owner")

    # ── Данные для страницы ───────────────────────────
    admins     = _get_all_admins()
    vip_list   = _vip_get_all()
    netban_list= _netban_get_all()
    on_duty    = get_all_on_duty()
    sessions_n = len(_dashboard_sessions)
    online_n   = shared.get_online_count()
    alerts_n   = len(shared.alerts)

    # Бэкап-лог
    backup_rows = []
    try:
        conn = db.get_conn()
        backup_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM backup_log ORDER BY created_at DESC LIMIT 10").fetchall()]
        conn.close()
    except: pass

    # Статистика команды за месяц
    team_stats = []
    for adm in admins:
        if adm["tg_uid"] == OWNER_TG_ID: continue
        st = _get_mod_stats(adm["name"], adm["tg_uid"])
        total = st["bans"] + st["warns"] + st["mutes"] + st["tickets"]
        team_stats.append({**adm, "total": total, "stats": st})
    team_stats.sort(key=lambda x: x["total"], reverse=True)

    # Рейтинговые медали
    medals = ["🥇","🥈","🥉"] + [""] * 20

    # HTML VIP
    vip_html = ""
    for v in vip_list[:15]:
        import datetime as _dt
        exp  = v.get("expires_at","")[:10]
        try:
            exp_dt = _dt.datetime.strptime(exp, "%Y-%m-%d")
            days_left = (exp_dt - _dt.datetime.now()).days
            if days_left < 0:   status_cls, status_txt = "badge-danger", "Истёк"
            elif days_left < 7: status_cls, status_txt = "badge-open",   f"{days_left}д"
            else:               status_cls, status_txt = "badge-closed",  f"{days_left}д"
        except:
            status_cls, status_txt = "badge-accent", exp
        tier_icon = {"premium":"💎","standard":"⭐","trial":"🔰"}.get(v.get("tier","standard"),"⭐")
        vip_html += (
            f'<tr><td><code>{v["uid"]}</code></td>'
            f'<td>{tier_icon} {v.get("username") or "—"}</td>'
            f'<td><span class="badge {status_cls}">{status_txt}</span></td>'
            f'<td style="font-size:11px;color:var(--t2);">{v.get("note","")[:30]}</td>'
            f'<td><form method="POST" style="display:inline;">'
            f'<input type="hidden" name="action" value="vip_revoke">'
            f'<input type="hidden" name="vip_uid" value="{v["uid"]}">'
            f'<button class="btn btn-xs btn-danger" type="submit">✕</button></form></td></tr>'
        )
    if not vip_html:
        vip_html = '<tr><td colspan="5" style="text-align:center;color:var(--t2);padding:20px;">Нет VIP участников</td></tr>'

    # HTML NetBan
    netban_html = ""
    for nb in netban_list[:15]:
        netban_html += (
            f'<tr><td><code>{nb["uid"]}</code></td>'
            f'<td>{nb.get("reason","—")[:40]}</td>'
            f'<td style="font-size:11px;color:var(--t2);">{str(nb.get("banned_at",""))[:10]}</td>'
            f'<td><form method="POST" style="display:inline;">'
            f'<input type="hidden" name="action" value="netban_remove">'
            f'<input type="hidden" name="nb_uid" value="{nb["uid"]}">'
            f'<button class="btn btn-xs btn-success" type="submit">🕊</button></form></td></tr>'
        )
    if not netban_html:
        netban_html = '<tr><td colspan="4" style="text-align:center;color:var(--t2);padding:20px;">Список пуст</td></tr>'

    # HTML команда
    team_html = ""
    for i, adm in enumerate(team_stats[:10]):
        ri    = DASHBOARD_RANKS.get(adm["rank"], DASHBOARD_RANKS[1])
        medal = medals[i]
        team_html += (
            f'<tr>'
            f'<td style="font-size:16px;">{medal or str(i+1)}</td>'
            f'<td style="font-weight:600;">{adm["name"]}</td>'
            f'<td><span class="badge" style="background:rgba({_hex_to_rgb(ri["color"])},.15);color:{ri["color"]};">'
            f'{ri["name"]}</span></td>'
            f'<td style="font-family:\'Space Mono\',monospace;color:var(--acc);font-weight:700;">{adm["total"]}</td>'
            f'<td style="font-size:11px;color:var(--t2);">'
            f'🔨{adm["stats"]["bans"]} ⚡{adm["stats"]["warns"]} 🔇{adm["stats"]["mutes"]}</td>'
            f'</tr>'
        )

    # HTML бэкапы
    backup_html = ""
    for b in backup_rows:
        sz = f"{b.get('size_bytes',0)//1024} КБ"
        backup_html += (
            f'<tr><td style="font-size:11px;font-family:monospace;">{b.get("filename","?")}</td>'
            f'<td style="font-size:11px;">{sz}</td>'
            f'<td style="font-size:11px;color:var(--t2);">{str(b.get("created_at",""))[:16]}</td>'
            f'<td><span class="badge badge-closed">✓</span></td></tr>'
        )
    if not backup_html:
        backup_html = '<tr><td colspan="4" style="text-align:center;color:var(--t2);padding:20px;">Бэкапов ещё нет</td></tr>'

    result_html = ""
    if result_msg:
        color = "var(--acc)" if result_ok else "var(--red)"
        result_html = (
            f'<div style="background:rgba(0,229,160,.08);border-left:3px solid {color};'
            f'padding:12px 16px;border-radius:0 8px 8px 0;margin-bottom:20px;font-size:13px;">'
            f'{result_msg}</div>'
        )

    body = navbar(sess, "owner") + f"""
    <div class="container">
      <div class="page-title">
        👑 Панель владельца
        <span style="font-size:12px;color:var(--ylw);font-family:monospace;
          background:rgba(251,191,36,.1);padding:3px 10px;border-radius:20px;margin-left:8px;">
          РАНГ {OWNER_RANK} · ТОЛЬКО ДЛЯ ВАС
        </span>
      </div>

      {result_html}

      <!-- ── Статы ── -->
      <div class="cards" style="grid-template-columns:repeat(6,1fr);margin-bottom:20px;">
        <div class="card" style="--c:var(--acc);">
          <span class="card-icon">👥</span>
          <div class="card-value">{len(admins)}</div>
          <div class="card-label">Администраторов</div>
        </div>
        <div class="card" style="--c:var(--blue);">
          <span class="card-icon">💬</span>
          <div class="card-value" id="ow-chats">...</div>
          <div class="card-label">Чатов</div>
        </div>
        <div class="card" style="--c:var(--ylw);">
          <span class="card-icon">🟢</span>
          <div class="card-value">{len(on_duty)}</div>
          <div class="card-label">Дежурных</div>
        </div>
        <div class="card" style="--c:var(--pur);">
          <span class="card-icon">🖥</span>
          <div class="card-value">{sessions_n}</div>
          <div class="card-label">Сессий</div>
        </div>
        <div class="card" style="--c:var(--red);">
          <span class="card-icon">🚨</span>
          <div class="card-value">{alerts_n}</div>
          <div class="card-label">Алертов</div>
        </div>
        <div class="card" style="--c:var(--cyan);">
          <span class="card-icon">⭐</span>
          <div class="card-value">{len(vip_list)}</div>
          <div class="card-label">VIP</div>
        </div>
      </div>

      <div class="grid-2">
        <!-- ── Безопасность ── -->
        <div class="section">
          <div class="section-header">🔒 Безопасность системы</div>
          <div class="section-body">
            <div style="display:flex;flex-direction:column;gap:10px;margin-bottom:16px;">
              <div style="display:flex;align-items:center;justify-content:space-between;font-size:13px;">
                <span>Активных сессий дашборда</span>
                <span style="font-family:monospace;color:var(--acc);">{sessions_n}</span>
              </div>
              <div style="display:flex;align-items:center;justify-content:space-between;font-size:13px;">
                <span>Пользователей онлайн</span>
                <span style="font-family:monospace;color:var(--acc);">{online_n}</span>
              </div>
              <div style="display:flex;align-items:center;justify-content:space-between;font-size:13px;">
                <span>В глобальном бан-листе</span>
                <span style="font-family:monospace;color:var(--red);">{len(netban_list)}</span>
              </div>
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap;">
              <form method="POST">
                <input type="hidden" name="action" value="kill_all_sessions">
                <button class="btn btn-danger btn-sm" type="submit"
                  onclick="return confirm('Завершить ВСЕ сессии кроме текущей?')">
                  🔴 Завершить все сессии
                </button>
              </form>
              <a href="/dashboard/export" class="btn btn-ghost btn-sm">📋 Лог доступа</a>
            </div>
          </div>
        </div>

        <!-- ── Управление системой ── -->
        <div class="section">
          <div class="section-header">⚙️ Управление системой</div>
          <div class="section-body">
            <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;">
              <form method="POST">
                <input type="hidden" name="action" value="backup_now">
                <button class="btn btn-success btn-sm" type="submit">💾 Бэкап сейчас</button>
              </form>
              <a href="/dashboard/bot_control" class="btn btn-ghost btn-sm">🤖 Управление ботом</a>
              <a href="/dashboard/settings" class="btn btn-ghost btn-sm">🔧 Настройки</a>
            </div>
            <div style="font-size:11px;color:var(--t2);margin-bottom:8px;text-transform:uppercase;letter-spacing:1px;">Токен дашборда</div>
            <div style="font-family:monospace;font-size:12px;
              background:var(--bg3);padding:8px 12px;border-radius:8px;
              display:flex;align-items:center;justify-content:space-between;">
              <span id="tok-val">••••••••••••••••</span>
              <button class="btn btn-xs btn-ghost" onclick="toggleToken()">👁</button>
            </div>
          </div>
        </div>
      </div>

      <!-- ── Рейтинг команды ── -->
      <div class="section" style="margin-bottom:16px;">
        <div class="section-header">📊 Рейтинг команды (всё время)</div>
        <table>
          <thead><tr>
            <th style="width:40px;">#</th>
            <th>Имя</th><th>Ранг</th>
            <th>Очки</th><th>Детали</th>
          </tr></thead>
          <tbody>{team_html or '<tr><td colspan="5" style="text-align:center;color:var(--t2);padding:20px;">Нет данных</td></tr>'}</tbody>
        </table>
      </div>

      <div class="grid-2">
        <!-- ── VIP ── -->
        <div class="section">
          <div class="section-header">⭐ VIP участники</div>
          <div style="padding:14px 16px;border-bottom:1px solid var(--br0);">
            <form method="POST" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
              <input type="hidden" name="action" value="vip_grant">
              <div>
                <div class="form-group" style="margin-bottom:8px;">
                  <label>Telegram ID</label>
                  <input class="form-control" name="vip_uid" type="number" placeholder="123456789" required>
                </div>
                <div class="form-group" style="margin-bottom:8px;">
                  <label>Username</label>
                  <input class="form-control" name="vip_username" placeholder="@username">
                </div>
              </div>
              <div>
                <div class="form-group" style="margin-bottom:8px;">
                  <label>Тип VIP</label>
                  <select class="form-control" name="vip_tier">
                    <option value="trial">🔰 Trial (тест)</option>
                    <option value="standard" selected>⭐ Standard</option>
                    <option value="premium">💎 Premium</option>
                  </select>
                </div>
                <div class="form-group" style="margin-bottom:8px;">
                  <label>Дней</label>
                  <input class="form-control" name="vip_days" type="number" value="30" min="1" max="3650">
                </div>
              </div>
              <div style="grid-column:1/-1;">
                <div class="form-group" style="margin-bottom:8px;">
                  <label>Заметка (опционально)</label>
                  <input class="form-control" name="vip_note" placeholder="Причина выдачи...">
                </div>
                <button class="btn btn-primary btn-sm" type="submit" style="width:100%;">⭐ Выдать VIP</button>
              </div>
            </form>
          </div>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>ID</th><th>Username</th><th>Срок</th><th>Заметка</th><th></th></tr></thead>
              <tbody>{vip_html}</tbody>
            </table>
          </div>
        </div>

        <!-- ── Сеть чатов ── -->
        <div class="section">
          <div class="section-header">🌐 Глобальный бан-лист</div>
          <div style="padding:14px 16px;border-bottom:1px solid var(--br0);">
            <form method="POST" style="display:flex;flex-direction:column;gap:8px;">
              <input type="hidden" name="action" value="netban_add">
              <div class="form-group" style="margin-bottom:0;">
                <label>Telegram ID</label>
                <input class="form-control" name="nb_uid" type="number" placeholder="ID пользователя" required>
              </div>
              <div class="form-group" style="margin-bottom:0;">
                <label>Причина</label>
                <input class="form-control" name="nb_reason" placeholder="Причина бана во всех чатах" required>
              </div>
              <button class="btn btn-danger btn-sm" type="submit"
                onclick="return confirm('Забанить во ВСЕХ чатах сети?')">
                🚫 Глобальный бан
              </button>
            </form>
          </div>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>ID</th><th>Причина</th><th>Дата</th><th></th></tr></thead>
              <tbody>{netban_html}</tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- ── Авто-бэкапы ── -->
      <div class="section" style="margin-bottom:16px;">
        <div class="section-header">💾 История бэкапов</div>
        <div style="overflow-x:auto;">
          <table>
            <thead><tr><th>Файл</th><th>Размер</th><th>Дата</th><th>Статус</th></tr></thead>
            <tbody>{backup_html}</tbody>
          </table>
        </div>
      </div>

      <!-- ── Push настройки ── -->
      <div class="section" style="margin-bottom:16px;">
        <div class="section-header">🔔 Push-уведомления (мои настройки)</div>
        <div class="section-body">
          <form method="POST">
            <input type="hidden" name="action" value="save_push">
            <input type="hidden" name="push_uid" value="{sess.get('uid', OWNER_TG_ID)}">
            <div class="grid-2" style="gap:16px;">
              <div>
                <div style="font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;">Получать уведомления о:</div>
                {"".join(f'''<div class="toggle-wrap">
                  <div class="toggle-info"><b>{lbl}</b></div>
                  <label class="toggle-switch">
                    <input type="checkbox" name="{key}" value="1"
                      {"checked" if _push_get_settings(sess.get("uid",OWNER_TG_ID)).get(key.replace("p_","new_"),1) else ""}>
                    <span class="toggle-slider"></span>
                  </label>
                </div>''' for key, lbl in [("p_ticket","Новые тикеты"),("p_alert","Критические алерты"),("p_report","Новые репорты"),("p_appeal","Апелляции")])}
              </div>
              <div>
                <div style="font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;">Тихий час (не беспокоить):</div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                  <div class="form-group"><label>С</label>
                    <input class="form-control" type="time" name="quiet_start"
                      value="{_push_get_settings(sess.get("uid",OWNER_TG_ID)).get("quiet_start","23:00")}">
                  </div>
                  <div class="form-group"><label>До</label>
                    <input class="form-control" type="time" name="quiet_end"
                      value="{_push_get_settings(sess.get("uid",OWNER_TG_ID)).get("quiet_end","08:00")}">
                  </div>
                </div>
              </div>
            </div>
            <button class="btn btn-primary btn-sm" type="submit" style="margin-top:12px;">💾 Сохранить</button>
          </form>
        </div>
      </div>

    </div>

    <script>
    fetch('/api/live').then(r=>r.json()).then(d=>{{
      if(d.online!==undefined){{
        var el=document.getElementById('ow-chats');
        if(el) el.textContent=d.online;
      }}
    }}).catch(()=>{{}});
    // Загружаем кол-во чатов
    fetch('/api/bot/chats').then(r=>r.json()).then(d=>{{
      var el=document.getElementById('ow-chats');
      if(el&&d.chats) el.textContent=d.chats.length;
    }}).catch(()=>{{}});
    // Показать/скрыть токен
    var _tokVisible=false;
    function toggleToken(){{
      var el=document.getElementById('tok-val');
      _tokVisible=!_tokVisible;
      el.textContent=_tokVisible?'{DASHBOARD_TOKEN}':'••••••••••••••••';
    }}
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_owner_panel = require_auth("view_overview")(handle_owner_panel)


# ══════════════════════════════════════════════════════════════════
#  💰 ДАШБОРД ЭКОНОМИКИ
# ══════════════════════════════════════════════════════════════════
async def handle_economy_dashboard(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    result_msg = ""
    result_ok  = True

    if request.method == "POST":
        data   = await request.post()
        action = data.get("action","")
        uid_s  = sess.get("uid",0) if sess else 0

        if action == "give_coins":
            try:
                target_uid = int(data.get("target_uid",0))
                amount     = int(data.get("amount",0))
                chat_id    = int(data.get("chat_id",0))
                # Меняем через shared если возможно, иначе через БД
                conn = db.get_conn()
                conn.execute("""INSERT INTO reputation (cid,uid,score)
                    VALUES (?,?,?) ON CONFLICT(cid,uid)
                    DO UPDATE SET score=score+?""",
                    (chat_id, target_uid, amount, amount))
                conn.commit(); conn.close()
                _log_admin_db(uid_s, "ECONOMY_GIVE",
                              f"uid={target_uid} cid={chat_id} amount={amount}")
                result_msg = f"✅ Выдано {amount:+d} реп. пользователю {target_uid}"
                if _bot and amount != 0:
                    try:
                        action_txt = f"выдал +{amount}" if amount > 0 else f"снял {amount}"
                        await _bot.send_message(target_uid,
                            f"💰 Администратор {action_txt} репутации.",
                            parse_mode="HTML")
                    except: pass
            except Exception as e:
                result_msg = f"❌ {e}"; result_ok = False

        elif action == "reset_economy":
            try:
                chat_id = int(data.get("reset_cid",0))
                conn = db.get_conn()
                conn.execute("DELETE FROM reputation WHERE cid=?", (chat_id,))
                conn.commit(); conn.close()
                _log_admin_db(uid_s, "ECONOMY_RESET", str(chat_id))
                result_msg = f"✅ Экономика чата {chat_id} сброшена"
            except Exception as e:
                result_msg = f"❌ {e}"; result_ok = False

        raise web.HTTPFound("/dashboard/economy")

    # ── Данные ──────────────────────────────────────────
    chats = []
    try: chats = [dict(r) for r in await db.get_all_chats()]
    except: pass
    chat_id_filter = int(request.rel_url.query.get("cid", 0) or 0)
    if not chat_id_filter and chats:
        chat_id_filter = chats[0]["cid"]

    # Топ богачей
    top_rich = []
    try:
        conn = db.get_conn()
        q = "SELECT cid,uid,score FROM reputation WHERE score>0"
        if chat_id_filter:
            q += f" AND cid={chat_id_filter}"
        q += " ORDER BY score DESC LIMIT 10"
        top_rich = [dict(r) for r in conn.execute(q).fetchall()]
        conn.close()
    except: pass

    # История покупок
    purchases_7  = []
    purchases_30 = []
    pop_items    = []
    try:
        conn = db.get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS shop_purchases
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
             cid INTEGER, uid INTEGER, item_name TEXT, price INTEGER,
             purchased_at TEXT DEFAULT (datetime('now')))""")
        purchases_7 = [dict(r) for r in conn.execute(
            "SELECT * FROM shop_purchases WHERE purchased_at >= datetime('now','-7 days')"
            + (f" AND cid={chat_id_filter}" if chat_id_filter else "")
            + " ORDER BY purchased_at DESC LIMIT 50").fetchall()]
        purchases_30 = conn.execute(
            "SELECT COUNT(*) FROM shop_purchases WHERE purchased_at >= datetime('now','-30 days')"
            + (f" AND cid={chat_id_filter}" if chat_id_filter else "")).fetchone()[0]
        pop_items = [dict(r) for r in conn.execute(
            "SELECT item_name, COUNT(*) as cnt, SUM(price) as total_spent "
            "FROM shop_purchases "
            + (f"WHERE cid={chat_id_filter} " if chat_id_filter else "")
            + "GROUP BY item_name ORDER BY cnt DESC LIMIT 10").fetchall()]
        conn.close()
    except: pass

    # График движения монет (по дням за 30 дней)
    chart_labels = []
    chart_earned = []
    chart_spent  = []
    try:
        conn = db.get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS rep_log
            (uid INTEGER, cid INTEGER, delta INTEGER,
             reason TEXT, created_at TEXT DEFAULT (datetime('now')))""")
        for i in range(29, -1, -1):
            import datetime as _dt
            day = (_dt.datetime.now() - _dt.timedelta(days=i)).strftime("%Y-%m-%d")
            chart_labels.append(day[5:])  # MM-DD
            # Заработано (положительные)
            earned = conn.execute(
                "SELECT COALESCE(SUM(delta),0) FROM rep_log "
                "WHERE delta>0 AND date(created_at)=?"
                + (f" AND cid={chat_id_filter}" if chat_id_filter else ""),
                (day,)).fetchone()[0] or 0
            # Потрачено (покупки)
            spent = conn.execute(
                "SELECT COALESCE(SUM(price),0) FROM shop_purchases "
                "WHERE date(purchased_at)=?"
                + (f" AND cid={chat_id_filter}" if chat_id_filter else ""),
                (day,)).fetchone()[0] or 0
            chart_earned.append(earned)
            chart_spent.append(spent)
        conn.close()
    except:
        chart_labels = [f"День {i}" for i in range(1,31)]
        chart_earned = [0]*30
        chart_spent  = [0]*30

    # Общая статистика
    total_rep = 0
    total_purchases = len(purchases_7)
    try:
        conn = db.get_conn()
        total_rep = conn.execute(
            "SELECT COALESCE(SUM(score),0) FROM reputation"
            + (f" WHERE cid={chat_id_filter}" if chat_id_filter else "")).fetchone()[0] or 0
        conn.close()
    except: pass

    # HTML
    chat_opts = "".join(
        f'<option value="{c["cid"]}" {"selected" if c["cid"]==chat_id_filter else ""}>'
        f'{c.get("title","") or c["cid"]}</option>'
        for c in chats)

    result_html = ""
    if result_msg:
        col = "var(--acc)" if result_ok else "var(--red)"
        result_html = (f'<div style="background:rgba(0,229,160,.08);border-left:3px solid {col};'
                       f'padding:12px 16px;border-radius:0 8px 8px 0;margin-bottom:20px;">'
                       f'{result_msg}</div>')

    # Топ богачей HTML
    rich_html = ""
    medals = ["🥇","🥈","🥉"] + [""] * 20
    for i, r in enumerate(top_rich):
        rich_html += (
            f'<tr><td style="font-size:15px;">{medals[i] or str(i+1)}</td>'
            f'<td><code>{r["uid"]}</code></td>'
            f'<td style="font-family:monospace;color:var(--acc);font-weight:700;">'
            f'{r["score"]:+d}</td>'
            f'<td><form method="POST" style="display:inline;gap:4px;display:flex;">'
            f'<input type="hidden" name="action" value="give_coins">'
            f'<input type="hidden" name="target_uid" value="{r["uid"]}">'
            f'<input type="hidden" name="chat_id" value="{r["cid"]}">'
            f'<input class="form-control" name="amount" type="number" value="100" '
            f'style="width:80px;padding:4px 8px;font-size:11px;">'
            f'<button class="btn btn-xs btn-success" type="submit">✅</button>'
            f'</form></td></tr>'
        )
    if not rich_html:
        rich_html = '<tr><td colspan="4" style="text-align:center;color:var(--t2);padding:20px;">Нет данных</td></tr>'

    # Топ товаров HTML
    items_html = ""
    for item in pop_items:
        pct = int(item["cnt"] / max(total_purchases, 1) * 100)
        items_html += (
            f'<tr><td style="font-weight:500;">{item["item_name"]}</td>'
            f'<td style="font-family:monospace;">{item["cnt"]}</td>'
            f'<td style="font-family:monospace;color:var(--red);">'
            f'{item["total_spent"] or 0}</td>'
            f'<td><div style="height:6px;background:var(--bg4);border-radius:3px;width:100px;">'
            f'<div style="height:6px;width:{pct}%;background:var(--acc);border-radius:3px;"></div>'
            f'</div></td></tr>'
        )
    if not items_html:
        items_html = '<tr><td colspan="4" style="text-align:center;color:var(--t2);padding:20px;">Покупок нет</td></tr>'

    # Последние покупки HTML
    purch_html = ""
    for p in purchases_7[:20]:
        purch_html += (
            f'<tr><td style="font-size:11px;color:var(--t2);">{str(p.get("purchased_at",""))[:16]}</td>'
            f'<td><code>{p.get("uid","?")}</code></td>'
            f'<td>{p.get("item_name","?")}</td>'
            f'<td style="color:var(--red);font-family:monospace;">'
            f'-{p.get("price",0)}</td></tr>'
        )
    if not purch_html:
        purch_html = '<tr><td colspan="4" style="text-align:center;color:var(--t2);padding:20px;">Покупок нет</td></tr>'

    import json as _json
    chart_labels_j = _json.dumps(chart_labels)
    chart_earned_j = _json.dumps(chart_earned)
    chart_spent_j  = _json.dumps(chart_spent)

    body = navbar(sess, "economy") + f"""
    <div class="container">
      <div class="page-title">💰 Экономика</div>

      {result_html}

      <!-- Фильтр чата -->
      <div style="margin-bottom:20px;display:flex;gap:10px;align-items:center;">
        <select class="form-control" style="max-width:280px;"
          onchange="window.location='/dashboard/economy?cid='+this.value">
          <option value="0">Все чаты</option>
          {chat_opts}
        </select>
      </div>

      <!-- Статы -->
      <div class="cards" style="grid-template-columns:repeat(4,1fr);margin-bottom:20px;">
        <div class="card" style="--c:var(--acc);">
          <span class="card-icon">💰</span>
          <div class="card-value">{total_rep:,}</div>
          <div class="card-label">Всего репутации</div>
        </div>
        <div class="card" style="--c:var(--blue);">
          <span class="card-icon">🛒</span>
          <div class="card-value">{total_purchases}</div>
          <div class="card-label">Покупок за 7 дней</div>
        </div>
        <div class="card" style="--c:var(--pur);">
          <span class="card-icon">📦</span>
          <div class="card-value">{purchases_30}</div>
          <div class="card-label">Покупок за 30 дней</div>
        </div>
        <div class="card" style="--c:var(--ylw);">
          <span class="card-icon">👑</span>
          <div class="card-value">{len(top_rich)}</div>
          <div class="card-label">Участников с реп.</div>
        </div>
      </div>

      <!-- График -->
      <div class="section" style="margin-bottom:16px;">
        <div class="section-header">📈 Движение репутации (30 дней)</div>
        <div style="padding:16px;height:220px;">
          <canvas id="ecoChart"></canvas>
        </div>
      </div>

      <div class="grid-2">
        <!-- Топ богачей -->
        <div class="section">
          <div class="section-header">🏆 Топ-10 богачей</div>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>#</th><th>ID</th><th>Репутация</th><th>Изменить</th></tr></thead>
              <tbody>{rich_html}</tbody>
            </table>
          </div>
        </div>

        <!-- Топ товаров -->
        <div class="section">
          <div class="section-header">🛍 Популярные товары</div>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>Товар</th><th>Продаж</th><th>Выручка</th><th>%</th></tr></thead>
              <tbody>{items_html}</tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- Управление экономикой -->
      <div class="grid-2">
        <div class="section">
          <div class="section-header">⚙️ Выдать / снять репутацию</div>
          <div class="section-body">
            <form method="POST">
              <input type="hidden" name="action" value="give_coins">
              <div class="form-group">
                <label>Telegram ID пользователя</label>
                <input class="form-control" name="target_uid" type="number" placeholder="123456789" required>
              </div>
              <div class="form-group">
                <label>Чат</label>
                <select class="form-control" name="chat_id">
                  {chat_opts}
                </select>
              </div>
              <div class="form-group">
                <label>Количество (минус = снять)</label>
                <input class="form-control" name="amount" type="number" value="100" placeholder="+100 или -50">
              </div>
              <button class="btn btn-primary" type="submit" style="width:100%;">💰 Применить</button>
            </form>
          </div>
        </div>

        <div class="section">
          <div class="section-header">🗑 Сброс экономики чата</div>
          <div class="section-body">
            <p style="font-size:13px;color:var(--t2);margin-bottom:16px;">
              Сбрасывает всю репутацию участников выбранного чата до нуля.
              <strong style="color:var(--red);">Действие необратимо!</strong>
            </p>
            <form method="POST" onsubmit="return confirm('Сбросить экономику? Это удалит ВСЮ репутацию чата!')">
              <input type="hidden" name="action" value="reset_economy">
              <div class="form-group">
                <label>Выбери чат</label>
                <select class="form-control" name="reset_cid">
                  {chat_opts}
                </select>
              </div>
              <button class="btn btn-danger" type="submit" style="width:100%;">🗑 Сбросить экономику</button>
            </form>
          </div>
        </div>
      </div>

      <!-- История покупок -->
      <div class="section">
        <div class="section-header">📋 История покупок (последние 7 дней)</div>
        <div style="overflow-x:auto;">
          <table>
            <thead><tr><th>Дата</th><th>Пользователь</th><th>Товар</th><th>Цена</th></tr></thead>
            <tbody>{purch_html}</tbody>
          </table>
        </div>
      </div>
    </div>

    <script>
    (function(){{
      var ctx = document.getElementById('ecoChart').getContext('2d');
      new Chart(ctx, {{
        type: 'line',
        data: {{
          labels: {chart_labels_j},
          datasets: [
            {{
              label: 'Заработано',
              data: {chart_earned_j},
              borderColor: '#00e5a0',
              backgroundColor: 'rgba(0,229,160,.08)',
              fill: true, tension: 0.4, pointRadius: 2,
            }},
            {{
              label: 'Потрачено',
              data: {chart_spent_j},
              borderColor: '#f87171',
              backgroundColor: 'rgba(248,113,113,.08)',
              fill: true, tension: 0.4, pointRadius: 2,
            }}
          ]
        }},
        options: {{
          responsive: true, maintainAspectRatio: false,
          plugins: {{ legend: {{ labels: {{ color: '#7b8cad', font: {{ size: 11 }} }} }} }},
          scales: {{
            x: {{ ticks: {{ color: '#7b8cad', font: {{ size: 10 }}, maxTicksLimit: 10 }} }},
            y: {{ ticks: {{ color: '#7b8cad', font: {{ size: 10 }} }} }}
          }}
        }}
      }});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_economy_dashboard = require_auth("view_overview")(handle_economy_dashboard)


# ══════════════════════════════════════════════════════════
#  ВОССТАНОВЛЕННЫЕ ХЕНДЛЕРЫ СТРАНИЦ
# ══════════════════════════════════════════════════════════

async def handle_chats(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    chats = [dict(r) for r in await db.get_all_chats()]
    rows = ""
    for c in chats:
        cid = c["cid"]; title = c.get("title") or str(cid)
        conn = db.get_conn()
        msgs  = conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        users = conn.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        bans  = conn.execute("SELECT COUNT(*) FROM ban_list WHERE cid=?", (cid,)).fetchone()[0] or 0
        conn.close()
        rows += f"<tr><td><a href='/dashboard/chats/{cid}' style='color:var(--acc);text-decoration:none;font-weight:600;'>{title}</a><br><small style='color:var(--t3);font-family:monospace;'>{cid}</small></td><td style='font-family:Space Mono,monospace;color:var(--blue);'>{users:,}</td><td style='font-family:Space Mono,monospace;color:var(--pur);'>{msgs:,}</td><td style='font-family:Space Mono,monospace;color:var(--red);'>{bans}</td><td><a href='/dashboard/chats/{cid}' class='btn btn-xs btn-ghost'>→</a></td></tr>"
    body = navbar(sess, "chats") + f"""
    <div class="container">
      <div class="page-title">💬 Чаты <span style="font-size:14px;color:var(--t2);font-weight:400;">({len(chats)} подключено)</span></div>
      <div class="section">
        <table><thead><tr><th>Чат</th><th>👥 Участников</th><th>💬 Сообщений</th><th>🔨 Банов</th><th></th></tr></thead>
        <tbody>{rows or "<tr><td colspan='5' class='empty-state'>Нет чатов</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_chats = require_auth("view_chats")(handle_chats)

async def handle_chat_detail(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    cid = int(request.match_info.get("cid", 0))
    conn = db.get_conn()
    chat = conn.execute("SELECT * FROM chats WHERE cid=?", (cid,)).fetchone()
    title = dict(chat)["title"] if chat else str(cid)
    stats = conn.execute("SELECT uid, msg_count FROM chat_stats WHERE cid=? ORDER BY msg_count DESC LIMIT 20", (cid,)).fetchall()
    bans  = conn.execute("SELECT * FROM ban_list WHERE cid=? LIMIT 50", (cid,)).fetchall()
    warns = conn.execute("SELECT uid, count FROM warnings WHERE cid=? ORDER BY count DESC LIMIT 20", (cid,)).fetchall()
    conn.close()
    rows = "".join(f"<tr><td><code>{r['uid']}</code></td><td style='font-family:Space Mono,monospace;color:var(--blue);'>{r['msg_count']}</td></tr>" for r in stats)
    warn_rows = "".join(f"<tr><td><code>{r['uid']}</code></td><td style='color:var(--ylw);font-weight:700;'>{r['count']}</td></tr>" for r in warns)
    body = navbar(sess, "chats") + f"""
    <div class="container">
      <div class="page-title">💬 {title}</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
        <div class="section"><div class="section-header">📊 Топ активных</div>
          <table><thead><tr><th>ID</th><th>Сообщений</th></tr></thead>
          <tbody>{rows or "<tr><td colspan='2' class='empty-state'>Нет данных</td></tr>"}</tbody></table></div>
        <div class="section"><div class="section-header">⚡ Варны</div>
          <table><thead><tr><th>ID</th><th>Варнов</th></tr></thead>
          <tbody>{warn_rows or "<tr><td colspan='2' class='empty-state'>Нет варнов</td></tr>"}</tbody></table></div>
      </div>
      <div class="section" style="margin-top:16px;"><div class="section-header">🔨 Баны ({len(bans)})</div>
        <table><thead><tr><th>ID</th><th>Причина</th></tr></thead>
        <tbody>{"".join(f"<tr><td><code>{r['uid']}</code></td><td>{r['reason'] or '—'}</td></tr>" for r in bans) or "<tr><td colspan='2' class='empty-state'>Нет банов</td></tr>"}</tbody></table></div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_chat_detail = require_auth("view_chats")(handle_chat_detail)

async def handle_users(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    conn = db.get_conn()
    users = conn.execute("SELECT uid, SUM(msg_count) as msgs FROM chat_stats GROUP BY uid ORDER BY msgs DESC LIMIT 100").fetchall()
    conn.close()
    rows = "".join(f"<tr><td><code>{r['uid']}</code></td><td style='font-family:Space Mono,monospace;color:var(--pur);'>{r['msgs']:,}</td></tr>" for r in users)
    body = navbar(sess, "users") + f"""
    <div class="container">
      <div class="page-title">👥 Пользователи</div>
      <div class="section">
        <table><thead><tr><th>ID</th><th>Сообщений</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='2' class='empty-state'>Нет данных</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_users = require_auth("view_users")(handle_users)

async def handle_user_detail(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    uid = int(request.match_info.get("uid", 0))
    conn = db.get_conn()
    msgs  = conn.execute("SELECT SUM(msg_count) FROM chat_stats WHERE uid=?", (uid,)).fetchone()[0] or 0
    warns = conn.execute("SELECT cid, count FROM warnings WHERE uid=?", (uid,)).fetchall()
    bans  = conn.execute("SELECT cid, reason FROM ban_list WHERE uid=?", (uid,)).fetchall()
    hist  = conn.execute("SELECT action, reason, by_name, created_at FROM mod_history WHERE uid=? ORDER BY created_at DESC LIMIT 30", (uid,)).fetchall()
    conn.close()
    hist_rows = "".join(f"<tr><td style='font-size:11px;color:var(--t3);white-space:nowrap;'>{r['created_at'][:16]}</td><td style='font-weight:600;'>{r['action']}</td><td>{r['reason'] or '—'}</td><td style='color:var(--t2);'>{r['by_name']}</td></tr>" for r in hist)
    body = navbar(sess, "users") + f"""
    <div class="container">
      <div class="page-title">👤 Пользователь <code style="font-size:18px;">{uid}</code></div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px;">
        <div class="card" style="text-align:center;"><div style="font-size:24px;font-weight:900;color:var(--pur);">{msgs:,}</div><div style="font-size:11px;color:var(--t2);">Сообщений</div></div>
        <div class="card" style="text-align:center;"><div style="font-size:24px;font-weight:900;color:var(--ylw);">{sum(r["count"] for r in warns)}</div><div style="font-size:11px;color:var(--t2);">Варнов</div></div>
        <div class="card" style="text-align:center;"><div style="font-size:24px;font-weight:900;color:var(--red);">{len(bans)}</div><div style="font-size:11px;color:var(--t2);">Банов</div></div>
      </div>
      <div class="section"><div class="section-header">📋 История действий</div>
        <table><thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Кто</th></tr></thead>
        <tbody>{hist_rows or "<tr><td colspan='4' class='empty-state'>Нет записей</td></tr>"}</tbody></table></div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_user_detail = require_auth("view_users")(handle_user_detail)

async def handle_moderation(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    conn = db.get_conn()
    try:
        hist = conn.execute("SELECT action, reason, by_name, created_at FROM mod_history ORDER BY created_at DESC LIMIT 100").fetchall()
    except: hist = []
    conn.close()
    rows = "".join(
        f"<tr><td style='font-size:11px;color:var(--t3);white-space:nowrap;'>{r['created_at'][:16]}</td>"
        f"<td style='font-weight:600;color:var(--ylw);'>{r['action']}</td>"
        f"<td style='font-size:12px;'>{(r['reason'] or '—')[:40]}</td>"
        f"<td style='color:var(--t2);font-size:12px;'>{r['by_name']}</td></tr>" for r in hist)
    body = navbar(sess, "moderation") + f"""
    <div class="container">
      <div class="page-title">🛡 История модерации</div>
      <div class="section">
        <table><thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Модератор</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='4' class='empty-state'>Нет записей</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_moderation = require_auth("view_moderation")(handle_moderation)

async def handle_tickets(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    try:
        tickets = await db.get_all_tickets(limit=100)
    except: tickets = []
    status_color = {"open":"var(--green)","closed":"var(--t3)","pending":"var(--ylw)"}
    rows = ""
    for t in tickets:
        _st = t.get("status", "open")
        _sc = status_color.get(_st, "var(--t2)")
        _id = t["id"]
        rows += (
            f"<tr><td><b>#{_id}</b></td>"
            f"<td>{t.get('subject','—')[:40]}</td>"
            f"<td>{t.get('user_name','—')}</td>"
            f"<td><span style='color:{_sc};font-weight:600;'>{_st}</span></td>"
            f"<td><a href='/dashboard/tickets/{_id}' class='btn btn-xs btn-ghost'>→</a></td></tr>"
        )
    body = navbar(sess, "tickets") + f"""
    <div class="container">
      <div class="page-title">🎫 Тикеты</div>
      <div class="section">
        <table><thead><tr><th>#</th><th>Тема</th><th>Пользователь</th><th>Статус</th><th></th></tr></thead>
        <tbody>{rows or "<tr><td colspan='5' class='empty-state'>Нет тикетов</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_tickets = require_auth("view_tickets")(handle_tickets)

async def handle_ticket_detail(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    tid = int(request.match_info.get("id", 0))
    try:
        ticket = await db.get_ticket(tid)
        msgs   = await db.get_ticket_messages(tid)
    except: ticket = None; msgs = []
    if not ticket:
        raise web.HTTPFound("/dashboard/tickets")
    msgs_html = "".join(
        f"<div style='padding:10px 14px;border-bottom:1px solid var(--br0);'>"
        f"<div style='font-size:11px;color:var(--t3);margin-bottom:4px;'>{m.get('created_at','')[:16]} · {m.get('sender_name','—')}</div>"
        f"<div style='font-size:14px;'>{m.get('text','')}</div></div>" for m in msgs)
    body = navbar(sess, "tickets") + f"""
    <div class="container">
      <div class="page-title">🎫 Тикет #{tid} — {ticket.get('subject','—')}</div>
      <div class="section" style="margin-bottom:16px;">
        <div style="padding:14px 18px;display:flex;gap:20px;font-size:13px;">
          <span>👤 {ticket.get('user_name','—')}</span>
          <span>📊 Статус: <b>{ticket.get('status','—')}</b></span>
        </div>
        <div>{msgs_html or '<div class="empty-state" style="padding:20px;">Нет сообщений</div>'}</div>
      </div>
      <form method="POST" action="/dashboard/tickets/{tid}/reply" style="display:flex;gap:10px;">
        <input name="text" placeholder="Ответить..." style="flex:1;padding:10px 14px;border-radius:8px;border:1px solid var(--br1);background:var(--bg3);color:var(--t1);font-size:14px;">
        <button class="btn btn-primary" type="submit">Отправить</button>
      </form>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_ticket_detail = require_auth("view_tickets")(handle_ticket_detail)

async def handle_ticket_reply(request: web.Request):
    sess = _get_session(request)
    tid = int(request.match_info.get("id", 0))
    if request.method == "POST":
        data = await request.post()
        text = data.get("text","").strip()
        if text and sess:
            try: await db.add_ticket_message(tid, 0, sess.get("name","Admin"), text, "admin")
            except: pass
    raise web.HTTPFound(f"/dashboard/tickets/{tid}")
handle_ticket_reply = require_auth("view_tickets")(handle_ticket_reply)

async def handle_ticket_close_web(request: web.Request):
    tid = int(request.match_info.get("id", 0))
    try: await db.close_ticket(tid)
    except: pass
    raise web.HTTPFound(f"/dashboard/tickets/{tid}")
handle_ticket_close_web = require_auth("view_tickets")(handle_ticket_close_web)

async def handle_reports(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    reports = shared.get_reports()
    status_color = {"new":"var(--red)","reviewed":"var(--ylw)","closed":"var(--t3)"}
    rows = ""
    for r in reports:
        _st = r.get("status", "new")
        _sc = status_color.get(_st, "var(--t2)")
        rows += (
            f"<tr><td>{r.get('time','—')}</td>"
            f"<td>{r.get('reporter_name','—')}</td>"
            f"<td>{r.get('target_name','—')}</td>"
            f"<td style='font-size:12px;'>{(r.get('reason','—'))[:40]}</td>"
            f"<td><span style='color:{_sc};font-weight:600;'>{_st}</span></td></tr>"
        )
    body = navbar(sess, "reports") + f"""
    <div class="container">
      <div class="page-title">🚨 Репорты</div>
      <div class="section">
        <table><thead><tr><th>Время</th><th>От кого</th><th>На кого</th><th>Причина</th><th>Статус</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='5' class='empty-state'>Нет репортов</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_reports = require_auth("view_reports")(handle_reports)

async def handle_alerts(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    level_color = {"danger":"var(--red)","warn":"var(--ylw)","info":"var(--acc)"}
    rows = "".join(
        f"<tr><td style='font-size:11px;color:var(--t3);white-space:nowrap;'>{a['time']}</td>"
        f"<td><span style='color:{level_color.get(a["level"],"var(--t2)")};font-weight:700;'>{a['title']}</span></td>"
        f"<td style='font-size:12px;'>{a['desc'][:60]}</td></tr>"
        for a in shared.alerts)
    body = navbar(sess, "alerts") + f"""
    <div class="container">
      <div class="page-title">🔴 Алерты <span style="font-size:14px;color:var(--t2);font-weight:400;">({len(shared.alerts)})</span>
        <a href="/dashboard/alerts/clear" class="btn btn-sm btn-ghost" style="margin-left:auto;">Очистить</a>
      </div>
      <div class="section">
        <table><thead><tr><th>Время</th><th>Тип</th><th>Описание</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='3' class='empty-state'>Нет алертов</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_alerts = require_auth("view_alerts")(handle_alerts)

async def handle_alerts_clear(request: web.Request):
    shared.alerts.clear()
    raise web.HTTPFound("/dashboard/alerts")
handle_alerts_clear = require_auth("view_alerts")(handle_alerts_clear)

async def handle_media(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    rows = "".join(
        f"<tr><td style='font-size:11px;color:var(--t3);white-space:nowrap;'>{m['time']}</td>"
        f"<td>{m['name']}</td><td style='color:var(--acc);'>{m['type']}</td>"
        f"<td style='font-size:12px;'>{m['chat']}</td></tr>"
        for m in shared.media_log[:100])
    body = navbar(sess, "media") + f"""
    <div class="container">
      <div class="page-title">🎬 Медиа-лог</div>
      <div class="section">
        <table><thead><tr><th>Время</th><th>Пользователь</th><th>Тип</th><th>Чат</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='4' class='empty-state'>Нет медиа</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_media = require_auth("view_media")(handle_media)

async def handle_deleted(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    body = navbar(sess, "deleted") + """
    <div class="container">
      <div class="page-title">🗑 Удалённые сообщения</div>
      <div class="section"><div class="empty-state" style="padding:40px;">Функция в разработке</div></div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_deleted = require_auth("view_deleted")(handle_deleted)

async def handle_broadcast(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    result = ""
    if request.method == "POST":
        data = await request.post()
        text = data.get("text","").strip()
        if text:
            chats = [dict(r) for r in await db.get_all_chats()]
            sent = 0
            for c in chats:
                try:
                    from shared import bot as _b
                    if _b: await _b.send_message(c["cid"], text, parse_mode="HTML")
                    sent += 1
                except: pass
            result = f'<div style="padding:12px 18px;background:rgba(34,197,94,.1);border-radius:8px;color:var(--green);margin-bottom:16px;">✅ Отправлено в {sent} чатов</div>'
    body = navbar(sess, "broadcast") + f"""
    <div class="container">
      <div class="page-title">📢 Рассылка</div>
      {result}
      <div class="section">
        <div style="padding:16px 18px;">
          <form method="POST">
            <div style="margin-bottom:12px;font-size:13px;color:var(--t2);">HTML поддерживается. Сообщение отправится во все подключённые чаты.</div>
            <textarea name="text" placeholder="Текст рассылки..." rows="6"
              style="width:100%;padding:12px;border-radius:8px;border:1px solid var(--br1);
                     background:var(--bg3);color:var(--t1);font-size:14px;resize:vertical;"></textarea>
            <button class="btn btn-primary" type="submit" style="width:100%;margin-top:12px;padding:13px;">
              📢 Разослать всем
            </button>
          </form>
        </div>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_broadcast = require_auth("broadcast")(handle_broadcast)

async def handle_plugins(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    body = navbar(sess, "plugins") + """
    <div class="container">
      <div class="page-title">🧩 Плагины</div>
      <div class="section"><div class="empty-state" style="padding:40px;">Управление плагинами доступно через /plugins в боте</div></div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_plugins = require_auth("manage_plugins")(handle_plugins)

async def handle_chat_settings(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    body = navbar(sess, "chat_settings") + """
    <div class="container">
      <div class="page-title">⚙️ Настройки чатов</div>
      <div class="section"><div class="empty-state" style="padding:40px;">Настройки чатов доступны через /settings в боте</div></div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_chat_settings = require_auth("manage_chat_settings")(handle_chat_settings)

async def handle_analytics(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    conn = db.get_conn()
    try:
        top_chats = conn.execute("SELECT cid, SUM(msg_count) as total FROM chat_stats GROUP BY cid ORDER BY total DESC LIMIT 10").fetchall()
        top_users = conn.execute("SELECT uid, SUM(msg_count) as total FROM chat_stats GROUP BY uid ORDER BY total DESC LIMIT 10").fetchall()
    except: top_chats = []; top_users = []
    conn.close()
    chat_rows = "".join(f"<tr><td><code>{r['cid']}</code></td><td style='font-family:Space Mono,monospace;color:var(--pur);'>{r['total']:,}</td></tr>" for r in top_chats)
    user_rows = "".join(f"<tr><td><code>{r['uid']}</code></td><td style='font-family:Space Mono,monospace;color:var(--blue);'>{r['total']:,}</td></tr>" for r in top_users)
    body = navbar(sess, "analytics") + f"""
    <div class="container">
      <div class="page-title">📈 Аналитика</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
        <div class="section"><div class="section-header">💬 Топ чатов</div>
          <table><thead><tr><th>Чат ID</th><th>Сообщений</th></tr></thead>
          <tbody>{chat_rows or "<tr><td colspan='2' class='empty-state'>Нет данных</td></tr>"}</tbody></table></div>
        <div class="section"><div class="section-header">👥 Топ пользователей</div>
          <table><thead><tr><th>User ID</th><th>Сообщений</th></tr></thead>
          <tbody>{user_rows or "<tr><td colspan='2' class='empty-state'>Нет данных</td></tr>"}</tbody></table></div>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_analytics = require_auth("view_overview")(handle_analytics)

async def handle_admins(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    sessions = _get_active_sessions()
    rows = "".join(
        f"<tr><td><code style='font-size:12px;'>{s['ip']}</code></td>"
        f"<td style='font-size:12px;'>{s.get('current','')[:40]}</td>"
        f"<td style='font-size:12px;color:var(--t2);'>{int((time.time()-s['last_seen'])//60)}м назад</td>"
        f"<td>{s.get('pages',0)}</td></tr>"
        for s in sessions)
    body = navbar(sess, "admins") + f"""
    <div class="container">
      <div class="page-title">👑 Администраторы</div>
      <div class="section">
        <div class="section-header">👁 Активные сессии ({len(sessions)})</div>
        <table><thead><tr><th>IP</th><th>Страница</th><th>Активность</th><th>Стр.</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='4' class='empty-state'>Нет активных сессий</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_admins = require_auth("view_overview")(handle_admins)



async def handle_economy(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    conn = db.get_conn()
    try:
        top_rep = conn.execute("SELECT uid, cid, score FROM reputation ORDER BY score DESC LIMIT 20").fetchall()
    except: top_rep = []
    conn.close()
    rows = "".join(
        f"<tr><td><code>{r['uid']}</code></td>"
        f"<td><code>{r['cid']}</code></td>"
        f"<td style='font-family:Space Mono,monospace;color:var(--green);font-weight:700;'>{r['score']:+d}</td></tr>"
        for r in top_rep)
    body = navbar(sess, "economy") + f"""
    <div class="container">
      <div class="page-title">💰 Экономика</div>
      <div class="section">
        <div class="section-header">🌟 Топ по репутации</div>
        <table><thead><tr><th>User ID</th><th>Чат ID</th><th>Репутация</th></tr></thead>
        <tbody>{rows or "<tr><td colspan='3' class='empty-state'>Нет данных</td></tr>"}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")
handle_economy = require_auth("view_economy")(handle_economy)


async def start_dashboard():
    _init_admin_db()
    app = web.Application()

    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)

    app.router.add_get("/dashboard", handle_overview)
    app.router.add_get("/dashboard/login", handle_login)
    app.router.add_post("/dashboard/login", handle_login)
    app.router.add_get("/dashboard/logout", handle_logout)

    app.router.add_get("/dashboard/admins", handle_admins)
    app.router.add_post("/dashboard/admins", handle_admins)
    app.router.add_get("/dashboard/mod/{uid}", handle_mod_profile)

    app.router.add_get("/dashboard/chats", handle_chats)
    app.router.add_get("/dashboard/chats/{cid}", handle_chat_detail)

    app.router.add_get("/dashboard/tickets", handle_tickets)
    app.router.add_get("/dashboard/tickets/{ticket_id}", handle_ticket_detail)
    app.router.add_post("/dashboard/tickets/{ticket_id}/reply", handle_ticket_reply)
    app.router.add_get("/dashboard/tickets/{ticket_id}/close", handle_ticket_close_web)

    app.router.add_get("/dashboard/moderation", handle_moderation)
    app.router.add_get("/dashboard/users", handle_users)
    app.router.add_get("/dashboard/users/{uid}", handle_user_detail)

    app.router.add_get("/dashboard/reports", handle_reports)
    app.router.add_post("/dashboard/reports", handle_reports)

    app.router.add_get("/dashboard/alerts", handle_alerts)
    app.router.add_get("/dashboard/alerts/clear", handle_alerts_clear)

    app.router.add_get("/dashboard/media", handle_media)
    app.router.add_get("/dashboard/deleted", handle_deleted)
    app.router.add_get("/dashboard/economy", handle_economy)
    app.router.add_get("/dashboard/analytics", handle_analytics)

    app.router.add_get("/dashboard/plugins", handle_plugins)
    app.router.add_post("/dashboard/plugins", handle_plugins)

    app.router.add_get("/dashboard/broadcast", handle_broadcast)
    app.router.add_post("/dashboard/broadcast", handle_broadcast)

    app.router.add_get("/dashboard/settings", handle_settings)
    app.router.add_post("/dashboard/settings", handle_settings)

    app.router.add_get("/dashboard/chat_settings", handle_chat_settings)
    app.router.add_post("/dashboard/chat_settings", handle_chat_settings)
    app.router.add_get("/dashboard/chat_settings/{cid}", handle_chat_settings)
    app.router.add_post("/dashboard/chat_settings/{cid}", handle_chat_settings)

    app.router.add_get("/dashboard/export/{type}", handle_export)
    app.router.add_get("/dashboard/events", handle_sse)

    app.router.add_get("/api/live", api_live)
    app.router.add_get("/api/hourly", api_hourly)
    app.router.add_get("/api/search", api_search)
    app.router.add_post("/api/modaction", api_modaction)
    app.router.add_get("/api/stats", api_stats)

    # ── Новые фичи v3.0 ──
    app.router.add_get("/dashboard/activity_map",  handle_activity_map)
    app.router.add_get("/dashboard/achievements",  handle_achievements)
    app.router.add_get("/dashboard/team_chat",     handle_team_chat)
    app.router.add_post("/dashboard/team_chat",    handle_team_chat)
    app.router.add_get("/dashboard/threats",       handle_threats)
    app.router.add_post("/dashboard/threats",      handle_threats)
    app.router.add_get("/dashboard/appeals",       handle_appeals)
    app.router.add_post("/dashboard/appeals",      handle_appeals)
    app.router.add_get("/dashboard/themes",        handle_themes)
    app.router.add_post("/dashboard/themes",       handle_themes)
    app.router.add_get("/dashboard/msg_search",    handle_msg_search)

    # ── Wiki ──────────────────────────────────────────────────
    app.router.add_get("/dashboard/wiki",              handle_wiki_list)
    app.router.add_get("/dashboard/wiki/new",          handle_wiki_edit)
    app.router.add_post("/dashboard/wiki/new",         handle_wiki_edit)
    app.router.add_get("/dashboard/wiki/{slug}",       handle_wiki_view)
    app.router.add_get("/dashboard/wiki/{slug}/edit",  handle_wiki_edit)
    app.router.add_post("/dashboard/wiki/{slug}/edit", handle_wiki_edit)
    app.router.add_post("/dashboard/wiki/{slug}/delete", handle_wiki_delete)
    app.router.add_get("/dashboard/wiki/{slug}/history", handle_wiki_history)
    app.router.add_get("/api/wiki/search",             api_wiki_search)
    app.router.add_get("/dashboard/incidents",         handle_incidents)
    app.router.add_post("/dashboard/incidents",        handle_incidents)

    app.router.add_get("/api/team_messages",       api_team_messages)
    app.router.add_get("/api/mod_stats",           api_mod_stats)
    app.router.add_get("/api/threats",             api_threats)

    # ── Голосовые команды ──────────────────────────────────
    app.router.add_post("/dashboard/voice",            handle_voice_cmd)
    app.router.add_get("/dashboard/voice",             handle_voice_cmd)
    # ── Авто-отчёты ────────────────────────────────────────
    app.router.add_get("/dashboard/reports_cfg",       handle_reports_cfg)
    app.router.add_post("/dashboard/reports_cfg",      handle_reports_cfg)
    app.router.add_post("/api/reports_cfg/send_now",   api_reports_send_now)
    # ── Конструктор правил ────────────────────────────────
    app.router.add_get("/dashboard/automations",       handle_automations)
    app.router.add_post("/dashboard/automations",      handle_automations)
    app.router.add_post("/api/automations/delete",     api_automations_delete)
    # ── Синхронизация с ботом ─────────────────────────────
    app.router.add_get("/dashboard/bot_control",       handle_bot_control)
    app.router.add_post("/dashboard/bot_control",      handle_bot_control)
    app.router.add_post("/api/bot/action",             api_bot_action)
    app.router.add_get("/api/bot/status",              api_bot_status)
    app.router.add_get("/api/bot/chats",               api_bot_chats)
    # ── Mini App ──────────────────────────────────────────────
    app.router.add_get("/mini",                        handle_mini_app)
    app.router.add_post("/api/mini/auth",              api_mini_auth)
    app.router.add_get("/api/mini/stats",              api_mini_stats)
    app.router.add_get("/api/mini/tickets",            api_mini_tickets)
    app.router.add_get("/api/mini/ticket/{id}",        api_mini_ticket_detail)
    app.router.add_post("/api/mini/ticket/{id}/close", api_mini_ticket_close)
    app.router.add_get("/api/mini/chats",              api_mini_chats)
    app.router.add_post("/api/mini/action",            api_mini_action)
    app.router.add_get("/api/mini/me",                 api_mini_me)
    app.router.add_get("/dashboard/command_center",    handle_command_center)
    app.router.add_post("/dashboard/command_center",   handle_command_center)
    # ── Owner Panel ───────────────────────────────────────
    app.router.add_get("/dashboard/economy", handle_economy_dashboard)
    app.router.add_post("/dashboard/economy", handle_economy_dashboard)
    app.router.add_get("/dashboard/owner",  handle_owner_panel)
    app.router.add_post("/dashboard/owner", handle_owner_panel)

    # ── Модуль защиты ────────────────────────────────────────
    app.router.add_get("/dashboard/security",            handle_security)
    app.router.add_post("/dashboard/security",           handle_security)
    app.router.add_get("/dashboard/security/{cid}",      handle_security)
    app.router.add_post("/dashboard/security/{cid}",     handle_security)
    app.router.add_post("/api/security/force_night",     api_force_night)
    app.router.add_post("/api/security/force_raid",      api_force_raid)
    app.router.add_get("/api/security/stats",            api_security_stats)

    # Запускаем фоновые задачи
    _start_background_tasks()

    # Инициализация таблиц owner features
    try:
        _owner_init_tables()
    except Exception as _oe:
        log.warning(f"owner_init: {_oe}")

    # Запуск авто-бэкапа
    import asyncio as _aio_bk
    _aio_bk.get_event_loop().create_task(_backup_loop())

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"✅ Dashboard v2.0 запущен на :{port}")
    return runner


# ══════════════════════════════════════════════════════════════════
#  НОВЫЕ ФИЧИ v3.0
#  1. 🗺  Карта активности
#  2. 🎯  Достижения модераторов
#  3. 💬  Внутренний чат команды
#  4. 🔭  Разведка угроз
#  5. ⚖️  Система апелляций
#  6. 🎨  Кастомные темы
#  7. 🔍  Поиск по истории сообщений
# ══════════════════════════════════════════════════════════════════

import math

# ─── In-memory хранилища для новых фич ───────────────────────────
_team_chat: list = []          # [{uid, name, rank_name, text, ts, color}]
_threat_log: list = []         # [{uid, name, cid, cid_title, type, score, ts, details}]
_appeals: list = []            # [{id, uid, name, cid, reason, status, ts, reply, votes}]
_wiki_pages: dict = {}         # {slug: {title, body, author, ts}}
_mod_achievements: dict = {}   # {uid: {key: ts}}
_user_themes: dict = {}        # {sess_token: {accent, bg, font}}

# Достижения
ACHIEVEMENTS_DEF = {
    "first_ticket":   ("🎫", "Первый тикет",        "Закрыл первый тикет"),
    "ticket_10":      ("📬", "Почтальон",            "Закрыл 10 тикетов"),
    "ticket_50":      ("📮", "Мастер тикетов",       "Закрыл 50 тикетов"),
    "ticket_100":     ("🏆", "Легенда поддержки",    "Закрыл 100 тикетов"),
    "ban_10":         ("🔨", "Молоток",              "Выдал 10 банов"),
    "ban_50":         ("⚒️", "Кувалда",             "Выдал 50 банов"),
    "warn_20":        ("⚡", "Громовержец",          "Выдал 20 варнов"),
    "mute_20":        ("🔇", "Тишина",               "Выдал 20 мутов"),
    "report_10":      ("🚨", "Детектив",             "Обработал 10 репортов"),
    "login_7":        ("📅", "Постоянство",          "7 дней подряд в дашборде"),
    "login_30":       ("🌟", "Ветеран",              "30 дней в дашборде"),
    "appeal_5":       ("⚖️", "Судья",               "Рассмотрел 5 апелляций"),
    "night_owl":      ("🦉", "Ночная смена",         "Активен после полуночи"),
    "speed_close":    ("⚡", "Молния",               "Закрыл тикет за 2 минуты"),
    "threat_caught":  ("🔭", "Разведчик",            "Поймал угрозу через разведку"),
}

# Типы угроз для разведки
THREAT_TYPES = {
    "flood":      ("🌊", "Флуд-атака",        "danger"),
    "raid":       ("⚔️", "Рейд",             "danger"),
    "spam_bot":   ("🤖", "Спам-бот",          "danger"),
    "mass_join":  ("👥", "Массовый вход",     "warn"),
    "link_spam":  ("🔗", "Ссылочный спам",    "warn"),
    "scam":       ("💸", "Скам/Фишинг",       "danger"),
    "coordinated":("🎭", "Координированный",  "danger"),
}


# ══════════════════════════════════════════
#  🗺  КАРТА АКТИВНОСТИ
# ══════════════════════════════════════════

async def handle_activity_map(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    chats = [dict(r) for r in await db.get_all_chats()]

    # Собираем данные активности по каждому чату
    chat_data = []
    conn = db.get_conn()
    for c in chats:
        cid = c["cid"]
        title = c.get("title") or str(cid)
        msgs = conn.execute(
            "SELECT COALESCE(SUM(msg_count),0) FROM chat_stats WHERE cid=?", (cid,)
        ).fetchone()[0] or 0
        users = conn.execute(
            "SELECT COUNT(DISTINCT uid) FROM chat_stats WHERE cid=?", (cid,)
        ).fetchone()[0] or 0
        warns = conn.execute(
            "SELECT COALESCE(SUM(count),0) FROM warnings WHERE cid=?", (cid,)
        ).fetchone()[0] or 0
        bans = conn.execute(
            "SELECT COUNT(*) FROM ban_list WHERE cid=?", (cid,)
        ).fetchone()[0] or 0
        # Активность за последний час (онлайн)
        online_now = sum(
            1 for d in shared.online_users.values()
            if d.get("cid") == cid and time.time() - d.get("ts", 0) < 300
        )
        # Температура: 0-100
        temp = min(100, int((online_now * 20) + (msgs / max(users, 1)) * 0.1))
        chat_data.append({
            "cid": cid, "title": title[:20], "msgs": msgs,
            "users": users, "warns": warns, "bans": bans,
            "online": online_now, "temp": temp,
        })
    conn.close()

    chat_data.sort(key=lambda x: x["temp"], reverse=True)

    # JSON для JS
    chat_json = json.dumps(chat_data)

    # Карточки чатов
    cards_html = ""
    for c in chat_data:
        temp = c["temp"]
        if temp >= 70:
            color = "#ef4444"; label = "🔥 Горячо"; bg = "rgba(239,68,68,.08)"
        elif temp >= 40:
            color = "#f59e0b"; label = "⚡ Активно"; bg = "rgba(245,158,11,.08)"
        elif temp >= 15:
            color = "#22c55e"; label = "💬 Нормально"; bg = "rgba(34,197,94,.08)"
        else:
            color = "#64748b"; label = "😴 Тихо"; bg = "rgba(100,116,139,.08)"

        bar_w = max(4, temp)
        cards_html += (
            f'<div class="section" style="padding:16px;background:{bg};border-color:{color}33;cursor:pointer;" '
            f'onclick="window.location=\'/dashboard/chats/{c["cid"]}\'">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">'
            f'<div style="font-weight:700;font-size:14px;">{c["title"]}</div>'
            f'<span style="font-size:12px;color:{color};font-weight:700;">{label}</span>'
            f'</div>'
            f'<div style="height:6px;background:var(--bg4);border-radius:3px;margin-bottom:10px;">'
            f'<div style="height:6px;width:{bar_w}%;background:{color};border-radius:3px;transition:width .5s;"></div>'
            f'</div>'
            f'<div style="display:flex;gap:16px;font-size:12px;color:var(--text2);">'
            f'<span>🟢 {c["online"]} онлайн</span>'
            f'<span>👥 {c["users"]:,}</span>'
            f'<span>💬 {c["msgs"]:,}</span>'
            f'<span style="color:#ef4444;">🔨 {c["bans"]}</span>'
            f'</div>'
            f'</div>'
        )

    body = navbar(sess, "activity_map") + f"""
    <div class="container">
      <div class="page-title">🗺 Карта активности
        <span style="font-size:12px;color:var(--text2);font-weight:400;margin-left:auto;">
          Обновляется каждые 10 сек
        </span>
      </div>

      <!-- Пульс системы -->
      <div class="cards" style="grid-template-columns:repeat(4,1fr);margin-bottom:24px;">
        <div class="card" style="border-color:var(--success)33;">
          <div class="card-icon">🟢</div>
          <div class="card-label">Горячих чатов</div>
          <div class="card-value" style="color:var(--success);" id="hot-count">
            {sum(1 for c in chat_data if c["temp"]>=70)}
          </div>
        </div>
        <div class="card" style="border-color:var(--warn)33;">
          <div class="card-icon">⚡</div>
          <div class="card-label">Активных чатов</div>
          <div class="card-value" style="color:var(--warn);" id="active-count">
            {sum(1 for c in chat_data if 15<=c["temp"]<70)}
          </div>
        </div>
        <div class="card">
          <div class="card-icon">😴</div>
          <div class="card-label">Тихих чатов</div>
          <div class="card-value" style="color:var(--text2);" id="quiet-count">
            {sum(1 for c in chat_data if c["temp"]<15)}
          </div>
        </div>
        <div class="card">
          <div class="card-icon">👥</div>
          <div class="card-label">Всего онлайн</div>
          <div class="card-value" data-live="online" id="total-online">
            {shared.get_online_count()}
          </div>
        </div>
      </div>

      <!-- Визуализация — пузыри -->
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">🫧 Визуализация активности
          <span style="font-size:12px;color:var(--text2);">размер = участники, цвет = температура</span>
        </div>
        <div style="padding:20px;position:relative;height:320px;overflow:hidden;" id="bubble-map">
          <canvas id="bubbleCanvas" style="width:100%;height:100%;"></canvas>
        </div>
      </div>

      <!-- Список чатов -->
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;">
        {cards_html}
      </div>
    </div>

    <script>
    var chatData = {chat_json};

    // Bubble визуализация
    (function() {{
      var canvas = document.getElementById('bubbleCanvas');
      var container = document.getElementById('bubble-map');
      if (!canvas) return;

      canvas.width = container.offsetWidth;
      canvas.height = 300;
      var ctx = canvas.getContext('2d');

      var colors = {{
        hot:    '#ef4444',
        active: '#f59e0b',
        normal: '#22c55e',
        quiet:  '#475569'
      }};

      var bubbles = chatData.map(function(c, i) {{
        var angle = (i / chatData.length) * Math.PI * 2;
        var radius = Math.max(20, Math.min(70, 15 + Math.sqrt(c.users || 1) * 2));
        var cx = canvas.width/2 + Math.cos(angle) * (canvas.width/2 - radius - 20);
        var cy = canvas.height/2 + Math.sin(angle) * (canvas.height/2 - radius - 20);
        var col = c.temp >= 70 ? colors.hot : (c.temp >= 40 ? colors.active : (c.temp >= 15 ? colors.normal : colors.quiet));
        return {{x: cx, y: cy, r: radius, color: col, title: c.title, online: c.online, temp: c.temp, vx: (Math.random()-0.5)*0.3, vy: (Math.random()-0.5)*0.3}};
      }});

      var hovered = -1;
      canvas.addEventListener('mousemove', function(e) {{
        var rect = canvas.getBoundingClientRect();
        var mx = e.clientX - rect.left, my = e.clientY - rect.top;
        hovered = -1;
        bubbles.forEach(function(b, i) {{
          var dx = mx - b.x, dy = my - b.y;
          if (Math.sqrt(dx*dx+dy*dy) < b.r) hovered = i;
        }});
      }});

      function draw() {{
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        // Grid
        ctx.strokeStyle = 'rgba(255,255,255,0.03)';
        ctx.lineWidth = 1;
        for (var i=0;i<canvas.width;i+=50) {{ ctx.beginPath(); ctx.moveTo(i,0); ctx.lineTo(i,canvas.height); ctx.stroke(); }}
        for (var j=0;j<canvas.height;j+=50) {{ ctx.beginPath(); ctx.moveTo(0,j); ctx.lineTo(canvas.width,j); ctx.stroke(); }}

        bubbles.forEach(function(b, i) {{
          // Animate
          b.x += b.vx; b.y += b.vy;
          if (b.x < b.r || b.x > canvas.width-b.r) b.vx *= -1;
          if (b.y < b.r || b.y > canvas.height-b.r) b.vy *= -1;

          // Glow
          var grd = ctx.createRadialGradient(b.x, b.y, 0, b.x, b.y, b.r);
          grd.addColorStop(0, b.color + 'cc');
          grd.addColorStop(1, b.color + '22');
          ctx.beginPath();
          ctx.arc(b.x, b.y, b.r, 0, Math.PI*2);
          ctx.fillStyle = grd;
          ctx.fill();

          // Border
          ctx.strokeStyle = b.color;
          ctx.lineWidth = hovered === i ? 3 : 1.5;
          ctx.stroke();

          // Pulse ring for hot chats
          if (b.temp >= 70) {{
            ctx.beginPath();
            ctx.arc(b.x, b.y, b.r + 6 + Math.sin(Date.now()/300)*4, 0, Math.PI*2);
            ctx.strokeStyle = b.color + '44';
            ctx.lineWidth = 2;
            ctx.stroke();
          }}

          // Label
          ctx.fillStyle = '#e2e8f0';
          ctx.font = (hovered===i ? 'bold ' : '') + '11px Syne, sans-serif';
          ctx.textAlign = 'center';
          ctx.fillText(b.title.substring(0,12), b.x, b.y + 2);
          if (b.online > 0) {{
            ctx.fillStyle = b.color;
            ctx.font = 'bold 10px monospace';
            ctx.fillText('● ' + b.online, b.x, b.y + 14);
          }}
        }});

        // Tooltip
        if (hovered >= 0) {{
          var b = bubbles[hovered];
          var tw = 140, th = 60, tx = Math.min(b.x + b.r + 8, canvas.width - tw - 4), ty = Math.max(4, b.y - 30);
          ctx.fillStyle = 'rgba(14,18,32,0.95)';
          ctx.strokeStyle = b.color;
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.roundRect(tx, ty, tw, th, 8);
          ctx.fill(); ctx.stroke();
          ctx.fillStyle = '#e2e8f0';
          ctx.font = 'bold 12px Syne, sans-serif';
          ctx.textAlign = 'left';
          ctx.fillText(b.title, tx+8, ty+18);
          ctx.fillStyle = '#64748b';
          ctx.font = '11px monospace';
          ctx.fillText('Online: ' + b.online + '  Temp: ' + b.temp + '%', tx+8, ty+36);
          ctx.fillText('Color = ' + (b.temp>=70?'🔥 Горячо':b.temp>=40?'⚡ Активно':'💬 Норм'), tx+8, ty+52);
        }}

        requestAnimationFrame(draw);
      }}
      draw();

      // Авто-обновление данных
      setInterval(function() {{
        fetch('/api/live').then(function(r) {{ return r.json(); }}).then(function(d) {{
          document.getElementById('total-online').textContent = d.online || 0;
        }}).catch(function() {{}});
      }}, 10000);
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_activity_map = require_auth("view_overview")(handle_activity_map)


# ══════════════════════════════════════════
#  🎯  ДОСТИЖЕНИЯ МОДЕРАТОРОВ
# ══════════════════════════════════════════

def _check_achievements(tg_uid: int, action: str):
    """Проверяет и выдаёт достижения. Вызывать после mod-действий."""
    if tg_uid not in _mod_achievements:
        _mod_achievements[tg_uid] = {}
    earned = _mod_achievements[tg_uid]
    new_badges = []

    try:
        conn = db.get_conn()
        bans  = conn.execute("SELECT COUNT(*) FROM mod_history WHERE uid=? AND action LIKE '%Бан%'", (tg_uid,)).fetchone()[0] or 0
        warns = conn.execute("SELECT COUNT(*) FROM mod_history WHERE uid=? AND action LIKE '%Варн%'", (tg_uid,)).fetchone()[0] or 0
        mutes = conn.execute("SELECT COUNT(*) FROM mod_history WHERE uid=? AND action LIKE '%Мут%'", (tg_uid,)).fetchone()[0] or 0
        tkt_closed = conn.execute(
            "SELECT COUNT(*) FROM dashboard_admin_log WHERE tg_uid=? AND action='TICKET_CLOSE'", (tg_uid,)
        ).fetchone()[0] or 0
        rep_handled = conn.execute(
            "SELECT COUNT(*) FROM dashboard_admin_log WHERE tg_uid=? AND action LIKE 'ACTION_%'", (tg_uid,)
        ).fetchone()[0] or 0
        conn.close()
    except:
        return []

    checks = [
        ("ban_10",    bans >= 10),
        ("ban_50",    bans >= 50),
        ("warn_20",   warns >= 20),
        ("mute_20",   mutes >= 20),
        ("ticket_10", tkt_closed >= 10),
        ("ticket_50", tkt_closed >= 50),
        ("ticket_100",tkt_closed >= 100),
        ("report_10", rep_handled >= 10),
        ("appeal_5",  sum(1 for a in _appeals if a.get("decided_by") == tg_uid) >= 5),
        ("night_owl", datetime.now().hour >= 0 and datetime.now().hour <= 5),
    ]
    for key, cond in checks:
        if cond and key not in earned:
            earned[key] = time.time()
            new_badges.append(key)

    return new_badges


async def handle_achievements(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    admins = _get_all_admins()

    # Строим таблицу достижений
    rows_html = ""
    for a in admins:
        uid = a["tg_uid"]
        rank = a["rank"]
        rank_info = DASHBOARD_RANKS.get(rank, DASHBOARD_RANKS[1])
        earned = _mod_achievements.get(uid, {})

        badges_html = ""
        for key, ts in sorted(earned.items(), key=lambda x: x[1], reverse=True):
            if key in ACHIEVEMENTS_DEF:
                emoji, name, desc = ACHIEVEMENTS_DEF[key]
                dt = datetime.fromtimestamp(ts).strftime("%d.%m")
                badges_html += (
                    f'<span title="{name}: {desc} ({dt})" style="font-size:20px;cursor:default;">{emoji}</span>'
                )
        if not badges_html:
            badges_html = '<span style="color:var(--text2);font-size:12px;">Нет достижений</span>'

        # Статистика
        try:
            conn = db.get_conn()
            total_actions = conn.execute(
                "SELECT COUNT(*) FROM mod_history WHERE by_name=?", (a["name"],)
            ).fetchone()[0] or 0
            conn.close()
        except:
            total_actions = 0

        rgb = _hex_to_rgb(rank_info["color"])
        rows_html += (
            f"<tr>"
            f'<td><b>{a["name"]}</b></td>'
            f'<td><span class="badge" style="background:rgba({rgb},.15);color:{rank_info["color"]};">{rank_info["name"]}</span></td>'
            f'<td style="font-family:Space Mono,monospace;color:var(--accent);">{total_actions}</td>'
            f'<td style="font-size:18px;line-height:1.8;">{badges_html}</td>'
            f'</tr>'
        )

    # Все достижения — справочник
    all_ach_html = ""
    for key, (emoji, name, desc) in ACHIEVEMENTS_DEF.items():
        all_ach_html += (
            f'<div style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border);">'
            f'<span style="font-size:24px;">{emoji}</span>'
            f'<div><div style="font-weight:700;">{name}</div>'
            f'<div style="font-size:12px;color:var(--text2);">{desc}</div></div>'
            f'</div>'
        )

    body = navbar(sess, "achievements") + f"""
    <div class="container">
      <div class="page-title">🎯 Достижения модераторов</div>

      <div class="grid-2">
        <div class="section">
          <div class="section-header">👥 Команда и награды</div>
          <table>
            <thead><tr><th>Модератор</th><th>Ранг</th><th>Действий</th><th>Достижения</th></tr></thead>
            <tbody>{rows_html or "<tr><td colspan='4' class='empty-state'>Нет модераторов</td></tr>"}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">📋 Все достижения ({len(ACHIEVEMENTS_DEF)})</div>
          <div style="padding:0 20px;max-height:500px;overflow-y:auto;">{all_ach_html}</div>
        </div>
      </div>

      <!-- Топ по действиям -->
      <div class="section" style="margin-top:20px;">
        <div class="section-header">🏆 Топ модераторов за всё время</div>
        <div style="padding:16px;"><canvas id="topChart" height="60"></canvas></div>
      </div>
    </div>
    <script>
    (function() {{
      if (typeof Chart === 'undefined') return;
      var ctx = document.getElementById('topChart');
      if (!ctx) return;
      fetch('/api/mod_stats').then(function(r) {{ return r.json(); }}).then(function(d) {{
        new Chart(ctx, {{
          type: 'bar',
          data: {{
            labels: d.names,
            datasets: [
              {{label:'Банов', data:d.bans, backgroundColor:'rgba(239,68,68,0.7)', borderRadius:4}},
              {{label:'Варнов', data:d.warns, backgroundColor:'rgba(245,158,11,0.7)', borderRadius:4}},
              {{label:'Мутов', data:d.mutes, backgroundColor:'rgba(168,85,247,0.7)', borderRadius:4}},
            ]
          }},
          options: {{
            responsive:true,
            plugins:{{legend:{{labels:{{color:'#94a3b8'}}}},tooltip:{{backgroundColor:'rgba(14,18,32,.95)'}}}},
            scales:{{
              x:{{ticks:{{color:'#64748b'}},grid:{{display:false}}}},
              y:{{ticks:{{color:'#64748b'}},grid:{{color:'rgba(255,255,255,0.04)'}}}}
            }}
          }}
        }});
      }}).catch(function() {{}});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_achievements = require_auth("view_overview")(handle_achievements)


# ══════════════════════════════════════════
#  💬  ВНУТРЕННИЙ ЧАТ КОМАНДЫ
# ══════════════════════════════════════════

async def handle_team_chat(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    if request.method == "POST":
        data = await request.post()
        text = (data.get("text") or "").strip()
        if text and sess:
            rank = sess.get("rank", 1)
            rank_info = DASHBOARD_RANKS.get(rank, DASHBOARD_RANKS[1])
            _team_chat.insert(0, {
                "uid":   sess.get("uid", 0),
                "name":  sess.get("name", "?"),
                "rank":  rank_info["name"],
                "color": rank_info["color"],
                "text":  text[:500],
                "ts":    time.time(),
            })
            if len(_team_chat) > 200:
                _team_chat.pop()
            # SSE уведомление
            await shared.notify_sse({
                "type": "team_msg",
                "from": sess.get("name", "?"),
                "text": text[:80],
            })
        raise web.HTTPFound("/dashboard/team_chat")

    messages_html = ""
    for m in _team_chat[:50]:
        dt = datetime.fromtimestamp(m["ts"]).strftime("%d.%m %H:%M")
        is_me = sess and m["uid"] == sess.get("uid")
        align = "flex-end" if is_me else "flex-start"
        bg = "rgba(59,130,246,.12)" if is_me else "var(--bg3)"
        messages_html += (
            f'<div style="display:flex;flex-direction:column;align-items:{align};margin-bottom:12px;">'
            f'<div style="font-size:11px;color:{m["color"]};font-weight:700;margin-bottom:3px;">'
            f'{m["name"]} · {m["rank"]} · {dt}</div>'
            f'<div style="background:{bg};padding:10px 14px;border-radius:12px;max-width:75%;font-size:13px;line-height:1.6;">'
            f'{m["text"]}</div>'
            f'</div>'
        )

    if not messages_html:
        messages_html = '<div class="empty-state" style="padding:40px;">Нет сообщений. Начни разговор!</div>'

    online_mods = [
        s for s in _dashboard_sessions.values()
        if time.time() - s.get("login_time", 0) < 3600
    ]
    online_html = "".join(
        f'<div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid var(--border);">'
        f'<span style="width:8px;height:8px;border-radius:50%;background:var(--success);display:inline-block;"></span>'
        f'<span style="font-size:13px;font-weight:600;">{s["name"]}</span>'
        f'<span style="font-size:11px;color:var(--text2);margin-left:auto;">'
        f'{DASHBOARD_RANKS.get(s["rank"],DASHBOARD_RANKS[1])["name"]}</span>'
        f'</div>'
        for s in online_mods
    ) or '<div style="color:var(--text2);font-size:13px;padding:12px 0;">Никого онлайн</div>'

    _quick_phrases = [
        ("✅", "Принял дежурство"), ("🔴", "Сдаю дежурство"),
        ("⚠️", "Нужна помощь"), ("👀", "Слежу за ситуацией"),
        ("🔨", "Работаю с нарушителем"), ("✔️", "Проблема решена"),
    ]
    _quick_phrases_html = "".join(
        f'<button class="btn btn-ghost btn-sm" style="text-align:left;" '
        f'onclick="setPhrase(this)" data-text="{icon} {t}">{icon} {t}</button>'
        for icon, t in _quick_phrases
    )
    body = navbar(sess, "team_chat") + f"""
    <div class="container">
      <div class="page-title">💬 Чат команды</div>
      <div class="grid-2" style="grid-template-columns:1fr 280px;gap:20px;">
        <div>
          <div class="section" style="margin-bottom:16px;">
            <div class="section-header">💬 Сообщения
              <span style="font-size:12px;color:var(--text2);">последние 50</span>
            </div>
            <div id="chat-messages" style="padding:16px;max-height:480px;overflow-y:auto;display:flex;flex-direction:column;">
              {messages_html}
            </div>
          </div>
          <div class="section">
            <div class="section-body">
              <form method="POST" id="chat-form" style="display:flex;gap:10px;">
                <input class="form-control" name="text" id="chat-input"
                  placeholder="Написать команде..." autocomplete="off"
                  style="flex:1;" maxlength="500">
                <button class="btn btn-primary" type="submit">📨 Отправить</button>
              </form>
            </div>
          </div>
        </div>
        <div>
          <div class="section" style="margin-bottom:16px;">
            <div class="section-header">🟢 Онлайн ({len(online_mods)})</div>
            <div style="padding:0 16px;">{online_html}</div>
          </div>
          <div class="section">
            <div class="section-header">⚡ Быстрые фразы</div>
            <div style="padding:12px;display:flex;flex-direction:column;gap:6px;">
              {_quick_phrases_html}
            </div>
          </div>
        </div>
      </div>
    </div>
    <script>
    // Авто-обновление чата каждые 5 секунд
    setInterval(function() {{
      fetch('/api/team_messages').then(function(r) {{ return r.json(); }}).then(function(d) {{
        if (d.count !== undefined) {{
          var container = document.getElementById('chat-messages');
          if (d.html) container.innerHTML = d.html;
        }}
      }}).catch(function() {{}});
    }}, 5000);

    // Быстрые фразы
    function setPhrase(btn) {{
      document.getElementById('chat-input').value = btn.getAttribute('data-text');
      document.getElementById('chat-input').focus();
    }}

    // Enter для отправки
    document.getElementById('chat-input').addEventListener('keydown', function(e) {{
      if (e.key === 'Enter' && !e.shiftKey) {{
        e.preventDefault();
        document.getElementById('chat-form').submit();
      }}
    }});

    // Скролл вниз
    var msgs = document.getElementById('chat-messages');
    if (msgs) msgs.scrollTop = msgs.scrollHeight;
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_team_chat = require_auth("view_overview")(handle_team_chat)


async def api_team_messages(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"error": "Unauthorized"}, status=401)
    sess = _dashboard_sessions[token]
    html = ""
    for m in _team_chat[:50]:
        dt = datetime.fromtimestamp(m["ts"]).strftime("%d.%m %H:%M")
        is_me = m["uid"] == sess.get("uid")
        align = "flex-end" if is_me else "flex-start"
        bg = "rgba(59,130,246,.12)" if is_me else "var(--bg3)"
        html += (
            f'<div style="display:flex;flex-direction:column;align-items:{align};margin-bottom:12px;">'
            f'<div style="font-size:11px;color:{m["color"]};font-weight:700;margin-bottom:3px;">'
            f'{m["name"]} · {m["rank"]} · {dt}</div>'
            f'<div style="background:{bg};padding:10px 14px;border-radius:12px;max-width:75%;font-size:13px;">'
            f'{m["text"]}</div></div>'
        )
    return web.json_response({"html": html or "", "count": len(_team_chat)})


# ══════════════════════════════════════════
#  🔭  РАЗВЕДКА УГРОЗ
# ══════════════════════════════════════════

def _analyze_threats():
    """Анализирует shared данные и генерирует угрозы."""
    now = time.time()
    new_threats = []

    # 1. Проверка флуда по spam_tracker
    for key, timestamps in shared.spam_tracker.items():
        recent = [t for t in timestamps if now - t < 60]
        if len(recent) >= 20:
            parts = key.split(":")
            if len(parts) == 2:
                cid, uid = int(parts[0]), int(parts[1])
                name = shared.online_users.get(uid, {}).get("name", f"User {uid}")
                threat_id = f"flood_{uid}_{int(now//60)}"
                if not any(t.get("id") == threat_id for t in _threat_log):
                    new_threats.append({
                        "id": threat_id, "uid": uid, "name": name,
                        "cid": cid, "cid_title": f"Chat {cid}",
                        "type": "flood", "score": min(100, len(recent) * 4),
                        "ts": now, "details": f"{len(recent)} сообщений за минуту",
                        "status": "active",
                    })

    # 2. Массовый вход (много новых онлайн за 5 минут)
    recent_online = [d for d in shared.online_users.values() if now - d.get("ts", 0) < 300]
    by_chat = {}
    for d in recent_online:
        cid = d.get("cid", 0)
        by_chat[cid] = by_chat.get(cid, 0) + 1
    for cid, count in by_chat.items():
        if count >= 15:
            threat_id = f"mass_join_{cid}_{int(now//300)}"
            if not any(t.get("id") == threat_id for t in _threat_log):
                new_threats.append({
                    "id": threat_id, "uid": 0, "name": "Группа",
                    "cid": cid, "cid_title": f"Chat {cid}",
                    "type": "mass_join", "score": min(100, count * 5),
                    "ts": now, "details": f"{count} новых юзеров за 5 минут",
                    "status": "active",
                })

    # 3. Из алертов
    for a in shared.alerts[:10]:
        if a.get("level") == "danger":
            threat_id = f"alert_{a.get('uid',0)}_{int(a.get('_ts', now)//60)}"
            if not any(t.get("id") == threat_id for t in _threat_log):
                new_threats.append({
                    "id": threat_id,
                    "uid": a.get("uid", 0),
                    "name": a.get("desc", "Неизвестно")[:40],
                    "cid": a.get("cid", 0),
                    "cid_title": f"Chat {a.get('cid',0)}",
                    "type": "spam_bot",
                    "score": 75,
                    "ts": now,
                    "details": a.get("desc", "")[:100],
                    "status": "active",
                })

    for t in new_threats:
        _threat_log.insert(0, t)
    if len(_threat_log) > 100:
        del _threat_log[100:]

    return new_threats


async def handle_threats(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    token = request.cookies.get("dsess_token")

    # Действие над угрозой
    if request.method == "POST":
        data = await request.post()
        action = data.get("action")
        threat_id = data.get("threat_id")
        for t in _threat_log:
            if t.get("id") == threat_id:
                if action == "resolve":
                    t["status"] = "resolved"
                elif action == "ban" and _bot and t.get("uid") and t.get("cid"):
                    try:
                        await _bot.ban_chat_member(t["cid"], t["uid"])
                        t["status"] = "resolved"
                        showmsg = "Забанен"
                    except:
                        pass
                elif action == "mute" and _bot and t.get("uid") and t.get("cid"):
                    try:
                        from aiogram.types import ChatPermissions
                        await _bot.restrict_chat_member(
                            t["cid"], t["uid"],
                            ChatPermissions(can_send_messages=False),
                            until_date=timedelta(hours=1)
                        )
                        t["status"] = "resolved"
                    except:
                        pass
                break
        raise web.HTTPFound("/dashboard/threats")

    # Запускаем анализ
    new_threats = _analyze_threats()

    active = [t for t in _threat_log if t.get("status") == "active"]
    resolved = [t for t in _threat_log if t.get("status") == "resolved"]

    def threat_row(t):
        ttype = t.get("type", "flood")
        emoji, label, level = THREAT_TYPES.get(ttype, ("⚠️", "Угроза", "warn"))
        score = t.get("score", 0)
        score_color = "#ef4444" if score >= 75 else ("#f59e0b" if score >= 40 else "#22c55e")
        dt = datetime.fromtimestamp(t.get("ts", 0)).strftime("%d.%m %H:%M")
        status = t.get("status", "active")
        status_badge = (
            '<span class="badge badge-danger">⚡ Активна</span>' if status == "active"
            else '<span class="badge badge-closed">✅ Решена</span>'
        )
        actions = ""
        if status == "active":
            tid = t.get("id", "")
            can_ban = _has_perm(token, "ban_users")
            can_mute = _has_perm(token, "mute_users")
            actions = f'<form method="POST" style="display:inline-flex;gap:6px;">'
            actions += f'<input type="hidden" name="threat_id" value="{tid}">'
            if can_ban and t.get("uid"):
                actions += f'<button name="action" value="ban" class="btn btn-xs btn-danger">🔨 Бан</button>'
            if can_mute and t.get("uid"):
                actions += f'<button name="action" value="mute" class="btn btn-xs btn-warn">🔇 Мут</button>'
            actions += f'<button name="action" value="resolve" class="btn btn-xs btn-ghost">✅ Закрыть</button>'
            actions += f'</form>'
        return (
            f"<tr>"
            f"<td>{emoji} <b>{label}</b></td>"
            f'<td>{t.get("name","—")}</td>'
            f'<td style="font-size:12px;color:var(--text2);">{t.get("cid_title","—")}</td>'
            f'<td style="color:{score_color};font-weight:700;font-family:Space Mono,monospace;">{score}</td>'
            f'<td style="font-size:12px;color:var(--text2);">{t.get("details","—")[:50]}</td>'
            f'<td>{status_badge}</td>'
            f'<td style="font-size:11px;color:var(--text2);">{dt}</td>'
            f'<td>{actions}</td>'
            f"</tr>"
        )

    active_rows = "".join(threat_row(t) for t in active) or "<tr><td colspan='8' class='empty-state'>✅ Угроз не обнаружено</td></tr>"
    resolved_rows = "".join(threat_row(t) for t in resolved[:20]) or "<tr><td colspan='8' class='empty-state'>Нет</td></tr>"

    # Счётчики по типам
    type_counts = {}
    for t in active:
        tt = t.get("type", "other")
        type_counts[tt] = type_counts.get(tt, 0) + 1

    type_cards = ""
    for ttype, (emoji, label, level) in THREAT_TYPES.items():
        cnt = type_counts.get(ttype, 0)
        color = "var(--danger)" if level == "danger" else "var(--warn)"
        type_cards += (
            f'<div class="card" style="{"border-color:rgba(239,68,68,.3);" if cnt>0 else ""}">'
            f'<div class="card-icon">{emoji}</div>'
            f'<div class="card-label">{label}</div>'
            f'<div class="card-value" style="color:{color if cnt>0 else "var(--text2)"};">{cnt}</div>'
            f'</div>'
        )

    body = navbar(sess, "threats") + f"""
    <div class="container">
      <div class="page-title">🔭 Разведка угроз
        <span class="badge badge-danger" style="margin-left:12px;">{len(active)} активных</span>
        <button class="btn btn-ghost btn-sm" style="margin-left:auto;" onclick="location.reload()">🔄 Обновить</button>
      </div>

      <div class="cards" style="grid-template-columns:repeat(auto-fill,minmax(140px,1fr));margin-bottom:24px;">
        {type_cards}
      </div>

      <div class="section" style="margin-bottom:20px;">
        <div class="section-header">
          🚨 Активные угрозы ({len(active)})
          <span style="font-size:12px;color:var(--text2);">Автообновление каждые 30с</span>
        </div>
        <table>
          <thead><tr><th>Тип</th><th>Кто</th><th>Чат</th><th>Score</th><th>Детали</th><th>Статус</th><th>Время</th><th>Действия</th></tr></thead>
          <tbody>{active_rows}</tbody>
        </table>
      </div>

      <div class="section">
        <div class="section-header">✅ Решённые угрозы ({len(resolved)})</div>
        <table>
          <thead><tr><th>Тип</th><th>Кто</th><th>Чат</th><th>Score</th><th>Детали</th><th>Статус</th><th>Время</th><th></th></tr></thead>
          <tbody>{resolved_rows}</tbody>
        </table>
      </div>
    </div>
    <script>
    setInterval(function() {{ location.reload(); }}, 30000);
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_threats = require_auth("view_alerts")(handle_threats)


# ══════════════════════════════════════════
#  ⚖️  СИСТЕМА АПЕЛЛЯЦИЙ
# ══════════════════════════════════════════

async def handle_appeals(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    token = request.cookies.get("dsess_token")

    # Хранилище голосов: {appeal_id: {voter_uid: "yes"/"no"}}
    if not hasattr(handle_appeals, "_votes"):
        handle_appeals._votes = {}
    _votes = handle_appeals._votes

    if request.method == "POST":
        data = await request.post()
        action = data.get("action")
        appeal_id = int(data.get("appeal_id", -1))
        reply_text = (data.get("reply") or "").strip()
        voter_uid  = sess.get("uid", 0) if sess else 0
        voter_rank = sess.get("rank", 1) if sess else 1

        # Голосование — доступно с ранга 5+
        if action in ("vote_yes", "vote_no") and voter_rank >= 5:
            vote_val = "yes" if action == "vote_yes" else "no"
            if appeal_id not in _votes:
                _votes[appeal_id] = {}
            _votes[appeal_id][voter_uid] = vote_val
            # Считаем голоса
            votes = _votes.get(appeal_id, {})
            yes_v = sum(1 for v in votes.values() if v == "yes")
            no_v  = sum(1 for v in votes.values() if v == "no")
            # Авторешение при 3+ голосах
            for ap in _appeals:
                if ap.get("id") == appeal_id and ap.get("status") == "pending":
                    if yes_v >= 3:
                        ap["status"] = "approved"
                        ap["reply"] = "✅ Одобрено командой модераторов (3+ голосов За)"
                        ap["decided_by"] = voter_uid
                        ap["decided_at"] = time.time()
                        if _bot and ap.get("uid") and ap.get("cid"):
                            try:
                                import asyncio as _aio
                                _aio.ensure_future(_bot.unban_chat_member(ap["cid"], ap["uid"], only_if_banned=True))
                                _aio.ensure_future(_bot.send_message(ap["uid"],
                                    "✅ <b>Твоя апелляция одобрена!</b>\nБлокировка снята командой модераторов.",
                                    parse_mode="HTML"))
                            except:
                                pass
                        _log_admin_db(voter_uid, "APPEAL_AUTO_APPROVE", f"id={appeal_id} yes={yes_v}")
                    elif no_v >= 3:
                        ap["status"] = "rejected"
                        ap["reply"] = "❌ Отклонено командой модераторов (3+ голосов Против)"
                        ap["decided_by"] = voter_uid
                        ap["decided_at"] = time.time()
                        if _bot and ap.get("uid"):
                            try:
                                import asyncio as _aio
                                _aio.ensure_future(_bot.send_message(ap["uid"],
                                    "❌ <b>Твоя апелляция отклонена</b> командой модераторов.",
                                    parse_mode="HTML"))
                            except:
                                pass
                        _log_admin_db(voter_uid, "APPEAL_AUTO_REJECT", f"id={appeal_id} no={no_v}")
            raise web.HTTPFound("/dashboard/appeals")

        for ap in _appeals:
            if ap.get("id") == appeal_id:
                if action == "approve" and _has_perm(token, "ban_users"):
                    ap["status"] = "approved"
                    ap["reply"] = reply_text or "Апелляция одобрена. Блокировка снята."
                    ap["decided_by"] = sess.get("uid") if sess else 0
                    ap["decided_at"] = time.time()
                    # Разбаниваем
                    if _bot and ap.get("uid") and ap.get("cid"):
                        try:
                            await _bot.unban_chat_member(ap["cid"], ap["uid"], only_if_banned=True)
                        except:
                            pass
                    # Уведомляем юзера
                    if _bot and ap.get("uid"):
                        try:
                            await _bot.send_message(
                                ap["uid"],
                                f"✅ <b>Апелляция #{appeal_id} одобрена</b>\n\n"
                                f"Ваша блокировка снята.\n\n"
                                f"💬 Ответ: {ap['reply']}",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                elif action == "reject":
                    ap["status"] = "rejected"
                    ap["reply"] = reply_text or "Апелляция отклонена. Блокировка остаётся в силе."
                    ap["decided_by"] = sess.get("uid") if sess else 0
                    ap["decided_at"] = time.time()
                    if _bot and ap.get("uid"):
                        try:
                            await _bot.send_message(
                                ap["uid"],
                                f"❌ <b>Апелляция #{appeal_id} отклонена</b>\n\n"
                                f"💬 Ответ: {ap['reply']}",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                elif action == "need_info":
                    ap["status"] = "pending_info"
                    ap["reply"] = reply_text
                    if _bot and ap.get("uid"):
                        try:
                            await _bot.send_message(
                                ap["uid"],
                                f"❓ <b>По апелляции #{appeal_id} нужна информация</b>\n\n"
                                f"{reply_text}",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                if sess:
                    _check_achievements(sess.get("uid", 0), "appeal")
                break
        raise web.HTTPFound("/dashboard/appeals")

    status_filter = request.rel_url.query.get("status", "pending")
    filtered = [a for a in _appeals if status_filter == "all" or a.get("status", "pending") == status_filter]

    counts = {}
    for a in _appeals:
        s = a.get("status", "pending")
        counts[s] = counts.get(s, 0) + 1

    tabs = ""
    for s, label, icon in [("pending","Ожидают","🟡"),("pending_info","Нужна инфо","❓"),("approved","Одобрены","✅"),("rejected","Отклонены","❌"),("all","Все","📋")]:
        active_cls = "btn-primary" if status_filter == s else "btn-ghost"
        cnt = counts.get(s, 0) if s != "all" else len(_appeals)
        tabs += f'<a href="?status={s}" class="btn btn-sm {active_cls}" style="margin-right:6px;">{icon} {label} ({cnt})</a>'

    can_decide = _has_perm(token, "ban_users")

    rows_html = ""
    for ap in filtered:
        ap_id = ap.get("id", 0)
        status = ap.get("status", "pending")
        st_cls = {"pending":"badge-open","pending_info":"badge-progress","approved":"badge-closed","rejected":"badge-danger"}.get(status,"badge-accent")
        st_label = {"pending":"🟡 Ожидает","pending_info":"❓ Нужна инфо","approved":"✅ Одобрена","rejected":"❌ Отклонена"}.get(status, status)
        dt = datetime.fromtimestamp(ap.get("ts", 0)).strftime("%d.%m %H:%M")

        action_form = ""
        if not hasattr(handle_appeals, "_votes"):
            handle_appeals._votes = {}
        _votes = handle_appeals._votes
        ap_votes = _votes.get(ap_id, {})
        yes_cnt  = sum(1 for v in ap_votes.values() if v == "yes")
        no_cnt   = sum(1 for v in ap_votes.values() if v == "no")
        voter_uid_cur  = sess.get("uid", 0) if sess else 0
        voter_rank_cur = sess.get("rank", 1) if sess else 1
        my_vote  = ap_votes.get(voter_uid_cur, "")

        vote_block = ""
        if status in ("pending", "pending_info") and voter_rank_cur >= 5:
            y_s = 'background:rgba(34,197,94,.35);' if my_vote == "yes" else 'background:rgba(34,197,94,.1);'
            n_s = 'background:rgba(239,68,68,.35);'  if my_vote == "no"  else 'background:rgba(239,68,68,.1);'
            vote_block = (
                f'<div style="display:flex;gap:6px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">'
                f'<span style="font-size:12px;color:var(--text2);">Голосование ({yes_cnt}✅/{no_cnt}❌ — нужно 3):</span>'
                f'<form method="POST" style="display:inline;">'
                f'<input type="hidden" name="appeal_id" value="{ap_id}">'
                f'<input type="hidden" name="action" value="vote_yes">'
                f'<button class="btn btn-xs" style="{y_s}color:var(--success);" type="submit">👍 За ({yes_cnt})</button>'
                f'</form>'
                f'<form method="POST" style="display:inline;">'
                f'<input type="hidden" name="appeal_id" value="{ap_id}">'
                f'<input type="hidden" name="action" value="vote_no">'
                f'<button class="btn btn-xs" style="{n_s}color:var(--danger);" type="submit">👎 Против ({no_cnt})</button>'
                f'</form>'
                f'<span style="font-size:11px;color:var(--text2);">Твой голос: {my_vote or "—"}</span>'
                f'</div>'
            )

        if status in ("pending", "pending_info") and can_decide:
            action_form = (
                vote_block +
                f'<form method="POST">'
                f'<input type="hidden" name="appeal_id" value="{ap_id}">'
                f'<textarea class="form-control" name="reply" rows="2" placeholder="Ответ юзеру..." style="margin-bottom:8px;font-size:12px;"></textarea>'
                f'<div style="display:flex;gap:6px;flex-wrap:wrap;">'
                f'<button name="action" value="approve" class="btn btn-xs btn-success">✅ Одобрить</button>'
                f'<button name="action" value="reject" class="btn btn-xs btn-danger">❌ Отклонить</button>'
                f'<button name="action" value="need_info" class="btn btn-xs btn-warn">❓ Нужна инфо</button>'
                f'</div></form>'
            )
        elif status in ("pending", "pending_info"):
            action_form = vote_block


        _ap_reply = ("<div style=\"font-size:12px;color:var(--success);margin-top:6px;\">💬 Ответ: " + ap.get("reply","—") + "</div>") if ap.get("reply") else ""
        rows_html += (
            f'<div class="section" style="margin-bottom:12px;padding:16px;">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start;">'
            f'<div style="flex:1;">'
            f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">'
            f'<span style="font-weight:700;">#{ap_id} · {ap.get("name","—")}</span>'
            f'<span class="badge {st_cls}">{st_label}</span>'
            f'<span style="font-size:11px;color:var(--text2);">{dt}</span>'
            f'</div>'
            f'<div style="font-size:13px;color:var(--text2);margin-bottom:4px);">Чат: {ap.get("cid_title","—")}</div>'
            f'<div style="font-size:13px;background:var(--bg3);padding:10px;border-radius:8px;margin-top:8px;">'
            f'<b>Причина:</b> {ap.get("reason","—")}</div>'
            f'{_ap_reply}'
            f'{action_form}'
            f'</div>'
            f'</div></div>'
        )

    if not rows_html:
        rows_html = '<div class="empty-state">Апелляций нет</div>'

    body = navbar(sess, "appeals") + f"""
    <div class="container">
      <div class="page-title">⚖️ Апелляции
        <span class="badge badge-open" style="margin-left:12px;">{counts.get("pending",0)} ожидают</span>
      </div>
      <div style="margin-bottom:20px;">{tabs}</div>
      {rows_html}
    </div>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_appeals = require_auth("view_reports")(handle_appeals)


def add_appeal(uid: int, name: str, cid: int, cid_title: str, reason: str):
    """Вызывается из bot.py когда юзер подаёт апелляцию."""
    ap_id = len(_appeals) + 1
    _appeals.insert(0, {
        "id": ap_id, "uid": uid, "name": name,
        "cid": cid, "cid_title": cid_title,
        "reason": reason, "status": "pending",
        "ts": time.time(), "reply": "", "decided_by": 0,
    })
    # SSE уведомление
    import asyncio as _aio
    try:
        loop = _aio.get_event_loop()
        loop.create_task(shared.notify_sse({
            "type": "new_appeal",
            "id": ap_id, "user": name,
        }))
    except:
        pass


# ══════════════════════════════════════════
#  🎨  КАСТОМНЫЕ ТЕМЫ
# ══════════════════════════════════════════

THEME_PRESETS = {
    "default":  {"accent":"var(--acc)", "purple":"#a855f7", "bg":"#080b14",  "name":"🌑 Тёмная (по умолч.)"},
    "ocean":    {"accent":"#06b6d4", "purple":"#0ea5e9", "bg":"#020f1a",  "name":"🌊 Океан"},
    "forest":   {"accent":"#22c55e", "purple":"#16a34a", "bg":"#020f08",  "name":"🌲 Лес"},
    "sunset":   {"accent":"#f97316", "purple":"#ef4444", "bg":"#150a03",  "name":"🌅 Закат"},
    "purple":   {"accent":"#a855f7", "purple":"#ec4899", "bg":"#0d0814",  "name":"💜 Фиолет"},
    "gold":     {"accent":"#fbbf24", "purple":"#f59e0b", "bg":"#120e00",  "name":"✨ Золото"},
    "red":      {"accent":"#ef4444", "purple":"#dc2626", "bg":"#130505",  "name":"🔴 Красный"},
    "mono":     {"accent":"#94a3b8", "purple":"#64748b", "bg":"#080808",  "name":"⬛ Моно"},
}

async def handle_themes(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    token = request.cookies.get("dsess_token") or ""

    if request.method == "POST":
        data = await request.post()
        preset = data.get("preset", "default")
        custom_accent = data.get("custom_accent", "")
        custom_bg = data.get("custom_bg", "")

        theme = THEME_PRESETS.get(preset, THEME_PRESETS["default"]).copy()
        if custom_accent and custom_accent.startswith("#"):
            theme["accent"] = custom_accent
        if custom_bg and custom_bg.startswith("#"):
            theme["bg"] = custom_bg

        _user_themes[token] = theme
        raise web.HTTPFound("/dashboard/themes")

    current = _user_themes.get(token, THEME_PRESETS["default"])

    presets_html = ""
    for key, t in THEME_PRESETS.items():
        is_active = _user_themes.get(token, {}).get("accent") == t["accent"]
        border = f"border:2px solid {t['accent']};" if is_active else "border:2px solid var(--border);"
        presets_html += (
            f'<form method="POST" style="display:inline;">'
            f'<input type="hidden" name="preset" value="{key}">'
            f'<button type="submit" style="{border}background:{t["bg"]};'
            f'padding:14px 18px;border-radius:12px;cursor:pointer;min-width:130px;'
            f'transition:all .2s;display:inline-flex;flex-direction:column;align-items:center;gap:6px;">'
            f'<div style="display:flex;gap:6px;">'
            f'<div style="width:20px;height:20px;border-radius:50%;background:{t["accent"]};"></div>'
            f'<div style="width:20px;height:20px;border-radius:50%;background:{t["purple"]};"></div>'
            f'</div>'
            f'<span style="color:{t["accent"]};font-size:12px;font-weight:700;">{t["name"]}</span>'
            f'</button></form> '
        )

    body = navbar(sess, "themes") + f"""
    <div class="container">
      <div class="page-title">🎨 Кастомные темы</div>

      <div class="section" style="margin-bottom:24px;max-width:700px;">
        <div class="section-header">🎨 Готовые пресеты</div>
        <div style="padding:20px;display:flex;flex-wrap:wrap;gap:10px;">
          {presets_html}
        </div>
      </div>

      <div class="section" style="max-width:480px;">
        <div class="section-header">✏️ Свои цвета</div>
        <div class="section-body">
          <form method="POST">
            <input type="hidden" name="preset" value="default">
            <div class="form-group">
              <label>Акцентный цвет</label>
              <div style="display:flex;gap:10px;align-items:center;">
                <input type="color" name="custom_accent" value="{current.get("accent","var(--acc)")}"
                  style="width:50px;height:40px;border-radius:8px;border:1px solid var(--border);background:transparent;cursor:pointer;">
                <input class="form-control" type="text" value="{current.get("accent","var(--acc)")}"
                  style="flex:1;" oninput="this.previousElementSibling.value=this.value" name="custom_accent_text">
              </div>
            </div>
            <div class="form-group">
              <label>Фон</label>
              <div style="display:flex;gap:10px;align-items:center;">
                <input type="color" name="custom_bg" value="{current.get("bg","#080b14")}"
                  style="width:50px;height:40px;border-radius:8px;border:1px solid var(--border);background:transparent;cursor:pointer;">
                <input class="form-control" type="text" value="{current.get("bg","#080b14")}"
                  style="flex:1;" oninput="this.previousElementSibling.value=this.value" name="custom_bg_text">
              </div>
            </div>
            <button class="btn btn-primary" type="submit" style="width:100%;">✅ Применить</button>
          </form>
        </div>
      </div>
    </div>

    <script>
    // Применяем тему сразу через CSS переменные
    (function() {{
      var accent = "{current.get("accent","var(--acc)")}";
      var bg = "{current.get("bg","#080b14")}";
      if (accent) {{
        document.documentElement.style.setProperty("--accent", accent);
        document.documentElement.style.setProperty("--accent2", accent);
      }}
      if (bg) {{
        document.documentElement.style.setProperty("--bg", bg);
      }}
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_themes = require_auth("view_overview")(handle_themes)


# ══════════════════════════════════════════
#  🔍  ПОИСК ПО ИСТОРИИ СООБЩЕНИЙ
# ══════════════════════════════════════════

async def handle_msg_search(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    query = request.rel_url.query.get("q", "").strip()
    uid_filter = request.rel_url.query.get("uid", "")
    cid_filter = request.rel_url.query.get("cid", "")
    date_from = request.rel_url.query.get("from", "")
    date_to = request.rel_url.query.get("to", "")

    chats = [dict(r) for r in await db.get_all_chats()]
    chat_opts = '<option value="">Все чаты</option>' + "".join(
        f'<option value="{c["cid"]}" {"selected" if str(c["cid"])==cid_filter else ""}>'
        f'{c.get("title","") or c["cid"]}</option>'
        for c in chats
    )

    results = []
    total = 0
    search_done = bool(query or uid_filter or cid_filter)

    if search_done:
        try:
            conn = db.get_conn()
            conditions = []
            params = []

            if query:
                conditions.append("text LIKE ?")
                params.append(f"%{query}%")
            if uid_filter:
                conditions.append("uid = ?")
                params.append(int(uid_filter))
            if cid_filter:
                conditions.append("cid = ?")
                params.append(int(cid_filter))
            if date_from:
                conditions.append("ts >= ?")
                try:
                    params.append(datetime.strptime(date_from, "%Y-%m-%d").timestamp())
                except:
                    pass
            if date_to:
                conditions.append("ts <= ?")
                try:
                    params.append((datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)).timestamp())
                except:
                    pass

            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            count_row = conn.execute(f"SELECT COUNT(*) FROM deleted_log {where}", params).fetchone()
            total = count_row[0] if count_row else 0
            rows = conn.execute(
                f"SELECT * FROM deleted_log {where} ORDER BY ts DESC LIMIT 50",
                params
            ).fetchall()
            results = [dict(r) for r in rows]
            conn.close()
        except Exception as e:
            results = []
            total = 0

    results_html = ""
    for r in results:
        uid = r.get("uid", 0)
        name = r.get("name") or f"User {uid}"
        text = r.get("text", "")
        ts = r.get("ts", 0) or r.get("deleted_at", "")
        try:
            dt = datetime.fromtimestamp(float(ts)).strftime("%d.%m.%Y %H:%M") if ts else "—"
        except:
            dt = str(ts)[:16]
        cid = r.get("cid", "")

        # Подсветка запроса
        highlighted = text
        if query and query.lower() in text.lower():
            idx = text.lower().find(query.lower())
            highlighted = (
                text[:idx] +
                f'<mark style="background:rgba(251,191,36,.25);color:#fbbf24;border-radius:3px;">' +
                text[idx:idx+len(query)] +
                '</mark>' +
                text[idx+len(query):]
            )

        results_html += (
            f'<div style="padding:14px 0;border-bottom:1px solid var(--border);">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">'
            f'<div style="display:flex;align-items:center;gap:10px;">'
            f'<a href="/dashboard/users/{uid}" style="font-weight:700;color:var(--accent);">{name}</a>'
            f'<code style="font-size:11px;">{uid}</code>'
            f'<span style="font-size:11px;color:var(--text2);">Chat: {cid}</span>'
            f'</div>'
            f'<span style="font-size:11px;color:var(--text2);">{dt}</span>'
            f'</div>'
            f'<div style="font-size:13px;color:var(--text);line-height:1.6;">{highlighted[:300]}</div>'
            f'</div>'
        )

    if search_done and not results_html:
        results_html = '<div class="empty-state">Ничего не найдено</div>'

    body = navbar(sess, "msg_search") + f"""
    <div class="container">
      <div class="page-title">🔍 Поиск по истории сообщений</div>

      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">🔎 Параметры поиска</div>
        <div class="section-body">
          <form method="GET">
            <div class="grid-2" style="gap:12px;">
              <div class="form-group">
                <label>Текст сообщения</label>
                <input class="form-control" name="q" value="{query}" placeholder="Ключевые слова...">
              </div>
              <div class="form-group">
                <label>Telegram ID пользователя</label>
                <input class="form-control" name="uid" value="{uid_filter}" placeholder="123456789">
              </div>
              <div class="form-group">
                <label>Чат</label>
                <select class="form-control" name="cid">{chat_opts}</select>
              </div>
              <div class="form-group">
                <label>Период</label>
                <div style="display:flex;gap:8px;align-items:center;">
                  <input class="form-control" type="date" name="from" value="{date_from}" style="flex:1;">
                  <span style="color:var(--text2);">—</span>
                  <input class="form-control" type="date" name="to" value="{date_to}" style="flex:1;">
                </div>
              </div>
            </div>
            <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;">🔍 Найти</button>
          </form>
        </div>
      </div>

      {"" if not search_done else f'''
      <div class="section">
        <div class="section-header">
          Результаты
          <span style="font-size:13px;color:var(--text2);font-weight:400;">
            Найдено: {total} · показано: {len(results)}
          </span>
        </div>
        <div style="padding:0 20px;">{results_html}</div>
      </div>
      '''}
    </div>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_msg_search = require_auth("view_deleted")(handle_msg_search)


# ══════════════════════════════════════════
#  API для новых фич
# ══════════════════════════════════════════

async def api_mod_stats(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        conn = db.get_conn()
        rows = conn.execute(
            "SELECT by_name, "
            "SUM(CASE WHEN action LIKE '%Бан%' THEN 1 ELSE 0 END) as bans, "
            "SUM(CASE WHEN action LIKE '%Варн%' THEN 1 ELSE 0 END) as warns, "
            "SUM(CASE WHEN action LIKE '%Мут%' THEN 1 ELSE 0 END) as mutes "
            "FROM mod_history GROUP BY by_name ORDER BY (bans+warns+mutes) DESC LIMIT 8"
        ).fetchall()
        conn.close()
        data = [dict(r) for r in rows]
        return web.json_response({
            "names": [r["by_name"] for r in data],
            "bans":  [r["bans"]  for r in data],
            "warns": [r["warns"] for r in data],
            "mutes": [r["mutes"] for r in data],
        })
    except:
        return web.json_response({"names":[],"bans":[],"warns":[],"mutes":[]})


async def api_threats(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not token or token not in _dashboard_sessions:
        return web.json_response({"error": "Unauthorized"}, status=401)
    _analyze_threats()
    active = [t for t in _threat_log if t.get("status") == "active"]
    return web.json_response({"count": len(active), "threats": active[:10]})

# ══════════════════════════════════════════════════════════════════════
#  🛡 СТРАНИЦА ЗАЩИТЫ — /dashboard/security
# ══════════════════════════════════════════════════════════════════════

async def handle_security(request: web.Request) -> web.Response:
    sess = _get_session(request)
    if not sess:
        raise web.HTTPFound("/dashboard/login")
    _track_session(request)

    if not _SF_LOADED:
        return web.Response(
            text=page(navbar(sess, "security") + """
            <div class="container">
              <div class="page-title">🛡 Защита</div>
              <div class="card" style="padding:32px;text-align:center;color:var(--t2);">
                Модули защиты не загружены.<br>
                Убедись что <code>antiraid.py</code>, <code>night_mode.py</code>,
                <code>security_features.py</code> находятся в папке бота
                и перезапусти бота.
              </div>
            </div>"""),
            content_type="text/html"
        )

    chats = [dict(r) for r in await db.get_all_chats()]
    cid_raw = request.match_info.get("cid", "") or request.rel_url.query.get("cid", "")
    cid = int(cid_raw) if cid_raw and cid_raw.isdigit() else (chats[0]["cid"] if chats else 0)
    chat_title = next((c.get("title", str(cid)) for c in chats if c["cid"] == cid), str(cid))

    # ── POST — сохранение ─────────────────────────────────────
    if request.method == "POST":
        data = await request.post()
        section = data.get("section", "")

        if section == "night_mode" and cid:
            upd = {}
            for f in ["enabled","read_only","block_media","block_stickers",
                      "block_voice","block_files","block_forwards",
                      "notify","delete_violations","warn_violators"]:
                upd[f] = data.get(f) == "1"
            for f in ["start_time","end_time","msg_start","msg_end"]:
                if f in data: upd[f] = data[f]
            try: upd["slowmode"] = int(data.get("slowmode", 0))
            except: pass
            night_mode.update_config(cid, upd)

        elif section == "antiraid" and cid:
            upd = {}
            for f in ["enabled","scam_check","notify_admins"]:
                upd[f] = data.get(f) == "1"
            for f, d in [("threshold",7),("window_secs",10),
                         ("lock_minutes",10),("slowmode_delay",30),("scam_threshold",60)]:
                try: upd[f] = int(data.get(f, d))
                except: pass
            if "action" in data: upd["action"] = data["action"]
            antiraid.update_chat_raid_cfg(cid, upd)

        elif section == "antilink" and cid:
            upd = {}
            for f in ["enabled","warn_on_del","allow_admins","block_tg_invites","block_masked"]:
                upd[f] = data.get(f) == "1"
            if "action" in data: upd["action"] = data["action"]
            wl_raw = data.get("whitelist", "")
            upd["whitelist"] = [d.strip() for d in wl_raw.split("\n") if d.strip()]
            sf.update_antilink_cfg(cid, upd)

        elif section == "avatarcheck" and cid:
            upd = {
                "enabled": data.get("enabled") == "1",
                "action":  data.get("action", "kick"),
            }
            try: upd["mute_mins"] = int(data.get("mute_mins", 60))
            except: pass
            sf.update_avatar_cfg(cid, upd)

        raise web.HTTPFound(f"/dashboard/security/{cid}?saved=1")

    # ── GET — отрисовка ───────────────────────────────────────
    nm_cfg   = night_mode.get_chat_config(cid) if cid else {}
    ar_cfg   = antiraid.get_chat_raid_cfg(cid) if cid else {}
    al_cfg   = sf.get_antilink_cfg(cid)        if cid else {}
    av_cfg   = sf.get_avatar_cfg(cid)          if cid else {}
    al_stats = sf.get_antilink_stats(cid)      if cid else {}
    rd_stats = antiraid.get_raid_stats_for_dashboard()

    saved = request.rel_url.query.get("saved") == "1"

    chat_opts = "".join(
        f'<option value="{c["cid"]}" {"selected" if c["cid"]==cid else ""}>'
        f'{c.get("title","") or c["cid"]}</option>'
        for c in chats
    )

    def tog(key, label, val):
        ch = "checked" if val else ""
        return (f'<label class="sec-toggle-row">'
                f'<span>{label}</span>'
                f'<input type="checkbox" name="{key}" value="1" {ch} class="sec-cb">'
                f'<span class="sec-slider"></span></label>')

    def dot(active):
        if active is True:   return '<span class="sdot sdot-on"></span>'
        if active is False:  return '<span class="sdot sdot-off"></span>'
        return '<span class="sdot sdot-warn"></span>'

    nm_active  = nm_cfg.get("is_active", False)
    nm_enabled = nm_cfg.get("enabled",   False)

    # Последние события антилинк
    al_recent = al_stats.get("recent", [])[:5]
    al_log_html = "".join(
        f'<div class="ev-row">'
        f'<span class="ev-badge ev-red">ССЫЛКА</span>'
        f'<span>@{r.get("username") or r.get("uid","?")} — <code>{r.get("url","")}</code></span>'
        f'<span class="ev-time">{_fmt_ts(r.get("ts",0))}</span></div>'
        for r in al_recent
    ) or '<div class="ev-empty">Нарушений не было</div>'

    # Последние рейды
    rd_recent = rd_stats.get("recent_raids", [])[:5]
    rd_log_html = "".join(
        f'<div class="ev-row">'
        f'<span class="ev-badge ev-red">РЕЙД</span>'
        f'<span>{r.get("chat_title","?")} — {r.get("join_count","?")} чел.</span>'
        f'<span class="ev-time">{_fmt_ts(r.get("ts",0))}</span></div>'
        for r in rd_recent
    ) or '<div class="ev-empty">Рейдов не было</div>'

    scam_recent = rd_stats.get("recent_scams", [])[:5]
    scam_log_html = "".join(
        f'<div class="ev-row">'
        f'<span class="ev-badge ev-ylw">СКАМ</span>'
        f'<span>@{r.get("username") or r.get("uid","?")} — риск {r.get("score","?")}%</span>'
        f'<span class="ev-time">{_fmt_ts(r.get("ts",0))}</span></div>'
        for r in scam_recent
    ) or '<div class="ev-empty">Скам-аккаунтов не обнаружено</div>'

    wl_val = "\n".join(al_cfg.get("whitelist", []))

    action_opts_al = "".join(
        f'<option value="{v}" {"selected" if al_cfg.get("action")==v else ""}>{l}</option>'
        for v, l in [("delete","Удалить"),("warn","Предупредить"),
                     ("mute","Замьютить 30м"),("kick","Кикнуть"),("ban","Забанить")]
    )
    action_opts_ar = "".join(
        f'<option value="{v}" {"selected" if ar_cfg.get("action")==v else ""}>{l}</option>'
        for v, l in [("lock","🔒 Заблокировать чат"),
                     ("slowmode","🐢 Замедленный режим"),
                     ("members_only","👥 Запрет новым")]
    )
    action_opts_av = "".join(
        f'<option value="{v}" {"selected" if av_cfg.get("action")==v else ""}>{l}</option>'
        for v, l in [("kick","👢 Кикнуть"),("ban","🔨 Забанить"),
                     ("mute","🔇 Замьютить"),("warn","⚠️ Предупредить")]
    )

    save_flash = ('<div class="flash flash-ok">✅ Настройки сохранены</div>' if saved else "")

    body = navbar(sess, "security") + f"""
    <div class="container">
      {save_flash}
      <div class="page-title">🛡 Защита чатов</div>

      <div class="sec-topbar">
        <select class="form-control sec-chat-sel"
          onchange="window.location='/dashboard/security/'+this.value">
          {chat_opts}
        </select>
        <div class="sec-counters">
          <div class="sec-cnt sec-cnt-red">
            <span>🚨</span><span>Рейдов сегодня</span>
            <strong>{rd_stats.get("raids_today",0)}</strong>
          </div>
          <div class="sec-cnt sec-cnt-ylw">
            <span>🤖</span><span>Скам всего</span>
            <strong>{rd_stats.get("scams_total",0)}</strong>
          </div>
          <div class="sec-cnt sec-cnt-blue">
            <span>🔗</span><span>Блок. ссылок сегодня</span>
            <strong>{al_stats.get("today",0)}</strong>
          </div>
          <div class="sec-cnt {"sec-cnt-green" if nm_active else "sec-cnt-dim"}">
            <span>🌙</span><span>Ночной режим</span>
            <strong>{"АКТИВЕН" if nm_active else "ВЫКЛЮЧЕН"}</strong>
          </div>
        </div>
      </div>

      <div class="sec-grid">

        <!-- ── НОЧНОЙ РЕЖИМ ───────────────────── -->
        <div class="sec-card">
          <div class="sec-card-head">
            <div class="sec-card-title">{dot(nm_active if nm_enabled else False)} Ночной режим</div>
            <div class="sec-force-btns">
              <button class="btn-force btn-force-on"  onclick="forceNight({cid},true)">▶ Вкл</button>
              <button class="btn-force btn-force-off" onclick="forceNight({cid},false)">■ Выкл</button>
            </div>
          </div>
          <form method="POST">
            <input type="hidden" name="section" value="night_mode">
            {tog("enabled","Включить ночной режим",nm_cfg.get("enabled",False))}
            <div class="sec-time-row">
              <div><label>Начало</label>
                <input class="form-control" type="time" name="start_time"
                  value="{nm_cfg.get("start_time","23:00")}"></div>
              <div><label>Конец</label>
                <input class="form-control" type="time" name="end_time"
                  value="{nm_cfg.get("end_time","07:00")}"></div>
            </div>
            <div class="sec-sublabel">Ограничения</div>
            {tog("read_only","🔇 Полный запрет (только чтение)",nm_cfg.get("read_only",False))}
            {tog("block_media","🖼 Блокировать медиа",nm_cfg.get("block_media",True))}
            {tog("block_stickers","😊 Блокировать стикеры",nm_cfg.get("block_stickers",True))}
            {tog("block_voice","🎤 Блокировать голосовые",nm_cfg.get("block_voice",True))}
            {tog("block_files","📎 Блокировать файлы",nm_cfg.get("block_files",False))}
            {tog("block_forwards","↩️ Блокировать форварды",nm_cfg.get("block_forwards",True))}
            <div class="sec-sublabel">Поведение</div>
            {tog("notify","📢 Уведомлять чат",nm_cfg.get("notify",True))}
            {tog("delete_violations","🗑 Удалять нарушения",nm_cfg.get("delete_violations",True))}
            {tog("warn_violators","⚠️ Предупреждать нарушителей",nm_cfg.get("warn_violators",False))}
            <div class="sec-field-row">
              <label>Замедленный режим (сек, 0=выкл)</label>
              <input class="form-control" type="number" name="slowmode" min="0" max="600"
                value="{nm_cfg.get("slowmode",30)}" style="width:90px">
            </div>
            <div class="sec-field-row">
              <label>Сообщение при включении</label>
              <textarea class="form-control sec-ta" name="msg_start">{nm_cfg.get("msg_start","🌙 Ночной режим активирован")}</textarea>
            </div>
            <div class="sec-field-row">
              <label>Сообщение при выключении</label>
              <textarea class="form-control sec-ta" name="msg_end">{nm_cfg.get("msg_end","☀️ Доброе утро!")}</textarea>
            </div>
            <button type="submit" class="btn btn-primary sec-save-btn">💾 Сохранить</button>
          </form>
        </div>

        <!-- ── АНТИРЕЙД + СКАМ ────────────────── -->
        <div class="sec-card">
          <div class="sec-card-head">
            <div class="sec-card-title">{dot("warn" if rd_stats.get("active_raids",0) > 0 else ar_cfg.get("enabled",False))} Антирейд + Скам</div>
          </div>
          <form method="POST">
            <input type="hidden" name="section" value="antiraid">
            {tog("enabled","Антирейд включён",ar_cfg.get("enabled",True))}
            {tog("scam_check","🤖 Детектор скам-аккаунтов",ar_cfg.get("scam_check",True))}
            {tog("notify_admins","🔔 Уведомлять администраторов",ar_cfg.get("notify_admins",True))}
            <div class="sec-sublabel">Порог рейда</div>
            <div class="sec-time-row">
              <div><label>Входов за окно</label>
                <input class="form-control" type="number" name="threshold" min="3" max="100"
                  value="{ar_cfg.get("threshold",7)}"></div>
              <div><label>Окно (сек)</label>
                <input class="form-control" type="number" name="window_secs" min="5" max="300"
                  value="{ar_cfg.get("window_secs",10)}"></div>
            </div>
            <div class="sec-sublabel">Действие при рейде</div>
            <select class="form-control" name="action" style="margin-bottom:10px;">
              {action_opts_ar}
            </select>
            <div class="sec-time-row">
              <div><label>Блокировка (мин)</label>
                <input class="form-control" type="number" name="lock_minutes" min="1" max="1440"
                  value="{ar_cfg.get("lock_minutes",10)}"></div>
              <div><label>Слоумод (сек)</label>
                <input class="form-control" type="number" name="slowmode_delay" min="5" max="600"
                  value="{ar_cfg.get("slowmode_delay",30)}"></div>
            </div>
            <div class="sec-sublabel">Порог скама (сейчас {ar_cfg.get("scam_threshold",60)}%)</div>
            <input type="range" name="scam_threshold" min="20" max="100"
              value="{ar_cfg.get("scam_threshold",60)}" class="sec-range"
              oninput="this.previousElementSibling.textContent='Порог скама (сейчас '+this.value+'%)'">
            <div class="sec-range-labels"><span>Агрессивно (20)</span><span>Строго (100)</span></div>
            <button type="submit" class="btn btn-primary sec-save-btn">💾 Сохранить</button>
          </form>
        </div>

        <!-- ── АНТИЛИНК ───────────────────────── -->
        <div class="sec-card">
          <div class="sec-card-head">
            <div class="sec-card-title">{dot(al_cfg.get("enabled",False))} Антилинк</div>
            <span class="sec-stat-pill sec-stat-red">{al_stats.get("total",0)} блок.</span>
          </div>
          <form method="POST">
            <input type="hidden" name="section" value="antilink">
            {tog("enabled","Антилинк включён",al_cfg.get("enabled",False))}
            {tog("block_tg_invites","📨 Блок. Telegram-инвайты",al_cfg.get("block_tg_invites",True))}
            {tog("block_masked","🎭 Блок. замаскированные ссылки",al_cfg.get("block_masked",True))}
            {tog("allow_admins","👑 Разрешить администраторам",al_cfg.get("allow_admins",True))}
            {tog("warn_on_del","⚠️ Предупреждать при удалении",al_cfg.get("warn_on_del",True))}
            <div class="sec-sublabel">Действие при нарушении</div>
            <select class="form-control" name="action" style="margin-bottom:12px;">
              {action_opts_al}
            </select>
            <div class="sec-sublabel">Белый список доменов (по одному на строку)</div>
            <textarea class="form-control sec-ta sec-ta-lg" name="whitelist">{wl_val}</textarea>
            <button type="submit" class="btn btn-primary sec-save-btn">💾 Сохранить</button>
          </form>
        </div>

        <!-- ── ПРОВЕРКА АВАТАРА ───────────────── -->
        <div class="sec-card">
          <div class="sec-card-head">
            <div class="sec-card-title">{dot(av_cfg.get("enabled",False))} Проверка аватара</div>
          </div>
          <form method="POST">
            <input type="hidden" name="section" value="avatarcheck">
            {tog("enabled","Проверка аватара при входе",av_cfg.get("enabled",False))}
            <div class="sec-sublabel">Действие при отсутствии аватара</div>
            <select class="form-control" name="action" style="margin-bottom:10px;">
              {action_opts_av}
            </select>
            <div class="sec-field-row">
              <label>Мут на (мин, при действии «Замьютить»)</label>
              <input class="form-control" type="number" name="mute_mins" min="1" max="10080"
                value="{av_cfg.get("mute_mins",60)}" style="width:90px">
            </div>
            <button type="submit" class="btn btn-primary sec-save-btn">💾 Сохранить</button>
          </form>
        </div>

        <!-- ── ЛОГ СОБЫТИЙ ────────────────────── -->
        <div class="sec-card sec-card-full">
          <div class="sec-card-head">
            <div class="sec-card-title">📋 Последние события</div>
          </div>
          <div class="sec-log-grid">
            <div>
              <div class="sec-sublabel" style="margin-bottom:8px;">🚨 Рейды</div>
              {rd_log_html}
            </div>
            <div>
              <div class="sec-sublabel" style="margin-bottom:8px;">🤖 Скам-аккаунты</div>
              {scam_log_html}
            </div>
            <div>
              <div class="sec-sublabel" style="margin-bottom:8px;">🔗 Блокировки ссылок</div>
              {al_log_html}
            </div>
          </div>
        </div>

      </div><!-- /.sec-grid -->
    </div><!-- /.container -->

    <style>
    .sec-topbar {{ display:flex; align-items:center; gap:16px; margin-bottom:20px; flex-wrap:wrap; }}
    .sec-chat-sel {{ max-width:260px; }}
    .sec-counters {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .sec-cnt {{ display:flex; align-items:center; gap:6px; padding:6px 12px;
                border-radius:8px; font-size:13px; border:1px solid var(--br1); }}
    .sec-cnt strong {{ font-size:15px; font-weight:700; margin-left:4px; }}
    .sec-cnt-red  {{ border-color:rgba(242,63,66,.3);  color:var(--red);   background:var(--red-g); }}
    .sec-cnt-ylw  {{ border-color:rgba(240,177,50,.3); color:var(--ylw);   background:var(--ylw-g); }}
    .sec-cnt-blue {{ border-color:rgba(59,130,246,.3); color:var(--blue);  background:var(--blue-g); }}
    .sec-cnt-green{{ border-color:rgba(35,165,90,.3);  color:var(--green); background:var(--green-g); }}
    .sec-cnt-dim  {{ border-color:var(--br1); color:var(--t3); }}
    .sec-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
    .sec-card {{ background:var(--bg3); border:1px solid var(--br1); border-radius:var(--r2); padding:20px; }}
    .sec-card-full {{ grid-column:1 / -1; }}
    .sec-card-head {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }}
    .sec-card-title {{ display:flex; align-items:center; gap:8px; font-size:14px; font-weight:600; color:var(--t1); }}
    .sdot {{ display:inline-block; width:8px; height:8px; border-radius:50%; flex-shrink:0; }}
    .sdot-on   {{ background:#23a55a; box-shadow:0 0 6px #23a55a; }}
    .sdot-off  {{ background:var(--t3); }}
    .sdot-warn {{ background:var(--ylw); box-shadow:0 0 6px var(--ylw); }}
    .sec-toggle-row {{ display:flex; align-items:center; justify-content:space-between;
                       padding:7px 0; border-bottom:1px solid var(--br0); cursor:pointer;
                       font-size:13px; color:var(--t2); gap:8px; }}
    .sec-toggle-row:last-of-type {{ border-bottom:none; }}
    .sec-cb {{ display:none; }}
    .sec-slider {{ position:relative; width:36px; height:20px; flex-shrink:0;
                   background:var(--br2); border-radius:10px; transition:.2s; }}
    .sec-slider::before {{ content:""; position:absolute; width:14px; height:14px;
                           background:#fff; border-radius:50%; top:3px; left:3px; transition:.2s; }}
    .sec-cb:checked + .sec-slider {{ background:var(--acc); }}
    .sec-cb:checked + .sec-slider::before {{ transform:translateX(16px); }}
    .sec-sublabel {{ font-size:11px; font-weight:600; text-transform:uppercase;
                     letter-spacing:.06em; color:var(--t3); margin:14px 0 6px; }}
    .sec-time-row {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:12px; }}
    .sec-time-row label {{ font-size:11px; color:var(--t3); display:block; margin-bottom:4px; }}
    .sec-field-row {{ margin:10px 0; }}
    .sec-field-row label {{ font-size:12px; color:var(--t2); display:block; margin-bottom:4px; }}
    .sec-ta {{ resize:vertical; min-height:64px; font-size:12px; }}
    .sec-ta-lg {{ min-height:110px; }}
    .sec-save-btn {{ width:100%; margin-top:16px; }}
    .sec-range {{ width:100%; margin:4px 0; accent-color:var(--acc); }}
    .sec-range-labels {{ display:flex; justify-content:space-between; font-size:10px; color:var(--t3); margin-bottom:10px; }}
    .sec-stat-pill {{ padding:2px 10px; border-radius:99px; font-size:11px; font-weight:600; }}
    .sec-stat-red {{ background:var(--red-g); color:var(--red); }}
    .sec-force-btns {{ display:flex; gap:6px; }}
    .btn-force {{ padding:4px 12px; border-radius:6px; border:1px solid var(--br2);
                  background:transparent; cursor:pointer; font-size:12px; font-weight:500;
                  transition:.15s; color:var(--t1); }}
    .btn-force-on:hover  {{ border-color:rgba(35,165,90,.5); color:var(--green); background:var(--green-g); }}
    .btn-force-off:hover {{ border-color:rgba(242,63,66,.5); color:var(--red);   background:var(--red-g); }}
    .sec-log-grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:20px; }}
    .ev-row {{ display:flex; align-items:center; gap:8px; padding:6px 0;
               border-bottom:1px solid var(--br0); font-size:12px; flex-wrap:wrap; }}
    .ev-row:last-child {{ border-bottom:none; }}
    .ev-badge {{ padding:2px 7px; border-radius:4px; font-size:10px; font-weight:700; flex-shrink:0; }}
    .ev-red  {{ background:var(--red-g);  color:var(--red); }}
    .ev-ylw  {{ background:var(--ylw-g);  color:var(--ylw); }}
    .ev-time {{ margin-left:auto; color:var(--t3); font-size:11px; white-space:nowrap; }}
    .ev-empty {{ color:var(--t3); font-size:13px; padding:12px 0; }}
    .flash {{ padding:10px 16px; border-radius:8px; margin-bottom:16px; font-size:13px; }}
    .flash-ok {{ background:var(--green-g); color:var(--green); border:1px solid rgba(35,165,90,.3); }}
    @media(max-width:900px){{
      .sec-grid {{ grid-template-columns:1fr; }}
      .sec-card-full {{ grid-column:1; }}
      .sec-log-grid {{ grid-template-columns:1fr; }}
    }}
    </style>

    <script>
    async function forceNight(cid, activate) {{
      const r = await fetch('/api/security/force_night', {{
        method:'POST', headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{cid, activate}})
      }});
      const d = await r.json();
      if(d.ok) {{ location.reload(); }}
      else {{ alert('Ошибка: ' + (d.error||'?')); }}
    }}
    </script>
    """

    return web.Response(text=page(body), content_type="text/html")


def _fmt_ts(ts):
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%d.%m %H:%M")
    except:
        return "—"


async def api_force_night(request: web.Request) -> web.Response:
    sess = _get_session(request)
    if not sess:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)
    try:
        data     = await request.json()
        cid      = int(data.get("cid", 0))
        activate = bool(data.get("activate", True))
        if not cid:
            return web.json_response({"ok": False, "error": "cid required"})
        if activate:
            await night_mode.force_activate(cid)
        else:
            await night_mode.force_deactivate(cid)
        return web.json_response({"ok": True})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})


async def api_force_raid(request: web.Request) -> web.Response:
    sess = _get_session(request)
    if not sess:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)
    return web.json_response({"ok": True})


async def api_security_stats(request: web.Request) -> web.Response:
    sess = _get_session(request)
    if not sess:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        cid_raw = request.rel_url.query.get("cid", "")
        cid     = int(cid_raw) if cid_raw.isdigit() else 0
        rd      = antiraid.get_raid_stats_for_dashboard()
        al      = sf.get_antilink_stats(cid) if cid else sf.get_antilink_stats()
        nm_on   = night_mode.is_active(cid) if cid else False
        return web.json_response({
            "ok": True,
            "raids_today":       rd.get("raids_today", 0),
            "scams_total":       rd.get("scams_total", 0),
            "active_raids":      rd.get("active_raids", 0),
            "antilink_today":    al.get("today", 0),
            "night_mode_active": nm_on,
        })
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})

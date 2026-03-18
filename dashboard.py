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

def set_bot(bot, admin_ids: set):
    global _bot, _admin_ids
    _bot = bot
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
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CHAT GUARD — Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  :root[data-theme="dark"] {
    --bg: #080b14; --bg2: #0e1220; --bg3: #131926; --bg4: #1a2035;
    --border: #1e2a40; --border2: #243050;
    --text: #e2e8f0; --text2: #64748b; --text3: #94a3b8;
    --accent: #3b82f6; --accent2: #1d4ed8; --accent-glow: rgba(59,130,246,0.15);
    --danger: #ef4444; --danger2: #dc2626;
    --success: #22c55e; --success2: #16a34a;
    --warn: #f59e0b; --warn2: #d97706;
    --purple: #a855f7;
    --gold: #fbbf24;
    --sidebar-w: 240px;
  }
  :root[data-theme="light"] {
    --bg: #f1f5f9; --bg2: #ffffff; --bg3: #f8fafc; --bg4: #e2e8f0;
    --border: #e2e8f0; --border2: #cbd5e1;
    --text: #0f172a; --text2: #64748b; --text3: #475569;
    --accent: #2563eb; --accent2: #1d4ed8; --accent-glow: rgba(37,99,235,0.1);
    --danger: #dc2626; --danger2: #b91c1c;
    --success: #16a34a; --success2: #15803d;
    --warn: #d97706; --warn2: #b45309;
    --purple: #9333ea;
    --gold: #d97706;
    --sidebar-w: 240px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Syne', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    display: flex;
  }

  /* ── SIDEBAR ───────────────────────────── */
  .sidebar {
    width: var(--sidebar-w);
    background: var(--bg2);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0; left: 0; bottom: 0;
    z-index: 200;
    transition: transform .3s;
  }
  .sidebar-brand {
    padding: 20px 18px 16px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .sidebar-brand .logo {
    width: 34px; height: 34px;
    background: linear-gradient(135deg, var(--accent), var(--purple));
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 16px; font-weight: 800;
    color: #fff;
    box-shadow: 0 0 20px var(--accent-glow);
  }
  .sidebar-brand .brand-text {
    font-size: 14px; font-weight: 800; letter-spacing: 1px;
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
  }
  .sidebar-brand .brand-sub {
    font-size: 10px; color: var(--text2); letter-spacing: 0.5px;
  }
  .sidebar-user {
    padding: 12px 18px;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
  }
  .sidebar-user .u-name { font-weight: 700; color: var(--text); font-size: 13px; }
  .sidebar-user .u-rank {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 10px; font-weight: 700; margin-top: 4px;
    background: var(--accent-glow); color: var(--accent);
  }
  .sidebar-nav { flex: 1; overflow-y: auto; padding: 8px 0; }
  .nav-section {
    font-size: 10px; color: var(--text2); letter-spacing: 1.5px;
    padding: 12px 18px 6px; text-transform: uppercase; font-weight: 700;
  }
  .nav-link {
    display: flex; align-items: center; gap: 10px;
    padding: 9px 18px;
    color: var(--text2); text-decoration: none;
    font-size: 13px; font-weight: 600;
    border-left: 3px solid transparent;
    transition: all .15s;
    position: relative;
  }
  .nav-link:hover { background: var(--bg3); color: var(--text); }
  .nav-link.active {
    background: var(--accent-glow); color: var(--accent);
    border-left-color: var(--accent);
  }
  .nav-link .nav-icon { font-size: 15px; width: 20px; text-align: center; }
  .nav-badge {
    margin-left: auto; background: var(--danger);
    color: #fff; font-size: 10px; font-weight: 700;
    padding: 1px 6px; border-radius: 10px; min-width: 18px; text-align: center;
  }
  .sidebar-footer {
    padding: 12px 18px;
    border-top: 1px solid var(--border);
    display: flex; justify-content: space-between; align-items: center;
  }
  .sidebar-toggle {
    display: none; position: fixed; top: 12px; left: 12px; z-index: 300;
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 8px; padding: 6px 10px; cursor: pointer; color: var(--text);
    font-size: 18px;
  }

  /* ── MAIN CONTENT ──────────────────────── */
  .main {
    margin-left: var(--sidebar-w);
    flex: 1;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
  }
  .topbar {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 12px 24px;
    display: flex; align-items: center; justify-content: space-between;
    position: sticky; top: 0; z-index: 100;
  }
  .topbar-left { display: flex; align-items: center; gap: 12px; }
  .topbar-right { display: flex; align-items: center; gap: 12px; }
  .topbar-title { font-size: 16px; font-weight: 700; }
  .search-global {
    position: relative;
  }
  .search-global input {
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; padding: 7px 12px 7px 34px;
    color: var(--text); font-size: 13px; width: 220px;
    font-family: 'Syne', sans-serif;
    transition: border-color .2s, width .2s;
  }
  .search-global input:focus {
    outline: none; border-color: var(--accent); width: 280px;
  }
  .search-global .search-icon {
    position: absolute; left: 10px; top: 50%; transform: translateY(-50%);
    color: var(--text2); font-size: 14px;
  }
  #search-results {
    position: absolute; top: calc(100% + 6px); left: 0; right: 0;
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 10px; box-shadow: 0 10px 40px rgba(0,0,0,.4);
    display: none; z-index: 999; max-height: 320px; overflow-y: auto;
  }
  #search-results .sr-item {
    padding: 10px 14px; display: flex; align-items: center; gap: 10px;
    cursor: pointer; transition: background .1s; font-size: 13px;
  }
  #search-results .sr-item:hover { background: var(--bg3); }
  .container { padding: 24px; flex: 1; }
  .page-title {
    font-size: 22px; font-weight: 800; margin-bottom: 24px;
    display: flex; align-items: center; gap: 12px;
  }

  /* ── CARDS ─────────────────────────────── */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px; margin-bottom: 28px;
  }
  .card {
    background: var(--bg2); border-radius: 14px; padding: 20px;
    border: 1px solid var(--border);
    position: relative; overflow: hidden;
    transition: border-color .2s, transform .2s;
  }
  .card:hover { border-color: var(--border2); transform: translateY(-2px); }
  .card::before {
    content: ''; position: absolute;
    top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, var(--accent), var(--purple));
    opacity: 0;
    transition: opacity .2s;
  }
  .card:hover::before { opacity: 1; }
  .card .card-icon { font-size: 22px; margin-bottom: 8px; }
  .card .card-label {
    font-size: 11px; color: var(--text2); text-transform: uppercase;
    letter-spacing: 1px; font-weight: 700; margin-bottom: 6px;
  }
  .card .card-value {
    font-size: 30px; font-weight: 800; color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    transition: color .3s;
  }
  .card .card-sub { font-size: 11px; color: var(--text2); margin-top: 4px; }
  .card .card-trend {
    position: absolute; top: 16px; right: 16px;
    font-size: 12px; font-weight: 700;
  }
  .card .card-trend.up { color: var(--success); }
  .card .card-trend.down { color: var(--danger); }
  .card .sparkline { margin-top: 12px; height: 32px; }

  /* ── SECTION ───────────────────────────── */
  .section {
    background: var(--bg2); border-radius: 14px;
    border: 1px solid var(--border); margin-bottom: 20px;
    overflow: hidden;
  }
  .section-header {
    padding: 14px 20px; border-bottom: 1px solid var(--border);
    font-weight: 700; font-size: 14px;
    display: flex; justify-content: space-between; align-items: center;
  }
  .section-body { padding: 20px; }

  /* ── TABLE ─────────────────────────────── */
  table { width: 100%; border-collapse: collapse; }
  th {
    font-size: 10px; text-transform: uppercase; letter-spacing: 1px;
    color: var(--text2); background: var(--bg3); padding: 10px 16px;
    text-align: left; font-weight: 700;
  }
  td { padding: 11px 16px; border-bottom: 1px solid var(--border); font-size: 13px; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: var(--bg3); }

  /* ── BADGES ────────────────────────────── */
  .badge {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 3px 9px; border-radius: 6px;
    font-size: 11px; font-weight: 700; white-space: nowrap;
  }
  .badge-open     { background: rgba(34,197,94,.12);  color: #22c55e; }
  .badge-progress { background: rgba(245,158,11,.12); color: #f59e0b; }
  .badge-closed   { background: rgba(100,116,139,.12);color: #64748b; }
  .badge-danger   { background: rgba(239,68,68,.12);  color: #ef4444; }
  .badge-urgent   { background: rgba(239,68,68,.15);  color: #ef4444; }
  .badge-high     { background: rgba(245,158,11,.15); color: #f59e0b; }
  .badge-normal   { background: rgba(34,197,94,.12);  color: #22c55e; }
  .badge-low      { background: rgba(100,116,139,.12);color: #94a3b8; }
  .badge-accent   { background: var(--accent-glow);   color: var(--accent); }

  /* ── BUTTONS ───────────────────────────── */
  .btn {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 8px 16px; border-radius: 8px; font-size: 13px;
    font-weight: 700; text-decoration: none; border: none;
    cursor: pointer; transition: all .15s;
    font-family: 'Syne', sans-serif;
    white-space: nowrap;
  }
  .btn:hover { transform: translateY(-1px); opacity: .9; }
  .btn:active { transform: translateY(0); }
  .btn-primary  { background: var(--accent); color: #fff; }
  .btn-danger   { background: var(--danger); color: #fff; }
  .btn-success  { background: var(--success); color: #fff; }
  .btn-warn     { background: var(--warn); color: #fff; }
  .btn-outline  { background: transparent; border: 1px solid var(--border); color: var(--text3); }
  .btn-ghost    { background: var(--bg3); color: var(--text3); }
  .btn-sm       { padding: 5px 10px; font-size: 12px; border-radius: 6px; }
  .btn-xs       { padding: 3px 8px; font-size: 11px; border-radius: 5px; }

  /* ── FORMS ─────────────────────────────── */
  .form-group { margin-bottom: 16px; }
  .form-group label {
    display: block; margin-bottom: 6px;
    font-size: 12px; color: var(--text2); font-weight: 700;
    letter-spacing: 0.5px; text-transform: uppercase;
  }
  .form-control {
    width: 100%; padding: 10px 14px;
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; color: var(--text); font-size: 13px;
    font-family: 'Syne', sans-serif;
    transition: border-color .2s;
  }
  .form-control:focus { outline: none; border-color: var(--accent); }
  select.form-control option { background: var(--bg2); }
  textarea.form-control { resize: vertical; min-height: 80px; }

  /* ── TOGGLE ────────────────────────────── */
  .toggle-wrap {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 0; border-bottom: 1px solid var(--border);
  }
  .toggle-wrap:last-child { border-bottom: none; }
  .toggle-info b { display: block; font-size: 13px; color: var(--text); }
  .toggle-info span { font-size: 12px; color: var(--text2); }
  .toggle-switch { position: relative; display: inline-block; width: 44px; height: 24px; flex-shrink: 0; }
  .toggle-switch input { opacity: 0; width: 0; height: 0; }
  .toggle-slider {
    position: absolute; cursor: pointer; inset: 0;
    background: var(--bg4); border-radius: 24px; transition: .25s;
  }
  .toggle-slider:before {
    position: absolute; content: ""; height: 18px; width: 18px;
    left: 3px; bottom: 3px; background: white;
    border-radius: 50%; transition: .25s;
  }
  input:checked + .toggle-slider { background: var(--accent); }
  input:checked + .toggle-slider:before { transform: translateX(20px); }

  /* ── ALERTS ────────────────────────────── */
  .alert-item {
    padding: 14px 18px; border-left: 3px solid var(--danger);
    margin-bottom: 8px; background: rgba(239,68,68,.05);
    border-radius: 0 10px 10px 0; transition: background .2s;
  }
  .alert-item:hover { background: rgba(239,68,68,.08); }
  .alert-item.warn { border-color: var(--warn); background: rgba(245,158,11,.05); }
  .alert-item.warn:hover { background: rgba(245,158,11,.08); }
  .alert-item.info { border-color: var(--accent); background: var(--accent-glow); }

  /* ── MODAL ─────────────────────────────── */
  .modal-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,.6);
    z-index: 1000; display: none; align-items: center; justify-content: center;
  }
  .modal-overlay.active { display: flex; }
  .modal {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 16px; padding: 28px; min-width: 380px; max-width: 520px;
    width: 90%; box-shadow: 0 20px 60px rgba(0,0,0,.5);
    animation: modalIn .2s ease;
  }
  @keyframes modalIn { from { transform: scale(.95); opacity: 0; } to { transform: scale(1); opacity: 1; } }
  .modal-title { font-size: 18px; font-weight: 800; margin-bottom: 20px; }
  .modal-footer { display: flex; gap: 10px; margin-top: 20px; justify-content: flex-end; }

  /* ── TOAST ─────────────────────────────── */
  #toast-container {
    position: fixed; bottom: 24px; right: 24px; z-index: 9999;
    display: flex; flex-direction: column; gap: 8px;
  }
  .toast {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 12px; padding: 14px 18px; min-width: 280px;
    box-shadow: 0 8px 30px rgba(0,0,0,.3);
    animation: toastIn .3s ease;
    display: flex; align-items: center; gap: 12px; font-size: 13px;
  }
  .toast.success { border-left: 3px solid var(--success); }
  .toast.danger  { border-left: 3px solid var(--danger); }
  .toast.info    { border-left: 3px solid var(--accent); }
  .toast.warn    { border-left: 3px solid var(--warn); }
  @keyframes toastIn { from { transform: translateX(20px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
  @keyframes toastOut { to { transform: translateX(20px); opacity: 0; } }

  /* ── RANK CARD ─────────────────────────── */
  .rank-card {
    border: 1px solid var(--border); border-radius: 12px; padding: 16px;
    display: flex; align-items: center; gap: 14px;
    transition: border-color .2s;
    cursor: pointer;
  }
  .rank-card:hover { border-color: var(--border2); }
  .rank-badge {
    width: 42px; height: 42px; border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px; font-weight: 800; flex-shrink: 0;
  }
  .rank-name { font-weight: 700; font-size: 14px; }
  .rank-desc { font-size: 12px; color: var(--text2); margin-top: 2px; }
  .rank-perms { font-size: 11px; color: var(--text2); margin-top: 6px; }

  /* ── CHAT BUBBLES ──────────────────────── */
  .bubble-wrap { display: flex; flex-direction: column; gap: 10px; }
  .bubble {
    max-width: 75%; padding: 12px 14px; border-radius: 12px;
    font-size: 13px; line-height: 1.6;
  }
  .bubble-user { background: var(--bg3); align-self: flex-start; border-radius: 4px 12px 12px 12px; }
  .bubble-mod  { background: rgba(59,130,246,.12); align-self: flex-end; border-radius: 12px 4px 12px 12px; }
  .bubble-meta { font-size: 11px; color: var(--text2); margin-bottom: 4px; }

  /* ── MISC ──────────────────────────────── */
  .empty-state { text-align: center; padding: 48px; color: var(--text2); font-size: 14px; }
  .divider { height: 1px; background: var(--border); margin: 16px 0; }
  code { font-family: 'JetBrains Mono', monospace; font-size: 12px; background: var(--bg3); padding: 2px 6px; border-radius: 4px; color: var(--accent); }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }

  /* ── LOGIN ─────────────────────────────── */
  .login-bg {
    min-height: 100vh; width: 100%;
    display: flex; align-items: center; justify-content: center;
    background: var(--bg);
    background-image:
      radial-gradient(ellipse at 20% 30%, rgba(59,130,246,.08) 0%, transparent 60%),
      radial-gradient(ellipse at 80% 70%, rgba(168,85,247,.06) 0%, transparent 60%);
  }
  .login-box {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 20px; padding: 40px; width: 400px;
    box-shadow: 0 20px 60px rgba(0,0,0,.3);
  }
  .login-logo {
    width: 56px; height: 56px; border-radius: 16px;
    background: linear-gradient(135deg, var(--accent), var(--purple));
    display: flex; align-items: center; justify-content: center;
    font-size: 24px; margin: 0 auto 20px;
    box-shadow: 0 0 30px var(--accent-glow);
  }
  .login-title { text-align: center; font-size: 22px; font-weight: 800; margin-bottom: 6px; }
  .login-sub { text-align: center; font-size: 13px; color: var(--text2); margin-bottom: 28px; }

  /* ── SCROLLBAR ─────────────────────────── */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 10px; }

  /* ── MOBILE ────────────────────────────── */
  @media (max-width: 900px) {
    .sidebar { transform: translateX(-100%); }
    .sidebar.open { transform: translateX(0); }
    .main { margin-left: 0; }
    .sidebar-toggle { display: flex; }
    .grid-2, .grid-3 { grid-template-columns: 1fr; }
    .cards { grid-template-columns: 1fr 1fr; }
    .search-global input { width: 160px; }
  }
  @media (max-width: 500px) {
    .cards { grid-template-columns: 1fr; }
  }

  /* ── QUICK SEARCH OVERLAY ──────────────── */
  #qs-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,.5);
    z-index: 500; display: none; padding-top: 80px;
    justify-content: center;
  }
  #qs-overlay.open { display: flex; }
  #qs-box {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 16px; width: 540px; max-width: 95vw;
    box-shadow: 0 20px 60px rgba(0,0,0,.5);
    overflow: hidden;
  }
  #qs-input {
    width: 100%; padding: 16px 20px;
    background: transparent; border: none; border-bottom: 1px solid var(--border);
    color: var(--text); font-size: 16px; font-family: 'Syne', sans-serif;
  }
  #qs-input:focus { outline: none; }
  #qs-results { max-height: 320px; overflow-y: auto; }
  #qs-results .qsr { padding: 12px 20px; cursor: pointer; font-size: 13px; display: flex; gap: 10px; align-items: center; }
  #qs-results .qsr:hover { background: var(--bg3); }
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
  document.querySelector('.sidebar').classList.toggle('open');
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
    <button class="sidebar-toggle" onclick="toggleSidebar()">☰</button>
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
        {link("admins", "/dashboard/admins", "👑", "Администраторы", "view_overview") if rank >= OWNER_RANK else ""}
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
    return "</div>"  # close .main


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

    recent_acts = []
    top_mods = []
    try:
        act_rows = conn.execute(
            "SELECT action, reason, by_name, created_at FROM mod_history ORDER BY created_at DESC LIMIT 8"
        ).fetchall()
        recent_acts = [dict(r) for r in act_rows]
        mod_rows = conn.execute(
            "SELECT by_name, COUNT(*) as cnt FROM mod_history GROUP BY by_name ORDER BY cnt DESC LIMIT 5"
        ).fetchall()
        top_mods = [dict(r) for r in mod_rows]
    except:
        pass
    conn.close()

    online_count = shared.get_online_count()
    alerts_count = len(shared.alerts)

    # Топ чатов
    chat_rows_html = ""
    for c in chats[:6]:
        cid = c["cid"]
        title = c.get("title") or str(cid)
        cc = db.get_conn()
        msgs = cc.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        users = cc.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        cc.close()
        chat_rows_html += f"""
        <tr>
          <td><a href="/dashboard/chats/{cid}" style="color:var(--text);font-weight:600;">{title[:25]}</a></td>
          <td>{users:,}</td>
          <td>{msgs:,}</td>
          <td><a href="/dashboard/chats/{cid}" class="btn btn-xs btn-ghost">→</a></td>
        </tr>"""

    act_rows_html = ""
    for r in recent_acts:
        dt = str(r.get("created_at", ""))[:16].replace("T", " ")
        act = r.get("action", "—")
        by = r.get("by_name", "—")
        color = "var(--danger)" if "Бан" in act else ("var(--warn)" if "Мут" in act or "Варн" in act else "var(--text2)")
        act_rows_html += f"""
        <tr>
          <td style="color:var(--text2);font-size:11px;font-family:'JetBrains Mono',monospace;">{dt}</td>
          <td style="color:{color};font-weight:600;">{act}</td>
          <td style="color:var(--text2);">{r.get('reason','—')[:30]}</td>
          <td>{by}</td>
        </tr>"""

    mod_rows_html = ""
    for r in top_mods:
        mod_rows_html += f"<tr><td>👮 {r['by_name']}</td><td style='font-family:JetBrains Mono,monospace;color:var(--accent);'>{r['cnt']}</td></tr>"

    body = navbar(sess, "overview") + f"""
    <div class="container">
      <div class="page-title">📊 Обзор
        <span style="font-size:12px;color:var(--text2);font-weight:400;margin-left:auto;">{datetime.now().strftime('%d.%m.%Y %H:%M')}</span>
      </div>

      <div class="cards">
        <div class="card">
          <div class="card-icon">💬</div>
          <div class="card-label">Чатов</div>
          <div class="card-value" data-live="chats">{len(chats)}</div>
          <div class="card-sub">активных чатов</div>
        </div>
        <div class="card">
          <div class="card-icon">🟢</div>
          <div class="card-label">Онлайн</div>
          <div class="card-value" data-live="online">{online_count}</div>
          <div class="card-sub">активны за 5 мин</div>
        </div>
        <div class="card">
          <div class="card-icon">👥</div>
          <div class="card-label">Участников</div>
          <div class="card-value" data-live="messages">{total_users:,}</div>
          <div class="card-sub">уникальных юзеров</div>
        </div>
        <div class="card">
          <div class="card-icon">💬</div>
          <div class="card-label">Сообщений</div>
          <div class="card-value">{total_msgs:,}</div>
          <div class="card-sub">всего обработано</div>
        </div>
        <div class="card">
          <div class="card-icon">🔨</div>
          <div class="card-label">Банов</div>
          <div class="card-value" style="color:var(--danger);">{total_bans}</div>
          <div class="card-sub">активных банов</div>
        </div>
        <div class="card">
          <div class="card-icon">⚡</div>
          <div class="card-label">Варнов</div>
          <div class="card-value" style="color:var(--warn);">{total_warns}</div>
          <div class="card-sub">активных варнов</div>
        </div>
        <div class="card">
          <div class="card-icon">🎫</div>
          <div class="card-label">Тикетов</div>
          <div class="card-value" style="color:var(--accent);" data-live="tickets_open">{ticket_stats['open']}</div>
          <div class="card-sub">открытых / {ticket_stats['total']} всего</div>
        </div>
        <div class="card">
          <div class="card-icon">🔴</div>
          <div class="card-label">Алертов</div>
          <div class="card-value" style="color:var(--danger);" data-live="alerts">{alerts_count}</div>
          <div class="card-sub">требуют внимания</div>
        </div>
      </div>

      <!-- Активные сессии -->
      <div class="section" style="margin-bottom:20px;">
        <div class="section-header">
          👁 Активные сессии дашборда
          <span style="font-size:12px;color:var(--text2);">{len(_get_active_sessions())} онлайн</span>
        </div>
        <div style="padding:4px 0;">
          {"".join(
            f'<div style="padding:10px 20px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;">'
            f'<div><code>{s["ip"]}</code><span style="margin-left:12px;color:var(--text2);font-size:12px;">{s.get("current","")}</span></div>'
            f'<div style="font-size:12px;color:var(--text2);">{int((time.time()-s["last_seen"])//60)} мин назад · {s.get("pages",0)} стр.</div>'
            f'</div>'
            for s in _get_active_sessions()
          ) or '<div class="empty-state" style="padding:20px;">Нет активных сессий</div>'}
        </div>
      </div>

      <div class="grid-2">
        <div class="section">
          <div class="section-header">⚡ Последние действия</div>
          <table>
            <thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Кто</th></tr></thead>
            <tbody>{act_rows_html or "<tr><td colspan='4' class='empty-state'>Нет данных</td></tr>"}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">👮 Топ модераторов</div>
          <table>
            <thead><tr><th>Модератор</th><th>Действий</th></tr></thead>
            <tbody>{mod_rows_html or "<tr><td colspan='2' class='empty-state'>Нет данных</td></tr>"}</tbody>
          </table>
        </div>
      </div>

      <div class="section" style="margin-top:20px;">
        <div class="section-header">
          📈 Активность по часам
          <span style="font-size:12px;color:var(--text2);">за сегодня</span>
        </div>
        <div style="padding:16px;"><canvas id="actChart" height="70"></canvas></div>
      </div>

      <div class="section" style="margin-top:20px;">
        <div class="section-header">💬 Топ чатов по активности</div>
        <table>
          <thead><tr><th>Чат</th><th>Участников</th><th>Сообщений</th><th></th></tr></thead>
          <tbody>{chat_rows_html or "<tr><td colspan='4' class='empty-state'>Нет данных</td></tr>"}</tbody>
        </table>
      </div>
    </div>

    <script>
    (function(){{
      fetch('/api/hourly').then(function(r){{return r.json();}}).then(function(d){{
        var ctx = document.getElementById('actChart');
        if(!ctx || typeof Chart==='undefined') return;
        var labels = Array.from({{length:24}},function(_,i){{return i+':00';}});
        var data = labels.map(function(_,i){{return d[i]||0;}});
        new Chart(ctx, {{
          type: 'bar',
          data: {{
            labels: labels,
            datasets: [{{
              label: 'Сообщений',
              data: data,
              backgroundColor: function(ctx){{
                var g = ctx.chart.ctx.createLinearGradient(0,0,0,100);
                g.addColorStop(0,'rgba(59,130,246,0.8)');
                g.addColorStop(1,'rgba(168,85,247,0.4)');
                return g;
              }},
              borderRadius: 4, borderSkipped: false,
            }}]
          }},
          options: {{
            responsive: true,
            plugins: {{legend:{{display:false}},tooltip:{{
              backgroundColor:'rgba(14,18,32,0.95)',
              borderColor:'rgba(30,42,64,.8)',borderWidth:1,
              titleColor:'#e2e8f0',bodyColor:'#94a3b8',
            }}}},
            scales: {{
              y: {{ticks:{{color:'#475569',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,0.04)'}}}},
              x: {{ticks:{{color:'#475569',font:{{size:10}},maxTicksLimit:8}},grid:{{display:false}}}}
            }}
          }}
        }});
      }}).catch(function(){{}});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_overview = require_auth("view_overview")(handle_overview)


# ══════════════════════════════════════════
#  СИСТЕМА АДМИНИСТРАЦИИ (только владелец)
# ══════════════════════════════════════════
#  СИСТЕМА АДМИНИСТРАЦИИ — 15 РАНГОВ (только владелец)
# ══════════════════════════════════════════

async def handle_admins(request: web.Request):
    sess = _get_session(request)
    if not sess or sess.get("rank") < OWNER_RANK:
        raise web.HTTPFound("/dashboard")
    _track_session(request)

    result_msg = ""
    ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()

    if request.method == "POST":
        data   = await request.post()
        action = data.get("action", "")
        try:
            tg_uid = int(data.get("tg_uid", 0))
            rank   = int(data.get("rank", 1))
        except:
            tg_uid = 0; rank = 1
        name = data.get("name", "").strip()

        if action == "grant" and tg_uid and name and 1 <= rank <= OWNER_RANK - 1:
            _grant_admin(tg_uid, name, rank, OWNER_TG_ID)
            _log_admin_db(OWNER_TG_ID, "GRANT_ADMIN",
                          f"Ранг {rank} ({DASHBOARD_RANKS[rank]['name']}) → {name} ({tg_uid})", ip)
            result_msg = f"✅ {name} получил ранг {DASHBOARD_RANKS[rank]['name']}"
            if _bot:
                try:
                    await _bot.send_message(tg_uid,
                        f"━━━━━━━━━━━━━━━\n👑 <b>ДОСТУП К ДАШБОРДУ</b>\n━━━━━━━━━━━━━━━\n\n"
                        f"🎖 Ранг: <b>{DASHBOARD_RANKS[rank]['name']}</b>\n"
                        f"📋 {DASHBOARD_RANKS[rank]['desc']}\n\n"
                        f"🔗 Войдите через ваш Telegram ID.", parse_mode="HTML")
                except: pass

        elif action == "revoke" and tg_uid and tg_uid != OWNER_TG_ID:
            admin = _get_admin(tg_uid)
            if admin:
                _revoke_admin(tg_uid)
                end_duty(tg_uid)
                _log_admin_db(OWNER_TG_ID, "REVOKE_ADMIN", f"Удалён {admin['name']} ({tg_uid})", ip)
                result_msg = f"✅ {admin['name']} удалён из администрации"
                for k, v in list(_dashboard_sessions.items()):
                    if v.get("uid") == tg_uid:
                        del _dashboard_sessions[k]
                if _bot:
                    try:
                        await _bot.send_message(tg_uid,
                            "⚠️ <b>Ваш доступ к дашборду отозван.</b>", parse_mode="HTML")
                    except: pass

        elif action == "update_rank" and tg_uid and tg_uid != OWNER_TG_ID:
            admin = _get_admin(tg_uid)
            if admin and 1 <= rank <= OWNER_RANK - 1:
                old_rank = admin["rank"]
                _grant_admin(tg_uid, admin["name"], rank, OWNER_TG_ID)
                _log_admin_db(OWNER_TG_ID, "UPDATE_RANK",
                              f"{admin['name']}: {old_rank}→{rank}", ip)
                result_msg = (
                    f"✅ {admin['name']}: "
                    f"{DASHBOARD_RANKS.get(old_rank,{}).get('name','?')} → "
                    f"{DASHBOARD_RANKS[rank]['name']}"
                )
                for k, v in _dashboard_sessions.items():
                    if v.get("uid") == tg_uid:
                        v["rank"] = rank
                if _bot:
                    arrow = "📈 повышен" if rank > old_rank else "📉 понижен"
                    try:
                        await _bot.send_message(tg_uid,
                            f"🔄 Ваш ранг {arrow}!\n\n"
                            f"🎖 Новый ранг: <b>{DASHBOARD_RANKS[rank]['name']}</b>\n"
                            f"📋 {DASHBOARD_RANKS[rank]['desc']}", parse_mode="HTML")
                    except: pass

        elif action == "duty_start" and tg_uid:
            try: hours = float(data.get("duty_hours", "8"))
            except: hours = 8.0
            admin = _get_admin(tg_uid)
            if admin:
                rn = DASHBOARD_RANKS.get(admin["rank"], {}).get("name", "?")
                start_duty(tg_uid, admin["name"], admin["rank"], hours, rn)
                _log_admin_db(OWNER_TG_ID, "DUTY_START",
                              f"{admin['name']} дежурство {hours}ч", ip)
                result_msg = f"✅ {admin['name']} поставлен на дежурство на {hours}ч"
                if _bot:
                    end_dt = datetime.fromtimestamp(time.time() + hours * 3600).strftime("%H:%M %d.%m")
                    try:
                        await _bot.send_message(tg_uid,
                            f"⏰ <b>Начало дежурства</b>\n\n"
                            f"Вы поставлены на дежурство на <b>{hours}ч</b>.\n"
                            f"Конец: {end_dt}\nУдачной смены! 🛡", parse_mode="HTML")
                    except: pass

        elif action == "duty_end" and tg_uid:
            admin = _get_admin(tg_uid)
            if admin:
                d = get_duty_status(tg_uid)
                end_duty(tg_uid)
                _log_admin_db(OWNER_TG_ID, "DUTY_END", f"{admin['name']} снят с дежурства", ip)
                result_msg = f"✅ {admin['name']} снят с дежурства"
                if _bot:
                    try:
                        await _bot.send_message(tg_uid,
                            "⏹ <b>Дежурство завершено.</b>\nСпасибо за службу! 👮",
                            parse_mode="HTML")
                    except: pass

    # ── Данные ──────────────────────────────────────────────────
    admins    = _get_all_admins()
    on_duty   = get_all_on_duty()
    duty_uids = {d["uid"] for d in on_duty}

    try:
        conn = db.get_conn()
        log_rows = [dict(r) for r in conn.execute(
            "SELECT tg_uid,action,details,ip,ts FROM dashboard_admin_log ORDER BY ts DESC LIMIT 30"
        ).fetchall()]
        conn.close()
    except:
        log_rows = []

    log_html = "".join(
        f"<tr>"
        f"<td style='font-size:11px;color:var(--text2);font-family:JetBrains Mono,monospace;'>{str(r['ts'])[:16]}</td>"
        f"<td><code>{r['tg_uid']}</code></td>"
        f"<td style='font-weight:600;'>{r['action']}</td>"
        f"<td style='color:var(--text2);font-size:12px;'>{str(r['details'])[:55]}</td>"
        f"<td style='color:var(--text2);font-size:11px;'>{r['ip']}</td>"
        f"</tr>"
        for r in log_rows
    ) or "<tr><td colspan='5' class='empty-state'>Лог пуст</td></tr>"

    # ── Карточки дежурства ───────────────────────────────────────
    duty_cards = ""
    for d in on_duty:
        ri  = DASHBOARD_RANKS.get(d["rank"], DASHBOARD_RANKS[1])
        eh  = d["elapsed_mins"] // 60; em = d["elapsed_mins"] % 60
        rh  = d["remaining_mins"] // 60; rm = d["remaining_mins"] % 60
        rgb = _hex_to_rgb(ri["color"])
        duty_cards += (
            f'<div style="background:var(--bg2);border:1px solid var(--border);border-radius:12px;'
            f'padding:16px;border-left:3px solid {ri["color"]};">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">'
            f'<div><div style="font-weight:700;">{d["name"]}</div>'
            f'<div style="font-size:12px;color:{ri["color"]};margin-top:2px;">{ri["name"]}</div></div>'
            f'<span style="font-size:11px;color:var(--success);font-weight:700;">🟢 ДЕЖУРИТ</span></div>'
            f'<div style="font-size:12px;color:var(--text2);margin-bottom:8px;">'
            f'⏱ {eh}ч {em}м &nbsp;|&nbsp; ⏳ осталось {rh}ч {rm}м</div>'
            f'<div style="height:4px;background:var(--bg4);border-radius:2px;margin-bottom:10px;">'
            f'<div style="height:4px;width:{d["pct"]}%;background:{ri["color"]};border-radius:2px;"></div></div>'
            f'<div style="font-size:11px;color:var(--text2);margin-bottom:10px;">'
            f'Действий за смену: <b style="color:var(--accent);">{d.get("actions",0)}</b></div>'
            f'<div style="display:flex;gap:6px;">'
            f'<form method="POST" style="display:inline;">'
            f'<input type="hidden" name="action" value="duty_end">'
            f'<input type="hidden" name="tg_uid" value="{d["uid"]}">'
            f'<button class="btn btn-xs btn-danger" type="submit">⏹ Снять</button></form>'
            f'<a href="/dashboard/mod/{d["uid"]}" class="btn btn-xs btn-ghost">👤 Профиль</a>'
            f'</div></div>'
        )
    if not duty_cards:
        duty_cards = '<div class="empty-state" style="padding:24px;">Никто не на дежурстве</div>'

    # ── Таблица по тирам ─────────────────────────────────────────
    tier_order = ["owner","top","high","senior","mid","junior"]
    by_tier: dict = {t: [] for t in tier_order}
    for a in admins:
        t = DASHBOARD_RANKS.get(a["rank"], DASHBOARD_RANKS[1]).get("tier","junior")
        by_tier.setdefault(t, []).append(a)

    admins_sections = ""
    for tier_key in tier_order:
        group = by_tier.get(tier_key, [])
        if not group:
            continue
        ti = RANK_TIERS.get(tier_key, {"label": tier_key, "color": "#607d8b"})
        rows_in_tier = ""
        for a in group:
            rank     = a["rank"]
            ri       = DASHBOARD_RANKS.get(rank, DASHBOARD_RANKS[1])
            is_owner = a["tg_uid"] == OWNER_TG_ID
            last     = str(a.get("last_login") or "—")[:16]
            stats    = _get_mod_stats(a["name"], a["tg_uid"])
            safe     = str(a["name"]).replace("'","").replace('"',"")
            uid      = a["tg_uid"]
            rgb      = _hex_to_rgb(ri["color"])
            duty_dot = (
                '<span style="width:8px;height:8px;border-radius:50%;background:var(--success);'
                'display:inline-block;margin-left:6px;" title="На дежурстве"></span>'
                if uid in duty_uids else ""
            )
            if is_owner:
                action_cell = '<span style="color:var(--gold);font-size:12px;">🌟 Владелец</span>'
            else:
                action_cell = (
                    f'<div style="display:flex;gap:4px;flex-wrap:wrap;">'
                    f'<button onclick="openRankModal({uid},\'{safe}\',{rank})" class="btn btn-xs btn-ghost">✏️ Ранг</button>'
                    f'<a href="/dashboard/mod/{uid}" class="btn btn-xs btn-ghost">👤</a>'
                    f'<button onclick="openDutyModal({uid},\'{safe}\')" class="btn btn-xs btn-ghost">⏰</button>'
                    f'<button onclick="revokeAdmin({uid},\'{safe}\')" class="btn btn-xs"'
                    f' style="background:rgba(239,68,68,.1);color:var(--danger);">🗑</button>'
                    f'</div>'
                )
            rows_in_tier += (
                f"<tr>"
                f"<td><code style='font-size:11px;'>{uid}</code></td>"
                f"<td style='font-weight:700;'>{a['name']}{duty_dot}</td>"
                f"<td><span class='badge' style='background:rgba({rgb},.15);color:{ri['color']};'>"
                f"{rank} · {ri['name']}</span></td>"
                f"<td style='font-size:12px;color:var(--text2);'>{ri['desc']}</td>"
                f"<td style='font-size:12px;font-family:JetBrains Mono,monospace;white-space:nowrap;'>"
                f"<span style='color:var(--danger);'>{stats['bans']}б</span> "
                f"<span style='color:var(--warn);'>{stats['warns']}в</span> "
                f"<span style='color:var(--purple);'>{stats['mutes']}м</span> "
                f"<span style='color:var(--accent);font-size:11px;'>🎫{stats['tickets']}</span></td>"
                f"<td style='font-size:11px;color:var(--text2);'>{last}</td>"
                f"<td>{action_cell}</td>"
                f"</tr>"
            )
        admins_sections += (
            f'<div style="margin-bottom:20px;">'
            f'<div style="font-size:11px;color:{ti["color"]};font-weight:700;letter-spacing:1.5px;'
            f'text-transform:uppercase;padding:8px 0 4px;'
            f'border-bottom:2px solid {ti["color"]}44;margin-bottom:8px;">{ti["label"]}</div>'
            f'<table style="width:100%;border-collapse:collapse;">'
            f'<thead><tr>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">TG ID</th>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">Имя</th>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">Ранг</th>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">Описание</th>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">Действия</th>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">Посл. вход</th>'
            f'<th style="font-size:10px;text-transform:uppercase;color:var(--text2);background:var(--bg3);padding:8px 12px;text-align:left;">Управление</th>'
            f'</tr></thead>'
            f'<tbody>{rows_in_tier}</tbody>'
            f'</table></div>'
        )

    # ── Карточки рангов ──────────────────────────────────────────
    rank_cards = ""
    cur_tier   = None
    for r_id in range(1, OWNER_RANK + 1):
        ri = DASHBOARD_RANKS[r_id]
        if ri["tier"] != cur_tier:
            cur_tier = ri["tier"]
            ti = RANK_TIERS.get(cur_tier, {"label": cur_tier, "color": "#607d8b"})
            rank_cards += (
                f'<div style="font-size:10px;color:{ti["color"]};font-weight:700;letter-spacing:1.5px;'
                f'text-transform:uppercase;margin:14px 0 6px;">── {ti["label"]} ──</div>'
            )
        perms_n  = len(ri["perms"])
        preview  = ", ".join(ri["perms"][:3]) + (f" +{perms_n-3}" if perms_n > 3 else "")
        rank_cards += (
            f'<div class="rank-card" style="margin-bottom:6px;">'
            f'<div class="rank-badge" style="background:rgba({_hex_to_rgb(ri["color"])},.15);color:{ri["color"]};">{r_id}</div>'
            f'<div style="flex:1;">'
            f'<div class="rank-name" style="color:{ri["color"]};font-size:13px;">{ri["name"]}</div>'
            f'<div class="rank-desc" style="font-size:11px;">{ri["desc"]}</div>'
            f'<div style="font-size:10px;color:var(--text2);margin-top:2px;">{preview}</div>'
            f'</div></div>'
        )

    result_html = (
        f'<div style="padding:12px 16px;background:rgba(34,197,94,.1);border-radius:8px;'
        f'margin-bottom:20px;color:var(--success);font-weight:600;">{result_msg}</div>'
        if result_msg else ""
    )

    rank_opts = "".join(
        f'<option value="{r_id}">{r_id} — {ri["name"]} [{ri["tier"].upper()}] — {ri["desc"]}</option>'
        for r_id, ri in sorted(DASHBOARD_RANKS.items()) if r_id < OWNER_RANK
    )
    rank_opts_short = "".join(
        f'<option value="{r_id}">{r_id} — {ri["name"]}</option>'
        for r_id, ri in sorted(DASHBOARD_RANKS.items()) if r_id < OWNER_RANK
    )
    ranks_js = json.dumps({
        str(r_id): {"name": ri["name"], "color": ri["color"],
                    "desc": ri["desc"], "tier": ri["tier"]}
        for r_id, ri in DASHBOARD_RANKS.items()
    })

    # Предвычисляем статы для тиров
    cnt_junior  = sum(1 for a in admins if DASHBOARD_RANKS.get(a["rank"], DASHBOARD_RANKS[1]).get("tier") == "junior")
    cnt_mid_sr  = sum(1 for a in admins if DASHBOARD_RANKS.get(a["rank"], DASHBOARD_RANKS[1]).get("tier") in ["mid", "senior"])
    cnt_top     = sum(1 for a in admins if DASHBOARD_RANKS.get(a["rank"], DASHBOARD_RANKS[1]).get("tier") in ["high", "top"])
    cnt_admins  = len(admins)
    cnt_duty    = len(on_duty)
    cnt_sess    = len(_dashboard_sessions)

    body = navbar(sess, "admins") + f"""
    <div class="container">
      <div class="page-title">👑 Администраторы
        <span style="font-size:12px;color:var(--gold);font-weight:600;margin-left:auto;">
          {OWNER_RANK} рангов · {cnt_admins} адм · {cnt_duty} дежурят
        </span>
      </div>
      {result_html}

      <div class="cards" style="grid-template-columns:repeat(6,1fr);margin-bottom:24px;">
        <div class="card"><div class="card-icon">👥</div><div class="card-label">Всего адм</div>
          <div class="card-value">{cnt_admins}</div></div>
        <div class="card"><div class="card-icon">🟢</div><div class="card-label">Дежурят</div>
          <div class="card-value" style="color:var(--success);">{cnt_duty}</div></div>
        <div class="card"><div class="card-icon">🔵</div><div class="card-label">Младший</div>
          <div class="card-value" style="color:#607d8b;">{cnt_junior}</div></div>
        <div class="card"><div class="card-icon">🟡</div><div class="card-label">Средний+</div>
          <div class="card-value" style="color:#ffa726;">{cnt_mid_sr}</div></div>
        <div class="card"><div class="card-icon">🔴</div><div class="card-label">Топ состав</div>
          <div class="card-value" style="color:#ff7043;">{cnt_top}</div></div>
        <div class="card"><div class="card-icon">🔐</div><div class="card-label">Сессий</div>
          <div class="card-value">{cnt_sess}</div></div>
      </div>

      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">⏰ Дежурство прямо сейчас
          <span style="font-size:12px;color:var(--text2);">{len(on_duty)} человек</span>
        </div>
        <div style="padding:16px;display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px;">
          {duty_cards}
        </div>
      </div>

      <div class="grid-2" style="margin-bottom:24px;">
        <div class="section">
          <div class="section-header">➕ Добавить / обновить</div>
          <div class="section-body">
            <form method="POST">
              <input type="hidden" name="action" value="grant">
              <div class="form-group">
                <label>Telegram ID</label>
                <input class="form-control" type="number" name="tg_uid" placeholder="123456789" required>
              </div>
              <div class="form-group">
                <label>Имя</label>
                <input class="form-control" type="text" name="name" placeholder="Иван Иванов" required>
              </div>
              <div class="form-group">
                <label>Ранг (1–{OWNER_RANK-1})</label>
                <select class="form-control" name="rank" id="grantRankSel" onchange="previewGrantRank(this.value)">
                  {rank_opts}
                </select>
                <div id="grantPreview" style="margin-top:6px;font-size:12px;padding:6px 10px;background:var(--bg3);border-radius:6px;"></div>
              </div>
              <button class="btn btn-primary" type="submit" style="width:100%;">✅ Добавить / Обновить</button>
            </form>
          </div>
        </div>
        <div class="section">
          <div class="section-header">📋 Все {OWNER_RANK} рангов</div>
          <div style="padding:12px 16px;max-height:440px;overflow-y:auto;">{rank_cards}</div>
        </div>
      </div>

      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">
          👥 Все администраторы ({len(admins)})
          <span style="font-size:12px;color:var(--text2);">🟢 = дежурит · сгруппированы по уровню</span>
        </div>
        <div style="padding:16px;">{admins_sections or "<div class='empty-state'>Нет администраторов</div>"}</div>
      </div>

      <div class="section">
        <div class="section-header">📜 Лог действий (последние 30)</div>
        <table>
          <thead><tr><th>Время</th><th>TG ID</th><th>Действие</th><th>Детали</th><th>IP</th></tr></thead>
          <tbody>{log_html}</tbody>
        </table>
      </div>
    </div>

    <div class="modal-overlay" id="rankModal">
      <div class="modal">
        <div class="modal-title">✏️ Изменить ранг</div>
        <form method="POST">
          <input type="hidden" name="action" value="update_rank">
          <input type="hidden" name="tg_uid" id="rankUid">
          <div class="form-group">
            <label>Администратор</label>
            <input class="form-control" id="rankName" readonly>
          </div>
          <div class="form-group">
            <label>Текущий ранг</label>
            <div id="rankCurrentBadge" style="margin-top:4px;font-size:13px;font-weight:700;"></div>
          </div>
          <div class="form-group">
            <label>Новый ранг</label>
            <select class="form-control" name="rank" id="rankSelect" onchange="updateRankPreview(this.value)">
              {rank_opts_short}
            </select>
          </div>
          <div id="rankPreview" style="padding:10px;background:var(--bg3);border-radius:8px;font-size:12px;margin-bottom:4px;"></div>
          <div class="modal-footer">
            <button type="button" class="btn btn-ghost" onclick="closeModal('rankModal')">Отмена</button>
            <button type="submit" class="btn btn-primary">✅ Сохранить</button>
          </div>
        </form>
      </div>
    </div>

    <div class="modal-overlay" id="dutyModal">
      <div class="modal">
        <div class="modal-title">⏰ Назначить дежурство</div>
        <form method="POST">
          <input type="hidden" name="action" value="duty_start">
          <input type="hidden" name="tg_uid" id="dutyUid">
          <div class="form-group">
            <label>Администратор</label>
            <input class="form-control" id="dutyName" readonly>
          </div>
          <div class="form-group">
            <label>Длительность смены</label>
            <select class="form-control" name="duty_hours">
              <option value="2">2 часа</option>
              <option value="4">4 часа</option>
              <option value="8" selected>8 часов (стандарт)</option>
              <option value="12">12 часов</option>
              <option value="24">24 часа</option>
            </select>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-ghost" onclick="closeModal('dutyModal')">Отмена</button>
            <button type="submit" class="btn btn-success">⏰ Назначить</button>
          </div>
        </form>
      </div>
    </div>

    <form method="POST" id="revokeForm">
      <input type="hidden" name="action" value="revoke">
      <input type="hidden" name="tg_uid" id="revokeUid">
    </form>

    <script>
    var RANKS = {ranks_js};
    function previewGrantRank(v) {{
      var r = RANKS[v]; if(!r) return;
      document.getElementById('grantPreview').innerHTML =
        '<span style="color:'+r.color+';font-weight:700;">'+v+' · '+r.name+'</span> — '+r.desc;
    }}
    if(document.getElementById('grantRankSel')) previewGrantRank(document.getElementById('grantRankSel').value);
    function openRankModal(uid, name, currentRank) {{
      document.getElementById('rankUid').value = uid;
      document.getElementById('rankName').value = name+' ('+uid+')';
      document.getElementById('rankSelect').value = currentRank;
      var r = RANKS[currentRank]||{{}};
      document.getElementById('rankCurrentBadge').innerHTML =
        '<span style="color:'+(r.color||'#fff')+';">'+currentRank+' · '+(r.name||'?')+'</span>';
      updateRankPreview(currentRank);
      openModal('rankModal');
    }}
    function updateRankPreview(v) {{
      var r = RANKS[v]||{{}};
      document.getElementById('rankPreview').innerHTML =
        '<b style="color:'+(r.color||'#fff')+'">'+(r.name||'?')+'</b> — '+(r.desc||'')+
        ' <span style="font-size:10px;color:var(--text2);">'+(r.tier||'').toUpperCase()+'</span>';
    }}
    function openDutyModal(uid, name) {{
      document.getElementById('dutyUid').value = uid;
      document.getElementById('dutyName').value = name+' ('+uid+')';
      openModal('dutyModal');
    }}
    function revokeAdmin(uid, name) {{
      if(confirm('Удалить '+name+' ('+uid+')? Нельзя отменить.')) {{
        document.getElementById('revokeUid').value = uid;
        document.getElementById('revokeForm').submit();
      }}
    }}
    setInterval(function(){{
      fetch('/api/live').then(function(r){{return r.json();}}).then(function(d){{
        if(d.online!==undefined){{var el=document.querySelector('[data-live="online"]');if(el)el.textContent=d.online;}}
      }}).catch(function(){{}});
    }},15000);
    </script>
    """ + close_main()
    try:
        rendered = page(body)
    except Exception as _e:
        import traceback
        tb = traceback.format_exc()
        log.error(f"handle_admins render ERROR: {_e}\n{tb}")
        return web.Response(text=f"<pre style='color:red'>RENDER ERROR:\n{tb}</pre>",
                            content_type="text/html")
    return web.Response(text=rendered, content_type="text/html")

#  ЧАТЫ
# ══════════════════════════════════════════

async def handle_chats(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    chats = [dict(r) for r in await db.get_all_chats()]

    search = request.rel_url.query.get("q", "").lower()
    if search:
        chats = [c for c in chats if search in (c.get("title") or "").lower() or search in str(c["cid"])]

    rows_html = ""
    for chat in chats:
        cid = chat["cid"]
        title = chat.get("title") or str(cid)
        cc = db.get_conn()
        msgs = cc.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        users = cc.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        warns = cc.execute("SELECT COALESCE(SUM(count),0) FROM warnings WHERE cid=?", (cid,)).fetchone()[0] or 0
        bans = cc.execute("SELECT COUNT(*) FROM ban_list WHERE cid=?", (cid,)).fetchone()[0] or 0
        cc.close()
        rows_html += f"""
        <tr>
          <td><code style="font-size:11px;">{cid}</code></td>
          <td><a href="/dashboard/chats/{cid}" style="color:var(--text);font-weight:700;">{title}</a></td>
          <td>{users:,}</td>
          <td>{msgs:,}</td>
          <td><span style="color:var(--warn);">{warns}</span></td>
          <td><span style="color:var(--danger);">{bans}</span></td>
          <td>
            <a class="btn btn-xs btn-primary" href="/dashboard/chats/{cid}">📊 Детали</a>
            <a class="btn btn-xs btn-ghost" href="/dashboard/chat_settings/{cid}">⚙️</a>
          </td>
        </tr>"""

    body = navbar(sess, "chats") + f"""
    <div class="container">
      <div class="page-title">💬 Чаты ({len(chats)})</div>
      <div style="margin-bottom:16px;">
        <form method="GET" style="display:flex;gap:10px;">
          <input class="form-control" name="q" value="{search}" placeholder="Поиск чата..." style="max-width:300px;">
          <button class="btn btn-primary btn-sm" type="submit">🔍</button>
        </form>
      </div>
      <div class="section">
        <table>
          <thead><tr><th>ID</th><th>Название</th><th>Участников</th><th>Сообщений</th><th>Варнов</th><th>Банов</th><th>Действия</th></tr></thead>
          <tbody>{rows_html or "<tr><td colspan='7' class='empty-state'>Нет чатов</td></tr>"}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_chats = require_auth("view_chats")(handle_chats)


async def handle_chat_detail(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    cid = int(request.match_info["cid"])

    conn = db.get_conn()
    try:
        chat_row = conn.execute("SELECT title FROM known_chats WHERE cid=?", (cid,)).fetchone()
        title = chat_row["title"] if chat_row else str(cid)
    except:
        title = str(cid)

    try:
        top_users = [dict(_r) for _r in conn.execute("SELECT uid, msg_count FROM chat_stats WHERE cid=? ORDER BY msg_count DESC LIMIT 10", (cid,)).fetchall()]
    except:
        top_users = []
    try:
        top_rep = [dict(_r) for _r in conn.execute("SELECT uid, score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT 10", (cid,)).fetchall()]
    except:
        top_rep = []
    try:
        bans = [dict(_r) for _r in conn.execute("SELECT uid FROM ban_list WHERE cid=?", (cid,)).fetchall()]
    except:
        bans = []
    try:
        mod_hist = [dict(_r) for _r in conn.execute("SELECT action, reason, by_name, created_at FROM mod_history WHERE cid=? ORDER BY created_at DESC LIMIT 20", (cid,)).fetchall()]
    except:
        mod_hist = []
    conn.close()

    try:
        hours = await db.get_hourly_totals(cid)
    except:
        hours = {}

    max_h = max(hours.values(), default=1)
    heatmap = ""
    for h in range(24):
        val = hours.get(h, 0)
        pct = int((val / max_h) * 100) if max_h else 0
        opacity = max(0.1, pct / 100)
        heatmap += f'<div style="display:inline-block;width:3.9%;vertical-align:bottom;height:{max(6,pct)}px;background:linear-gradient(0deg,#3b82f6,#a855f7);opacity:{opacity:.2f};margin:1px;border-radius:3px 3px 0 0;" title="{h}:00 — {val} сообщ."></div>'

    top_u = "".join(f"<tr><td>{i}</td><td><a href='/dashboard/users/{r['uid']}' style='color:var(--accent);'><code>{r['uid']}</code></a></td><td>{r['msg_count']:,}</td></tr>" for i, r in enumerate([dict(x) for x in top_users], 1)) or "<tr><td colspan='3' class='empty-state'>Нет</td></tr>"
    top_r = "".join(f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td style='color:var(--success);'>{r['score']:+d}</td></tr>" for i, r in enumerate([dict(x) for x in top_rep], 1)) or "<tr><td colspan='3' class='empty-state'>Нет</td></tr>"
    hist_html = "".join(f"<tr><td style='font-size:11px;color:var(--text2);'>{str(r['created_at'])[:16]}</td><td style='font-weight:600;'>{r['action']}</td><td style='color:var(--text2);'>{r['reason'] or '—'}</td><td>{r['by_name']}</td></tr>" for r in [dict(x) for x in mod_hist]) or "<tr><td colspan='4' class='empty-state'>Нет</td></tr>"

    body = navbar(sess, "chats") + f"""
    <div class="container">
      <div class="page-title">
        💬 {title}
        <a href="/dashboard/chats" class="btn btn-ghost btn-sm" style="margin-left:auto;">← Назад</a>
        <a href="/dashboard/chat_settings/{cid}" class="btn btn-primary btn-sm">⚙️ Настройки</a>
      </div>

      <div class="section" style="margin-bottom:20px;">
        <div class="section-header">📊 Активность по часам</div>
        <div style="padding:16px;overflow-x:auto;">
          <div style="min-width:300px;display:flex;align-items:flex-end;height:80px;gap:1px;">{heatmap}</div>
          <div style="font-size:10px;color:var(--text2);margin-top:6px;display:flex;justify-content:space-between;">
            <span>0:00</span><span>6:00</span><span>12:00</span><span>18:00</span><span>23:00</span>
          </div>
        </div>
      </div>

      <div class="grid-2" style="margin-bottom:20px;">
        <div class="section">
          <div class="section-header">🏆 Топ активных</div>
          <table><thead><tr><th>#</th><th>ID</th><th>Сообщений</th></tr></thead><tbody>{top_u}</tbody></table>
        </div>
        <div class="section">
          <div class="section-header">⭐ Топ репутации</div>
          <table><thead><tr><th>#</th><th>ID</th><th>Репутация</th></tr></thead><tbody>{top_r}</tbody></table>
        </div>
      </div>

      <div class="section">
        <div class="section-header">📋 История модерации <span style="font-size:12px;color:var(--text2);">последние 20</span></div>
        <table>
          <thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Модератор</th></tr></thead>
          <tbody>{hist_html}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_chat_detail = require_auth("view_chats")(handle_chat_detail)


# ══════════════════════════════════════════
#  ПОЛЬЗОВАТЕЛИ
# ══════════════════════════════════════════

async def handle_users(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    search = request.rel_url.query.get("q", "")

    conn = db.get_conn()
    try:
        rows = conn.execute(
            "SELECT cs.uid, SUM(cs.msg_count) as msgs, COALESCE(SUM(r.score),0) as rep "
            "FROM chat_stats cs LEFT JOIN reputation r ON cs.uid=r.uid "
            "GROUP BY cs.uid ORDER BY msgs DESC LIMIT 50"
        ).fetchall()
    except:
        rows = []
    conn.close()

    # Получаем имена из кеша bot
    user_rows = ""
    for r in [dict(x) for x in rows]:
        uid = r["uid"]
        name = shared.online_users.get(uid, {}).get("name") or f"ID {uid}"
        user_rows += f"""
        <tr>
          <td><code>{uid}</code></td>
          <td><a href="/dashboard/users/{uid}" style="color:var(--text);font-weight:600;">{name}</a></td>
          <td>{r['msgs']:,}</td>
          <td style="color:{'var(--success)' if r['rep']>=0 else 'var(--danger)'};">{r['rep']:+d}</td>
          <td>
            <a class="btn btn-xs btn-primary" href="/dashboard/users/{uid}">👤 Профиль</a>
          </td>
        </tr>"""

    body = navbar(sess, "users") + f"""
    <div class="container">
      <div class="page-title">👥 Пользователи</div>
      <div style="margin-bottom:16px;">
        <form method="GET" style="display:flex;gap:10px;">
          <input class="form-control" name="q" value="{search}" placeholder="Поиск по ID или имени..." style="max-width:320px;">
          <button class="btn btn-primary btn-sm" type="submit">🔍</button>
        </form>
      </div>
      <div class="section">
        <table>
          <thead><tr><th>TG ID</th><th>Имя</th><th>Сообщений</th><th>Репутация</th><th>Действия</th></tr></thead>
          <tbody>{user_rows or "<tr><td colspan='5' class='empty-state'>Нет данных</td></tr>"}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_users = require_auth("view_users")(handle_users)


async def handle_user_detail(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    uid = int(request.match_info["uid"])

    conn = db.get_conn()
    chats_rows = [dict(_r) for _r in conn.execute("SELECT k.cid, k.title, cs.msg_count FROM chat_stats cs LEFT JOIN known_chats k ON cs.cid=k.cid WHERE cs.uid=? ORDER BY cs.msg_count DESC", (uid,)).fetchall()]
    rep_rows = [dict(_r) for _r in conn.execute("SELECT cid, score FROM reputation WHERE uid=? ORDER BY score DESC", (uid,)).fetchall()]
    warn_rows = [dict(_r) for _r in conn.execute("SELECT cid, count FROM warnings WHERE uid=? AND count>0", (uid,)).fetchall()]
    try:
        hist_rows = [dict(_r) for _r in conn.execute("SELECT action, reason, by_name, created_at, cid FROM mod_history WHERE uid=? ORDER BY created_at DESC LIMIT 20", (uid,)).fetchall()]
    except:
        hist_rows = []
    try:
        ticket_rows = [dict(_r) for _r in conn.execute("SELECT id, subject, status, created_at FROM tickets WHERE uid=? ORDER BY created_at DESC LIMIT 5", (uid,)).fetchall()]
    except:
        ticket_rows = []
    try:
        notes_rows = [dict(_r) for _r in conn.execute("SELECT text, by_name, created_at FROM mod_notes WHERE uid=? ORDER BY created_at DESC LIMIT 10", (uid,)).fetchall()]
    except:
        notes_rows = []
    conn.close()

    total_msgs = sum(r["msg_count"] or 0 for r in chats_rows)
    total_rep  = sum(r["score"] or 0 for r in rep_rows)
    total_warns = sum(r["count"] or 0 for r in warn_rows)
    name = shared.online_users.get(uid, {}).get("name") or f"User {uid}"

    can_ban = _has_perm(request.cookies.get("dsess_token"), "ban_users")
    can_mute = _has_perm(request.cookies.get("dsess_token"), "mute_users")
    can_warn = _has_perm(request.cookies.get("dsess_token"), "warn_users")

    # Список чатов пользователя для кнопок
    chat_ids = [str(r["cid"]) for r in chats_rows if r["cid"]]
    first_cid = chat_ids[0] if chat_ids else "0"

    action_btns = ""
    _uid_s = str(uid)
    _cid_s = str(first_cid)
    if can_ban:
        action_btns += (
            "<button class=\"btn btn-sm btn-danger\" "
            "onclick=\"modAction(&apos;ban&apos;," + _uid_s + "," + _cid_s + ",&apos;Нарушение&apos;)\">🔨 Бан</button>"
        )
        action_btns += (
            "<button class=\"btn btn-sm btn-success\" "
            "onclick=\"modAction(&apos;unban&apos;," + _uid_s + "," + _cid_s + ",&apos;Разбан&apos;)\">🕊 Разбан</button>"
        )
    if can_mute:
        action_btns += (
            "<button class=\"btn btn-sm btn-warn\" "
            "onclick=\"modAction(&apos;mute&apos;," + _uid_s + "," + _cid_s + ",&apos;Мут 1ч&apos;)\">🔇 Мут</button>"
        )
    if can_warn:
        action_btns += (
            "<button class=\"btn btn-sm btn-ghost\" "
            "onclick=\"modAction(&apos;warn&apos;," + _uid_s + "," + _cid_s + ",&apos;Нарушение&apos;)\">⚡ Варн</button>"
        )

    hist_html = "".join(f"<tr><td style='font-size:11px;color:var(--text2);'>{str(r['created_at'])[:16]}</td><td style='font-weight:600;'>{r['action']}</td><td style='color:var(--text2);'>{r['reason'] or '—'}</td><td>{r['by_name']}</td></tr>" for r in [dict(x) for x in hist_rows]) or "<tr><td colspan='4' class='empty-state'>Чисто</td></tr>"

    notes_html = "".join(f'<div style="padding:10px 0;border-bottom:1px solid var(--border);"><div style="font-size:13px;">{dict(n)["text"]}</div><div style="font-size:11px;color:var(--text2);margin-top:3px;">👮 {dict(n)["by_name"]} · {str(dict(n)["created_at"])[:16]}</div></div>' for n in notes_rows) or "<div class='empty-state'>Заметок нет</div>"

    body = navbar(sess, "users") + f"""
    <div class="container">
      <div class="page-title">
        👤 {name} <code style="font-size:14px;">{uid}</code>
        <a href="/dashboard/users" class="btn btn-ghost btn-sm" style="margin-left:auto;">← Назад</a>
      </div>

      <div class="cards" style="grid-template-columns:repeat(4,1fr);">
        <div class="card"><div class="card-label">Сообщений</div><div class="card-value">{total_msgs:,}</div></div>
        <div class="card"><div class="card-label">Репутация</div><div class="card-value" style="color:{'var(--success)' if total_rep>=0 else 'var(--danger)'};">{total_rep:+d}</div></div>
        <div class="card"><div class="card-label">Варнов</div><div class="card-value" style="color:var(--warn);">{total_warns}</div></div>
        <div class="card"><div class="card-label">Тикетов</div><div class="card-value">{len(ticket_rows)}</div></div>
      </div>

      {"<div class='section' style='margin-bottom:20px;padding:16px;'><div style='display:flex;gap:8px;flex-wrap:wrap;align-items:center;'><span style='font-weight:700;margin-right:4px;'>⚡ Быстрые действия:</span>" + action_btns + "</div></div>" if action_btns else ""}

      <div class="grid-2" style="margin-bottom:20px;">
        <div class="section">
          <div class="section-header">💬 Активность по чатам</div>
          <table><thead><tr><th>Чат</th><th>Сообщений</th></tr></thead>
          <tbody>{"".join(f'<tr><td>{r["title"] or r["cid"]}</td><td>{r["msg_count"]:,}</td></tr>' for r in [dict(x) for x in chats_rows]) or "<tr><td colspan='2' class='empty-state'>Нет</td></tr>"}</tbody></table>
        </div>
        <div class="section">
          <div class="section-header">📝 Заметки модераторов</div>
          <div style="padding:0 16px;">{notes_html}</div>
        </div>
      </div>

      <div class="section">
        <div class="section-header">📋 История модерации</div>
        <table><thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Кто</th></tr></thead>
        <tbody>{hist_html}</tbody></table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_user_detail = require_auth("view_users")(handle_user_detail)


# ══════════════════════════════════════════
#  ТИКЕТЫ
# ══════════════════════════════════════════

async def handle_tickets(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    status_filter = request.rel_url.query.get("status", "open")
    tickets = [dict(r) for r in await db.ticket_list(status=status_filter, limit=50)]
    stats = await db.ticket_stats_all()

    tabs = ""
    for s, key, label, icon in [("open","open","Открытые","🟡"),("in_progress","in_progress","В работе","🔵"),("closed","closed","Закрытые","✅")]:
        active = "btn-primary" if status_filter == s else "btn-ghost"
        tabs += f'<a href="?status={s}" class="btn btn-sm {active}" style="margin-right:6px;">{icon} {label} ({stats[key]})</a>'

    rows = ""
    for t in [dict(x) for x in tickets]:
        st_cls = {"open":"badge-open","in_progress":"badge-progress","closed":"badge-closed"}.get(t["status"],"badge-accent")
        pr_cls = {"urgent":"badge-urgent","high":"badge-high","normal":"badge-normal","low":"badge-low"}.get(t.get("priority","normal"),"badge-normal")
        dt = t["created_at"].strftime("%d.%m %H:%M") if t.get("created_at") else "—"
        rows += f"""
        <tr>
          <td style="font-weight:700;color:var(--accent);">#{t['id']}</td>
          <td>
            <div style="font-weight:600;">{t.get('user_name','—')}</div>
            <div style="font-size:11px;color:var(--text2);">{t.get('chat_title','—')}</div>
          </td>
          <td>{t.get('subject','')[:45]}</td>
          <td><span class="badge {st_cls}">{t['status']}</span></td>
          <td><span class="badge {pr_cls}">{t.get('priority','—')}</span></td>
          <td style="color:var(--text2);">{t.get('assigned_mod','—')}</td>
          <td style="font-size:12px;color:var(--text2);">{dt}</td>
          <td><a class="btn btn-xs btn-primary" href="/dashboard/tickets/{t['id']}">Открыть</a></td>
        </tr>"""

    body = navbar(sess, "tickets") + f"""
    <div class="container">
      <div class="page-title">🎫 Тикеты
        <span style="font-size:13px;color:var(--text2);font-weight:400;margin-left:auto;">Всего: {stats['total']}</span>
      </div>
      <div style="margin-bottom:20px;">{tabs}</div>
      <div class="section">
        <table>
          <thead><tr><th>ID</th><th>Пользователь</th><th>Тема</th><th>Статус</th><th>Приоритет</th><th>Модератор</th><th>Создан</th><th></th></tr></thead>
          <tbody>{rows or "<tr><td colspan='8' class='empty-state'>Тикетов нет</td></tr>"}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_tickets = require_auth("view_tickets")(handle_tickets)


async def handle_ticket_detail(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    ticket_id = int(request.match_info["ticket_id"])
    t_raw = await db.ticket_get(ticket_id)
    if not t_raw:
        raise web.HTTPNotFound()
    msgs = [dict(m) for m in await db.ticket_msgs(ticket_id)]
    t = dict(t_raw)

    can_reply = _has_perm(request.cookies.get("dsess_token"), "reply_tickets")
    can_close = _has_perm(request.cookies.get("dsess_token"), "close_tickets")

    st_label = {"open":"🟡 Открыт","in_progress":"🔵 В работе","closed":"✅ Закрыт"}.get(t["status"], t["status"])
    dt_open = t["created_at"].strftime("%d.%m.%Y %H:%M") if t.get("created_at") else "—"

    bubbles = ""
    for m in [dict(x) for x in msgs]:
        cls = "bubble-mod" if m["is_mod"] else "bubble-user"
        who = f"👮 {m['sender_name']}" if m["is_mod"] else f"👤 {m['sender_name']}"
        when = m["sent_at"].strftime("%d.%m %H:%M") if m.get("sent_at") else ""
        align = "align-self:flex-end;" if m["is_mod"] else ""
        bubbles += f'<div class="bubble {cls}" style="{align}"><div class="bubble-meta">{who} · {when}</div>{m["text"]}</div>'

    TEMPLATES = [
        ("Принято", "Ваш тикет принят в работу. Рассмотрим в ближайшее время."),
        ("Нужна инфо", "Для рассмотрения укажите ID пользователя и время инцидента."),
        ("Отклонено", "Тикет отклонён как не соответствующий правилам подачи."),
        ("Решено", "Ваша проблема решена. Если есть вопросы — создайте новый тикет."),
        ("Бан справедлив", "После рассмотрения апелляция отклонена. Блокировка остаётся в силе."),
        ("Разбан", "После рассмотрения блокировка снята. Просьба соблюдать правила чата."),
    ]
    tmpl_btns = "".join(f'<button class="btn btn-xs btn-ghost" onclick="setReply(`{txt}`)">{name}</button>' for name, txt in TEMPLATES)

    reply_form = ""
    if can_reply and t["status"] != "closed":
        reply_form = f"""
        <div class="section" style="margin-top:20px;">
          <div class="section-header">✏️ Ответить
            <div style="display:flex;gap:6px;flex-wrap:wrap;">{tmpl_btns}</div>
          </div>
          <div class="section-body">
            <form method="POST" action="/dashboard/tickets/{ticket_id}/reply">
              <div class="form-group">
                <textarea class="form-control" name="text" id="replyText" rows="4" placeholder="Текст ответа..."></textarea>
              </div>
              <div style="display:flex;gap:10px;">
                <button class="btn btn-primary" type="submit">📨 Отправить</button>
                {"<a class='btn btn-danger' href='/dashboard/tickets/" + str(ticket_id) + "/close'>✅ Закрыть тикет</a>" if can_close else ""}
              </div>
            </form>
          </div>
        </div>"""

    body = navbar(sess, "tickets") + f"""
    <div class="container">
      <div class="page-title">
        🎫 Тикет #{ticket_id}
        <a href="/dashboard/tickets" class="btn btn-ghost btn-sm" style="margin-left:auto;">← Назад</a>
      </div>
      <div class="grid-2">
        <div>
          <div class="section">
            <div class="section-header">💬 Переписка</div>
            <div class="bubble-wrap" style="padding:16px;max-height:500px;overflow-y:auto;">
              {bubbles or "<div class='empty-state'>Сообщений нет</div>"}
            </div>
          </div>
          {reply_form}
        </div>
        <div>
          <div class="section">
            <div class="section-header">ℹ️ Информация</div>
            <div class="section-body" style="font-size:14px;line-height:2.2;">
              <div><b>Статус:</b> {st_label}</div>
              <div><b>Приоритет:</b> <span class="badge badge-{t.get('priority','normal')}">{t.get('priority','—')}</span></div>
              <div><b>Пользователь:</b> {t.get('user_name','—')}</div>
              <div><b>Чат:</b> {t.get('chat_title','—')}</div>
              <div><b>Тема:</b> {t.get('subject','—')}</div>
              <div><b>Модератор:</b> {t.get('assigned_mod','Не назначен')}</div>
              <div><b>Создан:</b> {dt_open}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <script>function setReply(txt){{ document.getElementById('replyText').value=txt; }}</script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_ticket_detail = require_auth("view_tickets")(handle_ticket_detail)


async def handle_ticket_reply(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not _has_perm(token, "reply_tickets"):
        raise web.HTTPFound("/dashboard/tickets")
    sess = _get_session(request)
    ticket_id = int(request.match_info["ticket_id"])
    data = await request.post()
    text = (data.get("text") or "").strip()
    if text and _bot:
        t_row = await db.ticket_get(ticket_id)
        t = dict(t_row) if t_row else None
        if t and t["status"] != "closed":
            mod_name = sess.get("name", "Модератор") if sess else "Модератор (Dashboard)"
            await db.ticket_msg_add(ticket_id=ticket_id, sender_id=0, sender_name=f"👮 {mod_name}", is_mod=True, text=text)
            try:
                await _bot.send_message(t["uid"],
                    f"━━━━━━━━━━━━━━━\n💬 <b>ОТВЕТ ПО ТИКЕТУ #{ticket_id}</b>\n━━━━━━━━━━━━━━━\n\n👮 <b>{mod_name}</b>:\n\n{text}",
                    parse_mode="HTML")
            except:
                pass
    raise web.HTTPFound(f"/dashboard/tickets/{ticket_id}")


async def handle_ticket_close_web(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not _has_perm(token, "close_tickets"):
        raise web.HTTPFound("/dashboard/tickets")
    sess = _get_session(request)
    ticket_id = int(request.match_info["ticket_id"])
    t_row = await db.ticket_get(ticket_id)
    t = dict(t_row) if t_row else None
    if t:
        await db.ticket_close(ticket_id)
        if _bot:
            try:
                await _bot.send_message(t["uid"],
                    f"━━━━━━━━━━━━━━━\n✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n━━━━━━━━━━━━━━━\n\nВаше обращение закрыто администрацией.",
                    parse_mode="HTML")
            except:
                pass
    raise web.HTTPFound("/dashboard/tickets")


# ══════════════════════════════════════════
#  РЕПОРТЫ
# ══════════════════════════════════════════

async def handle_reports(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    token = request.cookies.get("dsess_token")

    # AJAX обработка репорта
    if request.method == "POST":
        data = await request.post()
        action = data.get("action")
        cid = int(data.get("cid", 0))
        idx = int(data.get("idx", 0))
        target_id = int(data.get("target_id", 0))
        if action and _bot and _has_perm(token, "handle_reports"):
            try:
                if action == "warn":
                    # Варн через бот
                    pass
                elif action == "mute":
                    from aiogram.types import ChatPermissions
                    await _bot.restrict_chat_member(cid, target_id, ChatPermissions(can_send_messages=False), until_date=timedelta(hours=1))
                elif action == "ban":
                    await _bot.ban_chat_member(cid, target_id)
                # Обновляем статус
                shared.update_report_status(cid, idx, "accepted")
            except:
                pass
        raise web.HTTPFound("/dashboard/reports")

    reports = shared.get_reports(status=request.rel_url.query.get("status"))
    status_filter = request.rel_url.query.get("status", "")

    new_count = len(shared.get_reports("new"))
    tabs = ""
    for s, label in [("","Все"),("new","🆕 Новые"),("accepted","✅ Принятые"),("rejected","❌ Отклонённые")]:
        active = "btn-primary" if status_filter == s else "btn-ghost"
        count = len(shared.get_reports(s)) if s else len(shared.get_reports())
        tabs += f'<a href="?status={s}" class="btn btn-sm {active}" style="margin-right:6px;">{label} ({count})</a>'

    can_handle = _has_perm(token, "handle_reports")
    rows = ""
    for r in reports[:50]:
        pr = r.get("priority", "NORMAL")
        pr_cls = "badge-urgent" if "HIGH" in pr else ("badge-high" if "HIGH" in pr else "badge-normal")
        st = r.get("status", "new")
        st_cls = {"new":"badge-open","accepted":"badge-closed","rejected":"badge-danger"}.get(st, "badge-accent")
        ts_str = datetime.fromtimestamp(r.get("ts", 0)).strftime("%d.%m %H:%M") if r.get("ts") else "—"
        action_btns = ""
        if can_handle and st == "new":
            cid_v = r.get("cid", 0)
            tid_v = r.get("target_id", 0)
            idx_v = r.get("idx", 0)
            action_btns = f"""
            <form method="POST" style="display:inline;">
              <input type="hidden" name="cid" value="{cid_v}">
              <input type="hidden" name="idx" value="{idx_v}">
              <input type="hidden" name="target_id" value="{tid_v}">
              <button name="action" value="mute" class="btn btn-xs btn-warn">🔇 Мут</button>
              <button name="action" value="ban" class="btn btn-xs btn-danger">🔨 Бан</button>
              <button name="action" value="reject" class="btn btn-xs btn-ghost">❌</button>
            </form>"""
        rows += f"""
        <tr>
          <td>
            <div style="font-weight:600;">{r.get('reporter_name','—')}</div>
            <div style="font-size:11px;color:var(--text2);">{r.get('cid_title','—')}</div>
          </td>
          <td style="font-weight:600;color:var(--danger);">{r.get('target_name','—')}</td>
          <td style="color:var(--text2);">{r.get('reason','—')[:50]}</td>
          <td><span class="badge {pr_cls}">{pr}</span></td>
          <td><span class="badge {st_cls}">{st}</span></td>
          <td style="font-size:11px;color:var(--text2);">{ts_str}</td>
          <td>{action_btns}</td>
        </tr>"""

    body = navbar(sess, "reports") + f"""
    <div class="container">
      <div class="page-title">🚨 Репорты
        <span class="badge badge-urgent" style="margin-left:12px;">{new_count} новых</span>
      </div>
      <div style="margin-bottom:20px;">{tabs}</div>
      <div class="section">
        <table>
          <thead><tr><th>От кого</th><th>На кого</th><th>Причина</th><th>Приоритет</th><th>Статус</th><th>Время</th><th>Действия</th></tr></thead>
          <tbody>{rows or "<tr><td colspan='7' class='empty-state'>Репортов нет</td></tr>"}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_reports = require_auth("view_reports")(handle_reports)


# ══════════════════════════════════════════
#  МОДЕРАЦИЯ
# ══════════════════════════════════════════

async def handle_moderation(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    token = request.cookies.get("dsess_token")

    conn = db.get_conn()
    try:
        top_rows = conn.execute(
            "SELECT by_name, COUNT(*) as cnt, "
            "SUM(CASE WHEN action LIKE '%Бан%' THEN 1 ELSE 0 END) as bans, "
            "SUM(CASE WHEN action LIKE '%Варн%' THEN 1 ELSE 0 END) as warns, "
            "SUM(CASE WHEN action LIKE '%Мут%' THEN 1 ELSE 0 END) as mutes "
            "FROM mod_history GROUP BY by_name ORDER BY cnt DESC LIMIT 10"
        ).fetchall()
        top_mods = [dict(r) for r in top_rows]
        act_rows = conn.execute(
            "SELECT m.action, m.reason, m.by_name, m.created_at, COALESCE(k.title, CAST(m.cid AS TEXT)) as chat "
            "FROM mod_history m LEFT JOIN known_chats k ON m.cid=k.cid ORDER BY m.created_at DESC LIMIT 50"
        ).fetchall()
        recent = [dict(r) for r in act_rows]
    except:
        top_mods = []; recent = []
    conn.close()

    mod_rows = "".join(
        f"<tr><td style='font-weight:700;'>👮 {r['by_name']}</td><td style='color:var(--accent);font-family:JetBrains Mono,monospace;'>{r['cnt']}</td><td style='color:var(--danger);'>{r['bans']}</td><td style='color:var(--warn);'>{r['warns']}</td><td style='color:var(--purple);'>{r['mutes']}</td></tr>"
        for r in top_mods
    ) or "<tr><td colspan='5' class='empty-state'>Нет данных</td></tr>"

    act_rows_html = "".join(
        f"<tr>"
        f"<td style='font-size:11px;color:var(--text2);font-family:JetBrains Mono,monospace;'>{str(r['created_at'])[:16]}</td>"
        f"<td style='color:var(--text2);font-size:12px;'>{r['chat']}</td>"
        f"<td style='font-weight:600;color:{'var(--danger)' if 'Бан' in r['action'] else 'var(--warn)'}'>{r['action']}</td>"
        f"<td style='color:var(--text2);font-size:12px;'>{(r['reason'] or '—')[:40]}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in recent
    ) or "<tr><td colspan='5' class='empty-state'>Нет данных</td></tr>"

    # Форма быстрого действия
    can_ban = _has_perm(token, "ban_users")
    can_mute = _has_perm(token, "mute_users")
    chats = [dict(r) for r in await db.get_all_chats()]
    chat_opts = "".join(f'<option value="{c["cid"]}">{c.get("title","") or c["cid"]}</option>' for c in chats)

    _qa_ban_btns = (
        '<button class="btn btn-danger btn-sm" onclick="qaAction(\'ban\')">🔨 Бан</button>'
        '<button class="btn btn-success btn-sm" onclick="qaAction(\'unban\')">🕊 Разбан</button>'
    ) if can_ban else ""
    _qa_mute_btn = (
        '<button class="btn btn-warn btn-sm" onclick="qaAction(\'mute\')">🔇 Мут 1ч</button>'
    ) if can_mute else ""

    quick_form = ""
    if can_ban or can_mute:
        quick_form = f"""
        <div class="section" style="margin-bottom:20px;max-width:480px;">
          <div class="section-header">⚡ Быстрое действие</div>
          <div class="section-body">
            <div style="display:flex;flex-direction:column;gap:12px;">
              <div class="grid-2" style="gap:10px;">
                <div class="form-group" style="margin:0;">
                  <label>Telegram ID</label>
                  <input class="form-control" id="qa-uid" type="number" placeholder="123456789">
                </div>
                <div class="form-group" style="margin:0;">
                  <label>Чат</label>
                  <select class="form-control" id="qa-cid">{chat_opts}</select>
                </div>
              </div>
              <div class="form-group" style="margin:0;">
                <label>Причина</label>
                <input class="form-control" id="qa-reason" placeholder="Нарушение правил">
              </div>
              <div style="display:flex;gap:8px;flex-wrap:wrap;">
                {_qa_ban_btns}
                {_qa_mute_btn}
              </div>
            </div>
          </div>
        </div>"""

    body = navbar(sess, "moderation") + f"""
    <div class="container">
      <div class="page-title">🛡 Модерация</div>
      {quick_form}
      <div class="grid-2" style="margin-bottom:20px;">
        <div class="section">
          <div class="section-header">👮 Рейтинг модераторов</div>
          <table>
            <thead><tr><th>Модератор</th><th>Всего</th><th>Банов</th><th>Варнов</th><th>Мутов</th></tr></thead>
            <tbody>{mod_rows}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">📊 Статистика</div>
          <div class="section-body">
            <canvas id="modChart" height="180"></canvas>
          </div>
        </div>
      </div>
      <div class="section">
        <div class="section-header">📋 Последние 50 действий</div>
        <table>
          <thead><tr><th>Время</th><th>Чат</th><th>Действие</th><th>Причина</th><th>Модератор</th></tr></thead>
          <tbody>{act_rows_html}</tbody>
        </table>
      </div>
    </div>
    <script>
    function qaAction(action){{
      var uid = document.getElementById('qa-uid').value;
      var cid = document.getElementById('qa-cid').value;
      var reason = document.getElementById('qa-reason').value || 'Нарушение правил';
      if(!uid){{ showToast('Укажите Telegram ID','warn'); return; }}
      modAction(action, parseInt(uid), parseInt(cid), reason);
    }}
    (function(){{
      var ctx = document.getElementById('modChart');
      if(!ctx || typeof Chart==='undefined') return;
      var data = {json.dumps([r['bans'] for r in top_mods[:5]])};
      var warns = {json.dumps([r['warns'] for r in top_mods[:5]])};
      var mutes = {json.dumps([r['mutes'] for r in top_mods[:5]])};
      var labels = {json.dumps([r['by_name'][:12] for r in top_mods[:5]])};
      new Chart(ctx, {{
        type: 'bar',
        data: {{
          labels: labels,
          datasets: [
            {{label:'Банов', data:data, backgroundColor:'rgba(239,68,68,0.7)', borderRadius:4}},
            {{label:'Варнов', data:warns, backgroundColor:'rgba(245,158,11,0.7)', borderRadius:4}},
            {{label:'Мутов', data:mutes, backgroundColor:'rgba(168,85,247,0.7)', borderRadius:4}},
          ]
        }},
        options: {{
          responsive:true,
          plugins:{{legend:{{labels:{{color:'#94a3b8',font:{{size:11}}}}}},tooltip:{{backgroundColor:'rgba(14,18,32,.95)'}}}},
          scales:{{
            x:{{ticks:{{color:'#64748b',font:{{size:11}}}},grid:{{display:false}}}},
            y:{{ticks:{{color:'#64748b',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,0.04)'}}}}
          }}
        }}
      }});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_moderation = require_auth("view_moderation")(handle_moderation)


# ══════════════════════════════════════════
#  АЛЕРТЫ
# ══════════════════════════════════════════

async def handle_alerts(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    token = request.cookies.get("dsess_token")

    if not shared.alerts:
        body = navbar(sess, "alerts") + """
        <div class="container">
          <div class="page-title">🔴 Алерты</div>
          <div class="section"><div class="empty-state">✅ Всё спокойно — алертов нет</div></div>
        </div>""" + close_main()
        return web.Response(text=page(body), content_type="text/html")

    items = ""
    for a in shared.alerts[:50]:
        cls = {"danger": "alert-item", "warn": "alert-item warn", "info": "alert-item info"}.get(a["level"], "alert-item")
        action_btns = ""
        a_uid = a.get("uid", 0)
        a_cid = a.get("cid", 0)
        a_title = a.get("title", "")
        a_desc = a.get("desc", "")
        a_time = a.get("time", "")
        a_safe_title = a_title.replace("'", "")
        if a_uid and a_cid and _bot:
            if _has_perm(token, "ban_users"):
                action_btns += f'<button class="btn btn-xs btn-danger" onclick="modAction(\'ban\',{a_uid},{a_cid},\'Алерт: {a_safe_title}\')">🔨 Бан</button> '
            if _has_perm(token, "mute_users"):
                action_btns += f'<button class="btn btn-xs btn-warn" onclick="modAction(\'mute\',{a_uid},{a_cid},\'Алерт\')">🔇 Мут</button> '
        uid_line = f'<div style="font-size:11px;color:var(--text2);margin-top:3px;">CID: {a_cid} · UID: {a_uid}</div>' if a_uid else ""
        btn_line = f'<div style="margin-top:6px;display:flex;gap:6px;">{action_btns}</div>' if action_btns else ""
        items += (
            f'<div class="{cls}">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start;">'
            f'<div>'
            f'<div style="font-weight:700;">{a_title}</div>'
            f'<div style="font-size:12px;color:var(--text2);margin-top:3px;">{a_desc}</div>'
            f'{uid_line}'
            f'{btn_line}'
            f'</div>'
            f'<span style="font-size:11px;color:var(--text2);white-space:nowrap;margin-left:12px;">{a_time}</span>'
            f'</div>'
            f'</div>'
        )

    body = navbar(sess, "alerts") + f"""
    <div class="container">
      <div class="page-title">
        🔴 Алерты ({len(shared.alerts)})
        <a href="/dashboard/alerts/clear" class="btn btn-ghost btn-sm" style="margin-left:auto;">🗑 Очистить</a>
      </div>
      <div class="section" style="padding:16px;">{items}</div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_alerts = require_auth("view_alerts")(handle_alerts)


async def handle_alerts_clear(request: web.Request):
    token = request.cookies.get("dsess_token")
    if not _has_perm(token, "view_alerts"):
        raise web.HTTPFound("/dashboard")
    shared.alerts.clear()
    raise web.HTTPFound("/dashboard/alerts")


# ══════════════════════════════════════════
#  МЕДИА ЛОГ
# ══════════════════════════════════════════

async def handle_media(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    type_filter = request.rel_url.query.get("type", "all")
    items = shared.media_log
    if type_filter != "all":
        items = [m for m in items if m["type"] == type_filter]

    type_counts = {}
    for m in shared.media_log:
        type_counts[m["type"]] = type_counts.get(m["type"], 0) + 1

    tabs = ""
    for t, emoji in [("all","📁"),("photo","🖼"),("video","🎬"),("document","📄"),("voice","🎤"),("sticker","🎭")]:
        cnt = len(shared.media_log) if t == "all" else type_counts.get(t, 0)
        active = "btn-primary" if type_filter == t else "btn-ghost"
        tabs += f'<a href="?type={t}" class="btn btn-sm {active}" style="margin-right:6px;">{emoji} {t} ({cnt})</a>'

    EMOJIS = {"photo":"🖼","video":"🎬","document":"📄","voice":"🎤","sticker":"🎭","animation":"🎞"}
    rows = "".join(
        f"<tr><td style='font-size:11px;color:var(--text2);'>{m['time']}</td><td>{EMOJIS.get(m['type'],'📁')} {m['type']}</td><td>{m['name']}</td><td>{m['chat']}</td></tr>"
        for m in items[:100]
    ) or "<tr><td colspan='4' class='empty-state'>Нет медиа</td></tr>"

    body = navbar(sess, "media") + f"""
    <div class="container">
      <div class="page-title">🎬 Медиа-лог ({len(items)})</div>
      <div style="margin-bottom:16px;">{tabs}</div>
      <div class="section">
        <table>
          <thead><tr><th>Время</th><th>Тип</th><th>Отправитель</th><th>Чат</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_media = require_auth("view_media")(handle_media)


# ══════════════════════════════════════════
#  УДАЛЁННЫЕ СООБЩЕНИЯ
# ══════════════════════════════════════════

async def handle_deleted(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    cid_filter = request.rel_url.query.get("cid")
    chats = [dict(r) for r in await db.get_all_chats()]

    conn = db.get_conn()
    try:
        if cid_filter:
            rows = [dict(_r) for _r in conn.execute("SELECT * FROM deleted_log WHERE cid=? ORDER BY ts DESC LIMIT 100", (cid_filter,)).fetchall()]
        else:
            rows = [dict(_r) for _r in [dict(_r) for _r in conn.execute("SELECT * FROM deleted_log ORDER BY ts DESC LIMIT 100").fetchall()]]
    except:
        rows = []
    conn.close()

    chat_opts = '<option value="">Все чаты</option>' + "".join(
        f'<option value="{c["cid"]}" {"selected" if str(c["cid"]) == str(cid_filter or "") else ""}>{c.get("title","") or c["cid"]}</option>'
        for c in chats
    )

    rows_html = "".join(
        f"<tr><td style='font-size:11px;color:var(--text2);'>{datetime.fromtimestamp(dict(r).get('ts',0)).strftime('%d.%m %H:%M') if dict(r).get('ts') else '—'}</td><td>{dict(r).get('name','—')}</td><td style='max-width:400px;word-break:break-word;color:var(--text2);'>{dict(r).get('text','')[:200]}</td><td style='font-size:11px;'>{dict(r).get('cid','')}</td></tr>"
        for r in rows
    ) or "<tr><td colspan='4' class='empty-state'>Нет удалённых сообщений</td></tr>"

    body = navbar(sess, "deleted") + f"""
    <div class="container">
      <div class="page-title">🗑 Удалённые сообщения ({len(rows)})</div>
      <div style="margin-bottom:16px;">
        <form method="GET" style="display:flex;gap:10px;align-items:center;">
          <select class="form-control" name="cid" style="max-width:250px;">{chat_opts}</select>
          <button class="btn btn-primary btn-sm" type="submit">Фильтр</button>
        </form>
      </div>
      <div class="section">
        <table>
          <thead><tr><th>Время</th><th>Пользователь</th><th>Сообщение</th><th>Чат ID</th></tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_deleted = require_auth("view_deleted")(handle_deleted)


# ══════════════════════════════════════════
#  ЭКОНОМИКА
# ══════════════════════════════════════════

async def handle_economy(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    cid_filter = request.rel_url.query.get("cid")
    chats = [dict(r) for r in await db.get_all_chats()]

    conn = db.get_conn()
    if cid_filter:
        top_rep = [dict(_r) for _r in conn.execute("SELECT uid, score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT 20", (cid_filter,)).fetchall()]
        top_xp  = [dict(_r) for _r in conn.execute("SELECT uid, xp FROM xp_data WHERE cid=? ORDER BY xp DESC LIMIT 20", (cid_filter,)).fetchall()]
    else:
        top_rep = [dict(_r) for _r in [dict(_r) for _r in conn.execute("SELECT uid, SUM(score) as score FROM reputation GROUP BY uid ORDER BY score DESC LIMIT 20").fetchall()]]
        top_xp  = [dict(_r) for _r in [dict(_r) for _r in conn.execute("SELECT uid, SUM(xp) as xp FROM xp_data GROUP BY uid ORDER BY xp DESC LIMIT 20").fetchall()]]
    try:
        act_rows = [dict(_r) for _r in conn.execute("SELECT day, SUM(count) as total FROM user_activity " + ("WHERE cid=? " if cid_filter else "") + "GROUP BY day ORDER BY day DESC LIMIT 14", *((cid_filter,) if cid_filter else ())).fetchall()]
    except:
        act_rows = []
    conn.close()

    chat_opts = '<option value="">Все чаты</option>' + "".join(f'<option value="{c["cid"]}" {"selected" if str(c["cid"])==str(cid_filter or "") else ""}>{c.get("title","") or c["cid"]}</option>' for c in chats)

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    rep_html = "".join(f"<tr><td>{medals[i] if i<10 else i+1}</td><td><a href='/dashboard/users/{r['uid']}' style='color:var(--accent);'><code>{r['uid']}</code></a></td><td style='color:var(--success);font-weight:700;'>{r['score']:+d}</td></tr>" for i,r in enumerate([dict(x) for x in top_rep], 0)) or "<tr><td colspan='3' class='empty-state'>Нет</td></tr>"
    xp_html = "".join(f"<tr><td>{medals[i] if i<10 else i+1}</td><td><code>{r['uid']}</code></td><td style='color:var(--accent);font-weight:700;'>{r['xp']:,}</td></tr>" for i,r in enumerate([dict(x) for x in top_xp], 0)) or "<tr><td colspan='3' class='empty-state'>Нет</td></tr>"

    act_data = [dict(r) for r in act_rows][::-1]
    act_labels = json.dumps([str(r.get("day",""))[-5:] for r in act_data])
    act_values = json.dumps([r.get("total", 0) for r in act_data])

    body = navbar(sess, "economy") + f"""
    <div class="container">
      <div class="page-title">💰 Экономика</div>
      <div style="margin-bottom:20px;">
        <form method="GET" style="display:flex;gap:10px;align-items:center;">
          <select class="form-control" name="cid" style="max-width:250px;">{chat_opts}</select>
          <button class="btn btn-primary btn-sm">Фильтр</button>
        </form>
      </div>
      <div class="section" style="margin-bottom:20px;">
        <div class="section-header">📈 Активность за последние 14 дней</div>
        <div style="padding:16px;"><canvas id="actChart" height="70"></canvas></div>
      </div>
      <div class="grid-2">
        <div class="section">
          <div class="section-header">⭐ Топ репутации</div>
          <table><thead><tr><th>#</th><th>ID</th><th>Репутация</th></tr></thead><tbody>{rep_html}</tbody></table>
        </div>
        <div class="section">
          <div class="section-header">🏆 Топ XP</div>
          <table><thead><tr><th>#</th><th>ID</th><th>XP</th></tr></thead><tbody>{xp_html}</tbody></table>
        </div>
      </div>
    </div>
    <script>
    (function(){{
      var ctx = document.getElementById('actChart');
      if(!ctx||typeof Chart==='undefined') return;
      new Chart(ctx, {{
        type:'line',
        data:{{
          labels:{act_labels},
          datasets:[{{
            label:'Активность',
            data:{act_values},
            borderColor:'#3b82f6',
            backgroundColor:'rgba(59,130,246,0.1)',
            fill:true, tension:0.4, pointRadius:4,
            pointBackgroundColor:'#3b82f6',
          }}]
        }},
        options:{{
          responsive:true,
          plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'rgba(14,18,32,.95)'}}}},
          scales:{{
            y:{{ticks:{{color:'#64748b',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,0.04)'}}}},
            x:{{ticks:{{color:'#64748b',font:{{size:11}}}},grid:{{display:false}}}}
          }}
        }}
      }});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_economy = require_auth("view_economy")(handle_economy)


# ══════════════════════════════════════════
#  АНАЛИТИКА
# ══════════════════════════════════════════

async def handle_analytics(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    conn = db.get_conn()
    try:
        daily = [dict(_r) for _r in [dict(_r) for _r in conn.execute("SELECT day, SUM(count) as total FROM user_activity GROUP BY day ORDER BY day DESC LIMIT 30").fetchall()]]
        top_words = [dict(_r) for _r in [dict(_r) for _r in conn.execute("SELECT word, SUM(count) as cnt FROM word_stats GROUP BY word ORDER BY cnt DESC LIMIT 20").fetchall()]] if False else []
    except:
        daily = []
        top_words = []
    conn.close()

    daily_data = [dict(r) for r in daily][::-1]
    d_labels = json.dumps([str(r.get("day", ""))[-5:] for r in daily_data])
    d_values = json.dumps([r.get("total", 0) for r in daily_data])

    chats = [dict(r) for r in await db.get_all_chats()]
    ticket_stats = await db.ticket_stats_all()

    conn2 = db.get_conn()
    total_bans = conn2.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    total_warns = conn2.execute("SELECT COALESCE(SUM(count),0) FROM warnings").fetchone()[0] or 0
    try:
        bans_cnt = conn2.execute("SELECT COUNT(*) as c FROM mod_history WHERE action LIKE '%Бан%'").fetchone()["c"] or 0
        warns_cnt = conn2.execute("SELECT COUNT(*) as c FROM mod_history WHERE action LIKE '%Варн%'").fetchone()["c"] or 0
        mutes_cnt = conn2.execute("SELECT COUNT(*) as c FROM mod_history WHERE action LIKE '%Мут%'").fetchone()["c"] or 0
    except:
        bans_cnt = warns_cnt = mutes_cnt = 0
    conn2.close()

    body = navbar(sess, "analytics") + f"""
    <div class="container">
      <div class="page-title">📈 Аналитика</div>

      <div class="grid-3" style="margin-bottom:20px;">
        <div class="card">
          <div class="card-icon">💬</div>
          <div class="card-label">Всего чатов</div>
          <div class="card-value">{len(chats)}</div>
        </div>
        <div class="card">
          <div class="card-icon">🎫</div>
          <div class="card-label">Тикетов всего</div>
          <div class="card-value">{ticket_stats['total']}</div>
          <div class="card-sub">{ticket_stats['open']} открытых</div>
        </div>
        <div class="card">
          <div class="card-icon">🛡</div>
          <div class="card-label">Действий модерации</div>
          <div class="card-value">{bans_cnt + warns_cnt + mutes_cnt}</div>
          <div class="card-sub">бан+варн+мут</div>
        </div>
      </div>

      <div class="grid-2" style="margin-bottom:20px;">
        <div class="section">
          <div class="section-header">📊 Активность за 30 дней</div>
          <div style="padding:16px;"><canvas id="dailyChart" height="120"></canvas></div>
        </div>
        <div class="section">
          <div class="section-header">🍩 Типы действий модерации</div>
          <div style="padding:16px;display:flex;align-items:center;justify-content:center;"><canvas id="modPieChart" height="180" width="180"></canvas></div>
        </div>
      </div>

      <div class="section">
        <div class="section-header">🎫 Статистика тикетов</div>
        <div style="padding:20px;display:flex;gap:24px;flex-wrap:wrap;">
          <div style="text-align:center;">
            <div style="font-size:32px;font-weight:800;color:var(--warn);">{ticket_stats['open']}</div>
            <div style="font-size:12px;color:var(--text2);">Открытых</div>
          </div>
          <div style="text-align:center;">
            <div style="font-size:32px;font-weight:800;color:var(--accent);">{ticket_stats['in_progress']}</div>
            <div style="font-size:12px;color:var(--text2);">В работе</div>
          </div>
          <div style="text-align:center;">
            <div style="font-size:32px;font-weight:800;color:var(--success);">{ticket_stats['closed']}</div>
            <div style="font-size:12px;color:var(--text2);">Закрытых</div>
          </div>
          <div style="text-align:center;">
            <div style="font-size:32px;font-weight:800;">{ticket_stats['total']}</div>
            <div style="font-size:12px;color:var(--text2);">Всего</div>
          </div>
        </div>
      </div>
    </div>
    <script>
    (function(){{
      if(typeof Chart==='undefined') return;
      Chart.defaults.color = '#64748b';

      // Daily activity
      var ctx1 = document.getElementById('dailyChart');
      if(ctx1) new Chart(ctx1, {{
        type:'line',
        data:{{
          labels:{d_labels},
          datasets:[{{
            label:'Активность',
            data:{d_values},
            borderColor:'#a855f7',
            backgroundColor:'rgba(168,85,247,0.08)',
            fill:true,tension:0.4,pointRadius:3,
          }}]
        }},
        options:{{responsive:true,plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'rgba(14,18,32,.95)'}}}},
          scales:{{y:{{grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{size:11}}}}}},x:{{grid:{{display:false}},ticks:{{font:{{size:10}},maxTicksLimit:8}}}}}}
        }}
      }});

      // Mod pie
      var ctx2 = document.getElementById('modPieChart');
      if(ctx2) new Chart(ctx2, {{
        type:'doughnut',
        data:{{
          labels:['Банов','Варнов','Мутов'],
          datasets:[{{
            data:[{bans_cnt},{warns_cnt},{mutes_cnt}],
            backgroundColor:['rgba(239,68,68,0.8)','rgba(245,158,11,0.8)','rgba(168,85,247,0.8)'],
            borderWidth:0, hoverOffset:4,
          }}]
        }},
        options:{{
          responsive:false,
          plugins:{{legend:{{position:'bottom',labels:{{font:{{size:11}},padding:12}}}},tooltip:{{backgroundColor:'rgba(14,18,32,.95)'}}}}
        }}
      }});
    }})();
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_analytics = require_auth("view_overview")(handle_analytics)


# ══════════════════════════════════════════
#  РАССЫЛКА
# ══════════════════════════════════════════

async def handle_broadcast(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    chats = [dict(r) for r in await db.get_all_chats()]
    result = ""

    if request.method == "POST":
        data = await request.post()
        text = data.get("text","").strip()
        target = data.get("target","all")
        cid_sel = data.get("cid","")
        if text and _bot:
            targets = [c["cid"] for c in chats] if target == "all" else ([int(cid_sel)] if cid_sel else [])
            sent = failed = 0
            for cid in targets:
                try:
                    await _bot.send_message(cid, f"📢 <b>Сообщение от администрации</b>\n\n{text}", parse_mode="HTML")
                    sent += 1
                    await asyncio.sleep(0.1)
                except:
                    failed += 1
            _log_admin_db(sess.get("uid",0) if sess else 0, "BROADCAST", f"Отправлено в {sent} чатов: {text[:50]}")
            result = f'<div style="padding:12px 16px;background:rgba(34,197,94,.1);border-radius:8px;margin-bottom:16px;color:var(--success);font-weight:600;">✅ Отправлено в {sent} чатов. Ошибок: {failed}</div>'

    TEMPLATES = [
        ("Обновление бота", "Бот обновлён! Появились новые функции. Используйте /help для подробностей."),
        ("Техработы", "⚠️ Планируются технические работы. Бот может быть временно недоступен."),
        ("Конкурс", "🎉 Объявляем конкурс! Подробности у администраторов."),
        ("Правила", "📋 Напоминаем о соблюдении правил чата. /rules для ознакомления."),
        ("Поздравление", "🎊 Поздравляем всех участников! Спасибо что вы с нами!"),
    ]
    tmpl_btns = "".join(f'<button type="button" class="btn btn-xs btn-ghost" onclick="document.querySelector(\'[name=text]\').value=`{txt}`">{name}</button>' for name, txt in TEMPLATES)

    chat_opts = "".join(f'<option value="{c["cid"]}">{c.get("title","") or c["cid"]}</option>' for c in chats)

    body = navbar(sess, "broadcast") + f"""
    <div class="container">
      <div class="page-title">📢 Рассылка</div>
      {result}
      <div class="section" style="max-width:600px;">
        <div class="section-header">✉️ Отправить сообщение</div>
        <div class="section-body">
          <form method="POST">
            <div class="form-group">
              <label>Получатели</label>
              <select class="form-control" name="target" onchange="document.getElementById('cid_row').style.display=this.value=='one'?'block':'none'">
                <option value="all">Все чаты ({len(chats)})</option>
                <option value="one">Конкретный чат</option>
              </select>
            </div>
            <div class="form-group" id="cid_row" style="display:none;">
              <label>Чат</label>
              <select class="form-control" name="cid">{chat_opts}</select>
            </div>
            <div class="form-group">
              <label>Текст сообщения <span style="color:var(--text2);font-weight:400;">(поддерживает HTML)</span></label>
              <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;">{tmpl_btns}</div>
              <textarea class="form-control" name="text" rows="5" placeholder="Поддерживается HTML: <b>, <i>, <code>"></textarea>
            </div>
            <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;">📨 Отправить</button>
          </form>
        </div>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_broadcast = require_auth("broadcast")(handle_broadcast)


# ══════════════════════════════════════════
#  ПЛАГИНЫ
# ══════════════════════════════════════════

async def handle_plugins(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    chats = [dict(r) for r in await db.get_all_chats()]

    if request.method == "POST":
        data = await request.post()
        cid = int(data.get("cid", 0))
        plugin = data.get("plugin", "")
        enabled = data.get("enabled", "0") == "1"
        if cid and plugin:
            conn = db.get_conn()
            try:
                conn.execute("CREATE TABLE IF NOT EXISTS plugins (cid INTEGER, key TEXT, enabled INTEGER DEFAULT 1, PRIMARY KEY (cid, key))")
                conn.execute("INSERT INTO plugins (cid,key,enabled) VALUES (?,?,?) ON CONFLICT(cid,key) DO UPDATE SET enabled=?", (cid, plugin, int(enabled), int(enabled)))
                conn.commit()
            except:
                pass
            conn.close()
            _log_admin_db(sess.get("uid",0) if sess else 0, "PLUGIN", f"Плагин '{plugin}' {'вкл' if enabled else 'выкл'} для чата {cid}")
        raise web.HTTPFound("/dashboard/plugins")

    cid_filter = request.rel_url.query.get("cid")
    selected_cid = int(cid_filter) if cid_filter else (chats[0]["cid"] if chats else 0)

    PLUGINS = [
        ("economy","💰 Экономика","Репутация, магазин, лотерея"),
        ("games","🎮 Игры","Развлекательные команды"),
        ("xp","⭐ XP система","Опыт за сообщения, уровни"),
        ("antispam","🛡 Антиспам","Защита от флуда"),
        ("antimat","🧹 Антимат","Фильтр нецензурных слов"),
        ("reports","🚨 Репорты","Система жалоб"),
        ("events","🎉 Ивенты","Бонусные события"),
        ("newspaper","📰 Газета","Ежедневная статистика"),
        ("clans","🤝 Кланы","Система кланов"),
    ]

    conn = db.get_conn()
    plugin_states = {}
    for key, _, _ in PLUGINS:
        try:
            row = conn.execute("SELECT enabled FROM plugins WHERE cid=? AND key=?", (selected_cid, key)).fetchone()
            plugin_states[key] = bool(row["enabled"]) if row else True
        except:
            plugin_states[key] = True
    conn.close()

    chat_opts = "".join(f'<option value="{c["cid"]}" {"selected" if c["cid"]==selected_cid else ""}>{c.get("title","") or c["cid"]}</option>' for c in chats)

    plugins_html = ""
    for key, label, desc in PLUGINS:
        enabled = plugin_states.get(key, True)
        new_val = "0" if enabled else "1"
        btn_cls = "btn-danger" if enabled else "btn-success"
        btn_txt = "❌ Выкл" if enabled else "✅ Вкл"
        status_badge = f'<span class="badge {"badge-open" if enabled else "badge-closed"}">{"Включён" if enabled else "Выключен"}</span>'
        plugins_html += f"""
        <div style="display:flex;align-items:center;justify-content:space-between;padding:14px 0;border-bottom:1px solid var(--border);">
          <div>
            <div style="font-weight:700;font-size:14px;">{label}</div>
            <div style="font-size:12px;color:var(--text2);">{desc}</div>
          </div>
          <div style="display:flex;align-items:center;gap:10px;">
            {status_badge}
            <form method="POST" style="display:inline;">
              <input type="hidden" name="cid" value="{selected_cid}">
              <input type="hidden" name="plugin" value="{key}">
              <input type="hidden" name="enabled" value="{new_val}">
              <button class="btn btn-sm {btn_cls}" type="submit">{btn_txt}</button>
            </form>
          </div>
        </div>"""

    body = navbar(sess, "plugins") + f"""
    <div class="container">
      <div class="page-title">🧩 Плагины</div>
      <div style="margin-bottom:20px;">
        <form method="GET" style="display:flex;gap:10px;align-items:center;">
          <select class="form-control" name="cid" style="max-width:280px;">{chat_opts}</select>
          <button class="btn btn-primary btn-sm" type="submit">Выбрать чат</button>
        </form>
      </div>
      <div class="section" style="max-width:640px;">
        <div class="section-header">
          ⚙️ Плагины чата
          <span style="font-size:12px;color:var(--text2);">Включено: {sum(plugin_states.values())}/{len(PLUGINS)}</span>
        </div>
        <div style="padding:0 20px;">{plugins_html}</div>
      </div>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_plugins = require_auth("manage_plugins")(handle_plugins)


# ══════════════════════════════════════════
#  НАСТРОЙКИ ЧАТА
# ══════════════════════════════════════════

async def handle_chat_settings(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    import chat_settings as cs
    chats = [dict(r) for r in await db.get_all_chats()]
    cid = int(request.match_info.get("cid", 0))
    if not cid and chats:
        cid = chats[0]["cid"]

    if request.method == "POST":
        data = await request.post()
        settings = cs.get_settings(cid)
        for key in ["close_time","open_time","quiet_start","quiet_end","welcome_text","announce_text","flood_action","verify_question","verify_answer"]:
            if key in data:
                settings[key] = data[key]
        for key in ["schedule_enabled","quiet_enabled","auto_mute_newcomers","auto_kick_inactive","welcome_enabled","welcome_delete_prev","verify_enabled","antispam_enabled","antimat_enabled","antilink_enabled","antisticker_enabled","anticaps_enabled","xp_enabled","rep_enabled","games_enabled","economy_enabled","anon_enabled","clans_enabled","announce_enabled","rules_remind_enabled"]:
            settings[key] = data.get(key) == "1"
        for key, default in [("max_warns",3),("warn_expiry_days",30),("mute_duration",60),("newcomer_mute_hours",1),("inactive_days",30),("flood_msgs",10),("caps_percent",70),("xp_per_msg",5),("rep_cooldown_hours",1),("daily_bonus",50),("newcomer_bonus",100),("announce_interval",100),("rules_remind_interval",200)]:
            try:
                settings[key] = int(data.get(key, default))
            except:
                pass
        cs.save_settings(cid, settings)
        _log_admin_db(sess.get("uid",0) if sess else 0, "CHAT_SETTINGS", f"Настройки чата {cid} обновлены")
        raise web.HTTPFound(f"/dashboard/chat_settings/{cid}")

    s = cs.get_settings(cid)
    chat_title = next((c.get("title","") or str(cid) for c in chats if c["cid"]==cid), str(cid))
    chat_opts = "".join(f'<option value="{c["cid"]}" {"selected" if c["cid"]==cid else ""}>{c.get("title","") or c["cid"]}</option>' for c in chats)

    def tog(key, label, desc=""):
        val = s.get(key, False)
        desc_html = f"<span>{desc}</span>" if desc else ""
        checked = "checked" if val else ""
        return (
            f'<div class="toggle-wrap">'
            f'<div class="toggle-info"><b>{label}</b>{desc_html}</div>'
            f'<label class="toggle-switch">'
            f'<input type="checkbox" name="{key}" value="1" {checked}>'
            f'<span class="toggle-slider"></span>'
            f'</label>'
            f'</div>'
        )

    def inp(key, type_="text", min_=None, max_=None, default=""):
        val = s.get(key, default)
        extra = f' min="{min_}" max="{max_}"' if min_ is not None else ""
        return f'<input class="form-control" type="{type_}" name="{key}" value="{val}"{extra}>'

    def section_block(title, *content):
        return f'<div class="section" style="margin-bottom:16px;"><div class="section-header">{title}</div><div style="padding:0 20px;">{"".join(content)}</div></div>'

    body = navbar(sess, "chat_settings") + f"""
    <div class="container">
      <div class="page-title">⚙️ Настройки: {chat_title}</div>
      <div style="margin-bottom:20px;">
        <form method="GET" style="display:flex;gap:10px;">
          <select class="form-control" name="x" style="max-width:280px;" onchange="window.location='/dashboard/chat_settings/'+this.value">{chat_opts}</select>
        </form>
      </div>
      <form method="POST">
        <div class="grid-2">
          <div>
            {section_block("⏰ Расписание",
              tog("schedule_enabled","Авто-закрытие/открытие","По расписанию"),
              f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:10px 0;"><div><label style="font-size:11px;color:var(--text2);">Закрыть в</label>{inp("close_time","time")}</div><div><label style="font-size:11px;color:var(--text2);">Открыть в</label>{inp("open_time","time")}</div></div>',
              tog("quiet_enabled","Тихий час","Режим только чтение"),
              f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:10px 0;"><div><label style="font-size:11px;color:var(--text2);">Начало</label>{inp("quiet_start","time")}</div><div><label style="font-size:11px;color:var(--text2);">Конец</label>{inp("quiet_end","time")}</div></div>',
            )}
            {section_block("👋 Приветствие",
              tog("welcome_enabled","Приветствие включено"),
              f'<div class="form-group" style="margin-top:10px;"><label>Текст приветствия ({{name}})</label><textarea class="form-control" name="welcome_text" rows="2">{s.get("welcome_text","")}</textarea></div>',
              tog("welcome_delete_prev","Удалять старое приветствие"),
              tog("verify_enabled","Верификация при входе"),
              f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px;"><div><label style="font-size:11px;color:var(--text2);">Вопрос</label>{inp("verify_question")}</div><div><label style="font-size:11px;color:var(--text2);">Ответ</label>{inp("verify_answer")}</div></div>',
            )}
            {section_block("🎮 Функции",
              tog("xp_enabled","XP система"),
              tog("rep_enabled","Репутация"),
              tog("games_enabled","Игры"),
              tog("economy_enabled","Экономика"),
              tog("anon_enabled","Анонимки"),
              tog("clans_enabled","Кланы"),
            )}
          </div>
          <div>
            {section_block("🛡 Модерация",
              tog("auto_mute_newcomers","Автомут новичков"),
              f'<div class="form-group" style="margin-top:8px;"><label>Макс. варнов до бана</label>{inp("max_warns","number",1,10)}</div>',
              f'<div class="form-group"><label>Срок варна (дней)</label>{inp("warn_expiry_days","number",1,365)}</div>',
              f'<div class="form-group"><label>Длительность мута (мин)</label>{inp("mute_duration","number",1,10080)}</div>',
            )}
            {section_block("🤖 Антиспам",
              tog("antispam_enabled","Антиспам"),
              tog("antimat_enabled","Антимат"),
              tog("antilink_enabled","Антилинк"),
              tog("antisticker_enabled","Антистикер"),
              tog("anticaps_enabled","Антикапс"),
              f'<div class="form-group" style="margin-top:8px;"><label>Порог флуда (сообщ/мин)</label>{inp("flood_msgs","number",3,60)}</div>',
              f'<div class="form-group"><label>Действие при флуде</label><select class="form-control" name="flood_action"><option value="mute" {"selected" if s.get("flood_action")=="mute" else ""}>Мут</option><option value="warn" {"selected" if s.get("flood_action")=="warn" else ""}>Варн</option><option value="kick" {"selected" if s.get("flood_action")=="kick" else ""}>Кик</option></select></div>',
            )}
            {section_block("📢 Авто-объявления",
              tog("announce_enabled","Авто-объявление"),
              f'<div class="form-group" style="margin-top:8px;"><label>Текст объявления</label><textarea class="form-control" name="announce_text" rows="2">{s.get("announce_text","")}</textarea></div>',
              f'<div class="form-group"><label>Каждые N сообщений</label>{inp("announce_interval","number",10,10000)}</div>',
              tog("rules_remind_enabled","Напоминание правил"),
            )}
          </div>
        </div>
        <button class="btn btn-primary" type="submit" style="width:100%;padding:14px;font-size:15px;margin-top:8px;">
          💾 Сохранить настройки
        </button>
      </form>
    </div>""" + close_main()
    return web.Response(text=page(body), content_type="text/html")

handle_chat_settings = require_auth("manage_chat_settings")(handle_chat_settings)


# ══════════════════════════════════════════
#  НАСТРОЙКИ ДАШБОРДА
# ══════════════════════════════════════════

async def handle_settings(request: web.Request):
    sess = _get_session(request)
    _track_session(request)

    if request.method == "POST":
        data = await request.post()
        shared.dashboard_settings["alerts_enabled"]    = data.get("alerts_enabled") == "1"
        shared.dashboard_settings["media_log_enabled"] = data.get("media_log_enabled") == "1"
        shared.dashboard_settings["spam_threshold"]    = int(data.get("spam_threshold", 10))
        shared.dashboard_settings["flood_threshold"]   = int(data.get("flood_threshold", 15))
        shared.dashboard_settings["show_user_ids"]     = data.get("show_user_ids") == "1"
        shared.dashboard_settings["auto_refresh"]      = data.get("auto_refresh") == "1"
        shared.dashboard_settings["items_per_page"]    = int(data.get("items_per_page", 20))
        _log_admin_db(sess.get("uid",0) if sess else 0, "SETTINGS", "Настройки дашборда обновлены")
        raise web.HTTPFound("/dashboard/settings")

    s = shared.dashboard_settings
    def tog(key, label, desc=""):
        val = s.get(key, True)
        desc_html = f"<span>{desc}</span>" if desc else ""
        checked = "checked" if val else ""
        return (
            f'<div class="toggle-wrap">'
            f'<div class="toggle-info"><b>{label}</b>{desc_html}</div>'
            f'<label class="toggle-switch">'
            f'<input type="checkbox" name="{key}" value="1" {checked}>'
            f'<span class="toggle-slider"></span>'
            f'</label>'
            f'</div>'
        )

    log_html = "".join(f"<tr><td style='font-size:11px;color:var(--text2);'>{r['time']}</td><td>{r['action']}</td></tr>" for r in shared.admin_action_log[:20]) or "<tr><td colspan='2' class='empty-state'>Нет</td></tr>"

    body = navbar(sess, "settings") + f"""
    <div class="container">
      <div class="page-title">🔧 Настройки дашборда</div>
      <div class="grid-2">
        <div>
          <div class="section">
            <div class="section-header">⚙️ Параметры</div>
            <form method="POST" style="padding:0 20px;">
              {tog("alerts_enabled","Алерты","Детектор спама и флуда")}
              {tog("media_log_enabled","Медиа-лог","Логировать фото/видео")}
              {tog("show_user_ids","Показывать ID","ID пользователей в таблицах")}
              {tog("auto_refresh","Авто-обновление","Обновлять каждые 30с")}
              <div style="padding:16px 0;">
                <div class="form-group">
                  <label>Порог спама (сообщ/мин)</label>
                  <input class="form-control" type="number" name="spam_threshold" value="{s['spam_threshold']}" min="5" max="60">
                </div>
                <div class="form-group">
                  <label>Порог флуда (сообщ/мин)</label>
                  <input class="form-control" type="number" name="flood_threshold" value="{s['flood_threshold']}" min="5" max="100">
                </div>
                <div class="form-group">
                  <label>Записей на странице</label>
                  <select class="form-control" name="items_per_page">
                    {''.join(f'<option value="{n}" {"selected" if s["items_per_page"]==n else ""}>{n}</option>' for n in [10,20,50,100])}
                  </select>
                </div>
                <button class="btn btn-primary" type="submit" style="width:100%;">💾 Сохранить</button>
              </div>
            </form>
          </div>
          <div class="section" style="margin-top:16px;">
            <div class="section-header">📥 Экспорт данных</div>
            <div style="padding:16px;display:flex;flex-direction:column;gap:8px;">
              <a href="/dashboard/export/stats" class="btn btn-outline">📊 Статистика (.csv)</a>
              <a href="/dashboard/export/bans" class="btn btn-outline">🔨 Список банов (.csv)</a>
              <a href="/dashboard/export/modhistory" class="btn btn-outline">📋 История модерации (.csv)</a>
            </div>
          </div>
        </div>
        <div class="section">
          <div class="section-header">📜 Лог действий</div>
          <table>
            <thead><tr><th>Время</th><th>Действие</th></tr></thead>
            <tbody>{log_html}</tbody>
          </table>
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
    next_color  = DASHBOARD_RANKS.get(next_rank, {}).get("color", "#6366f1")
    rgb         = _hex_to_rgb(next_color)
    remaining   = max(0, threshold - total_acts)

    metrics = [
        ("🔨", "Банов",  stats.get("bans",0),    "#ef4444"),
        ("⚡", "Варнов", stats.get("warns",0),   "#f59e0b"),
        ("🔇", "Мутов",  stats.get("mutes",0),   "#8b5cf6"),
        ("🎫", "Тикетов",stats.get("tickets",0), "#3b82f6"),
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
            borderColor: "#6366f1",
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
.md-body code { background:var(--bg3); padding:2px 6px; border-radius:4px; font-family:'JetBrains Mono',monospace; font-size:12px; color:var(--accent); }
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
                style="font-family:'JetBrains Mono',monospace;font-size:13px;resize:vertical;">{f_body}</textarea>
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
            f'<span style="font-size:12px;font-weight:700;color:{color};font-family:JetBrains Mono,monospace;">{sign} симв</span>'
            f'</div>'
            f'<div style="font-size:12px;color:var(--text2);background:var(--bg3);padding:8px 12px;'
            f'border-radius:6px;font-family:JetBrains Mono,monospace;white-space:pre-wrap;word-break:break-all;">'
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

                elif action == "unlock_chat" and chat_id:
                    await bot_inst.set_chat_permissions(chat_id,
                        ChatPermissions(can_send_messages=True,
                            can_send_media_messages=True, can_send_polls=True,
                            can_send_other_messages=True, can_add_web_page_previews=True))
                    msg = f"🔓 Чат {chat_id} разблокирован"
                    _log_admin_db(sess_uid, "BOT_UNLOCK_CHAT", str(chat_id))

                elif action == "slowmode" and chat_id:
                    delay = int(data.get("slowmode_val","30") or 30)
                    await bot_inst.set_chat_slow_mode_delay(chat_id, delay)
                    msg = f"🐢 Slowmode {delay}с в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_SLOWMODE", f"{chat_id}: {delay}s")

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

                elif action == "unban_user" and chat_id and data.get("target_uid"):
                    target_uid = int(data.get("target_uid",0))
                    await bot_inst.unban_chat_member(chat_id, target_uid, only_if_banned=True)
                    msg = f"🕊 Разбан ID{target_uid} в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_UNBAN", f"{chat_id}:{target_uid}")

                elif action == "mute_user" and chat_id and data.get("target_uid"):
                    from datetime import timedelta
                    target_uid = int(data.get("target_uid",0))
                    mins = int(data.get("mute_mins","60") or 60)
                    await bot_inst.restrict_chat_member(chat_id, target_uid,
                        ChatPermissions(can_send_messages=False),
                        until_date=timedelta(minutes=mins))
                    msg = f"🔇 Мут ID{target_uid} на {mins}мин в чате {chat_id}"
                    _log_admin_db(sess_uid, "BOT_MUTE", f"{chat_id}:{target_uid} {mins}m")

                elif action == "kick_user" and chat_id and data.get("target_uid"):
                    target_uid = int(data.get("target_uid",0))
                    await bot_inst.ban_chat_member(chat_id, target_uid)
                    await bot_inst.unban_chat_member(chat_id, target_uid)
                    msg = f"👟 Кик ID{target_uid} из чата {chat_id}"
                    _log_admin_db(sess_uid, "BOT_KICK", f"{chat_id}:{target_uid}")

                elif action == "set_title" and chat_id and data.get("new_title"):
                    title = data.get("new_title","").strip()
                    await bot_inst.set_chat_title(chat_id, title)
                    msg = f"✏️ Название чата {chat_id} изменено на «{title}»"

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
                    await bot_inst.set_my_commands(commands)
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
        rows_html += (
            f"<div style='padding:16px;border-bottom:1px solid var(--border);cursor:pointer;' data-url='{_inc_url}' onclick='window.location=this.dataset.url'>"
            f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
            f"<div style='padding:16px;border-bottom:1px solid var(--border);cursor:pointer;'" 
            f"onclick='window.location=\"{_inc_url}\"'>"
            f'background:rgba({_hex_to_rgb(sev_color)},.15);color:{sev_color};">{sev_icon} {sev_label}</span>'
            f'<span style="font-size:11px;color:var(--text2);">{st_icon} {st_label}</span>'
            f'<span style="margin-left:auto;font-size:11px;color:var(--text2);">'
            f'{str(inc["created_at"])[:16]} · {inc["created_by"]}</span>'
            f'</div>'
            f'{"<div style=\"font-size:12px;color:var(--text2);margin-top:6px;\">" + inc["description"][:100] + "...</div>" if inc.get("description") else ""}'
            f'</div>'
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

    # Запускаем фоновые задачи
    _start_background_tasks()

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
            f'<td style="font-family:JetBrains Mono,monospace;color:var(--accent);">{total_actions}</td>'
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
            f'<td style="color:{score_color};font-weight:700;font-family:JetBrains Mono,monospace;">{score}</td>'
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
    "default":  {"accent":"#3b82f6", "purple":"#a855f7", "bg":"#080b14",  "name":"🌑 Тёмная (по умолч.)"},
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
                <input type="color" name="custom_accent" value="{current.get("accent","#3b82f6")}"
                  style="width:50px;height:40px;border-radius:8px;border:1px solid var(--border);background:transparent;cursor:pointer;">
                <input class="form-control" type="text" value="{current.get("accent","#3b82f6")}"
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
      var accent = "{current.get("accent","#3b82f6")}";
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

# -*- coding: utf-8 -*-
"""
dashboard.py — Глобально улучшенная веб-панель управления
Приоритеты 1-5 + Система администрации с 10 рангами
Только владелец (ID: 7823802800) может выдавать/забирать роли
"""
import os
import json
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

_bot = None
_admin_ids = set()

def set_bot(bot, admin_ids: set):
    global _bot, _admin_ids
    _bot = bot
    _admin_ids = admin_ids

# ══════════════════════════════════════════
#  СИСТЕМА АДМИНИСТРАЦИИ — 10 РАНГОВ
# ══════════════════════════════════════════

DASHBOARD_RANKS = {
    1: {
        "name": "👁 Наблюдатель",
        "color": "#607d8b",
        "perms": ["view_overview", "view_chats", "view_users"],
        "desc": "Только просмотр основной статистики"
    },
    2: {
        "name": "📋 Репортёр",
        "color": "#78909c",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports", "view_alerts"],
        "desc": "Просмотр репортов и алертов"
    },
    3: {
        "name": "🎫 Поддержка",
        "color": "#26a69a",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets"],
        "desc": "Работа с тикетами и просмотр репортов"
    },
    4: {
        "name": "🛡 Юниор-Мод",
        "color": "#42a5f5",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets",
                  "close_tickets", "handle_reports", "view_moderation"],
        "desc": "Закрытие тикетов, обработка репортов"
    },
    5: {
        "name": "⚔️ Мод",
        "color": "#66bb6a",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets", "close_tickets",
                  "handle_reports", "view_moderation", "mute_users", "warn_users",
                  "view_media", "view_deleted"],
        "desc": "Мут и варн пользователей, медиа-лог"
    },
    6: {
        "name": "⚡ Старший Мод",
        "color": "#ffa726",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets", "close_tickets",
                  "handle_reports", "view_moderation", "mute_users", "warn_users",
                  "view_media", "view_deleted", "ban_users", "unban_users",
                  "view_economy"],
        "desc": "Бан/разбан пользователей, экономика"
    },
    7: {
        "name": "🔱 Хед-Мод",
        "color": "#ef5350",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets", "close_tickets",
                  "handle_reports", "view_moderation", "mute_users", "warn_users",
                  "view_media", "view_deleted", "ban_users", "unban_users",
                  "view_economy", "manage_plugins", "view_settings"],
        "desc": "Управление плагинами, настройки просмотра"
    },
    8: {
        "name": "💎 Администратор",
        "color": "#ab47bc",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets", "close_tickets",
                  "handle_reports", "view_moderation", "mute_users", "warn_users",
                  "view_media", "view_deleted", "ban_users", "unban_users",
                  "view_economy", "manage_plugins", "view_settings",
                  "edit_settings", "broadcast", "manage_chat_settings"],
        "desc": "Полное управление настройками и рассылки"
    },
    9: {
        "name": "👑 Со-Владелец",
        "color": "#ff7043",
        "perms": ["view_overview", "view_chats", "view_users", "view_reports",
                  "view_alerts", "view_tickets", "reply_tickets", "close_tickets",
                  "handle_reports", "view_moderation", "mute_users", "warn_users",
                  "view_media", "view_deleted", "ban_users", "unban_users",
                  "view_economy", "manage_plugins", "view_settings",
                  "edit_settings", "broadcast", "manage_chat_settings",
                  "manage_admins_view"],
        "desc": "Просмотр панели администраторов"
    },
    10: {
        "name": "🌟 Владелец",
        "color": "#ffd700",
        "perms": ["ALL"],
        "desc": "Полный доступ ко всему. Только вы."
    },
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
            (OWNER_TG_ID, "Владелец", 10, OWNER_TG_ID)
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
    if rank == 10:
        return True
    rank_perms = DASHBOARD_RANKS.get(rank, {}).get("perms", [])
    return "ALL" in rank_perms or perm in rank_perms


def _get_session(request) -> dict | None:
    token = request.cookies.get("dsess_token")
    return _dashboard_sessions.get(token)


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
    return HTML_BASE.format(body=body)


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
        {link("admins", "/dashboard/admins", "👑", "Администраторы", "view_overview") if rank == 10 else ""}
        {link("settings", "/dashboard/settings", "🔧", "Настройки", "view_settings")}
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

async def handle_login(request: web.Request):
    error = ""

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
            return web.Response(text=HTML_BASE.format(body=body), content_type="text/html")

        # Step 1: проверяем токен + tg_uid
        if token == DASHBOARD_TOKEN and tg_uid_str:
            try:
                tg_uid = int(tg_uid_str)
            except:
                error = "❌ Неверный Telegram ID"
                tg_uid = 0

            if tg_uid:
                admin = _get_admin(tg_uid)
                if not admin:
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
                    return web.Response(text=HTML_BASE.format(body=body), content_type="text/html")
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
    return web.Response(text=HTML_BASE.format(body=body), content_type="text/html")


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

    chats = await db.get_all_chats()
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

async def handle_admins(request: web.Request):
    sess = _get_session(request)
    if not sess or sess.get("rank") != 10:
        raise web.HTTPFound("/dashboard")
    _track_session(request)

    result_msg = ""

    if request.method == "POST":
        data = await request.post()
        action = data.get("action", "")
        tg_uid_str = data.get("tg_uid", "")
        name = data.get("name", "").strip()
        rank_str = data.get("rank", "1")

        try:
            tg_uid = int(tg_uid_str)
            rank = int(rank_str)
        except:
            tg_uid = 0
            rank = 1

        ip = request.headers.get("X-Forwarded-For", request.remote or "").split(",")[0].strip()

        if action == "grant" and tg_uid and name and 1 <= rank <= 9:
            _grant_admin(tg_uid, name, rank, OWNER_TG_ID)
            _log_admin_db(OWNER_TG_ID, "GRANT_ADMIN", f"Выдан ранг {rank} ({DASHBOARD_RANKS[rank]['name']}) для {name} ({tg_uid})", ip)
            result_msg = f"✅ Администратор {name} (ранг {rank}) добавлен!"
            if _bot:
                try:
                    await _bot.send_message(
                        tg_uid,
                        f"━━━━━━━━━━━━━━━\n"
                        f"👑 <b>ДОСТУП К ДАШБОРДУ</b>\n"
                        f"━━━━━━━━━━━━━━━\n\n"
                        f"Вам выдан доступ к панели управления CHAT GUARD!\n\n"
                        f"🎖 Ранг: <b>{DASHBOARD_RANKS[rank]['name']}</b>\n"
                        f"📋 Права: {DASHBOARD_RANKS[rank]['desc']}\n\n"
                        f"🔗 Войдите на дашборд, используя ваш Telegram ID.",
                        parse_mode="HTML"
                    )
                except:
                    pass
        elif action == "revoke" and tg_uid and tg_uid != OWNER_TG_ID:
            admin = _get_admin(tg_uid)
            if admin:
                _revoke_admin(tg_uid)
                _log_admin_db(OWNER_TG_ID, "REVOKE_ADMIN", f"Удалён администратор {admin['name']} ({tg_uid})", ip)
                result_msg = f"✅ Администратор {admin['name']} удалён"
                # Убиваем активные сессии
                for k, v in list(_dashboard_sessions.items()):
                    if v.get("uid") == tg_uid:
                        del _dashboard_sessions[k]
                if _bot:
                    try:
                        await _bot.send_message(tg_uid, "⚠️ Ваш доступ к дашборду был отозван.")
                    except:
                        pass
        elif action == "update_rank" and tg_uid and tg_uid != OWNER_TG_ID:
            admin = _get_admin(tg_uid)
            if admin and 1 <= rank <= 9:
                _grant_admin(tg_uid, admin["name"], rank, OWNER_TG_ID)
                _log_admin_db(OWNER_TG_ID, "UPDATE_RANK", f"Ранг {admin['name']} изменён на {rank}", ip)
                result_msg = f"✅ Ранг {admin['name']} изменён на {DASHBOARD_RANKS[rank]['name']}"
                # Обновляем активную сессию
                for k, v in _dashboard_sessions.items():
                    if v.get("uid") == tg_uid:
                        v["rank"] = rank
                if _bot:
                    try:
                        await _bot.send_message(
                            tg_uid,
                            f"🔄 Ваш ранг в дашборде изменён на: <b>{DASHBOARD_RANKS[rank]['name']}</b>",
                            parse_mode="HTML"
                        )
                    except:
                        pass

    admins = _get_all_admins()

    # Лог действий
    try:
        conn = db.get_conn()
        log_rows = conn.execute(
            "SELECT tg_uid, action, details, ip, ts FROM dashboard_admin_log ORDER BY ts DESC LIMIT 20"
        ).fetchall()
        conn.close()
        log_html = "".join(
            f"<tr><td style='font-size:11px;color:var(--text2);font-family:JetBrains Mono,monospace;'>{str(r['ts'])[:16]}</td>"
            f"<td><code>{r['tg_uid']}</code></td>"
            f"<td style='font-weight:600;'>{r['action']}</td>"
            f"<td style='color:var(--text2);font-size:12px;'>{r['details'][:60]}</td>"
            f"<td style='color:var(--text2);font-size:11px;'>{r['ip']}</td>"
            f"</tr>"
            for r in [dict(r) for r in log_rows]
        ) or "<tr><td colspan='5' class='empty-state'>Лог пуст</td></tr>"
    except:
        log_html = "<tr><td colspan='5' class='empty-state'>Ошибка</td></tr>"

    # Таблица администраторов
    admins_html = ""
    for a in admins:
        rank = a["rank"]
        rank_info = DASHBOARD_RANKS.get(rank, DASHBOARD_RANKS[1])
        is_owner = a["tg_uid"] == OWNER_TG_ID
        last = str(a.get("last_login") or "—")[:16]
        admins_html += f"""
        <tr>
          <td><code>{a['tg_uid']}</code></td>
          <td style="font-weight:700;">{a['name']}</td>
          <td>
            <span class="badge" style="background:rgba({_hex_to_rgb(rank_info['color'])},.15);color:{rank_info['color']};">
              {rank_info['name']}
            </span>
          </td>
          <td style="font-size:12px;color:var(--text2);">{rank_info['desc']}</td>
          <td style="font-size:11px;color:var(--text2);">{last}</td>
          <td>
            {"<span style='color:var(--gold);font-size:12px;'>🌟 Владелец</span>" if is_owner else f"""
            <div style="display:flex;gap:6px;flex-wrap:wrap;">
              <button onclick="openRankModal({a['tg_uid']}, '{a['name'].replace("'", "")}', {rank})"
                class="btn btn-xs btn-ghost">✏️ Ранг</button>
              <button onclick="revokeAdmin({a['tg_uid']}, '{a['name'].replace("'", "")}')"
                class="btn btn-xs" style="background:rgba(239,68,68,.1);color:var(--danger);">🗑 Убрать</button>
            </div>"""}
          </td>
        </tr>"""

    # Карточки рангов
    ranks_html = ""
    for r_id, r_info in sorted(DASHBOARD_RANKS.items()):
        if r_id == 10:
            continue
        perms_preview = ", ".join(r_info["perms"][:4])
        if len(r_info["perms"]) > 4:
            perms_preview += f" +{len(r_info['perms'])-4}"
        ranks_html += f"""
        <div class="rank-card">
          <div class="rank-badge" style="background:rgba({_hex_to_rgb(r_info['color'])},.15);color:{r_info['color']};">{r_id}</div>
          <div style="flex:1;">
            <div class="rank-name" style="color:{r_info['color']};">{r_info['name']}</div>
            <div class="rank-desc">{r_info['desc']}</div>
            <div class="rank-perms">{perms_preview}</div>
          </div>
        </div>"""

    result_html = f'<div style="padding:12px 16px;background:rgba(34,197,94,.1);border-radius:8px;margin-bottom:20px;color:var(--success);font-weight:600;">{result_msg}</div>' if result_msg else ""

    body = navbar(sess, "admins") + f"""
    <div class="container">
      <div class="page-title">👑 Управление администраторами
        <span style="font-size:13px;color:var(--gold);font-weight:600;margin-left:auto;">Только для вас</span>
      </div>
      {result_html}

      <!-- Добавить администратора -->
      <div class="grid-2" style="margin-bottom:20px;">
        <div class="section">
          <div class="section-header">➕ Добавить / обновить администратора</div>
          <div class="section-body">
            <form method="POST">
              <input type="hidden" name="action" value="grant">
              <div class="form-group">
                <label>Telegram ID</label>
                <input class="form-control" type="number" name="tg_uid" placeholder="123456789" required>
              </div>
              <div class="form-group">
                <label>Имя администратора</label>
                <input class="form-control" type="text" name="name" placeholder="Иван Иванов" required>
              </div>
              <div class="form-group">
                <label>Ранг (1-9)</label>
                <select class="form-control" name="rank">
                  {''.join(f'<option value="{r_id}">{r_id} — {r_info["name"]} — {r_info["desc"]}</option>' for r_id, r_info in sorted(DASHBOARD_RANKS.items()) if r_id < 10)}
                </select>
              </div>
              <button class="btn btn-primary" type="submit" style="width:100%;">
                ✅ Добавить / Обновить
              </button>
            </form>
          </div>
        </div>

        <div class="section">
          <div class="section-header">📋 Описание рангов</div>
          <div style="padding:12px;display:flex;flex-direction:column;gap:8px;max-height:380px;overflow-y:auto;">
            {ranks_html}
          </div>
        </div>
      </div>

      <!-- Список администраторов -->
      <div class="section" style="margin-bottom:20px;">
        <div class="section-header">
          👥 Все администраторы ({len(admins)})
          <span style="font-size:12px;color:var(--text2);">Ранг 10 = Владелец (только вы)</span>
        </div>
        <table>
          <thead><tr><th>TG ID</th><th>Имя</th><th>Ранг</th><th>Описание</th><th>Последний вход</th><th>Действия</th></tr></thead>
          <tbody>{admins_html or "<tr><td colspan='6' class='empty-state'>Нет администраторов</td></tr>"}</tbody>
        </table>
      </div>

      <!-- Лог действий -->
      <div class="section">
        <div class="section-header">📜 Лог действий</div>
        <table>
          <thead><tr><th>Время</th><th>TG ID</th><th>Действие</th><th>Детали</th><th>IP</th></tr></thead>
          <tbody>{log_html}</tbody>
        </table>
      </div>
    </div>

    <!-- Modal: Изменить ранг -->
    <div class="modal-overlay" id="rankModal">
      <div class="modal">
        <div class="modal-title">✏️ Изменить ранг</div>
        <form method="POST" id="rankForm">
          <input type="hidden" name="action" value="update_rank">
          <input type="hidden" name="tg_uid" id="rankUid">
          <div class="form-group">
            <label>Администратор</label>
            <input class="form-control" id="rankName" readonly>
          </div>
          <div class="form-group">
            <label>Новый ранг</label>
            <select class="form-control" name="rank" id="rankSelect">
              {''.join(f'<option value="{r_id}">{r_id} — {r_info["name"]}</option>' for r_id, r_info in sorted(DASHBOARD_RANKS.items()) if r_id < 10)}
            </select>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-ghost" onclick="closeModal('rankModal')">Отмена</button>
            <button type="submit" class="btn btn-primary">✅ Сохранить</button>
          </div>
        </form>
      </div>
    </div>

    <!-- Modal: Подтверждение удаления -->
    <form method="POST" id="revokeForm">
      <input type="hidden" name="action" value="revoke">
      <input type="hidden" name="tg_uid" id="revokeUid">
    </form>

    <script>
    function openRankModal(uid, name, currentRank){{
      document.getElementById('rankUid').value = uid;
      document.getElementById('rankName').value = name + ' (' + uid + ')';
      document.getElementById('rankSelect').value = currentRank;
      openModal('rankModal');
    }}
    function revokeAdmin(uid, name){{
      if(confirm('Удалить администратора ' + name + ' (' + uid + ')? Это действие нельзя отменить.')){{
        document.getElementById('revokeUid').value = uid;
        document.getElementById('revokeForm').submit();
      }}
    }}
    </script>
    """ + close_main()
    return web.Response(text=page(body), content_type="text/html")


# ══════════════════════════════════════════
#  ЧАТЫ
# ══════════════════════════════════════════

async def handle_chats(request: web.Request):
    sess = _get_session(request)
    _track_session(request)
    chats = await db.get_all_chats()

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
        top_users = conn.execute("SELECT uid, msg_count FROM chat_stats WHERE cid=? ORDER BY msg_count DESC LIMIT 10", (cid,)).fetchall()
    except:
        top_users = []
    try:
        top_rep = conn.execute("SELECT uid, score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT 10", (cid,)).fetchall()
    except:
        top_rep = []
    try:
        bans = conn.execute("SELECT uid FROM ban_list WHERE cid=?", (cid,)).fetchall()
    except:
        bans = []
    try:
        mod_hist = conn.execute("SELECT action, reason, by_name, created_at FROM mod_history WHERE cid=? ORDER BY created_at DESC LIMIT 20", (cid,)).fetchall()
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
    chats_rows = conn.execute("SELECT k.cid, k.title, cs.msg_count FROM chat_stats cs LEFT JOIN known_chats k ON cs.cid=k.cid WHERE cs.uid=? ORDER BY cs.msg_count DESC", (uid,)).fetchall()
    rep_rows = conn.execute("SELECT cid, score FROM reputation WHERE uid=? ORDER BY score DESC", (uid,)).fetchall()
    warn_rows = conn.execute("SELECT cid, count FROM warnings WHERE uid=? AND count>0", (uid,)).fetchall()
    try:
        hist_rows = conn.execute("SELECT action, reason, by_name, created_at, cid FROM mod_history WHERE uid=? ORDER BY created_at DESC LIMIT 20", (uid,)).fetchall()
    except:
        hist_rows = []
    try:
        ticket_rows = conn.execute("SELECT id, subject, status, created_at FROM tickets WHERE uid=? ORDER BY created_at DESC LIMIT 5", (uid,)).fetchall()
    except:
        ticket_rows = []
    try:
        notes_rows = conn.execute("SELECT text, by_name, created_at FROM mod_notes WHERE uid=? ORDER BY created_at DESC LIMIT 10", (uid,)).fetchall()
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
    if can_ban:
        action_btns += f'<button class="btn btn-sm btn-danger" onclick="modAction(\'ban\',{uid},{first_cid},\'Нарушение\',function(){{showToast(\'Забанен\',\'success\');}})">🔨 Бан</button>'
        action_btns += f'<button class="btn btn-sm btn-success" onclick="modAction(\'unban\',{uid},{first_cid},\'Разбан\')">🕊 Разбан</button>'
    if can_mute:
        action_btns += f'<button class="btn btn-sm btn-warn" onclick="modAction(\'mute\',{uid},{first_cid},\'Мут 1ч\')">🔇 Мут</button>'
    if can_warn:
        action_btns += f'<button class="btn btn-sm btn-ghost" onclick="modAction(\'warn\',{uid},{first_cid},\'Нарушение\')">⚡ Варн</button>'

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
    tickets = await db.ticket_list(status=status_filter, limit=50)
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
    t = await db.ticket_get(ticket_id)
    if not t:
        raise web.HTTPNotFound()
    msgs = await db.ticket_msgs(ticket_id)
    t = dict(t)

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
        t = await db.ticket_get(ticket_id)
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
    t = await db.ticket_get(ticket_id)
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
    chats = await db.get_all_chats()
    chat_opts = "".join(f'<option value="{c["cid"]}">{c.get("title","") or c["cid"]}</option>' for c in chats)

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
                {"<button class='btn btn-danger btn-sm' onclick='qaAction(\"ban\")'>🔨 Бан</button><button class='btn btn-success btn-sm' onclick='qaAction(\"unban\")'>🕊 Разбан</button>" if can_ban else ""}
                {"<button class='btn btn-warn btn-sm' onclick='qaAction(\"mute\")'>🔇 Мут 1ч</button>" if can_mute else ""}
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
        if a.get("uid") and a.get("cid") and _bot:
            uid_a, cid_a = a["uid"], a["cid"]
            if _has_perm(token, "ban_users"):
                action_btns += f'<button class="btn btn-xs btn-danger" onclick="modAction(\'ban\',{uid_a},{cid_a},\'Алерт: {a["title"].replace(chr(39),"")}\')">🔨 Бан</button> '
            if _has_perm(token, "mute_users"):
                action_btns += f'<button class="btn btn-xs btn-warn" onclick="modAction(\'mute\',{uid_a},{cid_a},\'Алерт\')">🔇 Мут</button> '
        items += f"""
        <div class="{cls}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div>
              <div style="font-weight:700;">{a['title']}</div>
              <div style="font-size:12px;color:var(--text2);margin-top:3px;">{a['desc']}</div>
              {f'<div style="font-size:11px;color:var(--text2);margin-top:3px;">CID: {a["cid"]} · UID: {a["uid"]}</div>' if a.get('uid') else ''}
              {f'<div style="margin-top:6px;display:flex;gap:6px;">{action_btns}</div>' if action_btns else ''}
            </div>
            <span style="font-size:11px;color:var(--text2);white-space:nowrap;margin-left:12px;">{a['time']}</span>
          </div>
        </div>"""

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
    chats = await db.get_all_chats()

    conn = db.get_conn()
    try:
        if cid_filter:
            rows = conn.execute("SELECT * FROM deleted_log WHERE cid=? ORDER BY ts DESC LIMIT 100", (cid_filter,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM deleted_log ORDER BY ts DESC LIMIT 100").fetchall()
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
    chats = await db.get_all_chats()

    conn = db.get_conn()
    if cid_filter:
        top_rep = conn.execute("SELECT uid, score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT 20", (cid_filter,)).fetchall()
        top_xp  = conn.execute("SELECT uid, xp FROM xp_data WHERE cid=? ORDER BY xp DESC LIMIT 20", (cid_filter,)).fetchall()
    else:
        top_rep = conn.execute("SELECT uid, SUM(score) as score FROM reputation GROUP BY uid ORDER BY score DESC LIMIT 20").fetchall()
        top_xp  = conn.execute("SELECT uid, SUM(xp) as xp FROM xp_data GROUP BY uid ORDER BY xp DESC LIMIT 20").fetchall()
    try:
        act_rows = conn.execute("SELECT day, SUM(count) as total FROM user_activity " + ("WHERE cid=? " if cid_filter else "") + "GROUP BY day ORDER BY day DESC LIMIT 14", *((cid_filter,) if cid_filter else ())).fetchall()
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
        daily = conn.execute("SELECT day, SUM(count) as total FROM user_activity GROUP BY day ORDER BY day DESC LIMIT 30").fetchall()
        top_words = conn.execute("SELECT word, SUM(count) as cnt FROM word_stats GROUP BY word ORDER BY cnt DESC LIMIT 20").fetchall() if False else []
    except:
        daily = []
        top_words = []
    conn.close()

    daily_data = [dict(r) for r in daily][::-1]
    d_labels = json.dumps([str(r.get("day", ""))[-5:] for r in daily_data])
    d_values = json.dumps([r.get("total", 0) for r in daily_data])

    chats = await db.get_all_chats()
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
    chats = await db.get_all_chats()
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
    chats = await db.get_all_chats()

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
    chats = await db.get_all_chats()
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
        return f"""
        <div class="toggle-wrap">
          <div class="toggle-info"><b>{label}</b>{f'<span>{desc}</span>' if desc else ''}</div>
          <label class="toggle-switch">
            <input type="checkbox" name="{key}" value="1" {"checked" if val else ""}>
            <span class="toggle-slider"></span>
          </label>
        </div>"""

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
        return f"""
        <div class="toggle-wrap">
          <div class="toggle-info"><b>{label}</b>{f'<span>{desc}</span>' if desc else ''}</div>
          <label class="toggle-switch">
            <input type="checkbox" name="{key}" value="1" {"checked" if val else ""}>
            <span class="toggle-slider"></span>
          </label>
        </div>"""

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
        rows = conn.execute("SELECT hour, SUM(count) as total FROM hourly_stats GROUP BY hour").fetchall()
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
            rows = conn.execute("SELECT DISTINCT uid FROM chat_stats WHERE uid=? LIMIT 5", (int(q),)).fetchall()
        else:
            rows = []
        for r in rows:
            uid = r["uid"]
            name = shared.online_users.get(uid, {}).get("name") or f"User {uid}"
            users.append({"uid": uid, "name": name})
    except:
        pass

    try:
        trows = conn.execute("SELECT id, subject FROM tickets WHERE subject LIKE ? OR CAST(id AS TEXT)=? LIMIT 5", (f"%{q}%", q)).fetchall()
        tickets = [{"id": r["id"], "subject": r["subject"]} for r in trows]
    except:
        pass

    try:
        crows = conn.execute("SELECT cid, title FROM known_chats WHERE LOWER(title) LIKE ? OR CAST(cid AS TEXT)=? LIMIT 5", (f"%{q}%", q)).fetchall()
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
        rows = conn.execute("SELECT k.title, cs.uid, cs.msg_count, COALESCE(r.score,0) as rep, COALESCE(w.count,0) as warns FROM chat_stats cs LEFT JOIN known_chats k ON cs.cid=k.cid LEFT JOIN reputation r ON cs.cid=r.cid AND cs.uid=r.uid LEFT JOIN warnings w ON cs.cid=w.cid AND cs.uid=w.uid ORDER BY cs.msg_count DESC LIMIT 1000").fetchall()
        conn.close()
        csv = "Чат,UserID,Сообщений,Репутация,Варны\n" + "".join(f"{r['title'] or ''},{r['uid']},{r['msg_count']},{r['rep']},{r['warns']}\n" for r in rows)
    elif export_type == "bans":
        rows = conn.execute("SELECT k.title, b.uid FROM ban_list b LEFT JOIN known_chats k ON b.cid=k.cid").fetchall()
        conn.close()
        csv = "Чат,UserID\n" + "".join(f"{r['title'] or ''},{r['uid']}\n" for r in rows)
    elif export_type == "modhistory":
        rows = conn.execute("SELECT k.title, m.uid, m.action, m.reason, m.by_name, m.created_at FROM mod_history m LEFT JOIN known_chats k ON m.cid=k.cid ORDER BY m.created_at DESC LIMIT 2000").fetchall()
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

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"✅ Dashboard v2.0 запущен на :{port}")
    return runner

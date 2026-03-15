"""
dashboard.py — Веб-панель управления на FastAPI
Запускается вместе с ботом на порту 8080
Доступ: http://your-server:8080/dashboard
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
from functools import wraps

from aiohttp import web
import database as db

log = logging.getLogger(__name__)

# Секретный токен для доступа к дашборду
# Устанавливается в .env: DASHBOARD_TOKEN=your_secret_token
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "changeme123")

# Глобальная ссылка на бот (устанавливается из main)
_bot = None
_admin_ids = set()


def set_bot(bot, admin_ids: set):
    global _bot, _admin_ids
    _bot = bot
    _admin_ids = admin_ids


# ══════════════════════════════════════════
#  HTML ШАБЛОНЫ
# ══════════════════════════════════════════

HTML_BASE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CHAT GUARD — Dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0f0f13;
    color: #e0e0e0;
    min-height: 100vh;
  }}
  .navbar {{
    background: #1a1a2e;
    padding: 16px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #2a2a4a;
    position: sticky; top: 0; z-index: 100;
  }}
  .navbar .brand {{
    font-size: 20px;
    font-weight: 700;
    color: #7c6fcd;
    letter-spacing: 1px;
  }}
  .navbar nav a {{
    color: #9090b0;
    text-decoration: none;
    margin-left: 20px;
    font-size: 14px;
    transition: color .2s;
  }}
  .navbar nav a:hover {{ color: #fff; }}
  .container {{
    max-width: 1400px;
    margin: 0 auto;
    padding: 24px;
  }}
  .page-title {{
    font-size: 24px;
    font-weight: 700;
    margin-bottom: 24px;
    color: #fff;
  }}
  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
  }}
  .card {{
    background: #1a1a2e;
    border-radius: 12px;
    padding: 20px;
    border: 1px solid #2a2a4a;
  }}
  .card .label {{
    font-size: 12px;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 8px;
  }}
  .card .value {{
    font-size: 32px;
    font-weight: 700;
    color: #7c6fcd;
  }}
  .card .sub {{
    font-size: 12px;
    color: #666;
    margin-top: 4px;
  }}
  .section {{
    background: #1a1a2e;
    border-radius: 12px;
    border: 1px solid #2a2a4a;
    margin-bottom: 24px;
    overflow: hidden;
  }}
  .section-header {{
    padding: 16px 20px;
    border-bottom: 1px solid #2a2a4a;
    font-weight: 600;
    font-size: 15px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
  }}
  th, td {{
    padding: 12px 20px;
    text-align: left;
    border-bottom: 1px solid #1f1f35;
    font-size: 14px;
  }}
  th {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #666;
    background: #161625;
  }}
  tr:hover td {{ background: #1f1f35; }}
  .badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
  }}
  .badge-open     {{ background: #1a3a1a; color: #4caf50; }}
  .badge-progress {{ background: #2a2a0a; color: #ffc107; }}
  .badge-closed   {{ background: #1a1a3a; color: #9090cc; }}
  .badge-urgent   {{ background: #3a1a1a; color: #f44336; }}
  .badge-high     {{ background: #3a2a1a; color: #ff9800; }}
  .badge-normal   {{ background: #1a2a1a; color: #8bc34a; }}
  .badge-low      {{ background: #1a2a2a; color: #00bcd4; }}
  .btn {{
    display: inline-block;
    padding: 6px 14px;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 600;
    text-decoration: none;
    border: none;
    cursor: pointer;
    transition: opacity .2s;
  }}
  .btn:hover {{ opacity: .8; }}
  .btn-primary {{ background: #7c6fcd; color: #fff; }}
  .btn-danger  {{ background: #c0392b; color: #fff; }}
  .btn-success {{ background: #27ae60; color: #fff; }}
  .btn-sm {{ padding: 4px 10px; font-size: 12px; }}
  .form-group {{ margin-bottom: 16px; }}
  .form-group label {{ display: block; margin-bottom: 6px; font-size: 13px; color: #999; }}
  .form-control {{
    width: 100%;
    padding: 10px 14px;
    background: #0f0f1a;
    border: 1px solid #2a2a4a;
    border-radius: 8px;
    color: #e0e0e0;
    font-size: 14px;
  }}
  .form-control:focus {{ outline: none; border-color: #7c6fcd; }}
  .login-wrap {{
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
  }}
  .login-box {{
    background: #1a1a2e;
    border: 1px solid #2a2a4a;
    border-radius: 16px;
    padding: 40px;
    width: 380px;
  }}
  .login-box h2 {{
    text-align: center;
    font-size: 22px;
    margin-bottom: 24px;
    color: #7c6fcd;
  }}
  .chat-bubble {{
    padding: 10px 14px;
    border-radius: 10px;
    margin-bottom: 8px;
    max-width: 85%;
    font-size: 14px;
    line-height: 1.5;
  }}
  .bubble-user {{ background: #1f1f35; margin-right: auto; }}
  .bubble-mod  {{ background: #1a2e1a; margin-left: auto; text-align: right; }}
  .bubble-meta {{ font-size: 11px; color: #666; margin-bottom: 3px; }}
  .empty-state {{
    text-align: center;
    padding: 40px;
    color: #555;
  }}
  .search-box {{
    padding: 20px;
    border-bottom: 1px solid #2a2a4a;
  }}
  .search-box input {{
    width: 100%;
    padding: 10px 14px;
    background: #0f0f1a;
    border: 1px solid #2a2a4a;
    border-radius: 8px;
    color: #e0e0e0;
    font-size: 14px;
  }}
  @media (max-width: 768px) {{
    .container {{ padding: 12px; }}
    .cards {{ grid-template-columns: 1fr 1fr; }}
    table {{ font-size: 12px; }}
    th, td {{ padding: 8px 12px; }}
  }}
</style>
</head>
<body>
{body}
</body>
</html>"""


def page(body: str) -> str:
    return HTML_BASE.format(body=body)


def navbar(active: str = "") -> str:
    links = [
        ("overview", "/dashboard", "📊 Обзор"),
        ("chats", "/dashboard/chats", "💬 Чаты"),
        ("tickets", "/dashboard/tickets", "🎫 Тикеты"),
        ("moderation", "/dashboard/moderation", "🛡 Модерация"),
        ("users", "/dashboard/users", "👥 Участники"),
    ]
    nav_items = "".join(
        f'<a href="{url}" style="{"color:#fff;font-weight:600;" if k==active else ""}">{label}</a>'
        for k, url, label in links
    )
    return f"""
    <div class="navbar">
      <span class="brand">⚡ CHAT GUARD</span>
      <nav>
        {nav_items}
        <a href="/dashboard/logout" style="color:#c0392b;">🚪 Выход</a>
      </nav>
    </div>"""


# ══════════════════════════════════════════
#  AUTH MIDDLEWARE
# ══════════════════════════════════════════

def require_auth(handler):
    @wraps(handler)
    async def wrapper(request: web.Request):
        token = request.cookies.get("dtoken") or request.rel_url.query.get("token")
        if token != DASHBOARD_TOKEN:
            raise web.HTTPFound("/dashboard/login")
        return await handler(request)
    return wrapper


# ══════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════

async def handle_login(request: web.Request):
    if request.method == "POST":
        data = await request.post()
        token = data.get("token", "")
        if token == DASHBOARD_TOKEN:
            response = web.HTTPFound("/dashboard")
            response.set_cookie("dtoken", token, max_age=86400 * 7, httponly=True)
            raise response
        error = '<p style="color:#f44336;text-align:center;margin-top:12px;">Неверный токен</p>'
    else:
        error = ""

    body = navbar() + f"""
    <div class="login-wrap">
      <div class="login-box">
        <h2>⚡ CHAT GUARD</h2>
        <p style="text-align:center;color:#666;margin-bottom:24px;">Панель управления</p>
        <form method="POST">
          <div class="form-group">
            <label>Токен доступа</label>
            <input class="form-control" type="password" name="token" placeholder="Введи токен...">
          </div>
          <button class="btn btn-primary" style="width:100%;padding:12px;" type="submit">
            Войти
          </button>
        </form>
        {error}
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


async def handle_logout(request: web.Request):
    response = web.HTTPFound("/dashboard/login")
    response.del_cookie("dtoken")
    raise response


@require_auth
async def handle_overview(request: web.Request):
    # Собираем статистику
    chats = await db.get_all_chats()
    ticket_stats = await db.ticket_stats_all()

    # Общие числа
    total_chats = len(chats)

    conn = db.get_conn()
    total_users = conn.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats").fetchone()[0] or 0
    total_msgs  = conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats").fetchone()[0] or 0
    total_bans  = conn.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    total_warns = conn.execute("SELECT COALESCE(SUM(count),0) FROM warnings").fetchone()[0] or 0
    conn.close()
    # mod_history хранится как JSON {cid: {uid: [{action, reason, by, time}]}}
    import json as _json
    recent_mods = []
    recent_acts = []
    try:
        conn2 = db.get_conn()
        rows_mh = conn2.execute("SELECT cid, uid, history FROM mod_history").fetchall()
        conn2.close()
        mod_counts = {}
        all_acts = []
        for r in rows_mh:
            try:
                hist = _json.loads(r["history"]) if r["history"] else []
                for h in hist:
                    by = h.get("by", "—")
                    mod_counts[by] = mod_counts.get(by, 0) + 1
                    all_acts.append({
                        "action": h.get("action", "—"),
                        "reason": h.get("reason", "—"),
                        "by_name": by,
                        "created_at": h.get("time", "—"),
                    })
            except: pass
        recent_mods = [{"by_name": k, "cnt": v} for k, v in
                       sorted(mod_counts.items(), key=lambda x: -x[1])[:5]]
        recent_acts = sorted(all_acts, key=lambda x: x.get("created_at",""), reverse=True)[:10]
    except: pass

    cards = f"""
    <div class="cards">
      <div class="card">
        <div class="label">💬 Чатов</div>
        <div class="value">{total_chats}</div>
        <div class="sub">активных чатов</div>
      </div>
      <div class="card">
        <div class="label">👥 Участников</div>
        <div class="value">{total_users:,}</div>
        <div class="sub">уникальных юзеров</div>
      </div>
      <div class="card">
        <div class="label">💬 Сообщений</div>
        <div class="value">{total_msgs:,}</div>
        <div class="sub">всего обработано</div>
      </div>
      <div class="card">
        <div class="label">🔨 Банов</div>
        <div class="value">{total_bans}</div>
        <div class="sub">активных банов</div>
      </div>
      <div class="card">
        <div class="label">⚡ Варнов</div>
        <div class="value">{total_warns}</div>
        <div class="sub">активных варнов</div>
      </div>
      <div class="card">
        <div class="label">🎫 Тикетов</div>
        <div class="value">{ticket_stats['open']}</div>
        <div class="sub">открытых / {ticket_stats['total']} всего</div>
      </div>
    </div>"""

    # Топ модераторов
    mod_rows = "".join(
        f"<tr><td>👮 {r['by_name']}</td><td>{r['cnt']} действий</td></tr>"
        for r in recent_mods
    ) or "<tr><td colspan='2' class='empty-state'>Нет данных</td></tr>"

    # Последние действия
    act_rows = "".join(
        f"<tr>"
        f"<td>{r['created_at'][:16].replace('T',' ') if r['created_at'] else '—'}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '—'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in recent_acts
    ) or "<tr><td colspan='4' class='empty-state'>Нет действий</td></tr>"

    body = navbar("overview") + f"""
    <div class="container">
      <div class="page-title">📊 Обзор</div>
      {cards}
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;">
        <div class="section">
          <div class="section-header">👮 Топ модераторов</div>
          <table>
            <thead><tr><th>Модератор</th><th>Действий</th></tr></thead>
            <tbody>{mod_rows}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">⚡ Последние действия</div>
          <table>
            <thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Кто</th></tr></thead>
            <tbody>{act_rows}</tbody>
          </table>
        </div>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_chats(request: web.Request):
    chats = await db.get_all_chats()

    rows = ""
    for chat in chats:
        cid   = chat["cid"]
        title = chat["title"] or "Без названия"
        c2 = db.get_conn()
        msgs  = c2.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        users = c2.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats WHERE cid=?", (cid,)).fetchone()[0] or 0
        warns = c2.execute("SELECT COALESCE(SUM(count),0) FROM warnings WHERE cid=?", (cid,)).fetchone()[0] or 0
        bans  = c2.execute("SELECT COUNT(*) FROM ban_list WHERE cid=?", (cid,)).fetchone()[0] or 0
        c2.close()
        rows += (
            f"<tr>"
            f"<td><code>{cid}</code></td>"
            f"<td><b>{title}</b></td>"
            f"<td>{users:,}</td>"
            f"<td>{msgs:,}</td>"
            f"<td>{warns}</td>"
            f"<td>{bans}</td>"
            f"<td>"
            f"<a class='btn btn-sm btn-primary' href='/dashboard/chats/{cid}'>Подробнее</a>"
            f"</td>"
            f"</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='7' class='empty-state'>Нет чатов</td></tr>"

    body = navbar("chats") + f"""
    <div class="container">
      <div class="page-title">💬 Чаты</div>
      <div class="section">
        <div class="section-header">Все чаты ({len(chats)})</div>
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Название</th><th>Участников</th>
              <th>Сообщений</th><th>Варнов</th><th>Банов</th><th>Действия</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_chat_detail(request: web.Request):
    cid = int(request.match_info["cid"])

    c3 = db.get_conn()
    chat_row = c3.execute("SELECT title FROM known_chats WHERE cid=?", (cid,)).fetchone()
    title    = chat_row["title"] if chat_row else str(cid)
    top_users = c3.execute(
        "SELECT uid, msg_count FROM chat_stats WHERE cid=? ORDER BY msg_count DESC LIMIT 10", (cid,)
    ).fetchall()
    top_rep = c3.execute(
        "SELECT uid, score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT 10", (cid,)
    ).fetchall()
    bans = c3.execute(
        "SELECT uid, name, reason, banned_by, banned_at FROM ban_list WHERE cid=?", (cid,)
    ).fetchall()
    mod_hist = c3.execute(
        "SELECT action, reason, by_name, created_at FROM mod_history "
        "WHERE cid=? ORDER BY created_at DESC LIMIT 20", (cid,)
    ).fetchall()
    c3.close()
    hours = await db.get_hourly_totals(cid)

    # Хитмап активности
    max_h = max(hours.values(), default=1)
    heatmap = ""
    for h in range(24):
        val = hours.get(h, 0)
        pct = int((val / max_h) * 100) if max_h else 0
        heatmap += (
            f'<div style="display:inline-block;width:3.8%;vertical-align:bottom;'
            f'height:{max(4, pct)}px;background:#7c6fcd;opacity:{max(0.2, pct/100):.2f};'
            f'margin:1px;border-radius:2px;" title="{h}:00 — {val} сообщ."></div>'
        )

    def user_rows(rows):
        result = ""
        for i, r in enumerate(rows, 1):
            result += f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r[rows[0].keys()[2]]:,}</td></tr>"
        return result or "<tr><td colspan='3'>Нет данных</td></tr>"

    top_u_rows = "".join(
        f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r['msg_count']:,}</td></tr>"
        for i, r in enumerate(top_users, 1)
    ) or "<tr><td colspan='3'>Нет данных</td></tr>"

    top_r_rows = "".join(
        f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r['score']:+d}</td></tr>"
        for i, r in enumerate(top_rep, 1)
    ) or "<tr><td colspan='3'>Нет данных</td></tr>"

    ban_rows = "".join(
        f"<tr>"
        f"<td><code>{r['uid']}</code></td>"
        f"<td>{r['name'] or '—'}</td>"
        f"<td>{r['reason'] or '—'}</td>"
        f"<td>{r['banned_by'] or '—'}</td>"
        f"<td>{r['banned_at'].strftime('%d.%m.%Y') if r['banned_at'] else '—'}</td>"
        f"</tr>"
        for r in bans
    ) or "<tr><td colspan='5'>Нет банов</td></tr>"

    hist_rows = "".join(
        f"<tr>"
        f"<td>{r['created_at'][:16].replace('T',' ') if r['created_at'] else '—'}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '—'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in mod_hist
    ) or "<tr><td colspan='4'>Нет действий</td></tr>"

    body = navbar("chats") + f"""
    <div class="container">
      <div class="page-title">💬 {title}</div>
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">📊 Активность по часам</div>
        <div style="padding:16px;overflow-x:auto;">
          <div style="min-width:300px;">
            {heatmap}
          </div>
          <div style="font-size:11px;color:#666;margin-top:8px;">
            0:00 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            6:00 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            12:00 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            18:00 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            23:00
          </div>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:24px;">
        <div class="section">
          <div class="section-header">🏆 Топ активных</div>
          <table>
            <thead><tr><th>#</th><th>ID</th><th>Сообщений</th></tr></thead>
            <tbody>{top_u_rows}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">⭐ Топ репутации</div>
          <table>
            <thead><tr><th>#</th><th>ID</th><th>Репутация</th></tr></thead>
            <tbody>{top_r_rows}</tbody>
          </table>
        </div>
      </div>
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">🔨 Список банов ({len(bans)})</div>
        <table>
          <thead><tr><th>ID</th><th>Имя</th><th>Причина</th><th>Кто</th><th>Дата</th></tr></thead>
          <tbody>{ban_rows}</tbody>
        </table>
      </div>
      <div class="section">
        <div class="section-header">📋 История модерации</div>
        <table>
          <thead><tr><th>Время</th><th>Действие</th><th>Причина</th><th>Модератор</th></tr></thead>
          <tbody>{hist_rows}</tbody>
        </table>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_tickets(request: web.Request):
    status_filter = request.rel_url.query.get("status", "open")
    tickets = await db.ticket_list(status=status_filter, limit=50)
    stats   = await db.ticket_stats_all()

    status_tabs = "".join(
        f'<a href="?status={s}" class="btn btn-sm {"btn-primary" if status_filter==s else ""}" '
        f'style="margin-right:8px;">'
        f'{label} ({stats[key]})</a>'
        for s, key, label in [
            ("open", "open", "🆕 Открытые"),
            ("in_progress", "in_progress", "🔄 В работе"),
            ("closed", "closed", "✅ Закрытые"),
        ]
    )

    rows = ""
    for t in [dict(x) for x in tickets]:
        pri_badge = f'<span class="badge badge-{t["priority"]}">{t["priority"]}</span>'
        st_badge  = f'<span class="badge badge-{"open" if t["status"]=="open" else ("progress" if t["status"]=="in_progress" else "closed")}">{t["status"]}</span>'
        dt = t["created_at"].strftime("%d.%m %H:%M") if t.get("created_at") else "—"
        rows += (
            f"<tr>"
            f"<td>#{t['id']}</td>"
            f"<td>{t['user_name'] or 'Аноним'}<br><small style='color:#666'>{t['chat_title'] or '—'}</small></td>"
            f"<td>{t['subject'][:40]}</td>"
            f"<td>{st_badge}</td>"
            f"<td>{pri_badge}</td>"
            f"<td>{t['assigned_mod'] or '—'}</td>"
            f"<td>{dt}</td>"
            f"<td><a class='btn btn-sm btn-primary' href='/dashboard/tickets/{t['id']}'>Открыть</a></td>"
            f"</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='8' class='empty-state'>Тикетов нет</td></tr>"

    body = navbar("tickets") + f"""
    <div class="container">
      <div class="page-title" style="display:flex;justify-content:space-between;align-items:center;">
        <span>🎫 Тикеты</span>
        <div style="font-size:14px;color:#666;">
          Всего: {stats['total']}
        </div>
      </div>
      <div style="margin-bottom:20px;">{status_tabs}</div>
      <div class="section">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Пользователь</th><th>Тема</th>
              <th>Статус</th><th>Приоритет</th><th>Модератор</th><th>Создан</th><th></th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_ticket_detail(request: web.Request):
    ticket_id = int(request.match_info["ticket_id"])
    t    = await db.ticket_get(ticket_id)
    if not t:
        raise web.HTTPNotFound()
    msgs = await db.ticket_msgs(ticket_id)
    t    = dict(t)

    pri_badge = f'<span class="badge badge-{t["priority"]}">{t["priority"]}</span>'
    stat      = t["status"]
    st_label  = {"open": "🆕 Открыт", "in_progress": "🔄 В работе", "closed": "✅ Закрыт"}.get(stat, stat)
    dt_open   = t["created_at"].strftime("%d.%m.%Y %H:%M") if t.get("created_at") else "—"
    dt_upd    = t["updated_at"].strftime("%d.%m.%Y %H:%M") if t.get("updated_at") else "—"

    bubbles = ""
    for m in [dict(x) for x in msgs]:
        is_mod = m["is_mod"]
        who    = f"👮 {m['sender_name']}" if is_mod else f"👤 {m['sender_name']}"
        when   = m["sent_at"].strftime("%d.%m.%Y %H:%M") if m.get("sent_at") else ""
        cls    = "bubble-mod" if is_mod else "bubble-user"
        bubbles += (
            f'<div style="{"text-align:right;" if is_mod else ""}">'
            f'<div class="chat-bubble {cls}">'
            f'<div class="bubble-meta">{who} · {when}</div>'
            f'{m["text"]}'
            f'</div></div>'
        )

    if not bubbles:
        bubbles = '<div class="empty-state">Сообщений нет</div>'

    # Форма ответа (только если тикет открыт)
    reply_form = ""
    if stat != "closed":
        reply_form = f"""
        <div class="section" style="margin-top:24px;">
          <div class="section-header">✏️ Ответить</div>
          <div style="padding:20px;">
            <form method="POST" action="/dashboard/tickets/{ticket_id}/reply">
              <div class="form-group">
                <textarea class="form-control" name="text" rows="4"
                  placeholder="Введи ответ..."></textarea>
              </div>
              <div style="display:flex;gap:12px;">
                <button class="btn btn-primary" type="submit">Отправить</button>
                <a class="btn btn-danger" href="/dashboard/tickets/{ticket_id}/close">
                  Закрыть тикет
                </a>
              </div>
            </form>
          </div>
        </div>"""

    body = navbar("tickets") + f"""
    <div class="container">
      <div class="page-title">
        🎫 Тикет #{ticket_id}
        <a href="/dashboard/tickets" style="font-size:14px;color:#7c6fcd;margin-left:16px;">← Назад</a>
      </div>
      <div style="display:grid;grid-template-columns:1fr 320px;gap:24px;">
        <div>
          <div class="section">
            <div class="section-header">💬 Переписка</div>
            <div style="padding:16px;max-height:500px;overflow-y:auto;">
              {bubbles}
            </div>
          </div>
          {reply_form}
        </div>
        <div>
          <div class="section">
            <div class="section-header">ℹ️ Информация</div>
            <div style="padding:16px;font-size:14px;line-height:2;">
              <div><b>Статус:</b> {st_label}</div>
              <div><b>Приоритет:</b> {pri_badge}</div>
              <div><b>Пользователь:</b> {t['user_name'] or 'Аноним'}</div>
              <div><b>Чат:</b> {t['chat_title'] or '—'}</div>
              <div><b>Тема:</b> {t['subject']}</div>
              <div><b>Модератор:</b> {t['assigned_mod'] or 'Не назначен'}</div>
              <div><b>Создан:</b> {dt_open}</div>
              <div><b>Обновлён:</b> {dt_upd}</div>
            </div>
          </div>
          <div style="margin-top:12px;">
            {'<a class="btn btn-danger" style="width:100%;text-align:center;display:block;" href="/dashboard/tickets/' + str(ticket_id) + '/close">✅ Закрыть тикет</a>' if stat != 'closed' else ''}
          </div>
        </div>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_ticket_reply(request: web.Request):
    """POST — ответ модератора через веб-панель"""
    ticket_id = int(request.match_info["ticket_id"])
    data = await request.post()
    text = (data.get("text") or "").strip()

    if text and _bot:
        t = await db.ticket_get(ticket_id)
        if t and t["status"] != "closed":
            await db.ticket_msg_add(
                ticket_id=ticket_id,
                sender_id=0,
                sender_name="Модератор (Dashboard)",
                is_mod=True,
                text=text
            )
            # Отправляем пользователю
            try:
                await _bot.send_message(
                    t["uid"],
                    f"━━━━━━━━━━━━━━━\n"
                    f"💬 <b>ОТВЕТ ПО ТИКЕТУ #{ticket_id}</b>\n"
                    f"━━━━━━━━━━━━━━━\n\n"
                    f"👮 <b>Модератор</b>:\n\n{text}",
                    parse_mode="HTML"
                )
            except: pass

    raise web.HTTPFound(f"/dashboard/tickets/{ticket_id}")


@require_auth
async def handle_ticket_close_web(request: web.Request):
    ticket_id = int(request.match_info["ticket_id"])
    t = await db.ticket_get(ticket_id)
    if t:
        await db.ticket_close(ticket_id)
        if _bot:
            try:
                await _bot.send_message(
                    t["uid"],
                    f"━━━━━━━━━━━━━━━\n"
                    f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
                    f"━━━━━━━━━━━━━━━\n\n"
                    f"Твоё обращение закрыто администрацией.",
                    parse_mode="HTML"
                )
            except: pass
    raise web.HTTPFound("/dashboard/tickets")


@require_auth
async def handle_moderation(request: web.Request):
    import json as _json2
    c4 = db.get_conn()
    rows_mh2 = c4.execute(
        "SELECT m.cid, m.uid, m.history, k.title as chat_title "
        "FROM mod_history m LEFT JOIN known_chats k ON m.cid=k.cid"
    ).fetchall()
    c4.close()
    recent = []
    mod_stat = {}
    for r in rows_mh2:
        try:
            hist = _json2.loads(r["history"]) if r["history"] else []
            for h in hist:
                by = h.get("by", "—")
                act = h.get("action", "—")
                if by not in mod_stat:
                    mod_stat[by] = {"by_name": by, "cnt": 0, "bans": 0, "warns": 0, "mutes": 0}
                mod_stat[by]["cnt"] += 1
                if "БАН" in act: mod_stat[by]["bans"] += 1
                if "ВАРН" in act: mod_stat[by]["warns"] += 1
                if "МУТ" in act: mod_stat[by]["mutes"] += 1
                recent.append({
                    "created_at": h.get("time", "—"),
                    "chat_title": r["chat_title"] or str(r["cid"]),
                    "action": act,
                    "reason": h.get("reason", "—"),
                    "by_name": by,
                })
        except: pass
    recent = sorted(recent, key=lambda x: x.get("created_at",""), reverse=True)[:50]
    top_mods = sorted(mod_stat.values(), key=lambda x: -x["cnt"])[:10]

    mod_rows = "".join(
        f"<tr>"
        f"<td><b>{r['by_name']}</b></td>"
        f"<td>{r['cnt']}</td>"
        f"<td>{r['bans']}</td>"
        f"<td>{r['warns']}</td>"
        f"<td>{r['mutes']}</td>"
        f"</tr>"
        for r in top_mods
    ) or "<tr><td colspan='5'>Нет данных</td></tr>"

    act_rows = "".join(
        f"<tr>"
        f"<td>{r['created_at'][:16].replace('T',' ') if r['created_at'] else '—'}</td>"
        f"<td>{r.get('chat_title') or str(r['cid'])}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '—'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in recent
    ) or "<tr><td colspan='5'>Нет данных</td></tr>"

    body = navbar("moderation") + f"""
    <div class="container">
      <div class="page-title">🛡 Модерация</div>
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">👮 Рейтинг модераторов</div>
        <table>
          <thead>
            <tr><th>Модератор</th><th>Всего</th><th>Банов</th><th>Варнов</th><th>Мутов</th></tr>
          </thead>
          <tbody>{mod_rows}</tbody>
        </table>
      </div>
      <div class="section">
        <div class="section-header">📋 Последние действия (50)</div>
        <table>
          <thead>
            <tr><th>Время</th><th>Чат</th><th>Действие</th><th>Причина</th><th>Модератор</th></tr>
          </thead>
          <tbody>{act_rows}</tbody>
        </table>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_users(request: web.Request):
    search = request.rel_url.query.get("q", "")

    c5 = db.get_conn()
    if search:
        rows = c5.execute(
            "SELECT u.uid, u.full_name, u.username, "
            "COALESCE(SUM(cs.msg_count),0) as msgs, "
            "COALESCE(SUM(r.score),0) as rep "
            "FROM users u "
            "LEFT JOIN chat_stats cs ON u.uid=cs.uid "
            "LEFT JOIN reputation r ON u.uid=r.uid "
            "WHERE u.full_name LIKE ? OR u.username LIKE ? "
            "GROUP BY u.uid ORDER BY msgs DESC LIMIT 30",
            (f"%{search}%", f"%{search}%")
        ).fetchall()
    else:
        rows = c5.execute(
            "SELECT u.uid, u.full_name, u.username, "
            "COALESCE(SUM(cs.msg_count),0) as msgs, "
            "COALESCE(SUM(r.score),0) as rep "
            "FROM users u "
            "LEFT JOIN chat_stats cs ON u.uid=cs.uid "
            "LEFT JOIN reputation r ON u.uid=r.uid "
            "GROUP BY u.uid ORDER BY msgs DESC LIMIT 50"
        ).fetchall()
    c5.close()

    user_rows = "".join(
        f"<tr>"
        f"<td><code>{r['uid']}</code></td>"
        f"<td>{r['full_name'] or '—'}</td>"
        f"<td>{'@'+r['username'] if r['username'] else '—'}</td>"
        f"<td>{r['msgs']:,}</td>"
        f"<td>{r['rep']:+d}</td>"
        f"</tr>"
        for r in rows
    ) or "<tr><td colspan='5'>Нет данных</td></tr>"

    body = navbar("users") + f"""
    <div class="container">
      <div class="page-title">👥 Участники</div>
      <div class="search-box">
        <form method="GET">
          <input name="q" value="{search}"
            placeholder="Поиск по имени или @username..."
            style="width:100%;padding:10px 14px;background:#0f0f1a;border:1px solid #2a2a4a;
                   border-radius:8px;color:#e0e0e0;font-size:14px;">
        </form>
      </div>
      <div class="section">
        <table>
          <thead>
            <tr><th>ID</th><th>Имя</th><th>Username</th><th>Сообщений</th><th>Репутация</th></tr>
          </thead>
          <tbody>{user_rows}</tbody>
        </table>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


# ══════════════════════════════════════════
#  API — JSON эндпоинты для интеграций
# ══════════════════════════════════════════

async def api_stats(request: web.Request):
    token = request.headers.get("X-Token") or request.rel_url.query.get("token")
    if token != DASHBOARD_TOKEN:
        return web.json_response({"error": "Unauthorized"}, status=401)

    c6 = db.get_conn()
    total_users = c6.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats").fetchone()[0] or 0
    total_msgs  = c6.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats").fetchone()[0] or 0
    total_bans  = c6.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    c6.close()

    t_stats = await db.ticket_stats_all()

    return web.json_response({
        "chats":    len(await db.get_all_chats()),
        "users":    total_users,
        "messages": total_msgs,
        "bans":     total_bans,
        "tickets":  t_stats,
    })


async def handle_health(request: web.Request):
    return web.Response(text="OK")


# ══════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════

async def start_dashboard():
    app = web.Application()

    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)

    # Dashboard routes
    app.router.add_get("/dashboard",                        handle_overview)
    app.router.add_get("/dashboard/login",                  handle_login)
    app.router.add_post("/dashboard/login",                 handle_login)
    app.router.add_get("/dashboard/logout",                 handle_logout)
    app.router.add_get("/dashboard/chats",                  handle_chats)
    app.router.add_get("/dashboard/chats/{cid}",            handle_chat_detail)
    app.router.add_get("/dashboard/tickets",                handle_tickets)
    app.router.add_get("/dashboard/tickets/{ticket_id}",    handle_ticket_detail)
    app.router.add_post("/dashboard/tickets/{ticket_id}/reply",  handle_ticket_reply)
    app.router.add_get("/dashboard/tickets/{ticket_id}/close",   handle_ticket_close_web)
    app.router.add_get("/dashboard/moderation",             handle_moderation)
    app.router.add_get("/dashboard/users",                  handle_users)

    # API
    app.router.add_get("/api/stats", api_stats)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"✅ Dashboard запущен на :{port}")
    return runner

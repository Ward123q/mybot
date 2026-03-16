# -*- coding: utf-8 -*-
"""
dashboard.py &#8212; &#1042;&#1077;&#1073;-&#1087;&#1072;&#1085;&#1077;&#1083;&#1100; &#1091;&#1087;&#1088;&#1072;&#1074;&#1083;&#1077;&#1085;&#1080;&#1103; &#1085;&#1072; FastAPI
&#1047;&#1072;&#1087;&#1091;&#1089;&#1082;&#1072;&#1077;&#1090;&#1089;&#1103; &#1074;&#1084;&#1077;&#1089;&#1090;&#1077; &#1089; &#1073;&#1086;&#1090;&#1086;&#1084; &#1085;&#1072; &#1087;&#1086;&#1088;&#1090;&#1091; 8080
&#1044;&#1086;&#1089;&#1090;&#1091;&#1087;: http://your-server:8080/dashboard
"""
import os
import json
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional
from functools import wraps

from aiohttp import web
import database as db

log = logging.getLogger(__name__)

# &#1057;&#1077;&#1082;&#1088;&#1077;&#1090;&#1085;&#1099;&#1081; &#1090;&#1086;&#1082;&#1077;&#1085; &#1076;&#1083;&#1103; &#1076;&#1086;&#1089;&#1090;&#1091;&#1087;&#1072; &#1082; &#1076;&#1072;&#1096;&#1073;&#1086;&#1088;&#1076;&#1091;
# &#1059;&#1089;&#1090;&#1072;&#1085;&#1072;&#1074;&#1083;&#1080;&#1074;&#1072;&#1077;&#1090;&#1089;&#1103; &#1074; .env: DASHBOARD_TOKEN=your_secret_token
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "changeme123")

# &#1043;&#1083;&#1086;&#1073;&#1072;&#1083;&#1100;&#1085;&#1072;&#1103; &#1089;&#1089;&#1099;&#1083;&#1082;&#1072; &#1085;&#1072; &#1073;&#1086;&#1090; (&#1091;&#1089;&#1090;&#1072;&#1085;&#1072;&#1074;&#1083;&#1080;&#1074;&#1072;&#1077;&#1090;&#1089;&#1103; &#1080;&#1079; main)
_bot = None
_admin_ids = set()


def set_bot(bot, admin_ids: set):
    global _bot, _admin_ids
    _bot = bot
    _admin_ids = admin_ids


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  HTML &#1064;&#1040;&#1041;&#1051;&#1054;&#1053;&#1067;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

HTML_BASE = """<!DOCTYPE html>
<html lang="ru" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CHAT GUARD &#8212; Dashboard</title>
<style>
  :root[data-theme="dark"] {{
    --bg: #0f0f13; --bg2: #1a1a2e; --bg3: #161625;
    --border: #2a2a4a; --text: #e0e0e0; --text2: #9090b0;
    --accent: #7c6fcd; --danger: #c0392b; --success: #27ae60;
    --hover: #1f1f35;
  }}
  :root[data-theme="light"] {{
    --bg: #f0f2f5; --bg2: #ffffff; --bg3: #f8f9fa;
    --border: #dee2e6; --text: #212529; --text2: #6c757d;
    --accent: #6c5ce7; --danger: #e74c3c; --success: #2ecc71;
    --hover: #e9ecef;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
  }}
  .navbar {{
    background: var(--bg2);
    padding: 12px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 100;
    flex-wrap: wrap; gap: 8px;
  }}
  .navbar .brand {{ font-size: 18px; font-weight: 700; color: var(--accent); }}
  .navbar nav {{ display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }}
  .navbar nav a {{
    color: var(--text2); text-decoration: none;
    margin-left: 12px; font-size: 13px; transition: color .2s;
    white-space: nowrap;
  }}
  .navbar nav a:hover {{ color: var(--text); }}
  .theme-btn {{
    background: var(--bg3); border: 1px solid var(--border);
    color: var(--text2); padding: 4px 10px; border-radius: 6px;
    cursor: pointer; font-size: 13px; margin-left: 12px;
  }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
  .page-title {{ font-size: 22px; font-weight: 700; margin-bottom: 20px; color: var(--text); }}
  .cards {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px; margin-bottom: 28px;
  }}
  .card {{
    background: var(--bg2); border-radius: 12px; padding: 18px;
    border: 1px solid var(--border);
  }}
  .card .label {{ font-size: 11px; color: var(--text2); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }}
  .card .value {{ font-size: 28px; font-weight: 700; color: var(--accent); }}
  .card .sub {{ font-size: 11px; color: var(--text2); margin-top: 3px; }}
  .section {{
    background: var(--bg2); border-radius: 12px;
    border: 1px solid var(--border); margin-bottom: 20px; overflow: hidden;
  }}
  .section-header {{
    padding: 14px 20px; border-bottom: 1px solid var(--border);
    font-weight: 600; font-size: 14px; display: flex;
    justify-content: space-between; align-items: center;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 10px 16px; text-align: left; border-bottom: 1px solid var(--border); font-size: 13px; }}
  th {{ font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--text2); background: var(--bg3); }}
  tr:hover td {{ background: var(--hover); }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
  .badge-open     {{ background: #1a3a1a; color: #4caf50; }}
  .badge-progress {{ background: #2a2a0a; color: #ffc107; }}
  .badge-closed   {{ background: #1a1a3a; color: #9090cc; }}
  .badge-urgent   {{ background: #3a1a1a; color: #f44336; }}
  .badge-high     {{ background: #3a2a1a; color: #ff9800; }}
  .badge-normal   {{ background: #1a2a1a; color: #8bc34a; }}
  .badge-low      {{ background: #1a2a2a; color: #00bcd4; }}
  .badge-alert    {{ background: #3a1a1a; color: #ff5252; }}
  .btn {{ display: inline-block; padding: 6px 14px; border-radius: 6px; font-size: 13px; font-weight: 600; text-decoration: none; border: none; cursor: pointer; transition: opacity .2s; }}
  .btn:hover {{ opacity: .8; }}
  .btn-primary {{ background: var(--accent); color: #fff; }}
  .btn-danger  {{ background: var(--danger); color: #fff; }}
  .btn-success {{ background: var(--success); color: #fff; }}
  .btn-sm {{ padding: 4px 10px; font-size: 12px; }}
  .btn-outline {{ background: transparent; border: 1px solid var(--border); color: var(--text2); }}
  .form-group {{ margin-bottom: 14px; }}
  .form-group label {{ display: block; margin-bottom: 5px; font-size: 12px; color: var(--text2); }}
  .form-control {{
    width: 100%; padding: 9px 12px; background: var(--bg);
    border: 1px solid var(--border); border-radius: 8px;
    color: var(--text); font-size: 13px;
  }}
  .form-control:focus {{ outline: none; border-color: var(--accent); }}
  select.form-control option {{ background: var(--bg2); }}
  .login-wrap {{ min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
  .login-box {{
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 16px; padding: 40px; width: 380px;
  }}
  .login-box h2 {{ text-align: center; font-size: 22px; margin-bottom: 24px; color: var(--accent); }}
  .chat-bubble {{ padding: 10px 14px; border-radius: 10px; margin-bottom: 8px; max-width: 85%; font-size: 13px; line-height: 1.5; }}
  .bubble-user {{ background: var(--hover); margin-right: auto; }}
  .bubble-mod  {{ background: #1a2e1a; margin-left: auto; text-align: right; }}
  .bubble-meta {{ font-size: 11px; color: var(--text2); margin-bottom: 3px; }}
  .empty-state {{ text-align: center; padding: 40px; color: var(--text2); }}
  .search-box {{ padding: 16px 20px; border-bottom: 1px solid var(--border); }}
  .toggle-switch {{
    position: relative; display: inline-block; width: 44px; height: 24px;
  }}
  .toggle-switch input {{ opacity: 0; width: 0; height: 0; }}
  .toggle-slider {{
    position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0;
    background: #444; transition: .3s; border-radius: 24px;
  }}
  .toggle-slider:before {{
    position: absolute; content: ""; height: 18px; width: 18px;
    left: 3px; bottom: 3px; background: white;
    transition: .3s; border-radius: 50%;
  }}
  input:checked + .toggle-slider {{ background: var(--accent); }}
  input:checked + .toggle-slider:before {{ transform: translateX(20px); }}
  .alert-item {{
    padding: 12px 20px; border-left: 3px solid #f44336;
    margin-bottom: 8px; background: rgba(244,67,54,.05);
    border-radius: 0 8px 8px 0;
  }}
  .alert-item.warn {{ border-color: #ff9800; background: rgba(255,152,0,.05); }}
  .alert-item.info {{ border-color: var(--accent); background: rgba(124,111,205,.05); }}
  .notif-toast {{
    position: fixed; bottom: 24px; right: 24px;
    background: #1a2e1a; border: 1px solid #27ae60;
    border-radius: 12px; padding: 14px 20px; color: var(--text);
    font-size: 13px; z-index: 9999; max-width: 320px;
    display: none; animation: slidein .3s ease;
  }}
  @keyframes slidein {{ from {{ transform: translateY(20px); opacity: 0; }} to {{ transform: translateY(0); opacity: 1; }} }}
  @media (max-width: 768px) {{
    .container {{ padding: 12px; }}
    .cards {{ grid-template-columns: 1fr 1fr; }}
    table {{ font-size: 12px; }}
    th, td {{ padding: 8px 10px; }}
    .navbar nav a {{ margin-left: 8px; font-size: 12px; }}
  }}
</style>
<script>
// &#1058;&#1077;&#1084;&#1072;
(function() {{
  var t = localStorage.getItem('theme') || 'dark';
  document.documentElement.setAttribute('data-theme', t);
}})();
function toggleTheme() {{
  var t = document.documentElement.getAttribute('data-theme');
  var newT = t === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', newT);
  localStorage.setItem('theme', newT);
  document.querySelector('.theme-btn').textContent = newT === 'dark' ? '&#9728;' : '&#127769;';
}}
// SSE &#1091;&#1074;&#1077;&#1076;&#1086;&#1084;&#1083;&#1077;&#1085;&#1080;&#1103;
(function() {{
  var toast = null;
  function showToast(msg) {{
    if (!toast) {{ toast = document.createElement('div'); toast.className = 'notif-toast'; document.body.appendChild(toast); }}
    toast.innerHTML = '&#127987; <b>&#1053;&#1086;&#1074;&#1099;&#1081; &#1090;&#1080;&#1082;&#1077;&#1090;!</b><br>' + msg;
    toast.style.display = 'block';
    setTimeout(function() {{ toast.style.display = 'none'; }}, 6000);
  }}
  try {{
    var es = new EventSource('/dashboard/events');
    es.onmessage = function(e) {{
      if (e.data === 'connected') return;
      try {{ var d = JSON.parse(e.data); if (d.type === 'new_ticket') showToast('#' + d.id + ' &#1086;&#1090; ' + d.user); }} catch(err) {{}}
    }};
  }} catch(err) {{}}
}})();
</script>
</head>
<body>
{body}
</body>
</html>"""


def page(body: str) -> str:
    return HTML_BASE.format(body=body)


def navbar(active: str = "") -> str:
    links = [
        ("overview",    "/dashboard",             "&#128202; &#1054;&#1073;&#1079;&#1086;&#1088;"),
        ("chats",       "/dashboard/chats",        "&#128172; &#1063;&#1072;&#1090;&#1099;"),
        ("tickets",     "/dashboard/tickets",      "&#127987; &#1058;&#1080;&#1082;&#1077;&#1090;&#1099;"),
        ("reports",     "/dashboard/reports",      "&#128680; &#1056;&#1077;&#1087;&#1086;&#1088;&#1090;&#1099;"),
        ("moderation",  "/dashboard/moderation",   "&#128737; &#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1103;"),
        ("users",       "/dashboard/users",        "&#128101; &#1059;&#1095;&#1072;&#1089;&#1090;&#1085;&#1080;&#1082;&#1080;"),
        ("deleted",     "/dashboard/deleted",      "&#128203; &#1059;&#1076;&#1072;&#1083;&#1105;&#1085;&#1085;&#1099;&#1077;"),
        ("media",       "/dashboard/media",        "&#127902; &#1052;&#1077;&#1076;&#1080;&#1072;"),
        ("economy",     "/dashboard/economy",      "&#128176; &#1069;&#1082;&#1086;&#1085;&#1086;&#1084;&#1080;&#1082;&#1072;"),
        ("alerts",      "/dashboard/alerts",       "&#128308; &#1040;&#1083;&#1077;&#1088;&#1090;&#1099;"),
        ("broadcast",   "/dashboard/broadcast",    "&#128276; &#1056;&#1072;&#1089;&#1089;&#1099;&#1083;&#1082;&#1072;"),
        ("plugins",     "/dashboard/plugins",      "&#127899; &#1055;&#1083;&#1072;&#1075;&#1080;&#1085;&#1099;"),
        ("settings",    "/dashboard/settings",     "&#9881; &#1053;&#1072;&#1089;&#1090;&#1088;&#1086;&#1081;&#1082;&#1080;"),
    ]
    nav_items = "".join(
        f'<a href="{url}" style="{"color:#fff;font-weight:600;" if k==active else ""}">{label}</a>'
        for k, url, label in links
    )
    return (
        '<div class="navbar">'
        '<span class="brand">&#9889; CHAT GUARD</span>'
        '<nav>' + nav_items +
        '<button class="theme-btn" onclick="toggleTheme()">&#127769;</button>'
        '<a href="/dashboard/logout" style="color:#c0392b;">&#128682; &#1042;&#1099;&#1093;&#1086;&#1076;</a>'
        '</nav></div>'
    )


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  AUTH MIDDLEWARE
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

def require_auth(handler):
    @wraps(handler)
    async def wrapper(request: web.Request):
        token = request.cookies.get("dtoken") or request.rel_url.query.get("token")
        if token != DASHBOARD_TOKEN:
            raise web.HTTPFound("/dashboard/login")
        return await handler(request)
    return wrapper


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  ROUTES
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

async def handle_login(request: web.Request):
    if request.method == "POST":
        data = await request.post()
        token = data.get("token", "")
        if token == DASHBOARD_TOKEN:
            response = web.HTTPFound("/dashboard")
            response.set_cookie("dtoken", token, max_age=86400 * 7, httponly=True)
            raise response
        error = '<p style="color:#f44336;text-align:center;margin-top:12px;">&#1053;&#1077;&#1074;&#1077;&#1088;&#1085;&#1099;&#1081; &#1090;&#1086;&#1082;&#1077;&#1085;</p>'
    else:
        error = ""

    body = navbar() + f"""
    <div class="login-wrap">
      <div class="login-box">
        <h2>&#9889; CHAT GUARD</h2>
        <p style="text-align:center;color:#666;margin-bottom:24px;">&#1055;&#1072;&#1085;&#1077;&#1083;&#1100; &#1091;&#1087;&#1088;&#1072;&#1074;&#1083;&#1077;&#1085;&#1080;&#1103;</p>
        <form method="POST">
          <div class="form-group">
            <label>&#1058;&#1086;&#1082;&#1077;&#1085; &#1076;&#1086;&#1089;&#1090;&#1091;&#1087;&#1072;</label>
            <input class="form-control" type="password" name="token" placeholder="&#1042;&#1074;&#1077;&#1076;&#1080; &#1090;&#1086;&#1082;&#1077;&#1085;...">
          </div>
          <button class="btn btn-primary" style="width:100%;padding:12px;" type="submit">
            &#1042;&#1086;&#1081;&#1090;&#1080;
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
    # &#1057;&#1086;&#1073;&#1080;&#1088;&#1072;&#1077;&#1084; &#1089;&#1090;&#1072;&#1090;&#1080;&#1089;&#1090;&#1080;&#1082;&#1091;
    chats = await db.get_all_chats()
    ticket_stats = await db.ticket_stats_all()

    # &#1054;&#1073;&#1097;&#1080;&#1077; &#1095;&#1080;&#1089;&#1083;&#1072;
    total_chats = len(chats)

    conn = db.get_conn()
    total_users = conn.execute("SELECT COUNT(DISTINCT uid) FROM chat_stats").fetchone()[0] or 0
    total_msgs  = conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM chat_stats").fetchone()[0] or 0
    total_bans  = conn.execute("SELECT COUNT(*) FROM ban_list").fetchone()[0] or 0
    total_warns = conn.execute("SELECT COALESCE(SUM(count),0) FROM warnings").fetchone()[0] or 0
    conn.close()
    # mod_history &#1093;&#1088;&#1072;&#1085;&#1080;&#1090;&#1089;&#1103; &#1082;&#1072;&#1082; JSON {cid: {uid: [{action, reason, by, time}]}}
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
                    by = h.get("by", "&#8212;")
                    mod_counts[by] = mod_counts.get(by, 0) + 1
                    all_acts.append({
                        "action": h.get("action", "&#8212;"),
                        "reason": h.get("reason", "&#8212;"),
                        "by_name": by,
                        "created_at": h.get("time", "&#8212;"),
                    })
            except: pass
        recent_mods = [{"by_name": k, "cnt": v} for k, v in
                       sorted(mod_counts.items(), key=lambda x: -x[1])[:5]]
        recent_acts = sorted(all_acts, key=lambda x: x.get("created_at",""), reverse=True)[:10]
    except: pass

    cards = f"""
    <div class="cards">
      <div class="card">
        <div class="label">&#128172; &#1063;&#1072;&#1090;&#1086;&#1074;</div>
        <div class="value">{total_chats}</div>
        <div class="sub">&#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1099;&#1093; &#1095;&#1072;&#1090;&#1086;&#1074;</div>
      </div>
      <div class="card">
        <div class="label">&#128101; &#1059;&#1095;&#1072;&#1089;&#1090;&#1085;&#1080;&#1082;&#1086;&#1074;</div>
        <div class="value">{total_users:,}</div>
        <div class="sub">&#1091;&#1085;&#1080;&#1082;&#1072;&#1083;&#1100;&#1085;&#1099;&#1093; &#1102;&#1079;&#1077;&#1088;&#1086;&#1074;</div>
      </div>
      <div class="card">
        <div class="label">&#128172; &#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</div>
        <div class="value">{total_msgs:,}</div>
        <div class="sub">&#1074;&#1089;&#1077;&#1075;&#1086; &#1086;&#1073;&#1088;&#1072;&#1073;&#1086;&#1090;&#1072;&#1085;&#1086;</div>
      </div>
      <div class="card">
        <div class="label">&#128296; &#1041;&#1072;&#1085;&#1086;&#1074;</div>
        <div class="value">{total_bans}</div>
        <div class="sub">&#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1099;&#1093; &#1073;&#1072;&#1085;&#1086;&#1074;</div>
      </div>
      <div class="card">
        <div class="label">&#9889; &#1042;&#1072;&#1088;&#1085;&#1086;&#1074;</div>
        <div class="value">{total_warns}</div>
        <div class="sub">&#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1099;&#1093; &#1074;&#1072;&#1088;&#1085;&#1086;&#1074;</div>
      </div>
      <div class="card">
        <div class="label">&#127987; &#1058;&#1080;&#1082;&#1077;&#1090;&#1086;&#1074;</div>
        <div class="value">{ticket_stats['open']}</div>
        <div class="sub">&#1086;&#1090;&#1082;&#1088;&#1099;&#1090;&#1099;&#1093; / {ticket_stats['total']} &#1074;&#1089;&#1077;&#1075;&#1086;</div>
      </div>
    </div>"""

    # &#1058;&#1086;&#1087; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;&#1086;&#1074;
    mod_rows = "".join(
        f"<tr><td>&#128110; {r['by_name']}</td><td>{r['cnt']} &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1081;</td></tr>"
        for r in recent_mods
    ) or "<tr><td colspan='2' class='empty-state'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    # &#1055;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103;
    act_rows = "".join(
        f"<tr>"
        f"<td>{r['created_at'][:16].replace('T',' ') if r['created_at'] else '&#8212;'}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '&#8212;'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in recent_acts
    ) or "<tr><td colspan='4' class='empty-state'>&#1053;&#1077;&#1090; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1081;</td></tr>"

    body = navbar("overview") + f"""
    <div class="container">
      <div class="page-title">&#128202; &#1054;&#1073;&#1079;&#1086;&#1088;</div>
      {cards}
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;">
        <div class="section">
          <div class="section-header">&#128110; &#1058;&#1086;&#1087; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;&#1086;&#1074;</div>
          <table>
            <thead><tr><th>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1081;</th></tr></thead>
            <tbody>{mod_rows}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">&#9889; &#1055;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103;</div>
          <table>
            <thead><tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</th><th>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</th><th>&#1050;&#1090;&#1086;</th></tr></thead>
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
        title = chat["title"] or "&#1041;&#1077;&#1079; &#1085;&#1072;&#1079;&#1074;&#1072;&#1085;&#1080;&#1103;"
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
            f"<a class='btn btn-sm btn-primary' href='/dashboard/chats/{cid}'>&#1055;&#1086;&#1076;&#1088;&#1086;&#1073;&#1085;&#1077;&#1077;</a>"
            f"</td>"
            f"</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='7' class='empty-state'>&#1053;&#1077;&#1090; &#1095;&#1072;&#1090;&#1086;&#1074;</td></tr>"

    body = navbar("chats") + f"""
    <div class="container">
      <div class="page-title">&#128172; &#1063;&#1072;&#1090;&#1099;</div>
      <div class="section">
        <div class="section-header">&#1042;&#1089;&#1077; &#1095;&#1072;&#1090;&#1099; ({len(chats)})</div>
        <table>
          <thead>
            <tr>
              <th>ID</th><th>&#1053;&#1072;&#1079;&#1074;&#1072;&#1085;&#1080;&#1077;</th><th>&#1059;&#1095;&#1072;&#1089;&#1090;&#1085;&#1080;&#1082;&#1086;&#1074;</th>
              <th>&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</th><th>&#1042;&#1072;&#1088;&#1085;&#1086;&#1074;</th><th>&#1041;&#1072;&#1085;&#1086;&#1074;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103;</th>
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
        "SELECT uid, reason, banned_at FROM ban_list WHERE cid=?", (cid,)
    ).fetchall()
    mod_hist = c3.execute(
        "SELECT action, reason, by_name, created_at FROM mod_history "
        "WHERE cid=? ORDER BY created_at DESC LIMIT 20", (cid,)
    ).fetchall()
    c3.close()
    hours = await db.get_hourly_totals(cid)

    # &#1061;&#1080;&#1090;&#1084;&#1072;&#1087; &#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1080;
    max_h = max(hours.values(), default=1)
    heatmap = ""
    for h in range(24):
        val = hours.get(h, 0)
        pct = int((val / max_h) * 100) if max_h else 0
        heatmap += (
            f'<div style="display:inline-block;width:3.8%;vertical-align:bottom;'
            f'height:{max(4, pct)}px;background:#7c6fcd;opacity:{max(0.2, pct/100):.2f};'
            f'margin:1px;border-radius:2px;" title="{h}:00 &#8212; {val} &#1089;&#1086;&#1086;&#1073;&#1097;."></div>'
        )

    def user_rows(rows):
        result = ""
        for i, r in enumerate(rows, 1):
            result += f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r[rows[0].keys()[2]]:,}</td></tr>"
        return result or "<tr><td colspan='3'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    top_u_rows = "".join(
        f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r['msg_count']:,}</td></tr>"
        for i, r in enumerate(top_users, 1)
    ) or "<tr><td colspan='3'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    top_r_rows = "".join(
        f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r['score']:+d}</td></tr>"
        for i, r in enumerate(top_rep, 1)
    ) or "<tr><td colspan='3'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    ban_rows = "".join(
        "<tr><td><code>" + str(dict(r).get("uid","")) + "</code></td>"
        "<td>" + str(dict(r).get("reason") or "&#8212;") + "</td>"
        "<td>" + str(dict(r).get("banned_at") or "")[:10] + "</td></tr>"
        for r in bans
    ) or "<tr><td colspan='3'>&#1053;&#1077;&#1090; &#1073;&#1072;&#1085;&#1086;&#1074;</td></tr>"

    hist_rows = "".join(
        f"<tr>"
        f"<td>{r['created_at'][:16].replace('T',' ') if r['created_at'] else '&#8212;'}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '&#8212;'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in mod_hist
    ) or "<tr><td colspan='4'>&#1053;&#1077;&#1090; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1081;</td></tr>"

    body = navbar("chats") + f"""
    <div class="container">
      <div class="page-title">&#128172; {title}</div>
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">&#128202; &#1040;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1100; &#1087;&#1086; &#1095;&#1072;&#1089;&#1072;&#1084;</div>
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
          <div class="section-header">&#127942; &#1058;&#1086;&#1087; &#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1099;&#1093;</div>
          <table>
            <thead><tr><th>#</th><th>ID</th><th>&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</th></tr></thead>
            <tbody>{top_u_rows}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">&#11088; &#1058;&#1086;&#1087; &#1088;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1080;</div>
          <table>
            <thead><tr><th>#</th><th>ID</th><th>&#1056;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1103;</th></tr></thead>
            <tbody>{top_r_rows}</tbody>
          </table>
        </div>
      </div>
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">&#128296; &#1057;&#1087;&#1080;&#1089;&#1086;&#1082; &#1073;&#1072;&#1085;&#1086;&#1074; ({len(bans)})</div>
        <table>
          <thead><tr><th>ID</th><th>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</th><th>&#1044;&#1072;&#1090;&#1072;</th></tr></thead>
          <tbody>{ban_rows}</tbody>
        </table>
      </div>
      <div class="section">
        <div class="section-header">&#128203; &#1048;&#1089;&#1090;&#1086;&#1088;&#1080;&#1103; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1080;</div>
        <table>
          <thead><tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</th><th>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</th><th>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;</th></tr></thead>
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
            ("open", "open", "&#127381; &#1054;&#1090;&#1082;&#1088;&#1099;&#1090;&#1099;&#1077;"),
            ("in_progress", "in_progress", "&#128260; &#1042; &#1088;&#1072;&#1073;&#1086;&#1090;&#1077;"),
            ("closed", "closed", "&#9989; &#1047;&#1072;&#1082;&#1088;&#1099;&#1090;&#1099;&#1077;"),
        ]
    )

    rows = ""
    for t in [dict(x) for x in tickets]:
        pri_badge = f'<span class="badge badge-{t["priority"]}">{t["priority"]}</span>'
        st_badge  = f'<span class="badge badge-{"open" if t["status"]=="open" else ("progress" if t["status"]=="in_progress" else "closed")}">{t["status"]}</span>'
        dt = t["created_at"].strftime("%d.%m %H:%M") if t.get("created_at") else "&#8212;"
        rows += (
            f"<tr>"
            f"<td>#{t['id']}</td>"
            f"<td>{t['user_name'] or '&#1040;&#1085;&#1086;&#1085;&#1080;&#1084;'}<br><small style='color:#666'>{t['chat_title'] or '&#8212;'}</small></td>"
            f"<td>{t['subject'][:40]}</td>"
            f"<td>{st_badge}</td>"
            f"<td>{pri_badge}</td>"
            f"<td>{t['assigned_mod'] or '&#8212;'}</td>"
            f"<td>{dt}</td>"
            f"<td><a class='btn btn-sm btn-primary' href='/dashboard/tickets/{t['id']}'>&#1054;&#1090;&#1082;&#1088;&#1099;&#1090;&#1100;</a></td>"
            f"</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='8' class='empty-state'>&#1058;&#1080;&#1082;&#1077;&#1090;&#1086;&#1074; &#1085;&#1077;&#1090;</td></tr>"

    body = navbar("tickets") + f"""
    <div class="container">
      <div class="page-title" style="display:flex;justify-content:space-between;align-items:center;">
        <span>&#127987; &#1058;&#1080;&#1082;&#1077;&#1090;&#1099;</span>
        <div style="font-size:14px;color:#666;">
          &#1042;&#1089;&#1077;&#1075;&#1086;: {stats['total']}
        </div>
      </div>
      <div style="margin-bottom:20px;">{status_tabs}</div>
      <div class="section">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>&#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100;</th><th>&#1058;&#1077;&#1084;&#1072;</th>
              <th>&#1057;&#1090;&#1072;&#1090;&#1091;&#1089;</th><th>&#1055;&#1088;&#1080;&#1086;&#1088;&#1080;&#1090;&#1077;&#1090;</th><th>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;</th><th>&#1057;&#1086;&#1079;&#1076;&#1072;&#1085;</th><th></th>
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
    st_label  = {"open": "&#127381; &#1054;&#1090;&#1082;&#1088;&#1099;&#1090;", "in_progress": "&#128260; &#1042; &#1088;&#1072;&#1073;&#1086;&#1090;&#1077;", "closed": "&#9989; &#1047;&#1072;&#1082;&#1088;&#1099;&#1090;"}.get(stat, stat)
    dt_open   = t["created_at"].strftime("%d.%m.%Y %H:%M") if t.get("created_at") else "&#8212;"
    dt_upd    = t["updated_at"].strftime("%d.%m.%Y %H:%M") if t.get("updated_at") else "&#8212;"

    bubbles = ""
    for m in [dict(x) for x in msgs]:
        is_mod = m["is_mod"]
        who    = f"&#128110; {m['sender_name']}" if is_mod else f"&#128100; {m['sender_name']}"
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
        bubbles = '<div class="empty-state">&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081; &#1085;&#1077;&#1090;</div>'

    # &#1060;&#1086;&#1088;&#1084;&#1072; &#1086;&#1090;&#1074;&#1077;&#1090;&#1072; (&#1090;&#1086;&#1083;&#1100;&#1082;&#1086; &#1077;&#1089;&#1083;&#1080; &#1090;&#1080;&#1082;&#1077;&#1090; &#1086;&#1090;&#1082;&#1088;&#1099;&#1090;)
    reply_form = ""
    if stat != "closed":
        reply_form = f"""
        <div class="section" style="margin-top:24px;">
          <div class="section-header">&#9999; &#1054;&#1090;&#1074;&#1077;&#1090;&#1080;&#1090;&#1100;</div>
          <div style="padding:20px;">
            <form method="POST" action="/dashboard/tickets/{ticket_id}/reply">
              <div class="form-group">
                <textarea class="form-control" name="text" rows="4"
                  placeholder="&#1042;&#1074;&#1077;&#1076;&#1080; &#1086;&#1090;&#1074;&#1077;&#1090;..."></textarea>
              </div>
              <div style="display:flex;gap:12px;">
                <button class="btn btn-primary" type="submit">&#1054;&#1090;&#1087;&#1088;&#1072;&#1074;&#1080;&#1090;&#1100;</button>
                <a class="btn btn-danger" href="/dashboard/tickets/{ticket_id}/close">
                  &#1047;&#1072;&#1082;&#1088;&#1099;&#1090;&#1100; &#1090;&#1080;&#1082;&#1077;&#1090;
                </a>
              </div>
            </form>
          </div>
        </div>"""

    body = navbar("tickets") + f"""
    <div class="container">
      <div class="page-title">
        &#127987; &#1058;&#1080;&#1082;&#1077;&#1090; #{ticket_id}
        <a href="/dashboard/tickets" style="font-size:14px;color:#7c6fcd;margin-left:16px;">&larr; &#1053;&#1072;&#1079;&#1072;&#1076;</a>
      </div>
      <div style="display:grid;grid-template-columns:1fr 320px;gap:24px;">
        <div>
          <div class="section">
            <div class="section-header">&#128172; &#1055;&#1077;&#1088;&#1077;&#1087;&#1080;&#1089;&#1082;&#1072;</div>
            <div style="padding:16px;max-height:500px;overflow-y:auto;">
              {bubbles}
            </div>
          </div>
          {reply_form}
        </div>
        <div>
          <div class="section">
            <div class="section-header">&#8505;&#65039; &#1048;&#1085;&#1092;&#1086;&#1088;&#1084;&#1072;&#1094;&#1080;&#1103;</div>
            <div style="padding:16px;font-size:14px;line-height:2;">
              <div><b>&#1057;&#1090;&#1072;&#1090;&#1091;&#1089;:</b> {st_label}</div>
              <div><b>&#1055;&#1088;&#1080;&#1086;&#1088;&#1080;&#1090;&#1077;&#1090;:</b> {pri_badge}</div>
              <div><b>&#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100;:</b> {t['user_name'] or '&#1040;&#1085;&#1086;&#1085;&#1080;&#1084;'}</div>
              <div><b>&#1063;&#1072;&#1090;:</b> {t['chat_title'] or '&#8212;'}</div>
              <div><b>&#1058;&#1077;&#1084;&#1072;:</b> {t['subject']}</div>
              <div><b>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;:</b> {t['assigned_mod'] or '&#1053;&#1077; &#1085;&#1072;&#1079;&#1085;&#1072;&#1095;&#1077;&#1085;'}</div>
              <div><b>&#1057;&#1086;&#1079;&#1076;&#1072;&#1085;:</b> {dt_open}</div>
              <div><b>&#1054;&#1073;&#1085;&#1086;&#1074;&#1083;&#1105;&#1085;:</b> {dt_upd}</div>
            </div>
          </div>
          <div style="margin-top:12px;">
            {'<a class="btn btn-danger" style="width:100%;text-align:center;display:block;" href="/dashboard/tickets/' + str(ticket_id) + '/close">&#9989; &#1047;&#1072;&#1082;&#1088;&#1099;&#1090;&#1100; &#1090;&#1080;&#1082;&#1077;&#1090;</a>' if stat != 'closed' else ''}
          </div>
        </div>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_ticket_reply(request: web.Request):
    """POST &#8212; &#1086;&#1090;&#1074;&#1077;&#1090; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;&#1072; &#1095;&#1077;&#1088;&#1077;&#1079; &#1074;&#1077;&#1073;-&#1087;&#1072;&#1085;&#1077;&#1083;&#1100;"""
    ticket_id = int(request.match_info["ticket_id"])
    data = await request.post()
    text = (data.get("text") or "").strip()

    if text and _bot:
        t = await db.ticket_get(ticket_id)
        if t and t["status"] != "closed":
            await db.ticket_msg_add(
                ticket_id=ticket_id,
                sender_id=0,
                sender_name="&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088; (Dashboard)",
                is_mod=True,
                text=text
            )
            # &#1054;&#1090;&#1087;&#1088;&#1072;&#1074;&#1083;&#1103;&#1077;&#1084; &#1087;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1102;
            try:
                await _bot.send_message(
                    t["uid"],
                    f"&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;\n"
                    f"&#128172; <b>&#1054;&#1058;&#1042;&#1045;&#1058; &#1055;&#1054; &#1058;&#1048;&#1050;&#1045;&#1058;&#1059; #{ticket_id}</b>\n"
                    f"&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;\n\n"
                    f"&#128110; <b>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;</b>:\n\n{text}",
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
                    f"&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;\n"
                    f"&#9989; <b>&#1058;&#1048;&#1050;&#1045;&#1058; #{ticket_id} &#1047;&#1040;&#1050;&#1056;&#1067;&#1058;</b>\n"
                    f"&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;&#9473;\n\n"
                    f"&#1058;&#1074;&#1086;&#1105; &#1086;&#1073;&#1088;&#1072;&#1097;&#1077;&#1085;&#1080;&#1077; &#1079;&#1072;&#1082;&#1088;&#1099;&#1090;&#1086; &#1072;&#1076;&#1084;&#1080;&#1085;&#1080;&#1089;&#1090;&#1088;&#1072;&#1094;&#1080;&#1077;&#1081;.",
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
                by = h.get("by", "&#8212;")
                act = h.get("action", "&#8212;")
                if by not in mod_stat:
                    mod_stat[by] = {"by_name": by, "cnt": 0, "bans": 0, "warns": 0, "mutes": 0}
                mod_stat[by]["cnt"] += 1
                if "&#1041;&#1040;&#1053;" in act: mod_stat[by]["bans"] += 1
                if "&#1042;&#1040;&#1056;&#1053;" in act: mod_stat[by]["warns"] += 1
                if "&#1052;&#1059;&#1058;" in act: mod_stat[by]["mutes"] += 1
                recent.append({
                    "created_at": h.get("time", "&#8212;"),
                    "chat_title": r["chat_title"] or str(r["cid"]),
                    "action": act,
                    "reason": h.get("reason", "&#8212;"),
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
    ) or "<tr><td colspan='5'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    act_rows = "".join(
        f"<tr>"
        f"<td>{r['created_at'][:16].replace('T',' ') if r['created_at'] else '&#8212;'}</td>"
        f"<td>{r.get('chat_title') or str(r['cid'])}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '&#8212;'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in recent
    ) or "<tr><td colspan='5'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    body = navbar("moderation") + f"""
    <div class="container">
      <div class="page-title">&#128737; &#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1103;</div>
      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">&#128110; &#1056;&#1077;&#1081;&#1090;&#1080;&#1085;&#1075; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;&#1086;&#1074;</div>
        <table>
          <thead>
            <tr><th>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;</th><th>&#1042;&#1089;&#1077;&#1075;&#1086;</th><th>&#1041;&#1072;&#1085;&#1086;&#1074;</th><th>&#1042;&#1072;&#1088;&#1085;&#1086;&#1074;</th><th>&#1052;&#1091;&#1090;&#1086;&#1074;</th></tr>
          </thead>
          <tbody>{mod_rows}</tbody>
        </table>
      </div>
      <div class="section">
        <div class="section-header">&#128203; &#1055;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103; (50)</div>
        <table>
          <thead>
            <tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1063;&#1072;&#1090;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</th><th>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</th><th>&#1052;&#1086;&#1076;&#1077;&#1088;&#1072;&#1090;&#1086;&#1088;</th></tr>
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
    try:
        if search:
            rows = c5.execute(
                "SELECT cs.uid, SUM(cs.msg_count) as msgs, "
                "COALESCE(SUM(r.score),0) as rep "
                "FROM chat_stats cs "
                "LEFT JOIN reputation r ON cs.uid=r.uid "
                "GROUP BY cs.uid ORDER BY msgs DESC LIMIT 30"
            ).fetchall()
        else:
            rows = c5.execute(
                "SELECT cs.uid, SUM(cs.msg_count) as msgs, "
                "COALESCE(SUM(r.score),0) as rep "
                "FROM chat_stats cs "
                "LEFT JOIN reputation r ON cs.uid=r.uid "
                "GROUP BY cs.uid ORDER BY msgs DESC LIMIT 50"
            ).fetchall()
    except:
        rows = []
    c5.close()

    user_rows = "".join(
        f"<tr>"
        f"<td><code>{r['uid']}</code></td>"
        f"<td>&#8212;</td>"
        f"<td>&#8212;</td>"
        f"<td>{r['msgs']:,}</td>"
        f"<td>{r['rep']:+d}</td>"
        f"</tr>"
        for r in [dict(x) for x in rows]
    ) or "<tr><td colspan='5'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    body = navbar("users") + f"""
    <div class="container">
      <div class="page-title">&#128101; &#1059;&#1095;&#1072;&#1089;&#1090;&#1085;&#1080;&#1082;&#1080;</div>
      <div class="search-box">
        <form method="GET">
          <input name="q" value="{search}"
            placeholder="&#1055;&#1086;&#1080;&#1089;&#1082; &#1087;&#1086; &#1080;&#1084;&#1077;&#1085;&#1080; &#1080;&#1083;&#1080; @username..."
            style="width:100%;padding:10px 14px;background:#0f0f1a;border:1px solid #2a2a4a;
                   border-radius:8px;color:#e0e0e0;font-size:14px;">
        </form>
      </div>
      <div class="section">
        <table>
          <thead>
            <tr><th>ID</th><th>&#1048;&#1084;&#1103;</th><th>Username</th><th>&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</th><th>&#1056;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1103;</th></tr>
          </thead>
          <tbody>{user_rows}</tbody>
        </table>
      </div>
    </div>"""

    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  API &#8212; JSON &#1101;&#1085;&#1076;&#1087;&#1086;&#1080;&#1085;&#1090;&#1099; &#1076;&#1083;&#1103; &#1080;&#1085;&#1090;&#1077;&#1075;&#1088;&#1072;&#1094;&#1080;&#1081;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

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


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1040;&#1051;&#1045;&#1056;&#1058;&#1067;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

# &#1061;&#1088;&#1072;&#1085;&#1080;&#1083;&#1080;&#1097;&#1077; &#1072;&#1083;&#1077;&#1088;&#1090;&#1086;&#1074; &#1074; &#1087;&#1072;&#1084;&#1103;&#1090;&#1080;
_alerts: list = []
_spam_tracker: dict = {}  # {uid: [timestamps]}


def add_alert(level: str, title: str, desc: str, cid: int = 0, uid: int = 0):
    """&#1044;&#1086;&#1073;&#1072;&#1074;&#1083;&#1103;&#1077;&#1090; &#1072;&#1083;&#1077;&#1088;&#1090; (level: danger/warn/info)"""
    _alerts.insert(0, {
        "level": level, "title": title, "desc": desc,
        "cid": cid, "uid": uid,
        "time": datetime.now().strftime("%d.%m %H:%M")
    })
    if len(_alerts) > 200:
        _alerts.pop()


async def check_spam(uid: int, cid: int, name: str, chat_title: str):
    """&#1055;&#1088;&#1086;&#1074;&#1077;&#1088;&#1103;&#1077;&#1090; &#1085;&#1072; &#1089;&#1087;&#1072;&#1084;/&#1092;&#1083;&#1091;&#1076;"""
    now = time.time()
    key = f"{cid}:{uid}"
    if key not in _spam_tracker:
        _spam_tracker[key] = []
    _spam_tracker[key].append(now)
    # &#1054;&#1089;&#1090;&#1072;&#1074;&#1083;&#1103;&#1077;&#1084; &#1090;&#1086;&#1083;&#1100;&#1082;&#1086; &#1087;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; 60 &#1089;&#1077;&#1082;&#1091;&#1085;&#1076;
    _spam_tracker[key] = [t for t in _spam_tracker[key] if now - t < 60]
    count = len(_spam_tracker[key])
    if count >= 15:
        add_alert("danger", "&#128680; &#1060;&#1083;&#1091;&#1076; &#1086;&#1073;&#1085;&#1072;&#1088;&#1091;&#1078;&#1077;&#1085;",
                  f"{name} &#1086;&#1090;&#1087;&#1088;&#1072;&#1074;&#1080;&#1083; {count} &#1089;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081; &#1079;&#1072; &#1084;&#1080;&#1085;&#1091;&#1090;&#1091; &#1074; {chat_title}",
                  cid, uid)
    elif count >= 8:
        add_alert("warn", "&#9888; &#1055;&#1086;&#1076;&#1086;&#1079;&#1088;&#1080;&#1090;&#1077;&#1083;&#1100;&#1085;&#1072;&#1103; &#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1100;",
                  f"{name} &#1086;&#1090;&#1087;&#1088;&#1072;&#1074;&#1080;&#1083; {count} &#1089;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081; &#1079;&#1072; &#1084;&#1080;&#1085;&#1091;&#1090;&#1091; &#1074; {chat_title}",
                  cid, uid)


@require_auth
async def handle_alerts(request: web.Request):
    if not _alerts:
        body = navbar("alerts") + """
        <div class="container">
          <div class="page-title">&#128308; &#1040;&#1083;&#1077;&#1088;&#1090;&#1099;</div>
          <div class="section"><div class="empty-state">&#9989; &#1042;&#1089;&#1105; &#1089;&#1087;&#1086;&#1082;&#1086;&#1081;&#1085;&#1086; &#8212; &#1072;&#1083;&#1077;&#1088;&#1090;&#1086;&#1074; &#1085;&#1077;&#1090;</div></div>
        </div>"""
        return web.Response(text=page(body), content_type="text/html")

    items = ""
    for a in _alerts[:50]:
        cls = {"danger": "alert-item", "warn": "alert-item warn", "info": "alert-item info"}.get(a["level"], "alert-item")
        action = ""
        if a["uid"] and a["cid"] and _bot:
            action = (
                f'<a class="btn btn-sm btn-danger" style="margin-top:6px;" '
                f'href="/dashboard/modaction?uid={a["uid"]}&cid={a["cid"]}&action=mute">&#128263; &#1052;&#1091;&#1090;</a> '
                f'<a class="btn btn-sm btn-danger" style="margin-top:6px;" '
                f'href="/dashboard/modaction?uid={a["uid"]}&cid={a["cid"]}&action=ban">&#128296; &#1041;&#1072;&#1085;</a>'
            )
        items += f"""
        <div class="{cls}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div>
              <b>{a['title']}</b>
              <div style="font-size:12px;color:var(--text2);margin-top:3px;">{a['desc']}</div>
              {f'<div style="font-size:11px;color:var(--text2);">CID: {a["cid"]} · UID: {a["uid"]}</div>' if a['uid'] else ''}
              {action}
            </div>
            <span style="font-size:11px;color:var(--text2);white-space:nowrap;">{a['time']}</span>
          </div>
        </div>"""

    body = navbar("alerts") + f"""
    <div class="container">
      <div class="page-title" style="display:flex;justify-content:space-between;">
        <span>&#128308; &#1040;&#1083;&#1077;&#1088;&#1090;&#1099; ({len(_alerts)})</span>
        <a href="/dashboard/alerts/clear" class="btn btn-sm btn-outline">&#128465; &#1054;&#1095;&#1080;&#1089;&#1090;&#1080;&#1090;&#1100;</a>
      </div>
      <div class="section" style="padding:16px;">{items}</div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


@require_auth
async def handle_alerts_clear(request: web.Request):
    _alerts.clear()
    raise web.HTTPFound("/dashboard/alerts")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1044;&#1045;&#1058;&#1040;&#1051;&#1068;&#1053;&#1040;&#1071; &#1057;&#1058;&#1056;&#1040;&#1053;&#1048;&#1062;&#1040; &#1070;&#1047;&#1045;&#1056;&#1040;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_user_detail(request: web.Request):
    uid = int(request.match_info["uid"])
    conn = db.get_conn()

    # &#1042;&#1089;&#1077; &#1095;&#1072;&#1090;&#1099; &#1102;&#1079;&#1077;&#1088;&#1072;
    chats_rows = conn.execute(
        "SELECT k.cid, k.title, cs.msg_count "
        "FROM chat_stats cs LEFT JOIN known_chats k ON cs.cid=k.cid "
        "WHERE cs.uid=? ORDER BY cs.msg_count DESC",
        (uid,)
    ).fetchall()

    # &#1056;&#1077;&#1087;&#1072; &#1087;&#1086; &#1095;&#1072;&#1090;&#1072;&#1084;
    rep_rows = conn.execute(
        "SELECT cid, score FROM reputation WHERE uid=? ORDER BY score DESC", (uid,)
    ).fetchall()

    # &#1042;&#1072;&#1088;&#1085;&#1099; &#1087;&#1086; &#1095;&#1072;&#1090;&#1072;&#1084;
    warn_rows = conn.execute(
        "SELECT cid, count FROM warnings WHERE uid=? AND count>0", (uid,)
    ).fetchall()

    # &#1048;&#1089;&#1090;&#1086;&#1088;&#1080;&#1103; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1080;
    hist_rows = conn.execute(
        "SELECT action, reason, by_name, created_at, cid FROM mod_history "
        "WHERE uid=? ORDER BY created_at DESC LIMIT 30",
        (uid,)
    ).fetchall()

    # &#1058;&#1080;&#1082;&#1077;&#1090;&#1099;
    try:
        ticket_rows = conn.execute(
            "SELECT id, subject, status, created_at FROM tickets "
            "WHERE uid=? ORDER BY created_at DESC LIMIT 10",
            (uid,)
        ).fetchall()
    except:
        ticket_rows = []

    # &#1044;&#1086;&#1089;&#1090;&#1080;&#1078;&#1077;&#1085;&#1080;&#1103;
    try:
        ach_rows = conn.execute(
            "SELECT key FROM achievements WHERE uid=?", (uid,)
        ).fetchall()
    except:
        ach_rows = []

    conn.close()

    total_msgs = sum(r["msg_count"] or 0 for r in chats_rows)
    total_rep  = sum(r["score"] or 0 for r in rep_rows)
    total_warns = sum(r["count"] or 0 for r in warn_rows)

    # &#1063;&#1072;&#1090;&#1099; &#1090;&#1072;&#1073;&#1083;&#1080;&#1094;&#1072;
    chat_rows_html = "".join(
        f"<tr><td>{r['title'] or r['cid']}</td>"
        f"<td>{r['msg_count']:,}</td></tr>"
        for r in chats_rows
    ) or "<tr><td colspan='2'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    # &#1048;&#1089;&#1090;&#1086;&#1088;&#1080;&#1103; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1080;
    hist_html = "".join(
        f"<tr>"
        f"<td>{str(r['created_at'])[:16]}</td>"
        f"<td>{r['action']}</td>"
        f"<td>{r['reason'] or '&#8212;'}</td>"
        f"<td>{r['by_name']}</td>"
        f"</tr>"
        for r in hist_rows
    ) or "<tr><td colspan='4'>&#1053;&#1077;&#1090; &#1080;&#1089;&#1090;&#1086;&#1088;&#1080;&#1080;</td></tr>"

    # &#1058;&#1080;&#1082;&#1077;&#1090;&#1099;
    ticket_html = "".join(
        f"<tr>"
        f"<td>#{r['id']}</td>"
        f"<td>{r['subject'][:40]}</td>"
        f"<td><span class='badge badge-{r['status']}'>{r['status']}</span></td>"
        f"<td><a href='/dashboard/tickets/{r['id']}' class='btn btn-sm btn-primary'>&#1054;&#1090;&#1082;&#1088;&#1099;&#1090;&#1100;</a></td>"
        f"</tr>"
        for r in ticket_rows
    ) or "<tr><td colspan='4'>&#1053;&#1077;&#1090; &#1090;&#1080;&#1082;&#1077;&#1090;&#1086;&#1074;</td></tr>"

    # Достижения
    try:
        from features import ACHIEVEMENTS as _ACH
        ach_html = " ".join(
            f'<span title="{_ACH[r["key"]][1]}" style="font-size:20px;">{_ACH[r["key"]][0]}</span>'
            for r in ach_rows if r["key"] in _ACH
        ) or "&#1053;&#1077;&#1090; &#1076;&#1086;&#1089;&#1090;&#1080;&#1078;&#1077;&#1085;&#1080;&#1081;"
    except:
        ach_html = "&#8212;"

    body = navbar("users") + f"""
    <div class="container">
      <div class="page-title">
        &#128100; &#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100; <code>{uid}</code>
        <a href="/dashboard/users" style="font-size:14px;color:var(--accent);margin-left:16px;">&larr; &#1053;&#1072;&#1079;&#1072;&#1076;</a>
      </div>
      <div class="cards">
        <div class="card"><div class="label">&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</div><div class="value">{total_msgs:,}</div></div>
        <div class="card"><div class="label">&#1056;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1103;</div><div class="value">{total_rep:+d}</div></div>
        <div class="card"><div class="label">&#1042;&#1072;&#1088;&#1085;&#1086;&#1074;</div><div class="value">{total_warns}</div></div>
        <div class="card"><div class="label">&#1058;&#1080;&#1082;&#1077;&#1090;&#1086;&#1074;</div><div class="value">{len(ticket_rows)}</div></div>
      </div>

      <div class="section" style="margin-bottom:20px;padding:16px;">
        <div style="margin-bottom:8px;font-weight:600;">&#127942; &#1044;&#1086;&#1089;&#1090;&#1080;&#1078;&#1077;&#1085;&#1080;&#1103;</div>
        <div>{ach_html}</div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px;">
        <div class="section">
          <div class="section-header">&#128172; &#1040;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1100; &#1087;&#1086; &#1095;&#1072;&#1090;&#1072;&#1084;</div>
          <table><thead><tr><th>&#1063;&#1072;&#1090;</th><th>&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</th></tr></thead>
          <tbody>{chat_rows_html}</tbody></table>
        </div>
        <div class="section">
          <div class="section-header">&#127987; &#1058;&#1080;&#1082;&#1077;&#1090;&#1099;</div>
          <table><thead><tr><th>ID</th><th>&#1058;&#1077;&#1084;&#1072;</th><th>&#1057;&#1090;&#1072;&#1090;&#1091;&#1089;</th><th></th></tr></thead>
          <tbody>{ticket_html}</tbody></table>
        </div>
      </div>

      <div class="section" style="margin-bottom:20px;">
        <div class="section-header">&#128203; &#1048;&#1089;&#1090;&#1086;&#1088;&#1080;&#1103; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1080;</div>
        <table><thead><tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</th><th>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</th><th>&#1050;&#1090;&#1086;</th></tr></thead>
        <tbody>{hist_html}</tbody></table>
      </div>

      <div class="section" style="padding:20px;">
        <div style="font-weight:600;margin-bottom:16px;">&#128296; &#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103;</div>
        <div style="display:flex;gap:12px;flex-wrap:wrap;">
          <a class="btn btn-danger" href="/dashboard/modaction?uid={uid}&action=ban">&#128296; &#1047;&#1072;&#1073;&#1072;&#1085;&#1080;&#1090;&#1100;</a>
          <a class="btn btn-danger" href="/dashboard/modaction?uid={uid}&action=mute">&#128263; &#1047;&#1072;&#1084;&#1091;&#1090;&#1080;&#1090;&#1100;</a>
          <a class="btn btn-success" href="/dashboard/modaction?uid={uid}&action=unban">&#128330; &#1056;&#1072;&#1079;&#1073;&#1072;&#1085;&#1080;&#1090;&#1100;</a>
        </div>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1059;&#1055;&#1056;&#1040;&#1042;&#1051;&#1045;&#1053;&#1048;&#1045; &#1055;&#1051;&#1040;&#1043;&#1048;&#1053;&#1040;&#1052;&#1048;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_plugins(request: web.Request):
    chats = await db.get_all_chats()

    if request.method == "POST":
        data    = await request.post()
        cid     = int(data.get("cid", 0))
        plugin  = data.get("plugin", "")
        enabled = data.get("enabled", "0") == "1"

        if cid and plugin:
            conn = db.get_conn()
            conn.execute(
                "INSERT INTO plugins (cid,key,enabled) VALUES (?,?,?) "
                "ON CONFLICT(cid,key) DO UPDATE SET enabled=?",
                (cid, plugin, 1 if enabled else 0, 1 if enabled else 0)
            )
            conn.commit()
            conn.close()
            # &#1051;&#1086;&#1075;&#1080;&#1088;&#1091;&#1077;&#1084;
            _log_admin_action(f"&#1055;&#1083;&#1072;&#1075;&#1080;&#1085; '{plugin}' {'&#1074;&#1082;&#1083;&#1102;&#1095;&#1105;&#1085;' if enabled else '&#1074;&#1099;&#1082;&#1083;&#1102;&#1095;&#1105;&#1085;'} &#1076;&#1083;&#1103; &#1095;&#1072;&#1090;&#1072; {cid}")
        raise web.HTTPFound("/dashboard/plugins")

    cid_filter = request.rel_url.query.get("cid")
    selected_cid = int(cid_filter) if cid_filter else (chats[0]["cid"] if chats else 0)

    PLUGINS = [
        ("economy",   "&#128176; &#1069;&#1082;&#1086;&#1085;&#1086;&#1084;&#1080;&#1082;&#1072;"),
        ("games",     "&#127918; &#1048;&#1075;&#1088;&#1099;"),
        ("xp",        "&#11088; XP &#1089;&#1080;&#1089;&#1090;&#1077;&#1084;&#1072;"),
        ("antispam",  "&#128737; &#1040;&#1085;&#1090;&#1080;&#1089;&#1087;&#1072;&#1084;"),
        ("antimat",   "&#129324; &#1040;&#1085;&#1090;&#1080;&#1084;&#1072;&#1090;"),
        ("reports",   "&#128680; &#1056;&#1077;&#1087;&#1086;&#1088;&#1090;&#1099;"),
        ("events",    "&#127881; &#1057;&#1086;&#1073;&#1099;&#1090;&#1080;&#1103;"),
        ("newspaper", "&#128240; &#1043;&#1072;&#1079;&#1077;&#1090;&#1072;"),
        ("clans",     "&#129309; &#1050;&#1083;&#1072;&#1085;&#1099;"),
    ]

    conn = db.get_conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS plugins
                        (cid INTEGER, key TEXT, enabled INTEGER DEFAULT 1,
                         PRIMARY KEY (cid, key))""")
        conn.commit()
    except: pass
    plugin_states = {}
    for key, _ in PLUGINS:
        try:
            row = conn.execute(
                "SELECT enabled FROM plugins WHERE cid=? AND key=?",
                (selected_cid, key)
            ).fetchone()
            plugin_states[key] = bool(row["enabled"]) if row else True
        except:
            plugin_states[key] = True
    conn.close()

    chat_opts = "".join(
        f'<option value="{r["cid"]}" {"selected" if r["cid"]==selected_cid else ""}>'
        f'{r["title"] or r["cid"]}</option>'
        for r in chats
    )

    rows = ""
    for key, label in PLUGINS:
        enabled = plugin_states.get(key, True)
        status_badge = '<span class="badge badge-open">&#1042;&#1082;&#1083;&#1102;&#1095;&#1105;&#1085;</span>' if enabled else '<span class="badge badge-closed">&#1042;&#1099;&#1082;&#1083;&#1102;&#1095;&#1077;&#1085;</span>'
        btn_class = "btn-danger" if enabled else "btn-success"
        btn_text  = "&#10060; &#1042;&#1099;&#1082;&#1083;&#1102;&#1095;&#1080;&#1090;&#1100;" if enabled else "&#9989; &#1042;&#1082;&#1083;&#1102;&#1095;&#1080;&#1090;&#1100;"
        new_val   = "0" if enabled else "1"
        rows += (
            f"<tr><td>{label}</td><td><code>{key}</code></td>"
            f"<td>{status_badge}</td><td>"
            f'<form method="POST" style="display:inline;">'
            f'<input type="hidden" name="cid" value="{selected_cid}">'
            f'<input type="hidden" name="plugin" value="{key}">'
            f'<input type="hidden" name="enabled" value="{new_val}">'
            f'<button class="btn btn-sm {btn_class}" type="submit">{btn_text}</button>'
            f"</form></td></tr>"
        )

    body = navbar("plugins") + f"""
    <div class="container">
      <div class="page-title">&#127899; &#1059;&#1087;&#1088;&#1072;&#1074;&#1083;&#1077;&#1085;&#1080;&#1077; &#1087;&#1083;&#1072;&#1075;&#1080;&#1085;&#1072;&#1084;&#1080;</div>
      <div style="margin-bottom:16px;">
        <form method="GET" style="display:flex;gap:12px;align-items:center;">
          <select class="form-control" name="cid" style="width:250px;">{chat_opts}</select>
          <button class="btn btn-primary" type="submit">&#1042;&#1099;&#1073;&#1088;&#1072;&#1090;&#1100;</button>
        </form>
      </div>
      <div class="section">
        <table>
          <thead><tr><th>&#1055;&#1083;&#1072;&#1075;&#1080;&#1085;</th><th>&#1050;&#1083;&#1102;&#1095;</th><th>&#1057;&#1090;&#1072;&#1090;&#1091;&#1089;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1056;&#1040;&#1057;&#1057;&#1067;&#1051;&#1050;&#1040;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_broadcast(request: web.Request):
    chats = await db.get_all_chats()
    result = ""

    if request.method == "POST":
        data    = await request.post()
        text    = data.get("text", "").strip()
        target  = data.get("target", "all")
        cid_sel = data.get("cid", "")

        if text and _bot:
            sent = 0
            failed = 0
            targets = []

            if target == "all":
                targets = [r["cid"] for r in chats]
            elif target == "one" and cid_sel:
                targets = [int(cid_sel)]

            for cid in targets:
                try:
                    await _bot.send_message(
                        cid,
                        f"&#128226; <b>&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1077; &#1086;&#1090; &#1072;&#1076;&#1084;&#1080;&#1085;&#1080;&#1089;&#1090;&#1088;&#1072;&#1094;&#1080;&#1080;</b>\n\n{text}",
                        parse_mode="HTML"
                    )
                    sent += 1
                    await asyncio.sleep(0.1)
                except:
                    failed += 1

            _log_admin_action(f"&#1056;&#1072;&#1089;&#1089;&#1099;&#1083;&#1082;&#1072; &#1086;&#1090;&#1087;&#1088;&#1072;&#1074;&#1083;&#1077;&#1085;&#1072; &#1074; {sent} &#1095;&#1072;&#1090;&#1086;&#1074;: {text[:50]}")
            result = f'<div style="padding:12px;background:#1a3a1a;border-radius:8px;margin-bottom:16px;">&#9989; &#1054;&#1090;&#1087;&#1088;&#1072;&#1074;&#1083;&#1077;&#1085;&#1086; &#1074; <b>{sent}</b> &#1095;&#1072;&#1090;&#1086;&#1074;. &#1054;&#1096;&#1080;&#1073;&#1086;&#1082;: {failed}</div>'

    chat_opts = "".join(
        f'<option value="{r["cid"]}">{r["title"] or r["cid"]}</option>'
        for r in chats
    )

    body = navbar("broadcast") + f"""
    <div class="container">
      <div class="page-title">&#128276; &#1056;&#1072;&#1089;&#1089;&#1099;&#1083;&#1082;&#1072;</div>
      {result}
      <div class="section" style="max-width:600px;">
        <div class="section-header">&#1054;&#1090;&#1087;&#1088;&#1072;&#1074;&#1080;&#1090;&#1100; &#1089;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1077;</div>
        <div style="padding:20px;">
          <form method="POST">
            <div class="form-group">
              <label>&#1055;&#1086;&#1083;&#1091;&#1095;&#1072;&#1090;&#1077;&#1083;&#1080;</label>
              <select class="form-control" name="target" onchange="document.getElementById('cid_row').style.display=this.value=='one'?'block':'none'">
                <option value="all">&#1042;&#1089;&#1077; &#1095;&#1072;&#1090;&#1099; ({len(chats)})</option>
                <option value="one">&#1050;&#1086;&#1085;&#1082;&#1088;&#1077;&#1090;&#1085;&#1099;&#1081; &#1095;&#1072;&#1090;</option>
              </select>
            </div>
            <div class="form-group" id="cid_row" style="display:none;">
              <label>&#1063;&#1072;&#1090;</label>
              <select class="form-control" name="cid">{chat_opts}</select>
            </div>
            <div class="form-group">
              <label>&#1058;&#1077;&#1082;&#1089;&#1090; &#1089;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1103;</label>
              <textarea class="form-control" name="text" rows="5" placeholder="&#1055;&#1086;&#1076;&#1076;&#1077;&#1088;&#1078;&#1080;&#1074;&#1072;&#1077;&#1090;&#1089;&#1103; HTML: &lt;b&gt;, &lt;i&gt;, &lt;code&gt;"></textarea>
            </div>
            <button class="btn btn-primary" type="submit" style="width:100%;padding:12px;">
              &#128228; &#1054;&#1090;&#1087;&#1088;&#1072;&#1074;&#1080;&#1090;&#1100;
            </button>
          </form>
        </div>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1052;&#1045;&#1044;&#1048;&#1040; &#1051;&#1054;&#1043;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

# &#1061;&#1088;&#1072;&#1085;&#1080;&#1083;&#1080;&#1097;&#1077; &#1084;&#1077;&#1076;&#1080;&#1072; &#1074; &#1087;&#1072;&#1084;&#1103;&#1090;&#1080; (&#1087;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; 500)
_media_log: list = []


def log_media(cid: int, uid: int, name: str, chat_title: str,
              media_type: str, file_id: str = ""):
    """&#1051;&#1086;&#1075;&#1080;&#1088;&#1091;&#1077;&#1090; &#1084;&#1077;&#1076;&#1080;&#1072; &#1092;&#1072;&#1081;&#1083;"""
    _media_log.insert(0, {
        "cid": cid, "uid": uid, "name": name,
        "chat": chat_title, "type": media_type,
        "file_id": file_id,
        "time": datetime.now().strftime("%d.%m %H:%M")
    })
    if len(_media_log) > 500:
        _media_log.pop()


@require_auth
async def handle_media(request: web.Request):
    media_type_filter = request.rel_url.query.get("type", "all")

    items = _media_log
    if media_type_filter != "all":
        items = [m for m in _media_log if m["type"] == media_type_filter]

    type_counts = {}
    for m in _media_log:
        type_counts[m["type"]] = type_counts.get(m["type"], 0) + 1

    tabs = ""
    for t, count in [("all", len(_media_log)), ("photo", type_counts.get("photo",0)),
                     ("video", type_counts.get("video",0)), ("document", type_counts.get("document",0)),
                     ("voice", type_counts.get("voice",0)), ("sticker", type_counts.get("sticker",0))]:
        active = "btn-primary" if media_type_filter == t else "btn-outline"
        tabs += f'<a href="?type={t}" class="btn btn-sm {active}" style="margin-right:6px;">{t} ({count})</a>'

    TYPE_EMOJI = {"photo":"&#128444;","video":"&#127909;","document":"&#128196;","voice":"&#127908;","sticker":"&#127917;","animation":"&#127916;"}

    rows = ""
    for m in items[:100]:
        emoji = TYPE_EMOJI.get(m["type"], "&#128193;")
        rows += (
            f"<tr>"
            f"<td>{m['time']}</td>"
            f"<td>{emoji} {m['type']}</td>"
            f"<td>{m['name']}</td>"
            f"<td>{m['chat']}</td>"
            f"</tr>"
        )
    if not rows:
        rows = "<tr><td colspan='4' class='empty-state'>&#1052;&#1077;&#1076;&#1080;&#1072; &#1085;&#1077; &#1086;&#1073;&#1085;&#1072;&#1088;&#1091;&#1078;&#1077;&#1085;&#1086;</td></tr>"

    body = navbar("media") + f"""
    <div class="container">
      <div class="page-title">&#127902; &#1052;&#1077;&#1076;&#1080;&#1072; &#1083;&#1086;&#1075; ({len(items)})</div>
      <div style="margin-bottom:16px;">{tabs}</div>
      <div class="section">
        <table>
          <thead><tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1058;&#1080;&#1087;</th><th>&#1054;&#1090;&#1087;&#1088;&#1072;&#1074;&#1080;&#1090;&#1077;&#1083;&#1100;</th><th>&#1063;&#1072;&#1090;</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1051;&#1054;&#1043; &#1044;&#1045;&#1049;&#1057;&#1058;&#1042;&#1048;&#1049; &#1044;&#1040;&#1064;&#1041;&#1054;&#1056;&#1044;&#1040;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

_admin_log: list = []


def _log_admin_action(action: str):
    _admin_log.insert(0, {
        "action": action,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M")
    })
    if len(_admin_log) > 200:
        _admin_log.pop()


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1069;&#1050;&#1057;&#1055;&#1054;&#1056;&#1058; CSV
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_export(request: web.Request):
    export_type = request.match_info.get("type", "stats")
    chats = await db.get_all_chats()

    conn = db.get_conn()

    if export_type == "stats":
        rows = conn.execute(
            "SELECT k.title, cs.uid, cs.msg_count, "
            "COALESCE(r.score,0) as rep, COALESCE(w.count,0) as warns "
            "FROM chat_stats cs "
            "LEFT JOIN known_chats k ON cs.cid=k.cid "
            "LEFT JOIN reputation r ON cs.cid=r.cid AND cs.uid=r.uid "
            "LEFT JOIN warnings w ON cs.cid=w.cid AND cs.uid=w.uid "
            "ORDER BY cs.msg_count DESC LIMIT 1000"
        ).fetchall()
        conn.close()

        csv = "&#1063;&#1072;&#1090;,UserID,&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;,&#1056;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1103;,&#1042;&#1072;&#1088;&#1085;&#1099;\n"
        for r in rows:
            csv += f"{r['title'] or ''},{ r['uid']},{r['msg_count']},{r['rep']},{r['warns']}\n"

    elif export_type == "bans":
        rows = conn.execute(
            "SELECT k.title, b.uid, b.name, b.reason, b.banned_by, b.banned_at "
            "FROM ban_list b LEFT JOIN known_chats k ON b.cid=k.cid"
        ).fetchall()
        conn.close()

        csv = "&#1063;&#1072;&#1090;,UserID,&#1048;&#1084;&#1103;,&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;,&#1050;&#1090;&#1086; &#1079;&#1072;&#1073;&#1072;&#1085;&#1080;&#1083;,&#1044;&#1072;&#1090;&#1072;\n"
        for r in rows:
            csv += f"{r['title'] or ''},{r['uid']},{r['name'] or ''},{r['reason'] or ''},{r['banned_by'] or ''},{str(r['banned_at'])[:10]}\n"

    elif export_type == "modhistory":
        rows = conn.execute(
            "SELECT k.title, m.uid, m.action, m.reason, m.by_name, m.created_at "
            "FROM mod_history m LEFT JOIN known_chats k ON m.cid=k.cid "
            "ORDER BY m.created_at DESC LIMIT 2000"
        ).fetchall()
        conn.close()

        csv = "&#1063;&#1072;&#1090;,UserID,&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;,&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;,&#1050;&#1090;&#1086;,&#1044;&#1072;&#1090;&#1072;\n"
        for r in rows:
            csv += f"{r['title'] or ''},{r['uid']},{r['action']},{r['reason'] or ''},{r['by_name']},{str(r['created_at'])[:16]}\n"
    else:
        conn.close()
        raise web.HTTPNotFound()

    _log_admin_action(f"&#1069;&#1082;&#1089;&#1087;&#1086;&#1088;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;: {export_type}")
    return web.Response(
        text=csv,
        content_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={export_type}_{datetime.now().strftime('%d%m%Y')}.csv"}
    )


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1053;&#1040;&#1057;&#1058;&#1056;&#1054;&#1049;&#1050;&#1048; &#1044;&#1040;&#1064;&#1041;&#1054;&#1056;&#1044;&#1040;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

_dashboard_settings = {
    "alerts_enabled":     True,
    "media_log_enabled":  True,
    "spam_threshold":     10,
    "flood_threshold":    15,
    "show_user_ids":      True,
    "auto_refresh":       False,
    "items_per_page":     20,
}


@require_auth
async def handle_settings(request: web.Request):
    global _dashboard_settings

    if request.method == "POST":
        data = await request.post()
        _dashboard_settings["alerts_enabled"]    = data.get("alerts_enabled") == "1"
        _dashboard_settings["media_log_enabled"] = data.get("media_log_enabled") == "1"
        _dashboard_settings["spam_threshold"]    = int(data.get("spam_threshold", 10))
        _dashboard_settings["flood_threshold"]   = int(data.get("flood_threshold", 15))
        _dashboard_settings["show_user_ids"]     = data.get("show_user_ids") == "1"
        _dashboard_settings["auto_refresh"]      = data.get("auto_refresh") == "1"
        _dashboard_settings["items_per_page"]    = int(data.get("items_per_page", 20))
        _log_admin_action("&#1053;&#1072;&#1089;&#1090;&#1088;&#1086;&#1081;&#1082;&#1080; &#1076;&#1072;&#1096;&#1073;&#1086;&#1088;&#1076;&#1072; &#1086;&#1073;&#1085;&#1086;&#1074;&#1083;&#1077;&#1085;&#1099;")
        raise web.HTTPFound("/dashboard/settings")

    def toggle(key):
        val = _dashboard_settings.get(key, True)
        return f"""
        <label class="toggle-switch">
          <input type="checkbox" name="{key}" value="1" {'checked' if val else ''}>
          <span class="toggle-slider"></span>
        </label>"""

    s = _dashboard_settings
    admin_log_html = "".join(
        f"<tr><td>{r['time']}</td><td>{r['action']}</td></tr>"
        for r in _admin_log[:20]
    ) or "<tr><td colspan='2' class='empty-state'>&#1053;&#1077;&#1090; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1081;</td></tr>"

    body = navbar("settings") + f"""
    <div class="container">
      <div class="page-title">&#9881; &#1053;&#1072;&#1089;&#1090;&#1088;&#1086;&#1081;&#1082;&#1080; &#1076;&#1072;&#1096;&#1073;&#1086;&#1088;&#1076;&#1072;</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
        <div>
          <div class="section">
            <div class="section-header">&#128295; &#1060;&#1091;&#1085;&#1082;&#1094;&#1080;&#1080;</div>
            <form method="POST" style="padding:20px;">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                <div><b>&#128308; &#1040;&#1083;&#1077;&#1088;&#1090;&#1099;</b><div style="font-size:12px;color:var(--text2);">&#1044;&#1077;&#1090;&#1077;&#1082;&#1090;&#1086;&#1088; &#1089;&#1087;&#1072;&#1084;&#1072; &#1080; &#1092;&#1083;&#1091;&#1076;&#1072;</div></div>
                {toggle("alerts_enabled")}
              </div>
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                <div><b>&#127902; &#1052;&#1077;&#1076;&#1080;&#1072; &#1083;&#1086;&#1075;</b><div style="font-size:12px;color:var(--text2);">&#1051;&#1086;&#1075;&#1080;&#1088;&#1086;&#1074;&#1072;&#1090;&#1100; &#1092;&#1086;&#1090;&#1086;/&#1074;&#1080;&#1076;&#1077;&#1086;</div></div>
                {toggle("media_log_enabled")}
              </div>
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                <div><b>&#128100; &#1055;&#1086;&#1082;&#1072;&#1079;&#1099;&#1074;&#1072;&#1090;&#1100; ID</b><div style="font-size:12px;color:var(--text2);">ID &#1087;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1077;&#1081; &#1074; &#1090;&#1072;&#1073;&#1083;&#1080;&#1094;&#1072;&#1093;</div></div>
                {toggle("show_user_ids")}
              </div>
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
                <div><b>&#128260; &#1040;&#1074;&#1090;&#1086;-&#1086;&#1073;&#1085;&#1086;&#1074;&#1083;&#1077;&#1085;&#1080;&#1077;</b><div style="font-size:12px;color:var(--text2);">&#1054;&#1073;&#1085;&#1086;&#1074;&#1083;&#1103;&#1090;&#1100; &#1089;&#1090;&#1088;&#1072;&#1085;&#1080;&#1094;&#1091; &#1082;&#1072;&#1078;&#1076;&#1099;&#1077; 30&#1089;</div></div>
                {toggle("auto_refresh")}
              </div>
              <div class="form-group">
                <label>&#9888; &#1055;&#1086;&#1088;&#1086;&#1075; &#1089;&#1087;&#1072;&#1084;&#1072; (&#1089;&#1086;&#1086;&#1073;&#1097;/&#1084;&#1080;&#1085;)</label>
                <input class="form-control" type="number" name="spam_threshold" value="{s['spam_threshold']}" min="5" max="60">
              </div>
              <div class="form-group">
                <label>&#128680; &#1055;&#1086;&#1088;&#1086;&#1075; &#1092;&#1083;&#1091;&#1076;&#1072; (&#1089;&#1086;&#1086;&#1073;&#1097;/&#1084;&#1080;&#1085;)</label>
                <input class="form-control" type="number" name="flood_threshold" value="{s['flood_threshold']}" min="5" max="100">
              </div>
              <div class="form-group">
                <label>&#128196; &#1047;&#1072;&#1087;&#1080;&#1089;&#1077;&#1081; &#1085;&#1072; &#1089;&#1090;&#1088;&#1072;&#1085;&#1080;&#1094;&#1077;</label>
                <select class="form-control" name="items_per_page">
                  {''.join(f'<option value="{n}" {"selected" if s["items_per_page"]==n else ""}>{n}</option>' for n in [10,20,50,100])}
                </select>
              </div>
              <button class="btn btn-primary" type="submit" style="width:100%;">&#128190; &#1057;&#1086;&#1093;&#1088;&#1072;&#1085;&#1080;&#1090;&#1100;</button>
            </form>
          </div>
          <div class="section" style="margin-top:16px;">
            <div class="section-header">&#128228; &#1069;&#1082;&#1089;&#1087;&#1086;&#1088;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</div>
            <div style="padding:16px;display:flex;flex-direction:column;gap:8px;">
              <a href="/dashboard/export/stats"      class="btn btn-outline">&#128202; &#1057;&#1090;&#1072;&#1090;&#1080;&#1089;&#1090;&#1080;&#1082;&#1072; &#1091;&#1095;&#1072;&#1089;&#1090;&#1085;&#1080;&#1082;&#1086;&#1074; (.csv)</a>
              <a href="/dashboard/export/bans"       class="btn btn-outline">&#128296; &#1057;&#1087;&#1080;&#1089;&#1086;&#1082; &#1073;&#1072;&#1085;&#1086;&#1074; (.csv)</a>
              <a href="/dashboard/export/modhistory" class="btn btn-outline">&#128203; &#1048;&#1089;&#1090;&#1086;&#1088;&#1080;&#1103; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1080; (.csv)</a>
            </div>
          </div>
        </div>
        <div class="section">
          <div class="section-header">&#128221; &#1051;&#1086;&#1075; &#1076;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1081; &#1074; &#1076;&#1072;&#1096;&#1073;&#1086;&#1088;&#1076;&#1077;</div>
          <table>
            <thead><tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</th></tr></thead>
            <tbody>{admin_log_html}</tbody>
          </table>
        </div>
      </div>
    </div>
    {'<script>setTimeout(function(){{location.reload()}},30000)</script>' if s.get("auto_refresh") else ''}
    """
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1056;&#1045;&#1055;&#1054;&#1056;&#1058;&#1067;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_reports(request: web.Request):
    action = request.rel_url.query.get("action")
    rid    = request.rel_url.query.get("id")

    conn = db.get_conn()
    # &#1055;&#1088;&#1086;&#1073;&#1091;&#1077;&#1084; &#1088;&#1072;&#1079;&#1085;&#1099;&#1077; &#1089;&#1090;&#1088;&#1091;&#1082;&#1090;&#1091;&#1088;&#1099; &#1090;&#1072;&#1073;&#1083;&#1080;&#1094;&#1099; reports
    try:
        reports = conn.execute(
            "SELECT * FROM reports ORDER BY rowid DESC LIMIT 50"
        ).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM reports LIMIT 1").description or []]
    except:
        reports = []
        cols = []
    conn.close()

    if not reports:
        body = navbar("reports") + """
        <div class="container">
          <div class="page-title">&#128680; &#1056;&#1077;&#1087;&#1086;&#1088;&#1090;&#1099;</div>
          <div class="section"><div class="empty-state" style="padding:40px;">&#1056;&#1077;&#1087;&#1086;&#1088;&#1090;&#1086;&#1074; &#1085;&#1077;&#1090;</div></div>
        </div>"""
        return web.Response(text=page(body), content_type="text/html")

    rows = ""
    for r in [dict(zip(cols, row)) if not hasattr(r, 'keys') else dict(r) for r in reports]:
        reporter = r.get("reporter_name") or r.get("by") or r.get("uid") or "&#8212;"
        target   = r.get("target_name") or r.get("target") or r.get("target_uid") or "&#8212;"
        reason   = r.get("reason") or r.get("text") or "&#8212;"
        status   = r.get("status", "open")
        chat     = r.get("chat_title") or r.get("cid") or "&#8212;"
        rid_val  = r.get("id") or r.get("rowid", "")
        dt       = r.get("created_at") or r.get("time") or "&#8212;"
        if isinstance(dt, str) and len(dt) > 16:
            dt = dt[:16]

        st_badge = f'<span class="badge badge-{"open" if status=="open" else "closed"}">{status}</span>'
        rows += (
            f"<tr>"
            f"<td>{reporter}</td>"
            f"<td>{target}</td>"
            f"<td>{reason[:50]}</td>"
            f"<td>{chat}</td>"
            f"<td>{st_badge}</td>"
            f"<td>{dt}</td>"
            f"<td>"
            f"<a class='btn btn-sm btn-danger' href='/dashboard/reports/ban?id={rid_val}'>&#128296; &#1041;&#1072;&#1085;</a> "
            f"<a class='btn btn-sm btn-primary' href='/dashboard/reports/mute?id={rid_val}'>&#128263; &#1052;&#1091;&#1090;</a>"
            f"</td>"
            f"</tr>"
        )

    body = navbar("reports") + f"""
    <div class="container">
      <div class="page-title">&#128680; &#1056;&#1077;&#1087;&#1086;&#1088;&#1090;&#1099; ({len(reports)})</div>
      <div class="section">
        <table>
          <thead>
            <tr><th>&#1054;&#1090; &#1082;&#1086;&#1075;&#1086;</th><th>&#1053;&#1072; &#1082;&#1086;&#1075;&#1086;</th><th>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</th><th>&#1063;&#1072;&#1090;</th><th>&#1057;&#1090;&#1072;&#1090;&#1091;&#1089;</th><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103;</th></tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1041;&#1040;&#1053;/&#1052;&#1059;&#1058; &#1048;&#1047; &#1044;&#1040;&#1064;&#1041;&#1054;&#1056;&#1044;&#1040;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_ban_from_dashboard(request: web.Request):
    """&#1057;&#1090;&#1088;&#1072;&#1085;&#1080;&#1094;&#1072; &#1073;&#1072;&#1085;&#1072; &#1087;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1103; &#1087;&#1088;&#1103;&#1084;&#1086; &#1080;&#1079; &#1076;&#1072;&#1096;&#1073;&#1086;&#1088;&#1076;&#1072;"""
    if request.method == "POST":
        data    = await request.post()
        uid     = int(data.get("uid", 0))
        cid     = int(data.get("cid", 0))
        reason  = data.get("reason", "&#1053;&#1072;&#1088;&#1091;&#1096;&#1077;&#1085;&#1080;&#1077; &#1087;&#1088;&#1072;&#1074;&#1080;&#1083;")
        action  = data.get("action", "ban")

        if uid and cid and _bot:
            try:
                if action == "ban":
                    await _bot.ban_chat_member(cid, uid)
                    result = f"&#9989; &#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100; {uid} &#1079;&#1072;&#1073;&#1072;&#1085;&#1077;&#1085; &#1074; &#1095;&#1072;&#1090;&#1077; {cid}"
                elif action == "mute":
                    from datetime import timedelta
                    from aiogram.types import ChatPermissions
                    until = __import__("datetime").datetime.now() + timedelta(hours=1)
                    await _bot.restrict_chat_member(
                        cid, uid,
                        permissions=ChatPermissions(can_send_messages=False),
                        until_date=until
                    )
                    result = f"&#9989; &#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100; {uid} &#1079;&#1072;&#1084;&#1091;&#1095;&#1077;&#1085; &#1085;&#1072; 1 &#1095;&#1072;&#1089; &#1074; &#1095;&#1072;&#1090;&#1077; {cid}"
                elif action == "unban":
                    await _bot.unban_chat_member(cid, uid)
                    result = f"&#9989; &#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100; {uid} &#1088;&#1072;&#1079;&#1073;&#1072;&#1085;&#1077;&#1085; &#1074; &#1095;&#1072;&#1090;&#1077; {cid}"
            except Exception as e:
                result = f"&#10060; &#1054;&#1096;&#1080;&#1073;&#1082;&#1072;: {e}"
        else:
            result = "&#10060; &#1053;&#1077; &#1091;&#1082;&#1072;&#1079;&#1072;&#1085; uid &#1080;&#1083;&#1080; cid"

        chats = await db.get_all_chats()
        chat_opts = "".join(
            f'<option value="{r["cid"]}">{r["title"] or r["cid"]}</option>'
            for r in chats
        )
        body = navbar("moderation") + f"""
        <div class="container">
          <div class="page-title">&#128296; &#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077; &#1074;&#1099;&#1087;&#1086;&#1083;&#1085;&#1077;&#1085;&#1086;</div>
          <div class="section" style="padding:20px;">
            <p style="font-size:16px;">{result}</p>
            <a class="btn btn-primary" style="margin-top:16px;" href="/dashboard/modaction">&larr; &#1053;&#1072;&#1079;&#1072;&#1076;</a>
          </div>
        </div>"""
        return web.Response(text=page(body), content_type="text/html")

    # GET &#8212; &#1087;&#1086;&#1082;&#1072;&#1079;&#1099;&#1074;&#1072;&#1077;&#1084; &#1092;&#1086;&#1088;&#1084;&#1091;
    chats = await db.get_all_chats()
    chat_opts = "".join(
        f'<option value="{r["cid"]}">{r["title"] or r["cid"]}</option>'
        for r in chats
    )

    body = navbar("moderation") + f"""
    <div class="container">
      <div class="page-title">&#128296; &#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1103; &#1084;&#1086;&#1076;&#1077;&#1088;&#1072;&#1094;&#1080;&#1080;</div>
      <div class="section" style="max-width:500px;">
        <div class="section-header">&#1041;&#1072;&#1085; / &#1052;&#1091;&#1090; / &#1056;&#1072;&#1079;&#1073;&#1072;&#1085;</div>
        <div style="padding:20px;">
          <form method="POST">
            <div class="form-group">
              <label>Telegram ID &#1087;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1103;</label>
              <input class="form-control" type="number" name="uid" placeholder="123456789" required>
            </div>
            <div class="form-group">
              <label>&#1063;&#1072;&#1090;</label>
              <select class="form-control" name="cid">
                {chat_opts}
              </select>
            </div>
            <div class="form-group">
              <label>&#1055;&#1088;&#1080;&#1095;&#1080;&#1085;&#1072;</label>
              <input class="form-control" type="text" name="reason" value="&#1053;&#1072;&#1088;&#1091;&#1096;&#1077;&#1085;&#1080;&#1077; &#1087;&#1088;&#1072;&#1074;&#1080;&#1083;">
            </div>
            <div class="form-group">
              <label>&#1044;&#1077;&#1081;&#1089;&#1090;&#1074;&#1080;&#1077;</label>
              <select class="form-control" name="action">
                <option value="ban">&#128296; &#1041;&#1072;&#1085;</option>
                <option value="mute">&#128263; &#1052;&#1091;&#1090; (1 &#1095;&#1072;&#1089;)</option>
                <option value="unban">&#128330; &#1056;&#1072;&#1079;&#1073;&#1072;&#1085;</option>
              </select>
            </div>
            <button class="btn btn-danger" type="submit" style="width:100%;padding:12px;">
              &#1042;&#1099;&#1087;&#1086;&#1083;&#1085;&#1080;&#1090;&#1100;
            </button>
          </form>
        </div>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1051;&#1054;&#1043;&#1048; &#1059;&#1044;&#1040;&#1051;&#1025;&#1053;&#1053;&#1067;&#1061; &#1057;&#1054;&#1054;&#1041;&#1065;&#1045;&#1053;&#1048;&#1049;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_deleted(request: web.Request):
    cid_filter = request.rel_url.query.get("cid")
    chats = await db.get_all_chats()

    conn = db.get_conn()
    try:
        if cid_filter:
            rows = conn.execute(
                "SELECT * FROM deleted_log WHERE cid=? ORDER BY deleted_at DESC LIMIT 100",
                (cid_filter,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM deleted_log ORDER BY deleted_at DESC LIMIT 100"
            ).fetchall()
    except:
        rows = []
    conn.close()

    chat_filter_opts = '<option value="">&#1042;&#1089;&#1077; &#1095;&#1072;&#1090;&#1099;</option>' + "".join(
        f'<option value="{r["cid"]}" {"selected" if str(r["cid"])==str(cid_filter) else ""}>'
        f'{r["title"] or r["cid"]}</option>'
        for r in chats
    )

    msg_rows = ""
    for r in [dict(r) for r in rows]:
        dt   = str(r.get("deleted_at",""))[:16]
        name = r.get("name") or r.get("uid") or "&#8212;"
        text = r.get("text","")[:200]
        cid  = r.get("cid","")
        msg_rows += (
            f"<tr>"
            f"<td>{dt}</td>"
            f"<td>{name}</td>"
            f"<td style='max-width:400px;word-break:break-word;'>{text}</td>"
            f"<td>{cid}</td>"
            f"</tr>"
        )

    if not msg_rows:
        msg_rows = "<tr><td colspan='4' class='empty-state'>&#1059;&#1076;&#1072;&#1083;&#1105;&#1085;&#1085;&#1099;&#1093; &#1089;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081; &#1085;&#1077;&#1090;</td></tr>"

    body = navbar("deleted") + f"""
    <div class="container">
      <div class="page-title">&#128203; &#1051;&#1086;&#1075;&#1080; &#1091;&#1076;&#1072;&#1083;&#1105;&#1085;&#1085;&#1099;&#1093; &#1089;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1081;</div>
      <div style="margin-bottom:16px;">
        <form method="GET" style="display:flex;gap:12px;align-items:center;">
          <select class="form-control" name="cid" style="width:250px;">
            {chat_filter_opts}
          </select>
          <button class="btn btn-primary" type="submit">&#1060;&#1080;&#1083;&#1100;&#1090;&#1088;</button>
        </form>
      </div>
      <div class="section">
        <table>
          <thead>
            <tr><th>&#1042;&#1088;&#1077;&#1084;&#1103;</th><th>&#1055;&#1086;&#1083;&#1100;&#1079;&#1086;&#1074;&#1072;&#1090;&#1077;&#1083;&#1100;</th><th>&#1057;&#1086;&#1086;&#1073;&#1097;&#1077;&#1085;&#1080;&#1077;</th><th>&#1063;&#1072;&#1090; ID</th></tr>
          </thead>
          <tbody>{msg_rows}</tbody>
        </table>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1069;&#1050;&#1054;&#1053;&#1054;&#1052;&#1048;&#1050;&#1040;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

@require_auth
async def handle_economy(request: web.Request):
    cid_filter = request.rel_url.query.get("cid")
    chats = await db.get_all_chats()

    conn = db.get_conn()

    if cid_filter:
        top_rep_rows = conn.execute(
            "SELECT uid, score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT 20",
            (cid_filter,)
        ).fetchall()
        top_xp_rows = conn.execute(
            "SELECT uid, xp FROM xp_data WHERE cid=? ORDER BY xp DESC LIMIT 20",
            (cid_filter,)
        ).fetchall()
    else:
        top_rep_rows = conn.execute(
            "SELECT uid, SUM(score) as score FROM reputation GROUP BY uid ORDER BY score DESC LIMIT 20"
        ).fetchall()
        top_xp_rows = conn.execute(
            "SELECT uid, SUM(xp) as xp FROM xp_data GROUP BY uid ORDER BY xp DESC LIMIT 20"
        ).fetchall()

    # &#1040;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1100; &#1087;&#1086; &#1076;&#1085;&#1103;&#1084; (&#1087;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; 30 &#1076;&#1085;&#1077;&#1081;)
    try:
        activity_rows = conn.execute(
            "SELECT day, SUM(count) as total FROM user_activity "
            + ("WHERE cid=? " if cid_filter else "")
            + "GROUP BY day ORDER BY day DESC LIMIT 30",
            *((cid_filter,) if cid_filter else ())
        ).fetchall()
    except:
        activity_rows = []

    conn.close()

    chat_opts = '<option value="">&#1042;&#1089;&#1077; &#1095;&#1072;&#1090;&#1099;</option>' + "".join(
        f'<option value="{r["cid"]}" {"selected" if str(r["cid"])==str(cid_filter or "") else ""}>'
        f'{r["title"] or r["cid"]}</option>'
        for r in chats
    )

    rep_rows = "".join(
        f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r['score']:+d}</td></tr>"
        for i, r in enumerate([dict(x) for x in top_rep_rows], 1)
    ) or "<tr><td colspan='3'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    xp_rows = "".join(
        f"<tr><td>{i}</td><td><code>{r['uid']}</code></td><td>{r['xp']:,}</td></tr>"
        for i, r in enumerate([dict(x) for x in top_xp_rows], 1)
    ) or "<tr><td colspan='3'>&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</td></tr>"

    # &#1043;&#1088;&#1072;&#1092;&#1080;&#1082; &#1072;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1080; &#1087;&#1086; &#1076;&#1085;&#1103;&#1084;
    act_data = [dict(r) for r in activity_rows][::-1]  # &#1093;&#1088;&#1086;&#1085;&#1086;&#1083;&#1086;&#1075;&#1080;&#1095;&#1077;&#1089;&#1082;&#1080;&#1081; &#1087;&#1086;&#1088;&#1103;&#1076;&#1086;&#1082;
    max_act  = max((r["total"] for r in act_data), default=1)
    bars = ""
    for r in act_data[-14:]:  # &#1087;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; 14 &#1076;&#1085;&#1077;&#1081;
        pct = int((r["total"] / max_act) * 100) if max_act else 0
        day = str(r.get("day",""))[-5:]  # MM-DD
        bars += (
            f'<div style="display:inline-block;vertical-align:bottom;width:6%;margin:0 0.5%;'
            f'background:#7c6fcd;opacity:{max(0.3, pct/100):.2f};'
            f'height:{max(4,pct)}px;border-radius:3px 3px 0 0;" title="{day}: {r["total"]}"></div>'
        )

    body = navbar("economy") + f"""
    <div class="container">
      <div class="page-title">&#128176; &#1069;&#1082;&#1086;&#1085;&#1086;&#1084;&#1080;&#1082;&#1072;</div>
      <div style="margin-bottom:20px;">
        <form method="GET" style="display:flex;gap:12px;align-items:center;">
          <select class="form-control" name="cid" style="width:250px;">{chat_opts}</select>
          <button class="btn btn-primary" type="submit">&#1060;&#1080;&#1083;&#1100;&#1090;&#1088;</button>
        </form>
      </div>

      <div class="section" style="margin-bottom:24px;">
        <div class="section-header">&#128200; &#1040;&#1082;&#1090;&#1080;&#1074;&#1085;&#1086;&#1089;&#1090;&#1100; &#1079;&#1072; &#1087;&#1086;&#1089;&#1083;&#1077;&#1076;&#1085;&#1080;&#1077; 14 &#1076;&#1085;&#1077;&#1081;</div>
        <div style="padding:20px;">
          <div style="height:100px;display:flex;align-items:flex-end;">
            {bars or '<span style="color:#555;">&#1053;&#1077;&#1090; &#1076;&#1072;&#1085;&#1085;&#1099;&#1093;</span>'}
          </div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;">
        <div class="section">
          <div class="section-header">&#11088; &#1058;&#1086;&#1087; &#1088;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1080;</div>
          <table>
            <thead><tr><th>#</th><th>ID</th><th>&#1056;&#1077;&#1087;&#1091;&#1090;&#1072;&#1094;&#1080;&#1103;</th></tr></thead>
            <tbody>{rep_rows}</tbody>
          </table>
        </div>
        <div class="section">
          <div class="section-header">&#127942; &#1058;&#1086;&#1087; XP</div>
          <table>
            <thead><tr><th>#</th><th>ID</th><th>XP</th></tr></thead>
            <tbody>{xp_rows}</tbody>
          </table>
        </div>
      </div>
    </div>"""
    return web.Response(text=page(body), content_type="text/html")


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  SSE &#8212; &#1059;&#1042;&#1045;&#1044;&#1054;&#1052;&#1051;&#1045;&#1053;&#1048;&#1071; &#1054; &#1053;&#1054;&#1042;&#1067;&#1061; &#1058;&#1048;&#1050;&#1045;&#1058;&#1040;&#1061;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

# &#1057;&#1087;&#1080;&#1089;&#1086;&#1082; &#1087;&#1086;&#1076;&#1082;&#1083;&#1102;&#1095;&#1105;&#1085;&#1085;&#1099;&#1093; SSE &#1082;&#1083;&#1080;&#1077;&#1085;&#1090;&#1086;&#1074;
_sse_clients: list = []


async def handle_sse(request: web.Request):
    """Server-Sent Events &#1076;&#1083;&#1103; &#1091;&#1074;&#1077;&#1076;&#1086;&#1084;&#1083;&#1077;&#1085;&#1080;&#1081; &#1074; &#1073;&#1088;&#1072;&#1091;&#1079;&#1077;&#1088;&#1077;"""
    token = request.cookies.get("dtoken") or request.rel_url.query.get("token")
    if token != DASHBOARD_TOKEN:
        raise web.HTTPUnauthorized()

    response = web.StreamResponse()
    response.headers["Content-Type"] = "text/event-stream"
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    await response.prepare(request)

    queue = asyncio.Queue()
    _sse_clients.append(queue)

    try:
        await response.write(b"data: connected\n\n")
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=30)
                await response.write(f"data: {msg}\n\n".encode())
            except asyncio.TimeoutError:
                await response.write(b": ping\n\n")
    except Exception:
        pass
    finally:
        _sse_clients.remove(queue)

    return response


async def notify_new_ticket_sse(ticket_id: int, user_name: str, subject: str):
    """&#1042;&#1099;&#1079;&#1099;&#1074;&#1072;&#1077;&#1090;&#1089;&#1103; &#1082;&#1086;&#1075;&#1076;&#1072; &#1089;&#1086;&#1079;&#1076;&#1072;&#1105;&#1090;&#1089;&#1103; &#1085;&#1086;&#1074;&#1099;&#1081; &#1090;&#1080;&#1082;&#1077;&#1090; &#8212; &#1091;&#1074;&#1077;&#1076;&#1086;&#1084;&#1083;&#1103;&#1077;&#1090; &#1074;&#1089;&#1077; &#1086;&#1090;&#1082;&#1088;&#1099;&#1090;&#1099;&#1077; &#1076;&#1072;&#1096;&#1073;&#1086;&#1088;&#1076;&#1099;"""
    import json as _j
    msg = _j.dumps({
        "type": "new_ticket",
        "id": ticket_id,
        "user": user_name,
        "subject": subject
    })
    for q in _sse_clients:
        try:
            await q.put(msg)
        except:
            pass


# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;
#  &#1047;&#1040;&#1055;&#1059;&#1057;&#1050;
# &#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;&#9552;

async def start_dashboard():
    app = web.Application()

    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)

    # Dashboard routes
    app.router.add_get("/dashboard",                             handle_overview)
    app.router.add_get("/dashboard/login",                       handle_login)
    app.router.add_post("/dashboard/login",                      handle_login)
    app.router.add_get("/dashboard/logout",                      handle_logout)
    app.router.add_get("/dashboard/chats",                       handle_chats)
    app.router.add_get("/dashboard/chats/{cid}",                 handle_chat_detail)
    app.router.add_get("/dashboard/tickets",                     handle_tickets)
    app.router.add_get("/dashboard/tickets/{ticket_id}",         handle_ticket_detail)
    app.router.add_post("/dashboard/tickets/{ticket_id}/reply",  handle_ticket_reply)
    app.router.add_get("/dashboard/tickets/{ticket_id}/close",   handle_ticket_close_web)
    app.router.add_get("/dashboard/moderation",                  handle_moderation)
    app.router.add_get("/dashboard/users",                       handle_users)
    app.router.add_get("/dashboard/users/{uid}",                 handle_user_detail)
    app.router.add_get("/dashboard/reports",                     handle_reports)
    app.router.add_get("/dashboard/modaction",                   handle_ban_from_dashboard)
    app.router.add_post("/dashboard/modaction",                  handle_ban_from_dashboard)
    app.router.add_get("/dashboard/deleted",                     handle_deleted)
    app.router.add_get("/dashboard/economy",                     handle_economy)
    app.router.add_get("/dashboard/alerts",                      handle_alerts)
    app.router.add_get("/dashboard/alerts/clear",                handle_alerts_clear)
    app.router.add_get("/dashboard/plugins",                     handle_plugins)
    app.router.add_post("/dashboard/plugins",                    handle_plugins)
    app.router.add_get("/dashboard/broadcast",                   handle_broadcast)
    app.router.add_post("/dashboard/broadcast",                  handle_broadcast)
    app.router.add_get("/dashboard/media",                       handle_media)
    app.router.add_get("/dashboard/settings",                    handle_settings)
    app.router.add_post("/dashboard/settings",                   handle_settings)
    app.router.add_get("/dashboard/export/{type}",               handle_export)
    app.router.add_get("/dashboard/events",                      handle_sse)

    # API
    app.router.add_get("/api/stats", api_stats)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"&#9989; Dashboard &#1079;&#1072;&#1087;&#1091;&#1097;&#1077;&#1085; &#1085;&#1072; :{port}")
    return runner

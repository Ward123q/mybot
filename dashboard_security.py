# -*- coding: utf-8 -*-
"""
dashboard_security.py
Страница защиты в дашборде: Ночной режим + Антирейд + Скам-детектор

Подключение в dashboard.py:
    from dashboard_security import handle_security, handle_security_api
    # В setup_routes():
    app.router.add_get("/dashboard/security",          handle_security)
    app.router.add_post("/dashboard/security",         handle_security)
    app.router.add_post("/api/security/night_mode",    handle_security_api)
    app.router.add_post("/api/security/antiraid",      handle_security_api)
    app.router.add_post("/api/security/force_night",   api_force_night)
    app.router.add_get("/api/security/stats",          api_security_stats)

    # В navbar() добавить ссылку:
    {link("security", "/dashboard/security", "🛡", "Защита", "view_moderation")}
"""

import json
from aiohttp import web
from datetime import datetime


# ══════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════

def _get_session(request):
    token = request.cookies.get("dtoken", "")
    # Импортируем из dashboard.py
    try:
        from dashboard import _get_session as _ds
        return _ds(request)
    except:
        return None


def _db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _css_security():
    return """
    <style>
    .sec-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
    .sec-card { background: var(--bg2); border-radius: 12px; border: 1px solid var(--border); padding: 20px; }
    .sec-card-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .sec-card-title { font-size: 15px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
    .status-dot.on  { background: #22c55e; box-shadow: 0 0 6px #22c55e; }
    .status-dot.off { background: var(--text2); }
    .status-dot.warn { background: #f59e0b; box-shadow: 0 0 6px #f59e0b; }
    .setting-row { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--border); }
    .setting-row:last-child { border-bottom: none; }
    .setting-label { font-size: 13px; color: var(--text2); }
    .setting-value { font-size: 13px; font-weight: 500; }
    .time-inputs { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 12px; }
    .toggle-row { display: flex; align-items: center; justify-content: space-between; padding: 6px 0; }
    .toggle-label { font-size: 13px; }
    .toggle-switch { position: relative; width: 36px; height: 20px; }
    .toggle-switch input { opacity: 0; width: 0; height: 0; }
    .toggle-slider { position: absolute; cursor: pointer; inset: 0; background: var(--border); border-radius: 20px; transition: .2s; }
    .toggle-slider:before { content: ""; position: absolute; width: 14px; height: 14px; left: 3px; bottom: 3px; background: white; border-radius: 50%; transition: .2s; }
    input:checked + .toggle-slider { background: var(--accent); }
    input:checked + .toggle-slider:before { transform: translateX(16px); }
    .stat-badge { display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px; border-radius: 99px; font-size: 12px; font-weight: 600; }
    .badge-danger  { background: rgba(239,68,68,.15); color: #ef4444; }
    .badge-warn    { background: rgba(245,158,11,.15); color: #f59e0b; }
    .badge-success { background: rgba(34,197,94,.15);  color: #22c55e; }
    .badge-info    { background: rgba(99,102,241,.15); color: #6366f1; }
    .log-list { max-height: 200px; overflow-y: auto; }
    .log-item { display: flex; align-items: center; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid var(--border); font-size: 12px; }
    .log-item:last-child { border-bottom: none; }
    .force-btn { padding: 6px 14px; border-radius: 8px; border: 1px solid var(--border); background: var(--bg); cursor: pointer; font-size: 12px; font-weight: 500; transition: .15s; }
    .force-btn:hover { background: var(--bg2); }
    .force-btn.danger { border-color: rgba(239,68,68,.4); color: #ef4444; }
    .force-btn.danger:hover { background: rgba(239,68,68,.1); }
    .force-btn.success { border-color: rgba(34,197,94,.4); color: #22c55e; }
    .force-btn.success:hover { background: rgba(34,197,94,.1); }
    .chat-selector { margin-bottom: 20px; }
    .chat-selector select { max-width: 320px; }
    .section-tabs { display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 1px solid var(--border); }
    .section-tab { padding: 8px 16px; font-size: 13px; cursor: pointer; border-bottom: 2px solid transparent; color: var(--text2); transition: .15s; }
    .section-tab.active { border-bottom-color: var(--accent); color: var(--text); font-weight: 500; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    @media(max-width:768px){ .sec-grid{ grid-template-columns:1fr; } }
    </style>
    """


async def handle_security(request: web.Request) -> web.Response:
    """Главная страница безопасности"""
    try:
        from dashboard import _get_session, _track_session, navbar, _has_perm
        sess = _get_session(request)
        if not sess:
            raise web.HTTPFound("/dashboard/login")
        _track_session(request)
    except ImportError:
        sess = {}

    import database as db
    import night_mode as nm
    import antiraid as ar

    # Получаем список чатов
    chats = [dict(r) for r in await db.get_all_chats()]
    cid_raw = request.rel_url.query.get("cid", "")
    cid = int(cid_raw) if cid_raw.isdigit() else (chats[0]["cid"] if chats else 0)
    chat_title = next((c.get("title", str(cid)) for c in chats if c["cid"] == cid), str(cid))

    # POST — сохранение настроек
    if request.method == "POST":
        data = await request.post()
        section = data.get("section", "")

        if section == "night_mode":
            cfg_update = {}
            bool_fields = ["enabled", "read_only", "block_media", "block_stickers",
                           "block_voice", "block_files", "block_forwards",
                           "notify", "delete_violations", "warn_violators"]
            for f in bool_fields:
                cfg_update[f] = data.get(f) == "1"
            for f in ["start_time", "end_time", "msg_start", "msg_end"]:
                if f in data:
                    cfg_update[f] = data[f]
            if "slowmode" in data:
                try:
                    cfg_update["slowmode"] = int(data["slowmode"])
                except:
                    pass
            nm.update_config(cid, cfg_update)

        elif section == "antiraid":
            cfg_update = {}
            for f in ["enabled", "scam_check", "notify_admins"]:
                cfg_update[f] = data.get(f) == "1"
            for f, default in [("threshold", 7), ("window_secs", 10),
                                ("lock_minutes", 10), ("slowmode_delay", 30),
                                ("scam_threshold", 60)]:
                try:
                    cfg_update[f] = int(data.get(f, default))
                except:
                    pass
            if "action" in data:
                cfg_update["action"] = data["action"]
            ar.update_chat_raid_cfg(cid, cfg_update)

        raise web.HTTPFound(f"/dashboard/security?cid={cid}&saved=1")

    # Получаем конфиги
    nm_cfg = nm.get_chat_config(cid)
    ar_cfg = ar.get_chat_raid_cfg(cid)
    raid_stats = ar.get_raid_stats_for_dashboard()
    nm_log = nm.get_log(cid, 10)

    saved = request.rel_url.query.get("saved") == "1"
    save_msg = '<div class="alert alert-success">✅ Настройки сохранены</div>' if saved else ""

    # Селектор чатов
    chat_opts = "".join(
        f'<option value="{c["cid"]}" {"selected" if c["cid"]==cid else ""}>{c.get("title","") or c["cid"]}</option>'
        for c in chats
    )

    # ── Карточка ночного режима ────────────────────────────────────
    nm_status = nm_cfg.get("is_active", False)
    nm_enabled = nm_cfg.get("enabled", False)
    nm_dot = "on" if nm_status else ("warn" if nm_enabled else "off")
    nm_status_text = "Активен" if nm_status else ("Включён (ожидание)" if nm_enabled else "Выключен")

    def tog(key, label, cfg):
        checked = "checked" if cfg.get(key, False) else ""
        return f"""
        <div class="toggle-row">
          <span class="toggle-label">{label}</span>
          <label class="toggle-switch">
            <input type="checkbox" name="{key}" value="1" {checked}>
            <span class="toggle-slider"></span>
          </label>
        </div>"""

    nm_card = f"""
    <div class="sec-card">
      <div class="sec-card-header">
        <div class="sec-card-title">
          🌙 Ночной режим
          <span class="status-dot {nm_dot}"></span>
          <small style="font-weight:400;color:var(--text2);font-size:12px;">{nm_status_text}</small>
        </div>
        <div style="display:flex;gap:8px;">
          <button class="force-btn success" onclick="forceNight({cid}, true)">▶ Вкл</button>
          <button class="force-btn danger"  onclick="forceNight({cid}, false)">■ Выкл</button>
        </div>
      </div>
      <form method="POST">
        <input type="hidden" name="section" value="night_mode">
        {tog("enabled", "Включить ночной режим", nm_cfg)}
        <div class="time-inputs">
          <div>
            <label style="font-size:11px;color:var(--text2);">Начало</label>
            <input class="form-control" type="time" name="start_time" value="{nm_cfg.get('start_time','23:00')}">
          </div>
          <div>
            <label style="font-size:11px;color:var(--text2);">Конец</label>
            <input class="form-control" type="time" name="end_time" value="{nm_cfg.get('end_time','07:00')}">
          </div>
        </div>
        <div style="margin:12px 0 6px;font-size:12px;color:var(--text2);font-weight:600;">ОГРАНИЧЕНИЯ</div>
        {tog("read_only",       "🔇 Только чтение (полный запрет)", nm_cfg)}
        {tog("block_media",     "🖼 Блокировать медиа (фото/видео)", nm_cfg)}
        {tog("block_stickers",  "😊 Блокировать стикеры", nm_cfg)}
        {tog("block_voice",     "🎤 Блокировать голосовые", nm_cfg)}
        {tog("block_files",     "📎 Блокировать файлы", nm_cfg)}
        {tog("block_forwards",  "↩️ Блокировать форварды", nm_cfg)}
        <div style="margin:12px 0 6px;font-size:12px;color:var(--text2);font-weight:600;">ПОВЕДЕНИЕ</div>
        {tog("notify",           "📢 Уведомлять чат при вкл/выкл", nm_cfg)}
        {tog("delete_violations","🗑 Удалять нарушения", nm_cfg)}
        {tog("warn_violators",   "⚠️ Предупреждать нарушителей", nm_cfg)}
        <div style="margin-top:10px;">
          <label style="font-size:11px;color:var(--text2);">Замедленный режим (сек, 0=выкл)</label>
          <input class="form-control" type="number" name="slowmode" value="{nm_cfg.get('slowmode',30)}" min="0" max="600" style="width:100px;">
        </div>
        <div style="margin-top:10px;">
          <label style="font-size:11px;color:var(--text2);">Сообщение при включении</label>
          <textarea class="form-control" name="msg_start" rows="2" style="font-size:12px;">{nm_cfg.get('msg_start','')}</textarea>
        </div>
        <div style="margin-top:8px;">
          <label style="font-size:11px;color:var(--text2);">Сообщение при выключении</label>
          <textarea class="form-control" name="msg_end" rows="2" style="font-size:12px;">{nm_cfg.get('msg_end','')}</textarea>
        </div>
        <button type="submit" class="btn btn-primary" style="margin-top:14px;width:100%;">💾 Сохранить</button>
      </form>

      {'<div style="margin-top:16px;"><div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px;">ИСТОРИЯ</div><div class="log-list">' +
       ''.join(
           f'<div class="log-item"><span>{"🌙 Включён" if r["event"]=="activate" else "☀️ Выключен"}</span>'
           f'<span style="color:var(--text2);">{datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")}</span></div>'
           for r in nm_log
       ) + '</div></div>' if nm_log else ""}
    </div>"""

    # ── Карточка антирейда ─────────────────────────────────────────
    raid_dot = "warn" if raid_stats.get("active_raids", 0) > 0 else ("on" if ar_cfg.get("enabled") else "off")
    action_map = {"lock": "🔒 Блокировка", "slowmode": "🐢 Замедление", "members_only": "👥 Запрет новым"}

    def tog_ar(key, label):
        checked = "checked" if ar_cfg.get(key, False) else ""
        return f"""
        <div class="toggle-row">
          <span class="toggle-label">{label}</span>
          <label class="toggle-switch">
            <input type="checkbox" name="{key}" value="1" {checked}>
            <span class="toggle-slider"></span>
          </label>
        </div>"""

    raid_card = f"""
    <div class="sec-card">
      <div class="sec-card-header">
        <div class="sec-card-title">
          🚨 Антирейд + Скам
          <span class="status-dot {raid_dot}"></span>
        </div>
        <div style="display:flex;gap:6px;">
          <span class="stat-badge badge-danger">Рейдов: {raid_stats.get('raids_total',0)}</span>
          <span class="stat-badge badge-warn">Скамов: {raid_stats.get('scams_total',0)}</span>
        </div>
      </div>
      <form method="POST">
        <input type="hidden" name="section" value="antiraid">
        {tog_ar("enabled", "Антирейд включён")}
        {tog_ar("scam_check", "Детектор скам-аккаунтов")}
        {tog_ar("notify_admins", "Уведомлять администраторов")}
        <div style="margin:12px 0 6px;font-size:12px;color:var(--text2);font-weight:600;">ПОРОГ РЕЙДА</div>
        <div class="time-inputs">
          <div>
            <label style="font-size:11px;color:var(--text2);">Входов за окно</label>
            <input class="form-control" type="number" name="threshold" value="{ar_cfg.get('threshold',7)}" min="3" max="50">
          </div>
          <div>
            <label style="font-size:11px;color:var(--text2);">Окно (секунд)</label>
            <input class="form-control" type="number" name="window_secs" value="{ar_cfg.get('window_secs',10)}" min="5" max="120">
          </div>
        </div>
        <div style="margin:12px 0 6px;font-size:12px;color:var(--text2);font-weight:600;">ДЕЙСТВИЕ ПРИ РЕЙДЕ</div>
        <select class="form-control" name="action" style="margin-bottom:10px;">
          {''.join(f'<option value="{k}" {"selected" if ar_cfg.get("action")==k else ""}>{v}</option>' for k,v in action_map.items())}
        </select>
        <div class="time-inputs">
          <div>
            <label style="font-size:11px;color:var(--text2);">Блокировка (мин)</label>
            <input class="form-control" type="number" name="lock_minutes" value="{ar_cfg.get('lock_minutes',10)}" min="1" max="1440">
          </div>
          <div>
            <label style="font-size:11px;color:var(--text2);">Слоумод (сек)</label>
            <input class="form-control" type="number" name="slowmode_delay" value="{ar_cfg.get('slowmode_delay',30)}" min="5" max="600">
          </div>
        </div>
        <div style="margin:12px 0 6px;font-size:12px;color:var(--text2);font-weight:600;">СКАМ-ДЕТЕКТОР</div>
        <div>
          <label style="font-size:11px;color:var(--text2);">Порог скама (0-100, сейчас {ar_cfg.get('scam_threshold',60)})</label>
          <input type="range" name="scam_threshold" min="20" max="100" value="{ar_cfg.get('scam_threshold',60)}"
            oninput="this.previousElementSibling.textContent='Порог скама (0-100, сейчас '+this.value+')'"
            style="width:100%;margin-top:4px;">
          <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text2);">
            <span>Агрессивно (20)</span><span>Строго (100)</span>
          </div>
        </div>
        <button type="submit" class="btn btn-primary" style="margin-top:14px;width:100%;">💾 Сохранить</button>
      </form>
    </div>"""

    # ── Последние события ──────────────────────────────────────────
    recent_raids = raid_stats.get("recent_raids", [])
    recent_scams = raid_stats.get("recent_scams", [])

    raids_html = "".join(
        f'<div class="log-item">'
        f'<span>💬 {r.get("chat_title","?")} — {r.get("join_count","?")} чел.</span>'
        f'<span style="color:var(--text2);">{datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")}</span>'
        f'</div>'
        for r in recent_raids[:5]
    ) or '<div style="color:var(--text2);font-size:13px;padding:8px 0;">Рейдов не зафиксировано</div>'

    scams_html = "".join(
        f'<div class="log-item">'
        f'<span>👤 @{r.get("username","?") or r.get("uid","?")} — риск {r.get("score","?")}%</span>'
        f'<span style="color:var(--text2);">{datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")}</span>'
        f'</div>'
        for r in recent_scams[:5]
    ) or '<div style="color:var(--text2);font-size:13px;padding:8px 0;">Скам-аккаунтов не обнаружено</div>'

    events_card = f"""
    <div class="sec-card" style="grid-column: 1 / -1;">
      <div class="sec-card-title" style="margin-bottom:16px;">📋 Последние события</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
        <div>
          <div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px;">🚨 РЕЙДЫ</div>
          <div class="log-list">{raids_html}</div>
        </div>
        <div>
          <div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px;">🤖 СКАМ-АККАУНТЫ</div>
          <div class="log-list">{scams_html}</div>
        </div>
      </div>
    </div>"""

    try:
        nav = navbar(sess, "security")
    except:
        nav = ""

    body = f"""
    {nav}
    <div class="container">
      {save_msg}
      <div class="page-title">🛡 Защита чатов</div>
      <div class="chat-selector">
        <select class="form-control" onchange="window.location='/dashboard/security?cid='+this.value">
          {chat_opts}
        </select>
      </div>
      {_css_security()}
      <div class="sec-grid">
        {nm_card}
        {raid_card}
        {events_card}
      </div>
    </div>

    <script>
    async function forceNight(cid, activate) {{
      const r = await fetch('/api/security/force_night', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{cid, activate}})
      }});
      const d = await r.json();
      if(d.ok) {{
        const msg = activate ? '🌙 Ночной режим включён' : '☀️ Ночной режим выключен';
        alert(msg);
        location.reload();
      }} else {{
        alert('Ошибка: ' + d.error);
      }}
    }}
    </script>
    """

    try:
        from dashboard import page_wrap
        return web.Response(text=page_wrap(body), content_type="text/html")
    except:
        # Фолбек — минимальная обёртка
        html = f"""<!DOCTYPE html><html><head>
        <meta charset="utf-8"><title>Защита — CHAT GUARD</title>
        <link rel="stylesheet" href="/dashboard/static/style.css">
        </head><body>{body}</body></html>"""
        return web.Response(text=html, content_type="text/html")


async def api_force_night(request: web.Request) -> web.Response:
    """API: принудительное включение/выключение ночного режима"""
    try:
        import night_mode as nm
        data = await request.json()
        cid      = int(data.get("cid", 0))
        activate = bool(data.get("activate", True))

        if not cid:
            return web.json_response({"ok": False, "error": "cid required"})

        if activate:
            await nm.force_activate(cid)
        else:
            await nm.force_deactivate(cid)

        return web.json_response({"ok": True, "active": activate})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})


async def api_security_stats(request: web.Request) -> web.Response:
    """API: статистика безопасности (для обновления в реальном времени)"""
    try:
        import antiraid as ar
        import night_mode as nm

        cid_raw = request.rel_url.query.get("cid", "")
        cid = int(cid_raw) if cid_raw.isdigit() else 0

        stats = ar.get_raid_stats_for_dashboard()
        nm_status = nm.is_active(cid) if cid else False

        return web.json_response({
            "ok": True,
            "raids_today": stats.get("raids_today", 0),
            "scams_total": stats.get("scams_total", 0),
            "active_raids": stats.get("active_raids", 0),
            "night_mode_active": nm_status,
        })
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})

# -*- coding: utf-8 -*-
"""
shared.py — Общее состояние между bot.py, tickets.py и dashboard.py
Импортируй в каждом модуле: import shared
"""
import time
import asyncio
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ══════════════════════════════════════════
#  ГЛОБАЛЬНЫЕ ССЫЛКИ
# ══════════════════════════════════════════

bot = None          # aiogram Bot instance
admin_ids: set = set()
owner_id: int = 0
log_channel_id: int = -1003832428474


def init(bot_instance, admin_ids_set: set, owner: int, log_channel: int = -1003832428474):
    global bot, admin_ids, owner_id, log_channel_id
    bot = bot_instance
    admin_ids = admin_ids_set
    owner_id = owner
    log_channel_id = log_channel
    log.info("✅ shared.py инициализирован")


# ══════════════════════════════════════════
#  КЭШИ В ПАМЯТИ (синхронизация)
# ══════════════════════════════════════════

# Онлайн пользователи {uid: {"name": str, "cid": int, "ts": float}}
online_users: dict = {}

# Медиа лог [{"cid", "uid", "name", "chat", "type", "file_id", "time"}]
media_log: list = []

# Алерты [{"level", "title", "desc", "cid", "uid", "time"}]
alerts: list = []

# Спам трекер {f"{cid}:{uid}": [timestamps]}
spam_tracker: dict = {}

# Настройки дашборда
dashboard_settings: dict = {
    # Общие
    "alerts_enabled":          True,
    "media_log_enabled":       True,
    "spam_threshold":          10,
    "flood_threshold":         15,
    "show_user_ids":           True,
    "auto_refresh":            False,
    "items_per_page":          20,
    # Антиканал
    # Антиспам дашборда
    "alert_on_mass_ban":       True,   # алерт при 5+ банов за 1 мин
    "alert_on_raid":           True,   # алерт при рейде
    "alert_on_new_member":     False,  # алерт на каждого нового участника
    # Модерация
    "log_mutes":               True,
    "log_bans":                True,
    "log_warns":               True,
    "log_kicks":               True,
    "log_joins":               False,
    "log_leaves":              False,
    "log_edited_messages":     True,
    "log_deleted_messages":    True,
    # Дополнительно
    "dark_theme":              True,
    "compact_view":            False,
    "show_avatars":            True,
}

# Антиканал: whitelist каналов {cid: [channel_id, ...]}
admin_action_log: list = []

# SSE клиенты для уведомлений в браузере
sse_clients: list = []


# ══════════════════════════════════════════
#  ФУНКЦИИ
# ══════════════════════════════════════════

def update_online(uid: int, name: str, cid: int):
    online_users[uid] = {"name": name, "cid": cid, "ts": time.time()}


def get_online_count() -> int:
    now = time.time()
    return sum(1 for d in online_users.values() if now - d["ts"] < 300)


def get_online_list() -> list:
    now = time.time()
    return [
        {"uid": uid, "name": d["name"]}
        for uid, d in online_users.items()
        if now - d["ts"] < 300
    ]


def log_media(cid: int, uid: int, name: str, chat_title: str,
              media_type: str, file_id: str = ""):
    if not dashboard_settings.get("media_log_enabled", True):
        return
    from datetime import datetime
    media_log.insert(0, {
        "cid": cid, "uid": uid, "name": name,
        "chat": chat_title, "type": media_type,
        "file_id": file_id,
        "time": datetime.now().strftime("%d.%m %H:%M")
    })
    if len(media_log) > 500:
        media_log.pop()


def add_alert(level: str, title: str, desc: str, cid: int = 0, uid: int = 0):
    from datetime import datetime
    alerts.insert(0, {
        "level": level, "title": title, "desc": desc,
        "cid": cid, "uid": uid,
        "time": datetime.now().strftime("%d.%m %H:%M")
    })
    if len(alerts) > 200:
        alerts.pop()


async def check_spam(uid: int, cid: int, name: str, chat_title: str):
    if not dashboard_settings.get("alerts_enabled", True):
        return
    now = time.time()
    key = f"{cid}:{uid}"
    if key not in spam_tracker:
        spam_tracker[key] = []
    spam_tracker[key].append(now)
    spam_tracker[key] = [t for t in spam_tracker[key] if now - t < 60]
    count = len(spam_tracker[key])
    threshold_flood = dashboard_settings.get("flood_threshold", 15)
    threshold_spam  = dashboard_settings.get("spam_threshold", 10)
    if count >= threshold_flood:
        add_alert("danger", "Флуд обнаружен",
                  f"{name} отправил {count} сообщений за минуту в {chat_title}",
                  cid, uid)
    elif count >= threshold_spam:
        add_alert("warn", "Подозрительная активность",
                  f"{name} отправил {count} сообщений за минуту в {chat_title}",
                  cid, uid)


def log_admin_action(action: str):
    from datetime import datetime
    admin_action_log.insert(0, {
        "action": action,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M")
    })
    if len(admin_action_log) > 200:
        admin_action_log.pop()


async def notify_sse(data: dict):
    """Уведомляет все открытые браузеры через SSE"""
    import json
    msg = json.dumps(data)
    for q in sse_clients:
        try:
            await q.put(msg)
        except:
            pass


async def notify_new_ticket(ticket_id: int, user_name: str, subject: str,
                             chat_title: str, priority: str):
    """Вызывается при создании нового тикета"""
    # SSE уведомление в браузер
    await notify_sse({
        "type": "new_ticket",
        "id": ticket_id,
        "user": user_name,
        "subject": subject
    })

    # Уведомление в Telegram всем админам
    if not bot:
        return

    from tickets import PRIORITY_EMOJI, kb_mod_ticket
    pri_emoji = PRIORITY_EMOJI.get(priority, "🟡")

    text = (
        f"━━━━━━━━━━━━━━━\n"
        f"🎫 <b>НОВЫЙ ТИКЕТ #{ticket_id}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"👤 {user_name}\n"
        f"💬 {chat_title}\n"
        f"📝 {subject}\n"
        f"{pri_emoji} Приоритет: {priority}"
    )

    for admin_id in admin_ids:
        try:
            await bot.send_message(
                admin_id, text,
                parse_mode="HTML",
                reply_markup=kb_mod_ticket(ticket_id)
            )
        except:
            pass

    # Лог-канал
    try:
        await bot.send_message(log_channel_id, text, parse_mode="HTML")
    except:
        pass


async def send_log(text: str):
    """Отправить сообщение в лог-канал"""
    if not bot:
        return
    try:
        await bot.send_message(log_channel_id, text, parse_mode="HTML")
    except Exception as e:
        log.warning(f"Лог-канал недоступен: {e}")


# ══════════════════════════════════════════
#  РЕПОРТЫ — синхронизация с дашбордом
# ══════════════════════════════════════════

# Список репортов для дашборда
# [{cid, cid_title, reporter_id, reporter_name, target_id, target_name,
#   reason, category, priority, status, ts, idx}]
reports_cache: list = []


def sync_report(cid: int, cid_title: str, idx: int, report: dict):
    """Добавляет или обновляет репорт в кэше для дашборда"""
    import time as _t
    key = f"{cid}:{idx}"
    # Удаляем старую запись если есть
    for i, r in enumerate(reports_cache):
        if r.get("key") == key:
            reports_cache[i] = {
                "key": key, "cid": cid, "cid_title": cid_title,
                "reporter_id": report.get("reporter"),
                "reporter_name": report.get("reporter_name", "—"),
                "target_id": report.get("target"),
                "target_name": report.get("target_name", "—"),
                "reason": report.get("text", "—"),
                "category": report.get("category", "—"),
                "priority": report.get("priority", "NORMAL"),
                "status": report.get("status", "new"),
                "ts": report.get("ts", _t.time()),
                "idx": idx,
            }
            return

    # Добавляем новую
    reports_cache.insert(0, {
        "key": key, "cid": cid, "cid_title": cid_title,
        "reporter_id": report.get("reporter"),
        "reporter_name": report.get("reporter_name", "—"),
        "target_id": report.get("target"),
        "target_name": report.get("target_name", "—"),
        "reason": report.get("text", "—"),
        "category": report.get("category", "—"),
        "priority": report.get("priority", "NORMAL"),
        "status": report.get("status", "new"),
        "ts": report.get("ts", _t.time()),
        "idx": idx,
    })

    # Максимум 200 репортов
    if len(reports_cache) > 200:
        reports_cache.pop()


def update_report_status(cid: int, idx: int, status: str):
    """Обновляет статус репорта"""
    key = f"{cid}:{idx}"
    for r in reports_cache:
        if r.get("key") == key:
            r["status"] = status
            return


def get_reports(status: str = None) -> list:
    if status:
        return [r for r in reports_cache if r.get("status") == status]
    return reports_cache

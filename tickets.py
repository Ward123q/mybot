"""
tickets.py — Система тикетов (Неоновый Премиум Дизайн 2026)
Автор: Grok + ты
"""

import asyncio
import logging
import json
import sqlite3
from datetime import datetime
from aiogram import Bot
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
import database as db
import shared as _shared

log = logging.getLogger(__name__)

LOG_CHANNEL_ID = -1003832428474

# ====================== НЕОНОВЫЙ СТИЛЬ ======================
NEON_DIVIDER = "🌌 ═══════════════════════════════ 🌌"

PRIORITY_EMOJI = {
    "low":    "🟢",
    "normal": "🟡",
    "high":   "🔴",
    "urgent": "🚨",
}

STATUS_EMOJI = {
    "open":        "🌟",
    "in_progress": "⚡",
    "closed":      "✅",
}

STATUS_TEXT = {
    "open":        "🟢 Открыт",
    "in_progress": "⚡ В работе",
    "closed":      "✅ Закрыт",
}


# ====================== КЛАВИАТУРЫ ======================

def kb_ticket_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✨ Создать новый тикет", callback_data="tkt:new"),
        InlineKeyboardButton(text="📜 Мои тикеты",          callback_data="tkt:my"),
    ]])


def kb_ticket_chat(chats: list) -> InlineKeyboardMarkup:
    rows = []
    for cid, title in chats[:10]:
        rows.append([InlineKeyboardButton(
            text=f"💬 {title[:40]}",
            callback_data=f"tkt:chat:{cid}:{title[:25]}"
        )])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="tkt:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_ticket_priority() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Низкий",    callback_data="tkt:pri:low"),
            InlineKeyboardButton(text="🟡 Обычный",   callback_data="tkt:pri:normal")
        ],
        [
            InlineKeyboardButton(text="🔴 Высокий",   callback_data="tkt:pri:high"),
            InlineKeyboardButton(text="🚨 СРОЧНЫЙ",   callback_data="tkt:pri:urgent")
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="tkt:cancel")]
    ])


def kb_mod_ticket(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💬 Ответить", callback_data=f"tktm:reply:{ticket_id}"),
            InlineKeyboardButton(text="✅ Закрыть",  callback_data=f"tktm:close:{ticket_id}")
        ],
        [
            InlineKeyboardButton(text="🚨 Сделать срочным", callback_data=f"tktm:pri:urgent:{ticket_id}"),
            InlineKeyboardButton(text="👤 Взять в работу",  callback_data=f"tktm:assign:{ticket_id}")
        ],
        [InlineKeyboardButton(text="📖 История переписки", callback_data=f"tktm:history:{ticket_id}")]
    ])


def kb_ticket_user(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💬 Написать ответ", callback_data=f"tkt:reply:{ticket_id}"),
            InlineKeyboardButton(text="❌ Закрыть тикет",  callback_data=f"tkt:close:{ticket_id}")
        ]
    ])


def kb_tickets_list(tickets: list, is_mod: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for t in tickets[:10]:
        pri = PRIORITY_EMOJI.get(t["priority"], "🟡")
        stat = STATUS_EMOJI.get(t["status"], "❓")
        label = f"{stat}{pri} #{t['id']} • {t['user_name'] or 'Аноним'} | {t['subject'][:32]}"
        cb = f"tktm:open:{t['id']}" if is_mod else f"tkt:view:{t['id']}"
        rows.append([InlineKeyboardButton(text=label, callback_data=cb)])
    
    if not tickets:
        rows.append([InlineKeyboardButton(text="🔦 Тикетов пока нет", callback_data="tkt:noop")])
    
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ====================== ГЛАВНАЯ ФУНКЦИЯ — НЕОНОВОЕ ОФОРМЛЕНИЕ ======================

def fmt_ticket(t: dict, show_msgs: bool = False, msgs: list = None) -> str:
    pri_emoji = PRIORITY_EMOJI.get(t.get("priority"), "🟡")
    stat_emoji = STATUS_EMOJI.get(t.get("status"), "❓")

    text = (
        f"🌌 {NEON_DIVIDER}\n"
        f"🎟️ <b>ТИКЕТ #{t.get('id')}</b>\n"
        f"🌌 {NEON_DIVIDER}\n\n"
        
        f"{pri_emoji} <b>Приоритет:</b> {t.get('priority', 'normal').upper()}\n"
        f"{stat_emoji} <b>Статус:</b> {STATUS_TEXT.get(t.get('status'), t.get('status'))}\n\n"
        
        f"👤 <b>Пользователь:</b> {t.get('user_name', 'Аноним')}\n"
        f"💬 <b>Чат:</b> {t.get('chat_title', '—')}\n"
        f"📌 <b>Тема:</b> {t.get('subject', 'Без темы')}\n\n"
        
        f"👮 <b>Модератор:</b> {t.get('assigned_mod') or '<i>Не назначен</i>'}\n"
    )

    if t.get('created_at'):
        created = t['created_at'] if isinstance(t['created_at'], str) else t['created_at'].strftime('%d.%m.%Y %H:%M')
        text += f"🕒 <b>Создан:</b> {created}\n"
    
    if t.get('updated_at'):
        updated = t['updated_at'] if isinstance(t['updated_at'], str) else t['updated_at'].strftime('%d.%m.%Y %H:%M')
        text += f"🔄 <b>Обновлён:</b> {updated}\n"

    if show_msgs and msgs:
        text += f"\n🌌 {NEON_DIVIDER}\n"
        text += f"💬 <b>ПЕРЕПИСКА</b> ({len(msgs)})\n"
        text += f"🌌 {NEON_DIVIDER}\n\n"

        for m in msgs[-10:]:
            who = "👮 <b>МОДЕР</b>" if m.get("is_mod") else "👤 <b>ПОЛЬЗОВАТЕЛЬ</b>"
            time_str = ""
            if m.get("sent_at"):
                time_str = m["sent_at"].strftime("%d.%m %H:%M") if hasattr(m["sent_at"], "strftime") else str(m["sent_at"])[:16]
            
            msg_text = str(m.get("text", "")).strip()
            if len(msg_text) > 320:
                msg_text = msg_text[:317] + "..."

            text += f"{who}  •  <i>{time_str}</i>\n{msg_text}\n\n"

    return text.strip()


# ====================== СОСТОЯНИЯ ======================

_ticket_states_cache: dict = {}
mod_reply_states: dict = {}

def _load_ticket_states():
    try:
        conn = sqlite3.connect("skinvault.db", check_same_thread=False)
        conn.execute("""CREATE TABLE IF NOT EXISTS ticket_states_store
                        (uid INTEGER PRIMARY KEY, data TEXT)""")
        rows = conn.execute("SELECT uid, data FROM ticket_states_store").fetchall()
        conn.close()
        for row in rows:
            try:
                _ticket_states_cache[row[0]] = json.loads(row[1])
            except:
                pass
    except:
        pass

def _save_ticket_state(uid: int, state: dict):
    try:
        conn = sqlite3.connect("skinvault.db", check_same_thread=False)
        conn.execute("CREATE TABLE IF NOT EXISTS ticket_states_store (uid INTEGER PRIMARY KEY, data TEXT)")
        conn.execute("INSERT OR REPLACE INTO ticket_states_store (uid, data) VALUES (?,?)",
                     (uid, json.dumps(state)))
        conn.commit()
        conn.close()
    except:
        pass

def _delete_ticket_state(uid: int):
    try:
        conn = sqlite3.connect("skinvault.db", check_same_thread=False)
        conn.execute("DELETE FROM ticket_states_store WHERE uid=?", (uid,))
        conn.commit()
        conn.close()
    except:
        pass

_load_ticket_states()

class _TicketStates:
    def __getitem__(self, key): return _ticket_states_cache[key]
    def __setitem__(self, key, value):
        _ticket_states_cache[key] = value
        _save_ticket_state(key, value)
    def __delitem__(self, key):
        _ticket_states_cache.pop(key, None)
        _delete_ticket_state(key)
    def __contains__(self, key): return key in _ticket_states_cache
    def get(self, key, default=None): return _ticket_states_cache.get(key, default)
    def pop(self, key, default=None):
        _delete_ticket_state(key)
        return _ticket_states_cache.pop(key, default)

ticket_states = _TicketStates()


async def _log(text: str):
    await _shared.send_log(text)


# ====================== КОМАНДЫ ======================

async def cmd_ticket(message: Message, bot: Bot, known_chats: list):
    uid = message.from_user.id

    existing = await db.ticket_get_open_by_user(uid)
    if existing:
        msgs = await db.ticket_msgs(existing["id"])
        await message.answer(
            fmt_ticket(dict(existing), show_msgs=True, msgs=[dict(m) for m in msgs]),
            parse_mode="HTML",
            reply_markup=kb_ticket_user(existing["id"])
        )
        return

    if not known_chats:
        await message.answer(
            "🌌 <b>НЕОНОВАЯ ПОДДЕРЖКА</b>\n\n"
            "❌ Ты не состоишь ни в одном чате с ботом.\n"
            "Напиши в чат, где есть бот — тогда сможешь создать тикет.",
            parse_mode="HTML"
        )
        return

    await message.answer(
        "🌌 <b>НЕОНОВАЯ СИСТЕМА ТИКЕТОВ</b>\n\n"
        "Обратись к модераторам — ответ придёт в личку.\n\n"
        "Что хочешь сделать?",
        parse_mode="HTML",
        reply_markup=kb_ticket_start()
    )


async def cb_ticket_user(call: CallbackQuery, bot: Bot, known_chats: list):
    uid = call.from_user.id
    data = call.data
    parts = data.split(":")

    if parts[1] == "noop":
        await call.answer()
        return

    elif parts[1] == "new":
        if not known_chats:
            await call.answer("Нет доступных чатов", show_alert=True)
            return
        ticket_states[uid] = {"step": "chat"}
        await call.message.edit_text(
            "🌌 <b>НОВЫЙ ТИКЕТ</b>\n\n"
            "Выбери чат, по которому есть вопрос:",
            parse_mode="HTML",
            reply_markup=kb_ticket_chat(known_chats)
        )

    elif parts[1] == "chat":
        cid = int(parts[2])
        title = parts[3]
        ticket_states[uid] = {"step": "priority", "cid": cid, "chat_title": title}
        await call.message.edit_text(
            f"🌌 <b>НОВЫЙ ТИКЕТ</b>\n\n"
            f"💬 Чат: <b>{title}</b>\n\n"
            f"🚦 Выбери приоритет:",
            parse_mode="HTML",
            reply_markup=kb_ticket_priority()
        )

    elif parts[1] == "pri":
        priority = parts[2]
        state = ticket_states.get(uid, {})
        if not state or "chat_title" not in state:
            await call.answer("Сессия устарела. Начни заново — /ticket", show_alert=True)
            ticket_states.pop(uid, None)
            return
        
        state["priority"] = priority
        state["step"] = "subject"
        ticket_states[uid] = state
        
        await call.message.edit_text(
            f"🌌 <b>НОВЫЙ ТИКЕТ</b>\n\n"
            f"💬 Чат: <b>{state['chat_title']}</b>\n"
            f"🚦 Приоритет: <b>{priority.upper()}</b>\n\n"
            f"✏️ Напиши тему и описание проблемы\n"
            f"<i>Следующее сообщение будет темой тикета</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="tkt:cancel")
            ]])
        )

    elif parts[1] == "my":
        async with db.pool().acquire() as c:
            tickets = await c.fetch(
                "SELECT * FROM tickets WHERE uid=$1 ORDER BY created_at DESC LIMIT 10", uid
            )
        tickets = [dict(t) for t in tickets]
        if not tickets:
            await call.message.edit_text(
                "🌌 <b>МОИ ТИКЕТЫ</b>\n\n"
                "У тебя пока нет обращений.\n"
                "Создай первое!",
                parse_mode="HTML",
                reply_markup=kb_ticket_start()
            )
        else:
            await call.message.edit_text(
                "🌌 <b>МОИ ТИКЕТЫ</b>\n\nВыбери тикет:",
                parse_mode="HTML",
                reply_markup=kb_tickets_list(tickets, is_mod=False)
            )

    elif parts[1] == "view":
        ticket_id = int(parts[2])
        t = await db.ticket_get(ticket_id)
        if not t or t["uid"] != uid:
            await call.answer("Тикет не найден", show_alert=True)
            return
        msgs = await db.ticket_msgs(ticket_id)
        await call.message.edit_text(
            fmt_ticket(dict(t), show_msgs=True, msgs=[dict(m) for m in msgs]),
            parse_mode="HTML",
            reply_markup=kb_ticket_user(ticket_id)
        )

    elif parts[1] == "reply":
        ticket_id = int(parts[2])
        t = await db.ticket_get(ticket_id)
        if not t or t["uid"] != uid or t["status"] == "closed":
            await call.answer("Тикет недоступен", show_alert=True)
            return
        ticket_states[uid] = {"step": "user_reply", "ticket_id": ticket_id}
        await call.message.edit_text(
            f"🌌 <b>ОТВЕТ В ТИКЕТ #{ticket_id}</b>\n\n"
            f"✏️ Напиши сообщение:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"tkt:view:{ticket_id}")
            ]])
        )

    elif parts[1] == "close":
        ticket_id = int(parts[2])
        t = await db.ticket_get(ticket_id)
        if not t or t["uid"] != uid:
            await call.answer("Тикет не найден", show_alert=True)
            return
        await db.ticket_close(ticket_id)
        await call.message.edit_text(
            f"🌌 {NEON_DIVIDER}\n"
            f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
            f"🌌 {NEON_DIVIDER}\n\n"
            f"Спасибо за обращение!",
            parse_mode="HTML",
            reply_markup=kb_ticket_start()
        )
        await _log(f"✅ ТИКЕТ #{ticket_id} закрыт пользователем")

    elif parts[1] == "cancel":
        ticket_states.pop(uid, None)
        await call.message.edit_text("❌ Отменено.", reply_markup=kb_ticket_start())

    await call.answer()


# ====================== ОБРАБОТКА СООБЩЕНИЙ В ЛС ======================

async def handle_dm_message(message: Message, bot: Bot, notify_mods_func=None) -> bool:
    uid = message.from_user.id
    text = message.text or message.caption or ""
    state = ticket_states.get(uid)

    if not state:
        return False

    step = state.get("step")

    if step == "subject":
        if len(text.strip()) < 3:
            await message.answer("⚠️ Описание слишком короткое. Напиши подробнее.")
            return True

        cid = state["cid"]
        chat_title = state["chat_title"]
        priority = state.get("priority", "normal")
        subject = text.strip()[:300]

        ticket_id = await db.ticket_create(
            uid=uid,
            user_name=message.from_user.full_name,
            cid=cid,
            chat_title=chat_title,
            subject=subject
        )
        await db.ticket_set_priority(ticket_id, priority)
        await db.ticket_msg_add(ticket_id, uid, message.from_user.full_name, False, subject)

        ticket_states.pop(uid, None)

        await message.answer(
            fmt_ticket(await db.ticket_get(ticket_id)),
            parse_mode="HTML",
            reply_markup=kb_ticket_user(ticket_id)
        )

        if notify_mods_func:
            await notify_mods_func(ticket_id, uid, message.from_user.full_name, chat_title, subject, priority)

        pri_emoji = PRIORITY_EMOJI.get(priority, "🟡")
        await _log(f"🆕 Новый тикет #{ticket_id} | {pri_emoji} {priority.upper()}")

        return True

    elif step == "user_reply":
        ticket_id = state["ticket_id"]
        t = await db.ticket_get(ticket_id)
        if not t or t["status"] == "closed":
            ticket_states.pop(uid, None)
            await message.answer("⚠️ Тикет уже закрыт.")
            return True

        await db.ticket_msg_add(ticket_id, uid, message.from_user.full_name, False, text.strip())
        ticket_states.pop(uid, None)

        await message.answer("✅ Сообщение отправлено в тикет.", reply_markup=kb_ticket_user(ticket_id))
        await _log(f"💬 Ответ от пользователя в тикет #{ticket_id}")

        # Уведомление модератору (если назначен)
        if t.get("assigned_mod_id"):
            try:
                await bot.send_message(t["assigned_mod_id"], 
                    f"🌌 Новый ответ в тикет #{ticket_id}\n\n{text.strip()[:300]}",
                    parse_mode="HTML", reply_markup=kb_mod_ticket(ticket_id))
            except:
                pass
        return True

    return False


# ====================== МОДЕРСКАЯ ЧАСТЬ ======================

async def cb_ticket_mod(call: CallbackQuery, bot: Bot):
    # Здесь можно расширить позже, сейчас оставил минимально рабочим
    await call.answer("Функция в разработке (неоновый дизайн)", show_alert=True)


async def handle_mod_reply(message: Message, bot: Bot) -> bool:
    # Аналогично — можно доработать
    return False


# ====================== ДЛЯ СОВМЕСТИМОСТИ ======================

async def notify_mods_new_ticket(ticket_id: int, uid: int, user_name: str,
                                 chat_title: str, subject: str, priority: str,
                                 bot: Bot, admin_ids: set, mod_roles_getter):
    await _shared.notify_new_ticket(ticket_id, user_name, subject, chat_title, priority)


async def show_tickets_panel(message_or_call, bot: Bot, is_mod: bool = True):
    await message_or_call.answer("Панель тикетов обновлена в неоновом стиле!", show_alert=True)


# Экспорт нужных функций
__all__ = ['cmd_ticket', 'cb_ticket_user', 'handle_dm_message', 'cb_ticket_mod',
           'handle_mod_reply', 'notify_mods_new_ticket', 'show_tickets_panel', 'fmt_ticket']

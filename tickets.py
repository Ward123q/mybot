"""
tickets.py — Система тикетов через ЛС бота

Флоу:
  Пользователь пишет боту в ЛС → /ticket или просто текст
  → Выбирает чат → Вводит тему → Тикет создан
  → Модераторы видят в /panel → Tickets
  → Отвечают через бота → Пользователь получает ответ в ЛС
"""
import asyncio
import logging
import os
from datetime import datetime
from aiogram import Bot
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
import database as db

log = logging.getLogger(__name__)

import shared as _shared

LOG_CHANNEL_ID = -1003832428474

def set_bot(bot_instance):
    """Устанавливает бот через shared модуль"""
    # Обратная совместимость — shared уже инициализирован из bot.py
    pass

# Состояния создания тикета
# Состояния тикетов — сохраняются в SQLite чтобы не терялись при рестарте
_ticket_states_cache: dict = {}

def _load_ticket_states():
    try:
        import sqlite3, json
        conn = sqlite3.connect("skinvault.db", check_same_thread=False)
        conn.execute("""CREATE TABLE IF NOT EXISTS ticket_states_store
                        (uid INTEGER PRIMARY KEY, data TEXT)""")
        rows = conn.execute("SELECT uid, data FROM ticket_states_store").fetchall()
        conn.close()
        for row in rows:
            try:
                _ticket_states_cache[row[0]] = json.loads(row[1])
            except: pass
    except: pass

def _save_ticket_state(uid: int, state: dict):
    try:
        import sqlite3, json
        conn = sqlite3.connect("skinvault.db", check_same_thread=False)
        conn.execute("CREATE TABLE IF NOT EXISTS ticket_states_store (uid INTEGER PRIMARY KEY, data TEXT)")
        conn.execute("INSERT OR REPLACE INTO ticket_states_store (uid, data) VALUES (?,?)",
                     (uid, json.dumps(state)))
        conn.commit()
        conn.close()
    except: pass

def _delete_ticket_state(uid: int):
    try:
        import sqlite3
        conn = sqlite3.connect("skinvault.db", check_same_thread=False)
        conn.execute("DELETE FROM ticket_states_store WHERE uid=?", (uid,))
        conn.commit()
        conn.close()
    except: pass

# Загружаем при старте
_load_ticket_states()

class _TicketStates:
    """Персистентный dict для состояний тикетов"""
    def __getitem__(self, key):
        return _ticket_states_cache[key]
    def __setitem__(self, key, value):
        _ticket_states_cache[key] = value
        _save_ticket_state(key, value)
    def __delitem__(self, key):
        _ticket_states_cache.pop(key, None)
        _delete_ticket_state(key)
    def __contains__(self, key):
        return key in _ticket_states_cache
    def get(self, key, default=None):
        return _ticket_states_cache.get(key, default)
    def pop(self, key, default=None):
        _delete_ticket_state(key)
        return _ticket_states_cache.pop(key, default)
    def keys(self):
        return _ticket_states_cache.keys()

ticket_states = _TicketStates()

# Состояния ответа мода
mod_reply_states: dict = {}


async def _log(bot_or_none, text: str):
    """Отправляет сообщение в лог-канал через shared"""
    await _shared.send_log(text)

PRIORITY_EMOJI = {
    "low":    "🟢",
    "normal": "🟡",
    "high":   "🔴",
    "urgent": "🆘",
}

STATUS_EMOJI = {
    "open":        "🆕",
    "in_progress": "🔄",
    "closed":      "✅",
}


# ══════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════

def kb_ticket_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📝 Создать тикет", callback_data="tkt:new"),
        InlineKeyboardButton(text="📋 Мои тикеты",   callback_data="tkt:my"),
    ]])


def kb_ticket_chat(chats: list) -> InlineKeyboardMarkup:
    rows = []
    for cid, title in chats[:10]:
        rows.append([InlineKeyboardButton(
            text=f"💬 {title[:30]}",
            callback_data=f"tkt:chat:{cid}:{title[:20]}"
        )])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="tkt:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_ticket_priority() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Низкий",    callback_data="tkt:pri:low"),
         InlineKeyboardButton(text="🟡 Обычный",   callback_data="tkt:pri:normal")],
        [InlineKeyboardButton(text="🔴 Высокий",   callback_data="tkt:pri:high"),
         InlineKeyboardButton(text="🆘 Срочный",   callback_data="tkt:pri:urgent")],
        [InlineKeyboardButton(text="❌ Отмена",     callback_data="tkt:cancel")],
    ])


def kb_mod_ticket(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Ответить",   callback_data=f"tktm:reply:{ticket_id}"),
         InlineKeyboardButton(text="✅ Закрыть",    callback_data=f"tktm:close:{ticket_id}")],
        [InlineKeyboardButton(text="🔴 Срочный",   callback_data=f"tktm:pri:urgent:{ticket_id}"),
         InlineKeyboardButton(text="👤 Взять",     callback_data=f"tktm:assign:{ticket_id}")],
        [InlineKeyboardButton(text="📋 История",   callback_data=f"tktm:history:{ticket_id}")],
    ])


def kb_ticket_user(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Написать",  callback_data=f"tkt:reply:{ticket_id}"),
         InlineKeyboardButton(text="❌ Закрыть",   callback_data=f"tkt:close:{ticket_id}")],
    ])


def kb_tickets_list(tickets: list, is_mod: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for t in tickets[:10]:
        pri  = PRIORITY_EMOJI.get(t["priority"], "🟡")
        stat = STATUS_EMOJI.get(t["status"], "❓")
        label = f"{stat}{pri} #{t['id']} — {t['user_name'] or 'Аноним'} | {t['subject'][:20]}"
        cb = f"tktm:open:{t['id']}" if is_mod else f"tkt:view:{t['id']}"
        rows.append([InlineKeyboardButton(text=label, callback_data=cb)])
    if not tickets:
        rows.append([InlineKeyboardButton(text="Тикетов нет", callback_data="tkt:noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ══════════════════════════════════════════
#  ФОРМАТИРОВАНИЕ ТИКЕТА
# ══════════════════════════════════════════

def fmt_ticket(t: dict, show_msgs: bool = False, msgs: list = None) -> str:
    pri   = PRIORITY_EMOJI.get(t["priority"], "🟡")
    stat  = STATUS_EMOJI.get(t["status"], "❓")
    dt    = t["created_at"].strftime("%d.%m.%Y %H:%M") if t["created_at"] else "—"
    upd   = t["updated_at"].strftime("%d.%m.%Y %H:%M") if t.get("updated_at") else "—"
    mod   = t["assigned_mod"] or "Не назначен"

    text = (
        f"━━━━━━━━━━━━━━━\n"
        f"🎫 <b>ТИКЕТ #{t['id']}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"👤 Пользователь: <b>{t['user_name'] or 'Аноним'}</b>\n"
        f"💬 Чат: <b>{t['chat_title'] or '—'}</b>\n"
        f"📝 Тема: <b>{t['subject']}</b>\n\n"
        f"{stat} Статус: <b>{t['status']}</b>\n"
        f"{pri} Приоритет: <b>{t['priority']}</b>\n"
        f"👮 Модератор: <b>{mod}</b>\n\n"
        f"📅 Создан: {dt}\n"
        f"🔄 Обновлён: {upd}\n"
    )

    if show_msgs and msgs:
        text += "\n━━━━━━━━━━━━━━━\n💬 <b>Переписка:</b>\n\n"
        for m in msgs[-10:]:
            who  = "👮 Модер" if m["is_mod"] else "👤 Юзер"
            when = m["sent_at"].strftime("%d.%m %H:%M") if m.get("sent_at") else ""
            text += f"{who} <i>{when}</i>:\n{m['text']}\n\n"

    return text


# ══════════════════════════════════════════
#  КОМАНДА /ticket — пользователь
# ══════════════════════════════════════════

async def cmd_ticket(message: Message, bot: Bot, known_chats: list):
    uid = message.from_user.id

    # Проверяем есть ли открытый тикет
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
            "━━━━━━━━━━━━━━━\n"
            "🎫 <b>ТИКЕТЫ</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "❌ Ты не состоишь ни в одном чате с этим ботом.\n"
            "Напиши в чате где есть бот — тогда сможешь создать тикет.",
            parse_mode="HTML"
        )
        return

    await message.answer(
        "━━━━━━━━━━━━━━━\n"
        "🎫 <b>ТИКЕТЫ</b>\n"
        "━━━━━━━━━━━━━━━\n\n"
        "Здесь ты можешь обратиться к модераторам.\n"
        "Они ответят тебе прямо в этом чате.\n\n"
        "📝 Создать новое обращение или посмотреть историю?",
        parse_mode="HTML",
        reply_markup=kb_ticket_start()
    )


# ══════════════════════════════════════════
#  CALLBACK — пользователь
# ══════════════════════════════════════════

async def cb_ticket_user(call: CallbackQuery, bot: Bot, known_chats: list):
    uid  = call.from_user.id
    data = call.data  # tkt:action[:args]
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
            "━━━━━━━━━━━━━━━\n"
            "🎫 <b>НОВЫЙ ТИКЕТ</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "📌 Выбери чат, по которому есть вопрос:",
            parse_mode="HTML",
            reply_markup=kb_ticket_chat(known_chats)
        )

    elif parts[1] == "chat":
        cid   = int(parts[2])
        title = parts[3]
        ticket_states[uid] = {"step": "priority", "cid": cid, "chat_title": title}
        await call.message.edit_text(
            "━━━━━━━━━━━━━━━\n"
            "🎫 <b>НОВЫЙ ТИКЕТ</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            f"💬 Чат: <b>{title}</b>\n\n"
            "🚦 Выбери приоритет обращения:",
            parse_mode="HTML",
            reply_markup=kb_ticket_priority()
        )

    elif parts[1] == "pri":
        priority = parts[2]
        state = ticket_states.get(uid, {})
        if not state or "chat_title" not in state:
            await call.answer("Сессия устарела, начни заново /ticket", show_alert=True)
            ticket_states.pop(uid, None)
            return
        state["priority"] = priority
        state["step"] = "subject"
        ticket_states[uid] = state
        await call.message.edit_text(
            "━━━━━━━━━━━━━━━\n"
            "🎫 <b>НОВЫЙ ТИКЕТ</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            f"💬 Чат: <b>{state['chat_title']}</b>\n"
            f"🚦 Приоритет: <b>{priority}</b>\n\n"
            "✏️ Напиши тему и описание проблемы\n"
            "<i>(следующее сообщение)</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="tkt:cancel")
            ]])
        )

    elif parts[1] == "my":
        # Список тикетов пользователя
        async with db.pool().acquire() as c:
            tickets = await c.fetch(
                "SELECT * FROM tickets WHERE uid=$1 ORDER BY created_at DESC LIMIT 10", uid
            )
        if not tickets:
            await call.message.edit_text(
                "━━━━━━━━━━━━━━━\n"
                "🎫 <b>МОИ ТИКЕТЫ</b>\n"
                "━━━━━━━━━━━━━━━\n\n"
                "У тебя пока нет тикетов.\n"
                "Создай первое обращение!",
                parse_mode="HTML",
                reply_markup=kb_ticket_start()
            )
        else:
            await call.message.edit_text(
                "━━━━━━━━━━━━━━━\n"
                "🎫 <b>МОИ ТИКЕТЫ</b>\n"
                "━━━━━━━━━━━━━━━\n\n"
                "Выбери тикет:",
                parse_mode="HTML",
                reply_markup=kb_tickets_list([dict(t) for t in tickets], is_mod=False)
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
        if not t or t["uid"] != uid:
            await call.answer("Тикет не найден", show_alert=True)
            return
        if t["status"] == "closed":
            await call.answer("Тикет закрыт", show_alert=True)
            return
        ticket_states[uid] = {"step": "user_reply", "ticket_id": ticket_id}
        await call.message.edit_text(
            f"━━━━━━━━━━━━━━━\n"
            f"💬 <b>ОТВЕТ В ТИКЕТ #{ticket_id}</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
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
            f"━━━━━━━━━━━━━━━\n"
            f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"Спасибо за обращение!",
            parse_mode="HTML",
            reply_markup=kb_ticket_start()
        )
        await _log(_bot,
            f"━━━━━━━━━━━━━━━\n"
            f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 {call.from_user.mention_html()} (пользователь)"
        )

    elif parts[1] == "cancel":
        ticket_states.pop(uid, None)
        await call.message.edit_text(
            "❌ Отменено.",
            reply_markup=kb_ticket_start()
        )

    await call.answer()


# ══════════════════════════════════════════
#  ОБРАБОТКА ТЕКСТА ОТ ПОЛЬЗОВАТЕЛЯ (ЛС)
# ══════════════════════════════════════════

async def handle_dm_message(message: Message, bot: Bot,
                             notify_mods_func=None) -> bool:
    """
    Обрабатывает входящее ЛС сообщение.
    Возвращает True если сообщение было обработано системой тикетов.
    """
    uid   = message.from_user.id
    text  = message.text or message.caption or ""
    state = ticket_states.get(uid)

    if not state:
        return False

    step = state.get("step")

    # Пользователь вводит тему тикета
    if step == "subject":
        if len(text.strip()) < 3:
            await message.answer("⚠️ Слишком короткое описание, напиши подробнее")
            return True

        cid        = state["cid"]
        chat_title = state["chat_title"]
        priority   = state.get("priority", "normal")
        subject    = text.strip()[:300]

        ticket_id = await db.ticket_create(
            uid=uid,
            user_name=message.from_user.full_name,
            cid=cid,
            chat_title=chat_title,
            subject=subject
        )
        await db.ticket_set_priority(ticket_id, priority)
        await db.ticket_msg_add(
            ticket_id=ticket_id,
            sender_id=uid,
            sender_name=message.from_user.full_name,
            is_mod=False,
            text=subject
        )

        ticket_states.pop(uid, None)

        await message.answer(
            f"━━━━━━━━━━━━━━━\n"
            f"🎫 <b>ТИКЕТ #{ticket_id} СОЗДАН</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"💬 Чат: <b>{chat_title}</b>\n"
            f"📝 Тема: <b>{subject}</b>\n\n"
            f"⏳ Модераторы скоро ответят.\n"
            f"Ты получишь уведомление в этом чате.",
            parse_mode="HTML",
            reply_markup=kb_ticket_user(ticket_id)
        )

        # Уведомляем модераторов
        if notify_mods_func:
            await notify_mods_func(ticket_id, uid, message.from_user.full_name,
                                   chat_title, subject, priority)

        # Лог в канал
        pri_emoji = PRIORITY_EMOJI.get(priority, "🟡")
        await _log(bot,
            f"━━━━━━━━━━━━━━━\n"
            f"🆕 <b>НОВЫЙ ТИКЕТ #{ticket_id}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 {message.from_user.mention_html()}\n"
            f"💬 {chat_title}\n"
            f"📝 {subject}\n"
            f"{pri_emoji} Приоритет: {priority}"
        )
        return True

    # Пользователь пишет ответ в открытый тикет
    elif step == "user_reply":
        ticket_id = state["ticket_id"]
        t = await db.ticket_get(ticket_id)
        if not t or t["status"] == "closed":
            ticket_states.pop(uid, None)
            await message.answer("⚠️ Тикет закрыт")
            return True

        await db.ticket_msg_add(
            ticket_id=ticket_id,
            sender_id=uid,
            sender_name=message.from_user.full_name,
            is_mod=False,
            text=text.strip()
        )
        ticket_states.pop(uid, None)

        await message.answer(
            f"✅ Сообщение отправлено в тикет #{ticket_id}",
            reply_markup=kb_ticket_user(ticket_id)
        )

        # Лог в канал
        await _log(bot,
            f"━━━━━━━━━━━━━━━\n"
            f"💬 <b>ОТВЕТ В ТИКЕТ #{ticket_id}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 {message.from_user.mention_html()}:\n"
            f"{text.strip()[:200]}"
        )

        # Уведомляем назначенного мода
        if t["assigned_mod_id"]:
            try:
                await bot.send_message(
                    t["assigned_mod_id"],
                    f"━━━━━━━━━━━━━━━\n"
                    f"💬 <b>ОТВЕТ В ТИКЕТ #{ticket_id}</b>\n"
                    f"━━━━━━━━━━━━━━━\n\n"
                    f"👤 {message.from_user.full_name}:\n{text.strip()}",
                    parse_mode="HTML",
                    reply_markup=kb_mod_ticket(ticket_id)
                )
            except: pass
        return True

    return False


# ══════════════════════════════════════════
#  CALLBACK — модератор
# ══════════════════════════════════════════

async def cb_ticket_mod(call: CallbackQuery, bot: Bot):
    """Обработчик кнопок для модераторов (tktm:...)"""
    mod_id   = call.from_user.id
    mod_name = call.from_user.full_name
    parts    = call.data.split(":")

    action = parts[1]

    if action == "open":
        ticket_id = int(parts[2])
        t    = await db.ticket_get(ticket_id)
        msgs = await db.ticket_msgs(ticket_id)
        if not t:
            await call.answer("Тикет не найден", show_alert=True)
            return
        await call.message.edit_text(
            fmt_ticket(dict(t), show_msgs=True, msgs=[dict(m) for m in msgs]),
            parse_mode="HTML",
            reply_markup=kb_mod_ticket(ticket_id)
        )

    elif action == "assign":
        ticket_id = int(parts[2])
        await db.ticket_assign(ticket_id, mod_id, mod_name)
        await call.answer(f"✅ Тикет #{ticket_id} назначен тебе", show_alert=False)
        t = await db.ticket_get(ticket_id)
        await call.message.edit_text(
            fmt_ticket(dict(t)),
            parse_mode="HTML",
            reply_markup=kb_mod_ticket(ticket_id)
        )
        # Лог
        await _log(bot,
            f"━━━━━━━━━━━━━━━\n"
            f"👮 <b>ТИКЕТ #{ticket_id} ВЗЯТ</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👮 {call.from_user.mention_html()}"
        )
        # Уведомить пользователя
        try:
            await bot.send_message(
                t["uid"],
                f"━━━━━━━━━━━━━━━\n"
                f"🔄 <b>ТИКЕТ #{ticket_id}</b>\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"👮 Модератор <b>{mod_name}</b> взял твоё обращение!\n"
                f"Скоро ответим.",
                parse_mode="HTML"
            )
        except: pass

    elif action == "reply":
        ticket_id = int(parts[2])
        t = await db.ticket_get(ticket_id)
        if not t:
            await call.answer("Тикет не найден", show_alert=True)
            return
        mod_reply_states[mod_id] = ticket_id
        await call.message.edit_text(
            f"━━━━━━━━━━━━━━━\n"
            f"✏️ <b>ОТВЕТ В ТИКЕТ #{ticket_id}</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"👤 Пользователь: {t['user_name']}\n"
            f"📝 Тема: {t['subject']}\n\n"
            f"Напиши ответ следующим сообщением:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"tktm:open:{ticket_id}")
            ]])
        )

    elif action == "close":
        ticket_id = int(parts[2])
        t = await db.ticket_get(ticket_id)
        if not t:
            await call.answer("Тикет не найден", show_alert=True)
            return
        await db.ticket_close(ticket_id)
        await call.message.edit_text(
            f"━━━━━━━━━━━━━━━\n"
            f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"Закрыл: <b>{mod_name}</b>",
            parse_mode="HTML"
        )
        # Лог
        await _log(bot,
            f"━━━━━━━━━━━━━━━\n"
            f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👮 {call.from_user.mention_html()} (модератор)"
        )
        # Уведомить пользователя
        try:
            await bot.send_message(
                t["uid"],
                f"━━━━━━━━━━━━━━━\n"
                f"✅ <b>ТИКЕТ #{ticket_id} ЗАКРЫТ</b>\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"Твоё обращение закрыто модератором <b>{mod_name}</b>.\n"
                f"Если нужна ещё помощь — напиши /ticket",
                parse_mode="HTML"
            )
        except: pass

    elif action == "pri":
        priority  = parts[2]
        ticket_id = int(parts[3])
        await db.ticket_set_priority(ticket_id, priority)
        await call.answer(f"Приоритет изменён: {priority}")
        t = await db.ticket_get(ticket_id)
        await call.message.edit_text(
            fmt_ticket(dict(t)),
            parse_mode="HTML",
            reply_markup=kb_mod_ticket(ticket_id)
        )

    elif action == "history":
        ticket_id = int(parts[2])
        t    = await db.ticket_get(ticket_id)
        msgs = await db.ticket_msgs(ticket_id)
        if not t:
            await call.answer("Тикет не найден", show_alert=True)
            return
        await call.message.edit_text(
            fmt_ticket(dict(t), show_msgs=True, msgs=[dict(m) for m in msgs]),
            parse_mode="HTML",
            reply_markup=kb_mod_ticket(ticket_id)
        )

    await call.answer()


# ══════════════════════════════════════════
#  ОТВЕТ МОДЕРАТОРА (текстовое сообщение)
# ══════════════════════════════════════════

async def handle_mod_reply(message: Message, bot: Bot) -> bool:
    """
    Если модератор в состоянии ответа — обрабатываем его сообщение.
    Возвращает True если обработано.
    """
    mod_id = message.from_user.id
    if mod_id not in mod_reply_states:
        return False

    ticket_id = mod_reply_states.pop(mod_id)
    t = await db.ticket_get(ticket_id)
    if not t:
        await message.answer("⚠️ Тикет не найден")
        return True
    if t["status"] == "closed":
        await message.answer("⚠️ Тикет уже закрыт")
        return True

    text = (message.text or "").strip()
    if not text:
        await message.answer("⚠️ Пустое сообщение, попробуй снова")
        return True

    await db.ticket_msg_add(
        ticket_id=ticket_id,
        sender_id=mod_id,
        sender_name=message.from_user.full_name,
        is_mod=True,
        text=text
    )

    # Если тикет не взят — автоматически назначаем
    if not t["assigned_mod_id"]:
        await db.ticket_assign(ticket_id, mod_id, message.from_user.full_name)

    await message.answer(
        f"✅ Ответ отправлен в тикет #{ticket_id}",
        reply_markup=kb_mod_ticket(ticket_id)
    )

    # Лог в канал
    await _log(bot,
        f"━━━━━━━━━━━━━━━\n"
        f"💬 <b>ОТВЕТ МОДЕРАТОРА #{ticket_id}</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👮 {message.from_user.mention_html()}:\n"
        f"{text[:200]}"
    )

    # Отправляем пользователю
    try:
        await bot.send_message(
            t["uid"],
            f"━━━━━━━━━━━━━━━\n"
            f"💬 <b>ОТВЕТ ПО ТИКЕТУ #{ticket_id}</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"👮 <b>{message.from_user.full_name}</b>:\n\n"
            f"{text}",
            parse_mode="HTML",
            reply_markup=kb_ticket_user(ticket_id)
        )
    except Exception as e:
        log.warning(f"Не удалось отправить ответ по тикету пользователю: {e}")
        await message.answer("⚠️ Не удалось доставить ответ пользователю (он не писал боту в ЛС)")

    return True


# ══════════════════════════════════════════
#  УВЕДОМЛЕНИЕ МОДЕРАТОРОВ О НОВОМ ТИКЕТЕ
# ══════════════════════════════════════════

async def notify_mods_new_ticket(ticket_id: int, uid: int, user_name: str,
                                  chat_title: str, subject: str, priority: str,
                                  bot: Bot, admin_ids: set, mod_roles_getter):
    """Рассылает уведомление через shared модуль"""
    await _shared.notify_new_ticket(ticket_id, user_name, subject, chat_title, priority)


# ══════════════════════════════════════════
#  ПАНЕЛЬ ТИКЕТОВ ДЛЯ МОДЕРАТОРОВ
# ══════════════════════════════════════════

async def show_tickets_panel(message_or_call, bot: Bot, is_mod: bool = True):
    """Показывает список тикетов — вызывается из /panel"""
    tickets = await db.ticket_list(status="open", limit=15)
    stats   = await db.ticket_stats_all()

    text = (
        f"━━━━━━━━━━━━━━━\n"
        f"🎫 <b>ТИКЕТЫ</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🆕 Открытых: <b>{stats['open']}</b>\n"
        f"🔄 В работе: <b>{stats['in_progress']}</b>\n"
        f"✅ Закрытых: <b>{stats['closed']}</b>\n"
        f"📊 Всего: <b>{stats['total']}</b>\n\n"
        f"<b>Открытые тикеты:</b>"
    )

    kb_rows = []
    for t in [dict(x) for x in tickets][:8]:
        pri   = PRIORITY_EMOJI.get(t["priority"], "🟡")
        label = f"{pri} #{t['id']} {t['user_name'] or '?'} — {t['subject'][:25]}"
        kb_rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"tktm:open:{t['id']}"
        )])

    kb_rows.append([
        InlineKeyboardButton(text="🔄 В работе", callback_data="tktm:list:in_progress"),
        InlineKeyboardButton(text="✅ Закрытые", callback_data="tktm:list:closed"),
    ])
    kb_rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    if hasattr(message_or_call, "message"):
        await message_or_call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        await message_or_call.answer()
    else:
        await message_or_call.answer(text, parse_mode="HTML", reply_markup=kb)

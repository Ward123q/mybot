import asyncio
import logging
import random
import os
import aiohttp
from datetime import timedelta
from collections import defaultdict
from time import time

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, ChatPermissions, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiohttp import web

# ╔══════════════════════════════════════════════════════╗
#                   НАСТРОЙКИ БОТА
# ╚══════════════════════════════════════════════════════╝

BOT_TOKEN       = os.getenv("8668580535:AAEdYlluEfSWKqEf8E_YS-LX2emDSXtvIq4")
OPENAI_API_KEY  = os.getenv("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")
WEATHER_API_KEY = os.getenv("bae207595312fc0eea33c64bde578c71")

MAX_WARNINGS     = 3
FLOOD_LIMIT      = 5
FLOOD_TIME       = 5
ANTI_MAT_ENABLED = True
MAT_MUTE_MINUTES = 5

WELCOME_TEXT = "👋 Привет, {name}! Добро пожаловать!\nНапиши /rules — правила, /help — команды."

RULES_TEXT = (
    "📜 <b>Правила чата:</b>\n\n"
    "1. Уважайте друг друга\n"
    "2. Без спама и флуда\n"
    "3. Без оскорблений и мата\n"
    "4. Только по теме чата\n\n"
    "Нарушение — предупреждение — бан 🔨"
)

# Триггер-слова → автоответы
TRIGGER_RESPONSES = {
    "привет":    ["Привет! 👋", "Здарова! 😎", "О, живой человек! 👀"],
    "помогите":  ["Чем помочь? Напиши подробнее! 🤝", "Слушаю! Что случилось? 👂"],
    "скучно":    ["Поиграй в /8ball или проверь /iq 😄", "Брось кубик /roll 🎲"],
    "спасибо":   ["Пожалуйста! 😊", "Всегда рад помочь! 🤗", "Не за что! 👍"],
    "хорошо":    ["Вот и отлично! 😄", "Рад слышать! 🎉"],
    "утро":      ["Доброе утро! ☀️", "Утречко! ☕"],
    "ночь":      ["Спокойной ночи! 🌙", "Отдыхай! 😴"],
    "го":        ["Куда? 😂", "Поехали! 🚀"],
}

# ════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

warnings      = defaultdict(lambda: defaultdict(int))
flood_tracker = defaultdict(lambda: defaultdict(list))
notes         = defaultdict(dict)
afk_users     = {}
pending       = {}

# Статистика: chat_id -> user_id -> кол-во сообщений
chat_stats    = defaultdict(lambda: defaultdict(int))

# Репутация: chat_id -> user_id -> очки
reputation    = defaultdict(lambda: defaultdict(int))
rep_cooldown  = {}  # ключ: (chat_id, from_user_id, target_id) -> время

# История ИИ: user_id -> список сообщений
ai_history    = defaultdict(list)

MAT_WORDS = [
    "блядь","блять","бля","сука","пизда","пиздец",
    "хуй","хуйня","хуета","нахуй","похуй",
    "ебать","ебал","ёбаный","еблан","заебал",
    "мудак","мудила","долбоёб","долбоеб",
    "уёбок","уебок","шлюха","курва",
    "пиздобол","хуесос","залупа",
]

MAT_RESPONSES = [
    "🤬 {name}, какого хуя ты материшься?! Рот закрой на {minutes} мин.",
    "🧼 {name}, блядь, ещё раз — и навсегда заткну! Мут {minutes} мин.",
    "😤 {name}, пиздец твоим манерам! Мут на {minutes} мин.",
    "🚿 {name}, иди язык мылом помой, нахуй! Мут {minutes} мин.",
    "📵 {name}, охуел что ли? Мут {minutes} мин., иди остынь.",
]

MUTE_MESSAGES = [
    "🔇 {name} заткнут на {time}! Сиди молча, нахуй.",
    "😶 {name} — в рот воды набрал на {time}. Туда и дорога.",
    "🤫 {name} получил мут на {time}. Надоело слушать эту хуйню.",
]

BAN_MESSAGES = [
    "🔨 {name} — пиздец, улетел в бан! Причина: {reason}",
    "💥 {name} забанен нахуй! Причина: {reason}",
    "🚪 {name} выпнут из чата! Причина: {reason}",
]

WARN_MESSAGES = [
    "⚠️ {name} получил варн {count}/{max}! Причина: {reason}. Ещё раз — и пиздец.",
    "🚨 {name}, это предупреждение {count}/{max}! Причина: {reason}. Следи за собой.",
    "😡 {name} — варн {count}/{max}! Причина: {reason}. Ещё немного и улетишь.",
]

AUTOBAN_MESSAGES = [
    "🔨 {name} набрал {max} варнов и получил автобан! Пока-пока, нахуй.",
    "💀 {name} — пиздец! {max} варнов = бан. Скатертью дорога.",
]

RANDOM_BAN_REASONS = [
    "слишком умный, бля",
    "подозрение в адекватности, нахуй",
    "нарушение закона бутерброда",
    "превышение лимита здравого смысла",
    "слово 'кринж' больше 3 раз, пиздец",
    "нарушение пространственно-временного континуума",
]

QUOTES = [
    "«Не важно, как медленно ты идёшь, главное — не останавливаться.» — Конфуций",
    "«Жизнь — это то, что случается, пока строишь другие планы.» — Леннон",
    "«Будь собой. Все остальные роли заняты.» — Уайльд",
    "«Единственный способ делать великое — любить то, что делаешь.» — Джобс",
    "«Успех — идти от неудачи к неудаче без потери энтузиазма.» — Черчилль",
]

BALL_ANSWERS = [
    "🟢 Да, нахуй, однозначно!", "🟢 Без сомнений!", "🟢 Скорее всего да.",
    "🟡 Хуй его знает, спроси позже.", "🟡 Трудно сказать, бля.", "🟡 Да кто ж знает.",
    "🔴 Нет, нахуй.", "🔴 Однозначно нет!", "🔴 Даже не думай.",
]


# ─────────────────────────────────────────────────────
#  ВЕБ-СЕРВЕР (чтобы Render не засыпал)
# ─────────────────────────────────────────────────────

async def health(request):
    return web.Response(text="OK")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()


# ─────────────────────────────────────────────────────
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────

async def check_admin(message: Message) -> bool:
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return member.status in ("administrator", "creator")

async def is_admin_by_id(chat_id: int, user_id: int) -> bool:
    member = await bot.get_chat_member(chat_id, user_id)
    return member.status in ("administrator", "creator")

async def require_admin(message: Message) -> bool:
    if not await check_admin(message):
        await message.reply("🚫 <b>Только для администраторов, нахуй!</b>", parse_mode="HTML")
        return False
    return True

def contains_mat(text: str) -> bool:
    t = text.lower().replace("ё", "е")
    for w in MAT_WORDS:
        if w.replace("ё", "е") in t:
            return True
    return False

def parse_duration(arg: str):
    arg = arg.strip().lower()
    try:
        if arg.endswith("m"):
            m = int(arg[:-1]); return m, f"{m} мин."
        elif arg.endswith("h"):
            h = int(arg[:-1]); return h * 60, f"{h} ч."
        elif arg.endswith("d"):
            d = int(arg[:-1]); return d * 60 * 24, f"{d} дн."
        elif arg.isdigit():
            m = int(arg); return m, f"{m} мин."
    except ValueError:
        pass
    return None, None


# ─────────────────────────────────────────────────────
#  ИИ — ChatGPT
# ─────────────────────────────────────────────────────

async def ask_gpt(user_id: int, user_message: str) -> str:
    if not OPENAI_API_KEY:
        return "❌ OpenAI API ключ не настроен."

    ai_history[user_id].append({"role": "user", "content": user_message})
    # Храним последние 10 сообщений
    if len(ai_history[user_id]) > 10:
        ai_history[user_id] = ai_history[user_id][-10:]

    messages = [
        {"role": "system", "content": "Ты умный и дружелюбный помощник в Telegram чате. Отвечай кратко и по делу на русском языке."}
    ] + ai_history[user_id]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-3.5-turbo",
                    "messages": messages,
                    "max_tokens": 500,
                    "temperature": 0.7
                }
            ) as resp:
                data = await resp.json()
                if "choices" in data:
                    reply = data["choices"][0]["message"]["content"]
                    ai_history[user_id].append({"role": "assistant", "content": reply})
                    return reply
                else:
                    return f"❌ Ошибка API: {data.get('error', {}).get('message', 'Неизвестная ошибка')}"
    except Exception as e:
        return f"❌ Ошибка подключения: {e}"


# ─────────────────────────────────────────────────────
#  ПОГОДА
# ─────────────────────────────────────────────────────

async def get_weather(city: str) -> str:
    if not WEATHER_API_KEY:
        return "❌ Weather API ключ не настроен."
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={
                    "q": city,
                    "appid": WEATHER_API_KEY,
                    "units": "metric",
                    "lang": "ru"
                }
            ) as resp:
                data = await resp.json()
                if data.get("cod") != 200:
                    return f"❌ Город <b>{city}</b> не найден."

                name    = data["name"]
                temp    = round(data["main"]["temp"])
                feels   = round(data["main"]["feels_like"])
                desc    = data["weather"][0]["description"].capitalize()
                humid   = data["main"]["humidity"]
                wind    = round(data["wind"]["speed"])
                emoji   = "☀️" if "ясно" in desc.lower() else (
                          "🌧" if "дождь" in desc.lower() else (
                          "❄️" if "снег" in desc.lower() else (
                          "⛅" if "облач" in desc.lower() else "🌤")))

                return (
                    f"{emoji} <b>Погода в {name}</b>\n\n"
                    f"🌡 Температура: <b>{temp}°C</b> (ощущается {feels}°C)\n"
                    f"📋 {desc}\n"
                    f"💧 Влажность: <b>{humid}%</b>\n"
                    f"💨 Ветер: <b>{wind} м/с</b>"
                )
    except Exception as e:
        return f"❌ Ошибка: {e}"


# ─────────────────────────────────────────────────────
#  КЛАВИАТУРЫ
# ─────────────────────────────────────────────────────

def kb_main_panel(target_id: int, target_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔇 Мут", callback_data=f"panel:mute:{target_id}"),
            InlineKeyboardButton(text="🔊 Размут", callback_data=f"panel:unmute:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="⚠️ Варн", callback_data=f"panel:warn:{target_id}"),
            InlineKeyboardButton(text="✅ Снять варн", callback_data=f"panel:unwarn:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🔨 Бан", callback_data=f"panel:ban:{target_id}"),
            InlineKeyboardButton(text="♻️ Разбан", callback_data=f"panel:unban:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить сообщение", callback_data=f"panel:del:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="📊 Информация", callback_data=f"panel:info:{target_id}"),
            InlineKeyboardButton(text="❌ Закрыть", callback_data="panel:close:0"),
        ],
    ])

def kb_mute_time(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 мин", callback_data=f"mute:{target_id}:5"),
            InlineKeyboardButton(text="15 мин", callback_data=f"mute:{target_id}:15"),
            InlineKeyboardButton(text="30 мин", callback_data=f"mute:{target_id}:30"),
        ],
        [
            InlineKeyboardButton(text="1 час", callback_data=f"mute:{target_id}:60"),
            InlineKeyboardButton(text="3 часа", callback_data=f"mute:{target_id}:180"),
            InlineKeyboardButton(text="12 часов", callback_data=f"mute:{target_id}:720"),
        ],
        [
            InlineKeyboardButton(text="1 день", callback_data=f"mute:{target_id}:1440"),
            InlineKeyboardButton(text="7 дней", callback_data=f"mute:{target_id}:10080"),
            InlineKeyboardButton(text="✏️ Своё время", callback_data=f"mute:{target_id}:custom"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")],
    ])

def kb_warn_options(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤬 Мат", callback_data=f"warn:{target_id}:Мат в чате"),
            InlineKeyboardButton(text="💬 Спам", callback_data=f"warn:{target_id}:Спам"),
        ],
        [
            InlineKeyboardButton(text="😡 Оскорбление", callback_data=f"warn:{target_id}:Оскорбление"),
            InlineKeyboardButton(text="🚫 Флуд", callback_data=f"warn:{target_id}:Флуд"),
        ],
        [
            InlineKeyboardButton(text="📵 Реклама", callback_data=f"warn:{target_id}:Реклама"),
            InlineKeyboardButton(text="✏️ Своя причина", callback_data=f"warn:{target_id}:custom"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")],
    ])

def kb_ban_options(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤬 Грубые нарушения", callback_data=f"ban:{target_id}:Грубые нарушения правил"),
            InlineKeyboardButton(text="💬 Спам/реклама", callback_data=f"ban:{target_id}:Спам и реклама"),
        ],
        [
            InlineKeyboardButton(text="🔞 Неприемлемый контент", callback_data=f"ban:{target_id}:Неприемлемый контент"),
            InlineKeyboardButton(text="🤖 Бот/накрутка", callback_data=f"ban:{target_id}:Бот или накрутка"),
        ],
        [
            InlineKeyboardButton(text="✏️ Своя причина", callback_data=f"ban:{target_id}:custom"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")],
    ])


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — СТАТИСТИКА
# ─────────────────────────────────────────────────────

class StatsMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group", "supergroup"):
            chat_stats[event.chat.id][event.from_user.id] += 1
        return await handler(event, data)


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — АНТИФЛУД
# ─────────────────────────────────────────────────────

class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if event.chat.type not in ("group", "supergroup"): return await handler(event, data)
        if event.text and event.text.startswith("/"): return await handler(event, data)
        uid = event.from_user.id
        cid = event.chat.id
        member = await bot.get_chat_member(cid, uid)
        if member.status in ("administrator", "creator"): return await handler(event, data)
        now = time()
        flood_tracker[cid][uid] = [t for t in flood_tracker[cid][uid] if now - t < FLOOD_TIME]
        flood_tracker[cid][uid].append(now)
        if len(flood_tracker[cid][uid]) >= FLOOD_LIMIT:
            await event.delete()
            await bot.restrict_chat_member(cid, uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=timedelta(minutes=5))
            await event.answer(
                f"🚫 {event.from_user.mention_html()}, заткнись на 5 минут, достал флудить!",
                parse_mode="HTML")
            flood_tracker[cid][uid].clear()
            return
        return await handler(event, data)


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — АНТИМАТ
# ─────────────────────────────────────────────────────

class AntiMatMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if not ANTI_MAT_ENABLED: return await handler(event, data)
        if event.chat.type not in ("group", "supergroup"): return await handler(event, data)
        if not event.text or event.text.startswith("/"): return await handler(event, data)
        uid = event.from_user.id
        cid = event.chat.id
        member = await bot.get_chat_member(cid, uid)
        if member.status in ("administrator", "creator"): return await handler(event, data)
        if contains_mat(event.text):
            await event.delete()
            await bot.restrict_chat_member(cid, uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=timedelta(minutes=MAT_MUTE_MINUTES))
            resp = random.choice(MAT_RESPONSES).format(
                name=event.from_user.mention_html(), minutes=MAT_MUTE_MINUTES)
            sent = await event.answer(resp, parse_mode="HTML")
            await asyncio.sleep(10)
            try: await sent.delete()
            except: pass
            return
        return await handler(event, data)


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — AFK
# ─────────────────────────────────────────────────────

class AfkMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if event.from_user and event.from_user.id in afk_users:
            if not (event.text and event.text.startswith("/afk")):
                reason = afk_users.pop(event.from_user.id)
                await event.answer(
                    f"👋 {event.from_user.mention_html()} вернулся из AFK! (был: {reason})",
                    parse_mode="HTML")
        if event.reply_to_message and event.reply_to_message.from_user:
            tid = event.reply_to_message.from_user.id
            if tid in afk_users:
                await event.answer(
                    f"😴 {event.reply_to_message.from_user.mention_html()} сейчас AFK: {afk_users[tid]}",
                    parse_mode="HTML")
        return await handler(event, data)


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — ПЕРЕХВАТ ВВОДА ДЛЯ ПАНЕЛИ
# ─────────────────────────────────────────────────────

class PendingInputMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        uid = event.from_user.id if event.from_user else None
        if uid and uid in pending and not (event.text and event.text.startswith("/")):
            p = pending.pop(uid)
            action      = p["action"]
            target_id   = p["target_id"]
            target_name = p["target_name"]
            chat_id     = p["chat_id"]
            text        = event.text or ""
            if action == "mute_custom":
                mins, label = parse_duration(text)
                if mins:
                    await bot.restrict_chat_member(chat_id, target_id,
                        permissions=ChatPermissions(can_send_messages=False),
                        until_date=timedelta(minutes=mins))
                    msg = random.choice(MUTE_MESSAGES).format(name=f"<b>{target_name}</b>", time=label)
                    await event.answer(msg, parse_mode="HTML")
                else:
                    await event.reply("❗ Неверный формат. Примеры: 10, 10m, 2h, 1d")
            elif action == "warn_custom":
                reason = text.strip() or "Нарушение правил"
                warnings[chat_id][target_id] += 1
                count = warnings[chat_id][target_id]
                if count >= MAX_WARNINGS:
                    await bot.ban_chat_member(chat_id, target_id)
                    warnings[chat_id][target_id] = 0
                    msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{target_name}</b>", max=MAX_WARNINGS)
                else:
                    msg = random.choice(WARN_MESSAGES).format(
                        name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
                await event.answer(msg, parse_mode="HTML")
            elif action == "ban_custom":
                reason = text.strip() or "Нарушение правил"
                await bot.ban_chat_member(chat_id, target_id)
                msg = random.choice(BAN_MESSAGES).format(name=f"<b>{target_name}</b>", reason=reason)
                await event.answer(msg, parse_mode="HTML")
            await event.delete()
            return
        return await handler(event, data)


dp.message.middleware(PendingInputMiddleware())
dp.message.middleware(StatsMiddleware())
dp.message.middleware(AntiFloodMiddleware())
dp.message.middleware(AntiMatMiddleware())
dp.message.middleware(AfkMiddleware())


# ─────────────────────────────────────────────────────
#  /start
# ─────────────────────────────────────────────────────

@dp.message(Command("start"))
async def start_command(message: Message):
    await message.reply(
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "Я бот-модератор этого чата.\n"
        "📜 /rules — правила\n"
        "❓ /help — помощь\n\n"
        "<i>Панель управления — только для администраторов.</i>",
        parse_mode="HTML")

@dp.message(F.new_chat_members)
async def welcome_new_member(message: Message):
    for m in message.new_chat_members:
        if m.is_bot: continue
        await message.answer(WELCOME_TEXT.format(name=m.mention_html()), parse_mode="HTML")

@dp.message(Command("rules"))
async def rules_command(message: Message):
    await message.reply(RULES_TEXT, parse_mode="HTML")

@dp.message(Command("help"))
async def help_command(message: Message):
    is_adm = await check_admin(message)
    user_help = (
        "❓ <b>Доступные команды:</b>\n\n"
        "📜 /rules — правила чата\n\n"
        "🤖 <b>ИИ:</b>\n"
        "/ai [вопрос] — спросить ChatGPT 🧠\n"
        "/aiclear — сбросить историю диалога\n\n"
        "🌤 <b>Погода:</b>\n"
        "/weather [город] — погода в городе\n\n"
        "⭐ <b>Репутация:</b>\n"
        "/rep — узнать репутацию (реплай)\n"
        "+1 или -1 в реплае — дать/снять очко\n\n"
        "📊 <b>Статистика:</b>\n"
        "/top — топ активных участников\n\n"
        "🎮 <b>Игры и развлечения:</b>\n"
        "/roll [N] — бросить кубик 🎲\n"
        "/flip — подбросить монету 🪙\n"
        "/8ball [вопрос] — шар предсказаний 🎱\n"
        "/rate [что] — оценить что угодно ⭐\n"
        "/iq — проверить IQ 🧠\n"
        "/gay — шуточный % 🌈\n"
        "/quote — цитата дня 💬\n"
        "/afk [причина] — уйти в AFK 😴\n"
        "/info — инфо о пользователе (реплай)\n"
        "/warnings — варны (реплай)\n"
        "/note get [имя] — заметка\n"
        "/note list — список заметок\n"
    )
    admin_help = (
        "\n\n👮 <b>Команды для администраторов:</b>\n"
        "/panel — панель управления (реплай на юзера)\n"
        "/ban [причина] — бан (реплай)\n"
        "/unban — разбан (реплай)\n"
        "/mute [время] — мут: 10m, 2h, 1d (реплай)\n"
        "/unmute — размут (реплай)\n"
        "/warn [причина] — варн (реплай)\n"
        "/unwarn — снять варн (реплай)\n"
        "/del — удалить сообщение (реплай)\n"
        "/clear [N] — очистить N сообщений\n"
        "/announce [текст] — объявление\n"
        "/pin — закрепить (реплай)\n"
        "/unpin — открепить (реплай)\n"
        "/lock — заблокировать чат\n"
        "/unlock — разблокировать чат\n"
        "/slowmode [сек] — медленный режим\n"
        "/promote [должность] — выдать тег (реплай)\n"
        "/poll Вопрос|Вар1|Вар2 — голосование\n"
        "/antimat on/off — антимат\n"
        "/rban — шуточный бан (реплай) 😄\n"
        "/note set/del [имя] [текст] — заметки\n"
    )
    await message.reply(user_help + (admin_help if is_adm else ""), parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  ИИ КОМАНДЫ
# ─────────────────────────────────────────────────────

@dp.message(Command("ai"))
async def ai_cmd(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("🤖 Напиши вопрос: /ai [вопрос]"); return
    wait_msg = await message.reply("🤔 Думаю...")
    answer = await ask_gpt(message.from_user.id, command.args)
    await wait_msg.edit_text(f"🤖 <b>ChatGPT:</b>\n\n{answer}", parse_mode="HTML")

@dp.message(Command("aiclear"))
async def ai_clear_cmd(message: Message):
    ai_history[message.from_user.id].clear()
    await message.reply("🗑 История диалога с ИИ очищена!")


# ─────────────────────────────────────────────────────
#  ПОГОДА
# ─────────────────────────────────────────────────────

@dp.message(Command("weather"))
async def weather_cmd(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("🌤 Укажи город: /weather Москва"); return
    wait_msg = await message.reply("⏳ Получаю данные...")
    result = await get_weather(command.args)
    await wait_msg.edit_text(result, parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  РЕПУТАЦИЯ
# ─────────────────────────────────────────────────────

@dp.message(Command("rep"))
async def rep_cmd(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target = message.reply_to_message.from_user
    score  = reputation[message.chat.id][target.id]
    emoji  = "⭐" if score >= 0 else "💀"
    await message.reply(
        f"{emoji} Репутация {target.mention_html()}: <b>{score:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"+1", "+", "👍"}))
async def rep_plus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id:
        await message.reply("😏 Себе репу не накручивай!"); return
    key = (message.chat.id, message.from_user.id, target.id)
    now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        left = int(3600 - (now - rep_cooldown[key]))
        await message.reply(f"⏳ Подожди ещё {left//60} мин. перед следующим голосом."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] += 1
    score = reputation[message.chat.id][target.id]
    await message.reply(
        f"⬆️ {target.mention_html()} +1 к репутации! Теперь: <b>{score:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"-1", "-", "👎"}))
async def rep_minus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id:
        await message.reply("😏 Себе репу не снижай!"); return
    key = (message.chat.id, message.from_user.id, target.id)
    now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        left = int(3600 - (now - rep_cooldown[key]))
        await message.reply(f"⏳ Подожди ещё {left//60} мин. перед следующим голосом."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] -= 1
    score = reputation[message.chat.id][target.id]
    await message.reply(
        f"⬇️ {target.mention_html()} -1 к репутации! Теперь: <b>{score:+d}</b>",
        parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  СТАТИСТИКА — ТОП АКТИВНЫХ
# ─────────────────────────────────────────────────────

@dp.message(Command("top"))
async def top_cmd(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats:
        await message.reply("📊 Статистика пока пуста — пишите больше! 😄"); return
    sorted_users = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"] + ["4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["📊 <b>Топ активных участников:</b>\n"]
    for i, (uid, count) in enumerate(sorted_users):
        try:
            member = await bot.get_chat_member(message.chat.id, uid)
            name = member.user.full_name
        except:
            name = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{name}</b> — {count} сообщений")
    await message.reply("\n".join(lines), parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  АВТООТВЕТЫ НА ТРИГГЕР-СЛОВА
# ─────────────────────────────────────────────────────

@dp.message(F.text & ~F.text.startswith("/"))
async def auto_reply_handler(message: Message):
    if not message.text: return
    text_lower = message.text.lower()
    for trigger, responses in TRIGGER_RESPONSES.items():
        if trigger in text_lower:
            # Отвечаем с вероятностью 40% чтобы не спамить
            if random.random() < 0.4:
                await message.reply(random.choice(responses))
            break


# ─────────────────────────────────────────────────────
#  /panel
# ─────────────────────────────────────────────────────

@dp.message(Command("panel"))
async def admin_panel(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя чтобы открыть панель.")
        return
    target = message.reply_to_message.from_user
    warns  = warnings[message.chat.id].get(target.id, 0)
    await message.reply(
        f"🛠 <b>Панель управления</b>\n\n"
        f"👤 Пользователь: {target.mention_html()}\n"
        f"🆔 ID: <code>{target.id}</code>\n"
        f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n\n"
        f"Выбери действие:",
        parse_mode="HTML",
        reply_markup=kb_main_panel(target.id, target.full_name)
    )


# ─────────────────────────────────────────────────────
#  ОБРАБОТЧИКИ КНОПОК ПАНЕЛИ
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("panel:"))
async def panel_callback(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True)
        return
    _, action, target_id_str = call.data.split(":", 2)
    target_id = int(target_id_str)
    chat_id   = call.message.chat.id
    try:
        target_member = await bot.get_chat_member(chat_id, target_id)
        target_name   = target_member.user.full_name
    except:
        target_name = f"ID {target_id}"

    if action == "close":
        await call.message.delete()
    elif action == "back":
        warns = warnings[chat_id].get(target_id, 0)
        try:
            tm = await bot.get_chat_member(chat_id, target_id)
            mention = tm.user.mention_html()
        except:
            mention = f"<code>{target_id}</code>"
        await call.message.edit_text(
            f"🛠 <b>Панель управления</b>\n\n"
            f"👤 Пользователь: {mention}\n"
            f"🆔 ID: <code>{target_id}</code>\n"
            f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n\n"
            f"Выбери действие:",
            parse_mode="HTML",
            reply_markup=kb_main_panel(target_id, target_name))
    elif action == "mute":
        await call.message.edit_text(
            f"🔇 <b>Мут для {target_name}</b>\n\nВыбери время:",
            parse_mode="HTML", reply_markup=kb_mute_time(target_id))
    elif action == "unmute":
        await bot.restrict_chat_member(chat_id, target_id,
            permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
        await call.message.edit_text(f"🔊 <b>{target_name}</b> размучен.", parse_mode="HTML")
    elif action == "warn":
        await call.message.edit_text(
            f"⚠️ <b>Варн для {target_name}</b>\n\nВыбери причину:",
            parse_mode="HTML", reply_markup=kb_warn_options(target_id))
    elif action == "unwarn":
        if warnings[chat_id][target_id] > 0:
            warnings[chat_id][target_id] -= 1
        count = warnings[chat_id][target_id]
        await call.message.edit_text(
            f"✅ С <b>{target_name}</b> снят варн. Осталось: <b>{count}/{MAX_WARNINGS}</b>",
            parse_mode="HTML")
    elif action == "ban":
        await call.message.edit_text(
            f"🔨 <b>Бан для {target_name}</b>\n\nВыбери причину:",
            parse_mode="HTML", reply_markup=kb_ban_options(target_id))
    elif action == "unban":
        await bot.unban_chat_member(chat_id, target_id, only_if_banned=True)
        await call.message.edit_text(f"♻️ <b>{target_name}</b> разбанен.", parse_mode="HTML")
    elif action == "del":
        try: await call.message.reply_to_message.delete()
        except: pass
        await call.message.edit_text("🗑 Сообщение удалено.")
    elif action == "info":
        try:
            tm = await bot.get_chat_member(chat_id, target_id)
            user  = tm.user
            warns = warnings[chat_id].get(target_id, 0)
            rep   = reputation[chat_id].get(target_id, 0)
            msgs  = chat_stats[chat_id].get(target_id, 0)
            status_map = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
                "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
            afk = f"\n😴 AFK: {afk_users[target_id]}" if target_id in afk_users else ""
            await call.message.edit_text(
                f"👤 <b>Информация:</b>\n\n"
                f"🏷 Имя: {user.mention_html()}{afk}\n"
                f"🔗 Username: {'@'+user.username if user.username else 'нет'}\n"
                f"🆔 ID: <code>{user.id}</code>\n"
                f"📌 Статус: {status_map.get(tm.status, tm.status)}\n"
                f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
                f"⭐ Репутация: <b>{rep:+d}</b>\n"
                f"💬 Сообщений: <b>{msgs}</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")
                ]]))
        except Exception as e:
            await call.answer(f"Ошибка: {e}", show_alert=True)
    await call.answer()


@dp.callback_query(F.data.startswith("mute:"))
async def mute_callback(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts     = call.data.split(":")
    target_id = int(parts[1])
    time_val  = parts[2]
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
    except:
        target_name = f"ID {target_id}"
    if time_val == "custom":
        pending[call.from_user.id] = {"action":"mute_custom","target_id":target_id,
            "target_name":target_name,"chat_id":chat_id}
        await call.message.edit_text(
            f"✏️ Введи время мута для <b>{target_name}</b>:\n\n"
            f"Примеры: <code>10</code> (мин), <code>30m</code>, <code>2h</code>, <code>1d</code>",
            parse_mode="HTML")
        await call.answer(); return
    mins  = int(time_val)
    label = f"{mins} мин." if mins < 60 else (f"{mins//60} ч." if mins < 1440 else f"{mins//1440} дн.")
    await bot.restrict_chat_member(chat_id, target_id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    msg = random.choice(MUTE_MESSAGES).format(name=f"<b>{target_name}</b>", time=label)
    await call.message.edit_text(msg, parse_mode="HTML")
    await call.answer(f"Замутен на {label}!")

@dp.callback_query(F.data.startswith("warn:"))
async def warn_callback(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts     = call.data.split(":", 2)
    target_id = int(parts[1])
    reason    = parts[2]
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
    except:
        target_name = f"ID {target_id}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"warn_custom","target_id":target_id,
            "target_name":target_name,"chat_id":chat_id}
        await call.message.edit_text(
            f"✏️ Напиши причину варна для <b>{target_name}</b>:", parse_mode="HTML")
        await call.answer(); return
    warnings[chat_id][target_id] += 1
    count = warnings[chat_id][target_id]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(chat_id, target_id)
        warnings[chat_id][target_id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{target_name}</b>", max=MAX_WARNINGS)
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
    await call.message.edit_text(msg, parse_mode="HTML")
    await call.answer("Варн выдан!")

@dp.callback_query(F.data.startswith("ban:"))
async def ban_callback(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts     = call.data.split(":", 2)
    target_id = int(parts[1])
    reason    = parts[2]
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
    except:
        target_name = f"ID {target_id}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"ban_custom","target_id":target_id,
            "target_name":target_name,"chat_id":chat_id}
        await call.message.edit_text(
            f"✏️ Напиши причину бана для <b>{target_name}</b>:", parse_mode="HTML")
        await call.answer(); return
    await bot.ban_chat_member(chat_id, target_id)
    msg = random.choice(BAN_MESSAGES).format(name=f"<b>{target_name}</b>", reason=reason)
    await call.message.edit_text(msg, parse_mode="HTML")
    await call.answer("Забанен!")


# ─────────────────────────────────────────────────────
#  ТЕКСТОВЫЕ КОМАНДЫ МОДЕРАЦИИ
# ─────────────────────────────────────────────────────

@dp.message(Command("ban"))
async def ban_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    reason = command.args or "Нарушение правил"
    target = message.reply_to_message.from_user
    await bot.ban_chat_member(message.chat.id, target.id)
    msg = random.choice(BAN_MESSAGES).format(name=target.mention_html(), reason=reason)
    await message.reply(msg, parse_mode="HTML")

@dp.message(Command("unban"))
async def unban_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await message.reply(f"♻️ {target.mention_html()} разбанен.", parse_mode="HTML")

@dp.message(Command("mute"))
async def mute_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    mins, label = (parse_duration(command.args) if command.args else (60, "1 ч."))
    if not mins: mins, label = 60, "1 ч."
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    msg = random.choice(MUTE_MESSAGES).format(name=target.mention_html(), time=label)
    await message.reply(msg, parse_mode="HTML")

@dp.message(Command("unmute"))
async def unmute_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    await message.reply(f"🔊 {target.mention_html()} размучен.", parse_mode="HTML")

@dp.message(Command("warn"))
async def warn_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target  = message.reply_to_message.from_user
    reason  = command.args or "Нарушение правил"
    chat_id = message.chat.id
    warnings[chat_id][target.id] += 1
    count = warnings[chat_id][target.id]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(chat_id, target.id)
        warnings[chat_id][target.id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=target.mention_html(), max=MAX_WARNINGS)
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
    await message.reply(msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def unwarn_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target  = message.reply_to_message.from_user
    chat_id = message.chat.id
    if warnings[chat_id][target.id] > 0: warnings[chat_id][target.id] -= 1
    count = warnings[chat_id][target.id]
    await message.reply(
        f"✅ С {target.mention_html()} снят варн. Осталось: <b>{count}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

@dp.message(Command("del"))
async def del_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    await message.reply_to_message.delete()
    await message.delete()

@dp.message(Command("clear"))
async def clear_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError:
        await message.reply("❗ /clear 20"); return
    deleted = 0
    for i in range(message.message_id, message.message_id - count - 1, -1):
        try: await bot.delete_message(message.chat.id, i); deleted += 1
        except: pass
    sent = await message.answer(f"🧹 Удалено: <b>{deleted}</b> сообщений.", parse_mode="HTML")
    await asyncio.sleep(3)
    try: await sent.delete()
    except: pass

@dp.message(Command("announce"))
async def announce_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        await message.reply("❗ /announce [текст]"); return
    await message.delete()
    await message.answer(
        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}",
        parse_mode="HTML")

@dp.message(Command("pin"))
async def pin_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    await bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Закреплено!")

@dp.message(Command("unpin"))
async def unpin_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Откреплено!")

@dp.message(Command("lock"))
async def lock_cmd(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await message.reply("🔒 Чат <b>заблокирован</b>.", parse_mode="HTML")

@dp.message(Command("unlock"))
async def unlock_cmd(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id,
        ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True,
            can_add_web_page_previews=True, can_invite_users=True))
    await message.reply("🔓 Чат <b>разблокирован</b>.", parse_mode="HTML")

@dp.message(Command("slowmode"))
async def slowmode_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: delay = int(command.args) if command.args else 10
    except ValueError:
        await message.reply("❗ /slowmode 30"); return
    await bot.set_chat_slow_mode_delay(message.chat.id, delay)
    if delay == 0: await message.reply("🐇 Медленный режим выключен.")
    else: await message.reply(f"🐢 Медленный режим: <b>{delay} сек.</b>", parse_mode="HTML")

@dp.message(Command("promote"))
async def promote_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    title  = command.args or "Участник"
    target = message.reply_to_message.from_user
    await bot.set_chat_administrator_custom_title(message.chat.id, target.id, title)
    await message.reply(f"🏅 {target.mention_html()} получил тег: <b>{title}</b>", parse_mode="HTML")

@dp.message(Command("poll"))
async def poll_cmd(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args or "|" not in command.args:
        await message.reply("❗ /poll Вопрос|Вар1|Вар2"); return
    parts = [p.strip() for p in command.args.split("|")]
    if len(parts) < 3:
        await message.reply("❗ Нужно минимум 2 варианта."); return
    await message.delete()
    await bot.send_poll(message.chat.id, question=parts[0], options=parts[1:], is_anonymous=False)

@dp.message(Command("antimat"))
async def antimat_cmd(message: Message, command: CommandObject):
    global ANTI_MAT_ENABLED
    if not await require_admin(message): return
    if not command.args:
        s = "вкл ✅" if ANTI_MAT_ENABLED else "выкл ❌"
        await message.reply(f"🧼 Антимат: <b>{s}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on":
        ANTI_MAT_ENABLED = True
        await message.reply("🧼 Антимат <b>включён</b>!", parse_mode="HTML")
    elif a == "off":
        ANTI_MAT_ENABLED = False
        await message.reply("🔞 Антимат <b>выключен</b>.", parse_mode="HTML")
    else:
        await message.reply("❗ /antimat on или /antimat off")

@dp.message(Command("rban"))
async def rban_cmd(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target = message.reply_to_message.from_user
    reason = random.choice(RANDOM_BAN_REASONS)
    await message.reply(
        f"🎲 {target.mention_html()} получил <b>шуточный бан</b>!\n"
        f"📝 Причина: {reason} 😄\n<i>(реального бана нет, успокойся)</i>",
        parse_mode="HTML")

@dp.message(Command("note"))
async def note_cmd(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("📝 /note set/get/del/list [имя] [текст]"); return
    parts  = command.args.split(maxsplit=2)
    action = parts[0].lower()
    cid    = message.chat.id
    if action == "set":
        if not await require_admin(message): return
        if len(parts) < 3: await message.reply("❗ /note set [имя] [текст]"); return
        notes[cid][parts[1]] = parts[2]
        await message.reply(f"✅ Заметка <b>{parts[1]}</b> сохранена!", parse_mode="HTML")
    elif action == "get":
        if len(parts) < 2: await message.reply("❗ /note get [имя]"); return
        t = notes[cid].get(parts[1])
        if t: await message.reply(f"📝 <b>{parts[1]}:</b>\n{t}", parse_mode="HTML")
        else: await message.reply("❌ Заметка не найдена.")
    elif action == "del":
        if not await require_admin(message): return
        if len(parts) > 1 and parts[1] in notes[cid]:
            del notes[cid][parts[1]]
            await message.reply(f"🗑 Заметка <b>{parts[1]}</b> удалена.", parse_mode="HTML")
        else: await message.reply("❌ Не найдена.")
    elif action == "list":
        keys = list(notes[cid].keys())
        if keys: await message.reply("📋 <b>Заметки:</b>\n" + "\n".join(f"• {k}" for k in keys), parse_mode="HTML")
        else: await message.reply("📭 Заметок нет.")


# ─────────────────────────────────────────────────────
#  ИГРЫ
# ─────────────────────────────────────────────────────

@dp.message(Command("warnings"))
async def warnings_cmd(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    target = message.reply_to_message.from_user
    count  = warnings[message.chat.id].get(target.id, 0)
    await message.reply(
        f"📊 {target.mention_html()} — варнов: <b>{count}/{MAX_WARNINGS}</b>", parse_mode="HTML")

@dp.message(Command("info"))
async def info_cmd(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    user   = message.reply_to_message.from_user
    member = await bot.get_chat_member(message.chat.id, user.id)
    warns  = warnings[message.chat.id].get(user.id, 0)
    rep    = reputation[message.chat.id].get(user.id, 0)
    msgs   = chat_stats[message.chat.id].get(user.id, 0)
    status_map = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
        "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
    afk = f"\n😴 AFK: {afk_users[user.id]}" if user.id in afk_users else ""
    await message.reply(
        f"👤 <b>Инфо:</b>\n🏷 {user.mention_html()}{afk}\n"
        f"🔗 {'@'+user.username if user.username else 'нет'}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"📌 {status_map.get(member.status, member.status)}\n"
        f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
        f"⭐ Репутация: <b>{rep:+d}</b>\n"
        f"💬 Сообщений: <b>{msgs}</b>",
        parse_mode="HTML")

@dp.message(Command("afk"))
async def afk_cmd(message: Message, command: CommandObject):
    reason = command.args or "без причины"
    afk_users[message.from_user.id] = reason
    await message.reply(
        f"😴 {message.from_user.mention_html()} ушёл в AFK: {reason}", parse_mode="HTML")

@dp.message(Command("quote"))
async def quote_cmd(message: Message):
    await message.reply(f"💬 {random.choice(QUOTES)}")

@dp.message(Command("roll"))
async def roll_cmd(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 1000))
    except: sides = 6
    result = random.randint(1, sides)
    await message.reply(f"🎲 Бросаю d{sides}... выпало: <b>{result}</b>!", parse_mode="HTML")

@dp.message(Command("flip"))
async def flip_cmd(message: Message):
    await message.reply(random.choice(["🪙 Орёл!", "🪙 Решка!"]))

@dp.message(Command("8ball"))
async def ball_cmd(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("❓ /8ball [вопрос]"); return
    await message.reply(
        f"🎱 <b>Вопрос:</b> {command.args}\n\n<b>Ответ:</b> {random.choice(BALL_ANSWERS)}",
        parse_mode="HTML")

@dp.message(Command("rate"))
async def rate_cmd(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("❗ /rate [что]"); return
    score = random.randint(0, 10)
    bar   = "⭐" * score + "☆" * (10 - score)
    await message.reply(
        f"📊 <b>{command.args}</b>\n{bar}\nОценка: <b>{score}/10</b>", parse_mode="HTML")

@dp.message(Command("iq"))
async def iq_cmd(message: Message):
    user = (message.reply_to_message.from_user if message.reply_to_message else message.from_user)
    iq = random.randint(1, 200)
    if iq < 70:    c = "🥔 Картошка умнее, нахуй."
    elif iq < 100: c = "🐒 Обезьяна справилась бы лучше, бля."
    elif iq < 130: c = "😐 Сойдёт."
    elif iq < 160: c = "🧠 Умный человек!"
    else:          c = "🤖 Ты Эйнштейн что ли, пиздец?!"
    await message.reply(f"🧠 IQ {user.mention_html()}: <b>{iq}</b>\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def gay_cmd(message: Message):
    user = (message.reply_to_message.from_user if message.reply_to_message else message.from_user)
    p   = random.randint(0, 100)
    bar = "🌈" * (p // 10) + "⬜" * (10 - p // 10)
    await message.reply(
        f"🏳️‍🌈 {user.mention_html()}\n{bar}\n<b>{p}%</b> — это шутка, не ссы 😄",
        parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────────────────

async def main():
    await start_web()
    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN не задан!")
    print("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())





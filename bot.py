import asyncio
import logging
import random
import os
import re
import aiohttp
from datetime import timedelta, datetime
from collections import defaultdict
from time import time

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, ChatPermissions, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiohttp import web

BOT_TOKEN       = os.getenv("BOT_TOKEN")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "")
LOG_CHANNEL_ID  = -5293068734

MAX_WARNINGS     = 3
FLOOD_LIMIT      = 5
FLOOD_TIME       = 5
ANTI_MAT_ENABLED = True
MAT_MUTE_MINUTES = 5
AUTO_KICK_BOTS   = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", encoding="utf-8")]
)
logger = logging.getLogger("BOT")

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

warnings      = defaultdict(lambda: defaultdict(int))
flood_tracker = defaultdict(lambda: defaultdict(list))
notes         = defaultdict(dict)
afk_users     = {}
pending       = {}
chat_stats    = defaultdict(lambda: defaultdict(int))
reputation    = defaultdict(lambda: defaultdict(int))
rep_cooldown  = {}

_MAT_PATTERNS = [
    r"бля(дь)?", r"сука", r"пизд[аеёуыьюя]", r"пиздец",
    r"ху[йяею]", r"хуйн[яы]", r"нахуй", r"похуй",
    r"еб[аиоу]", r"[её]баный", r"еблан", r"заебал",
    r"мудак", r"мудила", r"долбо[её]б",
    r"у[её]бок", r"шлюх[аи]", r"курва",
    r"пиздобол", r"хуесос", r"залуп[аы]",
]
_MAT_RE = re.compile(
    "|".join(r"(?<![а-яёa-z0-9])" + p + r"(?![а-яёa-z0-9])" for p in _MAT_PATTERNS),
    re.IGNORECASE
)

def contains_mat(text: str) -> bool:
    return bool(_MAT_RE.search(text.lower().replace("ё", "е")))

RULES_TEXT = (
    "📜 <b>ПРАВИЛА ЧАТА</b>\n\n"
    "🔞 <b>Контент 18+:</b>\n• Видео, стикеры, гифки 18+\n• Ссылки на порнографические сайты\n"
    "⚠️ Наказание: <b>Бан</b>\n\n"
    "📢 <b>Реклама:</b>\n• Реклама своих соцсетей без разрешения\n"
    "⚠️ Наказание: <b>Варн → Мут 30 мин → Бан 24ч</b>\n\n"
    "🚫 <b>Общие правила:</b>\n• Уважайте друг друга\n• Без спама и флуда\n"
    "• Без оскорблений и мата\n• Только по теме чата\n\nНарушение → предупреждение → бан 🔨"
)

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
    "📵 {name} помолчит {time}. Давно пора, бля.",
]
BAN_MESSAGES = [
    "🔨 {name} — пиздец, улетел в бан! Причина: {reason}",
    "💥 {name} забанен нахуй! Причина: {reason}",
    "🚪 {name} выпнут из чата! Причина: {reason}",
]
WARN_MESSAGES = [
    "⚠️ {name} получил варн {count}/{max}! Причина: {reason}. Ещё раз — и пиздец.",
    "🚨 {name}, это предупреждение {count}/{max}! Причина: {reason}. Следи за собой, нахуй.",
    "😡 {name} — варн {count}/{max}! Причина: {reason}. Ещё немного и улетишь.",
]
AUTOBAN_MESSAGES = [
    "🔨 {name} набрал {max} варнов и получил автобан! Пока-пока, нахуй.",
    "💀 {name} — пиздец! {max} варнов = бан. Скатертью дорога.",
]
RANDOM_BAN_REASONS = [
    "слишком умный, бля",
    "подозрение в адекватности, нахуй",
    "нарушение закона бутерброда, пиздец",
    "превышение лимита здравого смысла",
    "слово «кринж» больше 3 раз, нахуй",
    "дышал слишком громко в тексте, бля",
    "смотрел на бота косо, ёбаный",
    "порвал шаблон реальности, нахуй",
]
QUOTES = [
    "«Не важно, как медленно ты идёшь, главное — не останавливаться.» — Конфуций",
    "«Жизнь — это то, что случается, пока строишь другие планы.» — Леннон",
    "«Будь собой. Все остальные роли заняты.» — Уайльд",
    "«Единственный способ делать великое — любить то, что делаешь.» — Джобс",
    "«Успех — идти от неудачи к неудаче без потери энтузиазма.» — Черчилль",
    "«Умный найдёт выход из любой ситуации. Мудрый в неё не попадёт.»",
]
BALL_ANSWERS = [
    "🟢 Да, нахуй, однозначно!", "🟢 Без сомнений, бля!", "🟢 Скорее всего да.",
    "🟡 Хуй его знает, спроси позже.", "🟡 Трудно сказать, бля.", "🟡 Да кто ж знает, нахуй.",
    "🔴 Нет, нахуй.", "🔴 Однозначно нет, бля!", "🔴 Даже не думай, пиздец.",
    "🟣 Звёзды молчат... попробуй ещё раз 🔮",
    "🟠 Мой хрустальный шар в отпуске, нахуй 🏖",
]
TRIGGER_RESPONSES = {
    "привет":   ["Привет, бля! 👋", "Здарова, нахуй! 😎", "О, живой человек припёрся! 👀"],
    "помогите": ["Чем помочь, нахуй? Напиши подробнее! 🤝"],
    "скучно":   ["Поиграй в /8ball 🎱, нахуй", "Брось кубик /roll 🎲", "Попробуй /slot 🎰, бля"],
    "спасибо":  ["Пожалуйста, бля! 😊", "Всегда рад, нахуй! 🤗", "Не за что, ёпта! 💪"],
    "утро":     ["Доброе утро, бля! ☀️", "Утречко, нахуй! ☕"],
    "ночь":     ["Спокойной ночи, нахуй! 🌙", "Сладких снов, бля! 💤"],
    "бот":      ["Я здесь, нахуй! 🤖", "Чего изволите, бля? 🎩", "Слушаю и повинуюсь, нахуй! 🫡"],
    "хорошо":   ["Вот и отлично, бля! 😊", "Так держать, нахуй! 💪"],
    "плохо":    ["Всё будет хорошо, нахуй! 🌈", "/quote поднимет настроение, бля 💬"],
    "го":       ["Куда, нахуй? 😂", "Поехали, бля! 🚀"],
}
TRUTH_QUESTIONS = [
    "Ты когда-нибудь врал другу, нахуй?", "Какой твой самый большой страх, бля?",
    "Твой самый неловкий момент в жизни, пиздец?", "Что ты никогда не расскажешь родителям, нахуй?",
    "Последняя ложь которую ты сказал, бля?", "На кого из чата ты хотел бы быть похожим, нахуй?",
]
DARE_CHALLENGES = [
    "Напиши комплимент случайному участнику, нахуй!",
    "Признайся в чём-нибудь стыдном прямо сейчас, бля!",
    "Напиши стих про себя — прямо сейчас, нахуй!",
    "Придумай и напиши анекдот прямо сейчас, бля!",
    "Напиши самое странное слово которое знаешь, нахуй!",
]
WOULD_YOU_RATHER = [
    "Быть богатым но одиноким или бедным но счастливым, нахуй?",
    "Уметь летать или быть невидимым, бля?",
    "Знать будущее или изменить прошлое, нахуй?",
    "Жить 200 лет в бедности или 50 лет в богатстве, бля?",
    "Читать мысли или телепортироваться, нахуй?",
]
HOROSCOPES = {
    "♈ Овен": "Сегодня звёзды говорят — делай что хочешь, нахуй, но с умом.",
    "♉ Телец": "День для отдыха, бля. Полежи, поешь, снова полежи.",
    "♊ Близнецы": "Раздвоение личности сегодня — твоя суперсила, нахуй.",
    "♋ Рак": "Спрячься в домик, бля. Там лучше. Там печеньки.",
    "♌ Лев": "Ты красивый и все это знают, нахуй. Используй по полной.",
    "♍ Дева": "Разложи всё по полочкам, нахуй. Буквально. Все полочки.",
    "♎ Весы": "Не можешь выбрать что поесть, бля? Это карма.",
    "♏ Скорпион": "Таинственность — твоё оружие, нахуй. Молчи и улыбайся.",
    "♐ Стрелец": "Стреляй в мечты, бля! Может и попадёшь.",
    "♑ Козерог": "Работай, нахуй. Работай ещё. Потом отдохнёшь на пенсии.",
    "♒ Водолей": "Ты уникальный, нахуй. Как и все остальные.",
    "♓ Рыбы": "Плыви по течению, бля. Или против. Главное — плыви.",
}
COMPLIMENTS = [
    "Ты просто огонь, нахуй! 🔥", "С тобой в чате теплее, бля! ☀️",
    "Ты делаешь этот чат лучше, нахуй! 💎", "Без тебя тут было бы скучнее, бля! 🌟",
    "Ты как редкий покемон — очень ценный, нахуй! ⭐", "Интеллект зашкаливает, бля! 🧠",
]
PREDICTIONS = [
    "🔮 Сегодня тебе повезёт, нахуй! Только не трать деньги.",
    "💀 Осторожно с незнакомцами сегодня, бля.",
    "⭐ Звёзды говорят — ты красавчик, нахуй!",
    "🍀 Удача на твоей стороне, бля!",
    "🐉 Тебя ждёт необычная встреча, нахуй. Возможно с котом.",
    "💤 Лучший план на сегодня — поспать, бля. Доверяй процессу.",
]

# ─── Веб-сервер ───────────────────────────────────────

async def health(request):
    return web.Response(text="OK")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()

# ─── Логи в канал ─────────────────────────────────────

LEVEL_ICONS = {
    "INFO":"ℹ️","WARN":"⚠️","BAN":"🔨","MUTE":"🔇","UNMUTE":"🔊",
    "UNWARN":"✅","FLOOD":"🌊","MAT":"🧼","ERROR":"🔴","JOIN":"👋",
    "LEAVE":"🚪","CMD":"📌","LOCK":"🔒",
}

async def send_log(text: str, level: str = "INFO"):
    icon = LEVEL_ICONS.get(level, "📌")
    now  = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    try:
        await bot.send_message(LOG_CHANNEL_ID,
            f"{icon} <b>[{level}]</b> <code>{now}</code>\n{text}", parse_mode="HTML")
    except Exception as e:
        logger.error(f"send_log: {e}")

# ─── Хелперы ──────────────────────────────────────────

async def check_admin(message: Message) -> bool:
    try:
        m = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return m.status in ("administrator", "creator")
    except Exception as e:
        logger.error(f"check_admin: {e}"); return False

async def is_admin_by_id(chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except Exception as e:
        logger.error(f"is_admin_by_id: {e}"); return False

async def require_admin(message: Message) -> bool:
    if not await check_admin(message):
        await message.reply("🚫 <b>Только для администраторов, нахуй!</b>", parse_mode="HTML")
        return False
    return True

def parse_duration(arg: str):
    arg = arg.strip().lower()
    try:
        if arg.endswith("m"):   m = int(arg[:-1]); return m, f"{m} мин."
        elif arg.endswith("h"): h = int(arg[:-1]); return h*60, f"{h} ч."
        elif arg.endswith("d"): d = int(arg[:-1]); return d*60*24, f"{d} дн."
        elif arg.isdigit():     m = int(arg); return m, f"{m} мин."
    except ValueError: pass
    return None, None

async def get_weather(city: str) -> str:
    if not WEATHER_API_KEY:
        return "❌ <b>WEATHER_API_KEY не задан, нахуй.</b>"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": WEATHER_API_KEY, "units": "metric", "lang": "ru"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                d = await r.json()
                if d.get("cod") != 200: return f"❌ Город <b>{city}</b> не найден, нахуй."
                temp  = round(d["main"]["temp"]); feels = round(d["main"]["feels_like"])
                desc  = d["weather"][0]["description"].capitalize()
                humid = d["main"]["humidity"]; wind = round(d["wind"]["speed"])
                dl    = desc.lower()
                emoji = "☀️" if "ясно" in dl else ("🌧" if "дождь" in dl else ("❄️" if "снег" in dl else ("⛈" if "гроз" in dl else "⛅")))
                return (f"{emoji} <b>Погода в {d['name']}, бля</b>\n\n"
                        f"🌡 Температура: <b>{temp}°C</b> (ощущается {feels}°C)\n"
                        f"📋 {desc}\n💧 Влажность: <b>{humid}%</b>\n💨 Ветер: <b>{wind} м/с</b>")
    except asyncio.TimeoutError: return "❌ Сервер погоды не отвечает, нахуй."
    except Exception as e: logger.error(f"get_weather: {e}"); return f"❌ Ошибка, бля: {e}"

# ─── Клавиатуры ───────────────────────────────────────

def kb_back(tid: int) -> list:
    return [InlineKeyboardButton(text="◀️ Назад в панель", callback_data=f"panel:back:{tid}")]

def kb_main(tid: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Выбрать участника", callback_data=f"panel:select:{tid}"),
         InlineKeyboardButton(text="💬 Сообщения",         callback_data=f"panel:messages:{tid}")],
        [InlineKeyboardButton(text="👥 Участники",         callback_data=f"panel:members:{tid}"),
         InlineKeyboardButton(text="🔧 Чат",               callback_data=f"panel:chat:{tid}")],
        [InlineKeyboardButton(text="🎮 Игры",              callback_data=f"panel:games:{tid}"),
         InlineKeyboardButton(text="📊 Статистика",        callback_data=f"panel:botstats2:{tid}")],
        [InlineKeyboardButton(text="❌ Закрыть",           callback_data="panel:close:0")],
    ])

def kb_user_panel(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔇 Мут",        callback_data=f"panel:mute:{tid}"),
         InlineKeyboardButton(text="🔊 Размут",     callback_data=f"panel:unmute:{tid}")],
        [InlineKeyboardButton(text="⚠️ Варн",       callback_data=f"panel:warn:{tid}"),
         InlineKeyboardButton(text="✅ Снять варн", callback_data=f"panel:unwarn:{tid}")],
        [InlineKeyboardButton(text="🔨 Бан",        callback_data=f"panel:ban:{tid}"),
         InlineKeyboardButton(text="♻️ Разбан",     callback_data=f"panel:unban:{tid}")],
        [InlineKeyboardButton(text="📊 Информация", callback_data=f"panel:info:{tid}"),
         InlineKeyboardButton(text="🗑 Удалить сообщ", callback_data=f"panel:del:{tid}")],
        [InlineKeyboardButton(text="🎭 Приколы",    callback_data=f"panel:fun:{tid}"),
         InlineKeyboardButton(text="◀️ Назад",      callback_data=f"panel:mainmenu:0")],
    ])

def kb_mute(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин",  callback_data=f"mute:{tid}:5"),
         InlineKeyboardButton(text="15 мин", callback_data=f"mute:{tid}:15"),
         InlineKeyboardButton(text="30 мин", callback_data=f"mute:{tid}:30")],
        [InlineKeyboardButton(text="1 час",   callback_data=f"mute:{tid}:60"),
         InlineKeyboardButton(text="3 часа",  callback_data=f"mute:{tid}:180"),
         InlineKeyboardButton(text="12 часов",callback_data=f"mute:{tid}:720")],
        [InlineKeyboardButton(text="1 день",  callback_data=f"mute:{tid}:1440"),
         InlineKeyboardButton(text="7 дней",  callback_data=f"mute:{tid}:10080"),
         InlineKeyboardButton(text="✏️ Своё", callback_data=f"mute:{tid}:custom")],
        kb_back(tid),
    ])

def kb_warn(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤬 Мат",         callback_data=f"warn:{tid}:Мат в чате"),
         InlineKeyboardButton(text="💬 Спам",         callback_data=f"warn:{tid}:Спам")],
        [InlineKeyboardButton(text="😡 Оскорбление", callback_data=f"warn:{tid}:Оскорбление"),
         InlineKeyboardButton(text="🚫 Флуд",         callback_data=f"warn:{tid}:Флуд")],
        [InlineKeyboardButton(text="📵 Реклама",     callback_data=f"warn:{tid}:Реклама"),
         InlineKeyboardButton(text="🔞 Контент 18+", callback_data=f"warn:{tid}:Контент 18+")],
        [InlineKeyboardButton(text="✏️ Своя причина",callback_data=f"warn:{tid}:custom")],
        kb_back(tid),
    ])

def kb_ban(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤬 Грубые нарушения", callback_data=f"ban:{tid}:Грубые нарушения правил"),
         InlineKeyboardButton(text="💬 Спам/реклама",     callback_data=f"ban:{tid}:Спам и реклама")],
        [InlineKeyboardButton(text="🔞 Контент 18+",  callback_data=f"ban:{tid}:Контент 18+"),
         InlineKeyboardButton(text="🤖 Бот/накрутка", callback_data=f"ban:{tid}:Бот или накрутка")],
        [InlineKeyboardButton(text="⏰ Бан на 24ч",   callback_data=f"ban:{tid}:tempban24"),
         InlineKeyboardButton(text="✏️ Своя причина", callback_data=f"ban:{tid}:custom")],
        kb_back(tid),
    ])

def kb_fun(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Шуточный бан",     callback_data=f"fun:rban:{tid}"),
         InlineKeyboardButton(text="🧠 Проверить IQ",     callback_data=f"fun:iq:{tid}")],
        [InlineKeyboardButton(text="🌈 % гейства",        callback_data=f"fun:gay:{tid}"),
         InlineKeyboardButton(text="💐 Комплимент",        callback_data=f"fun:compliment:{tid}")],
        [InlineKeyboardButton(text="🔮 Предсказание",     callback_data=f"fun:predict:{tid}"),
         InlineKeyboardButton(text="♈ Гороскоп",         callback_data=f"fun:horoscope:{tid}")],
        [InlineKeyboardButton(text="⭐ Оценить",          callback_data=f"fun:rate:{tid}"),
         InlineKeyboardButton(text="🤔 Вопрос правды",    callback_data=f"fun:truth:{tid}")],
        [InlineKeyboardButton(text="😈 Задание",          callback_data=f"fun:dare:{tid}"),
         InlineKeyboardButton(text="🤯 Выбор без выбора", callback_data=f"fun:wyr:{tid}")],
        kb_back(tid),
    ])

def kb_messages(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Закрепить",          callback_data=f"msg:pin:{tid}"),
         InlineKeyboardButton(text="📌 Открепить",          callback_data=f"msg:unpin:{tid}")],
        [InlineKeyboardButton(text="🗑 Удалить сообщение",  callback_data=f"msg:del:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 10",        callback_data=f"msg:clear10:{tid}")],
        [InlineKeyboardButton(text="🧹 Очистить 20",        callback_data=f"msg:clear20:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 50",        callback_data=f"msg:clear50:{tid}")],
        [InlineKeyboardButton(text="📢 Объявление",          callback_data=f"msg:announce:{tid}"),
         InlineKeyboardButton(text="📊 Голосование",         callback_data=f"msg:poll:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_members(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮 Список админов",      callback_data=f"members:adminlist:{tid}"),
         InlineKeyboardButton(text="📊 Топ активности",      callback_data=f"members:top:{tid}")],
        [InlineKeyboardButton(text="📵 Мут 24ч за рекламу",  callback_data=f"members:warn24:{tid}"),
         InlineKeyboardButton(text="⚠️ Варны участника",     callback_data=f"members:warninfo:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_chat(tid: int) -> InlineKeyboardMarkup:
    ms = "✅ вкл" if ANTI_MAT_ENABLED else "❌ выкл"
    ks = "✅ вкл" if AUTO_KICK_BOTS   else "❌ выкл"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Заблокировать чат",  callback_data=f"chat:lock:{tid}"),
         InlineKeyboardButton(text="🔓 Разблокировать",     callback_data=f"chat:unlock:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 10с",       callback_data=f"chat:slow10:{tid}"),
         InlineKeyboardButton(text="🐢 Slowmode 30с",       callback_data=f"chat:slow30:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 60с",       callback_data=f"chat:slow60:{tid}"),
         InlineKeyboardButton(text="🐇 Выкл slowmode",      callback_data=f"chat:slow0:{tid}")],
        [InlineKeyboardButton(text=f"🧼 Антимат: {ms}",    callback_data=f"chat:antimat:{tid}"),
         InlineKeyboardButton(text=f"🤖 Автокик: {ks}",    callback_data=f"chat:autokick:{tid}")],
        [InlineKeyboardButton(text="📜 Правила",             callback_data=f"chat:rules:{tid}"),
         InlineKeyboardButton(text="📊 Статистика бота",     callback_data=f"chat:botstats:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_games(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик d6",         callback_data=f"game:roll:{tid}"),
         InlineKeyboardButton(text="🪙 Монетка",          callback_data=f"game:flip:{tid}")],
        [InlineKeyboardButton(text="🎰 Слот-машина",      callback_data=f"game:slot:{tid}"),
         InlineKeyboardButton(text="🎱 Шар предсказаний", callback_data=f"game:8ball:{tid}")],
        [InlineKeyboardButton(text="✂️ КНБ — Камень",     callback_data=f"game:rps_k:{tid}"),
         InlineKeyboardButton(text="✂️ КНБ — Ножницы",    callback_data=f"game:rps_n:{tid}")],
        [InlineKeyboardButton(text="✂️ КНБ — Бумага",     callback_data=f"game:rps_b:{tid}"),
         InlineKeyboardButton(text="💬 Цитата дня",       callback_data=f"game:quote:{tid}")],
        [InlineKeyboardButton(text="🌤 Погода — Москва",  callback_data=f"game:weather_Москва:{tid}"),
         InlineKeyboardButton(text="🌤 Свой город",        callback_data=f"game:weather_custom:{tid}")],
        [InlineKeyboardButton(text="⏱ Отсчёт 5 сек",     callback_data=f"game:countdown5:{tid}"),
         InlineKeyboardButton(text="⏱ Отсчёт 10 сек",    callback_data=f"game:countdown10:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"panel:mainmenu:0")],
    ])

# ─── Middleware ────────────────────────────────────────

class StatsMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group","supergroup"):
            chat_stats[event.chat.id][event.from_user.id] += 1
        return await handler(event, data)

class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if event.chat.type not in ("group","supergroup"): return await handler(event, data)
        if event.text and event.text.startswith("/"): return await handler(event, data)
        uid, cid = event.from_user.id, event.chat.id
        try:
            m = await bot.get_chat_member(cid, uid)
            if m.status in ("administrator","creator"): return await handler(event, data)
        except Exception as e: logger.warning(f"AntiFlood: {e}")
        now = time()
        flood_tracker[cid][uid] = [t for t in flood_tracker[cid][uid] if now - t < FLOOD_TIME]
        flood_tracker[cid][uid].append(now)
        if len(flood_tracker[cid][uid]) >= FLOOD_LIMIT:
            try:
                await event.delete()
                await bot.restrict_chat_member(cid, uid,
                    permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=5))
                sent = await bot.send_message(cid,
                    f"🌊 {event.from_user.mention_html()}, хуле флудишь?! Мут на 5 минут, нахуй.", parse_mode="HTML")
                flood_tracker[cid][uid].clear()
                await send_log(f"🌊 <b>Флуд-мут</b>\n👤 {event.from_user.mention_html()} (<code>{uid}</code>)\n💬 {event.chat.title}", "FLOOD")
                await asyncio.sleep(8)
                try: await sent.delete()
                except: pass
            except Exception as e: logger.error(f"AntiFlood action: {e}")
            return
        return await handler(event, data)

class AntiMatMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if not ANTI_MAT_ENABLED: return await handler(event, data)
        if event.chat.type not in ("group","supergroup"): return await handler(event, data)
        if not event.text or event.text.startswith("/"): return await handler(event, data)
        uid, cid = event.from_user.id, event.chat.id
        try:
            m = await bot.get_chat_member(cid, uid)
            if m.status in ("administrator","creator"): return await handler(event, data)
        except Exception as e: logger.warning(f"AntiMat: {e}")
        if contains_mat(event.text):
            try:
                await event.delete()
                await bot.restrict_chat_member(cid, uid,
                    permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=MAT_MUTE_MINUTES))
                resp = random.choice(MAT_RESPONSES).format(name=event.from_user.mention_html(), minutes=MAT_MUTE_MINUTES)
                sent = await bot.send_message(cid, resp, parse_mode="HTML")
                await send_log(f"🧼 <b>Антимат</b>\n👤 {event.from_user.mention_html()} (<code>{uid}</code>)\n💬 {event.chat.title}\n📝 <code>{event.text[:120]}</code>", "MAT")
                await asyncio.sleep(10)
                try: await sent.delete()
                except: pass
            except Exception as e: logger.error(f"AntiMat action: {e}")
            return
        return await handler(event, data)

class AfkMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if event.from_user and event.from_user.id in afk_users:
            if not (event.text and event.text.startswith("/afk")):
                reason = afk_users.pop(event.from_user.id)
                try: await event.answer(f"👋 {event.from_user.mention_html()} вернулся из AFK, нахуй! (был: {reason})", parse_mode="HTML")
                except Exception as e: logger.warning(f"AFK return: {e}")
        if event.reply_to_message and event.reply_to_message.from_user:
            tid = event.reply_to_message.from_user.id
            if tid in afk_users:
                try: await event.answer(f"😴 {event.reply_to_message.from_user.mention_html()} сейчас AFK, бля: {afk_users[tid]}", parse_mode="HTML")
                except Exception as e: logger.warning(f"AFK notify: {e}")
        return await handler(event, data)

class PendingInputMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        uid = event.from_user.id if event.from_user else None
        if uid and uid in pending and not (event.text and event.text.startswith("/")):
            p = pending.pop(uid)
            action, target_id, target_name, chat_id = p["action"], p["target_id"], p["target_name"], p["chat_id"]
            text = event.text or ""
            try:
                if action == "mute_custom":
                    mins, label = parse_duration(text)
                    if mins:
                        await bot.restrict_chat_member(chat_id, target_id,
                            permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
                        await event.answer(random.choice(MUTE_MESSAGES).format(name=f"<b>{target_name}</b>", time=label), parse_mode="HTML")
                        await send_log(f"🔇 <b>Мут</b>\n👤 {target_name} (<code>{target_id}</code>)\n⏱ {label}", "MUTE")
                    else: await event.reply("❗ Примеры: 10, 30m, 2h, 1d")
                elif action == "warn_custom":
                    reason = text.strip() or "Нарушение правил"
                    warnings[chat_id][target_id] += 1; count = warnings[chat_id][target_id]
                    if count >= MAX_WARNINGS:
                        await bot.ban_chat_member(chat_id, target_id); warnings[chat_id][target_id] = 0
                        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{target_name}</b>", max=MAX_WARNINGS)
                        await send_log(f"🔨 <b>Автобан</b>\n👤 {target_name}", "BAN")
                    else:
                        msg = random.choice(WARN_MESSAGES).format(name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
                        await send_log(f"⚠️ <b>Варн {count}/{MAX_WARNINGS}</b>\n👤 {target_name}\n📝 {reason}", "WARN")
                    await event.answer(msg, parse_mode="HTML")
                elif action == "ban_custom":
                    reason = text.strip() or "Нарушение правил"
                    await bot.ban_chat_member(chat_id, target_id)
                    await event.answer(random.choice(BAN_MESSAGES).format(name=f"<b>{target_name}</b>", reason=reason), parse_mode="HTML")
                    await send_log(f"🔨 <b>Бан</b>\n👤 {target_name} (<code>{target_id}</code>)\n📝 {reason}", "BAN")
                elif action == "announce_text":
                    await bot.send_message(chat_id, f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{text}\n\n— <b>Администрация</b>", parse_mode="HTML")
                elif action == "poll_text":
                    parts = [x.strip() for x in text.split("|") if x.strip()]
                    if len(parts) >= 3: await bot.send_poll(chat_id, question=parts[0], options=parts[1:], is_anonymous=False)
                    else: await event.reply("❗ Формат: Вопрос|Вариант1|Вариант2")
                elif action == "promote_title":
                    title = text.strip() or "Участник"
                    await bot.set_chat_administrator_custom_title(chat_id, target_id, title)
                    await event.answer(f"🏅 <b>{target_name}</b> получил тег: <b>{title}</b>", parse_mode="HTML")
                elif action == "weather_city":
                    await event.answer(await get_weather(text.strip()), parse_mode="HTML")
            except Exception as e:
                logger.error(f"PendingInput {action}: {e}"); await event.reply("❗ Ошибка, нахуй.")
            try: await event.delete()
            except: pass
            return
        return await handler(event, data)

dp.message.middleware(PendingInputMiddleware())
dp.message.middleware(StatsMiddleware())
dp.message.middleware(AntiFloodMiddleware())
dp.message.middleware(AntiMatMiddleware())
dp.message.middleware(AfkMiddleware())

# ─── Новые участники ──────────────────────────────────

@dp.message(F.new_chat_members)
async def on_new_member(message: Message):
    for member in message.new_chat_members:
        if member.is_bot and AUTO_KICK_BOTS:
            try:
                await bot.ban_chat_member(message.chat.id, member.id)
                await bot.unban_chat_member(message.chat.id, member.id)
                await message.answer(f"🤖 Бот <b>{member.full_name}</b> автоматически выпнут, нахуй!", parse_mode="HTML")
                await send_log(f"🤖 <b>Автокик бота</b>\n🤖 {member.full_name} (<code>{member.id}</code>)", "LEAVE")
            except Exception as e: logger.error(f"AutoKick: {e}")
            continue
        funny = [
            f"🎉 {member.mention_html()} припёрся в чат! Читай правила, нахуй 👇",
            f"🚀 {member.mention_html()} телепортировался к нам, бля! Правила обязательны 👇",
            f"👀 О! {member.mention_html()} появился! Читай правила, нахуй 👇",
            f"🌟 {member.mention_html()} присоединился, бля! Добро пожаловать 👇",
        ]
        await message.answer(random.choice(funny) + "\n\n" + RULES_TEXT, parse_mode="HTML")
        await send_log(f"👋 <b>Новый участник</b>\n👤 {member.mention_html()} (<code>{member.id}</code>)\n💬 {message.chat.title}", "JOIN")

# ─── Команды ──────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        f"👋 Привет, <b>{message.from_user.first_name}</b>, нахуй!\n\n"
        "Я бот-модератор, бля.\n"
        "📜 /rules — правила\n❓ /help — помощь\n🛠 /panel — панель управления\n\n"
        "<i>Панель открывается без реплая!</i>", parse_mode="HTML")

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    await message.reply(RULES_TEXT, parse_mode="HTML")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_adm = await check_admin(message)
    text = ("❓ <b>Команды для всех, нахуй:</b>\n"
            "/rules /weather /rep /top /afk /info /warnings /quote\n"
            "/roll /flip /8ball /rate /iq /gay /truth /dare /wyr\n"
            "/rps /slot /choose /horoscope /predict /compliment\n\n")
    if is_adm:
        text += ("👮 <b>Для админов, бля:</b>\n"
                 "/panel — <b>ВСЁ управление в одном месте</b> 👈\n\n"
                 "Текстовые дубли: /ban /unban /mute /unmute /warn /unwarn\n"
                 "/del /clear /announce /pin /unpin /lock /unlock\n"
                 "/slowmode /promote /poll /antimat /autokick\n"
                 "/rban /warn24 /adminlist /note /botstats\n")
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("panel"))
async def cmd_panel(message: Message):
    if not await require_admin(message): return
    if message.reply_to_message:
        target = message.reply_to_message.from_user
        warns  = warnings[message.chat.id].get(target.id, 0)
        rep    = reputation[message.chat.id].get(target.id, 0)
        msgs   = chat_stats[message.chat.id].get(target.id, 0)
        afk    = f"\n😴 AFK: {afk_users[target.id]}" if target.id in afk_users else ""
        await message.reply(
            f"🛠 <b>Панель участника</b>\n\n👤 {target.mention_html()}{afk}\n"
            f"🆔 ID: <code>{target.id}</code>\n"
            f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
            f"⭐ Репутация: <b>{rep:+d}</b>\n"
            f"💬 Сообщений: <b>{msgs}</b>\n\nВыбери действие, нахуй:",
            parse_mode="HTML", reply_markup=kb_user_panel(target.id))
    else:
        total_msgs  = sum(chat_stats[message.chat.id].values())
        total_warns = sum(warnings[message.chat.id].values())
        await message.reply(
            f"🛠 <b>Панель управления, бля</b>\n\n"
            f"💬 Чат: <b>{message.chat.title}</b>\n"
            f"📊 Сообщений: <b>{total_msgs}</b>\n"
            f"⚠️ Варнов: <b>{total_warns}</b>\n"
            f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\n"
            f"Выбери раздел, нахуй:",
            parse_mode="HTML", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("panel:"))
async def cb_panel(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    _, action, tid_str = call.data.split(":", 2)
    tid = int(tid_str); cid = call.message.chat.id
    try:
        tm = await bot.get_chat_member(cid, tid) if tid != 0 else None
        tname   = tm.user.full_name if tm else "участник"
        mention = tm.user.mention_html() if tm else ""
    except Exception:
        tname = f"ID {tid}"; mention = f"<code>{tid}</code>"
    try:
        if action == "close":
            await call.message.delete()
        elif action == "mainmenu":
            total_msgs  = sum(chat_stats[cid].values())
            total_warns = sum(warnings[cid].values())
            await call.message.edit_text(
                f"🛠 <b>Панель управления, бля</b>\n\n"
                f"💬 Чат: <b>{call.message.chat.title}</b>\n"
                f"📊 Сообщений: <b>{total_msgs}</b>\n"
                f"⚠️ Варнов: <b>{total_warns}</b>\n"
                f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\n"
                f"Выбери раздел, нахуй:",
                parse_mode="HTML", reply_markup=kb_main())
        elif action == "select":
            await call.message.edit_text(
                "👤 <b>Выбор участника</b>\n\nОткрой панель реплаем:\n<code>/panel</code> → ответь на сообщение участника, нахуй",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        elif action == "back":
            if tid != 0:
                warns = warnings[cid].get(tid, 0)
                rep   = reputation[cid].get(tid, 0)
                msgs  = chat_stats[cid].get(tid, 0)
                afk   = f"\n😴 AFK: {afk_users[tid]}" if tid in afk_users else ""
                await call.message.edit_text(
                    f"🛠 <b>Панель участника</b>\n\n👤 {mention}{afk}\n"
                    f"🆔 ID: <code>{tid}</code>\n"
                    f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
                    f"⭐ Репутация: <b>{rep:+d}</b>\n"
                    f"💬 Сообщений: <b>{msgs}</b>\n\nВыбери действие, нахуй:",
                    parse_mode="HTML", reply_markup=kb_user_panel(tid))
            else:
                await call.message.edit_text("🛠 <b>Панель управления</b>\n\nВыбери раздел, бля:",
                    parse_mode="HTML", reply_markup=kb_main())
        elif action == "mute":
            await call.message.edit_text(f"🔇 <b>Мут для {tname}</b>\n\nВыбери время, нахуй:", parse_mode="HTML", reply_markup=kb_mute(tid))
        elif action == "unmute":
            await bot.restrict_chat_member(cid, tid, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            await call.message.edit_text(f"🔊 <b>{tname}</b> размучен, можешь болтать.", parse_mode="HTML")
            await send_log(f"🔊 <b>Размут</b>\n👤 {tname} (<code>{tid}</code>)", "UNMUTE")
        elif action == "warn":
            await call.message.edit_text(f"⚠️ <b>Варн для {tname}</b>\n\nВыбери причину, нахуй:", parse_mode="HTML", reply_markup=kb_warn(tid))
        elif action == "unwarn":
            if warnings[cid][tid] > 0: warnings[cid][tid] -= 1
            await call.message.edit_text(f"✅ С <b>{tname}</b> снят варн, бля. Осталось: <b>{warnings[cid][tid]}/{MAX_WARNINGS}</b>", parse_mode="HTML")
            await send_log(f"✅ <b>Снят варн</b>\n👤 {tname}\nОсталось: {warnings[cid][tid]}", "UNWARN")
        elif action == "ban":
            await call.message.edit_text(f"🔨 <b>Бан для {tname}</b>\n\nВыбери причину, нахуй:", parse_mode="HTML", reply_markup=kb_ban(tid))
        elif action == "unban":
            await bot.unban_chat_member(cid, tid, only_if_banned=True)
            await call.message.edit_text(f"♻️ <b>{tname}</b> разбанен. Дали второй шанс, нахуй.", parse_mode="HTML")
            await send_log(f"♻️ <b>Разбан</b>\n👤 {tname} (<code>{tid}</code>)", "UNMUTE")
        elif action == "del":
            try: await call.message.reply_to_message.delete()
            except: pass
            await call.message.edit_text("🗑 Сообщение удалено, нахуй.")
            await send_log(f"🗑 <b>Удалено</b>\n💬 <code>{cid}</code>", "CMD")
        elif action == "info":
            tm2 = await bot.get_chat_member(cid, tid); u = tm2.user
            smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор","member":"👤 Участник",
                    "restricted":"🔇 Ограничен","kicked":"🔨 Забанен","left":"🚶 Вышел"}
            afk = f"\n😴 AFK: {afk_users[tid]}" if tid in afk_users else ""
            await call.message.edit_text(
                f"👤 <b>Инфо, бля:</b>\n\n🏷 {u.mention_html()}{afk}\n"
                f"🔗 {'@'+u.username if u.username else 'нет'}\n🆔 <code>{u.id}</code>\n"
                f"📌 {smap.get(tm2.status, tm2.status)}\n"
                f"⚠️ Варнов: <b>{warnings[cid].get(tid,0)}/{MAX_WARNINGS}</b>\n"
                f"⭐ Репутация: <b>{reputation[cid].get(tid,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(tid,0)}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[kb_back(tid)]))
        elif action == "fun":
            await call.message.edit_text(f"🎭 <b>Приколы над {tname}, нахуй</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_fun(tid))
        elif action == "messages":
            await call.message.edit_text("💬 <b>Действия с сообщениями, бля</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_messages(tid))
        elif action == "members":
            await call.message.edit_text("👥 <b>Управление участниками, нахуй</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_members(tid))
        elif action == "chat":
            await call.message.edit_text(
                f"🔧 <b>Управление чатом, бля</b>\n\n🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_chat(tid))
        elif action == "games":
            await call.message.edit_text("🎮 <b>Игры и команды, нахуй</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_games(tid))
        elif action == "botstats2":
            total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
            total_warns = sum(sum(v.values()) for v in warnings.values())
            await call.message.edit_text(
                f"📊 <b>Статистика бота, бля</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
                f"⚠️ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
                f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
                f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
                f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
    except Exception as e:
        logger.error(f"cb_panel {action}: {e}"); await call.answer("❗ Ошибка, нахуй.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    _, tid_str, tval = call.data.split(":")
    tid = int(tid_str); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if tval == "custom":
        pending[call.from_user.id] = {"action":"mute_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Введи время мута для <b>{tname}</b>, нахуй:\nПримеры: <code>10</code>, <code>30m</code>, <code>2h</code>, <code>1d</code>", parse_mode="HTML")
        await call.answer(); return
    mins  = int(tval)
    label = f"{mins} мин." if mins < 60 else (f"{mins//60} ч." if mins < 1440 else f"{mins//1440} дн.")
    await bot.restrict_chat_member(cid, tid, permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    await call.message.edit_text(random.choice(MUTE_MESSAGES).format(name=f"<b>{tname}</b>", time=label), parse_mode="HTML")
    await send_log(f"🔇 <b>Мут</b>\n👤 {tname} (<code>{tid}</code>)\n⏱ {label}", "MUTE")
    await call.answer(f"Замутен на {label}, нахуй!")

@dp.callback_query(F.data.startswith("warn:"))
async def cb_warn(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"warn_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину варна для <b>{tname}</b>, нахуй:", parse_mode="HTML")
        await call.answer(); return
    warnings[cid][tid] += 1; count = warnings[cid][tid]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, tid); warnings[cid][tid] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{tname}</b>", max=MAX_WARNINGS)
        await send_log(f"🔨 <b>Автобан</b>\n👤 {tname} (<code>{tid}</code>)", "BAN")
    else:
        msg = random.choice(WARN_MESSAGES).format(name=f"<b>{tname}</b>", count=count, max=MAX_WARNINGS, reason=reason)
        await send_log(f"⚠️ <b>Варн {count}/{MAX_WARNINGS}</b>\n👤 {tname}\n📝 {reason}", "WARN")
    await call.message.edit_text(msg, parse_mode="HTML")
    await call.answer("Варн выдан, нахуй!")

@dp.callback_query(F.data.startswith("ban:"))
async def cb_ban(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"ban_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину бана для <b>{tname}</b>, нахуй:", parse_mode="HTML")
        await call.answer(); return
    if reason == "tempban24":
        await bot.ban_chat_member(cid, tid, until_date=timedelta(hours=24))
        await call.message.edit_text(f"⏰ <b>{tname}</b> забанен на <b>24 часа</b>, нахуй!", parse_mode="HTML")
        await send_log(f"⏰ <b>Бан 24ч</b>\n👤 {tname} (<code>{tid}</code>)", "BAN")
        await call.answer(); return
    await bot.ban_chat_member(cid, tid)
    await call.message.edit_text(random.choice(BAN_MESSAGES).format(name=f"<b>{tname}</b>", reason=reason), parse_mode="HTML")
    await send_log(f"🔨 <b>Бан</b>\n👤 {tname} (<code>{tid}</code>)\n📝 {reason}", "BAN")
    await call.answer("Забанен, нахуй!")

@dp.callback_query(F.data.startswith("fun:"))
async def cb_fun(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); mention = tm.user.mention_html()
    except: mention = f"<code>{tid}</code>"
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё раз, нахуй", callback_data=call.data)], kb_back(tid)])
    try:
        if action == "rban":
            await call.message.edit_text(
                f"🎲 {mention} получил <b>шуточный бан</b>!\n📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет, нахуй)</i>",
                parse_mode="HTML", reply_markup=back_kb)
        elif action == "iq":
            iq = random.randint(1, 200)
            c  = "🥔 Картошка умнее, нахуй." if iq<70 else ("🐒 Обезьяна лучше, бля." if iq<100 else ("😐 Сойдёт, нахуй." if iq<130 else ("🧠 Умный, нахуй!" if iq<160 else "🤖 Эйнштейн что ли, пиздец?!")))
            await call.message.edit_text(f"🧠 IQ {mention}: <b>{iq}</b>\n{c}", parse_mode="HTML", reply_markup=back_kb)
        elif action == "gay":
            p = random.randint(0, 100)
            await call.message.edit_text(f"🏳️‍🌈 {mention}\n{'🌈'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка, нахуй 😄", parse_mode="HTML", reply_markup=back_kb)
        elif action == "compliment":
            await call.message.edit_text(f"💐 <b>{mention}:</b>\n\n{random.choice(COMPLIMENTS)}", parse_mode="HTML", reply_markup=back_kb)
        elif action == "predict":
            await call.message.edit_text(f"🔮 <b>Предсказание для {mention}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML", reply_markup=back_kb)
        elif action == "horoscope":
            sign, text = random.choice(list(HOROSCOPES.items()))
            await call.message.edit_text(f"{sign} <b>Гороскоп для {mention}:</b>\n\n{text}", parse_mode="HTML", reply_markup=back_kb)
        elif action == "rate":
            score = random.randint(0, 10)
            await call.message.edit_text(f"📊 Оценка {mention}:\n{'⭐'*score+'☆'*(10-score)}\n<b>{score}/10</b>, нахуй", parse_mode="HTML", reply_markup=back_kb)
        elif action == "truth":
            await call.message.edit_text(f"🤔 <b>Вопрос для {mention}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML", reply_markup=back_kb)
        elif action == "dare":
            await call.message.edit_text(f"😈 <b>Задание для {mention}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML", reply_markup=back_kb)
        elif action == "wyr":
            await call.message.edit_text(f"🤯 <b>Выбор для {mention}:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML", reply_markup=back_kb)
    except Exception as e:
        logger.error(f"cb_fun {action}: {e}"); await call.answer("❗ Ошибка, нахуй.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("msg:"))
async def cb_msg(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try:
        if action == "pin":
            try: await bot.pin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📌 Закреплено, нахуй!", show_alert=True)
            except: await call.answer("❗ Открой панель реплаем на нужное сообщение, бля.", show_alert=True)
        elif action == "unpin":
            try: await bot.unpin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📌 Откреплено, нахуй!", show_alert=True)
            except: await call.answer("❗ Нет сообщения, бля.", show_alert=True)
        elif action == "del":
            try: await call.message.reply_to_message.delete(); await call.answer("🗑 Удалено, нахуй!", show_alert=True)
            except: await call.answer("❗ Нет сообщения, бля.", show_alert=True)
        elif action.startswith("clear"):
            n = int(action.replace("clear",""))
            deleted = 0
            for i in range(call.message.message_id, call.message.message_id - n - 1, -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            await call.answer(f"🧹 Удалено {deleted} сообщений, нахуй!", show_alert=True)
        elif action == "announce":
            pending[call.from_user.id] = {"action":"announce_text","target_id":0,"target_name":"","chat_id":cid}
            await call.message.edit_text("📢 Напиши текст объявления, нахуй:\n<i>(следующее твоё сообщение станет объявлением)</i>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
        elif action == "poll":
            pending[call.from_user.id] = {"action":"poll_text","target_id":0,"target_name":"","chat_id":cid}
            await call.message.edit_text("📊 Напиши голосование, нахуй:\n<code>Вопрос|Вариант 1|Вариант 2</code>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
        await call.message.edit_text("💬 <b>Действия с сообщениями, бля</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_messages(tid))
    except Exception as e:
        logger.error(f"cb_msg {action}: {e}"); await call.answer("❗ Ошибка, нахуй.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("members:"))
async def cb_members(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid) if tid != 0 else None; tname = tm.user.full_name if tm else "участник"
    except: tname = f"ID {tid}"
    try:
        if action == "adminlist":
            admins = await bot.get_chat_administrators(cid)
            lines = ["👮 <b>Администраторы чата, нахуй:</b>\n"]
            for adm in admins:
                if adm.user.is_bot: continue
                icon  = "👑" if adm.status == "creator" else "🛡"
                title = f" — <i>{adm.custom_title}</i>" if hasattr(adm,"custom_title") and adm.custom_title else ""
                lines.append(f"{icon} {adm.user.mention_html()}{title}")
            await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        elif action == "top":
            stats = chat_stats[cid]
            if not stats: await call.answer("📊 Статистика пуста, нахуй!", show_alert=True)
            else:
                sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
                medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
                lines    = ["📊 <b>Топ активных, бля:</b>\n"]
                for i, (uid, cnt) in enumerate(sorted_u):
                    try: m = await bot.get_chat_member(cid, uid); uname = m.user.full_name
                    except: uname = f"ID {uid}"
                    lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщ.")
                await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        elif action == "warn24":
            if tid != 0:
                await bot.restrict_chat_member(cid, tid, permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
                await call.message.edit_text(f"📵 <b>{tname}</b> — мут на <b>24 часа</b> за рекламу, нахуй!", parse_mode="HTML")
                await send_log(f"📵 <b>Мут 24ч (реклама)</b>\n👤 {tname} (<code>{tid}</code>)", "MUTE")
            else: await call.answer("❗ Открой панель реплаем на участника, бля.", show_alert=True)
        elif action == "warninfo":
            if tid != 0:
                count = warnings[cid].get(tid, 0)
                await call.message.edit_text(f"⚠️ Варнов у <b>{tname}</b>: <b>{count}/{MAX_WARNINGS}</b>, нахуй",
                    parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            else: await call.answer("❗ Открой панель реплаем на участника, бля.", show_alert=True)
    except Exception as e:
        logger.error(f"cb_members {action}: {e}"); await call.answer("❗ Ошибка, нахуй.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("chat:"))
async def cb_chat(call: CallbackQuery):
    global ANTI_MAT_ENABLED, AUTO_KICK_BOTS
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try:
        if action == "lock":
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
            await call.answer("🔒 Чат заблокирован, нахуй!", show_alert=True)
            await send_log(f"🔒 <b>Чат заблокирован</b>\n💬 <code>{cid}</code>", "LOCK")
        elif action == "unlock":
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=True,
                can_send_media_messages=True, can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True, can_invite_users=True))
            await call.answer("🔓 Чат разблокирован, нахуй!", show_alert=True)
        elif action.startswith("slow"):
            delay = int(action.replace("slow",""))
            await bot.set_chat_slow_mode_delay(cid, delay)
            await call.answer(f"{'🐇 Slowmode выключен, нахуй!' if delay==0 else f'🐢 Slowmode {delay}с, бля!'}", show_alert=True)
        elif action == "antimat":
            ANTI_MAT_ENABLED = not ANTI_MAT_ENABLED
            await call.answer(f"🧼 Антимат {'✅ включён, нахуй' if ANTI_MAT_ENABLED else '❌ выключен, бля'}!", show_alert=True)
        elif action == "autokick":
            AUTO_KICK_BOTS = not AUTO_KICK_BOTS
            await call.answer(f"🤖 Автокик {'✅ включён, нахуй' if AUTO_KICK_BOTS else '❌ выключен, бля'}!", show_alert=True)
        elif action == "rules":
            await call.message.edit_text(RULES_TEXT, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
        elif action == "botstats":
            total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
            total_warns = sum(sum(v.values()) for v in warnings.values())
            await call.message.edit_text(
                f"📊 <b>Статистика бота, нахуй</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
                f"⚠️ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
                f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
                f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
                f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
        await call.message.edit_text(
            f"🔧 <b>Управление чатом, бля</b>\n\n🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
            parse_mode="HTML", reply_markup=kb_chat(tid))
    except Exception as e:
        logger.error(f"cb_chat {action}: {e}"); await call.answer("❗ Ошибка, нахуй.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("game:"))
async def cb_game(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов, нахуй!", show_alert=True); return
    parts = call.data.split(":", 2); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё раз, нахуй", callback_data=call.data)],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]])
    try:
        if action == "roll":
            await call.message.edit_text(f"🎲 Бросаю d6, нахуй... выпало: <b>{random.randint(1,6)}</b>!", parse_mode="HTML", reply_markup=back_kb)
        elif action == "flip":
            await call.message.edit_text(random.choice(["🪙 Орёл, нахуй!", "🪙 Решка, бля!"]), reply_markup=back_kb)
        elif action == "slot":
            symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
            s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
            if s1==s2==s3=="💎":             res = "💰 ДЖЕКПОТ, НАХУЙ!!"
            elif s1==s2==s3:                 res = f"🎉 Три {s1}! Выиграл, нахуй!"
            elif s1==s2 or s2==s3 or s1==s3: res = "😐 Два одинаковых. Почти, бля!"
            else:                            res = "😢 Не повезло, нахуй."
            await call.message.edit_text(f"🎰 [ {s1} | {s2} | {s3} ]\n\n{res}", reply_markup=back_kb)
        elif action == "8ball":
            await call.message.edit_text(f"🎱 <b>Ответ шара, нахуй:</b>\n\n{random.choice(BALL_ANSWERS)}", parse_mode="HTML", reply_markup=back_kb)
        elif action.startswith("rps_"):
            mp = {"k":("к","🪨 Камень"),"n":("н","✂️ Ножницы"),"b":("б","📄 Бумага")}
            wins = {"к":"н","н":"б","б":"к"}
            key = action.split("_")[1]; pk,pl = mp[key]; bk,bl = random.choice(list(mp.values()))
            res = "🤝 Ничья, нахуй!" if pk==bk else ("🎉 Ты выиграл, бля!" if wins[pk]==bk else "😈 Я выиграл, нахуй!")
            await call.message.edit_text(f"Ты: {pl}\nЯ: {bl}\n\n{res}", reply_markup=back_kb)
        elif action == "quote":
            await call.message.edit_text(f"💬 {random.choice(QUOTES)}", reply_markup=back_kb)
        elif action.startswith("weather_"):
            city = action.replace("weather_","")
            if city == "custom":
                pending[call.from_user.id] = {"action":"weather_city","target_id":0,"target_name":"","chat_id":cid}
                await call.message.edit_text("🌤 Напиши название города, нахуй:",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
                await call.answer(); return
            await call.message.edit_text(await get_weather(city), parse_mode="HTML", reply_markup=back_kb)
        elif action.startswith("countdown"):
            n = int(action.replace("countdown",""))
            await call.message.edit_text(f"⏱ Отсчёт, нахуй: <b>{n}</b>...", parse_mode="HTML")
            for i in range(n-1, 0, -1):
                await asyncio.sleep(1)
                try: await call.message.edit_text(f"⏱ <b>{i}</b>...", parse_mode="HTML")
                except: pass
            await asyncio.sleep(1)
            await call.message.edit_text("🚀 <b>ПОЕХАЛИ, НАХУЙ!</b>", parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
    except Exception as e:
        logger.error(f"cb_game {action}: {e}"); await call.answer("❗ Ошибка, нахуй.", show_alert=True)
    await call.answer()

# ─── Текстовые команды ────────────────────────────────

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    reason = command.args or "Нарушение правил"; target = message.reply_to_message.from_user
    await bot.ban_chat_member(message.chat.id, target.id)
    await message.reply(random.choice(BAN_MESSAGES).format(name=target.mention_html(), reason=reason), parse_mode="HTML")
    await send_log(f"🔨 <b>Бан</b>\n👤 {target.full_name} (<code>{target.id}</code>)\n📝 {reason}", "BAN")

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await message.reply(f"♻️ {target.mention_html()} разбанен, нахуй. Дали второй шанс.", parse_mode="HTML")

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    mins, label = parse_duration(command.args) if command.args else (60, "1 ч.")
    if not mins: mins, label = 60, "1 ч."
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    await message.reply(random.choice(MUTE_MESSAGES).format(name=target.mention_html(), time=label), parse_mode="HTML")
    await send_log(f"🔇 <b>Мут</b>\n👤 {target.full_name} (<code>{target.id}</code>)\n⏱ {label}", "MUTE")

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    await message.reply(f"🔊 {target.mention_html()} размучен, нахуй.", parse_mode="HTML")

@dp.message(Command("warn"))
async def cmd_warn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user; reason = command.args or "Нарушение правил"; cid = message.chat.id
    warnings[cid][target.id] += 1; count = warnings[cid][target.id]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, target.id); warnings[cid][target.id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=target.mention_html(), max=MAX_WARNINGS)
        await send_log(f"🔨 <b>Автобан</b>\n👤 {target.full_name}", "BAN")
    else:
        msg = random.choice(WARN_MESSAGES).format(name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
        await send_log(f"⚠️ <b>Варн {count}/{MAX_WARNINGS}</b>\n👤 {target.full_name}\n📝 {reason}", "WARN")
    await message.reply(msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def cmd_unwarn(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1
    await message.reply(f"✅ С {target.mention_html()} снят варн, нахуй. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>", parse_mode="HTML")

@dp.message(Command("del"))
async def cmd_del(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    await message.reply_to_message.delete(); await message.delete()

@dp.message(Command("clear"))
async def cmd_clear(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError: await message.reply("❗ /clear 20, нахуй"); return
    deleted = 0
    for i in range(message.message_id, message.message_id - count - 1, -1):
        try: await bot.delete_message(message.chat.id, i); deleted += 1
        except: pass
    sent = await message.answer(f"🧹 Удалено: <b>{deleted}</b> сообщений, нахуй.", parse_mode="HTML")
    await asyncio.sleep(3)
    try: await sent.delete()
    except: pass

@dp.message(Command("announce"))
async def cmd_announce(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        pending[message.from_user.id] = {"action":"announce_text","target_id":0,"target_name":"","chat_id":message.chat.id}
        await message.reply("📢 Напиши текст объявления, нахуй:"); return
    await message.delete()
    await message.answer(f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}", parse_mode="HTML")

@dp.message(Command("pin"))
async def cmd_pin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    await bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Закреплено, нахуй!")

@dp.message(Command("unpin"))
async def cmd_unpin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Откреплено, нахуй!")

@dp.message(Command("lock"))
async def cmd_lock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await message.reply("🔒 Чат <b>заблокирован</b>, нахуй.", parse_mode="HTML")
    await send_log(f"🔒 <b>Чат заблокирован</b>\n💬 <code>{message.chat.id}</code>", "LOCK")

@dp.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(
        can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
        can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
    await message.reply("🔓 Чат <b>разблокирован</b>, нахуй.", parse_mode="HTML")

@dp.message(Command("slowmode"))
async def cmd_slowmode(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: delay = int(command.args) if command.args else 10
    except ValueError: await message.reply("❗ /slowmode 30, нахуй"); return
    await bot.set_chat_slow_mode_delay(message.chat.id, delay)
    await message.reply(f"🐢 Slowmode: <b>{delay}с</b>, нахуй" if delay else "🐇 Slowmode выключен, нахуй.", parse_mode="HTML")

@dp.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    title = command.args or "Участник"; target = message.reply_to_message.from_user
    await bot.set_chat_administrator_custom_title(message.chat.id, target.id, title)
    await message.reply(f"🏅 {target.mention_html()} получил тег: <b>{title}</b>, нахуй", parse_mode="HTML")

@dp.message(Command("poll"))
async def cmd_poll(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args or "|" not in command.args:
        await message.reply("❗ /poll Вопрос|Вар1|Вар2, нахуй"); return
    parts = [p.strip() for p in command.args.split("|")]
    if len(parts) < 3: await message.reply("❗ Нужно минимум 2 варианта, нахуй."); return
    await message.delete()
    await bot.send_poll(message.chat.id, question=parts[0], options=parts[1:], is_anonymous=False)

@dp.message(Command("antimat"))
async def cmd_antimat(message: Message, command: CommandObject):
    global ANTI_MAT_ENABLED
    if not await require_admin(message): return
    if not command.args:
        await message.reply(f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>, нахуй", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on":    ANTI_MAT_ENABLED = True;  await message.reply("🧼 Антимат <b>включён</b>, нахуй!", parse_mode="HTML")
    elif a == "off": ANTI_MAT_ENABLED = False; await message.reply("🔞 Антимат <b>выключен</b>, бля.", parse_mode="HTML")

@dp.message(Command("autokick"))
async def cmd_autokick(message: Message, command: CommandObject):
    global AUTO_KICK_BOTS
    if not await require_admin(message): return
    if not command.args:
        await message.reply(f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>, нахуй", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on":    AUTO_KICK_BOTS = True;  await message.reply("🤖 Автокик <b>включён</b>, нахуй!", parse_mode="HTML")
    elif a == "off": AUTO_KICK_BOTS = False; await message.reply("🤖 Автокик <b>выключен</b>, бля.", parse_mode="HTML")

@dp.message(Command("warn24"))
async def cmd_warn24(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
    await message.reply(f"📵 {target.mention_html()} — мут на <b>24 часа</b> за рекламу, нахуй!", parse_mode="HTML")
    await send_log(f"📵 <b>Мут 24ч (реклама)</b>\n👤 {target.full_name} (<code>{target.id}</code>)", "MUTE")

@dp.message(Command("rban"))
async def cmd_rban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user
    await message.reply(f"🎲 {target.mention_html()} получил <b>шуточный бан</b>!\n📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет, нахуй)</i>", parse_mode="HTML")

@dp.message(Command("adminlist"))
async def cmd_adminlist(message: Message):
    if not await require_admin(message): return
    admins = await bot.get_chat_administrators(message.chat.id)
    lines  = ["👮 <b>Администраторы чата, нахуй:</b>\n"]
    for adm in admins:
        if adm.user.is_bot: continue
        icon  = "👑" if adm.status == "creator" else "🛡"
        title = f" — <i>{adm.custom_title}</i>" if hasattr(adm,"custom_title") and adm.custom_title else ""
        lines.append(f"{icon} {adm.user.mention_html()}{title}")
    await message.reply("\n".join(lines), parse_mode="HTML")

@dp.message(Command("note"))
async def cmd_note(message: Message, command: CommandObject):
    if not command.args: await message.reply("📝 /note set/get/del/list [имя] [текст], нахуй"); return
    parts = command.args.split(maxsplit=2); action = parts[0].lower(); cid = message.chat.id
    if action == "set":
        if not await require_admin(message): return
        if len(parts) < 3: await message.reply("❗ /note set [имя] [текст], нахуй"); return
        notes[cid][parts[1]] = parts[2]; await message.reply(f"✅ Заметка <b>{parts[1]}</b> сохранена, нахуй!", parse_mode="HTML")
    elif action == "get":
        if len(parts) < 2: await message.reply("❗ /note get [имя], нахуй"); return
        t = notes[cid].get(parts[1])
        await message.reply(f"📝 <b>{parts[1]}:</b>\n{t}" if t else "❌ Заметка не найдена, нахуй.", parse_mode="HTML")
    elif action == "del":
        if not await require_admin(message): return
        if len(parts) > 1 and parts[1] in notes[cid]:
            del notes[cid][parts[1]]; await message.reply(f"🗑 Заметка <b>{parts[1]}</b> удалена, нахуй.", parse_mode="HTML")
        else: await message.reply("❌ Не найдена, нахуй.")
    elif action == "list":
        keys = list(notes[cid].keys())
        await message.reply("📋 <b>Заметки, нахуй:</b>\n" + "\n".join(f"• {k}" for k in keys) if keys else "📭 Заметок нет, нахуй.", parse_mode="HTML")

@dp.message(Command("botstats"))
async def cmd_botstats(message: Message):
    if not await require_admin(message): return
    total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
    total_warns = sum(sum(v.values()) for v in warnings.values())
    await message.reply(
        f"📊 <b>Статистика бота, нахуй</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n⚠️ Варнов: <b>{total_warns}</b>\n"
        f"😴 AFK: <b>{len(afk_users)}</b>\n🌐 Чатов: <b>{len(chat_stats)}</b>\n"
        f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
        parse_mode="HTML")

@dp.message(Command("weather"))
async def cmd_weather(message: Message, command: CommandObject):
    if not command.args: await message.reply("🌤 Укажи город, нахуй: /weather Москва"); return
    wait = await message.reply("⏳ Получаю данные, нахуй...")
    await wait.edit_text(await get_weather(command.args), parse_mode="HTML")

@dp.message(Command("rep"))
async def cmd_rep(message: Message):
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user; score = reputation[message.chat.id][target.id]
    await message.reply(f"{'⭐' if score>=0 else '💀'} Репутация {target.mention_html()}: <b>{score:+d}</b>, нахуй", parse_mode="HTML")

@dp.message(F.text.in_({"+1", "+", "👍"}))
async def rep_plus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await message.reply("😏 Себе репу не накручивай, нахуй!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await message.reply(f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин., нахуй."); return
    rep_cooldown[key] = now; reputation[message.chat.id][target.id] += 1
    await message.reply(f"⬆️ {target.mention_html()} +1, нахуй! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>", parse_mode="HTML")

@dp.message(F.text.in_({"-1", "-", "👎"}))
async def rep_minus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await message.reply("😏 Себе репу не снижай, нахуй!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await message.reply(f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин., нахуй."); return
    rep_cooldown[key] = now; reputation[message.chat.id][target.id] -= 1
    await message.reply(f"⬇️ {target.mention_html()} -1, нахуй! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>", parse_mode="HTML")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats: await message.reply("📊 Статистика пуста, нахуй!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = ["📊 <b>Топ активных, нахуй:</b>\n"]
    for i, (uid, count) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {count} сообщ.")
    await message.reply("\n".join(lines), parse_mode="HTML")

@dp.message(Command("afk"))
async def cmd_afk(message: Message, command: CommandObject):
    reason = command.args or "без причины, нахуй"; afk_users[message.from_user.id] = reason
    await message.reply(f"😴 {message.from_user.mention_html()} ушёл в AFK, нахуй: {reason}", parse_mode="HTML")

@dp.message(Command("info"))
async def cmd_info(message: Message):
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    user = message.reply_to_message.from_user; member = await bot.get_chat_member(message.chat.id, user.id)
    smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор","member":"👤 Участник",
            "restricted":"🔇 Ограничен","kicked":"🔨 Забанен","left":"🚶 Вышел"}
    afk  = f"\n😴 AFK: {afk_users[user.id]}" if user.id in afk_users else ""
    await message.reply(
        f"👤 <b>Инфо, бля:</b>\n{user.mention_html()}{afk}\n"
        f"🔗 {'@'+user.username if user.username else 'нет'}\n🆔 <code>{user.id}</code>\n"
        f"📌 {smap.get(member.status, member.status)}\n"
        f"⚠️ Варнов: <b>{warnings[message.chat.id].get(user.id,0)}/{MAX_WARNINGS}</b>\n"
        f"⭐ Репутация: <b>{reputation[message.chat.id].get(user.id,0):+d}</b>\n"
        f"💬 Сообщений: <b>{chat_stats[message.chat.id].get(user.id,0)}</b>", parse_mode="HTML")

@dp.message(Command("warnings"))
async def cmd_warnings(message: Message):
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение, нахуй."); return
    target = message.reply_to_message.from_user
    await message.reply(f"📊 {target.mention_html()} — варнов: <b>{warnings[message.chat.id].get(target.id,0)}/{MAX_WARNINGS}</b>, нахуй", parse_mode="HTML")

@dp.message(Command("quote"))
async def cmd_quote(message: Message):
    await message.reply(f"💬 {random.choice(QUOTES)}")

@dp.message(Command("roll"))
async def cmd_roll(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 10000))
    except: sides = 6
    await message.reply(f"🎲 Бросаю d{sides}, нахуй... выпало: <b>{random.randint(1,sides)}</b>!", parse_mode="HTML")

@dp.message(Command("flip"))
async def cmd_flip(message: Message):
    await message.reply(random.choice(["🪙 Орёл, нахуй!", "🪙 Решка, бля!"]))

@dp.message(Command("8ball"))
async def cmd_8ball(message: Message, command: CommandObject):
    if not command.args: await message.reply("❓ /8ball [вопрос], нахуй"); return
    await message.reply(f"🎱 <b>Вопрос:</b> {command.args}\n\n<b>Ответ:</b> {random.choice(BALL_ANSWERS)}", parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message, command: CommandObject):
    if not command.args: await message.reply("❗ /rate [что], нахуй"); return
    score = random.randint(0, 10)
    await message.reply(f"📊 <b>{command.args}</b>\n{'⭐'*score+'☆'*(10-score)}\nОценка: <b>{score}/10</b>, нахуй", parse_mode="HTML")

@dp.message(Command("iq"))
async def cmd_iq(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    iq   = random.randint(1, 200)
    c    = "🥔 Картошка умнее, нахуй." if iq<70 else ("🐒 Обезьяна лучше, бля." if iq<100 else ("😐 Сойдёт, нахуй." if iq<130 else ("🧠 Умный, нахуй!" if iq<160 else "🤖 Эйнштейн что ли, пиздец?!")))
    await message.reply(f"🧠 IQ {user.mention_html()}: <b>{iq}</b>\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def cmd_gay(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    p    = random.randint(0, 100)
    await message.reply(f"🏳️‍🌈 {user.mention_html()}\n{'🌈'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка, нахуй 😄", parse_mode="HTML")

@dp.message(Command("truth"))
async def cmd_truth(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(f"🤔 <b>Вопрос для {user.mention_html()}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML")

@dp.message(Command("dare"))
async def cmd_dare(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(f"😈 <b>Задание для {user.mention_html()}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML")

@dp.message(Command("wyr"))
async def cmd_wyr(message: Message):
    await message.reply(f"🤯 <b>Выбор без выбора, нахуй:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML")

@dp.message(Command("rps"))
async def cmd_rps(message: Message, command: CommandObject):
    choices = {"к":"🪨 Камень","н":"✂️ Ножницы","б":"📄 Бумага"}; wins = {"к":"н","н":"б","б":"к"}
    if not command.args or command.args.lower() not in choices:
        await message.reply("✂️ /rps к / н / б, нахуй"); return
    p = command.args.lower(); b = random.choice(list(choices.keys()))
    res = "🤝 Ничья, нахуй!" if p==b else ("🎉 Ты выиграл, бля!" if wins[p]==b else "😈 Я выиграл, нахуй!")
    await message.reply(f"Ты: {choices[p]}\nЯ: {choices[b]}\n\n{res}")

@dp.message(Command("slot"))
async def cmd_slot(message: Message):
    symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
    s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
    if s1==s2==s3=="💎":             res = "💰 ДЖЕКПОТ, НАХУЙ!!"
    elif s1==s2==s3:                 res = f"🎉 Три {s1}! Выиграл, нахуй!"
    elif s1==s2 or s2==s3 or s1==s3: res = "😐 Два одинаковых. Почти, бля!"
    else:                            res = "😢 Не повезло, нахуй."
    await message.reply(f"🎰 [ {s1} | {s2} | {s3} ]\n\n{res}")

@dp.message(Command("choose"))
async def cmd_choose(message: Message, command: CommandObject):
    if not command.args or "|" not in command.args:
        await message.reply("❗ /choose вар1|вар2|вар3, нахуй"); return
    options = [o.strip() for o in command.args.split("|") if o.strip()]
    if len(options) < 2: await message.reply("❗ Минимум 2 варианта, нахуй."); return
    await message.reply(f"🎯 ✅ <b>{random.choice(options)}</b>, нахуй!", parse_mode="HTML")

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    sign, text = random.choice(list(HOROSCOPES.items()))
    await message.reply(f"{sign} <b>Гороскоп для {user.mention_html()}:</b>\n\n{text}", parse_mode="HTML")

@dp.message(Command("predict"))
async def cmd_predict(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(f"🔮 <b>Предсказание для {user.mention_html()}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML")

@dp.message(Command("compliment"))
async def cmd_compliment(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(f"💐 <b>{user.mention_html()}</b>, {random.choice(COMPLIMENTS)}", parse_mode="HTML")

@dp.message(Command("countdown"))
async def cmd_countdown(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: n = min(int(command.args or 5), 10)
    except: n = 5
    sent = await message.reply(f"⏱ <b>{n}</b>..., нахуй", parse_mode="HTML")
    for i in range(n-1, 0, -1):
        await asyncio.sleep(1)
        try: await sent.edit_text(f"⏱ <b>{i}</b>...", parse_mode="HTML")
        except: pass
    await asyncio.sleep(1)
    await sent.edit_text("🚀 <b>ПОЕХАЛИ, НАХУЙ!</b>", parse_mode="HTML")

# ─── Автоответы ───────────────────────────────────────

@dp.message(F.text & ~F.text.startswith("/"))
async def auto_reply_handler(message: Message):
    if not message.text: return
    text_lower = message.text.lower()
    for trigger, responses in TRIGGER_RESPONSES.items():
        if trigger in text_lower:
            if random.random() < 0.3:
                await message.reply(random.choice(responses))
            break

# ─── Запуск ───────────────────────────────────────────

async def main():
    await start_web()
    if not BOT_TOKEN: raise ValueError("❌ BOT_TOKEN не задан, нахуй!")
    logger.info("✅ Бот запускается, нахуй...")
    await send_log("🚀 <b>Бот запущен, нахуй!</b>", "INFO")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

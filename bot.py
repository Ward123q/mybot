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
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiohttp import web
import json
from pathlib import Path

DATA_FILE = "data.json"

def load_data():
    global warnings, reputation, notes
    if Path(DATA_FILE).exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for cid, users in data.get("warnings", {}).items():
                for uid, count in users.items():
                    warnings[int(cid)][int(uid)] = count
            for cid, users in data.get("reputation", {}).items():
                for uid, score in users.items():
                    reputation[int(cid)][int(uid)] = score
            for cid, nts in data.get("notes", {}).items():
                notes[int(cid)] = nts
        except:
            pass

def save_data():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "warnings":   {str(cid): {str(uid): c for uid, c in users.items()} for cid, users in warnings.items()},
                "reputation": {str(cid): {str(uid): s for uid, s in users.items()} for cid, users in reputation.items()},
                "notes":      {str(cid): nts for cid, nts in notes.items()},
            }, f, ensure_ascii=False, indent=2)
    except:
        pass

LOG_CHANNEL_ID   = -1003832428474
BOT_TOKEN        = os.getenv("BOT_TOKEN")
WEATHER_API_KEY  = os.getenv("WEATHER_API_KEY", "")
OWNER_ID         = 7823802800
MAX_WARNINGS     = 3
FLOOD_LIMIT      = 5
FLOOD_TIME       = 5
ANTI_MAT_ENABLED = False
MAT_MUTE_MINUTES = 5
AUTO_KICK_BOTS   = True

logging.basicConfig(level=logging.INFO)
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
reminders     = {}
birthdays     = {}
levels        = defaultdict(lambda: defaultdict(int))
xp_data       = defaultdict(lambda: defaultdict(int))

RULES_TEXT = (
    "📄 <b>Правила анон чата</b>\n\n"
    "🔞 <b>Контент 18+:</b>\n"
    "• Видео 18+\n"
    "• Стикеры 18+\n"
    "• Гифки 18+\n"
    "• Ссылки на порнографические сайты\n"
    "⚠️ Наказание: предупреждение. При повторном нарушении — бан.\n\n"
    "📢 <b>Реклама:</b>\n"
    "• Реклама своих социальных сетей\n"
    "⚠️ Наказание: Предупреждение / Мут 30 мин / Бан на 24 часа\n\n"
    "🚫 <b>Общие правила:</b>\n"
    "• Уважайте друг друга\n"
    "• Без спама и флуда\n"
)

MUTE_MESSAGES = [
    "🔇 {name} заглушён на {time}.",
    "😶 {name} получил мут на {time}.",
    "🤫 {name} временно не может писать. Время: {time}.",
    "📵 {name} помолчи {time}.",
]
BAN_MESSAGES = [
    "🔨 {name} забанен. Причина: {reason}",
    "💥 {name} вылетел из чата. Причина: {reason}",
    "🚪 {name} удалён из чата. Причина: {reason}",
]
WARN_MESSAGES = [
    "⚠️ {name} получил предупреждение {count}/{max}. Причина: {reason}.",
    "🚨 {name} — предупреждение {count}/{max}. Причина: {reason}. Будь осторожен.",
    "😡 {name} — предупреждение {count}/{max}. Причина: {reason}.",
]
AUTOBAN_MESSAGES = [
    "🔨 {name} набрал {max} предупреждений и получил автобан.",
    "💀 {name} — {max} варнов = бан.",
]
RANDOM_BAN_REASONS = [
    "слишком умный", "подозрение в адекватности", "нарушение закона бутерброда",
    "превышение лимита здравого смысла", "нарушение пространственно-временного континуума",
    "слишком много смайликов",
]
QUOTES = [
    "«Не важно, как медленно ты идёшь, главное — не останавливаться.» — Конфуций",
    "«Будь изменением, которое хочешь видеть в мире.» — Ганди",
    "«Будь собой. Все остальные роли заняты.» — Уайльд",
    "«Единственный способ делать великое — любить то, что делаешь.» — Джобс",
    "«Успех — идти от неудачи к неудаче без потери энтузиазма.» — Черчилль",
    "«Делай что должен, и будь что будет.» — Толстой",
]
BALL_ANSWERS = [
    "🟢 Определённо да!", "🟢 Без сомнений!", "🟢 Скорее всего да.",
    "🟡 Трудно сказать.", "🟡 Спроси потом.", "🟡 Пока неясно.",
    "🔴 хз", "🔴 Нет.", "🔴 Определённо нет",
]
TRUTH_QUESTIONS = [
    "Ты когда-нибудь врал другу?", "Какой твой самый большой страх?",
    "Ты когда-нибудь влюблялся в друга?", "Что тебя раздражает больше всего?",
    "Твой самый неловкий момент в жизни?", "Ты когда-нибудь плакал из-за фильма?",
    "Что ты никогда не расскажешь родителям?", "ты когда нибудь писял под кровать?",
    "ты бы хотел себе ту самую девушку найти?", "ты бы хотел завести детей в будущем?",
]
DARE_CHALLENGES = [
    "Напиши комплимент случайному участнику чата!", "Признайся в чём-нибудь стыдном...",
    "Напиши стих про себя прямо сейчас.", "Расскажи смешной случай из жизни.",
    "Придумай и напиши анекдот прямо сейчас.", "сделай 10 приседаний если не лень",
]
WOULD_YOU_RATHER = [
    "Быть богатым но одиноким или бедным но счастливым?",
    "Уметь летать или быть невидимым?", "Знать будущее или изменить прошлое?",
    "Говорить только правду или постоянно врать?",
    "Жить 200 лет в бедности или 50 лет в богатстве?",
]
HOROSCOPES = {
    "♈ Овен":     "Сегодня звёзды говорят — делай что хочешь, но с умом.",
    "♉ Телец":    "День для отдыха. Полежи, поешь, снова полежи.",
    "♊ Близнецы": "Раздвоение личности сегодня — твоя суперсила.",
    "♋ Рак":      "Спрячься в домик. Там лучше. Там печеньки.",
    "♌ Лев":      "Ты красивый и все это знают. Используй по полной.",
    "♍ Дева":     "Разложи всё по полочкам. Буквально все полочки.",
    "♎ Весы":     "Не можешь выбрать что поесть? Это карма.",
    "♏ Скорпион": "Таинственность — твоё оружие. Молчи и улыбайся.",
    "♐ Стрелец":  "Стреляй в мечты! Может и попадёшь.",
    "♑ Козерог":  "Работай. Работай ещё. Потом отдохнёшь на пенсии.",
    "♒ Водолей":  "Ты уникальный. Как и все остальные.",
    "♓ Рыбы":     "Плыви по течению. Или против. Главное — плыви.",
}
COMPLIMENTS = [
    "Ты просто огонь! 🔥", "С тобой в чате теплее! ☀️",
    "Ты делаешь этот чат лучше! 💎", "Без тебя тут было бы скучнее! 🌟",
    "Ты как редкий покемон — очень ценный! ⭐", "Интеллект зашкаливает! 🧠",
]
PREDICTIONS = [
    "🔮 Сегодня тебе повезёт! Только не трать деньги.",
    "💀 Осторожно с незнакомцами сегодня.",
    "⭐ Звёзды говорят — ты красавчик!", "🍀 Удача на твоей стороне!",
    "🐉 Тебя ждёт необычная встреча. Возможно с котом.",
    "💤 Лучший план на сегодня — поспать. Доверяй процессу.",
]

# ═══════════════════════════════════════════
#         ВЕБ-СЕРВЕР (для Render)
# ═══════════════════════════════════════════
async def health(request):
    return web.Response(text="OK")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()

# ═══════════════════════════════════════════
#             ХЕЛПЕРЫ
# ═══════════════════════════════════════════
async def check_admin(message: Message) -> bool:
    if message.from_user.id == OWNER_ID:
        return True
    try:
        m = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return m.status in ("administrator", "creator")
    except:
        return False

async def is_admin_by_id(chat_id: int, user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except:
        return False

async def require_admin(message: Message) -> bool:
    if not await check_admin(message):
        await message.reply("🚫 <b>Только для администраторов!</b>", parse_mode="HTML")
        return False
    return True

def parse_duration(arg: str):
    arg = arg.strip().lower()
    try:
        if arg.endswith("m"):   return int(arg[:-1]), f"{int(arg[:-1])} мин."
        elif arg.endswith("h"): return int(arg[:-1])*60, f"{int(arg[:-1])} ч."
        elif arg.endswith("d"): return int(arg[:-1])*1440, f"{int(arg[:-1])} дн."
        elif arg.isdigit():     return int(arg), f"{int(arg)} мин."
    except:
        pass
    return None, None

async def log_action(text: str):
    try:
        await bot.send_message(LOG_CHANNEL_ID, text, parse_mode="HTML")
    except:
        pass

async def get_weather(city: str) -> str:
    if not WEATHER_API_KEY:
        return "❌ Weather API ключ не настроен."
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": WEATHER_API_KEY, "units": "metric", "lang": "ru"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                d = await r.json()
                if d.get("cod") != 200: return f"❌ Город <b>{city}</b> не найден."
                temp  = round(d["main"]["temp"]); feels = round(d["main"]["feels_like"])
                desc  = d["weather"][0]["description"].capitalize()
                humid = d["main"]["humidity"]; wind = round(d["wind"]["speed"])
                dl    = desc.lower()
                emoji = "☀️" if "ясно" in dl else ("🌧" if "дождь" in dl else ("❄️" if "снег" in dl else "⛅"))
                return (f"{emoji} <b>Погода в {d['name']}</b>\n\n"
                        f"🌡 Температура: <b>{temp}°C</b> (ощущается {feels}°C)\n"
                        f"📋 {desc}\n💧 Влажность: <b>{humid}%</b>\n💨 Ветер: <b>{wind} м/с</b>")
    except:
        return "❌ Ошибка при получении погоды."

# ═══════════════════════════════════════════
#            КЛАВИАТУРЫ
# ═══════════════════════════════════════════
def kb_back(tid: int) -> list:
    return [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{tid}")]

def kb_main_menu(tid: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Управление участником", callback_data=f"panel:select:{tid}"),
         InlineKeyboardButton(text="💬 Сообщения",             callback_data=f"panel:messages:{tid}")],
        [InlineKeyboardButton(text="👥 Участники",             callback_data=f"panel:members:{tid}"),
         InlineKeyboardButton(text="🔧 Чат",                   callback_data=f"panel:chat:{tid}")],
        [InlineKeyboardButton(text="🎮 Игры",                  callback_data=f"panel:games:{tid}"),
         InlineKeyboardButton(text="📊 Статистика",            callback_data=f"panel:botstats2:{tid}")],
        [InlineKeyboardButton(text="❌ Закрыть",               callback_data="panel:close:0")],
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
        [InlineKeyboardButton(text="5 мин",   callback_data=f"mute:{tid}:5"),
         InlineKeyboardButton(text="15 мин",  callback_data=f"mute:{tid}:15"),
         InlineKeyboardButton(text="30 мин",  callback_data=f"mute:{tid}:30")],
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
        [InlineKeyboardButton(text="🤬 Мат",          callback_data=f"warn:{tid}:Мат в чате"),
         InlineKeyboardButton(text="💬 Спам",          callback_data=f"warn:{tid}:Спам")],
        [InlineKeyboardButton(text="😡 Оскорбление",  callback_data=f"warn:{tid}:Оскорбление"),
         InlineKeyboardButton(text="🚫 Флуд",          callback_data=f"warn:{tid}:Флуд")],
        [InlineKeyboardButton(text="📵 Реклама",      callback_data=f"warn:{tid}:Реклама"),
         InlineKeyboardButton(text="🔞 Контент 18+",  callback_data=f"warn:{tid}:Контент 18+")],
        [InlineKeyboardButton(text="✏️ Своя причина", callback_data=f"warn:{tid}:custom")],
        kb_back(tid),
    ])

def kb_ban(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤬 Грубые нарушения",  callback_data=f"ban:{tid}:Грубые нарушения правил"),
         InlineKeyboardButton(text="💬 Спам/реклама",       callback_data=f"ban:{tid}:Спам и реклама")],
        [InlineKeyboardButton(text="🔞 Контент 18+",        callback_data=f"ban:{tid}:Контент 18+"),
         InlineKeyboardButton(text="🤖 Бот/накрутка",       callback_data=f"ban:{tid}:Бот или накрутка")],
        [InlineKeyboardButton(text="⏰ Бан на 24 часа",     callback_data=f"ban:{tid}:tempban24"),
         InlineKeyboardButton(text="✏️ Своя причина",       callback_data=f"ban:{tid}:custom")],
        kb_back(tid),
    ])

def kb_fun(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Шуточный бан",      callback_data=f"fun:rban:{tid}"),
         InlineKeyboardButton(text="🧠 Проверить IQ",      callback_data=f"fun:iq:{tid}")],
        [InlineKeyboardButton(text="🌈 % гейства",         callback_data=f"fun:gay:{tid}"),
         InlineKeyboardButton(text="💐 Комплимент",         callback_data=f"fun:compliment:{tid}")],
        [InlineKeyboardButton(text="🔮 Предсказание",      callback_data=f"fun:predict:{tid}"),
         InlineKeyboardButton(text="♈ Гороскоп",          callback_data=f"fun:horoscope:{tid}")],
        [InlineKeyboardButton(text="⭐ Оценить",           callback_data=f"fun:rate:{tid}"),
         InlineKeyboardButton(text="🤔 Вопрос правды",     callback_data=f"fun:truth:{tid}")],
        [InlineKeyboardButton(text="😈 Задание",           callback_data=f"fun:dare:{tid}"),
         InlineKeyboardButton(text="🤯 Выбор без выбора",  callback_data=f"fun:wyr:{tid}")],
        kb_back(tid),
    ])

def kb_messages(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Закрепить",         callback_data=f"msg:pin:{tid}"),
         InlineKeyboardButton(text="📌 Открепить",         callback_data=f"msg:unpin:{tid}")],
        [InlineKeyboardButton(text="🗑 Удалить сообщение", callback_data=f"msg:del:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 10",       callback_data=f"msg:clear10:{tid}")],
        [InlineKeyboardButton(text="🧹 Очистить 20",       callback_data=f"msg:clear20:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 50",       callback_data=f"msg:clear50:{tid}")],
        [InlineKeyboardButton(text="📢 Объявление",         callback_data=f"msg:announce:{tid}"),
         InlineKeyboardButton(text="📊 Голосование",        callback_data=f"msg:poll:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"panel:mainmenu:0")],
    ])

def kb_members(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮 Список админов",     callback_data=f"members:adminlist:{tid}"),
         InlineKeyboardButton(text="📊 Топ активности",     callback_data=f"members:top:{tid}")],
        [InlineKeyboardButton(text="📵 Мут 24ч за рекламу", callback_data=f"members:warn24:{tid}"),
         InlineKeyboardButton(text="⚠️ Варны участника",    callback_data=f"members:warninfo:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"panel:mainmenu:0")],
    ])

def kb_chat(tid: int) -> InlineKeyboardMarkup:
    ms = "✅ вкл" if ANTI_MAT_ENABLED else "❌ выкл"
    ks = "✅ вкл" if AUTO_KICK_BOTS   else "❌ выкл"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Заблокировать чат", callback_data=f"chat:lock:{tid}"),
         InlineKeyboardButton(text="🔓 Разблокировать",    callback_data=f"chat:unlock:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 10с",      callback_data=f"chat:slow:10:{tid}"),
         InlineKeyboardButton(text="🐢 Slowmode 30с",      callback_data=f"chat:slow:30:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 60с",      callback_data=f"chat:slow:60:{tid}"),
         InlineKeyboardButton(text="🐇 Выкл slowmode",     callback_data=f"chat:slow:0:{tid}")],
        [InlineKeyboardButton(text=f"🧼 Антимат: {ms}",   callback_data=f"chat:antimat:{tid}"),
         InlineKeyboardButton(text=f"🤖 Автокик: {ks}",   callback_data=f"chat:autokick:{tid}")],
        [InlineKeyboardButton(text="📜 Правила",            callback_data=f"chat:rules:{tid}"),
         InlineKeyboardButton(text="📊 Статистика",         callback_data=f"chat:botstats:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"panel:mainmenu:0")],
    ])

def kb_games(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик d6",          callback_data=f"game:roll:{tid}"),
         InlineKeyboardButton(text="🪙 Монетка",           callback_data=f"game:flip:{tid}")],
        [InlineKeyboardButton(text="🎰 Слот-машина",       callback_data=f"game:slot:{tid}"),
         InlineKeyboardButton(text="🎱 Шар предсказаний",  callback_data=f"game:8ball:{tid}")],
        [InlineKeyboardButton(text="✂️ КНБ — Камень",      callback_data=f"game:rps_k:{tid}"),
         InlineKeyboardButton(text="✂️ КНБ — Ножницы",     callback_data=f"game:rps_n:{tid}")],
        [InlineKeyboardButton(text="✂️ КНБ — Бумага",      callback_data=f"game:rps_b:{tid}"),
         InlineKeyboardButton(text="💬 Цитата дня",        callback_data=f"game:quote:{tid}")],
        [InlineKeyboardButton(text="🌤 Погода — Москва",   callback_data=f"game:weather_Москва:{tid}"),
         InlineKeyboardButton(text="🌤 Свой город",         callback_data=f"game:weather_custom:{tid}")],
        [InlineKeyboardButton(text="⏱ Отсчёт 5 сек",      callback_data=f"game:countdown5:{tid}"),
         InlineKeyboardButton(text="⏱ Отсчёт 10 сек",     callback_data=f"game:countdown10:{tid}")],
        [InlineKeyboardButton(text="◀️ Назад",             callback_data=f"panel:mainmenu:0")],
    ])

# ═══════════════════════════════════════════
#              MIDDLEWARE
# ═══════════════════════════════════════════
message_cache = {}

class StatsMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group","supergroup"):
            chat_stats[event.chat.id][event.from_user.id] += 1
            uid, cid = event.from_user.id, event.chat.id
            xp_data[cid][uid] += random.randint(1, 5)
            old_level = levels[cid][uid]
            new_level = xp_data[cid][uid] // 100
            if new_level > old_level:
                levels[cid][uid] = new_level
                title = (
                    "👑 Элита" if new_level >= 20 else
                    "🏆 Легенда" if new_level >= 10 else
                    "⭐ Ветеран" if new_level >= 5 else
                    "💪 Активный" if new_level >= 3 else
                    "👤 Участник")
                try:
                    await event.answer(
                        f"🎉 {event.from_user.mention_html()} достиг <b>{new_level} уровня</b>!\n"
                        f"🏅 Титул: <b>{title}</b>", parse_mode="HTML")
                except: pass
            if event.text:
                message_cache[event.message_id] = {
                    "text": event.text, "user": event.from_user.full_name,
                    "user_id": event.from_user.id, "chat_id": event.chat.id,
                    "chat_title": event.chat.title,
                }
        return await handler(event, data)

class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if event.chat.type not in ("group","supergroup"): return await handler(event, data)
        if event.text and event.text.startswith("/"): return await handler(event, data)
        if not event.from_user: return await handler(event, data)
        if event.new_chat_members or event.left_chat_member: return await handler(event, data)
        uid, cid = event.from_user.id, event.chat.id
        try:
            m = await bot.get_chat_member(cid, uid)
            if m.status in ("administrator","creator"): return await handler(event, data)
        except: pass
        now = time()
        flood_tracker[cid][uid] = [t for t in flood_tracker[cid][uid] if now - t < FLOOD_TIME]
        flood_tracker[cid][uid].append(now)
        if len(flood_tracker[cid][uid]) >= FLOOD_LIMIT:
            try:
                await event.delete()
                await bot.restrict_chat_member(cid, uid,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=timedelta(minutes=5))
                sent = await bot.send_message(cid,
                    f"🌊 {event.from_user.mention_html()}, флуд запрещён! Мут на 5 минут.",
                    parse_mode="HTML")
                flood_tracker[cid][uid].clear()
                await asyncio.sleep(8)
                try: await sent.delete()
                except: pass
            except: pass
            return
        return await handler(event, data)

class AntiMatMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if not ANTI_MAT_ENABLED: return await handler(event, data)
        if event.chat.type not in ("group","supergroup"): return await handler(event, data)
        if not event.text or event.text.startswith("/"): return await handler(event, data)
        if not event.from_user: return await handler(event, data)
        if event.new_chat_members or event.left_chat_member: return await handler(event, data)
        uid, cid = event.from_user.id, event.chat.id
        try:
            m = await bot.get_chat_member(cid, uid)
            if m.status in ("administrator","creator"): return await handler(event, data)
        except: pass
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
                            permissions=ChatPermissions(can_send_messages=False),
                            until_date=timedelta(minutes=mins))
                        await event.answer(random.choice(MUTE_MESSAGES).format(
                            name=f"<b>{target_name}</b>", time=label), parse_mode="HTML")
                    else: await event.reply("❗ Примеры: 10, 30m, 2h, 1d")
                elif action == "warn_custom":
                    reason = text.strip() or "Нарушение правил"
                    warnings[chat_id][target_id] += 1; count = warnings[chat_id][target_id]
                    if count >= MAX_WARNINGS:
                        await bot.ban_chat_member(chat_id, target_id); warnings[chat_id][target_id] = 0
                        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{target_name}</b>", max=MAX_WARNINGS)
                    else:
                        msg = random.choice(WARN_MESSAGES).format(
                            name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
                    await event.answer(msg, parse_mode="HTML")
                elif action == "ban_custom":
                    reason = text.strip() or "Нарушение правил"
                    await bot.ban_chat_member(chat_id, target_id)
                    await event.answer(random.choice(BAN_MESSAGES).format(
                        name=f"<b>{target_name}</b>", reason=reason), parse_mode="HTML")
                elif action == "announce_text":
                    await bot.send_message(chat_id,
                        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{text}\n\n— <b>Администрация</b>", parse_mode="HTML")
                elif action == "poll_text":
                    parts = [x.strip() for x in text.split("|") if x.strip()]
                    if len(parts) >= 3: await bot.send_poll(chat_id, question=parts[0], options=parts[1:], is_anonymous=False)
                    else: await event.reply("❗ Формат: Вопрос|Вариант1|Вариант2")
                elif action == "weather_city":
                    await event.answer(await get_weather(text.strip()), parse_mode="HTML")
            except Exception as e:
                await event.reply(f"❗ Ошибка: {e}")
            try: await event.delete()
            except: pass
            return
        return await handler(event, data)

dp.message.middleware(PendingInputMiddleware())
dp.message.middleware(StatsMiddleware())
dp.message.middleware(AntiFloodMiddleware())
dp.message.middleware(AntiMatMiddleware())

# ═══════════════════════════════════════════
#       НОВЫЕ УЧАСТНИКИ
# ═══════════════════════════════════════════
@dp.message(F.new_chat_members)
async def on_new_member(message: Message):
    for member in message.new_chat_members:
        if member.is_bot and AUTO_KICK_BOTS:
            try:
                await bot.ban_chat_member(message.chat.id, member.id)
                await bot.unban_chat_member(message.chat.id, member.id)
                sent = await message.answer(
                    f"🤖 Бот <b>{member.full_name}</b> автоматически удалён.", parse_mode="HTML")
                await asyncio.sleep(5)
                try: await sent.delete()
                except: pass
            except: pass
            continue
        await message.answer_photo(
            photo=FSInputFile("welcome.jpg"),
            caption=f"👋 Привет, {member.mention_html()}! Добро пожаловать в чат!\n\n"
            f"📜 Ознакомься с правилами чата перед тем как писать.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Правила чата", url="https://telegra.ph/Pravila-anon-chata-03-03-2")]
            ])
        )
        await log_action(f"👋 <b>ВХОД</b>\nУчастник: {member.mention_html()}\nЧат: {message.chat.title}")
        
@dp.message(F.left_chat_member)
async def on_left_member(message: Message):
    member = message.left_chat_member
    if member.is_bot: return
    await log_action(f"🚪 <b>ВЫХОД</b>\nУчастник: {member.mention_html()}\nЧат: {message.chat.title}")

# ═══════════════════════════════════════════
#         CALLBACK HANDLERS — ПАНЕЛЬ
# ═══════════════════════════════════════════
@dp.callback_query(F.data.startswith("panel:"))
async def cb_panel(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    _, action, tid_str = call.data.split(":", 2)
    tid = int(tid_str); cid = call.message.chat.id
    if tid != 0 and action in ("mute", "ban", "warn"):
        if await is_admin_by_id(cid, tid):
            await call.answer("🚫 Нельзя применить действие к администратору!", show_alert=True); return
    try:
        tm = await bot.get_chat_member(cid, tid) if tid != 0 else None
        tname   = tm.user.full_name if tm else "участник"
        mention = tm.user.mention_html() if tm else ""
    except:
        tname = f"ID {tid}"; mention = f"<code>{tid}</code>"
    try:
        if action == "close":
            await call.message.delete()
        elif action == "mainmenu":
            total_msgs  = sum(chat_stats[cid].values())
            total_warns = sum(warnings[cid].values())
            await call.message.edit_text(
                f"🛠 <b>Панель управления</b>\n\n"
                f"💬 Чат: <b>{call.message.chat.title}</b>\n"
                f"📊 Сообщений: <b>{total_msgs}</b>\n"
                f"⚠️ Варнов: <b>{total_warns}</b>\n"
                f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери раздел:",
                parse_mode="HTML", reply_markup=kb_main_menu())
        elif action == "select":
            await call.message.edit_text(
                "👤 <b>Управление участником</b>\n\nОткрой панель реплаем:\n"
                "<code>/panel</code> → ответь на сообщение участника",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        elif action == "back":
            if tid != 0:
                warns = warnings[cid].get(tid, 0); rep = reputation[cid].get(tid, 0)
                msgs  = chat_stats[cid].get(tid, 0)
                afk   = f"\n😴 AFK: {afk_users[tid]}" if tid in afk_users else ""
                await call.message.edit_text(
                    f"🛠 <b>Панель участника</b>\n\n👤 {mention}{afk}\n"
                    f"🆔 ID: <code>{tid}</code>\n"
                    f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
                    f"⭐ Репутация: <b>{rep:+d}</b>\n"
                    f"💬 Сообщений: <b>{msgs}</b>\n\nВыбери действие:",
                    parse_mode="HTML", reply_markup=kb_user_panel(tid))
            else:
                await call.message.edit_text("🛠 <b>Панель управления</b>\n\nВыбери раздел:",
                    parse_mode="HTML", reply_markup=kb_main_menu())
        elif action == "mute":
            await call.message.edit_text(f"🔇 <b>Мут для {tname}</b>\n\nВыбери время:",
                parse_mode="HTML", reply_markup=kb_mute(tid))
        elif action == "unmute":
            await bot.restrict_chat_member(cid, tid, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            await call.message.edit_text(f"🔊 <b>{tname}</b> размучен.", parse_mode="HTML")
            await log_action(f"🔊 <b>РАЗМУТ</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nЧат: {call.message.chat.title}")
        elif action == "warn":
            await call.message.edit_text(f"⚠️ <b>Варн для {tname}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_warn(tid))
        elif action == "unwarn":
            if warnings[cid][tid] > 0: warnings[cid][tid] -= 1
            await call.message.edit_text(
                f"✅ С <b>{tname}</b> снят варн. Осталось: <b>{warnings[cid][tid]}/{MAX_WARNINGS}</b>",
                parse_mode="HTML")
        elif action == "ban":
            await call.message.edit_text(f"🔨 <b>Бан для {tname}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_ban(tid))
        elif action == "unban":
            await bot.unban_chat_member(cid, tid, only_if_banned=True)
            await call.message.edit_text(f"♻️ <b>{tname}</b> разбанен.", parse_mode="HTML")
            await log_action(f"♻️ <b>РАЗБАН</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nЧат: {call.message.chat.title}")
        elif action == "del":
            try: await call.message.reply_to_message.delete()
            except: pass
            await call.message.edit_text("🗑 Сообщение удалено.")
        elif action == "info":
            tm2 = await bot.get_chat_member(cid, tid); u = tm2.user
            smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
                    "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
            afk = f"\n😴 AFK: {afk_users[tid]}" if tid in afk_users else ""
            await call.message.edit_text(
                f"👤 <b>Инфо:</b>\n\n🏷 {u.mention_html()}{afk}\n"
                f"🔗 {'@'+u.username if u.username else 'нет'}\n🆔 <code>{u.id}</code>\n"
                f"📌 {smap.get(tm2.status, tm2.status)}\n"
                f"⚠️ Варнов: <b>{warnings[cid].get(tid,0)}/{MAX_WARNINGS}</b>\n"
                f"⭐ Репутация: <b>{reputation[cid].get(tid,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(tid,0)}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[kb_back(tid)]))
        elif action == "fun":
            await call.message.edit_text(f"🎭 <b>Приколы над {tname}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_fun(tid))
        elif action == "messages":
            await call.message.edit_text("💬 <b>Действия с сообщениями</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_messages(tid))
        elif action == "members":
            await call.message.edit_text("👥 <b>Управление участниками</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_members(tid))
        elif action == "chat":
            await call.message.edit_text(
                f"🔧 <b>Управление чатом</b>\n\n"
                f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_chat(tid))
        elif action == "games":
            await call.message.edit_text("🎮 <b>Игры и команды</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_games(tid))
        elif action == "botstats2":
            total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
            total_warns = sum(sum(v.values()) for v in warnings.values())
            await call.message.edit_text(
                f"📊 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
                f"⚠️ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
                f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
                f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
                f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
    except Exception as e:
        await call.answer(f"❗ Ошибка: {e}", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    _, tid_str, tval = call.data.split(":")
    tid = int(tid_str); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if tval == "custom":
        pending[call.from_user.id] = {"action":"mute_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(
            f"✏️ Введи время мута для <b>{tname}</b>:\n"
            f"Примеры: <code>10</code>, <code>30m</code>, <code>2h</code>, <code>1d</code>",
            parse_mode="HTML")
        await call.answer(); return
    mins  = int(tval)
    label = f"{mins} мин." if mins < 60 else (f"{mins//60} ч." if mins < 1440 else f"{mins//1440} дн.")
    await bot.restrict_chat_member(cid, tid,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    await call.message.edit_text(
        random.choice(MUTE_MESSAGES).format(name=f"<b>{tname}</b>", time=label), parse_mode="HTML")
    await log_action(f"🔇 <b>МУТ</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nВремя: {label}\nЧат: {call.message.chat.title}")
    await call.answer(f"Замутен на {label}!")

@dp.callback_query(F.data.startswith("warn:"))
async def cb_warn(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"warn_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину варна для <b>{tname}</b>:", parse_mode="HTML")
        await call.answer(); return
    warnings[cid][tid] += 1; count = warnings[cid][tid]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, tid); warnings[cid][tid] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{tname}</b>", max=MAX_WARNINGS)
        await log_action(f"🔨 <b>АВТОБАН</b>\nКого: <b>{tname}</b>\nПричина: {MAX_WARNINGS} варнов\nЧат: {call.message.chat.title}")
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=f"<b>{tname}</b>", count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"⚠️ <b>ВАРН</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nПричина: {reason}\nЧат: {call.message.chat.title}")
    await call.message.edit_text(msg, parse_mode="HTML")
    await call.answer("Варн выдан!")

@dp.callback_query(F.data.startswith("ban:"))
async def cb_ban(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"ban_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину бана для <b>{tname}</b>:", parse_mode="HTML")
        await call.answer(); return
    if reason == "tempban24":
        await bot.ban_chat_member(cid, tid, until_date=timedelta(hours=24))
        await call.message.edit_text(f"⏰ <b>{tname}</b> забанен на <b>24 часа</b>.", parse_mode="HTML")
        await log_action(f"⏰ <b>БАН 24ч</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nЧат: {call.message.chat.title}")
        await call.answer(); return
    await bot.ban_chat_member(cid, tid)
    await call.message.edit_text(
        random.choice(BAN_MESSAGES).format(name=f"<b>{tname}</b>", reason=reason), parse_mode="HTML")
    await log_action(f"🔨 <b>БАН</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nПричина: {reason}\nЧат: {call.message.chat.title}")
    await call.answer("Забанен!")

@dp.callback_query(F.data.startswith("fun:"))
async def cb_fun(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); mention = tm.user.mention_html()
    except: mention = f"<code>{tid}</code>"
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё раз", callback_data=call.data)], kb_back(tid)])
    if action == "rban":
        await call.message.edit_text(
            f"🎲 {mention} получил <b>шуточный бан</b>!\n"
            f"📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет)</i>",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "iq":
        iq = random.randint(1, 200)
        c  = "🥔 Картошка умнее." if iq<70 else ("🐒 Обезьяна лучше." if iq<100 else ("😐 Сойдёт." if iq<130 else ("🧠 Умный!" if iq<160 else "🤖 Настоящий гений!")))
        await call.message.edit_text(f"🧠 IQ {mention}: <b>{iq}</b>\n{c}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "gay":
        p = random.randint(0, 100)
        await call.message.edit_text(
            f"🏳️‍🌈 {mention}\n{'🌈'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка 😄",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "compliment":
        await call.message.edit_text(
            f"💐 <b>{mention}:</b>\n\n{random.choice(COMPLIMENTS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "predict":
        await call.message.edit_text(
            f"🔮 <b>Предсказание для {mention}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "horoscope":
        sign, text = random.choice(list(HOROSCOPES.items()))
        await call.message.edit_text(
            f"{sign} <b>Гороскоп для {mention}:</b>\n\n{text}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "rate":
        score = random.randint(0, 10)
        await call.message.edit_text(
            f"📊 Оценка {mention}:\n{'⭐'*score+'☆'*(10-score)}\n<b>{score}/10</b>",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "truth":
        await call.message.edit_text(
            f"🤔 <b>Вопрос для {mention}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "dare":
        await call.message.edit_text(
            f"😈 <b>Задание для {mention}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "wyr":
        await call.message.edit_text(
            f"🤯 <b>Выбор для {mention}:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML", reply_markup=back_kb)
    await call.answer()

@dp.callback_query(F.data.startswith("msg:"))
async def cb_msg(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    if action == "pin":
        try: await bot.pin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📌 Закреплено!", show_alert=True)
        except: await call.answer("❗ Открой панель реплаем на нужное сообщение.", show_alert=True)
    elif action == "unpin":
        try: await bot.unpin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📌 Откреплено!", show_alert=True)
        except: await call.answer("❗ Нет сообщения.", show_alert=True)
    elif action == "del":
        try: await call.message.reply_to_message.delete(); await call.answer("🗑 Удалено!", show_alert=True)
        except: await call.answer("❗ Нет сообщения.", show_alert=True)
    elif action.startswith("clear"):
        n = int(action.replace("clear",""))
        deleted = 0
        for i in range(call.message.message_id, call.message.message_id - n - 1, -1):
            try: await bot.delete_message(cid, i); deleted += 1
            except: pass
        await call.answer(f"🧹 Удалено {deleted} сообщений!", show_alert=True)
    elif action == "announce":
        pending[call.from_user.id] = {"action":"announce_text","target_id":0,"target_name":"","chat_id":cid}
        await call.message.edit_text("📢 Напиши текст объявления:\n<i>(следующее сообщение станет объявлением)</i>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    elif action == "poll":
        pending[call.from_user.id] = {"action":"poll_text","target_id":0,"target_name":"","chat_id":cid}
        await call.message.edit_text("📊 Напиши голосование:\n<code>Вопрос|Вариант 1|Вариант 2</code>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    try: await call.message.edit_text("💬 <b>Действия с сообщениями</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_messages(tid))
    except: pass
    await call.answer()

@dp.callback_query(F.data.startswith("members:"))
async def cb_members(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid) if tid != 0 else None; tname = tm.user.full_name if tm else "участник"
    except: tname = f"ID {tid}"
    if action == "adminlist":
        admins = await bot.get_chat_administrators(cid)
        lines = ["👮 <b>Администраторы чата:</b>\n"]
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
        if not stats: await call.answer("📊 Статистика пуста!", show_alert=True)
        else:
            sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
            medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
            lines    = ["📊 <b>Топ активных:</b>\n"]
            for i, (uid, cnt) in enumerate(sorted_u):
                try: m = await bot.get_chat_member(cid, uid); uname = m.user.full_name
                except: uname = f"ID {uid}"
                lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщ.")
            await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
    elif action == "warn24":
        if tid != 0:
            await bot.restrict_chat_member(cid, tid,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
            await call.message.edit_text(
                f"📵 <b>{tname}</b> — мут на <b>24 часа</b> за рекламу.", parse_mode="HTML")
            await log_action(f"📵 <b>МУТ 24ч</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nЧат: {call.message.chat.title}")
        else: await call.answer("❗ Открой панель реплаем на участника.", show_alert=True)
    elif action == "warninfo":
        if tid != 0:
            count = warnings[cid].get(tid, 0)
            await call.message.edit_text(
                f"⚠️ Варнов у <b>{tname}</b>: <b>{count}/{MAX_WARNINGS}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        else: await call.answer("❗ Открой панель реплаем на участника.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("chat:"))
async def cb_chat(call: CallbackQuery):
    global ANTI_MAT_ENABLED, AUTO_KICK_BOTS
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); cid = call.message.chat.id
    if parts[1] == "slow":
        delay = int(parts[2]); tid = int(parts[3])
        await bot.set_chat_slow_mode_delay(cid, delay)
        label = f"Slowmode {delay}с включён!" if delay > 0 else "Slowmode выключен!"
        await call.answer(f"🐢 {label}", show_alert=True)
        await call.message.edit_text(
            f"🔧 <b>Управление чатом</b>\n\n"
            f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
            parse_mode="HTML", reply_markup=kb_chat(tid))
        return
    action = parts[1]; tid = int(parts[2])
    if action == "lock":
        await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
        await call.answer("🔒 Чат заблокирован!", show_alert=True)
    elif action == "unlock":
        await bot.set_chat_permissions(cid, ChatPermissions(
            can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
            can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
        await call.answer("🔓 Чат разблокирован!", show_alert=True)
    elif action == "antimat":
        ANTI_MAT_ENABLED = not ANTI_MAT_ENABLED
        await call.answer(f"🧼 Антимат {'✅ включён' if ANTI_MAT_ENABLED else '❌ выключен'}!", show_alert=True)
    elif action == "autokick":
        AUTO_KICK_BOTS = not AUTO_KICK_BOTS
        await call.answer(f"🤖 Автокик {'✅ включён' if AUTO_KICK_BOTS else '❌ выключен'}!", show_alert=True)
    elif action == "rules":
        await call.message.edit_text(RULES_TEXT, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    elif action == "botstats":
        total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
        total_warns = sum(sum(v.values()) for v in warnings.values())
        await call.message.edit_text(
            f"📊 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
            f"⚠️ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
            f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
            f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
            f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    await call.message.edit_text(
        f"🔧 <b>Управление чатом</b>\n\n"
        f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
        f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
        parse_mode="HTML", reply_markup=kb_chat(tid))
    await call.answer()

@dp.callback_query(F.data.startswith("game:"))
async def cb_game(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":", 2); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё раз", callback_data=call.data)],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]])
    if action == "roll":
        await call.message.edit_text(
            f"🎲 Бросаю d6... выпало: <b>{random.randint(1,6)}</b>!", parse_mode="HTML", reply_markup=back_kb)
    elif action == "flip":
        await call.message.edit_text(random.choice(["🪙 Орёл!", "🪙 Решка!"]), reply_markup=back_kb)
    elif action == "slot":
        symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
        s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
        if s1==s2==s3=="💎":              res = "💰 ДЖЕКПОТ!!"
        elif s1==s2==s3:                  res = f"🎉 Три {s1}! Выиграл!"
        elif s1==s2 or s2==s3 or s1==s3:  res = "😐 Два одинаковых. Почти!"
        else:                             res = "😢 Не повезло. Попробуй ещё!"
        await call.message.edit_text(f"🎰 [ {s1} | {s2} | {s3} ]\n\n{res}", reply_markup=back_kb)
    elif action == "8ball":
        await call.message.edit_text(
            f"🎱 <b>Ответ шара:</b>\n\n{random.choice(BALL_ANSWERS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action.startswith("rps_"):
        mp = {"k":("к","🪨 Камень"),"n":("н","✂️ Ножницы"),"b":("б","📄 Бумага")}
        wins = {"к":"н","н":"б","б":"к"}
        key = action.split("_")[1]; pk,pl = mp[key]; bk,bl = random.choice(list(mp.values()))
        res = "🤝 Ничья!" if pk==bk else ("🎉 Ты выиграл!" if wins[pk]==bk else "😈 Я выиграл!")
        await call.message.edit_text(f"Ты: {pl}\nЯ: {bl}\n\n{res}", reply_markup=back_kb)
    elif action == "quote":
        await call.message.edit_text(f"💬 {random.choice(QUOTES)}", reply_markup=back_kb)
    elif action.startswith("weather_"):
        city = action.replace("weather_","")
        if city == "custom":
            pending[call.from_user.id] = {"action":"weather_city","target_id":0,"target_name":"","chat_id":cid}
            await call.message.edit_text("🌤 Напиши название города:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
        await call.message.edit_text(await get_weather(city), parse_mode="HTML", reply_markup=back_kb)
    elif action.startswith("countdown"):
        n = int(action.replace("countdown",""))
        await call.message.edit_text(f"⏱ <b>{n}...</b>", parse_mode="HTML")
        for i in range(n-1, 0, -1):
            await asyncio.sleep(1)
            try: await call.message.edit_text(f"⏱ <b>{i}...</b>", parse_mode="HTML")
            except: pass
        await asyncio.sleep(1)
        await call.message.edit_text("🚀 <b>ПОЕХАЛИ!</b>", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    await call.answer()

# ═══════════════════════════════════════════
#      КОМАНДЫ — ОСНОВНЫЕ
# ═══════════════════════════════════════════
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "Я бот-модератор этого чата.\n"
        "📜 /rules — правила\n"
        "❓ /help — все команды\n"
        "🛠 /panel — панель управления (реплай на участника)",
        parse_mode="HTML")

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    await message.reply_photo(
        photo=FSInputFile("welcome.jpg"),
        caption="📜 <b>Правила чата</b>\n\nНажми кнопку ниже чтобы прочитать правила:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Правила чата", url="https://telegra.ph/Pravila-anon-chata-03-03-2")]
        ])
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_adm = await check_admin(message)
    text = (
        "❓ <b>Команды для всех:</b>\n"
        "/rules — правила чата\n"
        "/weather [город] — погода\n"
        "/rep — репутация (реплай)\n"
        "/top — топ активных\n"
        "/afk [причина] — уйти в AFK\n"
        "/info — инфо о пользователе (реплай)\n"
        "/warnings — варны пользователя (реплай)\n\n"
        "🎮 <b>Игры:</b>\n"
        "/roll [N] — кубик\n"
        "/flip — монетка\n"
        "/8ball [вопрос] — шар предсказаний\n"
        "/rate [что] — оценить\n"
        "/iq — проверить IQ\n"
        "/gay — шуточный %\n"
        "/quote — цитата дня\n"
        "/truth — вопрос правды\n"
        "/dare — задание смелости\n"
        "/wyr — выбор без выбора\n"
        "/rps к/н/б — камень ножницы бумага\n"
        "/slot — однорукий бандит\n"
        "/choose вар1|вар2 — случайный выбор\n"
        "/horoscope — гороскоп\n"
        "/predict — предсказание\n"
        "/compliment — комплимент\n"
        "/note get/list — заметки\n"
    )
    if is_adm:
        text += (
            "\n👮 <b>Только для администраторов:</b>\n"
            "/panel — панель управления (реплай)\n"
            "/ban [причина] — бан\n"
            "/unban — разбан\n"
            "/mute [время] — мут (10m / 2h / 1d)\n"
            "/unmute — размут\n"
            "/warn [причина] — варн\n"
            "/unwarn — снять варн\n"
            "/del — удалить сообщение\n"
            "/clear [N] — очистить N сообщений\n"
            "/announce [текст] — объявление\n"
            "/pin — закрепить\n"
            "/unpin — открепить\n"
            "/lock — заблокировать чат\n"
            "/unlock — разблокировать\n"
            "/slowmode [сек] — медленный режим\n"
            "/promote [тег] — выдать тег участнику\n"
            "/poll Вопрос|Вар1|Вар2 — голосование\n"
            "/antimat on/off — антимат\n"
            "/autokick on/off — автокик ботов\n"
            "/warn24 — мут 24ч за рекламу\n"
            "/adminlist — список администраторов\n"
            "/rban — шуточный бан\n"
            "/countdown [N] — обратный отсчёт\n"
            "/note set/del — управление заметками\n"
        )
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
            f"💬 Сообщений: <b>{msgs}</b>\n\nВыбери действие:",
            parse_mode="HTML", reply_markup=kb_user_panel(target.id))
    else:
        total_msgs  = sum(chat_stats[message.chat.id].values())
        total_warns = sum(warnings[message.chat.id].values())
        await message.reply(
            f"🛠 <b>Панель управления</b>\n\n"
            f"💬 Чат: <b>{message.chat.title}</b>\n"
            f"📊 Сообщений: <b>{total_msgs}</b>\n"
            f"⚠️ Варнов: <b>{total_warns}</b>\n"
            f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери раздел:",
            parse_mode="HTML", reply_markup=kb_main_menu())

# ═══════════════════════════════════════════
#      КОМАНДЫ МОДЕРАЦИИ
# ═══════════════════════════════════════════
@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await message.reply("🚫 Нельзя забанить администратора!"); return
    reason = command.args or "Нарушение правил"
    await bot.ban_chat_member(message.chat.id, target.id)
    await message.reply(random.choice(BAN_MESSAGES).format(name=target.mention_html(), reason=reason), parse_mode="HTML")
    await log_action(f"🔨 <b>БАН</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nПричина: {reason}\nЧат: {message.chat.title}")

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await message.reply(f"♻️ {target.mention_html()} разбанен.", parse_mode="HTML")
    await log_action(f"♻️ <b>РАЗБАН</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await message.reply("🚫 Нельзя замутить администратора!"); return
    mins, label = parse_duration(command.args or "60m")
    if not mins: mins = 60; label = "1 ч."
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    await message.reply(random.choice(MUTE_MESSAGES).format(name=target.mention_html(), time=label), parse_mode="HTML")
    await log_action(f"🔇 <b>МУТ</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nВремя: {label}\nЧат: {message.chat.title}")

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    await message.reply(f"🔊 {target.mention_html()} размучен.", parse_mode="HTML")
    await log_action(f"🔊 <b>РАЗМУТ</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("warn"))
async def cmd_warn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await message.reply("🚫 Нельзя выдать варн администратору!"); return
    reason = command.args or "Нарушение правил"
    cid = message.chat.id
    warnings[cid][target.id] += 1; count = warnings[cid][target.id]
    save_data()
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, target.id); warnings[cid][target.id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=target.mention_html(), max=MAX_WARNINGS)
        await log_action(f"🔨 <b>АВТОБАН</b>\nКого: {target.mention_html()}\nПричина: {MAX_WARNINGS} варнов\nЧат: {message.chat.title}")
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"⚠️ <b>ВАРН</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nПричина: {reason}\nЧат: {message.chat.title}")
    await message.reply(msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def cmd_unwarn(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1
    await message.reply(
        f"✅ С {target.mention_html()} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

@dp.message(Command("del"))
async def cmd_del(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    await message.reply_to_message.delete(); await message.delete()

@dp.message(Command("clear"))
async def cmd_clear(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError: await message.reply("❗ /clear 20"); return
    deleted = 0
    for i in range(message.message_id, message.message_id - count - 1, -1):
        try: await bot.delete_message(message.chat.id, i); deleted += 1
        except: pass
    sent = await message.answer(f"🧹 Удалено: <b>{deleted}</b> сообщений.", parse_mode="HTML")
    await asyncio.sleep(3)
    try: await sent.delete()
    except: pass

@dp.message(Command("announce"))
async def cmd_announce(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        pending[message.from_user.id] = {"action":"announce_text","target_id":0,"target_name":"","chat_id":message.chat.id}
        await message.reply("📢 Напиши текст объявления:"); return
    await message.delete()
    await message.answer(
        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}", parse_mode="HTML")

@dp.message(Command("pin"))
async def cmd_pin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    await bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Закреплено!")

@dp.message(Command("unpin"))
async def cmd_unpin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Откреплено!")

@dp.message(Command("lock"))
async def cmd_lock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await message.reply("🔒 Чат <b>заблокирован</b>.", parse_mode="HTML")

@dp.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(
        can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
        can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
    await message.reply("🔓 Чат <b>разблокирован</b>.", parse_mode="HTML")

@dp.message(Command("slowmode"))
async def cmd_slowmode(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: delay = int(command.args) if command.args else 10
    except ValueError: await message.reply("❗ /slowmode 30"); return
    await bot.set_chat_slow_mode_delay(message.chat.id, delay)
    if delay == 0: await message.reply("🐇 Slowmode выключен.")
    else:
        label = f"{delay} сек." if delay < 60 else f"{delay//60} мин."
        await message.reply(f"🐢 Slowmode: <b>{label}</b>", parse_mode="HTML")

@dp.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    title = command.args or "Участник"; target = message.reply_to_message.from_user
    await bot.set_chat_administrator_custom_title(message.chat.id, target.id, title)
    await message.reply(f"🏅 {target.mention_html()} получил тег: <b>{title}</b>", parse_mode="HTML")

@dp.message(Command("poll"))
async def cmd_poll(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args or "|" not in command.args:
        await message.reply("❗ /poll Вопрос|Вар1|Вар2"); return
    parts = [p.strip() for p in command.args.split("|")]
    if len(parts) < 3: await message.reply("❗ Нужно минимум 2 варианта."); return
    await message.delete()
    await bot.send_poll(message.chat.id, question=parts[0], options=parts[1:], is_anonymous=False)

@dp.message(Command("antimat"))
async def cmd_antimat(message: Message, command: CommandObject):
    global ANTI_MAT_ENABLED
    if not await require_admin(message): return
    if not command.args:
        await message.reply(f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on": ANTI_MAT_ENABLED = True; await message.reply("🧼 Антимат <b>включён</b>.", parse_mode="HTML")
    elif a == "off": ANTI_MAT_ENABLED = False; await message.reply("🔞 Антимат <b>выключен</b>.", parse_mode="HTML")

@dp.message(Command("autokick"))
async def cmd_autokick(message: Message, command: CommandObject):
    global AUTO_KICK_BOTS
    if not await require_admin(message): return
    if not command.args:
        await message.reply(f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on": AUTO_KICK_BOTS = True; await message.reply("🤖 Автокик <b>включён</b>.", parse_mode="HTML")
    elif a == "off": AUTO_KICK_BOTS = False; await message.reply("🤖 Автокик <b>выключен</b>.", parse_mode="HTML")

@dp.message(Command("warn24"))
async def cmd_warn24(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await message.reply("🚫 Нельзя замутить администратора!"); return
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
    await message.reply(f"📵 {target.mention_html()} замучен на <b>24 часа</b> за рекламу.", parse_mode="HTML")
    await log_action(f"📵 <b>МУТ 24ч</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("rban"))
async def cmd_rban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await message.reply(
        f"🎲 {target.mention_html()} получил <b>шуточный бан</b>!\n"
        f"📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет)</i>",
        parse_mode="HTML")

@dp.message(Command("adminlist"))
async def cmd_adminlist(message: Message):
    if not await require_admin(message): return
    admins = await bot.get_chat_administrators(message.chat.id)
    lines  = ["👮 <b>Администраторы чата:</b>\n"]
    for adm in admins:
        if adm.user.is_bot: continue
        icon  = "👑" if adm.status == "creator" else "🛡"
        title = f" — <i>{adm.custom_title}</i>" if hasattr(adm,"custom_title") and adm.custom_title else ""
        lines.append(f"{icon} {adm.user.mention_html()}{title}")
    await message.reply("\n".join(lines), parse_mode="HTML")

@dp.message(Command("note"))
async def cmd_note(message: Message, command: CommandObject):
    if not command.args: await message.reply("📝 /note set/get/del/list [имя] [текст]"); return
    parts = command.args.split(maxsplit=2); action = parts[0].lower(); cid = message.chat.id
    if action == "set":
        if not await require_admin(message): return
        if len(parts) < 3: await message.reply("❗ /note set [имя] [текст]"); return
        notes[cid][parts[1]] = parts[2]
        await message.reply(f"✅ Заметка <b>{parts[1]}</b> сохранена!", parse_mode="HTML")
    elif action == "get":
        if len(parts) < 2: await message.reply("❗ /note get [имя]"); return
        t = notes[cid].get(parts[1])
        await message.reply(f"📝 <b>{parts[1]}:</b>\n{t}" if t else "❌ Заметка не найдена.", parse_mode="HTML")
    elif action == "del":
        if not await require_admin(message): return
        if len(parts) > 1 and parts[1] in notes[cid]:
            del notes[cid][parts[1]]; await message.reply(f"🗑 Заметка <b>{parts[1]}</b> удалена.", parse_mode="HTML")
        else: await message.reply("❌ Не найдена.")
    elif action == "list":
        keys = list(notes[cid].keys())
        await message.reply("📋 <b>Заметки:</b>\n" + "\n".join(f"• {k}" for k in keys) if keys else "📭 Заметок нет.", parse_mode="HTML")

@dp.message(Command("birthday"))
async def cmd_birthday(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("🎂 Формат: /birthday ДД.ММ\nПример: <code>/birthday 25.03</code>", parse_mode="HTML"); return
    try:
        day, month = map(int, command.args.strip().split("."))
        if not (1 <= day <= 31 and 1 <= month <= 12): raise ValueError
    except:
        await message.reply("❗ Неверный формат. Пример: /birthday 25.03"); return
    uid = message.from_user.id
    birthdays[uid] = {"day": day, "month": month, "name": message.from_user.full_name, "chat_id": message.chat.id}
    await message.reply(
        f"🎂 {message.from_user.mention_html()}, день рождения <b>{day:02d}.{month:02d}</b> сохранён!\n🎉 Поздравлю тебя в этот день!",
        parse_mode="HTML")

async def birthday_checker():
    while True:
        from datetime import datetime
        today = datetime.now()
        for uid, data in list(birthdays.items()):
            if data["day"] == today.day and data["month"] == today.month:
                try:
                    await bot.send_message(data["chat_id"],
                        f"🎉🎂 Сегодня день рождения у <a href='tg://user?id={uid}'>{data['name']}</a>!\n\n🎊 Поздравляем! 🥳",
                        parse_mode="HTML")
                except: pass
        await asyncio.sleep(3600)

@dp.message(Command("remind"))
async def cmd_remind(message: Message, command: CommandObject):
    if not command.args or len(command.args.split(maxsplit=1)) < 2:
        await message.reply(
            "⏰ Формат: /remind 30m текст\n"
            "<code>/remind 10m Написать другу</code>\n"
            "<code>/remind 2h Встреча</code>", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    mins, label = parse_duration(parts[0])
    if not mins: await message.reply("❗ Неверный формат времени. Примеры: 10m, 2h, 1d"); return
    text = parts[1].strip()
    cid = message.chat.id
    await message.reply(f"⏰ Напомню через <b>{label}</b>!\n📝 {text}", parse_mode="HTML")
    async def remind_task():
        await asyncio.sleep(mins * 60)
        try:
            await bot.send_message(cid,
                f"⏰ {message.from_user.mention_html()}, напоминание!\n\n📝 <b>{text}</b>", parse_mode="HTML")
        except: pass
    asyncio.create_task(remind_task())

@dp.message(Command("countdown"))
async def cmd_countdown(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: n = min(int(command.args or 5), 10)
    except: n = 5
    sent = await message.reply(f"⏱ <b>{n}...</b>", parse_mode="HTML")
    for i in range(n-1, 0, -1):
        await asyncio.sleep(1)
        try: await sent.edit_text(f"⏱ <b>{i}...</b>", parse_mode="HTML")
        except: pass
    await asyncio.sleep(1)
    await sent.edit_text("🚀 <b>ПОЕХАЛИ!</b>", parse_mode="HTML")

# ═══════════════════════════════════════════
#         ПОГОДА / АФК / ИНФО
# ═══════════════════════════════════════════
@dp.message(Command("weather"))
async def cmd_weather(message: Message, command: CommandObject):
    if not command.args: await message.reply("🌤 Укажи город: /weather Москва"); return
    wait = await message.reply("⏳ Получаю данные...")
    await wait.edit_text(await get_weather(command.args), parse_mode="HTML")

@dp.message(Command("afk"))
async def cmd_afk(message: Message, command: CommandObject):
    reason = command.args or "без причины"
    afk_users[message.from_user.id] = reason
    await message.reply(f"😴 {message.from_user.mention_html()} ушёл в AFK: {reason}", parse_mode="HTML")

@dp.message(Command("info"))
async def cmd_info(message: Message):
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    user = message.reply_to_message.from_user
    member = await bot.get_chat_member(message.chat.id, user.id)
    smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор","member":"👤 Участник",
            "restricted":"🔇 Ограничен","kicked":"🔨 Забанен","left":"🚶 Вышел"}
    afk  = f"\n😴 AFK: {afk_users[user.id]}" if user.id in afk_users else ""
    await message.reply(
        f"👤 <b>Инфо:</b>\n{user.mention_html()}{afk}\n"
        f"🔗 {'@'+user.username if user.username else 'нет'}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"📌 {smap.get(member.status, member.status)}\n"
        f"⚠️ Варнов: <b>{warnings[message.chat.id].get(user.id,0)}/{MAX_WARNINGS}</b>\n"
        f"⭐ Репутация: <b>{reputation[message.chat.id].get(user.id,0):+d}</b>\n"
        f"💬 Сообщений: <b>{chat_stats[message.chat.id].get(user.id,0)}</b>",
        parse_mode="HTML")

@dp.message(Command("warnings"))
async def cmd_warnings(message: Message):
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await message.reply(
        f"📊 {target.mention_html()} — варнов: <b>{warnings[message.chat.id].get(target.id,0)}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

# ═══════════════════════════════════════════
#         РЕПУТАЦИЯ
# ═══════════════════════════════════════════
@dp.message(Command("rep"))
async def cmd_rep(message: Message):
    if not message.reply_to_message: await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    score  = reputation[message.chat.id][target.id]
    await message.reply(
        f"{'⭐' if score>=0 else '💀'} Репутация {target.mention_html()}: <b>{score:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"+1", "+", "👍"}))
async def rep_plus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await message.reply("😏 Себе репу не накручивай!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await message.reply(f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин."); return
    rep_cooldown[key] = now; reputation[message.chat.id][target.id] += 1
    save_data()
    await message.reply(
        f"⬆️ {target.mention_html()} +1 к репутации! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"-1", "👎"}))
async def rep_minus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await message.reply("😏 Себе репу не снижай!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await message.reply(f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин."); return
    rep_cooldown[key] = now; reputation[message.chat.id][target.id] -= 1
    save_data()
    await message.reply(
        f"⬇️ {target.mention_html()} -1 к репутации! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>",
        parse_mode="HTML")

@dp.message(Command("ранг"))
async def cmd_rank(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid, cid = user.id, message.chat.id
    xp = xp_data[cid][uid]; lvl = levels[cid][uid]
    xp_current = xp % 100
    bar = "█" * (xp_current // 10) + "░" * (10 - xp_current // 10)
    title = (
        "👑 Элита" if lvl >= 20 else "🏆 Легенда" if lvl >= 10 else
        "⭐ Ветеран" if lvl >= 5 else "💪 Активный" if lvl >= 3 else
        "👤 Участник" if lvl >= 1 else "🌱 Новичок")
    await message.reply(
        f"📊 <b>Уровень {user.mention_html()}</b>\n\n"
        f"🏅 Титул: <b>{title}</b>\n⚡ Уровень: <b>{lvl}</b>\n"
        f"✨ Опыт: <b>{xp_current}/100</b>\n[{bar}]", parse_mode="HTML")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats: await message.reply("📊 Статистика пока пуста!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = ["📊 <b>Топ активных участников:</b>\n"]
    for i, (uid, count) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {count} сообщений")
    await message.reply("\n".join(lines), parse_mode="HTML")

# ═══════════════════════════════════════════
#         ИГРЫ
# ═══════════════════════════════════════════
@dp.message(Command("quote"))
async def cmd_quote(message: Message):
    await message.reply(f"💬 {random.choice(QUOTES)}")

@dp.message(Command("roll"))
async def cmd_roll(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 10000))
    except: sides = 6
    await message.reply(f"🎲 Бросаю d{sides}... выпало: <b>{random.randint(1,sides)}</b>!", parse_mode="HTML")

@dp.message(Command("flip"))
async def cmd_flip(message: Message):
    await message.reply(random.choice(["🪙 Орёл!", "🪙 Решка!"]))

@dp.message(Command("8ball"))
async def cmd_8ball(message: Message, command: CommandObject):
    if not command.args: await message.reply("❓ /8ball [вопрос]"); return
    await message.reply(
        f"🎱 <b>Вопрос:</b> {command.args}\n\n<b>Ответ:</b> {random.choice(BALL_ANSWERS)}", parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message, command: CommandObject):
    if not command.args: await message.reply("❗ /rate [что]"); return
    score = random.randint(0, 10)
    await message.reply(
        f"📊 <b>{command.args}</b>\n{'⭐'*score+'☆'*(10-score)}\nОценка: <b>{score}/10</b>", parse_mode="HTML")

@dp.message(Command("iq"))
async def cmd_iq(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    iq   = random.randint(1, 200)
    c    = "🥔 Картошка умнее." if iq<70 else ("🐒 Обезьяна лучше." if iq<100 else ("😐 Сойдёт." if iq<130 else ("🧠 Умный!" if iq<160 else "🤖 Настоящий гений!")))
    await message.reply(f"🧠 IQ {user.mention_html()}: <b>{iq}</b>\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def cmd_gay(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    p    = random.randint(0, 100)
    await message.reply(
        f"🏳️‍🌈 {user.mention_html()}\n{'🌈'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка 😄",
        parse_mode="HTML")

@dp.message(Command("truth"))
async def cmd_truth(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"🤔 <b>Вопрос для {user.mention_html()}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML")

@dp.message(Command("dare"))
async def cmd_dare(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"😈 <b>Задание для {user.mention_html()}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML")

@dp.message(Command("wyr"))
async def cmd_wyr(message: Message):
    await message.reply(f"🤯 <b>Выбор без выбора:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML")

@dp.message(Command("rps"))
async def cmd_rps(message: Message, command: CommandObject):
    choices = {"к":"🪨 Камень","н":"✂️ Ножницы","б":"📄 Бумага"}
    wins    = {"к":"н","н":"б","б":"к"}
    if not command.args or command.args.lower() not in choices:
        await message.reply("✂️ /rps к — камень, /rps н — ножницы, /rps б — бумага"); return
    p = command.args.lower(); b = random.choice(list(choices.keys()))
    res = "🤝 Ничья!" if p==b else ("🎉 Ты выиграл!" if wins[p]==b else "😈 Я выиграл!")
    await message.reply(f"Ты: {choices[p]}\nЯ: {choices[b]}\n\n{res}")

@dp.message(Command("slot"))
async def cmd_slot(message: Message):
    symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
    s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
    if s1==s2==s3=="💎":              res = "💰 ДЖЕКПОТ!! Три бриллианта!"
    elif s1==s2==s3:                  res = f"🎉 Три {s1}! Выиграл!"
    elif s1==s2 or s2==s3 or s1==s3:  res = "😐 Два одинаковых. Почти!"
    else:                             res = "😢 Не повезло. Попробуй ещё!"
    await message.reply(f"🎰 [ {s1} | {s2} | {s3} ]\n\n{res}")

@dp.message(Command("choose"))
async def cmd_choose(message: Message, command: CommandObject):
    if not command.args or "|" not in command.args:
        await message.reply("❗ /choose вар1|вар2|вар3"); return
    options = [o.strip() for o in command.args.split("|") if o.strip()]
    if len(options) < 2: await message.reply("❗ Минимум 2 варианта."); return
    await message.reply(f"🎯 Выбираю... ✅ <b>{random.choice(options)}</b>!", parse_mode="HTML")

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    sign, text = random.choice(list(HOROSCOPES.items()))
    await message.reply(f"{sign} <b>Гороскоп для {user.mention_html()}:</b>\n\n{text}", parse_mode="HTML")

@dp.message(Command("predict"))
async def cmd_predict(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"🔮 <b>Предсказание для {user.mention_html()}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML")

@dp.message(Command("совместимость"))
async def cmd_compatibility(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение участника!"); return
    user1 = message.from_user; user2 = message.reply_to_message.from_user
    if user1.id == user2.id: await message.reply("😏 Сам с собой? Интересно..."); return
    percent = (user1.id * user2.id) % 101
    bar = "❤️" * (percent // 10) + "🖤" * (10 - percent // 10)
    if percent >= 80:   verdict = "💍 Идеальная пара! Женитесь!"
    elif percent >= 60: verdict = "💕 Хорошая совместимость!"
    elif percent >= 40: verdict = "😊 Неплохо, есть шанс!"
    elif percent >= 20: verdict = "😬 Сложно, но возможно..."
    else:               verdict = "💔 Катастрофа! Держитесь подальше!"
    await message.reply(
        f"💘 <b>Совместимость:</b>\n\n👤 {user1.mention_html()}\n💞 {bar}\n👤 {user2.mention_html()}\n\n<b>{percent}%</b> — {verdict}",
        parse_mode="HTML")

@dp.message(Command("compliment"))
async def cmd_compliment(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(f"💐 {user.mention_html()}, {random.choice(COMPLIMENTS)}", parse_mode="HTML")

@dp.message(Command("botstats"))
async def cmd_botstats(message: Message):
    if not await require_admin(message): return
    total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
    total_warns = sum(sum(v.values()) for v in warnings.values())
    await message.reply(
        f"📊 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
        f"⚠️ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
        f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
        f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
        f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML")

# ═══════════════════════════════════════════
#         АУТИСТ КОМАНДЫ
# ═══════════════════════════════════════════
@dp.message(F.text & ~F.text.startswith("/"))
async def autist_commands(message: Message):
    if not message.text: return
    text_lower = message.text.strip().lower()
    if not text_lower.startswith("аутист"): return
    fun_only = ["обозвать", "поженить", "казнить", "диагноз", "профессия", "похитить", "дуэль"]
    is_admin = await check_admin(message)
    is_fun = any(f in text_lower for f in fun_only)
    if not is_admin and not is_fun: return
    parts = text_lower.split(maxsplit=1)
    if len(parts) < 2: return
    rest = parts[1].strip()
    action = None
    for cmd in ["снять варн","размут","разбан","варн","мут навсегда","мут","бан","захуесосить","кик",
                "очистить","удалить","закрепить","предупредить","инфо","варны","репутация",
                "обозвать","поженить","проверить","казнить","диагноз","профессия","похитить","дуэль","экзамен"]:
        if rest.startswith(cmd):
            action = cmd; rest = rest[len(cmd):].strip(); break
    if not action: return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение участника."); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    duration_mins = None; duration_label = None; reason = "Нарушение правил"
    import re
    time_match = re.match(r"^(\d+)\s*(д|ч|м)\s*", rest)
    if time_match:
        num = int(time_match.group(1)); unit = time_match.group(2)
        if unit == "д":   duration_mins = num * 1440; duration_label = f"{num} дн."
        elif unit == "ч": duration_mins = num * 60;   duration_label = f"{num} ч."
        elif unit == "м": duration_mins = num;         duration_label = f"{num} мин."
        reason_part = rest[time_match.end():].strip()
        if reason_part: reason = reason_part
    else:
        if rest.strip(): reason = rest.strip()
    tname = target.mention_html()
    try:
        if action == "бан":
            if duration_mins:
                await bot.ban_chat_member(cid, target.id, until_date=timedelta(minutes=duration_mins))
                await message.reply(f"🔨 {tname} забанен на <b>{duration_label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            else:
                await bot.ban_chat_member(cid, target.id)
                await message.reply(f"🔨 {tname} забанен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "захуесосить":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await message.reply(f"👢 {tname} захуесошен из чата!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "кик":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await message.reply(f"👢 {tname} кикнут из чата!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "мут":
            mins = duration_mins or 60; label = duration_label or "1 ч."
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            await message.reply(f"🔇 {tname} замучен на <b>{label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "мут навсегда":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(can_send_messages=False))
            await message.reply(f"🔇 {tname} замучен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "варн":
            warnings[cid][target.id] += 1; count = warnings[cid][target.id]; save_data()
            if count >= MAX_WARNINGS:
                await bot.ban_chat_member(cid, target.id); warnings[cid][target.id] = 0
                await message.reply(f"🔨 {tname} — {MAX_WARNINGS} варна, автобан!\n📝 Причина: {reason}", parse_mode="HTML")
            else:
                await message.reply(f"⚠️ {tname} получил варн <b>{count}/{MAX_WARNINGS}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action in ("снять варн", "снятьварн"):
            if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1
            await message.reply(f"✅ С {tname} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "разбан":
            await bot.unban_chat_member(cid, target.id, only_if_banned=True)
            await message.reply(f"♻️ {tname} разбанен.", parse_mode="HTML")
        elif action == "размут":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            await message.reply(f"🔊 {tname} размучен.", parse_mode="HTML")
        elif action == "удалить":
            try: await message.reply_to_message.delete(); await message.reply("🗑 Сообщение удалено!")
            except: await message.reply("❗ Не удалось удалить сообщение.")
        elif action == "закрепить":
            try: await bot.pin_chat_message(cid, message.reply_to_message.message_id); await message.reply("📌 Сообщение закреплено!")
            except: await message.reply("❗ Не удалось закрепить сообщение.")
        elif action == "предупредить":
            text_warn = rest.strip() or "Нарушение правил"
            await message.reply(f"⚠️ Внимание {tname}!\n📝 {text_warn}", parse_mode="HTML")
        elif action == "очистить":
            count = duration_mins or 10; deleted = 0
            for i in range(message.message_id, message.message_id - count - 1, -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            await message.reply(f"🧹 Удалено <b>{deleted}</b> сообщений!", parse_mode="HTML")
        elif action == "инфо":
            member = await bot.get_chat_member(cid, target.id)
            smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
                    "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
            username = f"@{target.username}" if target.username else "нет"
            await message.reply(
                f"👤 <b>Инфо:</b>\n{tname}\n🔗 Юзернейм: <b>{username}</b>\n"
                f"🆔 ID: <code>{target.id}</code>\n📌 {smap.get(member.status, member.status)}\n"
                f"⚠️ Варнов: <b>{warnings[cid].get(target.id,0)}/{MAX_WARNINGS}</b>\n"
                f"⭐ Репутация: <b>{reputation[cid].get(target.id,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(target.id,0)}</b>", parse_mode="HTML")
        elif action == "варны":
            count = warnings[cid].get(target.id, 0)
            await message.reply(f"⚠️ Варнов у {tname}: <b>{count}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "репутация":
            rep = reputation[cid].get(target.id, 0)
            await message.reply(f"⭐ Репутация {tname}: <b>{rep:+d}</b>", parse_mode="HTML")
        elif action == "обозвать":
            обзывалки = ["🤡 клоун","🥴 тупица","🐸 лягушка","🦆 утка","🐷 хрюша","🤪 псих",
                         "🦊 хитрая лиса","🐌 улитка","🦄 единорог","🤖 сломанный робот","🥔 картошка","🧟 зомби"]
            await message.reply(f"😂 {tname} отныне ты — <b>{random.choice(обзывалки)}</b>!", parse_mode="HTML")
        elif action == "поженить":
            await message.reply(
                f"💍 Объявляю вас мужем и женой!\n\n👰 {target.mention_html()}\n🤵 {message.from_user.mention_html()}\n\n💑 Горько! 🥂",
                parse_mode="HTML")
        elif action == "казнить":
            казни = ["🔥 сожжён на костре","⚡ поражён молнией","🐊 съеден крокодилом",
                     "🍌 подавился бананом","🚀 отправлен в космос без скафандра",
                     "🌊 утоплен в стакане воды","🐝 закусан пчёлами",
                     "🎸 заслушан до смерти Шансоном","🥄 побит ложкой","🌵 упал на кактус"]
            await message.reply(
                f"⚰️ {tname} приговорён к казни!\n💀 Способ: <b>{random.choice(казни)}</b>", parse_mode="HTML")
        elif action == "диагноз":
            диагнозы = ["🧠 Хроническая адекватность","🤡 Острый клоунизм","😴 Синдром вечного AFK",
                        "🥔 Картофельный синдром","🐒 Обезьяний рефлекс","💤 Хроническая сонливость",
                        "🌵 Колючесть характера","🤖 Роботизация мозга","🦆 Утиная походка","🌈 Радужное мышление"]
            await message.reply(f"🏥 Диагноз для {tname}:\n📋 <b>{random.choice(диагнозы)}</b>", parse_mode="HTML")
        elif action == "профессия":
            профессии = ["🤡 Профессиональный клоун","🥔 Картофелевод","🐒 Дрессировщик обезьян",
                         "🌵 Смотритель кактусов","🦆 Переводчик с утиного","🤖 Ремонтник роботов",
                         "💤 Профессиональный соня","🎸 Игрок на банджо","🌈 Художник радуг","🧠 Продавец мозгов"]
            await message.reply(f"💼 Профессия {tname}:\n<b>{random.choice(профессии)}</b>", parse_mode="HTML")
        elif action == "похитить":
            mins = duration_mins or 5
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            await message.reply(
                f"👽 {tname} похищен пришельцами на <b>{mins} мин</b>!\n🛸 Вернётся через {mins} минут...",
                parse_mode="HTML")
        elif action == "дуэль":
            challenger = message.from_user
            winner = random.choice([challenger, target])
            loser = target if winner == challenger else challenger
            await message.reply(
                f"⚔️ <b>ДУЭЛЬ!</b>\n\n🔫 {challenger.mention_html()} vs {tname}\n\n"
                f"🏆 Победитель: <b>{winner.mention_html()}</b>\n💀 Проигравший: {loser.mention_html()}",
                parse_mode="HTML")
        elif action == "экзамен":
            вопросы = ["🧠 Сколько будет 2+2*2?","🌍 Какая столица Франции?",
                       "🔢 Назови простое число от 10 до 20.","🐘 Какое животное самое большое на суше?",
                       "🌊 Какой самый глубокий океан?","🎨 Смешай красный и синий — какой цвет получится?",
                       "⚡ Кто придумал лампочку?","🦁 Царь зверей — это?",
                       "🌙 Как называется спутник Земли?","🍎 Какой фрукт упал на Ньютона?"]
            await message.reply(
                f"📝 <b>ЭКЗАМЕН для {tname}!</b>\n\n{random.choice(вопросы)}\n\n⏰ У тебя <b>30 секунд</b>!",
                parse_mode="HTML")
        elif action == "проверить":
            await message.reply(f"ℹ️ Функция проверки (капча) отключена.", parse_mode="HTML")
    except Exception as e:
        await message.reply(f"❗ Ошибка: {e}")

# ═══════════════════════════════════════════
#         ЗАПУСК
# ═══════════════════════════════════════════
async def main():
    load_data()
    asyncio.create_task(birthday_checker())
    await start_web()
    if not BOT_TOKEN: raise ValueError("BOT_TOKEN не задан в переменных окружения!")
    print("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())






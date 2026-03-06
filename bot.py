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
mod_history   = defaultdict(lambda: defaultdict(list))  # {cid: {uid: [{"action":..., "reason":..., "by":..., "time":...}]}}
warn_expiry   = defaultdict(lambda: defaultdict(list))  # {cid: {uid: [expiry_timestamp, ...]}}
mute_timers   = {}  # {(cid, uid): task} — активные задачи автоснятия мута
ban_list      = defaultdict(dict)   # {cid: {uid: {"name":..., "reason":..., "by":..., "time":..., "until":...}}}
mod_reasons   = defaultdict(lambda: defaultdict(dict))  # {cid: {uid: {"mute":..., "ban":...}}}
tempban_timers = {}  # {(cid, uid): task}
afk_users     = {}
pending       = {}
chat_stats    = defaultdict(lambda: defaultdict(int))
reputation    = defaultdict(lambda: defaultdict(int))
rep_cooldown  = {}
reminders     = {}
birthdays     = {}
levels        = defaultdict(lambda: defaultdict(int))
xp_data       = defaultdict(lambda: defaultdict(int))
streaks       = defaultdict(lambda: defaultdict(int))
streak_dates  = defaultdict(lambda: defaultdict(str))

RULES_TEXT = (
    "🌌 <b>Правила анон чата</b>\n\n"
    "🔥 <b>Контент 18+:</b>\n"
    "• Видео 18+\n"
    "• Стикеры 18+\n"
    "• Гифки 18+\n"
    "• Ссылки на порнографические сайты\n"
    "⚠️ Наказание: предупреждение. При повторном нарушении — бан.\n\n"
    "📢 <b>Реклама:</b>\n"
    "• Реклама своих социальных сетей\n"
    "🛑 Наказание: Предупреждение / Мут 30 мин / Бан на 24 часа\n\n"
    "🌿 <b>Общие правила:</b>\n"
    "• Уважайте друг друга\n"
    "• Без спама и флуда\n"
)

MUTE_MESSAGES = [
    "🔇 {name} заглушён на {time}. Тишина — это сила.",
    "🧊 {name} заморожен на {time}. Остынь немного.",
    "🌑 {name} ушёл в тень на {time}. До встречи.",
    "⛓ {name} скован на {time}. Причина понятна.",
]
BAN_MESSAGES = [
    "🔨 {name} разлетелся в пыль. Причина: {reason}",
    "🚀 {name} запущен в открытый космос. Причина: {reason}",
    "🌊 {name} смыт волной. Причина: {reason}",
    "🗡 {name} пал в бою. Причина: {reason}",
]
WARN_MESSAGES = [
    "⚡ {name} — удар молнии #{count}/{max}. Причина: {reason}.",
    "🌪 {name} — предупреждение {count}/{max}. Причина: {reason}. Следи за собой.",
    "🔮 {name} — {count}/{max} знаков судьбы. Причина: {reason}.",
]
AUTOBAN_MESSAGES = [
    "💀 {name} собрал {max} ударов и исчез навсегда.",
    "🌋 {name} — {max} предупреждений. Извержение неизбежно. Бан.",
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
    "🌟 Определённо да!", "🔥 Без сомнений!", "🌈 Скорее всего да.",
    "🌫 Трудно сказать.", "⏳ Спроси потом.", "🌀 Пока неясно.",
    "🌑 хз", "❄️ Нет.", "🪨 Определённо нет",
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
    "Ты делаешь этот чат лучше! 🌟", "Без тебя тут было бы скучнее! 🎭",
    "Ты как редкий метеорит — очень ценный! 🌠", "Интеллект зашкаливает! 🧬",
]
PREDICTIONS = [
    "🔮 Сегодня тебе повезёт! Только не трать деньги.",
    "🌩 Осторожно с незнакомцами сегодня.",
    "🌟 Звёзды говорят — ты красавчик!", "🍀 Удача на твоей стороне!",
    "🐉 Тебя ждёт необычная встреча. Возможно с котом.",
    "🛋 Лучший план на сегодня — поспать. Доверяй процессу.",
]

async def health(request):
    return web.Response(text="OK")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()

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
        await reply_auto_delete(message, "🚫 <b>Только для администраторов!</b>", parse_mode="HTML")
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

AUTO_DELETE_DELAY = 30  # секунд

async def auto_delete(*msgs):
    """Удаляет сообщения через AUTO_DELETE_DELAY секунд"""
    await asyncio.sleep(AUTO_DELETE_DELAY)
    for m in msgs:
        try:
            await m.delete()
        except:
            pass

async def reply_auto_delete(message: Message, text: str, **kwargs) -> Message:
    """Отвечает на сообщение и удаляет оба через 30 секунд"""
    sent = await message.reply(text, **kwargs)
    asyncio.create_task(auto_delete(message, sent))
    return sent

def add_mod_history(cid: int, uid: int, action: str, reason: str, by_name: str):
    """Записывает действие модератора в историю пользователя"""
    from datetime import datetime
    mod_history[cid][uid].append({
        "action": action,
        "reason": reason,
        "by": by_name,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M")
    })
    if len(mod_history[cid][uid]) > 20:
        mod_history[cid][uid] = mod_history[cid][uid][-20:]
    # 👮 Считаем статистику модератора
    mod_stats[cid][by_name] += 1

WARN_EXPIRY_DAYS = 30  # варн сгорает через 30 дней

def add_warn_with_expiry(cid: int, uid: int):
    """Добавляет варн с временем истечения"""
    expiry = time() + WARN_EXPIRY_DAYS * 86400
    warn_expiry[cid][uid].append(expiry)
    warnings[cid][uid] = len(warn_expiry[cid][uid])

def clean_expired_warns(cid: int, uid: int):
    """Удаляет истёкшие варны"""
    now = time()
    warn_expiry[cid][uid] = [e for e in warn_expiry[cid][uid] if e > now]
    warnings[cid][uid] = len(warn_expiry[cid][uid])

async def auto_unmute(cid: int, uid: int, mins: int, uname: str):
    """Автоматически снимает мут через указанное время"""
    await asyncio.sleep(mins * 60)
    try:
        await bot.restrict_chat_member(
            cid, uid,
            permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True))
        await bot.send_message(
            cid,
            f"🔊 Мут с <b>{uname}</b> снят автоматически.",
            parse_mode="HTML")
        await log_action(
            f"🔄 <b>АВТОРАЗМУТ</b>\n"
            f"👤 <b>{uname}</b>\n"
            f"⏱ Время мута истекло автоматически")
    except:
        pass
    finally:
        mute_timers.pop((cid, uid), None)

def schedule_unmute(cid: int, uid: int, mins: int, uname: str):
    """Запускает задачу автоснятия мута"""
    key = (cid, uid)
    old = mute_timers.get(key)
    if old:
        old.cancel()
    task = asyncio.create_task(auto_unmute(cid, uid, mins, uname))
    mute_timers[key] = task

async def log_violation_screenshot(cid: int, uid: int, uname: str, msg_text: str,
                                    action: str, reason: str, by_name: str, chat_title: str):
    """Сохраняет текст сообщения-нарушения в лог"""
    from datetime import datetime
    preview = msg_text[:300] + ("…" if len(msg_text) > 300 else "")
    await log_action(
        f"📸 <b>СКРИНШОТ НАРУШЕНИЯ</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"👤 Нарушитель: <b>{uname}</b> (<code>{uid}</code>)\n"
        f"⚖️ Действие: <b>{action}</b>\n"
        f"📝 Причина: <b>{reason}</b>\n"
        f"👮 Модератор: <b>{by_name}</b>\n"
        f"💬 Чат: <b>{chat_title}</b>\n"
        f"🕐 Время: <b>{datetime.now().strftime('%d.%m.%Y %H:%M')}</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"💬 <b>Текст сообщения:</b>\n<code>{preview}</code>"
    )

async def dm_warn_user(uid: int, uname: str, reason: str, chat_title: str,
                       action: str, by_name: str):
    """Отправляет личное предупреждение нарушителю в лс"""
    try:
        await bot.send_message(
            uid,
            f"⚠️ <b>Предупреждение от администрации</b>\n\n"
            f"💬 Чат: <b>{chat_title}</b>\n"
            f"⚖️ Действие: <b>{action}</b>\n"
            f"📝 Причина: <b>{reason}</b>\n"
            f"👮 Модератор: <b>{by_name}</b>\n\n"
            f"⚡ Пожалуйста, соблюдай правила чата!",
            parse_mode="HTML")
        return True
    except:
        return False  # пользователь не начал диалог с ботом

async def get_weather(city: str) -> str:
    if not WEATHER_API_KEY:
        return "🌧 Weather API ключ не настроен."
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": WEATHER_API_KEY, "units": "metric", "lang": "ru"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                d = await r.json()
                if d.get("cod") != 200: return f"🌧 Город <b>{city}</b> не найден."
                temp  = round(d["main"]["temp"]); feels = round(d["main"]["feels_like"])
                desc  = d["weather"][0]["description"].capitalize()
                humid = d["main"]["humidity"]; wind = round(d["wind"]["speed"])
                dl    = desc.lower()
                emoji = "☀️" if "ясно" in dl else ("🌧" if "дождь" in dl else ("❄️" if "снег" in dl else "⛅"))
                return (f"{emoji} <b>Погода в {d['name']}</b>\n\n"
                        f"🌡 Температура: <b>{temp}°C</b> (ощущается {feels}°C)\n"
                        f"📋 {desc}\n💧 Влажность: <b>{humid}%</b>\n🌬 Ветер: <b>{wind} м/с</b>")
    except:
        return "⛈ Ошибка при получении погоды."

def kb_back(tid: int) -> list:
    return [InlineKeyboardButton(text="🔙 Назад", callback_data=f"panel:back:{tid}")]

def kb_main_menu(tid: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Управление участником", callback_data=f"panel:select:{tid}"),
         InlineKeyboardButton(text="✉️ Сообщения",             callback_data=f"panel:messages:{tid}")],
        [InlineKeyboardButton(text="👥 Участники",             callback_data=f"panel:members:{tid}"),
         InlineKeyboardButton(text="⚙️ Чат",                   callback_data=f"panel:chat:{tid}")],
        [InlineKeyboardButton(text="🎮 Игры",                  callback_data=f"panel:games:{tid}"),
         InlineKeyboardButton(text="📊 Статистика",            callback_data=f"panel:botstats2:{tid}")],
        [InlineKeyboardButton(text="✖️ Закрыть",               callback_data="panel:close:0")],
    ])

def kb_user_panel(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔇 Мут",         callback_data=f"panel:mute:{tid}"),
         InlineKeyboardButton(text="🔊 Размут",      callback_data=f"panel:unmute:{tid}")],
        [InlineKeyboardButton(text="⚡ Варн",        callback_data=f"panel:warn:{tid}"),
         InlineKeyboardButton(text="🌿 Снять варн",  callback_data=f"panel:unwarn:{tid}")],
        [InlineKeyboardButton(text="🔨 Бан",         callback_data=f"panel:ban:{tid}"),
         InlineKeyboardButton(text="🕊 Разбан",      callback_data=f"panel:unban:{tid}")],
        [InlineKeyboardButton(text="🔍 Информация",  callback_data=f"panel:info:{tid}"),
         InlineKeyboardButton(text="🗑 Удалить сообщ", callback_data=f"panel:del:{tid}")],
        [InlineKeyboardButton(text="🎭 Приколы",     callback_data=f"panel:fun:{tid}"),
         InlineKeyboardButton(text="🔙 Назад",       callback_data=f"panel:mainmenu:0")],
    ])

def kb_mute(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин",    callback_data=f"mute:{tid}:5"),
         InlineKeyboardButton(text="15 мин",   callback_data=f"mute:{tid}:15"),
         InlineKeyboardButton(text="30 мин",   callback_data=f"mute:{tid}:30")],
        [InlineKeyboardButton(text="1 час",    callback_data=f"mute:{tid}:60"),
         InlineKeyboardButton(text="3 часа",   callback_data=f"mute:{tid}:180"),
         InlineKeyboardButton(text="12 часов", callback_data=f"mute:{tid}:720")],
        [InlineKeyboardButton(text="1 день",   callback_data=f"mute:{tid}:1440"),
         InlineKeyboardButton(text="7 дней",   callback_data=f"mute:{tid}:10080"),
         InlineKeyboardButton(text="✏️ Своё",  callback_data=f"mute:{tid}:custom")],
        kb_back(tid),
    ])

def kb_warn(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤬 Мат",          callback_data=f"warn:{tid}:Мат в чате"),
         InlineKeyboardButton(text="📨 Спам",          callback_data=f"warn:{tid}:Спам")],
        [InlineKeyboardButton(text="😡 Оскорбление",   callback_data=f"warn:{tid}:Оскорбление"),
         InlineKeyboardButton(text="🌊 Флуд",          callback_data=f"warn:{tid}:Флуд")],
        [InlineKeyboardButton(text="📣 Реклама",       callback_data=f"warn:{tid}:Реклама"),
         InlineKeyboardButton(text="🔞 Контент 18+",   callback_data=f"warn:{tid}:Контент 18+")],
        [InlineKeyboardButton(text="✏️ Своя причина",  callback_data=f"warn:{tid}:custom")],
        kb_back(tid),
    ])

def kb_ban(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Грубые нарушения", callback_data=f"ban:{tid}:Грубые нарушения правил"),
         InlineKeyboardButton(text="📣 Спам/реклама",      callback_data=f"ban:{tid}:Спам и реклама")],
        [InlineKeyboardButton(text="🔞 Контент 18+",       callback_data=f"ban:{tid}:Контент 18+"),
         InlineKeyboardButton(text="🤖 Бот/накрутка",      callback_data=f"ban:{tid}:Бот или накрутка")],
        [InlineKeyboardButton(text="⏰ Бан на 24 часа",    callback_data=f"ban:{tid}:tempban24"),
         InlineKeyboardButton(text="✏️ Своя причина",      callback_data=f"ban:{tid}:custom")],
        kb_back(tid),
    ])

def kb_fun(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Шуточный бан",     callback_data=f"fun:rban:{tid}"),
         InlineKeyboardButton(text="🧠 Проверить IQ",     callback_data=f"fun:iq:{tid}")],
        [InlineKeyboardButton(text="🌈 % гейства",        callback_data=f"fun:gay:{tid}"),
         InlineKeyboardButton(text="🌸 Комплимент",        callback_data=f"fun:compliment:{tid}")],
        [InlineKeyboardButton(text="🔮 Предсказание",     callback_data=f"fun:predict:{tid}"),
         InlineKeyboardButton(text="🌌 Гороскоп",         callback_data=f"fun:horoscope:{tid}")],
        [InlineKeyboardButton(text="⭐ Оценить",          callback_data=f"fun:rate:{tid}"),
         InlineKeyboardButton(text="🤔 Вопрос правды",    callback_data=f"fun:truth:{tid}")],
        [InlineKeyboardButton(text="😈 Задание",          callback_data=f"fun:dare:{tid}"),
         InlineKeyboardButton(text="🎯 Выбор без выбора", callback_data=f"fun:wyr:{tid}")],
        kb_back(tid),
    ])

def kb_messages(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Закрепить",          callback_data=f"msg:pin:{tid}"),
         InlineKeyboardButton(text="📍 Открепить",          callback_data=f"msg:unpin:{tid}")],
        [InlineKeyboardButton(text="🗑 Удалить сообщение",  callback_data=f"msg:del:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 10",        callback_data=f"msg:clear10:{tid}")],
        [InlineKeyboardButton(text="🧹 Очистить 20",        callback_data=f"msg:clear20:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 50",        callback_data=f"msg:clear50:{tid}")],
        [InlineKeyboardButton(text="📢 Объявление",          callback_data=f"msg:announce:{tid}"),
         InlineKeyboardButton(text="📊 Голосование",         callback_data=f"msg:poll:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_members(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮 Список админов",      callback_data=f"members:adminlist:{tid}"),
         InlineKeyboardButton(text="🏆 Топ активности",      callback_data=f"members:top:{tid}")],
        [InlineKeyboardButton(text="📵 Мут 24ч за рекламу",  callback_data=f"members:warn24:{tid}"),
         InlineKeyboardButton(text="⚠️ Варны участника",     callback_data=f"members:warninfo:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_chat(tid: int) -> InlineKeyboardMarkup:
    ms = "✅ вкл" if ANTI_MAT_ENABLED else "❌ выкл"
    ks = "✅ вкл" if AUTO_KICK_BOTS   else "❌ выкл"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Заблокировать чат",  callback_data=f"chat:lock:{tid}"),
         InlineKeyboardButton(text="🔓 Разблокировать",     callback_data=f"chat:unlock:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 10с",       callback_data=f"chat:slow:10:{tid}"),
         InlineKeyboardButton(text="🐢 Slowmode 30с",       callback_data=f"chat:slow:30:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 60с",       callback_data=f"chat:slow:60:{tid}"),
         InlineKeyboardButton(text="🐇 Выкл slowmode",      callback_data=f"chat:slow:0:{tid}")],
        [InlineKeyboardButton(text=f"🧼 Антимат: {ms}",    callback_data=f"chat:antimat:{tid}"),
         InlineKeyboardButton(text=f"🤖 Автокик: {ks}",    callback_data=f"chat:autokick:{tid}")],
        [InlineKeyboardButton(text="📜 Правила",             callback_data=f"chat:rules:{tid}"),
         InlineKeyboardButton(text="📈 Статистика",          callback_data=f"chat:botstats:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_games(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик d6",           callback_data=f"game:roll:{tid}"),
         InlineKeyboardButton(text="🪙 Монетка",            callback_data=f"game:flip:{tid}")],
        [InlineKeyboardButton(text="🎰 Слот-машина",        callback_data=f"game:slot:{tid}"),
         InlineKeyboardButton(text="🎱 Шар предсказаний",   callback_data=f"game:8ball:{tid}")],
        [InlineKeyboardButton(text="✊ КНБ — Камень",       callback_data=f"game:rps_k:{tid}"),
         InlineKeyboardButton(text="✌️ КНБ — Ножницы",      callback_data=f"game:rps_n:{tid}")],
        [InlineKeyboardButton(text="🖐 КНБ — Бумага",       callback_data=f"game:rps_b:{tid}"),
         InlineKeyboardButton(text="📖 Цитата дня",         callback_data=f"game:quote:{tid}")],
        [InlineKeyboardButton(text="🌤 Погода — Москва",    callback_data=f"game:weather_Москва:{tid}"),
         InlineKeyboardButton(text="🌍 Свой город",          callback_data=f"game:weather_custom:{tid}")],
        [InlineKeyboardButton(text="⏱ Отсчёт 5 сек",       callback_data=f"game:countdown5:{tid}"),
         InlineKeyboardButton(text="⏱ Отсчёт 10 сек",      callback_data=f"game:countdown10:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

message_cache = {}

class StatsMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group","supergroup"):
            chat_stats[event.chat.id][event.from_user.id] += 1
            uid, cid = event.from_user.id, event.chat.id
            xp_data[cid][uid] += random.randint(1, 5)
            from datetime import datetime, timedelta
            today = datetime.now().strftime("%d.%m.%Y")
            last = streak_dates[cid][uid]
            if last != today:
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
                if last == yesterday:
                    streaks[cid][uid] += 1
                elif last == "":
                    streaks[cid][uid] = 1
                else:
                    streaks[cid][uid] = 1
                streak_dates[cid][uid] = today
            old_level = levels[cid][uid]
            new_level = xp_data[cid][uid] // 100
            if new_level > old_level:
                levels[cid][uid] = new_level
                title = (
                    "👑 Элита" if new_level >= 20 else
                    "🏆 Легенда" if new_level >= 10 else
                    "⚔️ Ветеран" if new_level >= 5 else
                    "🌱 Активный" if new_level >= 3 else
                    "🔰 Участник")
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
                    else: await event.reply("⚠️ Примеры: 10, 30m, 2h, 1d")
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
                    else: await event.reply("⚠️ Формат: Вопрос|Вариант1|Вариант2")
                elif action == "weather_city":
                    await event.answer(await get_weather(text.strip()), parse_mode="HTML")
            except Exception as e:
                await event.reply(f"⚠️ Ошибка: {e}")
            try: await event.delete()
            except: pass
            return
        return await handler(event, data)

dp.message.middleware(PendingInputMiddleware())
dp.message.middleware(StatsMiddleware())
dp.message.middleware(AntiFloodMiddleware())
dp.message.middleware(AntiMatMiddleware())

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

@dp.message(F.left_chat_member)
async def on_left_member(message: Message):
    member = message.left_chat_member
    if member.is_bot: return

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
                f"⚙️ <b>Панель управления</b>\n\n"
                f"💬 Чат: <b>{call.message.chat.title}</b>\n"
                f"📨 Сообщений: <b>{total_msgs}</b>\n"
                f"⚡ Варнов: <b>{total_warns}</b>\n"
                f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери раздел:",
                parse_mode="HTML", reply_markup=kb_main_menu())
        elif action == "select":
            await call.message.edit_text(
                "👤 <b>Управление участником</b>\n\nОткрой панель реплаем:\n"
                "<code>/panel</code> → ответь на сообщение участника",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        elif action == "back":
            if tid != 0:
                warns = warnings[cid].get(tid, 0); rep = reputation[cid].get(tid, 0)
                msgs  = chat_stats[cid].get(tid, 0)
                afk   = f"\n🎮 AFK: {afk_users[tid]}" if tid in afk_users else ""
                try:
                    safe_name = (tm.user.full_name
                        .replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
                    mention = f'<a href="tg://user?id={tid}">{safe_name}</a>'
                except:
                    mention = f"<code>{tid}</code>"
                await call.message.edit_text(
                    f"👤 <b>Панель участника</b>\n\n🆔 {mention}{afk}\n"
                    f"🆔 ID: <code>{tid}</code>\n"
                    f"⚡ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
                    f"⭐ Репутация: <b>{rep:+d}</b>\n"
                    f"💬 Сообщений: <b>{msgs}</b>\n\n➡️ Выбери действие:",
                    parse_mode="HTML", reply_markup=kb_user_panel(tid))
            else:
                await call.message.edit_text("🔧 <b>Панель управления</b>\n\n➡️ Выбери раздел:",
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
            await call.message.edit_text(f"⚡ <b>Варн для {tname}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_warn(tid))
        elif action == "unwarn":
            if warnings[cid][tid] > 0: warnings[cid][tid] -= 1
            await call.message.edit_text(
                f"🌿 С <b>{tname}</b> снят варн. Осталось: <b>{warnings[cid][tid]}/{MAX_WARNINGS}</b>",
                parse_mode="HTML")
        elif action == "ban":
            await call.message.edit_text(f"🔨 <b>Бан для {tname}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_ban(tid))
        elif action == "unban":
            await bot.unban_chat_member(cid, tid, only_if_banned=True)
            await call.message.edit_text(f"🕊 <b>{tname}</b> разбанен.", parse_mode="HTML")
            await log_action(f"🕊 <b>РАЗБАН</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nЧат: {call.message.chat.title}")
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
                f"🔍 <b>Инфо:</b>\n\n🏷 {u.mention_html()}{afk}\n"
                f"🔗 {'@'+u.username if u.username else 'нет'}\n🪪 <code>{u.id}</code>\n"
                f"📌 {smap.get(tm2.status, tm2.status)}\n"
                f"⚡ Варнов: <b>{warnings[cid].get(tid,0)}/{MAX_WARNINGS}</b>\n"
                f"🌟 Репутация: <b>{reputation[cid].get(tid,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(tid,0)}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[kb_back(tid)]))
        elif action == "fun":
            await call.message.edit_text(f"🎭 <b>Приколы над {tname}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_fun(tid))
        elif action == "messages":
            await call.message.edit_text("✉️ <b>Действия с сообщениями</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_messages(tid))
        elif action == "members":
            await call.message.edit_text("👥 <b>Управление участниками</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_members(tid))
        elif action == "chat":
            await call.message.edit_text(
                f"⚙️ <b>Управление чатом</b>\n\n"
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
                f"⚡ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
                f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
                f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
                f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
    except Exception as e:
        await call.answer(f"⚠️ Ошибка: {e}", show_alert=True)
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
        await log_action(f"⚡ <b>ВАРН</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nПричина: {reason}\nЧат: {call.message.chat.title}")
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
            f"🌈 {mention}\n{'🟣'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка 😄",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "compliment":
        await call.message.edit_text(
            f"🌸 <b>{mention}:</b>\n\n{random.choice(COMPLIMENTS)}", parse_mode="HTML", reply_markup=back_kb)
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
            f"⭐ Оценка {mention}:\n{'🌟'*score+'☆'*(10-score)}\n<b>{score}/10</b>",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "truth":
        await call.message.edit_text(
            f"🤔 <b>Вопрос для {mention}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "dare":
        await call.message.edit_text(
            f"😈 <b>Задание для {mention}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "wyr":
        await call.message.edit_text(
            f"🎯 <b>Выбор для {mention}:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML", reply_markup=back_kb)
    await call.answer()

@dp.callback_query(F.data.startswith("msg:"))
async def cb_msg(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    if action == "pin":
        try: await bot.pin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📌 Закреплено!", show_alert=True)
        except: await call.answer("⚠️ Открой панель реплаем на нужное сообщение.", show_alert=True)
    elif action == "unpin":
        try: await bot.unpin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📍 Откреплено!", show_alert=True)
        except: await call.answer("⚠️ Нет сообщения.", show_alert=True)
    elif action == "del":
        try: await call.message.reply_to_message.delete(); await call.answer("🗑 Удалено!", show_alert=True)
        except: await call.answer("⚠️ Нет сообщения.", show_alert=True)
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
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    elif action == "poll":
        pending[call.from_user.id] = {"action":"poll_text","target_id":0,"target_name":"","chat_id":cid}
        await call.message.edit_text("📊 Напиши голосование:\n<code>Вопрос|Вариант 1|Вариант 2</code>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    try: await call.message.edit_text("✉️ <b>Действия с сообщениями</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_messages(tid))
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
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
    elif action == "top":
        stats = chat_stats[cid]
        if not stats: await call.answer("📊 Статистика пуста!", show_alert=True)
        else:
            sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
            medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
            lines    = ["🏆 <b>Топ активных:</b>\n"]
            for i, (uid, cnt) in enumerate(sorted_u):
                try: m = await bot.get_chat_member(cid, uid); uname = m.user.full_name
                except: uname = f"ID {uid}"
                lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщ.")
            await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
    elif action == "warn24":
        if tid != 0:
            await bot.restrict_chat_member(cid, tid,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
            await call.message.edit_text(
                f"📵 <b>{tname}</b> — мут на <b>24 часа</b> за рекламу.", parse_mode="HTML")
            await log_action(f"📵 <b>МУТ 24ч</b>\nКто: {call.from_user.mention_html()}\nКого: <b>{tname}</b>\nЧат: {call.message.chat.title}")
        else: await call.answer("⚠️ Открой панель реплаем на участника.", show_alert=True)
    elif action == "warninfo":
        if tid != 0:
            count = warnings[cid].get(tid, 0)
            await call.message.edit_text(
                f"⚡ Варнов у <b>{tname}</b>: <b>{count}/{MAX_WARNINGS}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        else: await call.answer("⚠️ Открой панель реплаем на участника.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("chat:"))
async def cb_chat(call: CallbackQuery):
    global ANTI_MAT_ENABLED, AUTO_KICK_BOTS
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); cid = call.message.chat.id
    if parts[1] == "slow":
        delay = int(parts[2]); tid = int(parts[3])
        try:
            chat = await bot.get_chat(cid)
            await bot.set_chat_slow_mode_delay(cid, delay)
            label = f"Slowmode {delay}с включён!" if delay > 0 else "Slowmode выключен!"
            await call.answer(f"🐢 {label}", show_alert=True)
        except Exception as e:
            await call.answer(f"⚠️ Ошибка: {e}", show_alert=True)
        await call.message.edit_text(
            f"⚙️ <b>Управление чатом</b>\n\n"
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
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    elif action == "botstats":
        total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
        total_warns = sum(sum(v.values()) for v in warnings.values())
        await call.message.edit_text(
            f"📈 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
            f"⚡ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
            f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
            f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
            f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    await call.message.edit_text(
        f"⚙️ <b>Управление чатом</b>\n\n"
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
        [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]])
    if action == "roll":
        await call.message.edit_text(
            f"🎲 Бросаю кубик... выпало: <b>{random.randint(1,6)}</b>!", parse_mode="HTML", reply_markup=back_kb)
    elif action == "flip":
        await call.message.edit_text(random.choice(["🦅 Орёл!", "🪙 Решка!"]), reply_markup=back_kb)
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
        mp = {"k":("к","✊ Камень"),"n":("н","✌️ Ножницы"),"b":("б","🖐 Бумага")}
        wins = {"к":"н","н":"б","б":"к"}
        key = action.split("_")[1]; pk,pl = mp[key]; bk,bl = random.choice(list(mp.values()))
        res = "🤝 Ничья!" if pk==bk else ("🎉 Ты выиграл!" if wins[pk]==bk else "😈 Я выиграл!")
        await call.message.edit_text(f"Ты: {pl}\nЯ: {bl}\n\n{res}", reply_markup=back_kb)
    elif action == "quote":
        await call.message.edit_text(f"📖 {random.choice(QUOTES)}", reply_markup=back_kb)
    elif action.startswith("weather_"):
        city = action.replace("weather_","")
        if city == "custom":
            pending[call.from_user.id] = {"action":"weather_city","target_id":0,"target_name":"","chat_id":cid}
            await call.message.edit_text("🌍 Напиши название города:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
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
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    await call.answer()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await reply_auto_delete(message, 
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "🤖 Я бот-модератор этого чата.\n"
        "📜 /rules — правила\n"
        "❓ /help — все команды\n"
        "⚙️ /panel — панель управления (реплай на участника)",
        parse_mode="HTML")

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    await message.reply_photo(
        photo=FSInputFile("welcome.jpg"),
        caption="📜 <b>Правила чата</b>\n\n🔎 Нажми кнопку ниже чтобы прочитать правила:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Правила чата", url="https://telegra.ph/Pravila-anon-chata-03-03-2")]
        ])
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_adm = await check_admin(message)
    text = (
        "📖 <b>ВСЕ КОМАНДЫ БОТА</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"

        "👤 <b>Профиль и статистика:</b>\n"
        "👤 /profile — профиль участника\n"
        "📊 /ранг — уровень и опыт\n"
        "🔥 /streak — серия активности\n"
        "🔍 /info — инфо (реплай)\n"
        "⚡ /warnings — варны (реплай)\n\n"
        "⭐ <b>Репутация:</b>\n"
        "🌟 /rep — репутация (реплай)\n"
        "🏆 /toprep — топ по репутации\n"
        "🏆 /top — топ активных участников\n"
        "🎁 /daily — ежедневный бонус (+10 реп, растёт со стриком)\n"
        "🏪 /shop — магазин титулов за репутацию\n\n"

        "🎰 <b>Экономика и игры на репутацию:</b>\n"
        "🎰 /casino [сумма] — казино на репутацию\n"
        "⚔️ /duel [сумма] — дуэль (реплай)\n"
        "✅ /accept — принять дуэль\n"
        "❌ /decline — отказаться от дуэли\n\n"

        "🎪 <b>Турниры:</b>\n"
        "🎪 /join — записаться в турнир\n\n"

        "🏆 <b>МВП и голосования:</b>\n"
        "🏆 /mvp — проголосовать за МВП (реплай)\n"
        "📊 /mvpstats — топ МВП\n\n"

        "🕵 <b>Анонимные функции:</b>\n"
        "📬 /ask — открыть АСК\n"
        "📭 /askoff — закрыть АСК\n"
        "📩 /send @юзернейм текст — анонимный вопрос\n"
        "💌 /confession текст — анонимное признание в чат\n"
        "📩 /secret @юзернейм текст — анонимное лс\n\n"

        "🛠 <b>Утилиты:</b>\n"
        "📜 /rules — правила чата\n"
        "🌤 /weather [город] — погода\n"
        "😴 /afk [причина] — уйти в AFK\n"
        "📋 /report причина — пожаловаться (реплай)\n"
        "🎂 /birthday ДД.ММ — сохранить день рождения\n"
        "⏰ /remind 30m текст — напоминание\n"
        "📝 /note get/list — заметки\n"
        "🎯 /guess — угадай число\n\n"

        "🎮 <b>Развлечения и игры:</b>\n"
        "🎲 /roll [N] — кубик d6 или dN\n"
        "🪙 /flip — монетка\n"
        "🎱 /8ball [вопрос] — шар предсказаний\n"
        "🎰 /slot — однорукий бандит\n"
        "✊ /rps к/н/б — камень ножницы бумага\n"
        "🎯 /choose вар1|вар2 — случайный выбор\n"
        "⭐ /rate [что] — оценить что угодно\n"
        "🧠 /iq — проверить IQ (реплай)\n"
        "🌈 /gay — шуточный % (реплай)\n"
        "💘 /совместимость — совместимость (реплай)\n"
        "🌌 /horoscope — случайный гороскоп\n"
        "🔮 /predict — предсказание (реплай)\n"
        "🌸 /compliment — комплимент (реплай)\n"
        "📖 /quote — цитата дня\n"
        "🤔 /truth — вопрос правды (реплай)\n"
        "😈 /dare — задание смелости (реплай)\n"
        "🎯 /wyr — выбор без выбора\n"
    )
    if is_adm:
        text += (
            "\n━━━━━━━━━━━━━━━━━━━━\n"
            "👮 <b>Только для администраторов:</b>\n\n"

            "🔧 <b>Основные действия:</b>\n"
            "⚙️ /panel — панель управления (реплай)\n"
            "🔨 /ban [причина] — бан участника\n"
            "🕊 /unban — разбан\n"
            "🔇 /mute [время] — мут (10m / 2h / 1d)\n"
            "🔊 /unmute — размут\n"
            "⚡ /warn [причина] — варн\n"
            "🌿 /unwarn — снять варн\n"
            "📵 /warn24 — мут 24ч за рекламу\n"
            "🎲 /rban — шуточный бан\n\n"

            "✉️ <b>Сообщения:</b>\n"
            "🗑 /del — удалить сообщение (реплай)\n"
            "🧹 /clear [N] — очистить N сообщений\n"
            "📢 /announce [текст] — объявление\n"
            "📌 /pin — закрепить (реплай)\n"
            "📍 /unpin — открепить (реплай)\n"
            "📊 /poll Вопрос|Вар1|Вар2 — голосование\n\n"

            "⚙️ <b>Настройки чата:</b>\n"
            "🔒 /lock — заблокировать чат\n"
            "🔓 /unlock — разблокировать чат\n"
            "🐢 /slowmode [сек] — медленный режим\n"
            "🧼 /antimat on/off — антимат\n"
            "🤖 /autokick on/off — автокик ботов\n\n"

            "👥 <b>Участники:</b>\n"
            "👮 /adminlist — список администраторов\n"
            "🏅 /promote [тег] — выдать тег участнику\n"
            "📋 /modhistory — история модераций участника (реплай)\n"
            "🔇 /tempban [дни] [причина] — временный бан (реплай)\n"
            "👥 /banlist — список всех забаненных\n"
            "🧾 /modexport — экспорт истории модераций в файл\n"
            "👮 /modtop — топ самых активных модераторов\n"
            "📝 /warnmenu — выдать варн по шаблону (реплай)\n"
            "📈 /botstats — статистика бота\n"
            "🔄 /mvpreset — сбросить МВП голоса\n\n"

            "🎪 <b>Турниры:</b>\n"
            "🎪 /tournament start — открыть регистрацию\n"
            "🚀 /tournament begin — начать турнир\n"
            "⚔️ /tournament next — следующий раунд\n"
            "🛑 /tournament stop — отменить турнир\n\n"

            "🛠 <b>Прочее:</b>\n"
            "⏱ /countdown [N] — обратный отсчёт\n"
            "📝 /note set/del — управление заметками\n"
        )
    await reply_auto_delete(message, text, parse_mode="HTML")

@dp.message(Command("panel"))
async def cmd_panel(message: Message):
    if not await require_admin(message): return
    if message.reply_to_message:
        target = message.reply_to_message.from_user
        warns  = warnings[message.chat.id].get(target.id, 0)
        rep    = reputation[message.chat.id].get(target.id, 0)
        msgs   = chat_stats[message.chat.id].get(target.id, 0)
        afk    = f"\n😴 AFK: {afk_users[target.id]}" if target.id in afk_users else ""
        await reply_auto_delete(message, 
            f"👤 <b>Панель участника</b>\n\n🔎 {target.mention_html()}{afk}\n"
            f"🪪 ID: <code>{target.id}</code>\n"
            f"⚡ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
            f"🌟 Репутация: <b>{rep:+d}</b>\n"
            f"💬 Сообщений: <b>{msgs}</b>\n\nВыбери действие:",
            parse_mode="HTML", reply_markup=kb_user_panel(target.id))
    else:
        total_msgs  = sum(chat_stats[message.chat.id].values())
        total_warns = sum(warnings[message.chat.id].values())
        await reply_auto_delete(message, 
            f"⚙️ <b>Панель управления</b>\n\n"
            f"💬 Чат: <b>{message.chat.title}</b>\n"
            f"📨 Сообщений: <b>{total_msgs}</b>\n"
            f"⚡ Варнов: <b>{total_warns}</b>\n"
            f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери раздел:",
            parse_mode="HTML", reply_markup=kb_main_menu())

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя забанить администратора!"); return
    reason = command.args or "Нарушение правил"
    cid = message.chat.id
    # 📨 ЛС нарушителю ДО бана (после уже не получится)
    dm_ok = await dm_warn_user(target.id, target.full_name, reason,
                                message.chat.title, "🔨 Бан", message.from_user.full_name)
    await bot.ban_chat_member(cid, target.id)
    # 📸 Скриншот нарушения
    if message.reply_to_message.text:
        await log_violation_screenshot(
            cid, target.id, target.full_name,
            message.reply_to_message.text,
            "🔨 Бан", reason, message.from_user.full_name, message.chat.title)
    reply = random.choice(BAN_MESSAGES).format(name=target.mention_html(), reason=reason)
    if dm_ok: reply += "\n<i>📨 Нарушитель уведомлён в лс</i>"
    await reply_auto_delete(message, reply, parse_mode="HTML")
    await log_action(f"🔨 <b>БАН</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nПричина: {reason}\nЧат: {message.chat.title}")
    add_mod_history(cid, target.id, "🔨 Бан", reason, message.from_user.full_name)
    from datetime import datetime
    ban_list[cid][target.id] = {
        "name": target.full_name, "reason": reason,
        "by": message.from_user.full_name,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "temp": False
    }
    mod_reasons[cid][target.id]["ban"] = reason

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await reply_auto_delete(message, f"🕊 {target.mention_html()} разбанен.", parse_mode="HTML")
    await log_action(f"🕊 <b>РАЗБАН</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя замутить администратора!"); return
    mins, label = parse_duration(command.args or "60m")
    if not mins: mins = 60; label = "1 ч."
    cid = message.chat.id
    await bot.restrict_chat_member(cid, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    # 📸 Скриншот нарушения
    if message.reply_to_message.text:
        await log_violation_screenshot(
            cid, target.id, target.full_name,
            message.reply_to_message.text,
            f"🔇 Мут {label}", command.args or "—",
            message.from_user.full_name, message.chat.title)
    # 📨 ЛС нарушителю
    dm_ok = await dm_warn_user(target.id, target.full_name, command.args or "Нарушение правил",
                                message.chat.title, f"🔇 Мут на {label}", message.from_user.full_name)
    reply = random.choice(MUTE_MESSAGES).format(name=target.mention_html(), time=label)
    if dm_ok: reply += "\n<i>📨 Нарушитель уведомлён в лс</i>"
    await reply_auto_delete(message, reply, parse_mode="HTML")
    await log_action(f"🔇 <b>МУТ</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nВремя: {label}\nЧат: {message.chat.title}")
    add_mod_history(cid, target.id, f"🔇 Мут {label}", command.args or "—", message.from_user.full_name)
    mod_reasons[cid][target.id]["mute"] = f"{label} — {command.args or 'Нарушение правил'}"
    # 🔄 Запуск автоснятия мута
    schedule_unmute(cid, target.id, mins, target.full_name)

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    await reply_auto_delete(message, f"🔊 {target.mention_html()} размучен.", parse_mode="HTML")
    await log_action(f"🔊 <b>РАЗМУТ</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("warn"))
async def cmd_warn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя выдать варн администратору!"); return
    reason = command.args or "Нарушение правил"
    cid = message.chat.id

    # Чистим истёкшие варны перед добавлением
    clean_expired_warns(cid, target.id)
    add_warn_with_expiry(cid, target.id)
    count = warnings[cid][target.id]
    save_data()

    # 📸 Скриншот сообщения-нарушения в лог
    if message.reply_to_message.text:
        await log_violation_screenshot(
            cid, target.id, target.full_name,
            message.reply_to_message.text,
            f"⚡ Варн {count}/{MAX_WARNINGS}", reason,
            message.from_user.full_name, message.chat.title)

    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, target.id)
        warn_expiry[cid][target.id].clear()
        warnings[cid][target.id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=target.mention_html(), max=MAX_WARNINGS)
        await log_action(f"🔨 <b>АВТОБАН</b>\nКого: {target.mention_html()}\nПричина: {MAX_WARNINGS} варнов\nЧат: {message.chat.title}")
        add_mod_history(cid, target.id, "🔨 Автобан", f"{MAX_WARNINGS} варнов", message.from_user.full_name)
        # 📨 ЛС нарушителю
        await dm_warn_user(target.id, target.full_name, f"{MAX_WARNINGS} варнов — автобан",
                           message.chat.title, "🔨 Бан", message.from_user.full_name)
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"⚡ <b>ВАРН</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nПричина: {reason}\n⏳ Сгорит через {WARN_EXPIRY_DAYS} дн.\nЧат: {message.chat.title}")
        add_mod_history(cid, target.id, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, message.from_user.full_name)
        # 📨 ЛС нарушителю
        dm_ok = await dm_warn_user(target.id, target.full_name, reason,
                                    message.chat.title, f"⚡ Варн {count}/{MAX_WARNINGS}",
                                    message.from_user.full_name)
        if dm_ok:
            msg += "\n<i>📨 Нарушитель уведомлён в лс</i>"
    await reply_auto_delete(message, msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def cmd_unwarn(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1
    await reply_auto_delete(message, 
        f"🌿 С {target.mention_html()} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

@dp.message(Command("del"))
async def cmd_del(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    await message.reply_to_message.delete(); await message.delete()

@dp.message(Command("clear"))
async def cmd_clear(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError: await reply_auto_delete(message, "⚠️ /clear 20"); return
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
        await reply_auto_delete(message, "📢 Напиши текст объявления:"); return
    await message.delete()
    await message.answer(
        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}", parse_mode="HTML")

@dp.message(Command("pin"))
async def cmd_pin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    await bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await reply_auto_delete(message, "📌 Закреплено!")

@dp.message(Command("unpin"))
async def cmd_unpin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await reply_auto_delete(message, "📍 Откреплено!")

@dp.message(Command("lock"))
async def cmd_lock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await reply_auto_delete(message, "🔒 Чат <b>заблокирован</b>.", parse_mode="HTML")
    await log_action(f"🔒 <b>ЧАТ ЗАБЛОКИРОВАН</b>\nКто: {message.from_user.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(
        can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
        can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
    await reply_auto_delete(message, "🔓 Чат <b>разблокирован</b>.", parse_mode="HTML")
    await log_action(f"🔓 <b>ЧАТ РАЗБЛОКИРОВАН</b>\nКто: {message.from_user.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("slowmode"))
async def cmd_slowmode(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: delay = int(command.args) if command.args else 10
    except ValueError: await reply_auto_delete(message, "⚠️ /slowmode 30"); return
    if delay < 0 or delay > 900:
        await reply_auto_delete(message, "⚠️ Значение от 0 до 900 секунд."); return
    try:
        await bot.set_chat_slow_mode_delay(message.chat.id, delay)
        if delay == 0:
            await reply_auto_delete(message, "🐇 Slowmode <b>выключен</b>.", parse_mode="HTML")
        else:
            label = f"{delay} сек." if delay < 60 else f"{delay//60} мин. {delay%60} сек." if delay%60 else f"{delay//60} мин."
            await reply_auto_delete(message, f"🐢 Slowmode: <b>{label}</b>", parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message, f"⚠️ Не удалось установить slowmode: <code>{e}</code>", parse_mode="HTML")

@dp.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    title = command.args or "Участник"; target = message.reply_to_message.from_user
    await bot.set_chat_administrator_custom_title(message.chat.id, target.id, title)
    await reply_auto_delete(message, f"🏅 {target.mention_html()} получил тег: <b>{title}</b>", parse_mode="HTML")

@dp.message(Command("poll"))
async def cmd_poll(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args or "|" not in command.args:
        await reply_auto_delete(message, "⚠️ /poll Вопрос|Вар1|Вар2"); return
    parts = [p.strip() for p in command.args.split("|")]
    if len(parts) < 3: await reply_auto_delete(message, "⚠️ Нужно минимум 2 варианта."); return
    await message.delete()
    await bot.send_poll(message.chat.id, question=parts[0], options=parts[1:], is_anonymous=False)

@dp.message(Command("antimat"))
async def cmd_antimat(message: Message, command: CommandObject):
    global ANTI_MAT_ENABLED
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message, f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on": ANTI_MAT_ENABLED = True; await reply_auto_delete(message, "🧼 Антимат <b>включён</b>.", parse_mode="HTML")
    elif a == "off": ANTI_MAT_ENABLED = False; await reply_auto_delete(message, "🔞 Антимат <b>выключен</b>.", parse_mode="HTML")

@dp.message(Command("autokick"))
async def cmd_autokick(message: Message, command: CommandObject):
    global AUTO_KICK_BOTS
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message, f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on": AUTO_KICK_BOTS = True; await reply_auto_delete(message, "🤖 Автокик <b>включён</b>.", parse_mode="HTML")
    elif a == "off": AUTO_KICK_BOTS = False; await reply_auto_delete(message, "🤖 Автокик <b>выключен</b>.", parse_mode="HTML")

@dp.message(Command("warn24"))
async def cmd_warn24(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя замутить администратора!"); return
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
    await reply_auto_delete(message, f"📵 {target.mention_html()} замучен на <b>24 часа</b> за рекламу.", parse_mode="HTML")
    await log_action(f"📵 <b>МУТ 24ч</b>\nКто: {message.from_user.mention_html()}\nКого: {target.mention_html()}\nЧат: {message.chat.title}")

@dp.message(Command("rban"))
async def cmd_rban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await reply_auto_delete(message, 
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
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("note"))
async def cmd_note(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "📝 /note set/get/del/list [имя] [текст]"); return
    parts = command.args.split(maxsplit=2); action = parts[0].lower(); cid = message.chat.id
    if action == "set":
        if not await require_admin(message): return
        if len(parts) < 3: await reply_auto_delete(message, "⚠️ /note set [имя] [текст]"); return
        notes[cid][parts[1]] = parts[2]; save_data()
        await reply_auto_delete(message, f"📝 Заметка <b>{parts[1]}</b> сохранена!", parse_mode="HTML")
    elif action == "get":
        if len(parts) < 2: await reply_auto_delete(message, "⚠️ /note get [имя]"); return
        t = notes[cid].get(parts[1])
        await reply_auto_delete(message, f"📄 <b>{parts[1]}:</b>\n{t}" if t else "❌ Заметка не найдена.", parse_mode="HTML")
    elif action == "del":
        if not await require_admin(message): return
        if len(parts) > 1 and parts[1] in notes[cid]:
            del notes[cid][parts[1]]; save_data()
            await reply_auto_delete(message, f"🗑 Заметка <b>{parts[1]}</b> удалена.", parse_mode="HTML")
        else: await reply_auto_delete(message, "❌ Не найдена.")
    elif action == "list":
        keys = list(notes[cid].keys())
        await reply_auto_delete(message, "📋 <b>Заметки:</b>\n" + "\n".join(f"📌 {k}" for k in keys) if keys else "📭 Заметок нет.", parse_mode="HTML")

@dp.message(Command("birthday"))
async def cmd_birthday(message: Message, command: CommandObject):
    if not command.args:
        await reply_auto_delete(message, "🎂 Формат: /birthday ДД.ММ\nПример: <code>/birthday 25.03</code>", parse_mode="HTML"); return
    try:
        day, month = map(int, command.args.strip().split("."))
        if not (1 <= day <= 31 and 1 <= month <= 12): raise ValueError
    except:
        await reply_auto_delete(message, "⚠️ Неверный формат. Пример: /birthday 25.03"); return
    uid = message.from_user.id
    birthdays[uid] = {"day": day, "month": month, "name": message.from_user.full_name, "chat_id": message.chat.id}
    await reply_auto_delete(message, 
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
        await reply_auto_delete(message, 
            "⏰ Формат: /remind 30m текст\n"
            "<code>/remind 10m Написать другу</code>\n"
            "<code>/remind 2h Встреча</code>", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    mins, label = parse_duration(parts[0])
    if not mins: await reply_auto_delete(message, "⚠️ Неверный формат времени. Примеры: 10m, 2h, 1d"); return
    text = parts[1].strip()
    cid = message.chat.id
    await reply_auto_delete(message, f"⏰ Напомню через <b>{label}</b>!\n📝 {text}", parse_mode="HTML")
    async def remind_task():
        await asyncio.sleep(mins * 60)
        try:
            await bot.send_message(cid,
                f"⏰ {message.from_user.mention_html()}, напоминание!\n\n📌 <b>{text}</b>", parse_mode="HTML")
        except: pass
    asyncio.create_task(remind_task())

@dp.message(Command("countdown"))
async def cmd_countdown(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: n = min(int(command.args or 5), 10)
    except: n = 5
    sent = await reply_auto_delete(message, f"⏱ <b>{n}...</b>", parse_mode="HTML")
    for i in range(n-1, 0, -1):
        await asyncio.sleep(1)
        try: await sent.edit_text(f"⏱ <b>{i}...</b>", parse_mode="HTML")
        except: pass
    await asyncio.sleep(1)
    await sent.edit_text("🚀 <b>ПОЕХАЛИ!</b>", parse_mode="HTML")

@dp.message(Command("weather"))
async def cmd_weather(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "🌤 Укажи город: /weather Москва"); return
    wait = await reply_auto_delete(message, "⏳ Получаю данные...")
    await wait.edit_text(await get_weather(command.args), parse_mode="HTML")

@dp.message(Command("afk"))
async def cmd_afk(message: Message, command: CommandObject):
    reason = command.args or "без причины"
    afk_users[message.from_user.id] = reason
    await reply_auto_delete(message, f"😴 {message.from_user.mention_html()} ушёл в AFK: {reason}", parse_mode="HTML")

@dp.message(Command("info"))
async def cmd_info(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    user = message.reply_to_message.from_user
    member = await bot.get_chat_member(message.chat.id, user.id)
    smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор","member":"👤 Участник",
            "restricted":"🔇 Ограничен","kicked":"🔨 Забанен","left":"🚶 Вышел"}
    afk  = f"\n😴 AFK: {afk_users[user.id]}" if user.id in afk_users else ""
    await reply_auto_delete(message, 
        f"🔍 <b>Инфо:</b>\n{user.mention_html()}{afk}\n"
        f"🔗 {'@'+user.username if user.username else 'нет'}\n"
        f"🪪 <code>{user.id}</code>\n"
        f"📌 {smap.get(member.status, member.status)}\n"
        f"⚡ Варнов: <b>{warnings[message.chat.id].get(user.id,0)}/{MAX_WARNINGS}</b>\n"
        f"🌟 Репутация: <b>{reputation[message.chat.id].get(user.id,0):+d}</b>\n"
        f"💬 Сообщений: <b>{chat_stats[message.chat.id].get(user.id,0)}</b>",
        parse_mode="HTML")

@dp.message(Command("warnings"))
async def cmd_warnings(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await reply_auto_delete(message, 
        f"⚡ {target.mention_html()} — варнов: <b>{warnings[message.chat.id].get(target.id,0)}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

@dp.message(Command("modhistory"))
async def cmd_modhistory(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника чтобы посмотреть его историю."); return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    history = mod_history[cid].get(target.id, [])
    if not history:
        await reply_auto_delete(message,
            f"📋 История модераций {target.mention_html()}:\n\n✅ Чисто — нарушений не найдено.",
            parse_mode="HTML"); return
    lines = [f"📋 <b>История модераций {target.mention_html()}:</b>\n"]
    for entry in reversed(history):
        lines.append(
            f"{'─'*20}\n"
            f"{entry['action']}\n"
            f"📝 Причина: {entry['reason']}\n"
            f"👮 Модератор: {entry['by']}\n"
            f"🕐 {entry['time']}"
        )
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("tempban"))
async def cmd_tempban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение. Пример: /tempban 3 спам"); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя забанить администратора!"); return
    args = (command.args or "").split(None, 1)
    try:
        days = int(args[0])
        reason = args[1] if len(args) > 1 else "Нарушение правил"
    except (ValueError, IndexError):
        await reply_auto_delete(message, "⚠️ Пример: /tempban 3 спам"); return
    if days < 1 or days > 365:
        await reply_auto_delete(message, "⚠️ Срок от 1 до 365 дней."); return
    cid = message.chat.id
    from datetime import datetime
    # ЛС до бана
    await dm_warn_user(target.id, target.full_name, reason,
                       message.chat.title, f"🔇 Временный бан на {days} дн.", message.from_user.full_name)
    await bot.ban_chat_member(cid, target.id)
    ban_list[cid][target.id] = {
        "name": target.full_name, "reason": reason,
        "by": message.from_user.full_name,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "until": (datetime.now().timestamp() + days * 86400),
        "temp": True, "days": days
    }
    mod_reasons[cid][target.id]["ban"] = reason
    add_mod_history(cid, target.id, f"🔇 Темпбан {days} дн.", reason, message.from_user.full_name)
    # Запускаем таймер снятия
    key = (cid, target.id)
    old = tempban_timers.get(key)
    if old: old.cancel()
    tempban_timers[key] = asyncio.create_task(tempban_unban(cid, target.id, target.full_name, days))
    await reply_auto_delete(message,
        f"🔇 <b>{target.mention_html()}</b> временно забанен на <b>{days} дн.</b>\n"
        f"📝 Причина: {reason}\n"
        f"🔓 Разбан: автоматически через {days} дн.", parse_mode="HTML")
    await log_action(
        f"🔇 <b>ТЕМПБАН</b>\nКто: {message.from_user.mention_html()}\n"
        f"Кого: {target.mention_html()}\nСрок: {days} дн.\nПричина: {reason}\nЧат: {message.chat.title}")

@dp.message(Command("banlist"))
async def cmd_banlist(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    bans = ban_list[cid]
    if not bans:
        await reply_auto_delete(message, "👥 Список забаненных пуст."); return
    from datetime import datetime
    lines = [f"👥 <b>Забаненные участники ({len(bans)}):</b>\n"]
    for uid, info in list(bans.items()):
        until = ""
        if info.get("temp") and info.get("until"):
            dt = datetime.fromtimestamp(info["until"]).strftime("%d.%m.%Y %H:%M")
            until = f" (до {dt})"
        lines.append(
            f"{'─'*18}\n"
            f"👤 <b>{info['name']}</b> (<code>{uid}</code>)\n"
            f"📝 Причина: {info['reason']}\n"
            f"👮 Кто: {info['by']}\n"
            f"🕐 {info['time']}{until}"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n\n<i>...список обрезан</i>"
    await reply_auto_delete(message, text, parse_mode="HTML")

@dp.message(Command("modexport"))
async def cmd_modexport(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    target_uid = None
    if message.reply_to_message:
        target_uid = message.reply_to_message.from_user.id

    from datetime import datetime
    import io
    lines = [f"=== ИСТОРИЯ МОДЕРАЦИЙ | {message.chat.title} ===",
             f"Экспорт: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"]

    if target_uid:
        history = mod_history[cid].get(target_uid, [])
        lines.append(f"--- Участник ID {target_uid} ({len(history)} записей) ---")
        for e in history:
            lines.append(f"[{e['time']}] {e['action']} | Причина: {e['reason']} | Модератор: {e['by']}")
    else:
        total = 0
        for uid, history in mod_history[cid].items():
            if not history: continue
            lines.append(f"\n--- ID {uid} ({len(history)} записей) ---")
            for e in history:
                lines.append(f"[{e['time']}] {e['action']} | Причина: {e['reason']} | Модератор: {e['by']}")
            total += len(history)
        lines.append(f"\nИтого записей: {total}")

    content = "\n".join(lines).encode("utf-8")
    file = io.BytesIO(content)
    file.name = f"modhistory_{cid}_{datetime.now().strftime('%Y%m%d')}.txt"
    from aiogram.types import BufferedInputFile
    sent = await message.reply_document(
        BufferedInputFile(content, filename=file.name),
        caption="🧾 История модераций экспортирована")
    asyncio.create_task(auto_delete(message, sent))

@dp.message(Command("modtop"))
async def cmd_modtop(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    stats = mod_stats[cid]
    if not stats:
        await reply_auto_delete(message, "👮 Пока никто ничего не модерировал."); return
    sorted_mods = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    lines = ["👮 <b>Топ модераторов:</b>\n"]
    for i, (name, count) in enumerate(sorted_mods[:10]):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{medal} <b>{name}</b> — {count} действий")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

def kb_warn_templates(target_id: int) -> InlineKeyboardMarkup:
    rows = []
    items = list(WARN_TEMPLATES.items())
    for i in range(0, len(items), 2):
        row = []
        for tid, tmpl in items[i:i+2]:
            row.append(InlineKeyboardButton(
                text=tmpl["label"],
                callback_data=f"wt:{tid}:{target_id}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"wt:cancel:{target_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("warnmenu"))
async def cmd_warnmenu(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя варнить администратора!"); return
    sent = await message.reply(
        f"📝 <b>Шаблон предупреждения для {target.mention_html()}</b>\n\n"
        f"Выбери причину — варн выдастся автоматически:",
        parse_mode="HTML",
        reply_markup=kb_warn_templates(target.id))
    asyncio.create_task(auto_delete(message, sent))

@dp.callback_query(F.data.startswith("wt:"))
async def cb_warn_template(call: CallbackQuery):
    parts = call.data.split(":")
    tid, target_id = parts[1], int(parts[2])
    if tid == "cancel":
        await call.message.delete(); await call.answer(); return
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    tmpl = WARN_TEMPLATES.get(tid)
    if not tmpl:
        await call.answer("❌ Шаблон не найден!", show_alert=True); return
    cid = call.message.chat.id
    reason = tmpl["text"]
    clean_expired_warns(cid, target_id)
    add_warn_with_expiry(cid, target_id)
    count = warnings[cid][target_id]
    save_data()
    add_mod_history(cid, target_id, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, call.from_user.full_name)
    await dm_warn_user(target_id, f"ID{target_id}", reason, call.message.chat.title,
                       f"⚡ Варн {count}/{MAX_WARNINGS}", call.from_user.full_name)
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, target_id)
        warn_expiry[cid][target_id].clear(); warnings[cid][target_id] = 0
        await call.message.edit_text(
            f"🔨 <b>Автобан!</b> Достигнут лимит {MAX_WARNINGS} варнов.\n📝 {reason}",
            parse_mode="HTML")
        await log_action(f"🔨 <b>АВТОБАН</b> (шаблон)\nКого: <code>{target_id}</code>\nПричина: {reason}\nЧат: {call.message.chat.title}")
    else:
        await call.message.edit_text(
            f"⚡ <b>Варн выдан!</b> {tmpl['label']}\n"
            f"📝 {reason}\n"
            f"⚠️ Варнов: <b>{count}/{MAX_WARNINGS}</b>",
            parse_mode="HTML")
        await log_action(f"⚡ <b>ВАРН</b> (шаблон)\nКто: {call.from_user.mention_html()}\nКому: <code>{target_id}</code>\nПричина: {reason}\nЧат: {call.message.chat.title}")
    asyncio.create_task(auto_delete(call.message))
    await call.answer(f"✅ {tmpl['label']}")

@dp.message(Command("rep"))
async def cmd_rep(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    score  = reputation[message.chat.id].get(target.id, 0)
    await reply_auto_delete(message, 
        f"{'🌟' if score>=0 else '💀'} Репутация {target.mention_html()}: <b>{score:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"+1", "+", "👍"}))
async def rep_plus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await reply_auto_delete(message, "😏 Себе репу не накручивай!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await reply_auto_delete(message, f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] += 1
    save_data()
    await reply_auto_delete(message, 
        f"⬆️ {target.mention_html()} +1 к репутации! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"-1", "👎"}))
async def rep_minus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await reply_auto_delete(message, "😏 Себе репу не снижай!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await reply_auto_delete(message, f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] -= 1
    save_data()
    await reply_auto_delete(message, 
        f"⬇️ {target.mention_html()} -1 к репутации! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>",
        parse_mode="HTML")

@dp.message(Command("ранг"))
async def cmd_rank(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid, cid = user.id, message.chat.id
    xp = xp_data[cid][uid]; lvl = levels[cid][uid]
    xp_current = xp % 100
    bar = "🟩" * (xp_current // 10) + "⬜" * (10 - xp_current // 10)
    title = (
        "👑 Элита" if lvl >= 20 else "🏆 Легенда" if lvl >= 10 else
        "⚔️ Ветеран" if lvl >= 5 else "🌱 Активный" if lvl >= 3 else
        "🔰 Участник" if lvl >= 1 else "🐣 Новичок")
    await reply_auto_delete(message, 
        f"📊 <b>Уровень {user.mention_html()}</b>\n\n"
        f"🏅 Титул: <b>{title}</b>\n⚡ Уровень: <b>{lvl}</b>\n"
        f"✨ Опыт: <b>{xp_current}/100</b>\n[{bar}]", parse_mode="HTML")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats: await reply_auto_delete(message, "📊 Статистика пока пуста!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = ["🏆 <b>Топ активных участников:</b>\n"]
    for i, (uid, count) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {count} сообщений")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("quote"))
async def cmd_quote(message: Message):
    await reply_auto_delete(message, f"📖 {random.choice(QUOTES)}")

@dp.message(Command("roll"))
async def cmd_roll(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 10000))
    except: sides = 6
    await reply_auto_delete(message, f"🎲 Бросаю d{sides}... выпало: <b>{random.randint(1,sides)}</b>!", parse_mode="HTML")

@dp.message(Command("flip"))
async def cmd_flip(message: Message):
    await reply_auto_delete(message, random.choice(["🦅 Орёл!", "🪙 Решка!"]))

@dp.message(Command("8ball"))
async def cmd_8ball(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "❓ /8ball [вопрос]"); return
    await reply_auto_delete(message, 
        f"🎱 <b>Вопрос:</b> {command.args}\n\n<b>Ответ:</b> {random.choice(BALL_ANSWERS)}", parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "⚠️ /rate [что]"); return
    score = random.randint(0, 10)
    await reply_auto_delete(message, 
        f"⭐ <b>{command.args}</b>\n{'🌟'*score+'☆'*(10-score)}\nОценка: <b>{score}/10</b>", parse_mode="HTML")

@dp.message(Command("iq"))
async def cmd_iq(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    iq   = random.randint(1, 200)
    c    = "🥔 Картошка умнее." if iq<70 else ("🐒 Обезьяна лучше." if iq<100 else ("😐 Сойдёт." if iq<130 else ("🧠 Умный!" if iq<160 else "🤖 Настоящий гений!")))
    await reply_auto_delete(message, f"🧠 IQ {user.mention_html()}: <b>{iq}</b>\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def cmd_gay(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    p    = random.randint(0, 100)
    await reply_auto_delete(message, 
        f"🌈 {user.mention_html()}\n{'🟣'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка 😄",
        parse_mode="HTML")

@dp.message(Command("truth"))
async def cmd_truth(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"🤔 <b>Вопрос для {user.mention_html()}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML")

@dp.message(Command("dare"))
async def cmd_dare(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"😈 <b>Задание для {user.mention_html()}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML")

@dp.message(Command("wyr"))
async def cmd_wyr(message: Message):
    await reply_auto_delete(message, f"🎯 <b>Выбор без выбора:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML")

@dp.message(Command("rps"))
async def cmd_rps(message: Message, command: CommandObject):
    choices = {"к":"✊ Камень","н":"✌️ Ножницы","б":"🖐 Бумага"}
    wins    = {"к":"н","н":"б","б":"к"}
    if not command.args or command.args.lower() not in choices:
        await reply_auto_delete(message, "✊ /rps к — камень, /rps н — ножницы, /rps б — бумага"); return
    p = command.args.lower(); b = random.choice(list(choices.keys()))
    res = "🤝 Ничья!" if p==b else ("🎉 Ты выиграл!" if wins[p]==b else "😈 Я выиграл!")
    await reply_auto_delete(message, f"Ты: {choices[p]}\nЯ: {choices[b]}\n\n{res}")

@dp.message(Command("slot"))
async def cmd_slot(message: Message):
    symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
    s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
    if s1==s2==s3=="💎":              res = "💰 ДЖЕКПОТ!! Три бриллианта!"
    elif s1==s2==s3:                  res = f"🎉 Три {s1}! Выиграл!"
    elif s1==s2 or s2==s3 or s1==s3:  res = "😐 Два одинаковых. Почти!"
    else:                             res = "😢 Не повезло. Попробуй ещё!"
    await reply_auto_delete(message, f"🎰 [ {s1} | {s2} | {s3} ]\n\n{res}")

@dp.message(Command("choose"))
async def cmd_choose(message: Message, command: CommandObject):
    if not command.args or "|" not in command.args:
        await reply_auto_delete(message, "⚠️ /choose вар1|вар2|вар3"); return
    options = [o.strip() for o in command.args.split("|") if o.strip()]
    if len(options) < 2: await reply_auto_delete(message, "⚠️ Минимум 2 варианта."); return
    await reply_auto_delete(message, f"🎯 Выбираю... ✅ <b>{random.choice(options)}</b>!", parse_mode="HTML")

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    sign, text = random.choice(list(HOROSCOPES.items()))
    await reply_auto_delete(message, f"{sign} <b>Гороскоп для {user.mention_html()}:</b>\n\n{text}", parse_mode="HTML")

@dp.message(Command("predict"))
async def cmd_predict(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"🔮 <b>Предсказание для {user.mention_html()}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML")

@dp.message(Command("совместимость"))
async def cmd_compatibility(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника!"); return
    user1 = message.from_user; user2 = message.reply_to_message.from_user
    if user1.id == user2.id: await reply_auto_delete(message, "😏 Сам с собой? Интересно..."); return
    percent = (user1.id * user2.id) % 101
    bar = "❤️" * (percent // 10) + "🖤" * (10 - percent // 10)
    if percent >= 80:   verdict = "💍 Идеальная пара! Женитесь!"
    elif percent >= 60: verdict = "💕 Хорошая совместимость!"
    elif percent >= 40: verdict = "😊 Неплохо, есть шанс!"
    elif percent >= 20: verdict = "😬 Сложно, но возможно..."
    else:               verdict = "💔 Катастрофа! Держитесь подальше!"
    await reply_auto_delete(message, 
        f"💘 <b>Совместимость:</b>\n\n👤 {user1.mention_html()}\n{bar}\n👤 {user2.mention_html()}\n\n<b>{percent}%</b> — {verdict}",
        parse_mode="HTML")

@dp.message(Command("compliment"))
async def cmd_compliment(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, f"🌸 {user.mention_html()}, {random.choice(COMPLIMENTS)}", parse_mode="HTML")

@dp.message(Command("botstats"))
async def cmd_botstats(message: Message):
    if not await require_admin(message): return
    total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
    total_warns = sum(sum(v.values()) for v in warnings.values())
    await reply_auto_delete(message, 
        f"📈 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
        f"⚡ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
        f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
        f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
        f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML")

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
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника."); return
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
                await reply_auto_delete(message, f"🔨 {tname} забанен на <b>{duration_label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            else:
                await bot.ban_chat_member(cid, target.id)
                await reply_auto_delete(message, f"🔨 {tname} забанен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"🔨 <b>БАН (аутист)</b>\nКто: {message.from_user.mention_html()}\nКого: {tname}\nПричина: {reason}\nЧат: {message.chat.title}")
        elif action == "захуесосить":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await reply_auto_delete(message, f"👢 {tname} захуесошен из чата!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"👢 <b>КИК (аутист)</b>\nКто: {message.from_user.mention_html()}\nКого: {tname}\nЧат: {message.chat.title}")
        elif action == "кик":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await reply_auto_delete(message, f"👟 {tname} кикнут из чата!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"👟 <b>КИК (аутист)</b>\nКто: {message.from_user.mention_html()}\nКого: {tname}\nЧат: {message.chat.title}")
        elif action == "мут":
            mins = duration_mins or 60; label = duration_label or "1 ч."
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            await reply_auto_delete(message, f"🔇 {tname} замучен на <b>{label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"🔇 <b>МУТ (аутист)</b>\nКто: {message.from_user.mention_html()}\nКого: {tname}\nВремя: {label}\nЧат: {message.chat.title}")
        elif action == "мут навсегда":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(can_send_messages=False))
            await reply_auto_delete(message, f"🔇 {tname} замучен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "варн":
            warnings[cid][target.id] += 1; count = warnings[cid][target.id]; save_data()
            if count >= MAX_WARNINGS:
                await bot.ban_chat_member(cid, target.id); warnings[cid][target.id] = 0
                await reply_auto_delete(message, f"🔨 {tname} — {MAX_WARNINGS} варна, автобан!\n📝 Причина: {reason}", parse_mode="HTML")
                await log_action(f"🔨 <b>АВТОБАН (аутист)</b>\nКто: {message.from_user.mention_html()}\nКого: {tname}\nЧат: {message.chat.title}")
            else:
                await reply_auto_delete(message, f"⚡ {tname} получил варн <b>{count}/{MAX_WARNINGS}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
                await log_action(f"⚡ <b>ВАРН (аутист)</b>\nКто: {message.from_user.mention_html()}\nКого: {tname}\nПричина: {reason}\nЧат: {message.chat.title}")
        elif action in ("снять варн", "снятьварн"):
            if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1
            await reply_auto_delete(message, f"🌿 С {tname} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "разбан":
            await bot.unban_chat_member(cid, target.id, only_if_banned=True)
            await reply_auto_delete(message, f"🕊 {tname} разбанен.", parse_mode="HTML")
        elif action == "размут":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            await reply_auto_delete(message, f"🔊 {tname} размучен.", parse_mode="HTML")
        elif action == "удалить":
            try: await message.reply_to_message.delete(); await reply_auto_delete(message, "🗑 Сообщение удалено!")
            except: await reply_auto_delete(message, "⚠️ Не удалось удалить сообщение.")
        elif action == "закрепить":
            try: await bot.pin_chat_message(cid, message.reply_to_message.message_id); await reply_auto_delete(message, "📌 Сообщение закреплено!")
            except: await reply_auto_delete(message, "⚠️ Не удалось закрепить сообщение.")
        elif action == "предупредить":
            text_warn = rest.strip() or "Нарушение правил"
            await reply_auto_delete(message, f"⚠️ Внимание {tname}!\n📝 {text_warn}", parse_mode="HTML")
        elif action == "очистить":
            count = duration_mins or 10; deleted = 0
            for i in range(message.message_id, message.message_id - count - 1, -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            await reply_auto_delete(message, f"🧹 Удалено <b>{deleted}</b> сообщений!", parse_mode="HTML")
        elif action == "инфо":
            member = await bot.get_chat_member(cid, target.id)
            smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
                    "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
            username = f"@{target.username}" if target.username else "нет"
            await reply_auto_delete(message, 
                f"🔍 <b>Инфо:</b>\n{tname}\n🔗 Юзернейм: <b>{username}</b>\n"
                f"🪪 ID: <code>{target.id}</code>\n📌 {smap.get(member.status, member.status)}\n"
                f"⚡ Варнов: <b>{warnings[cid].get(target.id,0)}/{MAX_WARNINGS}</b>\n"
                f"🌟 Репутация: <b>{reputation[cid].get(target.id,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(target.id,0)}</b>", parse_mode="HTML")
        elif action == "варны":
            count = warnings[cid].get(target.id, 0)
            await reply_auto_delete(message, f"⚡ Варнов у {tname}: <b>{count}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "репутация":
            rep = reputation[cid].get(target.id, 0)
            await reply_auto_delete(message, f"🌟 Репутация {tname}: <b>{rep:+d}</b>", parse_mode="HTML")
        elif action == "обозвать":
            обзывалки = ["🤡 клоун","🥴 тупица","🐸 лягушка","🦆 утка","🐷 хрюша","🤪 псих",
                         "🦊 хитрая лиса","🐌 улитка","🦄 единорог","🤖 сломанный робот","🥔 картошка","🧟 зомби"]
            await reply_auto_delete(message, f"😂 {tname} отныне ты — <b>{random.choice(обзывалки)}</b>!", parse_mode="HTML")
        elif action == "поженить":
            await reply_auto_delete(message, 
                f"💍 Объявляю вас мужем и женой!\n\n👰 {target.mention_html()}\n🤵 {message.from_user.mention_html()}\n\n💑 Горько! 🥂",
                parse_mode="HTML")
        elif action == "казнить":
            казни = ["🔥 сожжён на костре","⚡ поражён молнией","🐊 съеден крокодилом",
                     "🍌 подавился бананом","🚀 отправлен в космос без скафандра",
                     "🌊 утоплен в стакане воды","🐝 закусан пчёлами",
                     "🎸 заслушан до смерти Шансоном","🥄 побит ложкой","🌵 упал на кактус"]
            await reply_auto_delete(message, 
                f"⚰️ {tname} приговорён к казни!\n💀 Способ: <b>{random.choice(казни)}</b>", parse_mode="HTML")
        elif action == "диагноз":
            диагнозы = ["🧠 Хроническая адекватность","🤡 Острый клоунизм","😴 Синдром вечного AFK",
                        "🥔 Картофельный синдром","🐒 Обезьяний рефлекс","💤 Хроническая сонливость",
                        "🌵 Колючесть характера","🤖 Роботизация мозга","🦆 Утиная походка","🌈 Радужное мышление"]
            await reply_auto_delete(message, f"🏥 Диагноз для {tname}:\n📋 <b>{random.choice(диагнозы)}</b>", parse_mode="HTML")
        elif action == "профессия":
            профессии = ["🤡 Профессиональный клоун","🥔 Картофелевод","🐒 Дрессировщик обезьян",
                         "🌵 Смотритель кактусов","🦆 Переводчик с утиного","🤖 Ремонтник роботов",
                         "💤 Профессиональный соня","🎸 Игрок на банджо","🌈 Художник радуг","🧠 Продавец мозгов"]
            await reply_auto_delete(message, f"💼 Профессия {tname}:\n<b>{random.choice(профессии)}</b>", parse_mode="HTML")
        elif action == "похитить":
            mins = duration_mins or 5
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            await reply_auto_delete(message, 
                f"👽 {tname} похищен пришельцами на <b>{mins} мин</b>!\n🛸 Вернётся через {mins} минут...",
                parse_mode="HTML")
        elif action == "дуэль":
            challenger = message.from_user
            winner = random.choice([challenger, target])
            loser = target if winner == challenger else challenger
            await reply_auto_delete(message, 
                f"⚔️ <b>ДУЭЛЬ!</b>\n\n🔫 {challenger.mention_html()} vs {tname}\n\n"
                f"🏆 Победитель: <b>{winner.mention_html()}</b>\n💀 Проигравший: {loser.mention_html()}",
                parse_mode="HTML")
        elif action == "экзамен":
            вопросы = ["🧠 Сколько будет 2+2*2?","🌍 Какая столица Франции?",
                       "🔢 Назови простое число от 10 до 20.","🐘 Какое животное самое большое на суше?",
                       "🌊 Какой самый глубокий океан?","🎨 Смешай красный и синий — какой цвет получится?",
                       "⚡ Кто придумал лампочку?","🦁 Царь зверей — это?",
                       "🌙 Как называется спутник Земли?","🍎 Какой фрукт упал на Ньютона?"]
            await reply_auto_delete(message, 
                f"📝 <b>ЭКЗАМЕН для {tname}!</b>\n\n{random.choice(вопросы)}\n\n⏰ У тебя <b>30 секунд</b>!",
                parse_mode="HTML")
        elif action == "проверить":
            await reply_auto_delete(message, f"ℹ️ Функция проверки (капча) отключена.", parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message, f"⚠️ Ошибка: {e}")

# ===== УГАДАЙ ЧИСЛО =====
guess_games = {}

@dp.message(Command("guess"))
async def cmd_guess(message: Message):
    cid = message.chat.id
    number = random.randint(1, 100)
    guess_games[cid] = {"number": number, "attempts": 0}
    await reply_auto_delete(message, 
        f"🎯 <b>Угадай число!</b>\n\n"
        f"🔢 Загадал число от <b>1 до 100</b>\n"
        f"💬 Просто напиши число в чат!\n"
        f"⚡ Есть <b>10 попыток</b>",
        parse_mode="HTML")

@dp.message(F.text.regexp(r'^\d+$'))
async def guess_handler(message: Message):
    cid = message.chat.id
    if cid not in guess_games: return
    game = guess_games[cid]
    try: num = int(message.text)
    except: return
    if num < 1 or num > 100: return
    game["attempts"] += 1
    if num == game["number"]:
        del guess_games[cid]
        await reply_auto_delete(message, 
            f"🎉 {message.from_user.mention_html()} угадал за <b>{game['attempts']}</b> попыток!\n"
            f"✅ Число было <b>{num}</b>!", parse_mode="HTML")
    elif game["attempts"] >= 10:
        n = game["number"]; del guess_games[cid]
        await reply_auto_delete(message, f"😢 Попытки кончились! Загадано было <b>{n}</b>.", parse_mode="HTML")
    elif num < game["number"]:
        await reply_auto_delete(message, f"⬆️ Больше! Попытка {game['attempts']}/10")
    else:
        await reply_auto_delete(message, f"⬇️ Меньше! Попытка {game['attempts']}/10")

# ===== АСК =====
ask_targets = {}

@dp.message(Command("ask"))
async def cmd_ask(message: Message):
    if message.chat.type == "private":
        await reply_auto_delete(message, "❓ Эту команду используй в чате!"); return
    ask_targets[message.from_user.id] = {"chat_id": message.chat.id, "name": message.from_user.full_name}
    await reply_auto_delete(message, 
        f"📬 <b>{message.from_user.mention_html()} открыл АСК!</b>\n\n"
        f"🔒 Напиши боту в личку анонимный вопрос!\n"
        f"👉 @AllAnonandbot", parse_mode="HTML")

@dp.message(Command("askoff"))
async def cmd_askoff(message: Message):
    if message.from_user.id in ask_targets:
        del ask_targets[message.from_user.id]
        await reply_auto_delete(message, "📭 АСК закрыт!")
    else:
        await reply_auto_delete(message, "❌ У тебя не открыт АСК.")

@dp.message(Command("send"))
async def cmd_send_ask(message: Message, command: CommandObject):
    if message.chat.type != "private":
        await reply_auto_delete(message, "📩 Эту команду используй в личке с ботом!"); return
    if not command.args:
        await reply_auto_delete(message, "❓ Формат: /send @юзернейм текст", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    if len(parts) < 2:
        await reply_auto_delete(message, "⚠️ Укажи юзернейм и текст вопроса!"); return
    username = parts[0].replace("@", "").lower()
    text = parts[1].strip()
    target_id = None
    for uid, data in ask_targets.items():
        try:
            chat_member = await bot.get_chat_member(data["chat_id"], uid)
            if chat_member.user.username and chat_member.user.username.lower() == username:
                target_id = uid; break
        except: pass
    if not target_id:
        await reply_auto_delete(message, "❌ Этот пользователь не открыл АСК или не найден."); return
    data = ask_targets[target_id]
    try:
        await bot.send_message(data["chat_id"],
            f"📬 <b>Анонимный вопрос для {data['name']}:</b>\n\n💬 {text}\n\n<i>Ответь командой /reply</i>",
            parse_mode="HTML")
        await reply_auto_delete(message, "✅ Вопрос отправлен анонимно!")
    except:
        await reply_auto_delete(message, "⚠️ Не удалось отправить вопрос.")

# ===== МВП =====
mvp_votes = {}
mvp_voted = {}

@dp.message(Command("mvp"))
async def cmd_mvp(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника чтобы проголосовать за МВП!"); return
    voter = message.from_user; target = message.reply_to_message.from_user; cid = message.chat.id
    if target.id == voter.id:
        await reply_auto_delete(message, "😏 За себя голосовать нельзя!"); return
    if cid not in mvp_voted: mvp_voted[cid] = {}
    if voter.id in mvp_voted[cid]:
        await reply_auto_delete(message, "⏳ Ты уже голосовал сегодня за МВП!"); return
    mvp_voted[cid][voter.id] = True
    if cid not in mvp_votes: mvp_votes[cid] = {}
    mvp_votes[cid][target.id] = mvp_votes[cid].get(target.id, 0) + 1
    votes = mvp_votes[cid][target.id]
    await reply_auto_delete(message, 
        f"⭐ {voter.mention_html()} проголосовал за <b>МВП</b>!\n\n"
        f"🏆 {target.mention_html()}\n👍 Голосов: <b>{votes}</b>", parse_mode="HTML")

@dp.message(Command("mvpstats"))
async def cmd_mvpstats(message: Message):
    cid = message.chat.id
    if cid not in mvp_votes or not mvp_votes[cid]:
        await reply_auto_delete(message, "📊 Голосов ещё нет!"); return
    sorted_mvp = sorted(mvp_votes[cid].items(), key=lambda x: x[1], reverse=True)[:5]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    lines = ["🏆 <b>Топ МВП:</b>\n"]
    for i, (uid, votes) in enumerate(sorted_mvp):
        try: m = await bot.get_chat_member(cid, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {votes} голосов")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("mvpreset"))
async def cmd_mvpreset(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    mvp_votes[cid] = {}; mvp_voted[cid] = {}
    await reply_auto_delete(message, "🔄 МВП голосование сброшено!")

# ===== CONFESSION =====
@dp.message(Command("confession"))
async def cmd_confession(message: Message, command: CommandObject):
    if not command.args:
        await reply_auto_delete(message, 
            "💌 Формат: /confession текст\n"
            "Пример: <code>/confession Я влюблён в одного человека в этом чате...</code>",
            parse_mode="HTML"); return
    text = command.args.strip()
    if len(text) < 5:
        await reply_auto_delete(message, "⚠️ Слишком короткое сообщение!"); return
    try: await message.delete()
    except: pass
    await bot.send_message(message.chat.id,
        f"💌 <b>Анонимное признание:</b>\n\n<i>{text}</i>\n\n🔒 <i>Автор неизвестен</i>",
        parse_mode="HTML")

# ===== SECRET =====
@dp.message(Command("secret"))
async def cmd_secret(message: Message, command: CommandObject):
    if message.chat.type == "private":
        await reply_auto_delete(message, "❌ Эту команду используй в чате!"); return
    if not command.args or len(command.args.split(maxsplit=1)) < 2:
        await reply_auto_delete(message, 
            "📩 Формат: /secret @юзернейм текст\n"
            "Пример: <code>/secret @username Ты мне нравишься 😊</code>", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    username = parts[0].replace("@", "").lower()
    text = parts[1].strip()
    target = None
    try:
        admins = await bot.get_chat_administrators(message.chat.id)
        members = await bot.get_chat_members_count(message.chat.id)
    except: pass
    async for member in bot.get_chat_members(message.chat.id):
        if member.user.username and member.user.username.lower() == username:
            target = member.user; break
    if not target:
        await reply_auto_delete(message, "❌ Участник не найден в чате!"); return
    try:
        await message.delete()
        await bot.send_message(target.id,
            f"💌 <b>Тебе анонимное сообщение!</b>\n\n<i>{text}</i>\n\n🔒 <i>Автор неизвестен</i>",
            parse_mode="HTML")
        await bot.send_message(message.chat.id, "📩 Анонимное сообщение отправлено!")
    except:
        await reply_auto_delete(message, "⚠️ Не удалось отправить — участник должен написать боту в лс хотя бы раз!")

# ===== КАЗИНО =====
@dp.message(Command("casino"))
async def cmd_casino(message: Message, command: CommandObject):
    cid = message.chat.id
    uid = message.from_user.id
    if not command.args:
        await reply_auto_delete(message, 
            "🎰 Формат: /casino [сумма]\n"
            "Пример: <code>/casino 10</code>\n\n"
            f"💰 Твоя репутация: <b>{reputation[cid].get(uid, 0):+d}</b>",
            parse_mode="HTML"); return
    try:
        bet = int(command.args.strip())
    except:
        await reply_auto_delete(message, "⚠️ Укажи число! Пример: /casino 10"); return
    if bet <= 0:
        await reply_auto_delete(message, "⚠️ Ставка должна быть больше 0!"); return
    current_rep = reputation[cid].get(uid, 0)
    if current_rep < bet:
        await reply_auto_delete(message, 
            f"💸 Недостаточно репутации!\n"
            f"💰 У тебя: <b>{current_rep:+d}</b>",
            parse_mode="HTML"); return
    symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
    s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
    if s1==s2==s3=="💎":
        mult = 5; res = "💰 ДЖЕКПОТ!! Три бриллианта!"
    elif s1==s2==s3:
        mult = 3; res = f"🎉 Три {s1}! Выиграл x3!"
    elif s1==s2 or s2==s3 or s1==s3:
        mult = 2; res = "😊 Два одинаковых! Выиграл x2!"
    else:
        mult = 0; res = "😢 Не повезло! Проиграл!"
    if mult > 0:
        win = bet * mult
        reputation[cid][uid] = current_rep + win
        result = f"✅ +{win} к репутации!"
    else:
        reputation[cid][uid] = current_rep - bet
        result = f"❌ -{bet} к репутации!"
    save_data()
    await reply_auto_delete(message, 
        f"🎰 [ {s1} | {s2} | {s3} ]\n\n"
        f"{res}\n{result}\n\n"
        f"💰 Репутация: <b>{reputation[cid][uid]:+d}</b>",
        parse_mode="HTML")

# ===== ДУЭЛИ =====
duel_requests = {}

@dp.message(Command("duel"))
async def cmd_duel(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника для дуэли!"); return
    challenger = message.from_user; target = message.reply_to_message.from_user; cid = message.chat.id
    if target.id == challenger.id:
        await reply_auto_delete(message, "😏 Сам с собой дуэль?"); return
    if target.is_bot:
        await reply_auto_delete(message, "🤖 С ботом не подерёшься!"); return
    try: bet = int(command.args) if command.args else 10
    except: bet = 10
    if bet <= 0: bet = 10
    if reputation[cid].get(challenger.id, 0) < bet:
        await reply_auto_delete(message, f"💸 Недостаточно репутации! У тебя: <b>{reputation[cid].get(challenger.id, 0):+d}</b>", parse_mode="HTML"); return
    if reputation[cid].get(target.id, 0) < bet:
        await reply_auto_delete(message, f"💸 У {target.mention_html()} недостаточно репутации!", parse_mode="HTML"); return
    duel_requests[cid] = {
        "challenger_id": challenger.id, "challenger_name": challenger.full_name,
        "target_id": target.id, "target_name": target.full_name, "bet": bet
    }
    await reply_auto_delete(message, 
        f"⚔️ <b>ВЫЗОВ НА ДУЭЛЬ!</b>\n\n"
        f"🔫 {challenger.mention_html()} вызывает {target.mention_html()}!\n"
        f"💰 Ставка: <b>{bet}</b> репутации\n\n"
        f"✅ {target.mention_html()}, напиши <b>/accept</b> чтобы принять!\n"
        f"❌ Или <b>/decline</b> чтобы отказаться!",
        parse_mode="HTML")

@dp.message(Command("accept"))
async def cmd_accept(message: Message):
    cid = message.chat.id; uid = message.from_user.id
    if cid not in duel_requests:
        await reply_auto_delete(message, "❌ Нет активных дуэлей!"); return
    duel = duel_requests[cid]
    if uid != duel["target_id"]:
        await reply_auto_delete(message, "❌ Это не твоя дуэль!"); return
    winner_id = random.choice([duel["challenger_id"], duel["target_id"]])
    loser_id = duel["target_id"] if winner_id == duel["challenger_id"] else duel["challenger_id"]
    winner_name = duel["challenger_name"] if winner_id == duel["challenger_id"] else duel["target_name"]
    loser_name = duel["target_name"] if winner_id == duel["challenger_id"] else duel["challenger_name"]
    bet = duel["bet"]
    reputation[cid][winner_id] += bet
    reputation[cid][loser_id] -= bet
    save_data()
    del duel_requests[cid]
    await reply_auto_delete(message, 
        f"⚔️ <b>ДУЭЛЬ!</b>\n\n"
        f"🔫 {duel['challenger_name']} vs {duel['target_name']}\n\n"
        f"🏆 Победитель: <b>{winner_name}</b> +{bet} репутации!\n"
        f"💀 Проигравший: <b>{loser_name}</b> -{bet} репутации!",
        parse_mode="HTML")
    await log_action(
        f"⚔️ <b>ДУЭЛЬ</b>\n"
        f"🏆 Победитель: <b>{winner_name}</b> +{bet}\n"
        f"💀 Проигравший: <b>{loser_name}</b> -{bet}\n"
        f"Чат: {message.chat.title}")

@dp.message(Command("decline"))
async def cmd_decline(message: Message):
    cid = message.chat.id; uid = message.from_user.id
    if cid not in duel_requests:
        await reply_auto_delete(message, "❌ Нет активных дуэлей!"); return
    duel = duel_requests[cid]
    if uid != duel["target_id"]:
        await reply_auto_delete(message, "❌ Это не твоя дуэль!"); return
    del duel_requests[cid]
    await reply_auto_delete(message, f"🏳 {message.from_user.mention_html()} отказался от дуэли!", parse_mode="HTML")

@dp.message(Command("streak"))
async def cmd_streak(message: Message):
    from datetime import datetime
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid = user.id; cid = message.chat.id
    streak = streaks[cid][uid]
    last_date = streak_dates[cid][uid]
    await reply_auto_delete(message, 
        f"🔥 <b>Серия активности {user.mention_html()}:</b>\n\n"
        f"📅 Дней подряд: <b>{streak}</b>\n"
        f"📆 Последний день: <b>{last_date or 'нет данных'}</b>\n\n"
        f"💬 Пиши каждый день чтобы серия росла!",
        parse_mode="HTML")

@dp.message(Command("toprep"))
async def cmd_toprep(message: Message):
    rep = reputation[message.chat.id]
    if not rep:
        await reply_auto_delete(message, "📊 Репутация пока пуста!"); return
    sorted_u = sorted(rep.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["🌟 <b>Топ по репутации:</b>\n"]
    for i, (uid, score) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        icon = "⬆️" if score >= 0 else "⬇️"
        lines.append(f"{medals[i]} <b>{uname}</b> — {icon} <b>{score:+d}</b>")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid, cid = user.id, message.chat.id
    xp = xp_data[cid][uid]; lvl = levels[cid][uid]
    xp_current = xp % 100
    bar = "🟩" * (xp_current // 10) + "⬜" * (10 - xp_current // 10)
    rep = reputation[cid].get(uid, 0)
    warns = warnings[cid].get(uid, 0)
    msgs = chat_stats[cid].get(uid, 0)
    streak = streaks[cid][uid]
    title = (
        "👑 Элита" if lvl >= 20 else "🏆 Легенда" if lvl >= 10 else
        "⚔️ Ветеран" if lvl >= 5 else "🌱 Активный" if lvl >= 3 else
        "🔰 Участник" if lvl >= 1 else "🐣 Новичок")
    shop_title = user_titles[uid].get("title")
    title_line = f"🎭 Титул магазина: <b>{shop_title}</b>\n" if shop_title else ""
    reasons = mod_reasons[cid].get(uid, {})
    mute_reason = f"🔇 Последний мут: <i>{reasons['mute']}</i>\n" if reasons.get("mute") else ""
    ban_reason  = f"🔨 Последний бан: <i>{reasons['ban']}</i>\n"  if reasons.get("ban")  else ""
    await reply_auto_delete(message,
        f"👤 <b>Профиль {user.mention_html()}</b>\n\n"
        f"🏅 Уровень: <b>{title}</b> (lvl {lvl})\n"
        f"✨ Опыт: <b>{xp_current}/100</b>\n[{bar}]\n\n"
        f"{title_line}"
        f"🌟 Репутация: <b>{rep:+d}</b>\n💬 Сообщений: <b>{msgs}</b>\n"
        f"🔥 Серия: <b>{streak}</b> дней\n⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
        f"{mute_reason}{ban_reason}",
        parse_mode="HTML")

@dp.message(Command("addrep"))
async def cmd_addrep(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID: return
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        target = message.from_user
    try: amount = int(command.args or 100)
    except: amount = 100
    cid = message.chat.id
    reputation[cid][target.id] += amount
    save_data()
    await reply_auto_delete(message, 
        f"✅ {target.mention_html()} добавлено <b>{amount}</b> репутации!\n"
        f"🌟 Теперь: <b>{reputation[cid][target.id]:+d}</b>",
        parse_mode="HTML")

# ===== ЕЖЕДНЕВНЫЙ БОНУС =====
daily_claimed = {}

@dp.message(Command("daily"))
async def cmd_daily(message: Message):
    uid = message.from_user.id; cid = message.chat.id
    from datetime import datetime
    today = datetime.now().strftime("%d.%m.%Y")
    key = (cid, uid)
    if daily_claimed.get(key) == today:
        await reply_auto_delete(message, 
            f"⏳ Ты уже забрал ежедневный бонус!\n"
            f"🔄 Приходи завтра.", parse_mode="HTML"); return
    streak = streaks[cid][uid]
    bonus = 10 + min(streak * 2, 40)
    daily_claimed[key] = today
    reputation[cid][uid] += bonus; save_data()
    await reply_auto_delete(message, 
        f"🎁 <b>Ежедневный бонус!</b>\n\n"
        f"🌟 +{bonus} репутации\n"
        f"🔥 Серия: <b>{streak}</b> дней\n"
        f"💰 Всего репутации: <b>{reputation[cid][uid]:+d}</b>\n\n"
        f"{'🎉 Бонус за серию +' + str(min(streak*2,40)) + '!' if streak > 1 else '💡 Заходи каждый день для бонуса!'}",
        parse_mode="HTML")

# ===== ТУРНИРЫ =====
tournament_data = {}  # {cid: {"active": bool, "participants": [], "bracket": [], "round": 0}}

@dp.message(Command("tournament"))
async def cmd_tournament(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    sub = command.args.strip().lower() if command.args else ""
    if sub == "start":
        if cid in tournament_data and tournament_data[cid].get("active"):
            await reply_auto_delete(message, "⚠️ Турнир уже идёт!"); return
        tournament_data[cid] = {"active": False, "registration": True, "participants": [], "round": 0}
        await reply_auto_delete(message, 
            f"🎪 <b>ТУРНИР ОТКРЫТ!</b>\n\n"
            f"📝 Пиши /join чтобы записаться!\n"
            f"🚀 Админ запустит турнир командой /tournament begin",
            parse_mode="HTML")
    elif sub == "begin":
        if cid not in tournament_data:
            await reply_auto_delete(message, "❌ Сначала открой регистрацию: /tournament start"); return
        parts = tournament_data[cid]["participants"]
        if len(parts) < 2:
            await reply_auto_delete(message, "⚠️ Нужно минимум 2 участника!"); return
        random.shuffle(parts)
        tournament_data[cid]["active"] = True
        tournament_data[cid]["registration"] = False
        names = "\n".join([f"• {p['name']}" for p in parts])
        await reply_auto_delete(message, 
            f"🎪 <b>ТУРНИР НАЧАЛСЯ!</b>\n\n"
            f"👥 Участников: <b>{len(parts)}</b>\n\n{names}\n\n"
            f"⚔️ Запускаем первый раунд с /tournament next!", parse_mode="HTML")
    elif sub == "next":
        if cid not in tournament_data or not tournament_data[cid].get("active"):
            await reply_auto_delete(message, "❌ Нет активного турнира!"); return
        parts = tournament_data[cid]["participants"]
        if len(parts) == 1:
            winner = parts[0]
            reputation[message.chat.id][winner["id"]] += 50; save_data()
            del tournament_data[cid]
            await reply_auto_delete(message, 
                f"🏆 <b>ПОБЕДИТЕЛЬ ТУРНИРА:</b>\n\n"
                f"👑 <b>{winner['name']}</b>\n🌟 +50 репутации!", parse_mode="HTML")
            return
        random.shuffle(parts)
        results = []; survivors = []
        for i in range(0, len(parts) - 1, 2):
            a = parts[i]; b = parts[i+1]
            winner = random.choice([a, b])
            loser  = b if winner == a else a
            survivors.append(winner)
            results.append(f"⚔️ {a['name']} vs {b['name']} → 🏆 <b>{winner['name']}</b>")
        if len(parts) % 2 == 1:
            bye = parts[-1]; survivors.append(bye)
            results.append(f"🎟 {bye['name']} — проходит автоматически")
        tournament_data[cid]["participants"] = survivors
        tournament_data[cid]["round"] += 1
        rnd = tournament_data[cid]["round"]
        await reply_auto_delete(message, 
            f"🎪 <b>Раунд {rnd} завершён!</b>\n\n" + "\n".join(results) +
            f"\n\n👥 Осталось: <b>{len(survivors)}</b>\n"
            f"{'⚔️ /tournament next для следующего раунда' if len(survivors) > 1 else '🏆 /tournament next для финала'}",
            parse_mode="HTML")
    elif sub == "stop":
        if cid in tournament_data:
            del tournament_data[cid]
            await reply_auto_delete(message, "🛑 Турнир отменён.")
    else:
        await reply_auto_delete(message, 
            "🎪 <b>Управление турниром:</b>\n\n"
            "/tournament start — открыть регистрацию\n"
            "/tournament begin — начать турнир\n"
            "/tournament next — следующий раунд\n"
            "/tournament stop — отменить", parse_mode="HTML")

@dp.message(Command("join"))
async def cmd_join(message: Message):
    cid = message.chat.id; uid = message.from_user.id
    if cid not in tournament_data or not tournament_data[cid].get("registration"):
        await reply_auto_delete(message, "❌ Регистрация на турнир не открыта!"); return
    parts = tournament_data[cid]["participants"]
    if any(p["id"] == uid for p in parts):
        await reply_auto_delete(message, "✅ Ты уже записан!"); return
    parts.append({"id": uid, "name": message.from_user.full_name})
    await reply_auto_delete(message, 
        f"✅ {message.from_user.mention_html()} записан в турнир!\n"
        f"👥 Участников: <b>{len(parts)}</b>", parse_mode="HTML")

# ===== НЕДЕЛЬНАЯ СТАТИСТИКА =====
async def warn_expiry_checker():
    """Каждые 6 часов чистит истёкшие варны и уведомляет если они сгорели"""
    while True:
        await asyncio.sleep(21600)  # 6 часов
        for cid in list(warn_expiry.keys()):
            for uid in list(warn_expiry[cid].keys()):
                old_count = warnings[cid].get(uid, 0)
                clean_expired_warns(cid, uid)
                new_count = warnings[cid].get(uid, 0)
                if old_count > new_count and new_count == 0:
                    try:
                        await bot.send_message(
                            cid,
                            f"⏳ Варны участника <code>{uid}</code> истекли и сброшены автоматически.",
                            parse_mode="HTML")
                    except: pass

mod_stats = defaultdict(lambda: defaultdict(int))  # {cid: {admin_name: count}}

# ===== ШАБЛОНЫ ПРЕДУПРЕЖДЕНИЙ =====
WARN_TEMPLATES = {
    "1": {"label": "🔞 Контент 18+",      "text": "Нарушение: публикация материалов 18+ без соответствующего разрешения"},
    "2": {"label": "📢 Реклама",           "text": "Нарушение: реклама и самопиар без разрешения администрации"},
    "3": {"label": "💬 Спам",              "text": "Нарушение: спам и флуд в чате"},
    "4": {"label": "🤬 Оскорбления",       "text": "Нарушение: оскорбления участников чата"},
    "5": {"label": "🔗 Ссылки",            "text": "Нарушение: публикация сторонних ссылок без разрешения"},
    "6": {"label": "🚫 Провокации",        "text": "Нарушение: провокации и разжигание конфликтов"},
    "7": {"label": "👤 Личные данные",     "text": "Нарушение: публикация личных данных других участников"},
    "8": {"label": "🤖 Флуд ботами",       "text": "Нарушение: использование ботов и спам-команд"},
    "9": {"label": "🗣 Оффтоп",            "text": "Нарушение: систематический оффтоп и мусор в чате"},
    "10": {"label": "⚠️ Правила",          "text": "Нарушение правил чата"},
}

async def tempban_unban(cid: int, uid: int, uname: str, days: int):
    """Снимает временный бан по истечению"""
    await asyncio.sleep(days * 86400)
    try:
        await bot.unban_chat_member(cid, uid, only_if_banned=True)
        await bot.send_message(
            cid,
            f"🔓 Временный бан <b>{uname}</b> истёк — участник может вернуться.",
            parse_mode="HTML")
        await log_action(
            f"🔓 <b>ТЕМПБАН ИСТЁК</b>\n"
            f"👤 <b>{uname}</b>\n"
            f"⏱ Срок {days} дн. истёк автоматически")
        ban_list[cid].pop(uid, None)
    except: pass
    finally:
        tempban_timers.pop((cid, uid), None)

async def send_weekly_stats():
    while True:
        await asyncio.sleep(604800)  # 7 дней
        for cid, stats in chat_stats.items():
            if not stats: continue
            sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:5]
            medals   = ["🥇","🥈","🥉","4️⃣","5️⃣"]
            lines    = [f"📊 <b>НЕДЕЛЬНАЯ СТАТИСТИКА</b>\n"]
            for i, (uid, cnt) in enumerate(sorted_u):
                try:
                    chat_member = await bot.get_chat_member(cid, uid)
                    uname = chat_member.user.full_name
                except: uname = f"ID {uid}"
                lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщений")
            top_rep = sorted(reputation[cid].items(), key=lambda x: x[1], reverse=True)
            if top_rep:
                try:
                    top_uid = top_rep[0][0]
                    top_member = await bot.get_chat_member(cid, top_uid)
                    lines.append(f"\n🌟 Богач недели: <b>{top_member.user.full_name}</b> ({top_rep[0][1]:+d} реп.)")
                except: pass
            try:
                await bot.send_message(LOG_CHANNEL_ID, "\n".join(lines), parse_mode="HTML")
            except: pass

# ===== МАГАЗИН РЕПУТАЦИИ =====
SHOP_ITEMS = {
    # ===== ДЕШЁВЫЕ (10–100) =====
    "1":  {"name": "🌱 Новичок",         "price": 10,    "type": "title"},
    "2":  {"name": "🐣 Птенец",          "price": 15,    "type": "title"},
    "3":  {"name": "🍃 Росток",          "price": 20,    "type": "title"},
    "4":  {"name": "🌊 Волна",           "price": 30,    "type": "title"},
    "5":  {"name": "🌙 Лунатик",         "price": 40,    "type": "title"},
    "6":  {"name": "🎈 Воздушный",       "price": 50,    "type": "title"},
    "7":  {"name": "🐢 Черепаха",        "price": 60,    "type": "title"},
    "8":  {"name": "🌸 Сакура",          "price": 75,    "type": "title"},
    "9":  {"name": "🌀 Вихрь",           "price": 90,    "type": "title"},
    "10": {"name": "🍀 Удачливый",       "price": 100,   "type": "title"},
    "11": {"name": "🐸 Лягушонок",       "price": 10,    "type": "title"},
    "12": {"name": "🌵 Кактус",          "price": 20,    "type": "title"},
    "13": {"name": "🍄 Грибок",          "price": 25,    "type": "title"},
    "14": {"name": "🦆 Утка",            "price": 35,    "type": "title"},
    "15": {"name": "🐧 Пингвин",         "price": 45,    "type": "title"},
    "16": {"name": "🌻 Подсолнух",       "price": 55,    "type": "title"},
    "17": {"name": "🍉 Арбуз",           "price": 65,    "type": "title"},
    "18": {"name": "🦋 Бабочка",         "price": 80,    "type": "title"},
    "19": {"name": "🐠 Рыбка",           "price": 95,    "type": "title"},
    "20": {"name": "🌺 Цветок",          "price": 100,   "type": "title"},
    # ===== СРЕДНИЕ (120–500) =====
    "21": {"name": "⚡ Активист",        "price": 150,   "type": "title"},
    "22": {"name": "🌈 Радуга",          "price": 200,   "type": "title"},
    "23": {"name": "🎭 Анонимус",        "price": 250,   "type": "title"},
    "24": {"name": "🔥 Ветеран",         "price": 300,   "type": "title"},
    "25": {"name": "🧠 Мудрец",          "price": 350,   "type": "title"},
    "26": {"name": "🚀 Космонавт",       "price": 400,   "type": "title"},
    "27": {"name": "🐉 Дракон",          "price": 450,   "type": "title"},
    "28": {"name": "🏹 Охотник",         "price": 500,   "type": "title"},
    "29": {"name": "🎸 Рокер",           "price": 120,   "type": "title"},
    "30": {"name": "🏄 Сёрфер",          "price": 140,   "type": "title"},
    "31": {"name": "🎮 Геймер",          "price": 160,   "type": "title"},
    "32": {"name": "🧙 Маг",             "price": 180,   "type": "title"},
    "33": {"name": "🦊 Лис",             "price": 220,   "type": "title"},
    "34": {"name": "🐺 Волк",            "price": 260,   "type": "title"},
    "35": {"name": "🦅 Орёл",            "price": 280,   "type": "title"},
    "36": {"name": "🏔 Альпинист",       "price": 320,   "type": "title"},
    "37": {"name": "🎪 Циркач",          "price": 370,   "type": "title"},
    "38": {"name": "🧪 Химик",           "price": 420,   "type": "title"},
    "39": {"name": "🕵 Детектив",        "price": 480,   "type": "title"},
    "40": {"name": "🎻 Скрипач",         "price": 490,   "type": "title"},
    # ===== ДОРОГИЕ (600–2000) =====
    "41": {"name": "💎 Элита",           "price": 600,   "type": "title"},
    "42": {"name": "🦁 Царь зверей",     "price": 700,   "type": "title"},
    "43": {"name": "🌟 Звезда",          "price": 800,   "type": "title"},
    "44": {"name": "🎯 Снайпер",         "price": 900,   "type": "title"},
    "45": {"name": "👑 Легенда",         "price": 1000,  "type": "title"},
    "46": {"name": "🔱 Посейдон",        "price": 1200,  "type": "title"},
    "47": {"name": "⚔️ Воитель",         "price": 1500,  "type": "title"},
    "48": {"name": "🌌 Галактика",       "price": 1800,  "type": "title"},
    "49": {"name": "🏆 Чемпион",         "price": 2000,  "type": "title"},
    "50": {"name": "🧬 Учёный",          "price": 650,   "type": "title"},
    "51": {"name": "🎖 Генерал",         "price": 750,   "type": "title"},
    "52": {"name": "🌠 Астронавт",       "price": 850,   "type": "title"},
    "53": {"name": "🔮 Провидец",        "price": 950,   "type": "title"},
    "54": {"name": "🦸 Супергерой",      "price": 1100,  "type": "title"},
    "55": {"name": "🧛 Вампир",          "price": 1300,  "type": "title"},
    "56": {"name": "🤖 Робот",           "price": 1400,  "type": "title"},
    "57": {"name": "🦄 Единорог",        "price": 1600,  "type": "title"},
    "58": {"name": "🌋 Вулкан",          "price": 1700,  "type": "title"},
    "59": {"name": "🧿 Артефакт",        "price": 1900,  "type": "title"},
    "60": {"name": "🏰 Замок",           "price": 2000,  "type": "title"},
    # ===== ЭКСКЛЮЗИВНЫЕ (2500–50000) =====
    "61": {"name": "🐲 Повелитель",      "price": 2500,  "type": "title"},
    "62": {"name": "🌊 Властелин морей", "price": 3000,  "type": "title"},
    "63": {"name": "🔥 Властелин огня",  "price": 3500,  "type": "title"},
    "64": {"name": "⚡ Громовержец",     "price": 4000,  "type": "title"},
    "65": {"name": "🌑 Тёмный лорд",     "price": 4500,  "type": "title"},
    "66": {"name": "☄️ Метеорит",        "price": 5000,  "type": "title"},
    "67": {"name": "🌌 Повелитель тьмы", "price": 6000,  "type": "title"},
    "68": {"name": "🧠 Оракул",          "price": 7000,  "type": "title"},
    "69": {"name": "👁 Всевидящий",      "price": 8000,  "type": "title"},
    "70": {"name": "🌀 Хаос",            "price": 9000,  "type": "title"},
    "71": {"name": "💀 Бессмертный",     "price": 10000, "type": "title"},
    "72": {"name": "🕯 Призрак",         "price": 12000, "type": "title"},
    "73": {"name": "⚗️ Алхимик богов",   "price": 15000, "type": "title"},
    "74": {"name": "🌟 Полубог",         "price": 20000, "type": "title"},
    "75": {"name": "👹 Демон",           "price": 25000, "type": "title"},
    "76": {"name": "😈 Сатана",          "price": 30000, "type": "title"},
    "77": {"name": "⚜️ Абсолют",         "price": 40000, "type": "title"},
    "78": {"name": "🌌 Творец",          "price": 50000, "type": "title"},
    "79": {"name": "☠️ Апокалипсис",     "price": 75000, "type": "title"},
    "80": {"name": "💫 БОГ",             "price": 100000,"type": "title"},
}
user_titles = defaultdict(dict)  # {uid: {"title": "...", "purchased": [...]}}

def kb_shop(cid: int, uid: int, page: int = 0) -> InlineKeyboardMarkup:
    items = list(SHOP_ITEMS.items())
    per_page = 8
    start = page * per_page
    page_items = items[start:start + per_page]
    rows = []
    for i in range(0, len(page_items), 2):
        row = []
        for item_id, item in page_items[i:i+2]:
            owned = item["name"] in user_titles[uid].get("purchased", [])
            active = user_titles[uid].get("title") == item["name"]
            icon = "🟢" if active else ("✅" if owned else "🛒")
            label = f"{icon} {item['name']} {item['price']}⭐"
            row.append(InlineKeyboardButton(text=label, callback_data=f"shop:buy:{item_id}:{uid}:{cid}:{page}"))
        rows.append(row)
    nav = []
    total_pages = (len(items) + per_page - 1) // per_page
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"shop:page:{page-1}:{uid}:{cid}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="shop:noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"shop:page:{page+1}:{uid}:{cid}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🎭 Мой титул", callback_data=f"shop:mytitle:{uid}:{cid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("shop"))
async def cmd_shop(message: Message):
    uid = message.from_user.id; cid = message.chat.id
    rep = reputation[cid].get(uid, 0)
    await reply_auto_delete(message, 
        f"🏪 <b>Магазин титулов</b>\n\n"
        f"💰 Твоя репутация: <b>{rep:+d}</b>\n\n"
        f"🟢 — активный | ✅ — куплено | 🛒 — купить\n"
        f"Всего <b>{len(SHOP_ITEMS)}</b> титулов, листай страницы!",
        parse_mode="HTML",
        reply_markup=kb_shop(cid, uid, 0))

@dp.callback_query(F.data.startswith("shop:"))
async def cb_shop(call: CallbackQuery):
    parts = call.data.split(":")
    action = parts[1]

    if action == "noop":
        await call.answer(); return

    elif action == "page":
        page, uid, cid = int(parts[2]), int(parts[3]), int(parts[4])
        if call.from_user.id != uid:
            await call.answer("❌ Это не твой магазин!", show_alert=True); return
        rep = reputation[cid].get(uid, 0)
        await call.message.edit_text(
            f"🏪 <b>Магазин титулов</b>\n\n"
            f"💰 Твоя репутация: <b>{rep:+d}</b>\n\n"
            f"🟢 — активный | ✅ — куплено | 🛒 — купить\n"
            f"Всего <b>{len(SHOP_ITEMS)}</b> титулов, листай страницы!",
            parse_mode="HTML",
            reply_markup=kb_shop(cid, uid, page))
        await call.answer()

    elif action == "buy":
        item_id, uid, cid = parts[2], int(parts[3]), int(parts[4])
        page = int(parts[5]) if len(parts) > 5 else 0
        if call.from_user.id != uid:
            await call.answer("❌ Это не твой магазин!", show_alert=True); return
        item = SHOP_ITEMS.get(item_id)
        if not item:
            await call.answer("❌ Товар не найден!", show_alert=True); return
        purchased = user_titles[uid].get("purchased", [])
        if item["name"] in purchased:
            user_titles[uid]["title"] = item["name"]
            await call.answer(f"🟢 Титул «{item['name']}» активирован!", show_alert=True)
            await call.message.edit_reply_markup(reply_markup=kb_shop(cid, uid, page))
            return
        rep = reputation[cid].get(uid, 0)
        if rep < item["price"]:
            await call.answer(
                f"💸 Недостаточно репутации!\nНужно: {item['price']} | У тебя: {rep}",
                show_alert=True); return
        reputation[cid][uid] -= item["price"]
        save_data()
        if "purchased" not in user_titles[uid]:
            user_titles[uid]["purchased"] = []
        user_titles[uid]["purchased"].append(item["name"])
        user_titles[uid]["title"] = item["name"]
        await call.answer(
            f"🎉 Куплено: {item['name']}\n💸 Потрачено: {item['price']} реп.\n💰 Осталось: {reputation[cid][uid]:+d}",
            show_alert=True)
        await call.message.edit_text(
            f"🏪 <b>Магазин титулов</b>\n\n"
            f"💰 Твоя репутация: <b>{reputation[cid].get(uid, 0):+d}</b>\n\n"
            f"🟢 — активный | ✅ — куплено | 🛒 — купить\n"
            f"Всего <b>{len(SHOP_ITEMS)}</b> титулов, листай страницы!",
            parse_mode="HTML",
            reply_markup=kb_shop(cid, uid, page))

    elif action == "mytitle":
        uid, cid = int(parts[2]), int(parts[3])
        if call.from_user.id != uid:
            await call.answer("❌ Это не твой магазин!", show_alert=True); return
        title = user_titles[uid].get("title", "нет")
        purchased = user_titles[uid].get("purchased", [])
        bought_str = ", ".join(purchased) if purchased else "ничего"
        await call.answer(
            f"🎭 Активный: {title}\n\n📦 Куплено ({len(purchased)}):\n{bought_str}",
            show_alert=True)

# ===== РЕПОРТЫ =====
report_cooldown = {}

@dp.message(Command("report"))
async def cmd_report(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await reply_auto_delete(message, 
            "📋 <b>Как использовать:</b>\n"
            "↩️ Ответь на сообщение нарушителя и напиши:\n"
            "<code>/report причина</code>", parse_mode="HTML"); return
    reporter = message.from_user; target = message.reply_to_message.from_user; cid = message.chat.id
    if target.id == reporter.id:
        await reply_auto_delete(message, "😏 Сам на себя жалуешься?"); return
    if await is_admin_by_id(cid, target.id):
        await reply_auto_delete(message, "🚫 Нельзя пожаловаться на администратора!"); return
    now = time(); key = (cid, reporter.id)
    if key in report_cooldown and now - report_cooldown[key] < 300:
        left = int(300 - (now - report_cooldown[key]))
        await reply_auto_delete(message, f"⏳ Подожди ещё <b>{left} сек.</b> перед следующим репортом!", parse_mode="HTML"); return
    report_cooldown[key] = now
    reason = command.args or "Без причины"
    report_text = (
        f"🚨 <b>НОВЫЙ РЕПОРТ</b>\n\n"
        f"👤 Жалоба от: {reporter.mention_html()}\n"
        f"🎯 На кого: {target.mention_html()}\n"
        f"📝 Причина: <b>{reason}</b>\n"
        f"💬 Чат: <b>{message.chat.title}</b>\n"
        f"🔗 Сообщение: <a href='https://t.me/c/{str(cid)[4:]}/{message.reply_to_message.message_id}'>перейти</a>"
    )
    await log_action(report_text)
    try:
        admins = await bot.get_chat_administrators(cid)
        for adm in admins:
            if adm.user.is_bot: continue
            try:
                await bot.send_message(adm.user.id,
                    f"🚨 <b>РЕПОРТ в чате {message.chat.title}</b>\n\n"
                    f"👤 От: {reporter.full_name}\n🎯 На: {target.full_name}\n"
                    f"📝 Причина: <b>{reason}</b>", parse_mode="HTML")
            except: pass
    except: pass
    sent = await reply_auto_delete(message, 
        f"✅ <b>Жалоба отправлена администраторам!</b>\n"
        f"🎯 На кого: {target.mention_html()}\n📝 Причина: <b>{reason}</b>",
        parse_mode="HTML")
    await asyncio.sleep(10)
    try:
        await sent.delete()
        await message.delete()
    except: pass

async def main():
    load_data()
    asyncio.create_task(birthday_checker())
    asyncio.create_task(send_weekly_stats())
    asyncio.create_task(warn_expiry_checker())
    await start_web()
    if not BOT_TOKEN: raise ValueError("BOT_TOKEN не задан в переменных окружения!")
    print("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

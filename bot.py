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

# ╔══════════════════════════════════════════════════════╗
#                   НАСТРОЙКИ БОТА
# ╚══════════════════════════════════════════════════════╝

BOT_TOKEN        = os.getenv("BOT_TOKEN")
WEATHER_API_KEY  = os.getenv("WEATHER_API_KEY", "")   # больше нет NameError
LOG_CHANNEL_ID   = -5293068734                         # канал для всех логов

MAX_WARNINGS     = 3
FLOOD_LIMIT      = 5
FLOOD_TIME       = 5
ANTI_MAT_ENABLED = True
MAT_MUTE_MINUTES = 5
AUTO_KICK_BOTS   = True

# ════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ]
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

# ─── Умный антимат через regex ───────────────────────
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

# ─── Тексты ──────────────────────────────────────────

RULES_TEXT = (
    "📜 <b>ПРАВИЛА ЧАТА</b>\n\n"
    "🔞 <b>Контент 18+:</b>\n"
    "• Видео, стикеры, гифки 18+\n"
    "• Ссылки на порнографические сайты\n"
    "⚠️ Наказание: <b>Бан</b>\n\n"
    "📢 <b>Реклама:</b>\n"
    "• Реклама своих соцсетей без разрешения\n"
    "⚠️ Наказание: <b>Варн → Мут 30 мин → Бан 24ч</b>\n\n"
    "🚫 <b>Общие правила:</b>\n"
    "• Уважайте друг друга\n"
    "• Без спама и флуда\n"
    "• Без оскорблений и мата\n"
    "• Только по теме чата\n\n"
    "Нарушение → предупреждение → бан 🔨"
)

MAT_RESPONSES = [
    "🤬 {name}, следи за языком! Мут на {minutes} мин.",
    "🧼 {name}, помой рот мылом! Мут {minutes} мин.",
    "😤 {name}, тут культурные люди. Мут на {minutes} мин.",
    "📵 {name}, полегче на поворотах. Мут {minutes} мин.",
    "🚿 {name}, в чате не матерятся! Мут {minutes} мин.",
]

MUTE_MESSAGES = [
    "🔇 {name} заткнут на {time}!",
    "😶 {name} помолчит {time}.",
    "🤫 {name} получил мут на {time}.",
    "📵 {name} в режиме тишины на {time}.",
]

BAN_MESSAGES = [
    "🔨 {name} улетел в бан! Причина: {reason}",
    "💥 {name} забанен! Причина: {reason}",
    "🚪 {name} выпнут из чата! Причина: {reason}",
    "🛫 {name} улетел! Причина: {reason}",
]

WARN_MESSAGES = [
    "⚠️ {name} получил варн {count}/{max}! Причина: {reason}.",
    "🚨 {name}, предупреждение {count}/{max}! Причина: {reason}.",
    "😡 {name} — варн {count}/{max}! Причина: {reason}.",
]

AUTOBAN_MESSAGES = [
    "🔨 {name} набрал {max} варнов — автобан!",
    "💀 {name} — {max} варнов = бан. Пока!",
]

RANDOM_BAN_REASONS = [
    "слишком умный",
    "подозрение в адекватности",
    "нарушение закона бутерброда",
    "превышение лимита здравого смысла",
    "нарушение пространственно-временного континуума",
    "порвал шаблон реальности",
    "был слишком харизматичным",
    "смотрел на бота косо",
    "слово 'кринж' больше 3 раз",
    "дышал слишком громко в тексте",
]

QUOTES = [
    "«Не важно, как медленно ты идёшь, главное — не останавливаться.» — Конфуций",
    "«Жизнь — это то, что случается, пока строишь другие планы.» — Леннон",
    "«Будь собой. Все остальные роли заняты.» — Уайльд",
    "«Единственный способ делать великое — любить то, что делаешь.» — Джобс",
    "«Успех — идти от неудачи к неудаче без потери энтузиазма.» — Черчилль",
    "«Делай что должен, и будь что будет.» — Толстой",
    "«Тот, кто смеётся последним, думал дольше всех.» — Народная",
    "«Умный человек найдёт выход из любой ситуации. Мудрый в неё не попадёт.» — Неизвестен",
]

BALL_ANSWERS = [
    "🟢 Да, однозначно!", "🟢 Без сомнений!", "🟢 Скорее всего да.",
    "🟡 Спроси позже.", "🟡 Трудно сказать.", "🟡 Хм, подумай ещё.",
    "🔴 Нет.", "🔴 Однозначно нет!", "🔴 Даже не думай.",
    "🟣 Звёзды молчат... попробуй ещё раз 🔮",
    "🟠 Мой хрустальный шар в отпуске 🏖",
    "⚫ Не могу раскрыть тайну вселенной за бесплатно 💸",
]

TRIGGER_RESPONSES = {
    "привет":   ["Привет! 👋", "Здарова! 😎", "О, живой человек! 👀", "Приветствую! 🫡"],
    "помогите": ["Чем помочь? Напиши подробнее! 🤝"],
    "скучно":   ["Поиграй в /8ball 🎱", "Брось кубик /roll 🎲", "Попробуй /slot 🎰", "Прочитай /quote 💬"],
    "спасибо":  ["Пожалуйста! 😊", "Всегда рад! 🤗", "Не за что! 💪", "Обращайся! 🫡"],
    "утро":     ["Доброе утро! ☀️", "Утречко! ☕", "Вставай, не ленись! 🌅"],
    "ночь":     ["Спокойной ночи! 🌙", "Отдыхай! 😴", "Сладких снов! 💤"],
    "бот":      ["Я здесь! 🤖", "Чего изволите? 🎩", "Слушаю и повинуюсь! 🫡"],
    "хорошо":   ["Вот и отлично! 😊", "Так держать! 💪"],
    "плохо":    ["Всё будет хорошо! 🌈", "Держись! /quote поднимет настроение 💬"],
    "хаха":     ["😂", "Хе-хе 😏", "Смешно тебе? 😄"],
    "ок":       ["Ок 👍", "Принято! ✅", "Как скажешь 🫡"],
}

TRUTH_QUESTIONS = [
    "Ты когда-нибудь врал другу?",
    "Какой твой самый большой страх?",
    "Ты когда-нибудь влюблялся в друга?",
    "Что тебя раздражает больше всего?",
    "Твой самый неловкий момент в жизни?",
    "Ты когда-нибудь плакал из-за фильма?",
    "Что ты никогда не расскажешь родителям?",
    "Последняя ложь которую ты сказал?",
    "Какой твой самый постыдный секрет?",
    "На кого из чата ты хотел бы быть похожим?",
]

DARE_CHALLENGES = [
    "Напиши комплимент случайному участнику чата!",
    "Признайся в чём-нибудь стыдном прямо сейчас!",
    "Напиши стих про себя — прямо сейчас!",
    "Напиши 20 смайликов без остановки!",
    "Придумай и напиши анекдот прямо сейчас!",
    "Напиши самое странное слово которое знаешь!",
    "Напиши что-нибудь на несуществующем языке!",
    "Опиши свой день одним смайликом!",
]

WOULD_YOU_RATHER = [
    "Быть богатым но одиноким или бедным но счастливым?",
    "Уметь летать или быть невидимым?",
    "Знать будущее или изменить прошлое?",
    "Никогда не спать или спать 20 часов в день?",
    "Говорить только правду или постоянно врать?",
    "Жить 200 лет в бедности или 50 лет в богатстве?",
    "Потерять память или потерять все деньги?",
    "Есть только сладкое или только солёное всю жизнь?",
    "Читать мысли или телепортироваться?",
    "Жить без интернета или без музыки?",
]

HOROSCOPES = {
    "♈ Овен":     "Сегодня звёзды говорят — делай что хочешь, но с умом.",
    "♉ Телец":    "День для отдыха. Полежи, поешь, снова полежи.",
    "♊ Близнецы": "Раздвоение личности сегодня — твоя суперсила.",
    "♋ Рак":      "Спрячься в домик. Там лучше. Там печеньки.",
    "♌ Лев":      "Ты красивый и все это знают. Используй по полной.",
    "♍ Дева":     "Разложи всё по полочкам. Буквально. Все полочки.",
    "♎ Весы":     "Не можешь выбрать что поесть? Это карма.",
    "♏ Скорпион": "Таинственность — твоё оружие. Молчи и улыбайся.",
    "♐ Стрелец":  "Стреляй в мечты! Может и попадёшь.",
    "♑ Козерог":  "Работай. Работай ещё. Потом отдохнёшь на пенсии.",
    "♒ Водолей":  "Ты уникальный. Как и все остальные.",
    "♓ Рыбы":     "Плыви по течению. Или против. Главное — плыви.",
}

COMPLIMENTS = [
    "Ты просто огонь! 🔥",
    "С тобой в чате теплее! ☀️",
    "Ты делаешь этот чат лучше каждый день! 💎",
    "Улыбка читается даже в тексте! 😊",
    "Ты — секретный ингредиент этого чата! 🍀",
    "Без тебя тут было бы скучнее! 🌟",
    "Ты как редкий покемон — очень ценный! ⭐",
    "Интеллект зашкаливает! 🧠",
    "Ты настоящий бриллиант этого чата! 💎",
    "Твоё чувство юмора на высшем уровне! 😂",
]

PREDICTIONS = [
    "🔮 Сегодня тебе повезёт! Только не трать деньги.",
    "💀 Осторожно с незнакомцами сегодня.",
    "⭐ Звёзды говорят — ты красавчик!",
    "🍀 Удача на твоей стороне!",
    "😈 Что-то пойдёт не так... но ты справишься!",
    "🌈 Сегодня отличный день чтобы ничего не делать.",
    "💪 Ты сильнее чем думаешь!",
    "🎯 Сегодня всё получится с первого раза!",
    "🐉 Тебя ждёт необычная встреча. Возможно с котом.",
    "💤 Лучший план на сегодня — поспать. Доверяй процессу.",
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
    logger.info("🌐 Веб-сервер запущен на :8080")


# ─────────────────────────────────────────────────────
#  ЛОГИРОВАНИЕ В ТЕЛЕГРАМ-КАНАЛ
# ─────────────────────────────────────────────────────

LEVEL_ICONS = {
    "INFO":  "ℹ️",
    "WARN":  "⚠️",
    "BAN":   "🔨",
    "MUTE":  "🔇",
    "UNMUTE":"🔊",
    "UNWARN":"✅",
    "FLOOD": "🌊",
    "MAT":   "🧼",
    "ERROR": "🔴",
    "JOIN":  "👋",
    "LEAVE": "🚪",
    "CMD":   "📌",
    "LOCK":  "🔒",
    "POLL":  "📊",
}

async def send_log(text: str, level: str = "INFO"):
    icon = LEVEL_ICONS.get(level, "📌")
    now  = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    msg  = f"{icon} <b>[{level}]</b> <code>{now}</code>\n{text}"
    try:
        await bot.send_message(LOG_CHANNEL_ID, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"send_log failed: {e}")


# ─────────────────────────────────────────────────────
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────

async def check_admin(message: Message) -> bool:
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return member.status in ("administrator", "creator")
    except Exception as e:
        logger.error(f"check_admin: {e}")
        return False

async def is_admin_by_id(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception as e:
        logger.error(f"is_admin_by_id: {e}")
        return False

async def require_admin(message: Message) -> bool:
    if not await check_admin(message):
        await message.reply("🚫 <b>Только для администраторов!</b>", parse_mode="HTML")
        return False
    return True

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
#  ПОГОДА
# ─────────────────────────────────────────────────────

async def get_weather(city: str) -> str:
    if not WEATHER_API_KEY:
        return (
            "❌ <b>Weather API ключ не настроен.</b>\n\n"
            "Добавь переменную окружения <code>WEATHER_API_KEY</code>\n"
            "(бесплатно на openweathermap.org)"
        )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": WEATHER_API_KEY, "units": "metric", "lang": "ru"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                if data.get("cod") != 200:
                    return f"❌ Город <b>{city}</b> не найден."
                name  = data["name"]
                temp  = round(data["main"]["temp"])
                feels = round(data["main"]["feels_like"])
                desc  = data["weather"][0]["description"].capitalize()
                humid = data["main"]["humidity"]
                wind  = round(data["wind"]["speed"])
                dl    = desc.lower()
                if "ясно"   in dl: emoji = "☀️"
                elif "дождь" in dl: emoji = "🌧"
                elif "снег"  in dl: emoji = "❄️"
                elif "гроз"  in dl: emoji = "⛈"
                elif "туман" in dl: emoji = "🌫"
                elif "облач" in dl: emoji = "⛅"
                else:               emoji = "🌤"
                return (
                    f"{emoji} <b>Погода в {name}</b>\n\n"
                    f"🌡 Температура: <b>{temp}°C</b> (ощущается {feels}°C)\n"
                    f"📋 {desc}\n"
                    f"💧 Влажность: <b>{humid}%</b>\n"
                    f"💨 Ветер: <b>{wind} м/с</b>"
                )
    except asyncio.TimeoutError:
        return "❌ Сервер погоды не отвечает. Попробуй позже."
    except Exception as e:
        logger.error(f"get_weather: {e}")
        return f"❌ Ошибка: {e}"


# ─────────────────────────────────────────────────────
#  КЛАВИАТУРЫ ПАНЕЛИ
# ─────────────────────────────────────────────────────

def kb_main_panel(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔇 Мут",        callback_data=f"panel:mute:{target_id}"),
            InlineKeyboardButton(text="🔊 Размут",     callback_data=f"panel:unmute:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="⚠️ Варн",       callback_data=f"panel:warn:{target_id}"),
            InlineKeyboardButton(text="✅ Снять варн", callback_data=f"panel:unwarn:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🔨 Бан",        callback_data=f"panel:ban:{target_id}"),
            InlineKeyboardButton(text="♻️ Разбан",     callback_data=f"panel:unban:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="📌 Закрепить",  callback_data=f"panel:pin:{target_id}"),
            InlineKeyboardButton(text="🗑 Удалить сообщение", callback_data=f"panel:del:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="📊 Информация", callback_data=f"panel:info:{target_id}"),
            InlineKeyboardButton(text="🎭 Приколы",    callback_data=f"panel:fun:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🔧 Чат",        callback_data=f"panel:chat:{target_id}"),
            InlineKeyboardButton(text="❌ Закрыть",    callback_data="panel:close:0"),
        ],
    ])

def kb_mute_time(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 мин",  callback_data=f"mute:{target_id}:5"),
            InlineKeyboardButton(text="15 мин", callback_data=f"mute:{target_id}:15"),
            InlineKeyboardButton(text="30 мин", callback_data=f"mute:{target_id}:30"),
        ],
        [
            InlineKeyboardButton(text="1 час",    callback_data=f"mute:{target_id}:60"),
            InlineKeyboardButton(text="3 часа",   callback_data=f"mute:{target_id}:180"),
            InlineKeyboardButton(text="12 часов", callback_data=f"mute:{target_id}:720"),
        ],
        [
            InlineKeyboardButton(text="1 день",   callback_data=f"mute:{target_id}:1440"),
            InlineKeyboardButton(text="7 дней",   callback_data=f"mute:{target_id}:10080"),
            InlineKeyboardButton(text="✏️ Своё",  callback_data=f"mute:{target_id}:custom"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")],
    ])

def kb_warn_options(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤬 Мат",         callback_data=f"warn:{target_id}:Мат в чате"),
            InlineKeyboardButton(text="💬 Спам",         callback_data=f"warn:{target_id}:Спам"),
        ],
        [
            InlineKeyboardButton(text="😡 Оскорбление", callback_data=f"warn:{target_id}:Оскорбление"),
            InlineKeyboardButton(text="🚫 Флуд",         callback_data=f"warn:{target_id}:Флуд"),
        ],
        [
            InlineKeyboardButton(text="📵 Реклама",     callback_data=f"warn:{target_id}:Реклама"),
            InlineKeyboardButton(text="🔞 Контент 18+", callback_data=f"warn:{target_id}:Контент 18+"),
        ],
        [InlineKeyboardButton(text="✏️ Своя причина",  callback_data=f"warn:{target_id}:custom")],
        [InlineKeyboardButton(text="◀️ Назад",          callback_data=f"panel:back:{target_id}")],
    ])

def kb_ban_options(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤬 Грубые нарушения", callback_data=f"ban:{target_id}:Грубые нарушения правил"),
            InlineKeyboardButton(text="💬 Спам/реклама",     callback_data=f"ban:{target_id}:Спам и реклама"),
        ],
        [
            InlineKeyboardButton(text="🔞 Контент 18+",  callback_data=f"ban:{target_id}:Контент 18+"),
            InlineKeyboardButton(text="🤖 Бот/накрутка", callback_data=f"ban:{target_id}:Бот или накрутка"),
        ],
        [
            InlineKeyboardButton(text="⏰ Бан на 24ч",   callback_data=f"ban:{target_id}:tempban24"),
            InlineKeyboardButton(text="✏️ Своя причина", callback_data=f"ban:{target_id}:custom"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")],
    ])

def kb_fun_panel(target_id: int) -> InlineKeyboardMarkup:
    """Раздел приколов в панели."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎲 Шуточный бан",   callback_data=f"fun:rban:{target_id}"),
            InlineKeyboardButton(text="🧠 Проверить IQ",   callback_data=f"fun:iq:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🌈 % гейства",      callback_data=f"fun:gay:{target_id}"),
            InlineKeyboardButton(text="💐 Комплимент",      callback_data=f"fun:compliment:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🔮 Предсказание",   callback_data=f"fun:predict:{target_id}"),
            InlineKeyboardButton(text="♈ Гороскоп",       callback_data=f"fun:horoscope:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="⭐ Оценить участника", callback_data=f"fun:rate:{target_id}"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")],
    ])

def kb_chat_panel(target_id: int) -> InlineKeyboardMarkup:
    """Раздел управления чатом в панели."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔒 Заблокировать чат",  callback_data=f"chat:lock:{target_id}"),
            InlineKeyboardButton(text="🔓 Разблокировать чат", callback_data=f"chat:unlock:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🐢 Медленный режим 30с", callback_data=f"chat:slow30:{target_id}"),
            InlineKeyboardButton(text="🐇 Выкл. медл. режим",   callback_data=f"chat:slow0:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🤖 Автокик ботов: вкл",  callback_data=f"chat:autokick_on:{target_id}"),
            InlineKeyboardButton(text="🤖 Автокик ботов: выкл", callback_data=f"chat:autokick_off:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="🧼 Антимат: вкл",  callback_data=f"chat:antimat_on:{target_id}"),
            InlineKeyboardButton(text="🧼 Антимат: выкл", callback_data=f"chat:antimat_off:{target_id}"),
        ],
        [
            InlineKeyboardButton(text="📊 Топ активности",    callback_data=f"chat:top:{target_id}"),
            InlineKeyboardButton(text="👮 Список админов",    callback_data=f"chat:adminlist:{target_id}"),
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
        try:
            member = await bot.get_chat_member(cid, uid)
            if member.status in ("administrator", "creator"): return await handler(event, data)
        except Exception as e:
            logger.warning(f"AntiFlood admin check: {e}")

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
                    f"🌊 {event.from_user.mention_html()}, полегче с флудом! Мут на 5 минут.",
                    parse_mode="HTML")
                flood_tracker[cid][uid].clear()
                await send_log(
                    f"🌊 <b>Флуд-мут</b>\n"
                    f"👤 {event.from_user.mention_html()} (<code>{uid}</code>)\n"
                    f"💬 Чат: {event.chat.title} (<code>{cid}</code>)",
                    "FLOOD")
                await asyncio.sleep(8)
                try: await sent.delete()
                except: pass
            except Exception as e:
                logger.error(f"AntiFlood action: {e}")
            return
        return await handler(event, data)


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — АНТИМАТ (regex)
# ─────────────────────────────────────────────────────

class AntiMatMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if not ANTI_MAT_ENABLED: return await handler(event, data)
        if event.chat.type not in ("group", "supergroup"): return await handler(event, data)
        if not event.text or event.text.startswith("/"): return await handler(event, data)
        uid = event.from_user.id
        cid = event.chat.id
        try:
            member = await bot.get_chat_member(cid, uid)
            if member.status in ("administrator", "creator"): return await handler(event, data)
        except Exception as e:
            logger.warning(f"AntiMat admin check: {e}")

        if contains_mat(event.text):
            try:
                await event.delete()
                await bot.restrict_chat_member(cid, uid,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=timedelta(minutes=MAT_MUTE_MINUTES))
                resp = random.choice(MAT_RESPONSES).format(
                    name=event.from_user.mention_html(), minutes=MAT_MUTE_MINUTES)
                sent = await bot.send_message(cid, resp, parse_mode="HTML")
                await send_log(
                    f"🧼 <b>Антимат</b>\n"
                    f"👤 {event.from_user.mention_html()} (<code>{uid}</code>)\n"
                    f"💬 Чат: {event.chat.title} (<code>{cid}</code>)\n"
                    f"📝 Текст: <code>{event.text[:120]}</code>",
                    "MAT")
                await asyncio.sleep(10)
                try: await sent.delete()
                except: pass
            except Exception as e:
                logger.error(f"AntiMat action: {e}")
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
                try:
                    await event.answer(
                        f"👋 {event.from_user.mention_html()} вернулся из AFK! (был: {reason})",
                        parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"AFK return: {e}")
        if event.reply_to_message and event.reply_to_message.from_user:
            tid = event.reply_to_message.from_user.id
            if tid in afk_users:
                try:
                    await event.answer(
                        f"😴 {event.reply_to_message.from_user.mention_html()} сейчас AFK: {afk_users[tid]}",
                        parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"AFK notify: {e}")
        return await handler(event, data)


# ─────────────────────────────────────────────────────
#  MIDDLEWARE — ПЕРЕХВАТ ВВОДА ДЛЯ ПАНЕЛИ
# ─────────────────────────────────────────────────────

class PendingInputMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        uid = event.from_user.id if event.from_user else None
        if uid and uid in pending and not (event.text and event.text.startswith("/")):
            p           = pending.pop(uid)
            action      = p["action"]
            target_id   = p["target_id"]
            target_name = p["target_name"]
            chat_id     = p["chat_id"]
            text        = event.text or ""
            try:
                if action == "mute_custom":
                    mins, label = parse_duration(text)
                    if mins:
                        await bot.restrict_chat_member(chat_id, target_id,
                            permissions=ChatPermissions(can_send_messages=False),
                            until_date=timedelta(minutes=mins))
                        msg = random.choice(MUTE_MESSAGES).format(name=f"<b>{target_name}</b>", time=label)
                        await event.answer(msg, parse_mode="HTML")
                        await send_log(
                            f"🔇 <b>Мут (custom)</b>\n👤 {target_name} (<code>{target_id}</code>)\n⏱ {label}",
                            "MUTE")
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
                        await send_log(f"🔨 <b>Автобан (custom warn)</b>\n👤 {target_name} (<code>{target_id}</code>)", "BAN")
                    else:
                        msg = random.choice(WARN_MESSAGES).format(
                            name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
                        await send_log(f"⚠️ <b>Варн {count}/{MAX_WARNINGS}</b>\n👤 {target_name}\n📝 {reason}", "WARN")
                    await event.answer(msg, parse_mode="HTML")

                elif action == "ban_custom":
                    reason = text.strip() or "Нарушение правил"
                    await bot.ban_chat_member(chat_id, target_id)
                    msg = random.choice(BAN_MESSAGES).format(name=f"<b>{target_name}</b>", reason=reason)
                    await event.answer(msg, parse_mode="HTML")
                    await send_log(f"🔨 <b>Бан (custom)</b>\n👤 {target_name} (<code>{target_id}</code>)\n📝 {reason}", "BAN")

                elif action == "announce_text":
                    await bot.send_message(chat_id,
                        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{text}\n\n— <b>Администрация</b>",
                        parse_mode="HTML")
                    await send_log(f"📢 <b>Объявление</b>\n💬 Чат: <code>{chat_id}</code>\n📝 {text[:100]}", "CMD")

                elif action == "adminpromote_username":
                    username = text.strip().lstrip("@")
                    await event.answer(
                        f"👑 Чтобы выдать админку @{username}:\n\n"
                        f"Настройки группы → Администраторы → Добавить администратора → @{username}",
                        parse_mode="HTML")
            except Exception as e:
                logger.error(f"PendingInput {action}: {e}")
                await event.reply("❗ Произошла ошибка при выполнении.")
            try: await event.delete()
            except: pass
            return
        return await handler(event, data)


dp.message.middleware(PendingInputMiddleware())
dp.message.middleware(StatsMiddleware())
dp.message.middleware(AntiFloodMiddleware())
dp.message.middleware(AntiMatMiddleware())
dp.message.middleware(AfkMiddleware())


# ─────────────────────────────────────────────────────
#  АВТОКИК БОТОВ + ПРИВЕТСТВИЕ
# ─────────────────────────────────────────────────────

@dp.message(F.new_chat_members)
async def on_new_member(message: Message):
    for member in message.new_chat_members:
        if member.is_bot and AUTO_KICK_BOTS:
            try:
                await bot.ban_chat_member(message.chat.id, member.id)
                await bot.unban_chat_member(message.chat.id, member.id)
                await message.answer(
                    f"🤖 Бот <b>{member.full_name}</b> автоматически удалён!\n"
                    f"Боты не приветствуются в этом чате.",
                    parse_mode="HTML")
                await send_log(
                    f"🤖 <b>Автокик бота</b>\n🤖 {member.full_name} (<code>{member.id}</code>)\n"
                    f"💬 Чат: {message.chat.title}",
                    "LEAVE")
            except Exception as e:
                logger.error(f"AutoKick: {e}")
            continue

        funny = [
            f"🎉 {member.mention_html()} залетел в чат! Ознакомься с правилами 👇",
            f"🚀 {member.mention_html()} телепортировался к нам! Правила обязательны 👇",
            f"👀 О! {member.mention_html()} появился в чате! Читай правила 👇",
            f"🌟 {member.mention_html()} присоединился! Добро пожаловать 👇",
        ]
        await message.answer(random.choice(funny) + "\n\n" + RULES_TEXT, parse_mode="HTML")
        await send_log(
            f"👋 <b>Новый участник</b>\n"
            f"👤 {member.mention_html()} (<code>{member.id}</code>)\n"
            f"💬 Чат: {message.chat.title} (<code>{message.chat.id}</code>)",
            "JOIN")


# ─────────────────────────────────────────────────────
#  /start  /rules  /help
# ─────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "Я бот-модератор этого чата.\n"
        "📜 /rules — правила\n"
        "❓ /help — помощь\n\n"
        "<i>Панель управления — только для администраторов.</i>",
        parse_mode="HTML")
    await send_log(
        f"▶️ <b>/start</b>\n👤 {message.from_user.mention_html()} (<code>{message.from_user.id}</code>)",
        "CMD")

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    await message.reply(RULES_TEXT, parse_mode="HTML")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_adm = await check_admin(message)
    user_help = (
        "❓ <b>Доступные команды:</b>\n\n"
        "📜 /rules — правила чата\n"
        "🌤 /weather [город] — погода\n"
        "⭐ /rep — репутация (реплай)\n"
        "+1 / -1 в реплае — дать/снять репу\n"
        "📊 /top — топ активных\n"
        "😴 /afk [причина] — уйти в AFK\n"
        "ℹ️ /info — инфо (реплай)\n"
        "⚠️ /warnings — варны (реплай)\n"
        "📝 /note get/list [имя] — заметки\n\n"
        "🎮 <b>Игры:</b>\n"
        "/roll /flip /8ball /rate /iq /gay\n"
        "/quote /truth /dare /wyr /rps /slot\n"
        "/choose /horoscope /predict /compliment /countdown\n"
    )
    admin_help = (
        "\n\n👮 <b>Для администраторов:</b>\n"
        "/panel — панель управления (реплай)\n"
        "/ban /unban /mute /unmute /warn /unwarn\n"
        "/del /clear /announce /pin /unpin\n"
        "/lock /unlock /slowmode /promote /poll\n"
        "/antimat /autokick /rban /warn24\n"
        "/adminlist /setadmin /note set/del\n"
        "\n<i>Почти всё доступно прямо в /panel 😎</i>"
    )
    await message.reply(user_help + (admin_help if is_adm else ""), parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  /panel — главная панель
# ─────────────────────────────────────────────────────

@dp.message(Command("panel"))
async def cmd_panel(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя чтобы открыть панель."); return
    target = message.reply_to_message.from_user
    warns  = warnings[message.chat.id].get(target.id, 0)
    rep    = reputation[message.chat.id].get(target.id, 0)
    msgs   = chat_stats[message.chat.id].get(target.id, 0)
    afk    = f"\n😴 AFK: {afk_users[target.id]}" if target.id in afk_users else ""
    await message.reply(
        f"🛠 <b>Панель управления</b>\n\n"
        f"👤 {target.mention_html()}{afk}\n"
        f"🆔 ID: <code>{target.id}</code>\n"
        f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
        f"⭐ Репутация: <b>{rep:+d}</b>\n"
        f"💬 Сообщений: <b>{msgs}</b>\n\n"
        f"Выбери действие:",
        parse_mode="HTML",
        reply_markup=kb_main_panel(target.id)
    )


# ─────────────────────────────────────────────────────
#  CALLBACK: panel:*
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("panel:"))
async def cb_panel(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return

    _, action, tid_str = call.data.split(":", 2)
    target_id = int(tid_str)
    chat_id   = call.message.chat.id

    # Получить имя цели
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
        mention     = tm.user.mention_html()
    except Exception:
        target_name = f"ID {target_id}"
        mention     = f"<code>{target_id}</code>"

    async def back_text():
        warns = warnings[chat_id].get(target_id, 0)
        rep   = reputation[chat_id].get(target_id, 0)
        msgs  = chat_stats[chat_id].get(target_id, 0)
        afk   = f"\n😴 AFK: {afk_users[target_id]}" if target_id in afk_users else ""
        return (
            f"🛠 <b>Панель управления</b>\n\n"
            f"👤 {mention}{afk}\n"
            f"🆔 ID: <code>{target_id}</code>\n"
            f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
            f"⭐ Репутация: <b>{rep:+d}</b>\n"
            f"💬 Сообщений: <b>{msgs}</b>\n\n"
            f"Выбери действие:"
        )

    try:
        if action == "close":
            await call.message.delete()

        elif action == "back":
            await call.message.edit_text(await back_text(), parse_mode="HTML",
                reply_markup=kb_main_panel(target_id))

        elif action == "mute":
            await call.message.edit_text(
                f"🔇 <b>Мут для {target_name}</b>\n\nВыбери время:",
                parse_mode="HTML", reply_markup=kb_mute_time(target_id))

        elif action == "unmute":
            await bot.restrict_chat_member(chat_id, target_id,
                permissions=ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=True,
                    can_add_web_page_previews=True))
            await call.message.edit_text(f"🔊 <b>{target_name}</b> размучен.", parse_mode="HTML")
            await send_log(f"🔊 <b>Размут</b>\n👤 {target_name} (<code>{target_id}</code>)", "UNMUTE")

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
            await send_log(f"✅ <b>Снят варн</b>\n👤 {target_name} (<code>{target_id}</code>)\nОсталось: {count}", "UNWARN")

        elif action == "ban":
            await call.message.edit_text(
                f"🔨 <b>Бан для {target_name}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_ban_options(target_id))

        elif action == "unban":
            await bot.unban_chat_member(chat_id, target_id, only_if_banned=True)
            await call.message.edit_text(f"♻️ <b>{target_name}</b> разбанен.", parse_mode="HTML")
            await send_log(f"♻️ <b>Разбан</b>\n👤 {target_name} (<code>{target_id}</code>)", "UNMUTE")

        elif action == "pin":
            try:
                await bot.pin_chat_message(chat_id, call.message.reply_to_message.message_id)
                await call.message.edit_text("📌 Сообщение закреплено!")
                await send_log(f"📌 <b>Закреплено</b>\n💬 Чат: <code>{chat_id}</code>", "CMD")
            except Exception:
                await call.answer("❗ Не могу закрепить — ответь на нужное сообщение.", show_alert=True)

        elif action == "del":
            try:
                await call.message.reply_to_message.delete()
            except Exception: pass
            await call.message.edit_text("🗑 Сообщение удалено.")
            await send_log(f"🗑 <b>Удалено сообщение</b>\n💬 Чат: <code>{chat_id}</code>", "CMD")

        elif action == "fun":
            await call.message.edit_text(
                f"🎭 <b>Приколы над {target_name}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_fun_panel(target_id))

        elif action == "chat":
            mat_s = "✅" if ANTI_MAT_ENABLED else "❌"
            kick_s = "✅" if AUTO_KICK_BOTS else "❌"
            await call.message.edit_text(
                f"🔧 <b>Управление чатом</b>\n\n"
                f"🧼 Антимат: <b>{mat_s}</b>\n"
                f"🤖 Автокик ботов: <b>{kick_s}</b>\n\n"
                f"Выбери действие:",
                parse_mode="HTML", reply_markup=kb_chat_panel(target_id))

        elif action == "adminpromote":
            await call.message.edit_text(
                f"👑 Чтобы выдать права <b>{target_name}</b>:\n\n"
                f"Настройки → Администраторы → Добавить → найди пользователя\n\n"
                f"<i>Telegram не позволяет ботам выдавать права напрямую.</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")
                ]]))

        elif action == "info":
            try:
                tm2   = await bot.get_chat_member(chat_id, target_id)
                user  = tm2.user
                warns = warnings[chat_id].get(target_id, 0)
                rep   = reputation[chat_id].get(target_id, 0)
                msgs  = chat_stats[chat_id].get(target_id, 0)
                status_map = {
                    "creator":"👑 Создатель", "administrator":"🛡 Администратор",
                    "member":"👤 Участник",   "restricted":"🔇 Ограничен",
                    "kicked":"🔨 Забанен",    "left":"🚶 Вышел",
                }
                afk = f"\n😴 AFK: {afk_users[target_id]}" if target_id in afk_users else ""
                await call.message.edit_text(
                    f"👤 <b>Информация:</b>\n\n"
                    f"🏷 {user.mention_html()}{afk}\n"
                    f"🔗 {'@'+user.username if user.username else 'нет'}\n"
                    f"🆔 <code>{user.id}</code>\n"
                    f"📌 {status_map.get(tm2.status, tm2.status)}\n"
                    f"⚠️ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
                    f"⭐ Репутация: <b>{rep:+d}</b>\n"
                    f"💬 Сообщений: <b>{msgs}</b>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{target_id}")
                    ]]))
            except Exception as e:
                await call.answer(f"Ошибка: {e}", show_alert=True)

    except Exception as e:
        logger.error(f"cb_panel action={action}: {e}")
        await call.answer("❗ Произошла ошибка.", show_alert=True)

    await call.answer()


# ─────────────────────────────────────────────────────
#  CALLBACK: mute:*
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    _, tid_str, time_val = call.data.split(":")
    target_id = int(tid_str)
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
    except Exception:
        target_name = f"ID {target_id}"

    if time_val == "custom":
        pending[call.from_user.id] = {
            "action": "mute_custom", "target_id": target_id,
            "target_name": target_name, "chat_id": chat_id}
        await call.message.edit_text(
            f"✏️ Введи время мута для <b>{target_name}</b>:\n"
            f"Примеры: <code>10</code>, <code>30m</code>, <code>2h</code>, <code>1d</code>",
            parse_mode="HTML")
        await call.answer(); return

    mins  = int(time_val)
    label = f"{mins} мин." if mins < 60 else (f"{mins//60} ч." if mins < 1440 else f"{mins//1440} дн.")
    await bot.restrict_chat_member(chat_id, target_id,
        permissions=ChatPermissions(can_send_messages=False),
        until_date=timedelta(minutes=mins))
    msg = random.choice(MUTE_MESSAGES).format(name=f"<b>{target_name}</b>", time=label)
    await call.message.edit_text(msg, parse_mode="HTML")
    await send_log(f"🔇 <b>Мут</b>\n👤 {target_name} (<code>{target_id}</code>)\n⏱ {label}", "MUTE")
    await call.answer(f"Замутен на {label}!")


# ─────────────────────────────────────────────────────
#  CALLBACK: warn:*
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("warn:"))
async def cb_warn(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts     = call.data.split(":", 2)
    target_id = int(parts[1])
    reason    = parts[2]
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
    except Exception:
        target_name = f"ID {target_id}"

    if reason == "custom":
        pending[call.from_user.id] = {
            "action": "warn_custom", "target_id": target_id,
            "target_name": target_name, "chat_id": chat_id}
        await call.message.edit_text(
            f"✏️ Напиши причину варна для <b>{target_name}</b>:", parse_mode="HTML")
        await call.answer(); return

    warnings[chat_id][target_id] += 1
    count = warnings[chat_id][target_id]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(chat_id, target_id)
        warnings[chat_id][target_id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{target_name}</b>", max=MAX_WARNINGS)
        await send_log(f"🔨 <b>Автобан</b>\n👤 {target_name} (<code>{target_id}</code>)", "BAN")
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
        await send_log(f"⚠️ <b>Варн {count}/{MAX_WARNINGS}</b>\n👤 {target_name}\n📝 {reason}", "WARN")
    await call.message.edit_text(msg, parse_mode="HTML")
    await call.answer("Варн выдан!")


# ─────────────────────────────────────────────────────
#  CALLBACK: ban:*
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("ban:"))
async def cb_ban(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts     = call.data.split(":", 2)
    target_id = int(parts[1])
    reason    = parts[2]
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        target_name = tm.user.full_name
    except Exception:
        target_name = f"ID {target_id}"

    if reason == "custom":
        pending[call.from_user.id] = {
            "action": "ban_custom", "target_id": target_id,
            "target_name": target_name, "chat_id": chat_id}
        await call.message.edit_text(
            f"✏️ Напиши причину бана для <b>{target_name}</b>:", parse_mode="HTML")
        await call.answer(); return

    if reason == "tempban24":
        await bot.ban_chat_member(chat_id, target_id, until_date=timedelta(hours=24))
        await call.message.edit_text(
            f"⏰ <b>{target_name}</b> забанен на <b>24 часа</b>!", parse_mode="HTML")
        await send_log(f"⏰ <b>Бан 24ч</b>\n👤 {target_name} (<code>{target_id}</code>)", "BAN")
        await call.answer("Временный бан!"); return

    await bot.ban_chat_member(chat_id, target_id)
    msg = random.choice(BAN_MESSAGES).format(name=f"<b>{target_name}</b>", reason=reason)
    await call.message.edit_text(msg, parse_mode="HTML")
    await send_log(f"🔨 <b>Бан</b>\n👤 {target_name} (<code>{target_id}</code>)\n📝 {reason}", "BAN")
    await call.answer("Забанен!")


# ─────────────────────────────────────────────────────
#  CALLBACK: fun:*  (приколы в панели)
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("fun:"))
async def cb_fun(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts     = call.data.split(":")
    action    = parts[1]
    target_id = int(parts[2])
    chat_id   = call.message.chat.id
    try:
        tm = await bot.get_chat_member(chat_id, target_id)
        user = tm.user
        mention = user.mention_html()
        name    = user.full_name
    except Exception:
        mention = f"<code>{target_id}</code>"
        name    = f"ID {target_id}"

    try:
        if action == "rban":
            reason = random.choice(RANDOM_BAN_REASONS)
            await call.message.edit_text(
                f"🎲 {mention} получил <b>шуточный бан</b>!\n"
                f"📝 Причина: {reason} 😄\n<i>(реального бана нет, успокойся)</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

        elif action == "iq":
            iq = random.randint(1, 200)
            if iq < 70:    c = "🥔 Картошка умнее."
            elif iq < 100: c = "🐒 Обезьяна справилась бы лучше."
            elif iq < 130: c = "😐 Сойдёт."
            elif iq < 160: c = "🧠 Умный человек!"
            else:          c = "🤖 Эйнштейн, это ты?!"
            await call.message.edit_text(
                f"🧠 IQ {mention}: <b>{iq}</b>\n{c}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

        elif action == "gay":
            p   = random.randint(0, 100)
            bar = "🌈" * (p // 10) + "⬜" * (10 - p // 10)
            await call.message.edit_text(
                f"🏳️‍🌈 {mention}\n{bar}\n<b>{p}%</b> — это шутка, не ссы 😄",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

        elif action == "compliment":
            await call.message.edit_text(
                f"💐 <b>Комплимент для {mention}:</b>\n\n{random.choice(COMPLIMENTS)}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

        elif action == "predict":
            await call.message.edit_text(
                f"🔮 <b>Предсказание для {mention}:</b>\n\n{random.choice(PREDICTIONS)}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

        elif action == "horoscope":
            sign, text = random.choice(list(HOROSCOPES.items()))
            await call.message.edit_text(
                f"{sign} <b>Гороскоп для {mention}:</b>\n\n{text}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

        elif action == "rate":
            score = random.randint(0, 10)
            bar   = "⭐" * score + "☆" * (10 - score)
            await call.message.edit_text(
                f"📊 Оценка участника {mention}:\n{bar}\n<b>{score}/10</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:fun:{target_id}")
                ]]))

    except Exception as e:
        logger.error(f"cb_fun action={action}: {e}")
        await call.answer("❗ Ошибка.", show_alert=True)

    await call.answer()


# ─────────────────────────────────────────────────────
#  CALLBACK: chat:*  (управление чатом из панели)
# ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("chat:"))
async def cb_chat(call: CallbackQuery):
    global ANTI_MAT_ENABLED, AUTO_KICK_BOTS
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts  = call.data.split(":")
    action = parts[1]
    tid    = int(parts[2])
    cid    = call.message.chat.id

    try:
        if action == "lock":
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
            await call.answer("🔒 Чат заблокирован!", show_alert=True)
            await send_log(f"🔒 <b>Чат заблокирован</b>\n💬 <code>{cid}</code>", "LOCK")

        elif action == "unlock":
            await bot.set_chat_permissions(cid, ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True, can_invite_users=True))
            await call.answer("🔓 Чат разблокирован!", show_alert=True)
            await send_log(f"🔓 <b>Чат разблокирован</b>\n💬 <code>{cid}</code>", "LOCK")

        elif action == "slow30":
            await bot.set_chat_slow_mode_delay(cid, 30)
            await call.answer("🐢 Медленный режим 30с включён!", show_alert=True)
            await send_log(f"🐢 <b>Slowmode 30с</b>\n💬 <code>{cid}</code>", "CMD")

        elif action == "slow0":
            await bot.set_chat_slow_mode_delay(cid, 0)
            await call.answer("🐇 Медленный режим выключен!", show_alert=True)
            await send_log(f"🐇 <b>Slowmode выкл</b>\n💬 <code>{cid}</code>", "CMD")

        elif action == "autokick_on":
            AUTO_KICK_BOTS = True
            await call.answer("🤖 Автокик ботов включён!", show_alert=True)

        elif action == "autokick_off":
            AUTO_KICK_BOTS = False
            await call.answer("🤖 Автокик ботов выключен!", show_alert=True)

        elif action == "antimat_on":
            ANTI_MAT_ENABLED = True
            await call.answer("🧼 Антимат включён!", show_alert=True)

        elif action == "antimat_off":
            ANTI_MAT_ENABLED = False
            await call.answer("🧼 Антимат выключен!", show_alert=True)

        elif action == "top":
            stats = chat_stats[cid]
            if not stats:
                await call.answer("📊 Статистика пуста!", show_alert=True)
            else:
                sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
                medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
                lines    = ["📊 <b>Топ активных:</b>\n"]
                for i, (uid, cnt) in enumerate(sorted_u):
                    try:
                        m    = await bot.get_chat_member(cid, uid)
                        uname = m.user.full_name
                    except Exception:
                        uname = f"ID {uid}"
                    lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщ.")
                await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:chat:{tid}")
                    ]]))
                await call.answer(); return

        elif action == "adminlist":
            admins = await bot.get_chat_administrators(cid)
            lines  = ["👮 <b>Администраторы:</b>\n"]
            for adm in admins:
                if adm.user.is_bot: continue
                icon  = "👑" if adm.status == "creator" else "🛡"
                title = f" — <i>{adm.custom_title}</i>" if hasattr(adm, "custom_title") and adm.custom_title else ""
                lines.append(f"{icon} {adm.user.mention_html()}{title}")
            await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:chat:{tid}")
                ]]))
            await call.answer(); return

        # Обновить отображение панели чата
        mat_s  = "✅" if ANTI_MAT_ENABLED else "❌"
        kick_s = "✅" if AUTO_KICK_BOTS else "❌"
        await call.message.edit_text(
            f"🔧 <b>Управление чатом</b>\n\n"
            f"🧼 Антимат: <b>{mat_s}</b>\n"
            f"🤖 Автокик ботов: <b>{kick_s}</b>\n\n"
            f"Выбери действие:",
            parse_mode="HTML", reply_markup=kb_chat_panel(tid))

    except Exception as e:
        logger.error(f"cb_chat action={action}: {e}")
        await call.answer("❗ Ошибка.", show_alert=True)

    await call.answer()


# ─────────────────────────────────────────────────────
#  ТЕКСТОВЫЕ КОМАНДЫ МОДЕРАЦИИ
# ─────────────────────────────────────────────────────

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение пользователя."); return
    reason = command.args or "Нарушение правил"
    target = message.reply_to_message.from_user
    await bot.ban_chat_member(message.chat.id, target.id)
    msg = random.choice(BAN_MESSAGES).format(name=target.mention_html(), reason=reason)
    await message.reply(msg, parse_mode="HTML")
    await send_log(f"🔨 <b>Бан</b>\n👤 {target.full_name} (<code>{target.id}</code>)\n📝 {reason}\n👮 {message.from_user.full_name}", "BAN")

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await message.reply(f"♻️ {target.mention_html()} разбанен.", parse_mode="HTML")
    await send_log(f"♻️ <b>Разбан</b>\n👤 {target.full_name} (<code>{target.id}</code>)", "UNMUTE")

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    mins, label = parse_duration(command.args) if command.args else (60, "1 ч.")
    if not mins: mins, label = 60, "1 ч."
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False),
        until_date=timedelta(minutes=mins))
    msg = random.choice(MUTE_MESSAGES).format(name=target.mention_html(), time=label)
    await message.reply(msg, parse_mode="HTML")
    await send_log(f"🔇 <b>Мут</b>\n👤 {target.full_name} (<code>{target.id}</code>)\n⏱ {label}", "MUTE")

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    await message.reply(f"🔊 {target.mention_html()} размучен.", parse_mode="HTML")
    await send_log(f"🔊 <b>Размут</b>\n👤 {target.full_name} (<code>{target.id}</code>)", "UNMUTE")

@dp.message(Command("warn"))
async def cmd_warn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target  = message.reply_to_message.from_user
    reason  = command.args or "Нарушение правил"
    chat_id = message.chat.id
    warnings[chat_id][target.id] += 1
    count = warnings[chat_id][target.id]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(chat_id, target.id)
        warnings[chat_id][target.id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=target.mention_html(), max=MAX_WARNINGS)
        await send_log(f"🔨 <b>Автобан</b>\n👤 {target.full_name} (<code>{target.id}</code>)", "BAN")
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
        await send_log(f"⚠️ <b>Варн {count}/{MAX_WARNINGS}</b>\n👤 {target.full_name}\n📝 {reason}", "WARN")
    await message.reply(msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def cmd_unwarn(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target  = message.reply_to_message.from_user
    chat_id = message.chat.id
    if warnings[chat_id][target.id] > 0: warnings[chat_id][target.id] -= 1
    count = warnings[chat_id][target.id]
    await message.reply(
        f"✅ С {target.mention_html()} снят варн. Осталось: <b>{count}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")
    await send_log(f"✅ <b>Снят варн</b>\n👤 {target.full_name} (<code>{target.id}</code>)", "UNWARN")

@dp.message(Command("del"))
async def cmd_del(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    await message.reply_to_message.delete()
    await message.delete()
    await send_log(f"🗑 <b>Удалено сообщение</b>\n💬 Чат: <code>{message.chat.id}</code>", "CMD")

@dp.message(Command("clear"))
async def cmd_clear(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError:
        await message.reply("❗ /clear 20"); return
    deleted = 0
    for i in range(message.message_id, message.message_id - count - 1, -1):
        try:
            await bot.delete_message(message.chat.id, i)
            deleted += 1
        except Exception: pass
    sent = await message.answer(f"🧹 Удалено: <b>{deleted}</b> сообщений.", parse_mode="HTML")
    await send_log(f"🧹 <b>Clear</b> {deleted} сообщ.\n💬 Чат: <code>{message.chat.id}</code>", "CMD")
    await asyncio.sleep(3)
    try: await sent.delete()
    except: pass

@dp.message(Command("announce"))
async def cmd_announce(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        # Войти в режим ожидания текста
        pending[message.from_user.id] = {
            "action": "announce_text", "target_id": 0,
            "target_name": "", "chat_id": message.chat.id}
        await message.reply("📢 Напиши текст объявления:"); return
    await message.delete()
    await message.answer(
        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}",
        parse_mode="HTML")
    await send_log(f"📢 <b>Объявление</b>\n💬 Чат: <code>{message.chat.id}</code>\n📝 {command.args[:100]}", "CMD")

@dp.message(Command("pin"))
async def cmd_pin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    await bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Закреплено!")
    await send_log(f"📌 <b>Закреплено</b>\n💬 Чат: <code>{message.chat.id}</code>", "CMD")

@dp.message(Command("unpin"))
async def cmd_unpin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await message.reply("📌 Откреплено!")

@dp.message(Command("lock"))
async def cmd_lock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await message.reply("🔒 Чат <b>заблокирован</b>.", parse_mode="HTML")
    await send_log(f"🔒 <b>Чат заблокирован</b>\n💬 <code>{message.chat.id}</code>\n👮 {message.from_user.full_name}", "LOCK")

@dp.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id,
        ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True,
            can_add_web_page_previews=True, can_invite_users=True))
    await message.reply("🔓 Чат <b>разблокирован</b>.", parse_mode="HTML")
    await send_log(f"🔓 <b>Чат разблокирован</b>\n💬 <code>{message.chat.id}</code>", "LOCK")

@dp.message(Command("slowmode"))
async def cmd_slowmode(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: delay = int(command.args) if command.args else 10
    except ValueError:
        await message.reply("❗ /slowmode 30"); return
    await bot.set_chat_slow_mode_delay(message.chat.id, delay)
    if delay == 0: await message.reply("🐇 Медленный режим выключен.")
    else:          await message.reply(f"🐢 Медленный режим: <b>{delay} сек.</b>", parse_mode="HTML")

@dp.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    title  = command.args or "Участник"
    target = message.reply_to_message.from_user
    await bot.set_chat_administrator_custom_title(message.chat.id, target.id, title)
    await message.reply(f"🏅 {target.mention_html()} получил тег: <b>{title}</b>", parse_mode="HTML")

@dp.message(Command("poll"))
async def cmd_poll(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args or "|" not in command.args:
        await message.reply("❗ /poll Вопрос|Вар1|Вар2"); return
    parts = [p.strip() for p in command.args.split("|")]
    if len(parts) < 3:
        await message.reply("❗ Нужно минимум 2 варианта."); return
    await message.delete()
    await bot.send_poll(message.chat.id, question=parts[0], options=parts[1:], is_anonymous=False)
    await send_log(f"📊 <b>Голосование</b>\n❓ {parts[0]}\n💬 Чат: <code>{message.chat.id}</code>", "POLL")

@dp.message(Command("antimat"))
async def cmd_antimat(message: Message, command: CommandObject):
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

@dp.message(Command("autokick"))
async def cmd_autokick(message: Message, command: CommandObject):
    global AUTO_KICK_BOTS
    if not await require_admin(message): return
    if not command.args:
        s = "вкл ✅" if AUTO_KICK_BOTS else "выкл ❌"
        await message.reply(f"🤖 Автокик ботов: <b>{s}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on":
        AUTO_KICK_BOTS = True
        await message.reply("🤖 Автокик ботов <b>включён</b>!", parse_mode="HTML")
    elif a == "off":
        AUTO_KICK_BOTS = False
        await message.reply("🤖 Автокик ботов <b>выключен</b>.", parse_mode="HTML")

@dp.message(Command("warn24"))
async def cmd_warn24(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False),
        until_date=timedelta(hours=24))
    await message.reply(
        f"📵 {target.mention_html()} — мут на <b>24 часа</b> за рекламу!\n"
        f"Следующее нарушение — бан.", parse_mode="HTML")
    await send_log(f"📵 <b>Мут 24ч (реклама)</b>\n👤 {target.full_name} (<code>{target.id}</code>)", "MUTE")

@dp.message(Command("rban"))
async def cmd_rban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    reason = random.choice(RANDOM_BAN_REASONS)
    await message.reply(
        f"🎲 {target.mention_html()} получил <b>шуточный бан</b>!\n"
        f"📝 Причина: {reason} 😄\n<i>(реального бана нет)</i>",
        parse_mode="HTML")

@dp.message(Command("adminlist"))
async def cmd_adminlist(message: Message):
    if not await require_admin(message): return
    admins = await bot.get_chat_administrators(message.chat.id)
    lines  = ["👮 <b>Администраторы чата:</b>\n"]
    for admin in admins:
        if admin.user.is_bot: continue
        icon  = "👑" if admin.status == "creator" else "🛡"
        title = f" — <i>{admin.custom_title}</i>" if hasattr(admin, "custom_title") and admin.custom_title else ""
        lines.append(f"{icon} {admin.user.mention_html()}{title}")
    await message.reply("\n".join(lines), parse_mode="HTML")

@dp.message(Command("setadmin"))
async def cmd_setadmin(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if command.args:
        username = command.args.strip().lstrip("@")
        await message.reply(
            f"👑 Чтобы выдать права @{username}:\n\n"
            f"Настройки группы → Администраторы → Добавить → найди @{username}\n\n"
            f"<i>Telegram не позволяет ботам выдавать права напрямую.</i>",
            parse_mode="HTML")
    else:
        pending[message.from_user.id] = {
            "action": "adminpromote_username",
            "target_id": 0, "target_name": "", "chat_id": message.chat.id}
        await message.reply("👑 Введи @username пользователя:")

@dp.message(Command("note"))
async def cmd_note(message: Message, command: CommandObject):
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
#  ИНФОРМАЦИЯ И СТАТИСТИКА
# ─────────────────────────────────────────────────────

@dp.message(Command("warnings"))
async def cmd_warnings(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    count  = warnings[message.chat.id].get(target.id, 0)
    await message.reply(f"📊 {target.mention_html()} — варнов: <b>{count}/{MAX_WARNINGS}</b>", parse_mode="HTML")

@dp.message(Command("info"))
async def cmd_info(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    user   = message.reply_to_message.from_user
    member = await bot.get_chat_member(message.chat.id, user.id)
    warns  = warnings[message.chat.id].get(user.id, 0)
    rep    = reputation[message.chat.id].get(user.id, 0)
    msgs   = chat_stats[message.chat.id].get(user.id, 0)
    status_map = {
        "creator":"👑 Создатель", "administrator":"🛡 Администратор",
        "member":"👤 Участник",   "restricted":"🔇 Ограничен",
        "kicked":"🔨 Забанен",    "left":"🚶 Вышел",
    }
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

@dp.message(Command("top"))
async def cmd_top(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats:
        await message.reply("📊 Статистика пуста!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = ["📊 <b>Топ активных участников:</b>\n"]
    for i, (uid, count) in enumerate(sorted_u):
        try:
            m     = await bot.get_chat_member(message.chat.id, uid)
            uname = m.user.full_name
        except Exception:
            uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {count} сообщ.")
    await message.reply("\n".join(lines), parse_mode="HTML")

@dp.message(Command("afk"))
async def cmd_afk(message: Message, command: CommandObject):
    reason = command.args or "без причины"
    afk_users[message.from_user.id] = reason
    await message.reply(f"😴 {message.from_user.mention_html()} ушёл в AFK: {reason}", parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  РЕПУТАЦИЯ
# ─────────────────────────────────────────────────────

@dp.message(Command("rep"))
async def cmd_rep(message: Message):
    if not message.reply_to_message:
        await message.reply("↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    score  = reputation[message.chat.id][target.id]
    emoji  = "⭐" if score >= 0 else "💀"
    await message.reply(f"{emoji} Репутация {target.mention_html()}: <b>{score:+d}</b>", parse_mode="HTML")

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
        await message.reply(f"⏳ Подожди ещё {left//60} мин."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] += 1
    score = reputation[message.chat.id][target.id]
    await message.reply(f"⬆️ {target.mention_html()} +1 к репутации! Теперь: <b>{score:+d}</b>", parse_mode="HTML")

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
        await message.reply(f"⏳ Подожди ещё {left//60} мин."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] -= 1
    score = reputation[message.chat.id][target.id]
    await message.reply(f"⬇️ {target.mention_html()} -1 к репутации! Теперь: <b>{score:+d}</b>", parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  ИГРЫ И ПРИКОЛЮХИ
# ─────────────────────────────────────────────────────

@dp.message(Command("weather"))
async def cmd_weather(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("🌤 Укажи город: /weather Москва"); return
    wait = await message.reply("⏳ Получаю данные...")
    result = await get_weather(command.args)
    await wait.edit_text(result, parse_mode="HTML")

@dp.message(Command("quote"))
async def cmd_quote(message: Message):
    await message.reply(f"💬 {random.choice(QUOTES)}")

@dp.message(Command("roll"))
async def cmd_roll(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 10000))
    except: sides = 6
    result = random.randint(1, sides)
    await message.reply(f"🎲 Бросаю d{sides}... выпало: <b>{result}</b>!", parse_mode="HTML")

@dp.message(Command("flip"))
async def cmd_flip(message: Message):
    await message.reply(random.choice(["🪙 Орёл!", "🪙 Решка!"]))

@dp.message(Command("8ball"))
async def cmd_8ball(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("❓ /8ball [вопрос]"); return
    await message.reply(
        f"🎱 <b>Вопрос:</b> {command.args}\n\n<b>Ответ:</b> {random.choice(BALL_ANSWERS)}",
        parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message, command: CommandObject):
    if not command.args:
        await message.reply("❗ /rate [что]"); return
    score = random.randint(0, 10)
    bar   = "⭐" * score + "☆" * (10 - score)
    await message.reply(f"📊 <b>{command.args}</b>\n{bar}\nОценка: <b>{score}/10</b>", parse_mode="HTML")

@dp.message(Command("iq"))
async def cmd_iq(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    iq = random.randint(1, 200)
    if iq < 70:    c = "🥔 Картошка умнее."
    elif iq < 100: c = "🐒 Обезьяна справилась бы лучше."
    elif iq < 130: c = "😐 Сойдёт."
    elif iq < 160: c = "🧠 Умный человек!"
    else:          c = "🤖 Эйнштейн, это ты?!"
    await message.reply(f"🧠 IQ {user.mention_html()}: <b>{iq}</b>\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def cmd_gay(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    p    = random.randint(0, 100)
    bar  = "🌈" * (p // 10) + "⬜" * (10 - p // 10)
    await message.reply(
        f"🏳️‍🌈 {user.mention_html()}\n{bar}\n<b>{p}%</b> — это шутка 😄", parse_mode="HTML")

@dp.message(Command("truth"))
async def cmd_truth(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"🤔 <b>Вопрос для {user.mention_html()}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}",
        parse_mode="HTML")

@dp.message(Command("dare"))
async def cmd_dare(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"😈 <b>Задание для {user.mention_html()}:</b>\n\n{random.choice(DARE_CHALLENGES)}",
        parse_mode="HTML")

@dp.message(Command("wyr"))
async def cmd_wyr(message: Message):
    await message.reply(f"🤯 <b>Выбор без выбора:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML")

@dp.message(Command("rps"))
async def cmd_rps(message: Message, command: CommandObject):
    choices = {"к": "🪨 Камень", "н": "✂️ Ножницы", "б": "📄 Бумага"}
    wins    = {"к": "н", "н": "б", "б": "к"}
    if not command.args or command.args.lower() not in choices:
        await message.reply("✂️ /rps к (камень) / н (ножницы) / б (бумага)"); return
    player     = command.args.lower()
    bot_choice = random.choice(list(choices.keys()))
    if player == bot_choice:       result = "🤝 Ничья!"
    elif wins[player] == bot_choice: result = "🎉 Ты выиграл!"
    else:                            result = "😈 Я выиграл!"
    await message.reply(f"Ты: {choices[player]}\nЯ: {choices[bot_choice]}\n\n{result}")

@dp.message(Command("slot"))
async def cmd_slot(message: Message):
    symbols = ["🍒", "🍋", "🍊", "🍇", "⭐", "7️⃣", "💎"]
    s1, s2, s3 = random.choice(symbols), random.choice(symbols), random.choice(symbols)
    if s1 == s2 == s3 == "💎":   result = "💰 ДЖЕКПОТ!! Три бриллианта!!"
    elif s1 == s2 == s3:         result = f"🎉 Три {s1}! Выиграл!"
    elif s1==s2 or s2==s3 or s1==s3: result = "😐 Два одинаковых. Почти!"
    else:                        result = "😢 Не повезло. Попробуй ещё!"
    await message.reply(f"🎰 [ {s1} | {s2} | {s3} ]\n\n{result}")

@dp.message(Command("choose"))
async def cmd_choose(message: Message, command: CommandObject):
    if not command.args or "|" not in command.args:
        await message.reply("❗ /choose вариант1|вариант2|вариант3"); return
    options = [o.strip() for o in command.args.split("|") if o.strip()]
    if len(options) < 2:
        await message.reply("❗ Нужно минимум 2 варианта."); return
    await message.reply(
        f"🎯 Выбираю из {len(options)} вариантов...\n\n✅ <b>{random.choice(options)}</b>!",
        parse_mode="HTML")

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    sign, text = random.choice(list(HOROSCOPES.items()))
    await message.reply(
        f"{sign} <b>Гороскоп для {user.mention_html()}:</b>\n\n{text}",
        parse_mode="HTML")

@dp.message(Command("predict"))
async def cmd_predict(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"🔮 <b>Предсказание для {user.mention_html()}:</b>\n\n{random.choice(PREDICTIONS)}",
        parse_mode="HTML")

@dp.message(Command("compliment"))
async def cmd_compliment(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"💐 <b>{user.mention_html()}</b>, {random.choice(COMPLIMENTS)}",
        parse_mode="HTML")

@dp.message(Command("countdown"))
async def cmd_countdown(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: n = min(int(command.args or 5), 10)
    except: n = 5
    sent = await message.reply(f"⏱ Обратный отсчёт: <b>{n}</b>...", parse_mode="HTML")
    for i in range(n - 1, 0, -1):
        await asyncio.sleep(1)
        try: await sent.edit_text(f"⏱ Обратный отсчёт: <b>{i}</b>...", parse_mode="HTML")
        except: pass
    await asyncio.sleep(1)
    await sent.edit_text("🚀 <b>ПОЕХАЛИ!</b>", parse_mode="HTML")

@dp.message(Command("botstats"))
async def cmd_botstats(message: Message):
    if not await require_admin(message): return
    total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
    total_warns = sum(sum(v.values()) for v in warnings.values())
    total_afk   = len(afk_users)
    await message.reply(
        f"📊 <b>Статистика бота</b>\n\n"
        f"💬 Всего сообщений: <b>{total_msgs}</b>\n"
        f"⚠️ Активных варнов: <b>{total_warns}</b>\n"
        f"😴 AFK пользователей: <b>{total_afk}</b>\n"
        f"🌐 Чатов отслеживается: <b>{len(chat_stats)}</b>\n"
        f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
        f"🤖 Автокик ботов: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
        parse_mode="HTML")


# ─────────────────────────────────────────────────────
#  АВТООТВЕТЫ НА ТРИГГЕР-СЛОВА
# ─────────────────────────────────────────────────────

@dp.message(F.text & ~F.text.startswith("/"))
async def auto_reply_handler(message: Message):
    if not message.text: return
    text_lower = message.text.lower()
    for trigger, responses in TRIGGER_RESPONSES.items():
        if trigger in text_lower:
            if random.random() < 0.3:
                await message.reply(random.choice(responses))
            break


# ─────────────────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────────────────

async def main():
    await start_web()
    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN не задан!")
    logger.info("✅ Бот запускается...")
    await send_log("🚀 <b>Бот запущен!</b>\n\nВсе системы работают нормально.", "INFO")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

# -*- coding: utf-8 -*-
"""
Единая тема интерфейса бота (Telegram HTML + подписи кнопок).
Меняйте константы здесь — внешний вид обновится во всех подключённых местах.
"""
from __future__ import annotations

# ── Текстовые акценты ─────────────────────────────────────
SEP = "────────────────────"
SEP_SHORT = "───────────────"
SEP_THICK = "━━━━━━━━━━━━━━━━━━━━"

BRAND = "SKIN VAULT"
TAGLINE = "консоль администратора"


def main_menu_text(
    chat_title: str,
    total_msgs: int,
    total_warns: int,
    antimat_on: bool,
    autokick_on: bool,
    active_members: int,
    open_reports: int,
) -> str:
    am = "вкл" if antimat_on else "выкл"
    ak = "вкл" if autokick_on else "выкл"
    return (
        f"<b>◆ {BRAND}</b>\n"
        f"<i>{TAGLINE}</i>\n"
        f"{SEP}\n\n"
        f"📍 <b>{chat_title}</b>\n\n"
        f"┊ людей в базе: <b>{active_members}</b>\n"
        f"┊ сообщений: <b>{total_msgs}</b>\n"
        f"┊ варнов всего: <b>{total_warns}</b>\n"
        f"┊ открытых жалоб: <b>{open_reports}</b>\n"
        f"┊ антимат: <b>{am}</b> · автокик: <b>{ak}</b>\n\n"
        f"<i>Выберите раздел</i>"
    )


def section_title(icon: str, title: str) -> str:
    return f"{icon} <b>{title}</b>"


# ── Навигация ───────────────────────────────────────────────
BTN_BACK = "◀ Назад"
BTN_CLOSE = "✕ Закрыть"
BTN_CANCEL = "✕ Отмена"

# ── Главное меню панели ───────────────────────────────────
BTN_MM_USER = "👤 Участник"
BTN_MM_REPORTS = "🚨 Жалобы"
BTN_MM_MEMBERS = "👥 Участники"
BTN_MM_BANLIST = "🚫 Баны"
BTN_MM_SETTINGS = "⚙ Настройки"
BTN_MM_MOD = "🛡 Модерация"
BTN_MM_STATS = "📊 Статистика"
BTN_MM_ROLES = "🎖 Роли"
BTN_MM_QUICK = "⚡ Быстрые ответы"
BTN_MM_PINS = "📌 Закрепы"
BTN_MM_WELCOME = "👋 Приветствие"
BTN_MM_PLUGINS = "🧩 Плагины"
BTN_MM_TICKETS = "🎫 Тикеты"
BTN_MM_WEBAPP = "📱 Mini App"

# ── Панель участника ──────────────────────────────────────
BTN_UP_MUTE = "🔇 Мут"
BTN_UP_UNMUTE = "🔊 Размут"
BTN_UP_WARN = "⚡ Варн"
BTN_UP_UNWARN = "✅ Снять варн"
BTN_UP_BAN = "🔨 Бан"
BTN_UP_UNBAN = "🕊 Разбан"
BTN_UP_SILENT = "👻 Тихий бан"
BTN_UP_TEMP24 = "⏳ Бан на 24ч"
BTN_UP_STICKER = "🙅 Стикермут"
BTN_UP_GIF = "🎬 Гифмут"
BTN_UP_VOICE = "🎙 Войсмут"
BTN_UP_ALLMED = "🚫 Всё медиа"
BTN_UP_INFO = "ℹ Инфо"
BTN_UP_HISTORY = "📋 История"
BTN_UP_NOTES = "📝 Заметки"
BTN_UP_VIP = "💎 VIP"
BTN_UP_FUN = "🎭 Развлечения"

# ── Владелец ──────────────────────────────────────────────
BTN_OW_CHATS = "🌍 Все чаты"
BTN_OW_STATUS = "📡 Статус"
BTN_OW_SOS = "🆘 SOS всем"
BTN_OW_SOSOFF = "🔓 Снять локдаун"
BTN_OW_BROADCAST = "📢 Рассылка"
BTN_OW_BLACKLIST = "🚷 Чёрный список"
BTN_OW_BACKUP = "💾 Бэкап"
BTN_OW_AUDIT = "🔎 Аудит"
BTN_OW_PLUGINS = "🧩 Плагины глобально"
BTN_OW_CAL = "📅 Календарь"
BTN_OW_TASKS = "🎯 Задачи модов"
BTN_OW_MODTOP = "🏆 Рейтинг модов"
BTN_OW_EVAC = "🚁 Эвакуация"
BTN_OW_QUAR = "🔬 Карантин"
BTN_OW_CLEAN = "🧹 Зачистка"
BTN_OW_LINK = "🔗 Связать чаты"

# ── Мут (время) ───────────────────────────────────────────
BTN_M_5 = "5 мин"
BTN_M_15 = "15 мин"
BTN_M_30 = "30 мин"
BTN_M_60 = "1 час"
BTN_M_180 = "3 часа"
BTN_M_720 = "12 часов"
BTN_M_1440 = "1 день"
BTN_M_10080 = "7 дней"
BTN_M_CUSTOM = "✎ Своя длительность"

# ── Варн (причины) — callback_data не трогать в bot.py ───
BTN_W_MAT = "🤬 Мат"
BTN_W_SPAM = "📨 Спам"
BTN_W_INSULT = "💢 Оскорбление"
BTN_W_FLOOD = "🌊 Флуд"
BTN_W_AD = "📣 Реклама"
BTN_W_18 = "🔞 18+"
BTN_W_PROV = "⚡ Провокация"
BTN_W_CUSTOM = "✎ Своя причина"

# ── Бан ─────────────────────────────────────────────────────
BTN_B_RULES = "🔥 Правила"
BTN_B_SPAM = "📣 Спам / реклама"
BTN_B_18 = "🔞 18+"
BTN_B_BOT = "🤖 Бот / накрутка"
BTN_B_24 = "⏱ 24 часа"
BTN_B_7D = "⏱ 7 дней"
BTN_B_CUSTOM = "✎ Своя причина"

# ── Fun ───────────────────────────────────────────────────
BTN_F_RBAN = "🎲 Шуточный бан"
BTN_F_PRED = "🔮 Предсказание"
BTN_F_HORO = "🌌 Гороскоп"
BTN_F_COMPL = "🌸 Комплимент"

# ── Сообщения ─────────────────────────────────────────────
BTN_MSG_PIN = "📌 Закрепить"
BTN_MSG_UNPIN = "📍 Открепить"
BTN_MSG_DEL = "🗑 Удалить"
BTN_MSG_C10 = "Очистить 10"
BTN_MSG_C20 = "Очистить 20"
BTN_MSG_C50 = "Очистить 50"
BTN_MSG_ANN = "📢 Объявление"
BTN_MSG_POLL = "📊 Опрос"

# ── Участники ─────────────────────────────────────────────
BTN_MEM_ADM = "👮 Админы"
BTN_MEM_TOP = "🏆 Топ активности"
BTN_MEM_XP = "📈 Топ XP"
BTN_MEM_MVP = "🥇 Топ MVP"
BTN_MEM_MUTE24 = "🔇 Мут 24ч (реклама)"
BTN_MEM_WARNI = "⚡ Варны"
BTN_MEM_BANS = "🚫 Баны"
BTN_MEM_MODREP = "📋 Отчёт модератора"

# ── Чат ───────────────────────────────────────────────────
BTN_CH_LOCK = "🔒 Закрыть чат"
BTN_CH_UNLOCK = "🔓 Открыть чат"
BTN_CH_S10 = "slow 10с"
BTN_CH_S30 = "slow 30с"
BTN_CH_S60 = "slow 60с"
BTN_CH_S0 = "выкл slow"
BTN_CH_RULES = "📜 Правила"
BTN_CH_BOTSTATS = "📈 Стат. бота"
BTN_CH_TOUR1 = "🎪 Турнир старт"
BTN_CH_TOUR2 = "🏁 Турнир стоп"

# ── Игры ──────────────────────────────────────────────────
BTN_G_DICE = "🎲 Кубик"
BTN_G_COIN = "🪙 Монетка"
BTN_G_8BALL = "🎱 Шар 8"
BTN_G_TRIVIA = "🧩 Викторина"
BTN_G_RPS_K = "✊ Камень"
BTN_G_RPS_N = "✌ Ножницы"
BTN_G_RPS_B = "🖐 Бумага"
BTN_G_GUESS = "🎯 Угадай число"
BTN_G_W_MOW = "🌤 Погода: Москва"
BTN_G_W_CITY = "🌍 Свой город"
BTN_G_CD5 = "⏱ 5 сек"
BTN_G_CD10 = "⏱ 10 сек"

# ── Тикеты (также для tickets.py) ─────────────────────────
PRIORITY_EMOJI = {
    "low": "●",
    "normal": "◐",
    "high": "◉",
    "urgent": "✦",
}
STATUS_EMOJI = {
    "open": "○",
    "in_progress": "◐",
    "closed": "●",
}

BTN_TKT_NEW = "＋ Новый тикет"
BTN_TKT_MY = "≡ Мои тикеты"
BTN_TKT_CANCEL = "✕ Отмена"
BTN_TKT_PRI_LOW = "● Низкий"
BTN_TKT_PRI_NORM = "◐ Обычный"
BTN_TKT_PRI_HIGH = "◉ Высокий"
BTN_TKT_PRI_URG = "✦ Срочно"
BTN_TKT_REPLY = "↩ Ответить"
BTN_TKT_CLOSE = "✓ Закрыть"
BTN_TKT_URGENT = "✦ В срочные"
BTN_TKT_TAKE = "👤 Взять"
BTN_TKT_HIST = "≡ История"
BTN_TKT_WRITE = "✎ Написать"
BTN_TKT_INPROG = "🔄 В работе"
BTN_TKT_CLOSED = "✅ Закрытые"
BTN_TKT_EMPTY = "— тикетов нет —"

# ── Уведомления ───────────────────────────────────────────
BTN_NOTIF_SETTINGS = "⚙ Настройки"
BTN_NOTIF_DISABLE = "🔕 Тишина"

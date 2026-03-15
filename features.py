"""
features.py — Новые фичи для CHAT GUARD бота
Подключение в bot.py:
    import features
    # В main(): await features.init(bot, dp, ADMIN_IDS, OWNER_ID)
"""
import asyncio
import random
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)

log = logging.getLogger(__name__)

# Глобальные ссылки
_bot: Optional[Bot] = None
_admin_ids: set = set()
_owner_id: int = 0

# ── Состояния ──────────────────────────────────────────────────
anon_reply_states: Dict[int, int] = {}       # {uid: anon_msg_id}
anon_box_states: Dict[int, dict] = {}        # {uid: {step, cid}}
auction_states: Dict[int, dict] = {}         # {cid: auction_data}
poll_states: Dict[int, dict] = {}            # {cid: poll_data}

# ── Блокировки анонимок ────────────────────────────────────────
anon_blocks: Dict[int, set] = {}             # {uid: {blocked_uid, ...}}


async def init(bot: Bot, dp: Dispatcher, admin_ids: set, owner_id: int):
    global _bot, _admin_ids, _owner_id
    _bot = bot
    _admin_ids = admin_ids
    _owner_id = owner_id
    _register_handlers(dp)
    log.info("✅ features.py инициализирован")


def db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_tables():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS anon_messages_v2 (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        from_uid    INTEGER,
        to_uid      INTEGER,
        cid         INTEGER,
        text        TEXT,
        anon_nick   TEXT,
        ts          INTEGER DEFAULT (strftime('%s','now')),
        reactions   TEXT DEFAULT '{}',
        is_box      INTEGER DEFAULT 0,
        reply_to_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS anon_blocks (
        uid         INTEGER,
        blocked_uid INTEGER,
        PRIMARY KEY (uid, blocked_uid)
    );
    CREATE TABLE IF NOT EXISTS anon_box (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER,
        owner_uid   INTEGER,
        question    TEXT,
        msg_id      INTEGER,
        created_at  INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS achievements (
        uid         INTEGER,
        key         TEXT,
        unlocked_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (uid, key)
    );
    CREATE TABLE IF NOT EXISTS polls_v2 (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER,
        creator_uid INTEGER,
        question    TEXT,
        options     TEXT,
        votes       TEXT DEFAULT '{}',
        anonymous   INTEGER DEFAULT 0,
        ends_at     INTEGER,
        msg_id      INTEGER,
        closed      INTEGER DEFAULT 0,
        created_at  INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS auction_items (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER,
        seller_uid  INTEGER,
        seller_name TEXT,
        item_name   TEXT,
        start_price INTEGER,
        current_bid INTEGER,
        bidder_uid  INTEGER,
        bidder_name TEXT,
        ends_at     INTEGER,
        msg_id      INTEGER,
        closed      INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS shop_items (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        category    TEXT,
        name        TEXT,
        emoji       TEXT,
        price       INTEGER,
        description TEXT,
        effect      TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_anon_to_uid ON anon_messages_v2(to_uid);
    CREATE INDEX IF NOT EXISTS idx_anon_cid    ON anon_messages_v2(cid);
    CREATE INDEX IF NOT EXISTS idx_polls_cid   ON polls_v2(cid);
    """)
    conn.commit()
    conn.close()
    _seed_shop()


def _seed_shop():
    """Заполняет магазин базовыми товарами если пусто"""
    conn = db()
    count = conn.execute("SELECT COUNT(*) FROM shop_items").fetchone()[0]
    if count == 0:
        items = [
            # Категория: Титулы
            ("titles", "👑 Король чата",   "👑", 500,  "Престижный титул", "title:king"),
            ("titles", "💎 Легенда",        "💎", 1000, "Для избранных",    "title:legend"),
            ("titles", "🔥 Огонёк",         "🔥", 200,  "Горячий участник", "title:fire"),
            ("titles", "🌟 Звезда",         "🌟", 300,  "Яркая личность",   "title:star"),
            ("titles", "🐺 Волк-одиночка",  "🐺", 250,  "Сам по себе",      "title:wolf"),
            ("titles", "🦅 Орёл",           "🦅", 400,  "Высоко летаешь",   "title:eagle"),
            # Категория: Бустеры
            ("boosters", "⚡ XP x2 (1ч)",   "⚡", 150, "Двойной XP на час",   "boost:xp2:3600"),
            ("boosters", "💰 Репа x2 (1ч)", "💰", 200, "Двойная репа на час", "boost:rep2:3600"),
            ("boosters", "🛡 Защита (24ч)", "🛡", 300, "Защита от -репы",     "boost:shield:86400"),
            # Категория: Эффекты
            ("effects", "✨ Блеск ника",    "✨", 100, "Красивый ник в топе", "effect:shine"),
            ("effects", "🎭 Маска",         "🎭", 150, "Скрыть реп в профиле","effect:mask"),
            ("effects", "🌈 Радуга",        "🌈", 200, "Цветной ник",         "effect:rainbow"),
            # Категория: Разное
            ("misc", "🎲 Лотерея x3",      "🎲", 50,  "3 лотерейных билета",  "lottery:3"),
            ("misc", "💌 Анонимка VIP",    "💌", 100, "Анонимка без лимитов", "anon:vip"),
            ("misc", "🔮 Предсказание+",   "🔮", 75,  "Точное предсказание",  "predict:plus"),
        ]
        conn.executemany(
            "INSERT INTO shop_items (category, name, emoji, price, description, effect) "
            "VALUES (?,?,?,?,?,?)",
            items
        )
        conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════
#  ДОСТИЖЕНИЯ
# ══════════════════════════════════════════════════════════════

ACHIEVEMENTS = {
    "first_msg":    ("💬", "Первое слово",      "Написал первое сообщение"),
    "msg_100":      ("📝", "Болтун",             "100 сообщений в чате"),
    "msg_1000":     ("🗣", "Оратор",             "1000 сообщений в чате"),
    "msg_10000":    ("📢", "Легенда чата",       "10000 сообщений в чате"),
    "rep_100":      ("⭐", "Уважаемый",          "100 очков репутации"),
    "rep_500":      ("🌟", "Авторитет",          "500 очков репутации"),
    "rep_1000":     ("💎", "Икона",              "1000 очков репутации"),
    "level_10":     ("🏅", "Опытный",            "Достиг 10 уровня"),
    "level_50":     ("🥇", "Ветеран",            "Достиг 50 уровня"),
    "level_100":    ("👑", "Мастер",             "Достиг 100 уровня"),
    "streak_7":     ("🔥", "Неделя подряд",      "7 дней активности подряд"),
    "streak_30":    ("💪", "Месяц подряд",       "30 дней активности подряд"),
    "birthday":     ("🎂", "День рождения",      "Сегодня твой день!"),
    "anon_10":      ("💌", "Анонимщик",          "Отправил 10 анонимок"),
    "first_win":    ("🏆", "Победитель",         "Выиграл первый аукцион"),
    "poll_creator": ("📊", "Социолог",           "Создал 5 опросов"),
}


async def check_achievements(uid: int, cid: int, msg_count: int = 0,
                               rep: int = 0, level: int = 0,
                               streak: int = 0) -> List[str]:
    """Проверяет и выдаёт новые достижения"""
    conn = db()
    earned = conn.execute(
        "SELECT key FROM achievements WHERE uid=?", (uid,)
    ).fetchall()
    earned_keys = {r["key"] for r in earned}
    conn.close()

    new_achievements = []

    checks = [
        ("first_msg",  msg_count >= 1),
        ("msg_100",    msg_count >= 100),
        ("msg_1000",   msg_count >= 1000),
        ("msg_10000",  msg_count >= 10000),
        ("rep_100",    rep >= 100),
        ("rep_500",    rep >= 500),
        ("rep_1000",   rep >= 1000),
        ("level_10",   level >= 10),
        ("level_50",   level >= 50),
        ("level_100",  level >= 100),
        ("streak_7",   streak >= 7),
        ("streak_30",  streak >= 30),
    ]

    conn = db()
    for key, condition in checks:
        if condition and key not in earned_keys:
            conn.execute(
                "INSERT OR IGNORE INTO achievements (uid, key) VALUES (?,?)",
                (uid, key)
            )
            new_achievements.append(key)
    conn.commit()
    conn.close()

    return new_achievements


async def notify_achievement(bot: Bot, cid: int, uid: int, keys: List[str]):
    """Уведомляет чат о новых достижениях"""
    for key in keys:
        if key not in ACHIEVEMENTS:
            continue
        emoji, name, desc = ACHIEVEMENTS[key]
        try:
            await bot.send_message(
                cid,
                f"🏆 <b>ДОСТИЖЕНИЕ РАЗБЛОКИРОВАНО!</b>\n\n"
                f"{emoji} <b>{name}</b>\n"
                f"📝 {desc}\n\n"
                f"👤 <a href='tg://user?id={uid}'>Поздравляем!</a>",
                parse_mode="HTML"
            )
        except:
            pass


# ══════════════════════════════════════════════════════════════
#  УЛУЧШЕННАЯ СИСТЕМА УРОВНЕЙ
# ══════════════════════════════════════════════════════════════

LEVEL_UP_MESSAGES = [
    "🚀 {name} взлетел на уровень {level}!",
    "⚡ {name} достиг уровня {level}! Сила растёт!",
    "🌟 {name} эволюционировал до уровня {level}!",
    "💥 БУМ! {name} теперь уровень {level}!",
    "🔥 {name} горит на уровне {level}!",
    "👑 {name} поднялся до уровня {level}! Уважение!",
    "🎯 {name} пробил отметку уровня {level}!",
    "💎 {name} засиял на уровне {level}!",
]

LEVEL_TITLES_NEW = {
    1:   ("🌱", "Росток"),
    5:   ("🌿", "Побег"),
    10:  ("🌳", "Дерево"),
    15:  ("⚡", "Искра"),
    20:  ("🔥", "Огонёк"),
    25:  ("💫", "Звёздочка"),
    30:  ("🌟", "Звезда"),
    35:  ("🏅", "Медалист"),
    40:  ("🥈", "Серебро"),
    45:  ("🥇", "Золото"),
    50:  ("💎", "Бриллиант"),
    60:  ("🦁", "Лев"),
    70:  ("🐉", "Дракон"),
    80:  ("⚔️", "Воин"),
    90:  ("🛡", "Страж"),
    100: ("👑", "Король"),
    120: ("🌌", "Космонавт"),
    150: ("🔮", "Мистик"),
    200: ("🌀", "Легенда"),
    250: ("💀", "Бессмертный"),
}


def get_level_title_new(level: int) -> tuple:
    title = ("🌱", "Новичок")
    for lvl, data in sorted(LEVEL_TITLES_NEW.items()):
        if level >= lvl:
            title = data
    return title


async def announce_level_up(bot: Bot, cid: int, uid: int,
                             name: str, new_level: int, new_xp: int):
    """Красивое объявление о повышении уровня"""
    emoji, title = get_level_title_new(new_level)
    msg_template = random.choice(LEVEL_UP_MESSAGES)
    msg = msg_template.format(name=name, level=new_level)

    # Специальные анонсы для круглых уровней
    if new_level % 50 == 0:
        special = f"🎊 <b>НЕВЕРОЯТНО!</b> Уровень {new_level}!\n\n"
    elif new_level % 25 == 0:
        special = f"🎉 <b>ОТЛИЧНО!</b> Уровень {new_level}!\n\n"
    elif new_level % 10 == 0:
        special = f"✨ <b>КРУТО!</b> Уровень {new_level}!\n\n"
    else:
        special = ""

    text = (
        f"{special}"
        f"━━━━━━━━━━━━━━━\n"
        f"⬆️ <b>НОВЫЙ УРОВЕНЬ!</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"👤 <a href='tg://user?id={uid}'>{name}</a>\n"
        f"{msg}\n\n"
        f"{emoji} <b>{title}</b>\n"
        f"📊 Уровень: <b>{new_level}</b>\n"
        f"⭐ XP: <b>{new_xp:,}</b>"
    )

    try:
        await bot.send_message(cid, text, parse_mode="HTML")
    except:
        pass

    # Проверяем достижения
    new_ach = await check_achievements(uid, cid, level=new_level)
    if new_ach:
        await notify_achievement(bot, cid, uid, new_ach)


# ══════════════════════════════════════════════════════════════
#  ПРОФИЛЬ-КАРТОЧКА
# ══════════════════════════════════════════════════════════════

async def cmd_profile_card(message: Message):
    """Красивая профиль-карточка пользователя"""
    target = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid = target.id
    cid = message.chat.id

    conn = db()

    # Собираем данные
    stats_row = conn.execute(
        "SELECT msg_count FROM chat_stats WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    rep_row = conn.execute(
        "SELECT score FROM reputation WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    xp_row = conn.execute(
        "SELECT xp FROM xp_data WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    level_row = conn.execute(
        "SELECT level FROM levels WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    streak_row = conn.execute(
        "SELECT streak FROM streaks WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    profile_row = conn.execute(
        "SELECT bio, mood FROM user_profiles WHERE uid=?", (uid,)
    ).fetchone()
    achievements_count = conn.execute(
        "SELECT COUNT(*) FROM achievements WHERE uid=?", (uid,)
    ).fetchone()[0]
    warns_row = conn.execute(
        "SELECT count FROM warnings WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()

    conn.close()

    msgs    = stats_row["msg_count"] if stats_row else 0
    rep     = rep_row["score"] if rep_row else 0
    xp      = xp_row["xp"] if xp_row else 0
    level   = level_row["level"] if level_row else 0
    streak  = streak_row["streak"] if streak_row else 0
    bio     = profile_row["bio"] if profile_row and profile_row["bio"] else "Нет описания"
    mood    = profile_row["mood"] if profile_row and profile_row["mood"] else "😶"
    warns   = warns_row["count"] if warns_row else 0

    emoji_lvl, title = get_level_title_new(level)

    # Ранг в чате
    conn = db()
    rank_row = conn.execute(
        "SELECT COUNT(*)+1 as rank FROM chat_stats "
        "WHERE cid=? AND msg_count > (SELECT msg_count FROM chat_stats WHERE cid=? AND uid=?)",
        (cid, cid, uid)
    ).fetchone()
    rank = rank_row["rank"] if rank_row else "—"
    conn.close()

    # Прогресс-бар XP
    xp_for_next = (level + 1) * 100
    xp_current = xp % 100 if level > 0 else xp
    progress = int((xp_current / xp_for_next) * 10) if xp_for_next else 0
    bar = "▓" * progress + "░" * (10 - progress)

    # Репутация emoji
    if rep >= 500:    rep_emoji = "💎"
    elif rep >= 200:  rep_emoji = "🌟"
    elif rep >= 100:  rep_emoji = "⭐"
    elif rep >= 50:   rep_emoji = "✨"
    elif rep >= 0:    rep_emoji = "😊"
    elif rep >= -50:  rep_emoji = "😐"
    else:             rep_emoji = "💀"

    text = (
        f"━━━━━━━━━━━━━━━\n"
        f"📸 <b>ПРОФИЛЬ</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"{mood} <b>{target.full_name}</b>\n"
        f"<i>{bio}</i>\n\n"
        f"{'─' * 20}\n"
        f"{emoji_lvl} <b>{title}</b> · Ур. <b>{level}</b>\n"
        f"[{bar}] {xp_current}/{xp_for_next} XP\n\n"
        f"💬 Сообщений: <b>{msgs:,}</b>  #{rank} в чате\n"
        f"{rep_emoji} Репутация: <b>{rep:+d}</b>\n"
        f"🔥 Стрик: <b>{streak}</b> дн.\n"
        f"🏆 Достижений: <b>{achievements_count}</b>\n"
        f"⚡ Варнов: <b>{warns}/3</b>\n"
        f"{'─' * 20}\n"
        f"🆔 <code>{uid}</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🏆 Достижения",
            callback_data=f"profile:achievements:{uid}"
        ),
        InlineKeyboardButton(
            text="📊 Статистика",
            callback_data=f"profile:stats:{uid}:{cid}"
        ),
    ]])

    await message.answer(text, parse_mode="HTML", reply_markup=kb)


# ══════════════════════════════════════════════════════════════
#  АНОНИМКИ v2
# ══════════════════════════════════════════════════════════════

ANON_NICKS = [
    "🌑 Тёмный незнакомец", "🌊 Морской волк", "🦊 Хитрая лиса",
    "🐺 Серый волк", "🌙 Ночной странник", "🔥 Огненный дух",
    "❄️ Ледяной воин", "⚡ Молния", "🌪 Вихрь", "💫 Звёздная пыль",
    "🎭 Актёр в маске", "👁 Всевидящее око", "🗡 Тайный клинок",
    "🌹 Красная роза", "🍀 Удачливый клевер", "🦋 Бабочка",
]

REACTION_EMOJIS = ["👍", "💔", "😂", "🔥", "😮", "👏", "💯", "🤡"]


def kb_anon_reactions(msg_id: int, reactions: dict) -> InlineKeyboardMarkup:
    """Клавиатура реакций для анонимки"""
    rows = []
    row = []
    for emoji in REACTION_EMOJIS:
        count = reactions.get(emoji, 0)
        label = f"{emoji} {count}" if count > 0 else emoji
        row.append(InlineKeyboardButton(
            text=label,
            callback_data=f"anon:react:{msg_id}:{emoji}"
        ))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text="🔄 Ответить анонимно", callback_data=f"anon:reply:{msg_id}"),
        InlineKeyboardButton(text="🚫 Заблокировать",     callback_data=f"anon:block:{msg_id}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_anon_mod(msg_id: int) -> InlineKeyboardMarkup:
    """Кнопка раскрытия автора для модераторов"""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🕵️ Раскрыть автора",
            callback_data=f"anon:reveal:{msg_id}"
        )
    ]])


async def cmd_anonmsg_v2(message: Message):
    """Улучшенная команда анонимного сообщения"""
    if not message.reply_to_message:
        await message.reply(
            "━━━━━━━━━━━━━━━\n"
            "💌 <b>АНОНИМНОЕ СООБЩЕНИЕ</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "Используй реплай на юзера:\n"
            "<code>/anonmsg текст сообщения</code>\n\n"
            "📬 Или создай анонимный ящик:\n"
            "<code>/anonbox вопрос</code>",
            parse_mode="HTML"
        )
        return

    text = (message.text or "").replace("/anonmsg", "").strip()
    if not text:
        await message.reply("⚠️ Напиши текст сообщения")
        return

    from_uid = message.from_user.id
    target   = message.reply_to_message.from_user
    to_uid   = target.id
    cid      = message.chat.id

    if from_uid == to_uid:
        await message.reply("❌ Нельзя отправить анонимку самому себе")
        return

    # Проверяем блокировку
    conn = db()
    blocked = conn.execute(
        "SELECT 1 FROM anon_blocks WHERE uid=? AND blocked_uid=?",
        (to_uid, from_uid)
    ).fetchone()
    if blocked:
        conn.close()
        await message.reply("❌ Этот пользователь заблокировал тебя")
        return

    # Получаем ник
    p = conn.execute(
        "SELECT anon_nick FROM user_profiles WHERE uid=?", (from_uid,)
    ).fetchone()
    nick = p["anon_nick"] if p and p["anon_nick"] else random.choice(ANON_NICKS)

    # Сохраняем
    cur = conn.execute(
        "INSERT INTO anon_messages_v2 (from_uid, to_uid, cid, text, anon_nick) VALUES (?,?,?,?,?)",
        (from_uid, to_uid, cid, text, nick)
    )
    msg_id = cur.lastrowid
    conn.commit()
    conn.close()

    # Отправляем получателю
    try:
        sent = await _bot.send_message(
            to_uid,
            f"━━━━━━━━━━━━━━━\n"
            f"💌 <b>АНОНИМНОЕ СООБЩЕНИЕ</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"👤 От: <b>{nick}</b>\n\n"
            f"📝 {text}",
            parse_mode="HTML",
            reply_markup=kb_anon_reactions(msg_id, {})
        )
        await message.reply("✅ Анонимное сообщение отправлено!")

        # Удаляем команду
        try:
            await message.delete()
        except:
            pass

    except Exception as e:
        await message.reply("❌ Не удалось отправить — юзер закрыл ЛС боту")


async def cb_anon(call: CallbackQuery):
    """Обработчик кнопок анонимок"""
    parts = call.data.split(":")
    action = parts[1]

    if action == "react":
        msg_id = int(parts[2])
        emoji  = parts[3]
        uid    = call.from_user.id

        conn = db()
        row = conn.execute(
            "SELECT reactions, to_uid, from_uid, text, anon_nick FROM anon_messages_v2 WHERE id=?",
            (msg_id,)
        ).fetchone()

        if not row:
            await call.answer("Сообщение не найдено", show_alert=True)
            conn.close()
            return

        reactions = json.loads(row["reactions"] or "{}")

        # Тогглим реакцию
        key = f"{uid}:{emoji}"
        user_reacts_key = f"_users_{emoji}"
        users = set(reactions.get(user_reacts_key, []))

        if uid in users:
            users.discard(uid)
            reactions[emoji] = max(0, reactions.get(emoji, 1) - 1)
        else:
            users.add(uid)
            reactions[emoji] = reactions.get(emoji, 0) + 1

        reactions[user_reacts_key] = list(users)
        conn.execute(
            "UPDATE anon_messages_v2 SET reactions=? WHERE id=?",
            (json.dumps(reactions), msg_id)
        )
        conn.commit()
        conn.close()

        # Обновляем кнопки
        try:
            await call.message.edit_reply_markup(
                reply_markup=kb_anon_reactions(msg_id, reactions)
            )
        except:
            pass

        # Уведомляем отправителя о реакции
        try:
            await _bot.send_message(
                row["from_uid"],
                f"💌 На твою анонимку поставили реакцию {emoji}!\n"
                f"📝 <i>{row['text'][:50]}...</i>",
                parse_mode="HTML"
            )
        except:
            pass

        await call.answer(f"Реакция {emoji} поставлена!")

    elif action == "reply":
        msg_id = int(parts[2])
        uid    = call.from_user.id

        conn = db()
        row = conn.execute(
            "SELECT from_uid, anon_nick, text FROM anon_messages_v2 WHERE id=?",
            (msg_id,)
        ).fetchone()
        conn.close()

        if not row:
            await call.answer("Сообщение не найдено", show_alert=True)
            return

        anon_reply_states[uid] = msg_id
        await call.message.answer(
            f"━━━━━━━━━━━━━━━\n"
            f"🔄 <b>АНОНИМНЫЙ ОТВЕТ</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"Отвечаешь на сообщение от <b>{row['anon_nick']}</b>:\n"
            f"<i>{row['text'][:100]}</i>\n\n"
            f"✏️ Напиши ответ следующим сообщением:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="anon:cancel_reply")
            ]])
        )
        await call.answer()

    elif action == "cancel_reply":
        uid = call.from_user.id
        anon_reply_states.pop(uid, None)
        await call.message.edit_text("❌ Ответ отменён")
        await call.answer()

    elif action == "block":
        msg_id   = int(parts[2])
        uid      = call.from_user.id

        conn = db()
        row = conn.execute(
            "SELECT from_uid FROM anon_messages_v2 WHERE id=?", (msg_id,)
        ).fetchone()
        if row:
            conn.execute(
                "INSERT OR IGNORE INTO anon_blocks (uid, blocked_uid) VALUES (?,?)",
                (uid, row["from_uid"])
            )
            conn.commit()
        conn.close()

        await call.answer("🚫 Отправитель заблокирован — больше не получишь от него анонимок", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except:
            pass

    elif action == "reveal":
        msg_id = int(parts[2])
        uid    = call.from_user.id

        # Только модераторы
        if uid not in _admin_ids and uid != _owner_id:
            await call.answer("🚫 Только для модераторов", show_alert=True)
            return

        conn = db()
        row = conn.execute(
            "SELECT from_uid FROM anon_messages_v2 WHERE id=?", (msg_id,)
        ).fetchone()
        conn.close()

        if row:
            try:
                member = await _bot.get_chat_member(call.message.chat.id, row["from_uid"])
                name = member.user.full_name
                await call.answer(
                    f"🕵️ Автор: {name} (ID: {row['from_uid']})",
                    show_alert=True
                )
            except:
                await call.answer(f"🕵️ ID автора: {row['from_uid']}", show_alert=True)
        else:
            await call.answer("Сообщение не найдено", show_alert=True)


async def handle_anon_reply_text(message: Message) -> bool:
    """Обрабатывает текстовый ответ на анонимку"""
    uid = message.from_user.id
    if uid not in anon_reply_states:
        return False

    msg_id = anon_reply_states.pop(uid)
    text   = (message.text or "").strip()

    conn = db()
    row = conn.execute(
        "SELECT from_uid, anon_nick FROM anon_messages_v2 WHERE id=?", (msg_id,)
    ).fetchone()
    conn.close()

    if not row:
        await message.reply("⚠️ Исходное сообщение не найдено")
        return True

    # Получаем ник отправителя ответа
    conn = db()
    p = conn.execute(
        "SELECT anon_nick FROM user_profiles WHERE uid=?", (uid,)
    ).fetchone()
    my_nick = p["anon_nick"] if p and p["anon_nick"] else random.choice(ANON_NICKS)

    # Сохраняем ответ
    conn.execute(
        "INSERT INTO anon_messages_v2 (from_uid, to_uid, text, anon_nick, reply_to_id) VALUES (?,?,?,?,?)",
        (uid, row["from_uid"], text, my_nick, msg_id)
    )
    conn.commit()
    conn.close()

    try:
        await _bot.send_message(
            row["from_uid"],
            f"━━━━━━━━━━━━━━━\n"
            f"🔄 <b>АНОНИМНЫЙ ОТВЕТ</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"👤 От: <b>{my_nick}</b>\n\n"
            f"📝 {text}",
            parse_mode="HTML",
            reply_markup=kb_anon_reactions(0, {})
        )
        await message.reply("✅ Анонимный ответ отправлен!")
    except:
        await message.reply("❌ Не удалось отправить ответ")

    return True


# ══════════════════════════════════════════════════════════════
#  АНОНИМНЫЙ ЯЩИК
# ══════════════════════════════════════════════════════════════

async def cmd_anonbox(message: Message):
    """/anonbox вопрос — создать анонимный ящик вопросов"""
    text = (message.text or "").replace("/anonbox", "").strip()
    if not text:
        await message.reply(
            "📬 <b>Анонимный ящик</b>\n\n"
            "Создай ящик вопросов:\n"
            "<code>/anonbox Что вы думаете обо мне?</code>\n\n"
            "Любой сможет ответить анонимно!",
            parse_mode="HTML"
        )
        return

    uid = message.from_user.id
    cid = message.chat.id

    sent = await message.answer(
        f"━━━━━━━━━━━━━━━\n"
        f"📬 <b>АНОНИМНЫЙ ЯЩИК</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"❓ <b>{text}</b>\n\n"
        f"👤 Автор: <a href='tg://user?id={uid}'>{message.from_user.first_name}</a>\n\n"
        f"💌 Нажми кнопку чтобы ответить анонимно:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="💌 Ответить анонимно",
                callback_data=f"anonbox:answer:0"  # обновим после сохранения
            )
        ]])
    )

    conn = db()
    conn.execute(
        "INSERT INTO anon_box (cid, owner_uid, question, msg_id) VALUES (?,?,?,?)",
        (cid, uid, text, sent.message_id)
    )
    box_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    # Обновляем кнопку с реальным box_id
    try:
        await sent.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="💌 Ответить анонимно",
                    callback_data=f"anonbox:answer:{box_id}"
                )
            ]])
        )
    except:
        pass

    try:
        await message.delete()
    except:
        pass


async def cb_anonbox(call: CallbackQuery):
    """Обработчик анонимного ящика"""
    parts  = call.data.split(":")
    action = parts[1]

    if action == "answer":
        box_id = int(parts[2])
        uid    = call.from_user.id

        conn = db()
        box = conn.execute(
            "SELECT * FROM anon_box WHERE id=?", (box_id,)
        ).fetchone()
        conn.close()

        if not box:
            await call.answer("Ящик не найден", show_alert=True)
            return

        anon_box_states[uid] = {
            "box_id":    box_id,
            "owner_uid": box["owner_uid"],
            "question":  box["question"],
            "cid":       box["cid"]
        }

        await call.message.answer(
            f"━━━━━━━━━━━━━━━\n"
            f"💌 <b>АНОНИМНЫЙ ОТВЕТ</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"❓ <b>{box['question']}</b>\n\n"
            f"✏️ Напиши ответ — он будет анонимным:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="anonbox:cancel")
            ]])
        )
        await call.answer()

    elif action == "cancel":
        uid = call.from_user.id
        anon_box_states.pop(uid, None)
        await call.message.edit_text("❌ Отменено")
        await call.answer()


async def handle_anonbox_reply(message: Message) -> bool:
    """Обрабатывает ответ в анонимный ящик"""
    uid = message.from_user.id
    if uid not in anon_box_states:
        return False

    state    = anon_box_states.pop(uid)
    text     = (message.text or "").strip()
    owner_uid = state["owner_uid"]

    if uid == owner_uid:
        await message.reply("❌ Нельзя отвечать в свой ящик")
        return True

    conn = db()
    p = conn.execute(
        "SELECT anon_nick FROM user_profiles WHERE uid=?", (uid,)
    ).fetchone()
    nick = p["anon_nick"] if p and p["anon_nick"] else random.choice(ANON_NICKS)
    conn.close()

    try:
        await _bot.send_message(
            owner_uid,
            f"━━━━━━━━━━━━━━━\n"
            f"📬 <b>ОТВЕТ В ЯЩИК</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"❓ <i>{state['question']}</i>\n\n"
            f"👤 От: <b>{nick}</b>\n"
            f"💬 {text}",
            parse_mode="HTML"
        )
        await message.reply("✅ Анонимный ответ отправлен!")
    except:
        await message.reply("❌ Не удалось отправить")

    return True


# ══════════════════════════════════════════════════════════════
#  УЛУЧШЕННЫЕ ОПРОСЫ v2
# ══════════════════════════════════════════════════════════════

async def cmd_poll_v2(message: Message):
    """/poll2 вопрос | вариант1 | вариант2 [| анон] [| 5м]"""
    args = (message.text or "").replace("/poll2", "").strip()
    if not args or "|" not in args:
        await message.reply(
            "📊 <b>Опрос v2</b>\n\n"
            "Формат:\n"
            "<code>/poll2 Вопрос | Вариант 1 | Вариант 2 | анон | 5м</code>\n\n"
            "• <code>анон</code> — анонимный опрос\n"
            "• <code>5м</code> — таймер 5 минут (1м-60м)\n\n"
            "Пример:\n"
            "<code>/poll2 Кто лучший? | Саша | Петя | анон | 10м</code>",
            parse_mode="HTML"
        )
        return

    parts     = [p.strip() for p in args.split("|")]
    question  = parts[0]
    options   = []
    anonymous = False
    duration  = 0  # секунды

    for p in parts[1:]:
        pl = p.lower()
        if pl == "анон":
            anonymous = True
        elif pl.endswith("м") and pl[:-1].isdigit():
            duration = int(pl[:-1]) * 60
        elif pl.endswith("ч") and pl[:-1].isdigit():
            duration = int(pl[:-1]) * 3600
        else:
            options.append(p)

    if len(options) < 2:
        await message.reply("⚠️ Нужно минимум 2 варианта ответа")
        return

    options = options[:8]  # максимум 8
    cid     = message.chat.id
    uid     = message.from_user.id
    ends_at = int(time.time()) + duration if duration else 0

    # Создаём опрос в БД
    conn = db()
    cur = conn.execute(
        "INSERT INTO polls_v2 (cid, creator_uid, question, options, anonymous, ends_at) "
        "VALUES (?,?,?,?,?,?)",
        (cid, uid, question, json.dumps(options), 1 if anonymous else 0, ends_at)
    )
    poll_id = cur.lastrowid
    conn.commit()
    conn.close()

    # Формируем сообщение
    anon_label = "🔒 Анонимный" if anonymous else "👁 Открытый"
    time_label = f"⏰ {duration//60} мин" if duration else "∞ Бессрочный"

    text = _format_poll(poll_id, question, options, {}, anonymous, ends_at)
    kb   = _poll_keyboard(poll_id, options)

    sent = await message.answer(text, parse_mode="HTML", reply_markup=kb)

    # Обновляем msg_id
    conn = db()
    conn.execute("UPDATE polls_v2 SET msg_id=? WHERE id=?", (sent.message_id, poll_id))
    conn.commit()
    conn.close()

    # Запускаем таймер если есть
    if duration:
        asyncio.create_task(_poll_timer(poll_id, duration, cid, sent.message_id))


def _format_poll(poll_id: int, question: str, options: list,
                 votes: dict, anonymous: bool, ends_at: int) -> str:
    total = sum(len(v) for v in votes.values()) if votes else 0
    anon  = "🔒" if anonymous else "👁"
    time_str = ""
    if ends_at:
        remaining = ends_at - int(time.time())
        if remaining > 0:
            m, s = divmod(remaining, 60)
            h, m = divmod(m, 60)
            if h:
                time_str = f"\n⏰ Осталось: {h}ч {m}м"
            else:
                time_str = f"\n⏰ Осталось: {m}м {s}с"
        else:
            time_str = "\n🔴 Опрос завершён"

    text = (
        f"━━━━━━━━━━━━━━━\n"
        f"📊 <b>ОПРОС</b> {anon}\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"❓ <b>{question}</b>{time_str}\n\n"
    )

    for i, opt in enumerate(options):
        opt_votes = len(votes.get(str(i), []))
        pct = int(opt_votes / total * 100) if total else 0
        bar_len = int(pct / 10)
        bar = "▓" * bar_len + "░" * (10 - bar_len)
        text += f"{i+1}. {opt}\n   [{bar}] {pct}% ({opt_votes})\n"

    text += f"\n👥 Всего голосов: <b>{total}</b>"
    return text


def _poll_keyboard(poll_id: int, options: list) -> InlineKeyboardMarkup:
    rows = []
    row  = []
    for i, opt in enumerate(options):
        row.append(InlineKeyboardButton(
            text=f"{i+1}. {opt[:20]}",
            callback_data=f"poll2:vote:{poll_id}:{i}"
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton(text="📊 Результаты", callback_data=f"poll2:results:{poll_id}"),
        InlineKeyboardButton(text="🔴 Завершить",  callback_data=f"poll2:close:{poll_id}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def cb_poll_v2(call: CallbackQuery):
    """Обработчик опросов v2"""
    parts  = call.data.split(":")
    action = parts[1]

    if action == "vote":
        poll_id    = int(parts[2])
        option_idx = int(parts[3])
        uid        = call.from_user.id

        conn = db()
        poll = conn.execute("SELECT * FROM polls_v2 WHERE id=?", (poll_id,)).fetchone()
        if not poll:
            await call.answer("Опрос не найден", show_alert=True)
            conn.close()
            return

        if poll["closed"]:
            await call.answer("Опрос уже завершён", show_alert=True)
            conn.close()
            return

        if poll["ends_at"] and int(time.time()) > poll["ends_at"]:
            await call.answer("Время опроса истекло", show_alert=True)
            conn.close()
            return

        votes   = json.loads(poll["votes"] or "{}")
        options = json.loads(poll["options"])
        key     = str(option_idx)

        # Убираем предыдущий голос
        for k in votes:
            if uid in votes[k]:
                votes[k].remove(uid)

        # Добавляем голос
        if key not in votes:
            votes[key] = []
        votes[key].append(uid)

        conn.execute("UPDATE polls_v2 SET votes=? WHERE id=?", (json.dumps(votes), poll_id))
        conn.commit()
        conn.close()

        # Обновляем сообщение
        text = _format_poll(
            poll_id, poll["question"], options, votes,
            bool(poll["anonymous"]), poll["ends_at"]
        )
        try:
            await call.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=_poll_keyboard(poll_id, options)
            )
        except:
            pass

        await call.answer(f"✅ Голос за: {options[option_idx]}")

    elif action == "results":
        poll_id = int(parts[2])
        conn    = db()
        poll    = conn.execute("SELECT * FROM polls_v2 WHERE id=?", (poll_id,)).fetchone()
        conn.close()

        if not poll:
            await call.answer("Опрос не найден", show_alert=True)
            return

        votes   = json.loads(poll["votes"] or "{}")
        options = json.loads(poll["options"])
        total   = sum(len(v) for v in votes.values())

        result_text = f"📊 <b>Результаты опроса</b>\n\n❓ {poll['question']}\n\n"
        for i, opt in enumerate(options):
            opt_votes = len(votes.get(str(i), []))
            pct = int(opt_votes / total * 100) if total else 0
            result_text += f"• {opt}: <b>{opt_votes}</b> ({pct}%)\n"

        result_text += f"\n👥 Всего: {total}"
        await call.answer(result_text[:200], show_alert=True)

    elif action == "close":
        poll_id = int(parts[2])
        uid     = call.from_user.id

        conn = db()
        poll = conn.execute("SELECT * FROM polls_v2 WHERE id=?", (poll_id,)).fetchone()

        if not poll:
            await call.answer("Опрос не найден", show_alert=True)
            conn.close()
            return

        if poll["creator_uid"] != uid and uid not in _admin_ids:
            await call.answer("Только создатель может завершить опрос", show_alert=True)
            conn.close()
            return

        conn.execute("UPDATE polls_v2 SET closed=1 WHERE id=?", (poll_id,))
        conn.commit()

        votes   = json.loads(poll["votes"] or "{}")
        options = json.loads(poll["options"])
        conn.close()

        text = _format_poll(
            poll_id, poll["question"], options, votes,
            bool(poll["anonymous"]), 0
        ) + "\n\n🔴 <b>ОПРОС ЗАВЕРШЁН</b>"

        try:
            await call.message.edit_text(text, parse_mode="HTML")
        except:
            pass
        await call.answer("✅ Опрос завершён")


async def _poll_timer(poll_id: int, duration: int, cid: int, msg_id: int):
    """Автоматически завершает опрос по таймеру"""
    await asyncio.sleep(duration)

    conn = db()
    poll = conn.execute("SELECT * FROM polls_v2 WHERE id=?", (poll_id,)).fetchone()
    if poll and not poll["closed"]:
        conn.execute("UPDATE polls_v2 SET closed=1 WHERE id=?", (poll_id,))
        conn.commit()

        votes   = json.loads(poll["votes"] or "{}")
        options = json.loads(poll["options"])
        conn.close()

        text = _format_poll(
            poll_id, poll["question"], options, votes,
            bool(poll["anonymous"]), 0
        ) + "\n\n🔴 <b>ОПРОС ЗАВЕРШЁН (время вышло)</b>"

        try:
            await _bot.edit_message_text(
                text, chat_id=cid, message_id=msg_id, parse_mode="HTML"
            )
        except:
            pass
    else:
        if conn:
            conn.close()


# ══════════════════════════════════════════════════════════════
#  МАГАЗИН v2 С КАТЕГОРИЯМИ
# ══════════════════════════════════════════════════════════════

SHOP_CATEGORIES = {
    "titles":   "👑 Титулы",
    "boosters": "⚡ Бустеры",
    "effects":  "✨ Эффекты",
    "misc":     "🎲 Разное",
}


async def cmd_shop_v2(message: Message):
    """Магазин v2 с категориями"""
    await message.answer(
        "━━━━━━━━━━━━━━━\n"
        "🏪 <b>МАГАЗИН</b>\n"
        "━━━━━━━━━━━━━━━\n\n"
        "Выбери категорию:",
        parse_mode="HTML",
        reply_markup=_shop_category_kb()
    )


def _shop_category_kb() -> InlineKeyboardMarkup:
    rows = []
    for key, label in SHOP_CATEGORIES.items():
        rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"shop2:cat:{key}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _shop_items_kb(category: str, items: list) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        rows.append([InlineKeyboardButton(
            text=f"{item['emoji']} {item['name']} — {item['price']} 💰",
            callback_data=f"shop2:buy:{item['id']}"
        )])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="shop2:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def cb_shop_v2(call: CallbackQuery):
    """Обработчик магазина v2"""
    parts  = call.data.split(":")
    action = parts[1]

    if action == "cat":
        category = parts[2]
        conn = db()
        items = conn.execute(
            "SELECT * FROM shop_items WHERE category=? ORDER BY price",
            (category,)
        ).fetchall()
        conn.close()

        label = SHOP_CATEGORIES.get(category, category)
        text  = f"━━━━━━━━━━━━━━━\n{label}\n━━━━━━━━━━━━━━━\n\n"

        for item in [dict(i) for i in items]:
            text += f"{item['emoji']} <b>{item['name']}</b> — <b>{item['price']}</b> 💰\n"
            text += f"   <i>{item['description']}</i>\n\n"

        await call.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=_shop_items_kb(category, [dict(i) for i in items])
        )

    elif action == "buy":
        item_id = int(parts[2])
        uid     = call.from_user.id
        cid     = call.message.chat.id

        conn = db()
        item = conn.execute(
            "SELECT * FROM shop_items WHERE id=?", (item_id,)
        ).fetchone()

        if not item:
            await call.answer("Товар не найден", show_alert=True)
            conn.close()
            return

        # Проверяем репутацию
        rep_row = conn.execute(
            "SELECT score FROM reputation WHERE cid=? AND uid=?", (cid, uid)
        ).fetchone()
        rep = rep_row["score"] if rep_row else 0

        if rep < item["price"]:
            await call.answer(
                f"❌ Недостаточно репутации!\nНужно: {item['price']} 💰\nЕсть: {rep} 💰",
                show_alert=True
            )
            conn.close()
            return

        # Списываем репутацию
        conn.execute(
            "UPDATE reputation SET score=score-? WHERE cid=? AND uid=?",
            (item["price"], cid, uid)
        )
        conn.commit()
        conn.close()

        await call.answer(
            f"✅ Куплено: {item['emoji']} {item['name']}!\n"
            f"Списано {item['price']} 💰",
            show_alert=True
        )

        # Применяем эффект
        await _apply_shop_effect(uid, cid, item["effect"], item["name"])

    elif action == "back":
        await call.message.edit_text(
            "━━━━━━━━━━━━━━━\n"
            "🏪 <b>МАГАЗИН</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "Выбери категорию:",
            parse_mode="HTML",
            reply_markup=_shop_category_kb()
        )

    await call.answer()


async def _apply_shop_effect(uid: int, cid: int, effect: str, item_name: str):
    """Применяет эффект купленного товара"""
    if effect.startswith("title:"):
        title = item_name
        conn = db()
        conn.execute(
            "INSERT INTO user_titles (uid, active, purchased) VALUES (?,?,?) "
            "ON CONFLICT(uid) DO UPDATE SET active=?, purchased=json_insert(purchased,'$[#]',?)",
            (uid, title, json.dumps([title]), title, title)
        )
        conn.commit()
        conn.close()

    elif effect.startswith("boost:"):
        parts = effect.split(":")
        boost_type = parts[1]
        duration   = int(parts[2]) if len(parts) > 2 else 3600
        expires    = int(time.time()) + duration
        conn = db()
        row = conn.execute("SELECT data FROM boosters WHERE uid=?", (uid,)).fetchone()
        data = json.loads(row["data"]) if row else {}
        data[boost_type] = expires
        conn.execute(
            "INSERT INTO boosters (uid,data) VALUES (?,?) ON CONFLICT(uid) DO UPDATE SET data=?",
            (uid, json.dumps(data), json.dumps(data))
        )
        conn.commit()
        conn.close()

    elif effect.startswith("lottery:"):
        count = int(effect.split(":")[1])
        conn = db()
        for _ in range(count):
            conn.execute(
                "INSERT OR IGNORE INTO lottery_tickets (cid,uid) VALUES (?,?)", (cid, uid)
            )
        conn.commit()
        conn.close()


# ══════════════════════════════════════════════════════════════
#  АУКЦИОН
# ══════════════════════════════════════════════════════════════

async def cmd_auction(message: Message):
    """/auction предмет | начальная_цена | время_минуты"""
    args = (message.text or "").replace("/auction", "").strip()
    if not args or "|" not in args:
        await message.reply(
            "━━━━━━━━━━━━━━━\n"
            "🔨 <b>АУКЦИОН</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "Формат:\n"
            "<code>/auction Предмет | цена | 30</code>\n\n"
            "Пример:\n"
            "<code>/auction VIP титул | 100 | 60</code>",
            parse_mode="HTML"
        )
        return

    parts = [p.strip() for p in args.split("|")]
    if len(parts) < 2:
        await message.reply("⚠️ Нужно указать предмет и цену")
        return

    item_name   = parts[0]
    start_price = int(parts[1]) if parts[1].isdigit() else 50
    duration    = int(parts[2]) * 60 if len(parts) > 2 and parts[2].isdigit() else 1800

    uid  = message.from_user.id
    cid  = message.chat.id
    name = message.from_user.full_name
    ends_at = int(time.time()) + duration

    conn = db()
    cur = conn.execute(
        "INSERT INTO auction_items (cid, seller_uid, seller_name, item_name, "
        "start_price, current_bid, ends_at) VALUES (?,?,?,?,?,?,?)",
        (cid, uid, name, item_name, start_price, start_price, ends_at)
    )
    auction_id = cur.lastrowid
    conn.commit()
    conn.close()

    m, s = divmod(duration, 60)
    h, m = divmod(m, 60)
    time_str = f"{h}ч {m}м" if h else f"{m}м"

    sent = await message.answer(
        f"━━━━━━━━━━━━━━━\n"
        f"🔨 <b>АУКЦИОН #{auction_id}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"📦 Лот: <b>{item_name}</b>\n"
        f"👤 Продавец: <b>{name}</b>\n"
        f"💰 Старт: <b>{start_price}</b> 💰\n"
        f"🏆 Текущая ставка: <b>{start_price}</b> 💰\n"
        f"⏰ Время: <b>{time_str}</b>\n\n"
        f"Нажми кнопку чтобы сделать ставку!",
        parse_mode="HTML",
        reply_markup=_auction_kb(auction_id, start_price)
    )

    conn = db()
    conn.execute("UPDATE auction_items SET msg_id=? WHERE id=?", (sent.message_id, auction_id))
    conn.commit()
    conn.close()

    asyncio.create_task(_auction_timer(auction_id, duration, cid, sent.message_id))


def _auction_kb(auction_id: int, current_bid: int) -> InlineKeyboardMarkup:
    bid_10  = current_bid + 10
    bid_50  = current_bid + 50
    bid_100 = current_bid + 100
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"+10 💰 ({bid_10})",  callback_data=f"auction:bid:{auction_id}:{bid_10}"),
            InlineKeyboardButton(text=f"+50 💰 ({bid_50})",  callback_data=f"auction:bid:{auction_id}:{bid_50}"),
        ],
        [
            InlineKeyboardButton(text=f"+100 💰 ({bid_100})", callback_data=f"auction:bid:{auction_id}:{bid_100}"),
            InlineKeyboardButton(text="💰 Своя ставка",       callback_data=f"auction:custom:{auction_id}"),
        ],
    ])


async def cb_auction(call: CallbackQuery):
    """Обработчик аукциона"""
    parts  = call.data.split(":")
    action = parts[1]

    if action == "bid":
        auction_id = int(parts[2])
        bid_amount = int(parts[3])
        uid        = call.from_user.id
        name       = call.from_user.full_name

        conn = db()
        auc = conn.execute(
            "SELECT * FROM auction_items WHERE id=?", (auction_id,)
        ).fetchone()

        if not auc:
            await call.answer("Аукцион не найден", show_alert=True)
            conn.close()
            return

        if auc["closed"]:
            await call.answer("Аукцион завершён", show_alert=True)
            conn.close()
            return

        if int(time.time()) > auc["ends_at"]:
            await call.answer("Время аукциона истекло", show_alert=True)
            conn.close()
            return

        if uid == auc["seller_uid"]:
            await call.answer("Нельзя ставить на свой аукцион", show_alert=True)
            conn.close()
            return

        # Проверяем репу
        rep_row = conn.execute(
            "SELECT score FROM reputation WHERE cid=? AND uid=?",
            (auc["cid"], uid)
        ).fetchone()
        rep = rep_row["score"] if rep_row else 0

        if rep < bid_amount:
            await call.answer(
                f"❌ Недостаточно репы!\nНужно: {bid_amount} 💰\nЕсть: {rep} 💰",
                show_alert=True
            )
            conn.close()
            return

        if bid_amount <= auc["current_bid"]:
            await call.answer(
                f"❌ Ставка должна быть больше текущей ({auc['current_bid']} 💰)",
                show_alert=True
            )
            conn.close()
            return

        conn.execute(
            "UPDATE auction_items SET current_bid=?, bidder_uid=?, bidder_name=? WHERE id=?",
            (bid_amount, uid, name, auction_id)
        )
        conn.commit()

        remaining = auc["ends_at"] - int(time.time())
        m2, s2 = divmod(remaining, 60)
        time_str = f"{m2}м {s2}с"

        try:
            await call.message.edit_text(
                f"━━━━━━━━━━━━━━━\n"
                f"🔨 <b>АУКЦИОН #{auction_id}</b>\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"📦 Лот: <b>{auc['item_name']}</b>\n"
                f"👤 Продавец: <b>{auc['seller_name']}</b>\n"
                f"💰 Старт: <b>{auc['start_price']}</b> 💰\n"
                f"🏆 Текущая ставка: <b>{bid_amount}</b> 💰\n"
                f"👤 Лидер: <b>{name}</b>\n"
                f"⏰ Осталось: <b>{time_str}</b>",
                parse_mode="HTML",
                reply_markup=_auction_kb(auction_id, bid_amount)
            )
        except:
            pass

        conn.close()
        await call.answer(f"✅ Ставка {bid_amount} 💰 принята!")

    elif action == "custom":
        auction_id = int(parts[2])
        uid        = call.from_user.id
        auction_states[uid] = auction_id
        await call.message.answer(
            "💰 Введи свою ставку (число):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"auction:cancel:{auction_id}")
            ]])
        )
        await call.answer()

    elif action == "cancel":
        uid = call.from_user.id
        auction_states.pop(uid, None)
        await call.message.edit_text("❌ Отменено")
        await call.answer()


async def _auction_timer(auction_id: int, duration: int, cid: int, msg_id: int):
    """Завершает аукцион по таймеру"""
    await asyncio.sleep(duration)

    conn = db()
    auc = conn.execute(
        "SELECT * FROM auction_items WHERE id=?", (auction_id,)
    ).fetchone()

    if auc and not auc["closed"]:
        conn.execute("UPDATE auction_items SET closed=1 WHERE id=?", (auction_id,))
        conn.commit()
        conn.close()

        if auc["bidder_uid"]:
            result = (
                f"━━━━━━━━━━━━━━━\n"
                f"🔨 <b>АУКЦИОН ЗАВЕРШЁН #{auction_id}</b>\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"📦 Лот: <b>{auc['item_name']}</b>\n"
                f"🏆 Победитель: <b>{auc['bidder_name']}</b>\n"
                f"💰 Финальная цена: <b>{auc['current_bid']}</b> 💰\n\n"
                f"🎉 Поздравляем победителя!"
            )
            # Списываем репу у победителя
            try:
                conn2 = db()
                conn2.execute(
                    "UPDATE reputation SET score=score-? WHERE cid=? AND uid=?",
                    (auc["current_bid"], cid, auc["bidder_uid"])
                )
                # Добавляем продавцу
                conn2.execute(
                    "INSERT INTO reputation (cid,uid,score) VALUES (?,?,?) "
                    "ON CONFLICT(cid,uid) DO UPDATE SET score=score+?",
                    (cid, auc["seller_uid"], auc["current_bid"], auc["current_bid"])
                )
                conn2.commit()
                conn2.close()
            except:
                pass
        else:
            result = (
                f"━━━━━━━━━━━━━━━\n"
                f"🔨 <b>АУКЦИОН ЗАВЕРШЁН #{auction_id}</b>\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"📦 Лот: <b>{auc['item_name']}</b>\n"
                f"😔 Ставок не было. Лот не продан."
            )

        try:
            await _bot.edit_message_text(
                result, chat_id=cid, message_id=msg_id, parse_mode="HTML"
            )
        except:
            try:
                await _bot.send_message(cid, result, parse_mode="HTML")
            except:
                pass
    else:
        if conn:
            conn.close()


# ══════════════════════════════════════════════════════════════
#  CALLBACK ДЛЯ ПРОФИЛЯ
# ══════════════════════════════════════════════════════════════

async def cb_profile(call: CallbackQuery):
    """Обработчик кнопок профиля"""
    parts  = call.data.split(":")
    action = parts[1]

    if action == "achievements":
        uid  = int(parts[2])
        conn = db()
        rows = conn.execute(
            "SELECT key, unlocked_at FROM achievements WHERE uid=? ORDER BY unlocked_at DESC",
            (uid,)
        ).fetchall()
        conn.close()

        if not rows:
            await call.answer("Достижений пока нет", show_alert=True)
            return

        text = "━━━━━━━━━━━━━━━\n🏆 <b>ДОСТИЖЕНИЯ</b>\n━━━━━━━━━━━━━━━\n\n"
        for r in rows:
            key = r["key"]
            if key in ACHIEVEMENTS:
                emoji, name, desc = ACHIEVEMENTS[key]
                dt = str(r["unlocked_at"])[:10] if r["unlocked_at"] else ""
                text += f"{emoji} <b>{name}</b>\n   <i>{desc}</i> · {dt}\n\n"

        await call.message.answer(text, parse_mode="HTML")
        await call.answer()

    elif action == "stats":
        uid = int(parts[2])
        cid = int(parts[3])
        conn = db()

        # Детальная статистика
        hourly = conn.execute(
            "SELECT hour, count FROM hourly_stats WHERE cid=? AND uid=? ORDER BY hour",
            (cid, uid)
        ).fetchall()
        activity = conn.execute(
            "SELECT day, count FROM user_activity WHERE cid=? AND uid=? "
            "ORDER BY day DESC LIMIT 7",
            (cid, uid)
        ).fetchall()
        conn.close()

        peak_hour = max(hourly, key=lambda x: x["count"])["hour"] if hourly else "—"
        total_days = len(activity)

        text = (
            f"━━━━━━━━━━━━━━━\n"
            f"📊 <b>ДЕТАЛЬНАЯ СТАТИСТИКА</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"⏰ Пик активности: <b>{peak_hour}:00</b>\n"
            f"📅 Активных дней (7д): <b>{total_days}</b>\n\n"
            f"<b>Активность по дням:</b>\n"
        )
        for r in reversed(list(activity)):
            text += f"• {r['day']}: {r['count']} сообщ.\n"

        await call.message.answer(text, parse_mode="HTML")
        await call.answer()


# ══════════════════════════════════════════════════════════════
#  РЕГИСТРАЦИЯ ХЕНДЛЕРОВ
# ══════════════════════════════════════════════════════════════

def _register_handlers(dp: Dispatcher):
    _init_tables()

    # Профиль карточка
    dp.message.register(
        cmd_profile_card,
        Command("card"),
    )

    # Анонимки v2
    dp.message.register(
        cmd_anonmsg_v2,
        Command("anonmsg"),
        F.chat.type.in_({"group", "supergroup"})
    )
    dp.message.register(
        cmd_anonbox,
        Command("anonbox"),
        F.chat.type.in_({"group", "supergroup"})
    )
    dp.callback_query.register(cb_anon,   F.data.startswith("anon:"))
    dp.callback_query.register(cb_anonbox, F.data.startswith("anonbox:"))

    # Опросы v2
    dp.message.register(
        cmd_poll_v2,
        Command("poll2"),
        F.chat.type.in_({"group", "supergroup"})
    )
    dp.callback_query.register(cb_poll_v2, F.data.startswith("poll2:"))

    # Магазин v2
    dp.message.register(
        cmd_shop_v2,
        Command("shop2"),
        F.chat.type.in_({"group", "supergroup"})
    )
    dp.callback_query.register(cb_shop_v2, F.data.startswith("shop2:"))

    # Аукцион
    dp.message.register(
        cmd_auction,
        Command("auction"),
        F.chat.type.in_({"group", "supergroup"})
    )
    dp.callback_query.register(cb_auction, F.data.startswith("auction:"))

    # Профиль callbacks
    dp.callback_query.register(cb_profile, F.data.startswith("profile:"))

    log.info("✅ Все хендлеры features.py зарегистрированы")

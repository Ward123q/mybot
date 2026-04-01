"""
notifications.py — Система умных уведомлений
Подключение в bot.py:
    import notifications as notif
    # В main(): await notif.init(bot, dp)
    # В StatsMiddleware: await notif.track_message(event)
"""
import asyncio
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
import theme

log = logging.getLogger(__name__)

_bot: Optional[Bot] = None

# ── Кэш событий (в памяти, сбрасывается при рестарте) ─────────
# {uid: {cid: {"mentions": int, "rep_delta": int, "msgs": int}}}
_user_events: Dict[int, Dict[int, dict]] = {}

# Время последней активности {uid: timestamp}
_last_active: Dict[int, float] = {}

# Время последнего дайджеста {uid: timestamp}
_last_digest: Dict[int, float] = {}

# Настройки уведомлений {uid: {setting: bool}}
_notif_settings: Dict[int, dict] = {}

DEFAULT_SETTINGS = {
    "digest":    True,   # Дайджест пока не было
    "mentions":  True,   # Упоминания
    "rep":       True,   # Изменения репы
    "levelup":   True,   # Повышение уровня
    "birthday":  True,   # Дни рождения
    "tickets":   True,   # Новые тикеты (для модов)
    "digest_min_hours": 2,   # Минимум часов отсутствия для дайджеста
}

DIGEST_INTERVALS = [2, 4, 8, 24]  # часы


def db():
    import sqlite3
    conn = sqlite3.connect("skinvault.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_tables():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS notif_settings (
        uid     INTEGER PRIMARY KEY,
        data    TEXT DEFAULT '{}'
    );
    CREATE TABLE IF NOT EXISTS notif_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER,
        type        TEXT,
        cid         INTEGER,
        data        TEXT,
        sent_at     TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS mention_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        cid         INTEGER,
        from_uid    INTEGER,
        from_name   TEXT,
        to_uid      INTEGER,
        text        TEXT,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_mention_to  ON mention_log(to_uid);
    CREATE INDEX IF NOT EXISTS idx_notif_uid   ON notif_log(uid);
    """)
    conn.commit()
    conn.close()


async def init(bot: Bot, dp: Dispatcher):
    global _bot
    _bot = bot
    _init_tables()
    _load_settings()
    _register_handlers(dp)
    asyncio.create_task(_digest_loop())
    log.info("✅ notifications.py инициализирован")


def _load_settings():
    """Загружает настройки из БД"""
    conn = db()
    rows = conn.execute("SELECT uid, data FROM notif_settings").fetchall()
    conn.close()
    for r in rows:
        try:
            _notif_settings[r["uid"]] = json.loads(r["data"])
        except:
            pass


def get_settings(uid: int) -> dict:
    """Получает настройки пользователя"""
    s = dict(DEFAULT_SETTINGS)
    s.update(_notif_settings.get(uid, {}))
    return s


def save_settings(uid: int, settings: dict):
    """Сохраняет настройки"""
    _notif_settings[uid] = settings
    conn = db()
    conn.execute(
        "INSERT INTO notif_settings (uid, data) VALUES (?,?) "
        "ON CONFLICT(uid) DO UPDATE SET data=?",
        (uid, json.dumps(settings), json.dumps(settings))
    )
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════
#  ТРЕКИНГ СОБЫТИЙ
# ══════════════════════════════════════════════════════════════

async def track_message(message: Message):
    """Вызывается из StatsMiddleware для каждого сообщения"""
    if not message.from_user or message.chat.type == "private":
        return

    uid  = message.from_user.id
    cid  = message.chat.id
    text = message.text or ""

    # Обновляем время последней активности
    _last_active[uid] = time.time()

    # Инициализируем хранилище
    if uid not in _user_events:
        _user_events[uid] = {}
    if cid not in _user_events[uid]:
        _user_events[uid][cid] = {"mentions": 0, "rep_delta": 0, "msgs_while_away": 0}

    # Проверяем упоминания других пользователей
    if message.entities:
        for entity in message.entities:
            if entity.type == "mention":
                mentioned = text[entity.offset:entity.offset + entity.length]
                await _track_mention(cid, uid, message.from_user.full_name, mentioned, text)
            elif entity.type == "text_mention" and entity.user:
                await _track_mention_by_id(
                    cid, uid, message.from_user.full_name,
                    entity.user.id, text
                )

    # Считаем сообщения пока юзеры отсутствовали
    conn = db()
    members = conn.execute(
        "SELECT DISTINCT uid FROM chat_stats WHERE cid=? AND uid!=?", (cid, uid)
    ).fetchall()
    conn.close()

    for m in members:
        muid = m["uid"]
        if muid not in _last_active:
            continue
        away_time = time.time() - _last_active.get(muid, time.time())
        if away_time > 3600:  # отсутствует больше часа
            if muid not in _user_events:
                _user_events[muid] = {}
            if cid not in _user_events[muid]:
                _user_events[muid][cid] = {"mentions": 0, "rep_delta": 0, "msgs_while_away": 0}
            _user_events[muid][cid]["msgs_while_away"] = \
                _user_events[muid][cid].get("msgs_while_away", 0) + 1


async def _track_mention(cid: int, from_uid: int, from_name: str,
                          username: str, text: str):
    """Трекает упоминание по @username"""
    conn = db()
    # Ищем uid по username (упрощённо — через chat_stats)
    conn.execute(
        "INSERT INTO mention_log (cid, from_uid, from_name, to_uid, text) "
        "VALUES (?,?,?,0,?)",
        (cid, from_uid, from_name, text[:200])
    )
    conn.commit()
    conn.close()


async def _track_mention_by_id(cid: int, from_uid: int, from_name: str,
                                to_uid: int, text: str):
    """Трекает упоминание по ID"""
    conn = db()
    conn.execute(
        "INSERT INTO mention_log (cid, from_uid, from_name, to_uid, text) "
        "VALUES (?,?,?,?,?)",
        (cid, from_uid, from_name, to_uid, text[:200])
    )
    conn.commit()
    conn.close()

    # Добавляем в события
    if to_uid not in _user_events:
        _user_events[to_uid] = {}
    if cid not in _user_events[to_uid]:
        _user_events[to_uid][cid] = {"mentions": 0, "rep_delta": 0, "msgs_while_away": 0}
    _user_events[to_uid][cid]["mentions"] = \
        _user_events[to_uid][cid].get("mentions", 0) + 1


async def track_rep_change(uid: int, cid: int, delta: int):
    """Трекает изменение репутации"""
    if uid not in _user_events:
        _user_events[uid] = {}
    if cid not in _user_events[uid]:
        _user_events[uid][cid] = {"mentions": 0, "rep_delta": 0, "msgs_while_away": 0}
    _user_events[uid][cid]["rep_delta"] = \
        _user_events[uid][cid].get("rep_delta", 0) + delta


async def track_level_up(uid: int, cid: int, new_level: int, chat_title: str):
    """Уведомляет о повышении уровня в ЛС"""
    settings = get_settings(uid)
    if not settings.get("levelup", True):
        return

    try:
        await _bot.send_message(
            uid,
            f"{theme.SEP_THICK}\n"
            f"⬆️ <b>НОВЫЙ УРОВЕНЬ!</b>\n"
            f"{theme.SEP_THICK}\n\n"
            f"💬 Чат: <b>{chat_title}</b>\n"
            f"🏅 Уровень: <b>{new_level}</b>\n\n"
            f"Продолжай в том же духе!",
            parse_mode="HTML"
        )
    except:
        pass


# ══════════════════════════════════════════════════════════════
#  ДАЙДЖЕСТ
# ══════════════════════════════════════════════════════════════

async def _digest_loop():
    """Фоновая задача — каждые 30 минут проверяет кому отправить дайджест"""
    while True:
        try:
            await _check_and_send_digests()
        except Exception as e:
            log.error(f"Ошибка дайджеста: {e}")
        await asyncio.sleep(1800)  # каждые 30 минут


async def _check_and_send_digests():
    """Проверяет и отправляет дайджесты пользователям"""
    now = time.time()

    for uid, chats in list(_user_events.items()):
        # Пропускаем активных пользователей
        last = _last_active.get(uid, 0)
        away_hours = (now - last) / 3600 if last else 0

        settings = get_settings(uid)
        min_hours = settings.get("digest_min_hours", 2)

        if away_hours < min_hours:
            continue

        if not settings.get("digest", True):
            continue

        # Не отправляем чаще чем раз в min_hours
        last_digest = _last_digest.get(uid, 0)
        if (now - last_digest) < min_hours * 3600:
            continue

        # Собираем статистику
        total_mentions    = 0
        total_rep_delta   = 0
        total_msgs        = 0
        chat_summaries    = []

        conn = db()
        for cid, events in chats.items():
            mentions  = events.get("mentions", 0)
            rep_delta = events.get("rep_delta", 0)
            msgs      = events.get("msgs_while_away", 0)

            if mentions == 0 and rep_delta == 0 and msgs == 0:
                continue

            total_mentions  += mentions
            total_rep_delta += rep_delta
            total_msgs      += msgs

            # Получаем название чата
            chat_row = conn.execute(
                "SELECT title FROM known_chats WHERE cid=?", (cid,)
            ).fetchone()
            title = chat_row["title"] if chat_row else str(cid)

            summary = f"💬 <b>{title}</b>:"
            if msgs:      summary += f"\n   📝 {msgs} новых сообщений"
            if mentions:  summary += f"\n   👤 {mentions} упоминаний"
            if rep_delta: summary += f"\n   {'⬆️' if rep_delta > 0 else '⬇️'} Репа: {rep_delta:+d}"
            chat_summaries.append(summary)

        conn.close()

        if not chat_summaries:
            continue

        # Формируем дайджест
        away_str = _format_away_time(away_hours)
        text = (
            f"{theme.SEP_THICK}\n"
            f"📡 <b>ДАЙДЖЕСТ</b>\n"
            f"{theme.SEP_THICK}\n\n"
            f"⏰ Тебя не было: <b>{away_str}</b>\n\n"
        )

        # Общая сводка
        if total_mentions:
            text += f"👤 Упомянули тебя: <b>{total_mentions}</b> раз\n"
        if total_rep_delta:
            text += f"{'⬆️' if total_rep_delta > 0 else '⬇️'} Репутация: <b>{total_rep_delta:+d}</b>\n"
        if total_msgs:
            text += f"💬 Новых сообщений: <b>{total_msgs}</b>\n"

        text += "\n<b>По чатам:</b>\n\n"
        text += "\n\n".join(chat_summaries[:5])  # максимум 5 чатов

        # Добавляем дни рождения
        birthdays = await _get_todays_birthdays()
        if birthdays:
            text += f"\n\n🎂 <b>Дни рождения сегодня:</b>\n"
            for name in birthdays[:3]:
                text += f"• {name}\n"

        try:
            await _bot.send_message(
                uid, text,
                parse_mode="HTML",
                reply_markup=_digest_kb()
            )
            _last_digest[uid] = now

            # Сбрасываем события
            _user_events[uid] = {}

            # Логируем
            conn = db()
            conn.execute(
                "INSERT INTO notif_log (uid, type, data) VALUES (?,?,?)",
                (uid, "digest", json.dumps({
                    "mentions": total_mentions,
                    "rep_delta": total_rep_delta,
                    "msgs": total_msgs
                }))
            )
            conn.commit()
            conn.close()

        except Exception as e:
            # Пользователь заблокировал бота
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                _user_events.pop(uid, None)


def _format_away_time(hours: float) -> str:
    if hours < 1:
        return f"{int(hours * 60)} мин"
    elif hours < 24:
        h = int(hours)
        m = int((hours - h) * 60)
        return f"{h}ч {m}м" if m else f"{h}ч"
    else:
        d = int(hours / 24)
        h = int(hours % 24)
        return f"{d}д {h}ч" if h else f"{d}д"


async def _get_todays_birthdays() -> List[str]:
    """Возвращает имена именинников сегодня"""
    conn = db()
    today = datetime.now()
    rows = conn.execute(
        "SELECT name FROM birthdays WHERE day=? AND month=?",
        (today.day, today.month)
    ).fetchall()
    conn.close()
    return [r["name"] for r in rows if r["name"]]


def _digest_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=theme.BTN_NOTIF_SETTINGS, callback_data="notif:settings"),
        InlineKeyboardButton(text=theme.BTN_NOTIF_DISABLE,   callback_data="notif:disable_all"),
    ]])


# ══════════════════════════════════════════════════════════════
#  УВЕДОМЛЕНИЯ О УПОМИНАНИЯХ
# ══════════════════════════════════════════════════════════════

async def notify_mention(to_uid: int, from_name: str,
                          chat_title: str, text: str):
    """Мгновенное уведомление об упоминании"""
    settings = get_settings(to_uid)
    if not settings.get("mentions", True):
        return

    # Не спамим если пользователь активен
    last = _last_active.get(to_uid, 0)
    if time.time() - last < 300:  # активен последние 5 минут
        return

    try:
        await _bot.send_message(
            to_uid,
            f"{theme.SEP_THICK}\n"
            f"👤 <b>ТЕБЯ УПОМЯНУЛИ</b>\n"
            f"{theme.SEP_THICK}\n\n"
            f"💬 Чат: <b>{chat_title}</b>\n"
            f"👤 Кто: <b>{from_name}</b>\n\n"
            f"📝 <i>{text[:200]}</i>",
            parse_mode="HTML"
        )
    except:
        pass


async def notify_rep_change(uid: int, cid: int, delta: int,
                             from_name: str, chat_title: str):
    """Уведомление об изменении репутации"""
    settings = get_settings(uid)
    if not settings.get("rep", True):
        return

    # Не спамим — только если изменение значительное
    if abs(delta) < 5:
        await track_rep_change(uid, cid, delta)
        return

    try:
        arrow = "⬆️" if delta > 0 else "⬇️"
        await _bot.send_message(
            uid,
            f"{arrow} <b>Репутация изменилась</b>\n\n"
            f"💬 {chat_title}\n"
            f"👤 {from_name}\n"
            f"{'⬆️' if delta > 0 else '⬇️'} {delta:+d} репутации",
            parse_mode="HTML"
        )
    except:
        await track_rep_change(uid, cid, delta)


# ══════════════════════════════════════════════════════════════
#  КОМАНДА /notifications
# ══════════════════════════════════════════════════════════════

async def cmd_notifications(message: Message):
    """Настройки уведомлений"""
    if message.chat.type != "private":
        await message.reply(
            "⚙️ Настройки уведомлений доступны в ЛС боту.\n"
            "Напиши /notifications мне в личку!",
        )
        return

    uid      = message.from_user.id
    settings = get_settings(uid)

    await message.answer(
        _format_settings_text(settings),
        parse_mode="HTML",
        reply_markup=_settings_kb(settings)
    )


def _format_settings_text(settings: dict) -> str:
    def st(key): return "✅" if settings.get(key, True) else "❌"
    hours = settings.get("digest_min_hours", 2)
    return (
        f"{theme.SEP_THICK}\n"
        f"🔔 <b>УВЕДОМЛЕНИЯ</b>\n"
        f"{theme.SEP_THICK}\n\n"
        f"{st('digest')} Дайджест пока не было\n"
        f"{st('mentions')} Упоминания в чате\n"
        f"{st('rep')} Изменения репутации\n"
        f"{st('levelup')} Повышение уровня\n"
        f"{st('birthday')} Дни рождения\n"
        f"{st('tickets')} Новые тикеты\n\n"
        f"⏰ Дайджест через: <b>{hours}ч</b> отсутствия"
    )


def _settings_kb(settings: dict) -> InlineKeyboardMarkup:
    def btn(key, label):
        st = "✅" if settings.get(key, True) else "❌"
        return InlineKeyboardButton(
            text=f"{st} {label}",
            callback_data=f"notif:toggle:{key}"
        )

    hours = settings.get("digest_min_hours", 2)
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("digest",   "Дайджест"),
         btn("mentions", "Упоминания")],
        [btn("rep",      "Репа"),
         btn("levelup",  "Уровень")],
        [btn("birthday", "Дни рождения"),
         btn("tickets",  "Тикеты")],
        [
            InlineKeyboardButton(
                text=f"⏰ Через {hours}ч",
                callback_data="notif:hours"
            ),
            InlineKeyboardButton(
                text="🔕 Откл. всё",
                callback_data="notif:disable_all"
            ),
        ],
        [InlineKeyboardButton(
            text="✅ Вкл. всё",
            callback_data="notif:enable_all"
        )],
    ])


async def cb_notifications(call: CallbackQuery):
    """Обработчик настроек уведомлений"""
    parts  = call.data.split(":")
    action = parts[1]
    uid    = call.from_user.id

    settings = get_settings(uid)

    if action == "settings":
        await call.message.edit_text(
            _format_settings_text(settings),
            parse_mode="HTML",
            reply_markup=_settings_kb(settings)
        )

    elif action == "toggle":
        key = parts[2]
        settings[key] = not settings.get(key, True)
        save_settings(uid, settings)
        await call.message.edit_text(
            _format_settings_text(settings),
            parse_mode="HTML",
            reply_markup=_settings_kb(settings)
        )
        status = "включены" if settings[key] else "выключены"
        await call.answer(f"{'✅' if settings[key] else '❌'} Уведомления {status}")

    elif action == "hours":
        hours = settings.get("digest_min_hours", 2)
        # Циклически меняем 2 → 4 → 8 → 24 → 2
        idx  = DIGEST_INTERVALS.index(hours) if hours in DIGEST_INTERVALS else 0
        new_hours = DIGEST_INTERVALS[(idx + 1) % len(DIGEST_INTERVALS)]
        settings["digest_min_hours"] = new_hours
        save_settings(uid, settings)
        await call.message.edit_text(
            _format_settings_text(settings),
            parse_mode="HTML",
            reply_markup=_settings_kb(settings)
        )
        await call.answer(f"⏰ Дайджест через {new_hours}ч отсутствия")

    elif action == "disable_all":
        for key in ["digest", "mentions", "rep", "levelup", "birthday", "tickets"]:
            settings[key] = False
        save_settings(uid, settings)
        await call.message.edit_text(
            "🔕 <b>Все уведомления отключены</b>\n\n"
            "Включить обратно: /notifications",
            parse_mode="HTML"
        )
        await call.answer("🔕 Всё отключено")

    elif action == "enable_all":
        for key in ["digest", "mentions", "rep", "levelup", "birthday", "tickets"]:
            settings[key] = True
        save_settings(uid, settings)
        await call.message.edit_text(
            _format_settings_text(settings),
            parse_mode="HTML",
            reply_markup=_settings_kb(settings)
        )
        await call.answer("✅ Всё включено")

    await call.answer()


# ══════════════════════════════════════════════════════════════
#  МГНОВЕННЫЙ ДАЙДЖЕСТ ПО ЗАПРОСУ
# ══════════════════════════════════════════════════════════════

async def cmd_digest_now(message: Message):
    """/digest — получить дайджест прямо сейчас"""
    uid = message.from_user.id

    if message.chat.type != "private":
        await message.reply("📡 Дайджест отправлен тебе в ЛС!")

    chats = _user_events.get(uid, {})
    if not chats:
        try:
            await _bot.send_message(
                uid,
                f"{theme.SEP_THICK}\n"
                "📡 <b>ДАЙДЖЕСТ</b>\n"
                f"{theme.SEP_THICK}\n\n"
                "✅ Всё спокойно — новых событий нет!\n\n"
                "Как только что-то произойдёт — я сообщу.",
                parse_mode="HTML"
            )
        except:
            await message.reply("📡 Новых событий нет!")
        return

    # Форсируем отправку дайджеста
    _last_digest[uid] = 0  # сбрасываем время чтобы дайджест точно ушёл
    _last_active[uid] = 0  # считаем что отсутствовал
    await _check_and_send_digests()


# ══════════════════════════════════════════════════════════════
#  СТАТИСТИКА УВЕДОМЛЕНИЙ
# ══════════════════════════════════════════════════════════════

async def cmd_notif_stats(message: Message):
    """/notifstats — статистика уведомлений (для владельца)"""
    conn = db()
    total     = conn.execute("SELECT COUNT(*) FROM notif_log").fetchone()[0]
    today     = conn.execute(
        "SELECT COUNT(*) FROM notif_log WHERE date(sent_at)=date('now')"
    ).fetchone()[0]
    users     = conn.execute(
        "SELECT COUNT(DISTINCT uid) FROM notif_settings"
    ).fetchone()[0]
    conn.close()

    active_tracking = len([u for u, e in _user_events.items() if e])

    await message.reply(
        f"{theme.SEP_THICK}\n"
        f"📊 <b>СТАТИСТИКА УВЕДОМЛЕНИЙ</b>\n"
        f"{theme.SEP_THICK}\n\n"
        f"📨 Всего отправлено: <b>{total}</b>\n"
        f"📅 Сегодня: <b>{today}</b>\n"
        f"👥 Пользователей с настройками: <b>{users}</b>\n"
        f"👁 Активно отслеживается: <b>{active_tracking}</b>",
        parse_mode="HTML"
    )


# ══════════════════════════════════════════════════════════════
#  РЕГИСТРАЦИЯ ХЕНДЛЕРОВ
# ══════════════════════════════════════════════════════════════

def _register_handlers(dp: Dispatcher):
    dp.message.register(
        cmd_notifications,
        Command("notifications")
    )
    dp.message.register(
        cmd_digest_now,
        Command("digest")
    )
    dp.message.register(
        cmd_notif_stats,
        Command("notifstats")
    )
    dp.callback_query.register(
        cb_notifications,
        F.data.startswith("notif:")
    )
    log.info("✅ notifications.py хендлеры зарегистрированы")

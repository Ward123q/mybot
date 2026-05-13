# -*- coding: utf-8 -*-
"""
ui.py — 🎨 UI-слой бота (Modern Dark)
═════════════════════════════════════════════════════════════════
Все redesign-функции построения сообщений и клавиатур.

Использование в bot.py:
  import ui

  # вместо большого блока с разметкой:
  text, kb = ui.start_screen(name=name, level=lvl, xp=xp,
                              xp_next=xp_next, warns=warns, uptime=uptime)
  await message.answer(text, parse_mode="HTML", reply_markup=kb)

ВАЖНО: модуль НЕ читает БД и НЕ дергает aiogram-API.
Только формирует текст и клавиатуру из переданных аргументов.
Это делает его легко переиспользуемым и тестируемым.
"""

from typing import Optional, List, Tuple, Dict, Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

import theme as T


# ══════════════════════════════════════════════════════════════
#  СТАРТ — главное приветствие
# ══════════════════════════════════════════════════════════════

def start_screen(
    *,
    name: str,
    level: int,
    level_title: str,
    xp: int,
    xp_next: int,
    warns: int,
    uptime_seconds: int,
    bot_username: str = "",
    dashboard_url: str = "",
) -> Tuple[str, InlineKeyboardMarkup]:
    """Главный экран /start в ЛС."""

    # Прогресс-бар XP
    xp_bar = T.progress(xp, xp_next, length=12) if xp_next else T.PROG_FULL * 12
    pct = int(100 * xp / xp_next) if xp_next else 100

    text = (
        f"{T.DIV}\n"
        f"{T.Icons.SHIELD}  <b>CHAT GUARD</b>\n"
        f"{T.DIV}\n\n"

        f"Добро пожаловать, <b>{name}</b>.\n\n"

        f"<b>СТАТУС</b>\n"
        f"{T.ITEM} {T.Icons.LEVEL} Уровень: <b>{level}</b> · <i>{level_title}</i>\n"
        f"{T.ITEM} {T.Icons.XP} {xp_bar}\n"
        f"{T.ITEM} XP: <b>{xp}</b> / <b>{xp_next}</b> ({pct}%)\n"
        f"{T.LAST} {T.Icons.WARN} Предупреждений: <b>{warns}</b>\n\n"

        f"<b>ВОЗМОЖНОСТИ</b>\n"
        f"{T.ITEM} {T.Icons.SHIELD} Модерация и защита чата\n"
        f"{T.ITEM} {T.Icons.LEVEL} Уровни · опыт · репутация\n"
        f"{T.ITEM} {T.Icons.FRIENDS} Социальная система\n"
        f"{T.ITEM} {T.Icons.SUPPORT} Поддержка через тикеты\n"
        f"{T.LAST} {T.Icons.TRANSLATE} Веб-дашборд\n"

        f"{T.footer(f'Аптайм бота · {T.fmt_uptime(uptime_seconds)}')}"
    )

    rows = [
        [
            InlineKeyboardButton(text=f"{T.Icons.HELP} Команды",  callback_data="start:help"),
            InlineKeyboardButton(text=f"{T.Icons.PROFILE} Профиль", callback_data="start:profile"),
        ],
        [
            InlineKeyboardButton(text=f"{T.Icons.SUPPORT} Тикет",  callback_data="start:ticket"),
            InlineKeyboardButton(text=f"{T.Icons.AUDIT} Правила",  callback_data="start:rules"),
        ],
        [
            InlineKeyboardButton(text=f"{T.Icons.FRIENDS} Друзья",   callback_data="start:friends"),
            InlineKeyboardButton(text=f"{T.Icons.LOVE} Комплимент", callback_data="start:compliment"),
        ],
    ]
    if dashboard_url:
        rows.append([InlineKeyboardButton(text=f"{T.Icons.TRANSLATE} Веб-дашборд", url=dashboard_url)])

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def start_screen_group(name: str, bot_username: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Краткий /start в группе."""
    text = (
        f"{T.DIV}\n"
        f"{T.Icons.SHIELD}  <b>CHAT GUARD</b>\n"
        f"{T.DIV}\n\n"
        f"Здравствуй, <b>{name}</b>.\n\n"
        f"Я — система автоматической модерации этого чата.\n"
        f"По вопросам обращайся в личные сообщения.\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"{T.Icons.CHAT} Перейти в ЛС",
            url=f"https://t.me/{bot_username}?start=hello"
        )
    ]])
    return text, kb


# ══════════════════════════════════════════════════════════════
#  HELP / КОМАНДЫ
# ══════════════════════════════════════════════════════════════

def help_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Экран /help в ЛС — карточки с категориями."""
    text = (
        f"{T.DIV}\n"
        f"{T.Icons.HELP}  <b>КОМАНДЫ БОТА</b>\n"
        f"{T.DIV}\n\n"

        f"<b>ПРОФИЛЬ</b>\n"
        f"{T.ITEM} <code>/profile</code> — мой профиль\n"
        f"{T.ITEM} <code>/me</code> — краткая карточка\n"
        f"{T.ITEM} <code>/setbio</code> — установить биографию\n"
        f"{T.LAST} <code>/setmood</code> — настроение\n\n"

        f"<b>СОЦИАЛЬНОЕ</b>\n"
        f"{T.ITEM} <code>/addfriend</code> — добавить друга\n"
        f"{T.ITEM} <code>/propose</code> — предложить отношения\n"
        f"{T.ITEM} <code>/gift</code> — подарок\n"
        f"{T.LAST} <code>/ship</code> — совместимость\n\n"

        f"<b>УТИЛИТЫ</b>\n"
        f"{T.ITEM} <code>/music</code> — поиск трека\n"
        f"{T.ITEM} <code>/imagine</code> — генерация картинки\n"
        f"{T.ITEM} <code>/tr</code> — перевод\n"
        f"{T.LAST} <code>/idea</code> — тема для обсуждения\n\n"

        f"<b>ПОДДЕРЖКА</b>\n"
        f"{T.ITEM} <code>/ticket</code> — открыть тикет\n"
        f"{T.LAST} <code>/appeal</code> — апелляция\n"

        f"{T.footer('В чате используй /help для полного списка')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{T.Icons.BACK} Назад", callback_data="start:back")
    ]])
    return text, kb


# ══════════════════════════════════════════════════════════════
#  ПРОФИЛЬ
# ══════════════════════════════════════════════════════════════

def profile_card(
    *,
    uid: int,
    name: str,
    level: int,
    level_title: str,
    xp: int,
    xp_next: int,
    reputation: int = 0,
    warns: int = 0,
    streak: int = 0,
    friends: int = 0,
    mood: str = "",
    bio: str = "",
    messages_count: int = 0,
    online: bool = False,
) -> Tuple[str, InlineKeyboardMarkup]:
    """Профиль пользователя — карточка."""

    status = T.status_dot(online)
    xp_bar = T.progress(xp, xp_next, length=12) if xp_next else T.PROG_FULL * 12
    pct = int(100 * xp / xp_next) if xp_next else 100

    text = (
        f"{T.DIV}\n"
        f"{T.Icons.PROFILE}  <b>ПРОФИЛЬ · {name.upper()}</b>\n"
        f"{T.DIV}\n\n"

        f"{status} <b>{name}</b>\n"
        f"<code>ID: {uid}</code>\n\n"

        f"<b>ПРОГРЕСС</b>\n"
        f"{T.ITEM} {T.Icons.LEVEL} Уровень: <b>{level}</b> · {level_title}\n"
        f"{T.ITEM} {T.Icons.XP} {xp_bar}  {pct}%\n"
        f"{T.LAST} XP: <b>{xp}</b> / <b>{xp_next}</b>\n\n"

        f"<b>СТАТИСТИКА</b>\n"
        f"{T.ITEM} {T.Icons.REP} Репутация: <b>{reputation}</b>\n"
        f"{T.ITEM} {T.Icons.STREAK} Стрик: <b>{streak}</b> дн.\n"
        f"{T.ITEM} {T.Icons.FRIENDS} Друзей: <b>{friends}</b>\n"
        f"{T.ITEM} {T.Icons.CHAT} Сообщений: <b>{T.fmt_number(messages_count)}</b>\n"
        f"{T.LAST} {T.Icons.WARN} Варнов: <b>{warns}</b>\n\n"

        f"<b>ЛИЧНОЕ</b>\n"
        f"{T.ITEM} {T.Icons.MOOD} Настроение: {mood or '<i>не указано</i>'}\n"
        f"{T.LAST} {T.Icons.BIO} Био: {bio or '<i>не указано</i>'}\n"

        f"{T.footer('Изменить · /setbio · /setmood')}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"{T.Icons.BIO} Биография",   callback_data="prof:editbio"),
            InlineKeyboardButton(text=f"{T.Icons.MOOD} Настроение", callback_data="prof:editmood"),
        ],
        [
            InlineKeyboardButton(text=f"{T.Icons.STATS} Подробно",  callback_data="prof:stats"),
            InlineKeyboardButton(text=f"{T.Icons.FRIENDS} Друзья",  callback_data="prof:friends"),
        ],
        [InlineKeyboardButton(text=f"{T.Icons.BACK} Назад", callback_data="start:back")],
    ])
    return text, kb


def me_card(
    *,
    name: str,
    level: int,
    level_title: str,
    xp: int,
    xp_next: int,
    rank_in_chat: int = 0,
    messages_today: int = 0,
) -> str:
    """Короткая карточка /me (для группы)."""

    xp_bar = T.progress(xp, xp_next, length=10) if xp_next else T.PROG_FULL * 10
    pct = int(100 * xp / xp_next) if xp_next else 100

    rank_str = f"#{rank_in_chat}" if rank_in_chat else "—"

    return (
        f"{T.Icons.PROFILE}  <b>{name.upper()}</b>\n"
        f"{T.DIV_SHORT}\n"
        f"{T.ITEM} {T.Icons.LEVEL} <b>{level}</b> · {level_title}\n"
        f"{T.ITEM} {xp_bar}  {pct}%\n"
        f"{T.ITEM} {T.Icons.TROPHY} Ранг в чате: <b>{rank_str}</b>\n"
        f"{T.LAST} {T.Icons.CHAT} Сегодня: <b>{messages_today}</b> сообщ."
    )


# ══════════════════════════════════════════════════════════════
#  ПРАВИЛА ЧАТА
# ══════════════════════════════════════════════════════════════

def rules_screen() -> Tuple[str, InlineKeyboardMarkup]:
    text = (
        f"{T.DIV}\n"
        f"{T.Icons.AUDIT}  <b>ПРАВИЛА ЧАТА</b>\n"
        f"{T.DIV}\n\n"

        f"<b>НАРУШЕНИЯ И САНКЦИИ</b>\n"
        f"{T.ITEM} Контент 18+ {T.ARROW} варн · бан\n"
        f"{T.ITEM} Наркотики {T.ARROW} бан\n"
        f"{T.ITEM} Реклама / спам {T.ARROW} мут · бан\n"
        f"{T.ITEM} Оскорбление администрации {T.ARROW} варн · бан\n"
        f"{T.LAST} Флуд {T.ARROW} мут\n\n"

        f"<b>КОНФИДЕНЦИАЛЬНОСТЬ</b>\n"
        f"{T.ITEM} Не публикуй чужие данные\n"
        f"{T.LAST} Уважай личные границы\n\n"

        f"<b>ПОВЕДЕНИЕ</b>\n"
        f"{T.ITEM} Уважение к участникам\n"
        f"{T.ITEM} Запрет провокаций и троллинга\n"
        f"{T.LAST} Решение споров через тикет\n"

        f"{T.footer('Подробнее · /help')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{T.Icons.BACK} Назад", callback_data="start:back")
    ]])
    return text, kb


# ══════════════════════════════════════════════════════════════
#  ГЛАВНАЯ ПАНЕЛЬ АДМИНА /panel
# ══════════════════════════════════════════════════════════════

def panel_main(
    *,
    chat_title: str,
    members_count: int = 0,
    bans_today: int = 0,
    warns_today: int = 0,
    raid_active: bool = False,
    is_owner: bool = False,
) -> Tuple[str, InlineKeyboardMarkup]:
    """Главная панель управления чатом."""

    raid_line = (
        f"{T.Icons.DANGER} <b>АНТИРЕЙД АКТИВЕН</b>"
        if raid_active else
        f"{T.Icons.ONLINE} Защита в норме"
    )

    text = (
        f"{T.DIV}\n"
        f"{T.Icons.SHIELD}  <b>ПАНЕЛЬ · {chat_title.upper()}</b>\n"
        f"{T.DIV}\n\n"

        f"<b>СТАТИСТИКА ЗА СЕГОДНЯ</b>\n"
        f"{T.ITEM} {T.Icons.FRIENDS} Участников: <b>{T.fmt_number(members_count)}</b>\n"
        f"{T.ITEM} {T.Icons.BAN} Банов: <b>{bans_today}</b>\n"
        f"{T.LAST} {T.Icons.WARN} Варнов: <b>{warns_today}</b>\n\n"

        f"<b>СОСТОЯНИЕ ЗАЩИТЫ</b>\n"
        f"{T.LAST} {raid_line}\n"

        f"{T.footer('Выбери раздел ниже')}"
    )

    rows = [
        [
            InlineKeyboardButton(text=f"{T.Icons.BAN} Модерация",  callback_data="panel:mod"),
            InlineKeyboardButton(text=f"{T.Icons.SHIELD} Защита",   callback_data="panel:guard"),
        ],
        [
            InlineKeyboardButton(text=f"{T.Icons.STATS} Статистика", callback_data="panel:stats"),
            InlineKeyboardButton(text=f"{T.Icons.GAME} Развлечения", callback_data="panel:fun"),
        ],
        [
            InlineKeyboardButton(text=f"{T.Icons.SETTINGS} Настройки", callback_data="panel:settings"),
            InlineKeyboardButton(text=f"{T.Icons.AUDIT} Журнал",       callback_data="panel:log"),
        ],
    ]
    if is_owner:
        rows.append([
            InlineKeyboardButton(text=f"{T.Icons.CROWN} Владелец", callback_data="panel:owner"),
        ])
    rows.append([
        InlineKeyboardButton(text=f"{T.Icons.CLOSE} Закрыть", callback_data="panel:close"),
    ])

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


# ══════════════════════════════════════════════════════════════
#  СООБЩЕНИЯ О ДЕЙСТВИЯХ МОДЕРАЦИИ
# ══════════════════════════════════════════════════════════════

def mod_action_msg(
    *,
    action: str,            # ban|mute|warn|kick|unban|unmute
    target_name: str,
    target_mention: str,    # HTML mention
    reason: str = "",
    by_name: str = "",
    duration: str = "",     # для мутов: "30м", "1ч"
    warns_total: int = 0,
) -> str:
    """
    Унифицированное сообщение о действии модерации.
    Используется вместо разрозненных random.choice(BAN_MESSAGES).
    """
    titles = {
        "ban":    ("БАН",    T.Icons.BAN),
        "unban":  ("РАЗБАН", T.Icons.UNBAN),
        "mute":   ("МУТ",    T.Icons.MUTE),
        "unmute": ("РАЗМУТ", T.Icons.UNMUTE),
        "warn":   ("ВАРН",   T.Icons.WARN),
        "unwarn": ("СНЯТ ВАРН", T.Icons.UNWARN),
        "kick":   ("КИК",    T.Icons.KICK),
    }
    title, icon = titles.get(action, ("ДЕЙСТВИЕ", T.Icons.SHIELD))

    text = (
        f"{T.DIV}\n"
        f"{icon}  <b>{title}</b>\n"
        f"{T.DIV}\n\n"
        f"{T.ITEM} Пользователь: {target_mention}\n"
    )
    if reason:
        text += f"{T.ITEM} Причина: <i>{reason}</i>\n"
    if duration:
        text += f"{T.ITEM} Срок: <b>{duration}</b>\n"
    if warns_total and action == "warn":
        text += f"{T.ITEM} Всего варнов: <b>{warns_total}</b>\n"
    if by_name:
        text += f"{T.LAST} Модератор: <b>{by_name}</b>\n"
    else:
        # Заменить последний ├ на └
        idx = text.rfind(T.ITEM)
        if idx != -1:
            text = text[:idx] + T.LAST + text[idx + len(T.ITEM):]
    return text


# ══════════════════════════════════════════════════════════════
#  СООБЩЕНИЯ ОБ ОШИБКАХ / ОТКАЗАХ
# ══════════════════════════════════════════════════════════════

def error_box(title: str, body: str) -> str:
    """Сообщение об ошибке."""
    return (
        f"{T.Icons.FAIL}  <b>{title.upper()}</b>\n"
        f"{T.DIV_SHORT}\n"
        f"{body}"
    )


def success_box(title: str, body: str = "") -> str:
    """Сообщение об успехе."""
    out = (
        f"{T.Icons.OK}  <b>{title.upper()}</b>\n"
    )
    if body:
        out += f"{T.DIV_SHORT}\n{body}"
    return out


def warning_box(title: str, body: str) -> str:
    """Предупреждение."""
    return (
        f"{T.Icons.WARN}  <b>{title.upper()}</b>\n"
        f"{T.DIV_SHORT}\n"
        f"{body}"
    )


def info_box(title: str, body: str) -> str:
    """Информационное сообщение."""
    return (
        f"{T.Icons.INFO}  <b>{title.upper()}</b>\n"
        f"{T.DIV_SHORT}\n"
        f"{body}"
    )


def need_reply() -> str:
    """Стандартный отказ — нет реплая."""
    return error_box("ТРЕБУЕТСЯ РЕПЛАЙ", "Ответь на сообщение пользователя, к которому хочешь применить действие.")


def need_admin() -> str:
    """Стандартный отказ — не админ."""
    return error_box("ДОСТУП ЗАПРЕЩЁН", "Эта команда доступна только администраторам.")


def need_owner() -> str:
    """Стандартный отказ — не владелец."""
    return error_box("ТОЛЬКО ВЛАДЕЛЕЦ", "Эта команда доступна только владельцу бота.")

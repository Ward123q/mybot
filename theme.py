# -*- coding: utf-8 -*-
"""
theme.py — 🎨 Design System «Modern Dark»
═════════════════════════════════════════════════════════════════
Единая дизайн-система для всех сообщений бота.

Принципы:
  • Жирные разделители ━━━━━━━━━━
  • Умеренное использование эмодзи (1–2 на блок, не больше)
  • Заголовки UPPERCASE для акцента
  • Прогресс-бары и визуальные показатели
  • Единые иконки для одинаковых действий

Использование:
  import theme as T

  await message.answer(
      T.header("ПРОФИЛЬ") +
      T.line("Уровень", "12 · Бриллиант") +
      T.line("XP", T.progress(842, 1000) + " 842/1000") +
      T.footer("Изменить: /setbio")
  )
"""

# ══════════════════════════════════════════════════════════════
#  СИМВОЛЫ И ИКОНКИ
# ══════════════════════════════════════════════════════════════

# ─── разделители ───
DIV       = "━━━━━━━━━━━━━━━━━━━"   # жирный разделитель (главный)
DIV_SHORT = "━━━━━━━━━"             # короткий
DIV_THIN  = "─────────────────"     # тонкий (для подсекций)
DIV_DOT   = "· · · · · · · · · · ·" # пунктир

# ─── ветвление списков ───
BRANCH      = "┃"   # вертикальная линия для секций
ITEM        = "├"   # элемент списка
LAST        = "└"   # последний элемент
ARROW       = "→"
RIGHT       = "›"
BULLET      = "•"

# ─── прогресс-бар ───
PROG_FULL   = "▰"   # заполненный
PROG_EMPTY  = "▱"   # пустой

# ─── символы статусов ───
OK          = "✓"
FAIL        = "✗"
WARN        = "⚠"
INFO        = "ⓘ"

# ─── общие иконки (одна на действие, всегда одна и та же) ───
class Icons:
    # Действия модерации
    BAN     = "🔨"
    UNBAN   = "🔓"
    MUTE    = "🔇"
    UNMUTE  = "🔊"
    KICK    = "👞"
    WARN    = "⚠️"
    UNWARN  = "✅"
    CLEAR   = "🧹"
    LOCK    = "🔒"
    UNLOCK  = "🔓"

    # Профиль
    PROFILE   = "👤"
    LEVEL     = "📊"
    XP        = "✨"
    REP       = "💎"
    MOOD      = "🎭"
    BIO       = "📝"
    FRIENDS   = "👥"
    STREAK    = "🔥"
    BIRTHDAY  = "🎂"

    # Социальное
    GIFT     = "🎁"
    LOVE     = "💝"
    SHIP     = "💞"
    CROWN    = "👑"

    # Игры/фан
    DICE      = "🎲"
    GAME      = "🎮"
    SHOP      = "🛒"
    CASINO    = "🎰"
    TROPHY    = "🏆"

    # Утилиты
    HELP      = "📋"
    SETTINGS  = "⚙️"
    STATS     = "📈"
    TIME      = "🕐"
    CALENDAR  = "📅"
    MUSIC     = "🎵"
    IMAGE     = "🖼"
    TRANSLATE = "🌐"

    # Безопасность / админ
    SHIELD    = "🛡"
    GUARD     = "🛡"
    ALERT     = "🚨"
    AUDIT     = "📜"
    FROZEN    = "🧊"
    SUPPORT   = "🎫"
    BACK      = "◀️"
    FORWARD   = "▶️"
    HOME      = "🏠"
    CLOSE     = "✖️"
    REFRESH   = "🔄"

    # Каналы / связь
    CHAT      = "💬"
    BOT       = "🤖"
    LINK      = "🔗"

    # Состояния
    ONLINE    = "🟢"
    AWAY      = "🟡"
    OFFLINE   = "⚪"
    DANGER    = "🔴"


# ══════════════════════════════════════════════════════════════
#  ХЕЛПЕРЫ — БЛОКИ ТЕКСТА
# ══════════════════════════════════════════════════════════════

def header(title: str, icon: str = "") -> str:
    """
    Жирный заголовок секции с двойным разделителем.

        ━━━━━━━━━━━━━━━━━━━
        🛡  ЗАГОЛОВОК
        ━━━━━━━━━━━━━━━━━━━
    """
    prefix = f"{icon}  " if icon else ""
    return f"{DIV}\n{prefix}<b>{title.upper()}</b>\n{DIV}\n\n"


def section(title: str, icon: str = "") -> str:
    """
    Подзаголовок секции — без разделителей.

        🔧 <b>НАСТРОЙКИ</b>
    """
    prefix = f"{icon} " if icon else ""
    return f"{prefix}<b>{title}</b>\n"


def divider(thin: bool = False) -> str:
    """Разделитель внутри сообщения."""
    return f"\n{DIV_THIN if thin else DIV}\n\n"


def footer(text: str = "") -> str:
    """
    Серая подпись внизу сообщения (italic).

        ━━━━━━━━━━━━━━━━━━━
        <i>💡 Подсказка...</i>
    """
    if not text:
        return ""
    return f"\n{DIV}\n<i>{text}</i>"


def kv(key: str, value, icon: str = "", last: bool = False) -> str:
    """
    Строка key-value с ветвлением.

        ├ 📊 Уровень: 12

    last=True даёт └ вместо ├
    """
    branch = LAST if last else ITEM
    prefix = f"{icon} " if icon else ""
    return f"{branch} {prefix}{key}: <b>{value}</b>\n"


def line(text: str, icon: str = "", last: bool = False) -> str:
    """
    Простая строка с ветвлением (без key:value).

        ├ 🔨 Бан пользователя
    """
    branch = LAST if last else ITEM
    prefix = f"{icon} " if icon else ""
    return f"{branch} {prefix}{text}\n"


def progress(current: int, total: int, length: int = 12) -> str:
    """
    Прогресс-бар.

        ▰▰▰▰▰▰▰▰▱▱▱▱  842/1000

    Возвращает только полоску — число добавляй сам.
    """
    if total <= 0:
        return PROG_EMPTY * length
    filled = min(length, int(round(length * current / total)))
    return PROG_FULL * filled + PROG_EMPTY * (length - filled)


def progress_line(label: str, current: int, total: int,
                  length: int = 10, last: bool = False) -> str:
    """
    Готовая строка с прогрессом.

        ├ XP: ▰▰▰▰▰▰▱▱▱▱  842/1000  (84%)
    """
    branch = LAST if last else ITEM
    bar = progress(current, total, length)
    pct = int(100 * current / total) if total else 0
    return f"{branch} {label}: {bar}  <b>{current}/{total}</b>  ({pct}%)\n"


def card(title: str, lines: list, icon: str = "", footer_text: str = "") -> str:
    """
    Полная «карточка» — заголовок + список + опц. футер.

    lines — список строк (уже отформатированных через kv/line)
    """
    body = header(title, icon) + "".join(lines)
    if footer_text:
        body += footer(footer_text)
    return body


def big_value(label: str, value, icon: str = "") -> str:
    """
    Большая центральная строка для важных значений.

        💎  УРОВЕНЬ
        ━━━━━━━━━━━
        Бриллиант · 50
    """
    prefix = f"{icon}  " if icon else ""
    return f"{prefix}<b>{label.upper()}</b>\n{DIV_SHORT}\n<b>{value}</b>\n"


def alert(level: str, title: str, body: str) -> str:
    """
    Алерт-сообщение трёх уровней: info / warn / danger.
    """
    icons = {
        "info":    Icons.INFO,
        "warn":    Icons.ALERT,
        "danger":  Icons.DANGER,
        "success": Icons.ONLINE,
    }
    icon = icons.get(level, Icons.INFO)
    return (
        f"{DIV}\n"
        f"{icon}  <b>{title.upper()}</b>\n"
        f"{DIV}\n\n"
        f"{body}"
    )


def confirm_block(action: str, target: str, reason: str = "") -> str:
    """
    Блок подтверждения действия.
    """
    out = (
        f"{Icons.WARN}  <b>ПОДТВЕРДИТЕ ДЕЙСТВИЕ</b>\n"
        f"{DIV_SHORT}\n\n"
        f"{ITEM} Действие: <b>{action}</b>\n"
        f"{LAST} Цель: <b>{target}</b>\n"
    )
    if reason:
        out += f"\n{Icons.BIO} Причина: <i>{reason}</i>\n"
    return out


# ══════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ══════════════════════════════════════════════════════════════

def fmt_time_ago(seconds: int) -> str:
    """Форматирует «N секунд/минут/часов/дней назад» компактно."""
    if seconds < 60:
        return f"{seconds}с"
    if seconds < 3600:
        return f"{seconds // 60}м"
    if seconds < 86400:
        return f"{seconds // 3600}ч"
    if seconds < 86400 * 7:
        return f"{seconds // 86400}д"
    return f"{seconds // (86400 * 7)}н"


def fmt_uptime(seconds: int) -> str:
    """Аптайм бота: «3д 4ч 12м» или «5ч 23м» или «12м»."""
    d = seconds // 86400
    h = (seconds % 86400) // 3600
    m = (seconds % 3600) // 60
    parts = []
    if d: parts.append(f"{d}д")
    if h: parts.append(f"{h}ч")
    if m or not parts: parts.append(f"{m}м")
    return " ".join(parts)


def fmt_number(n: int) -> str:
    """1234567 → '1.2M', 1234 → '1.2K', 999 → '999'."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{n/1_000:.1f}K".replace(".0K", "K")
    return str(n)


def status_dot(online: bool, away: bool = False) -> str:
    """Статус кружок."""
    if online:
        return Icons.ONLINE
    if away:
        return Icons.AWAY
    return Icons.OFFLINE

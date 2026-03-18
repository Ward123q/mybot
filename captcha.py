# captcha.py — Картинка-капча для Telegram бота (aiogram 3.x)
#
# УСТАНОВКА:
#   pip install Pillow
#
# ПОДКЛЮЧЕНИЕ В bot.py (добавить в начало):
#   import captcha as cap
#   cap.setup(bot, dp)
#
# Капча запускается автоматически при входе нового участника.
# Настройки — константы ниже.

import asyncio
import io
import random
import time
import math
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, ChatPermissions,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile,
)

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  ⚙️ НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════════

CAPTCHA_TIMEOUT   = 90      # секунд на решение
CAPTCHA_KICK      = True    # кикнуть при ошибке/таймауте (False = только мут)
CAPTCHA_DIGITS    = 5       # количество цифр в капче
CAPTCHA_WIDTH     = 280     # ширина картинки
CAPTCHA_HEIGHT    = 100     # высота картинки
CAPTCHA_MAX_TRIES = 2       # попыток перед киком (1 = сразу кик)

# Чаты где капча ОТКЛЮЧЕНА (список cid)
CAPTCHA_EXCLUDE_CHATS: set = set()

# ══════════════════════════════════════════════════════════════════
#  🗄 ХРАНИЛИЩЕ АКТИВНЫХ КАПЧ
# ══════════════════════════════════════════════════════════════════
# { (chat_id, user_id): {code, msg_id, task, tries, name} }
_active: dict = {}


# ══════════════════════════════════════════════════════════════════
#  🎨 ГЕНЕРАЦИЯ КАРТИНКИ
# ══════════════════════════════════════════════════════════════════

def _generate_image(code: str) -> bytes:
    """Генерирует PNG с кодом, шумом и искажениями через Pillow."""
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
    except ImportError:
        raise RuntimeError("Установи Pillow: pip install Pillow")

    W, H = CAPTCHA_WIDTH, CAPTCHA_HEIGHT

    # Фон — тёмно-серый с лёгким шумом
    img = Image.new("RGB", (W, H), color=(28, 28, 35))
    draw = ImageDraw.Draw(img)

    # Шумовые пиксели
    for _ in range(W * H // 6):
        x = random.randint(0, W - 1)
        y = random.randint(0, H - 1)
        c = random.randint(40, 90)
        draw.point((x, y), fill=(c, c, c + random.randint(0, 20)))

    # Линии помехи
    for _ in range(8):
        x1 = random.randint(0, W)
        y1 = random.randint(0, H)
        x2 = random.randint(0, W)
        y2 = random.randint(0, H)
        col = (
            random.randint(50, 120),
            random.randint(50, 120),
            random.randint(80, 150),
        )
        draw.line([(x1, y1), (x2, y2)], fill=col, width=1)

    # Дуги помехи
    for _ in range(4):
        x0 = random.randint(-20, W // 2)
        y0 = random.randint(-20, H // 2)
        x1 = random.randint(W // 2, W + 20)
        y1 = random.randint(H // 2, H + 20)
        col = (random.randint(60, 130), random.randint(60, 130), random.randint(100, 180))
        draw.arc([x0, y0, x1, y1], start=random.randint(0, 360),
                 end=random.randint(0, 360), fill=col, width=1)

    # Попробуем шрифт, иначе дефолтный
    font_size = 52
    font = None
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arial.ttf",
    ]
    for fp in font_paths:
        try:
            from PIL import ImageFont as _IF
            font = _IF.truetype(fp, font_size)
            break
        except Exception:
            pass

    if font is None:
        try:
            from PIL import ImageFont as _IF
            font = _IF.load_default(size=font_size)
        except Exception:
            from PIL import ImageFont as _IF
            font = _IF.load_default()

    # Рисуем каждую цифру с индивидуальным смещением и поворотом
    char_w = (W - 20) // len(code)
    colors = [
        (255, 220, 100), (100, 220, 255), (255, 130, 130),
        (130, 255, 130), (220, 130, 255), (255, 180, 80),
    ]
    for i, ch in enumerate(code):
        col = colors[i % len(colors)]
        angle = random.randint(-22, 22)
        offset_y = random.randint(-8, 8)
        x = 10 + i * char_w + random.randint(-3, 3)
        y = (H - font_size) // 2 + offset_y

        # Рисуем в маленький слой, поворачиваем, вставляем
        char_img = Image.new("RGBA", (char_w + 10, H), (0, 0, 0, 0))
        char_draw = ImageDraw.Draw(char_img)

        # Тень
        char_draw.text((4, (H - font_size) // 2 + 2), ch,
                       font=font, fill=(0, 0, 0, 180))
        # Основной текст
        char_draw.text((2, (H - font_size) // 2), ch,
                       font=font, fill=col + (255,))

        rotated = char_img.rotate(angle, expand=False, resample=Image.BICUBIC)
        img.paste(rotated, (x - 2, 0), rotated)

    # Лёгкий blur для слияния
    img = img.filter(ImageFilter.GaussianBlur(radius=0.5))

    # Точки поверх (второй слой шума)
    draw2 = ImageDraw.Draw(img)
    for _ in range(180):
        x = random.randint(0, W - 1)
        y = random.randint(0, H - 1)
        c = random.randint(80, 160)
        draw2.point((x, y), fill=(c, c + 10, c + 20))

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════
#  ⏱ ТАЙМЕР — КИК ПО ИСТЕЧЕНИЮ ВРЕМЕНИ
# ══════════════════════════════════════════════════════════════════

async def _timeout_task(bot: Bot, chat_id: int, user_id: int, name: str):
    """Ждёт CAPTCHA_TIMEOUT секунд, потом кикает если не решил."""
    await asyncio.sleep(CAPTCHA_TIMEOUT)
    entry = _active.pop((chat_id, user_id), None)
    if not entry:
        return  # уже решил или уже удалён

    log.info(f"[CAPTCHA] Таймаут: {name} ({user_id}) в чате {chat_id}")

    # Удаляем сообщение с капчей
    try:
        await bot.delete_message(chat_id, entry["msg_id"])
    except Exception:
        pass

    # Уведомление
    try:
        notif = await bot.send_message(
            chat_id,
            f"⏰ <b>{name}</b> не прошёл капчу за {CAPTCHA_TIMEOUT} сек — "
            f"{'кик' if CAPTCHA_KICK else 'мут'}.",
            parse_mode="HTML",
        )
        await asyncio.sleep(8)
        try:
            await notif.delete()
        except Exception:
            pass
    except Exception:
        pass

    # Кик или мут
    await _punish(bot, chat_id, user_id)


async def _punish(bot: Bot, chat_id: int, user_id: int):
    """Кик или мут нарушителя."""
    try:
        if CAPTCHA_KICK:
            await bot.ban_chat_member(chat_id, user_id)
            await bot.unban_chat_member(chat_id, user_id)
        else:
            from datetime import timedelta
            await bot.restrict_chat_member(
                chat_id, user_id,
                ChatPermissions(can_send_messages=False),
                until_date=timedelta(hours=24),
            )
    except Exception as e:
        log.warning(f"[CAPTCHA] _punish error: {e}")


async def _unlock(bot: Bot, chat_id: int, user_id: int):
    """Снимает ограничения после успешной капчи."""
    try:
        await bot.restrict_chat_member(
            chat_id, user_id,
            ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
                can_change_info=False,
                can_invite_users=True,
                can_pin_messages=False,
            ),
        )
    except Exception as e:
        log.warning(f"[CAPTCHA] _unlock error: {e}")


# ══════════════════════════════════════════════════════════════════
#  📨 ЗАПУСК КАПЧИ
# ══════════════════════════════════════════════════════════════════

async def start_captcha(bot: Bot, chat_id: int, user_id: int, name: str):
    """
    Вызывать из on_new_member.
    Мутит участника, отправляет картинку с кодом, запускает таймер.
    """
    if chat_id in CAPTCHA_EXCLUDE_CHATS:
        return

    # Если уже есть активная капча — не спамим
    if (chat_id, user_id) in _active:
        return

    # Мутим сразу
    try:
        await bot.restrict_chat_member(
            chat_id, user_id,
            ChatPermissions(can_send_messages=False),
        )
    except Exception as e:
        log.warning(f"[CAPTCHA] restrict error: {e}")

    # Генерируем код
    code = "".join(str(random.randint(0, 9)) for _ in range(CAPTCHA_DIGITS))

    # Картинка
    try:
        img_bytes = _generate_image(code)
    except Exception as e:
        log.error(f"[CAPTCHA] image gen error: {e}")
        # Фоллбэк — текстовая капча
        code = str(random.randint(10000, 99999))
        img_bytes = None

    # Кнопка пропустить (для владельца)
    skip_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Пропустить (для администраторов)",
            callback_data=f"captcha_skip:{chat_id}:{user_id}",
        )
    ]])

    safe_name = name.replace("<", "&lt;").replace(">", "&gt;")
    caption = (
        f"👋 <b>{safe_name}</b>, добро пожаловать!\n\n"
        f"🔐 <b>Введи цифры с картинки</b> одним сообщением.\n"
        f"⏰ У тебя <b>{CAPTCHA_TIMEOUT} секунд</b>.\n"
        f"❌ {CAPTCHA_MAX_TRIES} неверных попытки — {'кик' if CAPTCHA_KICK else 'мут на 24ч'}."
    )

    try:
        if img_bytes:
            sent = await bot.send_photo(
                chat_id,
                photo=BufferedInputFile(img_bytes, filename="captcha.png"),
                caption=caption,
                parse_mode="HTML",
                reply_markup=skip_kb,
            )
        else:
            # Текстовый фоллбэк
            sent = await bot.send_message(
                chat_id,
                caption + f"\n\n<code>{code}</code>",
                parse_mode="HTML",
                reply_markup=skip_kb,
            )
    except Exception as e:
        log.error(f"[CAPTCHA] send error: {e}")
        return

    # Запускаем таймер
    task = asyncio.create_task(
        _timeout_task(bot, chat_id, user_id, safe_name)
    )

    _active[(chat_id, user_id)] = {
        "code":   code,
        "msg_id": sent.message_id,
        "task":   task,
        "tries":  0,
        "name":   safe_name,
    }
    log.info(f"[CAPTCHA] Запущена для {name} ({user_id}) в чате {chat_id}, код={code}")


# ══════════════════════════════════════════════════════════════════
#  ✉️ ОБРАБОТЧИК ОТВЕТА ПОЛЬЗОВАТЕЛЯ
# ══════════════════════════════════════════════════════════════════

async def _on_message(message: Message):
    """Ловит любое сообщение — проверяет есть ли активная капча."""
    if not message.from_user or not message.text:
        return

    key = (message.chat.id, message.from_user.id)
    entry = _active.get(key)
    if not entry:
        return

    # Удаляем ответ пользователя из чата (не засоряем)
    try:
        await message.delete()
    except Exception:
        pass

    answer = message.text.strip()
    correct = entry["code"]

    if answer == correct:
        # ✅ ВЕРНО
        _active.pop(key, None)
        entry["task"].cancel()

        await _unlock(message.bot, message.chat.id, message.from_user.id)

        try:
            await message.bot.delete_message(message.chat.id, entry["msg_id"])
        except Exception:
            pass

        ok_msg = await message.answer(
            f"✅ <b>{entry['name']}</b> прошёл проверку! Добро пожаловать 🎉",
            parse_mode="HTML",
        )
        await asyncio.sleep(6)
        try:
            await ok_msg.delete()
        except Exception:
            pass

        log.info(f"[CAPTCHA] Успех: {entry['name']} ({message.from_user.id})")

    else:
        # ❌ НЕВЕРНО
        entry["tries"] += 1
        log.info(
            f"[CAPTCHA] Неверно: {entry['name']} ({message.from_user.id}), "
            f"попытка {entry['tries']}/{CAPTCHA_MAX_TRIES}"
        )

        if entry["tries"] >= CAPTCHA_MAX_TRIES:
            # Исчерпал попытки
            _active.pop(key, None)
            entry["task"].cancel()

            try:
                await message.bot.delete_message(message.chat.id, entry["msg_id"])
            except Exception:
                pass

            fail_msg = await message.answer(
                f"❌ <b>{entry['name']}</b> не прошёл капчу — "
                f"{'кик' if CAPTCHA_KICK else 'мут'}.",
                parse_mode="HTML",
            )
            await asyncio.sleep(5)
            try:
                await fail_msg.delete()
            except Exception:
                pass

            await _punish(message.bot, message.chat.id, message.from_user.id)
        else:
            remaining = CAPTCHA_MAX_TRIES - entry["tries"]
            try:
                warn_msg = await message.answer(
                    f"❌ Неверно, <b>{entry['name']}</b>! "
                    f"Осталось попыток: <b>{remaining}</b>",
                    parse_mode="HTML",
                )
                await asyncio.sleep(4)
                try:
                    await warn_msg.delete()
                except Exception:
                    pass
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════
#  🔘 CALLBACK — ПРОПУСТИТЬ (для администраторов)
# ══════════════════════════════════════════════════════════════════

async def _on_skip_callback(call: CallbackQuery):
    """Администратор нажал 'Пропустить'."""
    try:
        member = await call.bot.get_chat_member(call.message.chat.id, call.from_user.id)
        is_admin = member.status in ("administrator", "creator")
    except Exception:
        is_admin = False

    if not is_admin:
        await call.answer("❌ Только администраторы могут пропустить капчу", show_alert=True)
        return

    _, chat_id_s, user_id_s = call.data.split(":")
    chat_id = int(chat_id_s)
    user_id = int(user_id_s)
    key = (chat_id, user_id)

    entry = _active.pop(key, None)
    if not entry:
        await call.answer("Капча уже завершена", show_alert=False)
        return

    entry["task"].cancel()
    await _unlock(call.bot, chat_id, user_id)

    try:
        await call.message.delete()
    except Exception:
        pass

    await call.answer("✅ Капча пропущена администратором", show_alert=False)
    skip_msg = await call.bot.send_message(
        chat_id,
        f"✅ Капча для <b>{entry['name']}</b> пропущена администратором "
        f"<b>{call.from_user.full_name}</b>.",
        parse_mode="HTML",
    )
    await asyncio.sleep(8)
    try:
        await skip_msg.delete()
    except Exception:
        pass

    log.info(
        f"[CAPTCHA] Пропущена admin={call.from_user.id} "
        f"для user={user_id} в чате {chat_id}"
    )


# ══════════════════════════════════════════════════════════════════
#  🔌 ПОДКЛЮЧЕНИЕ К ДИСПЕТЧЕРУ
# ══════════════════════════════════════════════════════════════════

def setup(bot_instance: Bot, dp: Dispatcher):
    """
    Регистрирует обработчики в диспетчере.
    Вызывай один раз в bot.py до start_polling:
        import captcha as cap
        cap.setup(bot, dp)
    """
    # Ответы на капчу — только приватные или групповые сообщения с текстом
    dp.message.register(
        _on_message,
        F.text & ~F.text.startswith("/"),
    )

    # Кнопка "пропустить"
    dp.callback_query.register(
        _on_skip_callback,
        F.data.startswith("captcha_skip:"),
    )

    log.info("[CAPTCHA] Модуль зарегистрирован")


# ══════════════════════════════════════════════════════════════════
#  📊 СТАТУС (для дашборда)
# ══════════════════════════════════════════════════════════════════

def get_active_count() -> int:
    """Сколько сейчас активных капч."""
    return len(_active)


def get_active_list() -> list:
    """Список активных капч для дашборда."""
    result = []
    for (cid, uid), entry in _active.items():
        result.append({
            "chat_id":  cid,
            "user_id":  uid,
            "name":     entry["name"],
            "tries":    entry["tries"],
        })
    return result

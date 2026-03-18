# captcha.py — Капча: ЛС если доступно, иначе в группе без мута
#
# ЛОГИКА:
#   1. Новый участник входит
#   2. Бот ПРОБУЕТ отправить капчу в ЛС
#      - Успех  -> мутит в группе, пишет в ЛС "введи цифры"
#      - Неудача -> НЕ мутит, пишет в группу "введи цифры прямо здесь"
#   3. Таймер: не ответил за CAPTCHA_TIMEOUT сек -> кик
#   4. Администратор видит кнопку "Пропустить"
#
# УСТАНОВКА:  pip install Pillow
#
# В bot.py:
#   import captcha as cap
#   cap.setup(bot, dp)          # до start_polling
#   # в on_new_member:
#   await cap.start_captcha(bot, cid, member.id, member.full_name)

import asyncio, io, random, logging
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, ChatPermissions,
    BufferedInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

log = logging.getLogger(__name__)

# ── Настройки ────────────────────────────────────────────────────
CAPTCHA_TIMEOUT    = 120   # секунд
CAPTCHA_KICK       = True  # False = мут 24ч вместо кика
CAPTCHA_DIGITS     = 5
CAPTCHA_WIDTH      = 280
CAPTCHA_HEIGHT     = 100
CAPTCHA_MAX_TRIES  = 3     # 3 попытки — потом наказание
CAPTCHA_EXCLUDE_CHATS: set = set()

# ── Хранилище ────────────────────────────────────────────────────
# Ключ — user_id (уникален для ЛС и для группы)
# mode: "pm" | "group"
_active: dict = {}   # {user_id: entry}


# ── Генерация картинки ───────────────────────────────────────────
def _make_image(code: str) -> bytes | None:
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
    except ImportError:
        log.warning("[CAPTCHA] Pillow не установлен: pip install Pillow")
        return None

    W, H = CAPTCHA_WIDTH, CAPTCHA_HEIGHT
    img  = Image.new("RGB", (W, H), (22, 22, 30))
    draw = ImageDraw.Draw(img)

    # Шумовые пиксели
    for _ in range(W * H // 5):
        c = random.randint(35, 75)
        draw.point(
            (random.randint(0, W-1), random.randint(0, H-1)),
            (c, c, c + random.randint(0, 25)),
        )
    # Линии-помехи
    for _ in range(10):
        col = (random.randint(50,110), random.randint(50,110), random.randint(80,150))
        draw.line(
            [(random.randint(0,W), random.randint(0,H)),
             (random.randint(0,W), random.randint(0,H))],
            fill=col, width=1,
        )
    # Дуги
    for _ in range(5):
        col = (random.randint(60,120), random.randint(60,120), random.randint(100,170))
        x0, y0 = random.randint(-30, W//2), random.randint(-30, H//2)
        x1, y1 = random.randint(W//2, W+30), random.randint(H//2, H+30)
        draw.arc([x0,y0,x1,y1],
                 random.randint(0,360), random.randint(0,360),
                 fill=col, width=1)

    # Шрифт
    font = None
    for fp in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arialbd.ttf",
    ]:
        try:
            from PIL import ImageFont as _F
            font = _F.truetype(fp, 52); break
        except Exception:
            pass
    if font is None:
        try:
            from PIL import ImageFont as _F; font = _F.load_default(size=52)
        except Exception:
            from PIL import ImageFont as _F; font = _F.load_default()

    palette = [(255,210,80),(100,215,255),(255,120,120),
               (120,255,140),(210,120,255),(255,170,70)]
    char_w  = (W - 20) // len(code)
    for i, ch in enumerate(code):
        col    = palette[i % len(palette)]
        angle  = random.randint(-25, 25)
        offset = random.randint(-10, 10)
        x      = 10 + i * char_w + random.randint(-4, 4)
        layer  = Image.new("RGBA", (char_w+12, H), (0,0,0,0))
        ld     = ImageDraw.Draw(layer)
        cy     = (H - 52) // 2 + offset
        ld.text((4, cy+2), ch, font=font, fill=(0,0,0,160))
        ld.text((2, cy),   ch, font=font, fill=col+(255,))
        rot = layer.rotate(angle, expand=False, resample=Image.BICUBIC)
        img.paste(rot, (x-2, 0), rot)

    img = img.filter(ImageFilter.GaussianBlur(radius=0.6))
    d2  = ImageDraw.Draw(img)
    for _ in range(200):
        c = random.randint(70, 160)
        d2.point((random.randint(0,W-1), random.randint(0,H-1)), (c, c+5, c+15))

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ── Наказание / разблокировка ────────────────────────────────────
async def _punish(bot: Bot, chat_id: int, user_id: int):
    try:
        if CAPTCHA_KICK:
            await bot.ban_chat_member(chat_id, user_id)
            await asyncio.sleep(0.3)
            await bot.unban_chat_member(chat_id, user_id)
        else:
            from datetime import timedelta
            await bot.restrict_chat_member(
                chat_id, user_id,
                ChatPermissions(can_send_messages=False),
                until_date=timedelta(hours=24),
            )
    except Exception as e:
        log.warning(f"[CAPTCHA] punish: {e}")


async def _unlock(bot: Bot, chat_id: int, user_id: int):
    try:
        await bot.restrict_chat_member(
            chat_id, user_id,
            ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True, can_change_info=False,
                can_invite_users=True, can_pin_messages=False,
            ),
        )
    except Exception as e:
        log.warning(f"[CAPTCHA] unlock: {e}")


# ── Таймер ───────────────────────────────────────────────────────
async def _timer(bot: Bot, user_id: int):
    await asyncio.sleep(CAPTCHA_TIMEOUT)
    entry = _active.pop(user_id, None)
    if not entry:
        return

    chat_id = entry["chat_id"]
    name    = entry["name"]
    log.info(f"[CAPTCHA] Таймаут {name} ({user_id})")

    # Удаляем капча-сообщения
    for mid, cid in [
        (entry.get("pm_msg_id"),    user_id),
        (entry.get("group_msg_id"), chat_id),
    ]:
        if mid:
            try: await bot.delete_message(cid, mid)
            except Exception: pass

    await _punish(bot, chat_id, user_id)

    try:
        n = await bot.send_message(
            chat_id,
            f"⏰ <b>{name}</b> не прошёл капчу за {CAPTCHA_TIMEOUT} сек "
            f"— {'кик' if CAPTCHA_KICK else 'мут'}.",
            parse_mode="HTML",
        )
        await asyncio.sleep(10)
        try: await n.delete()
        except Exception: pass
    except Exception:
        pass


# ── Успех ────────────────────────────────────────────────────────
async def _success(bot: Bot, user_id: int, entry: dict):
    chat_id = entry["chat_id"]
    if entry["mode"] == "pm":
        await _unlock(bot, chat_id, user_id)

    for mid, cid in [
        (entry.get("pm_msg_id"),    user_id),
        (entry.get("group_msg_id"), chat_id),
    ]:
        if mid:
            try: await bot.delete_message(cid, mid)
            except Exception: pass

    # ЛС — подтверждение
    if entry["mode"] == "pm":
        try:
            await bot.send_message(
                user_id,
                "✅ <b>Верно!</b> Добро пожаловать в чат 🎉",
                parse_mode="HTML",
            )
        except Exception:
            pass

    # Уведомление в группу
    try:
        n = await bot.send_message(
            chat_id,
            f"✅ <b>{entry['name']}</b> прошёл капчу!",
            parse_mode="HTML",
        )
        await asyncio.sleep(8)
        try: await n.delete()
        except Exception: pass
    except Exception:
        pass

    log.info(f"[CAPTCHA] Успех: {entry['name']} ({user_id}) mode={entry['mode']}")


# ── Неудача ──────────────────────────────────────────────────────
async def _fail(bot: Bot, user_id: int, entry: dict):
    chat_id = entry["chat_id"]
    entry["task"].cancel()
    _active.pop(user_id, None)

    for mid, cid in [
        (entry.get("pm_msg_id"),    user_id),
        (entry.get("group_msg_id"), chat_id),
    ]:
        if mid:
            try: await bot.delete_message(cid, mid)
            except Exception: pass

    if entry["mode"] == "pm":
        try:
            await bot.send_message(
                user_id,
                f"❌ Исчерпаны все попытки — "
                f"{'кик из чата' if CAPTCHA_KICK else 'мут на 24ч'}.",
                parse_mode="HTML",
            )
        except Exception:
            pass

    await _punish(bot, chat_id, user_id)

    try:
        n = await bot.send_message(
            chat_id,
            f"❌ <b>{entry['name']}</b> не прошёл капчу "
            f"({'кик' if CAPTCHA_KICK else 'мут'}).",
            parse_mode="HTML",
        )
        await asyncio.sleep(8)
        try: await n.delete()
        except Exception: pass
    except Exception:
        pass


# ── ГЛАВНАЯ ФУНКЦИЯ ──────────────────────────────────────────────
async def start_captcha(bot: Bot, chat_id: int, user_id: int, name: str):
    """Вызывать из on_new_member."""
    if chat_id in CAPTCHA_EXCLUDE_CHATS or user_id in _active:
        return

    code      = "".join(str(random.randint(0, 9)) for _ in range(CAPTCHA_DIGITS))
    safe_name = name.replace("<", "&lt;").replace(">", "&gt;")
    img_bytes = _make_image(code)

    pm_caption = (
        f"🔐 <b>Капча для входа в чат</b>\n\n"
        f"Введи <b>{CAPTCHA_DIGITS} цифр</b> с картинки одним сообщением.\n"
        f"⏰ У тебя <b>{CAPTCHA_TIMEOUT} секунд</b>.\n"
        f"❌ {CAPTCHA_MAX_TRIES} ошибки — {'кик' if CAPTCHA_KICK else 'мут 24ч'}."
    )

    # ── Пытаемся отправить в ЛС ──────────────────────────────────
    pm_msg_id  = 0
    pm_success = False
    try:
        if img_bytes:
            pm = await bot.send_photo(
                user_id,
                BufferedInputFile(img_bytes, "captcha.png"),
                caption=pm_caption, parse_mode="HTML",
            )
        else:
            pm = await bot.send_message(
                user_id,
                pm_caption + f"\n\n🔢 Код: <code>{code}</code>",
                parse_mode="HTML",
            )
        pm_msg_id  = pm.message_id
        pm_success = True
    except Exception as e:
        log.info(f"[CAPTCHA] ЛС недоступны для {user_id}: {e}")

    skip_cb = f"captcha_skip:{chat_id}:{user_id}"

    if pm_success:
        # ── РЕЖИМ ЛС: мутим, говорим "иди в личку" ──────────────
        mode = "pm"
        try:
            await bot.restrict_chat_member(
                chat_id, user_id,
                ChatPermissions(can_send_messages=False),
            )
        except Exception as e:
            log.warning(f"[CAPTCHA] restrict: {e}")

        me      = await bot.get_me()
        bot_url = f"https://t.me/{me.username}"
        group_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📩 Открыть диалог с ботом", url=bot_url),
            InlineKeyboardButton(text="✅ Пропустить (адм)", callback_data=skip_cb),
        ]])
        group_text = (
            f"👋 <b>{safe_name}</b>, добро пожаловать!\n\n"
            f"🔐 Для входа в чат тебе отправлена капча в личку.\n"
            f"➡️ Открой диалог с ботом и введи там цифры.\n"
            f"⏰ У тебя <b>{CAPTCHA_TIMEOUT} секунд</b>."
        )

    else:
        # ── РЕЖИМ ГРУППЫ: не мутим, пишем прямо в чат ───────────
        mode = "group"
        group_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Пропустить (адм)", callback_data=skip_cb),
        ]])
        group_text = (
            f"👋 <b>{safe_name}</b>, добро пожаловать!\n\n"
            f"🔐 Для входа реши капчу — напиши <b>{CAPTCHA_DIGITS} цифр</b> "
            f"прямо здесь в чат ответом на это сообщение.\n"
            f"⏰ У тебя <b>{CAPTCHA_TIMEOUT} секунд</b>.\n"
            f"❌ {CAPTCHA_MAX_TRIES} ошибки — {'кик' if CAPTCHA_KICK else 'мут'}.\n\n"
        )
        if img_bytes:
            group_text += "⬇️ Смотри картинку:"

    # Шлём сообщение в группу
    group_msg_id = 0
    try:
        if mode == "group" and img_bytes:
            gm = await bot.send_photo(
                chat_id,
                BufferedInputFile(img_bytes, "captcha.png"),
                caption=group_text, parse_mode="HTML",
                reply_markup=group_kb,
            )
        else:
            gm = await bot.send_message(
                chat_id, group_text, parse_mode="HTML",
                reply_markup=group_kb,
            )
        group_msg_id = gm.message_id
    except Exception as e:
        log.error(f"[CAPTCHA] group msg: {e}")

    # Запускаем таймер
    task = asyncio.create_task(_timer(bot, user_id))
    _active[user_id] = {
        "code":         code,
        "chat_id":      chat_id,
        "group_msg_id": group_msg_id,
        "pm_msg_id":    pm_msg_id,
        "task":         task,
        "tries":        0,
        "name":         safe_name,
        "mode":         mode,          # "pm" | "group"
    }
    log.info(f"[CAPTCHA] Запущена: {name} ({user_id}) mode={mode} чат={chat_id}")


# ── Обработчик ответа в ЛС ───────────────────────────────────────
async def _on_pm(message: Message):
    if message.chat.type != "private" or not message.from_user:
        return
    if not message.text or message.text.startswith("/"):
        return

    uid   = message.from_user.id
    entry = _active.get(uid)
    if not entry or entry["mode"] != "pm":
        return

    answer = message.text.strip()
    if answer == entry["code"]:
        entry["task"].cancel()
        _active.pop(uid, None)
        await _success(message.bot, uid, entry)
    else:
        entry["tries"] += 1
        remaining = CAPTCHA_MAX_TRIES - entry["tries"]
        if entry["tries"] >= CAPTCHA_MAX_TRIES:
            await _fail(message.bot, uid, entry)
        else:
            await message.answer(
                f"❌ Неверно! Осталось попыток: <b>{remaining}</b>",
                parse_mode="HTML",
            )


# ── Обработчик ответа в группе ───────────────────────────────────
async def _on_group(message: Message):
    if message.chat.type not in ("group", "supergroup") or not message.from_user:
        return
    if not message.text or message.text.startswith("/"):
        return

    uid   = message.from_user.id
    entry = _active.get(uid)
    if not entry or entry["mode"] != "group" or entry["chat_id"] != message.chat.id:
        return

    # Удаляем ответ пользователя из чата
    try: await message.delete()
    except Exception: pass

    answer = message.text.strip()
    if answer == entry["code"]:
        entry["task"].cancel()
        _active.pop(uid, None)
        await _success(message.bot, uid, entry)
    else:
        entry["tries"] += 1
        remaining = CAPTCHA_MAX_TRIES - entry["tries"]
        if entry["tries"] >= CAPTCHA_MAX_TRIES:
            await _fail(message.bot, uid, entry)
        else:
            try:
                w = await message.answer(
                    f"❌ <b>{entry['name']}</b>, неверно! "
                    f"Осталось попыток: <b>{remaining}</b>",
                    parse_mode="HTML",
                )
                await asyncio.sleep(5)
                try: await w.delete()
                except Exception: pass
            except Exception: pass


# ── Кнопка "Пропустить" ──────────────────────────────────────────
async def _on_skip(call: CallbackQuery):
    try:
        m  = await call.bot.get_chat_member(call.message.chat.id, call.from_user.id)
        ok = m.status in ("administrator", "creator")
    except Exception:
        ok = False
    if not ok:
        await call.answer("❌ Только администраторы", show_alert=True)
        return

    _, cid_s, uid_s = call.data.split(":")
    chat_id, user_id = int(cid_s), int(uid_s)

    entry = _active.pop(user_id, None)
    if not entry:
        await call.answer("Капча уже завершена")
        return

    entry["task"].cancel()

    if entry["mode"] == "pm":
        await _unlock(call.bot, chat_id, user_id)

    try: await call.message.delete()
    except Exception: pass

    try:
        await call.bot.send_message(
            user_id,
            f"✅ Администратор <b>{call.from_user.full_name}</b> "
            f"пропустил капчу — добро пожаловать!",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await call.answer("✅ Капча пропущена")

    try:
        n = await call.bot.send_message(
            chat_id,
            f"✅ Капча для <b>{entry['name']}</b> пропущена администратором.",
            parse_mode="HTML",
        )
        await asyncio.sleep(8)
        try: await n.delete()
        except Exception: pass
    except Exception:
        pass

    log.info(f"[CAPTCHA] Пропущена admin={call.from_user.id} user={user_id}")


# ── Регистрация ───────────────────────────────────────────────────
def setup(bot_instance: Bot, dp: Dispatcher):
    # _on_pm НЕ регистрируем — ответы в ЛС обрабатываются в handle_private_message в bot.py
    # чтобы избежать конфликта с другими ЛС-обработчиками
    dp.message.register(_on_group, F.chat.type.in_({"group","supergroup"}), F.text, ~F.text.startswith("/"))
    dp.callback_query.register(_on_skip, F.data.startswith("captcha_skip:"))
    log.info("[CAPTCHA] Зарегистрирован (группа-режим; ЛС — через bot.py)")


# ── Статус для дашборда ──────────────────────────────────────────
def get_active_count() -> int: return len(_active)
def get_active_list()  -> list:
    return [{"user_id": uid, "chat_id": e["chat_id"],
             "name": e["name"], "tries": e["tries"], "mode": e["mode"]}
            for uid, e in _active.items()]

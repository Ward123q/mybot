# captcha.py — Картинка-капча для Telegram бота (aiogram 3.x)
#
# ЛОГИКА:
#   1. Новый участник входит в чат
#   2. Бот мутит его в ГРУППЕ
#   3. В группу: кнопка "Пройти капчу в личке"
#   4. В ЛИЧКЕ бот присылает картинку с 5 цифрами
#   5. Пользователь отвечает В ЛИЧКЕ
#   6. Верно -> размут в группе
#   7. Таймаут / неверно -> кик
#
# УСТАНОВКА:
#   pip install Pillow
#
# ПОДКЛЮЧЕНИЕ В bot.py:
#   import captcha as cap
#   cap.setup(bot, dp)   # до start_polling
#   # в on_new_member:
#   await cap.start_captcha(bot, cid, member.id, member.full_name)

import asyncio
import io
import random
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, ChatPermissions,
    BufferedInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

log = logging.getLogger(__name__)

# =====================================================================
#  НАСТРОЙКИ
# =====================================================================
CAPTCHA_TIMEOUT   = 120
CAPTCHA_KICK      = True
CAPTCHA_DIGITS    = 5
CAPTCHA_WIDTH     = 280
CAPTCHA_HEIGHT    = 100
CAPTCHA_MAX_TRIES = 2
CAPTCHA_EXCLUDE_CHATS: set = set()

# =====================================================================
#  ХРАНИЛИЩЕ  { user_id: {code, chat_id, group_msg_id, pm_msg_id, task, tries, name} }
# =====================================================================
_active: dict = {}


# =====================================================================
#  ГЕНЕРАЦИЯ КАРТИНКИ
# =====================================================================
def _generate_image(code: str) -> bytes:
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
    except ImportError:
        raise RuntimeError("pip install Pillow")

    W, H = CAPTCHA_WIDTH, CAPTCHA_HEIGHT
    img  = Image.new("RGB", (W, H), (22, 22, 30))
    draw = ImageDraw.Draw(img)

    for _ in range(W * H // 5):
        c = random.randint(35, 75)
        draw.point((random.randint(0,W-1), random.randint(0,H-1)), (c, c, c+random.randint(0,25)))

    for _ in range(10):
        col = (random.randint(50,110), random.randint(50,110), random.randint(80,150))
        draw.line([(random.randint(0,W), random.randint(0,H)),
                   (random.randint(0,W), random.randint(0,H))], fill=col, width=1)

    for _ in range(5):
        col = (random.randint(60,120), random.randint(60,120), random.randint(100,170))
        x0,y0 = random.randint(-30,W//2), random.randint(-30,H//2)
        x1,y1 = random.randint(W//2,W+30), random.randint(H//2,H+30)
        draw.arc([x0,y0,x1,y1], random.randint(0,360), random.randint(0,360), fill=col, width=1)

    font_size = 52
    font = None
    for fp in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arialbd.ttf",
    ]:
        try:
            from PIL import ImageFont as _F; font = _F.truetype(fp, font_size); break
        except Exception: pass
    if font is None:
        try:
            from PIL import ImageFont as _F; font = _F.load_default(size=font_size)
        except Exception:
            from PIL import ImageFont as _F; font = _F.load_default()

    palette = [(255,210,80),(100,215,255),(255,120,120),(120,255,140),(210,120,255),(255,170,70)]
    char_w  = (W - 20) // len(code)
    for i, ch in enumerate(code):
        col    = palette[i % len(palette)]
        angle  = random.randint(-25, 25)
        offset = random.randint(-10, 10)
        x      = 10 + i * char_w + random.randint(-4, 4)
        layer  = Image.new("RGBA", (char_w+12, H), (0,0,0,0))
        ld     = ImageDraw.Draw(layer)
        cy     = (H - font_size) // 2 + offset
        ld.text((4, cy+2), ch, font=font, fill=(0,0,0,160))
        ld.text((2, cy),   ch, font=font, fill=col+(255,))
        rot = layer.rotate(angle, expand=False, resample=Image.BICUBIC)
        img.paste(rot, (x-2, 0), rot)

    from PIL import ImageFilter
    img  = img.filter(ImageFilter.GaussianBlur(radius=0.6))
    d2   = ImageDraw.Draw(img)
    for _ in range(200):
        c = random.randint(70,160)
        d2.point((random.randint(0,W-1), random.randint(0,H-1)), (c, c+5, c+15))

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# =====================================================================
#  ТАЙМЕР КИКА
# =====================================================================
async def _timeout_task(bot: Bot, user_id: int):
    await asyncio.sleep(CAPTCHA_TIMEOUT)
    entry = _active.pop(user_id, None)
    if not entry:
        return
    chat_id = entry["chat_id"]
    log.info(f"[CAPTCHA] Таймаут: {entry['name']} ({user_id})")
    for mid, cid in [(entry["pm_msg_id"], user_id), (entry["group_msg_id"], chat_id)]:
        try:
            if mid: await bot.delete_message(cid, mid)
        except Exception: pass
    await _punish(bot, chat_id, user_id)
    try:
        n = await bot.send_message(chat_id,
            f"⏰ <b>{entry['name']}</b> не прошёл капчу — "
            f"{'кик' if CAPTCHA_KICK else 'мут'}.", parse_mode="HTML")
        await asyncio.sleep(10)
        try: await n.delete()
        except Exception: pass
    except Exception: pass


async def _punish(bot: Bot, chat_id: int, user_id: int):
    try:
        if CAPTCHA_KICK:
            await bot.ban_chat_member(chat_id, user_id)
            await asyncio.sleep(0.3)
            await bot.unban_chat_member(chat_id, user_id)
        else:
            from datetime import timedelta
            await bot.restrict_chat_member(chat_id, user_id,
                ChatPermissions(can_send_messages=False),
                until_date=timedelta(hours=24))
    except Exception as e:
        log.warning(f"[CAPTCHA] punish: {e}")


async def _unlock(bot: Bot, chat_id: int, user_id: int):
    try:
        await bot.restrict_chat_member(chat_id, user_id,
            ChatPermissions(can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True, can_change_info=False,
                can_invite_users=True, can_pin_messages=False))
    except Exception as e:
        log.warning(f"[CAPTCHA] unlock: {e}")


# =====================================================================
#  ЗАПУСК КАПЧИ
# =====================================================================
async def start_captcha(bot: Bot, chat_id: int, user_id: int, name: str):
    if chat_id in CAPTCHA_EXCLUDE_CHATS or user_id in _active:
        return

    # Мутим
    try:
        await bot.restrict_chat_member(chat_id, user_id,
            ChatPermissions(can_send_messages=False))
    except Exception as e:
        log.warning(f"[CAPTCHA] restrict: {e}")

    code      = "".join(str(random.randint(0,9)) for _ in range(CAPTCHA_DIGITS))
    safe_name = name.replace("<","&lt;").replace(">","&gt;")
    me        = await bot.get_me()
    bot_link  = f"https://t.me/{me.username}?start=captcha_{user_id}"

    # Кнопки в группе
    group_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔐 Пройти капчу в личке →", url=bot_link),
        InlineKeyboardButton(text="✅ Пропустить (адм)",
                             callback_data=f"captcha_skip:{chat_id}:{user_id}"),
    ]])

    # Сообщение в группу
    group_msg_id = 0
    try:
        gm = await bot.send_message(chat_id,
            f"👋 <b>{safe_name}</b>, добро пожаловать!\n\n"
            f"🔐 Для входа пройди капчу — нажми кнопку и реши её в личке с ботом.\n"
            f"⏰ У тебя <b>{CAPTCHA_TIMEOUT} секунд</b>.",
            parse_mode="HTML", reply_markup=group_kb)
        group_msg_id = gm.message_id
    except Exception as e:
        log.error(f"[CAPTCHA] group msg: {e}")

    # Шлём капчу в ЛС
    pm_msg_id = 0
    try:
        img_bytes = _generate_image(code)
    except Exception:
        img_bytes = None

    pm_text = (
        f"🔐 <b>Капча для входа</b>\n\n"
        f"Введи <b>{CAPTCHA_DIGITS} цифр</b> с картинки одним сообщением.\n"
        f"⏰ У тебя <b>{CAPTCHA_TIMEOUT} секунд</b>.\n"
        f"❌ {CAPTCHA_MAX_TRIES} ошибки — {'кик' if CAPTCHA_KICK else 'мут 24ч'}."
    )
    try:
        if img_bytes:
            pm = await bot.send_photo(user_id,
                BufferedInputFile(img_bytes, "captcha.png"),
                caption=pm_text, parse_mode="HTML")
        else:
            pm = await bot.send_message(user_id,
                pm_text + f"\n\n🔢 Код: <code>{code}</code>", parse_mode="HTML")
        pm_msg_id = pm.message_id
    except Exception as e:
        # Пользователь ещё не написал боту — ЛС закрыты
        log.warning(f"[CAPTCHA] PM недоступен для {user_id}: {e}")
        try:
            await bot.edit_message_reply_markup(chat_id, group_msg_id,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="📩 Сначала напиши боту", url=f"https://t.me/{me.username}"),
                    InlineKeyboardButton(text="🔐 Потом нажми сюда →", url=bot_link),
                ],[
                    InlineKeyboardButton(text="✅ Пропустить (адм)",
                                         callback_data=f"captcha_skip:{chat_id}:{user_id}"),
                ]]))
        except Exception: pass

    task = asyncio.create_task(_timeout_task(bot, user_id))
    _active[user_id] = {
        "code": code, "chat_id": chat_id,
        "group_msg_id": group_msg_id, "pm_msg_id": pm_msg_id,
        "task": task, "tries": 0, "name": safe_name,
    }
    log.info(f"[CAPTCHA] Запущена: {name} ({user_id}) чат={chat_id}")


# =====================================================================
#  /start captcha_xxx — повторная отправка если ЛС были закрыты
# =====================================================================
async def _on_start(message: Message):
    if not message.text:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].startswith("captcha_"):
        return

    uid   = message.from_user.id
    entry = _active.get(uid)
    if not entry:
        await message.answer("✅ Капча уже не активна.")
        return
    if entry.get("pm_msg_id"):
        await message.answer("⬆️ Капча уже отправлена выше — введи цифры.")
        return

    # Генерируем и шлём
    try: img_bytes = _generate_image(entry["code"])
    except Exception: img_bytes = None

    pm_text = (
        f"🔐 <b>Капча для входа</b>\n\n"
        f"Введи <b>{CAPTCHA_DIGITS} цифр</b> с картинки.\n"
        f"⏰ Осталось ~<b>{CAPTCHA_TIMEOUT} секунд</b>.\n"
        f"❌ {CAPTCHA_MAX_TRIES} ошибки — {'кик' if CAPTCHA_KICK else 'мут'}."
    )
    try:
        if img_bytes:
            pm = await message.answer_photo(BufferedInputFile(img_bytes,"captcha.png"),
                caption=pm_text, parse_mode="HTML")
        else:
            pm = await message.answer(
                pm_text + f"\n\n🔢 Код: <code>{entry['code']}</code>", parse_mode="HTML")
        entry["pm_msg_id"] = pm.message_id
    except Exception as e:
        log.error(f"[CAPTCHA] retry PM: {e}")


# =====================================================================
#  ОТВЕТ ПОЛЬЗОВАТЕЛЯ — ТОЛЬКО В ЛС
# =====================================================================
async def _on_answer(message: Message):
    if message.chat.type != "private" or not message.from_user or not message.text:
        return
    if message.text.startswith("/"):
        return

    uid   = message.from_user.id
    entry = _active.get(uid)
    if not entry:
        return

    chat_id = entry["chat_id"]
    correct = entry["code"]

    if message.text.strip() == correct:
        # ВЕРНО
        _active.pop(uid, None)
        entry["task"].cancel()
        await _unlock(message.bot, chat_id, uid)

        for mid, cid in [(entry["pm_msg_id"], uid), (entry["group_msg_id"], chat_id)]:
            try:
                if mid: await message.bot.delete_message(cid, mid)
            except Exception: pass

        await message.answer("✅ <b>Верно!</b> Капча пройдена — добро пожаловать в чат 🎉",
                              parse_mode="HTML")
        try:
            n = await message.bot.send_message(chat_id,
                f"✅ <b>{entry['name']}</b> прошёл капчу!", parse_mode="HTML")
            await asyncio.sleep(8)
            try: await n.delete()
            except Exception: pass
        except Exception: pass
        log.info(f"[CAPTCHA] Успех: {entry['name']} ({uid})")

    else:
        # НЕВЕРНО
        entry["tries"] += 1
        remaining = CAPTCHA_MAX_TRIES - entry["tries"]
        log.info(f"[CAPTCHA] Неверно: {entry['name']} ({uid}) попытка {entry['tries']}")
        if entry["tries"] >= CAPTCHA_MAX_TRIES:
            _active.pop(uid, None)
            entry["task"].cancel()
            try:
                if entry["group_msg_id"]:
                    await message.bot.delete_message(chat_id, entry["group_msg_id"])
            except Exception: pass
            await message.answer(
                f"❌ Все попытки исчерпаны — {'кик' if CAPTCHA_KICK else 'мут на 24ч'}.",
                parse_mode="HTML")
            await _punish(message.bot, chat_id, uid)
            try:
                n = await message.bot.send_message(chat_id,
                    f"❌ <b>{entry['name']}</b> не прошёл капчу.", parse_mode="HTML")
                await asyncio.sleep(8)
                try: await n.delete()
                except Exception: pass
            except Exception: pass
        else:
            await message.answer(
                f"❌ Неверно! Осталось попыток: <b>{remaining}</b>", parse_mode="HTML")


# =====================================================================
#  КНОПКА ПРОПУСТИТЬ
# =====================================================================
async def _on_skip(call: CallbackQuery):
    try:
        m = await call.bot.get_chat_member(call.message.chat.id, call.from_user.id)
        ok = m.status in ("administrator","creator")
    except Exception: ok = False
    if not ok:
        await call.answer("❌ Только администраторы", show_alert=True); return

    _, cid_s, uid_s = call.data.split(":")
    chat_id, user_id = int(cid_s), int(uid_s)
    entry = _active.pop(user_id, None)
    if not entry:
        await call.answer("Капча уже завершена"); return

    entry["task"].cancel()
    await _unlock(call.bot, chat_id, user_id)
    try: await call.message.delete()
    except Exception: pass
    try:
        await call.bot.send_message(user_id,
            f"✅ Администратор <b>{call.from_user.full_name}</b> пропустил капчу — ты в чате!",
            parse_mode="HTML")
    except Exception: pass
    await call.answer("✅ Пропущено")
    try:
        n = await call.bot.send_message(chat_id,
            f"✅ Капча для <b>{entry['name']}</b> пропущена администратором.",
            parse_mode="HTML")
        await asyncio.sleep(8)
        try: await n.delete()
        except Exception: pass
    except Exception: pass
    log.info(f"[CAPTCHA] Пропущена admin={call.from_user.id} user={user_id}")


# =====================================================================
#  SETUP
# =====================================================================
def setup(bot_instance: Bot, dp: Dispatcher):
    dp.message.register(_on_start,  F.chat.type=="private", F.text.startswith("/start"))
    dp.message.register(_on_answer, F.chat.type=="private", F.text, ~F.text.startswith("/"))
    dp.callback_query.register(_on_skip, F.data.startswith("captcha_skip:"))
    log.info("[CAPTCHA] Зарегистрирован (ЛС-режим)")

def get_active_count() -> int: return len(_active)
def get_active_list() -> list:
    return [{"user_id":uid,"chat_id":e["chat_id"],"name":e["name"],"tries":e["tries"]}
            for uid,e in _active.items()]

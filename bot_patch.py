# ══════════════════════════════════════════════════════════════════
#  ПАТЧ 1: bot.py — добавить в начало файла (после import shared)
# ══════════════════════════════════════════════════════════════════

import antiraid
import night_mode


# ══════════════════════════════════════════════════════════════════
#  ПАТЧ 2: bot.py — заменить on_new_member (строка ~1961)
#
#  Найти:    @dp.message(F.new_chat_members)
#            async def on_new_member(message: Message):
#
#  В начало цикла for member in message.new_chat_members: добавить:
# ══════════════════════════════════════════════════════════════════

# ВСТАВИТЬ ПОСЛЕ: for member in message.new_chat_members:
#     if member.is_bot and AUTO_KICK_BOTS:
#         ...

# ─── АНТИРЕЙД + СКАМ ─────────────────────────────────────────────
# Добавить ПЕРЕД существующей логикой приветствия:
async def _antiraid_and_scam_check(message: Message, member) -> bool:
    """Проверка антирейда и скам-детектора. True = пользователь заблокирован."""
    return await antiraid.on_join(message, member)

# В on_new_member, в начало цикла добавить:
#
#   blocked = await antiraid.on_join(message, member)
#   if blocked:
#       continue


# ══════════════════════════════════════════════════════════════════
#  ПАТЧ 3: bot.py — в StatsMiddleware или обработчике сообщений
#  Найти функцию обработки входящих сообщений (StatsMiddleware)
#  и добавить фильтрацию ночного режима:
# ══════════════════════════════════════════════════════════════════

# В классе StatsMiddleware в методе __call__ добавить:
#
#   # Фильтр ночного режима
#   if isinstance(event, Message) and event.chat.type != "private":
#       deleted = await night_mode.filter_message(event)
#       if deleted:
#           return  # сообщение удалено, не обрабатываем дальше


# ══════════════════════════════════════════════════════════════════
#  ПАТЧ 4: bot.py — функция main(), добавить инициализацию
#  После: await notif.init(bot, dp)
# ══════════════════════════════════════════════════════════════════

# Добавить:
#   await antiraid.init(bot, dp, ADMIN_IDS, LOG_CHANNEL_ID)
#   await night_mode.init(bot)


# ══════════════════════════════════════════════════════════════════
#  ПАТЧ 5: on_new_member — полная замена начала цикла
# ══════════════════════════════════════════════════════════════════

"""
ИТОГОВЫЙ on_new_member (начало):

@dp.message(F.new_chat_members)
async def on_new_member(message: Message):
    cid = message.chat.id
    chat_cfg = cs.get_settings(cid)

    for member in message.new_chat_members:
        if member.is_bot and AUTO_KICK_BOTS:
            try:
                await bot.ban_chat_member(cid, member.id)
                await bot.unban_chat_member(cid, member.id)
                sent = await message.answer(
                    f"🤖 Бот <b>{member.full_name}</b> автоматически удалён.", parse_mode="HTML")
                await asyncio.sleep(5)
                try: await sent.delete()
                except: pass
            except: pass
            continue

        # ── НОВОЕ: Антирейд + Скам-детектор ──────────────────────
        blocked = await antiraid.on_join(message, member)
        if blocked:
            continue
        # ─────────────────────────────────────────────────────────

        # ... остальной код приветствия без изменений ...
"""

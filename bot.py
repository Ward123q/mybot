import asyncio
import logging
import random
import os
import aiohttp
from datetime import timedelta
from collections import defaultdict
from time import time
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, ChatPermissions, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiohttp import web
import json
from pathlib import Path

DATA_FILE = "data.json"

def load_data():
    global warnings, reputation, notes, xp_data, streaks, birthdays, afk_users
    global user_titles, mvp_votes, known_chats, referrals, referral_used
    global boosters, avatars, clans, clan_members, lottery_tickets, stock_invested
    global quotes_data, journal_data, artifacts, color_titles, role_of_day
    global rep_transfer_cooldown, confession_reactions, confession_voters
    if not Path(DATA_FILE).exists():
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)

        for cid, users in d.get("warnings", {}).items():
            for uid, count in users.items():
                warnings[int(cid)][int(uid)] = count
        for cid, users in d.get("reputation", {}).items():
            for uid, score in users.items():
                reputation[int(cid)][int(uid)] = score
        for cid, nts in d.get("notes", {}).items():
            notes[int(cid)] = nts
        for cid, users in d.get("xp_data", {}).items():
            for uid, xp in users.items():
                xp_data[int(cid)][int(uid)] = xp
        for cid, users in d.get("streaks", {}).items():
            for uid, s in users.items():
                streaks[int(cid)][int(uid)] = s
        for uid, bday in d.get("birthdays", {}).items():
            birthdays[int(uid)] = bday
        for uid, data in d.get("user_titles", {}).items():
            user_titles[int(uid)] = data
        for cid, title in d.get("known_chats", {}).items():
            known_chats[int(cid)] = title
        for uid, inv in d.get("referrals", {}).items():
            referrals[uid] = set(inv)
        referral_used.update(d.get("referral_used", {}))
        for uid, bdata in d.get("boosters", {}).items():
            boosters[uid] = bdata
        avatars.update(d.get("avatars", {}))
        clans.update(d.get("clans", {}))
        clan_members.update({int(k): v for k, v in d.get("clan_members", {}).items()})
        for cid, tickets in d.get("lottery_tickets", {}).items():
            lottery_tickets[int(cid)] = set(tickets)
        for cid, investors in d.get("stock_invested", {}).items():
            for uid, amt in investors.items():
                stock_invested[int(cid)][int(uid)] = amt
        for cid, qs in d.get("quotes_data", {}).items():
            quotes_data[int(cid)] = qs
        for uid, entries in d.get("journal_data", {}).items():
            journal_data[uid] = entries
        for uid, arts in d.get("artifacts", {}).items():
            artifacts[uid] = arts
        color_titles.update(d.get("color_titles", {}))
        role_of_day.update(d.get("role_of_day", {}))
        mvp_votes.update(d.get("mvp_votes", {}))
        rep_transfer_cooldown.update(d.get("rep_transfer_cooldown", {}))
        # Новые поля
        for cid, users in d.get("mod_history", {}).items():
            for uid, h in users.items():
                mod_history[int(cid)][int(uid)] = h
        for cid, users in d.get("user_notes", {}).items():
            for uid, n in users.items():
                user_notes[int(cid)][int(uid)] = n
        for cid, uids in d.get("ban_list", {}).items():
            ban_list[int(cid)] = set(uids)
        for cid, users in d.get("levels", {}).items():
            for uid, l in users.items():
                levels[int(cid)][int(uid)] = l
        for cid, users in d.get("chat_stats", {}).items():
            for uid, c in users.items():
                chat_stats[int(cid)][int(uid)] = c
        for cid, users in d.get("last_seen", {}).items():
            for uid, ts in users.items():
                last_seen[int(cid)][int(uid)] = ts
        for cid, users in d.get("user_activity", {}).items():
            for uid, days in users.items():
                user_activity[int(cid)][int(uid)] = defaultdict(int, days)
        for cid, v in d.get("crown_holders", {}).items():
            crown_holders[int(cid)] = v
    except Exception as e:
        print(f"[load_data error] {e}")


def save_data():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "warnings":     {str(cid): {str(uid): c for uid, c in users.items()} for cid, users in warnings.items()},
                "reputation":   {str(cid): {str(uid): s for uid, s in users.items()} for cid, users in reputation.items()},
                "notes":        {str(cid): nts for cid, nts in notes.items()},
                "xp_data":      {str(cid): {str(uid): x for uid, x in users.items()} for cid, users in xp_data.items()},
                "streaks":      {str(cid): {str(uid): s for uid, s in users.items()} for cid, users in streaks.items()},
                "birthdays":    {str(uid): b for uid, b in birthdays.items()},
                "user_titles":  {str(uid): d for uid, d in user_titles.items()},
                "known_chats":  {str(cid): t for cid, t in known_chats.items()},
                "referrals":    {uid: list(inv) for uid, inv in referrals.items()},
                "referral_used": referral_used,
                "boosters":     dict(boosters),
                "avatars":      avatars,
                "clans":        clans,
                "clan_members": {str(uid): cid for uid, cid in clan_members.items()},
                "lottery_tickets": {str(cid): list(t) for cid, t in lottery_tickets.items()},
                "stock_invested": {str(cid): {str(uid): a for uid, a in inv.items()} for cid, inv in stock_invested.items()},
                "quotes_data":  {str(cid): q for cid, q in quotes_data.items()},
                "journal_data": dict(journal_data),
                "artifacts":    dict(artifacts),
                "color_titles": color_titles,
                "role_of_day":  role_of_day,
                "mvp_votes":    mvp_votes,
                "rep_transfer_cooldown": rep_transfer_cooldown,
                # Новые поля
                "mod_history":  {str(cid): {str(uid): h for uid, h in users.items()} for cid, users in mod_history.items()},
                "user_notes":   {str(cid): {str(uid): n for uid, n in users.items()} for cid, users in user_notes.items()},
                "ban_list":     {str(cid): list(uids) for cid, uids in ban_list.items()},
                "levels":       {str(cid): {str(uid): l for uid, l in users.items()} for cid, users in levels.items()},
                "chat_stats":   {str(cid): {str(uid): c for uid, c in users.items()} for cid, users in chat_stats.items()},
                "last_seen":    {str(cid): {str(uid): ts for uid, ts in users.items()} for cid, users in last_seen.items()},
                "user_activity":{str(cid): {str(uid): dict(days) for uid, days in users.items()} for cid, users in user_activity.items()},
                "crown_holders":{str(cid): v for cid, v in crown_holders.items()},
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[save_data error] {e}")





# ══════════════════════════════════════════
#  СИСТЕМА УРОВНЕЙ (500 уровней)
# ══════════════════════════════════════════
LEVEL_TITLES = {
    # 1-10 Новички
    1:  ("🌱", "Росток"),        2:  ("🌿", "Новичок"),
    3:  ("🍃", "Участник"),      4:  ("🌾", "Активный"),
    5:  ("⚡", "Энергичный"),    6:  ("🔥", "Горячий"),
    7:  ("💫", "Звёздный"),      8:  ("🎯", "Меткий"),
    9:  ("🛡", "Защитник"),      10: ("⚔️", "Воин"),
    # 11-20
    11: ("🗡", "Дуэлянт"),       12: ("🏹", "Лучник"),
    13: ("🔮", "Мистик"),        14: ("🧙", "Маг"),
    15: ("🌙", "Лунный"),        16: ("☄️", "Кометный"),
    17: ("🌟", "Звезда"),        18: ("💎", "Бриллиант"),
    19: ("👁", "Всевидящий"),    20: ("🏆", "Чемпион"),
    # 21-30
    21: ("🦁", "Лев"),           22: ("🐉", "Дракон"),
    23: ("🦅", "Орёл"),          24: ("🌊", "Волна"),
    25: ("⚜️", "Элита"),         26: ("🔱", "Посейдон"),
    27: ("🌌", "Космос"),        28: ("🌠", "Метеор"),
    29: ("🎖", "Медаль"),        30: ("👑", "Король"),
    # 31-40
    31: ("🏰", "Замок"),         32: ("⚡", "Молния"),
    33: ("🔥", "Пламя"),         34: ("💀", "Тёмный"),
    35: ("🌑", "Тень"),          36: ("🌀", "Вихрь"),
    37: ("🎇", "Фейерверк"),     38: ("🧬", "Мутант"),
    39: ("🤖", "Киборг"),        40: ("🛸", "Инопланетянин"),
    # 41-50
    41: ("🌍", "Хранитель"),     42: ("🌞", "Солнечный"),
    43: ("🌈", "Радужный"),      44: ("⚗️", "Алхимик"),
    45: ("🔯", "Чародей"),       46: ("🌺", "Сакура"),
    47: ("🏯", "Сёгун"),         48: ("🐲", "Повелитель"),
    49: ("💠", "Абсолют"),       50: ("👾", "Полубог"),
    # 51-75
    51: ("🌋", "Вулкан"),        55: ("🏔", "Горный"),
    60: ("🌪", "Торнадо"),       65: ("🧊", "Ледяной"),
    70: ("🦄", "Единорог"),      75: ("🦊", "Лис"),
    # 76-100
    76: ("🐺", "Волк"),          80: ("🦋", "Бабочка"),
    85: ("🦂", "Скорпион"),      90: ("🐍", "Змей"),
    95: ("🦁", "Царь зверей"),
    100: ("🌠", "✨ СОТЫЙ ✨"),
    # 101-150
    105: ("💣", "Бомба"),        110: ("🚀", "Ракета"),
    115: ("🧠", "Гений"),        120: ("🎓", "Профессор"),
    125: ("💻", "Хакер"),        130: ("🕹", "Геймер"),
    135: ("🎬", "Режиссёр"),     140: ("🎸", "Рокер"),
    145: ("🥋", "Мастер"),
    150: ("💀", "✨ ПОЛТОРА СТА ✨"),
    # 151-200
    155: ("🔥", "Адское пламя"),  160: ("⚡", "Гром"),
    165: ("🌊", "Цунами"),        170: ("🌋", "Апокалипсис"),
    175: ("☄️", "Астероид"),      180: ("🌌", "Галактика"),
    185: ("🛸", "Вселенная"),     190: ("🌠", "Сверхновая"),
    195: ("💥", "Большой взрыв"),
    200: ("👁", "✨ ДВУХСОТЫЙ ✨"),
    # 201-250
    205: ("🌍", "Хранитель мира"), 210: ("⚜️", "Верховный"),
    215: ("🏆", "Великий"),        220: ("💎", "Алмазный"),
    225: ("👑", "Император"),      230: ("🐉", "Повелитель драконов"),
    235: ("🌞", "Бессмертный"),    240: ("💠", "Вечный"),
    245: ("🔯", "Всемогущий"),     249: ("👾", "Предел"),
    250: ("✨", "✨ ДВЕСТИ ПЯТЬДЕСЯТ ✨"),
    # 251-300
    255: ("🌑", "Тёмный бог"),     260: ("☠️", "Смерть"),
    265: ("👻", "Призрак"),        270: ("🔮", "Оракул"),
    275: ("🌀", "Бездна"),         280: ("🧿", "Провидец"),
    285: ("🌌", "Межзвёздный"),    290: ("⚫", "Чёрная дыра"),
    295: ("🌠", "Квазар"),
    300: ("🔱", "✨ ТРЁХСОТЫЙ ✨"),
    # 301-350
    305: ("🏯", "Сёгун II"),       310: ("🐲", "Дракон-бог"),
    315: ("⚔️", "Легендарный воин"), 320: ("🗡", "Тёмный клинок"),
    325: ("🛡", "Непоколебимый"),  330: ("🔥", "Феникс"),
    335: ("❄️", "Вечная мерзлота"), 340: ("⚡", "Повелитель молний"),
    345: ("🌊", "Нептун"),
    350: ("🌟", "✨ ТРИСТА ПЯТЬДЕСЯТ ✨"),
    # 351-400
    355: ("🌋", "Вулкан-бог"),     360: ("☄️", "Комета смерти"),
    365: ("🌌", "Повелитель космоса"), 370: ("💀", "Жнец"),
    375: ("👑", "Верховный король"), 380: ("🐉", "Первородный дракон"),
    385: ("🌞", "Бог солнца"),     390: ("🌑", "Бог тьмы"),
    395: ("💎", "Кристальный"),
    400: ("👾", "✨ ЧЕТЫРЁХСОТЫЙ ✨"),
    # 401-450
    405: ("🔯", "Архимаг II"),     410: ("🌠", "Астральный"),
    415: ("⚜️", "Высший"),         420: ("🏆", "Абсолютный чемпион"),
    425: ("💠", "Кристальный бог"), 430: ("🌈", "Спектр"),
    435: ("🌀", "Хаос"),           440: ("🔥", "Вечный огонь"),
    445: ("🌌", "Бесконечность"),
    450: ("✨", "✨ ЧЕТЫРЕСТА ПЯТЬДЕСЯТ ✨"),
    # 451-500
    455: ("💥", "Сингулярность"),  460: ("⚫", "Абсолютная тьма"),
    465: ("🌟", "Абсолютный свет"), 470: ("🐲", "Бог драконов"),
    475: ("👁", "Всевидящий бог"),  480: ("🔱", "Посейдон II"),
    485: ("👑", "Бог богов"),       490: ("🌌", "Создатель"),
    495: ("💠", "Источник"),        499: ("🌠", "Грань"),
    500: ("🆚", "⚡ БОГ ЧАТА ⚡"),
}

def get_level_title(level: int) -> tuple:
    result = ("🌱", "Участник")
    for lvl in sorted(LEVEL_TITLES.keys()):
        if level >= lvl:
            result = LEVEL_TITLES[lvl]
    return result

# XP для каждого уровня (нарастающая сложность)
LEVEL_XP = {}
xp = 0
for lvl in range(1, 501):
    if lvl <= 10:    xp += 80
    elif lvl <= 25:  xp += 150
    elif lvl <= 50:  xp += 300
    elif lvl <= 75:  xp += 500
    elif lvl <= 100: xp += 800
    elif lvl <= 150: xp += 1500
    elif lvl <= 200: xp += 2500
    elif lvl <= 250: xp += 4000
    elif lvl <= 300: xp += 6000
    elif lvl <= 350: xp += 8500
    elif lvl <= 400: xp += 12000
    elif lvl <= 450: xp += 17000
    else:            xp += 25000
    LEVEL_XP[lvl] = xp

def get_level(total_xp: int) -> int:
    level = 0
    for lvl, needed in LEVEL_XP.items():
        if total_xp >= needed:
            level = lvl
        else:
            break
    return level

def get_xp_for_next(level: int) -> int:
    return LEVEL_XP.get(level + 1, LEVEL_XP[500])



LOG_CHANNEL_ID   = -1003832428474
BOT_TOKEN        = os.getenv("BOT_TOKEN")
WEATHER_API_KEY  = os.getenv("WEATHER_API_KEY", "")
OWNER_ID         = 7823802800
MAX_WARNINGS     = 3
ANTI_MAT_ENABLED  = False

MAT_MUTE_MINUTES = 5
AUTO_KICK_BOTS   = True

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

warnings      = defaultdict(lambda: defaultdict(int))
notes         = defaultdict(dict)
mod_history   = defaultdict(lambda: defaultdict(list))  # {cid: {uid: [{"action":..., "reason":..., "by":..., "time":...}]}}
warn_expiry   = defaultdict(lambda: defaultdict(list))  # {cid: {uid: [expiry_timestamp, ...]}}
mute_timers   = {}  # {(cid, uid): task} — активные задачи автоснятия мута
ban_list      = defaultdict(dict)   # {cid: {uid: {"name":..., "reason":..., "by":..., "time":..., "until":...}}}
mod_reasons   = defaultdict(lambda: defaultdict(dict))  # {cid: {uid: {"mute":..., "ban":...}}}
tempban_timers = {}  # {(cid, uid): task}
afk_users     = {}
pending       = {}
chat_stats    = defaultdict(lambda: defaultdict(int))
reputation    = defaultdict(lambda: defaultdict(int))
rep_cooldown  = {}
user_notes       = defaultdict(dict)   # {cid: {uid: [notes]}}
report_queue     = defaultdict(list)   # {cid: [{reporter,target,text,ts,category,priority,anon,context,status,assigned_mod}]}
report_blocked   = set()               # {uid} — заблокированные от репортов
report_mod_votes = {}                  # {report_key: {mod_id: vote}} — голосование по репортам на админов
report_mod_stats = defaultdict(lambda: defaultdict(int))  # {cid: {mod_id: handled_count}}
report_score     = defaultdict(int)    # {uid: score} — доверие репортера
report_archive   = defaultdict(list)   # {cid: [закрытые репорты]}
silent_bans      = {}                  # {uid: True}
clown_targets    = {}                  # {cid_uid: expire_ts}
spy_targets      = {}                  # {cid_uid: owner_id}
mirror_chats     = {}                  # {cid: expire_ts}
magnet_targets   = {}                  # {cid_uid: expire_ts}
target_doubles   = {}                  # {cid_uid: expire_ts}
crown_holders    = {}                  # {cid: {uid, name, expire}}
last_seen        = defaultdict(dict)   # {cid: {uid: ts}}
user_activity    = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # {cid:{uid:{date:count}}}

rep_transfer_cooldown = {}   # {uid_cid: timestamp}
xp_cooldowns          = {}   # {cid_uid: timestamp} кулдаун XP
known_chats   = {}           # {cid: title} — все чаты где есть бот
role_of_day   = {}           # {cid: {uid: {date: role}}}
reminders     = {}
birthdays     = {}
levels        = defaultdict(lambda: defaultdict(int))
xp_data       = defaultdict(lambda: defaultdict(int))
streaks       = defaultdict(lambda: defaultdict(int))
streak_dates  = defaultdict(lambda: defaultdict(str))

# ══════════════════════════════════════════
#  🆕 НОВЫЕ ГЛОБАЛЬНЫЕ СТРУКТУРЫ
# ══════════════════════════════════════════
# Роли модераторов: {cid: {uid: "junior"/"senior"/"head"}}
mod_roles = defaultdict(dict)

# Права ролей
MOD_ROLE_PERMISSIONS = {
    "junior": {"warn", "mute", "kick", "report_handle"},
    "senior": {"warn", "mute", "kick", "ban", "report_handle", "clear", "notes"},
    "head":   {"warn", "mute", "kick", "ban", "unban", "report_handle", "clear",
               "notes", "announce", "lockdown", "tempban"},
}
MOD_ROLE_LABELS = {
    "junior": "🟢 Junior Mod",
    "senior": "🔵 Senior Mod",
    "head":   "🔴 Head Mod",
}

# Плагины: {cid: {plugin_name: bool}}
plugins = defaultdict(lambda: {
    "economy": True, "games": True, "xp": True,
    "antispam": True, "antimat": True, "reports": True,
    "events": True, "newspaper": True, "clans": True,
})

# Связанные чаты: {uid: [cid1, cid2, ...]} — бан везде
linked_chats_bans = {}  # глобальный список чатов владельца

# Апелляции: {uid: {cid, reason, ts, status}}
appeals = {}

# Расписание: [{cid, action, text, ts, repeat}]
scheduled_actions = []
# ИИ чат
# Расширенная статистика
hourly_stats  = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # {cid: {uid: {hour: count}}}
word_stats    = defaultdict(lambda: defaultdict(int))  # {cid: {word: count}}
daily_stats   = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # {cid: {uid: {date: count}}}
# Исповеди с реакциями
confession_reactions = defaultdict(lambda: defaultdict(int))  # {msg_id: {emoji: count}}
confession_voters    = defaultdict(set)   # {msg_id: {uid}}

RULES_TEXT = (
    "✨ <b>CHAT GUARD</b> — Правила чата\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    "🔞 <b>Контент 18+</b>\n"
    "▸ Запрещено отправлять любой контент для взрослых:\n"
    "стикеры, видео, гифки, ссылки и сцены насилия.\n\n"

    "🥴 <b>Наркотики</b>\n"
    "▸ Запрещено отправлять или распространять\n"
    "любой контент, связанный с наркотиками.\n\n"

    "📢 <b>Реклама</b>\n"
    "▸ Запрещена реклама социальных сетей, брендов,\n"
    "услуг, проектов и любой рекламной деятельности.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    "⚠️ <b>Наказания:</b>\n"
    "▸ Первое нарушение — предупреждение (варн)\n"
    "▸ Повторное нарушение — мут или бан\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "<i>Соблюдай правила — чат будет комфортным для всех 🤝</i>\n"
)

MUTE_MESSAGES = [
    "🔇 {name} заглушён на {time}. Тишина — это сила.",
    "🧊 {name} заморожен на {time}. Остынь немного.",
    "🌑 {name} ушёл в тень на {time}. До встречи.",
    "⛓ {name} скован на {time}. Причина понятна.",
]
BAN_MESSAGES = [
    "🔨 {name} разлетелся в пыль. Причина: {reason}",
    "🚀 {name} запущен в открытый космос. Причина: {reason}",
    "🌊 {name} смыт волной. Причина: {reason}",
    "🗡 {name} пал в бою. Причина: {reason}",
]
WARN_MESSAGES = [
    "⚡ {name} — удар молнии #{count}/{max}. Причина: {reason}.",
    "🌪 {name} — предупреждение {count}/{max}. Причина: {reason}. Следи за собой.",
    "🔮 {name} — {count}/{max} знаков судьбы. Причина: {reason}.",
]
AUTOBAN_MESSAGES = [
    "💀 {name} собрал {max} ударов и исчез навсегда.",
    "🌋 {name} — {max} предупреждений. Извержение неизбежно. Бан.",
]
RANDOM_BAN_REASONS = [
    "слишком умный", "подозрение в адекватности", "нарушение закона бутерброда",
    "превышение лимита здравого смысла", "нарушение пространственно-временного континуума",
    "слишком много смайликов",
]
QUOTES = [
    "«Не важно, как медленно ты идёшь, главное — не останавливаться.» — Конфуций",
    "«Будь изменением, которое хочешь видеть в мире.» — Ганди",
    "«Будь собой. Все остальные роли заняты.» — Уайльд",
    "«Единственный способ делать великое — любить то, что делаешь.» — Джобс",
    "«Успех — идти от неудачи к неудаче без потери энтузиазма.» — Черчилль",
    "«Делай что должен, и будь что будет.» — Толстой",
]
BALL_ANSWERS = [
    "🌟 Определённо да!", "🔥 Без сомнений!", "🌈 Скорее всего да.",
    "🌫 Трудно сказать.", "⏳ Спроси потом.", "🌀 Пока неясно.",
    "🌑 хз", "❄️ Нет.", "🪨 Определённо нет",
]
TRUTH_QUESTIONS = [
    "Ты когда-нибудь врал другу?", "Какой твой самый большой страх?",
    "Ты когда-нибудь влюблялся в друга?", "Что тебя раздражает больше всего?",
    "Твой самый неловкий момент в жизни?", "Ты когда-нибудь плакал из-за фильма?",
    "Что ты никогда не расскажешь родителям?", "ты когда нибудь писял под кровать?",
    "ты бы хотел себе ту самую девушку найти?", "ты бы хотел завести детей в будущем?",
]
DARE_CHALLENGES = [
    "Напиши комплимент случайному участнику чата!", "Признайся в чём-нибудь стыдном...",
    "Напиши стих про себя прямо сейчас.", "Расскажи смешной случай из жизни.",
    "Придумай и напиши анекдот прямо сейчас.", "сделай 10 приседаний если не лень",
]
WOULD_YOU_RATHER = [
    "Быть богатым но одиноким или бедным но счастливым?",
    "Уметь летать или быть невидимым?", "Знать будущее или изменить прошлое?",
    "Говорить только правду или постоянно врать?",
    "Жить 200 лет в бедности или 50 лет в богатстве?",
]
HOROSCOPES = {
    "♈ Овен":     "Сегодня звёзды говорят — делай что хочешь, но с умом.",
    "♉ Телец":    "День для отдыха. Полежи, поешь, снова полежи.",
    "♊ Близнецы": "Раздвоение личности сегодня — твоя суперсила.",
    "♋ Рак":      "Спрячься в домик. Там лучше. Там печеньки.",
    "♌ Лев":      "Ты красивый и все это знают. Используй по полной.",
    "♍ Дева":     "Разложи всё по полочкам. Буквально все полочки.",
    "♎ Весы":     "Не можешь выбрать что поесть? Это карма.",
    "♏ Скорпион": "Таинственность — твоё оружие. Молчи и улыбайся.",
    "♐ Стрелец":  "Стреляй в мечты! Может и попадёшь.",
    "♑ Козерог":  "Работай. Работай ещё. Потом отдохнёшь на пенсии.",
    "♒ Водолей":  "Ты уникальный. Как и все остальные.",
    "♓ Рыбы":     "Плыви по течению. Или против. Главное — плыви.",
}
COMPLIMENTS = [
    "Ты просто огонь! 🔥", "С тобой в чате теплее! ☀️",
    "Ты делаешь этот чат лучше! 🌟", "Без тебя тут было бы скучнее! 🎭",
    "Ты как редкий метеорит — очень ценный! 🌠", "Интеллект зашкаливает! 🧬",
]
PREDICTIONS = [
    "🔮 Сегодня тебе повезёт! Только не трать деньги.",
    "🌩 Осторожно с незнакомцами сегодня.",
    "🌟 Звёзды говорят — ты красавчик!", "🍀 Удача на твоей стороне!",
    "🐉 Тебя ждёт необычная встреча. Возможно с котом.",
    "🛋 Лучший план на сегодня — поспать. Доверяй процессу.",
]

async def health(request):
    return web.Response(text="OK")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()

async def check_admin(message: Message) -> bool:
    if message.from_user.id == OWNER_ID:
        return True
    try:
        m = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return m.status in ("administrator", "creator")
    except:
        return False

async def is_admin_by_id(chat_id: int, user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except:
        return False

async def require_admin(message: Message) -> bool:
    if not await check_admin(message):
        await reply_auto_delete(message, "🚫 <b>Только для администраторов!</b>", parse_mode="HTML")
        return False
    return True

def parse_duration(arg: str):
    arg = arg.strip().lower()
    try:
        if arg.endswith("m"):   return int(arg[:-1]), f"{int(arg[:-1])} мин."
        elif arg.endswith("h"): return int(arg[:-1])*60, f"{int(arg[:-1])} ч."
        elif arg.endswith("d"): return int(arg[:-1])*1440, f"{int(arg[:-1])} дн."
        elif arg.isdigit():     return int(arg), f"{int(arg)} мин."
    except:
        pass
    return None, None

async def log_action(text: str):
    try:
        await bot.send_message(LOG_CHANNEL_ID, text, parse_mode="HTML")
    except:
        pass

AUTO_DELETE_DELAY = 30  # секунд

async def auto_delete(*msgs):
    """Удаляет сообщения через AUTO_DELETE_DELAY секунд"""
    await asyncio.sleep(AUTO_DELETE_DELAY)
    for m in msgs:
        try:
            await m.delete()
        except:
            pass

async def schedule_delete(msg, delay: int = AUTO_DELETE_DELAY):
    """Удаляет одно сообщение через delay секунд"""
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass


async def answer_auto_delete(message: Message, text: str, **kwargs) -> Message:
    """Отправляет сообщение и удаляет через 30 секунд (без удаления исходного)"""
    sent = await message.answer(text, **kwargs)
    asyncio.create_task(schedule_delete(sent))
    return sent

async def cb_auto_delete(call: CallbackQuery, text: str, **kwargs):
    """Редактирует сообщение колбэка и удаляет через 30 секунд"""
    try:
        sent = await call.message.edit_text(text, **kwargs)
        asyncio.create_task(auto_delete(call.message))
    except:
        sent = await call.message.answer(text, **kwargs)
        asyncio.create_task(auto_delete(sent))
    return sent

async def reply_auto_delete(message: Message, text: str, **kwargs) -> Message:
    """Отвечает на сообщение и удаляет оба через 30 секунд"""
    sent = await message.reply(text, **kwargs)
    asyncio.create_task(auto_delete(message, sent))
    return sent

def add_mod_history(cid: int, uid: int, action: str, reason: str, by_name: str):
    """Записывает действие модератора в историю пользователя"""
    from datetime import datetime
    mod_history[cid][uid].append({
        "action": action,
        "reason": reason,
        "by": by_name,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M")
    })
    if len(mod_history[cid][uid]) > 20:
        mod_history[cid][uid] = mod_history[cid][uid][-20:]
    # 👮 Считаем статистику модератора
    mod_stats[cid][by_name] += 1

WARN_EXPIRY_DAYS = 30  # варн сгорает через 30 дней

def add_warn_with_expiry(cid: int, uid: int):
    """Добавляет варн с временем истечения"""
    expiry = time() + WARN_EXPIRY_DAYS * 86400
    warn_expiry[cid][uid].append(expiry)
    warnings[cid][uid] = len(warn_expiry[cid][uid])

def clean_expired_warns(cid: int, uid: int):
    """Удаляет истёкшие варны"""
    now = time()
    warn_expiry[cid][uid] = [e for e in warn_expiry[cid][uid] if e > now]
    warnings[cid][uid] = len(warn_expiry[cid][uid])

async def auto_unmute(cid: int, uid: int, mins: int, uname: str):
    """Автоматически снимает мут через указанное время"""
    await asyncio.sleep(mins * 60)
    try:
        await bot.restrict_chat_member(
            cid, uid,
            permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True))
        await bot.send_message(
            cid,
            f"🔊 Мут с <b>{uname}</b> снят автоматически.",
            parse_mode="HTML")
        await log_action(
            f"🔄 <b>АВТОРАЗМУТ</b>\n"
            f"👤 <b>{uname}</b>\n"
            f"⏱ Время мута истекло автоматически")
    except:
        pass
    finally:
        mute_timers.pop((cid, uid), None)

def schedule_unmute(cid: int, uid: int, mins: int, uname: str):
    """Запускает задачу автоснятия мута"""
    key = (cid, uid)
    old = mute_timers.get(key)
    if old:
        old.cancel()
    task = asyncio.create_task(auto_unmute(cid, uid, mins, uname))
    mute_timers[key] = task

async def log_violation_screenshot(cid: int, uid: int, uname: str, msg_text: str,
                                    action: str, reason: str, by_name: str, chat_title: str):
    """Сохраняет текст сообщения-нарушения в лог"""
    from datetime import datetime
    preview = msg_text[:300] + ("…" if len(msg_text) > 300 else "")
    await log_action(
        f"╔═══════════════════╗\n"
        f"📸  <b>СКРИНШОТ НАРУШЕНИЯ</b>\n"
        f"╚═══════════════════╝\n\n"
        f"👤 <b>Нарушитель:</b> {uname} (<code>{uid}</code>)\n"
        f"⚖️ <b>Действие:</b> {action}\n"
        f"📝 <b>Причина:</b> {reason}\n"
        f"👮 <b>Модератор:</b> {by_name}\n"
        f"💬 <b>Чат:</b> {chat_title}\n"
        f"🕐 <b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"💬 <b>Текст сообщения:</b>\n<code>{preview}</code>"
    )

async def dm_warn_user(uid: int, uname: str, reason: str, chat_title: str,
                       action: str, by_name: str):
    """Отправляет личное предупреждение нарушителю в лс"""
    try:
        await bot.send_message(
            uid,
            f"╔═══════════════════╗\n"
            f"⚠️  <b>ПРЕДУПРЕЖДЕНИЕ</b>\n"
            f"╚═══════════════════╝\n\n"
            f"💬 <b>Чат:</b> {chat_title}\n"
            f"⚖️ <b>Действие:</b> {action}\n"
            f"📝 <b>Причина:</b> {reason}\n"
            f"👮 <b>Модератор:</b> {by_name}\n\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"⚡ Пожалуйста, соблюдай правила чата!",
            parse_mode="HTML")
        return True
    except:
        return False

async def get_weather(city: str) -> str:
    if not WEATHER_API_KEY:
        return "🌧 Weather API ключ не настроен."
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": WEATHER_API_KEY, "units": "metric", "lang": "ru"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                d = await r.json()
                if d.get("cod") != 200: return f"🌧 Город <b>{city}</b> не найден."
                temp  = round(d["main"]["temp"]); feels = round(d["main"]["feels_like"])
                desc  = d["weather"][0]["description"].capitalize()
                humid = d["main"]["humidity"]; wind = round(d["wind"]["speed"])
                dl    = desc.lower()
                emoji = "☀️" if "ясно" in dl else ("🌧" if "дождь" in dl else ("❄️" if "снег" in dl else "⛅"))
                return (f"{emoji} <b>Погода в {d['name']}</b>\n\n"
                        f"🌡 Температура: <b>{temp}°C</b> (ощущается {feels}°C)\n"
                        f"📋 {desc}\n💧 Влажность: <b>{humid}%</b>\n🌬 Ветер: <b>{wind} м/с</b>")
    except:
        return "⛈ Ошибка при получении погоды."

def kb_back(tid: int) -> list:
    return [InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{tid}")]

def kb_main_menu(tid: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Участник",       callback_data=f"panel:select:{tid}"),
         InlineKeyboardButton(text="✉️ Сообщения",      callback_data=f"panel:messages:{tid}")],
        [InlineKeyboardButton(text="👥 Участники",      callback_data=f"panel:members:{tid}"),
         InlineKeyboardButton(text="⚙️ Настройки",     callback_data=f"panel:chat:{tid}")],
        [InlineKeyboardButton(text="🎮 Игры",           callback_data=f"panel:games:{tid}"),
         InlineKeyboardButton(text="📊 Статистика",     callback_data=f"panel:botstats2:{tid}")],
        [InlineKeyboardButton(text="🚨 Жалобы",         callback_data=f"panel:reports:{tid}"),
         InlineKeyboardButton(text="📈 Экономика",      callback_data=f"panel:economy:{tid}")],
        [InlineKeyboardButton(text="🏆 Топ XP",         callback_data=f"panel:topxp:{tid}"),
         InlineKeyboardButton(text="📋 Список банов",   callback_data=f"members:banlist:{tid}")],
        [InlineKeyboardButton(text="✖️ Закрыть",        callback_data="panel:close:0")],
    ])

def kb_user_panel(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔇 Мут",              callback_data=f"panel:mute:{tid}"),
         InlineKeyboardButton(text="🔊 Размут",            callback_data=f"panel:unmute:{tid}")],
        [InlineKeyboardButton(text="⚡ Варн",              callback_data=f"panel:warn:{tid}"),
         InlineKeyboardButton(text="🌿 Снять варн",        callback_data=f"panel:unwarn:{tid}")],
        [InlineKeyboardButton(text="🔨 Бан",               callback_data=f"panel:ban:{tid}"),
         InlineKeyboardButton(text="🕊 Разбан",             callback_data=f"panel:unban:{tid}")],
        [InlineKeyboardButton(text="🔕 Тихий бан",         callback_data=f"panel:silentban:{tid}"),
         InlineKeyboardButton(text="⏳ Темпбан 24ч",       callback_data=f"ban:{tid}:tempban24")],
        [InlineKeyboardButton(text="📵 Мут 24ч (реклама)", callback_data=f"members:warn24:{tid}"),
         InlineKeyboardButton(text="🗑 Удалить сообщ",     callback_data=f"panel:del:{tid}")],
        [InlineKeyboardButton(text="🔍 Информация",        callback_data=f"panel:info:{tid}"),
         InlineKeyboardButton(text="📋 История",            callback_data=f"panel:modhistory:{tid}")],
        [InlineKeyboardButton(text="📝 Заметки",           callback_data=f"panel:usernotes:{tid}"),
         InlineKeyboardButton(text="🏷 Выдать тег",        callback_data=f"panel:promote:{tid}")],
        [InlineKeyboardButton(text="🎭 Приколы",           callback_data=f"panel:fun:{tid}"),
         InlineKeyboardButton(text="◀️ Назад",             callback_data=f"panel:mainmenu:0")],
    ])

def kb_mute(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин",    callback_data=f"mute:{tid}:5"),
         InlineKeyboardButton(text="15 мин",   callback_data=f"mute:{tid}:15"),
         InlineKeyboardButton(text="30 мин",   callback_data=f"mute:{tid}:30")],
        [InlineKeyboardButton(text="1 час",    callback_data=f"mute:{tid}:60"),
         InlineKeyboardButton(text="3 часа",   callback_data=f"mute:{tid}:180"),
         InlineKeyboardButton(text="12 часов", callback_data=f"mute:{tid}:720")],
        [InlineKeyboardButton(text="1 день",   callback_data=f"mute:{tid}:1440"),
         InlineKeyboardButton(text="7 дней",   callback_data=f"mute:{tid}:10080"),
         InlineKeyboardButton(text="✏️ Своё",  callback_data=f"mute:{tid}:custom")],
        kb_back(tid),
    ])

def kb_warn(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤬 Мат",           callback_data=f"warn:{tid}:Мат в чате"),
         InlineKeyboardButton(text="📨 Спам",           callback_data=f"warn:{tid}:Спам")],
        [InlineKeyboardButton(text="😡 Оскорбление",    callback_data=f"warn:{tid}:Оскорбление"),
         InlineKeyboardButton(text="🌊 Флуд",           callback_data=f"warn:{tid}:Флуд")],
        [InlineKeyboardButton(text="📣 Реклама",        callback_data=f"warn:{tid}:Реклама"),
         InlineKeyboardButton(text="🔞 Контент 18+",    callback_data=f"warn:{tid}:Контент 18+")],
        [InlineKeyboardButton(text="🚫 Провокация",     callback_data=f"warn:{tid}:Провокация"),
         InlineKeyboardButton(text="✏️ Своя причина",   callback_data=f"warn:{tid}:custom")],
        kb_back(tid),
    ])

def kb_ban(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Грубые нарушения", callback_data=f"ban:{tid}:Грубые нарушения правил"),
         InlineKeyboardButton(text="📣 Спам/реклама",      callback_data=f"ban:{tid}:Спам и реклама")],
        [InlineKeyboardButton(text="🔞 Контент 18+",       callback_data=f"ban:{tid}:Контент 18+"),
         InlineKeyboardButton(text="🤖 Бот/накрутка",      callback_data=f"ban:{tid}:Бот или накрутка")],
        [InlineKeyboardButton(text="⏰ Бан на 24 часа",    callback_data=f"ban:{tid}:tempban24"),
         InlineKeyboardButton(text="⏰ Бан на 7 дней",     callback_data=f"ban:{tid}:tempban168")],
        [InlineKeyboardButton(text="✏️ Своя причина",      callback_data=f"ban:{tid}:custom")],
        kb_back(tid),
    ])

def kb_fun(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Шуточный бан",     callback_data=f"fun:rban:{tid}"),
         InlineKeyboardButton(text="🔮 Предсказание",      callback_data=f"fun:predict:{tid}")],
        [InlineKeyboardButton(text="🌌 Гороскоп",          callback_data=f"fun:horoscope:{tid}"),
         InlineKeyboardButton(text="🌸 Комплимент",         callback_data=f"fun:compliment:{tid}")],
        kb_back(tid),
    ])

def kb_messages(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Закрепить",          callback_data=f"msg:pin:{tid}"),
         InlineKeyboardButton(text="📍 Открепить",          callback_data=f"msg:unpin:{tid}")],
        [InlineKeyboardButton(text="🗑 Удалить сообщение",  callback_data=f"msg:del:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 10",        callback_data=f"msg:clear10:{tid}")],
        [InlineKeyboardButton(text="🧹 Очистить 20",        callback_data=f"msg:clear20:{tid}"),
         InlineKeyboardButton(text="🧹 Очистить 50",        callback_data=f"msg:clear50:{tid}")],
        [InlineKeyboardButton(text="📢 Объявление",          callback_data=f"msg:announce:{tid}"),
         InlineKeyboardButton(text="📊 Голосование",         callback_data=f"msg:poll:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_members(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮 Список админов",      callback_data=f"members:adminlist:{tid}"),
         InlineKeyboardButton(text="🏆 Топ активности",      callback_data=f"members:top:{tid}")],
        [InlineKeyboardButton(text="📈 Топ XP",              callback_data=f"members:topxp:{tid}"),
         InlineKeyboardButton(text="📊 Топ МВП",             callback_data=f"members:mvpstats:{tid}")],
        [InlineKeyboardButton(text="📵 Мут 24ч реклама",     callback_data=f"members:warn24:{tid}"),
         InlineKeyboardButton(text="⚠️ Варны участника",     callback_data=f"members:warninfo:{tid}")],
        [InlineKeyboardButton(text="📋 Список банов",        callback_data=f"members:banlist:{tid}"),
         InlineKeyboardButton(text="📊 Отчёт модератора",    callback_data=f"members:modreport:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

def kb_chat(tid: int) -> InlineKeyboardMarkup:
    ms = "✅" if ANTI_MAT_ENABLED else "❌"
    ks = "✅" if AUTO_KICK_BOTS   else "❌"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Заблокировать чат",   callback_data=f"chat:lock:{tid}"),
         InlineKeyboardButton(text="🔓 Разблокировать чат",  callback_data=f"chat:unlock:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 10с",        callback_data=f"chat:slow:10:{tid}"),
         InlineKeyboardButton(text="🐢 Slowmode 30с",        callback_data=f"chat:slow:30:{tid}")],
        [InlineKeyboardButton(text="🐢 Slowmode 60с",        callback_data=f"chat:slow:60:{tid}"),
         InlineKeyboardButton(text="🐇 Выкл slowmode",       callback_data=f"chat:slow:0:{tid}")],
        [InlineKeyboardButton(text=f"🧼 Антимат {ms}",       callback_data=f"chat:antimat:{tid}"),
         InlineKeyboardButton(text=f"🤖 Автокик {ks}",       callback_data=f"chat:autokick:{tid}")],
        [InlineKeyboardButton(text="📜 Правила чата",         callback_data=f"chat:rules:{tid}"),
         InlineKeyboardButton(text="📈 Статистика бота",      callback_data=f"chat:botstats:{tid}")],
        [InlineKeyboardButton(text="🎪 Турнир старт",         callback_data=f"chat:tournament_start:{tid}"),
         InlineKeyboardButton(text="🏁 Турнир стоп",          callback_data=f"chat:tournament_stop:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",               callback_data=f"panel:mainmenu:0")],
    ])

def kb_games(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик d6",           callback_data=f"game:roll:{tid}"),
         InlineKeyboardButton(text="🪙 Монетка",            callback_data=f"game:flip:{tid}")],
        [InlineKeyboardButton(text="🎱 Шар предсказаний",   callback_data=f"game:8ball:{tid}"),
         InlineKeyboardButton(text="🧩 Викторина",          callback_data=f"game:trivia:{tid}")],
        [InlineKeyboardButton(text="✊ КНБ — Камень",       callback_data=f"game:rps_k:{tid}"),
         InlineKeyboardButton(text="✌️ КНБ — Ножницы",      callback_data=f"game:rps_n:{tid}")],
        [InlineKeyboardButton(text="🖐 КНБ — Бумага",       callback_data=f"game:rps_b:{tid}"),
         InlineKeyboardButton(text="🎯 Угадай число",        callback_data=f"game:guess:{tid}")],
        [InlineKeyboardButton(text="🌤 Погода — Москва",    callback_data=f"game:weather_Москва:{tid}"),
         InlineKeyboardButton(text="🌍 Свой город",          callback_data=f"game:weather_custom:{tid}")],
        [InlineKeyboardButton(text="⏱ Отсчёт 5 сек",       callback_data=f"game:countdown5:{tid}"),
         InlineKeyboardButton(text="⏱ Отсчёт 10 сек",      callback_data=f"game:countdown10:{tid}")],
        [InlineKeyboardButton(text="🔙 Назад",              callback_data=f"panel:mainmenu:0")],
    ])

message_cache = {}
user_msg_ids  = defaultdict(lambda: defaultdict(list))  # {cid: {uid: [(msg_id, ts)]}}

class StatsMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group","supergroup"):
            chat_stats[event.chat.id][event.from_user.id] += 1
            known_chats[event.chat.id] = event.chat.title or str(event.chat.id)
            uid, cid = event.from_user.id, event.chat.id
            from datetime import datetime, timedelta
            import time as _time
            now_dt = datetime.now()
            today  = now_dt.strftime("%d.%m.%Y")

            # ── Кулдаун XP: 1 раз в минуту ──
            xp_cd_key = f"{cid}_{uid}"
            now_ts = _time.time()
            if now_ts - xp_cooldowns.get(xp_cd_key, 0) >= 60:
                xp_cooldowns[xp_cd_key] = now_ts
                text = event.text or ""
                length = len(text)
                if length == 0:         _xp = 1
                elif length < 10:       _xp = random.randint(1, 3)
                elif length < 50:       _xp = random.randint(3, 6)
                elif length < 150:      _xp = random.randint(5, 10)
                else:                   _xp = random.randint(8, 15)
                streak = streaks[cid].get(uid, 0)
                if streak >= 30:        _xp = int(_xp * 2.0)
                elif streak >= 14:      _xp = int(_xp * 1.5)
                elif streak >= 7:       _xp = int(_xp * 1.25)
                if now_dt.weekday() >= 5: _xp = int(_xp * 2)
                ev = current_event.get(cid)
                import time as _t2
                if ev and _t2.time() < ev.get("end_ts", 0):
                    _xp = int(_xp * ev.get("multiplier", 1))
                uid_str = str(uid)
                b = boosters.get(uid_str, {})
                if b.get("b1", 0) > now_ts or b.get("b4", 0) > now_ts: _xp = int(_xp * 2)
                xp_data[cid][uid] += _xp

            # ── Стрик ──
            last = streak_dates[cid][uid]
            if last != today:
                yesterday = (now_dt - timedelta(days=1)).strftime("%d.%m.%Y")
                streaks[cid][uid] = streaks[cid].get(uid, 0) + 1 if last == yesterday else 1
                streak_dates[cid][uid] = today

            # ── Уровень ──
            old_level = levels[cid].get(uid, 0)
            new_level = get_level(xp_data[cid][uid])
            if new_level > old_level:
                levels[cid][uid] = new_level
                emoji, title = get_level_title(new_level)
                bonus_rep = new_level * 5
                reputation[cid][uid] = reputation[cid].get(uid, 0) + bonus_rep
                try:
                    await event.answer(
                        f"🎉 {event.from_user.mention_html()} достиг <b>{new_level} уровня</b>!\n"
                        f"{emoji} Титул: <b>{title}</b>\n"
                        f"💰 Бонус: <b>+{bonus_rep} репы</b>", parse_mode="HTML")
                except: pass
            if event.text:
                message_cache[event.message_id] = {
                    "text": event.text, "user": event.from_user.full_name,
                    "user_id": event.from_user.id, "chat_id": event.chat.id,
                    "chat_title": event.chat.title,
                }
                # Запомнить ID сообщения для чистки
                import time as _tc
                user_msg_ids[cid][uid].append((event.message_id, _tc.time()))
                # Хранить только последние 500 сообщений на юзера
                if len(user_msg_ids[cid][uid]) > 500:
                    user_msg_ids[cid][uid] = user_msg_ids[cid][uid][-500:]
                # last_seen и активность по дням
                last_seen[cid][uid] = _tc.time()
                user_activity[cid][uid][today] += 1
                # Расширенная статистика
                from datetime import datetime
                hour = datetime.now().hour
                hourly_stats[cid][uid][hour] += 1
                daily_stats[cid][uid][datetime.now().strftime("%d.%m.%Y")] += 1
                for word in event.text.lower().split():
                    if len(word) > 3:
                        word_stats[cid][word] += 1
        return await handler(event, data)

class SpecialEffectsMiddleware(BaseMiddleware):
    """Клоун, зеркало, слежка, магнит"""
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group","supergroup"):
            if not event.new_chat_members and not event.left_chat_member:
                import time as _t; now = _t.time()
                uid, cid = event.from_user.id, event.chat.id
                key = f"{cid}_{uid}"
                # 🤡 Клоун
                if clown_targets.get(key, 0) > now:
                    try:
                        sent = await bot.send_message(cid, "🤡", reply_to_message_id=event.message_id)
                        asyncio.create_task(schedule_delete(sent))
                    except: pass
                elif key in clown_targets: del clown_targets[key]
                # 🔁 Зеркало
                if mirror_chats.get(cid, 0) > now and event.text and not event.text.startswith("аутист"):
                    try: await bot.send_message(cid, event.text)
                    except: pass
                elif cid in mirror_chats and mirror_chats[cid] <= now: del mirror_chats[cid]
                # 👁 Слежка
                if key in spy_targets and event.text:
                    try:
                        await bot.send_message(spy_targets[key],
                            f"👁 <b>Слежка</b> [{event.chat.title}]\n"
                            f"👤 {event.from_user.full_name}:\n{event.text}", parse_mode="HTML")
                    except: pass
                # 🎯 Цель — x2 варн обрабатывается в autist_commands
        return await handler(event, data)

class AntiMatMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        if not ANTI_MAT_ENABLED: return await handler(event, data)
        if event.chat.type not in ("group","supergroup"): return await handler(event, data)
        if not event.text or event.text.startswith("/"): return await handler(event, data)
        if not event.from_user: return await handler(event, data)
        if event.new_chat_members or event.left_chat_member: return await handler(event, data)
        uid, cid = event.from_user.id, event.chat.id
        try:
            m = await bot.get_chat_member(cid, uid)
            if m.status in ("administrator","creator"): return await handler(event, data)
        except: pass
        return await handler(event, data)


class PendingInputMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        uid = event.from_user.id if event.from_user else None
        if uid and uid in pending and not (event.text and event.text.startswith("/")):
            p = pending.pop(uid)
            action, target_id, target_name, chat_id = p["action"], p["target_id"], p["target_name"], p["chat_id"]
            text = event.text or ""
            try:
                if action == "mute_custom":
                    mins, label = parse_duration(text)
                    if mins:
                        await bot.restrict_chat_member(chat_id, target_id,
                            permissions=ChatPermissions(can_send_messages=False),
                            until_date=timedelta(minutes=mins))
                        await event.answer(random.choice(MUTE_MESSAGES).format(
                            name=f"<b>{target_name}</b>", time=label), parse_mode="HTML")
                    else: await event.reply("⚠️ Примеры: 10, 30m, 2h, 1d")
                elif action == "warn_custom":
                    reason = text.strip() or "Нарушение правил"
                    warnings[chat_id][target_id] += 1; count = warnings[chat_id][target_id]
                    if count >= MAX_WARNINGS:
                        await bot.ban_chat_member(chat_id, target_id); warnings[chat_id][target_id] = 0
                        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{target_name}</b>", max=MAX_WARNINGS)
                    else:
                        msg = random.choice(WARN_MESSAGES).format(
                            name=f"<b>{target_name}</b>", count=count, max=MAX_WARNINGS, reason=reason)
                    await event.answer(msg, parse_mode="HTML")
                elif action == "ban_custom":
                    reason = text.strip() or "Нарушение правил"
                    await bot.ban_chat_member(chat_id, target_id)
                    await event.answer(random.choice(BAN_MESSAGES).format(
                        name=f"<b>{target_name}</b>", reason=reason), parse_mode="HTML")
                elif action == "announce_text":
                    await bot.send_message(chat_id,
                        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{text}\n\n— <b>Администрация</b>", parse_mode="HTML")
                elif action == "poll_text":
                    parts = [x.strip() for x in text.split("|") if x.strip()]
                    if len(parts) >= 3: await bot.send_poll(chat_id, question=parts[0], options=parts[1:], is_anonymous=False)
                    else: await event.reply("⚠️ Формат: Вопрос|Вариант1|Вариант2")
                elif action == "weather_city":
                    await event.answer(await get_weather(text.strip()), parse_mode="HTML")
            except Exception as e:
                await event.reply(f"⚠️ Ошибка: {e}")
            try: await event.delete()
            except: pass
            return
        return await handler(event, data)

class AntiNSFWMiddleware(BaseMiddleware):
    """Удаляет NSFW стикеры и гифки"""
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.chat.type in ("group", "supergroup"):
            # Проверка стикеров
            if event.sticker:
                sticker = event.sticker
                is_nsfw = False
                # Проверяем флаг набора
                if sticker.set_name:
                    try:
                        sticker_set = await bot.get_sticker_set(sticker.set_name)
                        if getattr(sticker_set, 'is_nsfw', False):
                            is_nsfw = True
                    except: pass
                # Emoji-стикеры без набора или premium nsfw
                if getattr(sticker, 'is_video', False) and not sticker.set_name:
                    pass  # не трогаем обычные видео-стикеры без набора
                if is_nsfw:
                    try: await event.delete()
                    except: pass
                    return  # не передаём дальше
            # Проверка анимаций (гифки через animation)
            if event.animation:
                anim = event.animation
                # Telegram помечает NSFW анимации через has_spoiler или mime
                if getattr(anim, 'file_name', '').lower() in ('nsfw', 'adult') or \
                   getattr(event, 'has_media_spoiler', False):
                    try: await event.delete()
                    except: pass
                    return
        return await handler(event, data)

dp.message.middleware(PendingInputMiddleware())
dp.message.middleware(StatsMiddleware())
dp.message.middleware(SpecialEffectsMiddleware())
dp.message.middleware(AntiNSFWMiddleware())
dp.message.middleware(AntiMatMiddleware())

@dp.message(F.new_chat_members)
async def on_new_member(message: Message):
    for member in message.new_chat_members:
        if member.is_bot and AUTO_KICK_BOTS:
            try:
                await bot.ban_chat_member(message.chat.id, member.id)
                await bot.unban_chat_member(message.chat.id, member.id)
                sent = await message.answer(
                    f"🤖 Бот <b>{member.full_name}</b> автоматически удалён.", parse_mode="HTML")
                await asyncio.sleep(5)
                try: await sent.delete()
                except: pass
            except: pass
            continue
        await message.answer_photo(
            photo=FSInputFile("welcome.jpg"),
            caption=(
                f"👋 Привет, {member.mention_html()}!\n"
                f"Добро пожаловать в <b>{message.chat.title}</b>!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📜 <b>Обязательно ознакомься с правилами</b>\n"
                f"перед тем как начать писать!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Нажми кнопку ниже чтобы прочитать правила 👇</i>"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Читать правила чата", url="https://telegra.ph/Pravila-anon-chata-03-11-3")]
            ])
        )

@dp.message(F.left_chat_member)
async def on_left_member(message: Message):
    member = message.left_chat_member
    if member.is_bot: return

@dp.callback_query(F.data.startswith("panel:"))
async def cb_panel(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    _, action, tid_str = call.data.split(":", 2)
    tid = int(tid_str); cid = call.message.chat.id
    if tid != 0 and action in ("mute", "ban", "warn"):
        if await is_admin_by_id(cid, tid):
            await call.answer("🚫 Нельзя применить действие к администратору!", show_alert=True); return
    try:
        tm = await bot.get_chat_member(cid, tid) if tid != 0 else None
        tname   = tm.user.full_name if tm else "участник"
        mention = tm.user.mention_html() if tm else ""
    except:
        tname = f"ID {tid}"; mention = f"<code>{tid}</code>"
    try:
        if action == "close":
            await call.message.delete()
        elif action == "mainmenu":
            total_msgs  = sum(chat_stats[cid].values())
            total_warns = sum(warnings[cid].values())
            await call.message.edit_text(
                f"⚙️ <b>Панель управления</b>\n\n"
                f"💬 Чат: <b>{call.message.chat.title}</b>\n"
                f"📨 Сообщений: <b>{total_msgs}</b>\n"
                f"⚡ Варнов: <b>{total_warns}</b>\n"
                f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери раздел:",
                parse_mode="HTML", reply_markup=kb_main_menu())
        elif action == "select":
            await call.message.edit_text(
                "👤 <b>Управление участником</b>\n\nОткрой панель реплаем:\n"
                "<code>/panel</code> → ответь на сообщение участника",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        elif action == "back":
            if tid != 0:
                warns = warnings[cid].get(tid, 0); rep = reputation[cid].get(tid, 0)
                msgs  = chat_stats[cid].get(tid, 0)
                afk   = f"\n🎮 AFK: {afk_users[tid]}" if tid in afk_users else ""
                try:
                    safe_name = (tm.user.full_name
                        .replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
                    mention = f'<a href="tg://user?id={tid}">{safe_name}</a>'
                except:
                    mention = f"<code>{tid}</code>"
                await call.message.edit_text(
                    f"👤 <b>Панель участника</b>\n\n🆔 {mention}{afk}\n"
                    f"🆔 ID: <code>{tid}</code>\n"
                    f"⚡ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
                    f"⭐ Репутация: <b>{rep:+d}</b>\n"
                    f"💬 Сообщений: <b>{msgs}</b>\n\n➡️ Выбери действие:",
                    parse_mode="HTML", reply_markup=kb_user_panel(tid))
            else:
                await call.message.edit_text("🔧 <b>Панель управления</b>\n\n➡️ Выбери раздел:",
                    parse_mode="HTML", reply_markup=kb_main_menu())
        elif action == "mute":
            await call.message.edit_text(f"🔇 <b>Мут для {tname}</b>\n\nВыбери время:",
                parse_mode="HTML", reply_markup=kb_mute(tid))
        elif action == "unmute":
            await bot.restrict_chat_member(cid, tid, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            await call.message.edit_text(f"🔊 <b>{tname}</b> размучен.", parse_mode="HTML")
            asyncio.create_task(schedule_delete(call.message))
            await log_action(f"╔═══════════════════╗\n🔊  <b>РАЗМУТ</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "warn":
            await call.message.edit_text(f"⚡ <b>Варн для {tname}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_warn(tid))
        elif action == "unwarn":
            if warnings[cid][tid] > 0: warnings[cid][tid] -= 1
            await call.message.edit_text(
                f"🌿 С <b>{tname}</b> снят варн. Осталось: <b>{warnings[cid][tid]}/{MAX_WARNINGS}</b>",
                parse_mode="HTML")
            asyncio.create_task(schedule_delete(call.message))
        elif action == "ban":
            await call.message.edit_text(f"🔨 <b>Бан для {tname}</b>\n\nВыбери причину:",
                parse_mode="HTML", reply_markup=kb_ban(tid))
        elif action == "unban":
            await bot.unban_chat_member(cid, tid, only_if_banned=True)
            await call.message.edit_text(f"🕊 <b>{tname}</b> разбанен.", parse_mode="HTML")
            asyncio.create_task(schedule_delete(call.message))
            await log_action(f"╔═══════════════════╗\n🕊  <b>РАЗБАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "del":
            try: await call.message.reply_to_message.delete()
            except: pass
            await call.message.edit_text("🗑 Сообщение удалено.")
            asyncio.create_task(auto_delete(call.message))
        elif action == "info":
            tm2 = await bot.get_chat_member(cid, tid); u = tm2.user
            smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
                    "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
            afk = f"\n😴 AFK: {afk_users[tid]}" if tid in afk_users else ""
            await call.message.edit_text(
                f"🔍 <b>Инфо:</b>\n\n🏷 {u.mention_html()}{afk}\n"
                f"🔗 {'@'+u.username if u.username else 'нет'}\n🪪 <code>{u.id}</code>\n"
                f"📌 {smap.get(tm2.status, tm2.status)}\n"
                f"⚡ Варнов: <b>{warnings[cid].get(tid,0)}/{MAX_WARNINGS}</b>\n"
                f"🌟 Репутация: <b>{reputation[cid].get(tid,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(tid,0)}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[kb_back(tid)]))
        elif action == "fun":
            await call.message.edit_text(f"🎭 <b>Приколы над {tname}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_fun(tid))
        elif action == "messages":
            await call.message.edit_text("✉️ <b>Действия с сообщениями</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_messages(tid))
        elif action == "members":
            await call.message.edit_text("👥 <b>Управление участниками</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_members(tid))
        elif action == "chat":
            await call.message.edit_text(
                f"⚙️ <b>Управление чатом</b>\n\n"
                f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
                f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_chat(tid))
        elif action == "games":
            await call.message.edit_text("🎮 <b>Игры и команды</b>\n\nВыбери:",
                parse_mode="HTML", reply_markup=kb_games(tid))
        elif action == "botstats2":
            total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
            total_warns = sum(sum(v.values()) for v in warnings.values())
            await call.message.edit_text(
                f"📊 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
                f"⚡ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
                f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
                f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
                f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))

        elif action == "reports":
            queue = report_queue.get(cid, [])
            if not queue:
                await call.answer("✅ Жалоб нет!", show_alert=True); return
            text_lines = [f"🚨 <b>Очередь жалоб</b> ({len(queue)} шт.)\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            for i, r in enumerate(queue[:5]):
                try:
                    tm_r = await bot.get_chat_member(cid, r['target'])
                    rname = tm_r.user.full_name
                except: rname = f"ID{r['target']}"
                try:
                    tm_rep = await bot.get_chat_member(cid, r['reporter'])
                    repname = tm_rep.user.full_name
                except: repname = f"ID{r['reporter']}"
                text_lines.append(f"#{i+1} 👤 На: <b>{rname}</b>\n👮 От: {repname}\n💬 {r['text'][:80]}\n")
            kb_rep = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Варн",    callback_data=f"rpt:warn:{queue[0]['target']}:0"),
                 InlineKeyboardButton(text="🔇 Мут",     callback_data=f"rpt:mute:{queue[0]['target']}:0")],
                [InlineKeyboardButton(text="🔨 Бан",     callback_data=f"rpt:ban:{queue[0]['target']}:0"),
                 InlineKeyboardButton(text="❌ Отклонить", callback_data=f"rpt:reject:{queue[0]['target']}:0")],
                [InlineKeyboardButton(text="🔙 Назад",   callback_data="panel:mainmenu:0")]
            ])
            await call.message.edit_text("\n".join(text_lines), parse_mode="HTML", reply_markup=kb_rep)

        elif action == "economy":
            total_rep = sum(reputation[cid].values())
            total_invested = sum(stock_invested[cid].values())
            lottery_count = len(lottery_tickets[cid])
            await call.message.edit_text(
                "✨ <b>CHAT GUARD</b> — Экономика\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💰 Всего репы в чате: <b>{total_rep}</b>\n"
                f"📈 Вложено на бирже: <b>{total_invested}</b>\n"
                f"🎰 Билетов в лотерее: <b>{lottery_count}</b>\n\n"
                "▸ /toprep · /топxp · /stock · /lottery",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")
                ]]))

        elif action == "topxp":
            await cmd_topxp(call.message)

        elif action == "banlist":
            bans = ban_list.get(cid, set())
            lines = [f"🚫 <b>Список банов</b> ({len(bans)} чел.)\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            for uid2 in list(bans)[:20]:
                try:
                    tm2 = await bot.get_chat_member(cid, uid2)
                    lines.append(f"▸ {tm2.user.full_name} — <code>{uid2}</code>")
                except: lines.append(f"▸ <code>{uid2}</code>")
            if not bans: lines.append("Банов нет 🎉")
            await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")
                ]]))
    except Exception as e:
        await call.answer(f"⚠️ Ошибка: {e}", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    _, tid_str, tval = call.data.split(":")
    tid = int(tid_str); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if tval == "custom":
        pending[call.from_user.id] = {"action":"mute_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(
            f"✏️ Введи время мута для <b>{tname}</b>:\n"
            f"Примеры: <code>10</code>, <code>30m</code>, <code>2h</code>, <code>1d</code>",
            parse_mode="HTML")
        asyncio.create_task(schedule_delete(call.message))
        await call.answer(); return
    mins  = int(tval)
    label = f"{mins} мин." if mins < 60 else (f"{mins//60} ч." if mins < 1440 else f"{mins//1440} дн.")
    await bot.restrict_chat_member(cid, tid,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    await call.message.edit_text(
        random.choice(MUTE_MESSAGES).format(name=f"<b>{tname}</b>", time=label), parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔇  <b>МУТ</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n⏱ <b>Время:</b> {label}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.answer(f"Замутен на {label}!")

@dp.callback_query(F.data.startswith("warn:"))
async def cb_warn(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"warn_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину варна для <b>{tname}</b>:", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
        await call.answer(); return
    warnings[cid][tid] += 1; count = warnings[cid][tid]
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, tid); warnings[cid][tid] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=f"<b>{tname}</b>", max=MAX_WARNINGS)
        await log_action(f"╔═══════════════════╗\n🔨  <b>АВТОБАН</b>\n╚═══════════════════╝\n\n🤖 <b>Причина:</b> {MAX_WARNINGS} варнов — лимит\n🎯 <b>Кого:</b> <b>{tname}</b>\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=f"<b>{tname}</b>", count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"╔═══════════════════╗\n⚡  <b>ВАРН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.message.edit_text(msg, parse_mode="HTML")
    asyncio.create_task(schedule_delete(call.message))
    await call.answer("Варн выдан!")

@dp.callback_query(F.data.startswith("ban:"))
async def cb_ban(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"ban_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину бана для <b>{tname}</b>:", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
        await call.answer(); return
    if reason == "tempban24":
        await bot.ban_chat_member(cid, tid, until_date=timedelta(hours=24))
        await call.message.edit_text(f"⏰ <b>{tname}</b> забанен на <b>24 часа</b>.", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
        await log_action(f"╔═══════════════════╗\n⏰  <b>БАН 24ч</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n⏱ <b>Время:</b> 24 часа\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        await call.answer(); return
    await bot.ban_chat_member(cid, tid)
    await call.message.edit_text(
        random.choice(BAN_MESSAGES).format(name=f"<b>{tname}</b>", reason=reason), parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔨  <b>БАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.answer("Забанен!")

@dp.callback_query(F.data.startswith("fun:"))
async def cb_fun(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); mention = tm.user.mention_html()
    except: mention = f"<code>{tid}</code>"
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё раз", callback_data=call.data)], kb_back(tid)])
    if action == "rban":
        await call.message.edit_text(
            f"🎲 {mention} получил <b>шуточный бан</b>!\n"
            f"📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет)</i>",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "iq":
        iq = random.randint(1, 200)
        c  = "🥔 Картошка умнее." if iq<70 else ("🐒 Обезьяна лучше." if iq<100 else ("😐 Сойдёт." if iq<130 else ("🧠 Умный!" if iq<160 else "🤖 Настоящий гений!")))
        await call.message.edit_text(f"🧠 IQ {mention}: <b>{iq}</b>\n{c}", parse_mode="HTML", reply_markup=back_kb)
        asyncio.create_task(auto_delete(call.message))
    elif action == "gay":
        p = random.randint(0, 100)
        await call.message.edit_text(
            f"🌈 {mention}\n{'🟣'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка 😄",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "compliment":
        await call.message.edit_text(
            f"🌸 <b>{mention}:</b>\n\n{random.choice(COMPLIMENTS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "predict":
        await call.message.edit_text(
            f"🔮 <b>Предсказание для {mention}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "horoscope":
        sign, text = random.choice(list(HOROSCOPES.items()))
        await call.message.edit_text(
            f"{sign} <b>Гороскоп для {mention}:</b>\n\n{text}", parse_mode="HTML", reply_markup=back_kb)
        asyncio.create_task(schedule_delete(call.message))
    elif action == "rate":
        score = random.randint(0, 10)
        await call.message.edit_text(
            f"⭐ Оценка {mention}:\n{'🌟'*score+'☆'*(10-score)}\n<b>{score}/10</b>",
            parse_mode="HTML", reply_markup=back_kb)
    elif action == "truth":
        await call.message.edit_text(
            f"🤔 <b>Вопрос для {mention}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "dare":
        await call.message.edit_text(
            f"😈 <b>Задание для {mention}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML", reply_markup=back_kb)
    elif action == "wyr":
        await call.message.edit_text(
            f"🎯 <b>Выбор для {mention}:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML", reply_markup=back_kb)
    await call.answer()

@dp.callback_query(F.data.startswith("msg:"))
async def cb_msg(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    if action == "pin":
        try: await bot.pin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📌 Закреплено!", show_alert=True)
        except: await call.answer("⚠️ Открой панель реплаем на нужное сообщение.", show_alert=True)
    elif action == "unpin":
        try: await bot.unpin_chat_message(cid, call.message.reply_to_message.message_id); await call.answer("📍 Откреплено!", show_alert=True)
        except: await call.answer("⚠️ Нет сообщения.", show_alert=True)
    elif action == "del":
        try: await call.message.reply_to_message.delete(); await call.answer("🗑 Удалено!", show_alert=True)
        except: await call.answer("⚠️ Нет сообщения.", show_alert=True)
    elif action.startswith("clear"):
        n = int(action.replace("clear",""))
        deleted = 0
        for i in range(call.message.message_id, call.message.message_id - n - 1, -1):
            try: await bot.delete_message(cid, i); deleted += 1
            except: pass
        await call.answer(f"🧹 Удалено {deleted} сообщений!", show_alert=True)
    elif action == "announce":
        pending[call.from_user.id] = {"action":"announce_text","target_id":0,"target_name":"","chat_id":cid}
        await call.message.edit_text("📢 Напиши текст объявления:\n<i>(следующее сообщение станет объявлением)</i>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    elif action == "poll":
        pending[call.from_user.id] = {"action":"poll_text","target_id":0,"target_name":"","chat_id":cid}
        await call.message.edit_text("📊 Напиши голосование:\n<code>Вопрос|Вариант 1|Вариант 2</code>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    try: await call.message.edit_text("✉️ <b>Действия с сообщениями</b>\n\nВыбери:", parse_mode="HTML", reply_markup=kb_messages(tid))
    except: pass
    await call.answer()

@dp.callback_query(F.data.startswith("members:"))
async def cb_members(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid) if tid != 0 else None; tname = tm.user.full_name if tm else "участник"
    except: tname = f"ID {tid}"
    if action == "adminlist":
        admins = await bot.get_chat_administrators(cid)
        lines = ["👮 <b>Администраторы чата:</b>\n"]
        for adm in admins:
            if adm.user.is_bot: continue
            icon  = "👑" if adm.status == "creator" else "🛡"
            title = f" — <i>{adm.custom_title}</i>" if hasattr(adm,"custom_title") and adm.custom_title else ""
            lines.append(f"{icon} {adm.user.mention_html()}{title}")
        await call.message.edit_text("\n".join(lines), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
    elif action == "top":
        stats = chat_stats[cid]
        if not stats: await call.answer("📊 Статистика пуста!", show_alert=True)
        else:
            sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
            medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
            lines    = ["🏆 <b>Топ активных:</b>\n"]
            for i, (uid, cnt) in enumerate(sorted_u):
                try: m = await bot.get_chat_member(cid, uid); uname = m.user.full_name
                except: uname = f"ID {uid}"
                lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщ.")
            await call.message.edit_text("\n".join(lines), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
    elif action == "warn24":
        if tid != 0:
            await bot.restrict_chat_member(cid, tid,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
            await call.message.edit_text(
                f"📵 <b>{tname}</b> — мут на <b>24 часа</b> за рекламу.", parse_mode="HTML")
            asyncio.create_task(schedule_delete(call.message))
            await log_action(f"╔═══════════════════╗\n📵  <b>МУТ 24ч</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n⏱ <b>Время:</b> 24 часа\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        else: await call.answer("⚠️ Открой панель реплаем на участника.", show_alert=True)
    elif action == "warninfo":
        if tid != 0:
            count = warnings[cid].get(tid, 0)
            await call.message.edit_text(
                f"⚡ Варнов у <b>{tname}</b>: <b>{count}/{MAX_WARNINGS}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        else: await call.answer("⚠️ Открой панель реплаем на участника.", show_alert=True)
    await call.answer()

@dp.callback_query(F.data.startswith("chat:"))
async def cb_chat(call: CallbackQuery):
    global ANTI_MAT_ENABLED, AUTO_KICK_BOTS
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":"); cid = call.message.chat.id
    if parts[1] == "slow":
        delay = int(parts[2]); tid = int(parts[3])
        try:
            chat = await bot.get_chat(cid)
            await bot.set_chat_slow_mode_delay(cid, delay)
            label = f"Slowmode {delay}с включён!" if delay > 0 else "Slowmode выключен!"
            await call.answer(f"🐢 {label}", show_alert=True)
        except Exception as e:
            await call.answer(f"⚠️ Ошибка: {e}", show_alert=True)
        await call.message.edit_text(
            f"⚙️ <b>Управление чатом</b>\n\n"
            f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
            parse_mode="HTML", reply_markup=kb_chat(tid))
        return
    action = parts[1]; tid = int(parts[2])
    if action == "lock":
        await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
        await call.answer("🔒 Чат заблокирован!", show_alert=True)
    elif action == "unlock":
        await bot.set_chat_permissions(cid, ChatPermissions(
            can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
            can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
        await call.answer("🔓 Чат разблокирован!", show_alert=True)
    elif action == "antimat":
        ANTI_MAT_ENABLED = not ANTI_MAT_ENABLED
        await call.answer(f"🧼 Антимат {'✅ включён' if ANTI_MAT_ENABLED else '❌ выключен'}!", show_alert=True)
    elif action == "autokick":
        AUTO_KICK_BOTS = not AUTO_KICK_BOTS
        await call.answer(f"🤖 Автокик {'✅ включён' if AUTO_KICK_BOTS else '❌ выключен'}!", show_alert=True)
    elif action == "rules":
        await call.message.edit_text(RULES_TEXT, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    elif action == "botstats":
        total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
        total_warns = sum(sum(v.values()) for v in warnings.values())
        await call.message.edit_text(
            f"📈 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
            f"⚡ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
            f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
            f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
            f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    await call.message.edit_text(
        f"⚙️ <b>Управление чатом</b>\n\n"
        f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
        f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\nВыбери:",
        parse_mode="HTML", reply_markup=kb_chat(tid))
    await call.answer()

@dp.callback_query(F.data.startswith("game:"))
async def cb_game(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":", 2); action = parts[1]; tid = int(parts[2]); cid = call.message.chat.id
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё раз", callback_data=call.data)],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]])
    if action == "roll":
        await call.message.edit_text(
            f"🎲 Бросаю кубик... выпало: <b>{random.randint(1,6)}</b>!", parse_mode="HTML", reply_markup=back_kb)
    elif action == "flip":
        await call.message.edit_text(random.choice(["🦅 Орёл!", "🪙 Решка!"]), reply_markup=back_kb)
        asyncio.create_task(auto_delete(call.message))
        symbols = ["🍒","🍋","🍊","🍇","⭐","7️⃣","💎"]
        s1,s2,s3 = random.choice(symbols),random.choice(symbols),random.choice(symbols)
        if s1==s2==s3=="💎":              res = "💰 ДЖЕКПОТ!!"
        elif s1==s2==s3:                  res = f"🎉 Три {s1}! Выиграл!"
        elif s1==s2 or s2==s3 or s1==s3:  res = "😐 Два одинаковых. Почти!"
        else:                             res = "😢 Не повезло. Попробуй ещё!"
        await call.message.edit_text(f"🎰 [ {s1} | {s2} | {s3} ]\n\n{res}", reply_markup=back_kb)
        asyncio.create_task(auto_delete(call.message))
    elif action == "8ball":
        await call.message.edit_text(
            f"🎱 <b>Ответ шара:</b>\n\n{random.choice(BALL_ANSWERS)}", parse_mode="HTML", reply_markup=back_kb)
    elif action.startswith("rps_"):
        mp = {"k":("к","✊ Камень"),"n":("н","✌️ Ножницы"),"b":("б","🖐 Бумага")}
        wins = {"к":"н","н":"б","б":"к"}
        key = action.split("_")[1]; pk,pl = mp[key]; bk,bl = random.choice(list(mp.values()))
        res = "🤝 Ничья!" if pk==bk else ("🎉 Ты выиграл!" if wins[pk]==bk else "😈 Я выиграл!")
        await call.message.edit_text(f"Ты: {pl}\nЯ: {bl}\n\n{res}", reply_markup=back_kb)
        asyncio.create_task(auto_delete(call.message))
    elif action == "quote":
        await call.message.edit_text(f"📖 {random.choice(QUOTES)}", reply_markup=back_kb)
        asyncio.create_task(auto_delete(call.message))
    elif action.startswith("weather_"):
        city = action.replace("weather_","")
        if city == "custom":
            pending[call.from_user.id] = {"action":"weather_city","target_id":0,"target_name":"","chat_id":cid}
            await call.message.edit_text("🌍 Напиши название города:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
            await call.answer(); return
        await call.message.edit_text(await get_weather(city), parse_mode="HTML", reply_markup=back_kb)
        asyncio.create_task(auto_delete(call.message))
    elif action.startswith("countdown"):
        n = int(action.replace("countdown",""))
        await call.message.edit_text(f"⏱ <b>{n}...</b>", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
        for i in range(n-1, 0, -1):
            await asyncio.sleep(1)
            try: await call.message.edit_text(f"⏱ <b>{i}...</b>", parse_mode="HTML")
            except: pass
        await asyncio.sleep(1)
        await call.message.edit_text("🚀 <b>ПОЕХАЛИ!</b>", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="panel:mainmenu:0")]]))
        await call.answer(); return
    await call.answer()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await reply_auto_delete(message, 
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "🤖 Я бот-модератор этого чата.\n"
        "📜 /rules — правила\n"
        "❓ /help — все команды\n"
        "⚙️ /panel — панель управления (реплай на участника)",
        parse_mode="HTML")

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    await message.reply_photo(
        photo=FSInputFile("welcome.jpg"),
        caption="📜 <b>Правила чата</b>\n\n🔎 Нажми кнопку ниже чтобы прочитать правила:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Правила чата", url="https://telegra.ph/Pravila-anon-chata-03-11-3")]
        ])
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_adm = await check_admin(message)
    text = (
        "✨ <b>CHAT GUARD</b> ✨\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"

        "🪪 <b>ПРОФИЛЬ</b>\n"
        "▸ /profile — твой профиль\n"
        "▸ /ранг — уровень и опыт\n"
        "▸ /streak — серия активности\n"
        "▸ /info — инфо об участнике <i>(реплай)</i>\n"
        "▸ /avatar — выбрать эмодзи аватар\n\n"

        "⭐ <b>РЕПУТАЦИЯ</b>\n"
        "▸ /rep — репутация <i>(реплай)</i>\n"
        "▸ /toprep — топ по репутации\n"
        "▸ /daily — ежедневный бонус\n"
        "▸ /shop — магазин титулов\n"
        "▸ /giverep [сумма] — перевести репу <i>(реплай)</i>\n"
        "▸ /like · /dislike — лайк/дизлайк <i>(реплай)</i>\n\n"

        "🎰 <b>ЭКОНОМИКА</b>\n"
        "▸ /casino [сумма] — казино\n"
        "▸ /duel [сумма] — дуэль <i>(реплай)</i>\n"
        "▸ /roulette [сумма] — рулетка репы\n"
        "▸ /lottery · /lottery_buy — лотерея\n"
        "▸ /stock · /stock_invest — биржа\n"
        "▸ /boost — магазин бустеров\n"
        "▸ /trivia — викторина за репу\n\n"

        "🧙 <b>АРТЕФАКТЫ</b>\n"
        "▸ /artifact — мои артефакты\n"
        "▸ /artifact_roll — получить (100 репы)\n\n"

        "🤝 <b>КЛАНЫ</b>\n"
        "▸ /clan — мой клан · /clan_top — топ\n"
        "▸ /clan_create ТЕГ Название — создать\n"
        "▸ /clan_join ТЕГ — вступить\n"
        "▸ /clan_leave — покинуть клан\n\n"

        "💬 <b>КОНТЕНТ</b>\n"
        "▸ /confession [текст] — анонимная исповедь\n"
        "▸ /quote_save — цитата <i>(реплай)</i> · /quote_random\n"
        "▸ /journal · /journal текст — дневник\n"
        "▸ /meme верх|низ — мем-генератор\n\n"

        "📊 <b>СТАТИСТИКА</b>\n"
        "▸ /chatstats · /mystats · /topactive\n""▸ /topxp — таблица лидеров по XP\n\n"

        "🕵️ <b>АНОНИМНО</b>\n"
        "▸ /ask — открыть АСК\n"
        "▸ /send @юзер текст — анонимный вопрос\n"
        "▸ /secret @юзер текст — анонимное лс\n\n"

        "🛠 <b>УТИЛИТЫ</b>\n"
        "▸ /weather [город] · /rules · /afk\n"
        "▸ /birthday ДД.ММ · /remind 30m текст\n"
        "▸ /subscribe · /ref (+30 репы за друга)\n\n"

        "🎲 <b>РАЗВЛЕЧЕНИЯ</b>\n"
        "▸ /role — роль дня · /mvp · /join\n"
        "▸ /roll · /flip · /slot · /rps · /8ball\n"
        "▸ /choose вар1|вар2 · /guess\n\n"

        "🤖 <b>АУТИСТ-КОМАНДЫ (fun)</b>\n"
        "▸ аутист поженить <i>(реплай)</i> — поженить с юзером\n"
        "▸ аутист казнить <i>(реплай)</i> — казнить юзера\n"
        "▸ аутист диагноз <i>(реплай)</i> — поставить диагноз\n"
        "▸ аутист профессия <i>(реплай)</i> — назначить профессию\n"
        "▸ аутист обозвать <i>(реплай)</i> — обозвать юзера\n"
        "▸ аутист дуэль <i>(реплай)</i> — дуэль с юзером\n"
        "▸ аутист похитить <i>(реплай)</i> — похитить юзера\n"
        "▸ аутист экзамен <i>(реплай)</i> — задать вопрос юзеру\n"
    )
    if is_adm:
        text += (
            "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            "👮 <b>АДМИНИСТРАТОРАМ</b>\n\n"

            "🔨 <b>SLASH-МОДЕРАЦИЯ</b>\n"
            "▸ /warn @user — предупреждение\n"
            "▸ /unwarn @user — снять варн\n"
            "▸ /ban @user — бан\n"
            "▸ /unban @user — разбан\n"
            "▸ /mute @user — мут\n"
            "▸ /unmute @user — размут\n"
            "▸ /tempban @user 1ч — временный бан\n"
            "▸ /warn24 @user — варн на 24ч\n"
            "▸ /rban @user — разбан по ID\n"
            "▸ /warnmenu — меню варнов\n"
            "▸ /banlist — список банов\n"
            "▸ /panel — панель управления\n\n"

            "✉️ <b>ЧАТ И СООБЩЕНИЯ</b>\n"
            "▸ /del — удалить сообщение <i>(реплай)</i>\n"
            "▸ /clear N — удалить N сообщений\n"
            "▸ /pin — закрепить <i>(реплай)</i>\n"
            "▸ /unpin — открепить\n"
            "▸ /announce текст — объявление\n"
            "▸ /poll — создать опрос\n"
            "▸ /lock · /unlock — закрыть/открыть чат\n"
            "▸ /slowmode N — замедление (сек)\n"
            "▸ /antimat — антимат вкл/выкл\n"
            "▸ /autokick — автокик ботов\n\n"

            "👥 <b>УЧАСТНИКИ И СТАТИСТИКА</b>\n"
            "▸ /adminlist — список админов\n"
            "▸ /promote @user тег — выдать тег\n"
            "▸ /removetag @user — убрать тег\n"
            "▸ /modhistory @user — история модерации\n"
            "▸ /modtop — топ модераторов\n"
            "▸ /modexport — экспорт истории\n"
            "▸ /modreport — отчёт модерации\n"
            "▸ /usernote @user — заметки\n"
            "▸ /report — жалобы\n"
            "▸ /status — отчёт за сегодня\n"
            "▸ /botstats · /broadcast · /chats\n\n"

            "🤖 <b>АУТИСТ-КОМАНДЫ (модерация)</b>\n"
            "▸ аутист варн @user причина\n"
            "▸ аутист варн @user 30м причина — с автосбросом\n"
            "▸ аутист разварн @user\n"
            "▸ аутист мут @user 1ч причина\n"
            "▸ аутист размут @user\n"
            "▸ аутист бан @user причина\n"
            "▸ аутист разбан @user\n"
            "▸ аутист кик @user\n"
            "▸ аутист мут навсегда @user\n\n"

            "🔍 <b>АУТИСТ-КОМАНДЫ (инфо)</b>\n"
            "▸ аутист инфо @user — карточка юзера\n"
            "▸ аутист варны @user — кол-во варнов\n"
            "▸ аутист поиск @user — полное досье\n"
            "▸ аутист история — 20 последних действий\n"
            "▸ аутист статус — отчёт за сегодня\n"
            "▸ аутист пересмотр @user — история варнов\n"
            "▸ аутист последний @user — когда писал\n"
            "▸ аутист причина @user — причина последнего варна\n"
            "▸ аутист топ нарушителей — топ-10\n\n"

            "🛠 <b>АУТИСТ-КОМАНДЫ (действия)</b>\n"
            "▸ аутист чистка @user 30м — удалить сообщения\n"
            "▸ аутист заморозка @user — мут на 24ч\n"
            "▸ аутист только чтение @user — навсегда\n"
            "▸ аутист медиамут @user — без медиа/стикеров\n"
                "▸ аутист стикермут @user — только стикеры запрещены\n"
                "▸ аутист гифмут @user — только гифки запрещены\n"
                "▸ аутист войсмут @user — голосовые запрещены\n"
                "▸ аутист всёмут @user — только текст разрешён\n"
            "▸ аутист антиспам вкл/выкл — только текст\n"
            "▸ аутист уровень @user +5 — изменить уровень\n"
            "▸ аутист объяви @user причина — публичное предупреждение\n"
            "▸ аутист алерт текст — предупреждение всему чату\n"
            "▸ аутист напомни мод 1ч текст — напомнить всем модераторам\n\n"

            "🎪 <b>ТУРНИРЫ</b>\n"
            "▸ /tournament start · begin · next · stop\n"
        )
    if message.from_user.id == OWNER_ID:
        text += (
            "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            "👑 <b>ТОЛЬКО ДЛЯ ВЛАДЕЛЬЦА</b>\n\n"

            "💣 <b>ЖЁСТКАЯ МОДЕРАЦИЯ</b>\n"
            "▸ аутист ядерка @user — варн+мут+удалить всё\n"
            "▸ аутист молния @user — удалить все сообщения юзера\n"
            "▸ аутист взрыв — удалить последние 50 сообщений\n"
            "▸ аутист локдаун — закрыть чат\n"
            "▸ аутист локдаун выкл — открыть чат\n"
            "▸ аутист тишина 10м — закрыть на N мин, авторазмут\n"
            "▸ /yaderna @user — ядерка через слэш\n\n"

            "🎭 <b>ПРИКОЛЫ</b>\n"
            "▸ аутист клоун @user — 🤡 под сообщениями 10 мин\n"
            "▸ аутист смерть @user — некролог в чат\n"
            "▸ аутист маска @user текст — пишет от чужого имени\n"
            "▸ аутист магнит @user — 👍 под сообщениями 10 мин\n"
            "▸ аутист цель @user — варны x2 на 30 мин\n"
            "▸ аутист зеркало — повторяет все сообщения 5 мин\n"
            "▸ аутист хаос — случайное действие над случайным юзером\n\n"

            "👑 <b>ВЛАСТЬ</b>\n"
            "▸ аутист корона @user — титул Король чата 24ч\n"
            "▸ аутист анонс текст — объявление от бота\n"
            "▸ аутист вызов @user вопрос — публичный вызов\n"
            "▸ аутист громко текст — CAPS LOCK сообщение\n"
            "▸ аутист закреп текст — красивое закреплённое\n"
            "▸ аутист голос Вопрос? | Вар1 | Вар2 — голосование\n"
            "▸ /corona @user · /announce текст\n\n"

            "💰 <b>ЭКОНОМИКА</b>\n"
            "▸ аутист дать репу @user 100 — репа вручную\n"
            "▸ аутист сброс @user — обнулить всё\n"
            "▸ аутист лотерея — принудит. розыгрыш\n"
            "▸ /sbros @user\n\n"

            "🕵️ <b>СЛЕЖКА И РАЗВЕДКА</b>\n"
            "▸ аутист слежка @user — сообщения в личку\n"
            "▸ аутист шпион — топ активных за час в личку\n"
            "▸ аутист скрин — статистика чата в личку\n"
            "▸ аутист рост @user — график активности 14 дней\n\n"

            "🎲 <b>РАНДОМ</b>\n"
            "▸ аутист жребий — случайный участник\n"
            "▸ аутист хаос — случайное действие\n\n"

            "⏰ <b>НАПОМИНАНИЯ</b>\n"
            "▸ аутист напомни 30м текст — напомнит тебе в личку\n"
        )
    if message.from_user.id == OWNER_ID:
        chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for chunk in chunks:
            try: await bot.send_message(OWNER_ID, chunk, parse_mode="HTML")
            except: pass
        await reply_auto_delete(message, "📬 Справка отправлена тебе в личку!", parse_mode="HTML")
    else:
        await reply_auto_delete(message, text, parse_mode="HTML")

@dp.message(Command("panel"))
async def cmd_panel(message: Message):
    if not await require_admin(message): return
    if message.reply_to_message:
        target = message.reply_to_message.from_user
        warns  = warnings[message.chat.id].get(target.id, 0)
        rep    = reputation[message.chat.id].get(target.id, 0)
        msgs   = chat_stats[message.chat.id].get(target.id, 0)
        afk    = f"\n😴 AFK: {afk_users[target.id]}" if target.id in afk_users else ""
        await reply_auto_delete(message, 
            "✨ <b>CHAT GUARD</b> — Участник\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 {target.mention_html()}{afk}\n"
            f"🪪 ID: <code>{target.id}</code>\n"
            f"⚡ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
            f"⭐ Репутация: <b>{rep:+d}</b>\n"
            f"💬 Сообщений: <b>{msgs}</b>\n"
            f"🏅 Уровень: <b>{xp_data[message.chat.id].get(target.id, 0)} XP</b>\n\n"
            "▸ Выбери действие:",
            
            parse_mode="HTML", reply_markup=kb_user_panel(target.id))
    else:
        total_msgs  = sum(chat_stats[message.chat.id].values())
        total_warns = sum(warnings[message.chat.id].values())
        await reply_auto_delete(message, 
            "✨ <b>CHAT GUARD</b> — Панель управления\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💬 Чат: <b>{message.chat.title}</b>\n"
            f"👥 Участников: <b>{len(chat_stats[message.chat.id])}</b> активных\n"
            f"📨 Сообщений: <b>{total_msgs}</b>\n"
            f"⚡ Варнов выдано: <b>{total_warns}</b>\n"
            f"🧼 Антимат: <b>{'✅ вкл' if ANTI_MAT_ENABLED else '❌ выкл'}</b>\n"
            f"🤖 Автокик: <b>{'✅ вкл' if AUTO_KICK_BOTS else '❌ выкл'}</b>\n\n"
            "▸ Выбери раздел:",
            
            parse_mode="HTML", reply_markup=kb_main_menu())

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя забанить администратора!"); return
    reason = command.args or "Нарушение правил"
    cid = message.chat.id
    # 📨 ЛС нарушителю ДО бана (после уже не получится)
    dm_ok = await dm_warn_user(target.id, target.full_name, reason,
                                message.chat.title, "🔨 Бан", message.from_user.full_name)
    await bot.ban_chat_member(cid, target.id)
    # 📸 Скриншот нарушения
    if message.reply_to_message.text:
        await log_violation_screenshot(
            cid, target.id, target.full_name,
            message.reply_to_message.text,
            "🔨 Бан", reason, message.from_user.full_name, message.chat.title)
    reply = random.choice(BAN_MESSAGES).format(name=target.mention_html(), reason=reason)
    if dm_ok: reply += "\n<i>📨 Нарушитель уведомлён в лс</i>"
    await reply_auto_delete(message, reply, parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔨  <b>БАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    add_mod_history(cid, target.id, "🔨 Бан", reason, message.from_user.full_name)
    from datetime import datetime
    ban_list[cid][target.id] = {
        "name": target.full_name, "reason": reason,
        "by": message.from_user.full_name,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "temp": False
    }
    mod_reasons[cid][target.id]["ban"] = reason

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await reply_auto_delete(message, f"🕊 {target.mention_html()} разбанен.", parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🕊  <b>РАЗБАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя замутить администратора!"); return
    mins, label = parse_duration(command.args or "60m")
    if not mins: mins = 60; label = "1 ч."
    cid = message.chat.id
    await bot.restrict_chat_member(cid, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
    # 📸 Скриншот нарушения
    if message.reply_to_message.text:
        await log_violation_screenshot(
            cid, target.id, target.full_name,
            message.reply_to_message.text,
            f"🔇 Мут {label}", command.args or "—",
            message.from_user.full_name, message.chat.title)
    # 📨 ЛС нарушителю
    dm_ok = await dm_warn_user(target.id, target.full_name, command.args or "Нарушение правил",
                                message.chat.title, f"🔇 Мут на {label}", message.from_user.full_name)
    reply = random.choice(MUTE_MESSAGES).format(name=target.mention_html(), time=label)
    if dm_ok: reply += "\n<i>📨 Нарушитель уведомлён в лс</i>"
    await reply_auto_delete(message, reply, parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔇  <b>МУТ</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n⏱ <b>Время:</b> {label}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    add_mod_history(cid, target.id, f"🔇 Мут {label}", command.args or "—", message.from_user.full_name)
    mod_reasons[cid][target.id]["mute"] = f"{label} — {command.args or 'Нарушение правил'}"
    # 🔄 Запуск автоснятия мута
    schedule_unmute(cid, target.id, mins, target.full_name)

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    await reply_auto_delete(message, f"🔊 {target.mention_html()} размучен.", parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔊  <b>РАЗМУТ</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("warn"))
async def cmd_warn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя выдать варн администратору!"); return
    reason = command.args or "Нарушение правил"
    cid = message.chat.id

    # Чистим истёкшие варны перед добавлением
    clean_expired_warns(cid, target.id)
    add_warn_with_expiry(cid, target.id)
    count = warnings[cid][target.id]
    save_data()

    # 📸 Скриншот сообщения-нарушения в лог
    if message.reply_to_message.text:
        await log_violation_screenshot(
            cid, target.id, target.full_name,
            message.reply_to_message.text,
            f"⚡ Варн {count}/{MAX_WARNINGS}", reason,
            message.from_user.full_name, message.chat.title)

    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, target.id)
        warn_expiry[cid][target.id].clear()
        warnings[cid][target.id] = 0
        msg = random.choice(AUTOBAN_MESSAGES).format(name=target.mention_html(), max=MAX_WARNINGS)
        await log_action(f"╔═══════════════════╗\n🔨  <b>АВТОБАН</b>\n╚═══════════════════╝\n\n🤖 <b>Причина:</b> {MAX_WARNINGS} варнов — лимит достигнут\n🎯 <b>Кого:</b> {target.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        add_mod_history(cid, target.id, "🔨 Автобан", f"{MAX_WARNINGS} варнов", message.from_user.full_name)
        # 📨 ЛС нарушителю
        await dm_warn_user(target.id, target.full_name, f"{MAX_WARNINGS} варнов — автобан",
                           message.chat.title, "🔨 Бан", message.from_user.full_name)
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"╔═══════════════════╗\n⚡  <b>ВАРН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n📝 <b>Причина:</b> {reason}\n⚠️ <b>Варнов:</b> {warnings[message.chat.id][target.id]}/{MAX_WARNINGS}\n⏳ <b>Сгорит через:</b> {WARN_EXPIRY_DAYS} дн.\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        add_mod_history(cid, target.id, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, message.from_user.full_name)
        # 📨 ЛС нарушителю
        dm_ok = await dm_warn_user(target.id, target.full_name, reason,
                                    message.chat.title, f"⚡ Варн {count}/{MAX_WARNINGS}",
                                    message.from_user.full_name)
        if dm_ok:
            msg += "\n<i>📨 Нарушитель уведомлён в лс</i>"
    await reply_auto_delete(message, msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def cmd_unwarn(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1
    await reply_auto_delete(message, 
        f"🌿 С {target.mention_html()} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

@dp.message(Command("del"))
async def cmd_del(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    try: await message.reply_to_message.delete()
    except: pass
    try: await message.delete()
    except: pass

@dp.message(Command("clear"))
async def cmd_clear(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError: await reply_auto_delete(message, "⚠️ /clear 20"); return
    deleted = 0
    for i in range(message.message_id, message.message_id - count - 1, -1):
        try: await bot.delete_message(message.chat.id, i); deleted += 1
        except: pass
    sent = await message.answer(f"🧹 Удалено: <b>{deleted}</b> сообщений.", parse_mode="HTML")
    await asyncio.sleep(3)
    try: await sent.delete()
    except: pass

@dp.message(Command("announce"))
async def cmd_announce(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        pending[message.from_user.id] = {"action":"announce_text","target_id":0,"target_name":"","chat_id":message.chat.id}
        await reply_auto_delete(message, "📢 Напиши текст объявления:"); return
    try:
        try: await message.delete()
        except: pass
    except: pass
    await answer_auto_delete(
        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}", parse_mode="HTML")

@dp.message(Command("pin"))
async def cmd_pin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    await bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await reply_auto_delete(message, "📌 Закреплено!")

@dp.message(Command("unpin"))
async def cmd_unpin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await reply_auto_delete(message, "📍 Откреплено!")

@dp.message(Command("lock"))
async def cmd_lock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await reply_auto_delete(message, "🔒 Чат <b>заблокирован</b>.", parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔒  <b>ЧАТ ЗАБЛОКИРОВАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(
        can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
        can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
    await reply_auto_delete(message, "🔓 Чат <b>разблокирован</b>.", parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n🔓  <b>ЧАТ РАЗБЛОКИРОВАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("slowmode"))
async def cmd_slowmode(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: delay = int(command.args) if command.args else 10
    except ValueError: await reply_auto_delete(message, "⚠️ /slowmode 30"); return
    if delay < 0 or delay > 900:
        await reply_auto_delete(message, "⚠️ Значение от 0 до 900 секунд."); return
    try:
        await bot.set_chat_slow_mode_delay(message.chat.id, delay)
        if delay == 0:
            await reply_auto_delete(message, "🐇 Slowmode <b>выключен</b>.", parse_mode="HTML")
        else:
            label = f"{delay} сек." if delay < 60 else f"{delay//60} мин. {delay%60} сек." if delay%60 else f"{delay//60} мин."
            await reply_auto_delete(message, f"🐢 Slowmode: <b>{label}</b>", parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message, f"⚠️ Не удалось установить slowmode: <code>{e}</code>", parse_mode="HTML")

@dp.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    if not command.args:
        await reply_auto_delete(message, "⚠️ Пример: /promote Модератор"); return
    title = command.args.strip()
    if len(title) > 32:
        await reply_auto_delete(message, "⚠️ Тег максимум 32 символа."); return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    try:
        # Новый Bot API 9.5 метод setChatMemberTag
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMemberTag"
            data = {"chat_id": cid, "user_id": target.id, "tag": title}
            async with session.post(url, json=data) as resp:
                result = await resp.json()
        if result.get("ok"):
            await reply_auto_delete(message,
                f"🏷 {target.mention_html()} получил тег: <b>{title}</b>",
                parse_mode="HTML")
        else:
            # Fallback — старый способ через custom title (только для админов)
            await bot.promote_chat_member(
                cid, target.id,
                can_change_info=False, can_post_messages=False,
                can_edit_messages=False, can_delete_messages=False,
                can_invite_users=False, can_restrict_members=False,
                can_pin_messages=False, can_promote_members=False)
            await bot.set_chat_administrator_custom_title(cid, target.id, title[:16])
            await reply_auto_delete(message,
                f"🏅 {target.mention_html()} получил тег: <b>{title}</b>",
                parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message,
            f"⚠️ Ошибка: <code>{e}</code>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("panel:promote:"))
async def cb_panel_promote(call: CallbackQuery):
    if not await check_admin(call.message): return
    tid = int(call.data.split(":")[2])
    try:
        await call.message.edit_text(
            f"🏷 <b>Выдать тег участнику</b>\n\n"
            f"Напиши команду реплаем на сообщение участника:\n"
            f"<code>/promote Название тега</code>\n\n"
            f"Или через аутист команду:\n"
            f"<code>аутист тег @username Название</code>\n\n"
            f"⚠️ Бот должен иметь право <b>can_manage_tags</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{tid}")
            ]]))
    except: pass
    await call.answer()

@dp.message(Command("removetag"))
async def cmd_removetag(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMemberTag"
            data = {"chat_id": cid, "user_id": target.id, "tag": ""}
            async with session.post(url, json=data) as resp:
                result = await resp.json()
        if result.get("ok"):
            await reply_auto_delete(message,
                f"🗑 Тег {target.mention_html()} удалён.", parse_mode="HTML")
        else:
            await reply_auto_delete(message,
                f"⚠️ {result.get('description', 'Ошибка')}", parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message,
            f"⚠️ Ошибка: <code>{e}</code>", parse_mode="HTML")

@dp.message(Command("poll"))
async def cmd_poll(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args or "|" not in command.args:
        await reply_auto_delete(message, "⚠️ /poll Вопрос|Вар1|Вар2"); return
    parts = [p.strip() for p in command.args.split("|")]
    if len(parts) < 3: await reply_auto_delete(message, "⚠️ Нужно минимум 2 варианта."); return
    try: await message.delete()
    except: pass
    await bot.send_poll(message.chat.id, question=parts[0], options=parts[1:], is_anonymous=False)

@dp.message(Command("antimat"))
async def cmd_antimat(message: Message, command: CommandObject):
    global ANTI_MAT_ENABLED
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message, f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on": ANTI_MAT_ENABLED = True; await reply_auto_delete(message, "🧼 Антимат <b>включён</b>.", parse_mode="HTML")
    elif a == "off": ANTI_MAT_ENABLED  = False


async def _antimat_disabled_reply(message, text):
    await reply_auto_delete(message, text, parse_mode="HTML")

@dp.message(Command("autokick"))
async def cmd_autokick(message: Message, command: CommandObject):
    global AUTO_KICK_BOTS
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message, f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML"); return
    a = command.args.strip().lower()
    if a == "on": AUTO_KICK_BOTS = True; await reply_auto_delete(message, "🤖 Автокик <b>включён</b>.", parse_mode="HTML")
    elif a == "off": AUTO_KICK_BOTS = False; await reply_auto_delete(message, "🤖 Автокик <b>выключен</b>.", parse_mode="HTML")

@dp.message(Command("warn24"))
async def cmd_warn24(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя замутить администратора!"); return
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(hours=24))
    await reply_auto_delete(message, f"📵 {target.mention_html()} замучен на <b>24 часа</b> за рекламу.", parse_mode="HTML")
    await log_action(f"╔═══════════════════╗\n📵  <b>МУТ 24ч</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n⏱ <b>Время:</b> 24 часа\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("rban"))
async def cmd_rban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await reply_auto_delete(message, 
        f"🎲 {target.mention_html()} получил <b>шуточный бан</b>!\n"
        f"📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет)</i>",
        parse_mode="HTML")

@dp.message(Command("adminlist"))
async def cmd_adminlist(message: Message):
    if not await require_admin(message): return
    admins = await bot.get_chat_administrators(message.chat.id)
    lines  = ["👮 <b>Администраторы чата:</b>\n"]
    for adm in admins:
        if adm.user.is_bot: continue
        icon  = "👑" if adm.status == "creator" else "🛡"
        title = f" — <i>{adm.custom_title}</i>" if hasattr(adm,"custom_title") and adm.custom_title else ""
        lines.append(f"{icon} {adm.user.mention_html()}{title}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("note"))
async def cmd_note(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "📝 /note set/get/del/list [имя] [текст]"); return
    parts = command.args.split(maxsplit=2); action = parts[0].lower(); cid = message.chat.id
    if action == "set":
        if not await require_admin(message): return
        if len(parts) < 3: await reply_auto_delete(message, "⚠️ /note set [имя] [текст]"); return
        notes[cid][parts[1]] = parts[2]; save_data()
        await reply_auto_delete(message, f"📝 Заметка <b>{parts[1]}</b> сохранена!", parse_mode="HTML")
    elif action == "get":
        if len(parts) < 2: await reply_auto_delete(message, "⚠️ /note get [имя]"); return
        t = notes[cid].get(parts[1])
        await reply_auto_delete(message, f"📄 <b>{parts[1]}:</b>\n{t}" if t else "❌ Заметка не найдена.", parse_mode="HTML")
    elif action == "del":
        if not await require_admin(message): return
        if len(parts) > 1 and parts[1] in notes[cid]:
            del notes[cid][parts[1]]; save_data()
            await reply_auto_delete(message, f"🗑 Заметка <b>{parts[1]}</b> удалена.", parse_mode="HTML")
        else: await reply_auto_delete(message, "❌ Не найдена.")
    elif action == "list":
        keys = list(notes[cid].keys())
        await reply_auto_delete(message, "📋 <b>Заметки:</b>\n" + "\n".join(f"📌 {k}" for k in keys) if keys else "📭 Заметок нет.", parse_mode="HTML")

@dp.message(Command("birthday"))
async def cmd_birthday(message: Message, command: CommandObject):
    if not command.args:
        await reply_auto_delete(message, "🎂 Формат: /birthday ДД.ММ\nПример: <code>/birthday 25.03</code>", parse_mode="HTML"); return
    try:
        day, month = map(int, command.args.strip().split("."))
        if not (1 <= day <= 31 and 1 <= month <= 12): raise ValueError
    except:
        await reply_auto_delete(message, "⚠️ Неверный формат. Пример: /birthday 25.03"); return
    uid = message.from_user.id
    birthdays[uid] = {"day": day, "month": month, "name": message.from_user.full_name, "chat_id": message.chat.id}
    await reply_auto_delete(message, 
        f"🎂 {message.from_user.mention_html()}, день рождения <b>{day:02d}.{month:02d}</b> сохранён!\n🎉 Поздравлю тебя в этот день!",
        parse_mode="HTML")

async def birthday_checker():
    while True:
        from datetime import datetime
        today = datetime.now()
        for uid, data in list(birthdays.items()):
            if data["day"] == today.day and data["month"] == today.month:
                try:
                    await bot.send_message(data["chat_id"],
                        f"🎉🎂 Сегодня день рождения у <a href='tg://user?id={uid}'>{data['name']}</a>!\n\n🎊 Поздравляем! 🥳",
                        parse_mode="HTML")
                except: pass
        await asyncio.sleep(3600)

@dp.message(Command("remind"))
async def cmd_remind(message: Message, command: CommandObject):
    if not command.args or len(command.args.split(maxsplit=1)) < 2:
        await reply_auto_delete(message, 
            "⏰ Формат: /remind 30m текст\n"
            "<code>/remind 10m Написать другу</code>\n"
            "<code>/remind 2h Встреча</code>", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    mins, label = parse_duration(parts[0])
    if not mins: await reply_auto_delete(message, "⚠️ Неверный формат времени. Примеры: 10m, 2h, 1d"); return
    text = parts[1].strip()
    cid = message.chat.id
    await reply_auto_delete(message, f"⏰ Напомню через <b>{label}</b>!\n📝 {text}", parse_mode="HTML")
    async def remind_task():
        await asyncio.sleep(mins * 60)
        try:
            await bot.send_message(cid,
                f"⏰ {message.from_user.mention_html()}, напоминание!\n\n📌 <b>{text}</b>", parse_mode="HTML")
        except: pass
    asyncio.create_task(remind_task())

@dp.message(Command("countdown"))
async def cmd_countdown(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: n = min(int(command.args or 5), 10)
    except: n = 5
    sent = await reply_auto_delete(message, f"⏱ <b>{n}...</b>", parse_mode="HTML")
    for i in range(n-1, 0, -1):
        await asyncio.sleep(1)
        try: await sent.edit_text(f"⏱ <b>{i}...</b>", parse_mode="HTML")
        except: pass
    await asyncio.sleep(1)
    await sent.edit_text("🚀 <b>ПОЕХАЛИ!</b>", parse_mode="HTML")

@dp.message(Command("weather"))
async def cmd_weather(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "🌤 Укажи город: /weather Москва"); return
    wait = await reply_auto_delete(message, "⏳ Получаю данные...")
    await wait.edit_text(await get_weather(command.args), parse_mode="HTML")

@dp.message(Command("afk"))
async def cmd_afk(message: Message, command: CommandObject):
    reason = command.args or "без причины"
    afk_users[message.from_user.id] = reason
    await reply_auto_delete(message, f"😴 {message.from_user.mention_html()} ушёл в AFK: {reason}", parse_mode="HTML")

@dp.message(Command("info"))
async def cmd_info(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    user = message.reply_to_message.from_user
    member = await bot.get_chat_member(message.chat.id, user.id)
    smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор","member":"👤 Участник",
            "restricted":"🔇 Ограничен","kicked":"🔨 Забанен","left":"🚶 Вышел"}
    afk  = f"\n😴 AFK: {afk_users[user.id]}" if user.id in afk_users else ""
    await reply_auto_delete(message, 
        f"🔍 <b>Инфо:</b>\n{user.mention_html()}{afk}\n"
        f"🔗 {'@'+user.username if user.username else 'нет'}\n"
        f"🪪 <code>{user.id}</code>\n"
        f"📌 {smap.get(member.status, member.status)}\n"
        f"⚡ Варнов: <b>{warnings[message.chat.id].get(user.id,0)}/{MAX_WARNINGS}</b>\n"
        f"🌟 Репутация: <b>{reputation[message.chat.id].get(user.id,0):+d}</b>\n"
        f"💬 Сообщений: <b>{chat_stats[message.chat.id].get(user.id,0)}</b>",
        parse_mode="HTML")

@dp.message(Command("warnings"))
async def cmd_warnings(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    await reply_auto_delete(message, 
        f"⚡ {target.mention_html()} — варнов: <b>{warnings[message.chat.id].get(target.id,0)}/{MAX_WARNINGS}</b>",
        parse_mode="HTML")

@dp.message(Command("modhistory"))
async def cmd_modhistory(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника чтобы посмотреть его историю."); return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    history = mod_history[cid].get(target.id, [])
    if not history:
        await reply_auto_delete(message,
            f"📋 История модераций {target.mention_html()}:\n\n✅ Чисто — нарушений не найдено.",
            parse_mode="HTML"); return
    lines = [f"📋 <b>История модераций {target.mention_html()}:</b>\n"]
    for entry in reversed(history):
        lines.append(
            f"{'─'*20}\n"
            f"{entry['action']}\n"
            f"📝 Причина: {entry['reason']}\n"
            f"👮 Модератор: {entry['by']}\n"
            f"🕐 {entry['time']}"
        )
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("tempban"))
async def cmd_tempban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение. Пример: /tempban 3 спам"); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя забанить администратора!"); return
    args = (command.args or "").split(None, 1)
    try:
        days = int(args[0])
        reason = args[1] if len(args) > 1 else "Нарушение правил"
    except (ValueError, IndexError):
        await reply_auto_delete(message, "⚠️ Пример: /tempban 3 спам"); return
    if days < 1 or days > 365:
        await reply_auto_delete(message, "⚠️ Срок от 1 до 365 дней."); return
    cid = message.chat.id
    from datetime import datetime
    # ЛС до бана
    await dm_warn_user(target.id, target.full_name, reason,
                       message.chat.title, f"🔇 Временный бан на {days} дн.", message.from_user.full_name)
    await bot.ban_chat_member(cid, target.id)
    ban_list[cid][target.id] = {
        "name": target.full_name, "reason": reason,
        "by": message.from_user.full_name,
        "time": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "until": (datetime.now().timestamp() + days * 86400),
        "temp": True, "days": days
    }
    mod_reasons[cid][target.id]["ban"] = reason
    add_mod_history(cid, target.id, f"🔇 Темпбан {days} дн.", reason, message.from_user.full_name)
    # Запускаем таймер снятия
    key = (cid, target.id)
    old = tempban_timers.get(key)
    if old: old.cancel()
    tempban_timers[key] = asyncio.create_task(tempban_unban(cid, target.id, target.full_name, days))
    await reply_auto_delete(message,
        f"🔇 <b>{target.mention_html()}</b> временно забанен на <b>{days} дн.</b>\n"
        f"📝 Причина: {reason}\n"
        f"🔓 Разбан: автоматически через {days} дн.", parse_mode="HTML")
    await log_action(
        f"🔇 <b>ТЕМПБАН</b>\nКто: {message.from_user.mention_html()}\n"
        f"Кого: {target.mention_html()}\nСрок: {days} дн.\nПричина: {reason}\nЧат: {message.chat.title}")

@dp.message(Command("banlist"))
async def cmd_banlist(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    bans = ban_list[cid]
    if not bans:
        await reply_auto_delete(message, "👥 Список забаненных пуст."); return
    from datetime import datetime
    lines = [f"👥 <b>Забаненные участники ({len(bans)}):</b>\n"]
    for uid, info in list(bans.items()):
        until = ""
        if info.get("temp") and info.get("until"):
            dt = datetime.fromtimestamp(info["until"]).strftime("%d.%m.%Y %H:%M")
            until = f" (до {dt})"
        lines.append(
            f"{'─'*18}\n"
            f"👤 <b>{info['name']}</b> (<code>{uid}</code>)\n"
            f"📝 Причина: {info['reason']}\n"
            f"👮 Кто: {info['by']}\n"
            f"🕐 {info['time']}{until}"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n\n<i>...список обрезан</i>"
    await reply_auto_delete(message, text, parse_mode="HTML")

@dp.message(Command("modexport"))
async def cmd_modexport(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    target_uid = None
    if message.reply_to_message:
        target_uid = message.reply_to_message.from_user.id

    from datetime import datetime
    import io
    lines = [f"=== ИСТОРИЯ МОДЕРАЦИЙ | {message.chat.title} ===",
             f"Экспорт: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"]

    if target_uid:
        history = mod_history[cid].get(target_uid, [])
        lines.append(f"--- Участник ID {target_uid} ({len(history)} записей) ---")
        for e in history:
            lines.append(f"[{e['time']}] {e['action']} | Причина: {e['reason']} | Модератор: {e['by']}")
    else:
        total = 0
        for uid, history in mod_history[cid].items():
            if not history: continue
            lines.append(f"\n--- ID {uid} ({len(history)} записей) ---")
            for e in history:
                lines.append(f"[{e['time']}] {e['action']} | Причина: {e['reason']} | Модератор: {e['by']}")
            total += len(history)
        lines.append(f"\nИтого записей: {total}")

    content = "\n".join(lines).encode("utf-8")
    file = io.BytesIO(content)
    file.name = f"modhistory_{cid}_{datetime.now().strftime('%Y%m%d')}.txt"
    from aiogram.types import BufferedInputFile
    sent = await message.reply_document(
        BufferedInputFile(content, filename=file.name),
        caption="🧾 История модераций экспортирована")
    asyncio.create_task(auto_delete(message, sent))

@dp.message(Command("modtop"))
async def cmd_modtop(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    stats = mod_stats[cid]
    if not stats:
        await reply_auto_delete(message, "👮 Пока никто ничего не модерировал."); return
    sorted_mods = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    lines = ["👮 <b>Топ модераторов:</b>\n"]
    for i, (name, count) in enumerate(sorted_mods[:10]):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{medal} <b>{name}</b> — {count} действий")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

def kb_warn_templates(target_id: int) -> InlineKeyboardMarkup:
    rows = []
    items = list(WARN_TEMPLATES.items())
    for i in range(0, len(items), 2):
        row = []
        for tid, tmpl in items[i:i+2]:
            row.append(InlineKeyboardButton(
                text=tmpl["label"],
                callback_data=f"wt:{tid}:{target_id}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"wt:cancel:{target_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("warnmenu"))
async def cmd_warnmenu(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "🚫 Нельзя варнить администратора!"); return
    sent = await message.reply(
        f"📝 <b>Шаблон предупреждения для {target.mention_html()}</b>\n\n"
        f"Выбери причину — варн выдастся автоматически:",
        parse_mode="HTML",
        reply_markup=kb_warn_templates(target.id))
    asyncio.create_task(auto_delete(message, sent))

@dp.callback_query(F.data.startswith("wt:"))
async def cb_warn_template(call: CallbackQuery):
    parts = call.data.split(":")
    tid, target_id = parts[1], int(parts[2])
    if tid == "cancel":
        await call.message.delete(); await call.answer(); return
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    tmpl = WARN_TEMPLATES.get(tid)
    if not tmpl:
        await call.answer("❌ Шаблон не найден!", show_alert=True); return
    cid = call.message.chat.id
    reason = tmpl["text"]
    clean_expired_warns(cid, target_id)
    add_warn_with_expiry(cid, target_id)
    count = warnings[cid][target_id]
    save_data()
    add_mod_history(cid, target_id, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, call.from_user.full_name)
    await dm_warn_user(target_id, f"ID{target_id}", reason, call.message.chat.title,
                       f"⚡ Варн {count}/{MAX_WARNINGS}", call.from_user.full_name)
    if count >= MAX_WARNINGS:
        await bot.ban_chat_member(cid, target_id)
        warn_expiry[cid][target_id].clear(); warnings[cid][target_id] = 0
        await call.message.edit_text(
            f"🔨 <b>Автобан!</b> Достигнут лимит {MAX_WARNINGS} варнов.\n📝 {reason}",
            parse_mode="HTML")
        asyncio.create_task(schedule_delete(call.message))
        await log_action(f"╔═══════════════════╗\n🔨  <b>АВТОБАН</b> (шаблон)\n╚═══════════════════╝\n\n🎯 <b>Кого:</b> <code>{target_id}</code>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    else:
        await call.message.edit_text(
            f"⚡ <b>Варн выдан!</b> {tmpl['label']}\n"
            f"📝 {reason}\n"
            f"⚠️ Варнов: <b>{count}/{MAX_WARNINGS}</b>",
            parse_mode="HTML")
        asyncio.create_task(schedule_delete(call.message))
        await log_action(f"╔═══════════════════╗\n⚡  <b>ВАРН</b> (шаблон)\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кому:</b> <code>{target_id}</code>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.answer(f"✅ {tmpl['label']}")

@dp.message(Command("rep"))
async def cmd_rep(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "↩️ Ответь на сообщение."); return
    target = message.reply_to_message.from_user
    score  = reputation[message.chat.id].get(target.id, 0)
    await reply_auto_delete(message, 
        f"{'🌟' if score>=0 else '💀'} Репутация {target.mention_html()}: <b>{score:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"+1", "+", "👍"}))
async def rep_plus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await reply_auto_delete(message, "😏 Себе репу не накручивай!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await reply_auto_delete(message, f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] += 1
    save_data()
    await reply_auto_delete(message, 
        f"⬆️ {target.mention_html()} +1 к репутации! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>",
        parse_mode="HTML")

@dp.message(F.text.in_({"-1", "👎"}))
async def rep_minus(message: Message):
    if not message.reply_to_message: return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id: await reply_auto_delete(message, "😏 Себе репу не снижай!"); return
    key = (message.chat.id, message.from_user.id, target.id); now = time()
    if key in rep_cooldown and now - rep_cooldown[key] < 3600:
        await reply_auto_delete(message, f"⏳ Подожди ещё {int(3600-(now-rep_cooldown[key]))//60} мин."); return
    rep_cooldown[key] = now
    reputation[message.chat.id][target.id] -= 1
    save_data()
    await reply_auto_delete(message, 
        f"⬇️ {target.mention_html()} -1 к репутации! Теперь: <b>{reputation[message.chat.id][target.id]:+d}</b>",
        parse_mode="HTML")

@dp.message(Command("ранг"))
async def cmd_rank(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid, cid = user.id, message.chat.id
    xp = xp_data[cid][uid]; lvl = levels[cid][uid]
    xp_current = xp % 100
    bar = "🟩" * (xp_current // 10) + "⬜" * (10 - xp_current // 10)
    title = (
        "👑 Элита" if lvl >= 20 else "🏆 Легенда" if lvl >= 10 else
        "⚔️ Ветеран" if lvl >= 5 else "🌱 Активный" if lvl >= 3 else
        "🔰 Участник" if lvl >= 1 else "🐣 Новичок")
    await reply_auto_delete(message, 
        f"📊 <b>Уровень {user.mention_html()}</b>\n\n"
        f"🏅 Титул: <b>{title}</b>\n⚡ Уровень: <b>{lvl}</b>\n"
        f"✨ Опыт: <b>{xp_current}/100</b>\n[{bar}]", parse_mode="HTML")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats: await reply_auto_delete(message, "📊 Статистика пока пуста!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = ["🏆 <b>Топ активных участников:</b>\n"]
    for i, (uid, count) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {count} сообщений")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


@dp.message(Command("roll"))
async def cmd_roll(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 10000))
    except: sides = 6
    await reply_auto_delete(message, f"🎲 Бросаю d{sides}... выпало: <b>{random.randint(1,sides)}</b>!", parse_mode="HTML")

@dp.message(Command("flip"))
async def cmd_flip(message: Message):
    await reply_auto_delete(message, random.choice(["🦅 Орёл!", "🪙 Решка!"]))

@dp.message(Command("8ball"))
async def cmd_8ball(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "❓ /8ball [вопрос]"); return
    await reply_auto_delete(message, 
        f"🎱 <b>Вопрос:</b> {command.args}\n\n<b>Ответ:</b> {random.choice(BALL_ANSWERS)}", parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "⚠️ /rate [что]"); return
    score = random.randint(0, 10)
    await reply_auto_delete(message, 
        f"⭐ <b>{command.args}</b>\n{'🌟'*score+'☆'*(10-score)}\nОценка: <b>{score}/10</b>", parse_mode="HTML")

@dp.message(Command("iq"))
async def cmd_iq(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    iq   = random.randint(1, 200)
    c    = "🥔 Картошка умнее." if iq<70 else ("🐒 Обезьяна лучше." if iq<100 else ("😐 Сойдёт." if iq<130 else ("🧠 Умный!" if iq<160 else "🤖 Настоящий гений!")))
    await reply_auto_delete(message, f"🧠 IQ {user.mention_html()}: <b>{iq}</b>\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def cmd_gay(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    p    = random.randint(0, 100)
    await reply_auto_delete(message, 
        f"🌈 {user.mention_html()}\n{'🟣'*(p//10)+'⬜'*(10-p//10)}\n<b>{p}%</b> — это шутка 😄",
        parse_mode="HTML")

@dp.message(Command("truth"))
async def cmd_truth(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"🤔 <b>Вопрос для {user.mention_html()}:</b>\n\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML")

@dp.message(Command("dare"))
async def cmd_dare(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"😈 <b>Задание для {user.mention_html()}:</b>\n\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML")

@dp.message(Command("wyr"))
async def cmd_wyr(message: Message):
    await reply_auto_delete(message, f"🎯 <b>Выбор без выбора:</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode="HTML")

@dp.message(Command("rps"))
async def cmd_rps(message: Message, command: CommandObject):
    choices = {"к":"✊ Камень","н":"✌️ Ножницы","б":"🖐 Бумага"}
    wins    = {"к":"н","н":"б","б":"к"}
    if not command.args or command.args.lower() not in choices:
        await reply_auto_delete(message, "✊ /rps к — камень, /rps н — ножницы, /rps б — бумага"); return
    p = command.args.lower(); b = random.choice(list(choices.keys()))
    res = "🤝 Ничья!" if p==b else ("🎉 Ты выиграл!" if wins[p]==b else "😈 Я выиграл!")
    await reply_auto_delete(message, f"Ты: {choices[p]}\nЯ: {choices[b]}\n\n{res}")

@dp.message(Command("choose"))
async def cmd_choose(message: Message, command: CommandObject):
    if not command.args or "|" not in command.args:
        await reply_auto_delete(message, "⚠️ /choose вар1|вар2|вар3"); return
    options = [o.strip() for o in command.args.split("|") if o.strip()]
    if len(options) < 2: await reply_auto_delete(message, "⚠️ Минимум 2 варианта."); return
    await reply_auto_delete(message, f"🎯 Выбираю... ✅ <b>{random.choice(options)}</b>!", parse_mode="HTML")

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    sign, text = random.choice(list(HOROSCOPES.items()))
    await reply_auto_delete(message, f"{sign} <b>Гороскоп для {user.mention_html()}:</b>\n\n{text}", parse_mode="HTML")

@dp.message(Command("predict"))
async def cmd_predict(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"🔮 <b>Предсказание для {user.mention_html()}:</b>\n\n{random.choice(PREDICTIONS)}", parse_mode="HTML")

@dp.message(Command("совместимость"))
async def cmd_compatibility(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника!"); return
    user1 = message.from_user; user2 = message.reply_to_message.from_user
    if user1.id == user2.id: await reply_auto_delete(message, "😏 Сам с собой? Интересно..."); return
    percent = (user1.id * user2.id) % 101
    bar = "❤️" * (percent // 10) + "🖤" * (10 - percent // 10)
    if percent >= 80:   verdict = "💍 Идеальная пара! Женитесь!"
    elif percent >= 60: verdict = "💕 Хорошая совместимость!"
    elif percent >= 40: verdict = "😊 Неплохо, есть шанс!"
    elif percent >= 20: verdict = "😬 Сложно, но возможно..."
    else:               verdict = "💔 Катастрофа! Держитесь подальше!"
    await reply_auto_delete(message, 
        f"💘 <b>Совместимость:</b>\n\n👤 {user1.mention_html()}\n{bar}\n👤 {user2.mention_html()}\n\n<b>{percent}%</b> — {verdict}",
        parse_mode="HTML")


@dp.message(Command("botstats"))
async def cmd_botstats(message: Message):
    if not await require_admin(message): return
    total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
    total_warns = sum(sum(v.values()) for v in warnings.values())
    await reply_auto_delete(message, 
        f"📈 <b>Статистика бота</b>\n\n💬 Сообщений: <b>{total_msgs}</b>\n"
        f"⚡ Варнов: <b>{total_warns}</b>\n😴 AFK: <b>{len(afk_users)}</b>\n"
        f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
        f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
        f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML")

@dp.message(F.text & ~F.text.startswith("/"))
async def autist_commands(message: Message):
    if not message.text: return
    text_lower = message.text.strip().lower()
    if not text_lower.startswith("аутист"): return
    fun_only = ["обозвать", "поженить", "казнить", "диагноз", "профессия", "похитить", "дуэль"]
    is_owner = message.from_user.id == OWNER_ID
    is_admin = is_owner or await check_admin(message)
    is_fun = any(f in text_lower for f in fun_only)
    if not is_admin and not is_fun: return
    parts = text_lower.split(maxsplit=1)
    if len(parts) < 2: return
    rest = parts[1].strip()
    action = None
    for cmd in ["снять варн","разварн","размут","разбан","варн","мут навсегда","мут","бан","захуесосить","кик",
                "тег","убрать тег","очистить","удалить","закрепить","предупредить","инфо","варны","репутация",
                "обозвать","поженить","проверить","казнить","диагноз","профессия","похитить","дуэль","экзамен",
                # 🛡 Модераторские
                "статус","чистка","поиск","антиспам",
                "алерт","пересмотр","последний","заморозка",
                "объяви","история","уровень","только чтение","топ нарушителей","медиамут","причина","напомни мод",
                # 👑 Owner
                "ядерка","анонс","локдаун","маска","клоун",
                "слежка","дать репу","хаос","сброс","лотерея","смерть","зеркало",
                "скрин","взрыв","корона","вызов","шпион","жребий","громко","молния","магнит","цель",
                "напомни","закреп","голос","рост","тишина",
                "температура","неделя","режим","лог","рестарт","сос",
                "стикермут","гифмут","войсмут","всёмут"]:
        if rest.startswith(cmd):
            action = cmd; rest = rest[len(cmd):].strip(); break
    if not action: return
    cid = message.chat.id
    target = None

    # Команды которым target не нужен
    NO_TARGET_CMDS = {"статус", "хаос", "скрин", "взрыв", "шпион", "жребий", "громко",
                      "антиспам", "зеркало", "локдаун", "анонс", "лотерея",
                      "тишина", "история", "топ нарушителей",
                      "температура", "неделя", "рестарт", "сос", "лог"}

    # ── Поиск цели: реплай или @юзернейм или ID ──
    import re as _re
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        # Пробуем найти @username или числовой ID в rest
        username_match = _re.match(r"^@?([A-Za-z]\w{3,})", rest)
        id_match = _re.match(r"^(-?\d+)", rest)
        if id_match:
            try:
                uid_target = int(id_match.group(1))
                tm = await bot.get_chat_member(cid, uid_target)
                target = tm.user
                rest = rest[id_match.end():].strip()
            except: pass
        elif username_match:
            uname = username_match.group(1).lstrip("@")
            try:
                tm = await bot.get_chat_member(cid, f"@{uname}")
                target = tm.user
                rest = rest[username_match.end():].strip()
            except:
                for uid in chat_stats[cid]:
                    try:
                        tm = await bot.get_chat_member(cid, uid)
                        if tm.user.username and tm.user.username.lower() == uname.lower():
                            target = tm.user
                            rest = rest[username_match.end():].strip()
                            break
                    except: pass

    if not target and action not in NO_TARGET_CMDS:
        await reply_auto_delete(message, "↩️ Ответь на сообщение или укажи @юзернейм / ID."); return

    duration_mins = None; duration_label = None; reason = "Нарушение правил"
    time_match = _re.match(r"^(\d+)\s*(д|ч|м)\s*", rest)
    if time_match:
        num = int(time_match.group(1)); unit = time_match.group(2)
        if unit == "д":   duration_mins = num * 1440; duration_label = f"{num} дн."
        elif unit == "ч": duration_mins = num * 60;   duration_label = f"{num} ч."
        elif unit == "м": duration_mins = num;         duration_label = f"{num} мин."
        reason_part = rest[time_match.end():].strip()
        if reason_part: reason = reason_part
    else:
        if rest.strip(): reason = rest.strip()
    tname = target.mention_html() if target else "участник"
    try:
        if action == "бан":
            if duration_mins:
                await bot.ban_chat_member(cid, target.id, until_date=timedelta(minutes=duration_mins))
                await reply_auto_delete(message, f"🔨 {tname} забанен на <b>{duration_label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            else:
                await bot.ban_chat_member(cid, target.id)
                await reply_auto_delete(message, f"🔨 {tname} забанен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"╔═══════════════════╗\n🔨  <b>БАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "захуесосить":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await reply_auto_delete(message, f"👢 {tname} захуесошен из чата!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"╔═══════════════════╗\n👢  <b>КИК</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "кик":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await reply_auto_delete(message, f"👟 {tname} кикнут из чата!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"╔═══════════════════╗\n👟  <b>КИК</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "мут":
            mins = duration_mins or 60; label = duration_label or "1 ч."
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            await reply_auto_delete(message, f"🔇 {tname} замучен на <b>{label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"╔═══════════════════╗\n🔇  <b>МУТ</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n⏱ <b>Время:</b> {label}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "мут навсегда":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(can_send_messages=False))
            await reply_auto_delete(message, f"🔇 {tname} замучен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
        elif action == "варн":
            # x2 если цель
            import time as _tw
            mult = 2 if target_doubles.get(f"{cid}_{target.id}", 0) > _tw.time() else 1
            for _ in range(mult): warnings[cid][target.id] += 1
            count = warnings[cid][target.id]; save_data()
            # Если указано время — автосброс варна
            if duration_mins:
                import asyncio as _aio
                async def _auto_unwarn(c, u, delay):
                    await _aio.sleep(delay * 60)
                    if warnings[c][u] > 0: warnings[c][u] -= 1; save_data()
                _aio.create_task(_auto_unwarn(cid, target.id, duration_mins))
            if count >= MAX_WARNINGS:
                await bot.ban_chat_member(cid, target.id); warnings[cid][target.id] = 0
                await reply_auto_delete(message, f"🔨 {tname} — {MAX_WARNINGS} варна, автобан!\n📝 Причина: {reason}", parse_mode="HTML")
                await log_action(f"╔═══════════════════╗\n🔨  <b>АВТОБАН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n🤖 <b>Причина:</b> лимит варнов\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
            else:
                time_note = f"\n⏰ Автосброс через: <b>{duration_label}</b>" if duration_mins else ""
                await reply_auto_delete(message, f"⚡ {tname} получил варн <b>{count}/{MAX_WARNINGS}</b>!\n📝 Причина: {reason}{time_note}", parse_mode="HTML")
                await log_action(f"╔═══════════════════╗\n⚡  <b>ВАРН</b>\n╚═══════════════════╝\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action in ("снять варн", "разварн"):
            if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1; save_data()
            await reply_auto_delete(message, f"🌿 С {tname} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "разбан":
            await bot.unban_chat_member(cid, target.id, only_if_banned=True)
            await reply_auto_delete(message, f"🕊 {tname} разбанен.", parse_mode="HTML")
        elif action == "размут":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            await reply_auto_delete(message, f"🔊 {tname} размучен.", parse_mode="HTML")
        elif action == "тег":
            tag_text = rest.strip() or reason
            if not tag_text or tag_text == "Нарушение правил":
                await reply_auto_delete(message, "⚠️ Укажи название тега. Пример: <code>аутист тег @user Сигма</code>", parse_mode="HTML"); return
            import aiohttp
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMemberTag"
                data = {"chat_id": cid, "user_id": target.id, "tag": tag_text[:32]}
                async with session.post(url, json=data) as resp:
                    result = await resp.json()
            if result.get("ok"):
                await reply_auto_delete(message, f"🏷 {tname} получил тег: <b>{tag_text}</b>", parse_mode="HTML")
            else:
                err = result.get('description', 'Ошибка')
                await reply_auto_delete(message, f"⚠️ {err}\n\n<i>Убедись что у бота есть право can_manage_tags</i>", parse_mode="HTML")
        elif action == "убрать тег":
            import aiohttp
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMemberTag"
                data = {"chat_id": cid, "user_id": target.id, "tag": ""}
                async with session.post(url, json=data) as resp:
                    result = await resp.json()
            if result.get("ok"):
                await reply_auto_delete(message, f"🗑 Тег {tname} удалён.", parse_mode="HTML")
            else:
                await reply_auto_delete(message, f"⚠️ {result.get('description', 'Ошибка')}", parse_mode="HTML")
        elif action == "удалить":
            try: await message.reply_to_message.delete(); await reply_auto_delete(message, "🗑 Сообщение удалено!")
            except: await reply_auto_delete(message, "⚠️ Не удалось удалить сообщение.")
        elif action == "закрепить":
            try: await bot.pin_chat_message(cid, message.reply_to_message.message_id); await reply_auto_delete(message, "📌 Сообщение закреплено!")
            except: await reply_auto_delete(message, "⚠️ Не удалось закрепить сообщение.")
        elif action == "предупредить":
            text_warn = rest.strip() or "Нарушение правил"
            await reply_auto_delete(message, f"⚠️ Внимание {tname}!\n📝 {text_warn}", parse_mode="HTML")
        elif action == "очистить":
            count = duration_mins or 10; deleted = 0
            for i in range(message.message_id, message.message_id - count - 1, -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            await reply_auto_delete(message, f"🧹 Удалено <b>{deleted}</b> сообщений!", parse_mode="HTML")
        elif action == "инфо":
            member = await bot.get_chat_member(cid, target.id)
            smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор",
                    "member":"👤 Участник","restricted":"🔇 Ограничен","kicked":"🔨 Забанен"}
            username = f"@{target.username}" if target.username else "нет"
            await reply_auto_delete(message, 
                f"🔍 <b>Инфо:</b>\n{tname}\n🔗 Юзернейм: <b>{username}</b>\n"
                f"🪪 ID: <code>{target.id}</code>\n📌 {smap.get(member.status, member.status)}\n"
                f"⚡ Варнов: <b>{warnings[cid].get(target.id,0)}/{MAX_WARNINGS}</b>\n"
                f"🌟 Репутация: <b>{reputation[cid].get(target.id,0):+d}</b>\n"
                f"💬 Сообщений: <b>{chat_stats[cid].get(target.id,0)}</b>", parse_mode="HTML")
        elif action == "варны":
            count = warnings[cid].get(target.id, 0)
            await reply_auto_delete(message, f"⚡ Варнов у {tname}: <b>{count}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "репутация":
            rep = reputation[cid].get(target.id, 0)
            await reply_auto_delete(message, f"🌟 Репутация {tname}: <b>{rep:+d}</b>", parse_mode="HTML")
        elif action == "обозвать":
            обзывалки = ["🤡 клоун","🥴 тупица","🐸 лягушка","🦆 утка","🐷 хрюша","🤪 псих",
                         "🦊 хитрая лиса","🐌 улитка","🦄 единорог","🤖 сломанный робот","🥔 картошка","🧟 зомби"]
            await reply_auto_delete(message, f"😂 {tname} отныне ты — <b>{random.choice(обзывалки)}</b>!", parse_mode="HTML")
        elif action == "поженить":
            await reply_auto_delete(message, 
                f"💍 Объявляю вас мужем и женой!\n\n👰 {target.mention_html()}\n🤵 {message.from_user.mention_html()}\n\n💑 Горько! 🥂",
                parse_mode="HTML")
        elif action == "казнить":
            казни = ["🔥 сожжён на костре","⚡ поражён молнией","🐊 съеден крокодилом",
                     "🍌 подавился бананом","🚀 отправлен в космос без скафандра",
                     "🌊 утоплен в стакане воды","🐝 закусан пчёлами",
                     "🎸 заслушан до смерти Шансоном","🥄 побит ложкой","🌵 упал на кактус"]
            await reply_auto_delete(message, 
                f"⚰️ {tname} приговорён к казни!\n💀 Способ: <b>{random.choice(казни)}</b>", parse_mode="HTML")
        elif action == "диагноз":
            диагнозы = ["🧠 Хроническая адекватность","🤡 Острый клоунизм","😴 Синдром вечного AFK",
                        "🥔 Картофельный синдром","🐒 Обезьяний рефлекс","💤 Хроническая сонливость",
                        "🌵 Колючесть характера","🤖 Роботизация мозга","🦆 Утиная походка","🌈 Радужное мышление"]
            await reply_auto_delete(message, f"🏥 Диагноз для {tname}:\n📋 <b>{random.choice(диагнозы)}</b>", parse_mode="HTML")
        elif action == "профессия":
            профессии = ["🤡 Профессиональный клоун","🥔 Картофелевод","🐒 Дрессировщик обезьян",
                         "🌵 Смотритель кактусов","🦆 Переводчик с утиного","🤖 Ремонтник роботов",
                         "💤 Профессиональный соня","🎸 Игрок на банджо","🌈 Художник радуг","🧠 Продавец мозгов"]
            await reply_auto_delete(message, f"💼 Профессия {tname}:\n<b>{random.choice(профессии)}</b>", parse_mode="HTML")
        elif action == "похитить":
            mins = duration_mins or 5
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=mins))
            await reply_auto_delete(message, 
                f"👽 {tname} похищен пришельцами на <b>{mins} мин</b>!\n🛸 Вернётся через {mins} минут...",
                parse_mode="HTML")
        elif action == "дуэль":
            challenger = message.from_user
            winner = random.choice([challenger, target])
            loser = target if winner == challenger else challenger
            await reply_auto_delete(message, 
                f"⚔️ <b>ДУЭЛЬ!</b>\n\n🔫 {challenger.mention_html()} vs {tname}\n\n"
                f"🏆 Победитель: <b>{winner.mention_html()}</b>\n💀 Проигравший: {loser.mention_html()}",
                parse_mode="HTML")
        elif action == "экзамен":
            вопросы = ["🧠 Сколько будет 2+2*2?","🌍 Какая столица Франции?",
                       "🔢 Назови простое число от 10 до 20.","🐘 Какое животное самое большое на суше?",
                       "🌊 Какой самый глубокий океан?","🎨 Смешай красный и синий — какой цвет получится?",
                       "⚡ Кто придумал лампочку?","🦁 Царь зверей — это?",
                       "🌙 Как называется спутник Земли?","🍎 Какой фрукт упал на Ньютона?"]
            await reply_auto_delete(message, 
                f"📝 <b>ЭКЗАМЕН для {tname}!</b>\n\n{random.choice(вопросы)}\n\n⏰ У тебя <b>30 секунд</b>!",
                parse_mode="HTML")
        elif action == "проверить":
            await reply_auto_delete(message, f"ℹ️ Функция проверки (капча) отключена.", parse_mode="HTML")

        # ══════════════════════════════════════════
        #  🛡 КОМАНДЫ ДЛЯ МОДЕРАТОРОВ
        # ══════════════════════════════════════════
        elif action == "статус":
            from datetime import datetime
            today = datetime.now().strftime("%d.%m.%Y")
            w_today = sum(1 for uid in warnings[cid] for _ in range(warnings[cid][uid]))
            b_today = len(ban_list[cid])
            history_today = [h for uid_h in mod_history[cid].values() for h in uid_h
                             if h.get("time","").startswith(today)]
            lines = [f"📊 <b>Статус модерации — {today}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            lines.append(f"⚡ Варнов всего: <b>{w_today}</b>")
            lines.append(f"🔨 Банов всего: <b>{b_today}</b>")
            lines.append(f"📋 Действий сегодня: <b>{len(history_today)}</b>\n")
            for h in history_today[-10:]:
                lines.append(f"▸ {h.get('action','?')} — {h.get('by','?')}")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "чистка":
            mins = duration_mins or 30
            import time as _tc
            cutoff = _tc.time() - (mins * 60)
            msgs_to_delete = [
                msg_id for msg_id, ts in user_msg_ids[cid].get(target.id, [])
                if ts >= cutoff
            ]
            deleted = 0
            for msg_id in msgs_to_delete:
                try:
                    await bot.delete_message(cid, msg_id)
                    deleted += 1
                except: pass
            # Очистить удалённые из кэша
            user_msg_ids[cid][target.id] = [
                (mid, ts) for mid, ts in user_msg_ids[cid].get(target.id, [])
                if ts < cutoff
            ]
            await reply_auto_delete(message,
                f"🧹 Удалено <b>{deleted}</b> сообщений {tname} за последние <b>{mins} мин</b>",
                parse_mode="HTML")

        elif action == "поиск":
            w = warnings[cid].get(target.id, 0)
            r = reputation[cid].get(target.id, 0)
            msgs = chat_stats[cid].get(target.id, 0)
            xp = xp_data[cid].get(target.id, 0)
            lvl = levels[cid].get(target.id, 0)
            history = mod_history[cid].get(target.id, [])
            notes_list = user_notes[cid].get(target.id, [])
            in_ban = target.id in ban_list[cid]
            lines = [
                f"🔍 <b>ДОСЬЕ: {tname}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n",
                f"🪪 ID: <code>{target.id}</code>",
                f"🔗 @{target.username}" if target.username else "🔗 Юзернейм: нет",
                f"⚡ Варнов: <b>{w}/{MAX_WARNINGS}</b>",
                f"⭐ Репутация: <b>{r:+d}</b>",
                f"📈 XP: <b>{xp}</b> | Уровень: <b>{lvl}</b>",
                f"💬 Сообщений: <b>{msgs}</b>",
                f"🔨 В бане: <b>{'да' if in_ban else 'нет'}</b>",
            ]
            if history:
                lines.append(f"\n📋 История ({len(history)} действий):")
                for h in history[-5:]:
                    lines.append(f"  ▸ {h.get('action','?')} — {h.get('reason','?')} ({h.get('by','?')})")
            if notes_list:
                lines.append(f"\n📝 Заметки ({len(notes_list)}):")
                for n in notes_list[-3:]:
                    lines.append(f"  ▸ {n['text']} ({n['date']})")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "антиспам":
            arg = rest.strip().lower()
            if arg == "вкл":
                await bot.set_chat_permissions(cid, ChatPermissions(
                    can_send_messages=True, can_send_media_messages=False,
                    can_send_polls=False, can_send_other_messages=False))
                await reply_auto_delete(message, "⏰ <b>Антиспам режим включён!</b>\nТолько текст, без медиа.", parse_mode="HTML")
            elif arg == "выкл":
                await bot.set_chat_permissions(cid, ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=True,
                    can_add_web_page_previews=True))
                await reply_auto_delete(message, "✅ <b>Антиспам режим выключен!</b>", parse_mode="HTML")
            else:
                await reply_auto_delete(message, "⚠️ Укажи: <b>аутист антиспам вкл</b> или <b>выкл</b>", parse_mode="HTML")

        # ══════════════════════════════════════════
        #  👑 OWNER ONLY КОМАНДЫ
        # ══════════════════════════════════════════
        elif action == "ядерка":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            # Варн + мут навсегда + удалить сообщения
            warnings[cid][target.id] += 1
            await bot.restrict_chat_member(cid, target.id, ChatPermissions(can_send_messages=False))
            deleted = 0
            for i in range(message.message_id, max(message.message_id - 200, 0), -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            save_data()
            await reply_auto_delete(message,
                f"💣 <b>ЯДЕРКА</b>\n\n👤 {tname}\n"
                f"⚡ Варн выдан\n🔇 Мут навсегда\n🗑 Удалено ~{deleted} сообщений",
                parse_mode="HTML")
            await log_action(f"💣 <b>ЯДЕРКА</b>\n👤 {tname}\n🏠 {message.chat.title}")

        elif action == "анонс":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            text = rest.strip()
            if not text:
                await reply_auto_delete(message, "⚠️ Укажи текст: <b>аутист анонс текст</b>", parse_mode="HTML"); return
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n{text}\n\n━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode="HTML")

        elif action == "локдаун":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            arg = rest.strip().lower()
            if arg == "выкл":
                await bot.set_chat_permissions(cid, ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=True,
                    can_add_web_page_previews=True))
                await reply_auto_delete(message, "🔓 <b>Локдаун снят!</b> Чат открыт.", parse_mode="HTML")
            else:
                await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
                await reply_auto_delete(message,
                    "🔐 <b>ЛОКДАУН!</b>\n\nЧат закрыт для всех участников.\n"
                    "Снять: <b>аутист локдаун выкл</b>", parse_mode="HTML")

        elif action == "маска":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            text = rest.strip()
            if not text:
                await reply_auto_delete(message, "⚠️ Укажи текст после юзернейма", parse_mode="HTML"); return
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"👤 <b>{target.full_name}</b>:\n{text}", parse_mode="HTML")

        elif action == "клоун":
            if not await check_admin(message) and message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для администраторов!"); return
            clown_targets[f"{cid}_{target.id}"] = __import__('time').time() + 600
            await reply_auto_delete(message,
                f"🤡 {tname} теперь клоун на <b>10 минут</b>!", parse_mode="HTML")

        # ── Предыдущий набор (слежка, репа, хаос, сброс, лотерея, смерть, зеркало) ──
        elif action == "слежка":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import time as _t
            spy_targets[f"{cid}_{target.id}"] = OWNER_ID
            await reply_auto_delete(message, f"👁 Слежка за {tname} включена!\nКаждое сообщение придёт тебе в личку.", parse_mode="HTML")

        elif action == "дать репу":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import re as _re
            m2 = _re.match(r"^(-?\d+)", rest)
            amount = int(m2.group(1)) if m2 else 100
            reputation[cid][target.id] += amount
            save_data()
            await reply_auto_delete(message,
                f"💰 {tname}: репа {'+'if amount>0 else ''}{amount} → <b>{reputation[cid][target.id]}</b>", parse_mode="HTML")

        elif action == "хаос":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            members = list(chat_stats[cid].keys())
            if not members:
                await reply_auto_delete(message, "⚠️ Нет участников!"); return
            victim = random.choice(members)
            roll = random.choice(["варн","мут","ничего","ничего","разварн"])
            try: tm2 = await bot.get_chat_member(cid, victim); vname = tm2.user.mention_html()
            except: vname = f"<code>{victim}</code>"
            if roll == "варн":
                warnings[cid][victim] += 1; save_data()
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 Жертва: {vname}\n⚡ Получил варн!", parse_mode="HTML")
            elif roll == "мут":
                await bot.restrict_chat_member(cid, victim, ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=5))
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 Жертва: {vname}\n🔇 Замучен на 5 мин!", parse_mode="HTML")
            elif roll == "разварн":
                if warnings[cid][victim] > 0: warnings[cid][victim] -= 1; save_data()
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 Счастливчик: {vname}\n🌿 Снят варн!", parse_mode="HTML")
            else:
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 {vname} отделался лёгким испугом!", parse_mode="HTML")

        elif action == "сброс":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            warnings[cid][target.id] = 0
            reputation[cid][target.id] = 0
            xp_data[cid][target.id] = 0
            levels[cid][target.id] = 0
            mod_history[cid][target.id] = []
            save_data()
            await reply_auto_delete(message, f"⚙️ {tname} — всё обнулено!\nВарны, репа, XP, история.", parse_mode="HTML")

        elif action == "лотерея":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            tickets = lottery_tickets.get(cid, [])
            if not tickets:
                await reply_auto_delete(message, "🎰 Билетов в лотерее нет!"); return
            winner_id = random.choice(tickets)
            try: wm = await bot.get_chat_member(cid, winner_id); wname = wm.user.mention_html()
            except: wname = f"ID{winner_id}"
            lottery_tickets[cid] = []
            save_data()
            await reply_auto_delete(message,
                f"🎰 <b>ПРИНУДИТЕЛЬНЫЙ РОЗЫГРЫШ!</b>\n\n🏆 Победитель: {wname}\n🎉 Поздравляем!", parse_mode="HTML")

        elif action == "смерть":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            некрологи = [
                f"упал с дивана и не выжил",
                f"был засмеян до смерти",
                f"пропал при невыясненных обстоятельствах",
                f"ушёл в закат и не вернулся",
                f"был похищен инопланетянами навсегда",
            ]
            await reply_auto_delete(message,
                f"💀 <b>НЕКРОЛОГ</b>\n\n"
                f"Сегодня наш чат покинул {tname}.\n"
                f"Причина: <i>{random.choice(некрологи)}</i>.\n\n"
                f"😔 Помним. Скорбим. Не забудем.", parse_mode="HTML")

        elif action == "зеркало":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            mirror_chats[cid] = __import__('time').time() + 300
            await reply_auto_delete(message, "🔁 <b>Режим зеркала включён на 5 минут!</b>\nБот будет повторять каждое сообщение.", parse_mode="HTML")

        # ── Новые owner команды ──
        elif action == "скрин":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            from datetime import datetime
            total_msgs = sum(chat_stats[cid].values())
            total_warns = sum(warnings[cid].values())
            total_bans = len(ban_list[cid])
            top = sorted(chat_stats[cid].items(), key=lambda x: x[1], reverse=True)[:3]
            top_lines = []
            for i, (uid2, cnt) in enumerate(top, 1):
                try: tm2 = await bot.get_chat_member(cid, uid2); uname2 = tm2.user.full_name
                except: uname2 = f"ID{uid2}"
                top_lines.append(f"  {i}. {uname2} — {cnt} сообщ.")
            text = (
                f"📸 <b>СТАТИСТИКА ЧАТА</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💬 Чат: <b>{message.chat.title}</b>\n"
                f"📨 Всего сообщений: <b>{total_msgs}</b>\n"
                f"⚡ Активных варнов: <b>{total_warns}</b>\n"
                f"🔨 Банов: <b>{total_bans}</b>\n\n"
                f"🏆 Топ активных:\n" + "\n".join(top_lines) + "\n\n"
                f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            try: await bot.send_message(OWNER_ID, text, parse_mode="HTML")
            except: pass
            await reply_auto_delete(message, "📸 Статистика отправлена тебе в личку!", parse_mode="HTML")

        elif action == "взрыв":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            deleted = 0
            for i in range(message.message_id, max(message.message_id - 50, 0), -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            # Отправить уведомление которое тоже удалится
            sent = await bot.send_message(cid, f"🧨 <b>ВЗРЫВ!</b> Удалено {deleted} сообщений.", parse_mode="HTML")
            asyncio.create_task(schedule_delete(sent))

        elif action == "корона":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            crown_holders[cid] = {"uid": target.id, "name": target.full_name, "expire": __import__('time').time() + 86400}
            await bot.send_message(cid,
                f"👑 <b>КОРОЛЬ ЧАТА</b>\n\n"
                f"Отныне и на 24 часа титул короля носит:\n"
                f"🎖 {tname}\n\n"
                f"Да здравствует король! 👑", parse_mode="HTML")

        elif action == "вызов":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            question = rest.strip() or "Что ты об этом думаешь?"
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"🎤 <b>ВЫЗОВ!</b>\n\n"
                f"👉 {tname}, тебя вызывают!\n"
                f"❓ Вопрос: <b>{question}</b>\n\n"
                f"<i>Ответь на сообщение выше 👆</i>", parse_mode="HTML")

        elif action == "шпион":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            from datetime import datetime, timedelta
            hour_ago = datetime.now() - timedelta(hours=1)
            top_hour = sorted(chat_stats[cid].items(), key=lambda x: x[1], reverse=True)[:5]
            lines = ["🕵️ <b>Активность за последний час:</b>\n"]
            for uid2, cnt in top_hour:
                try: tm2 = await bot.get_chat_member(cid, uid2); uname2 = tm2.user.full_name
                except: uname2 = f"ID{uid2}"
                lines.append(f"▸ {uname2}: {cnt} сообщ.")
            try: await bot.send_message(OWNER_ID, "\n".join(lines), parse_mode="HTML")
            except: pass
            await reply_auto_delete(message, "🕵️ Отчёт отправлен в личку!", parse_mode="HTML")

        elif action == "жребий":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            members = [uid2 for uid2 in chat_stats[cid] if chat_stats[cid][uid2] > 0]
            if not members:
                await reply_auto_delete(message, "⚠️ Нет активных участников!"); return
            chosen = random.choice(members)
            try: tm2 = await bot.get_chat_member(cid, chosen); cname = tm2.user.mention_html()
            except: cname = f"ID{chosen}"
            await reply_auto_delete(message,
                f"🃏 <b>ЖРЕБИЙ БРОШЕН!</b>\n\n🎯 Выбран: {cname}", parse_mode="HTML")

        elif action == "громко":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            text_loud = rest.strip().upper() if rest.strip() else "ВНИМАНИЕ"
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"🔊 <b>{text_loud}!!!</b>", parse_mode="HTML")

        elif action == "молния":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            from datetime import datetime
            today_str = datetime.now().strftime("%d.%m.%Y")
            deleted = 0
            for i in range(message.message_id, max(message.message_id - 300, 0), -1):
                try:
                    await bot.delete_message(cid, i)
                    deleted += 1
                except: pass
            await reply_auto_delete(message,
                f"⚡ <b>МОЛНИЯ!</b>\nУдалено ~{deleted} сообщений {tname} за сегодня.", parse_mode="HTML")

        elif action == "магнит":
            if not await check_admin(message) and message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для администраторов!"); return
            magnet_targets[f"{cid}_{target.id}"] = __import__('time').time() + 600
            await reply_auto_delete(message,
                f"🧲 <b>Магнит активирован!</b>\nБот будет лайкать каждое сообщение {tname} 10 минут.", parse_mode="HTML")

        elif action == "цель":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            target_doubles[f"{cid}_{target.id}"] = __import__('time').time() + 1800
            await reply_auto_delete(message,
                f"🎯 <b>Цель установлена!</b>\n{tname} — следующие 30 мин все варны x2!", parse_mode="HTML")

        # ══════════════════════════════════════════
        #  🛡 НОВЫЕ МОДЕРАТОРСКИЕ КОМАНДЫ
        # ══════════════════════════════════════════
        elif action == "алерт":
            text_alert = rest.strip() or "Внимание!"
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"⚠️ <b>ВНИМАНИЕ!</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{text_alert}\n\n━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode="HTML")

        elif action == "пересмотр":
            w = warnings[cid].get(target.id, 0)
            history = mod_history[cid].get(target.id, [])
            warn_history = [h for h in history if "варн" in h.get("action","").lower() or "warn" in h.get("action","").lower()]
            lines = [f"🔄 <b>Пересмотр варнов: {tname}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n",
                     f"⚡ Активных варнов: <b>{w}/{MAX_WARNINGS}</b>\n"]
            if warn_history:
                lines.append(f"📋 История ({len(warn_history)} варнов):")
                for h in warn_history:
                    lines.append(f"  ▸ {h.get('time','?')} — {h.get('reason','?')} (by {h.get('by','?')})")
            else:
                lines.append("📋 История варнов пуста")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "последний":
            import time as _tl
            from datetime import datetime
            ts = last_seen[cid].get(target.id, 0)
            if ts:
                dt = datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
                ago = int((_tl.time() - ts) / 60)
                if ago < 60:    ago_str = f"{ago} мин. назад"
                elif ago < 1440: ago_str = f"{ago//60} ч. назад"
                else:           ago_str = f"{ago//1440} дн. назад"
                await reply_auto_delete(message,
                    f"🕐 <b>Последняя активность:</b>\n👤 {tname}\n📅 {dt}\n⏰ {ago_str}", parse_mode="HTML")
            else:
                await reply_auto_delete(message,
                    f"🕐 {tname} — активность не зафиксирована (с момента запуска бота)", parse_mode="HTML")

        elif action == "заморозка":
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=timedelta(hours=24))
            save_data()
            await reply_auto_delete(message,
                f"🧊 <b>ЗАМОРОЗКА</b>\n\n"
                f"👤 {tname}\n"
                f"❄️ Замолчал на <b>24 часа</b>\n"
                f"🕐 Размут автоматически через 24ч", parse_mode="HTML")
            await log_action(f"🧊 <b>ЗАМОРОЗКА</b>\n👤 {tname}\n⏱ 24ч\n👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

        elif action == "объяви":
            violation = rest.strip() or "нарушение правил"
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"📣 <b>ВНИМАНИЕ ЧАТУ!</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 Участник {tname}\n"
                f"📝 Нарушение: <b>{violation}</b>\n\n"
                f"⚠️ Просим соблюдать правила чата → /rules\n"
                f"━━━━━━━━━━━━━━━━━━━━━━", parse_mode="HTML")

        elif action == "история":
            all_actions = []
            for uid2, actions in mod_history[cid].items():
                for h in actions:
                    all_actions.append(h)
            all_actions.sort(key=lambda x: x.get("time", ""), reverse=True)
            last20 = all_actions[:20]
            lines = [f"⏳ <b>Последние 20 действий в чате</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            for h in last20:
                lines.append(f"▸ {h.get('time','?')} | {h.get('action','?')} | {h.get('by','?')}")
            if not last20:
                lines.append("Действий пока нет")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "уровень":
            import re as _re3
            m4 = _re3.match(r"^([+-]?\d+)", rest)
            if not m4:
                await reply_auto_delete(message, "⚠️ Формат: <b>аутист уровень @user +5</b> или <b>-3</b> или <b>10</b>", parse_mode="HTML"); return
            val = int(m4.group(1))
            if val > 0 and not rest.startswith("+") and not rest.startswith("-"):
                levels[cid][target.id] = val  # установить абсолютно
                action_str = f"установлен на {val}"
            else:
                levels[cid][target.id] = max(0, levels[cid].get(target.id, 0) + val)
                action_str = f"{'повышен' if val > 0 else 'понижен'} на {abs(val)}"
            save_data()
            await reply_auto_delete(message,
                f"🚦 {tname} — уровень {action_str}\n"
                f"📊 Новый уровень: <b>{levels[cid][target.id]}</b>", parse_mode="HTML")

        elif action == "только чтение":
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(
                    can_send_messages=False, can_send_media_messages=False,
                    can_send_polls=False, can_send_other_messages=False))
            save_data()
            await reply_auto_delete(message,
                f"🔒 {tname} — <b>режим только чтение</b>\n"
                f"👁 Видит чат, но не может писать (навсегда)", parse_mode="HTML")
            await log_action(f"🔒 <b>ТОЛЬКО ЧТЕНИЕ</b>\n👤 {tname}\n👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

        elif action == "топ нарушителей":
            top = sorted(warnings[cid].items(), key=lambda x: x[1], reverse=True)[:10]
            lines = [f"📊 <b>Топ нарушителей чата</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            for i, (uid2, w) in enumerate(top, 1):
                if w == 0: continue
                try: tm2 = await bot.get_chat_member(cid, uid2); uname2 = tm2.user.full_name
                except: uname2 = f"ID{uid2}"
                lines.append(f"{i}. {uname2} — ⚡ <b>{w}</b> варн.")
            if len(lines) == 1:
                lines.append("Нарушителей нет 🎉")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "медиамут":
            import re as _rem2
            time_str2 = rest.strip() if rest else ""
            time_str2 = _rem2.sub(r"@\S+", "", time_str2).strip()
            mins2, label2 = parse_duration(time_str2) if time_str2 else (None, None)
            until2 = timedelta(minutes=mins2) if mins2 else None
            time_text2 = f"⏱ На: <b>{label2}</b>" if label2 else "⏱ Навсегда"
            perms2 = ChatPermissions(
                can_send_messages=True, can_send_media_messages=False,
                can_send_polls=False, can_send_other_messages=False,
                can_add_web_page_previews=False)
            if until2:
                await bot.restrict_chat_member(cid, target.id, permissions=perms2, until_date=until2)
            else:
                await bot.restrict_chat_member(cid, target.id, permissions=perms2)
            save_data()
            await reply_auto_delete(message,
                f"🤐 {tname} — <b>медиамут</b>\n"
                f"✍️ Только текст\n🚫 Медиа, стикеры, гифки — запрещены\n{time_text2}", parse_mode="HTML")
            await log_action(f"🤐 <b>МЕДИАМУТ</b>\n👤 {tname}\n{time_text2}\n👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

        elif action in ("стикермут", "гифмут", "войсмут", "всёмут", "медиамут2"):
            # Парсим время из rest: "аутист стикермут @user 30м"
            import re as _rem
            time_str = rest.strip() if rest else ""
            # Убираем упоминание если осталось
            time_str = _rem.sub(r"@\S+", "", time_str).strip()
            mins, label = parse_duration(time_str) if time_str else (None, None)
            until_date = timedelta(minutes=mins) if mins else None
            time_text = f"⏱ На: <b>{label}</b>" if label else "⏱ Навсегда"

            if action == "стикермут":
                perms = ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=False,
                    can_add_web_page_previews=True)
                emoji, name, desc = "🙅", "СТИКЕРМУТ", "Стикеры — запрещены\n✍️ Текст и медиа — можно"

            elif action == "гифмут":
                perms = ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=False,
                    can_add_web_page_previews=True)
                emoji, name, desc = "🎭", "ГИФМУТ", "Гифки и анимации — запрещены\n✍️ Текст и фото — можно"

            elif action == "войсмут":
                try:
                    perms = ChatPermissions(
                        can_send_messages=True, can_send_media_messages=True,
                        can_send_polls=True, can_send_other_messages=True,
                        can_add_web_page_previews=True,
                        can_send_voice_notes=False, can_send_video_notes=False)
                except Exception:
                    perms = ChatPermissions(
                        can_send_messages=True, can_send_media_messages=True,
                        can_send_polls=True, can_send_other_messages=False,
                        can_add_web_page_previews=True)
                emoji, name, desc = "🔇", "ВОЙСМУТ", "Голосовые — запрещены\n✍️ Текст и медиа — можно"

            else:  # всёмут
                perms = ChatPermissions(
                    can_send_messages=True, can_send_media_messages=False,
                    can_send_polls=False, can_send_other_messages=False,
                    can_add_web_page_previews=False)
                emoji, name, desc = "🚫", "ВСЁМУТ", "Только текст разрешён\n🚫 Медиа, стикеры, гифки, голосовые — запрещены"

            if until_date:
                await bot.restrict_chat_member(cid, target.id, permissions=perms, until_date=until_date)
            else:
                await bot.restrict_chat_member(cid, target.id, permissions=perms)

            await reply_auto_delete(message,
                f"{emoji} {tname} — <b>{action}</b>\n{desc}\n{time_text}", parse_mode="HTML")
            await log_action(
                f"{emoji} <b>{name}</b>\n👤 {tname}\n{time_text}\n"
                f"👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

        elif action == "причина":
            history = mod_history[cid].get(target.id, [])
            warn_acts = [h for h in history if "варн" in h.get("action","").lower()]
            if warn_acts:
                last_warn = warn_acts[-1]
                await reply_auto_delete(message,
                    f"💬 <b>Последний варн {tname}:</b>\n\n"
                    f"📝 Причина: <b>{last_warn.get('reason','не указана')}</b>\n"
                    f"👮 Выдал: {last_warn.get('by','?')}\n"
                    f"🕐 Время: {last_warn.get('time','?')}", parse_mode="HTML")
            else:
                await reply_auto_delete(message, f"💬 У {tname} нет варнов в истории", parse_mode="HTML")

        elif action == "напомни мод":
            import re as _re4
            m5 = _re4.match(r"^(\d+)\s*(д|ч|м)\s*(.*)", rest)
            if not m5:
                await reply_auto_delete(message, "⚠️ Формат: <b>аутист напомни мод 1ч текст</b>", parse_mode="HTML"); return
            num3, unit3, text3 = int(m5.group(1)), m5.group(2), m5.group(3).strip() or "Напоминание для модераторов!"
            if unit3 == "д":   mins3 = num3 * 1440; lbl3 = f"{num3} дн."
            elif unit3 == "ч": mins3 = num3 * 60;   lbl3 = f"{num3} ч."
            else:              mins3 = num3;          lbl3 = f"{num3} мин."
            async def _remind_mods(c, delay, txt, mod_name):
                await asyncio.sleep(delay * 60)
                try:
                    admins = await bot.get_chat_administrators(c)
                    for adm in admins:
                        if not adm.user.is_bot:
                            try:
                                await bot.send_message(adm.user.id,
                                    f"🔔 <b>Напоминание модераторам!</b>\n\n{txt}\n\n"
                                    f"<i>Установил: {mod_name}</i>", parse_mode="HTML")
                            except: pass
                except: pass
            asyncio.create_task(_remind_mods(cid, mins3, text3, message.from_user.full_name))
            await reply_auto_delete(message,
                f"🔔 Все модераторы получат напоминание через <b>{lbl3}</b>!", parse_mode="HTML")

        # ══════════════════════════════════════════
        #  👑 НОВЫЕ OWNER КОМАНДЫ
        # ══════════════════════════════════════════
        elif action == "напомни":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            # Формат: аутист напомни 30м текст
            import re as _re2
            m3 = _re2.match(r"^(\d+)\s*(д|ч|м)\s*(.*)", rest)
            if not m3:
                await reply_auto_delete(message, "⚠️ Формат: <b>аутист напомни 30м текст</b>", parse_mode="HTML"); return
            num2, unit2, text2 = int(m3.group(1)), m3.group(2), m3.group(3).strip() or "Напоминание!"
            if unit2 == "д":   mins2 = num2 * 1440; lbl2 = f"{num2} дн."
            elif unit2 == "ч": mins2 = num2 * 60;   lbl2 = f"{num2} ч."
            else:              mins2 = num2;          lbl2 = f"{num2} мин."
            async def _remind(delay, txt):
                await asyncio.sleep(delay * 60)
                try: await bot.send_message(OWNER_ID, f"⏰ <b>Напоминание!</b>\n\n{txt}", parse_mode="HTML")
                except: pass
            asyncio.create_task(_remind(mins2, text2))
            await reply_auto_delete(message, f"⏰ Напомню тебе через <b>{lbl2}</b>!", parse_mode="HTML")

        elif action == "закреп":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            text_pin = rest.strip()
            if not text_pin and not message.reply_to_message:
                await reply_auto_delete(message, "⚠️ Укажи текст или реплайни на сообщение", parse_mode="HTML"); return
            try: await message.delete()
            except: pass
            if message.reply_to_message and not text_pin:
                await bot.pin_chat_message(cid, message.reply_to_message.message_id, disable_notification=False)
                await reply_auto_delete(message, "📌 Сообщение закреплено!")
            else:
                sent = await bot.send_message(cid,
                    f"📌 <b>ЗАКРЕПЛЕНО</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n{text_pin}\n\n━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode="HTML")
                await bot.pin_chat_message(cid, sent.message_id, disable_notification=False)

        elif action == "голос":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            # Формат: аутист голос Вопрос? | Вариант1 | Вариант2
            parts_poll = rest.split("|")
            if len(parts_poll) < 3:
                await reply_auto_delete(message,
                    "⚠️ Формат: <b>аутист голос Вопрос? | Вариант1 | Вариант2</b>", parse_mode="HTML"); return
            question_poll = parts_poll[0].strip()
            options_poll = [o.strip() for o in parts_poll[1:] if o.strip()]
            try: await message.delete()
            except: pass
            await bot.send_poll(cid, question=question_poll, options=options_poll, is_anonymous=False)

        elif action == "рост":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            activity = user_activity[cid].get(target.id, {})
            if not activity:
                await reply_auto_delete(message, f"📈 {tname} — нет данных об активности", parse_mode="HTML"); return
            sorted_days = sorted(activity.items())[-14:]  # последние 14 дней
            lines = [f"📈 <b>Активность {tname}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            max_count = max(c for _, c in sorted_days) or 1
            for date, count in sorted_days:
                bar_len = int((count / max_count) * 12)
                bar = "█" * bar_len + "░" * (12 - bar_len)
                lines.append(f"{date}: {bar} <b>{count}</b>")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "тишина":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            # Парсим время прямо из rest если target не найден
            import re as _re5
            t_match = _re5.match(r"^(\d+)\s*(д|ч|м)", rest)
            if t_match:
                num_s = int(t_match.group(1)); unit_s = t_match.group(2)
                if unit_s == "д":   mins_silence = num_s * 1440; label_silence = f"{num_s} дн."
                elif unit_s == "ч": mins_silence = num_s * 60;   label_silence = f"{num_s} ч."
                else:               mins_silence = num_s;          label_silence = f"{num_s} мин."
            else:
                mins_silence = duration_mins or 5
                label_silence = duration_label or f"{mins_silence} мин."
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
            await reply_auto_delete(message,
                f"🔇 <b>ТИШИНА на {label_silence}!</b>\nЧат закрыт для всех.", parse_mode="HTML")
            async def _unsilence(c, delay):
                await asyncio.sleep(delay * 60)
                try:
                    await bot.set_chat_permissions(c, ChatPermissions(
                        can_send_messages=True, can_send_media_messages=True,
                        can_send_polls=True, can_send_other_messages=True,
                        can_add_web_page_previews=True))
                    await bot.send_message(c, "🔊 <b>Тишина закончилась!</b> Можно говорить.", parse_mode="HTML")
                except: pass
            asyncio.create_task(_unsilence(cid, mins_silence))

        # ══════════════════════════════════════════
        #  📊 ТЕМПЕРАТУРА, НЕДЕЛЯ, РЕЖИМ, ЛОГ, РЕСТАРТ, СОС
        # ══════════════════════════════════════════
        elif action == "температура":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import time as _tt
            now_ts = _tt.time()
            # Считаем сообщения за последние 10 минут
            active_10 = sum(
                1 for uid2, msgs in user_msg_ids[cid].items()
                for mid, ts in msgs if now_ts - ts <= 600
            )
            active_60 = sum(
                1 for uid2, msgs in user_msg_ids[cid].items()
                for mid, ts in msgs if now_ts - ts <= 3600
            )
            if active_10 >= 30:   temp = "🔥🔥🔥 ОГОНЬ"
            elif active_10 >= 15: temp = "🔥🔥 Горячо"
            elif active_10 >= 5:  temp = "🔥 Тепло"
            elif active_10 >= 1:  temp = "😐 Прохладно"
            else:                 temp = "🧊 Мертво"
            await reply_auto_delete(message,
                f"🌡 <b>Температура чата</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"За 10 мин: <b>{active_10}</b> сообщ. — {temp}\n"
                f"За 1 час: <b>{active_60}</b> сообщений\n"
                f"Участников в базе: <b>{len(chat_stats[cid])}</b>",
                parse_mode="HTML")

        elif action == "неделя":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            from datetime import datetime, timedelta as _td
            lines = [f"📊 <b>Итоги недели — {message.chat.title}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            # Топ активных за 7 дней
            week_dates = [(datetime.now() - _td(days=i)).strftime("%d.%m.%Y") for i in range(7)]
            week_activity = {}
            for uid2, days in user_activity[cid].items():
                total = sum(days.get(d, 0) for d in week_dates)
                if total > 0:
                    week_activity[uid2] = total
            top5 = sorted(week_activity.items(), key=lambda x: x[1], reverse=True)[:5]
            lines.append("🏆 <b>Топ активных:</b>")
            for i, (uid2, cnt) in enumerate(top5, 1):
                try: tm2 = await bot.get_chat_member(cid, uid2); uname2 = tm2.user.full_name
                except: uname2 = f"ID{uid2}"
                lines.append(f"  {i}. {uname2} — {cnt} сообщ.")
            # Варны и баны за неделю
            total_warns = sum(warnings[cid].values())
            total_bans = len(ban_list[cid])
            lines.append(f"\n⚡ Всего варнов: <b>{total_warns}</b>")
            lines.append(f"🔨 Забанено: <b>{total_bans}</b>")
            lines.append(f"👥 Участников: <b>{len(chat_stats[cid])}</b>")
            await bot.send_message(OWNER_ID, "\n".join(lines), parse_mode="HTML")
            await reply_auto_delete(message, "📊 Итоги недели отправлены в личку!", parse_mode="HTML")

        elif action == "режим":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            new_name = rest.strip()
            if not new_name:
                await reply_auto_delete(message, "⚠️ Укажи имя: <b>аутист режим Имя</b>", parse_mode="HTML"); return
            try:
                await bot.set_my_name(new_name)
                await reply_auto_delete(message, f"🤖 Имя бота изменено на: <b>{new_name}</b>", parse_mode="HTML")
            except Exception as ex:
                await reply_auto_delete(message, f"⚠️ Не удалось: {ex}", parse_mode="HTML")

        elif action == "лог":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import re as _re6
            n_log = int(_re6.match(r"^(\d+)", rest).group(1)) if _re6.match(r"^(\d+)", rest) else 10
            n_log = min(n_log, 30)
            # Пересылаем последние N сообщений из лог-канала
            sent_count = 0
            try:
                for i in range(n_log):
                    try:
                        await bot.forward_message(OWNER_ID, LOG_CHANNEL_ID,
                            message.message_id - i)
                        sent_count += 1
                    except: pass
            except: pass
            await reply_auto_delete(message,
                f"📝 Переслал последние ~<b>{sent_count}</b> записей из лога в личку",
                parse_mode="HTML")

        elif action == "рестарт":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            await reply_auto_delete(message, "🔄 Перезапускаю бота...", parse_mode="HTML")
            import os, sys
            await asyncio.sleep(1)
            os.execv(sys.executable, [sys.executable] + sys.argv)

        elif action == "сос":
            if message.from_user.id != OWNER_ID:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import time as _ts2
            uptime_sec = int(_ts2.time() - bot_start_time) if 'bot_start_time' in globals() else 0
            h, m = uptime_sec // 3600, (uptime_sec % 3600) // 60
            total_warns_all = sum(sum(u.values()) for u in warnings.values())
            total_bans_all  = sum(len(b) for b in ban_list.values())
            total_users_all = sum(len(u) for u in chat_stats.values())
            await bot.send_message(OWNER_ID,
                f"🚨 <b>SOS — Состояние бота</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⏱ Аптайм: <b>{h}ч {m}мин</b>\n"
                f"💬 Чатов: <b>{len(known_chats)}</b>\n"
                f"👥 Участников: <b>{total_users_all}</b>\n"
                f"⚡ Всего варнов: <b>{total_warns_all}</b>\n"
                f"🔨 Всего банов: <b>{total_bans_all}</b>\n"
                f"📋 Очередей жалоб: <b>{sum(len(q) for q in report_queue.values())}</b>",
                parse_mode="HTML")
            await reply_auto_delete(message, "🚨 SOS-отчёт отправлен в личку!", parse_mode="HTML")

    except Exception as e:
        await reply_auto_delete(message, f"⚠️ Ошибка: {e}")




# ===== УГАДАЙ ЧИСЛО =====
guess_games = {}

@dp.message(Command("guess"))
async def cmd_guess(message: Message):
    cid = message.chat.id
    number = random.randint(1, 100)
    guess_games[cid] = {"number": number, "attempts": 0}
    await reply_auto_delete(message, 
        f"🎯 <b>Угадай число!</b>\n\n"
        f"🔢 Загадал число от <b>1 до 100</b>\n"
        f"💬 Просто напиши число в чат!\n"
        f"⚡ Есть <b>10 попыток</b>",
        parse_mode="HTML")

@dp.message(F.text.regexp(r'^\d+$'))
async def guess_handler(message: Message):
    cid = message.chat.id
    if cid not in guess_games: return
    game = guess_games[cid]
    try: num = int(message.text)
    except: return
    if num < 1 or num > 100: return
    game["attempts"] += 1
    if num == game["number"]:
        del guess_games[cid]
        await reply_auto_delete(message, 
            f"🎉 {message.from_user.mention_html()} угадал за <b>{game['attempts']}</b> попыток!\n"
            f"✅ Число было <b>{num}</b>!", parse_mode="HTML")
    elif game["attempts"] >= 10:
        n = game["number"]; del guess_games[cid]
        await reply_auto_delete(message, f"😢 Попытки кончились! Загадано было <b>{n}</b>.", parse_mode="HTML")
    elif num < game["number"]:
        await reply_auto_delete(message, f"⬆️ Больше! Попытка {game['attempts']}/10")
    else:
        await reply_auto_delete(message, f"⬇️ Меньше! Попытка {game['attempts']}/10")

# ===== АСК =====
ask_targets = {}

@dp.message(Command("ask"))
async def cmd_ask(message: Message):
    if message.chat.type == "private":
        await reply_auto_delete(message, "❓ Эту команду используй в чате!"); return
    ask_targets[message.from_user.id] = {"chat_id": message.chat.id, "name": message.from_user.full_name}
    await reply_auto_delete(message, 
        f"📬 <b>{message.from_user.mention_html()} открыл АСК!</b>\n\n"
        f"🔒 Напиши боту в личку анонимный вопрос!\n"
        f"👉 @AllAnonandbot", parse_mode="HTML")

@dp.message(Command("askoff"))
async def cmd_askoff(message: Message):
    if message.from_user.id in ask_targets:
        del ask_targets[message.from_user.id]
        await reply_auto_delete(message, "📭 АСК закрыт!")
    else:
        await reply_auto_delete(message, "❌ У тебя не открыт АСК.")

@dp.message(Command("send"))
async def cmd_send_ask(message: Message, command: CommandObject):
    if message.chat.type != "private":
        await reply_auto_delete(message, "📩 Эту команду используй в личке с ботом!"); return
    if not command.args:
        await reply_auto_delete(message, "❓ Формат: /send @юзернейм текст", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    if len(parts) < 2:
        await reply_auto_delete(message, "⚠️ Укажи юзернейм и текст вопроса!"); return
    username = parts[0].replace("@", "").lower()
    text = parts[1].strip()
    target_id = None
    for uid, data in ask_targets.items():
        try:
            chat_member = await bot.get_chat_member(data["chat_id"], uid)
            if chat_member.user.username and chat_member.user.username.lower() == username:
                target_id = uid; break
        except: pass
    if not target_id:
        await reply_auto_delete(message, "❌ Этот пользователь не открыл АСК или не найден."); return
    data = ask_targets[target_id]
    try:
        await bot.send_message(data["chat_id"],
            f"📬 <b>Анонимный вопрос для {data['name']}:</b>\n\n💬 {text}\n\n<i>Ответь командой /reply</i>",
            parse_mode="HTML")
        await reply_auto_delete(message, "✅ Вопрос отправлен анонимно!")
    except:
        await reply_auto_delete(message, "⚠️ Не удалось отправить вопрос.")

# ===== МВП =====
mvp_votes = {}
mvp_voted = {}

@dp.message(Command("mvp"))
async def cmd_mvp(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника чтобы проголосовать за МВП!"); return
    voter = message.from_user; target = message.reply_to_message.from_user; cid = message.chat.id
    if target.id == voter.id:
        await reply_auto_delete(message, "😏 За себя голосовать нельзя!"); return
    if cid not in mvp_voted: mvp_voted[cid] = {}
    if voter.id in mvp_voted[cid]:
        await reply_auto_delete(message, "⏳ Ты уже голосовал сегодня за МВП!"); return
    mvp_voted[cid][voter.id] = True
    if cid not in mvp_votes: mvp_votes[cid] = {}
    mvp_votes[cid][target.id] = mvp_votes[cid].get(target.id, 0) + 1
    votes = mvp_votes[cid][target.id]
    await reply_auto_delete(message, 
        f"⭐ {voter.mention_html()} проголосовал за <b>МВП</b>!\n\n"
        f"🏆 {target.mention_html()}\n👍 Голосов: <b>{votes}</b>", parse_mode="HTML")

@dp.message(Command("mvpstats"))
async def cmd_mvpstats(message: Message):
    cid = message.chat.id
    if cid not in mvp_votes or not mvp_votes[cid]:
        await reply_auto_delete(message, "📊 Голосов ещё нет!"); return
    sorted_mvp = sorted(mvp_votes[cid].items(), key=lambda x: x[1], reverse=True)[:5]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    lines = ["🏆 <b>Топ МВП:</b>\n"]
    for i, (uid, votes) in enumerate(sorted_mvp):
        try: m = await bot.get_chat_member(cid, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {votes} голосов")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("mvpreset"))
async def cmd_mvpreset(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    mvp_votes[cid] = {}; mvp_voted[cid] = {}
    await reply_auto_delete(message, "🔄 МВП голосование сброшено!")

# ===== CONFESSION =====
@dp.message(Command("confession"))
async def cmd_confession(message: Message, command: CommandObject):
    if not command.args:
        await reply_auto_delete(message, 
            "💌 Формат: /confession текст\n"
            "Пример: <code>/confession Я влюблён в одного человека в этом чате...</code>",
            parse_mode="HTML"); return
    text = command.args.strip()
    if len(text) < 5:
        await reply_auto_delete(message, "⚠️ Слишком короткое сообщение!"); return
    try: await message.delete()
    except: pass
    await bot.send_message(message.chat.id,
        f"💌 <b>Анонимное признание:</b>\n\n<i>{text}</i>\n\n🔒 <i>Автор неизвестен</i>",
        parse_mode="HTML")

# ===== SECRET =====
@dp.message(Command("secret"))
async def cmd_secret(message: Message, command: CommandObject):
    if message.chat.type == "private":
        await reply_auto_delete(message, "❌ Эту команду используй в чате!"); return
    if not command.args or len(command.args.split(maxsplit=1)) < 2:
        await reply_auto_delete(message, 
            "📩 Формат: /secret @юзернейм текст\n"
            "Пример: <code>/secret @username Ты мне нравишься 😊</code>", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    username = parts[0].replace("@", "").lower()
    text = parts[1].strip()
    target = None
    try:
        admins = await bot.get_chat_administrators(message.chat.id)
        members = await bot.get_chat_members_count(message.chat.id)
    except: pass
    async for member in bot.get_chat_members(message.chat.id):
        if member.user.username and member.user.username.lower() == username:
            target = member.user; break
    if not target:
        await reply_auto_delete(message, "❌ Участник не найден в чате!"); return
    try:
        try: await message.delete()
        except: pass
        await bot.send_message(target.id,
            f"💌 <b>Тебе анонимное сообщение!</b>\n\n<i>{text}</i>\n\n🔒 <i>Автор неизвестен</i>",
            parse_mode="HTML")
        await bot.send_message(message.chat.id, "📩 Анонимное сообщение отправлено!")
    except:
        await reply_auto_delete(message, "⚠️ Не удалось отправить — участник должен написать боту в лс хотя бы раз!")
# ===== ДУЭЛИ =====
duel_requests = {}

@dp.message(Command("duel"))
async def cmd_duel(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Ответь на сообщение участника для дуэли!"); return
    challenger = message.from_user; target = message.reply_to_message.from_user; cid = message.chat.id
    if target.id == challenger.id:
        await reply_auto_delete(message, "😏 Сам с собой дуэль?"); return
    if target.is_bot:
        await reply_auto_delete(message, "🤖 С ботом не подерёшься!"); return
    try: bet = int(command.args) if command.args else 10
    except: bet = 10
    if bet <= 0: bet = 10
    if reputation[cid].get(challenger.id, 0) < bet:
        await reply_auto_delete(message, f"💸 Недостаточно репутации! У тебя: <b>{reputation[cid].get(challenger.id, 0):+d}</b>", parse_mode="HTML"); return
    if reputation[cid].get(target.id, 0) < bet:
        await reply_auto_delete(message, f"💸 У {target.mention_html()} недостаточно репутации!", parse_mode="HTML"); return
    duel_requests[cid] = {
        "challenger_id": challenger.id, "challenger_name": challenger.full_name,
        "target_id": target.id, "target_name": target.full_name, "bet": bet
    }
    await reply_auto_delete(message, 
        f"⚔️ <b>ВЫЗОВ НА ДУЭЛЬ!</b>\n\n"
        f"🔫 {challenger.mention_html()} вызывает {target.mention_html()}!\n"
        f"💰 Ставка: <b>{bet}</b> репутации\n\n"
        f"✅ {target.mention_html()}, напиши <b>/accept</b> чтобы принять!\n"
        f"❌ Или <b>/decline</b> чтобы отказаться!",
        parse_mode="HTML")

@dp.message(Command("accept"))
async def cmd_accept(message: Message):
    cid = message.chat.id; uid = message.from_user.id
    if cid not in duel_requests:
        await reply_auto_delete(message, "❌ Нет активных дуэлей!"); return
    duel = duel_requests[cid]
    if uid != duel["target_id"]:
        await reply_auto_delete(message, "❌ Это не твоя дуэль!"); return
    winner_id = random.choice([duel["challenger_id"], duel["target_id"]])
    loser_id = duel["target_id"] if winner_id == duel["challenger_id"] else duel["challenger_id"]
    winner_name = duel["challenger_name"] if winner_id == duel["challenger_id"] else duel["target_name"]
    loser_name = duel["target_name"] if winner_id == duel["challenger_id"] else duel["challenger_name"]
    bet = duel["bet"]
    reputation[cid][winner_id] += bet
    reputation[cid][loser_id] -= bet
    save_data()
    del duel_requests[cid]
    await reply_auto_delete(message, 
        f"⚔️ <b>ДУЭЛЬ!</b>\n\n"
        f"🔫 {duel['challenger_name']} vs {duel['target_name']}\n\n"
        f"🏆 Победитель: <b>{winner_name}</b> +{bet} репутации!\n"
        f"💀 Проигравший: <b>{loser_name}</b> -{bet} репутации!",
        parse_mode="HTML")
    await log_action(
        f"⚔️ <b>ДУЭЛЬ</b>\n"
        f"🏆 Победитель: <b>{winner_name}</b> +{bet}\n"
        f"💀 Проигравший: <b>{loser_name}</b> -{bet}\n"
        f"Чат: {message.chat.title}")

@dp.message(Command("decline"))
async def cmd_decline(message: Message):
    cid = message.chat.id; uid = message.from_user.id
    if cid not in duel_requests:
        await reply_auto_delete(message, "❌ Нет активных дуэлей!"); return
    duel = duel_requests[cid]
    if uid != duel["target_id"]:
        await reply_auto_delete(message, "❌ Это не твоя дуэль!"); return
    del duel_requests[cid]
    await reply_auto_delete(message, f"🏳 {message.from_user.mention_html()} отказался от дуэли!", parse_mode="HTML")

@dp.message(Command("streak"))
async def cmd_streak(message: Message):
    from datetime import datetime
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid = user.id; cid = message.chat.id
    streak = streaks[cid][uid]
    last_date = streak_dates[cid][uid]
    await reply_auto_delete(message, 
        f"🔥 <b>Серия активности {user.mention_html()}:</b>\n\n"
        f"📅 Дней подряд: <b>{streak}</b>\n"
        f"📆 Последний день: <b>{last_date or 'нет данных'}</b>\n\n"
        f"💬 Пиши каждый день чтобы серия росла!",
        parse_mode="HTML")

@dp.message(Command("toprep"))
async def cmd_toprep(message: Message):
    rep = reputation[message.chat.id]
    if not rep:
        await reply_auto_delete(message, "📊 Репутация пока пуста!"); return
    sorted_u = sorted(rep.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["🌟 <b>Топ по репутации:</b>\n"]
    for i, (uid, score) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        icon = "⬆️" if score >= 0 else "⬇️"
        lines.append(f"{medals[i]} <b>{uname}</b> — {icon} <b>{score:+d}</b>")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid, cid = user.id, message.chat.id

    xp     = xp_data[cid].get(uid, 0)
    lvl    = get_level(xp)
    levels[cid][uid] = lvl
    emoji_lvl, title_lvl = get_level_title(lvl)
    rep    = reputation[cid].get(uid, 0)
    warns  = warnings[cid].get(uid, 0)
    msgs   = chat_stats[cid].get(uid, 0)
    streak = streaks[cid].get(uid, 0)

    # Прогресс до следующего уровня
    cur_xp  = LEVEL_XP.get(lvl, 0)
    next_xp = get_xp_for_next(lvl)
    prog    = xp - cur_xp
    needed  = next_xp - cur_xp
    pct     = min(int((prog / needed) * 12), 12) if needed > 0 else 12
    bar     = "█" * pct + "░" * (12 - pct)

    # Ранг по XP в чате
    sorted_xp = sorted(xp_data[cid].items(), key=lambda x: x[1], reverse=True)
    rank_xp = next((i+1 for i,(u,_) in enumerate(sorted_xp) if u == uid), "?")

    # Ранг по репе в чате
    sorted_rep = sorted(reputation[cid].items(), key=lambda x: x[1], reverse=True)
    rank_rep = next((i+1 for i,(u,_) in enumerate(sorted_rep) if u == uid), "?")

    # Доп инфо
    avatar     = avatars.get(str(uid), "👤")
    shop_title = user_titles[uid].get("title", "")
    clan_id    = clan_members.get(uid)
    clan_line  = f"🤝 Клан: <b>[{clans[clan_id]['tag']}] {clans[clan_id]['name']}</b>\n" if clan_id and clan_id in clans else ""
    title_line = f"🎭 Титул: <b>{shop_title}</b>\n" if shop_title else ""
    arts_count = len(artifacts.get(str(uid), []))
    color_badge, _ = get_color_badge(rep)

    await reply_auto_delete(message,
        f"{avatar} <b>{user.mention_html()}</b> {color_badge}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{emoji_lvl} <b>{title_lvl}</b>  •  Уровень <b>{lvl}</b> / 250\n"
        f"<code>[{bar}]</code> {prog}/{needed} XP\n\n"
        f"{title_line}"
        f"{clan_line}"
        f"⭐ Репутация: <b>{rep:+d}</b>  (#{rank_rep} в чате)\n"
        f"📊 XP ранг: <b>#{rank_xp}</b>  •  Всего XP: <b>{xp}</b>\n"
        f"💬 Сообщений: <b>{msgs}</b>\n"
        f"🔥 Стрик: <b>{streak}</b> дней\n"
        f"⚡ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
        f"🧙 Артефактов: <b>{arts_count}</b>\n",
        parse_mode="HTML")

@dp.message(Command("addrep"))
async def cmd_addrep(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID: return
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        target = message.from_user
    try: amount = int(command.args or 100)
    except: amount = 100
    cid = message.chat.id
    reputation[cid][target.id] += amount
    save_data()
    await reply_auto_delete(message, 
        f"✅ {target.mention_html()} добавлено <b>{amount}</b> репутации!\n"
        f"🌟 Теперь: <b>{reputation[cid][target.id]:+d}</b>",
        parse_mode="HTML")

# ===== ЕЖЕДНЕВНЫЙ БОНУС =====
daily_claimed = {}

@dp.message(Command("daily"))
async def cmd_daily(message: Message):
    uid = message.from_user.id; cid = message.chat.id
    from datetime import datetime
    today = datetime.now().strftime("%d.%m.%Y")
    key = (cid, uid)
    if daily_claimed.get(key) == today:
        await reply_auto_delete(message, 
            f"⏳ Ты уже забрал ежедневный бонус!\n"
            f"🔄 Приходи завтра.", parse_mode="HTML"); return
    streak = streaks[cid][uid]
    bonus = 10 + min(streak * 2, 40)
    daily_claimed[key] = today
    reputation[cid][uid] += bonus; save_data()
    await reply_auto_delete(message, 
        f"🎁 <b>Ежедневный бонус!</b>\n\n"
        f"🌟 +{bonus} репутации\n"
        f"🔥 Серия: <b>{streak}</b> дней\n"
        f"💰 Всего репутации: <b>{reputation[cid][uid]:+d}</b>\n\n"
        f"{'🎉 Бонус за серию +' + str(min(streak*2,40)) + '!' if streak > 1 else '💡 Заходи каждый день для бонуса!'}",
        parse_mode="HTML")

# ===== ТУРНИРЫ =====
tournament_data = {}  # {cid: {"active": bool, "participants": [], "bracket": [], "round": 0}}

@dp.message(Command("tournament"))
async def cmd_tournament(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    sub = command.args.strip().lower() if command.args else ""
    if sub == "start":
        if cid in tournament_data and tournament_data[cid].get("active"):
            await reply_auto_delete(message, "⚠️ Турнир уже идёт!"); return
        tournament_data[cid] = {"active": False, "registration": True, "participants": [], "round": 0}
        await reply_auto_delete(message, 
            f"🎪 <b>ТУРНИР ОТКРЫТ!</b>\n\n"
            f"📝 Пиши /join чтобы записаться!\n"
            f"🚀 Админ запустит турнир командой /tournament begin",
            parse_mode="HTML")
    elif sub == "begin":
        if cid not in tournament_data:
            await reply_auto_delete(message, "❌ Сначала открой регистрацию: /tournament start"); return
        parts = tournament_data[cid]["participants"]
        if len(parts) < 2:
            await reply_auto_delete(message, "⚠️ Нужно минимум 2 участника!"); return
        random.shuffle(parts)
        tournament_data[cid]["active"] = True
        tournament_data[cid]["registration"] = False
        names = "\n".join([f"• {p['name']}" for p in parts])
        await reply_auto_delete(message, 
            f"🎪 <b>ТУРНИР НАЧАЛСЯ!</b>\n\n"
            f"👥 Участников: <b>{len(parts)}</b>\n\n{names}\n\n"
            f"⚔️ Запускаем первый раунд с /tournament next!", parse_mode="HTML")
    elif sub == "next":
        if cid not in tournament_data or not tournament_data[cid].get("active"):
            await reply_auto_delete(message, "❌ Нет активного турнира!"); return
        parts = tournament_data[cid]["participants"]
        if len(parts) == 1:
            winner = parts[0]
            reputation[message.chat.id][winner["id"]] += 50; save_data()
            del tournament_data[cid]
            await reply_auto_delete(message, 
                f"🏆 <b>ПОБЕДИТЕЛЬ ТУРНИРА:</b>\n\n"
                f"👑 <b>{winner['name']}</b>\n🌟 +50 репутации!", parse_mode="HTML")
            return
        random.shuffle(parts)
        results = []; survivors = []
        for i in range(0, len(parts) - 1, 2):
            a = parts[i]; b = parts[i+1]
            winner = random.choice([a, b])
            loser  = b if winner == a else a
            survivors.append(winner)
            results.append(f"⚔️ {a['name']} vs {b['name']} → 🏆 <b>{winner['name']}</b>")
        if len(parts) % 2 == 1:
            bye = parts[-1]; survivors.append(bye)
            results.append(f"🎟 {bye['name']} — проходит автоматически")
        tournament_data[cid]["participants"] = survivors
        tournament_data[cid]["round"] += 1
        rnd = tournament_data[cid]["round"]
        await reply_auto_delete(message, 
            f"🎪 <b>Раунд {rnd} завершён!</b>\n\n" + "\n".join(results) +
            f"\n\n👥 Осталось: <b>{len(survivors)}</b>\n"
            f"{'⚔️ /tournament next для следующего раунда' if len(survivors) > 1 else '🏆 /tournament next для финала'}",
            parse_mode="HTML")
    elif sub == "stop":
        if cid in tournament_data:
            del tournament_data[cid]
            await reply_auto_delete(message, "🛑 Турнир отменён.")
    else:
        await reply_auto_delete(message, 
            "🎪 <b>Управление турниром:</b>\n\n"
            "/tournament start — открыть регистрацию\n"
            "/tournament begin — начать турнир\n"
            "/tournament next — следующий раунд\n"
            "/tournament stop — отменить", parse_mode="HTML")

@dp.message(Command("join"))
async def cmd_join(message: Message):
    cid = message.chat.id; uid = message.from_user.id
    if cid not in tournament_data or not tournament_data[cid].get("registration"):
        await reply_auto_delete(message, "❌ Регистрация на турнир не открыта!"); return
    parts = tournament_data[cid]["participants"]
    if any(p["id"] == uid for p in parts):
        await reply_auto_delete(message, "✅ Ты уже записан!"); return
    parts.append({"id": uid, "name": message.from_user.full_name})
    await reply_auto_delete(message, 
        f"✅ {message.from_user.mention_html()} записан в турнир!\n"
        f"👥 Участников: <b>{len(parts)}</b>", parse_mode="HTML")

# ===== НЕДЕЛЬНАЯ СТАТИСТИКА =====
async def warn_expiry_checker():
    """Каждые 6 часов чистит истёкшие варны и уведомляет если они сгорели"""
    while True:
        await asyncio.sleep(21600)  # 6 часов
        for cid in list(warn_expiry.keys()):
            for uid in list(warn_expiry[cid].keys()):
                old_count = warnings[cid].get(uid, 0)
                clean_expired_warns(cid, uid)
                new_count = warnings[cid].get(uid, 0)
                if old_count > new_count and new_count == 0:
                    try:
                        await bot.send_message(
                            cid,
                            f"⏳ Варны участника <code>{uid}</code> истекли и сброшены автоматически.",
                            parse_mode="HTML")
                    except: pass

mod_stats = defaultdict(lambda: defaultdict(int))  # {cid: {admin_name: count}}

# ===== ШАБЛОНЫ ПРЕДУПРЕЖДЕНИЙ =====
WARN_TEMPLATES = {
    "1": {"label": "🔞 Контент 18+",      "text": "Нарушение: публикация материалов 18+ без соответствующего разрешения"},
    "2": {"label": "📢 Реклама",           "text": "Нарушение: реклама и самопиар без разрешения администрации"},
    "3": {"label": "💬 Спам",              "text": "Нарушение: спам и флуд в чате"},
    "4": {"label": "🤬 Оскорбления",       "text": "Нарушение: оскорбления участников чата"},
    "5": {"label": "🔗 Ссылки",            "text": "Нарушение: публикация сторонних ссылок без разрешения"},
    "6": {"label": "🚫 Провокации",        "text": "Нарушение: провокации и разжигание конфликтов"},
    "7": {"label": "👤 Личные данные",     "text": "Нарушение: публикация личных данных других участников"},
    "8": {"label": "🤖 Флуд ботами",       "text": "Нарушение: использование ботов и спам-команд"},
    "9": {"label": "🗣 Оффтоп",            "text": "Нарушение: систематический оффтоп и мусор в чате"},
    "10": {"label": "⚠️ Правила",          "text": "Нарушение правил чата"},
}

async def tempban_unban(cid: int, uid: int, uname: str, days: int):
    """Снимает временный бан по истечению"""
    await asyncio.sleep(days * 86400)
    try:
        await bot.unban_chat_member(cid, uid, only_if_banned=True)
        await bot.send_message(
            cid,
            f"🔓 Временный бан <b>{uname}</b> истёк — участник может вернуться.",
            parse_mode="HTML")
        await log_action(
            f"🔓 <b>ТЕМПБАН ИСТЁК</b>\n"
            f"👤 <b>{uname}</b>\n"
            f"⏱ Срок {days} дн. истёк автоматически")
        ban_list[cid].pop(uid, None)
    except: pass
    finally:
        tempban_timers.pop((cid, uid), None)

async def send_weekly_stats():
    while True:
        await asyncio.sleep(604800)  # 7 дней
        for cid, stats in chat_stats.items():
            if not stats: continue
            sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:5]
            medals   = ["🥇","🥈","🥉","4️⃣","5️⃣"]
            lines    = [f"📊 <b>НЕДЕЛЬНАЯ СТАТИСТИКА</b>\n"]
            for i, (uid, cnt) in enumerate(sorted_u):
                try:
                    chat_member = await bot.get_chat_member(cid, uid)
                    uname = chat_member.user.full_name
                except: uname = f"ID {uid}"
                lines.append(f"{medals[i]} <b>{uname}</b> — {cnt} сообщений")
            top_rep = sorted(reputation[cid].items(), key=lambda x: x[1], reverse=True)
            if top_rep:
                try:
                    top_uid = top_rep[0][0]
                    top_member = await bot.get_chat_member(cid, top_uid)
                    lines.append(f"\n🌟 Богач недели: <b>{top_member.user.full_name}</b> ({top_rep[0][1]:+d} реп.)")
                except: pass
            try:
                await bot.send_message(LOG_CHANNEL_ID, "\n".join(lines), parse_mode="HTML")
            except: pass

# ===== МАГАЗИН РЕПУТАЦИИ =====
SHOP_ITEMS = {
    # ===== ДЕШЁВЫЕ (10–100) =====
    "1":   {"name": "🌱 Новичок",          "price": 10,    "type": "title"},
    "2":   {"name": "🐣 Птенец",           "price": 15,    "type": "title"},
    "3":   {"name": "🍃 Росток",           "price": 20,    "type": "title"},
    "4":   {"name": "🌊 Волна",            "price": 30,    "type": "title"},
    "5":   {"name": "🌙 Лунатик",          "price": 40,    "type": "title"},
    "6":   {"name": "🎈 Воздушный",        "price": 50,    "type": "title"},
    "7":   {"name": "🐢 Черепаха",         "price": 60,    "type": "title"},
    "8":   {"name": "🌸 Сакура",           "price": 75,    "type": "title"},
    "9":   {"name": "🌀 Вихрь",            "price": 90,    "type": "title"},
    "10":  {"name": "🍀 Удачливый",        "price": 100,   "type": "title"},
    "11":  {"name": "🐸 Лягушонок",        "price": 10,    "type": "title"},
    "12":  {"name": "🌵 Кактус",           "price": 20,    "type": "title"},
    "13":  {"name": "🍄 Грибок",           "price": 25,    "type": "title"},
    "14":  {"name": "🦆 Утка",             "price": 35,    "type": "title"},
    "15":  {"name": "🐧 Пингвин",          "price": 45,    "type": "title"},
    "16":  {"name": "🌻 Подсолнух",        "price": 55,    "type": "title"},
    "17":  {"name": "🍉 Арбуз",            "price": 65,    "type": "title"},
    "18":  {"name": "🦋 Бабочка",          "price": 80,    "type": "title"},
    "19":  {"name": "🐠 Рыбка",            "price": 95,    "type": "title"},
    "20":  {"name": "🌺 Цветок",           "price": 100,   "type": "title"},
    "21":  {"name": "🐱 Котик",            "price": 12,    "type": "title"},
    "22":  {"name": "🐶 Щенок",            "price": 18,    "type": "title"},
    "23":  {"name": "🐰 Зайчик",           "price": 22,    "type": "title"},
    "24":  {"name": "🐹 Хомяк",            "price": 28,    "type": "title"},
    "25":  {"name": "🦔 Ёжик",             "price": 32,    "type": "title"},
    "26":  {"name": "🐥 Цыплёнок",         "price": 38,    "type": "title"},
    "27":  {"name": "🌼 Ромашка",          "price": 42,    "type": "title"},
    "28":  {"name": "🍓 Клубника",         "price": 48,    "type": "title"},
    "29":  {"name": "🎀 Бантик",           "price": 52,    "type": "title"},
    "30":  {"name": "🫧 Пузырёк",          "price": 58,    "type": "title"},
    "31":  {"name": "🌿 Травка",           "price": 62,    "type": "title"},
    "32":  {"name": "🍁 Листик",           "price": 68,    "type": "title"},
    "33":  {"name": "❄️ Снежинка",         "price": 72,    "type": "title"},
    "34":  {"name": "🌞 Солнышко",         "price": 78,    "type": "title"},
    "35":  {"name": "🍩 Пончик",           "price": 85,    "type": "title"},
    "36":  {"name": "🧁 Капкейк",          "price": 92,    "type": "title"},
    "37":  {"name": "🫐 Черника",          "price": 98,    "type": "title"},
    "38":  {"name": "🌝 Луна",             "price": 100,   "type": "title"},
    "39":  {"name": "🐊 Крокодил",         "price": 55,    "type": "title"},
    "40":  {"name": "🦩 Фламинго",         "price": 88,    "type": "title"},
    # ===== СРЕДНИЕ (120–500) =====
    "41":  {"name": "⚡ Активист",         "price": 150,   "type": "title"},
    "42":  {"name": "🌈 Радуга",           "price": 200,   "type": "title"},
    "43":  {"name": "🎭 Анонимус",         "price": 250,   "type": "title"},
    "44":  {"name": "🔥 Ветеран",          "price": 300,   "type": "title"},
    "45":  {"name": "🧠 Мудрец",           "price": 350,   "type": "title"},
    "46":  {"name": "🚀 Космонавт",        "price": 400,   "type": "title"},
    "47":  {"name": "🐉 Дракон",           "price": 450,   "type": "title"},
    "48":  {"name": "🏹 Охотник",          "price": 500,   "type": "title"},
    "49":  {"name": "🎸 Рокер",            "price": 120,   "type": "title"},
    "50":  {"name": "🏄 Сёрфер",           "price": 140,   "type": "title"},
    "51":  {"name": "🎮 Геймер",           "price": 160,   "type": "title"},
    "52":  {"name": "🧙 Маг",              "price": 180,   "type": "title"},
    "53":  {"name": "🦊 Лис",              "price": 220,   "type": "title"},
    "54":  {"name": "🐺 Волк",             "price": 260,   "type": "title"},
    "55":  {"name": "🦅 Орёл",             "price": 280,   "type": "title"},
    "56":  {"name": "🏔 Альпинист",        "price": 320,   "type": "title"},
    "57":  {"name": "🎪 Циркач",           "price": 370,   "type": "title"},
    "58":  {"name": "🧪 Химик",            "price": 420,   "type": "title"},
    "59":  {"name": "🕵 Детектив",         "price": 480,   "type": "title"},
    "60":  {"name": "🎻 Скрипач",          "price": 490,   "type": "title"},
    "61":  {"name": "🏊 Пловец",           "price": 125,   "type": "title"},
    "62":  {"name": "🚵 Байкер",           "price": 135,   "type": "title"},
    "63":  {"name": "🤺 Фехтовальщик",     "price": 155,   "type": "title"},
    "64":  {"name": "🎨 Художник",         "price": 175,   "type": "title"},
    "65":  {"name": "📸 Фотограф",         "price": 190,   "type": "title"},
    "66":  {"name": "🎬 Режиссёр",         "price": 210,   "type": "title"},
    "67":  {"name": "🧑‍💻 Программист",     "price": 230,   "type": "title"},
    "68":  {"name": "🎤 Певец",            "price": 245,   "type": "title"},
    "69":  {"name": "🥊 Боксёр",           "price": 270,   "type": "title"},
    "70":  {"name": "🏋 Силач",            "price": 290,   "type": "title"},
    "71":  {"name": "🤸 Гимнаст",          "price": 310,   "type": "title"},
    "72":  {"name": "🧗 Скалолаз",         "price": 330,   "type": "title"},
    "73":  {"name": "🏇 Наездник",         "price": 345,   "type": "title"},
    "74":  {"name": "🎯 Стрелок",          "price": 360,   "type": "title"},
    "75":  {"name": "🧩 Стратег",          "price": 380,   "type": "title"},
    "76":  {"name": "📚 Книжник",          "price": 395,   "type": "title"},
    "77":  {"name": "🔭 Астроном",         "price": 410,   "type": "title"},
    "78":  {"name": "🎲 Игрок",            "price": 430,   "type": "title"},
    "79":  {"name": "🃏 Шулер",            "price": 460,   "type": "title"},
    "80":  {"name": "🧲 Притяжение",       "price": 475,   "type": "title"},
    # ===== ДОРОГИЕ (600–2000) =====
    "81":  {"name": "💎 Элита",            "price": 600,   "type": "title"},
    "82":  {"name": "🦁 Царь зверей",      "price": 700,   "type": "title"},
    "83":  {"name": "🌟 Звезда",           "price": 800,   "type": "title"},
    "84":  {"name": "🎯 Снайпер",          "price": 900,   "type": "title"},
    "85":  {"name": "👑 Легенда",          "price": 1000,  "type": "title"},
    "86":  {"name": "🔱 Посейдон",         "price": 1200,  "type": "title"},
    "87":  {"name": "⚔️ Воитель",          "price": 1500,  "type": "title"},
    "88":  {"name": "🌌 Галактика",        "price": 1800,  "type": "title"},
    "89":  {"name": "🏆 Чемпион",          "price": 2000,  "type": "title"},
    "90":  {"name": "🧬 Учёный",           "price": 650,   "type": "title"},
    "91":  {"name": "🎖 Генерал",          "price": 750,   "type": "title"},
    "92":  {"name": "🌠 Астронавт",        "price": 850,   "type": "title"},
    "93":  {"name": "🔮 Провидец",         "price": 950,   "type": "title"},
    "94":  {"name": "🦸 Супергерой",       "price": 1100,  "type": "title"},
    "95":  {"name": "🧛 Вампир",           "price": 1300,  "type": "title"},
    "96":  {"name": "🤖 Робот",            "price": 1400,  "type": "title"},
    "97":  {"name": "🦄 Единорог",         "price": 1600,  "type": "title"},
    "98":  {"name": "🌋 Вулкан",           "price": 1700,  "type": "title"},
    "99":  {"name": "🧿 Артефакт",         "price": 1900,  "type": "title"},
    "100": {"name": "🏰 Замок",            "price": 2000,  "type": "title"},
    "101": {"name": "🦂 Скорпион",         "price": 620,   "type": "title"},
    "102": {"name": "🐍 Змея",             "price": 680,   "type": "title"},
    "103": {"name": "🦈 Акула",            "price": 720,   "type": "title"},
    "104": {"name": "🐻‍❄️ Полярный",       "price": 760,   "type": "title"},
    "105": {"name": "🦬 Бизон",            "price": 820,   "type": "title"},
    "106": {"name": "🐘 Слон",             "price": 880,   "type": "title"},
    "107": {"name": "🦏 Носорог",          "price": 920,   "type": "title"},
    "108": {"name": "🐆 Леопард",          "price": 960,   "type": "title"},
    "109": {"name": "🦁 Прайд",            "price": 1050,  "type": "title"},
    "110": {"name": "🐅 Тигр",             "price": 1150,  "type": "title"},
    "111": {"name": "🦅 Беркут",           "price": 1250,  "type": "title"},
    "112": {"name": "🦉 Мудрая сова",      "price": 1350,  "type": "title"},
    "113": {"name": "🐉 Дракон огня",      "price": 1450,  "type": "title"},
    "114": {"name": "🦕 Динозавр",         "price": 1550,  "type": "title"},
    "115": {"name": "🦖 Тираннозавр",      "price": 1650,  "type": "title"},
    "116": {"name": "🌊 Цунами",           "price": 1750,  "type": "title"},
    "117": {"name": "⛈ Гроза",            "price": 1850,  "type": "title"},
    "118": {"name": "🌪 Торнадо",          "price": 1950,  "type": "title"},
    "119": {"name": "☄️ Комета",           "price": 2000,  "type": "title"},
    "120": {"name": "🌑 Затмение",         "price": 2000,  "type": "title"},
    # ===== ЭКСКЛЮЗИВНЫЕ (2500–100000) =====
    "121": {"name": "🐲 Повелитель",       "price": 2500,  "type": "title"},
    "122": {"name": "🌊 Властелин морей",  "price": 3000,  "type": "title"},
    "123": {"name": "🔥 Властелин огня",   "price": 3500,  "type": "title"},
    "124": {"name": "⚡ Громовержец",      "price": 4000,  "type": "title"},
    "125": {"name": "🌑 Тёмный лорд",      "price": 4500,  "type": "title"},
    "126": {"name": "☄️ Метеорит",         "price": 5000,  "type": "title"},
    "127": {"name": "🌌 Повелитель тьмы",  "price": 6000,  "type": "title"},
    "128": {"name": "🧠 Оракул",           "price": 7000,  "type": "title"},
    "129": {"name": "👁 Всевидящий",       "price": 8000,  "type": "title"},
    "130": {"name": "🌀 Хаос",             "price": 9000,  "type": "title"},
    "131": {"name": "💀 Бессмертный",      "price": 10000, "type": "title"},
    "132": {"name": "🕯 Призрак",          "price": 12000, "type": "title"},
    "133": {"name": "⚗️ Алхимик богов",    "price": 15000, "type": "title"},
    "134": {"name": "🌟 Полубог",          "price": 20000, "type": "title"},
    "135": {"name": "👹 Демон",            "price": 25000, "type": "title"},
    "136": {"name": "😈 Сатана",           "price": 30000, "type": "title"},
    "137": {"name": "⚜️ Абсолют",          "price": 40000, "type": "title"},
    "138": {"name": "🌌 Творец",           "price": 50000, "type": "title"},
    "139": {"name": "☠️ Апокалипсис",      "price": 75000, "type": "title"},
    "140": {"name": "💫 БОГ",              "price": 100000,"type": "title"},
    "141": {"name": "🗡 Убийца теней",     "price": 2700,  "type": "title"},
    "142": {"name": "🧟 Зомби",            "price": 2800,  "type": "title"},
    "143": {"name": "🧜 Русалка",          "price": 3200,  "type": "title"},
    "144": {"name": "🧚 Фея",              "price": 3300,  "type": "title"},
    "145": {"name": "🧝 Эльф",             "price": 3600,  "type": "title"},
    "146": {"name": "🧞 Джинн",            "price": 3800,  "type": "title"},
    "147": {"name": "🧌 Тролль",           "price": 4100,  "type": "title"},
    "148": {"name": "🧙‍♀️ Ведьма",          "price": 4200,  "type": "title"},
    "149": {"name": "🧝‍♂️ Лесной страж",    "price": 4300,  "type": "title"},
    "150": {"name": "🦸‍♀️ Героиня",         "price": 4600,  "type": "title"},
    "151": {"name": "🦹 Злодей",           "price": 4800,  "type": "title"},
    "152": {"name": "🧠 Гений",            "price": 5200,  "type": "title"},
    "153": {"name": "🔱 Нептун",           "price": 5500,  "type": "title"},
    "154": {"name": "⚡ Зевс",             "price": 5800,  "type": "title"},
    "155": {"name": "🔥 Прометей",         "price": 6200,  "type": "title"},
    "156": {"name": "🌙 Артемида",         "price": 6500,  "type": "title"},
    "157": {"name": "☀️ Аполлон",          "price": 6800,  "type": "title"},
    "158": {"name": "⚔️ Арес",             "price": 7200,  "type": "title"},
    "159": {"name": "🦅 Зоркий",           "price": 7500,  "type": "title"},
    "160": {"name": "🌊 Посейдон II",      "price": 7800,  "type": "title"},
    "161": {"name": "🏛 Олимпиец",         "price": 8200,  "type": "title"},
    "162": {"name": "🌌 Вселенная",        "price": 8500,  "type": "title"},
    "163": {"name": "🔮 Мистик",           "price": 8800,  "type": "title"},
    "164": {"name": "💥 Взрыв",            "price": 9200,  "type": "title"},
    "165": {"name": "🌠 Сверхновая",       "price": 9500,  "type": "title"},
    "166": {"name": "🕳 Чёрная дыра",      "price": 9800,  "type": "title"},
    "167": {"name": "👾 Пришелец",         "price": 11000, "type": "title"},
    "168": {"name": "🛸 НЛО",              "price": 13000, "type": "title"},
    "169": {"name": "🌍 Планета",          "price": 14000, "type": "title"},
    "170": {"name": "⭐ Квазар",           "price": 16000, "type": "title"},
    "171": {"name": "🌌 Туманность",       "price": 17000, "type": "title"},
    "172": {"name": "💠 Кристалл",         "price": 18000, "type": "title"},
    "173": {"name": "🔱 Трезубец богов",   "price": 19000, "type": "title"},
    "174": {"name": "👁‍🗨 Третий глаз",     "price": 21000, "type": "title"},
    "175": {"name": "🌑 Пустота",          "price": 22000, "type": "title"},
    "176": {"name": "💀 Смерть",           "price": 23000, "type": "title"},
    "177": {"name": "⚰️ Гробовщик",        "price": 24000, "type": "title"},
    "178": {"name": "🩸 Кровавый",         "price": 26000, "type": "title"},
    "179": {"name": "🌋 Магма",            "price": 27000, "type": "title"},
    "180": {"name": "🧿 Древний",          "price": 28000, "type": "title"},
    "181": {"name": "📿 Шаман",            "price": 29000, "type": "title"},
    "182": {"name": "🗿 Идол",             "price": 31000, "type": "title"},
    "183": {"name": "⚡ Молния богов",     "price": 32000, "type": "title"},
    "184": {"name": "🌊 Великий потоп",    "price": 33000, "type": "title"},
    "185": {"name": "🔥 Адское пламя",     "price": 34000, "type": "title"},
    "186": {"name": "❄️ Ледяной трон",     "price": 35000, "type": "title"},
    "187": {"name": "🌪 Буря хаоса",       "price": 36000, "type": "title"},
    "188": {"name": "🌑 Тьма вечная",      "price": 37000, "type": "title"},
    "189": {"name": "✨ Свет вечный",      "price": 38000, "type": "title"},
    "190": {"name": "⚖️ Судья",            "price": 39000, "type": "title"},
    "191": {"name": "🏴‍☠️ Пират",           "price": 41000, "type": "title"},
    "192": {"name": "👻 Дух",              "price": 42000, "type": "title"},
    "193": {"name": "🦋 Бессмертная душа", "price": 43000, "type": "title"},
    "194": {"name": "🌀 Бесконечность",    "price": 44000, "type": "title"},
    "195": {"name": "🎭 Маска богов",      "price": 45000, "type": "title"},
    "196": {"name": "👁 Мировое зло",      "price": 46000, "type": "title"},
    "197": {"name": "🔱 Владыка",          "price": 47000, "type": "title"},
    "198": {"name": "💎 Алмазный трон",    "price": 48000, "type": "title"},
    "199": {"name": "🌌 Антиматерия",      "price": 49000, "type": "title"},
    "200": {"name": "🕳 Сингулярность",    "price": 55000, "type": "title"},
    "201": {"name": "🌑 Конец света",      "price": 60000, "type": "title"},
    "202": {"name": "⚡ Первозданный",     "price": 65000, "type": "title"},
    "203": {"name": "🔥 Феникс богов",     "price": 70000, "type": "title"},
    "204": {"name": "💀 Жнец душ",         "price": 80000, "type": "title"},
    "205": {"name": "🌌 Омега",            "price": 85000, "type": "title"},
    "206": {"name": "⚜️ Альфа",            "price": 90000, "type": "title"},
    "207": {"name": "👁 Всесущий",         "price": 95000, "type": "title"},
    "208": {"name": "🌀 Абсолютный хаос",  "price": 110000,"type": "title"},
    "209": {"name": "💫 Архангел",         "price": 120000,"type": "title"},
    "210": {"name": "☠️ Конец всего",      "price": 130000,"type": "title"},
    "211": {"name": "🌌 Начало времён",    "price": 140000,"type": "title"},
    "212": {"name": "⚡ Источник силы",    "price": 150000,"type": "title"},
    "213": {"name": "🔱 Трон богов",       "price": 175000,"type": "title"},
    "214": {"name": "💀 Вечная тьма",      "price": 200000,"type": "title"},
    "215": {"name": "🌟 Вечный свет",      "price": 200000,"type": "title"},
    "216": {"name": "🌌 Мультивселенная",  "price": 250000,"type": "title"},
    "217": {"name": "👁 Создатель миров",  "price": 300000,"type": "title"},
    "218": {"name": "⚜️ Высший разум",     "price": 350000,"type": "title"},
    "219": {"name": "💫 Всемогущий",       "price": 400000,"type": "title"},
    "220": {"name": "🌀 Первопричина",     "price": 450000,"type": "title"},
    "221": {"name": "🐉 Древний дракон",   "price": 500000,"type": "title"},
    "222": {"name": "🌌 Бесконечность²",   "price": 600000,"type": "title"},
    "223": {"name": "⚡ Над богами",       "price": 700000,"type": "title"},
    "224": {"name": "👑 Король королей",   "price": 800000,"type": "title"},
    "225": {"name": "🌟 Сверхсущество",    "price": 900000,"type": "title"},
    "226": {"name": "💀 Смерть богов",     "price": 1000000,"type": "title"},
    "227": {"name": "🌌 Пустота богов",    "price": 1000000,"type": "title"},
    "228": {"name": "🔥 Огонь творения",   "price": 1000000,"type": "title"},
    "229": {"name": "⚜️ Абсолют богов",    "price": 1000000,"type": "title"},
    "230": {"name": "👁 Создатель",        "price": 1000000,"type": "title"},
    "231": {"name": "🍕 Пиццалюб",         "price": 15,    "type": "title"},
    "232": {"name": "😴 Соня",             "price": 18,    "type": "title"},
    "233": {"name": "🤓 Ботаник",          "price": 22,    "type": "title"},
    "234": {"name": "😎 Крутой",           "price": 30,    "type": "title"},
    "235": {"name": "🤡 Клоун",            "price": 25,    "type": "title"},
    "236": {"name": "👽 Инопланетянин",    "price": 35,    "type": "title"},
    "237": {"name": "🥷 Ниндзя",           "price": 130,   "type": "title"},
    "238": {"name": "🤠 Ковбой",           "price": 145,   "type": "title"},
    "239": {"name": "🧑‍🚀 Пилот",           "price": 165,   "type": "title"},
    "240": {"name": "👨‍🍳 Шеф-повар",       "price": 185,   "type": "title"},
    "241": {"name": "👨‍🎤 Артист",           "price": 195,   "type": "title"},
    "242": {"name": "🧑‍🔬 Исследователь",   "price": 215,   "type": "title"},
    "243": {"name": "🧑‍⚕️ Доктор",          "price": 235,   "type": "title"},
    "244": {"name": "👨‍✈️ Капитан",          "price": 255,   "type": "title"},
    "245": {"name": "🧑‍🏫 Наставник",        "price": 275,   "type": "title"},
    "246": {"name": "👨‍🚒 Пожарный",         "price": 295,   "type": "title"},
    "247": {"name": "👮 Блюститель",        "price": 315,   "type": "title"},
    "248": {"name": "🕴 Агент",             "price": 355,   "type": "title"},
    "249": {"name": "🧑‍⚖️ Судья",           "price": 385,   "type": "title"},
    "250": {"name": "🌈 Легенда чата",      "price": 999,   "type": "title"},

    # ── Роли (специальные теги) ──
    "r1":  {"name": "🔥 [ОГОНЬ]",        "price": 500,    "type": "role", "desc": "Тег в профиле"},
    "r2":  {"name": "❄️ [ЛЁД]",           "price": 500,    "type": "role", "desc": "Тег в профиле"},
    "r3":  {"name": "⚡ [ГРОМ]",          "price": 500,    "type": "role", "desc": "Тег в профиле"},
    "r4":  {"name": "🌙 [НОЧЬ]",          "price": 500,    "type": "role", "desc": "Тег в профиле"},
    "r5":  {"name": "☀️ [СОЛНЦЕ]",        "price": 500,    "type": "role", "desc": "Тег в профиле"},
    "r6":  {"name": "💀 [ТЬМА]",          "price": 750,    "type": "role", "desc": "Тег в профиле"},
    "r7":  {"name": "👑 [КОРОЛЬ]",         "price": 1000,   "type": "role", "desc": "Тег в профиле"},
    "r8":  {"name": "🐉 [ДРАКОН]",        "price": 1000,   "type": "role", "desc": "Тег в профиле"},
    "r9":  {"name": "🌈 [РАДУГА]",        "price": 1500,   "type": "role", "desc": "Тег в профиле"},
    "r10": {"name": "✨ [ЛЕГЕНДА]",        "price": 5000,   "type": "role", "desc": "Эксклюзивный тег"},
    # ── Цвета ника ──
    "c1":  {"name": "🔴 Красный ник",     "price": 800,    "type": "color", "desc": "Цветной бейдж в профиле"},
    "c2":  {"name": "🔵 Синий ник",       "price": 800,    "type": "color", "desc": "Цветной бейдж в профиле"},
    "c3":  {"name": "🟢 Зелёный ник",     "price": 800,    "type": "color", "desc": "Цветной бейдж в профиле"},
    "c4":  {"name": "🟡 Золотой ник",     "price": 1200,   "type": "color", "desc": "Премиум цвет"},
    "c5":  {"name": "🟣 Фиолетовый ник",  "price": 1200,   "type": "color", "desc": "Премиум цвет"},
    "c6":  {"name": "⚫ Чёрный ник",      "price": 2000,   "type": "color", "desc": "Редкий цвет"},
    "c7":  {"name": "🌈 Радужный ник",    "price": 10000,  "type": "color", "desc": "Легендарный цвет"},
    # ── Эффекты ──
    "e1":  {"name": "💫 Эффект: Звезда",  "price": 2000,   "type": "effect", "desc": "+15% к репе постоянно"},
    "e2":  {"name": "🔮 Эффект: Магия",   "price": 3000,   "type": "effect", "desc": "+20% к XP постоянно"},
    "e3":  {"name": "🛡 Эффект: Щит",    "price": 5000,   "type": "effect", "desc": "Защита от -репы"},
    "e4":  {"name": "⚡ Эффект: Молния",  "price": 7500,   "type": "effect", "desc": "x1.5 XP навсегда"},
    "e5":  {"name": "👑 Эффект: Корона",  "price": 50000,  "type": "effect", "desc": "x2 ко всему навсегда"},

}
user_titles = defaultdict(dict)  # {uid: {"title": "...", "purchased": [...]}}

def kb_shop(cid: int, uid: int, page: int = 0) -> InlineKeyboardMarkup:
    items = list(SHOP_ITEMS.items())
    per_page = 8
    start = page * per_page
    page_items = items[start:start + per_page]
    rows = []
    for i in range(0, len(page_items), 2):
        row = []
        for item_id, item in page_items[i:i+2]:
            owned = item["name"] in user_titles[uid].get("purchased", [])
            active = user_titles[uid].get("title") == item["name"]
            icon = "🟢" if active else ("✅" if owned else "🛒")
            label = f"{icon} {item['name']} {item['price']}⭐"
            row.append(InlineKeyboardButton(text=label, callback_data=f"shop:buy:{item_id}:{uid}:{cid}:{page}"))
        rows.append(row)
    nav = []
    total_pages = (len(items) + per_page - 1) // per_page
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"shop:page:{page-1}:{uid}:{cid}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="shop:noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"shop:page:{page+1}:{uid}:{cid}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🎭 Мой титул", callback_data=f"shop:mytitle:{uid}:{cid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("shop"))
async def cmd_shop(message: Message):
    uid = message.from_user.id; cid = message.chat.id
    rep = reputation[cid].get(uid, 0)
    try:
        try: await message.delete()
        except: pass
    except: pass
    await answer_auto_delete(
        f"🏪 <b>Магазин титулов</b>\n\n"
        f"💰 Твоя репутация: <b>{rep:+d}</b>\n\n"
        f"🟢 — активный | ✅ — куплено | 🛒 — купить\n"
        f"Всего <b>{len(SHOP_ITEMS)}</b> титулов, листай страницы!",
        parse_mode="HTML",
        reply_markup=kb_shop(cid, uid, 0))

@dp.callback_query(F.data.startswith("shop:"))
async def cb_shop(call: CallbackQuery):
    parts = call.data.split(":")
    action = parts[1]

    if action == "noop":
        await call.answer(); return

    elif action == "page":
        page, uid, cid = int(parts[2]), int(parts[3]), int(parts[4])
        if call.from_user.id != uid:
            await call.answer("❌ Это не твой магазин!", show_alert=True); return
        rep = reputation[cid].get(uid, 0)
        await call.message.edit_text(
            f"🏪 <b>Магазин титулов</b>\n\n"
            f"💰 Твоя репутация: <b>{rep:+d}</b>\n\n"
            f"🟢 — активный | ✅ — куплено | 🛒 — купить\n"
            f"Всего <b>{len(SHOP_ITEMS)}</b> титулов, листай страницы!",
            parse_mode="HTML",
            reply_markup=kb_shop(cid, uid, page))
        await call.answer()

    elif action == "buy":
        item_id, uid, cid = parts[2], int(parts[3]), int(parts[4])
        page = int(parts[5]) if len(parts) > 5 else 0
        if call.from_user.id != uid:
            await call.answer("❌ Это не твой магазин!", show_alert=True); return
        item = SHOP_ITEMS.get(item_id)
        if not item:
            await call.answer("❌ Товар не найден!", show_alert=True); return
        purchased = user_titles[uid].get("purchased", [])
        if item["name"] in purchased:
            user_titles[uid]["title"] = item["name"]
            await call.answer(f"🟢 Титул «{item['name']}» активирован!", show_alert=True)
            await call.message.edit_reply_markup(reply_markup=kb_shop(cid, uid, page))
            return
        rep = reputation[cid].get(uid, 0)
        if rep < item["price"]:
            await call.answer(
                f"💸 Недостаточно репутации!\nНужно: {item['price']} | У тебя: {rep}",
                show_alert=True); return
        reputation[cid][uid] -= item["price"]
        save_data()
        if "purchased" not in user_titles[uid]:
            user_titles[uid]["purchased"] = []
        user_titles[uid]["purchased"].append(item["name"])
        user_titles[uid]["title"] = item["name"]
        await call.answer(
            f"🎉 Куплено: {item['name']}\n💸 Потрачено: {item['price']} реп.\n💰 Осталось: {reputation[cid][uid]:+d}",
            show_alert=True)
        await call.message.edit_text(
            f"🏪 <b>Магазин титулов</b>\n\n"
            f"💰 Твоя репутация: <b>{reputation[cid].get(uid, 0):+d}</b>\n\n"
            f"🟢 — активный | ✅ — куплено | 🛒 — купить\n"
            f"Всего <b>{len(SHOP_ITEMS)}</b> титулов, листай страницы!",
            parse_mode="HTML",
            reply_markup=kb_shop(cid, uid, page))

    elif action == "mytitle":
        uid, cid = int(parts[2]), int(parts[3])
        if call.from_user.id != uid:
            await call.answer("❌ Это не твой магазин!", show_alert=True); return
        title = user_titles[uid].get("title", "нет")
        purchased = user_titles[uid].get("purchased", [])
        bought_str = ", ".join(purchased) if purchased else "ничего"
        await call.answer(
            f"🎭 Активный: {title}\n\n📦 Куплено ({len(purchased)}):\n{bought_str}",
            show_alert=True)

# ===== РЕПОРТЫ — УЛУЧШЕННАЯ СИСТЕМА =====
report_cooldown = {}

REPORT_CATEGORIES = {
    "18":     ("🔞", "Контент 18+"),
    "ads":    ("📢", "Реклама"),
    "insult": ("💢", "Оскорбления"),
    "spam":   ("🤖", "Спам"),
    "other":  ("⚠️", "Другое"),
}

def kb_report_category(target_id: int, msg_id: int) -> InlineKeyboardMarkup:
    rows = []
    items = list(REPORT_CATEGORIES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, (emoji, label) in items[i:i+2]:
            row.append(InlineKeyboardButton(
                text=f"{emoji} {label}",
                callback_data=f"report_cat:{key}:{target_id}:{msg_id}"
            ))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="report_cat:cancel:0:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_report_action_v2(cid: int, idx: int, target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Варн",      callback_data=f"rpt2:warn:{cid}:{idx}:{target_id}"),
         InlineKeyboardButton(text="🔇 Мут 1ч",   callback_data=f"rpt2:mute:{cid}:{idx}:{target_id}")],
        [InlineKeyboardButton(text="🔨 Бан",       callback_data=f"rpt2:ban:{cid}:{idx}:{target_id}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"rpt2:reject:{cid}:{idx}:{target_id}")],
        [InlineKeyboardButton(text="📝 Заметка",   callback_data=f"rpt2:note:{cid}:{idx}:{target_id}")],
    ])

@dp.message(Command("report"))
async def cmd_report(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await reply_auto_delete(message,
            "📋 <b>Как использовать:</b>\n"
            "↩️ Ответь на сообщение нарушителя:\n"
            "<code>/report</code> — выбери категорию\n"
            "<code>/report причина</code> — с причиной", parse_mode="HTML"); return
    reporter = message.from_user
    target = message.reply_to_message.from_user
    cid = message.chat.id
    if target.id == reporter.id:
        await reply_auto_delete(message, "😏 Сам на себя жалуешься?"); return
    if reporter.id in report_blocked:
        await reply_auto_delete(message, "🚫 Твоё право на репорты заблокировано!"); return
    now = time(); key = (cid, reporter.id)
    if key in report_cooldown and now - report_cooldown[key] < 300:
        left = int(300 - (now - report_cooldown[key]))
        await reply_auto_delete(message, f"⏳ Подожди ещё <b>{left} сек.</b>", parse_mode="HTML"); return
    report_cooldown[key] = now
    # Контекст сообщений
    context_msgs = []
    for mid, ts in user_msg_ids[cid].get(target.id, []):
        if mid < message.reply_to_message.message_id and mid in message_cache:
            context_msgs.append(f"  ▸ {message_cache[mid].get('text','[медиа]')[:80]}")
    context_msgs = context_msgs[-3:]
    try: await message.delete()
    except: pass
    cat_msg = await message.answer(
        f"🚨 <b>Репорт на {target.mention_html()}</b>\nВыбери категорию:",
        parse_mode="HTML",
        reply_markup=kb_report_category(target.id, message.reply_to_message.message_id))
    pending[reporter.id] = {
        "action": "report_pending",
        "target_id": target.id, "target_name": target.full_name,
        "chat_id": cid, "msg_id": message.reply_to_message.message_id,
        "reason": command.args or "", "context": context_msgs,
        "reporter_id": reporter.id, "reporter_name": reporter.full_name,
    }
    asyncio.create_task(schedule_delete(cat_msg))

@dp.callback_query(F.data.startswith("report_cat:"))
async def cb_report_category(call: CallbackQuery):
    parts = call.data.split(":")
    cat = parts[1]
    if cat == "cancel":
        await call.message.delete()
        if call.from_user.id in pending: del pending[call.from_user.id]
        await call.answer("Отменено"); return
    target_id = int(parts[2]); msg_id = int(parts[3])
    cid = call.message.chat.id
    p = pending.get(call.from_user.id, {})
    if not p or p.get("action") != "report_pending":
        await call.answer("⚠️ Устарело", show_alert=True); return
    cat_emoji, cat_label = REPORT_CATEGORIES.get(cat, ("⚠️", "Другое"))
    reason = p.get("reason") or cat_label
    context = p.get("context", [])
    reporter_id = p.get("reporter_id", call.from_user.id)
    reporter_name = p.get("reporter_name", call.from_user.full_name)
    target_name = p.get("target_name", f"ID{target_id}")
    import time as _tr
    existing = [r for r in report_queue[cid] if r.get("target") == target_id]
    priority = "🔴 HIGH" if len(existing) >= 2 else "🟡 NORMAL"
    auto_action = ""
    # Авто-варн при 5+ репортах
    if len(existing) >= 4:
        warnings[cid][target_id] += 1
        auto_action = f"\n⚡ <b>Авто-варн</b> ({len(existing)+1} репортов)"
        save_data()
    # Авто-мут при 3+ уникальных репортерах
    unique_reporters = set(r.get("reporter") for r in existing) | {reporter_id}
    if len(unique_reporters) >= 3:
        try:
            await bot.restrict_chat_member(cid, target_id,
                ChatPermissions(can_send_messages=False), until_date=timedelta(minutes=10))
            auto_action += "\n🔇 <b>Авто-мут 10 мин</b> (3+ жалобы)"
        except: pass
    report_entry = {
        "reporter": reporter_id, "reporter_name": reporter_name,
        "target": target_id, "target_name": target_name,
        "text": reason, "category": cat_label, "category_emoji": cat_emoji,
        "priority": priority, "ts": _tr.time(), "msg_id": msg_id,
        "context": context, "status": "new", "assigned_mod": None, "note": "",
    }
    report_queue[cid].append(report_entry)
    idx = len(report_queue[cid]) - 1
    ctx_text = "\n".join(context) if context else "  нет данных"
    is_admin_target = await is_admin_by_id(cid, target_id)
    report_log = (
        f"🚨 <b>РЕПОРТ</b> {priority}\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{cat_emoji} <b>{cat_label}</b>\n"
        f"🎯 На: <b>{target_name}</b> (<code>{target_id}</code>)\n"
        f"👤 От: 🕵️ анонимно\n"
        f"📝 Причина: <b>{reason}</b>\n"
        f"💬 Чат: <b>{call.message.chat.title}</b>\n"
        f"🔗 <a href=\'https://t.me/c/{str(cid)[4:]}/{msg_id}\'>перейти</a>\n"
        f"📜 Контекст:\n{ctx_text}{auto_action}"
    )
    await log_action(report_log)
    if is_admin_target:
        vote_key = f"{cid}_{target_id}_{idx}"
        report_mod_votes[vote_key] = {}
        vote_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"rpt_vote:yes:{vote_key}"),
            InlineKeyboardButton(text="❌ Отклонить",   callback_data=f"rpt_vote:no:{vote_key}"),
        ]])
        await bot.send_message(OWNER_ID,
            f"⚠️ <b>РЕПОРТ НА АДМИНИСТРАТОРА</b>\n🎯 {target_name}\n{cat_emoji} {cat_label}\n📝 {reason}\nТребует 2 голоса:",
            parse_mode="HTML", reply_markup=vote_kb)
        await call.message.edit_text("✅ Жалоба на админа отправлена — ожидает 2 голоса модераторов.")
    else:
        await call.message.edit_text(
            f"✅ <b>Жалоба принята!</b>\n{cat_emoji} {cat_label} | {priority}\n🕐 Модераторы рассмотрят в течение часа.",
            parse_mode="HTML")
        try:
            admins = await bot.get_chat_administrators(cid)
            for adm in admins:
                if adm.user.is_bot: continue
                try:
                    await bot.send_message(adm.user.id,
                        f"🚨 <b>НОВЫЙ РЕПОРТ</b> {priority}\n{cat_emoji} {cat_label}\n🎯 На: <b>{target_name}</b>\n📝 {reason}\n💬 {call.message.chat.title}",
                        parse_mode="HTML", reply_markup=kb_report_action_v2(cid, idx, target_id))
                except: pass
        except: pass
    # Дедлайн 1ч
    async def _deadline(c, i, tname):
        await asyncio.sleep(3600)
        q = report_queue.get(c, [])
        if i < len(q) and q[i].get("status") == "new":
            await bot.send_message(OWNER_ID,
                f"⏰ <b>ДЕДЛАЙН!</b> Репорт на <b>{tname}</b> не обработан за 1ч!", parse_mode="HTML")
    asyncio.create_task(_deadline(cid, idx, target_name))
    if call.from_user.id in pending: del pending[call.from_user.id]
    await call.answer()

@dp.callback_query(F.data.startswith("rpt2:"))
async def cb_report_action_v2(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫 Только для администраторов!", show_alert=True); return
    parts = call.data.split(":")
    action, cid, idx, target_id = parts[1], int(parts[2]), int(parts[3]), int(parts[4])
    queue = report_queue.get(cid, [])
    if idx >= len(queue):
        await call.answer("❌ Уже обработан", show_alert=True); return
    report = queue[idx]
    mod_name = call.from_user.full_name
    if action == "reject":
        report["status"] = "rejected"; report["assigned_mod"] = mod_name
        report_mod_stats[cid][call.from_user.id] += 1
        try: await bot.send_message(report["reporter"], f"ℹ️ Твоя жалоба на <b>{report['target_name']}</b> отклонена.", parse_mode="HTML")
        except: pass
        await call.message.edit_text(
            f"❌ Отклонено | 👮 {mod_name}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔄 Взять снова", callback_data=f"rpt2:takeover:{cid}:{idx}:{target_id}")
            ]]))
        await call.answer("Отклонено"); return
    elif action == "takeover":
        report["status"] = "new"; report["assigned_mod"] = None
        await call.message.edit_text(
            f"🔄 Репорт снова доступен",
            reply_markup=kb_report_action_v2(cid, idx, target_id))
        await call.answer("✅ Доступен снова"); return
    elif action == "note":
        pending[call.from_user.id] = {"action": "report_note", "chat_id": cid, "report_idx": idx, "target_id": target_id}
        await call.message.edit_text("📝 Напиши заметку к репорту:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"rpt2:cancel_note:{cid}:{idx}:{target_id}")
            ]]))
        await call.answer(); return
    elif action == "cancel_note":
        await call.message.edit_text("📋 Репорт:", reply_markup=kb_report_action_v2(cid, idx, target_id))
        await call.answer(); return
    if report.get("status") != "new":
        await call.answer(f"⚠️ Уже обработан: {report.get('assigned_mod','?')}", show_alert=True); return
    try:
        result = ""
        if action == "warn":
            warnings[cid][target_id] += 1; result = "⚡ Варн"
        elif action == "mute":
            await bot.restrict_chat_member(cid, target_id, ChatPermissions(can_send_messages=False), until_date=timedelta(hours=1))
            result = "🔇 Мут 1ч"
        elif action == "ban":
            await bot.ban_chat_member(cid, target_id); result = "🔨 Бан"
        report["status"] = "accepted"; report["assigned_mod"] = mod_name
        report_mod_stats[cid][call.from_user.id] += 1
        save_data()
        try: await bot.send_message(report["reporter"], f"✅ Жалоба на <b>{report['target_name']}</b> принята! {result}", parse_mode="HTML")
        except: pass
        await log_action(f"✅ <b>РЕПОРТ ОБРАБОТАН</b>\n🎯 {report['target_name']}\n👮 {mod_name}\n⚙️ {result}")
        await call.message.edit_text(f"✅ <b>Обработано</b>\n{result}\n👮 {mod_name}", parse_mode="HTML")
        await call.answer("✅ Готово")
    except Exception as e:
        await call.answer(f"❌ {e}", show_alert=True)

@dp.callback_query(F.data.startswith("rpt_vote:"))
async def cb_report_vote(call: CallbackQuery):
    parts = call.data.split(":", 2)
    vote, vote_key = parts[1], parts[2]
    if vote_key not in report_mod_votes:
        await call.answer("Устарело", show_alert=True); return
    report_mod_votes[vote_key][call.from_user.id] = vote
    yes_v = sum(1 for v in report_mod_votes[vote_key].values() if v == "yes")
    no_v  = sum(1 for v in report_mod_votes[vote_key].values() if v == "no")
    await call.answer(f"Твой голос: {'✅' if vote=='yes' else '❌'}")
    if yes_v >= 2:
        await call.message.edit_text("✅ 2 модератора подтвердили репорт на админа. Решение — за владельцем.")
        await bot.send_message(OWNER_ID, f"⚠️ <b>2 голоса за репорт на админа!</b>\nКлюч: {vote_key}", parse_mode="HTML")
        del report_mod_votes[vote_key]
    elif no_v >= 2:
        await call.message.edit_text("❌ Репорт на администратора отклонён (2 против)")
        del report_mod_votes[vote_key]
    else:
        await call.message.edit_text(f"🗳 Голоса: ✅ {yes_v} / ❌ {no_v} (нужно 2)", reply_markup=call.message.reply_markup)

@dp.message(Command("blockreport"))
async def cmd_block_report(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение юзера"); return
    uid = message.reply_to_message.from_user.id
    report_blocked.add(uid)
    await reply_auto_delete(message,
        f"🚫 {message.reply_to_message.from_user.mention_html()} — право на репорты заблокировано",
        parse_mode="HTML")

@dp.message(Command("reportstats"))
async def cmd_report_stats(message: Message):
    if message.from_user.id != OWNER_ID: return
    cid = message.chat.id
    queue = report_queue.get(cid, [])
    from collections import Counter
    total = len(queue)
    new_r = sum(1 for r in queue if r.get("status") == "new")
    done_r = sum(1 for r in queue if r.get("status") == "accepted")
    rej_r = sum(1 for r in queue if r.get("status") == "rejected")
    top_targets = Counter(r["target_name"] for r in queue).most_common(5)
    lines = [f"📊 <b>Статистика репортов</b>\n━━━━━━━━━━━━━━━━━━━━━━\n",
             f"📋 Всего: <b>{total}</b> | 🆕 Новых: <b>{new_r}</b>",
             f"✅ Принято: <b>{done_r}</b> | ❌ Отклонено: <b>{rej_r}</b>\n",
             "🎯 <b>Чаще жалуются на:</b>"]
    for name, cnt in top_targets:
        lines.append(f"  ▸ {name} — {cnt}x")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")
@dp.message(Command("chatstats"))
async def cmd_chatstats(message: Message):
    cid = message.chat.id
    from datetime import datetime
    from collections import Counter
    hour_totals = Counter()
    for uid_hours in hourly_stats[cid].values():
        for h, cnt in uid_hours.items():
            hour_totals[h] += cnt
    top_hours = hour_totals.most_common(3)
    hours_text = "  ".join(f"{h}:00 ({c})" for h, c in top_hours) if top_hours else "нет данных"
    top_words = word_stats[cid].most_common(10) if word_stats[cid] else []
    words_text = ", ".join(f"<b>{w}</b> ({c})" for w, c in top_words) if top_words else "нет данных"
    today = datetime.now().strftime("%d.%m.%Y")
    today_msgs = sum(uid_days.get(today, 0) for uid_days in daily_stats[cid].values())
    total_msgs = sum(chat_stats[cid].values())
    top_user_id = max(chat_stats[cid], key=chat_stats[cid].get) if chat_stats[cid] else None
    if top_user_id:
        try:
            member = await bot.get_chat_member(cid, top_user_id)
            top_name = member.user.full_name
            top_count = chat_stats[cid][top_user_id]
        except:
            top_name = f"ID{top_user_id}"
            top_count = chat_stats[cid][top_user_id]
    else:
        top_name, top_count = "—", 0
    unique_users = len(chat_stats[cid])
    lines = [
        "╔═══════════════════╗",
        "📊  <b>СТАТИСТИКА ЧАТА</b>",
        "╚═══════════════════╝",
        "",
        f"💬 <b>Всего сообщений:</b> {total_msgs}",
        f"📅 <b>Сегодня:</b> {today_msgs}",
        f"👥 <b>Участников:</b> {unique_users}",
        "",
        f"🏆 <b>Самый активный:</b>",
        f"    {top_name} — {top_count} сообщений",
        "",
        f"⏰ <b>Пиковые часы:</b>",
        f"    {hours_text}",
        "",
        f"🔤 <b>Топ слова:</b>",
        f"    {words_text}",
    ]
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


@dp.message(Command("mystats"))
async def cmd_mystats(message: Message):
    uid = message.from_user.id
    cid = message.chat.id
    from datetime import datetime
    total = chat_stats[cid].get(uid, 0)
    today = datetime.now().strftime("%d.%m.%Y")
    today_count = daily_stats[cid][uid].get(today, 0)
    my_hours = hourly_stats[cid][uid]
    if my_hours:
        best_hour = max(my_hours, key=my_hours.get)
        best_hour_str = f"{best_hour}:00–{best_hour+1}:00"
    else:
        best_hour_str = "нет данных"
    active_days = len(daily_stats[cid][uid])
    streak = streaks[cid].get(uid, 0)
    level = levels[cid].get(uid, 0)
    xp = xp_data[cid].get(uid, 0)
    sorted_users = sorted(chat_stats[cid].items(), key=lambda x: x[1], reverse=True)
    rank = next((i+1 for i, (u, _) in enumerate(sorted_users) if u == uid), 0)
    lines = [
        "╔═══════════════════╗",
        "📈  <b>МОЯ СТАТИСТИКА</b>",
        "╚═══════════════════╝",
        "",
        f"👤 {message.from_user.mention_html()}",
        "",
        f"💬 <b>Всего сообщений:</b> {total}",
        f"📅 <b>Сегодня:</b> {today_count}",
        f"🗓 <b>Активных дней:</b> {active_days}",
        f"🔥 <b>Стрик:</b> {streak} дней",
        f"⚡ <b>Уровень:</b> {level} (XP: {xp})",
        f"🏅 <b>Ранг в чате:</b> #{rank}",
        f"⏰ <b>Лучшее время:</b> {best_hour_str}",
    ]
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


@dp.message(Command("topactive"))
async def cmd_topactive(message: Message):
    cid = message.chat.id
    from datetime import datetime
    today = datetime.now().strftime("%d.%m.%Y")
    today_scores = [
        (uid, days.get(today, 0))
        for uid, days in daily_stats[cid].items()
        if days.get(today, 0) > 0
    ]
    today_scores.sort(key=lambda x: x[1], reverse=True)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["╔═══════════════════╗", "🔥  <b>ТОП АКТИВНЫХ СЕГОДНЯ</b>", "╚═══════════════════╝", ""]
    for i, (uid, cnt) in enumerate(today_scores[:10]):
        try:
            m = await bot.get_chat_member(cid, int(uid))
            name = m.user.full_name
        except:
            name = f"ID{uid}"
        medal = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{medal} <b>{name}</b> — {cnt} сообщений")
    if not today_scores:
        lines.append("Сегодня ещё никто не писал!")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")



# ===== 💸 ПЕРЕВОД РЕПУТАЦИИ =====
@dp.message(Command("giverep"))
async def cmd_giverep(message: Message, command: CommandObject):
    if not message.reply_to_message:
        await reply_auto_delete(message,
            "💸 Реплайни на сообщение и напиши:\n<code>/giverep 50</code>",
            parse_mode="HTML"); return
    if not command.args:
        await reply_auto_delete(message, "⚠️ Укажи сумму: <code>/giverep 50</code>", parse_mode="HTML"); return
    try:
        amount = int(command.args.strip())
        if amount <= 0: raise ValueError
    except:
        await reply_auto_delete(message, "❌ Сумма должна быть числом больше 0"); return
    uid = message.from_user.id
    cid = message.chat.id
    target = message.reply_to_message.from_user
    if target.id == uid:
        await reply_auto_delete(message, "❌ Нельзя переводить самому себе!"); return
    if target.is_bot:
        await reply_auto_delete(message, "❌ Нельзя переводить боту!"); return
    from time import time
    now = time()
    cd_key = f"{uid}_{cid}"
    if cd_key in rep_transfer_cooldown and now - rep_transfer_cooldown[cd_key] < 3600:
        left = int(3600 - (now - rep_transfer_cooldown[cd_key])) // 60
        await reply_auto_delete(message, f"⏳ Следующий перевод через {left} мин."); return
    sender_rep = reputation[cid].get(uid, 0)
    if sender_rep < amount:
        await reply_auto_delete(message,
            f"❌ Недостаточно репутации!\nУ тебя: <b>{sender_rep}</b> | Нужно: <b>{amount}</b>",
            parse_mode="HTML"); return
    reputation[cid][uid] -= amount
    reputation[cid][target.id] = reputation[cid].get(target.id, 0) + amount
    rep_transfer_cooldown[cd_key] = now
    save_data()
    await reply_auto_delete(message,
        "╔═══════════════════╗\n"
        "💸  <b>ПЕРЕВОД РЕПУТАЦИИ</b>\n"
        "╚═══════════════════╝\n\n"
        f"👤 От: {message.from_user.mention_html()}\n"
        f"🎯 Кому: {target.mention_html()}\n"
        f"💰 Сумма: <b>{amount:+d}</b>\n\n"
        f"📊 Твой баланс: <b>{reputation[cid][uid]:+d}</b>",
        parse_mode="HTML")
    try:
        await bot.send_message(target.id,
            f"💸 <b>{message.from_user.full_name}</b> перевёл тебе <b>{amount:+d}</b> репутации!\n"
            f"📊 Твой новый баланс: <b>{reputation[cid][target.id]:+d}</b>",
            parse_mode="HTML")
    except: pass


# ===== 🎭 РОЛЬ/ПРОФЕССИЯ ДНЯ =====
ROLES_LIST = [
    ("👑", "Король чата"), ("🤡", "Клоун дня"), ("🧙", "Маг слова"),
    ("🕵", "Тайный агент"), ("🦸", "Супергерой"), ("🤖", "Робот-советник"),
    ("🐉", "Дракон"), ("🧛", "Вампир"), ("🧝", "Эльф"),
    ("🏴‍☠️", "Пират"), ("🥷", "Ниндзя"), ("🧑‍🚀", "Космонавт"),
    ("🎭", "Актёр"), ("🧪", "Учёный"), ("🎸", "Рок-звезда"),
    ("🍕", "Профессиональный едок"), ("😴", "Главный соня"),
    ("🔮", "Провидец"), ("🤠", "Ковбой"), ("👻", "Призрак чата"),
    ("🦊", "Хитрый лис"), ("🐺", "Одинокий волк"), ("🌪", "Хаос"),
    ("🧠", "Главный умник"), ("💀", "Тёмный лорд"), ("🌈", "Радужный"),
    ("🎯", "Снайпер слова"), ("🦋", "Свободная душа"), ("🔥", "Огонь"),
    ("❄️", "Ледяной"),
]

@dp.message(Command("role"))
async def cmd_role(message: Message):
    uid = message.from_user.id
    cid = message.chat.id
    from datetime import datetime
    today = datetime.now().strftime("%d.%m.%Y")
    key = f"{cid}_{uid}_{today}"
    if key in role_of_day:
        emoji, role = role_of_day[key]
        await reply_auto_delete(message,
            f"🎭 {message.from_user.mention_html()}, твоя роль сегодня:\n\n"
            f"{emoji} <b>{role}</b>\n\n"
            f"<i>Роль меняется каждый день в полночь</i>",
            parse_mode="HTML"); return
    random.seed(f"{uid}{today}")
    emoji, role = random.choice(ROLES_LIST)
    role_of_day[key] = (emoji, role)
    await reply_auto_delete(message,
        f"╔═══════════════════╗\n"
        f"🎭  <b>РОЛЬ ДНЯ</b>\n"
        f"╚═══════════════════╝\n\n"
        f"👤 {message.from_user.mention_html()}\n\n"
        f"Сегодня ты — {emoji} <b>{role}</b>!\n\n"
        f"<i>Роль меняется каждый день</i>",
        parse_mode="HTML")


# ===== 📸 МЕМ-ГЕНЕРАТОР =====
@dp.message(Command("meme"))
async def cmd_meme(message: Message, command: CommandObject):
    if not command.args:
        await reply_auto_delete(message,
            "📸 Формат: <code>/meme верхний текст | нижний текст</code>\n"
            "Пример: <code>/meme когда пишешь /help | и читаешь всё</code>",
            parse_mode="HTML"); return
    parts = command.args.split("|", 1)
    top_text = parts[0].strip().upper()
    bot_text = parts[1].strip().upper() if len(parts) > 1 else ""
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
        # Создаём мем-картинку
        W, H = 600, 400
        img = Image.new("RGB", (W, H), color=(30, 30, 30))
        draw = ImageDraw.Draw(img)
        # Градиентный фон
        for y in range(H):
            r = int(20 + (y / H) * 40)
            g = int(20 + (y / H) * 20)
            b = int(40 + (y / H) * 60)
            draw.line([(0, y), (W, y)], fill=(r, g, b))
        # Попробуем шрифт
        try:
            font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 48)
            font_sm  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        except:
            font_big = ImageFont.load_default()
            font_sm  = font_big

        def draw_text_with_outline(draw, text, font, y, color=(255,255,255), outline=(0,0,0)):
            bbox = draw.textbbox((0, 0), text, font=font)
            tw = bbox[2] - bbox[0]
            x = (W - tw) // 2
            for dx in [-2, -1, 0, 1, 2]:
                for dy in [-2, -1, 0, 1, 2]:
                    draw.text((x+dx, y+dy), text, font=font, fill=outline)
            draw.text((x, y), text, font=font, fill=color)

        # Верхний текст
        if top_text:
            draw_text_with_outline(draw, top_text, font_big, 30)
        # Нижний текст
        if bot_text:
            draw_text_with_outline(draw, bot_text, font_big, H - 80)
        # Логотип
        draw_text_with_outline(draw, "© МЕМ-ГЕНЕРАТОР", font_sm, H // 2 - 20,
                                color=(200, 200, 200), outline=(0, 0, 0))

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        try: await message.delete()
        except: pass
        from aiogram.types import BufferedInputFile
        await bot.send_photo(
            message.chat.id,
            BufferedInputFile(buf.read(), filename="meme.png"),
            caption=f"📸 Мем от {message.from_user.mention_html()}",
            parse_mode="HTML")
    except ImportError:
        # Без Pillow — текстовый мем
        try: await message.delete()
        except: pass
        lines = []
        if top_text:
            lines.append(f"<b>{top_text}</b>")
        lines.append("\n" + "─" * 20 + "\n")
        if bot_text:
            lines.append(f"<b>{bot_text}</b>")
        await bot.send_message(message.chat.id,
            f"📸 <b>МЕМ</b> от {message.from_user.mention_html()}\n\n" + "\n".join(lines),
            parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message, f"⚠️ Ошибка генерации: {e}")



# ===== 📢 РАССЫЛКА ПО ВСЕМ ЧАТАМ (только владелец) =====
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца бота!"); return
    if not command.args:
        await reply_auto_delete(message,
            "📢 Формат: <code>/broadcast текст</code>\n\n"
            f"Бот находится в <b>{len(known_chats)}</b> чатах.",
            parse_mode="HTML"); return
    text = command.args.strip()
    if len(text) < 3:
        await reply_auto_delete(message, "⚠️ Слишком короткий текст!"); return
    broadcast_text = (
        "╔═══════════════════╗\n"
        "📢  <b>ОБЪЯВЛЕНИЕ</b>\n"
        "╚═══════════════════╝\n\n"
        f"{text}\n\n"
        f"<i>— Администрация бота</i>"
    )
    status_msg = await message.reply(f"📤 Начинаю рассылку в {len(known_chats)} чатов...")
    sent_ok = 0
    sent_fail = 0
    for cid in list(known_chats.keys()):
        try:
            await bot.send_message(cid, broadcast_text, parse_mode="HTML")
            sent_ok += 1
            await asyncio.sleep(0.05)  # anti-flood
        except:
            sent_fail += 1
    await status_msg.edit_text(
        f"╔═══════════════════╗\n"
        f"📢  <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n"
        f"╚═══════════════════╝\n\n"
        f"✅ Доставлено: <b>{sent_ok}</b> чатов\n"
        f"❌ Ошибок: <b>{sent_fail}</b>\n"
        f"📊 Всего чатов: <b>{len(known_chats)}</b>",
        parse_mode="HTML")


@dp.message(Command("chats"))
async def cmd_chats(message: Message):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца бота!"); return
    lines = [
        "╔═══════════════════╗",
        "🌐  <b>ЧАТЫ БОТА</b>",
        "╚═══════════════════╝",
        f"\nВсего чатов: <b>{len(known_chats)}</b>\n"
    ]
    for cid, title in list(known_chats.items())[:30]:
        msgs = sum(chat_stats[cid].values())
        lines.append(f"• <b>{title}</b> ({msgs} сообщений)")
    if len(known_chats) > 30:
        lines.append(f"\n<i>...и ещё {len(known_chats)-30} чатов</i>")
    await message.reply("\n".join(lines), parse_mode="HTML")


# ══════════════════════════════════════════════════════
#  НОВЫЕ ДАННЫЕ
# ══════════════════════════════════════════════════════
reactions_data   = defaultdict(lambda: defaultdict(set))   # {msg_id: {"👍": {uid,...}}}
referrals        = defaultdict(set)    # {uid: {invited_uid,...}}
referral_used    = {}                  # {uid: inviter_uid}
boosters         = defaultdict(dict)   # {uid: {"booster_id": expires_ts}}
avatars          = {}                  # {uid: emoji}
clans            = {}                  # {clan_id: {name,tag,leader,members,rep,created}}
clan_members     = {}                  # {uid: clan_id}
lottery_tickets  = defaultdict(set)    # {cid: {uid,...}}
lottery_last     = {}                  # {cid: date}
stock_invested   = defaultdict(dict)   # {cid: {uid: amount}}
stock_last       = {}                  # {cid: date}
quotes_data      = defaultdict(list)   # {cid: [{text,author,date,msg_id}]}
journal_data     = defaultdict(list)   # {uid: [{date,text}]}
artifacts        = defaultdict(list)   # {uid: [{id,name,emoji,rarity,obtained}]}
event_subs       = defaultdict(set)    # {cid: {uid}} — подписаны на события
trivia_active    = {}                  # {cid: {q,a,reward,msg_id,answerer}}
color_titles     = {}                  # {uid: color_emoji}

BOOSTERS_SHOP = {
    "b1": {"name": "⚡ Ускоритель XP",   "desc": "x2 опыт на 1 час",      "price": 50,  "duration": 3600,  "type": "xp2"},
    "b2": {"name": "🍀 Удача",            "desc": "+20% к дуэлям на 2 часа","price": 80,  "duration": 7200,  "type": "luck"},
    "b3": {"name": "🛡 Щит репы",         "desc": "Защита от потерь 30 мин","price": 100, "duration": 1800,  "type": "shield"},
    "b4": {"name": "🎯 Снайпер",          "desc": "x3 опыт на 30 мин",     "price": 150, "duration": 1800,  "type": "xp3"},
    "b5": {"name": "💰 Магнит репы",      "desc": "+5 репы каждые 10 мин", "price": 200, "duration": 3600,  "type": "rep_magnet"},
}

ARTIFACTS_LIST = [
    ("🗡️", "Меч Судьбы",      "legendary", "+15% к дуэлям"),
    ("🔮", "Хрустальный шар", "epic",      "Приносит удачу в играх"),
    ("👑", "Корона Хаоса",    "divine",    "x2 репа от всех источников 24ч"),
    ("🌙", "Лунный амулет",   "rare",      "+10 репы каждую ночь"),
    ("🔑", "Ключ удачи",      "epic",      "Открывает секретный бонус"),
    ("📜", "Древний свиток",  "legendary", "+50 репы при получении"),
    ("🐉", "Чешуя дракона",   "divine",    "Иммунитет к потерям репы 1ч"),
    ("⚗️", "Зелье силы",      "rare",      "x2 XP на 2 часа"),
    ("🎭", "Маска обмана",    "epic",      "+30% в дуэлях на 1 день"),
    ("💎", "Алмаз вечности",  "divine",    "Постоянный +2 репы в час"),
]

TRIVIA_QUESTIONS = [
    ("Сколько планет в Солнечной системе?", "8", 20),
    ("Столица Франции?", "Париж", 15),
    ("Сколько сторон у шестиугольника?", "6", 10),
    ("Какой газ мы вдыхаем?", "Кислород", 15),
    ("Автор 'Войны и мира'?", "Толстой", 20),
    ("2 в степени 10?", "1024", 25),
    ("Самая большая страна мира?", "Россия", 15),
    ("Химический символ золота?", "Au", 25),
    ("Сколько цветов у радуги?", "7", 10),
    ("Самый быстрый наземный зверь?", "Гепард", 20),
    ("Сколько нот в октаве?", "7", 15),
    ("Год основания Москвы?", "1147", 30),
]

# ══════════════════════════════════════════════════════
#  📣 РЕАКЦИИ НА СООБЩЕНИЯ
# ══════════════════════════════════════════════════════
@dp.message(Command("like"))
async def cmd_like(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "👍 Реплайни на сообщение!"); return
    target_msg = message.reply_to_message
    mid = target_msg.message_id
    uid = message.from_user.id
    if target_msg.from_user and target_msg.from_user.id == uid:
        await reply_auto_delete(message, "❌ Нельзя лайкать себя!"); return
    if uid in reactions_data[mid]["👍"]:
        reactions_data[mid]["👍"].discard(uid)
        await reply_auto_delete(message, "👎 Лайк убран")
    else:
        reactions_data[mid]["👍"].add(uid)
        reactions_data[mid]["👎"].discard(uid)
        if target_msg.from_user:
            reputation[message.chat.id][target_msg.from_user.id] += 1
        await reply_auto_delete(message, f"👍 +1 репутация для {target_msg.from_user.mention_html() if target_msg.from_user else 'автора'}!", parse_mode="HTML")
    save_data()

@dp.message(Command("dislike"))
async def cmd_dislike(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "👎 Реплайни на сообщение!"); return
    target_msg = message.reply_to_message
    mid = target_msg.message_id
    uid = message.from_user.id
    if target_msg.from_user and target_msg.from_user.id == uid:
        await reply_auto_delete(message, "❌ Нельзя дизлайкать себя!"); return
    if uid in reactions_data[mid]["👎"]:
        reactions_data[mid]["👎"].discard(uid)
        await reply_auto_delete(message, "✅ Дизлайк убран")
    else:
        reactions_data[mid]["👎"].add(uid)
        reactions_data[mid]["👍"].discard(uid)
        if target_msg.from_user:
            reputation[message.chat.id][target_msg.from_user.id] -= 1
        await reply_auto_delete(message, f"👎 -1 репутация для {target_msg.from_user.mention_html() if target_msg.from_user else 'автора'}!", parse_mode="HTML")
    save_data()

# ══════════════════════════════════════════════════════
#  🔗 РЕФЕРАЛЬНАЯ СИСТЕМА
# ══════════════════════════════════════════════════════
@dp.message(Command("ref"))
async def cmd_ref(message: Message):
    uid = str(message.from_user.id)
    bot_me = await bot.get_me()
    ref_link = f"https://t.me/{bot_me.username}?start=ref_{uid}"
    invited = len(referrals.get(uid, set()))
    await reply_auto_delete(message,
        "╔═══════════════════╗\n"
        "🔗  <b>РЕФЕРАЛЬНАЯ СИСТЕМА</b>\n"
        "╚═══════════════════╝\n\n"
        f"👥 Ты пригласил: <b>{invited}</b> чел.\n"
        f"💰 Заработано: <b>{invited * 30}</b> репы\n\n"
        f"🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
        f"<i>За каждого приглашённого +30 репы!</i>",
        parse_mode="HTML")

@dp.message(Command("start"))
async def cmd_start_ref(message: Message, command: CommandObject):
    if not command.args or not command.args.startswith("ref_"): return
    inviter_id = command.args.replace("ref_", "").strip()
    uid = str(message.from_user.id)
    if uid == inviter_id: return
    if uid in referral_used: return
    referral_used[uid] = inviter_id
    referrals[inviter_id].add(uid)
    cid = message.chat.id
    reputation[cid][int(inviter_id)] = reputation[cid].get(int(inviter_id), 0) + 30
    save_data()
    try:
        await bot.send_message(int(inviter_id),
            f"🎉 {message.from_user.full_name} зашёл по твоей ссылке!\n+30 репы тебе!")
    except: pass

# ══════════════════════════════════════════════════════
#  🌈 ЦВЕТНЫЕ ТИТУЛЫ / АВАТАРКА
# ══════════════════════════════════════════════════════
COLOR_BADGES = [
    (0,   "⬜", "Новичок"),
    (50,  "🟩", "Участник"),
    (200, "🟦", "Активный"),
    (500, "🟪", "Ветеран"),
    (1000,"🟨", "Элита"),
    (3000,"🔴", "Легенда"),
    (10000,"💎","Бог чата"),
]

def get_color_badge(rep: int) -> tuple:
    badge = COLOR_BADGES[0]
    for threshold, emoji, title in COLOR_BADGES:
        if rep >= threshold:
            badge = (emoji, title)
    return badge

AVATAR_EMOJIS = ["😎","🐉","👑","🔥","💎","🌙","⚡","🦊","🐺","🎭","🌌","💀","🤖","🦋","🌈","❄️","🎯","🗡️","🔮","🌸"]

@dp.message(Command("avatar"))
async def cmd_avatar(message: Message, command: CommandObject):
    uid = str(message.from_user.id)
    if not command.args:
        current = avatars.get(uid, "👤")
        rows = []
        for i in range(0, len(AVATAR_EMOJIS), 5):
            rows.append([InlineKeyboardButton(text=e, callback_data=f"setavatar:{e}") for e in AVATAR_EMOJIS[i:i+5]])
        await message.reply(
            f"📸 <b>Выбери аватар</b>\nТекущий: {current}\n\n",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return
    emoji = command.args.strip()
    if emoji in AVATAR_EMOJIS:
        avatars[uid] = emoji
        await reply_auto_delete(message, f"✅ Аватар установлен: {emoji}")
    else:
        await reply_auto_delete(message, f"❌ Выбери из списка: {' '.join(AVATAR_EMOJIS)}")

@dp.callback_query(F.data.startswith("setavatar:"))
async def cb_setavatar(call: CallbackQuery):
    uid = str(call.from_user.id)
    emoji = call.data.split(":")[1]
    avatars[uid] = emoji
    await call.message.edit_text(f"✅ Аватар установлен: {emoji}")
    asyncio.create_task(auto_delete(call.message))
    await call.answer()

# ══════════════════════════════════════════════════════
#  🍭 МАГАЗИН БУСТЕРОВ
# ══════════════════════════════════════════════════════
@dp.message(Command("boost"))
async def cmd_boost(message: Message, command: CommandObject):
    uid = str(message.from_user.id)
    cid = message.chat.id
    if not command.args:
        from time import time
        now = time()
        lines = [
            "╔═══════════════════╗",
            "🍭  <b>МАГАЗИН БУСТЕРОВ</b>",
            "╚═══════════════════╝",
            f"\n💰 Твоя репа: <b>{reputation[cid].get(message.from_user.id, 0)}</b>\n"
        ]
        for bid, b in BOOSTERS_SHOP.items():
            active = boosters[uid].get(bid, 0)
            status = f"✅ активен ещё {int((active-now)//60)} мин" if active > now else "⬜ неактивен"
            lines.append(f"<b>{b['name']}</b> — {b['price']} репы\n  {b['desc']} | {status}")
        lines.append("\nКупить: <code>/boost b1</code> (или b2..b5)")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")
        return
    bid = command.args.strip().lower()
    if bid not in BOOSTERS_SHOP:
        await reply_auto_delete(message, "❌ Такого бустера нет. /boost — список"); return
    b = BOOSTERS_SHOP[bid]
    rep = reputation[cid].get(message.from_user.id, 0)
    if rep < b["price"]:
        await reply_auto_delete(message, f"❌ Нужно {b['price']} репы, у тебя {rep}"); return
    from time import time
    now = time()
    reputation[cid][message.from_user.id] -= b["price"]
    boosters[uid][bid] = now + b["duration"]
    save_data()
    await reply_auto_delete(message,
        f"✅ Куплен {b['name']}!\n{b['desc']}\nДействует {b['duration']//60} минут.", parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🎲 РУЛЕТКА РЕПУТАЦИИ
# ══════════════════════════════════════════════════════
roulette_cd = {}

@dp.message(Command("roulette"))
async def cmd_roulette(message: Message, command: CommandObject):
    uid = message.from_user.id
    cid = message.chat.id
    from time import time
    now = time()
    if uid in roulette_cd and now - roulette_cd[uid] < 1800:
        left = int((1800 - (now - roulette_cd[uid])) // 60)
        await reply_auto_delete(message, f"⏳ Рулетка кулдаун: {left} мин."); return
    if not command.args:
        await reply_auto_delete(message, "🎲 Формат: <code>/roulette 50</code>\nПоставь репу — x2 или потеряешь всё!", parse_mode="HTML"); return
    try:
        bet = int(command.args.strip())
        if bet <= 0: raise ValueError
    except:
        await reply_auto_delete(message, "❌ Ставка должна быть числом больше 0"); return
    rep = reputation[cid].get(uid, 0)
    if rep < bet:
        await reply_auto_delete(message, f"❌ Недостаточно репы! У тебя {rep}"); return
    roulette_cd[uid] = now
    # Спин
    result = random.randint(0, 36)
    win = result % 2 == 0 and result != 0  # чётное = победа
    if win:
        reputation[cid][uid] += bet
        outcome = f"🎉 <b>ПОБЕДА!</b> Выпало {result}!\n+{bet} репы → баланс: {reputation[cid][uid]:+d}"
    else:
        reputation[cid][uid] -= bet
        outcome = f"💀 <b>ПРОИГРЫШ!</b> Выпало {result}.\n-{bet} репы → баланс: {reputation[cid][uid]:+d}"
    save_data()
    # Анимация
    symbols = ["🔴","⚫","🔴","⚫","🔴","⚫","🟢"]
    spin_display = " ".join(random.choices(symbols, k=7))
    await reply_auto_delete(message,
        f"╔═══════════════════╗\n"
        f"🎲  <b>РУЛЕТКА</b>\n"
        f"╚═══════════════════╝\n\n"
        f"{spin_display}\n\n"
        f"🎯 Ставка: <b>{bet}</b> репы\n\n"
        f"{outcome}",
        parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🧙 МАГИЧЕСКИЙ АРТЕФАКТ
# ══════════════════════════════════════════════════════
artifact_cd = {}

@dp.message(Command("artifact"))
async def cmd_artifact(message: Message):
    uid = str(message.from_user.id)
    cid = message.chat.id
    from time import time
    now = time()
    # Показать инвентарь артефактов
    inv = artifacts.get(uid, [])
    if inv:
        lines = ["╔═══════════════════╗", "🧙  <b>АРТЕФАКТЫ</b>", "╚═══════════════════╝", ""]
        for a in inv:
            lines.append(f"{a['emoji']} <b>{a['name']}</b> [{a['rarity']}]\n   {a['effect']}")
        lines.append(f"\n🎰 /artifact_roll — попытать удачу (кулдаун 6ч, стоит 100 репы)")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")
    else:
        await reply_auto_delete(message,
            "🧙 У тебя нет артефактов!\n\n"
            "🎰 Используй <code>/artifact_roll</code> чтобы попытать удачу!\nСтоит 100 репы, кулдаун 6 часов.",
            parse_mode="HTML")

@dp.message(Command("artifact_roll"))
async def cmd_artifact_roll(message: Message):
    uid = str(message.from_user.id)
    cid = message.chat.id
    from time import time
    now = time()
    if uid in artifact_cd and now - artifact_cd[uid] < 21600:
        left = int((21600 - (now - artifact_cd[uid])) // 3600)
        await reply_auto_delete(message, f"⏳ Следующий ролл через {left} ч."); return
    rep = reputation[cid].get(message.from_user.id, 0)
    if rep < 100:
        await reply_auto_delete(message, "❌ Нужно 100 репы для ролла артефакта!"); return
    reputation[cid][message.from_user.id] -= 100
    artifact_cd[uid] = now
    # Шанс получить артефакт 40%
    if random.random() < 0.4:
        art = random.choice(ARTIFACTS_LIST)
        emoji, name, rarity, effect = art
        artifacts[uid].append({"emoji": emoji, "name": name, "rarity": rarity, "effect": effect, "obtained": __import__('datetime').datetime.now().strftime("%d.%m.%Y")})
        save_data()
        await reply_auto_delete(message,
            f"╔═══════════════════╗\n"
            f"🧙  <b>АРТЕФАКТ НАЙДЕН!</b>\n"
            f"╚═══════════════════╝\n\n"
            f"{emoji} <b>{name}</b>\n"
            f"✨ Редкость: {rarity}\n"
            f"⚡ Эффект: {effect}",
            parse_mode="HTML")
    else:
        save_data()
        await reply_auto_delete(message, "💨 Артефакт не найден... Попробуй снова через 6 часов.")

# ══════════════════════════════════════════════════════
#  🎰 ЛОТЕРЕЯ
# ══════════════════════════════════════════════════════
@dp.message(Command("lottery"))
async def cmd_lottery(message: Message):
    cid = message.chat.id
    uid = message.from_user.id
    from datetime import datetime
    today = datetime.now().strftime("%d.%m.%Y")
    tickets = lottery_tickets[cid]
    participants = len(tickets)
    prize = participants * 20
    last = lottery_last.get(cid)
    already_in = uid in tickets
    await reply_auto_delete(message,
        f"╔═══════════════════╗\n"
        f"🎰  <b>ЛОТЕРЕЯ</b>\n"
        f"╚═══════════════════╝\n\n"
        f"🎫 Участников: <b>{participants}</b>\n"
        f"💰 Призовой фонд: <b>{prize}</b> репы\n"
        f"📅 Розыгрыш: сегодня в 23:00\n\n"
        f"{'✅ Ты уже участвуешь!' if already_in else 'Купить билет: /lottery_buy (цена: 20 репы)'}",
        parse_mode="HTML")

@dp.message(Command("lottery_buy"))
async def cmd_lottery_buy(message: Message):
    cid = message.chat.id
    uid = message.from_user.id
    if uid in lottery_tickets[cid]:
        await reply_auto_delete(message, "✅ Ты уже купил билет на сегодня!"); return
    rep = reputation[cid].get(uid, 0)
    if rep < 20:
        await reply_auto_delete(message, "❌ Нужно 20 репы для билета!"); return
    reputation[cid][uid] -= 20
    lottery_tickets[cid].add(uid)
    save_data()
    await reply_auto_delete(message,
        f"🎫 Билет куплен! Ты участник #{len(lottery_tickets[cid])}\n"
        f"💰 Призовой фонд: {len(lottery_tickets[cid])*20} репы\n"
        f"🎲 Розыгрыш сегодня в 23:00!")

async def run_lottery():
    """Запускается каждый день в 23:00"""
    while True:
        from datetime import datetime
        now = datetime.now()
        # Ждём до 23:00
        target = now.replace(hour=23, minute=0, second=0, microsecond=0)
        if now >= target:
            import datetime as dt
            target += dt.timedelta(days=1)
        wait_secs = (target - now).total_seconds()
        await asyncio.sleep(wait_secs)
        # Розыгрыш во всех чатах
        for cid, tickets in lottery_tickets.items():
            if not tickets: continue
            winner_id = random.choice(list(tickets))
            prize = len(tickets) * 20
            reputation[cid][winner_id] = reputation[cid].get(winner_id, 0) + prize
            try:
                m = await bot.get_chat_member(cid, winner_id)
                winner_name = m.user.mention_html()
            except:
                winner_name = f"ID{winner_id}"
            try:
                await bot.send_message(cid,
                    f"🎰 <b>РОЗЫГРЫШ ЛОТЕРЕИ!</b>\n\n"
                    f"🎉 Победитель: {winner_name}\n"
                    f"💰 Приз: <b>{prize}</b> репы!\n"
                    f"🎫 Участвовало: {len(tickets)} человек",
                    parse_mode="HTML")
            except: pass
        lottery_tickets.clear()
        save_data()

# ══════════════════════════════════════════════════════
#  📈 БИРЖА РЕПУТАЦИИ
# ══════════════════════════════════════════════════════
stock_cd = {}

@dp.message(Command("stock"))
async def cmd_stock(message: Message):
    cid = message.chat.id
    uid = message.from_user.id
    invested = stock_invested[cid].get(uid, 0)
    rep = reputation[cid].get(uid, 0)
    await reply_auto_delete(message,
        f"╔═══════════════════╗\n"
        f"📈  <b>БИРЖА РЕПУТАЦИИ</b>\n"
        f"╚═══════════════════╝\n\n"
        f"💰 Твоя репа: <b>{rep}</b>\n"
        f"📊 Вложено: <b>{invested}</b>\n\n"
        f"📉 Риск: каждый день в 20:00 биржа выплачивает\n"
        f"от -50% до +100% от вложенной суммы\n\n"
        f"/stock_invest [сумма] — вложить\n"
        f"/stock_withdraw — вывести всё",
        parse_mode="HTML")

@dp.message(Command("stock_invest"))
async def cmd_stock_invest(message: Message, command: CommandObject):
    cid = message.chat.id
    uid = message.from_user.id
    if not command.args:
        await reply_auto_delete(message, "📈 Формат: <code>/stock_invest 100</code>", parse_mode="HTML"); return
    try:
        amount = int(command.args.strip())
        if amount <= 0: raise ValueError
    except:
        await reply_auto_delete(message, "❌ Введи число больше 0"); return
    rep = reputation[cid].get(uid, 0)
    if rep < amount:
        await reply_auto_delete(message, f"❌ Недостаточно репы! У тебя {rep}"); return
    reputation[cid][uid] -= amount
    stock_invested[cid][uid] = stock_invested[cid].get(uid, 0) + amount
    save_data()
    await reply_auto_delete(message,
        f"✅ Вложено <b>{amount}</b> репы на биржу!\n"
        f"📊 Итого вложено: <b>{stock_invested[cid][uid]}</b>\n"
        f"💸 Выплата сегодня в 20:00",
        parse_mode="HTML")

@dp.message(Command("stock_withdraw"))
async def cmd_stock_withdraw(message: Message):
    cid = message.chat.id
    uid = message.from_user.id
    invested = stock_invested[cid].get(uid, 0)
    if invested == 0:
        await reply_auto_delete(message, "❌ У тебя нет вложений!"); return
    # Вывод с штрафом 10%
    withdraw = int(invested * 0.9)
    reputation[cid][uid] = reputation[cid].get(uid, 0) + withdraw
    stock_invested[cid][uid] = 0
    save_data()
    await reply_auto_delete(message,
        f"💸 Выведено <b>{withdraw}</b> репы (штраф 10% за досрочный вывод)\n"
        f"💰 Баланс: <b>{reputation[cid][uid]:+d}</b>",
        parse_mode="HTML")

async def run_stock():
    """Биржа — выплаты каждый день в 20:00"""
    while True:
        from datetime import datetime
        now = datetime.now()
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now >= target:
            import datetime as dt
            target += dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        for cid, investors in stock_invested.items():
            for uid, amount in list(investors.items()):
                if amount <= 0: continue
                # -50% до +100%
                multiplier = random.uniform(-0.5, 1.0)
                change = int(amount * multiplier)
                reputation[cid][uid] = reputation[cid].get(uid, 0) + amount + change
                stock_invested[cid][uid] = 0
                result_text = f"📈 +{change}" if change >= 0 else f"📉 {change}"
                try:
                    await bot.send_message(uid,
                        f"📊 <b>Биржа — выплата!</b>\n"
                        f"Вложено: {amount} | {result_text} репы\n"
                        f"Итого получено: {amount + change}",
                        parse_mode="HTML")
                except: pass
        save_data()

# ══════════════════════════════════════════════════════
#  💬 ЦИТАТНИК
# ══════════════════════════════════════════════════════
@dp.message(Command("quote_save"))
async def cmd_quote_save(message: Message):
    if not message.reply_to_message or not message.reply_to_message.text:
        await reply_auto_delete(message, "💬 Реплайни на текстовое сообщение!"); return
    cid = message.chat.id
    author = message.reply_to_message.from_user
    text = message.reply_to_message.text
    if len(text) > 300:
        await reply_auto_delete(message, "❌ Цитата слишком длинная (макс 300 символов)"); return
    from datetime import datetime
    quotes_data[cid].append({
        "text": text,
        "author": author.full_name if author else "Аноним",
        "date": datetime.now().strftime("%d.%m.%Y"),
    })
    if len(quotes_data[cid]) > 100:
        quotes_data[cid] = quotes_data[cid][-100:]
    await reply_auto_delete(message,
        f"💬 Цитата сохранена!\n\n"
        f"«{text[:100]}{'...' if len(text)>100 else ''}»\n"
        f"— {author.full_name if author else 'Аноним'}")

@dp.message(Command("quote_random"))
async def cmd_quote_random(message: Message):
    cid = message.chat.id
    if not quotes_data[cid]:
        await reply_auto_delete(message, "💬 Цитат пока нет! Сохрани через /quote_save (реплай)"); return
    q = random.choice(quotes_data[cid])
    await reply_auto_delete(message,
        f"╔═══════════════════╗\n"
        f"💬  <b>ЦИТАТА ЧА ТА</b>\n"
        f"╚═══════════════════╝\n\n"
        f"«{q['text']}»\n\n"
        f"— <b>{q['author']}</b>, {q['date']}",
        parse_mode="HTML")

@dp.message(Command("quotes"))
async def cmd_quotes(message: Message):
    cid = message.chat.id
    total = len(quotes_data[cid])
    if not total:
        await reply_auto_delete(message, "💬 Цитат пока нет!"); return
    last5 = quotes_data[cid][-5:]
    lines = [f"╔═══════════════════╗\n💬  <b>ЦИТАТНИК</b> ({total} цитат)\n╚═══════════════════╝\n"]
    for q in reversed(last5):
        lines.append(f"«{q['text'][:80]}{'...' if len(q['text'])>80 else ''}»\n— {q['author']}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  📝 ДНЕВНИК ЧАТА
# ══════════════════════════════════════════════════════
@dp.message(Command("journal"))
async def cmd_journal(message: Message, command: CommandObject):
    uid = str(message.from_user.id)
    from datetime import datetime
    if command.args:
        text = command.args.strip()
        if len(text) < 3:
            await reply_auto_delete(message, "⚠️ Слишком короткая запись!"); return
        journal_data[uid].append({
            "date": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "text": text
        })
        if len(journal_data[uid]) > 50:
            journal_data[uid] = journal_data[uid][-50:]
        await reply_auto_delete(message, f"📝 Запись сохранена!\n\n_{text[:100]}_", parse_mode="HTML")
        return
    entries = journal_data.get(uid, [])
    if not entries:
        await reply_auto_delete(message,
            "📝 <b>Твой дневник пуст!</b>\n\nДобавь запись:\n<code>/journal сегодня был отличный день</code>",
            parse_mode="HTML"); return
    lines = [f"╔═══════════════════╗\n📝  <b>МОЙ ДНЕВНИК</b> ({len(entries)} записей)\n╚═══════════════════╝\n"]
    for e in reversed(entries[-5:]):
        lines.append(f"🗓 <b>{e['date']}</b>\n{e['text']}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🧩 ВИКТОРИНА
# ══════════════════════════════════════════════════════
@dp.message(Command("trivia"))
async def cmd_trivia(message: Message):
    cid = message.chat.id
    if cid in trivia_active:
        q = trivia_active[cid]
        await reply_auto_delete(message,
            f"❓ Уже идёт викторина!\n\n<b>{q['q']}</b>\n\nОтвечай в чате!", parse_mode="HTML"); return
    question, answer, reward = random.choice(TRIVIA_QUESTIONS)
    trivia_active[cid] = {"q": question, "a": answer.lower(), "reward": reward, "answerer": None}
    await answer_auto_delete(
        f"╔═══════════════════╗\n"
        f"🧩  <b>ВИКТОРИНА</b>\n"
        f"╚═══════════════════╝\n\n"
        f"❓ <b>{question}</b>\n\n"
        f"💰 Награда: <b>{reward}</b> репы\n"
        f"⏰ Есть 60 секунд!",
        parse_mode="HTML")
    await asyncio.sleep(60)
    if cid in trivia_active and trivia_active[cid]["answerer"] is None:
        del trivia_active[cid]
        try:
            await bot.send_message(cid,
                f"⏰ Время вышло! Правильный ответ: <b>{answer}</b>", parse_mode="HTML")
        except: pass

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_trivia_answer(message: Message):
    cid = message.chat.id
    if cid not in trivia_active: return
    q = trivia_active[cid]
    if q["answerer"] is not None: return
    if message.text.strip().lower() == q["a"]:
        uid = message.from_user.id
        q["answerer"] = uid
        reputation[cid][uid] = reputation[cid].get(uid, 0) + q["reward"]
        save_data()
        del trivia_active[cid]
        await message.reply(
            f"🎉 <b>{message.from_user.mention_html()} ответил правильно!</b>\n"
            f"✅ Ответ: <b>{q['a'].capitalize()}</b>\n"
            f"💰 +{q['reward']} репы!",
            parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🤝 КЛАНЫ
# ══════════════════════════════════════════════════════
@dp.message(Command("clan"))
async def cmd_clan(message: Message, command: CommandObject):
    uid = message.from_user.id
    cid = message.chat.id
    my_clan_id = clan_members.get(uid)
    if not command.args:
        if my_clan_id and my_clan_id in clans:
            c = clans[my_clan_id]
            members_rep = sum(reputation[cid].get(m, 0) for m in c["members"])
            await reply_auto_delete(message,
                f"╔═══════════════════╗\n"
                f"🤝  <b>КЛАН {c['tag']}</b>\n"
                f"╚═══════════════════╝\n\n"
                f"🏷 Название: <b>{c['name']}</b>\n"
                f"👥 Участников: <b>{len(c['members'])}</b>\n"
                f"💰 Суммарная репа: <b>{members_rep}</b>\n\n"
                f"/clan_leave — покинуть клан",
                parse_mode="HTML")
        else:
            await reply_auto_delete(message,
                "🤝 <b>КЛАНЫ</b>\n\n"
                "/clan_create [тег] [название] — создать клан\n"
                "/clan_join [тег] — вступить\n"
                "/clan_top — топ кланов\n\n"
                "<i>Создание клана стоит 200 репы</i>",
                parse_mode="HTML")
        return

@dp.message(Command("clan_create"))
async def cmd_clan_create(message: Message, command: CommandObject):
    uid = message.from_user.id
    cid = message.chat.id
    if uid in clan_members:
        await reply_auto_delete(message, "❌ Ты уже в клане! /clan_leave чтобы выйти"); return
    if not command.args or len(command.args.split()) < 2:
        await reply_auto_delete(message, "⚠️ Формат: <code>/clan_create ТЕГ Название клана</code>", parse_mode="HTML"); return
    parts = command.args.split(maxsplit=1)
    tag = parts[0].upper()[:5]
    name = parts[1].strip()[:30]
    if any(c["tag"] == tag for c in clans.values()):
        await reply_auto_delete(message, f"❌ Тег [{tag}] уже занят!"); return
    rep = reputation[cid].get(uid, 0)
    if rep < 200:
        await reply_auto_delete(message, "❌ Нужно 200 репы для создания клана!"); return
    reputation[cid][uid] -= 200
    clan_id = f"clan_{uid}_{int(__import__('time').time())}"
    clans[clan_id] = {"name": name, "tag": tag, "leader": uid, "members": [uid], "created": __import__('datetime').datetime.now().strftime("%d.%m.%Y")}
    clan_members[uid] = clan_id
    save_data()
    await reply_auto_delete(message,
        f"✅ Клан <b>[{tag}] {name}</b> создан!\n"
        f"👑 Ты лидер\n"
        f"📢 Другие могут вступить через /clan_join {tag}",
        parse_mode="HTML")

@dp.message(Command("clan_join"))
async def cmd_clan_join(message: Message, command: CommandObject):
    uid = message.from_user.id
    if uid in clan_members:
        await reply_auto_delete(message, "❌ Ты уже в клане!"); return
    if not command.args:
        await reply_auto_delete(message, "⚠️ Формат: <code>/clan_join ТЕГ</code>", parse_mode="HTML"); return
    tag = command.args.strip().upper()
    target_clan = next(((cid, c) for cid, c in clans.items() if c["tag"] == tag), None)
    if not target_clan:
        await reply_auto_delete(message, f"❌ Клан [{tag}] не найден!"); return
    clan_id, c = target_clan
    c["members"].append(uid)
    clan_members[uid] = clan_id
    save_data()
    await reply_auto_delete(message, f"✅ Ты вступил в клан <b>[{tag}] {c['name']}</b>!", parse_mode="HTML")

@dp.message(Command("clan_leave"))
async def cmd_clan_leave(message: Message):
    uid = message.from_user.id
    clan_id = clan_members.get(uid)
    if not clan_id or clan_id not in clans:
        await reply_auto_delete(message, "❌ Ты не в клане!"); return
    c = clans[clan_id]
    if c["leader"] == uid:
        await reply_auto_delete(message, "❌ Лидер не может покинуть клан! Используй /clan_disband"); return
    c["members"].remove(uid)
    del clan_members[uid]
    save_data()
    await reply_auto_delete(message, f"✅ Ты покинул клан [{c['tag']}]")

@dp.message(Command("clan_top"))
async def cmd_clan_top(message: Message):
    cid = message.chat.id
    if not clans:
        await reply_auto_delete(message, "🤝 Кланов пока нет!"); return
    clan_scores = []
    for clan_id, c in clans.items():
        total_rep = sum(reputation[cid].get(m, 0) for m in c["members"])
        clan_scores.append((c["tag"], c["name"], len(c["members"]), total_rep))
    clan_scores.sort(key=lambda x: x[3], reverse=True)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    lines = ["╔═══════════════════╗", "🤝  <b>ТОП КЛАНОВ</b>", "╚═══════════════════╝", ""]
    for i, (tag, name, members, rep) in enumerate(clan_scores[:5]):
        m = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{m} <b>[{tag}]</b> {name}\n   👥 {members} чел | 💰 {rep} репы")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🥇 ДВОЙНОЙ ОПЫТ В ВЫХОДНЫЕ
# ══════════════════════════════════════════════════════
# (встроено в StatsMiddleware — проверяем день недели)

# ══════════════════════════════════════════════════════
#  🔔 ПОДПИСКА НА СОБЫТИЯ
# ══════════════════════════════════════════════════════
@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    uid = message.from_user.id
    cid = message.chat.id
    if uid in event_subs[cid]:
        event_subs[cid].discard(uid)
        await reply_auto_delete(message, "🔕 Ты отписался от уведомлений о событиях")
    else:
        event_subs[cid].add(uid)
        await reply_auto_delete(message,
            "🔔 Ты подписан на события!\n"
            "Получишь уведомление о днях рождения участников чата.\n"
            "Отписаться: /subscribe")


async def autosave_loop():
    """Автосохранение каждые 5 минут"""
    while True:
        await asyncio.sleep(300)
        save_data()
        print('[autosave] данные сохранены')


@dp.message(Command("topxp"))
async def cmd_topxp(message: Message):
    cid = message.chat.id
    if not xp_data[cid]:
        await reply_auto_delete(message, "📊 XP пока нет!"); return
    sorted_u = sorted(xp_data[cid].items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = [
        "✨ <b>CHAT GUARD</b> — Топ XP",
        "━━━━━━━━━━━━━━━━━━━━━━",
        ""
    ]
    for i, (uid, xp) in enumerate(sorted_u):
        lvl = get_level(xp)
        emoji_lvl, title = get_level_title(lvl)
        try:
            m = await bot.get_chat_member(cid, uid)
            uname = m.user.full_name[:18]
        except:
            uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b>\n   {emoji_lvl} Ур.{lvl} • {xp} XP")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


@dp.message(F.chat.type == "private")
async def handle_private_message(message: Message):
    uid  = message.from_user.id
    text = message.text or ""

    if text.startswith("/"): return  # команды обрабатываются отдельно

    responses = [
        "👋 Привет! Я работаю в групповых чатах.\nДобавь меня в группу и используй /help",
        "🤖 Я бот для чатов! Используй меня в группе.\nКоманды: /help",
        "💬 Хочешь поговорить? Я работаю в группах!\nПропиши /help в своём чате.",
        "🎮 Все мои функции доступны в групповых чатах!\nДобавь меня туда: /help",
        f"👤 Твой ID: <code>{uid}</code>\n\n🤖 Я групповой бот. Добавь меня в чат!",
    ]

    # Проверяем слова
    tl = text.lower()
    if any(w in tl for w in ["помог", "help", "помощь", "что умеешь"]):
        reply = (
            "🤖 <b>Я CHAT GUARD!</b>\n\n"
            "Умею:\n"
            "⭐ Система репутации и XP\n"
            "🏆 Уровни до 250\n"
            "🤝 Кланы\n"
            "🎰 Лотерея и биржа\n"
            "🧙 Артефакты и бустеры\n"
            "👮 Модерация чата\n\n"
            "Добавь меня в группу и используй /help!"
        )
    elif any(w in tl for w in ["привет", "хай", "здарова", "hi", "hello"]):
        reply = f"👋 Привет, {message.from_user.first_name}!\nЯ работаю в групповых чатах. Добавь меня туда!"
    elif any(w in tl for w in ["репа", "репутация", "уровень", "xp"]):
        reply = "⭐ Репутация и XP доступны только в групповых чатах!\nДобавь меня в свою группу."
    elif any(w in tl for w in ["спасибо", "благодар", "thanks"]):
        reply = f"😊 Пожалуйста, {message.from_user.first_name}! Рад помочь!"
    else:
        import random as _r
        reply = _r.choice(responses)

    await answer_auto_delete(reply, parse_mode="HTML")


# ══════════════════════════════════════════
#  📈 ТОП XP
# ══════════════════════════════════════════
@dp.message(Command("топxp"))
async def cmd_topxp(message: Message):
    cid = message.chat.id
    stats = xp_data[cid]
    if not stats:
        await reply_auto_delete(message, "📈 XP статистика пока пуста!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["✨ <b>CHAT GUARD</b> — Топ по XP\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    for i, (uid, xp) in enumerate(sorted_u):
        try:
            m = await bot.get_chat_member(cid, uid)
            name = m.user.full_name
        except:
            name = f"ID {uid}"
        lvl = get_level(xp)
        emoji, title = get_level_title(lvl)
        lines.append(f"{medals[i]} <b>{name}</b>\n   {emoji} Ур. {lvl} · {xp} XP")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════
#  🎪 ИВЕНТЫ
# ══════════════════════════════════════════
current_event = {}

EVENT_TYPES = [
    ("⚡ Час молнии",        3, 3600,  "x3 XP целый час!"),
    ("🔥 Огненный вечер",    2, 7200,  "x2 XP на 2 часа!"),
    ("🌟 Звёздная ночь",     3, 10800, "x3 XP на 3 часа!"),
    ("💎 Бриллиантовый час", 5, 3600,  "x5 XP целый час!"),
    ("🎪 Карнавал чата",     2, 14400, "x2 XP на 4 часа!"),
]

async def run_events():
    while True:
        import datetime as dt
        now = dt.datetime.now()
        days_until_wed = (2 - now.weekday()) % 7 or 7
        target = now.replace(hour=19, minute=0, second=0, microsecond=0) + dt.timedelta(days=days_until_wed)
        await asyncio.sleep((target - now).total_seconds())
        name, mult, duration, desc = random.choice(EVENT_TYPES)
        import time as _t
        end_ts = _t.time() + duration
        for cid in list(known_chats.keys()):
            current_event[cid] = {"active": True, "end_ts": end_ts, "multiplier": mult, "name": name}
            try:
                await bot.send_message(cid,
                    f"🎪 <b>ИВЕНТ НАЧАЛСЯ!</b>\n\n"
                    f"{name}\n✨ {desc}\n"
                    f"⏰ Длительность: {duration//3600}ч {(duration%3600)//60}мин\n\n"
                    f"<i>Пиши сообщения и получай больше XP!</i>", parse_mode="HTML")
            except: pass
        await asyncio.sleep(duration)
        for cid in list(current_event.keys()):
            current_event.pop(cid, None)
            try:
                await bot.send_message(cid, f"⏰ Ивент <b>{name}</b> завершён!", parse_mode="HTML")
            except: pass

@dp.message(Command("event"))
async def cmd_event(message: Message):
    cid = message.chat.id
    import time as _t
    ev = current_event.get(cid)
    if not ev or _t.time() > ev.get("end_ts", 0):
        await reply_auto_delete(message, "🎪 Сейчас ивентов нет.\n⏰ Следующий — в среду в 19:00!"); return
    left = int(ev["end_ts"] - _t.time())
    h, m = left // 3600, (left % 3600) // 60
    await reply_auto_delete(message,
        f"🎪 <b>АКТИВНЫЙ ИВЕНТ!</b>\n\n{ev['name']}\n"
        f"⚡ Множитель: x{ev['multiplier']} XP\n⏰ Осталось: {h}ч {m}мин", parse_mode="HTML")


# ══════════════════════════════════════════
#  🌍 КАРТА АКТИВНОСТИ
# ══════════════════════════════════════════
@dp.message(Command("activity"))
async def cmd_activity(message: Message):
    cid = message.chat.id
    if not hourly_stats[cid]:
        await reply_auto_delete(message, "📊 Данных пока нет!"); return
    hour_totals = defaultdict(int)
    for uid, hours in hourly_stats[cid].items():
        for h, count in hours.items():
            hour_totals[int(h)] += count
    if not hour_totals:
        await reply_auto_delete(message, "📊 Данных пока нет!"); return
    max_val = max(hour_totals.values()) or 1
    lines = ["✨ <b>CHAT GUARD</b> — Карта активности\n━━━━━━━━━━━━━━━━━━━━━━"]
    periods = [("🌙 Ночь", range(0,6)), ("🌅 Утро", range(6,12)), ("☀️ День", range(12,18)), ("🌆 Вечер", range(18,24))]
    for period_name, hours in periods:
        lines.append(f"\n<b>{period_name}</b>")
        for h in hours:
            count = hour_totals.get(h, 0)
            bar = "█" * int((count/max_val)*10) + "░" * (10 - int((count/max_val)*10))
            lines.append(f"{h:02d}:00 [{bar}] {count}")
    peak_hour = max(hour_totals, key=hour_totals.get)
    lines.append(f"\n📊 Всего: <b>{sum(hour_totals.values())}</b> | 🔥 Пик: <b>{peak_hour:02d}:00</b>")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


# ══════════════════════════════════════════
#  🗞 ГАЗЕТА ЧАТА — 09:00 КАЖДЫЙ ДЕНЬ
# ══════════════════════════════════════════
async def run_newspaper():
    while True:
        import datetime as dt
        now = dt.datetime.now()
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= target:
            target += dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        for cid in list(known_chats.keys()):
            try:
                yesterday = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%d.%m.%Y")
                top_users = sorted(
                    [(uid, days.get(yesterday, 0)) for uid, days in daily_stats[cid].items() if days.get(yesterday, 0) > 0],
                    key=lambda x: x[1], reverse=True)[:3]
                medals = ["🥇","🥈","🥉"]
                top_lines = []
                for i, (uid, count) in enumerate(top_users):
                    try:
                        m = await bot.get_chat_member(cid, uid); name = m.user.full_name
                    except:
                        name = f"ID {uid}"
                    top_lines.append(f"{medals[i]} {name} — {count} сообщ.")
                quote_line = ""
                if quotes_data[cid]:
                    q = random.choice(quotes_data[cid])
                    quote_line = f"\n\n💬 <b>Цитата дня:</b>\n«{q['text'][:100]}»\n— {q['author']}"
                ev = current_event.get(cid)
                event_line = f"\n\n🎪 Активен ивент: <b>{ev['name']}</b>!" if ev else ""
                await bot.send_message(cid,
                    f"🗞 <b>ГАЗЕТА ЧАТА</b> — {dt.datetime.now().strftime('%d.%m.%Y')}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🏆 <b>Топ вчера:</b>\n"
                    + ("\n".join(top_lines) if top_lines else "Нет данных")
                    + quote_line + event_line +
                    f"\n\n📊 /топxp · /toprep · /activity",
                    parse_mode="HTML")
            except: pass


# ══════════════════════════════════════════════════════
#  🔕 ТИХИЙ БАН
# ══════════════════════════════════════════════════════
@dp.callback_query(F.data.startswith("panel:silentban:"))
async def cb_silentban(call: CallbackQuery):
    if not await check_admin(call.message): return
    tid = int(call.data.split(":")[2])
    cid = call.message.chat.id
    try:
        await bot.ban_chat_member(cid, tid)
        silent_bans[tid] = True
        add_mod_history(cid, tid, "🔕 Тихий бан", "Без причины", call.from_user.full_name)
        await log_action(f"🔕 <b>ТИХИЙ БАН</b>\n👤 Модер: {call.from_user.mention_html()}\n🎯 Цель: ID{tid}\n💬 Чат: {call.message.chat.title}", parse_mode="HTML")
        await call.answer("✅ Тихий бан применён", show_alert=False)
        await call.message.edit_text("🔕 Тихий бан применён.\n<i>Сообщение в чат не отправлено.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
    except Exception as e:
        await call.answer(f"❌ Ошибка: {e}", show_alert=True)

# ══════════════════════════════════════════════════════
#  📝 ЗАМЕТКИ НА УЧАСТНИКА (/usernote)
# ══════════════════════════════════════════════════════
@dp.message(Command("usernote"))
async def cmd_usernote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    if not message.reply_to_message:
        await reply_auto_delete(message, "📝 Реплайни на сообщение участника!"); return
    target = message.reply_to_message.from_user
    uid = target.id
    if not command.args:
        notes_list = user_notes[cid].get(uid, [])
        if not notes_list:
            await reply_auto_delete(message, f"📝 Заметок на {target.full_name} нет.\nДобавить: <code>/usernote текст</code>", parse_mode="HTML"); return
        lines = [f"📝 <b>Заметки на {target.full_name}:</b>\n"]
        for i, n in enumerate(notes_list, 1):
            lines.append(f"{i}. {n['text']} <i>({n['date']} — {n['by']})</i>")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML"); return
    from datetime import datetime
    if uid not in user_notes[cid]:
        user_notes[cid][uid] = []
    user_notes[cid][uid].append({
        "text": command.args.strip(),
        "date": datetime.now().strftime("%d.%m.%Y"),
        "by": message.from_user.full_name
    })
    save_data()
    await reply_auto_delete(message, f"✅ Заметка добавлена к профилю {target.mention_html()}!", parse_mode="HTML")

@dp.callback_query(F.data.startswith("panel:usernotes:"))
async def cb_usernotes(call: CallbackQuery):
    tid = int(call.data.split(":")[2])
    cid = call.message.chat.id
    notes_list = user_notes[cid].get(tid, [])
    if not notes_list:
        await call.answer("📝 Заметок нет. Добавь через /usernote (реплай)", show_alert=True); return
    lines = [f"📝 <b>Заметки на ID{tid}:</b>\n"]
    for i, n in enumerate(notes_list, 1):
        lines.append(f"{i}. {n['text']} <i>({n['date']} — {n['by']})</i>")
    try:
        await call.message.edit_text("\n".join(lines), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:back:{tid}")
            ]]))
    except: pass
    await call.answer()

# ══════════════════════════════════════════════════════
#  📊 ОТЧЁТ МОДЕРАТОРА (/modreport)
# ══════════════════════════════════════════════════════
@dp.message(Command("modreport"))
async def cmd_modreport(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    from datetime import datetime, timedelta
    week_ago = datetime.now() - timedelta(days=7)
    history = mod_history.get(cid, [])
    recent = [h for h in history if True]  # берём все последние
    recent = recent[-50:]  # последние 50 действий
    if not recent:
        await reply_auto_delete(message, "📊 Нет данных за последнюю неделю."); return
    # Статистика по модераторам
    mod_stats = defaultdict(lambda: defaultdict(int))
    for h in recent:
        mod_stats[h.get("by","?")][h.get("action","?")] += 1
    lines = ["✨ <b>CHAT GUARD</b> — Отчёт модерации\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    for mod_name, actions in sorted(mod_stats.items(), key=lambda x: sum(x[1].values()), reverse=True):
        total = sum(actions.values())
        lines.append(f"👮 <b>{mod_name}</b> — {total} действий")
        for action, count in sorted(actions.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"   ▸ {action}: {count}")
    # Топ нарушителей
    violators = defaultdict(int)
    for h in recent:
        if h.get("uid"):
            violators[h["uid"]] += 1
    if violators:
        lines.append("\n🚨 <b>Топ нарушителей:</b>")
        for uid, count in sorted(violators.items(), key=lambda x: x[1], reverse=True)[:5]:
            try:
                m = await bot.get_chat_member(cid, uid)
                name = m.user.full_name
            except:
                name = f"ID{uid}"
            lines.append(f"   ▸ {name}: {count} нарушений")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.callback_query(F.data.startswith("members:modreport:"))
async def cb_modreport(call: CallbackQuery):
    if not await check_admin(call.message): return
    await cmd_modreport(call.message)
    await call.answer()

@dp.callback_query(F.data.startswith("members:topxp:"))
async def cb_topxp_panel(call: CallbackQuery):
    await cmd_topxp(call.message)
    await call.answer()

# ══════════════════════════════════════════════════════
#  🚨 СИСТЕМА ЖАЛОБ — УЛУЧШЕННАЯ
# ══════════════════════════════════════════════════════
def kb_report_action(reporter_id: int, target_id: int, idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять — варн",   callback_data=f"rpt:warn:{target_id}:{idx}"),
         InlineKeyboardButton(text="🔇 Принять — мут",    callback_data=f"rpt:mute:{target_id}:{idx}")],
        [InlineKeyboardButton(text="🔨 Принять — бан",    callback_data=f"rpt:ban:{target_id}:{idx}"),
         InlineKeyboardButton(text="❌ Отклонить",         callback_data=f"rpt:reject:{target_id}:{idx}")],
    ])

@dp.callback_query(F.data.startswith("rpt:"))
async def cb_report_action(call: CallbackQuery):
    if not await check_admin(call.message): return
    parts = call.data.split(":")
    action, target_id, idx = parts[1], int(parts[2]), int(parts[3])
    cid = call.message.chat.id
    queue = report_queue.get(cid, [])
    if idx >= len(queue):
        await call.answer("❌ Жалоба уже обработана", show_alert=True); return
    report = queue[idx]
    if action == "reject":
        queue.pop(idx)
        await call.message.edit_text("❌ Жалоба отклонена.")
        asyncio.create_task(auto_delete(call.message))
        await call.answer("Отклонено")
        return
    try:
        if action == "warn":
            warnings[cid][target_id] += 1
            result = f"⚡ Варн выдан (ID{target_id})"
        elif action == "mute":
            from datetime import datetime, timedelta
            until = datetime.now() + timedelta(minutes=60)
            from aiogram.types import ChatPermissions
            await bot.restrict_chat_member(cid, target_id, ChatPermissions(can_send_messages=False), until_date=until)
            result = f"🔇 Мут 1ч (ID{target_id})"
        elif action == "ban":
            await bot.ban_chat_member(cid, target_id)
            result = f"🔨 Бан применён (ID{target_id})"
        queue.pop(idx)
        save_data()
        await call.message.edit_text(
            f"✅ <b>Жалоба обработана</b>\n{result}\n👮 Модер: {call.from_user.full_name}", parse_mode="HTML")
        asyncio.create_task(schedule_delete(call.message))
        await call.answer("✅ Готово")
    except Exception as e:
        await call.answer(f"❌ {e}", show_alert=True)

@dp.callback_query(F.data.startswith("panel:reports:"))
async def cb_panel_reports(call: CallbackQuery):
    if not await check_admin(call.message): return
    cid = call.message.chat.id
    queue = report_queue.get(cid, [])
    if not queue:
        await call.answer("✅ Жалоб нет!", show_alert=True); return
    text_lines = [f"🚨 <b>Очередь жалоб</b> ({len(queue)} шт.)\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    for i, r in enumerate(queue[:5]):
        text_lines.append(f"#{i+1} 👤 На ID{r['target']} от ID{r['reporter']}\n💬 {r['text'][:80]}")
    try:
        await call.message.edit_text("\n".join(text_lines), parse_mode="HTML",
            reply_markup=kb_report_action(queue[0]["reporter"], queue[0]["target"], 0) if queue else None)
    except: pass
    await call.answer()

# ══════════════════════════════════════════════════════
#  📈 ЭКОНОМИКА ПАНЕЛЬ
# ══════════════════════════════════════════════════════
@dp.callback_query(F.data.startswith("panel:economy:"))
async def cb_panel_economy(call: CallbackQuery):
    if not await check_admin(call.message): return
    cid = call.message.chat.id
    total_rep = sum(reputation[cid].values())
    total_invested = sum(stock_invested[cid].values())
    lottery_count = len(lottery_tickets[cid])
    clan_count = sum(1 for c in clans.values() if any(m in reputation[cid] for m in c.get("members", [])))
    try:
        await call.message.edit_text(
            "✨ <b>CHAT GUARD</b> — Экономика\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Всего репы в чате: <b>{total_rep}</b>\n"
            f"📈 Вложено на бирже: <b>{total_invested}</b>\n"
            f"🎰 Билетов в лотерее: <b>{lottery_count}</b>\n"
            f"🤝 Активных кланов: <b>{clan_count}</b>\n\n"
            "▸ /toprep · /топxp · /stock · /lottery",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")
            ]]))
        asyncio.create_task(schedule_delete(call.message))
    except: pass
    await call.answer()

@dp.callback_query(F.data.startswith("panel:topxp:"))
async def cb_panel_topxp(call: CallbackQuery):
    await cmd_topxp(call.message)
    await call.answer()

# ══════════════════════════════════════════════════════
#  🍬 МАГАЗИН ПОДАРКОВ
# ══════════════════════════════════════════════════════
@dp.message(Command("gift"))
async def cmd_gift(message: Message, command: CommandObject):
    cid = message.chat.id
    uid = message.from_user.id
    if not message.reply_to_message:
        lines = [
            "🍬 <b>Магазин подарков</b>\n━━━━━━━━━━━━━━━━━━━━━━\n",
            "Реплайни на сообщение и отправь подарок!\n",
        ]
        for bid, b in BOOSTERS_SHOP.items():
            lines.append(f"▸ <code>/gift {bid}</code> — {b['name']} ({b['price']} репы)")
        lines.append("\n▸ <code>/gift artifact</code> — случайный артефакт (150 репы)")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML"); return
    target = message.reply_to_message.from_user
    if target.id == uid:
        await reply_auto_delete(message, "❌ Нельзя дарить самому себе!"); return
    if not command.args:
        await reply_auto_delete(message, "⚠️ Укажи что дарить. Пример: <code>/gift b1</code>", parse_mode="HTML"); return
    arg = command.args.strip().lower()
    import time as _t
    now = _t.time()
    if arg == "artifact":
        rep = reputation[cid].get(uid, 0)
        if rep < 150:
            await reply_auto_delete(message, "❌ Нужно 150 репы!"); return
        reputation[cid][uid] -= 150
        art = random.choice(ARTIFACTS_LIST)
        emoji, name, rarity, effect = art
        uid_str = str(target.id)
        artifacts[uid_str].append({"emoji": emoji, "name": name, "rarity": rarity, "effect": effect, "obtained": __import__('datetime').datetime.now().strftime("%d.%m.%Y")})
        save_data()
        await reply_auto_delete(message, f"🎁 {message.from_user.mention_html()} подарил {target.mention_html()} артефакт!\n{emoji} <b>{name}</b> [{rarity}]", parse_mode="HTML")
        try:
            await bot.send_message(target.id, f"🎁 Тебе подарили артефакт!\n{emoji} <b>{name}</b>\n⚡ {effect}", parse_mode="HTML")
        except: pass
    elif arg in BOOSTERS_SHOP:
        b = BOOSTERS_SHOP[arg]
        rep = reputation[cid].get(uid, 0)
        if rep < b["price"]:
            await reply_auto_delete(message, f"❌ Нужно {b['price']} репы!"); return
        reputation[cid][uid] -= b["price"]
        uid_str = str(target.id)
        boosters[uid_str][arg] = now + b["duration"]
        save_data()
        await reply_auto_delete(message, f"🎁 {message.from_user.mention_html()} подарил {target.mention_html()}!\n{b['name']} — {b['desc']}", parse_mode="HTML")
        try:
            await bot.send_message(target.id, f"🎁 Тебе подарили бустер!\n{b['name']}\n{b['desc']}", parse_mode="HTML")
        except: pass
    else:
        await reply_auto_delete(message, "❌ Неверный подарок. /gift — список", parse_mode="HTML")


# ── Slash алиасы для owner/mod команд ──
@dp.message(Command("yaderna", "nuclear"))
async def cmd_yaderna_slash(message: Message, command: CommandObject):
    """Alias: /yaderna = аутист ядерка"""
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца!"); return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение!"); return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    tname = target.mention_html()
    warnings[cid][target.id] += 1
    await bot.restrict_chat_member(cid, target.id, ChatPermissions(can_send_messages=False))
    deleted = 0
    for i in range(message.message_id, max(message.message_id - 200, 0), -1):
        try: await bot.delete_message(cid, i); deleted += 1
        except: pass
    save_data()
    await reply_auto_delete(message, f"💣 <b>ЯДЕРКА</b>\n\n👤 {tname}\n⚡ Варн | 🔇 Мут | 🗑 ~{deleted} сообщ.", parse_mode="HTML")

@dp.message(Command("sbros"))
async def cmd_sbros_slash(message: Message):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца!"); return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение!"); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    warnings[cid][target.id] = 0; reputation[cid][target.id] = 0
    xp_data[cid][target.id] = 0; levels[cid][target.id] = 0
    mod_history[cid][target.id] = []; save_data()
    await reply_auto_delete(message, f"⚙️ {target.mention_html()} — всё обнулено!", parse_mode="HTML")

@dp.message(Command("status", "modstatus"))
async def cmd_modstatus_slash(message: Message):
    if not await require_admin(message): return
    from datetime import datetime
    cid = message.chat.id; today = datetime.now().strftime("%d.%m.%Y")
    w_today = sum(warnings[cid].values())
    b_today = len(ban_list[cid])
    history_today = [h for uid_h in mod_history[cid].values() for h in uid_h if h.get("time","").startswith(today)]
    lines = [f"📊 <b>Статус модерации — {today}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    lines += [f"⚡ Варнов: <b>{w_today}</b>", f"🔨 Банов: <b>{b_today}</b>", f"📋 Действий: <b>{len(history_today)}</b>"]
    for h in history_today[-10:]:
        lines.append(f"▸ {h.get('action','?')} — {h.get('by','?')}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("announce", "объявление"))
async def cmd_announce_slash(message: Message, command: CommandObject):
    if not await require_admin(message): return
    text = command.args or (message.reply_to_message.text if message.reply_to_message else None)
    if not text:
        await reply_auto_delete(message, "⚠️ Укажи текст: /announce текст"); return
    try: await message.delete()
    except: pass
    await bot.send_message(message.chat.id,
        f"📢 <b>ОБЪЯВЛЕНИЕ</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n{text}\n\n━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode="HTML")

@dp.message(Command("corona", "корона"))
async def cmd_corona_slash(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца!"); return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение!"); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    import time as _t
    crown_holders[cid] = {"uid": target.id, "name": target.full_name, "expire": _t.time() + 86400}
    await bot.send_message(cid,
        f"👑 <b>КОРОЛЬ ЧАТА</b>\n\nОтныне и на 24 часа:\n🎖 {target.mention_html()}\n\nДа здравствует король! 👑",
        parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  🔐 РОЛИ МОДЕРАТОРОВ (только владелец выдаёт)
# ══════════════════════════════════════════════════════════
def has_mod_permission(cid: int, uid: int, perm: str) -> bool:
    """Проверяет есть ли у юзера права через роль модератора"""
    role = mod_roles[cid].get(uid)
    if not role: return False
    return perm in MOD_ROLE_PERMISSIONS.get(role, set())

def get_mod_role_label(cid: int, uid: int) -> str:
    role = mod_roles[cid].get(uid)
    return MOD_ROLE_LABELS.get(role, "") if role else ""

@dp.message(Command("giverole"))
async def cmd_give_role(message: Message):
    """аутист дать роль @user junior/senior/head — только владелец"""
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только владелец может выдавать роли!"); return
    args = message.text.split()[1:] if message.text else []
    if len(args) < 2:
        await reply_auto_delete(message,
            "⚙️ <b>Использование:</b>\n"
            "<code>/giverole @user junior|senior|head</code>\n\n"
            "🟢 Junior — варн, мут, кик\n"
            "🔵 Senior — + бан, заметки, очистка\n"
            "🔴 Head — + разбан, локдаун, темпбан",
            parse_mode="HTML"); return
    role_arg = args[-1].lower()
    if role_arg not in MOD_ROLE_PERMISSIONS:
        await reply_auto_delete(message, "⚠️ Роль: junior / senior / head"); return
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        uname = args[0].lstrip("@")
        try:
            target_member = await bot.get_chat_member(message.chat.id, uname)
            target = target_member.user
        except: pass
    if not target:
        await reply_auto_delete(message, "⚠️ Юзер не найден"); return
    cid = message.chat.id
    mod_roles[cid][target.id] = role_arg
    save_data()
    await reply_auto_delete(message,
        f"✅ {target.mention_html()} получил роль <b>{MOD_ROLE_LABELS[role_arg]}</b>",
        parse_mode="HTML")
    await log_action(
        f"🔐 <b>РОЛЬ ВЫДАНА</b>\n"
        f"👤 {target.full_name}\n"
        f"🎖 Роль: {MOD_ROLE_LABELS[role_arg]}\n"
        f"👑 Кем: {message.from_user.full_name}\n"
        f"💬 Чат: {message.chat.title}")

@dp.message(Command("takerole"))
async def cmd_take_role(message: Message):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только владелец!"); return
    target = message.reply_to_message.from_user if message.reply_to_message else None
    if not target:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение"); return
    cid = message.chat.id
    if target.id in mod_roles[cid]:
        old = MOD_ROLE_LABELS.get(mod_roles[cid].pop(target.id), "")
        save_data()
        await reply_auto_delete(message,
            f"❌ У {target.mention_html()} забрана роль <b>{old}</b>", parse_mode="HTML")
    else:
        await reply_auto_delete(message, "⚠️ У юзера нет роли")

@dp.message(Command("roles"))
async def cmd_roles(message: Message):
    if message.from_user.id != OWNER_ID and not await is_admin_by_id(message.chat.id, message.from_user.id):
        return
    cid = message.chat.id
    roles_in_chat = mod_roles.get(cid, {})
    if not roles_in_chat:
        await reply_auto_delete(message, "📋 Ролей нет — выдай через /giverole"); return
    lines = ["🔐 <b>Роли модераторов</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    for uid2, role in roles_in_chat.items():
        try:
            tm = await bot.get_chat_member(cid, uid2)
            uname = tm.user.full_name
        except: uname = f"ID{uid2}"
        lines.append(f"{MOD_ROLE_LABELS[role]} — {uname}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  🧩 ПЛАГИН-СИСТЕМА
# ══════════════════════════════════════════════════════════
PLUGIN_LABELS = {
    "economy":   "💰 Экономика",
    "games":     "🎮 Игры",
    "xp":        "⭐ XP система",
    "antispam":  "🛡 Антиспам",
    "antimat":   "🧼 Антимат",
    "reports":   "🚨 Репорты",
    "events":    "🎉 Ивенты",
    "newspaper": "📰 Газета",
    "clans":     "🤝 Кланы",
}

@dp.message(Command("plugins"))
async def cmd_plugins(message: Message):
    if message.from_user.id != OWNER_ID: return
    cid = message.chat.id
    p = plugins[cid]
    rows = []
    for key, label in PLUGIN_LABELS.items():
        status = "✅" if p.get(key, True) else "❌"
        rows.append([InlineKeyboardButton(
            text=f"{status} {label}",
            callback_data=f"plugin:toggle:{key}:{cid}"
        )])
    rows.append([InlineKeyboardButton(text="✖️ Закрыть", callback_data="plugin:close:_:0")])
    await message.answer(
        "🧩 <b>Управление плагинами</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        "Нажми чтобы вкл/выкл модуль:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data.startswith("plugin:"))
async def cb_plugin(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫 Только владелец!", show_alert=True); return
    parts = call.data.split(":")
    action, key, cid_str = parts[1], parts[2], parts[3]
    if action == "close":
        await call.message.delete(); await call.answer(); return
    cid = int(cid_str)
    plugins[cid][key] = not plugins[cid].get(key, True)
    status = "✅ включён" if plugins[cid][key] else "❌ выключен"
    # Обновить клавиатуру
    p = plugins[cid]
    rows = []
    for k, label in PLUGIN_LABELS.items():
        st = "✅" if p.get(k, True) else "❌"
        rows.append([InlineKeyboardButton(text=f"{st} {label}", callback_data=f"plugin:toggle:{k}:{cid}")])
    rows.append([InlineKeyboardButton(text="✖️ Закрыть", callback_data="plugin:close:_:0")])
    await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer(f"{PLUGIN_LABELS.get(key, key)} — {status}")

# ══════════════════════════════════════════════════════════
#  🔗 СВЯЗАННЫЕ ЧАТЫ — бан в одном = бан везде
# ══════════════════════════════════════════════════════════
@dp.message(Command("linkchats"))
async def cmd_link_chats(message: Message):
    if message.from_user.id != OWNER_ID: return
    linked = list(known_chats.keys())
    linked_chats_bans["owner"] = linked
    save_data()
    chat_names = [f"▸ {known_chats[c]}" for c in linked]
    await reply_auto_delete(message,
        f"🔗 <b>Связаны {len(linked)} чатов</b>\n\n"
        + "\n".join(chat_names) +
        "\n\n⚠️ Теперь бан в одном = бан везде",
        parse_mode="HTML")

# Хук: глобальный бан через связанные чаты (вызывается из бан-обработчиков)
async def global_ban_if_linked(uid: int, reason: str = "Глобальный бан"):
    if "owner" not in linked_chats_bans: return
    for cid in linked_chats_bans["owner"]:
        try:
            await bot.ban_chat_member(cid, uid)
            ban_list[cid].add(uid)
        except: pass

# ══════════════════════════════════════════════════════════
#  🔁 АПЕЛЛЯЦИИ — забаненный оспаривает решение
# ══════════════════════════════════════════════════════════
@dp.message(Command("appeal"))
async def cmd_appeal(message: Message):
    """Работает в ЛС боту: /appeal причина"""
    if message.chat.type != "private":
        await reply_auto_delete(message, "📬 Апелляции подаются в ЛС боту: @твой_бот"); return
    uid = message.from_user.id
    if uid in appeals and appeals[uid].get("status") == "pending":
        await message.answer("⏳ Твоя апелляция уже на рассмотрении. Подожди."); return
    reason = message.text.replace("/appeal", "").strip()
    if not reason:
        await message.answer(
            "📋 <b>Апелляция</b>\n\nНапиши:\n<code>/appeal причина почему тебя нужно разбанить</code>",
            parse_mode="HTML"); return
    appeals[uid] = {
        "uid": uid, "name": message.from_user.full_name,
        "reason": reason, "ts": __import__("time").time(), "status": "pending"
    }
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Разбанить", callback_data=f"appeal:accept:{uid}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"appeal:reject:{uid}"),
    ]])
    await bot.send_message(OWNER_ID,
        f"🔁 <b>АПЕЛЛЯЦИЯ</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {message.from_user.full_name} (<code>{uid}</code>)\n"
        f"📝 Причина: {reason}",
        parse_mode="HTML", reply_markup=kb)
    await message.answer("✅ Апелляция отправлена! Ожидай решения владельца.")

@dp.callback_query(F.data.startswith("appeal:"))
async def cb_appeal(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫 Только владелец!", show_alert=True); return
    parts = call.data.split(":")
    action, uid = parts[1], int(parts[2])
    if uid not in appeals:
        await call.answer("Устарело", show_alert=True); return
    appeals[uid]["status"] = action
    if action == "accept":
        # Разбанить во всех чатах
        for cid in known_chats:
            try:
                await bot.unban_chat_member(cid, uid, only_if_banned=True)
                ban_list[cid].discard(uid)
            except: pass
        save_data()
        try: await bot.send_message(uid, "✅ Твоя апелляция <b>одобрена</b>! Ты разбанен.", parse_mode="HTML")
        except: pass
        await call.message.edit_text(f"✅ Апелляция одобрена — ID{uid} разбанен")
    else:
        try: await bot.send_message(uid, "❌ Твоя апелляция <b>отклонена</b>.", parse_mode="HTML")
        except: pass
        await call.message.edit_text(f"❌ Апелляция отклонена — ID{uid}")
    await call.answer("Готово")

# ══════════════════════════════════════════════════════════
#  📱 ЛИЧНЫЙ КАБИНЕТ — юзер пишет боту в ЛС
# ══════════════════════════════════════════════════════════
@dp.message(Command("profile"), F.chat.type == "private")
async def cmd_profile_dm(message: Message):
    uid = message.from_user.id
    lines = [f"👤 <b>Твой профиль</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    lines.append(f"🆔 ID: <code>{uid}</code>")
    lines.append(f"📛 Имя: {message.from_user.full_name}")
    total_warns = sum(warnings[cid].get(uid, 0) for cid in warnings)
    total_rep   = sum(reputation[cid].get(uid, 0) for cid in reputation)
    total_msgs  = sum(chat_stats[cid].get(uid, 0) for cid in chat_stats)
    total_xp    = sum(xp_data[cid].get(uid, 0) for cid in xp_data)
    lines.append(f"\n⚡ Варнов всего: <b>{total_warns}</b>")
    lines.append(f"⭐ Репутация: <b>{total_rep:+d}</b>")
    lines.append(f"💬 Сообщений: <b>{total_msgs}</b>")
    lines.append(f"🏆 XP: <b>{total_xp}</b>")
    # Роли в чатах
    roles_list = [(cid, mod_roles[cid][uid]) for cid in mod_roles if uid in mod_roles[cid]]
    if roles_list:
        lines.append("\n🎖 <b>Роли:</b>")
        for cid, role in roles_list:
            cname = known_chats.get(cid, f"Чат {cid}")
            lines.append(f"  ▸ {cname}: {MOD_ROLE_LABELS[role]}")
    # Апелляция
    if uid in ban_list or any(uid in ban_list[cid] for cid in ban_list):
        lines.append("\n🔨 Ты забанен в одном из чатов")
        lines.append("📋 Подать апелляцию: /appeal причина")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📊 Статистика", callback_data=f"profile:stats:{uid}"),
        InlineKeyboardButton(text="📋 Мои репорты", callback_data=f"profile:reports:{uid}"),
    ]])
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("profile:"))
async def cb_profile(call: CallbackQuery):
    parts = call.data.split(":")
    action, uid = parts[1], int(parts[2])
    if call.from_user.id != uid:
        await call.answer("🚫 Это не твой профиль!", show_alert=True); return
    if action == "stats":
        total_xp = sum(xp_data[cid].get(uid, 0) for cid in xp_data)
        total_msgs = sum(chat_stats[cid].get(uid, 0) for cid in chat_stats)
        await call.answer(
            f"📊 Сообщений: {total_msgs}\n🏆 XP: {total_xp}",
            show_alert=True)
    elif action == "reports":
        my_reports = sum(
            1 for cid in report_queue
            for r in report_queue[cid] if r.get("reporter") == uid
        )
        accepted = sum(
            1 for cid in report_queue
            for r in report_queue[cid]
            if r.get("reporter") == uid and r.get("status") == "accepted"
        )
        score = report_score.get(uid, 0)
        await call.answer(
            f"🚨 Репортов подано: {my_reports}\n✅ Принято: {accepted}\n⭐ Скор: {score}",
            show_alert=True)

# ══════════════════════════════════════════════════════════
#  🌍 МУЛЬТИ-ЧАТ ПАНЕЛЬ — управление всеми чатами из ЛС
# ══════════════════════════════════════════════════════════
@dp.message(Command("mypanel"), F.chat.type == "private")
async def cmd_mypanel(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.answer("🚫 Только для владельца!"); return
    if not known_chats:
        await message.answer("📭 Бот ещё не добавлен ни в один чат"); return
    rows = []
    for cid, title in known_chats.items():
        total_w = sum(warnings[cid].values())
        new_r = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
        label = f"💬 {title[:20]} | ⚡{total_w} 🚨{new_r}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"mypanel:chat:{cid}")])
    rows.append([InlineKeyboardButton(text="🔗 Связать чаты", callback_data="mypanel:link:0")])
    rows.append([InlineKeyboardButton(text="✖️ Закрыть", callback_data="mypanel:close:0")])
    await message.answer(
        f"🌍 <b>Мульти-чат панель</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Чатов: <b>{len(known_chats)}</b>\n\nВыбери чат:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data.startswith("mypanel:"))
async def cb_mypanel(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫 Только владелец!", show_alert=True); return
    parts = call.data.split(":")
    action, val = parts[1], parts[2]
    if action == "close":
        await call.message.delete(); await call.answer(); return
    elif action == "link":
        linked = list(known_chats.keys())
        linked_chats_bans["owner"] = linked
        save_data()
        await call.answer(f"🔗 Связано {len(linked)} чатов!", show_alert=True); return
    elif action == "chat":
        cid = int(val)
        title = known_chats.get(cid, f"Чат {cid}")
        total_msgs  = sum(chat_stats[cid].values())
        total_warns = sum(warnings[cid].values())
        new_reports = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
        bans_count  = len(ban_list.get(cid, set()))
        roles_count = len(mod_roles.get(cid, {}))
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔇 Мут локдаун",  callback_data=f"mypanel:lockdown:{cid}"),
             InlineKeyboardButton(text="📢 Анонс",         callback_data=f"mypanel:announce:{cid}")],
            [InlineKeyboardButton(text="🚨 Репорты",       callback_data=f"mypanel:reports:{cid}"),
             InlineKeyboardButton(text="🔐 Роли",          callback_data=f"mypanel:roles:{cid}")],
            [InlineKeyboardButton(text="🧩 Плагины",       callback_data=f"mypanel:plugins:{cid}"),
             InlineKeyboardButton(text="📊 Статистика",    callback_data=f"mypanel:stats:{cid}")],
            [InlineKeyboardButton(text="◀️ Назад",         callback_data="mypanel:back:0")],
        ])
        await call.message.edit_text(
            f"💬 <b>{title}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📨 Сообщений: <b>{total_msgs}</b>\n"
            f"⚡ Варнов: <b>{total_warns}</b>\n"
            f"🚨 Новых репортов: <b>{new_reports}</b>\n"
            f"🔨 Банов: <b>{bans_count}</b>\n"
            f"🎖 Ролей: <b>{roles_count}</b>",
            parse_mode="HTML", reply_markup=kb)
    elif action == "back":
        # Перестроить главное меню
        rows = []
        for cid2, title2 in known_chats.items():
            total_w = sum(warnings[cid2].values())
            new_r = sum(1 for r in report_queue.get(cid2, []) if r.get("status") == "new")
            rows.append([InlineKeyboardButton(
                text=f"💬 {title2[:20]} | ⚡{total_w} 🚨{new_r}",
                callback_data=f"mypanel:chat:{cid2}")])
        rows.append([InlineKeyboardButton(text="🔗 Связать чаты", callback_data="mypanel:link:0")])
        rows.append([InlineKeyboardButton(text="✖️ Закрыть", callback_data="mypanel:close:0")])
        await call.message.edit_text(
            f"🌍 <b>Мульти-чат панель</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Чатов: <b>{len(known_chats)}</b>\n\nВыбери чат:",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action == "lockdown":
        cid = int(val)
        try:
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
            await call.answer(f"🔇 Локдаун в {known_chats.get(cid,'чате')}!", show_alert=True)
        except Exception as e:
            await call.answer(f"❌ {e}", show_alert=True)
    elif action == "announce":
        cid = int(val)
        pending[call.from_user.id] = {"action": "mypanel_announce", "chat_id": cid}
        await call.message.edit_text(
            "📢 Напиши текст объявления для отправки в чат:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"mypanel:chat:{cid}")
            ]]))
    elif action == "reports":
        cid = int(val)
        queue = [r for r in report_queue.get(cid, []) if r.get("status") == "new"]
        if not queue:
            await call.answer("✅ Новых репортов нет!", show_alert=True); return
        r = queue[0]
        await call.message.edit_text(
            f"🚨 <b>Репорты в {known_chats.get(cid,'чате')}</b>\n"
            f"Новых: {len(queue)}\n\n"
            f"🎯 На: {r.get('target_name','?')}\n"
            f"📝 {r.get('text','')[:80]}",
            parse_mode="HTML",
            reply_markup=kb_report_action_v2(cid, report_queue[cid].index(r), r["target"]))
    elif action == "stats":
        cid = int(val)
        from datetime import datetime
        today = datetime.now().strftime("%d.%m.%Y")
        today_msgs = sum(daily_stats[cid][uid].get(today, 0) for uid in daily_stats[cid])
        await call.answer(
            f"📊 {known_chats.get(cid,'Чат')}\n"
            f"Сегодня: {today_msgs} сообщ.\n"
            f"Участников: {len(chat_stats[cid])}",
            show_alert=True)
    elif action == "roles":
        cid = int(val)
        roles_in = mod_roles.get(cid, {})
        if not roles_in:
            await call.answer("Ролей нет. Выдай через /giverole в чате.", show_alert=True); return
        text = "\n".join(f"{MOD_ROLE_LABELS[r]} — ID{u}" for u, r in roles_in.items())
        await call.answer(f"🎖 Роли:\n{text}", show_alert=True)
    elif action == "plugins":
        cid = int(val)
        p = plugins[cid]
        text = "\n".join(f"{'✅' if p.get(k,True) else '❌'} {l}" for k, l in PLUGIN_LABELS.items())
        await call.answer(f"🧩 Плагины:\n{text}\n\nУпр: /plugins в чате", show_alert=True)
    await call.answer()

# ══════════════════════════════════════════════════════════
#  📦 АРХИВ РЕПОРТОВ
# ══════════════════════════════════════════════════════════
@dp.message(Command("reportarchive"))
async def cmd_report_archive(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    args = message.text.split()[1:] if message.text else []
    # Найти по имени/юзернейму если указано
    target_filter = args[0].lstrip("@").lower() if args else None
    closed = [r for r in report_queue.get(cid, [])
              if r.get("status") in ("accepted", "rejected")]
    if target_filter:
        closed = [r for r in closed
                  if target_filter in r.get("target_name", "").lower()]
    if not closed:
        await reply_auto_delete(message, "📦 Архив пуст"); return
    lines = [f"📦 <b>Архив репортов</b> ({len(closed)} шт.)\n━━━━━━━━━━━━━━━━━━━━━━\n"]
    for r in closed[-10:]:
        status_icon = "✅" if r.get("status") == "accepted" else "❌"
        import datetime as _dt2
        ts = _dt2.datetime.fromtimestamp(r.get("ts", 0)).strftime("%d.%m %H:%M")
        lines.append(
            f"{status_icon} <b>{r.get('target_name','?')}</b> — {r.get('category','?')}\n"
            f"  📝 {r.get('text','')[:60]}\n"
            f"  👮 {r.get('assigned_mod','?')} | 🕐 {ts}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")



async def main():

    global bot_start_time
    import time as _tstart
    bot_start_time = _tstart.time()
    load_data()
    asyncio.create_task(birthday_checker())
    asyncio.create_task(send_weekly_stats())
    asyncio.create_task(warn_expiry_checker())
    asyncio.create_task(run_lottery())
    asyncio.create_task(autosave_loop())
    asyncio.create_task(run_events())
    asyncio.create_task(run_newspaper())
    asyncio.create_task(run_stock())
    await start_web()
    if not BOT_TOKEN: raise ValueError("BOT_TOKEN не задан в переменных окружения!")
    print("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

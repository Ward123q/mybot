import asyncio
import logging
import random
import os
import sqlite3
import time as _tstart
import time as _time_module
import aiohttp
from datetime import datetime, timedelta
from collections import defaultdict
from time import time
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, ChatPermissions, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, WebAppInfo
)
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiohttp import web
import json
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# ── Новые модули ──────────────────────────
import database as db
import tickets as tkt
import dashboard
import features
import notifications as notif
import shared
import chat_settings as cs

DB_FILE_MAIN = "skinvault.db"

def db_connect():
    conn = sqlite3.connect(DB_FILE_MAIN, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # быстрее при параллельных запросах
    conn.execute("PRAGMA synchronous=NORMAL") # баланс скорость/надёжность
    return conn

def db_init():
    conn = db_connect()
    conn.executescript("""
    -- ── Основные данные ──────────────────────────────────────
    CREATE TABLE IF NOT EXISTS warnings (
        cid INTEGER, uid INTEGER, count INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS reputation (
        cid INTEGER, uid INTEGER, score INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS xp_data (
        cid INTEGER, uid INTEGER, xp INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS levels (
        cid INTEGER, uid INTEGER, level INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS streaks (
        cid INTEGER, uid INTEGER, streak INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS chat_stats (
        cid INTEGER, uid INTEGER, msg_count INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS last_seen (
        cid INTEGER, uid INTEGER, ts REAL,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS ban_list (
        cid INTEGER, uid INTEGER,
        PRIMARY KEY (cid, uid));
    -- ── Профили ──────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS birthdays (
        uid INTEGER PRIMARY KEY, date TEXT);
    CREATE TABLE IF NOT EXISTS user_titles (
        uid INTEGER PRIMARY KEY, data TEXT);
    CREATE TABLE IF NOT EXISTS avatars (
        uid INTEGER PRIMARY KEY, emoji TEXT);
    CREATE TABLE IF NOT EXISTS known_chats (
        cid INTEGER PRIMARY KEY, title TEXT);
    CREATE TABLE IF NOT EXISTS notes (
        cid INTEGER, key TEXT, value TEXT,
        PRIMARY KEY (cid, key));
    -- ── Экономика ────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS lottery_tickets (
        cid INTEGER, uid INTEGER,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS stock_invested (
        cid INTEGER, uid INTEGER, amount INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS boosters (
        uid TEXT PRIMARY KEY, data TEXT);
    CREATE TABLE IF NOT EXISTS referrals (
        uid TEXT PRIMARY KEY, invited TEXT);
    CREATE TABLE IF NOT EXISTS referral_used (
        uid TEXT PRIMARY KEY, val TEXT);
    CREATE TABLE IF NOT EXISTS rep_transfer_cooldown (
        key TEXT PRIMARY KEY, ts REAL);
    -- ── Кланы ────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS clans (
        tag TEXT PRIMARY KEY, data TEXT);
    CREATE TABLE IF NOT EXISTS clan_members (
        uid INTEGER PRIMARY KEY, clan_tag TEXT);
    -- ── Контент ──────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS quotes_data (
        cid INTEGER, idx INTEGER, data TEXT,
        PRIMARY KEY (cid, idx));
    CREATE TABLE IF NOT EXISTS journal_data (
        uid TEXT PRIMARY KEY, entries TEXT);
    CREATE TABLE IF NOT EXISTS artifacts (
        uid TEXT PRIMARY KEY, data TEXT);
    CREATE TABLE IF NOT EXISTS color_titles (
        key TEXT PRIMARY KEY, val TEXT);
    CREATE TABLE IF NOT EXISTS role_of_day (
        key TEXT PRIMARY KEY, val TEXT);
    CREATE TABLE IF NOT EXISTS mvp_votes (
        key TEXT PRIMARY KEY, val TEXT);
    -- ── История модерации ────────────────────────────────────
    CREATE TABLE IF NOT EXISTS mod_history (
        cid INTEGER, uid INTEGER, history TEXT,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS user_notes (
        cid INTEGER, uid INTEGER, note_data TEXT,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS crown_holders (
        cid INTEGER PRIMARY KEY, data TEXT);
    CREATE TABLE IF NOT EXISTS user_activity (
        cid INTEGER, uid INTEGER, day TEXT, count INTEGER DEFAULT 0,
        PRIMARY KEY (cid, uid, day));
    -- ── Новые модули ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS user_memory (
        uid INTEGER, cid INTEGER, key TEXT, value TEXT,
        PRIMARY KEY (uid, cid, key));
    CREATE TABLE IF NOT EXISTS mod_journal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cid INTEGER, mod_id INTEGER, mod_name TEXT,
        action TEXT, target_id INTEGER, target_name TEXT,
        reason TEXT, ts INTEGER);
    CREATE TABLE IF NOT EXISTS mod_shifts (
        cid INTEGER, mod_id INTEGER, mod_name TEXT,
        start_hour INTEGER, end_hour INTEGER,
        PRIMARY KEY (cid, mod_id));
    CREATE TABLE IF NOT EXISTS mod_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cid INTEGER, mod_id INTEGER, mod_name TEXT,
        task TEXT, deadline INTEGER, done INTEGER DEFAULT 0,
        created_by TEXT);
    CREATE TABLE IF NOT EXISTS quick_replies (
        cid INTEGER, key TEXT, text TEXT,
        PRIMARY KEY (cid, key));
    CREATE TABLE IF NOT EXISTS pinned_messages (
        cid INTEGER, msg_id INTEGER, title TEXT, ts INTEGER,
        PRIMARY KEY (cid, msg_id));
    CREATE TABLE IF NOT EXISTS vip_users (
        uid INTEGER, cid INTEGER, granted_by TEXT, ts INTEGER,
        PRIMARY KEY (uid, cid));
    CREATE TABLE IF NOT EXISTS events_calendar (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cid INTEGER, uid INTEGER, action TEXT,
        target_id INTEGER, target_name TEXT, ts INTEGER);
    CREATE TABLE IF NOT EXISTS global_blacklist (
        uid INTEGER PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS mod_roles_db (
        cid INTEGER, uid INTEGER, role TEXT,
        PRIMARY KEY (cid, uid));
    CREATE TABLE IF NOT EXISTS plugins_db (
        cid INTEGER, key TEXT, enabled INTEGER DEFAULT 1,
        PRIMARY KEY (cid, key));
    CREATE TABLE IF NOT EXISTS appeals_db (
        uid INTEGER PRIMARY KEY, data TEXT);
    CREATE TABLE IF NOT EXISTS welcome_settings (
        cid INTEGER PRIMARY KEY, text TEXT,
        photo TEXT, is_gif INTEGER DEFAULT 0,
        enabled INTEGER DEFAULT 1, buttons INTEGER DEFAULT 1);
    CREATE TABLE IF NOT EXISTS surveillance_chats (
        cid INTEGER PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS deleted_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cid INTEGER, uid INTEGER, name TEXT,
        text TEXT, ts REAL);
    """)
    conn.commit()
    conn.close()

# ── Хелперы чтения/записи ────────────────────────────────────
def db_get_int(table: str, cid: int, uid: int, col: str = "count") -> int:
    conn = db_connect()
    row = conn.execute(f"SELECT {col} FROM {table} WHERE cid=? AND uid=?", (cid, uid)).fetchone()
    conn.close()
    return row[col] if row else 0

def db_set_int(table: str, cid: int, uid: int, col: str, val: int):
    conn = db_connect()
    conn.execute(
        f"INSERT INTO {table} (cid, uid, {col}) VALUES (?,?,?) "
        f"ON CONFLICT(cid,uid) DO UPDATE SET {col}=excluded.{col}",
        (cid, uid, val))
    conn.commit(); conn.close()

def db_incr(table: str, cid: int, uid: int, col: str, delta: int = 1):
    cur = db_get_int(table, cid, uid, col)
    db_set_int(table, cid, uid, col, cur + delta)

# ── Миграция из data.json если существует ─────────────────────
def migrate_json_to_sqlite():
    if not Path("data.json").exists():
        return
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            d = json.load(f)
        conn = db_connect()
        # warnings
        for cid, users in d.get("warnings", {}).items():
            for uid, count in users.items():
                conn.execute("INSERT OR REPLACE INTO warnings VALUES (?,?,?)", (int(cid), int(uid), count))
        # reputation
        for cid, users in d.get("reputation", {}).items():
            for uid, score in users.items():
                conn.execute("INSERT OR REPLACE INTO reputation VALUES (?,?,?)", (int(cid), int(uid), score))
        # xp
        for cid, users in d.get("xp_data", {}).items():
            for uid, xp in users.items():
                conn.execute("INSERT OR REPLACE INTO xp_data VALUES (?,?,?)", (int(cid), int(uid), xp))
        # streaks
        for cid, users in d.get("streaks", {}).items():
            for uid, s in users.items():
                conn.execute("INSERT OR REPLACE INTO streaks VALUES (?,?,?)", (int(cid), int(uid), s))
        # levels
        for cid, users in d.get("levels", {}).items():
            for uid, l in users.items():
                conn.execute("INSERT OR REPLACE INTO levels VALUES (?,?,?)", (int(cid), int(uid), l))
        # chat_stats
        for cid, users in d.get("chat_stats", {}).items():
            for uid, c in users.items():
                conn.execute("INSERT OR REPLACE INTO chat_stats VALUES (?,?,?)", (int(cid), int(uid), c))
        # ban_list
        for cid, uids in d.get("ban_list", {}).items():
            for uid in uids:
                conn.execute("INSERT OR IGNORE INTO ban_list VALUES (?,?)", (int(cid), int(uid)))
        # known_chats
        for cid, title in d.get("known_chats", {}).items():
            conn.execute("INSERT OR REPLACE INTO known_chats VALUES (?,?)", (int(cid), title))
        # birthdays
        for uid, bday in d.get("birthdays", {}).items():
            conn.execute("INSERT OR REPLACE INTO birthdays VALUES (?,?)", (int(uid), bday))
        # mod_history
        for cid, users in d.get("mod_history", {}).items():
            for uid, h in users.items():
                conn.execute("INSERT OR REPLACE INTO mod_history VALUES (?,?,?)",
                             (int(cid), int(uid), json.dumps(h, ensure_ascii=False)))
        # crown_holders
        for cid, v in d.get("crown_holders", {}).items():
            conn.execute("INSERT OR REPLACE INTO crown_holders VALUES (?,?)",
                         (int(cid), json.dumps(v, ensure_ascii=False)))
        # last_seen
        for cid, users in d.get("last_seen", {}).items():
            for uid, ts in users.items():
                conn.execute("INSERT OR REPLACE INTO last_seen VALUES (?,?,?)", (int(cid), int(uid), ts))
        conn.commit(); conn.close()
        # Переименовываем json чтобы не мигрировать снова
        import os as _os
        _os.rename("data.json", "data.json.bak")
        print("✅ Миграция data.json → SQLite завершена!")
    except Exception as e:
        print(f"[migrate error] {e}")

# ── Новые load/save через SQLite ──────────────────────────────
def save_data():
    """Сохраняет RAM данные в SQLite"""
    try:
        conn = db_connect()
        # warnings
        for cid, users in warnings.items():
            for uid, count in users.items():
                conn.execute("INSERT OR REPLACE INTO warnings VALUES (?,?,?)", (cid, uid, count))
        # reputation
        for cid, users in reputation.items():
            for uid, score in users.items():
                conn.execute("INSERT OR REPLACE INTO reputation VALUES (?,?,?)", (cid, uid, score))
        # xp
        for cid, users in xp_data.items():
            for uid, xp in users.items():
                conn.execute("INSERT OR REPLACE INTO xp_data VALUES (?,?,?)", (cid, uid, xp))
        # streaks
        for cid, users in streaks.items():
            for uid, s in users.items():
                conn.execute("INSERT OR REPLACE INTO streaks VALUES (?,?,?)", (cid, uid, s))
        # levels
        for cid, users in levels.items():
            for uid, l in users.items():
                conn.execute("INSERT OR REPLACE INTO levels VALUES (?,?,?)", (cid, uid, l))
        # chat_stats
        for cid, users in chat_stats.items():
            for uid, c in users.items():
                conn.execute("INSERT OR REPLACE INTO chat_stats VALUES (?,?,?)", (cid, uid, c))
        # ban_list
        for cid, uids in ban_list.items():
            for uid in uids:
                conn.execute("INSERT OR IGNORE INTO ban_list VALUES (?,?)", (cid, uid))
        # known_chats
        for cid, title in known_chats.items():
            conn.execute("INSERT OR REPLACE INTO known_chats VALUES (?,?)", (cid, title))
        # birthdays
        for uid, bday in birthdays.items():
            conn.execute("INSERT OR REPLACE INTO birthdays VALUES (?,?)", (uid, bday))
        # last_seen
        for cid, users in last_seen.items():
            for uid, ts in users.items():
                conn.execute("INSERT OR REPLACE INTO last_seen VALUES (?,?,?)", (cid, uid, ts))
        # crown_holders
        for cid, v in crown_holders.items():
            conn.execute("INSERT OR REPLACE INTO crown_holders VALUES (?,?)",
                         (cid, json.dumps(v, ensure_ascii=False)))
        # mod_history
        for cid, users in mod_history.items():
            for uid, h in users.items():
                conn.execute("INSERT OR REPLACE INTO mod_history VALUES (?,?,?)",
                             (cid, uid, json.dumps(h, ensure_ascii=False)))
        # global_blacklist
        for uid in global_blacklist:
            conn.execute("INSERT OR IGNORE INTO global_blacklist VALUES (?)", (uid,))
        # mod_roles
        for cid, roles in mod_roles.items():
            for uid, role in roles.items():
                conn.execute("INSERT OR REPLACE INTO mod_roles_db VALUES (?,?,?)", (cid, uid, role))
        # plugins
        for cid, mods in plugins.items():
            for key, enabled in mods.items():
                conn.execute("INSERT OR REPLACE INTO plugins_db VALUES (?,?,?)", (cid, key, int(enabled)))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"[save_data error] {e}")

def load_data():
    """Загружает данные из SQLite в RAM при запуске"""
    global warnings, reputation, xp_data, streaks, levels, chat_stats
    global ban_list, known_chats, birthdays, last_seen, crown_holders
    global mod_history, global_blacklist, mod_roles, plugins
    try:
        conn = db_connect()
        for r in conn.execute("SELECT cid,uid,count FROM warnings"):
            warnings[r["cid"]][r["uid"]] = r["count"]
        for r in conn.execute("SELECT cid,uid,score FROM reputation"):
            reputation[r["cid"]][r["uid"]] = r["score"]
        for r in conn.execute("SELECT cid,uid,xp FROM xp_data"):
            xp_data[r["cid"]][r["uid"]] = r["xp"]
        for r in conn.execute("SELECT cid,uid,streak FROM streaks"):
            streaks[r["cid"]][r["uid"]] = r["streak"]
        for r in conn.execute("SELECT cid,uid,level FROM levels"):
            levels[r["cid"]][r["uid"]] = r["level"]
        for r in conn.execute("SELECT cid,uid,msg_count FROM chat_stats"):
            chat_stats[r["cid"]][r["uid"]] = r["msg_count"]
        for r in conn.execute("SELECT cid,uid FROM ban_list"):
            ban_list[r["cid"]][r["uid"]] = True
        for r in conn.execute("SELECT cid,title FROM known_chats"):
            known_chats[r["cid"]] = r["title"]
        for r in conn.execute("SELECT uid,date FROM birthdays"):
            birthdays[r["uid"]] = r["date"]
        for r in conn.execute("SELECT cid,uid,ts FROM last_seen"):
            last_seen[r["cid"]][r["uid"]] = r["ts"]
        for r in conn.execute("SELECT cid,data FROM crown_holders"):
            crown_holders[r["cid"]] = json.loads(r["data"])
        for r in conn.execute("SELECT cid,uid,history FROM mod_history"):
            mod_history[r["cid"]][r["uid"]] = json.loads(r["history"])
        for r in conn.execute("SELECT uid FROM global_blacklist"):
            global_blacklist.add(r["uid"])
        for r in conn.execute("SELECT cid,uid,role FROM mod_roles_db"):
            mod_roles[r["cid"]][r["uid"]] = r["role"]
        for r in conn.execute("SELECT cid,key,enabled FROM plugins_db"):
            plugins[r["cid"]][r["key"]] = bool(r["enabled"])
        conn.close()
        print("✅ Данные загружены из SQLite")
    except Exception as e:
        print(f"[load_data error] {e}")

# ══════════════════════════════════════════
#  СИСТЕМА УРОВНЕЙ (500 уровней)
# ══════════════════════════════════════════
LEVEL_TITLES = {
    # ── 1–10: Новички ──────────────────────────
    1:  ("🌱", "Росток"),
    2:  ("🌿", "Новичок"),
    3:  ("🍃", "Участник"),
    4:  ("🌾", "Активный"),
    5:  ("⚡", "Энергичный"),
    6:  ("🔥", "Горячий"),
    7:  ("💫", "Звёздный"),
    8:  ("🎯", "Меткий"),
    9:  ("🛡", "Страж"),
    10: ("⚔️", "Воин"),
    # ── 11–20: Бойцы ───────────────────────────
    11: ("🗡", "Дуэлянт"),
    12: ("🏹", "Лучник"),
    13: ("🔮", "Мистик"),
    14: ("🧙", "Маг"),
    15: ("🌙", "Ночной страж"),
    16: ("☄️", "Метеорит"),
    17: ("🌟", "Звезда"),
    18: ("💎", "Бриллиант"),
    19: ("👁", "Всевидящий"),
    20: ("🏆", "Чемпион"),
    # ── 21–30: Элита ───────────────────────────
    21: ("🦁", "Лев"),
    22: ("🐉", "Дракон"),
    23: ("🦅", "Орёл"),
    24: ("🌊", "Волна"),
    25: ("⚜️", "Элита"),
    26: ("🔱", "Посейдон"),
    27: ("🌌", "Космонавт"),
    28: ("🌠", "Метеор"),
    29: ("🎖", "Ветеран"),
    30: ("👑", "Король"),
    # ── 31–40: Легенды ─────────────────────────
    31: ("🏰", "Рыцарь"),
    32: ("⚡", "Молния"),
    33: ("🔥", "Пламя"),
    34: ("💀", "Тёмный рыцарь"),
    35: ("🌑", "Тень"),
    36: ("🌀", "Вихрь"),
    37: ("🎇", "Феникс"),
    38: ("🐺", "Одинокий волк"),
    39: ("🦂", "Скорпион"),
    40: ("🛸", "Пришелец"),
    # ── 41–50: Мифы ────────────────────────────
    41: ("🌍", "Хранитель"),
    42: ("🌞", "Солнечный"),
    43: ("🌈", "Радужный"),
    44: ("⚗️", "Алхимик"),
    45: ("🔯", "Чародей"),
    46: ("🌺", "Сакура"),
    47: ("🏯", "Сёгун"),
    48: ("🐲", "Повелитель"),
    49: ("💠", "Абсолют"),
    50: ("👾", "Полубог"),
    # ── 51–60: Высшие ──────────────────────────
    51: ("🌋", "Вулкан"),
    52: ("🧊", "Ледяной"),
    53: ("🌪", "Торнадо"),
    54: ("🦄", "Единорог"),
    55: ("🧬", "Эволюция"),
    56: ("🕷", "Паук"),
    57: ("🦊", "Лис"),
    58: ("🦋", "Бабочка"),
    59: ("🐍", "Змей"),
    60: ("🌟", "★ ШЕСТИДЕСЯТЫЙ ★"),
    # ── 61–70: Боги природы ────────────────────
    61: ("🌊", "Цунами"),
    62: ("🔥", "Адское пламя"),
    63: ("❄️", "Вечный лёд"),
    64: ("⚡", "Гром"),
    65: ("🌋", "Апокалипсис"),
    66: ("☄️", "Астероид"),
    67: ("🌌", "Галактика"),
    68: ("🛸", "Вселенная"),
    69: ("🌠", "Сверхновая"),
    70: ("💥", "Большой взрыв"),
    # ── 71–80: Тёмные силы ─────────────────────
    71: ("🌑", "Тёмный бог"),
    72: ("☠️", "Смерть"),
    73: ("👻", "Призрак"),
    74: ("🔮", "Оракул"),
    75: ("🌀", "Бездна"),
    76: ("🧿", "Провидец"),
    77: ("⚫", "Чёрная дыра"),
    78: ("🌠", "Квазар"),
    79: ("💀", "Жнец"),
    80: ("🌟", "★ ВОСЬМИДЕСЯТЫЙ ★"),
    # ── 81–90: Повелители ──────────────────────
    81: ("🧠", "Гений"),
    82: ("🚀", "Ракета"),
    83: ("💻", "Хакер"),
    84: ("🎓", "Профессор"),
    85: ("🥋", "Мастер"),
    86: ("🎬", "Режиссёр"),
    87: ("🎸", "Рокер"),
    88: ("🏆", "Великий"),
    89: ("💎", "Алмазный"),
    90: ("👑", "Император"),
    # ── 91–99: Пред-сотый ──────────────────────
    91: ("🐉", "Повелитель драконов"),
    92: ("🌞", "Бессмертный"),
    93: ("💠", "Вечный"),
    94: ("🔯", "Всемогущий"),
    95: ("⚜️", "Верховный"),
    96: ("🌌", "Межзвёздный"),
    97: ("🔱", "Нептун"),
    98: ("🔥", "Феникс"),
    99: ("🌠", "Грань сотни"),
    # ── 100: Рубеж ─────────────────────────────
    100: ("💯", "⚡ СОТЫЙ ⚡"),
    # ── 101–149: Легендарные ───────────────────
    101: ("🗡", "Тёмный клинок"),
    105: ("🛡", "Непоколебимый"),
    110: ("⚔️", "Легендарный воин"),
    115: ("🌋", "Вулкан-бог"),
    120: ("☄️", "Комета смерти"),
    125: ("🌌", "Повелитель космоса"),
    130: ("🔥", "Вечный огонь"),
    135: ("🌀", "Хаос"),
    140: ("🌈", "Спектр"),
    145: ("🏆", "Абсолютный чемпион"),
    # ── 150: Рубеж ─────────────────────────────
    150: ("💎", "⚡ ПОЛТОРАСТА ⚡"),
    # ── 151–199: Боги ──────────────────────────
    155: ("🌑", "Бог тьмы"),
    160: ("🌞", "Бог солнца"),
    165: ("🐉", "Бог драконов"),
    170: ("👁", "Всевидящий бог"),
    175: ("🔱", "Посейдон II"),
    180: ("👑", "Бог богов"),
    185: ("🌌", "Создатель"),
    190: ("💠", "Источник"),
    195: ("🔯", "Архимаг"),
    # ── 200: Рубеж ─────────────────────────────
    200: ("👁", "⚡ ДВУХСОТЫЙ ⚡"),
    # ── 201–299: Демиурги ──────────────────────
    210: ("⚜️", "Демиург"),
    220: ("🌠", "Астральный"),
    230: ("💥", "Сингулярность"),
    240: ("⚫", "Абсолютная тьма"),
    250: ("🌟", "⚡ ДВЕСТИ ПЯТЬДЕСЯТ ⚡"),
    260: ("🌟", "Абсолютный свет"),
    270: ("🌌", "Бесконечность"),
    280: ("💠", "Кристальный бог"),
    290: ("🔥", "Пламя вечности"),
    # ── 300: Рубеж ─────────────────────────────
    300: ("🔱", "⚡ ТРЁХСОТЫЙ ⚡"),
    # ── 301–399: Первородные ───────────────────
    325: ("🐉", "Первородный дракон"),
    350: ("👑", "⚡ ТРИСТА ПЯТЬДЕСЯТ ⚡"),
    375: ("💀", "Первородная смерть"),
    # ── 400: Рубеж ─────────────────────────────
    400: ("☠️", "⚡ ЧЕТЫРЁХСОТЫЙ ⚡"),
    # ── 401–499: Запредельные ──────────────────
    425: ("🌌", "Запредельный"),
    450: ("✨", "⚡ ЧЕТЫРЕСТА ПЯТЬДЕСЯТ ⚡"),
    475: ("💫", "Трансцендентный"),
    499: ("🌠", "Грань"),
    # ── 500: Максимум ──────────────────────────
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
ADMIN_IDS        = {7823802800, 8046083268, 7397338777, 7991589995}
MAX_WARNINGS     = 3
ANTI_MAT_ENABLED  = False

# 📸 Кэш последних сообщений для доказательной базы
# {cid: {uid: [msg_id, msg_id, ...]}}  — хранит последние 10 msg_id
_violation_msg_cache: dict = defaultdict(lambda: defaultdict(list))

# 💳 Платёжная система отключена

MAT_MUTE_MINUTES = 5
AUTO_KICK_BOTS   = True

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

@dp.errors()
async def global_error_handler(event, exception: Exception):
    """Глобальный обработчик ошибок — ловит flood control и другие"""
    err = str(exception)
    if "Too Many Requests" in err or "Flood" in err or "retry after" in err.lower():
        import re as _rfe
        m = _rfe.search(r"retry after (\d+)", err, _rfe.IGNORECASE)
        wait = int(m.group(1)) + 1 if m else 5
        await asyncio.sleep(wait)
    return True

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

# Глобальный чёрный список — авто-бан во всех чатах
global_blacklist = set()  # {uid}

# Карантин — авто-мут новых участников
quarantine_chats = set()  # {cid}

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
    "📋 <b>Правила чата</b>\n\n"

    "🔞 <b>Контент 18+</b>\n"
    "▸ Запрещено отправлять любой контент для взрослых:\n"
    "стикеры, видео, гифки, ссылки и сцены насилия.\n\n"

    "🥴 <b>Наркотики</b>\n"
    "▸ Запрещено отправлять или распространять\n"
    "любой контент, связанный с наркотиками.\n\n"

    "📢 <b>Реклама</b>\n"
    "▸ Запрещена реклама социальных сетей, брендов,\n"
    "услуг, проектов и любой рекламной деятельности.\n\n"

    "🛡 <b>Оскорбление администрации</b>\n"
    "▸ Запрещено оскорблять, грубить и хамить\n"
    "модераторам и администраторам чата.\n"
    "⚠️ Наказание: варн. При повторном — бан.\n\n"

    "⚠️ <b>Наказания:</b>\n"
    "▸ Первое нарушение — предупреждение (варн)\n"
    "▸ Повторное нарушение — мут или бан\n\n"

    ""
    "<i>Соблюдай правила — чат будет комфортным для всех 🤝</i>\n"
)

MUTE_MESSAGES = [
    "╔══════════════════╗\n║   🔇  МУТ ВЫДАН   ║\n╚══════════════════╝\n\n👤 Цель: {name}\n⏱ Время: <b>{time}</b>\n──────────────────\n<i>Соблюдайте правила сообщества.</i>",
    "╔══════════════════╗\n║   🔇  ТИШИНА      ║\n╚══════════════════╝\n\n👤 Цель: {name}\n⏱ Срок: <b>{time}</b>\n──────────────────\n<i>Нарушение правил чата.</i>",
]
BAN_MESSAGES = [
    "╔══════════════════╗\n║   🔨  БАН ВЫДАН   ║\n╚══════════════════╝\n\n👤 Цель: {name}\n📋 Причина: <b>{reason}</b>\n──────────────────\n<i>Пользователь удалён из чата.</i>",
    "╔══════════════════╗\n║  🔨  БЛОКИРОВКА   ║\n╚══════════════════╝\n\n👤 Нарушитель: {name}\n📋 Причина: <b>{reason}</b>\n──────────────────\n<i>До свидания.</i>",
]
WARN_MESSAGES = [
    "╔══════════════════╗\n║  ⚠️  ПРЕДУПРЕЖДЕНИЕ ║\n╚══════════════════╝\n\n👤 Нарушитель: {name}\n📋 Причина: <b>{reason}</b>\n⚡ Варнов: <b>{count}/{max}</b>\n──────────────────\n<i>При достижении лимита — бан.</i>",
    "╔══════════════════╗\n║  ⚠️  ВАРН ВЫДАН    ║\n╚══════════════════╝\n\n👤 Цель: {name}\n📋 Причина: <b>{reason}</b>\n⚡ Счёт: <b>{count}/{max}</b>\n──────────────────\n<i>Следи за поведением.</i>",
]
AUTOBAN_MESSAGES = [
    "╔══════════════════╗\n║  🤖  АВТОБАН      ║\n╚══════════════════╝\n\n👤 Нарушитель: {name}\n⚡ Варнов: <b>{max}/{max}</b>\n──────────────────\n<i>Лимит предупреждений исчерпан.\nЗаблокирован автоматически.</i>",
    "╔══════════════════╗\n║  🔨  СИСТЕМА      ║\n╚══════════════════╝\n\n👤 Цель: {name}\n⚡ Лимит: <b>{max} варна</b>\n──────────────────\n<i>Автоматическая блокировка активирована.</i>",
]
RANDOM_BAN_REASONS = [
    "слишком умный", "подозрение в адекватности", "нарушение закона бутерброда",
    "превышение лимита здравого смысла", "нарушение пространственно-временного континуума",
    "слишком много смайликов", "мигание слишком громко", "дышит не в том ритме",
    "нарушение закона сохранения мемов", "избыток харизмы в общественном месте",
    "подозрение в наличии собственного мнения", "слишком красивый для этого чата",
    "превышение суточной нормы здравомыслия", "опасно близко к сарказму",
    "нелегальный уровень иронии", "несанкционированное веселье",
    "слишком адекватные ответы", "нарушение правил гравитации",
    "чрезмерное количество правильных ответов", "подозрение в реальной жизни",
]
QUOTES = [
    "«Не важно, как медленно ты идёшь, главное — не останавливаться.» — Конфуций",
    "«Будь изменением, которое хочешь видеть в мире.» — Ганди",
    "«Будь собой. Все остальные роли заняты.» — Уайльд",
    "«Единственный способ делать великое — любить то, что делаешь.» — Джобс",
    "«Успех — идти от неудачи к неудаче без потери энтузиазма.» — Черчилль",
    "«Делай что должен, и будь что будет.» — Толстой",
    "«Тот, кто хочет, ищет возможности. Кто не хочет — ищет причины.» — Сократ",
    "«Жизнь — это то, что происходит, пока ты строишь другие планы.» — Леннон",
    "«Упасть семь раз, встать восемь.» — Японская пословица",
    "«Мы то, что мы делаем постоянно.» — Аристотель",
    "«Лучший способ предсказать будущее — создать его.» — Друкер",
    "«Всё, что я знаю — я ничего не знаю.» — Сократ",
    "«Не жди. Время никогда не будет подходящим.» — Наполеон Хилл",
    "«Счастье — это когда тебя понимают.» — Конфуций",
    "«Сила не в том чтобы никогда не падать, а в том чтобы вставать каждый раз.» — Конфуций",
    "«Мечтай большими мечтами — только они способны воспламенить душу.» — Марк Аврелий",
    "«Побеждает тот, кто думает, что может.» — Вергилий",
    "«Где нет борьбы, там нет силы.» — Фредерик Дуглас",
]
BALL_ANSWERS = [
    "🌟 Определённо да!", "🔥 Без сомнений!", "🌈 Скорее всего да.",
    "🌫 Трудно сказать.", "⏳ Спроси потом.", "🌀 Пока неясно.",
    "🌑 хз", "❄️ Нет.", "🪨 Определённо нет",
    "✅ Да, и ещё раз да!", "💯 Абсолютно точно!", "🎯 Попал в точку — да!",
    "😂 Смешной вопрос. Нет.", "🤔 Мои источники говорят — нет.",
    "🧿 Картина неоднозначная.", "🌊 Спроси у моря — оно знает.",
    "🦋 Вселенная говорит да, но осторожно.", "💀 Даже не надейся.",
    "🎲 Подбрось монетку — результат тот же.", "🤖 Ответ вычислен: нет.",
    "⚡ Знаки говорят да!", "🌙 Луна на твоей стороне.",
    "🐉 Дракон внутри тебя говорит: иди на риск.",
    "😴 Проснись и переспроси.", "🎪 Шоу продолжается — ответ да!",
]
TRUTH_QUESTIONS = [
    "Ты когда-нибудь врал другу?", "Какой твой самый большой страх?",
    "Ты когда-нибудь влюблялся в друга?", "Что тебя раздражает больше всего?",
    "Твой самый неловкий момент в жизни?", "Ты когда-нибудь плакал из-за фильма?",
    "Что ты никогда не расскажешь родителям?", "Ты когда-нибудь писял под кровать?",
    "Ты бы хотел себе ту самую девушку найти?", "Ты бы хотел завести детей в будущем?",
    "Какую песню ты слушаешь когда грустишь?", "Что ты делал вчера вечером на самом деле?",
    "Есть ли человек которому ты завидуешь?", "Что ты купил бы если бы был миллионером?",
    "Твой самый большой секрет который никто не знает?",
    "Что ты думаешь о людях в этом чате на самом деле?",
    "Какую ложь ты повторял так часто что сам поверил?",
    "Есть кто-то кого ты не можешь простить?",
    "Что ты сделал из чего тебе сейчас стыдно?",
    "Какая твоя самая странная привычка?",
    "Если бы тебе осталось жить один день — что бы сделал?",
    "Ты когда-нибудь читал чужую переписку?",
    "Что бы изменил в себе если бы мог?",
    "Какое твоё самое большое сожаление?",
    "Ты влюблён прямо сейчас?",
    "Какой человек в твоей жизни тебя разочаровал больше всего?",
    "Ты когда-нибудь притворялся больным чтобы не идти куда-то?",
    "Что ты делаешь когда думаешь что никто не смотрит?",
    "Твоя самая большая трата денег о которой жалеешь?",
    "Есть что-то чего ты боишься но никому не говоришь?",
]
DARE_CHALLENGES = [
    "Напиши комплимент случайному участнику чата!", "Признайся в чём-нибудь стыдном...",
    "Напиши стих про себя прямо сейчас.", "Расскажи смешной случай из жизни.",
    "Придумай и напиши анекдот прямо сейчас.", "Сделай 10 приседаний если не лень.",
    "Напиши следующее сообщение заглавными буквами.", "Расскажи о своём кринже за последнюю неделю.",
    "Напиши что ты думаешь о последнем человеке кто написал в чат.",
    "Поставь любой странный статус на 10 минут.",
    "Напиши сообщение задом наперёд.", "Признайся в своей тайной суперсиле.",
    "Расскажи самый плохой анекдот который знаешь.",
    "Напиши сообщение только эмодзи без слов — пусть угадают смысл.",
    "Скажи что-нибудь приятное каждому кто напишет в чат в ближайшие 5 минут.",
    "Напиши своё имя песней (каждая буква — отдельное слово).",
    "Расскажи о самом странном сне который тебе снился.",
    "Напиши монолог от лица своего кота/собаки/питомца.",
    "Придумай новое правило для этого чата.",
    "Объясни почему ты лучший участник чата — 3 аргумента.",
    "Напиши что ты ел сегодня и дай этому романтичное название.",
    "Расскажи свою самую провальную историю знакомства.",
    "Напиши самое длинное сообщение за свою историю в этом чате прямо сейчас.",
    "Придумай прозвище для следующего кто напишет в чат.",
    "Спой куплет любой песни текстом прямо сейчас.",
]
WOULD_YOU_RATHER = [
    "Быть богатым но одиноким или бедным но счастливым?",
    "Уметь летать или быть невидимым?", "Знать будущее или изменить прошлое?",
    "Говорить только правду или постоянно врать?",
    "Жить 200 лет в бедности или 50 лет в богатстве?",
    "Никогда не спать или спать 16 часов в сутки?",
    "Уметь читать мысли или видеть будущее?",
    "Потерять все воспоминания или никогда не иметь новых?",
    "Жить без интернета или без музыки?",
    "Есть только сладкое или только солёное всю жизнь?",
    "Быть самым умным в комнате дураков или самым глупым среди гениев?",
    "Никогда не чувствовать усталости или никогда не чувствовать боли?",
    "Путешествовать в прошлое или в будущее?",
    "Иметь суперсилу но никому не говорить или не иметь суперсилы?",
    "Жить в мире без болезней или в мире без войн?",
    "Всегда опаздывать или всегда приходить на час раньше?",
    "Уметь говорить на всех языках или играть на всех инструментах?",
    "Потерять телефон или кошелёк?",
    "Знать когда умрёшь или как умрёшь?",
    "Иметь миллион друзей или одного настоящего?",
    "Никогда не краснеть или никогда не потеть?",
    "Прожить 10 раз по 10 лет или один раз 100 лет?",
    "Быть знаменитым и несчастным или неизвестным но счастливым?",
    "Иметь фотографическую память или уметь забывать всё плохое?",
    "Жить в горах или у моря?",
    "Есть любимую еду каждый день или пробовать что-то новое каждый раз?",
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
    "Твоё чувство юмора — настоящее сокровище! 😂",
    "Ты из тех людей с которыми приятно общаться! 🤝",
    "Твои сообщения всегда поднимают настроение! 💫",
    "Ты как глоток свежего воздуха в этом чате! 🌬️",
    "Общение с тобой — одно удовольствие! ✨",
    "Ты умеешь видеть то что другие не замечают! 👁",
    "Твоя энергия заряжает всех вокруг! ⚡",
    "Ты один из тех кто делает интернет лучше! 🌐",
    "Твоё присутствие здесь — уже событие! 🎉",
    "Ты мыслишь нестандартно — это редкость! 🧠",
    "С таким человеком как ты хочется общаться вечно! 💎",
    "Твой вайб — просто космос! 🚀",
    "Ты как хорошая музыка — всегда к месту! 🎵",
    "Твой юмор — высший класс! Респект! 🎩",
    "Ты из тех кто запоминается! 🌺",
    "Рядом с тобой даже скучный день становится интересным! 🎪",
    "Ты настоящий! Это ценнее всего! 💯",
    "Твои мысли всегда интересны — продолжай! 📚",
    "Ты тот кто украшает любую компанию! 👑",
]
PREDICTIONS = [
    "🔮 Сегодня тебе повезёт! Только не трать деньги.",
    "🌩 Осторожно с незнакомцами сегодня.",
    "🌟 Звёзды говорят — ты красавчик!", "🍀 Удача на твоей стороне!",
    "🐉 Тебя ждёт необычная встреча. Возможно с котом.",
    "🛋 Лучший план на сегодня — поспать. Доверяй процессу.",
    "💰 Скоро придут деньги. Или уйдут. Звёзды не уточнили.",
    "📱 Тебе напишет кто-то из прошлого. Готовься.",
    "🍕 Сегодня день пиццы. Закажи — не пожалеешь.",
    "😴 Твоя кровать скучает по тебе. Вернись к ней пораньше.",
    "🎲 Рискни сегодня — звёзды за тебя.",
    "🌈 После серой полосы уже виден цвет — держись.",
    "🤝 Новое знакомство изменит многое.",
    "⚡ Сегодня ты на пике — используй этот день.",
    "🧹 Время убраться — в доме и в голове.",
    "🎵 Включи любимую музыку — станет лучше.",
    "🌙 Ночь принесёт ответы на вопросы которые мучают.",
    "🦋 Маленькое изменение сегодня даст большой результат.",
    "🏆 Ты ближе к цели чем думаешь — не останавливайся.",
    "👀 Кто-то тайно восхищается тобой прямо сейчас.",
    "🎯 Твоя интуиция сегодня особенно точна — доверяй ей.",
    "🌊 Плыви по течению — оно приведёт куда надо.",
    "🔑 Ответ который ищешь — уже знаешь. Просто признай.",
    "🐢 Медленно но верно — ты придёшь к своему.",
    "😂 Сегодня будет повод посмеяться от души.",
    "🌺 Скажи кому-нибудь что они важны для тебя — не пожалеешь.",
    "🚀 Большая возможность появится неожиданно — не пропусти.",
    "🧿 Доверяй только тем кого знаешь давно.",
    "💫 Сегодня твой день — просто поверь в это.",
    "🎪 Жизнь подкинет что-то неожиданное — прими с юмором.",
]

async def health(request):
    return web.Response(text="OK")

async def serve_mini_app(request):
    return web.FileResponse("mini_app.html")

# Ожидающие верификацию: {uid: {"cid": int, "name": str, "ts": float}}
_verify_pending: dict = {}
# Результаты верификации: {uid: {"passed": bool, "reason": str}}
_verify_results: dict = {}

async def serve_verify(request):
    """Страница верификации через WebApp"""
    vpn_key = os.getenv("VPN_API_KEY", "")
    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Верификация — Chat Guard</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#0d0f14;color:#e8eaf0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px;}}
.card{{background:#111318;border:1px solid rgba(255,255,255,.07);border-radius:16px;padding:32px 24px;max-width:360px;width:100%;text-align:center;}}
.icon{{font-size:56px;margin-bottom:16px;}}
h1{{font-size:20px;font-weight:700;margin-bottom:8px;}}
p{{color:#7c8299;font-size:14px;line-height:1.5;margin-bottom:24px;}}
.btn{{background:#5865f2;color:#fff;border:none;border-radius:12px;padding:14px 24px;font-size:16px;font-weight:600;cursor:pointer;width:100%;transition:opacity .2s;}}
.btn:active{{opacity:.8;}}
.btn:disabled{{opacity:.5;cursor:not-allowed;}}
.status{{margin-top:16px;font-size:13px;color:#7c8299;min-height:20px;}}
.ok{{color:#23a55a;}} .err{{color:#f23f42;}}
</style>
</head>
<body>
<div class="card">
  <div class="icon">🛡</div>
  <h1>Верификация</h1>
  <p>Для входа в чат нам нужно убедиться что ты не бот и не используешь VPN/прокси.</p>
  <button class="btn" id="btn" onclick="verify()">✅ Пройти проверку</button>
  <div class="status" id="status">Нажми кнопку для проверки</div>
</div>
<script>
const tg = window.Telegram.WebApp;
tg.ready();
tg.expand();

async function verify() {{
  const btn = document.getElementById('btn');
  const status = document.getElementById('status');
  btn.disabled = true;
  status.textContent = '⏳ Проверяем...';

  try {{
    // Получаем IP через публичный сервис
    const ipResp = await fetch('https://api.ipify.org?format=json');
    const ipData = await ipResp.json();
    const ip = ipData.ip;

    // Проверяем IP через vpnapi.io
    const vpnKey = '{vpn_key}';
    let isVpn = false;
    let vpnReason = '';

    if (vpnKey) {{
      const vpnResp = await fetch(`https://vpnapi.io/api/${{ip}}?key=${{vpnKey}}`);
      const vpnData = await vpnResp.json();
      const sec = vpnData.security || {{}};
      isVpn = sec.vpn || sec.proxy || sec.tor || sec.relay || false;
      if (sec.vpn) vpnReason += 'VPN ';
      if (sec.proxy) vpnReason += 'Proxy ';
      if (sec.tor) vpnReason += 'Tor ';
      if (sec.relay) vpnReason += 'Relay ';
    }}

    // Отправляем результат на сервер
    const uid = tg.initDataUnsafe?.user?.id;
    await fetch('/verify_result', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{
        uid: uid,
        ip: ip,
        is_vpn: isVpn,
        reason: vpnReason.trim(),
        init_data: tg.initData
      }})
    }});

    if (isVpn) {{
      status.innerHTML = `<span class="err">❌ Обнаружен ${{vpnReason.trim()}}. Отключи и попробуй снова.</span>`;
      btn.disabled = false;
    }} else {{
      status.innerHTML = '<span class="ok">✅ Проверка пройдена! Можешь закрыть окно.</span>';
      btn.textContent = '✅ Готово';
      setTimeout(() => tg.close(), 2000);
    }}
  }} catch(e) {{
    status.innerHTML = '<span class="err">❌ Ошибка соединения. Попробуй снова.</span>';
    btn.disabled = false;
  }}
}}
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_verify_result(request):
    """Принимает результат верификации от WebApp"""
    try:
        data = await request.json()
        uid = int(data.get("uid", 0))
        is_vpn = data.get("is_vpn", False)
        reason = data.get("reason", "")
        ip = data.get("ip", "")

        if uid not in _verify_pending:
            return web.Response(text="ok")

        pending = _verify_pending.pop(uid)
        cid = pending["cid"]
        name = pending["name"]
        action = pending.get("action", "kick")

        _verify_results[uid] = {"passed": not is_vpn, "reason": reason, "ip": ip}

        if is_vpn:
            # VPN обнаружен — применяем действие
            await log_action(
                f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
                f"👤 {name} (<code>{uid}</code>)\n"
                f"🌐 IP: <code>{ip}</code>\n"
                f"🔍 Обнаружено: <b>{reason}</b>\n"
                f"⚖️ Действие: <b>{action}</b>\n"
                f"💬 Чат: <code>{cid}</code>"
            )
            try:
                if action == "ban":
                    await bot.ban_chat_member(cid, uid)
                elif action in ("kick", "warn"):
                    await bot.ban_chat_member(cid, uid)
                    await bot.unban_chat_member(cid, uid)
                # Уведомляем юзера
                await bot.send_message(uid,
                    f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
                    f"❌ Обнаружен: <b>{reason}</b>\n"
                    f"Отключи VPN/прокси и попробуй зайти снова.",
                    parse_mode="HTML")
            except: pass
        else:
            # Чистый — снимаем ограничения
            try:
                await bot.restrict_chat_member(
                    cid, uid,
                    permissions=ChatPermissions(
                        can_send_messages=True,
                        can_send_media_messages=True,
                        can_send_polls=True,
                        can_send_other_messages=True,
                        can_add_web_page_previews=True
                    )
                )
            except: pass

    except Exception as e:
        logging.warning(f"verify_result error: {e}")

    return web.Response(text="ok")


async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/mini", serve_mini_app)
    app.router.add_get("/verify", serve_verify)
    app.router.add_post("/verify_result", handle_verify_result)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()

async def check_admin(message: Message) -> bool:
    if message.from_user.id in ADMIN_IDS:
        return True
    try:
        m = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return m.status in ("administrator", "creator")
    except:
        return False

async def is_admin_by_id(chat_id: int, user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except:
        return False

async def require_admin(message: Message) -> bool:
    if not await check_admin(message):
        await reply_auto_delete(message, "⛔ Команда доступна только администраторам.", parse_mode="HTML")
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

async def safe_send(coro, retries=3):
    """Выполняет запрос к Telegram API с автоповтором при flood control"""
    for attempt in range(retries):
        try:
            return await coro
        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "Flood" in err or "retry after" in err.lower():
                import re as _re_flood
                m = _re_flood.search(r"retry after (\d+)", err, _re_flood.IGNORECASE)
                wait = int(m.group(1)) + 1 if m else (attempt + 1) * 3
                await asyncio.sleep(wait)
            else:
                break
    return None

async def log_action(text: str):
    try:
        await safe_send(bot.send_message(LOG_CHANNEL_ID, text, parse_mode="HTML"))
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
    for attempt in range(3):
        try:
            sent = await message.answer(text, **kwargs)
            asyncio.create_task(schedule_delete(sent))
            return sent
        except Exception as e:
            if "Too Many Requests" in str(e) or "retry after" in str(e).lower():
                import re as _re2
                m = _re2.search(r"retry after (\d+)", str(e), _re2.IGNORECASE)
                await asyncio.sleep(int(m.group(1)) + 1 if m else (attempt+1)*3)
            else:
                break
    return None

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
    for attempt in range(3):
        try:
            sent = await message.reply(text, **kwargs)
            asyncio.create_task(auto_delete(message, sent))
            return sent
        except Exception as e:
            if "Too Many Requests" in str(e) or "retry after" in str(e).lower():
                import re as _re3
                m = _re3.search(r"retry after (\d+)", str(e), _re3.IGNORECASE)
                await asyncio.sleep(int(m.group(1)) + 1 if m else (attempt+1)*3)
            else:
                break
    return None

def add_mod_history(cid: int, uid: int, action: str, reason: str, by_name: str):
    """Записывает действие модератора в историю — память + SQLite"""
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
    # 💾 Пишем в SQLite чтобы дашборд видел
    try:
        conn = db_connect()
        conn.execute("""CREATE TABLE IF NOT EXISTS mod_history
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
             cid INTEGER, uid INTEGER, action TEXT,
             reason TEXT, by_name TEXT,
             created_at TEXT DEFAULT (datetime('now')))""")
        conn.execute(
            "INSERT INTO mod_history (cid, uid, action, reason, by_name) VALUES (?,?,?,?,?)",
            (cid, uid, action, reason or "—", by_name)
        )
        conn.commit()
        conn.close()
    except Exception as _e:
        pass

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
            f"🔄 <b>Авторазмут</b>\n"
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
    """Логирует нарушение + форвардит последние сообщения как доказательство"""
    preview = msg_text[:300] + ("…" if len(msg_text) > 300 else "")
    await log_action(
        f"╔══════════════════╗\n║  📸  НАРУШЕНИЕ    ║\n╚══════════════════╝\n\n"
        f"👤 <b>Нарушитель:</b> {uname} (<code>{uid}</code>)\n"
        f"⚖️ <b>Действие:</b> {action}\n"
        f"📝 <b>Причина:</b> {reason}\n"
        f"👮 <b>Модератор:</b> {by_name}\n"
        f"💬 <b>Чат:</b> {chat_title}\n"
        f"🕐 <b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"💬 <b>Сообщение:</b>\n<code>{preview}</code>"
    )
    # Форвардим последние сообщения нарушителя из кэша как доказательства
    if uid in _violation_msg_cache.get(cid, {}):
        msgs = _violation_msg_cache[cid][uid][-5:]
        if msgs:
            try:
                await bot.send_message(
                    LOG_CHANNEL_ID,
                    f"📎 <b>Доказательная база — последние {len(msgs)} сообщений {uname}:</b>",
                    parse_mode="HTML"
                )
                for msg_id in msgs:
                    try:
                        await bot.forward_message(LOG_CHANNEL_ID, cid, msg_id)
                        await asyncio.sleep(0.2)
                    except: pass
            except: pass

async def dm_warn_user(uid: int, uname: str, reason: str, chat_title: str,
                       action: str, by_name: str):
    """Отправляет личное предупреждение нарушителю в лс"""
    try:
        await bot.send_message(
            uid,
            f"⚠️ <b>Уведомление — {chat_title}</b>\n\n"
            f"· Действие: <b>{action}</b>\n"
            f"· Причина: <b>{reason}</b>\n"
            f"· Модератор: <b>{by_name}</b>\n\n"
            f"<i>Если считаете решение несправедливым, подайте апелляцию командой /appeal</i>",
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
    return [InlineKeyboardButton(text="‹ Назад", callback_data=f"panel:back:{tid}")]

def kb_main_menu(tid: int = 0) -> InlineKeyboardMarkup:
    """Главное меню панели для администраторов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤  Участник",         callback_data=f"panel:select:{tid}"),
         InlineKeyboardButton(text="🚨  Репорты",          callback_data=f"panel:reports:{tid}")],
        [InlineKeyboardButton(text="👥  Участники",        callback_data=f"panel:members:{tid}"),
         InlineKeyboardButton(text="🚫  Список банов",     callback_data=f"members:banlist:{tid}")],
        [InlineKeyboardButton(text="⚙️  Настройки",        callback_data=f"panel:chatsettings:{tid}"),
         InlineKeyboardButton(text="🛡️  Модерация",        callback_data=f"panel:modtools:{tid}")],
        [InlineKeyboardButton(text="📊  Статистика",       callback_data=f"panel:stats:{tid}"),
         InlineKeyboardButton(text="🎖️  Роли",             callback_data=f"panel:roles:{tid}")],
        [InlineKeyboardButton(text="⚡  Быстрые ответы",   callback_data=f"panel:quickreplies:{tid}"),
         InlineKeyboardButton(text="📌  Закреплённые",     callback_data=f"panel:pins:{tid}")],
        [InlineKeyboardButton(text="👋  Welcome",          callback_data=f"panel:welcome:{tid}"),
         InlineKeyboardButton(text="🧩  Плагины",          callback_data=f"panel:plugins:{tid}")],
        [InlineKeyboardButton(text="🎫  Тикеты",           callback_data=f"panel:tickets:{tid}"),
         InlineKeyboardButton(text="📱  Open App",         web_app=WebAppInfo(url="https://mybot-1s9l.onrender.com/mini"))],
        [InlineKeyboardButton(text="✕  Закрыть",          callback_data="panel:close:0")],
    ])

def kb_user_panel(tid: int) -> InlineKeyboardMarkup:
    """Панель действий над участником"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔇  Мут",               callback_data=f"panel:mute:{tid}"),
         InlineKeyboardButton(text="🔊  Размут",             callback_data=f"panel:unmute:{tid}")],
        [InlineKeyboardButton(text="⚠️  Варн",              callback_data=f"panel:warn:{tid}"),
         InlineKeyboardButton(text="✅  Снять варн",         callback_data=f"panel:unwarn:{tid}")],
        [InlineKeyboardButton(text="🔨  Бан",                callback_data=f"panel:ban:{tid}"),
         InlineKeyboardButton(text="🕊️  Разбан",             callback_data=f"panel:unban:{tid}")],
        [InlineKeyboardButton(text="👻  Тихий бан",          callback_data=f"panel:silentban:{tid}"),
         InlineKeyboardButton(text="⏳  Темпбан 24ч",        callback_data=f"ban:{tid}:tempban24")],
        [InlineKeyboardButton(text="🙅  Стикермут",          callback_data=f"panel:stickermute:{tid}"),
         InlineKeyboardButton(text="🎬  Гифмут",             callback_data=f"panel:gifmute:{tid}")],
        [InlineKeyboardButton(text="🎙️  Войсмут",            callback_data=f"panel:voicemute:{tid}"),
         InlineKeyboardButton(text="🚫  Всёмут",             callback_data=f"panel:allmedmute:{tid}")],
        [InlineKeyboardButton(text="ℹ️  Информация",         callback_data=f"panel:info:{tid}"),
         InlineKeyboardButton(text="📋  История",             callback_data=f"panel:modhistory:{tid}")],
        [InlineKeyboardButton(text="📝  Заметки",            callback_data=f"panel:usernotes:{tid}"),
         InlineKeyboardButton(text="💎  VIP",                callback_data=f"panel:vip:{tid}")],
        [InlineKeyboardButton(text="🎭  Приколы",            callback_data=f"panel:fun:{tid}"),
         InlineKeyboardButton(text="‹ Назад",               callback_data=f"panel:mainmenu:0")],
    ])

def kb_owner_panel() -> InlineKeyboardMarkup:
    """Панель владельца — только для OWNER_ID"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍  Все чаты",          callback_data="owner:chats:0"),
         InlineKeyboardButton(text="📡  Статус бота",       callback_data="owner:status:0")],
        [InlineKeyboardButton(text="🆘  SOS ALL",           callback_data="owner:sosall:0"),
         InlineKeyboardButton(text="🔓  Разлокдаун всех",   callback_data="owner:sosoff:0")],
        [InlineKeyboardButton(text="📢  Бродкаст",          callback_data="owner:broadcast:0"),
         InlineKeyboardButton(text="🚷  Чёрный список",     callback_data="owner:blacklist:0")],
        [InlineKeyboardButton(text="💾  Бэкап",             callback_data="owner:backup:0"),
         InlineKeyboardButton(text="🔎  Аудит чата",        callback_data="owner:audit:0")],
        [InlineKeyboardButton(text="🧩  Плагины глобал",    callback_data="owner:plugins:0"),
         InlineKeyboardButton(text="📅  Календарь",         callback_data="owner:calendar:0")],
        [InlineKeyboardButton(text="🎯  Задачи модов",      callback_data="owner:tasks:0"),
         InlineKeyboardButton(text="🏆  Рейтинг модов",     callback_data="owner:modrating:0")],
        [InlineKeyboardButton(text="🚁  Эвакуация",         callback_data="owner:evacuation:0"),
         InlineKeyboardButton(text="🔬  Карантин",          callback_data="owner:quarantine:0")],
        [InlineKeyboardButton(text="🧹  Зачистка",          callback_data="owner:cleanup:0"),
         InlineKeyboardButton(text="🔗  Связать чаты",      callback_data="owner:linkchats:0")],
        [InlineKeyboardButton(text="✕  Закрыть",           callback_data="panel:close:0")],
    ])

def kb_mute(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин",     callback_data=f"mute:{tid}:5"),
         InlineKeyboardButton(text="15 мин",    callback_data=f"mute:{tid}:15"),
         InlineKeyboardButton(text="30 мин",    callback_data=f"mute:{tid}:30")],
        [InlineKeyboardButton(text="1 час",     callback_data=f"mute:{tid}:60"),
         InlineKeyboardButton(text="3 часа",    callback_data=f"mute:{tid}:180"),
         InlineKeyboardButton(text="12 часов",  callback_data=f"mute:{tid}:720")],
        [InlineKeyboardButton(text="1 день",    callback_data=f"mute:{tid}:1440"),
         InlineKeyboardButton(text="7 дней",    callback_data=f"mute:{tid}:10080"),
         InlineKeyboardButton(text="✎  Своё",   callback_data=f"mute:{tid}:custom")],
        kb_back(tid),
    ])

def kb_warn(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤬  Мат",            callback_data=f"warn:{tid}:Мат в чате"),
         InlineKeyboardButton(text="📨  Спам",            callback_data=f"warn:{tid}:Спам")],
        [InlineKeyboardButton(text="💢  Оскорбление",     callback_data=f"warn:{tid}:Оскорбление"),
         InlineKeyboardButton(text="🌊  Флуд",            callback_data=f"warn:{tid}:Флуд")],
        [InlineKeyboardButton(text="📣  Реклама",         callback_data=f"warn:{tid}:Реклама"),
         InlineKeyboardButton(text="🔞  Контент 18+",     callback_data=f"warn:{tid}:Контент 18+")],
        [InlineKeyboardButton(text="⚡  Провокация",      callback_data=f"warn:{tid}:Провокация"),
         InlineKeyboardButton(text="✎  Своя причина",    callback_data=f"warn:{tid}:custom")],
        kb_back(tid),
    ])

def kb_ban(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥  Грубые нарушения",  callback_data=f"ban:{tid}:Грубые нарушения правил"),
         InlineKeyboardButton(text="📣  Спам / реклама",    callback_data=f"ban:{tid}:Спам и реклама")],
        [InlineKeyboardButton(text="🔞  Контент 18+",       callback_data=f"ban:{tid}:Контент 18+"),
         InlineKeyboardButton(text="🤖  Бот / накрутка",    callback_data=f"ban:{tid}:Бот или накрутка")],
        [InlineKeyboardButton(text="⏱  Бан 24 часа",       callback_data=f"ban:{tid}:tempban24"),
         InlineKeyboardButton(text="⏱  Бан 7 дней",        callback_data=f"ban:{tid}:tempban168")],
        [InlineKeyboardButton(text="✎  Своя причина",      callback_data=f"ban:{tid}:custom")],
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
        [InlineKeyboardButton(text="📌  Закрепить",          callback_data=f"msg:pin:{tid}"),
         InlineKeyboardButton(text="📍  Открепить",          callback_data=f"msg:unpin:{tid}")],
        [InlineKeyboardButton(text="🗑️  Удалить",            callback_data=f"msg:del:{tid}"),
         InlineKeyboardButton(text="🧹  Очистить 10",        callback_data=f"msg:clear10:{tid}")],
        [InlineKeyboardButton(text="🧹  Очистить 20",        callback_data=f"msg:clear20:{tid}"),
         InlineKeyboardButton(text="🧹  Очистить 50",        callback_data=f"msg:clear50:{tid}")],
        [InlineKeyboardButton(text="📢  Объявление",         callback_data=f"msg:announce:{tid}"),
         InlineKeyboardButton(text="📊  Голосование",        callback_data=f"msg:poll:{tid}")],
        [InlineKeyboardButton(text="‹ Назад",               callback_data=f"panel:mainmenu:0")],
    ])

def kb_members(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮  Список админов",     callback_data=f"members:adminlist:{tid}"),
         InlineKeyboardButton(text="🏆  Топ активности",     callback_data=f"members:top:{tid}")],
        [InlineKeyboardButton(text="📈  Топ XP",             callback_data=f"members:topxp:{tid}"),
         InlineKeyboardButton(text="🥇  Топ МВП",            callback_data=f"members:mvpstats:{tid}")],
        [InlineKeyboardButton(text="🔇  Мут 24ч реклама",    callback_data=f"members:warn24:{tid}"),
         InlineKeyboardButton(text="⚠️  Варны участника",    callback_data=f"members:warninfo:{tid}")],
        [InlineKeyboardButton(text="🚫  Список банов",       callback_data=f"members:banlist:{tid}"),
         InlineKeyboardButton(text="📋  Отчёт модератора",   callback_data=f"members:modreport:{tid}")],
        [InlineKeyboardButton(text="‹ Назад",               callback_data=f"panel:mainmenu:0")],
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

class PendingInputMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message): return await handler(event, data)
        uid = event.from_user.id if event.from_user else None
        if uid and uid in pending and not (event.text and event.text.startswith("/")):
            p = pending.pop(uid)
            action   = p.get("action", "")
            target_id   = p.get("target_id", 0)
            target_name = p.get("target_name", "")
            chat_id     = p.get("chat_id", event.chat.id)
            text = event.text or ""
            try:
                if action == "mute_custom":
                    mins, label = parse_duration(text)
                    if mins:
                        await bot.restrict_chat_member(chat_id, target_id,
                            permissions=ChatPermissions(can_send_messages=False),
                            until_date=datetime.now() + timedelta(minutes=mins))
                        await event.answer(random.choice(MUTE_MESSAGES).format(
                            name=f"<b>{target_name}</b>", time=label), parse_mode="HTML")
                    else: await event.reply("⚠️ Примеры: 10, 30m, 2h, 1d")
                elif action == "warn_custom":
                    reason = text.strip() or "Нарушение правил"
                    warnings[chat_id][target_id] += 1; count = warnings[chat_id][target_id]
                    if count >= MAX_WARNINGS:
                        await bot.ban_chat_member(chat_id, target_id)
                        warnings[chat_id][target_id] = 0
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
                        f"📢 <b>Объявление</b>\n\n{text}\n\n— <b>Администрация</b>", parse_mode="HTML")
                elif action == "poll_text":
                    parts = [x.strip() for x in text.split("|") if x.strip()]
                    if len(parts) >= 3:
                        await bot.send_poll(chat_id, question=parts[0], options=parts[1:], is_anonymous=False)
                    else:
                        await event.reply("⚠️ Формат: Вопрос|Вариант1|Вариант2")
                elif action == "weather_city":
                    await event.answer(await get_weather(text.strip()), parse_mode="HTML")
                elif action == "mypanel_announce":
                    await bot.send_message(chat_id,
                        f"📢 <b>Объявление</b>\n\n{text}\n\n— <b>Администрация</b>", parse_mode="HTML")
                    await event.answer("✅ Объявление отправлено!")
            except Exception as e:
                await event.reply(f"⚠️ Ошибка: {e}")
            try: await event.delete()
            except: pass
            return
        return await handler(event, data)

class StatsMiddleware(BaseMiddleware):
    # Трекер флуда: {cid: {uid: [timestamps]}}
    _flood_tracker: dict = {}

    async def _check_flood(self, uid: int, cid: int, name: str) -> bool:
        """Проверяет флуд по настройкам чата. Возвращает True если флуд обнаружен."""
        try:
            chat_cfg = cs.get_settings(cid)
            if not chat_cfg.get("antispam_enabled", True):
                return False

            threshold = chat_cfg.get("flood_msgs", 10)
            action    = chat_cfg.get("flood_action", "mute")

            now = _time_module.time()
            if cid not in self._flood_tracker:
                self._flood_tracker[cid] = {}
            if uid not in self._flood_tracker[cid]:
                self._flood_tracker[cid][uid] = []

            # Оставляем только последние 60 секунд
            self._flood_tracker[cid][uid] = [
                t for t in self._flood_tracker[cid][uid] if now - t < 60
            ]
            self._flood_tracker[cid][uid].append(now)
            count = len(self._flood_tracker[cid][uid])

            if count >= threshold:
                # Сбрасываем счётчик
                self._flood_tracker[cid][uid] = []

                # Применяем действие
                if action == "mute":
                    mins = chat_cfg.get("mute_duration", 10)
                    try:
                        from aiogram.types import ChatPermissions
                        await bot.restrict_chat_member(
                            cid, uid,
                            permissions=ChatPermissions(can_send_messages=False),
                            until_date=datetime.now() + timedelta(minutes=mins)
                        )
                        await bot.send_message(
                            cid,
                            f"🔇 <a href='tg://user?id={uid}'>{name}</a> замучен на {mins} мин. за флуд ({count} сообщ/мин)",
                            parse_mode="HTML"
                        )
                        add_mod_history(cid, uid, f"🔇 Мут {mins}м (флуд)", f"{count} сообщений за минуту", "AutoMod")
                    except: pass

                elif action == "warn":
                    warnings[cid][uid] += 1
                    save_data()
                    add_mod_history(cid, uid, f"⚡ Варн (флуд)", f"{count} сообщений за минуту", "AutoMod")
                    try:
                        await bot.send_message(
                            cid,
                            f"⚡ <a href='tg://user?id={uid}'>{name}</a> получил варн за флуд ({count} сообщ/мин) "
                            f"— {warnings[cid][uid]}/{cs.get_settings(cid).get('max_warns', MAX_WARNINGS)}",
                            parse_mode="HTML"
                        )
                        if warnings[cid][uid] >= chat_cfg.get("max_warns", MAX_WARNINGS):
                            await bot.ban_chat_member(cid, uid)
                            add_mod_history(cid, uid, "🔨 Автобан (варны)", "Лимит варнов", "AutoMod")
                    except: pass

                elif action == "kick":
                    try:
                        await bot.ban_chat_member(cid, uid)
                        await bot.unban_chat_member(cid, uid)
                        add_mod_history(cid, uid, "👟 Кик (флуд)", f"{count} сообщений за минуту", "AutoMod")
                        await bot.send_message(
                            cid,
                            f"👟 <a href='tg://user?id={uid}'>{name}</a> кикнут за флуд ({count} сообщ/мин)",
                            parse_mode="HTML"
                        )
                    except: pass

                return True
        except Exception as _e:
            pass
        return False
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and event.from_user and event.chat.type in ("group","supergroup"):
            chat_stats[event.chat.id][event.from_user.id] += 1
            known_chats[event.chat.id] = event.chat.title or str(event.chat.id)
            uid, cid = event.from_user.id, event.chat.id
            # Сохраняем чат в БД
            try:
                await db.upsert_chat(cid, event.chat.title or str(cid))
            except: pass
            # Проверка флуда по настройкам чата
            try:
                if not await is_admin_by_id(cid, uid):
                    flooded = await self._check_flood(uid, cid, event.from_user.full_name)
                    if flooded:
                        return  # не обрабатываем сообщение если флуд
            except: pass
            # Трекинг для уведомлений
            try:
                await notif.track_message(event)
            except: pass
            # Синхронизация через shared
            try:
                shared.update_online(uid, event.from_user.full_name, cid)
                await shared.check_spam(uid, cid, event.from_user.full_name, event.chat.title or "")
                # 📸 Кэшируем msg_id для доказательной базы (последние 10)
                _violation_msg_cache[cid][uid].append(event.message_id)
                if len(_violation_msg_cache[cid][uid]) > 10:
                    _violation_msg_cache[cid][uid].pop(0)
                # 🔴 Проверка триггер-слов
                if event.text:
                    await _check_trigger_words(event, cid, uid)
                if shared.dashboard_settings.get("media_log_enabled", True):
                    media_type = None
                    file_id = ""
                    if event.photo:      media_type, file_id = "photo",    event.photo[-1].file_id
                    elif event.video:    media_type, file_id = "video",    event.video.file_id
                    elif event.document: media_type, file_id = "document", event.document.file_id
                    elif event.voice:    media_type, file_id = "voice",    event.voice.file_id
                    elif event.sticker:  media_type, file_id = "sticker",  event.sticker.file_id
                    elif event.animation:media_type, file_id = "animation",event.animation.file_id
                    if media_type:
                        shared.log_media(cid, uid, event.from_user.full_name,
                                         event.chat.title or "", media_type, file_id)
            except: pass
            # Авто-объявления
            try:
                await cs.on_message(cid)
            except: pass
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
        if event.chat.type not in ("group","supergroup"): return await handler(event, data)
        if not event.text or event.text.startswith("/"): return await handler(event, data)
        if not event.from_user: return await handler(event, data)
        if event.new_chat_members or event.left_chat_member: return await handler(event, data)
        uid, cid = event.from_user.id, event.chat.id
        # Проверяем настройки чата
        try:
            chat_cfg = cs.get_settings(cid)
            if not chat_cfg.get("antimat_enabled", True):
                return await handler(event, data)
        except: pass
        if not ANTI_MAT_ENABLED: return await handler(event, data)
        try:
            m = await bot.get_chat_member(cid, uid)
            if m.status in ("administrator","creator"): return await handler(event, data)
        except: pass
        return await handler(event, data)
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
                            until_date=datetime.now() + timedelta(minutes=mins))
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
                        f"📢 <b>Объявление</b>\n\n{text}\n\n— <b>Администрация</b>", parse_mode="HTML")
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

        # Приветствие из настроек
        if chat_cfg.get("welcome_enabled", True):
            welcome_text = chat_cfg.get("welcome_text", "👋 Добро пожаловать, {name}!")
            welcome_text = welcome_text.replace("{name}", f"<b>{member.full_name}</b>")
            sent = await message.answer(welcome_text, parse_mode="HTML")

            # Удаляем предыдущее приветствие
            if chat_cfg.get("welcome_delete_prev", True):
                mins = chat_cfg.get("welcome_delete_mins", 5)
                async def _del_welcome(msg, delay):
                    await asyncio.sleep(delay * 60)
                    try: await msg.delete()
                    except: pass
                asyncio.create_task(_del_welcome(sent, mins))
        else:
            sent = await message.answer(
                f"👋 Добро пожаловать, <b>{member.full_name}</b>!\n"
                f"📋 Ознакомься с правилами чата.",
                parse_mode="HTML"
            )

        # Автомут новичков
        if chat_cfg.get("auto_mute_newcomers", False):
            hours = chat_cfg.get("newcomer_mute_hours", 1)
            try:
                await bot.restrict_chat_member(
                    cid, member.id,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=datetime.now() + timedelta(hours=hours)
                )
            except: pass

        # Бонус новичку
        bonus = chat_cfg.get("newcomer_bonus", 0)
        if bonus > 0:
            try:
                conn = db_connect()
                conn.execute(
                    "INSERT INTO reputation (cid,uid,score) VALUES (?,?,?) "
                    "ON CONFLICT(cid,uid) DO UPDATE SET score=score+?",
                    (cid, member.id, bonus, bonus)
                )
                conn.commit()
                conn.close()
            except: pass

        # 🌐 АнтиVPN проверка
        asyncio.create_task(_process_new_member_vpn(message, member))

@dp.message(F.left_chat_member)
async def on_left_member(message: Message):
    member = message.left_chat_member
    if member.is_bot: return

@dp.callback_query(F.data.startswith("panel:"))
async def cb_panel(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
            await log_action(f"🔊 <b>Размут</b>\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
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
            await log_action(f"🕊️ <b>Разбан</b>\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
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
            text_lines = [f"🚨 <b>Очередь жалоб</b> ({len(queue)} шт.)\n"]
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
                "💰 <b>Экономика</b>\n\n"
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
            lines = [f"🚫 <b>Список банов</b> ({len(bans)} чел.)\n"]
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

        # ── Новые разделы панели ──────────────────────────────
        elif action == "mainmenu":
            total_msgs  = sum(chat_stats[cid].values())
            total_warns = sum(warnings[cid].values())
            open_reps   = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
            await call.message.edit_text(
                f"✨ <b>CHAT GUARD</b> — Панель администратора\n"
                f"💬 <b>{call.message.chat.title}</b>\n"
                f"👥 Активных: <b>{len(chat_stats[cid])}</b>\n"
                f"📨 Сообщений: <b>{total_msgs}</b>\n"
                f"⚡ Варнов: <b>{total_warns}</b>\n"
                f"🚨 Репортов: <b>{open_reps}</b>\n"
                f"",
                parse_mode="HTML", reply_markup=kb_main_menu())

        elif action == "chatsettings":
            await call.message.edit_text(
                f"⚙️ <b>Настройки чата</b>\n"
                f"🧼 Антимат: <b>{'✅' if ANTI_MAT_ENABLED else '❌'}</b>\n"
                f"🤖 Автокик ботов: <b>{'✅' if AUTO_KICK_BOTS else '❌'}</b>\n"
                f"🔔 Welcome: <b>{'✅' if welcome_get(cid)['enabled'] else '❌'}</b>\n"
                f"👁 Наблюдение: <b>{'✅' if surveillance_enabled(cid) else '❌'}</b>\n"
                f"🔄 Авто-правила: <b>{'✅' if cid in auto_rules_chats else '❌'}</b>\n"
                f"🔬 Карантин: <b>{'✅' if cid in quarantine_chats else '❌'}</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🧼 Антимат вкл/выкл", callback_data=f"chat:antimat:{cid}"),
                     InlineKeyboardButton(text="🔔 Welcome вкл/выкл", callback_data=f"panelset:welcome:{cid}")],
                    [InlineKeyboardButton(text="👁 Наблюдение",       callback_data=f"panelset:surveillance:{cid}"),
                     InlineKeyboardButton(text="🔄 Авто-правила",     callback_data=f"panelset:autorules:{cid}")],
                    [InlineKeyboardButton(text="🔬 Карантин",         callback_data=f"panelset:quarantine:{cid}"),
                     InlineKeyboardButton(text="🌍 Язык бота",        callback_data=f"panelset:lang:{cid}")],
                    [InlineKeyboardButton(text="◀️ Назад",            callback_data="panel:mainmenu:0")],
                ]))

        elif action == "modtools":
            await call.message.edit_text(
                f"🛡 <b>Инструменты модерации</b>\n"
                f"Выбери действие:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡ Выдать варн",      callback_data=f"panel:warn:{tid}"),
                     InlineKeyboardButton(text="🔇 Мут чата",         callback_data=f"panelset:lockdown:{cid}")],
                    [InlineKeyboardButton(text="🧹 Очистить сообщ.",  callback_data=f"panelset:clear:{cid}"),
                     InlineKeyboardButton(text="👁 Удалённые сообщ.", callback_data=f"panelset:deletedlog:{cid}")],
                    [InlineKeyboardButton(text="🚨 Репорты",          callback_data=f"panel:reports:{tid}"),
                     InlineKeyboardButton(text="📋 История мод.",     callback_data=f"panel:modhistory:{tid}")],
                    [InlineKeyboardButton(text="⏰ Смены",            callback_data=f"panelset:shifts:{cid}"),
                     InlineKeyboardButton(text="📊 Рейтинг модов",    callback_data=f"panelset:modrating:{cid}")],
                    [InlineKeyboardButton(text="◀️ Назад",            callback_data="panel:mainmenu:0")],
                ]))

        elif action == "stats":
            today = datetime.now().strftime("%d.%m.%Y")
            today_msgs = sum(daily_stats[cid][u].get(today, 0) for u in daily_stats[cid])
            top = sorted(chat_stats[cid].items(), key=lambda x: x[1], reverse=True)[:3]
            top_lines = []
            for u2, c2 in top:
                try:
                    tm2 = await bot.get_chat_member(cid, u2)
                    top_lines.append(f"▸ {tm2.user.full_name}: {c2}")
                except: top_lines.append(f"▸ ID{u2}: {c2}")
            await call.message.edit_text(
                f"📊 <b>Статистика чата</b>\n"
                f"📅 Сегодня: <b>{today_msgs}</b> сообщ.\n"
                f"👥 Всего активных: <b>{len(chat_stats[cid])}</b>\n"
                f"🔨 Забанено: <b>{len(ban_list.get(cid, set()))}</b>\n\n"
                f"🏆 <b>Топ-3 активных:</b>\n" + "\n".join(top_lines),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📊 Хитмап", callback_data=f"panelset:heatmap:{cid}")],
                    [InlineKeyboardButton(text="◀️ Назад",  callback_data="panel:mainmenu:0")],
                ]))

        elif action == "roles":
            roles_in = mod_roles.get(cid, {})
            if not roles_in:
                text = "🎖 <b>Роли модераторов</b>\n\nРолей нет.\nВладелец выдаёт через /giverole"
            else:
                lines2 = ["🎖 <b>Роли модераторов</b>\n"]
                for u2, r2 in roles_in.items():
                    try:
                        tm2 = await bot.get_chat_member(cid, u2)
                        n2 = tm2.user.full_name
                    except: n2 = f"ID{u2}"
                    lines2.append(f"{MOD_ROLE_LABELS.get(r2,r2)} — {n2}")
                text = "\n".join(lines2)
            await call.message.edit_text(text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")
                ]]))

        elif action == "quickreplies":
            conn = db_connect()
            rows = conn.execute("SELECT key, text FROM quick_replies WHERE cid=?", (cid,)).fetchall()
            conn.close()
            if not rows:
                text = "⚡ <b>Быстрые ответы</b>\n\nПусто.\nДобавь: /addreply ключ текст"
            else:
                lines2 = ["⚡ <b>Быстрые ответы</b>\n"]
                for r2 in rows:
                    lines2.append(f"▸ <code>!{r2['key']}</code> — {r2['text'][:40]}")
                lines2.append("\n<i>Использование: !ключ в чате</i>")
                text = "\n".join(lines2)
            await call.message.edit_text(text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")
                ]]))

        elif action == "pins":
            conn = db_connect()
            rows = conn.execute("SELECT msg_id, title FROM pinned_messages WHERE cid=?", (cid,)).fetchall()
            conn.close()
            if not rows:
                text = "📌 <b>Закреплённые</b>\n\nПусто.\n/pin заголовок (реплай)"
            else:
                lines2 = ["📌 <b>Закреплённые</b>\n"]
                for r2 in rows:
                    lines2.append(f"▸ {r2['title']}")
                text = "\n".join(lines2)
            await call.message.edit_text(text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")
                ]]))

        elif action == "welcome":
            s = welcome_get(cid)
            status = "✅ включён" if s["enabled"] else "❌ выключен"
            await call.message.edit_text(
                f"🔔 <b>Welcome экран</b>\n"
                f"Статус: {status}\n"
                f"Медиа: {'✅' if s['photo'] else '❌'}\n\n"
                f"📝 {s['text'][:100]}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Вкл",    callback_data=f"panelset:welcomeon:{cid}"),
                     InlineKeyboardButton(text="❌ Выкл",   callback_data=f"panelset:welcomeoff:{cid}")],
                    [InlineKeyboardButton(text="🧪 Тест",   callback_data=f"panelset:testwelcome:{cid}")],
                    [InlineKeyboardButton(text="◀️ Назад",  callback_data="panel:mainmenu:0")],
                ]))

        elif action == "plugins":
            p = plugins[cid]
            rows2 = []
            for k, label in PLUGIN_LABELS.items():
                st = "✅" if p.get(k, True) else "❌"
                rows2.append([InlineKeyboardButton(
                    text=f"{st} {label}",
                    callback_data=f"plugin:toggle:{k}:{cid}")])
            rows2.append([InlineKeyboardButton(text="◀️ Назад", callback_data="panel:mainmenu:0")])
            await call.message.edit_text(
                "🧩 <b>Плагины</b>\nНажми чтобы вкл/выкл:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows2))

        elif action == "tickets":
            await tkt.show_tickets_panel(call, bot, is_mod=True)

        elif action == "vip":
            if tid == 0:
                await call.answer("↩️ Открой через реплай на юзера", show_alert=True); return
            v = is_vip(tid, cid)
            import time as _tvp
            conn = db_connect()
            if v:
                conn.execute("DELETE FROM vip_users WHERE uid=? AND cid=?", (tid, cid))
                conn.commit(); conn.close()
                await call.answer(f"💎 VIP снят с {tname}", show_alert=True)
            else:
                conn.execute("INSERT OR REPLACE INTO vip_users VALUES (?,?,?,?)",
                             (tid, cid, call.from_user.full_name, int(_tvp.time())))
                conn.commit(); conn.close()
                await call.answer(f"💎 {tname} получил VIP!", show_alert=True)

        elif action in ("stickermute", "gifmute", "voicemute", "allmedmute"):
            if tid == 0:
                await call.answer("↩️ Реплай на юзера", show_alert=True); return
            perm_map = {
                "stickermute":  ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=False, can_add_web_page_previews=True),
                "gifmute":      ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=False, can_add_web_page_previews=True),
                "voicemute":    ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=True,  can_add_web_page_previews=True),
                "allmedmute":   ChatPermissions(can_send_messages=True, can_send_media_messages=False, can_send_polls=False, can_send_other_messages=False, can_add_web_page_previews=False),
            }
            label_map = {"stickermute":"🙅 Стикермут","gifmute":"🎭 Гифмут","voicemute":"🔇 Войсмут","allmedmute":"🚫 Всёмут"}
            await bot.restrict_chat_member(cid, tid, permissions=perm_map[action])
            await call.answer(f"{label_map[action]} применён к {tname}", show_alert=True)
            journal_add(cid, call.from_user.id, call.from_user.full_name, label_map[action], tid, tname)

    except Exception as e:
        await call.answer(f"⚠️ Ошибка: {e}", show_alert=True)
    await call.answer()

# ── Вспомогательные колбэки настроек панели ──────────────────
@dp.callback_query(F.data.startswith("panelset:"))
async def cb_panelset(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫", show_alert=True); return
    parts = call.data.split(":")
    action, cid_str = parts[1], parts[2]
    cid = int(cid_str)

    if action == "welcome" or action == "welcomeon":
        s = welcome_get(cid); s["enabled"] = True; welcome_save(cid, s)
        await call.answer("✅ Welcome включён")
    elif action == "welcomeoff":
        s = welcome_get(cid); s["enabled"] = False; welcome_save(cid, s)
        await call.answer("❌ Welcome выключен")
    elif action == "testwelcome":
        await send_welcome(cid, call.from_user, test=True)
        await call.answer("✅ Тест отправлен")
    elif action == "surveillance":
        enabled = surveillance_toggle(cid)
        await call.answer(f"👁 Наблюдение {'включено' if enabled else 'выключено'}")
    elif action == "autorules":
        if cid in auto_rules_chats: auto_rules_chats.discard(cid); await call.answer("❌ Авто-правила выкл")
        else: auto_rules_chats.add(cid); await call.answer("✅ Авто-правила вкл")
    elif action == "quarantine":
        if cid in quarantine_chats: quarantine_chats.discard(cid); await call.answer("❌ Карантин выкл")
        else: quarantine_chats.add(cid); await call.answer("✅ Карантин вкл")
    elif action == "lockdown":
        try:
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
            await call.answer("🔒 Чат закрыт")
        except Exception as e: await call.answer(f"❌ {e}", show_alert=True)
    elif action == "deletedlog":
        logs = surveillance_log_get(cid)
        if not logs: await call.answer("Лог пуст", show_alert=True); return
        lines2 = ["👁 Удалённые:\n"]
        for r2 in logs[:5]:
            dt = datetime.fromtimestamp(r2["ts"]).strftime("%d.%m %H:%M")
            lines2.append(f"{dt} {r2['name']}: {r2['text'][:40]}")
        await call.answer("\n".join(lines2), show_alert=True)
    elif action == "heatmap":
        hours = defaultdict(int)
        for u2 in hourly_stats[cid]:
            for h2, cnt in hourly_stats[cid][u2].items():
                hours[int(h2)] += cnt
        if not hours: await call.answer("Данных нет", show_alert=True); return
        mx = max(hours.values()) or 1
        blocks = ["▁","▂","▃","▄","▅","▆","▇","█"]
        bars = "".join(blocks[int(hours.get(h2,0)/mx*7)] for h2 in range(24))
        peak = max(hours, key=hours.get)
        await call.answer(f"📊 Активность:\n{bars}\nПик: {peak}:00", show_alert=True)
    elif action == "shifts":
        conn = db_connect()
        rows = conn.execute("SELECT * FROM mod_shifts WHERE cid=?", (cid,)).fetchall()
        conn.close()
        now_h = datetime.now().hour
        if not rows: await call.answer("Смен нет. /setshift", show_alert=True); return
        text = "\n".join(f"{'🟢' if r['start_hour']<=now_h<r['end_hour'] else '⚫'} {r['mod_name']} {r['start_hour']}–{r['end_hour']}ч" for r in rows)
        await call.answer(f"⏰ Смены:\n{text}", show_alert=True)
    elif action == "modrating":
        conn = db_connect()
        rows = conn.execute("SELECT mod_name,COUNT(*) as cnt FROM mod_journal WHERE cid=? GROUP BY mod_id ORDER BY cnt DESC LIMIT 5",(cid,)).fetchall()
        conn.close()
        if not rows: await call.answer("Статистики нет", show_alert=True); return
        text = "\n".join(f"{i+1}. {r['mod_name']}: {r['cnt']}" for i, r in enumerate(rows))
        await call.answer(f"📊 Рейтинг модов:\n{text}", show_alert=True)
    await call.answer()

# ── ПАНЕЛЬ ВЛАДЕЛЬЦА ──────────────────────────────────────────
@dp.callback_query(F.data.startswith("owner:"))
async def cb_owner_panel(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫 Только для владельца!", show_alert=True); return
    parts = call.data.split(":")
    action = parts[1]
    cid = call.message.chat.id

    if action == "chats":
        lines2 = [f"🌍 <b>Все чаты</b> ({len(known_chats)})\n"]
        for c2, t2 in list(known_chats.items())[:15]:
            w2 = sum(warnings[c2].values())
            r2 = sum(1 for r3 in report_queue.get(c2,[]) if r3.get("status")=="new")
            lines2.append(f"▸ <b>{t2}</b>\n  ⚡{w2} варн | 🚨{r2} реп")
        await call.message.edit_text("\n".join(lines2), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="owner:back:0")
            ]]))

    elif action == "status":
        import time as _tos
        uptime = int(_tos.time() - bot_start_time)
        h2, m2 = uptime // 3600, (uptime % 3600) // 60
        total_warns = sum(sum(warnings[c2].values()) for c2 in warnings)
        open_reps = sum(1 for c2 in report_queue for r2 in report_queue[c2] if r2.get("status")=="new")
        conn = db_connect()
        tasks_cnt = conn.execute("SELECT COUNT(*) as c FROM mod_tasks WHERE done=0").fetchone()["c"]
        conn.close()
        await call.answer(
            f"🤖 CHAT GUARD\n"
            f"⏱ Аптайм: {h2}ч {m2}м\n"
            f"💬 Чатов: {len(known_chats)}\n"
            f"⚡ Варнов: {total_warns}\n"
            f"🚨 Репортов: {open_reps}\n"
            f"🎯 Задач: {tasks_cnt}\n"
            f"🔒 Ч.список: {len(global_blacklist)}",
            show_alert=True)

    elif action == "sosall":
        locked = 0
        for c2 in list(known_chats.keys()):
            try: await bot.set_chat_permissions(c2, ChatPermissions(can_send_messages=False)); locked += 1; await asyncio.sleep(0.1)
            except: pass
        await call.answer(f"🚨 Локдаун {locked} чатов!", show_alert=True)
        await log_action(f"🚨 <b>SOS ALL</b> из панели\n👑 {call.from_user.full_name}")

    elif action == "sosoff":
        unlocked = 0
        for c2 in list(known_chats.keys()):
            try:
                await bot.set_chat_permissions(c2, ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=True,
                    can_add_web_page_previews=True, can_invite_users=True))
                unlocked += 1; await asyncio.sleep(0.1)
            except: pass
        await call.answer(f"✅ Разлокдаун {unlocked} чатов!", show_alert=True)

    elif action == "broadcast":
        pending[call.from_user.id] = {"action": "owner_broadcast", "chat_id": 0, "target_id": 0, "target_name": ""}
        await call.message.edit_text(
            "📢 <b>Бродкаст</b>\nНапиши текст — отправится во все чаты:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="owner:back:0")
            ]]))

    elif action == "blacklist":
        if not global_blacklist:
            await call.answer("🔒 Чёрный список пуст", show_alert=True); return
        text = f"🔒 Чёрный список ({len(global_blacklist)}):\n" + "\n".join(f"▸ {u}" for u in list(global_blacklist)[:10])
        await call.answer(text, show_alert=True)

    elif action == "backup":
        await call.answer("⏳ Создаю бэкап...", show_alert=False)
        import json as _js, io as _io
        bd = {
            "warnings": {str(c2):{str(u2):v for u2,v in d2.items()} for c2,d2 in warnings.items()},
            "known_chats": {str(k):v for k,v in known_chats.items()},
            "ban_list": {str(c2):list(v) for c2,v in ban_list.items()},
            "global_blacklist": list(global_blacklist),
        }
        buf = _io.BytesIO(_js.dumps(bd,ensure_ascii=False,indent=2).encode())
        from datetime import datetime as _dt3
        buf.name = f"backup_{_dt3.now().strftime('%d%m%Y_%H%M')}.json"
        await bot.send_document(OWNER_ID, buf, caption="💾 Бэкап из панели")
        await call.answer("✅ Бэкап отправлен в ЛС!")

    elif action == "audit":
        total_warns = sum(warnings[cid].values())
        bans = len(ban_list.get(cid, set()))
        open_reps = sum(1 for r2 in report_queue.get(cid,[]) if r2.get("status")=="new")
        await call.answer(
            f"🔍 Аудит {known_chats.get(cid,'?')}\n"
            f"⚡ Варнов: {total_warns}\n"
            f"🔨 Банов: {bans}\n"
            f"🚨 Репортов: {open_reps}\n"
            f"🎖 Ролей: {len(mod_roles.get(cid,{}))}", show_alert=True)

    elif action == "plugins":
        p = plugins[cid]
        rows2 = []
        for k, label in PLUGIN_LABELS.items():
            st = "✅" if p.get(k,True) else "❌"
            rows2.append([InlineKeyboardButton(text=f"{st} {label}", callback_data=f"plugin:toggle:{k}:{cid}")])
        rows2.append([InlineKeyboardButton(text="◀️ Назад", callback_data="owner:back:0")])
        await call.message.edit_text("🧩 <b>Плагины</b>:", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows2))

    elif action == "calendar":
        conn = db_connect()
        rows2 = conn.execute("SELECT action,target_name,ts FROM events_calendar WHERE cid=? ORDER BY ts DESC LIMIT 10",(cid,)).fetchall()
        conn.close()
        if not rows2: await call.answer("📅 Календарь пуст", show_alert=True); return
        text = "📅 События:\n" + "\n".join(
            f"{datetime.fromtimestamp(r2['ts']).strftime('%d.%m %H:%M')} {r2['action']} → {r2['target_name']}"
            for r2 in rows2)
        await call.answer(text[:200], show_alert=True)

    elif action == "tasks":
        conn = db_connect()
        rows2 = conn.execute("SELECT * FROM mod_tasks WHERE done=0 ORDER BY deadline LIMIT 8").fetchall()
        conn.close()
        if not rows2: await call.answer("✅ Задач нет", show_alert=True); return
        text = "🎯 Задачи:\n" + "\n".join(
            f"#{r2['id']} {r2['mod_name']}: {r2['task'][:30]}" for r2 in rows2)
        await call.answer(text, show_alert=True)

    elif action == "modrating":
        conn = db_connect()
        rows2 = conn.execute("SELECT mod_name,COUNT(*) as cnt FROM mod_journal GROUP BY mod_id ORDER BY cnt DESC LIMIT 5").fetchall()
        conn.close()
        if not rows2: await call.answer("Статистики нет", show_alert=True); return
        text = "📊 Рейтинг:\n" + "\n".join(f"{i+1}. {r2['mod_name']}: {r2['cnt']}" for i,r2 in enumerate(rows2))
        await call.answer(text, show_alert=True)

    elif action == "evacuation":
        import time as _tev2
        now2 = _tev2.time()
        new_users = [u2 for u2,ts2 in last_seen.get(cid,{}).items() if now2-ts2 < 3600]
        kicked2 = 0
        for u2 in new_users:
            if await is_admin_by_id(cid, u2): continue
            try: await bot.ban_chat_member(cid,u2); await bot.unban_chat_member(cid,u2); kicked2+=1; await asyncio.sleep(0.05)
            except: pass
        await call.answer(f"🚁 Эвакуация: {kicked2} удалено", show_alert=True)

    elif action == "quarantine":
        if cid in quarantine_chats: quarantine_chats.discard(cid); await call.answer("❌ Карантин выкл")
        else: quarantine_chats.add(cid); await call.answer("✅ Карантин вкл")

    elif action == "cleanup":
        await call.answer("⏳ Используй /cleanup в чате для подтверждения", show_alert=True)

    elif action == "linkchats":
        linked = list(known_chats.keys())
        linked_chats_bans["owner"] = linked
        save_data()
        await call.answer(f"🔗 Связано {len(linked)} чатов!", show_alert=True)

    elif action == "back":
        import time as _tp3
        uptime2 = int(_tp3.time() - bot_start_time)
        h3, m3 = uptime2//3600, (uptime2%3600)//60
        total_warns2 = sum(sum(warnings[c2].values()) for c2 in warnings)
        open_reps2 = sum(1 for c2 in report_queue for r2 in report_queue[c2] if r2.get("status")=="new")
        await call.message.edit_text(
            f"👑 <b>Панель владельца</b>\n"
            f"🤖 Бот: <b>CHAT GUARD</b>\n"
            f"⏱ Аптайм: <b>{h3}ч {m3}м</b>\n"
            f"💬 Чатов: <b>{len(known_chats)}</b>\n"
            f"⚡ Варнов: <b>{total_warns2}</b>\n"
            f"🚨 Репортов: <b>{open_reps2}</b>\n"
            f"🔒 Чёрный список: <b>{len(global_blacklist)}</b>\n"
            f"",
            parse_mode="HTML", reply_markup=kb_owner_panel())

    await call.answer()

@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
        permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(minutes=mins))
    await call.message.edit_text(
        random.choice(MUTE_MESSAGES).format(name=f"<b>{tname}</b>", time=label), parse_mode="HTML")
    await log_action(f"🔇 <b>МУТ</b>\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n⏱ <b>Время:</b> {label}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.answer(f"Замутен на {label}!")

@dp.callback_query(F.data.startswith("warn:"))
async def cb_warn(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
        await log_action(f"🔨 <b>Автоматическая блокировка</b>\n\n🤖 <b>Причина:</b> {MAX_WARNINGS} варнов — лимит\n🎯 <b>Кого:</b> <b>{tname}</b>\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=f"<b>{tname}</b>", count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"⚠️ <b>Предупреждение</b>\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.message.edit_text(msg, parse_mode="HTML")
    asyncio.create_task(schedule_delete(call.message))
    await call.answer("Варн выдан!")

@dp.callback_query(F.data.startswith("ban:"))
async def cb_ban(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
    parts = call.data.split(":", 2); tid = int(parts[1]); reason = parts[2]; cid = call.message.chat.id
    try: tm = await bot.get_chat_member(cid, tid); tname = tm.user.full_name
    except: tname = f"ID {tid}"
    if reason == "custom":
        pending[call.from_user.id] = {"action":"ban_custom","target_id":tid,"target_name":tname,"chat_id":cid}
        await call.message.edit_text(f"✏️ Напиши причину бана для <b>{tname}</b>:", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
        await call.answer(); return
    if reason == "tempban24":
        await bot.ban_chat_member(cid, tid, until_date=datetime.now() + timedelta(hours=24))
        await call.message.edit_text(f"⏰ <b>{tname}</b> забанен на <b>24 часа</b>.", parse_mode="HTML")
        asyncio.create_task(auto_delete(call.message))
        await log_action(f"👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n⏱ <b>Время:</b> 24 часа\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        await call.answer(); return
    await bot.ban_chat_member(cid, tid)
    await call.message.edit_text(
        random.choice(BAN_MESSAGES).format(name=f"<b>{tname}</b>", reason=reason), parse_mode="HTML")
    await log_action(f"🔨 <b>БАН</b>\n\n👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.answer("Забанен!")

@dp.callback_query(F.data.startswith("fun:"))
async def cb_fun(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
                permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(hours=24))
            await call.message.edit_text(
                f"📵 <b>{tname}</b> — мут на <b>24 часа</b> за рекламу.", parse_mode="HTML")
            asyncio.create_task(schedule_delete(call.message))
            await log_action(f"👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кого:</b> <b>{tname}</b>\n⏱ <b>Время:</b> 24 часа\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
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
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
        await call.answer("🔒 Чат переведён в режим только для чтения.", show_alert=True)
    elif action == "unlock":
        await bot.set_chat_permissions(cid, ChatPermissions(
            can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
            can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
        await call.answer("🔓 Ограничения сняты. Чат открыт.", show_alert=True)
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
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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

@dp.callback_query(F.data == "start:ticket")
async def cb_start_ticket(call: CallbackQuery):
    chats = await db.get_all_chats()
    if not chats:
        chat_list = [(cid, title) for cid, title in known_chats.items()]
    else:
        chat_list = [(r["cid"], r["title"]) for r in chats]
    await tkt.cmd_ticket(call.message, bot, chat_list)
    await call.answer()

@dp.callback_query(F.data == "start:help")
async def cb_start_help(call: CallbackQuery):
    await call.message.answer(
        "👤 <b>Профиль</b>\n"
        "/myprofile — мой профиль\n"
        "/card — профиль-карточка\n"
        "/setbio — установить био\n\n"
        "📊 <b>Статистика</b>\n"
        "/top — топ активных\n"
        "/toprep — топ репутации\n"
        "/mystats — моя статистика\n\n"
        "💰 <b>Экономика</b>\n"
        "/daily — ежедневный бонус\n"
        "/shop2 — магазин\n"
        "/auction — аукцион\n\n"
        "👥 <b>Социалка</b>\n"
        "/addfriend — добавить в друзья\n"
        "/propose — предложить отношения\n"
        "/anonmsg — анонимное сообщение\n"
        "/anonbox — анонимный ящик\n\n"
        "🎮 <b>Игры (аутист)</b>\n"
        "аутист обозвать @юзер\n"
        "аутист поженить @юзер\n"
        "аутист казнить @юзер\n"
        "аутист диагноз @юзер\n"
        "аутист дуэль @юзер\n\n"
        "🚨 <b>Жалобы</b>\n"
        "/report — пожаловаться\n"
        "/ticket — написать в поддержку\n"
        "/appeal — апелляция\n\n"
        "📋 Полный список: /help",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Политика бота", url="https://telegra.ph/politika-bota-03-15")],
            [InlineKeyboardButton(text="Открыть тикет", callback_data="tkt:new")],
        ])
    )
    await call.answer()

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    await reply_auto_delete(message,
        "📋 <b>Правила сообщества</b>\n\nОзнакомьтесь с правилами, нажав кнопку ниже.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Читать правила", url="https://telegra.ph/Pravila-soobshchestva-03-13-6")]
        ])
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_adm  = await check_admin(message)
    is_owner = message.from_user.id == OWNER_ID

    # ── ПОЛЬЗОВАТЕЛЬ ──────────────────────────────────────
    text_user = (
        "╔══════════════════════╗\n"
        "║   🛡  CHAT GUARD     ║\n"
        "╚══════════════════════╝\n\n"

        "👤 <b>Профиль</b>\n"
        "├ /me — моя карточка\n"
        "├ /myprofile — полный профиль\n"
        "├ /setbio текст — установить био\n"
        "├ /setmood — настроение\n"
        "├ /rank — мой уровень и XP\n"
        "├ /top — топ чата по активности\n"
        "├ /stats — статистика чата\n"
        "└ /daily — ежедневный бонус XP\n\n"

        "👥 <b>Социальное</b>\n"
        "├ /addfriend — добавить друга <i>(реплай)</i>\n"
        "├ /friends — список друзей\n"
        "├ /unfriend — удалить из друзей <i>(реплай)</i>\n"
        "├ /propose — предложить отношения <i>(реплай)</i>\n"
        "├ /breakup — завершить отношения\n"
        "├ /anonmsg текст — анонимное сообщение <i>(реплай)</i>\n"
        "├ /follow — подписаться <i>(реплай)</i>\n"
        "└ /followers — мои подписчики\n\n"

        "🎮 <b>Игры и развлечения</b>\n"
        "├ /roll [N] — бросить кубик\n"
        "├ /flip — монетка орёл/решка\n"
        "├ /rps к/н/б — камень ножницы бумага\n"
        "├ /ship — совместимость <i>(реплай или /ship Имя1 Имя2)</i>\n"
        "├ /rate текст — оценить от 0 до 10\n"
        "├ /8ball вопрос — магический шар\n"
        "├ /choose вар1|вар2|вар3 — выбор\n"
        "├ /truth — правда или действие (вопрос)\n"
        "├ /dare — правда или действие (задание)\n"
        "├ /wyr — что бы ты выбрал?\n"
        "├ /iq — проверка IQ <i>(реплай или себе)</i>\n"
        "├ /совместимость — совместимость <i>(реплай)</i>\n"
        "└ /coinflip — подбросить монетку\n\n"

        "🔮 <b>Гадания и судьба</b>\n"
        "├ /horoscope — случайный гороскоп\n"
        "├ /zodiac — гороскоп по знаку зодиака\n"
        "├ /predict — предсказание <i>(реплай или себе)</i>\n"
        "├ /fortune — предсказание судьбы\n"
        "└ /ask вопрос — ответ вселенной\n\n"

        "🛠 <b>Утилиты</b>\n"
        "├ /calc выражение — калькулятор\n"
        "├ /password [длина] — генератор паролей\n"
        "├ /qr текст — создать QR-код\n"
        "├ /mock текст — СтИлЬ МоКиНгА\n"
        "├ /reverse текст — текст задом наперёд\n"
        "├ /count текст — подсчёт символов\n"
        "├ /tr [язык] — перевести <i>(реплай)</i>\n"
        "├ /music название — найти трек\n"
        "├ /imagine описание — сгенерировать картинку\n"
        "├ /weather город — погода\n"
        "├ /afk [текст] — статус отсутствия\n"
        "├ /remind 30m текст — напоминание\n"
        "└ /rules — правила чата\n\n"

        "🏰 <b>Кланы</b>\n"
        "├ /clan — информация о клане\n"
        "├ /clan_create ТЕГ Название — создать клан\n"
        "├ /clan_join ТЕГ — вступить в клан\n"
        "└ /clan_leave — покинуть клан\n\n"

        "📨 <b>Жалобы и апелляции</b>\n"
        "├ /report — пожаловаться <i>(реплай)</i>\n"
        "└ /appeal причина — апелляция на бан\n\n"

        "🔒 <b>Приватность</b>\n"
        "├ /privacy — политика конфиденциальности\n"
        "└ /deleteme — удалить свои данные\n\n"

        "──────────────────────\n"
        "💡 <i>Команды «аутист»: аутист поженить / казнить / диагноз / профессия / обозвать / дуэль / похитить / подарить 🌹</i>"
    )

    # ── АДМИНИСТРАТОР ─────────────────────────────────────
    text_admin = (
        "\n\n"
        "╔══════════════════════╗\n"
        "║   👮  АДМИНИСТРАТОР  ║\n"
        "╚══════════════════════╝\n\n"

        "⚖️ <b>Модерация</b>\n"
        "├ /warn · /unwarn — варн / снять <i>(реплай)</i>\n"
        "├ /ban · /unban — бан / разбан <i>(реплай)</i>\n"
        "├ /mute · /unmute — мут / размут <i>(реплай)</i>\n"
        "├ /tempban 3 причина — временный бан <i>(реплай)</i>\n"
        "├ /banid ID причина — бан по ID\n"
        "├ /muteid ID 60m — мут по ID\n"
        "├ /kick — кик <i>(реплай)</i>\n"
        "├ /warn24 — предупреждение с автоснятием <i>(реплай)</i>\n"
        "└ /warnmenu — шаблоны варнов <i>(реплай)</i>\n\n"

        "📋 <b>Информация</b>\n"
        "├ /info — досье пользователя <i>(реплай)</i>\n"
        "├ /warnings — варны <i>(реплай)</i>\n"
        "├ /modhistory — история нарушений <i>(реплай)</i>\n"
        "├ /banlist — список банов\n"
        "├ /adminlist — список администраторов\n"
        "├ /modlog [N] — журнал модерации\n"
        "├ /modtop — рейтинг модераторов\n"
        "└ /violators — топ нарушителей\n\n"

        "🚨 <b>Алерты и инциденты</b>\n"
        "├ /alerts — текущие алерты\n"
        "├ /alertsclear — очистить алерты\n"
        "└ /incidents — критические инциденты\n\n"

        "📨 <b>Апелляции</b>\n"
        "├ /appeals — список ожидающих апелляций\n"
        "├ /appealapprove ID — одобрить апелляцию\n"
        "└ /appealdeny ID причина — отклонить\n\n"

        "⚙️ <b>Управление чатом</b>\n"
        "├ /chatsettings — настройки чата (инлайн-меню)\n"
        "├ /lock · /unlock — закрыть / открыть чат\n"
        "├ /slowmode N — режим замедления\n"
        "├ /del — удалить сообщение <i>(реплай)</i>\n"
        "├ /clear N — удалить N последних\n"
        "├ /pin заголовок — закрепить <i>(реплай)</i>\n"
        "├ /pinmanager — менеджер закреплённых\n"
        "├ /announce текст — объявление\n"
        "├ /poll вопрос|вар1|вар2 — голосование\n"
        "├ /antimat вкл/выкл — фильтр мата\n"
        "├ /autokick — автокик ботов\n"
        "├ /setwelcome — настроить приветствие\n"
        "├ /welcomeon · /welcomeoff — вкл/выкл\n"
        "├ /surveillance — режим наблюдения\n"
        "└ /deletedlog — лог удалённых сообщений\n\n"

        "📊 <b>Аналитика</b>\n"
        "├ /botstats — общая статистика бота\n"
        "├ /heatmap — тепловая карта активности\n"
        "├ /calendar — события чата\n"
        "└ /vip @user — выдать VIP статус <i>(реплай)</i>\n\n"

        "⚡ <b>Быстрые инструменты</b>\n"
        "├ /addreply ключ текст — шаблон ответа\n"
        "├ /replies — список шаблонов\n"
        "├ !ключ — отправить шаблон\n"
        "├ /setq 1 текст — горячая команда\n"
        "├ /q1 /q2 /q3 — отправить горячую команду\n"
        "├ /note set/get/del — заметки чата\n"
        "└ /modexport — экспорт истории модерации\n\n"

        "🔧 <b>Тех. работы</b>\n"
        "├ /techwork [время] — запустить тех. работы\n"
        "└ /techstatus — статус тех. работ"
    )

    # ── ВЛАДЕЛЕЦ ──────────────────────────────────────────
    text_owner = (
        "\n\n"
        "╔══════════════════════╗\n"
        "║   👑  ВЛАДЕЛЕЦ       ║\n"
        "╚══════════════════════╝\n\n"

        "📢 <b>Рассылка</b>\n"
        "└ /broadcast текст — рассылка во все чаты\n\n"

        "🚨 <b>Экстренные меры</b>\n"
        "├ /evacuation — кик новых участников за 1ч\n"
        "├ /quarantine — автомут новых на 24ч\n"
        "└ /cleanup — удалить неактивных\n\n"

        "🛡 <b>Безопасность</b>\n"
        "├ /blacklist ID — глобальный чёрный список\n"
        "├ /unblacklist ID — убрать из ЧС\n"
        "├ /giverole @user роль — выдать роль мода\n"
        "├ /takerole @user — забрать роль\n"
        "└ /setperm команда роль — права доступа\n\n"

        "💾 <b>База данных</b>\n"
        "├ /backupnow — создать бэкап прямо сейчас\n"
        "└ /restoredb — восстановить <i>(реплай на .db файл)</i>\n\n"

        "👮 <b>Управление модераторами</b>\n"
        "├ /task текст [часы] — поставить задачу\n"
        "├ /tasks — все задачи\n"
        "├ /setshift 9 21 — назначить смену\n"
        "└ /createqr [XP] — создать QR-код\n\n"

        "🎭 <b>Команды «аутист» (владелец)</b>\n"
        "├ аутист ядерка / молния / взрыв / хаос\n"
        "├ аутист локдаун / локдаун выкл\n"
        "├ аутист тишина 10м\n"
        "└ аутист корона / анонс / сос / рестарт\n\n"

        "──────────────────────\n"
        "⚙️ <i>/panel — панель управления | /ownersettings — настройки бота</i>"
    )

    if is_owner:
        full = text_user + text_admin + text_owner
        chunks = [full[i:i+4000] for i in range(0, len(full), 4000)]
        for chunk in chunks:
            try: await bot.send_message(OWNER_ID, chunk, parse_mode="HTML")
            except: pass
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  📬  СПРАВКА      ║\n╚══════════════════╝\n\n"
            "<i>Полная справка отправлена в личные сообщения.</i>", parse_mode="HTML")
    elif is_adm:
        full = text_user + text_admin
        chunks = [full[i:i+4000] for i in range(0, len(full), 4000)]
        for chunk in chunks:
            try: await bot.send_message(message.from_user.id, chunk, parse_mode="HTML")
            except: pass
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  📬  СПРАВКА      ║\n╚══════════════════╝\n\n"
            "<i>Справка отправлена в личные сообщения.</i>", parse_mode="HTML")
    else:
        await reply_auto_delete(message, text_user, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Политика бота", url="https://telegra.ph/politika-bota-03-15")],
                [InlineKeyboardButton(text="🎫 Открыть тикет", callback_data="tkt:new")],
                [InlineKeyboardButton(text="📨 Подать апелляцию", callback_data="appeal:new")],
            ])
        )

@dp.message(Command("panel"))
async def cmd_panel(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    uid = message.from_user.id

    # Владелец видит свою панель
    if uid == OWNER_ID:
        import time as _tp2
        uptime = int(_tp2.time() - bot_start_time)
        h, m = uptime // 3600, (uptime % 3600) // 60
        total_warns = sum(sum(warnings[c].values()) for c in warnings)
        open_reps = sum(1 for c in report_queue for r in report_queue[c] if r.get("status") == "new")
        await reply_auto_delete(message,
            f"👑 <b>Панель владельца</b>\n"
            f"<i>CHAT GUARD · Управление системой</i>\n\n"
            f"· Аптайм: <b>{h}ч {m}м</b>\n"
            f"· Активных чатов: <b>{len(known_chats)}</b>\n"
            f"· Предупреждений: <b>{total_warns}</b>\n"
            f"· Открытых репортов: <b>{open_reps}</b>\n"
            f"· Чёрный список: <b>{len(global_blacklist)}</b>",
            parse_mode="HTML",
            reply_markup=kb_owner_panel())
        return

    if message.reply_to_message:
        target = message.reply_to_message.from_user
        warns  = warnings[cid].get(target.id, 0)
        xp     = xp_data[cid].get(target.id, 0)
        msgs   = chat_stats[cid].get(target.id, 0)
        role   = mod_roles.get(cid, {}).get(target.id, "")
        role_label = MOD_ROLE_LABELS.get(role, "") if role else ""
        vip_badge = "💎 VIP-участник\n" if is_vip(target.id, cid) else ""
        await reply_auto_delete(message,
            f"👤 <b>Карточка участника</b>\n\n"
            f"{vip_badge}"
            f"{target.mention_html()}\n"
            f"<code>{target.id}</code>\n\n"
            f"· Предупреждений: <b>{warns}/{MAX_WARNINGS}</b>\n"
            f"· Очки опыта: <b>{xp} XP</b>\n"
            f"· Сообщений: <b>{msgs}</b>\n"
            f"{role_label}",
            parse_mode="HTML",
            reply_markup=kb_user_panel(target.id))
    else:
        total_msgs  = sum(chat_stats[cid].values())
        total_warns = sum(warnings[cid].values())
        open_reps   = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
        await reply_auto_delete(message,
            f"🛡 <b>Панель администратора</b>\n"
            f"<i>{message.chat.title}</i>\n\n"
            f"· Активных участников: <b>{len(chat_stats[cid])}</b>\n"
            f"· Сообщений: <b>{total_msgs}</b>\n"
            f"· Предупреждений: <b>{total_warns}</b>\n"
            f"· Открытых репортов: <b>{open_reps}</b>\n"
            f"· Плагины: <b>{sum(1 for v in plugins[cid].values() if v)}/{len(PLUGIN_LABELS)}</b>\n"
            f"· Welcome: <b>{'вкл' if welcome_get(cid)['enabled'] else 'выкл'}</b>\n"
            f"· Наблюдение: <b>{'вкл' if surveillance_enabled(cid) else 'выкл'}</b>",
            parse_mode="HTML",
            reply_markup=kb_main_menu())

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "⛔ Невозможно применить действие к администратору."); return
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
    if dm_ok: reply += "\n<i>Пользователь уведомлён в личные сообщения.</i>"
    await reply_auto_delete(message, reply, parse_mode="HTML")
    await log_action(f"🔨 <b>БАН</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    add_mod_history(cid, target.id, "🔨 Бан", reason, message.from_user.full_name)
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
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    add_mod_history(message.chat.id, target.id, "🕊 Разбан", "—", message.from_user.full_name)
    ban_list[message.chat.id].pop(target.id, None); save_data()
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🕊  РАЗ БАН      ║\n╚══════════════════╝\n\n👤 Цель: {target.mention_html()}\n──────────────────\n<i>Блокировка снята.</i>", parse_mode="HTML")
    await log_action(f"🕊️ <b>Разбан</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "⛔ Невозможно применить действие к администратору."); return
    mins, label = parse_duration(command.args or "60m")
    if not mins: mins = 60; label = "1 ч."
    cid = message.chat.id
    await bot.restrict_chat_member(cid, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(minutes=mins))
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
    if dm_ok: reply += "\n<i>Пользователь уведомлён в личные сообщения.</i>"
    await reply_auto_delete(message, reply, parse_mode="HTML")
    await log_action(f"🔇 <b>МУТ</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n⏱ <b>Время:</b> {label}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    add_mod_history(cid, target.id, f"🔇 Мут {label}", command.args or "—", message.from_user.full_name)
    mod_reasons[cid][target.id]["mute"] = f"{label} — {command.args or 'Нарушение правил'}"
    # 🔄 Запуск автоснятия мута
    schedule_unmute(cid, target.id, mins, target.full_name)

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
            can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
    add_mod_history(message.chat.id, target.id, "🔊 Размут", "—", message.from_user.full_name)
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🔊  РАЗМУТ       ║\n╚══════════════════╝\n\n👤 Цель: {target.mention_html()}\n──────────────────\n<i>Голос восстановлен.</i>", parse_mode="HTML")
    await log_action(f"🔊 <b>Размут</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("warn"))
async def cmd_warn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "⛔ Невозможно выдать предупреждение администратору."); return
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
        await log_action(f"🔨 <b>Автоматическая блокировка</b>\n\n🤖 <b>Причина:</b> {MAX_WARNINGS} варнов — лимит достигнут\n🎯 <b>Кого:</b> {target.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        add_mod_history(cid, target.id, "🔨 Автобан", f"{MAX_WARNINGS} варнов", message.from_user.full_name)
        # 📨 ЛС нарушителю
        await dm_warn_user(target.id, target.full_name, f"{MAX_WARNINGS} варнов — автобан",
                           message.chat.title, "🔨 Бан", message.from_user.full_name)
    else:
        msg = random.choice(WARN_MESSAGES).format(
            name=target.mention_html(), count=count, max=MAX_WARNINGS, reason=reason)
        await log_action(f"⚠️ <b>Предупреждение</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n📝 <b>Причина:</b> {reason}\n⚠️ <b>Варнов:</b> {warnings[message.chat.id][target.id]}/{MAX_WARNINGS}\n⏳ <b>Сгорит через:</b> {WARN_EXPIRY_DAYS} дн.\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        add_mod_history(cid, target.id, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, message.from_user.full_name)
        # 📨 ЛС нарушителю
        dm_ok = await dm_warn_user(target.id, target.full_name, reason,
                                    message.chat.title, f"⚡ Варн {count}/{MAX_WARNINGS}",
                                    message.from_user.full_name)
        if dm_ok:
            msg += "\n<i>Пользователь уведомлён в личные сообщения.</i>"
    await reply_auto_delete(message, msg, parse_mode="HTML")

@dp.message(Command("unwarn"))
async def cmd_unwarn(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1; save_data()
    add_mod_history(cid, target.id, "🌿 Снят варн", "—", message.from_user.full_name)
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  🌿  ВАРН СНЯТ    ║\n╚══════════════════╝\n\n👤 Цель: {target.mention_html()}\n⚡ Остаток: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>\n──────────────────\n<i>Предупреждение аннулировано.</i>",
        parse_mode="HTML")

@dp.message(Command("del"))
async def cmd_del(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    try: await message.reply_to_message.delete()
    except: pass
    try: await message.delete()
    except: pass

@dp.message(Command("clear"))
async def cmd_clear(message: Message, command: CommandObject):
    if not await require_admin(message): return
    try: count = min(int(command.args or 10), 50)
    except ValueError: await reply_auto_delete(message, "⚠️ Использование: /clear [кол-во]"); return
    deleted = 0
    for i in range(message.message_id, message.message_id - count - 1, -1):
        try: await bot.delete_message(message.chat.id, i); deleted += 1
        except: pass
    sent = await message.answer(f"🗑️ Удалено сообщений: <b>{deleted}</b>.", parse_mode="HTML")
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
        f"📢 <b>Объявление</b>\n\n{command.args}\n\n— {message.from_user.mention_html()}", parse_mode="HTML")

@dp.message(Command("unpin"))
async def cmd_unpin(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    await bot.unpin_chat_message(message.chat.id, message.reply_to_message.message_id)
    await reply_auto_delete(message, "📍 Откреплено!")

@dp.message(Command("lock"))
async def cmd_lock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
    await reply_auto_delete(message, "╔══════════════════╗\n║  🔒  ЧАТ ЗАКРЫТ  ║\n╚══════════════════╝\n\n<i>Отправка сообщений запрещена.</i>", parse_mode="HTML")
    await log_action(f"👤 <b>Кто:</b> {message.from_user.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not await require_admin(message): return
    await bot.set_chat_permissions(message.chat.id, ChatPermissions(
        can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
        can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
    await reply_auto_delete(message, "╔══════════════════╗\n║  🔓  ЧАТ ОТКРЫТ  ║\n╚══════════════════╝\n\n<i>Отправка сообщений разрешена.</i>", parse_mode="HTML")
    await log_action(f"👤 <b>Кто:</b> {message.from_user.mention_html()}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

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
            await reply_auto_delete(message, "╔══════════════════╗\n║  🐢  СЛОУМОД      ║\n╚══════════════════╝\n\n<i>Режим замедления отключён.</i>", parse_mode="HTML")
        else:
            label = f"{delay} сек." if delay < 60 else f"{delay//60} мин. {delay%60} сек." if delay%60 else f"{delay//60} мин."
            await reply_auto_delete(message, f"╔══════════════════╗\n║  🐢  СЛОУМОД      ║\n╚══════════════════╝\n\n⏱ Задержка: <b>{label}</b>\n──────────────────\n<i>Режим замедления активирован.</i>", parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message, f"⚠️ Не удалось установить slowmode: <code>{e}</code>", parse_mode="HTML")

@dp.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
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
        await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
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
    if a == "on": ANTI_MAT_ENABLED = True; await reply_auto_delete(message, "✅ Фильтр нецензурной лексики <b>включён</b>.", parse_mode="HTML")
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
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    if await is_admin_by_id(message.chat.id, target.id):
        await reply_auto_delete(message, "⛔ Невозможно применить действие к администратору."); return
    await bot.restrict_chat_member(message.chat.id, target.id,
        permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(hours=24))
    await reply_auto_delete(message, f"📵 {target.mention_html()} замучен на <b>24 часа</b> за рекламу.", parse_mode="HTML")
    await log_action(f"👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {target.mention_html()}\n⏱ <b>Время:</b> 24 часа\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")

@dp.message(Command("rban"))
async def cmd_rban(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    await reply_auto_delete(message, 
        f"🎲 {target.mention_html()} получил <b>шуточный бан</b>!\n"
        f"📝 Причина: {random.choice(RANDOM_BAN_REASONS)} 😄\n<i>(реального бана нет)</i>",
        parse_mode="HTML")

@dp.message(Command("adminlist"))
async def cmd_adminlist(message: Message):
    if not await require_admin(message): return
    admins = await bot.get_chat_administrators(message.chat.id)
    lines  = ["╔══════════════════╗\n║  👮  АДМИНИСТРАТОРЫ ║\n╚══════════════════╝\n"]
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
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    user = message.reply_to_message.from_user
    member = await bot.get_chat_member(message.chat.id, user.id)
    smap = {"creator":"👑 Создатель","administrator":"🛡 Администратор","member":"👤 Участник",
            "restricted":"🔇 Ограничен","kicked":"🔨 Забанен","left":"🚶 Вышел"}
    afk  = f"\n😴 AFK: {afk_users[user.id]}" if user.id in afk_users else ""
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  🔍  ДОСЬЕ        ║\n╚══════════════════╝\n\n"
        f"👤 {user.mention_html()}{afk}\n"
        f"🔗 {'@'+user.username if user.username else 'нет'}\n"
        f"🪪 <code>{user.id}</code>\n"
        f"📌 {smap.get(member.status, member.status)}\n"
        f"──────────────────\n"
        f"⚡ Варнов: <b>{warnings[message.chat.id].get(user.id,0)}/{MAX_WARNINGS}</b>\n"
        f"🌟 Репутация: <b>{reputation[message.chat.id].get(user.id,0):+d}</b>\n"
        f"💬 Сообщений: <b>{chat_stats[message.chat.id].get(user.id,0)}</b>\n"
        f"──────────────────",
        parse_mode="HTML")

@dp.message(Command("warnings"))
async def cmd_warnings(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  ⚡  ПРЕДУПРЕЖДЕНИЯ ║\n╚══════════════════╝\n\n👤 Цель: {target.mention_html()}\n⚡ Варнов: <b>{warnings[message.chat.id].get(target.id,0)}/{MAX_WARNINGS}</b>\n──────────────────",
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
        f"╔══════════════════╗\n║  🔇  ТЕМПБАН      ║\n╚══════════════════╝\n\n"
        f"👤 Цель: {target.mention_html()}\n"
        f"⏱ Срок: <b>{days} дн.</b>\n"
        f"📝 Причина: {reason}\n"
        f"──────────────────\n"
        f"<i>🔓 Автоматический разбан через {days} дн.</i>", parse_mode="HTML")
    await log_action(
        f"🔇 <b>Темпбан</b>\nКто: {message.from_user.mention_html()}\n"
        f"Кого: {target.mention_html()}\nСрок: {days} дн.\nПричина: {reason}\nЧат: {message.chat.title}")

@dp.message(Command("banlist"))
async def cmd_banlist(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    bans = ban_list[cid]
    if not bans:
        await reply_auto_delete(message, "👥 Список забаненных пуст."); return
    lines = [f"╔══════════════════╗\n║  🔨  БАН-ЛИСТ     ║\n╚══════════════════╝\n\n<b>Всего забанено: {len(bans)}</b>\n"]
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
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
        await log_action(f"🎯 <b>Кого:</b> <code>{target_id}</code>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    else:
        await call.message.edit_text(
            f"⚡ <b>Варн выдан!</b> {tmpl['label']}\n"
            f"📝 {reason}\n"
            f"⚠️ Варнов: <b>{count}/{MAX_WARNINGS}</b>",
            parse_mode="HTML")
        asyncio.create_task(schedule_delete(call.message))
        await log_action(f"👤 <b>Кто:</b> {call.from_user.mention_html()}\n🎯 <b>Кому:</b> <code>{target_id}</code>\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {call.message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
    await call.answer(f"✅ {tmpl['label']}")

async def cmd_rep(message: Message):
    if not message.reply_to_message: await reply_auto_delete(message, "⚠️ Ответьте на сообщение нарушителя."); return
    target = message.reply_to_message.from_user
    score  = reputation[message.chat.id].get(target.id, 0)
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  {'🌟' if score>=0 else '💀'}  РЕПУТАЦИЯ    ║\n╚══════════════════╝\n\n👤 {target.mention_html()}\n📈 Счёт: <b>{score:+d}</b>\n──────────────────",
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
        f"╔══════════════════╗\n║  ⬆️  РЕПУТАЦИЯ +1  ║\n╚══════════════════╝\n\n👤 {target.mention_html()}\n📈 Итого: <b>{reputation[message.chat.id][target.id]:+d}</b>\n──────────────────",
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
        f"╔══════════════════╗\n║  ⬇️  РЕПУТАЦИЯ -1  ║\n╚══════════════════╝\n\n👤 {target.mention_html()}\n📉 Итого: <b>{reputation[message.chat.id][target.id]:+d}</b>\n──────────────────",
        parse_mode="HTML")

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
        f"╔══════════════════╗\n║  📊  ПРОФИЛЬ      ║\n╚══════════════════╝\n\n👤 {user.mention_html()}\n🏅 Титул: <b>{title}</b>\n⚡ Уровень: <b>{lvl}</b>\n✨ Опыт: <b>{xp_current}/100</b>\n[{bar}]\n──────────────────", parse_mode="HTML")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    stats = chat_stats[message.chat.id]
    if not stats: await reply_auto_delete(message, "📊 Статистика пока пуста!"); return
    sorted_u = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = ["╔══════════════════╗\n║  🏆  ТОП ЧАТА     ║\n╚══════════════════╝\n"]
    for i, (uid, count) in enumerate(sorted_u):
        try: m = await bot.get_chat_member(message.chat.id, uid); uname = m.user.full_name
        except: uname = f"ID {uid}"
        lines.append(f"{medals[i]} <b>{uname}</b> — {count} сообщений")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

async def cmd_roll(message: Message, command: CommandObject):
    try: sides = max(2, min(int(command.args or 6), 10000))
    except: sides = 6
    result = random.randint(1, sides)
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🎲  БРОСОК       ║\n╚══════════════════╝\n\nd{sides} → <b>{result}</b>\n──────────────────", parse_mode="HTML")

async def cmd_flip(message: Message):
    result = random.choice(["🦅 Орёл", "🪙 Решка"])
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🪙  МОНЕТКА      ║\n╚══════════════════╝\n\nРезультат: <b>{result}</b>\n──────────────────", parse_mode="HTML")

async def cmd_8ball(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "❓ /8ball [вопрос]"); return
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  🎱  ШАР СУДЬБЫ   ║\n╚══════════════════╝\n\n❓ {command.args}\n──────────────────\n{random.choice(BALL_ANSWERS)}", parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message, command: CommandObject):
    if not command.args: await reply_auto_delete(message, "⚠️ /rate [что]"); return
    score = random.randint(0, 10)
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  ⭐  ОЦЕНКА       ║\n╚══════════════════╝\n\n📌 {command.args}\n{'🌟'*score+'☆'*(10-score)}\n<b>{score}/10</b>\n──────────────────", parse_mode="HTML")

@dp.message(Command("iq"))
async def cmd_iq(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    iq   = random.randint(1, 200)
    c    = "🥔 Картошка умнее." if iq<70 else ("🐒 Обезьяна лучше." if iq<100 else ("😐 Сойдёт." if iq<130 else ("🧠 Умный!" if iq<160 else "🤖 Настоящий гений!")))
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🧠  IQ ТЕСТ      ║\n╚══════════════════╝\n\n👤 {user.mention_html()}\n📊 IQ: <b>{iq}</b>\n──────────────────\n{c}", parse_mode="HTML")

@dp.message(Command("gay"))
async def cmd_gay(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    p    = random.randint(0, 100)
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  🌈  РАДАР        ║\n╚══════════════════╝\n\n👤 {user.mention_html()}\n{'🟣'*(p//10)+'⬜'*(10-p//10)} <b>{p}%</b>\n──────────────────\n<i>это шутка 😄</i>",
        parse_mode="HTML")

@dp.message(Command("truth"))
async def cmd_truth(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  🤔  ПРАВДА       ║\n╚══════════════════╝\n\n👤 {user.mention_html()}\n──────────────────\n{random.choice(TRUTH_QUESTIONS)}", parse_mode="HTML")

@dp.message(Command("dare"))
async def cmd_dare(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  😈  ДЕЙСТВИЕ     ║\n╚══════════════════╝\n\n👤 {user.mention_html()}\n──────────────────\n{random.choice(DARE_CHALLENGES)}", parse_mode="HTML")

@dp.message(Command("wyr"))
async def cmd_wyr(message: Message):
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🎯  ЧТО ВЫБЕРЕШЬ ║\n╚══════════════════╝\n\n{random.choice(WOULD_YOU_RATHER)}\n──────────────────", parse_mode="HTML")

async def cmd_rps(message: Message, command: CommandObject):
    choices = {"к":"✊ Камень","н":"✌️ Ножницы","б":"🖐 Бумага"}
    wins    = {"к":"н","н":"б","б":"к"}
    if not command.args or command.args.lower() not in choices:
        await reply_auto_delete(message, "✊ /rps к — камень, /rps н — ножницы, /rps б — бумага"); return
    p = command.args.lower(); b = random.choice(list(choices.keys()))
    res = "🤝 Ничья!" if p==b else ("🎉 Ты выиграл!" if wins[p]==b else "😈 Я выиграл!")
    await reply_auto_delete(message, f"╔══════════════════╗\n║  ✊  КНБ          ║\n╚══════════════════╝\n\n👤 Ты: {choices[p]}\n🤖 Я: {choices[b]}\n──────────────────\n{res}")

async def cmd_choose(message: Message, command: CommandObject):
    if not command.args or "|" not in command.args:
        await reply_auto_delete(message, "⚠️ /choose вар1|вар2|вар3"); return
    options = [o.strip() for o in command.args.split("|") if o.strip()]
    if len(options) < 2: await reply_auto_delete(message, "⚠️ Минимум 2 варианта."); return
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🎯  ВЫБОР        ║\n╚══════════════════╝\n\n✅ <b>{random.choice(options)}</b>\n──────────────────", parse_mode="HTML")

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    sign, text = random.choice(list(HOROSCOPES.items()))
    await reply_auto_delete(message, f"╔══════════════════╗\n║  🔮  ГОРОСКОП     ║\n╚══════════════════╝\n\n{sign} {user.mention_html()}\n──────────────────\n{text}", parse_mode="HTML")

@dp.message(Command("predict"))
async def cmd_predict(message: Message):
    user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  🔮  ПРЕДСКАЗАНИЕ ║\n╚══════════════════╝\n\n👤 {user.mention_html()}\n──────────────────\n{random.choice(PREDICTIONS)}", parse_mode="HTML")

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
        f"╔══════════════════╗\n║  💘  СОВМЕСТИМОСТЬ ║\n╚══════════════════╝\n\n👤 {user1.mention_html()}\n{bar}\n👤 {user2.mention_html()}\n──────────────────\n<b>{percent}%</b> — {verdict}",
        parse_mode="HTML")

@dp.message(Command("botstats"))
async def cmd_botstats(message: Message):
    if not await require_admin(message): return
    total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
    total_warns = sum(sum(v.values()) for v in warnings.values())
    await reply_auto_delete(message, 
        f"╔══════════════════╗\n║  📈  СТАТИСТИКА   ║\n╚══════════════════╝\n\n"
        f"💬 Сообщений: <b>{total_msgs}</b>\n"
        f"⚡ Варнов: <b>{total_warns}</b>\n"
        f"😴 AFK: <b>{len(afk_users)}</b>\n"
        f"🌐 Чатов: <b>{len(chat_stats)}</b>\n"
        f"──────────────────\n"
        f"🧼 Антимат: <b>{'вкл' if ANTI_MAT_ENABLED else 'выкл'}</b>\n"
        f"🤖 Автокик: <b>{'вкл' if AUTO_KICK_BOTS else 'выкл'}</b>", parse_mode="HTML")

@dp.message(F.text & ~F.text.startswith("/") & F.chat.type.in_({"group", "supergroup"}))
async def autist_commands(message: Message):
    if not message.text: return
    text_lower = message.text.strip().lower()
    if not text_lower.startswith("аутист"): return
    fun_only = ["обозвать", "поженить", "казнить", "диагноз", "профессия", "похитить", "дуэль",
                "история",
                "пидорас", "гей", "красавчик", "умник", "дурак", "богатый", "бедный",
                "победитель", "неудачник", "король", "лох", "чемпион", "легенда", "токсик",
                "няша", "краш", "маньяк", "детектив", "злодей", "герой", "криптан",
                "рофлер", "скучный", "активный",
                "матерщинник", "сквернослов", "грубиян", "хулиган", "невинный",
                "чистоуст", "банщик", "цензор", "матбот", "ругатель",
                "гандон", "мудак", "долбоёб", "шлюха", "петух", "чмо", "урод",
                "дебил", "идиот", "придурок", "тупица", "лузер", "задрот", "нытик",
                "предатель", "стукач", "жлоб", "жадина", "трус", "псих",
                # 🆕 Новые
                "философ", "поэт", "художник", "программист", "блогер", "стример",
                "спортсмен", "повар", "врач", "учитель", "бизнесмен", "политик",
                "шпион", "волшебник", "робот", "зомби", "вампир", "пришелец",
                "ниндзя", "пират", "рыцарь", "самурай", "викинг", "фараон",
                "миллионер", "бездомный", "звезда", "аутсайдер", "инфлюенсер",
                "симпатяга", "мемлорд", "флудер", "молчун", "болтун", "философ",
                "оптимист", "пессимист", "параноик", "нарцисс", "альтруист",
                "везунчик", "невезучий", "сонный", "голодный", "влюблённый",
                "ревнивый", "обиженный", "счастливый", "грустный", "бешеный",
                "фанат", "хейтер", "критик", "защитник", "провокатор",
                "шутник", "серьёзный", "рандом", "легаси", "ноунейм"]
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
                "обозвать","поженить","проверить","казнить","диагноз","профессия","похитить","дуэль","экзамен","история",
                "подарить","предложить","разлюбить",
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
                "стикермут","гифмут","войсмут","всёмут","подарить","предложить","разлюбить",
                "пидорас","гей","красавчик","умник","дурак","богатый","бедный",
                "победитель","неудачник","король","лох","чемпион","легенда","токсик",
                "няша","краш","маньяк","детектив","злодей","герой","криптан",
                "рофлер","скучный","активный",
                "матерщинник","сквернослов","грубиян","хулиган","невинный",
                "чистоуст","банщик","цензор","матбот","ругатель",
                "гандон","мудак","долбоёб","шлюха","петух","чмо","урод",
                "дебил","идиот","придурок","тупица","лузер","задрот","нытик",
                "предатель","стукач","жлоб","жадина","трус","псих",
                "философ","поэт","художник","программист","блогер","стример",
                "спортсмен","повар","врач","учитель","бизнесмен","политик",
                "шпион","волшебник","робот","зомби","вампир","пришелец",
                "ниндзя","пират","рыцарь","самурай","викинг","фараон",
                "миллионер","бездомный","звезда","аутсайдер","инфлюенсер",
                "симпатяга","мемлорд","флудер","молчун","болтун",
                "оптимист","пессимист","параноик","нарцисс","альтруист",
                "везунчик","невезучий","сонный","голодный","влюблённый",
                "ревнивый","обиженный","счастливый","грустный","бешеный",
                "фанат","хейтер","критик","защитник","провокатор",
                "шутник","серьёзный","рандом","легаси","ноунейм"]:
        if rest.startswith(cmd):
            action = cmd; rest = rest[len(cmd):].strip(); break
    if not action: return
    cid = message.chat.id
    target = None

    # Команды которым target не нужен
    NO_TARGET_CMDS = {"статус", "хаос", "скрин", "взрыв", "шпион", "жребий", "громко",
                      "антиспам", "зеркало", "локдаун", "анонс", "лотерея",
                      "тишина", "история", "топ нарушителей",
                      "температура", "неделя", "рестарт", "сос", "лог",
                      # 🎲 Ирис-команды — выбирают рандомного юзера
                      "пидорас","гей","красавчик","умник","дурак","богатый","бедный",
                      "победитель","неудачник","король","лох","чемпион","легенда","токсик",
                      "няша","краш","маньяк","детектив","злодей","герой","криптан",
                      "рофлер","скучный","активный","матерщинник","сквернослов","грубиян",
                      "хулиган","невинный","чистоуст","банщик","цензор","матбот","ругатель",
                      "гандон","мудак","долбоёб","шлюха","петух","чмо","урод",
                      "дебил","идиот","придурок","тупица","лузер","задрот","нытик",
                      "предатель","стукач","жлоб","жадина","трус","псих"}

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
                await bot.ban_chat_member(cid, target.id, until_date=datetime.now() + timedelta(minutes=duration_mins))
                await reply_auto_delete(message, f"🔨 {tname} забанен на <b>{duration_label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            else:
                await bot.ban_chat_member(cid, target.id)
                await reply_auto_delete(message, f"🔨 {tname} забанен навсегда!\n📝 Причина: {reason}", parse_mode="HTML")
            add_mod_history(cid, target.id, "🔨 Бан", reason, message.from_user.full_name)
            ban_list[cid][target.id] = True; save_data()
            await log_action(f"🔨 <b>БАН</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "захуесосить":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await reply_auto_delete(message, f"👢 {tname} захуесошен из чата!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "кик":
            await bot.ban_chat_member(cid, target.id)
            await bot.unban_chat_member(cid, target.id)
            await reply_auto_delete(message, f"👟 {tname} кикнут из чата!\n📝 Причина: {reason}", parse_mode="HTML")
            await log_action(f"👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action == "мут":
            mins = duration_mins or 60; label = duration_label or "1 ч."
            await bot.restrict_chat_member(cid, target.id,
                permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(minutes=mins))
            await reply_auto_delete(message, f"🔇 {tname} замучен на <b>{label}</b>!\n📝 Причина: {reason}", parse_mode="HTML")
            add_mod_history(cid, target.id, f"🔇 Мут {label}", reason, message.from_user.full_name)
            await log_action(f"🔇 <b>МУТ</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n⏱ <b>Время:</b> {label}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
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
                ban_list[cid][target.id] = True; save_data()
                add_mod_history(cid, target.id, "🔨 Автобан", f"{MAX_WARNINGS} варнов", message.from_user.full_name)
                await reply_auto_delete(message, f"🔨 {tname} — {MAX_WARNINGS} варна, автобан!\n📝 Причина: {reason}", parse_mode="HTML")
                await log_action(f"🔨 <b>Автоматическая блокировка</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n🤖 <b>Причина:</b> лимит варнов\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
            else:
                add_mod_history(cid, target.id, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, message.from_user.full_name)
                time_note = f"\n⏰ Автосброс через: <b>{duration_label}</b>" if duration_mins else ""
                await reply_auto_delete(message, f"⚡ {tname} получил варн <b>{count}/{MAX_WARNINGS}</b>!\n📝 Причина: {reason}{time_note}", parse_mode="HTML")
                await log_action(f"⚠️ <b>Предупреждение</b>\n\n👤 <b>Кто:</b> {message.from_user.mention_html()}\n🎯 <b>Кого:</b> {tname}\n📝 <b>Причина:</b> {reason}\n💬 <b>Чат:</b> {message.chat.title}\n🕐 <b>Время:</b> {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}")
        elif action in ("снять варн", "разварн"):
            if warnings[cid][target.id] > 0: warnings[cid][target.id] -= 1; save_data()
            add_mod_history(cid, target.id, "🌿 Снят варн", reason, message.from_user.full_name)
            await reply_auto_delete(message, f"🌿 С {tname} снят варн. Осталось: <b>{warnings[cid][target.id]}/{MAX_WARNINGS}</b>", parse_mode="HTML")
        elif action == "разбан":
            await bot.unban_chat_member(cid, target.id, only_if_banned=True)
            ban_list[cid].pop(target.id, None); save_data()
            add_mod_history(cid, target.id, "🕊 Разбан", reason, message.from_user.full_name)
            await reply_auto_delete(message, f"🕊 {tname} разбанен.", parse_mode="HTML")
        elif action == "размут":
            await bot.restrict_chat_member(cid, target.id, permissions=ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, can_send_polls=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            add_mod_history(cid, target.id, "🔊 Размут", reason, message.from_user.full_name)
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
                permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(minutes=mins))
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

        elif action == "подарить":
            if not target:
                await reply_auto_delete(message, "↩️ Реплайни на сообщение"); return
            gifts_emojis = list(GIFT_LIST.keys())
            gift_emoji = rest.strip() if rest and rest.strip() in GIFT_LIST else random.choice(gifts_emojis)
            gift_name, gift_price = GIFT_LIST[gift_emoji]
            await reply_auto_delete(message,
                f"{gift_emoji} <b>{message.from_user.full_name}</b> дарит <b>{gift_name}</b> пользователю {tname}!\n"
                f"✨ Приятный сюрприз!",
                parse_mode="HTML")

        elif action == "предложить":
            if not target:
                await reply_auto_delete(message, "↩️ Реплайни на сообщение"); return
            proposals = [
                f"💝 {message.from_user.full_name} опускается на одно колено перед {tname}...\n❤️ Ты будешь моей второй половинкой?",
                f"🌹 {message.from_user.full_name} дарит {tname} красную розу...\n💫 Встречаемся?",
                f"💌 {message.from_user.full_name} пишет записку {tname}:\n'Ты мне нравишься ❤️'",
            ]
            await reply_auto_delete(message, random.choice(proposals), parse_mode="HTML")

        elif action == "разлюбить":
            if not target:
                await reply_auto_delete(message, "↩️ Реплайни на сообщение"); return
            breakups = [
                f"💔 {message.from_user.full_name} и {tname} расстались...\nАнекдот закончился.",
                f"🥀 {message.from_user.full_name} бросил(а) {tname} через сообщение\n😢 Это жестоко.",
                f"📱 {message.from_user.full_name} разблокировал(а) {tname}...\nНет погоди — заблокировал(а).",
            ]
            await reply_auto_delete(message, random.choice(breakups), parse_mode="HTML")

        elif action == "история":
            # Берём случайных участников из chat_stats
            members_ids = list(chat_stats[cid].keys())
            if len(members_ids) < 2:
                await reply_auto_delete(message,
                    "😅 Нужно минимум 2 активных участника для истории!"); return

            # Берём от 2 до 4 участников
            count = min(random.randint(2, 4), len(members_ids))
            chosen_ids = random.sample(members_ids, count)
            heroes = []
            for uid_h in chosen_ids:
                try:
                    tm = await bot.get_chat_member(cid, uid_h)
                    heroes.append(tm.user.full_name)
                except:
                    heroes.append(f"Участник{uid_h % 100}")

            # Шаблоны историй
            STORY_PLACES = [
                "в магазине", "на рынке", "в лифте", "в метро", "на даче",
                "в McDonald's", "в больнице", "на парковке", "в кино",
                "на рыбалке", "в спортзале", "в ночном клубе", "на работе",
                "в автобусе", "на вечеринке", "в аптеке", "на пляже",
                "в банке", "в очереди", "на крыше", "в лесу"
            ]
            STORY_ITEMS = [
                "сковородку", "зонтик", "кота", "шаурму", "ноутбук",
                "три тысячи рублей", "чужой телефон", "пакет молока",
                "велосипед", "старый диван", "стакан кофе", "красный шарик",
                "пиццу", "чемодан", "гитару", "резиновую утку"
            ]
            STORY_EVENTS = [
                "нашли {item}", "потеряли {item}", "украли {item}",
                "подрались из-за {item}", "продали {item}",
                "обменяли {item} на пирожок", "закопали {item}",
                "уронили {item} в унитаз", "подарили {item} незнакомцу",
                "съели {item}", "сдали {item} в ломбард"
            ]
            STORY_ENDINGS = [
                "и ушли как ни в чём не бывало.",
                "и с тех пор никто их не видел.",
                "и стали лучшими друзьями.",
                "но всё равно остались довольны.",
                "и решили никому не рассказывать.",
                "но полиция уже едет.",
                "и написали об этом в чат.",
                "и попали в местные новости.",
                "хотя никто так и не понял зачем.",
                "но это уже другая история.",
                "и выложили это в сторис.",
                "а очевидцы до сих пор в шоке.",
                "и с тех пор в чате стало тише.",
                "хотя мама запрещала так делать.",
            ]
            STORY_MIDDLES = [
                "{h0} позвонил(а) {h1} и сказал(а): «Нам надо поговорить».",
                "{h0} случайно отправил(а) {h1} голосовое сообщение не туда.",
                "{h0} поспорил(а) с {h1} на {item}.",
                "{h0} попросил(а) {h1} подержать {item} «на секунду».",
                "{h0} обнаружил(а) что {h1} уже давно {event}.",
                "{h0} и {h1} решили действовать вместе.",
                "{h0} увидел(а) как {h1} прячет {item} в кармане.",
                "{h0} предложил(а) {h1} сделку которую нельзя отклонить.",
            ]

            place = random.choice(STORY_PLACES)
            item = random.choice(STORY_ITEMS)
            event = random.choice(STORY_EVENTS).format(item=item)
            ending = random.choice(STORY_ENDINGS)

            h0 = heroes[0]
            h1 = heroes[1] if len(heroes) > 1 else heroes[0]
            h2 = heroes[2] if len(heroes) > 2 else None
            h3 = heroes[3] if len(heroes) > 3 else None

            middle_tpl = random.choice(STORY_MIDDLES)
            middle = middle_tpl.format(h0=h0, h1=h1, item=item, event=event)

            # Собираем историю
            intros = [
                f"Однажды {h0} и {h1} оказались {place}.",
                f"Это произошло {place}. {h0} и {h1} даже не подозревали чем закончится день.",
                f"Никто не знает зачем {h0} позвал(а) {h1} {place}.",
                f"Всё началось когда {h0} встретил(а) {h1} {place}.",
                f"Говорят что {place} меняет людей. {h0} и {h1} убедились в этом лично.",
            ]
            intro = random.choice(intros)

            # Добавляем третьего персонажа если есть
            extra = ""
            if h2:
                extra_lines = [
                    f" Тут неожиданно появился(ась) {h2} с {item}.",
                    f" В этот момент {h2} проходил(а) мимо и всё видел(а).",
                    f" {h2} пытался(ась) остановить это безумие но тщетно.",
                    f" Единственным свидетелем был(а) {h2}.",
                ]
                extra += random.choice(extra_lines)

            if h3:
                extra_lines2 = [
                    f" А {h3} снимал(а) всё это на телефон.",
                    f" {h3} уже вызвал(а) такси чтобы сбежать.",
                    f" {h3} в этот момент спал(а) дома и ни о чём не знал(а).",
                ]
                extra += random.choice(extra_lines2)

            story = (
                f"╔══════════════════╗\n"
                f"║  📖  ИСТОРИЯ      ║\n"
                f"╚══════════════════╝\n\n"
                f"{intro} {middle}{extra} {ending}"
            )

            await reply_auto_delete(message, story, parse_mode="HTML")

        elif action == "проверить":
            await reply_auto_delete(message, f"ℹ️ Функция проверки (капча) отключена.", parse_mode="HTML")

        # ══════════════════════════════════════════
        #  🎲 КОМАНДЫ ТИПА ИРИС — случайный юзер
        # ══════════════════════════════════════════
        elif action in ("пидорас", "гей", "красавчик", "умник", "дурак",
                        "богатый", "бедный", "победитель", "неудачник",
                        "король", "лох", "чемпион", "легенда", "токсик",
                        "няша", "краш", "маньяк", "детектив", "злодей",
                        "герой", "криптан", "рофлер", "скучный", "активный",
                        "матерщинник", "сквернослов", "грубиян", "хулиган", "невинный",
                        "чистоуст", "банщик", "цензор", "матбот", "ругатель",
                        "гандон", "мудак", "долбоёб", "шлюха", "петух", "чмо", "урод",
                        "дебил", "идиот", "придурок", "тупица", "лузер", "задрот", "нытик",
                        "предатель", "стукач", "жлоб", "жадина", "трус", "псих",
                        "философ", "поэт", "художник", "программист", "блогер", "стример",
                        "спортсмен", "повар", "врач", "учитель", "бизнесмен", "политик",
                        "шпион", "волшебник", "робот", "зомби", "вампир", "пришелец",
                        "ниндзя", "пират", "рыцарь", "самурай", "викинг", "фараон",
                        "миллионер", "бездомный", "звезда", "аутсайдер", "инфлюенсер",
                        "симпатяга", "мемлорд", "флудер", "молчун", "болтун",
                        "оптимист", "пессимист", "параноик", "нарцисс", "альтруист",
                        "везунчик", "невезучий", "сонный", "голодный", "влюблённый",
                        "ревнивый", "обиженный", "счастливый", "грустный", "бешеный",
                        "фанат", "хейтер", "критик", "защитник", "провокатор",
                        "шутник", "серьёзный", "рандом", "легаси", "ноунейм"):

            # Берём случайного участника из chat_stats
            members = list(chat_stats[cid].keys())
            if not members:
                await reply_auto_delete(message,
                    "😅 Ещё никто не писал в чате — некого выбирать!"); return

            chosen_uid = random.choice(members)
            try:
                tm = await bot.get_chat_member(cid, chosen_uid)
                chosen_name = tm.user.full_name
                chosen_mention = f"<a href='tg://user?id={chosen_uid}'>{chosen_name}</a>"
            except:
                chosen_mention = f"ID{chosen_uid}"
                chosen_name = f"ID{chosen_uid}"

            # Тексты для каждой команды
            IRIS_TEXTS = {
                "пидорас":   [f"🌈 Сегодняшний пидорас чата — {chosen_mention}! 🏳️‍🌈",
                               f"🎯 Рулетка решила! Пидорас дня: {chosen_mention} 🌈",
                               f"📢 Внимание! Пидорас дня — {chosen_mention}! Поздравляем! 🎉"],
                "гей":       [f"🏳️‍🌈 Гей дня — {chosen_mention}! Прими звание с честью!",
                               f"🎀 Сегодня гей чата — {chosen_mention}!"],
                "красавчик": [f"😎 Красавчик дня — {chosen_mention}! Все завидуют!",
                               f"💅 Самый красивый сегодня — {chosen_mention}!",
                               f"🔥 {chosen_mention} — красавчик этого чата!"],
                "умник":     [f"🧠 Самый умный сегодня — {chosen_mention}!",
                               f"📚 Умник дня: {chosen_mention}. ИИ нервно курит!"],
                "дурак":     [f"🤡 Дурак дня — {chosen_mention}! Держи корону!",
                               f"💫 {chosen_mention} сегодня отличился особой глупостью!"],
                "богатый":   [f"💰 Богач дня — {chosen_mention}! Угощает всех!",
                               f"🤑 {chosen_mention} сегодня самый богатый в чате!"],
                "бедный":    [f"😢 Бедняга дня — {chosen_mention}. Скинемся?",
                               f"🪙 {chosen_mention} сегодня без копейки..."],
                "победитель":[f"🏆 Победитель дня — {chosen_mention}!",
                               f"🥇 {chosen_mention} выиграл всё что можно сегодня!"],
                "неудачник": [f"😭 Неудачник дня — {chosen_mention}. Не везёт...",
                               f"🍀 Удача покинула {chosen_mention} сегодня."],
                "король":    [f"👑 Король чата сегодня — {chosen_mention}!",
                               f"🎖 {chosen_mention} — КОРОЛЬ! Все склоните головы!"],
                "лох":       [f"🥚 Лох дня — {chosen_mention}! Осторожнее!",
                               f"😬 {chosen_mention} сегодня повёлся как лох."],
                "чемпион":   [f"🏅 Чемпион дня — {chosen_mention}!",
                               f"⚡ {chosen_mention} — чемпион этого чата!"],
                "легенда":   [f"🌟 Легенда чата сегодня — {chosen_mention}!",
                               f"💫 {chosen_mention} вошёл в историю!"],
                "токсик":    [f"☠️ Токсик дня — {chosen_mention}. Берегитесь!",
                               f"🧪 {chosen_mention} сегодня максимально токсичен!"],
                "няша":      [f"🥰 Няша дня — {chosen_mention}! Все обнимают!",
                               f"🌸 {chosen_mention} — самый милый сегодня!"],
                "краш":      [f"😍 Краш дня — {chosen_mention}! Все влюблены!",
                               f"💝 {chosen_mention} — краш этого чата!"],
                "маньяк":    [f"🔪 Маньяк дня — {chosen_mention}. Закройте двери!",
                               f"😈 {chosen_mention} сегодня в образе маньяка!"],
                "детектив":  [f"🕵️ Детектив дня — {chosen_mention}! Ничего не скроешь!",
                               f"🔍 {chosen_mention} расследует все дела чата!"],
                "злодей":    [f"😈 Злодей дня — {chosen_mention}! Бойтесь!",
                               f"🦹 {chosen_mention} сегодня на тёмной стороне!"],
                "герой":     [f"🦸 Герой дня — {chosen_mention}! Спасибо за службу!",
                               f"⚔️ {chosen_mention} — герой этого чата!"],
                "криптан":   [f"📈 Криптан дня — {chosen_mention}! Buy the dip!",
                               f"💎 {chosen_mention} уже держит биток с 2009!"],
                "рофлер":    [f"😂 Рофлер дня — {chosen_mention}! Всех рассмешил!",
                               f"🎭 {chosen_mention} — главный комик чата!"],
                "скучный":   [f"😴 Скучный дня — {chosen_mention}. Зевааа...",
                               f"💤 {chosen_mention} прям усыпляет своими сообщениями."],
                "активный":  [f"⚡ Самый активный сегодня — {chosen_mention}!",
                               f"🔥 {chosen_mention} не молчит ни минуты!"],
                "матерщинник":[f"🤬 Матерщинник дня — {chosen_mention}!\n💬 Цензуре придётся поработать...",
                                f"🔞 Рекордсмен по мату сегодня — {chosen_mention}!\n⚠️ Родители были бы разочарованы.",
                                f"🧼 {chosen_mention} сегодня моет рот с мылом — самый матерный!"],
                "сквернослов":[f"🗯 Сквернослов дня — {chosen_mention}!\n📵 Даже боты краснеют от таких слов.",
                                f"😤 {chosen_mention} сегодня отличился отборной лексикой!"],
                "грубиян":   [f"😠 Грубиян дня — {chosen_mention}!\n🙏 Будь добрее, дружище.",
                               f"💢 {chosen_mention} сегодня в режиме максимальной грубости!"],
                "хулиган":   [f"😈 Хулиган чата — {chosen_mention}!\n🚔 Полиция уже едет.",
                               f"🤘 {chosen_mention} — главный хулиган сегодня!"],
                "невинный":  [f"😇 Самый невинный сегодня — {chosen_mention}!\n✨ Ни единого плохого слова.",
                               f"🕊 {chosen_mention} — чистая душа этого чата!",
                               f"😊 {chosen_mention} сегодня как ангел — ни одного мата!"],
                "чистоуст":  [f"🧼 Чистоуст дня — {chosen_mention}!\n✅ Образец культурной речи.",
                               f"📖 {chosen_mention} говорит как диктор на телевидении!"],
                "банщик":    [f"🔨 Банщик дня — {chosen_mention}!\n⚠️ Все боятся получить бан.",
                               f"🚫 {chosen_mention} сегодня банит направо и налево!"],
                "цензор":    [f"📵 Цензор дня — {chosen_mention}!\n🔍 Следит за каждым словом.",
                               f"🧐 {chosen_mention} проверяет все сообщения на цензуру!"],
                "матбот":    [f"🤖 Матбот дня — {chosen_mention}!\n💬 Говорит только матом как робот.",
                               f"⚙️ {chosen_mention} сегодня работает в режиме матбота!"],
                "ругатель":  [f"🗣 Ругатель дня — {chosen_mention}!\n🔊 Слышно на весь чат.",
                               f"😤 {chosen_mention} ругается громче всех сегодня!"],
                "гандон":    [f"🎈 Гандон дня — {chosen_mention}!\n🏆 Заслуженная награда.",
                               f"📢 Внимание! Гандон дня — {chosen_mention}! Аплодисменты! 👏",
                               f"🎖 {chosen_mention} получает почётное звание гандона дня!"],
                "мудак":     [f"🤡 Мудак дня — {chosen_mention}!\n😤 Все устали от него.",
                               f"🎯 Рулетка выбрала! Мудак дня: {chosen_mention}",
                               f"💢 {chosen_mention} сегодня в ударе — мудак дня!"],
                "долбоёб":   [f"🔔 Долбоёб дня — {chosen_mention}!\n🧠 Мозг в аренде.",
                               f"📣 {chosen_mention} — долбоёб дня! Поздравляем с титулом!",
                               f"🎪 {chosen_mention} выиграл звание долбоёба дня!"],
                "шлюха":     [f"💋 Шлюха дня — {chosen_mention}!\n😏 Ну ты понял.",
                               f"🌹 {chosen_mention} — шлюха дня по итогам голосования чата!"],
                "петух":     [f"🐓 Петух дня — {chosen_mention}!\n🏳️ Опустите голову.",
                               f"📢 КО-КО-КО! Петух дня — {chosen_mention}!",
                               f"🎖 {chosen_mention} получает звание петуха дня!"],
                "чмо":       [f"🦠 Чмо дня — {chosen_mention}!\n😷 Все держитесь подальше.",
                               f"🎯 {chosen_mention} — чмо дня! Достижение разблокировано!"],
                "урод":      [f"🪞 Урод дня — {chosen_mention}!\n😬 Зеркала плачут.",
                               f"😵 {chosen_mention} сегодня в номинации 'урод дня'!"],
                "дебил":     [f"🤪 Дебил дня — {chosen_mention}!\n🧩 Не все пазлы на месте.",
                               f"📋 Диагноз дня: дебил. Пациент: {chosen_mention}",
                               f"🏥 {chosen_mention} — дебил дня по заключению чата!"],
                "идиот":     [f"🙃 Идиот дня — {chosen_mention}!\n📉 IQ в минус.",
                               f"🎓 {chosen_mention} окончил школу идиотов с отличием!"],
                "придурок":  [f"🌀 Придурок дня — {chosen_mention}!\n💫 Диагноз очевиден.",
                               f"😜 {chosen_mention} — придурок дня! Все поздравляют!"],
                "тупица":    [f"🐢 Тупица дня — {chosen_mention}!\n🐌 Даже улитки обгоняют.",
                               f"📉 {chosen_mention} сегодня в режиме тупицы!"],
                "лузер":     [f"😭 Лузер дня — {chosen_mention}!\n📉 Проигрывает во всём.",
                               f"🏳️ {chosen_mention} — лузер дня! L в чат!"],
                "задрот":    [f"🖥 Задрот дня — {chosen_mention}!\n⌨️ Не выходит из дома.",
                               f"🎮 {chosen_mention} задрачивает сильнее всех!"],
                "нытик":     [f"😭 Нытик дня — {chosen_mention}!\n🎻 Мир играет на скрипочке.",
                               f"😢 {chosen_mention} нывает громче всех сегодня!"],
                "предатель": [f"🗡 Предатель дня — {chosen_mention}!\n🐍 Змей в нашем чате.",
                               f"😤 {chosen_mention} — предатель дня! Не доверяй ему!"],
                "стукач":    [f"🚔 Стукач дня — {chosen_mention}!\n📞 Уже звонит администрации.",
                               f"👮 {chosen_mention} — главный стукач чата!"],
                "жлоб":      [f"💰 Жлоб дня — {chosen_mention}!\n🤑 Жмёт каждую копейку.",
                               f"😒 {chosen_mention} — жлоб дня! Угостить не допросишься."],
                "жадина":    [f"🍬 Жадина дня — {chosen_mention}!\n😤 Ничем не делится.",
                               f"💸 {chosen_mention} — жадина дня! Всё только себе!"],
                "трус":      [f"🐔 Трус дня — {chosen_mention}!\n😱 Боится собственной тени.",
                               f"🏃 {chosen_mention} первым убегает от проблем!"],
                "псих":      [f"🔪 Псих дня — {chosen_mention}!\n🏥 Пора к доктору.",
                               f"🌀 {chosen_mention} сегодня ведёт себя как псих!",
                               f"😵 {chosen_mention} — псих дня! Осторожно!"],
                # 🆕 Профессии
                "философ":   [f"🧘 Философ дня — {chosen_mention}!\n💭 Задаёт вопросы без ответов.",
                               f"📜 {chosen_mention} сегодня рассуждает о смысле бытия!"],
                "поэт":      [f"🖊 Поэт дня — {chosen_mention}!\n🌹 Душа чата рифмует на лету.",
                               f"📝 {chosen_mention} сегодня пишет стихи в голове!"],
                "художник":  [f"🎨 Художник дня — {chosen_mention}!\n🖼 Видит мир иначе.",
                               f"✏️ {chosen_mention} — творческая душа чата!"],
                "программист":[f"💻 Программист дня — {chosen_mention}!\n🐛 Дебажит жизнь с утра.",
                                f"⌨️ {chosen_mention} пишет код быстрее чем думает!",
                                f"🖥 {chosen_mention} — самый технический участник дня!"],
                "блогер":    [f"📸 Блогер дня — {chosen_mention}!\n📱 Снимает всё подряд.",
                               f"🎬 {chosen_mention} сегодня снимает контент для подписчиков!"],
                "стример":   [f"🎮 Стример дня — {chosen_mention}!\n📡 Донаты принимаются.",
                               f"🕹 {chosen_mention} сейчас в прямом эфире в голове!"],
                "спортсмен": [f"🏋️ Спортсмен дня — {chosen_mention}!\n💪 Тело — храм.",
                               f"⚽ {chosen_mention} сегодня побьёт все рекорды!"],
                "повар":     [f"👨‍🍳 Повар дня — {chosen_mention}!\n🍝 Кормит весь чат воображаемыми блюдами.",
                               f"🍕 {chosen_mention} — мастер кулинарии этого чата!"],
                "врач":      [f"🩺 Врач дня — {chosen_mention}!\n💊 Ставит диагнозы без лицензии.",
                               f"🏥 {chosen_mention} сегодня лечит весь чат!"],
                "учитель":   [f"📚 Учитель дня — {chosen_mention}!\n✏️ Объясняет очевидное.",
                               f"🎓 {chosen_mention} сегодня просвещает весь чат!"],
                "бизнесмен": [f"💼 Бизнесмен дня — {chosen_mention}!\n📊 Считает чужие деньги.",
                               f"🤝 {chosen_mention} уже составляет бизнес-план в голове!"],
                "политик":   [f"🏛 Политик дня — {chosen_mention}!\n🗣 Обещает всё подряд.",
                               f"📢 {chosen_mention} сегодня баллотируется в чат-президенты!"],
                # 🆕 Фэнтези/игровые
                "шпион":     [f"🕵️ Шпион дня — {chosen_mention}!\n🔍 Следит за каждым.",
                               f"🎯 {chosen_mention} — агент 007 этого чата!"],
                "волшебник": [f"🧙 Волшебник дня — {chosen_mention}!\n✨ Творит чудеса прямо здесь.",
                               f"🔮 {chosen_mention} сегодня колдует на весь чат!"],
                "робот":     [f"🤖 Робот дня — {chosen_mention}!\n⚙️ Работает без эмоций.",
                               f"🔩 {chosen_mention} — самый механистичный участник дня!"],
                "зомби":     [f"🧟 Зомби дня — {chosen_mention}!\n🧠 Ищет мозги в чате.",
                               f"☠️ {chosen_mention} сегодня бродит как зомби!"],
                "вампир":    [f"🧛 Вампир дня — {chosen_mention}!\n🌙 Активен только ночью.",
                               f"🩸 {chosen_mention} — граф Дракула этого чата!"],
                "пришелец":  [f"👽 Пришелец дня — {chosen_mention}!\n🛸 Явно не с этой планеты.",
                               f"🌌 {chosen_mention} прилетел из далёкой галактики!"],
                "ниндзя":    [f"🥷 Ниндзя дня — {chosen_mention}!\n⚡ Невидим и смертоносен.",
                               f"🗡 {chosen_mention} растворяется в тени чата!"],
                "пират":     [f"🏴‍☠️ Пират дня — {chosen_mention}!\n⚓ Йо-хо-хо!",
                               f"🦜 {chosen_mention} захватывает чат как пират!"],
                "рыцарь":    [f"⚔️ Рыцарь дня — {chosen_mention}!\n🛡 Защищает честь чата.",
                               f"🏰 {chosen_mention} — благородный рыцарь без страха!"],
                "самурай":   [f"⚔️ Самурай дня — {chosen_mention}!\n🎌 Следует кодексу бусидо.",
                               f"🗡 {chosen_mention} — воин без хозяина этого чата!"],
                "викинг":    [f"🪓 Викинг дня — {chosen_mention}!\n⚡ Грабит и жжёт сообщения.",
                               f"🛡 {chosen_mention} приплыл на дракаре в этот чат!"],
                "фараон":    [f"𓂀 Фараон дня — {chosen_mention}!\n👑 Все склоняются.",
                               f"🏺 {chosen_mention} — повелитель Египта и этого чата!"],
                # 🆕 Социальные типажи
                "миллионер": [f"💰 Миллионер дня — {chosen_mention}!\n🤑 Деньги есть (в мечтах).",
                               f"💎 {chosen_mention} сегодня купается в деньгах!"],
                "бездомный": [f"📦 Бездомный дня — {chosen_mention}!\n🏠 Ищет крышу над головой.",
                               f"🛒 {chosen_mention} сегодня живёт в картонной коробке!"],
                "звезда":    [f"⭐ Звезда дня — {chosen_mention}!\n🎤 Все смотрят только на него.",
                               f"🌟 {chosen_mention} — настоящая суперзвезда чата!"],
                "аутсайдер": [f"😶 Аутсайдер дня — {chosen_mention}!\n🚪 Стоит у входа.",
                               f"🌑 {chosen_mention} сегодня держится особняком!"],
                "инфлюенсер":[f"📱 Инфлюенсер дня — {chosen_mention}!\n🤳 Всё фоткает и постит.",
                               f"✨ {chosen_mention} — лидер мнений этого чата!"],
                "симпатяга": [f"😊 Симпатяга дня — {chosen_mention}!\n💫 Все рады его видеть.",
                               f"🤗 {chosen_mention} — самый приятный человек дня!"],
                "мемлорд":   [f"😂 Мемлорд дня — {chosen_mention}!\n🖼 Поставляет мемы оптом.",
                               f"🃏 {chosen_mention} знает все мемы которые существуют!"],
                "флудер":    [f"💬 Флудер дня — {chosen_mention}!\n📩 Пишет без остановки.",
                               f"🌊 {chosen_mention} затопил чат сообщениями!"],
                "молчун":    [f"🤐 Молчун дня — {chosen_mention}!\n🔇 Слова на вес золота.",
                               f"🪨 {chosen_mention} молчит громче всех!"],
                "болтун":    [f"🗣 Болтун дня — {chosen_mention}!\n📢 Говорит не останавливаясь.",
                               f"🎙 {chosen_mention} может говорить часами!"],
                # 🆕 Психотипы
                "оптимист":  [f"☀️ Оптимист дня — {chosen_mention}!\n😁 Всё будет хорошо!",
                               f"🌈 {chosen_mention} видит только лучшее в жизни!"],
                "пессимист": [f"🌧 Пессимист дня — {chosen_mention}!\n😞 Всё плохо и будет хуже.",
                               f"☁️ {chosen_mention} ждёт только плохого!"],
                "параноик":  [f"👀 Параноик дня — {chosen_mention}!\n🔍 Везде заговор.",
                               f"🕵️ {chosen_mention} уверен что за ним следят!"],
                "нарцисс":   [f"🪞 Нарцисс дня — {chosen_mention}!\n💅 Любит себя больше всех.",
                               f"👑 {chosen_mention} думает что он лучший!"],
                "альтруист": [f"🤝 Альтруист дня — {chosen_mention}!\n❤️ Помогает всем.",
                               f"🕊 {chosen_mention} — добрый человек этого чата!"],
                # 🆕 Состояния
                "везунчик":  [f"🍀 Везунчик дня — {chosen_mention}!\n🎰 Сегодня его день.",
                               f"✨ {chosen_mention} — баловень судьбы!"],
                "невезучий": [f"🌧 Невезучий дня — {chosen_mention}!\n😬 Всё идёт не так.",
                               f"🪙 У {chosen_mention} сегодня всё падает из рук!"],
                "сонный":    [f"😴 Сонный дня — {chosen_mention}!\n💤 Засыпает прямо в чате.",
                               f"🛌 {chosen_mention} — самый сонный участник дня!"],
                "голодный":  [f"🍕 Голодный дня — {chosen_mention}!\n😋 Думает только о еде.",
                               f"🍔 {chosen_mention} готов съесть весь чат!"],
                "влюблённый":[f"❤️ Влюблённый дня — {chosen_mention}!\n😍 Витает в облаках.",
                               f"💕 {chosen_mention} сегодня думает только о ком-то одном!"],
                "ревнивый":  [f"😤 Ревнивый дня — {chosen_mention}!\n👀 Следит за каждым.",
                               f"💢 {chosen_mention} ревнует весь чат!"],
                "обиженный": [f"😒 Обиженный дня — {chosen_mention}!\n😤 Всё не так.",
                               f"😔 {chosen_mention} сегодня на всех обиделся!"],
                "счастливый":[f"😄 Счастливый дня — {chosen_mention}!\n☀️ Сияет как солнце.",
                               f"🎉 {chosen_mention} сегодня на вершине мира!"],
                "грустный":  [f"😢 Грустный дня — {chosen_mention}!\n🎻 Мир играет грустную мелодию.",
                               f"💧 {chosen_mention} сегодня немного не в духе!"],
                "бешеный":   [f"😡 Бешеный дня — {chosen_mention}!\n🔥 На всё злится.",
                               f"💢 {chosen_mention} сегодня взрывоопасен!"],
                # 🆕 Роли в сообществе
                "фанат":     [f"🎪 Фанат дня — {chosen_mention}!\n🙌 Боготворит кого-то.",
                               f"📣 {chosen_mention} — преданный фанат чата!"],
                "хейтер":    [f"😒 Хейтер дня — {chosen_mention}!\n👎 Ненавидит всё подряд.",
                               f"☠️ {chosen_mention} сегодня хейтит всех!"],
                "критик":    [f"🧐 Критик дня — {chosen_mention}!\n📝 Найдёт изъян в чём угодно.",
                               f"🔍 {chosen_mention} критикует всё что видит!"],
                "защитник":  [f"🛡 Защитник дня — {chosen_mention}!\n⚔️ Стоит за правду.",
                               f"🦸 {chosen_mention} защищает слабых в чате!"],
                "провокатор":[f"😈 Провокатор дня — {chosen_mention}!\n🔥 Специально троллит.",
                               f"💣 {chosen_mention} раздувает конфликты мастерски!"],
                "шутник":    [f"😂 Шутник дня — {chosen_mention}!\n🎭 Шутит даже на похоронах.",
                               f"🃏 {chosen_mention} — главный комик и клоун чата!"],
                "серьёзный": [f"😐 Серьёзный дня — {chosen_mention}!\n📋 Смеётся раз в год.",
                               f"🗿 {chosen_mention} серьёзен как никто другой!"],
                "рандом":    [f"🎲 Рандом дня — {chosen_mention}!\n❓ Никто не знает что от него ждать.",
                               f"🌀 {chosen_mention} — самый непредсказуемый участник!"],
                "легаси":    [f"👴 Легаси дня — {chosen_mention}!\n📜 Старожил, помнит всё.",
                               f"🏛 {chosen_mention} — живая история этого чата!"],
                "ноунейм":   [f"👤 Ноунейм дня — {chosen_mention}!\n❓ Кто это вообще?",
                               f"🌫 {chosen_mention} — самый загадочный участник чата!"],
            }

            texts = IRIS_TEXTS.get(action, [f"🎯 {action.capitalize()} дня — {chosen_mention}!"])
            await reply_auto_delete(message, random.choice(texts), parse_mode="HTML")
        elif action == "статус":
            today = datetime.now().strftime("%d.%m.%Y")
            w_today = sum(1 for uid in warnings[cid] for _ in range(warnings[cid][uid]))
            b_today = len(ban_list[cid])
            history_today = [h for uid_h in mod_history[cid].values() for h in uid_h
                             if h.get("time","").startswith(today)]
            lines = [f"📊 <b>Статус модерации — {today}</b>\n"]
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
                f"🔍 <b>ДОСЬЕ: {tname}</b>\n",
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
            if message.from_user.id not in ADMIN_IDS:
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
                f"💣 <b>Ядерка</b>\n\n👤 {tname}\n"
                f"⚡ Варн выдан\n🔇 Мут навсегда\n🗑 Удалено ~{deleted} сообщений",
                parse_mode="HTML")
            await log_action(f"💣 <b>Ядерка</b>\n👤 {tname}\n🏠 {message.chat.title}")

        elif action == "анонс":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            text = rest.strip()
            if not text:
                await reply_auto_delete(message, "⚠️ Укажи текст: <b>аутист анонс текст</b>", parse_mode="HTML"); return
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"📢 <b>Объявление</b>\n\n{text}\n",
                parse_mode="HTML")

        elif action == "локдаун":
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import time as _t
            spy_targets[f"{cid}_{target.id}"] = OWNER_ID
            await reply_auto_delete(message, f"👁 Слежка за {tname} включена!\nКаждое сообщение придёт тебе в личку.", parse_mode="HTML")

        elif action == "дать репу":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import re as _re
            m2 = _re.match(r"^(-?\d+)", rest)
            amount = int(m2.group(1)) if m2 else 100
            reputation[cid][target.id] += amount
            save_data()
            await reply_auto_delete(message,
                f"💰 {tname}: репа {'+'if amount>0 else ''}{amount} → <b>{reputation[cid][target.id]}</b>", parse_mode="HTML")

        elif action == "хаос":
            if message.from_user.id not in ADMIN_IDS:
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
                await bot.restrict_chat_member(cid, victim, ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(minutes=5))
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 Жертва: {vname}\n🔇 Замучен на 5 мин!", parse_mode="HTML")
            elif roll == "разварн":
                if warnings[cid][victim] > 0: warnings[cid][victim] -= 1; save_data()
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 Счастливчик: {vname}\n🌿 Снят варн!", parse_mode="HTML")
            else:
                await reply_auto_delete(message, f"🌪 <b>ХАОС!</b>\n🎲 {vname} отделался лёгким испугом!", parse_mode="HTML")

        elif action == "сброс":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            warnings[cid][target.id] = 0
            reputation[cid][target.id] = 0
            xp_data[cid][target.id] = 0
            levels[cid][target.id] = 0
            mod_history[cid][target.id] = []
            save_data()
            await reply_auto_delete(message, f"⚙️ {tname} — всё обнулено!\nВарны, репа, XP, история.", parse_mode="HTML")

        elif action == "лотерея":
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            некрологи = [
                f"упал с дивана и не выжил",
                f"был засмеян до смерти",
                f"пропал при невыясненных обстоятельствах",
                f"ушёл в закат и не вернулся",
                f"был похищен инопланетянами навсегда",
            ]
            await reply_auto_delete(message,
                f"💀 <b>Некролог</b>\n\n"
                f"Сегодня наш чат покинул {tname}.\n"
                f"Причина: <i>{random.choice(некрологи)}</i>.\n\n"
                f"😔 Помним. Скорбим. Не забудем.", parse_mode="HTML")

        elif action == "зеркало":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            mirror_chats[cid] = __import__('time').time() + 300
            await reply_auto_delete(message, "🔁 <b>Режим зеркала включён на 5 минут!</b>\nБот будет повторять каждое сообщение.", parse_mode="HTML")

        # ── Новые owner команды ──
        elif action == "скрин":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
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
                f"📸 <b>Статистика чата</b>\n"
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            deleted = 0
            for i in range(message.message_id, max(message.message_id - 50, 0), -1):
                try: await bot.delete_message(cid, i); deleted += 1
                except: pass
            # Отправить уведомление которое тоже удалится
            sent = await bot.send_message(cid, f"🧨 <b>ВЗРЫВ!</b> Удалено {deleted} сообщений.", parse_mode="HTML")
            asyncio.create_task(schedule_delete(sent))

        elif action == "корона":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            crown_holders[cid] = {"uid": target.id, "name": target.full_name, "expire": __import__('time').time() + 86400}
            await bot.send_message(cid,
                f"👑 <b>Король чата</b>\n\n"
                f"Отныне и на 24 часа титул короля носит:\n"
                f"🎖 {tname}\n\n"
                f"Да здравствует король! 👑", parse_mode="HTML")

        elif action == "вызов":
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
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
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            text_loud = rest.strip().upper() if rest.strip() else "ВНИМАНИЕ"
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"🔊 <b>{text_loud}!!!</b>", parse_mode="HTML")

        elif action == "молния":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
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
            if message.from_user.id not in ADMIN_IDS:
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
                f"⚠️ <b>ВНИМАНИЕ!</b>\n\n"
                f"{text_alert}\n",
                parse_mode="HTML")

        elif action == "пересмотр":
            w = warnings[cid].get(target.id, 0)
            history = mod_history[cid].get(target.id, [])
            warn_history = [h for h in history if "варн" in h.get("action","").lower() or "warn" in h.get("action","").lower()]
            lines = [f"🔄 <b>Пересмотр варнов: {tname}</b>\n",
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
                until_date=datetime.now() + timedelta(hours=24))
            save_data()
            await reply_auto_delete(message,
                f"🧊 <b>Заморозка</b>\n\n"
                f"👤 {tname}\n"
                f"❄️ Замолчал на <b>24 часа</b>\n"
                f"🕐 Размут автоматически через 24ч", parse_mode="HTML")
            await log_action(f"🧊 <b>Заморозка</b>\n👤 {tname}\n⏱ 24ч\n👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

        elif action == "объяви":
            violation = rest.strip() or "нарушение правил"
            try: await message.delete()
            except: pass
            await bot.send_message(cid,
                f"📣 <b>ВНИМАНИЕ ЧАТУ!</b>\n\n"
                f"👤 Участник {tname}\n"
                f"📝 Нарушение: <b>{violation}</b>\n\n"
                f"⚠️ Просим соблюдать правила чата → /rules\n"
                f"", parse_mode="HTML")

        elif action == "история":
            all_actions = []
            for uid2, actions in mod_history[cid].items():
                for h in actions:
                    all_actions.append(h)
            all_actions.sort(key=lambda x: x.get("time", ""), reverse=True)
            last20 = all_actions[:20]
            lines = [f"⏳ <b>Последние 20 действий в чате</b>\n"]
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
            await log_action(f"🔒 <b>Только чтение</b>\n👤 {tname}\n👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

        elif action == "топ нарушителей":
            top = sorted(warnings[cid].items(), key=lambda x: x[1], reverse=True)[:10]
            lines = [f"📊 <b>Топ нарушителей чата</b>\n"]
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
            await log_action(f"🤐 <b>Медиамут</b>\n👤 {tname}\n{time_text2}\n👮 {message.from_user.mention_html()}\n🏠 {message.chat.title}")

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
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
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
                    f"📌 <b>Закреплено</b>\n\n{text_pin}\n",
                    parse_mode="HTML")
                await bot.pin_chat_message(cid, sent.message_id, disable_notification=False)

        elif action == "голос":
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            activity = user_activity[cid].get(target.id, {})
            if not activity:
                await reply_auto_delete(message, f"📈 {tname} — нет данных об активности", parse_mode="HTML"); return
            sorted_days = sorted(activity.items())[-14:]  # последние 14 дней
            lines = [f"📈 <b>Активность {tname}</b>\n"]
            max_count = max(c for _, c in sorted_days) or 1
            for date, count in sorted_days:
                bar_len = int((count / max_count) * 12)
                bar = "█" * bar_len + "░" * (12 - bar_len)
                lines.append(f"{date}: {bar} <b>{count}</b>")
            await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

        elif action == "тишина":
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
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
                f"🌡 <b>Температура чата</b>\n\n"
                f"За 10 мин: <b>{active_10}</b> сообщ. — {temp}\n"
                f"За 1 час: <b>{active_60}</b> сообщений\n"
                f"Участников в базе: <b>{len(chat_stats[cid])}</b>",
                parse_mode="HTML")

        elif action == "неделя":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            _td = timedelta
            lines = [f"📊 <b>Итоги недели — {message.chat.title}</b>\n"]
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
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
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
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            await reply_auto_delete(message, "🔄 Перезапускаю бота...", parse_mode="HTML")
            import os, sys
            await asyncio.sleep(1)
            os.execv(sys.executable, [sys.executable] + sys.argv)

        elif action == "сос":
            if message.from_user.id not in ADMIN_IDS:
                await reply_auto_delete(message, "🚫 Только для владельца!"); return
            import time as _ts2
            uptime_sec = int(_ts2.time() - bot_start_time) if 'bot_start_time' in globals() else 0
            h, m = uptime_sec // 3600, (uptime_sec % 3600) // 60
            total_warns_all = sum(sum(u.values()) for u in warnings.values())
            total_bans_all  = sum(len(b) for b in ban_list.values())
            total_users_all = sum(len(u) for u in chat_stats.values())
            await bot.send_message(OWNER_ID,
                f"🚨 <b>SOS — Состояние бота</b>\n\n"
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
        f"⚔️ <b>Дуэль</b>\n"
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

async def cmd_streak(message: Message):
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

    # Читаем актуальные данные из SQLite напрямую
    conn = db_connect()
    xp_row   = conn.execute("SELECT xp FROM xp_data WHERE cid=? AND uid=?", (cid, uid)).fetchone()
    rep_row  = conn.execute("SELECT score FROM reputation WHERE cid=? AND uid=?", (cid, uid)).fetchone()
    warn_row = conn.execute("SELECT count FROM warnings WHERE cid=? AND uid=?", (cid, uid)).fetchone()
    stat_row = conn.execute("SELECT msg_count FROM chat_stats WHERE cid=? AND uid=?", (cid, uid)).fetchone()
    str_row  = conn.execute("SELECT streak FROM streaks WHERE cid=? AND uid=?", (cid, uid)).fetchone()
    # Ранг по XP
    rank_xp_row = conn.execute(
        "SELECT COUNT(*)+1 as r FROM xp_data WHERE cid=? AND xp > (SELECT COALESCE(xp,0) FROM xp_data WHERE cid=? AND uid=?)",
        (cid, cid, uid)).fetchone()
    # Ранг по репе
    rank_rep_row = conn.execute(
        "SELECT COUNT(*)+1 as r FROM reputation WHERE cid=? AND score > (SELECT COALESCE(score,0) FROM reputation WHERE cid=? AND uid=?)",
        (cid, cid, uid)).fetchone()
    conn.close()

    xp     = xp_row["xp"] if xp_row else 0
    rep    = rep_row["score"] if rep_row else 0
    warns  = warn_row["count"] if warn_row else 0
    msgs   = stat_row["msg_count"] if stat_row else 0
    streak = str_row["streak"] if str_row else 0
    rank_xp  = rank_xp_row["r"] if rank_xp_row else "?"
    rank_rep = rank_rep_row["r"] if rank_rep_row else "?"

    # Синхронизируем RAM
    xp_data[cid][uid]      = xp
    reputation[cid][uid]   = rep
    warnings[cid][uid]     = warns
    chat_stats[cid][uid]   = msgs
    streaks[cid][uid]      = streak

    lvl    = get_level(xp)
    levels[cid][uid] = lvl
    emoji_lvl, title_lvl = get_level_title(lvl)

    # Прогресс до следующего уровня
    cur_xp  = LEVEL_XP.get(lvl, 0)
    next_xp = get_xp_for_next(lvl)
    prog    = xp - cur_xp
    needed  = next_xp - cur_xp
    pct     = min(int((prog / needed) * 12), 12) if needed > 0 else 12
    bar     = "█" * pct + "░" * (12 - pct)

    # Доп инфо
    avatar     = avatars.get(str(uid), "👤")
    shop_title = user_titles[uid].get("title", "") if uid in user_titles else ""
    clan_id    = clan_members.get(uid)
    clan_line  = f"🤝 Клан: <b>[{clans[clan_id]['tag']}] {clans[clan_id]['name']}</b>\n" if clan_id and clan_id in clans else ""
    title_line = f"🎭 Титул: <b>{shop_title}</b>\n" if shop_title else ""
    color_badge, _ = get_color_badge(rep)

    # Профиль из SQLite (друзья, отношения, настроение)
    conn2 = db_connect()
    p = conn2.execute("SELECT mood, bio FROM user_profiles WHERE uid=?", (uid,)).fetchone()
    friends_cnt = conn2.execute("SELECT COUNT(*) as c FROM friends WHERE uid=?", (uid,)).fetchone()["c"]
    rel = conn2.execute("SELECT rel_type FROM relationships WHERE uid1=?", (uid,)).fetchone()
    conn2.close()
    mood_line = f"😊 {p['mood']}\n" if p and p["mood"] else ""
    rel_line  = f"❤️ В отношениях\n" if rel else ""

    await reply_auto_delete(message,
        f"{avatar} <b>{user.full_name}</b> {color_badge}\n"
        f"{emoji_lvl} <b>{title_lvl}</b>  •  Уровень <b>{lvl}</b>\n"
        f"<code>[{bar}]</code> {prog}/{needed} XP\n\n"
        f"{mood_line}"
        f"{title_line}"
        f"{clan_line}"
        f"{rel_line}"
        f"⭐ Репутация: <b>{rep:+d}</b>  (#{rank_rep} в чате)\n"
        f"📊 XP ранг: <b>#{rank_xp}</b>  •  Всего XP: <b>{xp}</b>\n"
        f"💬 Сообщений: <b>{msgs}</b>\n"
        f"🔥 Стрик: <b>{streak}</b> дней\n"
        f"⚡ Варнов: <b>{warns}/{MAX_WARNINGS}</b>\n"
        f"👥 Друзей: <b>{friends_cnt}</b>\n",
        parse_mode="HTML")

@dp.message(Command("addrep"))
async def cmd_addrep(message: Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS: return
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

async def cmd_daily(message: Message):
    uid = message.from_user.id; cid = message.chat.id
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
            lines    = [f"📊 <b>Недельная статистика</b>\n"]
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
                ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(minutes=10))
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
    # Синхронизируем с дашбордом
    try:
        chat_title = ""
        try: chat_title = (await bot.get_chat(cid)).title or str(cid)
        except: chat_title = str(cid)
        shared.sync_report(cid, chat_title, idx, report_entry)
    except: pass
    ctx_text = "\n".join(context) if context else "  нет данных"
    is_admin_target = await is_admin_by_id(cid, target_id)
    report_log = (
        f"🚨 <b>Репорт</b> {priority}\n"
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
            f"⚠️ <b>Репорт на администратора</b>\n🎯 {target_name}\n{cat_emoji} {cat_label}\n📝 {reason}\nТребует 2 голоса:",
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
                        f"🚨 <b>Новый репорт</b> {priority}\n{cat_emoji} {cat_label}\n🎯 На: <b>{target_name}</b>\n📝 {reason}\n💬 {call.message.chat.title}",
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
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
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
            await bot.restrict_chat_member(cid, target_id, ChatPermissions(can_send_messages=False), until_date=datetime.now() + timedelta(hours=1))
            result = "🔇 Мут 1ч"
        elif action == "ban":
            await bot.ban_chat_member(cid, target_id); result = "🔨 Бан"
        report["status"] = "accepted"; report["assigned_mod"] = mod_name
        report_mod_stats[cid][call.from_user.id] += 1
        save_data()
        try: await bot.send_message(report["reporter"], f"✅ Жалоба на <b>{report['target_name']}</b> принята! {result}", parse_mode="HTML")
        except: pass
        await log_action(f"✅ <b>Репорт обработан</b>\n🎯 {report['target_name']}\n👮 {mod_name}\n⚙️ {result}")
        await call.message.edit_text(f"✅ <b>Обработано</b>\n{result}\n👮 {mod_name}", parse_mode="HTML")
        await call.answer("✅ Выполнено.")
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
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    queue = report_queue.get(cid, [])
    from collections import Counter
    total = len(queue)
    new_r = sum(1 for r in queue if r.get("status") == "new")
    done_r = sum(1 for r in queue if r.get("status") == "accepted")
    rej_r = sum(1 for r in queue if r.get("status") == "rejected")
    top_targets = Counter(r["target_name"] for r in queue).most_common(5)
    lines = [f"📊 <b>Статистика репортов</b>\n",
             f"📋 Всего: <b>{total}</b> | 🆕 Новых: <b>{new_r}</b>",
             f"✅ Принято: <b>{done_r}</b> | ❌ Отклонено: <b>{rej_r}</b>\n",
             "🎯 <b>Чаще жалуются на:</b>"]
    for name, cnt in top_targets:
        lines.append(f"  ▸ {name} — {cnt}x")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")
async def cmd_chatstats(message: Message):
    cid = message.chat.id
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
                "📊  <b>Статистика чата</b>",
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

async def cmd_mystats(message: Message):
    uid = message.from_user.id
    cid = message.chat.id
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
                "📈  <b>Моя статистика</b>",
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

async def cmd_topactive(message: Message):
    cid = message.chat.id
    today = datetime.now().strftime("%d.%m.%Y")
    today_scores = [
        (uid, days.get(today, 0))
        for uid, days in daily_stats[cid].items()
        if days.get(today, 0) > 0
    ]
    today_scores.sort(key=lambda x: x[1], reverse=True)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["🔥  <b>Топ активных сегодня</b>", ""]
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

async def cmd_role(message: Message):
    uid = message.from_user.id
    cid = message.chat.id
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
        f"🎭  <b>Роль дня</b>\n"
        f"👤 {message.from_user.mention_html()}\n\n"
        f"Сегодня ты — {emoji} <b>{role}</b>!\n\n"
        f"<i>Роль меняется каждый день</i>",
        parse_mode="HTML")

# ===== 📸 МЕМ-ГЕНЕРАТОР =====
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
    if message.from_user.id not in ADMIN_IDS:
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
        f"📢  <b>Рассылка завершена</b>\n"
        f"✅ Доставлено: <b>{sent_ok}</b> чатов\n"
        f"❌ Ошибок: <b>{sent_fail}</b>\n"
        f"📊 Всего чатов: <b>{len(known_chats)}</b>",
        parse_mode="HTML")

@dp.message(Command("chats"))
async def cmd_chats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await reply_auto_delete(message, "🚫 Только для владельца бота!"); return
    lines = [
                "🌐  <b>Чаты бота</b>",
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
async def cmd_ref(message: Message):
    uid = str(message.from_user.id)
    bot_me = await bot.get_me()
    ref_link = f"https://t.me/{bot_me.username}?start=ref_{uid}"
    invited = len(referrals.get(uid, set()))
    await reply_auto_delete(message,

        f"👥 Ты пригласил: <b>{invited}</b> чел.\n"
        f"💰 Заработано: <b>{invited * 30}</b> репы\n\n"
        f"🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
        f"<i>За каждого приглашённого +30 репы!</i>",
        parse_mode="HTML")

@dp.message(Command("start"))
async def cmd_start_ref(message: Message, command: CommandObject):
    uid = message.from_user.id
    name = message.from_user.first_name
    cid  = message.chat.id

    # ── /start ticket — открыть тикет сразу ──────────────
    if command.args == "ticket" and message.chat.type == "private":
        chats = await db.get_all_chats()
        if not chats:
            chat_list = [(c, t) for c, t in known_chats.items()]
        else:
            chat_list = [(r["cid"], r["title"]) for r in chats]
        await tkt.cmd_ticket(message, bot, chat_list)
        return

    # ── /start ref_XXX — реферальная система ─────────────
    if command.args and command.args.startswith("ref_"):
        inviter_id = command.args.replace("ref_", "").strip()
        uid_str = str(uid)
        if uid_str != inviter_id and uid_str not in referral_used:
            referral_used[uid_str] = inviter_id
            referrals[inviter_id].add(uid_str)
            reputation[cid][int(inviter_id)] = reputation[cid].get(int(inviter_id), 0) + 30
            save_data()
            try:
                await bot.send_message(int(inviter_id),
                    f"✅ <b>{message.from_user.full_name}</b> перешёл по вашей реферальной ссылке.\n"
                    f"<b>+30 к репутации</b> начислено.", parse_mode="HTML")
            except: pass

    # ── В группе — кнопка перейти в ЛС ───────────────────
    if message.chat.type in ("group", "supergroup"):
        bot_info = await bot.get_me()
        await message.answer(
            f"<b>CHAT GUARD</b>\n\n"
            f"Здравствуйте, <b>{name}</b>.\n\n"
            f"Я — система автоматической модерации этого чата. "
            f"По вопросам поддержки обращайтесь в личные сообщения.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Написать в поддержку",
                    url=f"https://t.me/{bot_info.username}?start=hello"
                )
            ]])
        )
        return

    # ── В ЛС — красивое главное меню ─────────────────────
    import time as _ts
    uptime = int(_ts.time() - bot_start_time)
    h, m = uptime // 3600, (uptime % 3600) // 60

    # Статы пользователя
    user_xp    = xp_data[cid].get(uid, 0) if cid in xp_data else 0
    user_warns = sum(warnings[c].get(uid, 0) for c in warnings)
    user_lvl   = get_level(user_xp) if user_xp else 0
    _, lvl_title = get_level_title(user_lvl)

    await message.answer(
        f"🛡 <b>CHAT GUARD</b>\n"
        f"<i>Система управления и модерации</i>\n\n"
        f"Здравствуйте, <b>{name}</b>.\n\n"
        f"<b>Ваш профиль</b>\n"
        f"· Уровень: <b>{user_lvl}</b> — {lvl_title}\n"
        f"· Очки опыта: <b>{user_xp} XP</b>\n"
        f"· Предупреждений: <b>{user_warns}</b>\n\n"
        f"<b>Возможности системы</b>\n"
        f"· Модерация — предупреждения, ограничения, блокировки\n"
        f"· XP и уровни — прогрессия до 500 уровня\n"
        f"· Тикеты — система обращений в поддержку\n"
        f"· Дашборд — веб-панель администратора\n\n"
        f"<i>Аптайм: {h}ч {m}м</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Команды",          callback_data="start:help"),
             InlineKeyboardButton(text="Мой профиль",      callback_data="start:profile")],
            [InlineKeyboardButton(text="Открыть тикет",    callback_data="start:ticket"),
             InlineKeyboardButton(text="Правила",           callback_data="start:rules")],
            [InlineKeyboardButton(text="Мои друзья",        callback_data="start:friends"),
             InlineKeyboardButton(text="Комплимент",         callback_data="start:compliment")],
            [InlineKeyboardButton(text="Веб-дашборд",
             url=f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME','mybot.onrender.com')}/")],
        ])
    )

@dp.callback_query(F.data.startswith("start:"))
async def cb_start_menu(call: CallbackQuery):
    action = call.data.split(":")[1]
    uid = call.from_user.id
    name = call.from_user.first_name

    if action == "help":
        await call.message.edit_text(
            f"📋 <b>Основные команды</b>\n"
            f"👤 <b>Профиль</b>\n"
            f"▸ /profile — мой профиль\n"
            f"▸ /setbio — установить био\n"
            f"▸ /setmood — настроение\n\n"
            f"👥 <b>Социалка</b>\n"
            f"▸ /addfriend — добавить друга\n"
            f"▸ /propose — предложить отношения\n"
            f"▸ /gift 🌹 — подарить\n\n"
            f"🛠 <b>Утилиты</b>\n"
            f"▸ /music — поиск трека\n"
            f"▸ /imagine — генерация картинки\n"
            f"▸ /tr — перевод\n"
            f"▸ /idea — тема для обсуждения\n\n"
            f"🎫 <b>Поддержка</b>\n"
            f"▸ /ticket — открыть тикет\n"
            f"▸ /appeal — апелляция\n\n"
            f"💡 В чате доступно ещё /help",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="start:back")
            ]])
        )

    elif action == "profile":
        cid = call.message.chat.id
        user_xp = xp_data[cid].get(uid, 0) if cid in xp_data else 0
        user_lvl = get_level(user_xp)
        _, lvl_title = get_level_title(user_lvl)
        conn = db_connect()
        p = conn.execute("SELECT mood, bio FROM user_profiles WHERE uid=?", (uid,)).fetchone()
        friends = conn.execute("SELECT COUNT(*) as c FROM friends WHERE uid=?", (uid,)).fetchone()["c"]
        conn.close()
        mood = p["mood"] if p and p["mood"] else "😐 Нормально"
        bio  = p["bio"]  if p and p["bio"]  else "Не указано"
        await call.message.edit_text(
            f"👤 <b>Профиль — {name}</b>\n"
            f"🆔 ID: <code>{uid}</code>\n"
            f"🏅 Уровень: <b>{user_lvl}</b> — {lvl_title}\n"
            f"⭐ XP: <b>{user_xp}</b>\n"
            f"{mood}\n"
            f"📝 Био: {bio}\n"
            f"👥 Друзей: <b>{friends}</b>\n",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="start:back")
            ]])
        )

    elif action == "ticket":
        chats = await db.get_all_chats()
        chat_list = [(r["cid"], r["title"]) for r in chats] if chats else list(known_chats.items())
        await call.message.delete()
        await tkt.cmd_ticket(call.message, bot, chat_list)

    elif action == "rules":
        await call.message.edit_text(
            f"📄 <b>Правила чата</b>\n"
            f"🔞 Контент 18+ — варн → бан\n"
            f"🥴 Наркотики — бан без предупреждений\n"
            f"📢 Реклама — мут / бан\n"
            f"🛡 Оскорбление администрации — варн → бан\n"
            f"🚫 Спам и флуд — мут\n\n"
            f"🔒 <b>Конфиденциальность</b>\n"
            f"▸ Бот не хранит личные данные\n"
            f"▸ Анонимность соблюдается\n"
            f"▸ Данные только для модерации\n\n"
            f"<i>Соблюдай правила — чат будет комфортным 🤝</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="start:back")
            ]])
        )

    elif action == "friends":
        conn = db_connect()
        rows = conn.execute("SELECT friend_name FROM friends WHERE uid=? LIMIT 10", (uid,)).fetchall()
        reqs = conn.execute("SELECT COUNT(*) as c FROM friend_requests WHERE to_uid=?", (uid,)).fetchone()["c"]
        conn.close()
        if not rows:
            text = f"👥 <b>Друзья</b>\n\nДрузей нет пока 😔\nДобавь через /addfriend в чате!"
        else:
            text = f"👥 <b>Друзья ({len(rows)})</b>\n\n"
            text += "\n".join(f"▸ {r['friend_name']}" for r in rows)
        if reqs:
            text += f"\n\n📨 Запросов в ожидании: <b>{reqs}</b>"
        await call.message.edit_text(text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="start:back")
            ]]))

    elif action == "compliment":
        import random as _r
        comp = _r.choice(COMPLIMENTS)
        await call.answer(f"💌 {comp}", show_alert=True)

    elif action == "back":
        import time as _ts2
        uptime = int(_ts2.time() - bot_start_time)
        h, m = uptime // 3600, (uptime % 3600) // 60
        cid2 = call.message.chat.id
        user_xp2 = xp_data[cid2].get(uid, 0) if cid2 in xp_data else 0
        user_lvl2 = get_level(user_xp2)
        _, lvl_title2 = get_level_title(user_lvl2)
        await call.message.edit_text(
            f"👋 Привет, <b>{name}</b>!\n\n"
            f"🏅 Уровень: <b>{user_lvl2}</b> — {lvl_title2}\n"
            f"⭐ XP: <b>{user_xp2}</b>\n"
            f"🤖 Я умный бот-модератор с кучей функций:\n\n"
            f"🛡 <b>Модерация</b> — варны, муты, баны\n"
            f"⭐ <b>XP и уровни</b> — 500 уровней\n"
            f"👥 <b>Социалка</b> — друзья, отношения\n"
            f"🎮 <b>Развлечения</b> — 55+ аутист-команд\n"
            f"🎫 <b>Тикеты</b> — поддержка через ЛС\n\n"
            f"⏱ Аптайм: <b>{h}ч {m}м</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Команды",       callback_data="start:help"),
                 InlineKeyboardButton(text="👤 Мой профиль",   callback_data="start:profile")],
                [InlineKeyboardButton(text="Открыть тикет", callback_data="start:ticket"),
                 InlineKeyboardButton(text="📜 Правила",        callback_data="start:rules")],
                [InlineKeyboardButton(text="👥 Мои друзья",     callback_data="start:friends"),
                 InlineKeyboardButton(text="💌 Комплимент",      callback_data="start:compliment")],
                [InlineKeyboardButton(text="🌐 Дашборд",
                 url=f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME','mybot.onrender.com')}/")],
            ])
        )
    await call.answer()

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
async def cmd_boost(message: Message, command: CommandObject):
    uid = str(message.from_user.id)
    cid = message.chat.id
    if not command.args:
        from time import time
        now = time()
        lines = [
                        "🍭  <b>Магазин бустеров</b>",
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
        f"🎲  <b>Рулетка</b>\n"
        f"{spin_display}\n\n"
        f"🎯 Ставка: <b>{bet}</b> репы\n\n"
        f"{outcome}",
        parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🧙 МАГИЧЕСКИЙ АРТЕФАКТ
# ══════════════════════════════════════════════════════
artifact_cd = {}

async def cmd_artifact(message: Message):
    uid = str(message.from_user.id)
    cid = message.chat.id
    from time import time
    now = time()
    # Показать инвентарь артефактов
    inv = artifacts.get(uid, [])
    if inv:
        lines = ["🧙  <b>Артефакты</b>", ""]
        for a in inv:
            lines.append(f"{a['emoji']} <b>{a['name']}</b> [{a['rarity']}]\n   {a['effect']}")
        lines.append(f"\n🎰 /artifact_roll — попытать удачу (кулдаун 6ч, стоит 100 репы)")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")
    else:
        await reply_auto_delete(message,
            "🧙 У тебя нет артефактов!\n\n"
            "🎰 Используй <code>/artifact_roll</code> чтобы попытать удачу!\nСтоит 100 репы, кулдаун 6 часов.",
            parse_mode="HTML")

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
            f"🧙  <b>АРТЕФАКТ НАЙДЕН!</b>\n"
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
async def cmd_lottery(message: Message):
    cid = message.chat.id
    uid = message.from_user.id
    today = datetime.now().strftime("%d.%m.%Y")
    tickets = lottery_tickets[cid]
    participants = len(tickets)
    prize = participants * 20
    last = lottery_last.get(cid)
    already_in = uid in tickets
    await reply_auto_delete(message,
        f"🎰  <b>Лотерея</b>\n"
        f"🎫 Участников: <b>{participants}</b>\n"
        f"💰 Призовой фонд: <b>{prize}</b> репы\n"
        f"📅 Розыгрыш: сегодня в 23:00\n\n"
        f"{'✅ Ты уже участвуешь!' if already_in else 'Купить билет: /lottery_buy (цена: 20 репы)'}",
        parse_mode="HTML")

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

async def cmd_stock(message: Message):
    cid = message.chat.id
    uid = message.from_user.id
    invested = stock_invested[cid].get(uid, 0)
    rep = reputation[cid].get(uid, 0)
    await reply_auto_delete(message,
        f"📈  <b>Биржа репутации</b>\n"
        f"💰 Твоя репа: <b>{rep}</b>\n"
        f"📊 Вложено: <b>{invested}</b>\n\n"
        f"📉 Риск: каждый день в 20:00 биржа выплачивает\n"
        f"от -50% до +100% от вложенной суммы\n\n"
        f"/stock_invest [сумма] — вложить\n"
        f"/stock_withdraw — вывести всё",
        parse_mode="HTML")

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
async def cmd_quote_save(message: Message):
    if not message.reply_to_message or not message.reply_to_message.text:
        await reply_auto_delete(message, "💬 Реплайни на текстовое сообщение!"); return
    cid = message.chat.id
    author = message.reply_to_message.from_user
    text = message.reply_to_message.text
    if len(text) > 300:
        await reply_auto_delete(message, "❌ Цитата слишком длинная (макс 300 символов)"); return
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

async def cmd_quote_random(message: Message):
    cid = message.chat.id
    if not quotes_data[cid]:
        await reply_auto_delete(message, "💬 Цитат пока нет! Сохрани через /quote_save (реплай)"); return
    q = random.choice(quotes_data[cid])
    await reply_auto_delete(message,
        f"💬  <b>Цитата ча та</b>\n"
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
    lines = [f"💬  <b>Цитатник</b> ({total} цитат)\n"]
    for q in reversed(last5):
        lines.append(f"«{q['text'][:80]}{'...' if len(q['text'])>80 else ''}»\n— {q['author']}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  📝 ДНЕВНИК ЧАТА
# ══════════════════════════════════════════════════════
async def cmd_journal(message: Message, command: CommandObject):
    uid = str(message.from_user.id)
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
    lines = [f"📝  <b>Мой дневник</b> ({len(entries)} записей)\n"]
    for e in reversed(entries[-5:]):
        lines.append(f"🗓 <b>{e['date']}</b>\n{e['text']}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════
#  🧩 ВИКТОРИНА
# ══════════════════════════════════════════════════════
async def cmd_trivia(message: Message):
    cid = message.chat.id
    if cid in trivia_active:
        q = trivia_active[cid]
        await reply_auto_delete(message,
            f"❓ Уже идёт викторина!\n\n<b>{q['q']}</b>\n\nОтвечай в чате!", parse_mode="HTML"); return
    question, answer, reward = random.choice(TRIVIA_QUESTIONS)
    trivia_active[cid] = {"q": question, "a": answer.lower(), "reward": reward, "answerer": None}
    await answer_auto_delete(
        f"🧩  <b>Викторина</b>\n"
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

@dp.message(F.text & ~F.text.startswith("/") & F.chat.type.in_({"group", "supergroup"}))
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
                f"🤝  <b>КЛАН {c['tag']}</b>\n"
                f"🏷 Название: <b>{c['name']}</b>\n"
                f"👥 Участников: <b>{len(c['members'])}</b>\n"
                f"💰 Суммарная репа: <b>{members_rep}</b>\n\n"
                f"/clan_leave — покинуть клан",
                parse_mode="HTML")
        else:
            await reply_auto_delete(message,
                "🤝 <b>Кланы</b>\n\n"
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
    lines = ["🤝  <b>Топ кланов</b>", ""]
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

async def cmd_topxp(message: Message):
    cid = message.chat.id
    if not xp_data[cid]:
        await reply_auto_delete(message, "📊 XP пока нет!"); return
    sorted_u = sorted(xp_data[cid].items(), key=lambda x: x[1], reverse=True)[:10]
    medals   = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines    = [
        "✨ <b>CHAT GUARD</b> — Топ XP",
        "",
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

    # Команды обрабатываются отдельно — кроме /ticket
    if text.startswith("/") and not text.startswith("/ticket"):
        return

    # ── /ticket команда ───────────────────────────────────
    if text.startswith("/ticket"):
        chats = await db.get_all_chats()
        if not chats:
            chat_list = [(cid, title) for cid, title in known_chats.items()]
        else:
            chat_list = [(r["cid"], r["title"]) for r in chats]
        if not chat_list:
            await message.answer(
                "❌ Бот ещё не добавлен ни в один чат.\n"
                "Сначала добавь бота в группу и напиши там любое сообщение.",
                parse_mode="HTML"
            )
            return
        await tkt.cmd_ticket(message, bot, chat_list)
        return

    # ── Система тикетов (приоритет) ──────────────────────
    if await tkt.handle_dm_message(message, bot, notify_mods_func=_notify_mods_ticket):
        return

    # ── Ответ модератора на тикет ─────────────────────────
    if uid in ADMIN_IDS or uid in tkt.mod_reply_states:
        if await tkt.handle_mod_reply(message, bot):
            return

    # ── Анонимные ответы и ящики ──────────────────────────
    if await features.handle_anon_reply_text(message):
        return
    if await features.handle_anonbox_reply(message):
        return

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
    lines = ["✨ <b>CHAT GUARD</b> — Карта активности"]
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
                    f"🗞 <b>Газета чата</b> — {dt.datetime.now().strftime('%d.%m.%Y')}\n"
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
        await log_action(f"🔕 <b>Тихий бан</b>\n👤 Модер: {call.from_user.mention_html()}\n🎯 Цель: ID{tid}\n💬 Чат: {call.message.chat.title}", parse_mode="HTML")
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
    lines = ["✨ <b>CHAT GUARD</b> — Отчёт модерации\n"]
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
        await call.answer("✅ Выполнено.")
    except Exception as e:
        await call.answer(f"❌ {e}", show_alert=True)

@dp.callback_query(F.data.startswith("panel:reports:"))
async def cb_panel_reports(call: CallbackQuery):
    if not await check_admin(call.message): return
    cid = call.message.chat.id
    queue = report_queue.get(cid, [])
    if not queue:
        await call.answer("✅ Жалоб нет!", show_alert=True); return
    text_lines = [f"🚨 <b>Очередь жалоб</b> ({len(queue)} шт.)\n"]
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
            "💰 <b>Экономика</b>\n\n"
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
            "🍬 <b>Магазин подарков</b>\n",
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
    if message.from_user.id not in ADMIN_IDS:
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
    await reply_auto_delete(message, f"💣 <b>Ядерка</b>\n\n👤 {tname}\n⚡ Варн | 🔇 Мут | 🗑 ~{deleted} сообщ.", parse_mode="HTML")

@dp.message(Command("sbros"))
async def cmd_sbros_slash(message: Message):
    if message.from_user.id not in ADMIN_IDS:
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
    cid = message.chat.id; today = datetime.now().strftime("%d.%m.%Y")
    w_today = sum(warnings[cid].values())
    b_today = len(ban_list[cid])
    history_today = [h for uid_h in mod_history[cid].values() for h in uid_h if h.get("time","").startswith(today)]
    lines = [f"📊 <b>Статус модерации — {today}</b>\n"]
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
        f"📢 <b>Объявление</b>\n\n{text}\n",
        parse_mode="HTML")

@dp.message(Command("corona", "корона"))
async def cmd_corona_slash(message: Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        await reply_auto_delete(message, "🚫 Только для владельца!"); return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение!"); return
    target = message.reply_to_message.from_user; cid = message.chat.id
    import time as _t
    crown_holders[cid] = {"uid": target.id, "name": target.full_name, "expire": _t.time() + 86400}
    await bot.send_message(cid,
        f"👑 <b>Король чата</b>\n\nОтныне и на 24 часа:\n🎖 {target.mention_html()}\n\nДа здравствует король! 👑",
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
    if message.from_user.id not in ADMIN_IDS:
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
        f"🔐 <b>Роль выдана</b>\n"
        f"👤 {target.full_name}\n"
        f"🎖 Роль: {MOD_ROLE_LABELS[role_arg]}\n"
        f"👑 Кем: {message.from_user.full_name}\n"
        f"💬 Чат: {message.chat.title}")

@dp.message(Command("takerole"))
async def cmd_take_role(message: Message):
    if message.from_user.id not in ADMIN_IDS:
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
    lines = ["🔐 <b>Роли модераторов</b>\n"]
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
    if message.from_user.id not in ADMIN_IDS: return
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
        "🧩 <b>Управление плагинами</b>\n"
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
    if message.from_user.id not in ADMIN_IDS: return
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
            ban_list[cid][uid] = True
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
        f"🔁 <b>Апелляция</b>\n"
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
                ban_list[cid].pop(uid, None)
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
    lines = [f"👤 <b>Твой профиль</b>\n"]
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
    if message.from_user.id not in ADMIN_IDS:
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
        f"🌍 <b>Мульти-чат панель</b>\n"
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
            f"💬 <b>{title}</b>\n"
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
            f"🌍 <b>Мульти-чат панель</b>\n"
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
    lines = [f"📦 <b>Архив репортов</b> ({len(closed)} шт.)\n"]
    for r in closed[-10:]:
        status_icon = "✅" if r.get("status") == "accepted" else "❌"
        import datetime as _dt2
        ts = _dt2.datetime.fromtimestamp(r.get("ts", 0)).strftime("%d.%m %H:%M")
        lines.append(
            f"{status_icon} <b>{r.get('target_name','?')}</b> — {r.get('category','?')}\n"
            f"  📝 {r.get('text','')[:60]}\n"
            f"  👮 {r.get('assigned_mod','?')} | 🕐 {ts}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  📢 БРОДКАСТ — сообщение во все чаты
# ══════════════════════════════════════════════════════════
@dp.message(Command("broadcast2"))
async def cmd_broadcast_all(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    text = message.text.replace("/broadcast2", "").strip()
    if not text:
        await reply_auto_delete(message,
            "📢 <b>Использование:</b>\n<code>/broadcast2 текст</code>\n\nОтправит во все чаты бота",
            parse_mode="HTML"); return
    success, fail = 0, 0
    status_msg = await message.answer(f"📢 Отправляю в {len(known_chats)} чатов...")
    for cid in list(known_chats.keys()):
        try:
            await bot.send_message(cid,
                f"📢 <b>Сообщение от владельца</b>\n\n{text}",
                parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.1)  # антифлуд
        except: fail += 1
    await status_msg.edit_text(
        f"📢 <b>Бродкаст завершён</b>\n✅ Доставлено: {success}\n❌ Ошибок: {fail}",
        parse_mode="HTML")
    asyncio.create_task(schedule_delete(status_msg, 30))

# ══════════════════════════════════════════════════════════
#  🔒 ГЛОБАЛЬНЫЙ ЧЁРНЫЙ СПИСОК
# ══════════════════════════════════════════════════════════
@dp.message(Command("blacklist"))
async def cmd_blacklist(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    target = message.reply_to_message.from_user if message.reply_to_message else None
    if not target:
        # Показать список
        if not global_blacklist:
            await reply_auto_delete(message, "🔒 Чёрный список пуст"); return
        lines = [f"🔒 <b>Глобальный чёрный список</b> ({len(global_blacklist)} чел.)\n"]
        for uid2 in list(global_blacklist)[:20]:
            lines.append(f"▸ <code>{uid2}</code>")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML"); return
    uid = target.id
    if uid in global_blacklist:
        await reply_auto_delete(message, f"⚠️ {target.full_name} уже в чёрном списке"); return
    global_blacklist.add(uid)
    save_data()
    # Банить во всех чатах
    banned_in = 0
    for cid in list(known_chats.keys()):
        try:
            await bot.ban_chat_member(cid, uid)
            ban_list[cid][uid] = True
            banned_in += 1
            await asyncio.sleep(0.05)
        except: pass
    await reply_auto_delete(message,
        f"🔒 {target.mention_html()} добавлен в <b>глобальный чёрный список</b>\n"
        f"🔨 Забанен в {banned_in} чатах навсегда",
        parse_mode="HTML")
    await log_action(
        f"🔒 <b>Глобальный бан</b>\n👤 {target.full_name} (<code>{uid}</code>)\n"
        f"🔨 Забанен в {banned_in} чатах\n👑 {message.from_user.full_name}")

@dp.message(Command("unblacklist"))
async def cmd_unblacklist(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    target = message.reply_to_message.from_user if message.reply_to_message else None
    if not target:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение юзера"); return
    global_blacklist.discard(target.id)
    save_data()
    await reply_auto_delete(message,
        f"✅ {target.mention_html()} удалён из чёрного списка", parse_mode="HTML")

# Автобан при входе если в чёрном списке
@dp.chat_member()
async def check_blacklist_on_join(event):
    if not event.new_chat_member: return
    uid = event.new_chat_member.user.id
    if uid in global_blacklist:
        try:
            await bot.ban_chat_member(event.chat.id, uid)
        except: pass

# ══════════════════════════════════════════════════════════
#  🌍 АУДИТ ЧАТА
# ══════════════════════════════════════════════════════════
@dp.message(Command("audit"))
async def cmd_audit(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    lines = [f"🔍 <b>Аудит чата: {message.chat.title}</b>\n"]
    # Топ нарушителей
    top_warn = sorted(warnings[cid].items(), key=lambda x: x[1], reverse=True)[:5]
    lines.append("⚡ <b>Топ нарушителей:</b>")
    for uid2, w in top_warn:
        try: tm = await bot.get_chat_member(cid, uid2); n = tm.user.full_name
        except: n = f"ID{uid2}"
        lines.append(f"  ▸ {n} — {w} варнов")
    # Неактивные (0 сообщений)
    inactive = [uid2 for uid2 in chat_stats[cid] if chat_stats[cid][uid2] == 0]
    lines.append(f"\n😴 Неактивных юзеров: <b>{len(inactive)}</b>")
    # Забаненные
    lines.append(f"🔨 Забанено: <b>{len(ban_list[cid])}</b>")
    # Репорты
    open_reports = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
    lines.append(f"🚨 Открытых репортов: <b>{open_reports}</b>")
    # Роли
    lines.append(f"🎖 Модераторов с ролями: <b>{len(mod_roles.get(cid, {}))}</b>")
    # Плагины
    disabled = [l for k, l in PLUGIN_LABELS.items() if not plugins[cid].get(k, True)]
    if disabled:
        lines.append(f"🧩 Отключены: {', '.join(disabled)}")
    await bot.send_message(OWNER_ID, "\n".join(lines), parse_mode="HTML")
    await reply_auto_delete(message, "🔍 Аудит отправлен в личку!", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  ⚙️ БЭКАП И ВОССТАНОВЛЕНИЕ
# ══════════════════════════════════════════════════════════
@dp.message(Command("backup"))
async def cmd_backup(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    import json, io
    backup_data = {
        "warnings": {str(c): {str(u): v for u, v in d.items()} for c, d in warnings.items()},
        "reputation": {str(c): {str(u): v for u, v in d.items()} for c, d in reputation.items()},
        "xp_data": {str(c): {str(u): v for u, v in d.items()} for c, d in xp_data.items()},
        "ban_list": {str(c): list(v) for c, v in ban_list.items()},
        "global_blacklist": list(global_blacklist),
        "known_chats": {str(k): v for k, v in known_chats.items()},
        "mod_roles": {str(c): {str(u): r for u, r in d.items()} for c, d in mod_roles.items()},
    }
    json_bytes = json.dumps(backup_data, ensure_ascii=False, indent=2).encode("utf-8")
    buf = io.BytesIO(json_bytes)
    buf.name = "backup.json"
    fname = f"backup_{datetime.now().strftime('%d%m%Y_%H%M')}.json"
    buf.name = fname
    await bot.send_document(OWNER_ID, buf,
        caption=f"💾 <b>Бэкап базы</b>\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                f"💬 Чатов: {len(known_chats)}\n👥 Юзеров: {sum(len(d) for d in chat_stats.values())}",
        parse_mode="HTML")
    await reply_auto_delete(message, "💾 Бэкап отправлен в личку!", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  🗑 СБРОС ДАННЫХ ЧАТА
# ══════════════════════════════════════════════════════════
@dp.message(Command("resetchat"))
async def cmd_reset_chat(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    # Подтверждение
    pending[message.from_user.id] = {"action": "confirm_reset", "chat_id": cid}
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💥 ДА, СБРОСИТЬ", callback_data=f"resetchat:yes:{cid}"),
        InlineKeyboardButton(text="❌ Отмена",         callback_data=f"resetchat:no:{cid}"),
    ]])
    await reply_auto_delete(message,
        f"⚠️ <b>Сброс данных чата</b>\n\n"
        f"Будут удалены: варны, репа, XP, история, статистика\n"
        f"💬 Чат: <b>{message.chat.title}</b>\n\n<b>Уверен?</b>",
        parse_mode="HTML")
    sent = await message.answer("👇", reply_markup=kb)
    asyncio.create_task(schedule_delete(sent, 30))

@dp.callback_query(F.data.startswith("resetchat:"))
async def cb_reset_chat(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫", show_alert=True); return
    parts = call.data.split(":")
    action, cid = parts[1], int(parts[2])
    if action == "no":
        await call.message.delete(); await call.answer("Отменено"); return
    # Сброс
    warnings[cid].clear()
    reputation[cid].clear()
    xp_data[cid].clear()
    chat_stats[cid].clear()
    levels[cid].clear()
    mod_history[cid].clear()
    report_queue[cid].clear()
    ban_list[cid].clear()
    save_data()
    await call.message.edit_text(
        f"💥 <b>Данные чата сброшены!</b>\n💬 {known_chats.get(cid, cid)}",
        parse_mode="HTML")
    await log_action(f"💥 <b>Сброс данных чата</b>\n💬 {known_chats.get(cid, cid)}\n👑 {call.from_user.full_name}")
    await call.answer("✅ Сброшено")

# ══════════════════════════════════════════════════════════
#  💣 ЭКСТРЕННЫЕ КОМАНДЫ
# ══════════════════════════════════════════════════════════

# SOS ALL — локдаун всех чатов
@dp.message(Command("sosall"))
async def cmd_sos_all(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    locked, fail = 0, 0
    for cid in list(known_chats.keys()):
        try:
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
            locked += 1
            await asyncio.sleep(0.1)
        except: fail += 1
    await reply_auto_delete(message,
        f"🚨 <b>SOS — ЛОКДАУН ВСЕХ ЧАТОВ</b>\n"
        f"🔒 Заблокировано: {locked}\n❌ Ошибок: {fail}",
        parse_mode="HTML")
    await log_action(f"🚨 <b>SOS ALL</b>\n🔒 {locked} чатов заблокированы\n👑 {message.from_user.full_name}")

# РАЗЛОКДАУН всех чатов
@dp.message(Command("sosoff"))
async def cmd_sos_off(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    unlocked = 0
    for cid in list(known_chats.keys()):
        try:
            await bot.set_chat_permissions(cid, ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True, can_invite_users=True))
            unlocked += 1
            await asyncio.sleep(0.1)
        except: pass
    await reply_auto_delete(message,
        f"✅ <b>Все чаты разблокированы</b> ({unlocked})", parse_mode="HTML")

# ЭВАКУАЦИЯ — удалить всех кто зашёл за последний час
@dp.message(Command("evacuation"))
async def cmd_evacuation(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    import time as _tev
    now = _tev.time()
    kicked = 0
    # Кикаем всех кто зашёл за последний час (нет в last_seen или last_seen < 1ч)
    new_users = [uid2 for uid2, ts in last_seen.get(cid, {}).items()
                 if now - ts < 3600]
    for uid2 in new_users:
        if await is_admin_by_id(cid, uid2): continue
        try:
            await bot.ban_chat_member(cid, uid2)
            await bot.unban_chat_member(cid, uid2)
            kicked += 1
            await asyncio.sleep(0.05)
        except: pass
    await reply_auto_delete(message,
        f"🚁 <b>Эвакуация</b>\n"
        f"🚪 Удалено {kicked} юзеров зашедших за последний час",
        parse_mode="HTML")
    await log_action(f"🚁 <b>Эвакуация</b>\n🚪 {kicked} юзеров\n💬 {message.chat.title}\n👑 {message.from_user.full_name}")

# КАРАНТИН — автомут всех новых участников
@dp.message(Command("quarantine"))
async def cmd_quarantine(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    if cid in quarantine_chats:
        quarantine_chats.discard(cid)
        await reply_auto_delete(message, "✅ Карантин снят — новые участники могут писать", parse_mode="HTML")
    else:
        quarantine_chats.add(cid)
        await reply_auto_delete(message,
            "🔬 <b>Карантин включён</b>\n"
            "Все новые участники получают мут на 24ч автоматически",
            parse_mode="HTML")

# Хук карантина — срабатывает при входе нового участника
@dp.message(F.new_chat_members)
async def on_new_member_quarantine(message: Message):
    cid = message.chat.id
    for user in message.new_chat_members:
        if user.is_bot: continue
        # Проверка чёрного списка
        if user.id in global_blacklist:
            try: await bot.ban_chat_member(cid, user.id)
            except: pass
            continue
        # 🎨 Welcome экран
        await send_welcome(cid, user)
        # Карантин
        if cid in quarantine_chats:
            try:
                await bot.restrict_chat_member(cid, user.id,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=datetime.now() + timedelta(hours=24))
                await message.answer(
                    f"🔬 {user.mention_html()} на карантине 24ч — не может писать",
                    parse_mode="HTML")
            except: pass
        # Авто-правила в ЛС
        if cid in auto_rules_chats:
            try:
                await bot.send_message(user.id,
                    f"👋 Привет, {user.full_name}! Ты вступил в <b>{message.chat.title}</b>\n\n"
                    f"{RULES_TEXT}\n\n"
                    f"📋 <a href='https://telegra.ph/Pravila-soobshchestva-03-13-6'>Полные правила</a>",
                    parse_mode="HTML")
                await message.answer(
                    f"📋 {user.mention_html()}, правила отправлены тебе в ЛС!",
                    parse_mode="HTML")
            except: pass

# ЗАЧИСТКА — удалить всех с 0 сообщений за 30 дней
@dp.message(Command("cleanup"))
async def cmd_cleanup(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    from datetime import datetime, timedelta as _td2
    cutoff = (datetime.now() - _td2(days=30)).strftime("%d.%m.%Y")
    # Ищем юзеров без активности за 30 дней
    inactive_uids = []
    for uid2 in list(chat_stats[cid].keys()):
        if await is_admin_by_id(cid, uid2): continue
        activity_30 = sum(
            user_activity[cid][uid2].get(
                (datetime.now() - _td2(days=i)).strftime("%d.%m.%Y"), 0)
            for i in range(30))
        if activity_30 == 0:
            inactive_uids.append(uid2)
    if not inactive_uids:
        await reply_auto_delete(message, "✅ Неактивных за 30 дней нет!"); return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"💥 Удалить {len(inactive_uids)} юзеров",
                             callback_data=f"cleanup:yes:{cid}:{len(inactive_uids)}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cleanup:no:0:0"),
    ]])
    sent = await message.answer(
        f"🧹 <b>Зачистка</b>\n\n"
        f"Неактивных за 30 дней: <b>{len(inactive_uids)}</b>\n"
        f"Будут исключены из чата. Продолжить?",
        parse_mode="HTML", reply_markup=kb)
    pending[message.from_user.id] = {"action": "cleanup_uids", "uids": inactive_uids, "cid": cid}
    asyncio.create_task(schedule_delete(sent, 60))

@dp.callback_query(F.data.startswith("cleanup:"))
async def cb_cleanup(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫", show_alert=True); return
    parts = call.data.split(":")
    action = parts[1]
    if action == "no":
        await call.message.delete(); await call.answer("Отменено"); return
    cid = int(parts[2])
    p = pending.get(call.from_user.id, {})
    uids = p.get("uids", [])
    kicked = 0
    await call.message.edit_text(f"🧹 Удаляю {len(uids)} юзеров...")
    for uid2 in uids:
        try:
            await bot.ban_chat_member(cid, uid2)
            await bot.unban_chat_member(cid, uid2)
            kicked += 1
            await asyncio.sleep(0.05)
        except: pass
    save_data()
    await call.message.edit_text(
        f"🧹 <b>Зачистка завершена</b>\n✅ Удалено: {kicked} неактивных юзеров",
        parse_mode="HTML")
    await log_action(f"🧹 <b>Зачистка</b>\n✅ {kicked} юзеров\n💬 {known_chats.get(cid, cid)}\n👑 {call.from_user.full_name}")
    await call.answer("✅ Выполнено.")

# ══════════════════════════════════════════════════════════
#  🔄 КЛОН НАСТРОЕК
# ══════════════════════════════════════════════════════════
@dp.message(Command("clonechat"))
async def cmd_clone_chat(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split()[1:] if message.text else []
    if len(args) < 2:
        chats_list = "\n".join(f"▸ <code>{cid}</code> — {title}"
                               for cid, title in list(known_chats.items())[:10])
        await reply_auto_delete(message,
            f"🔄 <b>Клон настроек</b>\n\n"
            f"<code>/clonechat ID_откуда ID_куда</code>\n\n"
            f"Доступные чаты:\n{chats_list}",
            parse_mode="HTML"); return
    try:
        src, dst = int(args[0]), int(args[1])
    except:
        await reply_auto_delete(message, "⚠️ Укажи числовые ID чатов"); return
    # Копируем плагины и настройки
    plugins[dst] = dict(plugins[src])
    mod_roles[dst] = dict(mod_roles[src])
    save_data()
    await reply_auto_delete(message,
        f"✅ <b>Настройки скопированы</b>\n"
        f"📤 Из: {known_chats.get(src, src)}\n"
        f"📥 В: {known_chats.get(dst, dst)}\n"
        f"Скопировано: плагины, роли модераторов",
        parse_mode="HTML")

import re as _re_global

# ══════════════════════════════════════════════════════════
#  🗄 SQLITE — персистентное хранилище
# ══════════════════════════════════════════════════════════
DB_FILE = "skinvault.db"

# ══════════════════════════════════════════════════════════
#  🌍 МУЛЬТИЯЗЫЧНОСТЬ
# ══════════════════════════════════════════════════════════
LANGS = {
    "ru": {
        "warn_issued": "⚠️ {name} получает варн! ({count}/{max})",
        "ban_issued":  "🔨 {name} забанен!",
        "mute_issued": "🔇 {name} заглушён на {time}",
        "welcome":     "👋 Добро пожаловать, {name}!",
        "rules_sent":  "📋 Правила отправлены тебе в ЛС!",
        "lang_changed":"✅ Язык изменён на Русский 🇷🇺",
    },
    "en": {
        "warn_issued": "⚠️ {name} gets a warning! ({count}/{max})",
        "ban_issued":  "🔨 {name} is banned!",
        "mute_issued": "🔇 {name} is muted for {time}",
        "welcome":     "👋 Welcome, {name}!",
        "rules_sent":  "📋 Rules sent to your DM!",
        "lang_changed":"✅ Language changed to English 🇬🇧",
    },
    "uk": {
        "warn_issued": "⚠️ {name} отримує попередження! ({count}/{max})",
        "ban_issued":  "🔨 {name} заблокований!",
        "mute_issued": "🔇 {name} заглушений на {time}",
        "welcome":     "👋 Ласкаво просимо, {name}!",
        "rules_sent":  "📋 Правила надіслані тобі в ЛС!",
        "lang_changed":"✅ Мова змінена на Українську 🇺🇦",
    },
}
chat_lang = defaultdict(lambda: "ru")  # {cid: lang}

def t(cid: int, key: str, **kwargs) -> str:
    lang = chat_lang.get(cid, "ru")
    text = LANGS.get(lang, LANGS["ru"]).get(key, key)
    return text.format(**kwargs) if kwargs else text

@dp.message(Command("lang"))
async def cmd_lang(message: Message):
    if not await require_admin(message): return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🇷🇺 Русский",    callback_data=f"lang:ru:{message.chat.id}"),
        InlineKeyboardButton(text="🇬🇧 English",    callback_data=f"lang:en:{message.chat.id}"),
        InlineKeyboardButton(text="🇺🇦 Українська", callback_data=f"lang:uk:{message.chat.id}"),
    ]])
    await reply_auto_delete(message, "🌍 <b>Выбери язык бота:</b>", parse_mode="HTML")
    sent = await message.answer("👇", reply_markup=kb)
    asyncio.create_task(schedule_delete(sent, 30))

@dp.callback_query(F.data.startswith("lang:"))
async def cb_lang(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫", show_alert=True); return
    _, lang, cid_str = call.data.split(":")
    cid = int(cid_str)
    chat_lang[cid] = lang
    await call.message.edit_text(t(cid, "lang_changed"))
    await call.answer()

# ══════════════════════════════════════════════════════════
#  🧠 ПАМЯТЬ БОТА — запоминает юзеров между сессиями
# ══════════════════════════════════════════════════════════
def mem_set(uid: int, cid: int, key: str, value: str):
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO user_memory VALUES (?,?,?,?)",
                 (uid, cid, key, value))
    conn.commit(); conn.close()

def mem_get(uid: int, cid: int, key: str) -> str:
    conn = db_connect()
    row = conn.execute("SELECT value FROM user_memory WHERE uid=? AND cid=? AND key=?",
                       (uid, cid, key)).fetchone()
    conn.close()
    return row["value"] if row else None

def mem_get_all(uid: int, cid: int) -> dict:
    conn = db_connect()
    rows = conn.execute("SELECT key, value FROM user_memory WHERE uid=? AND cid=?",
                        (uid, cid)).fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}

# ══════════════════════════════════════════════════════════
#  📋 ЖУРНАЛ ДЕЙСТВИЙ МОДЕРАТОРОВ
# ══════════════════════════════════════════════════════════
def journal_add(cid, mod_id, mod_name, action, target_id, target_name, reason=""):
    import time as _tj
    conn = db_connect()
    conn.execute(
        "INSERT INTO mod_journal (cid,mod_id,mod_name,action,target_id,target_name,reason,ts) VALUES (?,?,?,?,?,?,?,?)",
        (cid, mod_id, mod_name, action, target_id, target_name, reason, int(_tj.time())))
    conn.commit(); conn.close()
    # Календарь событий
    conn2 = db_connect()
    conn2.execute(
        "INSERT INTO events_calendar (cid,uid,action,target_id,target_name,ts) VALUES (?,?,?,?,?,?)",
        (cid, mod_id, action, target_id, target_name, int(_tj.time())))
    conn2.commit(); conn2.close()

@dp.message(Command("myjournal"))
async def cmd_my_journal(message: Message):
    """Мод видит свои действия"""
    uid = message.from_user.id
    cid = message.chat.id
    is_mod = uid in mod_roles.get(cid, {}) or await is_admin_by_id(cid, uid)
    if not is_mod: return
    conn = db_connect()
    rows = conn.execute(
        "SELECT action, target_name, reason, ts FROM mod_journal WHERE mod_id=? AND cid=? ORDER BY ts DESC LIMIT 15",
        (uid, cid)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "📋 Твой журнал пуст"); return
    lines = [f"📋 <b>Твой журнал действий</b>\n"]
    for r in rows:
        dt = datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")
        lines.append(f"▸ <b>{r['action']}</b> → {r['target_name']}\n"
                     f"  📝 {r['reason'] or '—'} | 🕐 {dt}\n")
    await bot.send_message(uid, "\n".join(lines), parse_mode="HTML")
    await reply_auto_delete(message, "📋 Журнал отправлен тебе в ЛС!", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  ⏰ СМЕНЫ МОДЕРАТОРОВ
# ══════════════════════════════════════════════════════════
@dp.message(Command("setshift"))
async def cmd_set_shift(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split()[1:] if message.text else []
    if len(args) < 3 or not message.reply_to_message:
        await reply_auto_delete(message,
            "⏰ <b>Использование:</b>\n<code>/setshift начало конец</code> (реплай на мода)\n"
            "Пример: <code>/setshift 9 21</code> — дежурит с 9 до 21",
            parse_mode="HTML"); return
    target = message.reply_to_message.from_user
    try: start, end = int(args[0]), int(args[1])
    except: await reply_auto_delete(message, "⚠️ Часы числами"); return
    cid = message.chat.id
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO mod_shifts VALUES (?,?,?,?,?)",
                 (cid, target.id, target.full_name, start, end))
    conn.commit(); conn.close()
    await reply_auto_delete(message,
        f"⏰ Смена назначена: {target.mention_html()}\n🕐 {start}:00 — {end}:00",
        parse_mode="HTML")

@dp.message(Command("shifts"))
async def cmd_shifts(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute("SELECT * FROM mod_shifts WHERE cid=?", (cid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "⏰ Смены не назначены"); return
    now_h = datetime.now().hour
    lines = [f"⏰ <b>Расписание смен</b>\n"]
    for r in rows:
        on = "🟢" if r["start_hour"] <= now_h < r["end_hour"] else "⚫"
        lines.append(f"{on} {r['mod_name']} — {r['start_hour']}:00–{r['end_hour']}:00")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  📊 РЕЙТИНГ МОДЕРАТОРОВ
# ══════════════════════════════════════════════════════════
@dp.message(Command("modrating"))
async def cmd_mod_rating(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute(
        "SELECT mod_name, COUNT(*) as cnt FROM mod_journal WHERE cid=? GROUP BY mod_id ORDER BY cnt DESC LIMIT 10",
        (cid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "📊 Статистики ещё нет"); return
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = [f"📊 <b>Рейтинг модераторов</b>\n"]
    for i, r in enumerate(rows):
        lines.append(f"{medals[i]} {r['mod_name']} — <b>{r['cnt']}</b> действий")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  🎯 ЗАДАЧИ МОДЕРАТОРАМ
# ══════════════════════════════════════════════════════════
@dp.message(Command("task"))
async def cmd_task(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split(None, 3)[1:] if message.text else []
    if not message.reply_to_message or len(args) < 1:
        await reply_auto_delete(message,
            "🎯 <b>Использование:</b>\n<code>/task текст задачи [дедлайн в часах]</code>\n"
            "Реплайни на мода. Пример: <code>/task Обработать репорты 24</code>",
            parse_mode="HTML"); return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    task_text = " ".join(args[:-1]) if args[-1].isdigit() else " ".join(args)
    hours = int(args[-1]) if args[-1].isdigit() else 24
    import time as _tt
    deadline = int(_tt.time()) + hours * 3600
    conn = db_connect()
    conn.execute(
        "INSERT INTO mod_tasks (cid,mod_id,mod_name,task,deadline,created_by) VALUES (?,?,?,?,?,?)",
        (cid, target.id, target.full_name, task_text, deadline, message.from_user.full_name))
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit(); conn.close()
    dl_str = datetime.fromtimestamp(deadline).strftime("%d.%m %H:%M")
    # Уведомить мода в ЛС
    try:
        await bot.send_message(target.id,
            f"🎯 <b>Новая задача от {message.from_user.full_name}</b>\n"
            f"📝 {task_text}\n⏰ Дедлайн: <b>{dl_str}</b>\n\n"
            f"Когда выполнишь: /donetask {task_id}",
            parse_mode="HTML")
    except: pass
    await reply_auto_delete(message,
        f"🎯 Задача #{task_id} поставлена для {target.mention_html()}\n"
        f"📝 {task_text}\n⏰ Дедлайн: {dl_str}",
        parse_mode="HTML")

@dp.message(Command("donetask"))
async def cmd_done_task(message: Message):
    args = message.text.split()[1:] if message.text else []
    if not args:
        await reply_auto_delete(message, "⚠️ Укажи ID задачи: /donetask 5"); return
    try: task_id = int(args[0])
    except: await reply_auto_delete(message, "⚠️ ID числом"); return
    uid = message.from_user.id
    conn = db_connect()
    row = conn.execute("SELECT * FROM mod_tasks WHERE id=? AND mod_id=?", (task_id, uid)).fetchone()
    if not row:
        conn.close(); await reply_auto_delete(message, "❌ Задача не найдена"); return
    conn.execute("UPDATE mod_tasks SET done=1 WHERE id=?", (task_id,))
    conn.commit(); conn.close()
    await reply_auto_delete(message, f"✅ Задача #{task_id} выполнена!")
    try:
        await bot.send_message(OWNER_ID,
            f"✅ <b>Задача выполнена!</b>\n"
            f"👤 {message.from_user.full_name}\n"
            f"📝 {row['task']}", parse_mode="HTML")
    except: pass

@dp.message(Command("tasks"))
async def cmd_tasks(message: Message):
    uid = message.from_user.id
    is_owner = uid == OWNER_ID
    cid = message.chat.id
    conn = db_connect()
    if is_owner:
        rows = conn.execute("SELECT * FROM mod_tasks WHERE cid=? AND done=0 ORDER BY deadline", (cid,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM mod_tasks WHERE mod_id=? AND done=0 ORDER BY deadline", (uid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "✅ Активных задач нет!"); return
    import time as _tsk
    lines = [f"🎯 <b>Активные задачи</b>\n"]
    for r in rows:
        dl = datetime.fromtimestamp(r["deadline"]).strftime("%d.%m %H:%M")
        overdue = "⚠️ ПРОСРОЧЕНО" if r["deadline"] < _tsk.time() else ""
        lines.append(f"#{r['id']} {r['mod_name'] if is_owner else ''} — {r['task']}\n"
                     f"  ⏰ {dl} {overdue}\n")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  💬 ЧАТ МОДЕРАТОРОВ
# ══════════════════════════════════════════════════════════
mod_chat_active = set()  # {uid} — кто сейчас в чате модов

@dp.message(Command("modchat"))
async def cmd_modchat(message: Message):
    uid = message.from_user.id
    cid = message.chat.id
    is_mod = uid == OWNER_ID or uid in mod_roles.get(cid, {}) or await is_admin_by_id(cid, uid)
    if not is_mod:
        await reply_auto_delete(message, "🚫 Только для модераторов"); return
    if uid in mod_chat_active:
        mod_chat_active.discard(uid)
        await reply_auto_delete(message, "💬 Вышел из чата модераторов")
    else:
        mod_chat_active.add(uid)
        await reply_auto_delete(message,
            "💬 <b>Чат модераторов активен</b>\n"
            "Пиши боту в ЛС — сообщение увидят все моды\n"
            "Выйти: /modchat ещё раз", parse_mode="HTML")

@dp.message(F.chat.type == "private")
async def handle_modchat_dm(message: Message):
    uid = message.from_user.id
    if uid not in mod_chat_active: return
    if not message.text or message.text.startswith("/"): return
    # Рассылаем всем активным модам
    uname = message.from_user.full_name
    text = f"💬 <b>[МОД-ЧАТ]</b> {uname}:\n{message.text}"
    sent_to = 0
    for mod_uid in list(mod_chat_active):
        if mod_uid == uid: continue
        try:
            await bot.send_message(mod_uid, text, parse_mode="HTML")
            sent_to += 1
        except: pass
    if sent_to == 0:
        await message.answer("💬 Других модов онлайн нет")

# ══════════════════════════════════════════════════════════
#  ⚡ БЫСТРЫЕ ОТВЕТЫ
# ══════════════════════════════════════════════════════════
@dp.message(Command("addreply"))
async def cmd_add_reply(message: Message):
    if not await require_admin(message): return
    args = message.text.split(None, 2)[1:] if message.text else []
    if len(args) < 2:
        await reply_auto_delete(message,
            "⚡ <b>Добавить быстрый ответ:</b>\n"
            "<code>/addreply ключ текст ответа</code>\n"
            "Пример: <code>/addreply реклама Реклама запрещена!</code>",
            parse_mode="HTML"); return
    key, text = args[0].lower(), args[1]
    cid = message.chat.id
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO quick_replies VALUES (?,?,?)", (cid, key, text))
    conn.commit(); conn.close()
    await reply_auto_delete(message, f"✅ Быстрый ответ <b>!{key}</b> сохранён", parse_mode="HTML")

@dp.message(Command("replies"))
async def cmd_replies(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute("SELECT key, text FROM quick_replies WHERE cid=?", (cid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "⚡ Быстрых ответов нет — добавь через /addreply"); return
    lines = ["⚡ <b>Быстрые ответы</b>\n"]
    for r in rows:
        lines.append(f"▸ <code>!{r['key']}</code> — {r['text'][:40]}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# Хук: если мод пишет !ключ — бот отвечает шаблоном
@dp.message(F.text.startswith("!"))
async def handle_quick_reply(message: Message):
    if message.chat.type not in ("group", "supergroup"): return
    uid = message.from_user.id
    cid = message.chat.id
    if not (await is_admin_by_id(cid, uid) or uid in mod_roles.get(cid, {})): return
    key = message.text[1:].split()[0].lower()
    conn = db_connect()
    row = conn.execute("SELECT text FROM quick_replies WHERE cid=? AND key=?", (cid, key)).fetchone()
    conn.close()
    if not row: return
    try: await message.delete()
    except: pass
    if message.reply_to_message:
        await message.reply_to_message.reply(row["text"])
    else:
        await message.answer(row["text"])

# ══════════════════════════════════════════════════════════
#  📌 ЗАКРЕП-МЕНЕДЖЕР
# ══════════════════════════════════════════════════════════
@dp.message(Command("pinmanager"))
async def cmd_pin_manager(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute("SELECT * FROM pinned_messages WHERE cid=? ORDER BY ts DESC", (cid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message,
            "📌 <b>Закреп-менеджер</b>\n\nЗакреплённых нет.\n"
            "Реплайни на сообщение + /pin заголовок — добавить",
            parse_mode="HTML"); return
    lines = ["📌 <b>Закреп-менеджер</b>\n"]
    kb_rows = []
    for r in rows:
        dt = datetime.fromtimestamp(r["ts"]).strftime("%d.%m")
        lines.append(f"▸ <b>{r['title']}</b> — {dt}")
        kb_rows.append([
            InlineKeyboardButton(text=f"📌 {r['title'][:15]}", url=f"https://t.me/c/{str(cid)[4:]}/{r['msg_id']}"),
            InlineKeyboardButton(text="🗑", callback_data=f"unpin:{cid}:{r['msg_id']}"),
        ])
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")
    sent = await message.answer("Управление:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    asyncio.create_task(schedule_delete(sent, 60))

@dp.message(Command("pin"))
async def cmd_pin_add(message: Message):
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение"); return
    title = message.text.replace("/pin", "").strip() or "Без названия"
    cid = message.chat.id
    msg_id = message.reply_to_message.message_id
    import time as _tp
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO pinned_messages VALUES (?,?,?,?)",
                 (cid, msg_id, title, int(_tp.time())))
    conn.commit(); conn.close()
    try: await bot.pin_chat_message(cid, msg_id, disable_notification=True)
    except: pass
    await reply_auto_delete(message, f"📌 Закреплено: <b>{title}</b>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("unpin:"))
async def cb_unpin(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("🚫", show_alert=True); return
    _, cid_str, msg_id_str = call.data.split(":")
    cid, msg_id = int(cid_str), int(msg_id_str)
    conn = db_connect()
    conn.execute("DELETE FROM pinned_messages WHERE cid=? AND msg_id=?", (cid, msg_id))
    conn.commit(); conn.close()
    try: await bot.unpin_chat_message(cid, msg_id)
    except: pass
    await call.answer("📌 Откреплено")
    await call.message.delete()

# ══════════════════════════════════════════════════════════
#  🔄 АВТО-ПРАВИЛА ПРИ ВХОДЕ
# ══════════════════════════════════════════════════════════
auto_rules_chats = set()  # {cid} — в каких чатах слать правила в ЛС

@dp.message(Command("autorules"))
async def cmd_auto_rules(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    if cid in auto_rules_chats:
        auto_rules_chats.discard(cid)
        await reply_auto_delete(message, "🔄 Авто-правила выключены")
    else:
        auto_rules_chats.add(cid)
        await reply_auto_delete(message,
            "🔄 <b>Авто-правила включены</b>\n"
            "Новые участники будут получать правила в ЛС автоматически",
            parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  💎 VIP СИСТЕМА
# ══════════════════════════════════════════════════════════
@dp.message(Command("vip"))
async def cmd_vip(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    target = message.reply_to_message.from_user if message.reply_to_message else None
    if not target:
        # Показать VIP список
        conn = db_connect()
        rows = conn.execute("SELECT uid, granted_by FROM vip_users WHERE cid=?",
                            (message.chat.id,)).fetchall()
        conn.close()
        if not rows:
            await reply_auto_delete(message, "💎 VIP список пуст"); return
        lines = [f"💎 <b>VIP участники</b>\n"]
        for r in rows:
            lines.append(f"▸ ID{r['uid']} (выдал: {r['granted_by']})")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML"); return
    import time as _tv
    cid = message.chat.id
    conn = db_connect()
    existing = conn.execute("SELECT uid FROM vip_users WHERE uid=? AND cid=?",
                            (target.id, cid)).fetchone()
    if existing:
        conn.execute("DELETE FROM vip_users WHERE uid=? AND cid=?", (target.id, cid))
        conn.commit(); conn.close()
        await reply_auto_delete(message, f"💎 VIP снят с {target.mention_html()}", parse_mode="HTML")
    else:
        conn.execute("INSERT OR REPLACE INTO vip_users VALUES (?,?,?,?)",
                     (target.id, cid, message.from_user.full_name, int(_tv.time())))
        conn.commit(); conn.close()
        await reply_auto_delete(message,
            f"💎 {target.mention_html()} получил <b>VIP статус</b>!\n"
            f"✅ Иммунитет к анти-спаму и анти-мату", parse_mode="HTML")
        try:
            await bot.send_message(target.id,
                f"💎 <b>Ты получил VIP статус</b> в {message.chat.title}!\n"
                f"✅ Особые привилегии активированы", parse_mode="HTML")
        except: pass

def is_vip(uid: int, cid: int) -> bool:
    conn = db_connect()
    row = conn.execute("SELECT uid FROM vip_users WHERE uid=? AND cid=?", (uid, cid)).fetchone()
    conn.close()
    return row is not None

# ══════════════════════════════════════════════════════════
#  📊 АНАЛИТИКА — топы и хитмапы
# ══════════════════════════════════════════════════════════
@dp.message(Command("heatmap"))
async def cmd_heatmap(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    hours = defaultdict(int)
    for uid2 in hourly_stats[cid]:
        for h, cnt in hourly_stats[cid][uid2].items():
            hours[int(h)] += cnt
    if not hours:
        await reply_auto_delete(message, "📊 Данных пока нет"); return
    max_cnt = max(hours.values()) or 1
    blocks = ["▁","▂","▃","▄","▅","▆","▇","█"]
    bars = ""
    for h in range(24):
        val = hours.get(h, 0)
        idx = int(val / max_cnt * 7)
        bars += blocks[idx]
    lines = [
        "📊 <b>Активность по часам</b>\n",
        f"<code>{bars}</code>",
        f"0ч {'':>10} 12ч {'':>9} 23ч",
        f"\n🔥 Пик: <b>{max(hours, key=hours.get)}:00</b> ({hours[max(hours, key=hours.get)]} сообщ.)",
        f"😴 Мин: <b>{min(hours, key=hours.get)}:00</b> ({hours[min(hours, key=hours.get)]} сообщ.)",
    ]
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  📅 КАЛЕНДАРЬ СОБЫТИЙ
# ══════════════════════════════════════════════════════════
@dp.message(Command("calendar"))
async def cmd_calendar(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute(
        "SELECT action, target_name, ts FROM events_calendar WHERE cid=? ORDER BY ts DESC LIMIT 20",
        (cid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "📅 Календарь пуст"); return
    lines = ["📅 <b>Календарь событий</b>\n"]
    for r in rows:
        dt = datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")
        lines.append(f"▸ {dt} — <b>{r['action']}</b> → {r['target_name']}")
    await bot.send_message(OWNER_ID, "\n".join(lines), parse_mode="HTML")
    await reply_auto_delete(message, "📅 Календарь отправлен в ЛС!", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  💾 АВТОБЭКАП БАЗЫ В ТЕЛЕГРАМ КАЖДЫЕ 6 ЧАСОВ
# ══════════════════════════════════════════════════════════
async def auto_backup_loop():
    """Каждые 6 часов отправляет skinvault.db владельцу в ЛС"""
    await asyncio.sleep(30)  # Ждём 30 сек после старта
    while True:
        try:
            save_data()  # Сначала сохраняем актуальные данные
            import os as _os2, io as _io2
            from datetime import datetime as _dt_bk
            db_path = "skinvault.db"
            if _os2.path.exists(db_path):
                size = _os2.path.getsize(db_path)
                with open(db_path, "rb") as f:
                    buf = _io2.BytesIO(f.read())
                buf.name = f"skinvault_{_dt_bk.now().strftime('%d%m%Y_%H%M')}.db"
                await bot.send_document(
                    OWNER_ID, buf,
                    caption=(
                        f"💾 <b>Автобэкап базы данных</b>\n"
                        f"📅 {_dt_bk.now().strftime('%d.%m.%Y %H:%M')}\n"
                        f"📦 Размер: <b>{size // 1024} KB</b>\n"
                        f"💬 Чатов: <b>{len(known_chats)}</b>\n"
                        f"👥 Юзеров: <b>{sum(len(chat_stats[c]) for c in chat_stats)}</b>\n\n"
                        f"<i>Для восстановления: /restoredb</i>"
                    ),
                    parse_mode="HTML"
                )
                print(f"✅ Автобэкап отправлен ({size // 1024} KB)")
        except Exception as e:
            print(f"[auto_backup_loop] {e}")
        await asyncio.sleep(6 * 3600)  # Каждые 6 часов

@dp.message(Command("restoredb"))
async def cmd_restore_db(message: Message):
    """Восстановить базу из файла — /restoredb (реплай на .db файл)"""
    if message.from_user.id not in ADMIN_IDS: return
    if not message.reply_to_message or not message.reply_to_message.document:
        await reply_auto_delete(message,
            "💾 <b>Восстановление базы</b>\n\n"
            "Реплайни на .db файл из бэкапа и напиши /restoredb\n\n"
            "⚠️ Текущие данные будут заменены!",
            parse_mode="HTML"); return
    doc = message.reply_to_message.document
    if not doc.file_name.endswith(".db"):
        await reply_auto_delete(message, "⚠️ Нужен файл с расширением .db"); return
    status = await message.answer("⏳ Скачиваю и восстанавливаю базу...")
    try:
        import io as _io3
        file = await bot.get_file(doc.file_id)
        buf = _io3.BytesIO()
        await bot.download_file(file.file_path, buf)
        buf.seek(0)
        # Сохраняем старую базу как резерв
        import os as _os3, shutil as _sh
        if _os3.path.exists("skinvault.db"):
            _sh.copy("skinvault.db", "skinvault_old.db")
        with open("skinvault.db", "wb") as f:
            f.write(buf.read())
        # Перезагружаем данные
        load_data()
        await status.edit_text(
            f"✅ <b>База восстановлена!</b>\n"
            f"📦 Файл: {doc.file_name}\n"
            f"💬 Чатов: {len(known_chats)}\n"
            f"👥 Юзеров: {sum(len(chat_stats[c]) for c in chat_stats)}\n\n"
            f"<i>Старая база сохранена как skinvault_old.db</i>",
            parse_mode="HTML")
        await log_action(f"💾 <b>База восстановлена</b>\n👑 {message.from_user.full_name}")
    except Exception as e:
        await status.edit_text(f"❌ Ошибка восстановления: {e}")

@dp.message(Command("backupnow"))
async def cmd_backup_now(message: Message):
    """Немедленный бэкап базы"""
    if message.from_user.id not in ADMIN_IDS: return
    import os as _osbn, io as _iobn
    from datetime import datetime as _dtbn
    save_data()
    db_path = "skinvault.db"
    if not _osbn.path.exists(db_path):
        await reply_auto_delete(message, "❌ База данных не найдена"); return
    size = _osbn.path.getsize(db_path)
    with open(db_path, "rb") as f:
        buf = _iobn.BytesIO(f.read())
    buf.name = f"skinvault_{_dtbn.now().strftime('%d%m%Y_%H%M')}.db"
    await bot.send_document(
        OWNER_ID, buf,
        caption=(
            f"💾 <b>Ручной бэкап</b>\n"
            f"📅 {_dtbn.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"📦 Размер: <b>{size // 1024} KB</b>\n"
            f"💬 Чатов: <b>{len(known_chats)}</b>"
        ),
        parse_mode="HTML")
    await reply_auto_delete(message, "✅ Бэкап отправлен в ЛС!", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  🔔 УМНЫЕ УВЕДОМЛЕНИЯ — бот сам пишет владельцу
# ══════════════════════════════════════════════════════════
async def smart_notify_loop():
    """Фоновая задача — мониторинг и уведомления"""
    import time as _tsn
    await asyncio.sleep(60)
    while True:
        try:
            now = _tsn.time()

            # 1. Просроченные задачи
            conn = db_connect()
            overdue = conn.execute(
                "SELECT * FROM mod_tasks WHERE done=0 AND deadline < ?", (now,)).fetchall()
            conn.close()
            if overdue:
                lines = [f"⚠️ <b>Просроченные задачи ({len(overdue)})</b>\n"]
                for t2 in overdue:
                    dl = datetime.fromtimestamp(t2["deadline"]).strftime("%d.%m %H:%M")
                    lines.append(f"▸ {t2['mod_name']}: {t2['task']} (до {dl})")
                await safe_send(bot.send_message(OWNER_ID, "\n".join(lines), parse_mode="HTML"))

            # 2. Дейли отчёт в 21:00
            now_dt = datetime.now()
            if now_dt.hour == 21 and now_dt.minute < 5:
                for cid in list(known_chats.keys()):
                    today = now_dt.strftime("%d.%m.%Y")
                    today_msgs = sum(daily_stats[cid][u].get(today, 0) for u in daily_stats[cid])
                    new_warns  = sum(1 for u in warnings[cid] if warnings[cid][u] > 0)
                    open_reps  = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
                    await safe_send(bot.send_message(cid,
                        f"📰 <b>Дейли {today}</b>\n"
                        f"💬 Сообщений за день: <b>{today_msgs}</b>\n"
                        f"⚡ Активных варнов: <b>{new_warns}</b>\n"
                        f"🚨 Открытых репортов: <b>{open_reps}</b>\n"
                        f"👥 Активных юзеров: <b>{len(daily_stats[cid])}</b>",
                        parse_mode="HTML"))
                    await asyncio.sleep(0.2)

            # 3. Мониторинг 24/7 — пинг владельцу если очередь репортов большая
            for cid in list(known_chats.keys()):
                open_reps = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
                if open_reps >= 5:
                    await safe_send(bot.send_message(OWNER_ID,
                        f"📡 <b>Мониторинг 24/7</b>\n"
                        f"⚠️ В <b>{known_chats.get(cid,'?')}</b> накопилось <b>{open_reps}</b> необработанных репортов!\n"
                        f"🔗 Открой /mypanel → Репорты",
                        parse_mode="HTML"))

        except Exception as e:
            print(f"[smart_notify_loop] {e}")
        await asyncio.sleep(300)  # каждые 5 минут

# ══════════════════════════════════════════════════════════
#  🤖 ПЕРСОНАЛЬНЫЙ АССИСТЕНТ В ЛС
# ══════════════════════════════════════════════════════════
ASSISTANT_COMMANDS = {
    "стат": "cmd_sos",
    "статус": "cmd_sos",
    "репорты": "reports_summary",
    "задачи": "tasks_summary",
    "помощь": "assistant_help",
    "чаты": "chats_summary",
}

@dp.message(F.chat.type == "private", F.from_user.id == OWNER_ID)
async def owner_assistant(message: Message):
    if not message.text or message.text.startswith("/"): return
    if message.from_user.id in mod_chat_active: return
    text = message.text.lower().strip()
    # Простые ответы ассистента
    if any(w in text for w in ["привет", "хай", "hello"]):
        await message.answer(
            f"👋 Привет! Я твой ассистент.\n\n"
            f"💬 Чатов: {len(known_chats)}\n"
            f"🚨 Репортов: {sum(len(report_queue.get(c,[])) for c in known_chats)}\n\n"
            f"Спроси меня что угодно или используй /mypanel"); return
    if any(w in text for w in ["сколько", "чатов", "статус"]):
        total_msgs = sum(sum(chat_stats[c].values()) for c in known_chats)
        await message.answer(
            f"📊 <b>Статус</b>\n"
            f"💬 Чатов: {len(known_chats)}\n"
            f"👥 Юзеров: {sum(len(chat_stats[c]) for c in known_chats)}\n"
            f"📨 Сообщений: {total_msgs}\n"
            f"🚨 Репортов: {sum(len(report_queue.get(c,[])) for c in known_chats)}",
            parse_mode="HTML"); return
    if any(w in text for w in ["задач", "task"]):
        conn = db_connect()
        rows = conn.execute("SELECT COUNT(*) as cnt FROM mod_tasks WHERE done=0").fetchone()
        conn.close()
        await message.answer(f"🎯 Активных задач: <b>{rows['cnt']}</b>\n/tasks — подробнее", parse_mode="HTML"); return
    if any(w in text for w in ["помощ", "help", "что умеешь"]):
        await message.answer(
            "🤖 <b>Я умею отвечать на:</b>\n"
            "▸ 'привет' — приветствие\n"
            "▸ 'сколько чатов' — статистика\n"
            "▸ 'задачи' — активные задачи\n\n"
            "Также используй:\n"
            "/mypanel — панель всех чатов\n"
            "/profile — твой профиль\n"
            "/backup — бэкап базы",
            parse_mode="HTML"); return

# ══════════════════════════════════════════════════════════
#  🎨 КАСТОМНЫЙ ПРИВЕТСТВЕННЫЙ ЭКРАН
# ══════════════════════════════════════════════════════════
# {cid: {"text": str, "photo": file_id или None, "enabled": bool}}
# ── Welcome helpers ──────────────────────────────────────────
def welcome_get(cid: int) -> dict:
    conn = db_connect()
    row = conn.execute("SELECT * FROM welcome_settings WHERE cid=?", (cid,)).fetchone()
    conn.close()
    if row:
        return {"text": row["text"], "photo": row["photo"],
                "is_gif": bool(row["is_gif"]), "enabled": bool(row["enabled"]),
                "buttons": bool(row["buttons"])}
    return {"text": "👋 Добро пожаловать, {name}!\n\n📋 Ознакомься с правилами чата.",
            "photo": None, "is_gif": False, "enabled": True, "buttons": True}

def welcome_save(cid: int, data: dict):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO welcome_settings VALUES (?,?,?,?,?,?)",
        (cid, data.get("text",""), data.get("photo"), int(data.get("is_gif",False)),
         int(data.get("enabled",True)), int(data.get("buttons",True))))
    conn.commit(); conn.close()

# ── Surveillance helpers ──────────────────────────────────────
def surveillance_enabled(cid: int) -> bool:
    conn = db_connect()
    row = conn.execute("SELECT cid FROM surveillance_chats WHERE cid=?", (cid,)).fetchone()
    conn.close()
    return row is not None

def surveillance_toggle(cid: int) -> bool:
    """Returns True if now enabled, False if disabled"""
    conn = db_connect()
    row = conn.execute("SELECT cid FROM surveillance_chats WHERE cid=?", (cid,)).fetchone()
    if row:
        conn.execute("DELETE FROM surveillance_chats WHERE cid=?", (cid,))
        conn.commit(); conn.close()
        return False
    else:
        conn.execute("INSERT INTO surveillance_chats VALUES (?)", (cid,))
        conn.commit(); conn.close()
        return True

def surveillance_log_add(cid: int, uid: int, name: str, text: str):
    import time as _tsl
    conn = db_connect()
    conn.execute("INSERT INTO deleted_log (cid,uid,name,text,ts) VALUES (?,?,?,?,?)",
                 (cid, uid, name, text[:500], _tsl.time()))
    # Keep only last 100 per chat
    conn.execute("""DELETE FROM deleted_log WHERE cid=? AND id NOT IN
                    (SELECT id FROM deleted_log WHERE cid=? ORDER BY ts DESC LIMIT 100)""",
                 (cid, cid))
    conn.commit(); conn.close()

def surveillance_log_get(cid: int) -> list:
    conn = db_connect()
    rows = conn.execute(
        "SELECT uid,name,text,ts FROM deleted_log WHERE cid=? ORDER BY ts DESC LIMIT 20",
        (cid,)).fetchall()
    conn.close()
    return [{"uid": r["uid"], "name": r["name"], "text": r["text"], "ts": r["ts"]} for r in rows]

@dp.message(Command("setwelcome"))
async def cmd_set_welcome(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    text = message.text.replace("/setwelcome", "").strip()

    if message.reply_to_message:
        # Если реплай на фото — сохраняем фото
        if message.reply_to_message.photo:
            s = welcome_get(cid)
            s["photo"] = message.reply_to_message.photo[-1].file_id
            s["is_gif"] = False
            if text: s["text"] = text
            welcome_save(cid, s)
            await reply_auto_delete(message,
                "✅ <b>Welcome фото обновлено!</b>\n"
                "Переменные: {name} {mention} {count}",
                parse_mode="HTML"); return
        elif message.reply_to_message.animation:
            s = welcome_get(cid)
            s["photo"] = message.reply_to_message.animation.file_id
            s["is_gif"] = True
            if text: s["text"] = text
            welcome_save(cid, s)
            await reply_auto_delete(message, "✅ Welcome гифка обновлена!", parse_mode="HTML"); return

    if not text:
        # Показать текущие настройки
        s = welcome_get(cid)
        status = "✅ включён" if s["enabled"] else "❌ выключен"
        await reply_auto_delete(message,
            f"🎨 <b>Настройки Welcome</b>\n"
            f"Статус: {status}\n"
            f"Медиа: {'✅ есть' if s['photo'] else '❌ нет'}\n\n"
            f"📝 Текст:\n{s['text']}\n\n"
            f"<b>Команды:</b>\n"
            f"/setwelcome текст — изменить текст\n"
            f"/setwelcome (реплай на фото) — добавить фото\n"
            f"/welcomeoff — выключить\n"
            f"/welcomeon — включить\n"
            f"/testwelcome — протестировать\n\n"
            f"<b>Переменные:</b>\n"
            f"{{name}} — имя юзера\n"
            f"{{mention}} — упоминание\n"
            f"{{count}} — номер участника",
            parse_mode="HTML"); return

    s = welcome_get(cid)
    s["text"] = text
    welcome_save(cid, s)
    await reply_auto_delete(message,
        f"✅ <b>Welcome текст обновлён!</b>\n\n{text}",
        parse_mode="HTML")

@dp.message(Command("welcomeoff"))
async def cmd_welcome_off(message: Message):
    if not await require_admin(message): return
    s = welcome_get(message.chat.id)
    s["enabled"] = False
    welcome_save(message.chat.id, s)
    await reply_auto_delete(message, "❌ Welcome выключен")

@dp.message(Command("welcomeon"))
async def cmd_welcome_on(message: Message):
    if not await require_admin(message): return
    s = welcome_get(message.chat.id)
    s["enabled"] = True
    welcome_save(message.chat.id, s)
    await reply_auto_delete(message, "✅ Welcome включён")

@dp.message(Command("testwelcome"))
async def cmd_test_welcome(message: Message):
    if not await require_admin(message): return
    await send_welcome(message.chat.id, message.from_user, test=True)

async def send_welcome(cid: int, user, test: bool = False):
    """Отправляет красивое приветствие"""
    s = welcome_get(cid)
    if not s["enabled"] and not test: return

    full_text = (
        f"👋 <b>Новый участник!</b>\n"
        f"👋 Добро пожаловать, <b>{user.full_name}</b>!\n"
        f"📋 Ознакомься с правилами чата."
    )

    try:
        await bot.send_message(cid, full_text, parse_mode="HTML")
    except Exception as e:
        print(f"[send_welcome] {e}")

# ══════════════════════════════════════════════════════════
#  👁 РЕЖИМ НАБЛЮДЕНИЯ — логирует удалённые сообщения
# ══════════════════════════════════════════════════════════
surveillance_chats = set()   # {cid} — чаты где включён режим (RAM кеш для скорости)
deleted_log = defaultdict(list)  # не используется — данные в SQLite

@dp.message(Command("surveillance"))
async def cmd_surveillance(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    enabled = surveillance_toggle(cid)
    if enabled:
        await reply_auto_delete(message,
            "👁 <b>Режим наблюдения включён</b>\n"
            "Все удалённые сообщения будут логироваться\n"
            "/deletedlog — посмотреть лог",
            parse_mode="HTML")
    else:
        await reply_auto_delete(message,
            "👁 <b>Режим наблюдения выключен</b>", parse_mode="HTML")

@dp.message(Command("deletedlog"))
async def cmd_deleted_log(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    logs = surveillance_log_get(cid)
    if not logs:
        await reply_auto_delete(message, "👁 Удалённых сообщений не зафиксировано"); return
    lines = [f"👁 <b>Удалённые сообщения</b> ({len(logs)} шт.)\n"]
    for r in logs:
        dt = datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")
        preview = r["text"][:80] if r["text"] else "[медиа]"
        lines.append(f"🕐 {dt} — <b>{r['name']}</b>\n💬 {preview}\n")
    await bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="HTML")
    await reply_auto_delete(message, "👁 Лог отправлен в ЛС!", parse_mode="HTML")

# Middleware для перехвата удалённых сообщений уже в message_cache
# Хук удаления: переопределяем delete через обёртку
_original_delete_handler = None

async def log_deleted_message(cid: int, uid: int, name: str, text: str):
    if not surveillance_enabled(cid): return
    surveillance_log_add(cid, uid, name, text)
    await log_action(
        f"👁 <b>Удалено сообщение</b>\n"
        f"👤 {name} (<code>{uid}</code>)\n"
        f"💬 {text[:200] if text else '[медиа]'}\n"
        f"🏠 Чат ID: {cid}")

async def track_deletion(message: Message):
    if not surveillance_enabled(message.chat.id): return
    if not message.from_user: return
    text = message.text or message.caption or ""
    await log_deleted_message(
        message.chat.id, message.from_user.id,
        message.from_user.full_name, text)

class SurveillanceMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if isinstance(event, Message) and surveillance_enabled(event.chat.id):
            if event.from_user and (event.text or event.caption):
                import time as _tsm
                # Кешируем в RAM для отслеживания последующего удаления
                key = f"{event.chat.id}_{event.message_id}"
                surveillance_chats_cache[key] = {
                    "cid": event.chat.id,
                    "uid": event.from_user.id,
                    "name": event.from_user.full_name,
                    "text": event.text or event.caption or "",
                    "ts": _tsm.time()
                }
        return await handler(event, data)

# RAM кеш для перехвата удалений (ключ = cid_msgid)
surveillance_chats_cache = {}

dp.message.middleware(SurveillanceMiddleware())

# ══════════════════════════════════════════════════════════
#  👥 ФРЕНДЛИСТ — друзья и онлайн статус
# ══════════════════════════════════════════════════════════
def db_friends_init():
    conn = db_connect()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS friends (
        uid INTEGER, friend_id INTEGER, friend_name TEXT, ts INTEGER,
        PRIMARY KEY (uid, friend_id));
    CREATE TABLE IF NOT EXISTS friend_requests (
        from_uid INTEGER, to_uid INTEGER, from_name TEXT, ts INTEGER,
        PRIMARY KEY (from_uid, to_uid));
    CREATE TABLE IF NOT EXISTS online_status (
        uid INTEGER PRIMARY KEY, last_seen REAL, status TEXT DEFAULT 'online');
    CREATE TABLE IF NOT EXISTS user_profiles (
        uid INTEGER PRIMARY KEY, bio TEXT, mood TEXT, interests TEXT,
        anon_nick TEXT, anon_enabled INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS relationships (
        uid1 INTEGER, uid2 INTEGER, rel_type TEXT, ts INTEGER,
        PRIMARY KEY (uid1, uid2));
    CREATE TABLE IF NOT EXISTS gifts_sent (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_uid INTEGER, to_uid INTEGER, gift TEXT, ts INTEGER);
    CREATE TABLE IF NOT EXISTS subscriptions (
        subscriber INTEGER, target INTEGER,
        PRIMARY KEY (subscriber, target));
    CREATE TABLE IF NOT EXISTS qr_codes (
        code TEXT PRIMARY KEY, uid INTEGER, reward INTEGER, used INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS daily_ideas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cid INTEGER, idea TEXT, ts INTEGER);
    CREATE TABLE IF NOT EXISTS anon_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_uid INTEGER, to_uid INTEGER, text TEXT, ts INTEGER, read INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS cmd_permissions (
        cid INTEGER, cmd TEXT, min_role TEXT,
        PRIMARY KEY (cid, cmd));
    """)
    conn.commit(); conn.close()

MOOD_LIST = ["😊 Отлично", "😐 Нормально", "😔 Грустно", "😤 Злой", "🤔 Задумчив",
             "🥳 Праздник", "😴 Устал", "💪 Бодрый", "❤️ Влюблён", "🔥 На подъёме"]

GIFT_LIST = {
    "🌹": ("Роза", 10),    "🎂": ("Торт", 20),   "💎": ("Алмаз", 50),
    "🍕": ("Пицца", 15),   "🎵": ("Музыка", 5),  "⭐": ("Звезда", 30),
    "🏆": ("Кубок", 40),   "🎁": ("Сюрприз", 25),"💐": ("Цветы", 12),
    "🍫": ("Шоколад", 8),
}

ANON_NICKS = ["Призрак", "Тень", "Ветер", "Загадка", "Туман", "Шёпот",
              "Молния", "Огонь", "Лёд", "Буря", "Звезда", "Луна"]

# ── Профиль ───────────────────────────────────────────────────
@dp.message(Command("setbio"))
async def cmd_set_bio(message: Message):
    bio = message.text.replace("/setbio", "").strip()
    if not bio:
        await reply_auto_delete(message,
            "🏠 <b>Использование:</b> <code>/setbio твоё био</code>\nМакс 150 символов",
            parse_mode="HTML"); return
    if len(bio) > 150:
        await reply_auto_delete(message, "⚠️ Макс 150 символов"); return
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO user_profiles (uid, bio) VALUES (?,?) "
                 "ON CONFLICT(uid) DO UPDATE SET bio=excluded.bio",
                 (message.from_user.id, bio))
    conn.commit(); conn.close()
    await reply_auto_delete(message, f"✅ Био обновлено!", parse_mode="HTML")

@dp.message(Command("setmood"))
async def cmd_set_mood(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=m, callback_data=f"mood:{i}")]
        for i, m in enumerate(MOOD_LIST)
    ])
    await reply_auto_delete(message, "😊 Выбери настроение:", reply_markup=kb)

@dp.callback_query(F.data.startswith("mood:"))
async def cb_mood(call: CallbackQuery):
    idx = int(call.data.split(":")[1])
    mood = MOOD_LIST[idx]
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO user_profiles (uid, mood) VALUES (?,?) "
                 "ON CONFLICT(uid) DO UPDATE SET mood=excluded.mood",
                 (call.from_user.id, mood))
    conn.commit(); conn.close()
    await call.message.edit_text(f"✅ Настроение: {mood}")
    await call.answer()

@dp.message(Command("myprofile"))
async def cmd_my_profile(message: Message):
    uid = message.from_user.id
    conn = db_connect()
    p = conn.execute("SELECT * FROM user_profiles WHERE uid=?", (uid,)).fetchone()
    friends_cnt = conn.execute("SELECT COUNT(*) as c FROM friends WHERE uid=?", (uid,)).fetchone()["c"]
    rel = conn.execute("SELECT rel_type, uid2 FROM relationships WHERE uid1=?", (uid,)).fetchone()
    conn.close()
    bio = p["bio"] if p and p["bio"] else "Не указано"
    mood = p["mood"] if p and p["mood"] else "😐 Нормально"
    interests = p["interests"] if p and p["interests"] else "Не указаны"
    anon = "✅ вкл" if p and p["anon_enabled"] else "❌ выкл"
    rel_text = ""
    if rel:
        try:
            tm = await bot.get_chat_member(message.chat.id, rel["uid2"])
            rel_text = f"\n{'❤️' if rel['rel_type']=='couple' else '🤝'} {rel['rel_type'].title()}: {tm.user.full_name}"
        except: pass
    await reply_auto_delete(message,
        f"🏠 <b>Профиль</b>\n"
        f"👤 {message.from_user.full_name}\n"
        f"😊 Настроение: {mood}\n"
        f"📝 Био: {bio}\n"
        f"🎯 Интересы: {interests}\n"
        f"👥 Друзей: <b>{friends_cnt}</b>\n"
        f"🎭 Аноним: {anon}"
        f"{rel_text}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Био", callback_data=f"profile_edit:bio:{uid}"),
             InlineKeyboardButton(text="😊 Настроение", callback_data=f"profile_edit:mood:{uid}")],
            [InlineKeyboardButton(text="🎯 Интересы", callback_data=f"profile_edit:interests:{uid}"),
             InlineKeyboardButton(text="🎭 Аноним", callback_data=f"profile_edit:anon:{uid}")],
        ]))

@dp.callback_query(F.data.startswith("profile_edit:"))
async def cb_profile_edit(call: CallbackQuery):
    parts = call.data.split(":")
    field, uid = parts[1], int(parts[2])
    if call.from_user.id != uid:
        await call.answer("🚫 Это не твой профиль!", show_alert=True); return
    if field == "mood":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=m, callback_data=f"mood:{i}")]
            for i, m in enumerate(MOOD_LIST)
        ])
        await call.message.edit_text("😊 Выбери настроение:", reply_markup=kb)
    elif field == "anon":
        conn = db_connect()
        p = conn.execute("SELECT anon_enabled, anon_nick FROM user_profiles WHERE uid=?", (uid,)).fetchone()
        cur = p["anon_enabled"] if p else 0
        new_val = 0 if cur else 1
        nick = p["anon_nick"] if p and p["anon_nick"] else random.choice(ANON_NICKS)
        conn.execute("INSERT OR REPLACE INTO user_profiles (uid, anon_enabled, anon_nick) VALUES (?,?,?) "
                     "ON CONFLICT(uid) DO UPDATE SET anon_enabled=excluded.anon_enabled, anon_nick=excluded.anon_nick",
                     (uid, new_val, nick))
        conn.commit(); conn.close()
        status = "✅ включён" if new_val else "❌ выключен"
        await call.answer(f"🎭 Анонимный режим {status}\nНик: {nick}", show_alert=True)
    elif field in ("bio", "interests"):
        pending[call.from_user.id] = {"action": f"set_{field}", "chat_id": call.message.chat.id,
                                       "target_id": uid, "target_name": ""}
        await call.message.edit_text(f"✏️ Напиши {'био (макс 150 симв.)' if field=='bio' else 'интересы'}:")
    await call.answer()

# ── Друзья ────────────────────────────────────────────────────
@dp.message(Command("addfriend"))
async def cmd_add_friend(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение юзера"); return
    target = message.reply_to_message.from_user
    uid = message.from_user.id
    if target.id == uid:
        await reply_auto_delete(message, "😅 Нельзя добавить себя в друзья"); return
    import time as _taf
    conn = db_connect()
    existing = conn.execute("SELECT * FROM friends WHERE uid=? AND friend_id=?", (uid, target.id)).fetchone()
    if existing:
        conn.close()
        await reply_auto_delete(message, f"✅ {target.full_name} уже в твоём списке друзей!"); return
    # Отправить запрос
    conn.execute("INSERT OR REPLACE INTO friend_requests VALUES (?,?,?,?)",
                 (uid, target.id, message.from_user.full_name, int(_taf.time())))
    conn.commit(); conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Принять", callback_data=f"friend:accept:{uid}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"friend:reject:{uid}"),
    ]])
    try:
        await bot.send_message(target.id,
            f"👥 <b>{message.from_user.full_name}</b> хочет добавить тебя в друзья!",
            parse_mode="HTML", reply_markup=kb)
    except: pass
    await reply_auto_delete(message, f"📨 Запрос отправлен {target.mention_html()}!", parse_mode="HTML")

@dp.callback_query(F.data.startswith("friend:"))
async def cb_friend(call: CallbackQuery):
    parts = call.data.split(":")
    action, from_uid = parts[1], int(parts[2])
    to_uid = call.from_user.id
    import time as _tfr
    conn = db_connect()
    req = conn.execute("SELECT * FROM friend_requests WHERE from_uid=? AND to_uid=?",
                       (from_uid, to_uid)).fetchone()
    if not req:
        conn.close(); await call.answer("Устарело", show_alert=True); return
    conn.execute("DELETE FROM friend_requests WHERE from_uid=? AND to_uid=?", (from_uid, to_uid))
    if action == "accept":
        ts = int(_tfr.time())
        conn.execute("INSERT OR REPLACE INTO friends VALUES (?,?,?,?)",
                     (to_uid, from_uid, req["from_name"], ts))
        conn.execute("INSERT OR REPLACE INTO friends VALUES (?,?,?,?)",
                     (from_uid, to_uid, call.from_user.full_name, ts))
        conn.commit(); conn.close()
        await call.message.edit_text(f"✅ Теперь вы с {req['from_name']} друзья! 👥")
        try:
            await bot.send_message(from_uid,
                f"✅ <b>{call.from_user.full_name}</b> принял(а) твой запрос в друзья!",
                parse_mode="HTML")
        except: pass
    else:
        conn.commit(); conn.close()
        await call.message.edit_text(f"❌ Запрос от {req['from_name']} отклонён")
    await call.answer()

@dp.message(Command("friends"))
async def cmd_friends(message: Message):
    uid = message.from_user.id
    conn = db_connect()
    rows = conn.execute("SELECT * FROM friends WHERE uid=?", (uid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "👥 Друзей нет — добавь через /addfriend (реплай)"); return
    lines = [f"👥 <b>Мои друзья</b> ({len(rows)})\n"]
    for r in rows:
        lines.append(f"▸ {r['friend_name']}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("unfriend"))
async def cmd_unfriend(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение юзера"); return
    target = message.reply_to_message.from_user
    uid = message.from_user.id
    conn = db_connect()
    conn.execute("DELETE FROM friends WHERE uid=? AND friend_id=?", (uid, target.id))
    conn.execute("DELETE FROM friends WHERE uid=? AND friend_id=?", (target.id, uid))
    conn.commit(); conn.close()
    await reply_auto_delete(message, f"💔 {target.full_name} удалён из друзей")

# ── Система отношений ─────────────────────────────────────────
@dp.message(Command("propose"))
async def cmd_propose(message: Message):
    """Предложение отношений"""
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение юзера"); return
    target = message.reply_to_message.from_user
    uid = message.from_user.id
    if target.id == uid:
        await reply_auto_delete(message, "😅 Нельзя встречаться с собой"); return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❤️ Да!", callback_data=f"rel:accept:{uid}:couple"),
        InlineKeyboardButton(text="💔 Нет", callback_data=f"rel:reject:{uid}:couple"),
    ]])
    try:
        await bot.send_message(target.id,
            f"💝 <b>{message.from_user.full_name}</b> предлагает тебе отношения! ❤️",
            parse_mode="HTML", reply_markup=kb)
    except: pass
    await reply_auto_delete(message,
        f"💌 Предложение отправлено {target.mention_html()}! ❤️", parse_mode="HTML")

@dp.callback_query(F.data.startswith("rel:"))
async def cb_rel(call: CallbackQuery):
    parts = call.data.split(":")
    action, from_uid, rel_type = parts[1], int(parts[2]), parts[3]
    to_uid = call.from_user.id
    import time as _trel
    conn = db_connect()
    if action == "accept":
        # Удаляем старые отношения
        conn.execute("DELETE FROM relationships WHERE uid1=? OR uid2=?", (to_uid, to_uid))
        conn.execute("DELETE FROM relationships WHERE uid1=? OR uid2=?", (from_uid, from_uid))
        conn.execute("INSERT OR REPLACE INTO relationships VALUES (?,?,?,?)",
                     (from_uid, to_uid, rel_type, int(_trel.time())))
        conn.execute("INSERT OR REPLACE INTO relationships VALUES (?,?,?,?)",
                     (to_uid, from_uid, rel_type, int(_trel.time())))
        conn.commit(); conn.close()
        await call.message.edit_text(f"❤️ Теперь вы пара!")
        try:
            await bot.send_message(from_uid,
                f"❤️ <b>{call.from_user.full_name}</b> принял(а) твоё предложение!", parse_mode="HTML")
        except: pass
    else:
        conn.close()
        await call.message.edit_text("💔 Предложение отклонено...")
        try: await bot.send_message(from_uid, "💔 Предложение отклонено...")
        except: pass
    await call.answer()

@dp.message(Command("breakup"))
async def cmd_breakup(message: Message):
    uid = message.from_user.id
    conn = db_connect()
    rel = conn.execute("SELECT * FROM relationships WHERE uid1=?", (uid,)).fetchone()
    if not rel:
        conn.close()
        await reply_auto_delete(message, "💔 У тебя нет отношений"); return
    partner_id = rel["uid2"]
    conn.execute("DELETE FROM relationships WHERE uid1=? OR uid2=?", (uid, uid))
    conn.commit(); conn.close()
    await reply_auto_delete(message, "💔 Отношения завершены")
    try: await bot.send_message(partner_id, f"💔 <b>{message.from_user.full_name}</b> завершил(а) ваши отношения...", parse_mode="HTML")
    except: pass

# ── Подарки ───────────────────────────────────────────────────
# ── Анонимные сообщения ───────────────────────────────────────
@dp.message(Command("anonmsg"))
async def cmd_anon_msg(message: Message):
    """Написать анонимное сообщение юзеру"""
    if not message.reply_to_message:
        await reply_auto_delete(message,
            "💌 <b>Анонимное сообщение</b>\n"
            "<code>/anonmsg текст</code> (реплай на юзера)",
            parse_mode="HTML"); return
    text = message.text.replace("/anonmsg", "").strip()
    if not text:
        await reply_auto_delete(message, "⚠️ Напиши текст сообщения"); return
    target = message.reply_to_message.from_user
    uid = message.from_user.id
    # Получаем аноним-ник отправителя
    conn = db_connect()
    p = conn.execute("SELECT anon_nick, anon_enabled FROM user_profiles WHERE uid=?", (uid,)).fetchone()
    nick = p["anon_nick"] if p and p["anon_nick"] else random.choice(ANON_NICKS)
    conn.execute("INSERT INTO anon_messages (from_uid,to_uid,text,ts) VALUES (?,?,?,?)",
                 (uid, target.id, text, int(__import__("time").time())))
    conn.commit(); conn.close()
    try:
        await bot.send_message(target.id,
            f"💌 <b>Анонимное сообщение</b>\n"
            f"👤 От: <b>{nick}</b>\n\n"
            f"📝 {text}",
            parse_mode="HTML")
    except:
        await reply_auto_delete(message, "❌ Не удалось отправить — юзер закрыл ЛС"); return
    await reply_auto_delete(message, f"✅ Анонимное сообщение отправлено!", parse_mode="HTML")

# ── Подписки ──────────────────────────────────────────────────
@dp.message(Command("follow"))
async def cmd_follow(message: Message):
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение юзера"); return
    target = message.reply_to_message.from_user
    uid = message.from_user.id
    if target.id == uid:
        await reply_auto_delete(message, "😅 Нельзя подписаться на себя"); return
    conn = db_connect()
    existing = conn.execute("SELECT * FROM subscriptions WHERE subscriber=? AND target=?",
                            (uid, target.id)).fetchone()
    if existing:
        conn.execute("DELETE FROM subscriptions WHERE subscriber=? AND target=?", (uid, target.id))
        conn.commit(); conn.close()
        await reply_auto_delete(message, f"🔕 Отписался от {target.full_name}")
    else:
        conn.execute("INSERT OR REPLACE INTO subscriptions VALUES (?,?)", (uid, target.id))
        conn.commit(); conn.close()
        await reply_auto_delete(message, f"🔔 Подписался на {target.mention_html()}!", parse_mode="HTML")
        try:
            await bot.send_message(target.id,
                f"🔔 <b>{message.from_user.full_name}</b> подписался на тебя!", parse_mode="HTML")
        except: pass

@dp.message(Command("followers"))
async def cmd_followers(message: Message):
    uid = message.from_user.id
    conn = db_connect()
    subs = conn.execute("SELECT COUNT(*) as c FROM subscriptions WHERE target=?", (uid,)).fetchone()["c"]
    following = conn.execute("SELECT COUNT(*) as c FROM subscriptions WHERE subscriber=?", (uid,)).fetchone()["c"]
    conn.close()
    await reply_auto_delete(message,
        f"🔔 <b>Подписки</b>\n"
        f"👥 Подписчиков: <b>{subs}</b>\n"
        f"➡️ Подписок: <b>{following}</b>",
        parse_mode="HTML")

# ── КюАр-коды ────────────────────────────────────────────────
@dp.message(Command("createqr"))
async def cmd_create_qr(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split()[1:] if message.text else []
    reward = int(args[0]) if args and args[0].isdigit() else 50
    import uuid as _uuid
    code = _uuid.uuid4().hex[:8].upper()
    conn = db_connect()
    conn.execute("INSERT INTO qr_codes VALUES (?,?,?,?)",
                 (code, message.from_user.id, reward, 0))
    conn.commit(); conn.close()
    await reply_auto_delete(message,
        f"📲 <b>КюАр-код создан!</b>\n\n"
        f"🔑 Код: <code>{code}</code>\n"
        f"🎁 Награда: <b>{reward} XP</b>\n\n"
        f"Юзеры активируют: /scanqr {code}",
        parse_mode="HTML")

@dp.message(Command("scanqr"))
async def cmd_scan_qr(message: Message):
    args = message.text.split()[1:] if message.text else []
    if not args:
        await reply_auto_delete(message,
            "📲 <b>Сканировать код:</b>\n<code>/scanqr КОД</code>",
            parse_mode="HTML"); return
    code = args[0].upper()
    uid = message.from_user.id
    conn = db_connect()
    qr = conn.execute("SELECT * FROM qr_codes WHERE code=?", (code,)).fetchone()
    if not qr:
        conn.close(); await reply_auto_delete(message, "❌ Код не найден"); return
    if qr["used"]:
        conn.close(); await reply_auto_delete(message, "⚠️ Код уже использован"); return
    if qr["uid"] == uid:
        conn.close(); await reply_auto_delete(message, "😅 Нельзя активировать свой код"); return
    conn.execute("UPDATE qr_codes SET used=1 WHERE code=?", (code,))
    conn.commit(); conn.close()
    cid = message.chat.id
    xp_data[cid][uid] = xp_data[cid].get(uid, 0) + qr["reward"]
    db_set_int("xp_data", cid, uid, "xp", xp_data[cid][uid])
    await reply_auto_delete(message,
        f"📲 <b>Код активирован!</b>\n🎁 +{qr['reward']} XP получено!",
        parse_mode="HTML")

# ── Идея дня ─────────────────────────────────────────────────
DAILY_IDEAS = [
    "🤔 Что бы вы сделали, если бы у вас был 1 миллион?",
    "🌍 Какую страну вы бы хотели посетить и почему?",
    "🎮 Какая игра изменила вашу жизнь?",
    "📚 Какую книгу вы бы порекомендовали всем?",
    "🎵 Какой трек у вас сейчас на повторе?",
    "🍕 Пицца или суши — что выбираете?",
    "🚀 Если бы можно было улететь в космос — полетели бы?",
    "💭 О чём вы мечтаете прямо сейчас?",
    "🎬 Какой фильм смотрели последним?",
    "🌙 Вы жаворонок или сова?",
    "🔥 Что вас мотивирует каждый день?",
    "🤝 Что для вас важнее — дружба или карьера?",
    "🎯 Какова ваша цель на этот год?",
    "😂 Расскажите смешной случай из жизни",
    "🌟 За что вы благодарны сегодня?",
]
daily_idea_chats = set()  # {cid} — чаты где включена идея дня

@dp.message(Command("dailyidea"))
async def cmd_daily_idea_toggle(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    if cid in daily_idea_chats:
        daily_idea_chats.discard(cid)
        await reply_auto_delete(message, "💡 Идея дня выключена")
    else:
        daily_idea_chats.add(cid)
        await reply_auto_delete(message,
            "💡 <b>Идея дня включена!</b>\nКаждое утро в 9:00 бот будет предлагать тему для обсуждения",
            parse_mode="HTML")

@dp.message(Command("idea"))
async def cmd_idea_now(message: Message):
    """Получить идею прямо сейчас"""
    idea = random.choice(DAILY_IDEAS)
    await reply_auto_delete(message,
        f"💡 <b>Тема для обсуждения</b>\n\n{idea}",
        parse_mode="HTML")

# ── Авто-перевод ─────────────────────────────────────────────
translate_chats = {}  # {cid: target_lang}

@dp.message(Command("translate"))
async def cmd_translate_toggle(message: Message):
    if not await require_admin(message): return
    args = message.text.split()[1:] if message.text else []
    cid = message.chat.id
    if not args:
        if cid in translate_chats:
            del translate_chats[cid]
            await reply_auto_delete(message, "🌐 Авто-перевод выключен"); return
        await reply_auto_delete(message,
            "🌐 <b>Авто-перевод</b>\n<code>/translate en</code> — переводить на английский\n"
            "Языки: ru, en, uk, de, fr, es, zh, ja, ko, ar",
            parse_mode="HTML"); return
    lang = args[0].lower()
    translate_chats[cid] = lang
    await reply_auto_delete(message, f"🌐 Авто-перевод на <b>{lang}</b> включён", parse_mode="HTML")

@dp.message(Command("tr"))
async def cmd_translate_msg(message: Message):
    """Перевести конкретное сообщение"""
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение для перевода"); return
    args = message.text.split()[1:] if message.text else []
    lang = args[0] if args else "en"
    text = message.reply_to_message.text or message.reply_to_message.caption or ""
    if not text:
        await reply_auto_delete(message, "⚠️ Нет текста для перевода"); return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://translate.googleapis.com/translate_a/single",
                params={"client": "gtx", "sl": "auto", "tl": lang, "dt": "t", "q": text}
            ) as resp:
                data = await resp.json()
                translated = "".join(part[0] for part in data[0] if part[0])
        await reply_auto_delete(message,
            f"🌐 <b>Перевод ({lang}):</b>\n{translated}", parse_mode="HTML")
    except Exception as e:
        await reply_auto_delete(message, f"❌ Ошибка перевода: {e}")

# ── Поиск музыки ─────────────────────────────────────────────
@dp.message(Command("music"))
async def cmd_music(message: Message):
    args = message.text.replace("/music", "").strip() if message.text else ""
    if not args:
        await reply_auto_delete(message,
            "🎵 <b>Поиск музыки:</b>\n<code>/music название трека</code>",
            parse_mode="HTML"); return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://itunes.apple.com/search",
                params={"term": args, "media": "music", "limit": "5", "entity": "song"}
            ) as resp:
                text_data = await resp.text()
                import json as _json
                data = _json.loads(text_data)
        results = data.get("results", [])
        if not results:
            await reply_auto_delete(message, "🎵 Ничего не найдено"); return
        lines = [f"🎵 <b>Результаты поиска: {args}</b>\n"]
        for r in results[:5]:
            artist = r.get("artistName","?")
            track = r.get("trackName","?")
            album = r.get("collectionName","?")
            duration = r.get("trackTimeMillis",0) // 1000
            m2, s2 = duration // 60, duration % 60
            preview = r.get("previewUrl","")
            lines.append(f"🎤 <b>{artist}</b> — {track}\n"
                        f"💿 {album} | ⏱ {m2}:{s2:02d}")
            if preview:
                lines.append(f"🎧 <a href='{preview}'>Слушать превью</a>")
            lines.append("")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML",
                                disable_web_page_preview=True)
    except Exception as e:
        await reply_auto_delete(message, f"❌ Ошибка поиска: {e}")

# ── Генерация изображений через Pollinations ─────────────────
@dp.message(Command("imagine"))
async def cmd_imagine(message: Message):
    prompt = message.text.replace("/imagine", "").strip() if message.text else ""
    if not prompt:
        await reply_auto_delete(message,
            "🖼 <b>Генерация изображения:</b>\n<code>/imagine описание на английском</code>\n"
            "Пример: <code>/imagine cyberpunk city night rain</code>",
            parse_mode="HTML"); return
    status = await message.answer("🖼 Генерирую изображение...")
    try:
        import urllib.parse
        encoded = urllib.parse.quote(prompt)
        url = f"https://image.pollinations.ai/prompt/{encoded}?width=512&height=512&nologo=true"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    img_data = await resp.read()
                    import io
                    buf = io.BytesIO(img_data)
                    buf.name = "image.jpg"
                    await message.answer_photo(buf,
                        caption=f"🖼 <b>{prompt}</b>\n<i>Сгенерировано AI</i>",
                        parse_mode="HTML")
                    await status.delete()
                else:
                    await status.edit_text("❌ Не удалось сгенерировать")
    except Exception as e:
        await status.edit_text(f"❌ Ошибка: {e}")

# ── Система прав доступа ──────────────────────────────────────
CMD_ROLES = {}  # {cid: {cmd: min_role}}

@dp.message(Command("setperm"))
async def cmd_set_perm(message: Message):
    """Установить минимальную роль для команды"""
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split()[1:] if message.text else []
    if len(args) < 2:
        await reply_auto_delete(message,
            "🔐 <b>Использование:</b>\n<code>/setperm команда роль</code>\n"
            "Роли: junior / senior / head / admin / owner\n"
            "Пример: <code>/setperm warn junior</code>",
            parse_mode="HTML"); return
    cmd, role = args[0].lower(), args[1].lower()
    if role not in ("junior", "senior", "head", "admin", "owner"):
        await reply_auto_delete(message, "⚠️ Роли: junior / senior / head / admin / owner"); return
    cid = message.chat.id
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO cmd_permissions VALUES (?,?,?)", (cid, cmd, role))
    conn.commit(); conn.close()
    if cid not in CMD_ROLES: CMD_ROLES[cid] = {}
    CMD_ROLES[cid][cmd] = role
    await reply_auto_delete(message,
        f"🔐 Команда <code>/{cmd}</code> — минимальная роль: <b>{role}</b>",
        parse_mode="HTML")

@dp.message(Command("perms"))
async def cmd_perms(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute("SELECT cmd, min_role FROM cmd_permissions WHERE cid=?", (cid,)).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "🔐 Кастомных прав нет"); return
    lines = ["🔐 <b>Права доступа</b>\n"]
    for r in rows:
        lines.append(f"▸ <code>/{r['cmd']}</code> — {r['min_role']}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ── Фоновая задача: идея дня + перевод ───────────────────────
async def daily_idea_loop():
    """Каждый день в 9:00 отправляет идею дня"""
    while True:
        now = datetime.now()
        if now.hour == 9 and now.minute < 5:
            idea = random.choice(DAILY_IDEAS)
            for cid in list(daily_idea_chats):
                try:
                    await safe_send(bot.send_message(cid,
                        f"💡 <b>Идея дня</b>\n\n{idea}\n\n"
                        f"<i>Обсудите в чате!</i>",
                        parse_mode="HTML"))
                    await asyncio.sleep(0.2)
                except: pass
        await asyncio.sleep(300)

# ══════════════════════════════════════════════════════════
#  🎫 СИСТЕМА ТИКЕТОВ
# ══════════════════════════════════════════════════════════

@dp.message(Command("ticket"), F.chat.type == "private")
async def cmd_ticket_handler(message: Message):
    chats = await db.get_all_chats()
    chat_list = [(r["cid"], r["title"]) for r in chats]
    await tkt.cmd_ticket(message, bot, chat_list)

@dp.message(Command("ticket"), F.chat.type != "private")
async def cmd_ticket_group_handler(message: Message):
    """В группе — отправляем в ЛС"""
    await reply_auto_delete(message,
        "Напиши мне в <b>личку</b> чтобы открыть тикет!\n"
        "Там сможешь описать проблему — модераторы ответят.",
        parse_mode="HTML")

@dp.callback_query(F.data.startswith("tkt:"))
async def cb_ticket_user_handler(call: CallbackQuery):
    chats = await db.get_all_chats()
    chat_list = [(r["cid"], r["title"]) for r in chats]
    await tkt.cb_ticket_user(call, bot, chat_list)

@dp.callback_query(F.data.startswith("tktm:"))
async def cb_ticket_mod_handler(call: CallbackQuery):
    is_mod = (call.from_user.id in ADMIN_IDS)
    if not is_mod:
        try:
            is_mod = await is_admin_by_id(call.message.chat.id, call.from_user.id)
        except:
            pass
    if not is_mod:
        await call.answer("🚫 Только для модераторов!", show_alert=True)
        return
    await tkt.cb_ticket_mod(call, bot)

async def _notify_mods_ticket(ticket_id, uid, user_name, chat_title, subject, priority):
    """Уведомляет всех админов о новом тикете через shared"""
    await shared.notify_new_ticket(ticket_id, user_name, subject, chat_title, priority)

# ══════════════════════════════════════════════════════════
#  🔔 РАССЫЛКА ОБ ОБНОВЛЕНИИ БОТА
# ══════════════════════════════════════════════════════════

@dp.message(Command("botupdate"))
async def cmd_botupdate(message: Message, command: CommandObject):
    """Рассылает сообщение об обновлении во все чаты. Только владелец."""
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца")
        return

    text = command.args or ""
    if not text:
        await reply_auto_delete(message,
            "Использование:\n"
            "<code>/botupdate текст обновления</code>\n\n"
            "Пример:\n"
            "<code>/botupdate Бот обновлён до v2.0! Добавлены тикеты и дашборд.</code>",
            parse_mode="HTML")
        return

    # Формируем красивое сообщение об обновлении
    update_text = (
        f"🔔 <b>Обновление бота</b>\n"
        f"{text}\n\n"
        f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        f"👤 Команда CHAT GUARD"
    )

    sent = 0
    failed = 0
    chats = list(known_chats.keys())

    status_msg = await message.answer(
        f"⏳ Рассылаю в {len(chats)} чатов...",
        parse_mode="HTML"
    )

    for cid in chats:
        try:
            await bot.send_message(cid, update_text, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1

    # Также логируем в лог-канал
    try:
        await log_action(update_text)
    except: pass

    try:
        await status_msg.edit_text(
            f"✅ <b>Рассылка завершена</b>\n"
            f"📨 Отправлено: <b>{sent}</b> чатов\n"
            f"❌ Ошибок: <b>{failed}</b>",
            parse_mode="HTML"
        )
    except: pass

# ══════════════════════════════════════════════════════════
#  📝 СИСТЕМА ЗАМЕТОК МОДЕРАТОРА
# ══════════════════════════════════════════════════════════

@dp.message(Command("modnote"))
async def cmd_note_mod(message: Message, command: CommandObject):
    """Оставить заметку на юзера — только для модераторов"""
    if not await check_admin(message):
        return
    if not message.reply_to_message:
        await reply_auto_delete(message,
            "Реплай на сообщение юзера:\n"
            "<code>/note текст заметки</code>",
            parse_mode="HTML")
        return

    target = message.reply_to_message.from_user
    text   = command.args or ""
    if not text:
        await reply_auto_delete(message, "⚠️ Напиши текст заметки")
        return

    cid = message.chat.id
    by  = message.from_user.full_name

    conn = db_connect()
    # Создаём таблицу если нет
    conn.execute("""CREATE TABLE IF NOT EXISTS mod_notes
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     cid INTEGER, uid INTEGER, text TEXT,
                     by_name TEXT, created_at TEXT DEFAULT (datetime('now')))""")
    conn.execute(
        "INSERT INTO mod_notes (cid, uid, text, by_name) VALUES (?,?,?,?)",
        (cid, target.id, text, by)
    )
    conn.commit()
    count = conn.execute(
        "SELECT COUNT(*) FROM mod_notes WHERE cid=? AND uid=?", (cid, target.id)
    ).fetchone()[0]
    conn.close()

    await reply_auto_delete(message,
        f"📝 <b>Заметка добавлена</b>\n"
        f"👤 {target.mention_html()}\n"
        f"📋 {text}\n\n"
        f"📊 Всего заметок: {count}",
        parse_mode="HTML")

@dp.message(Command("notes"))
async def cmd_notes_mod(message: Message):
    """Показать заметки на юзера"""
    if not await check_admin(message):
        return
    if not message.reply_to_message:
        await reply_auto_delete(message, "⚠️ Реплай на сообщение юзера")
        return

    target = message.reply_to_message.from_user
    cid    = message.chat.id

    conn = db_connect()
    try:
        notes = conn.execute(
            "SELECT text, by_name, created_at FROM mod_notes "
            "WHERE cid=? AND uid=? ORDER BY created_at DESC LIMIT 10",
            (cid, target.id)
        ).fetchall()
    except:
        notes = []
    conn.close()

    if not notes:
        await reply_auto_delete(message,
            f"📝 Заметок на {target.mention_html()} нет",
            parse_mode="HTML")
        return

    text = (
        f"📝 <b>Заметки</b>\n"
        f"👤 {target.mention_html()}\n\n"
    )
    for i, n in enumerate(notes, 1):
        dt = str(n["created_at"] or "")[:16]
        text += f"{i}. <i>{n['text']}</i>\n   👮 {n['by_name']} · {dt}\n\n"

    await reply_auto_delete(message, text, parse_mode="HTML")

@dp.message(Command("delnote"))
async def cmd_delnote_mod(message: Message, command: CommandObject):
    """Удалить заметку по номеру"""
    if not await check_admin(message):
        return
    if not message.reply_to_message:
        await reply_auto_delete(message, "⚠️ Реплай на сообщение юзера + номер заметки")
        return

    target = message.reply_to_message.from_user
    cid    = message.chat.id
    num    = int(command.args or "1")

    conn = db_connect()
    try:
        notes = conn.execute(
            "SELECT id FROM mod_notes WHERE cid=? AND uid=? ORDER BY created_at DESC",
            (cid, target.id)
        ).fetchall()
        if notes and num <= len(notes):
            conn.execute("DELETE FROM mod_notes WHERE id=?", (notes[num-1]["id"],))
            conn.commit()
            await reply_auto_delete(message, f"✅ Заметка #{num} удалена")
        else:
            await reply_auto_delete(message, "❌ Заметка не найдена")
    except:
        await reply_auto_delete(message, "⚠️ Произошла ошибка.")
    conn.close()

# ══════════════════════════════════════════════════════════════════
#  🎙 ГОЛОСОВЫЕ КОМАНДЫ ЧЕРЕЗ WHISPER
#  Только для администраторов из ADMIN_IDS
#  Отправь войс боту в ЛС → транскрипция → выполнение команды
# ══════════════════════════════════════════════════════════════════

# Паттерны разбора команд (без ИИ, на ключевых словах)
_VC_PATTERNS = [
    (["забань", "забан", "бан ", " бан"],       "ban"),
    (["замути", "мут ", " мут", "заглуши"],      "mute"),
    (["сними мут", "размути", "убери мут"],       "unmute"),
    (["варн", "предупреди", "выдай предупреждение"], "warn"),
    (["разбань", "разбан", "убери бан"],          "unban"),
    (["кик", "выгони", "исключи"],                "kick"),
    (["заблокируй чат", "локдаун", "закрой чат"], "lockdown"),
    (["разблокируй чат", "открой чат"],           "unlock"),
    (["объяви", "объявление", "анонс"],           "announce"),
    (["статус", "состояние", "как дела"],         "status"),
    (["топ ", "статистика", "активные"],          "top"),
]

def _vc_parse(text: str) -> dict:
    """Разбирает транскрипт в структурированную команду."""
    import re
    t = text.lower().strip()
    result = {"action": None, "target": None, "arg": "", "raw": text}

    # Определяем действие
    for keywords, action in _VC_PATTERNS:
        if any(kw in t for kw in keywords):
            result["action"] = action
            break
    if not result["action"]:
        return result

    # Ищем цель: числовой ID или @username
    id_m  = re.search(r"id[:\s]*(\d+)", t)
    at_m  = re.search(r"@(\w+)", text)  # оригинальный регистр
    num_m = re.search(r"\b(\d{5,12})\b", t)
    if id_m:    result["target"] = id_m.group(1)
    elif at_m:  result["target"] = "@" + at_m.group(1)
    elif num_m: result["target"] = num_m.group(1)

    # Время для мута
    tm = re.search(r"на\s*(\d+)\s*(мин|минут|ч|час|д|день|дн)", t)
    if tm:
        n, u = int(tm.group(1)), tm.group(2)
        if   "мин" in u: result["arg"] = f"{n}m"
        elif "ч"   in u or "час" in u: result["arg"] = f"{n}h"
        elif "д"   in u or "ден" in u or "дн" in u: result["arg"] = f"{n}d"
    if not result["arg"] and result["action"] == "mute":
        result["arg"] = "60m"  # дефолт 1 час

    # Текст объявления
    if result["action"] == "announce":
        for kw in ["объяви", "объявление", "анонс"]:
            idx = t.find(kw)
            if idx >= 0:
                after = text[idx + len(kw):].strip(" :–-")
                if after: result["arg"] = after
                break

    return result

async def _vc_transcribe(file_path: str) -> str | None:
    """Отправляет аудио в Whisper API, возвращает транскрипт."""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return None
    try:
        import aiohttp as _ah
        async with _ah.ClientSession() as sess:
            with open(file_path, "rb") as f:
                form = _ah.FormData()
                form.add_field("model", "whisper-1")
                form.add_field("language", "ru")
                form.add_field("file", f,
                               filename="voice.ogg",
                               content_type="audio/ogg")
                async with sess.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {api_key}"},
                    data=form,
                    timeout=_ah.ClientTimeout(total=30)
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        return d.get("text", "").strip()
                    else:
                        err = await r.text()
                        logging.warning(f"[VOICE] Whisper {r.status}: {err[:100]}")
                        return None
    except Exception as e:
        logging.error(f"[VOICE] transcribe: {e}")
        return None

async def _vc_resolve_target(target: str, cid: int) -> tuple[int | None, str]:
    """Резолвит target (@username или ID) в (uid, имя)."""
    if not target:
        return None, "?"
    try:
        if target.startswith("@"):
            m = await bot.get_chat_member(cid, target)
        else:
            m = await bot.get_chat_member(cid, int(target))
        return m.user.id, m.user.full_name
    except Exception as e:
        logging.warning(f"[VOICE] resolve {target}: {e}")
        return None, target

async def _vc_execute(parsed: dict, admin_uid: int, admin_name: str) -> str:
    """Выполняет распознанную команду. Возвращает текст результата."""
    action = parsed["action"]
    target = parsed["target"]
    arg    = parsed["arg"]

    # Берём первый известный чат для команд без cid
    # (в ЛС нет cid — берём из known_chats)
    chats = list(known_chats.keys())
    if not chats:
        return "❌ Бот не добавлен ни в один чат"

    if action == "status":
        total_msgs  = sum(sum(v.values()) for v in chat_stats.values())
        total_bans  = sum(len(v) for v in ban_list.values())
        online_now  = len([c for c in chats])
        return (f"📊 <b>Статус бота</b>\n\n"
                f"💬 Чатов: {len(chats)}\n"
                f"✉️ Сообщений: {total_msgs:,}\n"
                f"🔨 Банов: {total_bans}\n"
                f"⏰ Uptime: активен")

    if action == "top":
        cid = chats[0]
        top = sorted(chat_stats[cid].items(), key=lambda x: x[1], reverse=True)[:5]
        if not top:
            return "📊 Статистики пока нет"
        lines = [f"🏆 <b>Топ чата {known_chats.get(cid, cid)}</b>\n"]
        for i, (uid, cnt) in enumerate(top, 1):
            lines.append(f"{i}. <code>{uid}</code> — {cnt} сообщений")
        return "\n".join(lines)

    if action in ("ban", "mute", "unmute", "warn", "unban", "kick"):
        if not target:
            return f"❌ Укажи цель: «{action} ID 123456789»"
        # Ищем цель в каждом чате
        executed = []
        for cid in chats:
            uid, name = await _vc_resolve_target(target, cid)
            if not uid:
                continue
            try:
                if action == "ban":
                    await bot.ban_chat_member(cid, uid)
                    ban_list[cid][uid] = {"name": name, "reason": "Голосовая команда", "by": admin_name}
                    save_data()
                    add_mod_history(cid, uid, "🔨 Бан", "голосовая команда", admin_name)
                    executed.append(f"🔨 Забанен {name} в {known_chats.get(cid, cid)}")

                elif action == "mute":
                    mins = 60
                    if arg.endswith("m"): mins = int(arg[:-1])
                    elif arg.endswith("h"): mins = int(arg[:-1]) * 60
                    elif arg.endswith("d"): mins = int(arg[:-1]) * 1440
                    await bot.restrict_chat_member(cid, uid,
                        ChatPermissions(can_send_messages=False),
                        until_date=datetime.now() + timedelta(minutes=mins))
                    add_mod_history(cid, uid, f"🔇 Мут {mins}м", "голосовая команда", admin_name)
                    executed.append(f"🔇 Замучен {name} на {mins}мин в {known_chats.get(cid, cid)}")

                elif action == "unmute":
                    await bot.restrict_chat_member(cid, uid,
                        ChatPermissions(can_send_messages=True,
                            can_send_media_messages=True, can_send_polls=True,
                            can_send_other_messages=True, can_add_web_page_previews=True))
                    executed.append(f"🔊 Размучен {name} в {known_chats.get(cid, cid)}")

                elif action == "warn":
                    warnings[cid][uid] += 1; save_data()
                    add_mod_history(cid, uid, "⚡ Варн", "голосовая команда", admin_name)
                    executed.append(f"⚡ Варн {name} ({warnings[cid][uid]}) в {known_chats.get(cid, cid)}")

                elif action == "unban":
                    await bot.unban_chat_member(cid, uid, only_if_banned=True)
                    executed.append(f"🕊 Разбанен {name} в {known_chats.get(cid, cid)}")

                elif action == "kick":
                    await bot.ban_chat_member(cid, uid)
                    await bot.unban_chat_member(cid, uid)
                    add_mod_history(cid, uid, "👟 Кик", "голосовая команда", admin_name)
                    executed.append(f"👟 Кикнут {name} из {known_chats.get(cid, cid)}")

            except Exception as e:
                executed.append(f"❌ {known_chats.get(cid, cid)}: {e}")

        return "\n".join(executed) if executed else f"❌ Пользователь {target} не найден ни в одном чате"

    if action == "lockdown":
        locked = 0
        for cid in chats:
            try:
                await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
                await bot.send_message(cid,
                    f"🔒 <b>Чат заблокирован</b> голосовой командой администратора.",
                    parse_mode="HTML")
                locked += 1
            except: pass
        return f"🔒 Локдаун применён к {locked} чатам"

    if action == "unlock":
        unlocked = 0
        for cid in chats:
            try:
                await bot.set_chat_permissions(cid, ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_polls=True, can_send_other_messages=True,
                    can_add_web_page_previews=True))
                unlocked += 1
            except: pass
        return f"🔓 Разблокировано {unlocked} чатов"

    if action == "announce":
        if not arg:
            return "❌ Укажи текст объявления"
        sent = 0
        for cid in chats:
            try:
                await bot.send_message(cid,
                    f"📢 <b>Объявление</b>\n\n{arg}\n\n— {admin_name}",
                    parse_mode="HTML")
                sent += 1
                await asyncio.sleep(0.05)
            except: pass
        return f"📢 Объявление отправлено в {sent} чатов"

    return f"❓ Команда не распознана: {parsed['raw']}"

@dp.message(F.voice, F.chat.type == "private")
async def handle_voice_command(message: Message):
    """Голосовые команды для администраторов в ЛС."""
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("🚫 Голосовые команды только для администраторов.")
        return

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        await message.answer(
            "⚠️ <b>OPENAI_API_KEY не задан</b>\n\n"
            "Добавь в переменные окружения Render:\n"
            "<code>OPENAI_API_KEY=sk-...</code>",
            parse_mode="HTML")
        return

    # Показываем что обрабатываем
    processing = await message.answer("🎙 Распознаю команду...")

    # Скачиваем файл
    import tempfile, os as _os
    voice = message.voice
    try:
        file = await bot.get_file(voice.file_id)
        tmp  = tempfile.NamedTemporaryFile(suffix=".ogg", delete=False)
        tmp_path = tmp.name
        tmp.close()
        await bot.download_file(file.file_path, destination=tmp_path)
    except Exception as e:
        await processing.edit_text(f"❌ Ошибка загрузки файла: {e}")
        return

    # Транскрипция
    try:
        transcript = await _vc_transcribe(tmp_path)
    finally:
        try: _os.unlink(tmp_path)
        except: pass

    if not transcript:
        await processing.edit_text(
            "❌ Не удалось распознать речь.\n"
            "Говори чётко и близко к микрофону.")
        return

    # Парсим команду
    parsed = _vc_parse(transcript)

    if not parsed["action"]:
        await processing.edit_text(
            f"🎙 Распознано: <i>{transcript}</i>\n\n"
            f"❓ Команда не распознана.\n\n"
            f"<b>Доступные команды:</b>\n"
            f"• забань / мут / варн / кик [ID]\n"
            f"• размути / разбань [ID]\n"
            f"• замути [ID] на 30 минут\n"
            f"• заблокируй чат / разблокируй чат\n"
            f"• объяви [текст]\n"
            f"• статус / топ активных",
            parse_mode="HTML")
        return

    # Выполняем
    admin_name = message.from_user.full_name
    result = await _vc_execute(parsed, uid, admin_name)

    # Красивый ответ
    action_icons = {
        "ban": "🔨", "mute": "🔇", "unmute": "🔊",
        "warn": "⚡", "unban": "🕊", "kick": "👟",
        "lockdown": "🔒", "unlock": "🔓",
        "announce": "📢", "status": "📊", "top": "🏆",
    }
    icon = action_icons.get(parsed["action"], "✅")

    await processing.edit_text(
        f"🎙 <b>Распознано:</b> <i>{transcript}</i>\n"
        f"⚡ <b>Команда:</b> {icon} {parsed['action']}"
        + (f" → {parsed['target']}" if parsed['target'] else "")
        + (f" ({parsed['arg']})"    if parsed['arg']    else "")
        + f"\n\n{result}",
        parse_mode="HTML")

    logging.info(f"[VOICE] {admin_name}({uid}): {transcript} → {parsed['action']}")

@dp.message(Command("voicehelp"), F.chat.type == "private")
async def cmd_voicehelp(message: Message):
    """Справка по голосовым командам."""
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "🎙 <b>Голосовые команды</b>\n\n"
        "Отправь мне войс-сообщение — я выполню команду.\n\n"
        "<b>Примеры фраз:</b>\n"
        "• «Забань пользователя 123456789»\n"
        "• «Замути @username на 30 минут»\n"
        "• «Выдай варн ID 987654321»\n"
        "• «Разбань 123456»\n"
        "• «Заблокируй чат»\n"
        "• «Объяви завтра плановые работы»\n"
        "• «Статус» / «Топ активных»\n\n"
        "⚙️ Требуется: <code>OPENAI_API_KEY</code> в переменных окружения.",
        parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  🔧 СИСТЕМА ТЕХНИЧЕСКИХ РАБОТ
# ══════════════════════════════════════════════════════════

TECHWORK_TEXTS = [
    (

        "🔧 Бот временно уходит на техобслуживание\n"
        "⏱ Чат будет закрыт на время работ\n\n"
        "😴 Можно поспать пока мы чиним\n"
        ""
        "🚀 Скоро вернёмся с обновлениями!"
    ),
    (

        "👨‍💻 Наши технари уже всё чинят\n"
        "☕ Выпейте кофе пока ждёте\n\n"
        "⏳ Это займёт совсем немного времени\n"
        ""
        "💫 Возвращаемся скоро!"
    ),
    (

        "🤖 Бот уходит подзарядиться\n"
        "⚡ Заряжаем батарейки и чиним баги\n\n"
        "🎮 Пока есть время — отдыхайте!\n"
        ""
        "🔋 Скоро вернёмся на 100%!"
    ),
]

TECHWORK_END_TEXTS = [
    (

        "🚀 Бот снова в строю!\n"
        "⚡ Всё работает быстрее и лучше\n\n"
        "🎉 Спасибо за ожидание!\n"
        ""
        "💬 Чат снова открыт — добро пожаловать!"
    ),
    (

        "✨ Технические работы завершены\n"
        "🛠 Всё починено и улучшено\n\n"
        "🔥 Готовы к работе!\n"
        ""
        "😎 Погнали!"
    ),
]

techwork_active = False  # флаг активных тех.работ

@dp.message(Command("techwork"))
async def cmd_techwork(message: Message):
    """Включить/выключить режим тех.работ в текущем чате"""
    if message.from_user.id != OWNER_ID: return
    global techwork_active

    cid = message.chat.id
    args = message.text.split()[1:] if message.text else []
    duration_text = f"\n⏱ Примерное время: <b>{' '.join(args)}</b>" if args else ""

    if techwork_active:
        # Завершаем
        techwork_active = False
        end_text = random.choice(TECHWORK_END_TEXTS)
        try:
            await bot.set_chat_permissions(cid, ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True,
                can_add_web_page_previews=True, can_invite_users=True))
            await bot.send_message(cid, end_text, parse_mode="HTML")
        except Exception as e:
            await reply_auto_delete(message, f"❌ Ошибка: {e}"); return
        await log_action(f"✅ <b>ТЕХ.РАБОТЫ ЗАВЕРШЕНЫ</b>\n💬 {message.chat.title}\n👑 {message.from_user.full_name}")
    else:
        # Запускаем
        techwork_active = True
        text = random.choice(TECHWORK_TEXTS) + duration_text
        try:
            await bot.send_message(cid, text, parse_mode="HTML")
            await bot.set_chat_permissions(cid, ChatPermissions(can_send_messages=False))
        except Exception as e:
            await reply_auto_delete(message, f"❌ Ошибка: {e}"); return
        await reply_auto_delete(message,
            "🔧 Тех.работы запущены!\n💡 Завершить: /techwork", parse_mode="HTML")
        await log_action(f"🔧 <b>ТЕХ.РАБОТЫ</b>\n💬 {message.chat.title}\n👑 {message.from_user.full_name}")

@dp.message(Command("techstatus"))
async def cmd_techstatus(message: Message):
    """Статус тех.работ"""
    if message.from_user.id != OWNER_ID: return
    if techwork_active:
        await reply_auto_delete(message,
            "🔧 <b>Тех.работы активны</b>\n"
            f"🔒 Закрыто чатов: {len(known_chats)}\n\n"
            "Для завершения: /techwork",
            parse_mode="HTML")
    else:
        await reply_auto_delete(message,
            "✅ <b>Тех.работы не активны</b>\n"
            "Все чаты работают в штатном режиме",
            parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  💌 АНОНИМНЫЙ КОМПЛИМЕНТ ДНЯ
# ══════════════════════════════════════════════════════════

COMPLIMENTS = [
    "✨ Ты реально крутой человек — даже если сам этого не замечаешь!",
    "💫 С тобой в чате гораздо интереснее. Серьёзно!",
    "🌟 Ты один из тех кто делает этот чат живым!",
    "🔥 Харизма зашкаливает. Так держать!",
    "💎 Редкий человек — умный, интересный и не нудный!",
    "🦁 Ты сильнее чем думаешь. Продолжай в том же духе!",
    "🌈 Когда ты пишешь — чат сразу оживает!",
    "⚡ Энергия от тебя передаётся всем вокруг!",
    "🎯 Ты точно знаешь что говоришь. Уважение!",
    "🏆 Если бы был конкурс на лучшего участника — ты бы выиграл!",
    "🌺 Приятно иметь таких людей в чате. Спасибо что ты здесь!",
    "💪 Ты вдохновляешь других даже не зная об этом!",
    "🎭 С тобой никогда не скучно — это ценность!",
    "🚀 Ты точно добьёшься всего чего хочешь!",
    "🌙 Даже когда молчишь — твоё присутствие ощущается!",
    "😎 Ты просто огонь. Без лишних слов.",
    "🎵 Ты как хорошая музыка — поднимаешь настроение!",
    "🦋 Удача точно на твоей стороне сегодня!",
    "💝 Кто-то в этом чате думает о тебе хорошо. И не один!",
    "🌊 Ты как волна — захватываешь всё вокруг своей энергией!",
]

compliment_last: dict = {}  # {cid: timestamp} — кулдаун

@dp.message(Command("compliment"))
async def cmd_compliment(message: Message):
    """Отправить анонимный комплимент случайному участнику"""
    cid = message.chat.id
    import time as _tc

    # Кулдаун 30 минут на чат
    last = compliment_last.get(cid, 0)
    if _tc.time() - last < 1800:
        remaining = int((1800 - (_tc.time() - last)) / 60)
        await reply_auto_delete(message,
            f"💌 Следующий комплимент через <b>{remaining} мин</b>",
            parse_mode="HTML"); return

    # Берём случайного участника (не того кто написал)
    members = [u for u in chat_stats[cid].keys() if u != message.from_user.id]
    if not members:
        await reply_auto_delete(message, "😅 Некому отправить — в чате нет других участников"); return

    chosen_uid = random.choice(members)
    try:
        tm = await bot.get_chat_member(cid, chosen_uid)
        chosen_name = tm.user.full_name
    except:
        chosen_name = f"участник"

    compliment = random.choice(COMPLIMENTS)
    compliment_last[cid] = _tc.time()

    await message.answer(
        f"💌 <b>Анонимный комплимент</b>\n"
        f"🎯 Для: <b>{chosen_name}</b>\n\n"
        f"{compliment}\n\n"
        f"<i>💝 Отправлено анонимно</i>",
        parse_mode="HTML")

@dp.message(Command("selfcompliment"))
async def cmd_self_compliment(message: Message):
    """Получить комплимент себе"""
    compliment = random.choice(COMPLIMENTS)
    await reply_auto_delete(message,
        f"💌 <b>Комплимент для тебя</b>\n"
        f"{compliment}",
        parse_mode="HTML")

# Фоновая задача — авто-комплимент каждый день в 12:00
compliment_auto_chats: set = set()

@dp.message(Command("autocompliment"))
async def cmd_auto_compliment(message: Message):
    """Вкл/выкл авто-комплимент каждый день"""
    if not await require_admin(message): return
    cid = message.chat.id
    if cid in compliment_auto_chats:
        compliment_auto_chats.discard(cid)
        await reply_auto_delete(message, "💌 Авто-комплимент выключен")
    else:
        compliment_auto_chats.add(cid)
        await reply_auto_delete(message,
            "💌 <b>Авто-комплимент включён!</b>\n"
            "Каждый день в 12:00 бот будет делать комплимент случайному участнику 💝",
            parse_mode="HTML")

async def compliment_daily_loop():
    """Каждый день в 12:00 отправляет комплимент"""
    while True:
        now = datetime.now()
        if now.hour == 12 and now.minute < 5:
            for cid in list(compliment_auto_chats):
                members = list(chat_stats[cid].keys())
                if not members: continue
                try:
                    chosen_uid = random.choice(members)
                    tm = await bot.get_chat_member(cid, chosen_uid)
                    chosen_name = tm.user.full_name
                    compliment = random.choice(COMPLIMENTS)
                    await bot.send_message(cid,
                        f"💌 <b>Комплимент дня</b>\n"
                        f"🎯 Для: <b>{chosen_name}</b>\n\n"
                        f"{compliment}\n\n"
                        f"<i>💝 Каждый день один из вас получает комплимент!</i>",
                        parse_mode="HTML")
                    await asyncio.sleep(0.2)
                except: pass
        await asyncio.sleep(300)

# ══════════════════════════════════════════════════════════
#  📋 УМНЫЕ ШАБЛОНЫ ВАРНОВ
# ══════════════════════════════════════════════════════════

WARN_TEMPLATES = {
    "mat":      ("🤬 Мат в чате",           "⚠️"),
    "reklama":  ("📢 Реклама/спам",          "📵"),
    "18plus":   ("🔞 Контент 18+",           "🔞"),
    "flood":    ("🌊 Флуд/оффтоп",           "💬"),
    "insult":   ("😤 Оскорбление участника", "👊"),
    "admin":    ("🛡 Оскорбление админа",    "⚔️"),
    "drugs":    ("💊 Наркотики",             "🚫"),
    "scam":     ("💸 Мошенничество",         "🕵️"),
    "link":     ("🔗 Запрещённые ссылки",    "🔗"),
    "other":    ("📝 Другая причина",        "📌"),
}

def kb_warn_templates(tid: int) -> InlineKeyboardMarkup:
    """Красивая клавиатура шаблонов варнов"""
    rows = []
    items = list(WARN_TEMPLATES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, (label, emoji) in items[i:i+2]:
            row.append(InlineKeyboardButton(
                text=f"{emoji} {label.split()[0]}",
                callback_data=f"warntp:{tid}:{key}"
            ))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"panel:select:{tid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("warnmenu"))
async def cmd_warnmenu(message: Message):
    """Меню шаблонов варнов"""
    if not await require_admin(message): return
    if not message.reply_to_message:
        await reply_auto_delete(message, "↩️ Реплайни на сообщение нарушителя"); return
    tid = message.reply_to_message.from_user.id
    tname = message.reply_to_message.from_user.full_name
    await reply_auto_delete(message,
        f"📋 <b>Шаблоны варнов</b>\n"
        f"👤 Нарушитель: <b>{tname}</b>\n\n"
        f"Выбери причину:",
        parse_mode="HTML",
        reply_markup=kb_warn_templates(tid))

@dp.callback_query(F.data.startswith("warntp:"))
async def cb_warn_template(call: CallbackQuery):
    if not await is_admin_by_id(call.message.chat.id, call.from_user.id):
        await call.answer("⛔ Команда доступна только администраторам.", show_alert=True); return
    _, tid_str, key = call.data.split(":")
    tid = int(tid_str)
    cid = call.message.chat.id
    label, emoji = WARN_TEMPLATES.get(key, ("Нарушение", "⚠️"))
    if await is_admin_by_id(cid, tid):
        await call.answer("🚫 Нельзя варнить администратора!", show_alert=True); return
    try:
        tm = await bot.get_chat_member(cid, tid)
        tname = tm.user.full_name
    except: tname = f"ID{tid}"
    warnings[cid][tid] = warnings[cid].get(tid, 0) + 1
    warn_count = warnings[cid][tid]
    db_set_int("warnings", cid, tid, "count", warn_count)
    journal_add(cid, call.from_user.id, call.from_user.full_name, f"Варн: {label}", tid, tname)
    if warn_count >= MAX_WARNINGS:
        try:
            await bot.ban_chat_member(cid, tid)
            await call.message.edit_text(
                f"🔨 <b>{tname}</b> забанен!\n"
                f"📌 Причина: {label}\n"
                f"⚡ Варнов было: {warn_count}/{MAX_WARNINGS}",
                parse_mode="HTML")
        except: pass
        await call.answer(f"🔨 {tname} забанен за {label}")
    else:
        await call.message.edit_text(
            f"{emoji} <b>Варн выдан!</b>\n"
            f"👤 {tname}\n"
            f"📌 Причина: {label}\n"
            f"⚡ Варнов: <b>{warn_count}/{MAX_WARNINGS}</b>",
            parse_mode="HTML")
        await call.answer(f"{emoji} Варн: {label}")
    await log_action(
        f"{emoji} <b>ВАРН (шаблон)</b>\n"
        f"👤 {tname} (<code>{tid}</code>)\n"
        f"📌 {label}\n"
        f"⚡ {warn_count}/{MAX_WARNINGS}\n"
        f"👮 {call.from_user.full_name}")

# ══════════════════════════════════════════════════════════
#  🗂 БАЗА НАРУШИТЕЛЕЙ
# ══════════════════════════════════════════════════════════

def db_violators_init():
    conn = db_connect()
    conn.execute("""CREATE TABLE IF NOT EXISTS violators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cid INTEGER, uid INTEGER, name TEXT,
        action TEXT, reason TEXT, mod_name TEXT,
        ts REAL)""")
    conn.commit(); conn.close()

def violator_add(cid: int, uid: int, name: str, action: str, reason: str, mod_name: str):
    import time as _tv
    conn = db_connect()
    conn.execute(
        "INSERT INTO violators (cid,uid,name,action,reason,mod_name,ts) VALUES (?,?,?,?,?,?,?)",
        (cid, uid, name, action, reason, mod_name, _tv.time()))
    conn.commit(); conn.close()

def violator_get(cid: int, uid: int) -> list:
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM violators WHERE cid=? AND uid=? ORDER BY ts DESC LIMIT 20",
        (cid, uid)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def violator_search(cid: int, query: str) -> list:
    conn = db_connect()
    rows = conn.execute(
        "SELECT DISTINCT uid, name, COUNT(*) as cnt FROM violators "
        "WHERE cid=? AND (name LIKE ? OR uid LIKE ?) GROUP BY uid ORDER BY cnt DESC LIMIT 10",
        (cid, f"%{query}%", f"%{query}%")).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@dp.message(Command("violators"))
async def cmd_violators(message: Message):
    """База нарушителей чата"""
    if not await require_admin(message): return
    cid = message.chat.id
    args = message.text.split(None, 1)[1:] if message.text else []

    if message.reply_to_message:
        # Досье конкретного юзера
        target = message.reply_to_message.from_user
        records = violator_get(cid, target.id)
        if not records:
            await reply_auto_delete(message,
                f"✅ <b>{target.full_name}</b> — нарушений не найдено!",
                parse_mode="HTML"); return
        lines = [
            f"🗂 <b>Досье: {target.full_name}</b>\n"
            f"🆔 ID: <code>{target.id}</code>\n"
            f"📊 Всего нарушений: <b>{len(records)}</b>\n"
            ]
        for r in records[:10]:
            dt = datetime.fromtimestamp(r["ts"]).strftime("%d.%m.%Y %H:%M")
            lines.append(f"▸ <b>{r['action']}</b> — {r['reason']}\n"
                        f"  👮 {r['mod_name']} | 🕐 {dt}")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

    elif args:
        # Поиск по имени или ID
        query = args[0]
        results = violator_search(cid, query)
        if not results:
            await reply_auto_delete(message, f"🔍 По запросу <b>{query}</b> ничего не найдено", parse_mode="HTML"); return
        lines = [f"🔍 <b>Поиск: {query}</b>\n"]
        for r in results:
            lines.append(f"👤 <b>{r['name']}</b> (<code>{r['uid']}</code>) — {r['cnt']} нарушений")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

    else:
        # Топ нарушителей чата
        conn = db_connect()
        rows = conn.execute(
            "SELECT uid, name, COUNT(*) as cnt FROM violators WHERE cid=? "
            "GROUP BY uid ORDER BY cnt DESC LIMIT 10", (cid,)).fetchall()
        conn.close()
        if not rows:
            await reply_auto_delete(message, "🗂 База нарушителей пуста — чат чистый! ✅"); return
        lines = ["🗂 <b>Топ нарушителей чата</b>\n"]
        medals = ["🥇", "🥈", "🥉"] + ["▸"] * 10
        for i, r in enumerate(rows):
            lines.append(f"{medals[i]} <b>{r['name']}</b> — {r['cnt']} нарушений")
        lines.append(f"\n💡 /violators (реплай) — досье юзера\n💡 /violators имя — поиск")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  ⚡ ГОРЯЧИЕ КОМАНДЫ /q1 /q2 /q3
# ══════════════════════════════════════════════════════════
# Каждый мод настраивает свои горячие команды
# Хранится: {uid: {1: "текст", 2: "текст", 3: "текст"}}

def db_hotkeys_init():
    conn = db_connect()
    conn.execute("""CREATE TABLE IF NOT EXISTS hotkeys (
        uid INTEGER, slot INTEGER, text TEXT,
        PRIMARY KEY (uid, slot))""")
    conn.commit(); conn.close()

def hotkey_set(uid: int, slot: int, text: str):
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO hotkeys VALUES (?,?,?)", (uid, slot, text))
    conn.commit(); conn.close()

def hotkey_get(uid: int, slot: int) -> str | None:
    conn = db_connect()
    row = conn.execute("SELECT text FROM hotkeys WHERE uid=? AND slot=?", (uid, slot)).fetchone()
    conn.close()
    return row["text"] if row else None

def hotkey_get_all(uid: int) -> dict:
    conn = db_connect()
    rows = conn.execute("SELECT slot, text FROM hotkeys WHERE uid=?", (uid,)).fetchall()
    conn.close()
    return {r["slot"]: r["text"] for r in rows}

@dp.message(Command("setq"))
async def cmd_setq(message: Message):
    """Установить горячую команду: /setq 1 текст"""
    if not await require_admin(message): return
    args = message.text.split(None, 2)[1:] if message.text else []
    if len(args) < 2:
        hk = hotkey_get_all(message.from_user.id)
        lines = ["⚡ <b>Мои горячие команды</b>\n"]
        for slot in [1, 2, 3]:
            text = hk.get(slot, "не задана")
            lines.append(f"▸ /q{slot} — {text[:50] if text != 'не задана' else '❌ не задана'}")
        lines.append("\n📝 <b>Как настроить:</b>\n<code>/setq 1 твой текст</code>")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML"); return
    try:
        slot = int(args[0])
        if slot not in (1, 2, 3):
            await reply_auto_delete(message, "⚠️ Слот должен быть 1, 2 или 3"); return
    except:
        await reply_auto_delete(message, "⚠️ Укажи номер слота: /setq 1 текст"); return
    text = args[1]
    hotkey_set(message.from_user.id, slot, text)
    await reply_auto_delete(message,
        f"✅ <b>Горячая команда /q{slot} сохранена!</b>\n\n"
        f"📝 Текст: {text[:100]}",
        parse_mode="HTML")

async def _send_hotkey(message: Message, slot: int):
    """Общая логика для /q1 /q2 /q3"""
    if not await require_admin(message): return
    text = hotkey_get(message.from_user.id, slot)
    if not text:
        await reply_auto_delete(message,
            f"❌ <b>/q{slot} не настроена</b>\n"
            f"Установи через: <code>/setq {slot} твой текст</code>",
            parse_mode="HTML"); return
    if message.reply_to_message:
        await message.reply_to_message.reply(text, parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")
    try: await message.delete()
    except: pass

@dp.message(Command("q1"))
async def cmd_q1(message: Message): await _send_hotkey(message, 1)

@dp.message(Command("q2"))
async def cmd_q2(message: Message): await _send_hotkey(message, 2)

@dp.message(Command("q3"))
async def cmd_q3(message: Message): await _send_hotkey(message, 3)

# ══════════════════════════════════════════════════════════
#  🚀 ЗАПУСК БОТА
# ══════════════════════════════════════════════════════════
# (секция выше)

# ══════════════════════════════════════════════════════════
#  🎮 НОВЫЕ ОСНОВНЫЕ КОМАНДЫ
# ══════════════════════════════════════════════════════════

@dp.message(Command("privacy"))
async def cmd_privacy(message: Message):
    await reply_auto_delete(message,

        "📊 <b>Что мы собираем:</b>\n"
        "▸ Telegram ID и имя — для идентификации\n"
        "▸ Счётчик сообщений — для XP и уровней\n"
        "▸ История варнов/банов — для модерации\n"
        "▸ Данные профиля — только те что ты указал\n\n"
        "❌ <b>Что НЕ собираем:</b>\n"
        "▸ Содержимое личных сообщений\n"
        "▸ Медиафайлы и голосовые\n"
        "▸ Геолокацию и контакты\n"
        "▸ Платёжные данные\n\n"
        "🔐 <b>Защита данных:</b>\n"
        "▸ Зашифрованная SQLite база\n"
        "▸ Автобэкап каждые 6 часов\n"
        "▸ Данные не продаются третьим лицам\n\n"
        "🗑 <b>Удаление данных:</b>\n"
        "▸ Напиши /deleteme — удалим всё\n\n"
        "📄 Полная версия: PRIVACY_POLICY.md в репозитории",
        parse_mode="HTML")

@dp.message(Command("deleteme"))
async def cmd_deleteme(message: Message):
    uid = message.from_user.id
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"deleteme:confirm:{uid}"),
        InlineKeyboardButton(text="❌ Отмена",      callback_data=f"deleteme:cancel:{uid}"),
    ]])
    await reply_auto_delete(message,
        "🗑 <b>Удаление данных</b>\n\n"
        "Будут удалены:\n"
        "▸ Профиль (био, настроение)\n"
        "▸ XP, уровень, стрик\n"
        "▸ Список друзей\n"
        "▸ Анонимные сообщения\n\n"
        "❗ История нарушений останется для безопасности чата\n\n"
        "Уверен?",
        parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("deleteme:"))
async def cb_deleteme(call: CallbackQuery):
    parts = call.data.split(":")
    action, uid = parts[1], int(parts[2])
    if call.from_user.id != uid:
        await call.answer("🚫 Это не твои данные!", show_alert=True); return
    if action == "cancel":
        await call.message.edit_text("✅ Отменено — данные сохранены")
        return
    try:
        conn = db_connect()
        conn.execute("DELETE FROM user_profiles WHERE uid=?", (uid,))
        conn.execute("DELETE FROM friends WHERE uid=? OR friend_id=?", (uid, uid))
        conn.execute("DELETE FROM relationships WHERE uid1=? OR uid2=?", (uid, uid))
        conn.execute("DELETE FROM subscriptions WHERE subscriber=? OR target=?", (uid, uid))
        conn.execute("DELETE FROM anon_messages WHERE from_uid=? OR to_uid=?", (uid, uid))
        conn.execute("DELETE FROM birthdays WHERE uid=?", (uid,))
        conn.commit(); conn.close()
        # Очищаем RAM
        for cid in list(xp_data.keys()):
            xp_data[cid].pop(uid, None)
            streaks[cid].pop(uid, None)
        await call.message.edit_text(
            "✅ <b>Данные удалены!</b>\n\n"
            "Твой профиль, друзья и история активности очищены.\n"
            "История нарушений сохранена для безопасности чата.",
            parse_mode="HTML")
        await log_action(f"🗑 <b>DELETEME</b>\n👤 {call.from_user.full_name} (<code>{uid}</code>)")
    except Exception as e:
        await call.message.edit_text(f"❌ Ошибка: {e}")
    await call.answer()

# ── Новые команды для обычных пользователей ───────────────

@dp.message(Command("me"))
async def cmd_me(message: Message):
    """Краткая карточка себя"""
    uid = message.from_user.id
    cid = message.chat.id
    conn = db_connect()
    p = conn.execute("SELECT mood, bio FROM user_profiles WHERE uid=?", (uid,)).fetchone()
    friends = conn.execute("SELECT COUNT(*) as c FROM friends WHERE uid=?", (uid,)).fetchone()["c"]
    rel = conn.execute("SELECT rel_type, uid2 FROM relationships WHERE uid1=?", (uid,)).fetchone()
    conn.close()
    xp    = xp_data[cid].get(uid, 0)
    lvl   = get_level(xp)
    emoji_lvl, title_lvl = get_level_title(lvl)
    warns = warnings[cid].get(uid, 0)
    msgs  = chat_stats[cid].get(uid, 0)
    mood  = p["mood"] if p and p["mood"] else "😐"
    bio   = p["bio"]  if p and p["bio"]  else "не указано"
    rel_text = ""
    if rel:
        try:
            tm = await bot.get_chat_member(cid, rel["uid2"])
            rel_text = f"\n❤️ Отношения: {tm.user.full_name}"
        except: pass
    await reply_auto_delete(message,
        f"{emoji_lvl} <b>{message.from_user.full_name}</b>\n"
        f"{mood}  •  {bio}\n"
        f"🏅 Ур. {lvl} — {title_lvl}\n"
        f"⭐ XP: {xp}  •  💬 Сообщ: {msgs}\n"
        f"⚡ Варнов: {warns}/{MAX_WARNINGS}\n"
        f"👥 Друзей: {friends}"
        f"{rel_text}",
        parse_mode="HTML")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    """Топ чата по XP"""
    cid = message.chat.id
    conn = db_connect()
    rows = conn.execute(
        "SELECT uid, xp FROM xp_data WHERE cid=? ORDER BY xp DESC LIMIT 10", (cid,)
    ).fetchall()
    conn.close()
    if not rows:
        await reply_auto_delete(message, "📊 Пока нет данных — пиши больше!"); return
    medals = ["🥇","🥈","🥉"] + ["▸"]*10
    lines = ["🏆 <b>Топ чата по XP</b>\n"]
    for i, r in enumerate(rows):
        try:
            tm = await bot.get_chat_member(cid, r["uid"])
            name = tm.user.full_name
        except: name = f"ID{r['uid']}"
        lvl = get_level(r["xp"])
        lines.append(f"{medals[i]} <b>{name}</b> — {r['xp']} XP (ур.{lvl})")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Статистика чата"""
    cid = message.chat.id
    total_msgs  = sum(chat_stats[cid].values())
    total_warns = sum(warnings[cid].values())
    total_users = len(chat_stats[cid])
    bans_count  = len(ban_list.get(cid, set()))
    conn = db_connect()
    reports_cnt = sum(1 for r in report_queue.get(cid, []) if r.get("status") == "new")
    conn.close()
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  📊  СТАТИСТИКА   ║\n╚══════════════════╝\n\n"
        f"💬 Сообщений: <b>{total_msgs}</b>\n"
        f"👥 Активных: <b>{total_users}</b>\n"
        f"⚡ Варнов: <b>{total_warns}</b>\n"
        f"🔨 Банов: <b>{bans_count}</b>\n"
        f"🚨 Репортов: <b>{reports_cnt}</b>\n"
        f"──────────────────",
        parse_mode="HTML")

@dp.message(Command("daily"))
async def cmd_daily(message: Message):
    """Ежедневный бонус XP"""
    uid = message.from_user.id
    cid = message.chat.id
    import time as _td
    conn = db_connect()
    row = conn.execute(
        "SELECT ts FROM rep_transfer_cooldown WHERE key=?",
        (f"daily_{uid}_{cid}",)).fetchone()
    now = _td.time()
    if row and now - row["ts"] < 86400:
        remaining = int((86400 - (now - row["ts"])) / 3600)
        conn.close()
        await reply_auto_delete(message,
            f"⏰ Следующий бонус через <b>{remaining}ч</b>",
            parse_mode="HTML"); return
    bonus = random.randint(20, 100)
    xp_data[cid][uid] = xp_data[cid].get(uid, 0) + bonus
    db_set_int("xp_data", cid, uid, "xp", xp_data[cid][uid])
    conn.execute("INSERT OR REPLACE INTO rep_transfer_cooldown VALUES (?,?)",
                 (f"daily_{uid}_{cid}", now))
    conn.commit(); conn.close()
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  🎁  ДЕЙЛИ БОНУС  ║\n╚══════════════════╝\n\n"
        f"⭐ +{bonus} XP\n"
        f"💰 Итого XP: <b>{xp_data[cid][uid]}</b>\n"
        f"──────────────────\n"
        f"<i>⏰ Следующий бонус через 24ч</i>",
        parse_mode="HTML")
async def cmd_coinflip(message: Message):
    result = random.choice(["👑 Орёл!", "🪙 Решка!"])
    sides  = ["Орёл" if "Орёл" in result else "Решка"]
    gif    = "🎲" if random.random() > 0.5 else "🪙"
    await reply_auto_delete(message,
        f"🪙 <b>Подбрасываю монетку...</b>\n\n"
        f"{gif} Результат: <b>{result}</b>",
        parse_mode="HTML")

@dp.message(Command("dice"))
async def cmd_dice(message: Message):
    args = message.text.split()[1:] if message.text else []
    try:
        sides = max(2, min(100, int(args[0]))) if args else 6
    except: sides = 6
    result = random.randint(1, sides)
    bar = "█" * int(result / sides * 10) + "░" * (10 - int(result / sides * 10))
    await reply_auto_delete(message,
        f"🎲 <b>Бросаю кубик D{sides}...</b>\n\n"
        f"[{bar}]\n\n"
        f"Выпало: <b>{result}</b> из {sides}",
        parse_mode="HTML")

@dp.message(Command("rate"))
async def cmd_rate(message: Message):
    text = message.text.replace("/rate", "").strip() if message.text else ""
    if not text and message.reply_to_message:
        text = message.reply_to_message.text or "это"
    if not text: text = "это"
    score = random.randint(0, 10)
    bar   = "⭐" * score + "☆" * (10 - score)
    comments = {
        (0,2):  "💀 Полный провал",
        (3,4):  "😬 Так себе",
        (5,6):  "😐 Сойдёт",
        (7,8):  "👍 Неплохо!",
        (9,9):  "🔥 Очень хорошо!",
        (10,10):"💎 ИДЕАЛЬНО!",
    }
    comment = next(v for (lo,hi),v in comments.items() if lo <= score <= hi)
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  ⭐  ОЦЕНКА       ║\n╚══════════════════╝\n\n"
        f"📌 {text[:80]}\n"
        f"{bar}\n"
        f"<b>{score}/10</b> — {comment}\n"
        f"──────────────────",
        parse_mode="HTML")

@dp.message(Command("ship"))
async def cmd_ship(message: Message):
    args = message.text.split()[1:] if message.text else []
    if message.reply_to_message and not args:
        name1 = message.from_user.first_name
        name2 = message.reply_to_message.from_user.first_name
    elif len(args) >= 2:
        name1, name2 = args[0], args[1]
    else:
        await reply_auto_delete(message,
            "💕 Использование:\n<code>/ship Имя1 Имя2</code> или реплай на юзера",
            parse_mode="HTML"); return
    pct = random.randint(0, 100)
    hearts = int(pct / 10)
    bar = "❤️" * hearts + "🤍" * (10 - hearts)
    if pct < 20:   comment = "💔 Шансов нет..."
    elif pct < 40: comment = "😕 Маловато"
    elif pct < 60: comment = "🤔 Может быть..."
    elif pct < 80: comment = "😍 Хорошие шансы!"
    elif pct < 95: comment = "🔥 Огонь!"
    else:          comment = "💞 ИДЕАЛЬНАЯ ПАРА!"
    ship_name = name1[:len(name1)//2] + name2[len(name2)//2:]
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  💕  ШИП          ║\n╚══════════════════╝\n\n"
        f"👤 {name1} + {name2}\n"
        f"💑 Шипнейм: <b>{ship_name}</b>\n"
        f"──────────────────\n"
        f"{bar}\n"
        f"<b>{pct}%</b> — {comment}",
        parse_mode="HTML")

ZODIAC_SIGNS = {
    "♈ Овен":     ["Сегодня звёзды на твоей стороне! Действуй решительно.",
                   "Не торопись с важными решениями — подожди завтра.",
                   "Энергия бьёт через край — направь её в нужное русло.",
                   "Кто-то из близких ждёт твоего первого шага.",
                   "День для смелых решений. Ты как раз такой.",
                   "Не спорь сегодня — даже если ты прав. Особенно если прав.",
                   "Деньги любят тех кто не боится их тратить с умом.",
                   "Прямолинейность сегодня — твоя суперсила и слабость одновременно."],
    "♉ Телец":    ["Финансовый день обещает быть удачным. Следи за деньгами.",
                   "Отличный день для общения с близкими людьми.",
                   "Наслаждайся моментом — ты это заслужил.",
                   "Не меняй планы в последний момент — всё идёт как надо.",
                   "Уютный вечер дома лучше любой вечеринки. И ты знаешь это.",
                   "Твоё упорство сегодня принесёт конкретный результат.",
                   "Покупка которую откладываешь — сделай её. Не пожалеешь.",
                   "Кто-то завидует твоей стабильности. Это комплимент."],
    "♊ Близнецы": ["Твоя коммуникабельность сегодня на пике — используй это!",
                   "Избегай конфликтов — планеты не благоволят ссорам.",
                   "Идея которая пришла утром — запиши. Она важная.",
                   "Сегодня ты можешь убедить кого угодно в чём угодно.",
                   "Раздвоение мнений — норма. Оба варианта могут быть правильными.",
                   "Позвони тому о ком думаешь — он тоже думает о тебе.",
                   "Слова сегодня — твоё оружие. Выбирай их тщательно.",
                   "Новая информация перевернёт твои планы. Не сопротивляйся."],
    "♋ Рак":      ["День интроверта — побудь наедине с собой.",
                   "Интуиция подскажет правильный путь. Доверяй себе.",
                   "Домашний уют сегодня важнее любых приключений.",
                   "Кто-то нуждается в твоей поддержке — ты это почувствуешь.",
                   "Старые воспоминания всплывут сегодня — и это хорошо.",
                   "Не принимай чужую боль близко к сердцу — у тебя своя жизнь.",
                   "Твоя забота о других — сила. Но не забывай о себе.",
                   "Сегодня хороший день чтобы простить кого-то (или себя)."],
    "♌ Лев":      ["Ты в центре внимания сегодня. Сияй!",
                   "Твоя энергия заряжает всех вокруг. Используй это!",
                   "Не скромничай — скромность сегодня неуместна.",
                   "Люди тянутся к тебе — это не случайно.",
                   "Покажи на что ты способен. Сегодня все смотрят.",
                   "Твоя уверенность заразительна. Используй это во благо.",
                   "Грива должна быть в порядке — выгляди на все сто.",
                   "День для больших объявлений и смелых заявлений."],
    "♍ Дева":     ["Детали важны — проверь всё дважды.",
                   "Отличный день для планирования и организации.",
                   "Твой анализ ситуации сегодня безупречен — доверяй ему.",
                   "Порядок в делах принесёт порядок в голове.",
                   "Не критикуй других — они делают как могут.",
                   "Маленький шаг к большой цели — сделай его сегодня.",
                   "Твоя внимательность спасёт кого-то от ошибки.",
                   "Список дел — твой лучший друг сегодня."],
    "♎ Весы":     ["Гармония во всём — твоё кредо сегодня.",
                   "Найди баланс между работой и отдыхом.",
                   "Не можешь выбрать? Подбрось монетку — и сразу поймёшь чего хочешь.",
                   "Красота вокруг тебя сегодня особенно заметна.",
                   "Дипломатия решит то что силой не решить.",
                   "Кто-то ждёт твоего справедливого суждения.",
                   "Компромисс сегодня — не слабость, а мудрость.",
                   "Окружи себя красивыми вещами — настроение улучшится."],
    "♏ Скорпион": ["Твоя проницательность сегодня поразительна.",
                   "Тайны раскроются сами по себе — жди.",
                   "Не показывай карты — пусть думают что ты непредсказуем.",
                   "Глубокий разговор сегодня изменит многое.",
                   "Твои подозрения не беспочвенны — доверяй инстинктам.",
                   "Трансформация которой ты боишься — именно то что нужно.",
                   "Страсть и контроль — найди баланс между ними.",
                   "Отпусти то что держит тебя — освободи место для нового."],
    "♐ Стрелец":  ["Приключения ждут! Не бойся новых горизонтов.",
                   "Оптимизм — твоё оружие сегодня.",
                   "Спонтанное решение окажется лучшим.",
                   "Юмор спасёт любую ситуацию — ты умеешь.",
                   "Путешествие (даже мысленное) освежит взгляд на жизнь.",
                   "Философский вопрос который тебя мучает — ответ придёт сам.",
                   "Честность сегодня важнее дипломатии.",
                   "Мир больше чем твой привычный круг — выйди за его рамки."],
    "♑ Козерог":  ["Упорный труд принесёт плоды уже сегодня.",
                   "Карьерный день — покажи себя с лучшей стороны.",
                   "Терпение — твоя суперсила. Используй её.",
                   "Долгосрочная цель ближе чем кажется.",
                   "Не трать время на тех кто не ценит твои усилия.",
                   "Репутация строится годами — береги её.",
                   "Сегодня хороший день для серьёзного разговора.",
                   "Финансовое решение которое откладываешь — прими его."],
    "♒ Водолей":  ["Нестандартные идеи — твой козырь сегодня.",
                   "Дружба важнее всего — уделяй время близким.",
                   "Твоя уникальность — не странность, а дар.",
                   "Идея которая кажется безумной — может изменить всё.",
                   "Помоги кому-то сегодня — это вернётся сторицей.",
                   "Будущее которое ты представляешь — реально. Иди к нему.",
                   "Одиночество сегодня — не грусть, а перезагрузка.",
                   "Твой взгляд на ситуацию самый объективный — поделись."],
    "♓ Рыбы":     ["Творческий день — вдохновение разлито в воздухе.",
                   "Слушай своё сердце, а не разум.",
                   "Сон который приснился — несёт послание. Вспомни детали.",
                   "Музыка сегодня будет особенно резонировать.",
                   "Не давай всем подряд доступ к своей душе.",
                   "Твоя эмпатия — дар. Не позволяй использовать её против тебя.",
                   "Граница между фантазией и реальностью сегодня очень тонкая.",
                   "Отпусти контроль — и удивишься что произойдёт."],
}

@dp.message(Command("zodiac"))
async def cmd_zodiac(message: Message):
    signs = list(ZODIAC_SIGNS.keys())
    sign  = random.choice(signs)
    pred  = random.choice(ZODIAC_SIGNS[sign])
    lucky_num = random.randint(1, 99)
    lucky_col = random.choice(["🔴 Красный","🔵 Синий","🟢 Зелёный","🟡 Жёлтый",
                                "🟣 Фиолетовый","🟠 Оранжевый","⚪ Белый","⚫ Чёрный"])
    await reply_auto_delete(message,
        f"🔮 <b>Гороскоп дня</b>\n\n"
        f"✨ Знак дня: <b>{sign}</b>\n\n"
        f"📜 {pred}\n\n"
        f"🍀 Счастливое число: <b>{lucky_num}</b>\n"
        f"🎨 Счастливый цвет: {lucky_col}",
        parse_mode="HTML")

FORTUNES = [
    # 🌟 Позитивные
    "🌟 Удача улыбнётся тебе в самый неожиданный момент",
    "💰 Деньги придут откуда не ждёшь — будь готов",
    "❤️ Любовь стучится в твою дверь — открой её",
    "⚡ Большие перемены уже на горизонте",
    "🎯 Твоя цель ближе чем ты думаешь",
    "🌈 После трудностей наступит светлая полоса",
    "🤝 Новая встреча изменит твою жизнь",
    "🔑 Ключ к успеху уже в твоих руках",
    "🌙 Ночью придёт ответ на твой главный вопрос",
    "🦋 Маленькое решение изменит всё",
    "💎 Твоя ценность выше чем ты сам думаешь",
    "🚀 2026 год — твой год. Серьёзно.",
    "🏆 Победа близко — не сдавайся на последнем шаге",
    "🌺 Скоро произойдёт что-то о чём ты давно мечтал",
    "💫 Вселенная уже готовит тебе подарок",
    "🎪 Жизнь скоро устроит тебе сюрприз — хороший",
    "🌠 Звезда которую ты загадал — слышит тебя",
    "🔥 Твоя уверенность сегодня — твоё главное оружие",
    "🧲 Ты притягиваешь правильных людей — доверяй этому",
    "🌊 Волна удачи идёт к тебе — не уходи с берега",
    "🎵 Твоя история только начинается — лучшие главы впереди",
    "🏅 Усилия которые ты вкладываешь — скоро окупятся",
    "🌍 Мир больше чем кажется — и в нём есть место для тебя",

    # 😂 Юмористические
    "☕ Твоё предсказание: выпей кофе и всё станет понятнее",
    "🛌 Звёзды говорят — ложись спать пораньше",
    "📱 Удали несколько приложений — жизнь улучшится",
    "🍕 Сегодня точно стоит заказать пиццу",
    "🤡 Кто-то в этом чате думает что он умнее всех. Это не ты.",
    "💀 Твоя продуктивность сегодня: 404 Not Found",
    "🗑 Выброси что-нибудь ненужное — освободи место для нового",
    "😴 Усталость — это не слабость, это сигнал. Ложись спать.",
    "🐢 Медленно но верно — черепаха уже обгоняет тебя",
    "📺 Ты снова смотришь что-то вместо того чтоб делать важное",
    "🧦 Потерянный носок найдётся. Второй — нет.",
    "🍔 Диета начнётся со следующего понедельника. Как обычно.",
    "📵 Телефон разрядится в самый неподходящий момент",
    "🐱 Кот смотрит на тебя с осуждением. Он прав.",
    "🛒 Ты снова купишь что-то ненужное. Но оно такое красивое.",
    "🎮 Ещё один уровень — и спать. Это ложь и ты знаешь.",
    "🌧 Зонтик оставишь дома. Дождь будет.",
    "🤳 Ты откроешь холодильник и закроешь его ничего не взяв. Трижды.",
    "💬 Напишешь сообщение и удалишь. Потом напишешь снова.",
    "🛵 Курьер доставит еду именно когда ты зайдёшь в душ.",

    # ⚠️ Тревожные
    "⚠️ Осторожно — кто-то завидует твоему успеху",
    "🎲 Рискни — сегодня удача на твоей стороне",
    "📚 Знание которое ты ищешь — уже внутри тебя",
    "🌊 Плыви против течения — там и есть успех",
    "👁 Кто-то следит за тобой... и восхищается",
    "🕵️ Не всё то золото что блестит — проверь дважды",
    "🐍 Среди близких есть тот кто говорит одно а думает другое",
    "🔒 Что-то скрыто от тебя — скоро узнаешь правду",
    "⚡ Не игнорируй интуицию — она сегодня особенно точна",
    "🌑 Тёмная полоса скоро закончится — продержись",

    # 📅 Тема 2026
    "🤖 В 2026 ИИ захватит мир — но тебя пощадит",
    "📈 Крипта снова вырастет — ты же не продал?",
    "🌍 Климат меняется — но твои проблемы остаются прежними",
    "🎮 В 2026 выйдет игра которая сломает тебе жизнь",
    "📱 Следующий iPhone будет стоить как твоя почка",
    "🚗 Электрокары везде — но зарядок всё равно нет",
    "🎵 Тот трек который ты слушаешь уже 100 раз — слушай дальше",
    "🌐 Интернет станет ещё медленнее в самый нужный момент",
    "💸 Цены вырастут — зарплата нет. Классика.",
    "🎄 До Нового года ещё далеко — живи настоящим",
    "🤳 Твоя следующая фотка наберёт много лайков",
    "📊 Дашборд твоей жизни показывает рост — продолжай",
    "⚡ Энергия Меркурия ретроградного влияет на твой WiFi",
    "🎭 2026 год подкинет сюжет круче любого сериала",
    "🔮 Будущее туманно — но твой чай уже остыл. Выпей.",

    # 🎭 Философские
    "🌀 Всё что происходит — происходит именно так как должно",
    "🧘 Остановись на секунду. Подышишь. Всё не так страшно.",
    "🌸 Лучший момент в твоей жизни — тот в котором ты сейчас",
    "🎯 Не сравнивай себя с другими — у них другой старт",
    "💡 Идея которая кажется странной — может быть гениальной",
    "🌱 Рост незаметен изнутри — но он есть",
    "🔭 Смотри дальше чем сегодня — там интереснее",
    "🦅 Орлы не собираются в стаи. Думай об этом.",
]

@dp.message(Command("fortune"))
async def cmd_fortune(message: Message):
    fortune = random.choice(FORTUNES)
    num = random.randint(1, 9999)
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  🔮  СУДЬБА       ║\n╚══════════════════╝\n\n"
        f"✨ {fortune}\n"
        f"──────────────────\n"
        f"🎱 Счастливое число: <b>{num}</b>",
        parse_mode="HTML")

@dp.message(Command("calc"))
async def cmd_calc(message: Message):
    expr = message.text.replace("/calc", "").strip() if message.text else ""
    if not expr:
        await reply_auto_delete(message,
            "🧮 Использование: <code>/calc 2+2*2</code>", parse_mode="HTML"); return
    try:
        # Безопасный eval — только числа и операторы
        import re as _re
        safe = _re.sub(r'[^0-9+\-*/().% ]', '', expr)
        if not safe:
            await reply_auto_delete(message, "⚠️ Недопустимые символы"); return
        result = eval(safe, {"__builtins__": {}})
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  🧮  КАЛЬКУЛЯТОР  ║\n╚══════════════════╝\n\n"
            f"📌 <code>{safe}</code>\n"
            f"= <b>{result}</b>\n"
            f"──────────────────",
            parse_mode="HTML")
    except ZeroDivisionError:
        await reply_auto_delete(message, "❌ Делить на ноль нельзя!")
    except Exception:
        await reply_auto_delete(message, "❌ Неверное выражение")

@dp.message(Command("password"))
async def cmd_password(message: Message):
    import string as _str, secrets as _sec
    args = message.text.split()[1:] if message.text else []
    try:    length = max(4, min(64, int(args[0]))) if args else 16
    except: length = 16
    chars  = _str.ascii_letters + _str.digits + "!@#$%^&*"
    passwd = ''.join(_sec.choice(chars) for _ in range(length))
    strength = "💪 Сильный" if length >= 12 else "😐 Средний" if length >= 8 else "😟 Слабый"
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  🔐  ПАРОЛЬ       ║\n╚══════════════════╝\n\n"
        f"<code>{passwd}</code>\n"
        f"──────────────────\n"
        f"📏 Длина: {length} символов\n"
        f"💪 Надёжность: {strength}\n"
        f"<i>⚠️ Сохрани в надёжном месте!</i>",
        parse_mode="HTML")

@dp.message(Command("qr"))
async def cmd_qr(message: Message):
    text = message.text.replace("/qr", "").strip() if message.text else ""
    if not text:
        await reply_auto_delete(message,
            "📲 Использование: <code>/qr текст или ссылка</code>", parse_mode="HTML"); return
    import urllib.parse
    encoded = urllib.parse.quote(text)
    qr_url  = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={encoded}"
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(qr_url) as resp:
                if resp.status == 200:
                    import io
                    buf = io.BytesIO(await resp.read())
                    buf.name = "qr.png"
                    await message.answer_photo(buf,
                        caption=f"📲 <b>QR-код</b>\n📌 {text[:80]}",
                        parse_mode="HTML")
                    try: await message.delete()
                    except: pass
                else:
                    await reply_auto_delete(message, "❌ Не удалось создать QR-код")
    except Exception as e:
        await reply_auto_delete(message, f"❌ Ошибка: {e}")

@dp.message(Command("ask"))
async def cmd_ask(message: Message):
    question = message.text.replace("/ask", "").strip() if message.text else ""
    answers = [
        "✅ Определённо да!", "✅ Скорее всего да", "✅ Всё указывает на это",
        "🤔 Не уверен...", "🤔 Спроси позже", "🤔 Сложно сказать",
        "❌ Очень сомнительно", "❌ Скорее нет", "❌ Определённо нет!",
        "🔮 Звёзды молчат", "💫 Судьба решит сама", "⚡ Даже не думай об этом",
    ]
    answer = random.choice(answers)
    q_text = f"\n❓ {question}" if question else ""
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  🎱  ВСЕЛЕННАЯ    ║\n╚══════════════════╝\n"
        f"{q_text}\n"
        f"──────────────────\n"
        f"{answer}",
        parse_mode="HTML")

@dp.message(Command("mock"))
async def cmd_mock(message: Message):
    text = message.text.replace("/mock", "").strip() if message.text else ""
    if not text and message.reply_to_message:
        text = message.reply_to_message.text or ""
    if not text:
        await reply_auto_delete(message, "🤪 Использование: /mock текст"); return
    mocked = "".join(c.upper() if i % 2 else c.lower() for i, c in enumerate(text))
    await reply_auto_delete(message, f"🤪 {mocked}")

@dp.message(Command("reverse"))
async def cmd_reverse(message: Message):
    text = message.text.replace("/reverse", "").strip() if message.text else ""
    if not text and message.reply_to_message:
        text = message.reply_to_message.text or ""
    if not text:
        await reply_auto_delete(message, "🔄 Использование: /reverse текст"); return
    await reply_auto_delete(message, f"🔄 {text[::-1]}")

@dp.message(Command("count"))
async def cmd_count(message: Message):
    text = message.text.replace("/count", "").strip() if message.text else ""
    if not text and message.reply_to_message:
        text = message.reply_to_message.text or ""
    if not text:
        await reply_auto_delete(message, "📊 Использование: /count текст"); return
    words   = len(text.split())
    chars   = len(text)
    no_sp   = len(text.replace(" ", ""))
    lines   = text.count("\n") + 1
    await reply_auto_delete(message,
        f"📊 <b>Статистика текста</b>\n\n"
        f"📝 Символов: <b>{chars}</b>\n"
        f"📝 Без пробелов: <b>{no_sp}</b>\n"
        f"💬 Слов: <b>{words}</b>\n"
        f"↩️ Строк: <b>{lines}</b>",
        parse_mode="HTML")
# ══════════════════════════════════════════════════════════
#  БАН / МУТ ПО ID (без реплая)
# ══════════════════════════════════════════════════════════

@dp.message(Command("banid"))
async def cmd_banid(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🔨  БАН ПО ID    ║\n╚══════════════════╝\n\n"
            "⚠️ Использование:\n<code>/banid 123456789 причина</code>", parse_mode="HTML"); return
    parts = command.args.split(None, 1)
    try: uid = int(parts[0])
    except ValueError:
        await reply_auto_delete(message, "⚠️ Укажи корректный Telegram ID"); return
    reason = parts[1] if len(parts) > 1 else "Нарушение правил"
    cid = message.chat.id
    try:
        await bot.ban_chat_member(cid, uid)
        ban_list[cid][uid] = {
            "name": f"ID {uid}", "reason": reason,
            "by": message.from_user.full_name,
            "time": datetime.now().strftime("%d.%m.%Y %H:%M"), "temp": False
        }
        save_data()
        add_mod_history(cid, uid, "🔨 Бан по ID", reason, message.from_user.full_name)
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  🔨  БАН ПО ID    ║\n╚══════════════════╝\n\n"
            f"🪪 ID: <code>{uid}</code>\n📋 Причина: <b>{reason}</b>\n──────────────────\n"
            f"<i>Пользователь заблокирован.</i>", parse_mode="HTML")
        await log_action(f"🔨 <b>Бан по ID</b>\n👤 Кто: {message.from_user.mention_html()}\n🪪 ID: <code>{uid}</code>\n📋 Причина: {reason}\n💬 Чат: {message.chat.title}")
    except Exception as e:
        await reply_auto_delete(message, f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")


@dp.message(Command("muteid"))
async def cmd_muteid(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🔇  МУТ ПО ID    ║\n╚══════════════════╝\n\n"
            "⚠️ Использование:\n<code>/muteid 123456789 60m причина</code>", parse_mode="HTML"); return
    parts = command.args.split(None, 2)
    try: uid = int(parts[0])
    except ValueError:
        await reply_auto_delete(message, "⚠️ Укажи корректный Telegram ID"); return
    mins, label = parse_duration(parts[1]) if len(parts) > 1 else (60, "1 ч.")
    if not mins: mins, label = 60, "1 ч."
    reason = parts[2] if len(parts) > 2 else "Нарушение правил"
    cid = message.chat.id
    try:
        await bot.restrict_chat_member(cid, uid,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=datetime.now() + timedelta(minutes=mins))
        add_mod_history(cid, uid, f"🔇 Мут {label}", reason, message.from_user.full_name)
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  🔇  МУТ ПО ID    ║\n╚══════════════════╝\n\n"
            f"🪪 ID: <code>{uid}</code>\n⏱ Время: <b>{label}</b>\n📋 Причина: <b>{reason}</b>\n──────────────────",
            parse_mode="HTML")
        await log_action(f"🔇 <b>Мут по ID</b>\n👤 Кто: {message.from_user.mention_html()}\n🪪 ID: <code>{uid}</code>\n⏱ {label}\n📋 {reason}\n💬 Чат: {message.chat.title}")
        schedule_unmute(cid, uid, mins, f"ID {uid}")
    except Exception as e:
        await reply_auto_delete(message, f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  РАССЫЛКА /broadcast
# ══════════════════════════════════════════════════════════

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "⛔ Только для владельца."); return
    if not command.args:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  📢  РАССЫЛКА     ║\n╚══════════════════╝\n\n"
            "Использование:\n<code>/broadcast текст сообщения</code>\n\n"
            "<i>Отправит во все известные чаты.</i>", parse_mode="HTML"); return
    text = command.args
    conn = db_connect()
    chats = [row[0] for row in conn.execute("SELECT cid FROM known_chats").fetchall()]
    conn.close()
    sent = failed = 0
    status_msg = await message.answer(f"⏳ Рассылка в {len(chats)} чатов...", parse_mode="HTML")
    for cid in chats:
        try:
            await bot.send_message(cid,
                f"╔══════════════════╗\n║  📢  ОБЪЯВЛЕНИЕ   ║\n╚══════════════════╝\n\n{text}\n──────────────────",
                parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    try:
        await status_msg.edit_text(
            f"╔══════════════════╗\n║  📢  РАССЫЛКА     ║\n╚══════════════════╝\n\n"
            f"✅ Отправлено: <b>{sent}</b>\n❌ Ошибок: <b>{failed}</b>", parse_mode="HTML")
    except: pass
    await log_action(f"📢 <b>Рассылка</b>\n👤 {message.from_user.mention_html()}\n📨 Отправлено: {sent}/{len(chats)}\n📝 {text[:100]}")


# ══════════════════════════════════════════════════════════
#  ЖУРНАЛ МОДЕРАЦИИ /modlog
# ══════════════════════════════════════════════════════════

@dp.message(Command("modlog"))
async def cmd_modlog(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    limit = 20
    try: limit = max(5, min(int(command.args), 50)) if command.args else 20
    except: pass
    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT action, reason, by_name, target_name, created_at FROM mod_history "
            "WHERE cid=? ORDER BY created_at DESC LIMIT ?", (cid, limit)
        ).fetchall()
    except:
        rows = []
    conn.close()
    if not rows:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  📋  MODLOG       ║\n╚══════════════════╝\n\n"
            "<i>История пуста.</i>", parse_mode="HTML"); return
    lines = [f"╔══════════════════╗\n║  📋  ЖУРНАЛ МОД   ║\n╚══════════════════╝\n\n<b>Последние {limit} действий:</b>\n"]
    for r in rows:
        ts = str(r["created_at"] if "created_at" in r.keys() else "—")[:16]
        lines.append(
            f"──────────────────\n"
            f"🕐 {ts}\n"
            f"⚡ {r['action']}\n"
            f"👤 {r['target_name'] if 'target_name' in r.keys() else '—'}\n"
            f"📋 {(r['reason'] or '—')[:50]}\n"
            f"👮 {r['by_name'] if 'by_name' in r.keys() else '—'}"
        )
    text = "\n".join(lines)
    if len(text) > 4000: text = text[:4000] + "\n\n<i>...обрезано</i>"
    await reply_auto_delete(message, text, parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  АЛЕРТЫ /alerts
# ══════════════════════════════════════════════════════════

@dp.message(Command("alerts"))
async def cmd_alerts(message: Message, command: CommandObject):
    if not await require_admin(message): return
    alerts_list = shared.alerts
    if not alerts_list:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🚨  АЛЕРТЫ       ║\n╚══════════════════╝\n\n"
            "✅ <i>Алертов нет.</i>", parse_mode="HTML"); return
    limit = 10
    lines = [f"╔══════════════════╗\n║  🚨  АЛЕРТЫ       ║\n╚══════════════════╝\n\n<b>Последние {min(limit, len(alerts_list))}:</b>\n"]
    level_icon = {"danger": "🔴", "warn": "🟡", "info": "🔵"}
    for a in alerts_list[:limit]:
        icon = level_icon.get(a.get("level", "info"), "⚪")
        lines.append(
            f"──────────────────\n"
            f"{icon} <b>{a.get('title', '—')}</b>\n"
            f"📝 {a.get('desc', '—')[:80]}\n"
            f"🕐 {a.get('time', '—')}"
        )
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


@dp.message(Command("alertsclear"))
async def cmd_alerts_clear(message: Message):
    if not await require_admin(message): return
    shared.alerts.clear()
    await reply_auto_delete(message,
        "╔══════════════════╗\n║  🚨  АЛЕРТЫ       ║\n╚══════════════════╝\n\n"
        "✅ <i>Все алерты очищены.</i>", parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  АПЕЛЛЯЦИИ /appeals
# ══════════════════════════════════════════════════════════

@dp.message(Command("appeal"))
async def cmd_appeal(message: Message, command: CommandObject):
    """Пользователь подаёт апелляцию на бан"""
    uid = message.from_user.id
    if not command.args:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  📨  АПЕЛЛЯЦИЯ    ║\n╚══════════════════╝\n\n"
            "Использование:\n<code>/appeal причина разбана</code>", parse_mode="HTML"); return
    conn = db_connect()
    try:
        existing = conn.execute("SELECT data FROM appeals_db WHERE uid=?", (uid,)).fetchone()
        import json as _json
        if existing:
            data = _json.loads(existing["data"])
            if data.get("status") == "pending":
                conn.close()
                await reply_auto_delete(message,
                    "╔══════════════════╗\n║  📨  АПЕЛЛЯЦИЯ    ║\n╚══════════════════╝\n\n"
                    "⏳ <i>У тебя уже есть активная апелляция. Ожидай решения.</i>", parse_mode="HTML"); return
        appeal_data = {
            "uid": uid, "name": message.from_user.full_name,
            "text": command.args, "status": "pending",
            "cid": message.chat.id, "ts": _time_module.time()
        }
        conn.execute("INSERT OR REPLACE INTO appeals_db VALUES (?, ?)",
                     (uid, _json.dumps(appeal_data)))
        conn.commit()
    finally:
        conn.close()
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  📨  АПЕЛЛЯЦИЯ    ║\n╚══════════════════╝\n\n"
        f"✅ Апелляция подана!\n──────────────────\n"
        f"📝 {command.args[:200]}\n──────────────────\n"
        f"<i>Администраторы рассмотрят твой запрос.</i>", parse_mode="HTML")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"╔══════════════════╗\n║  📨  АПЕЛЛЯЦИЯ    ║\n╚══════════════════╝\n\n"
                f"👤 {message.from_user.mention_html()}\n"
                f"🪪 ID: <code>{uid}</code>\n"
                f"📝 {command.args[:300]}\n──────────────────\n"
                f"<i>Используй /appeals для просмотра всех апелляций.</i>",
                parse_mode="HTML")
        except: pass


@dp.message(Command("appeals"))
async def cmd_appeals_list(message: Message):
    if not await require_admin(message): return
    conn = db_connect()
    import json as _json
    rows = conn.execute("SELECT uid, data FROM appeals_db").fetchall()
    conn.close()
    pending = []
    for row in rows:
        try:
            d = _json.loads(row["data"])
            if d.get("status") == "pending":
                pending.append(d)
        except: pass
    if not pending:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  📨  АПЕЛЛЯЦИИ    ║\n╚══════════════════╝\n\n"
            "✅ <i>Активных апелляций нет.</i>", parse_mode="HTML"); return
    lines = [f"╔══════════════════╗\n║  📨  АПЕЛЛЯЦИИ    ║\n╚══════════════════╝\n\n<b>Ожидают решения: {len(pending)}</b>\n"]
    for d in pending[:10]:
        lines.append(
            f"──────────────────\n"
            f"👤 {d.get('name','—')} (<code>{d.get('uid','—')}</code>)\n"
            f"📝 {str(d.get('text','—'))[:100]}\n"
            f"✅ /appealapprove {d.get('uid')}  ❌ /appealdeny {d.get('uid')}"
        )
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


@dp.message(Command("appealapprove"))
async def cmd_appeal_approve(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message, "⚠️ /appealapprove [user_id]"); return
    try: uid = int(command.args.split()[0])
    except: await reply_auto_delete(message, "⚠️ Укажи корректный ID"); return
    conn = db_connect()
    import json as _json
    row = conn.execute("SELECT data FROM appeals_db WHERE uid=?", (uid,)).fetchone()
    if not row:
        conn.close()
        await reply_auto_delete(message, "❌ Апелляция не найдена."); return
    d = _json.loads(row["data"])
    d["status"] = "approved"
    conn.execute("INSERT OR REPLACE INTO appeals_db VALUES (?,?)", (uid, _json.dumps(d)))
    conn.commit(); conn.close()
    cid = d.get("cid", message.chat.id)
    try:
        await bot.unban_chat_member(cid, uid, only_if_banned=True)
        ban_list[cid].pop(uid, None); save_data()
    except: pass
    try:
        await bot.send_message(uid,
            "╔══════════════════╗\n║  ✅  АПЕЛЛЯЦИЯ    ║\n╚══════════════════╝\n\n"
            "Твоя апелляция <b>одобрена</b>!\nБлокировка снята.", parse_mode="HTML")
    except: pass
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  ✅  ОДОБРЕНО     ║\n╚══════════════════╝\n\n"
        f"🪪 ID: <code>{uid}</code>\n<i>Пользователь разбанен и уведомлён.</i>", parse_mode="HTML")


@dp.message(Command("appealdeny"))
async def cmd_appeal_deny(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message, "⚠️ /appealdeny [user_id] [причина]"); return
    parts = command.args.split(None, 1)
    try: uid = int(parts[0])
    except: await reply_auto_delete(message, "⚠️ Укажи корректный ID"); return
    reason = parts[1] if len(parts) > 1 else "Без объяснений"
    conn = db_connect()
    import json as _json
    row = conn.execute("SELECT data FROM appeals_db WHERE uid=?", (uid,)).fetchone()
    if not row: conn.close(); await reply_auto_delete(message, "❌ Апелляция не найдена."); return
    d = _json.loads(row["data"])
    d["status"] = "rejected"; d["reply"] = reason
    conn.execute("INSERT OR REPLACE INTO appeals_db VALUES (?,?)", (uid, _json.dumps(d)))
    conn.commit(); conn.close()
    try:
        await bot.send_message(uid,
            f"╔══════════════════╗\n║  ❌  АПЕЛЛЯЦИЯ    ║\n╚══════════════════╝\n\n"
            f"Твоя апелляция <b>отклонена</b>.\n📋 Причина: {reason}", parse_mode="HTML")
    except: pass
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  ❌  ОТКЛОНЕНО    ║\n╚══════════════════╝\n\n"
        f"🪪 ID: <code>{uid}</code>\n📋 {reason}", parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  НАСТРОЙКИ ЧАТА /chatsettings
# ══════════════════════════════════════════════════════════

def _bool_str(val) -> str:
    return "✅ вкл" if val else "❌ выкл"


def kb_chatsettings_main(cid: int) -> InlineKeyboardMarkup:
    s = cs.get_settings(cid)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🛡 Антиспам {_bool_str(s.get('antispam_enabled'))}", callback_data=f"cs_toggle:{cid}:antispam_enabled")],
        [InlineKeyboardButton(text=f"🧼 Антимат {_bool_str(s.get('antimat_enabled'))}", callback_data=f"cs_toggle:{cid}:antimat_enabled")],
        [InlineKeyboardButton(text=f"🔗 Антиссылки {_bool_str(s.get('antilink_enabled'))}", callback_data=f"cs_toggle:{cid}:antilink_enabled")],
        [InlineKeyboardButton(text=f"🔠 Антикапс {_bool_str(s.get('anticaps_enabled'))}", callback_data=f"cs_toggle:{cid}:anticaps_enabled")],
        [InlineKeyboardButton(text=f"👋 Приветствие {_bool_str(s.get('welcome_enabled'))}", callback_data=f"cs_toggle:{cid}:welcome_enabled")],
        [InlineKeyboardButton(text=f"✅ Верификация {_bool_str(s.get('verify_enabled'))}", callback_data=f"cs_toggle:{cid}:verify_enabled")],
        [InlineKeyboardButton(text=f"⭐ XP система {_bool_str(s.get('xp_enabled'))}", callback_data=f"cs_toggle:{cid}:xp_enabled")],
        [InlineKeyboardButton(text=f"💰 Экономика {_bool_str(s.get('economy_enabled'))}", callback_data=f"cs_toggle:{cid}:economy_enabled")],
        [InlineKeyboardButton(text=f"🎮 Игры {_bool_str(s.get('games_enabled'))}", callback_data=f"cs_toggle:{cid}:games_enabled")],
        [InlineKeyboardButton(text=f"📢 Авто-анонс {_bool_str(s.get('announce_enabled'))}", callback_data=f"cs_toggle:{cid}:announce_enabled")],
        [InlineKeyboardButton(text=f"⏰ Расписание {_bool_str(s.get('schedule_enabled'))}", callback_data=f"cs_toggle:{cid}:schedule_enabled")],
        [InlineKeyboardButton(text=f"🌙 Тихий час {_bool_str(s.get('quiet_enabled'))}", callback_data=f"cs_toggle:{cid}:quiet_enabled")],
        [InlineKeyboardButton(text="⚙️ Параметры модерации", callback_data=f"cs_mod:{cid}")],
        [InlineKeyboardButton(text="🔢 Параметры XP/экономики", callback_data=f"cs_xp:{cid}")],
        [InlineKeyboardButton(text="🕐 Расписание чата", callback_data=f"cs_schedule:{cid}")],
    ])


def kb_cs_mod(cid: int) -> InlineKeyboardMarkup:
    s = cs.get_settings(cid)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚠️ Макс варнов: {s.get('max_warns',3)}", callback_data=f"cs_set:{cid}:max_warns")],
        [InlineKeyboardButton(text=f"⏳ Срок варна: {s.get('warn_expiry_days',30)} дн.", callback_data=f"cs_set:{cid}:warn_expiry_days")],
        [InlineKeyboardButton(text=f"🔇 Мут по умолч.: {s.get('mute_duration',60)} мин.", callback_data=f"cs_set:{cid}:mute_duration")],
        [InlineKeyboardButton(text=f"💬 Флуд порог: {s.get('flood_msgs',10)} msg/мин", callback_data=f"cs_set:{cid}:flood_msgs")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"cs_main:{cid}")],
    ])


def kb_cs_xp(cid: int) -> InlineKeyboardMarkup:
    s = cs.get_settings(cid)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⭐ XP за сообщение: {s.get('xp_per_msg',5)}", callback_data=f"cs_set:{cid}:xp_per_msg")],
        [InlineKeyboardButton(text=f"🎁 Дейли бонус: {s.get('daily_bonus',50)}", callback_data=f"cs_set:{cid}:daily_bonus")],
        [InlineKeyboardButton(text=f"🎉 Бонус новичка: {s.get('newcomer_bonus',100)}", callback_data=f"cs_set:{cid}:newcomer_bonus")],
        [InlineKeyboardButton(text=f"⏱ Кулдаун репы: {s.get('rep_cooldown_hours',1)} ч.", callback_data=f"cs_set:{cid}:rep_cooldown_hours")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"cs_main:{cid}")],
    ])


def kb_cs_schedule(cid: int) -> InlineKeyboardMarkup:
    s = cs.get_settings(cid)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔒 Закрытие: {s.get('close_time','00:00')}", callback_data=f"cs_set:{cid}:close_time")],
        [InlineKeyboardButton(text=f"🔓 Открытие: {s.get('open_time','08:00')}", callback_data=f"cs_set:{cid}:open_time")],
        [InlineKeyboardButton(text=f"🌙 Тихий с: {s.get('quiet_start','23:00')}", callback_data=f"cs_set:{cid}:quiet_start")],
        [InlineKeyboardButton(text=f"☀️ Тихий до: {s.get('quiet_end','07:00')}", callback_data=f"cs_set:{cid}:quiet_end")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"cs_main:{cid}")],
    ])


@dp.message(Command("chatsettings"))
async def cmd_chatsettings(message: Message):
    if not await require_admin(message): return
    cid = message.chat.id
    s = cs.get_settings(cid)
    text = (
        f"╔══════════════════╗\n║  ⚙️  НАСТРОЙКИ ЧАТА ║\n╚══════════════════╝\n\n"
        f"💬 Чат: <b>{message.chat.title}</b>\n"
        f"🪪 ID: <code>{cid}</code>\n"
        f"──────────────────\n"
        f"Нажми кнопку чтобы переключить настройку:"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=kb_chatsettings_main(cid))


@dp.message(Command("antivpn"))
async def cmd_antivpn(message: Message, command: CommandObject):
    if not await require_admin(message): return
    cid = message.chat.id
    current = _antivpn_get(cid)

    if not command.args:
        status = "✅ включён" if current["enabled"] else "❌ выключен"
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
            f"Статус: <b>{status}</b>\n"
            f"Действие: <b>{current['action']}</b>\n\n"
            f"<b>Команды:</b>\n"
            f"/antivpn on — включить\n"
            f"/antivpn off — выключить\n"
            f"/antivpn action warn/kick/ban — действие\n\n"
            f"<i>⚠️ Для проверки IP нужен VPN_API_KEY в .env\nПолучить: vpnapi.io</i>",
            parse_mode="HTML"); return

    args = command.args.lower().split()
    if args[0] == "on":
        _antivpn_settings[cid] = {**current, "enabled": True}
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
            "✅ АнтиVPN <b>включён</b>!\n"
            "<i>Новые участники будут проверяться.</i>", parse_mode="HTML")
    elif args[0] == "off":
        _antivpn_settings[cid] = {**current, "enabled": False}
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
            "❌ АнтиVPN <b>выключен</b>.", parse_mode="HTML")
    elif args[0] == "action" and len(args) > 1 and args[1] in ("warn", "kick", "ban"):
        _antivpn_settings[cid] = {**current, "action": args[1]}
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
            f"⚙️ Действие изменено: <b>{args[1]}</b>", parse_mode="HTML")
    else:
        await reply_auto_delete(message, "⚠️ /antivpn on/off/action warn|kick|ban")


@dp.callback_query(F.data.startswith("cs_toggle:"))
async def cb_cs_toggle(call: CallbackQuery):
    if not await check_admin(call.message): 
        await call.answer("⛔ Только для админов", show_alert=True); return
    _, cid_str, key = call.data.split(":", 2)
    cid = int(cid_str)
    s = cs.get_settings(cid)
    s[key] = not s.get(key, False)
    cs.save_settings(cid, s)
    val_str = "включено ✅" if s[key] else "выключено ❌"
    await call.answer(f"{key}: {val_str}")
    try:
        await call.message.edit_reply_markup(reply_markup=kb_chatsettings_main(cid))
    except: pass


@dp.callback_query(F.data.startswith("cs_mod:"))
async def cb_cs_mod(call: CallbackQuery):
    if not await check_admin(call.message):
        await call.answer("⛔", show_alert=True); return
    cid = int(call.data.split(":")[1])
    await call.message.edit_text(
        f"╔══════════════════╗\n║  ⚙️  МОДЕРАЦИЯ    ║\n╚══════════════════╝\n\nПараметры модерации чата:",
        parse_mode="HTML", reply_markup=kb_cs_mod(cid))
    await call.answer()


@dp.callback_query(F.data.startswith("cs_xp:"))
async def cb_cs_xp(call: CallbackQuery):
    if not await check_admin(call.message):
        await call.answer("⛔", show_alert=True); return
    cid = int(call.data.split(":")[1])
    await call.message.edit_text(
        f"╔══════════════════╗\n║  ⚙️  XP / ЭКОНОМИКА ║\n╚══════════════════╝\n\nПараметры XP и экономики:",
        parse_mode="HTML", reply_markup=kb_cs_xp(cid))
    await call.answer()


@dp.callback_query(F.data.startswith("cs_schedule:"))
async def cb_cs_schedule(call: CallbackQuery):
    if not await check_admin(call.message):
        await call.answer("⛔", show_alert=True); return
    cid = int(call.data.split(":")[1])
    await call.message.edit_text(
        f"╔══════════════════╗\n║  ⚙️  РАСПИСАНИЕ   ║\n╚══════════════════╝\n\nВремя открытия/закрытия чата:",
        parse_mode="HTML", reply_markup=kb_cs_schedule(cid))
    await call.answer()


@dp.callback_query(F.data.startswith("cs_main:"))
async def cb_cs_main(call: CallbackQuery):
    if not await check_admin(call.message):
        await call.answer("⛔", show_alert=True); return
    cid = int(call.data.split(":")[1])
    await call.message.edit_text(
        f"╔══════════════════╗\n║  ⚙️  НАСТРОЙКИ ЧАТА ║\n╚══════════════════╝\n\n"
        f"🪪 ID: <code>{cid}</code>\n──────────────────\nНажми кнопку чтобы переключить:",
        parse_mode="HTML", reply_markup=kb_chatsettings_main(cid))
    await call.answer()


# Ожидание ввода значения настройки
_cs_pending_input: dict = {}  # {uid: {"cid": int, "key": str, "msg_id": int}}

@dp.callback_query(F.data.startswith("cs_set:"))
async def cb_cs_set(call: CallbackQuery):
    if not await check_admin(call.message):
        await call.answer("⛔", show_alert=True); return
    _, cid_str, key = call.data.split(":", 2)
    cid = int(cid_str)
    key_labels = {
        "max_warns": "максимальное количество варнов (число)",
        "warn_expiry_days": "срок действия варна в днях (число)",
        "mute_duration": "длительность мута по умолчанию в минутах (число)",
        "flood_msgs": "порог флуда — сообщений в минуту (число)",
        "xp_per_msg": "XP за одно сообщение (число)",
        "daily_bonus": "размер дейли бонуса (число)",
        "newcomer_bonus": "бонус новичка (число)",
        "rep_cooldown_hours": "кулдаун репутации в часах (число)",
        "close_time": "время закрытия чата (формат HH:MM)",
        "open_time": "время открытия чата (формат HH:MM)",
        "quiet_start": "начало тихого часа (формат HH:MM)",
        "quiet_end": "конец тихого часа (формат HH:MM)",
    }
    label = key_labels.get(key, key)
    _cs_pending_input[call.from_user.id] = {"cid": cid, "key": key}
    await call.message.answer(
        f"╔══════════════════╗\n║  ✏️  ИЗМЕНЕНИЕ    ║\n╚══════════════════╝\n\n"
        f"Введи новое значение для:\n<b>{label}</b>",
        parse_mode="HTML")
    await call.answer()


@dp.message(F.text & ~F.text.startswith("/") & F.chat.type.in_({"group", "supergroup"}))
async def handle_cs_input(message: Message):
    uid = message.from_user.id
    if uid not in _cs_pending_input: return
    if not message.text: return
    data = _cs_pending_input.pop(uid)
    cid, key = data["cid"], data["key"]
    val = message.text.strip()
    # Определяем тип значения
    time_keys = {"close_time", "open_time", "quiet_start", "quiet_end"}
    if key in time_keys:
        import re as _re
        if not _re.match(r"^\d{2}:\d{2}$", val):
            await reply_auto_delete(message, "⚠️ Формат: HH:MM (например 08:00)"); return
        cs.update_setting(cid, key, val)
    else:
        try:
            cs.update_setting(cid, key, int(val))
        except ValueError:
            await reply_auto_delete(message, "⚠️ Введи числовое значение"); return
    await reply_auto_delete(message,
        f"╔══════════════════╗\n║  ✅  СОХРАНЕНО    ║\n╚══════════════════╝\n\n"
        f"⚙️ <b>{key}</b> = <code>{val}</code>", parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  ИНЦИДЕНТЫ /incidents
# ══════════════════════════════════════════════════════════

@dp.message(Command("incidents"))
async def cmd_incidents(message: Message):
    if not await require_admin(message): return
    alerts_list = [a for a in shared.alerts if a.get("level") == "danger"]
    if not alerts_list:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🚨  ИНЦИДЕНТЫ    ║\n╚══════════════════╝\n\n"
            "✅ <i>Критических инцидентов нет.</i>", parse_mode="HTML"); return
    lines = [f"╔══════════════════╗\n║  🚨  ИНЦИДЕНТЫ    ║\n╚══════════════════╝\n\n<b>Критических: {len(alerts_list)}</b>\n"]
    for a in alerts_list[:15]:
        lines.append(
            f"──────────────────\n"
            f"🔴 <b>{a.get('title','—')}</b>\n"
            f"📝 {a.get('desc','—')[:100]}\n"
            f"🕐 {a.get('time','—')}"
        )
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  🔴 ТРИГГЕР-СЛОВА
# ══════════════════════════════════════════════════════════

# Хранилище триггеров: {cid: [{word, action, reason, added_by, ts}]}
_trigger_words: dict = {}

def _triggers_db_init():
    conn = db_connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trigger_words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cid INTEGER, word TEXT, action TEXT DEFAULT 'warn',
            reason TEXT DEFAULT 'Использование запрещённого слова',
            added_by TEXT, ts INTEGER
        )""")
    conn.commit(); conn.close()

def _triggers_load(cid: int) -> list:
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM trigger_words WHERE cid=?", (cid,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def _trigger_add(cid: int, word: str, action: str, reason: str, by: str):
    conn = db_connect()
    conn.execute(
        "INSERT INTO trigger_words (cid,word,action,reason,added_by,ts) VALUES (?,?,?,?,?,?)",
        (cid, word.lower(), action, reason, by, int(_time_module.time()))
    )
    conn.commit(); conn.close()

def _trigger_del(cid: int, word: str):
    conn = db_connect()
    conn.execute("DELETE FROM trigger_words WHERE cid=? AND word=?", (cid, word.lower()))
    conn.commit(); conn.close()

async def _check_trigger_words(message: Message, cid: int, uid: int):
    """Проверяет сообщение на триггер-слова и применяет действие"""
    if uid in ADMIN_IDS: return
    try:
        member = await bot.get_chat_member(cid, uid)
        if member.status in ("administrator", "creator"): return
    except: pass

    triggers = _triggers_load(cid)
    if not triggers: return

    text_lower = message.text.lower()
    for t in triggers:
        if t["word"] in text_lower:
            action = t.get("action", "warn")
            reason = t.get("reason", "Триггер-слово")
            uname = message.from_user.full_name

            # Логируем
            await log_action(
                f"╔══════════════════╗\n║  🔴  ТРИГГЕР      ║\n╚══════════════════╝\n\n"
                f"👤 {uname} (<code>{uid}</code>)\n"
                f"🔤 Слово: <b>{t['word']}</b>\n"
                f"⚖️ Действие: <b>{action}</b>\n"
                f"💬 Чат: {message.chat.title}"
            )
            # Кэшируем для доказательной базы
            _violation_msg_cache[cid][uid].append(message.message_id)

            try:
                if action == "delete":
                    await message.delete()
                elif action == "warn":
                    await message.delete()
                    clean_expired_warns(cid, uid)
                    add_warn_with_expiry(cid, uid)
                    count = warnings[cid][uid]
                    add_mod_history(cid, uid, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, "Автомодератор")
                    await log_violation_screenshot(cid, uid, uname, message.text, f"⚡ Варн {count}/{MAX_WARNINGS}", reason, "Автомодератор", message.chat.title or "")
                    sent = await message.answer(
                        f"╔══════════════════╗\n║  🔴  ТРИГГЕР      ║\n╚══════════════════╝\n\n"
                        f"👤 {message.from_user.mention_html()}\n"
                        f"⚡ Варн: <b>{count}/{MAX_WARNINGS}</b>\n"
                        f"📋 Причина: {reason}\n──────────────────",
                        parse_mode="HTML")
                    asyncio.create_task(_auto_delete_after(sent, 15))
                elif action == "mute":
                    await message.delete()
                    await bot.restrict_chat_member(
                        cid, uid,
                        permissions=ChatPermissions(can_send_messages=False),
                        until_date=datetime.now() + timedelta(minutes=30)
                    )
                    add_mod_history(cid, uid, "🔇 Мут 30 мин", reason, "Автомодератор")
                    await log_violation_screenshot(cid, uid, uname, message.text, "🔇 Мут 30 мин", reason, "Автомодератор", message.chat.title or "")
                    sent = await message.answer(
                        f"╔══════════════════╗\n║  🔴  ТРИГГЕР      ║\n╚══════════════════╝\n\n"
                        f"👤 {message.from_user.mention_html()}\n"
                        f"🔇 Мут: <b>30 минут</b>\n"
                        f"📋 Причина: {reason}\n──────────────────",
                        parse_mode="HTML")
                    asyncio.create_task(_auto_delete_after(sent, 15))
                elif action == "ban":
                    await message.delete()
                    await bot.ban_chat_member(cid, uid)
                    add_mod_history(cid, uid, "🔨 Бан", reason, "Автомодератор")
                    await log_violation_screenshot(cid, uid, uname, message.text, "🔨 Бан", reason, "Автомодератор", message.chat.title or "")
                    sent = await message.answer(
                        f"╔══════════════════╗\n║  🔴  ТРИГГЕР      ║\n╚══════════════════╝\n\n"
                        f"👤 {message.from_user.mention_html()}\n"
                        f"🔨 Забанен автоматически\n"
                        f"📋 Причина: {reason}\n──────────────────",
                        parse_mode="HTML")
                    asyncio.create_task(_auto_delete_after(sent, 15))
            except Exception as e:
                logging.warning(f"Trigger word action error: {e}")
            break


async def _auto_delete_after(msg, seconds: int):
    await asyncio.sleep(seconds)
    try: await msg.delete()
    except: pass


@dp.message(Command("trigger"))
async def cmd_trigger(message: Message, command: CommandObject):
    if not await require_admin(message): return
    if not command.args:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🔴  ТРИГГЕРЫ     ║\n╚══════════════════╝\n\n"
            "<b>Использование:</b>\n"
            "/trigger add слово [действие] [причина]\n"
            "/trigger del слово\n"
            "/trigger list\n\n"
            "<b>Действия:</b> delete / warn / mute / ban\n"
            "<i>По умолчанию: warn</i>", parse_mode="HTML"); return

    parts = command.args.split(None, 3)
    sub = parts[0].lower()
    cid = message.chat.id

    if sub == "list":
        triggers = _triggers_load(cid)
        if not triggers:
            await reply_auto_delete(message,
                "╔══════════════════╗\n║  🔴  ТРИГГЕРЫ     ║\n╚══════════════════╝\n\n"
                "<i>Список пуст. Добавь: /trigger add слово</i>", parse_mode="HTML"); return
        lines = ["╔══════════════════╗\n║  🔴  ТРИГГЕРЫ     ║\n╚══════════════════╝\n"]
        for t in triggers:
            lines.append(f"──────────────────\n🔤 <code>{t['word']}</code> → <b>{t['action']}</b>\n📋 {t['reason']}\n👮 {t['added_by']}")
        await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")

    elif sub == "add" and len(parts) >= 2:
        word = parts[1].lower()
        action = parts[2].lower() if len(parts) > 2 and parts[2] in ("delete","warn","mute","ban") else "warn"
        reason = parts[3] if len(parts) > 3 else "Использование запрещённого слова"
        _trigger_add(cid, word, action, reason, message.from_user.full_name)
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  ✅  ТРИГГЕР ДОБАВЛЕН ║\n╚══════════════════╝\n\n"
            f"🔤 Слово: <code>{word}</code>\n⚖️ Действие: <b>{action}</b>\n📋 {reason}", parse_mode="HTML")

    elif sub == "del" and len(parts) >= 2:
        word = parts[1].lower()
        _trigger_del(cid, word)
        await reply_auto_delete(message,
            f"╔══════════════════╗\n║  🗑  ТРИГГЕР УДАЛЁН ║\n╚══════════════════╝\n\n"
            f"🔤 <code>{word}</code> удалён из списка.", parse_mode="HTML")
    else:
        await reply_auto_delete(message, "⚠️ /trigger add/del/list")


# ══════════════════════════════════════════════════════════
#  🕊 АВТОПОМИЛОВАНИЕ
# ══════════════════════════════════════════════════════════

async def autopardons_loop():
    """Каждые 6 часов снимает устаревшие варны у хорошо ведущих себя юзеров"""
    while True:
        try:
            await asyncio.sleep(6 * 3600)
            await _run_autopardons()
        except Exception as e:
            logging.warning(f"autopardons_loop: {e}")


async def _run_autopardons():
    conn = db_connect()
    # Берём юзеров у которых есть варны
    rows = conn.execute("SELECT cid, uid, count FROM warnings WHERE count > 0").fetchall()
    conn.close()

    now = _time_module.time()
    pardoned = 0
    for row in rows:
        cid, uid, count = row["cid"], row["uid"], row["count"]
        # Проверяем — не было ли новых нарушений за последние 7 дней
        if uid not in warn_expiry.get(cid, {}): continue
        expiries = warn_expiry[cid][uid]
        if not expiries: continue
        last_warn_ts = max(expiries)
        days_since = (now - last_warn_ts) / 86400
        if days_since >= 7:
            # Снимаем 1 варн автоматически
            warnings[cid][uid] = max(0, count - 1)
            db_set_int("warnings", cid, uid, "count", warnings[cid][uid])
            add_mod_history(cid, uid, "🕊 Автопомилование", "Хорошее поведение 7+ дней", "Система")
            pardoned += 1
            try:
                await bot.send_message(
                    uid,
                    f"╔══════════════════╗\n║  🕊  ПОМИЛОВАНИЕ  ║\n╚══════════════════╝\n\n"
                    f"Одно предупреждение автоматически снято!\n"
                    f"⚡ Осталось варнов: <b>{warnings[cid][uid]}/{MAX_WARNINGS}</b>\n\n"
                    f"<i>Продолжай соблюдать правила 👍</i>",
                    parse_mode="HTML"
                )
            except: pass

    if pardoned > 0:
        await log_action(
            f"╔══════════════════╗\n║  🕊  АВТОПОМИЛОВАНИЕ ║\n╚══════════════════╝\n\n"
            f"Снято предупреждений: <b>{pardoned}</b>\n"
            f"<i>Юзеры без нарушений 7+ дней</i>"
        )


@dp.message(Command("autopardonstatus"))
async def cmd_autopardon_status(message: Message):
    if not await require_admin(message): return
    conn = db_connect()
    rows = conn.execute("SELECT cid, uid, count FROM warnings WHERE count > 0 AND cid=?", (message.chat.id,)).fetchall()
    conn.close()
    now = _time_module.time()
    candidates = []
    for row in rows:
        cid, uid, count = row["cid"], row["uid"], row["count"]
        if uid in warn_expiry.get(cid, {}) and warn_expiry[cid][uid]:
            last = max(warn_expiry[cid][uid])
            days = (now - last) / 86400
            if days >= 5:
                candidates.append((uid, count, round(days, 1)))
    if not candidates:
        await reply_auto_delete(message,
            "╔══════════════════╗\n║  🕊  АВТОПОМИЛОВАНИЕ ║\n╚══════════════════╝\n\n"
            "<i>Кандидатов на помилование нет.\nТребуется 7 дней без нарушений.</i>", parse_mode="HTML"); return
    lines = ["╔══════════════════╗\n║  🕊  КАНДИДАТЫ    ║\n╚══════════════════╝\n\n<b>Скоро будут помилованы:</b>\n"]
    for uid, count, days in candidates[:10]:
        lines.append(f"──────────────────\n🪪 <code>{uid}</code>\n⚡ Варнов: {count} | Дней чисто: {days}")
    await reply_auto_delete(message, "\n".join(lines), parse_mode="HTML")


# ══════════════════════════════════════════════════════════
#  🌐 АНТИVPN / АНТИПРОКСИ
# ══════════════════════════════════════════════════════════

# Настройки антиVPN по чатам: {cid: {"enabled": bool, "action": "warn"/"kick"/"ban"}}
_antivpn_settings: dict = {}

# Кэш результатов проверки: {uid: {"is_vpn": bool, "ts": float}}
_vpn_check_cache: dict = {}


def _antivpn_get(cid: int) -> dict:
    return _antivpn_settings.get(cid, {"enabled": False, "action": "warn"})


async def _check_vpn(uid: int) -> tuple[bool, str]:
    """Проверяет юзера через vpnapi.io (бесплатный тариф)"""
    # Проверяем кэш (24 часа)
    if uid in _vpn_check_cache:
        cached = _vpn_check_cache[uid]
        if _time_module.time() - cached["ts"] < 86400:
            return cached["is_vpn"], cached.get("reason", "")

    vpn_api_key = os.getenv("VPN_API_KEY", "")
    if not vpn_api_key:
        return False, ""

    # Получаем IP юзера через Telegram (недоступно напрямую)
    # Используем обходной путь — проверяем по user_id через публичный сервис
    try:
        async with aiohttp.ClientSession() as sess:
            # Используем vpnapi.io
            async with sess.get(
                f"https://vpnapi.io/api/?key={vpn_api_key}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    security = data.get("security", {})
                    is_vpn = (
                        security.get("vpn", False) or
                        security.get("proxy", False) or
                        security.get("tor", False) or
                        security.get("relay", False)
                    )
                    reason = []
                    if security.get("vpn"): reason.append("VPN")
                    if security.get("proxy"): reason.append("Прокси")
                    if security.get("tor"): reason.append("Tor")
                    if security.get("relay"): reason.append("Relay")
                    reason_str = ", ".join(reason)
                    _vpn_check_cache[uid] = {"is_vpn": is_vpn, "reason": reason_str, "ts": _time_module.time()}
                    return is_vpn, reason_str
    except: pass
    return False, ""


async def _process_new_member_vpn(message: Message, member):
    """Полная антиVPN проверка: профиль + WebApp верификация"""
    cid = message.chat.id
    cfg = _antivpn_get(cid)
    if not cfg.get("enabled"): return

    uid = member.id
    name = member.full_name
    action = cfg.get("action", "kick")

    # ── Шаг 1: Проверка профиля ──────────────────────────
    risk_score = 0
    risk_flags = []

    # Нет username
    if not member.username:
        risk_score += 2
        risk_flags.append("нет username")

    # Нет фото профиля
    try:
        photos = await bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count == 0:
            risk_score += 2
            risk_flags.append("нет фото")
    except: pass

    # Имя состоит только из цифр или очень короткое
    import re as _re
    if _re.match(r'^[\d\s]+$', name):
        risk_score += 3
        risk_flags.append("имя из цифр")
    if len(name) <= 2:
        risk_score += 2
        risk_flags.append("очень короткое имя")

    # Имя на кириллице но без фото и username — подозрительно меньше
    # Имя на латинице/иероглифах при остальных признаках
    if not _re.search(r'[а-яёА-ЯЁ]', name) and risk_score >= 2:
        risk_score += 1
        risk_flags.append("не кириллица")

    # Premium аккаунт — снижаем риск
    if getattr(member, 'is_premium', False):
        risk_score -= 3

    # ── Если высокий риск — сразу действуем ─────────────
    if risk_score >= 5:
        flags_str = ", ".join(risk_flags)
        await log_action(
            f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
            f"⚠️ <b>Подозрительный профиль</b>\n"
            f"👤 {name} (<code>{uid}</code>)\n"
            f"🔍 Признаки: {flags_str}\n"
            f"📊 Риск: {risk_score}/10\n"
            f"⚖️ Действие: <b>{action}</b>\n"
            f"💬 Чат: {message.chat.title}"
        )
        try:
            if action == "ban":
                await bot.ban_chat_member(cid, uid)
                sent = await message.answer(
                    f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
                    f"👤 {member.mention_html()}\n"
                    f"🔍 Подозрительный профиль: {flags_str}\n"
                    f"🔨 <b>Забанен автоматически</b>", parse_mode="HTML")
                asyncio.create_task(_auto_delete_after(sent, 20))
                return
            elif action in ("kick", "warn"):
                await bot.ban_chat_member(cid, uid)
                await bot.unban_chat_member(cid, uid)
                sent = await message.answer(
                    f"╔══════════════════╗\n║  🌐  АНТИVPN      ║\n╚══════════════════╝\n\n"
                    f"👤 {member.mention_html()}\n"
                    f"🔍 Подозрительный профиль: {flags_str}\n"
                    f"🚪 <b>Кикнут автоматически</b>", parse_mode="HTML")
                asyncio.create_task(_auto_delete_after(sent, 20))
                return
        except: pass

    # ── Шаг 2: WebApp верификация (всегда если включена) ─
    render_url = os.getenv("RENDER_URL", "https://mybot-1s9l.onrender.com")
    verify_url = f"{render_url}/verify"

    # Мутим на время верификации
    try:
        await bot.restrict_chat_member(
            cid, uid,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=datetime.now() + timedelta(minutes=10)
        )
    except: pass

    # Сохраняем в очередь ожидания
    _verify_pending[uid] = {
        "cid": cid, "name": name,
        "action": action, "ts": _time_module.time(),
        "risk_score": risk_score, "risk_flags": risk_flags
    }

    # Отправляем кнопку верификации
    try:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="🛡 Пройти верификацию",
                web_app=WebAppInfo(url=verify_url)
            )
        ]])
        await bot.send_message(
            uid,
            f"╔══════════════════╗\n║  🛡  ВЕРИФИКАЦИЯ  ║\n╚══════════════════╝\n\n"
            f"👋 Привет, <b>{name}</b>!\n\n"
            f"Для доступа в чат нужно пройти быструю проверку.\n"
            f"⏱ У тебя есть <b>10 минут</b>.\n\n"
            f"Нажми кнопку ниже 👇",
            parse_mode="HTML",
            reply_markup=kb
        )
    except:
        # Если не можем написать в ЛС — пишем в чат
        try:
            risk_text = f"\n⚠️ Признаки: {', '.join(risk_flags)}" if risk_flags else ""
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🛡 Пройти верификацию",
                    web_app=WebAppInfo(url=verify_url)
                )
            ]])
            sent = await message.answer(
                f"╔══════════════════╗\n║  🛡  ВЕРИФИКАЦИЯ  ║\n╚══════════════════╝\n\n"
                f"👤 {member.mention_html()}, пройди верификацию чтобы получить доступ к чату.{risk_text}\n"
                f"⏱ <b>10 минут</b>",
                parse_mode="HTML", reply_markup=kb
            )
            asyncio.create_task(_auto_delete_after(sent, 600))
        except: pass

    # Автокик если не прошёл верификацию за 10 минут
    asyncio.create_task(_verify_timeout_kick(uid, cid, name, action))


async def _verify_timeout_kick(uid: int, cid: int, name: str, action: str):
    """Кикает юзера если не прошёл верификацию за 10 минут"""
    await asyncio.sleep(600)
    if uid not in _verify_pending: return  # уже прошёл
    _verify_pending.pop(uid, None)
    try:
        await bot.ban_chat_member(cid, uid)
        await bot.unban_chat_member(cid, uid)
        await log_action(
            f"╔══════════════════╗\n║  🛡  ВЕРИФИКАЦИЯ  ║\n╚══════════════════╝\n\n"
            f"⏱ Таймаут верификации\n"
            f"👤 {name} (<code>{uid}</code>)\n"
            f"🚪 Кикнут за неактивность"
        )
    except: pass


@dp.callback_query(F.data.startswith("vpn_"))
async def cb_vpn_action(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("⛔", show_alert=True); return
    parts = call.data.split(":")
    action, cid, uid = parts[0], int(parts[1]), int(parts[2])
    if action == "vpn_ban":
        try:
            await bot.ban_chat_member(cid, uid)
            await call.answer("🔨 Забанен!", show_alert=True)
        except Exception as e:
            await call.answer(f"❌ {e}", show_alert=True)
    elif action == "vpn_kick":
        try:
            await bot.ban_chat_member(cid, uid)
            await bot.unban_chat_member(cid, uid)
            await call.answer("🚪 Кикнут!", show_alert=True)
        except Exception as e:
            await call.answer(f"❌ {e}", show_alert=True)
    else:
        await call.answer("✅ Проигнорировано")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except: pass


# ══════════════════════════════════════════════════════════

async def main():
    import time as _tstart
    global bot_start_time
    bot_start_time = _tstart.time()

    # ── Инициализация БД ─────────────────────────────────
    db_init()
    db_friends_init()
    db_violators_init()
    db_hotkeys_init()
    migrate_json_to_sqlite()
    load_data()
    await db.init_db()

    # ── Модули ───────────────────────────────────────────
    shared.init(bot, ADMIN_IDS, OWNER_ID, LOG_CHANNEL_ID)
    cs.init_tables()
    cs.set_bot(bot)
    dashboard.set_bot(bot, ADMIN_IDS)
    tkt.set_bot(bot)
    await features.init(bot, dp, ADMIN_IDS, OWNER_ID)
    await notif.init(bot, dp)

    # ── Инициализация систем ─────────────────────────────
    _triggers_db_init()

    # ── Фоновые задачи ───────────────────────────────────
    asyncio.create_task(cs.schedule_loop())
    asyncio.create_task(birthday_checker())
    asyncio.create_task(send_weekly_stats())
    asyncio.create_task(warn_expiry_checker())
    asyncio.create_task(run_lottery())
    asyncio.create_task(autosave_loop())
    asyncio.create_task(run_events())
    asyncio.create_task(run_newspaper())
    asyncio.create_task(run_stock())
    asyncio.create_task(smart_notify_loop())
    asyncio.create_task(auto_backup_loop())
    asyncio.create_task(daily_idea_loop())
    asyncio.create_task(autopardons_loop())
    asyncio.create_task(compliment_daily_loop())

    # ── Веб дашборд ──────────────────────────────────────
    await dashboard.start_dashboard()

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не задан в переменных окружения!")
    print("✅ CHAT GUARD запущен!")
    await log_action("🚀 <b>CHAT GUARD запущен!</b>")
    await dp.start_polling(bot)

# ══════════════════════════════════════════════════════════
#  👑 ПАНЕЛЬ НАСТРОЕК ВЛАДЕЛЬЦА — /ownersettings
#  Полное управление ботом через Telegram (инлайн-кнопки)
# ══════════════════════════════════════════════════════════

# ── Хранилище настроек владельца ──────────────────────────
def _owner_settings_get() -> dict:
    conn = db_connect()
    conn.execute("""CREATE TABLE IF NOT EXISTS owner_settings
        (key TEXT PRIMARY KEY, value TEXT)""")
    rows = conn.execute("SELECT key, value FROM owner_settings").fetchall()
    conn.close()
    defaults = {
        # XP / Уровни
        "xp_per_msg_min":     "1",
        "xp_per_msg_max":     "15",
        "xp_weekend_double":  "1",
        "xp_cooldown_secs":   "60",
        "level_up_announce":  "1",
        # Модерация
        "max_warns":          "3",
        "warn_expiry_days":   "30",
        "auto_ban_on_warns":  "1",
        "default_mute_mins":  "60",
        "antimat":            "0",
        "anti_nsfw":          "1",
        "auto_kick_bots":     "1",
        # Экономика
        "daily_xp_min":       "20",
        "daily_xp_max":       "100",
        "lottery_ticket_cost":"20",
        "lottery_prize_mult": "20",
        "rep_cooldown_hours": "1",
        "stock_min_mult":     "-50",
        "stock_max_mult":     "100",
        # Рейтинг
        "top_size":           "10",
        "rating_reset_days":  "0",
        "show_xp_in_top":     "1",
        "show_rep_in_top":    "1",
        # Анончат
        "anon_enabled":       "1",
        "anon_cooldown_secs": "30",
        "anon_reveal_admins": "1",
        # Прочее
        "welcome_enabled":    "1",
        "newspaper_enabled":  "1",
        "events_enabled":     "1",
        "compliment_enabled": "1",
        "bot_name":           "CHAT GUARD",
    }
    for row in rows:
        defaults[row["key"]] = row["value"]
    return defaults

def _owner_settings_set(key: str, value: str):
    conn = db_connect()
    conn.execute("""CREATE TABLE IF NOT EXISTS owner_settings
        (key TEXT PRIMARY KEY, value TEXT)""")
    conn.execute("INSERT OR REPLACE INTO owner_settings (key, value) VALUES (?,?)",
                 (key, str(value)))
    conn.commit()
    conn.close()

def _bool_icon(val) -> str:
    return "✅" if str(val) == "1" else "❌"

# ── Главное меню настроек ──────────────────────────────────
def kb_owner_settings_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ XP и Уровни",        callback_data="os:menu:xp"),
         InlineKeyboardButton(text="🏆 Рейтинг/Топы",       callback_data="os:menu:rating")],
        [InlineKeyboardButton(text="🛡 Модерация",           callback_data="os:menu:mod"),
         InlineKeyboardButton(text="💰 Экономика",           callback_data="os:menu:eco")],
        [InlineKeyboardButton(text="💌 Анонимки",            callback_data="os:menu:anon"),
         InlineKeyboardButton(text="🎮 Функции",             callback_data="os:menu:features")],
        [InlineKeyboardButton(text="📊 Текущие настройки",   callback_data="os:view:all"),
         InlineKeyboardButton(text="🔄 Сброс к дефолту",    callback_data="os:reset:confirm")],
        [InlineKeyboardButton(text="✖️ Закрыть",             callback_data="os:close")],
    ])

# ── Меню XP ────────────────────────────────────────────────
def kb_os_xp(s: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"📈 XP за сообщение: {s['xp_per_msg_min']}–{s['xp_per_msg_max']}",
            callback_data="os:edit:xp_range")],
        [InlineKeyboardButton(
            text=f"⏱ Кулдаун XP: {s['xp_cooldown_secs']}с",
            callback_data="os:edit:xp_cooldown_secs")],
        [InlineKeyboardButton(
            text=f"🗓 Двойной XP в выходные: {_bool_icon(s['xp_weekend_double'])}",
            callback_data="os:toggle:xp_weekend_double")],
        [InlineKeyboardButton(
            text=f"📢 Анонс повышения уровня: {_bool_icon(s['level_up_announce'])}",
            callback_data="os:toggle:level_up_announce")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")],
    ])

# ── Меню Рейтинг ───────────────────────────────────────────
def kb_os_rating(s: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"📊 Размер топа: {s['top_size']} мест",
            callback_data="os:edit:top_size")],
        [InlineKeyboardButton(
            text=f"⭐ XP в топе: {_bool_icon(s['show_xp_in_top'])}",
            callback_data="os:toggle:show_xp_in_top"),
         InlineKeyboardButton(
            text=f"🌟 Репа в топе: {_bool_icon(s['show_rep_in_top'])}",
            callback_data="os:toggle:show_rep_in_top")],
        [InlineKeyboardButton(
            text=f"🔄 Сброс рейтинга каждые {s['rating_reset_days']} дн (0=выкл)",
            callback_data="os:edit:rating_reset_days")],
        [InlineKeyboardButton(text="🗑 Сбросить XP всех",  callback_data="os:action:reset_xp"),
         InlineKeyboardButton(text="🗑 Сбросить репу всех", callback_data="os:action:reset_rep")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")],
    ])

# ── Меню Модерация ─────────────────────────────────────────
def kb_os_mod(s: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"⚡ Макс варнов: {s['max_warns']}",
            callback_data="os:edit:max_warns")],
        [InlineKeyboardButton(
            text=f"⏳ Варн сгорает через: {s['warn_expiry_days']} дн",
            callback_data="os:edit:warn_expiry_days")],
        [InlineKeyboardButton(
            text=f"🔇 Мут по умолчанию: {s['default_mute_mins']} мин",
            callback_data="os:edit:default_mute_mins")],
        [InlineKeyboardButton(
            text=f"🔨 Автобан при {s['max_warns']} варнах: {_bool_icon(s['auto_ban_on_warns'])}",
            callback_data="os:toggle:auto_ban_on_warns")],
        [InlineKeyboardButton(
            text=f"🧼 Антимат: {_bool_icon(s['antimat'])}",
            callback_data="os:toggle:antimat"),
         InlineKeyboardButton(
            text=f"🔞 Анти-NSFW: {_bool_icon(s['anti_nsfw'])}",
            callback_data="os:toggle:anti_nsfw")],
        [InlineKeyboardButton(
            text=f"🤖 Автокик ботов: {_bool_icon(s['auto_kick_bots'])}",
            callback_data="os:toggle:auto_kick_bots")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")],
    ])

# ── Меню Экономика ─────────────────────────────────────────
def kb_os_eco(s: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"🎁 Дейли бонус: {s['daily_xp_min']}–{s['daily_xp_max']} XP",
            callback_data="os:edit:daily_range")],
        [InlineKeyboardButton(
            text=f"🎰 Билет лотереи: {s['lottery_ticket_cost']} репы",
            callback_data="os:edit:lottery_ticket_cost")],
        [InlineKeyboardButton(
            text=f"💸 Множитель приза лотереи: x{s['lottery_prize_mult']}",
            callback_data="os:edit:lottery_prize_mult")],
        [InlineKeyboardButton(
            text=f"⏱ Кулдаун репы: {s['rep_cooldown_hours']} ч",
            callback_data="os:edit:rep_cooldown_hours")],
        [InlineKeyboardButton(
            text=f"📉 Биржа мин: {s['stock_min_mult']}% / макс: +{s['stock_max_mult']}%",
            callback_data="os:edit:stock_range")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")],
    ])

# ── Меню Анонимки ──────────────────────────────────────────
def kb_os_anon(s: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"💌 Анонимки включены: {_bool_icon(s['anon_enabled'])}",
            callback_data="os:toggle:anon_enabled")],
        [InlineKeyboardButton(
            text=f"⏱ Кулдаун анонимки: {s['anon_cooldown_secs']} сек",
            callback_data="os:edit:anon_cooldown_secs")],
        [InlineKeyboardButton(
            text=f"🕵️ Раскрытие автора для модов: {_bool_icon(s['anon_reveal_admins'])}",
            callback_data="os:toggle:anon_reveal_admins")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")],
    ])

# ── Меню Функции ───────────────────────────────────────────
def kb_os_features(s: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"👋 Welcome-экран: {_bool_icon(s['welcome_enabled'])}",
            callback_data="os:toggle:welcome_enabled")],
        [InlineKeyboardButton(
            text=f"🗞 Газета чата: {_bool_icon(s['newspaper_enabled'])}",
            callback_data="os:toggle:newspaper_enabled")],
        [InlineKeyboardButton(
            text=f"🎪 Ивенты (XP буст): {_bool_icon(s['events_enabled'])}",
            callback_data="os:toggle:events_enabled")],
        [InlineKeyboardButton(
            text=f"💌 Авто-комплименты: {_bool_icon(s['compliment_enabled'])}",
            callback_data="os:toggle:compliment_enabled")],
        [InlineKeyboardButton(
            text=f"🤖 Имя бота: {s['bot_name']}",
            callback_data="os:edit:bot_name")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")],
    ])

# ── Команда /ownersettings ──────────────────────────────────
@dp.message(Command("ownersettings", "settings"))
async def cmd_owner_settings(message: Message):
    if message.from_user.id != OWNER_ID:
        await reply_auto_delete(message, "🚫 Только для владельца!")
        return
    s = _owner_settings_get()
    await message.answer(
        f"👑 <b>Настройки владельца</b>\n"
        f"🤖 Бот: <b>{s['bot_name']}</b>\n"
        f"💬 Чатов: <b>{len(known_chats)}</b>\n"
        f"⭐ XP за сообщение: <b>{s['xp_per_msg_min']}–{s['xp_per_msg_max']}</b>\n"
        f"⚡ Макс варнов: <b>{s['max_warns']}</b>\n"
        f"🎰 Лотерея: <b>{s['lottery_ticket_cost']} репы/билет</b>\n\n"
        f"Выбери раздел для настройки:",
        parse_mode="HTML",
        reply_markup=kb_owner_settings_main()
    )

# ── Callback handler для всех настроек ─────────────────────
# Хранилище ожидания ввода
_os_pending: dict = {}  # {uid: {key, menu}}

@dp.callback_query(F.data.startswith("os:"))
async def cb_owner_settings(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("🚫 Только для владельца!", show_alert=True)
        return

    parts  = call.data.split(":", 2)
    action = parts[1]
    param  = parts[2] if len(parts) > 2 else ""
    s      = _owner_settings_get()

    # ── Навигация по меню ──────────────────────────────────
    if action == "close":
        await call.message.delete()
        await call.answer()
        return

    elif action == "menu":
        if param == "main":
            await call.message.edit_text(
                f"👑 <b>Настройки владельца</b>\n"
                f"🤖 Бот: <b>{s['bot_name']}</b>\n"
                f"💬 Чатов: <b>{len(known_chats)}</b>\n\n"
                f"Выбери раздел:",
                parse_mode="HTML",
                reply_markup=kb_owner_settings_main()
            )
        elif param == "xp":
            await call.message.edit_text(
                "⭐ <b>Настройки XP и Уровней</b>",
                parse_mode="HTML", reply_markup=kb_os_xp(s))
        elif param == "rating":
            await call.message.edit_text(
                "🏆 <b>Настройки Рейтинга</b>",
                parse_mode="HTML", reply_markup=kb_os_rating(s))
        elif param == "mod":
            await call.message.edit_text(
                "🛡 <b>Настройки Модерации</b>",
                parse_mode="HTML", reply_markup=kb_os_mod(s))
        elif param == "eco":
            await call.message.edit_text(
                "💰 <b>Настройки Экономики</b>",
                parse_mode="HTML", reply_markup=kb_os_eco(s))
        elif param == "anon":
            await call.message.edit_text(
                "💌 <b>Настройки Анонимок</b>",
                parse_mode="HTML", reply_markup=kb_os_anon(s))
        elif param == "features":
            await call.message.edit_text(
                "🎮 <b>Настройки Функций</b>",
                parse_mode="HTML", reply_markup=kb_os_features(s))

    # ── Переключатель вкл/выкл ────────────────────────────
    elif action == "toggle":
        cur = s.get(param, "0")
        new = "0" if cur == "1" else "1"
        _owner_settings_set(param, new)

        # Применяем сразу если нужно
        if param == "antimat":
            global ANTI_MAT_ENABLED
            ANTI_MAT_ENABLED = new == "1"
        elif param == "auto_kick_bots":
            global AUTO_KICK_BOTS
            AUTO_KICK_BOTS = new == "1"
        elif param == "max_warns":
            pass  # применяется через get

        await call.answer(f"{'✅ Включено' if new == '1' else '❌ Выключено'}")
        # Обновляем нужное меню
        s = _owner_settings_get()
        menus = {
            "xp_weekend_double":   ("xp",       "⭐ <b>Настройки XP и Уровней</b>"),
            "level_up_announce":   ("xp",       "⭐ <b>Настройки XP и Уровней</b>"),
            "auto_ban_on_warns":   ("mod",      "🛡 <b>Настройки Модерации</b>"),
            "antimat":             ("mod",      "🛡 <b>Настройки Модерации</b>"),
            "anti_nsfw":           ("mod",      "🛡 <b>Настройки Модерации</b>"),
            "auto_kick_bots":      ("mod",      "🛡 <b>Настройки Модерации</b>"),
            "anon_enabled":        ("anon",     "💌 <b>Настройки Анонимок</b>"),
            "anon_reveal_admins":  ("anon",     "💌 <b>Настройки Анонимок</b>"),
            "welcome_enabled":     ("features", "🎮 <b>Настройки Функций</b>"),
            "newspaper_enabled":   ("features", "🎮 <b>Настройки Функций</b>"),
            "events_enabled":      ("features", "🎮 <b>Настройки Функций</b>"),
            "compliment_enabled":  ("features", "🎮 <b>Настройки Функций</b>"),
            "show_xp_in_top":      ("rating",   "🏆 <b>Настройки Рейтинга</b>"),
            "show_rep_in_top":     ("rating",   "🏆 <b>Настройки Рейтинга</b>"),
        }
        menu_key, menu_title = menus.get(param, ("main", ""))
        kb_map = {
            "xp": kb_os_xp, "mod": kb_os_mod, "eco": kb_os_eco,
            "anon": kb_os_anon, "features": kb_os_features, "rating": kb_os_rating,
        }
        if menu_key in kb_map:
            try:
                await call.message.edit_text(
                    f"{menu_title}",
                    parse_mode="HTML",
                    reply_markup=kb_map[menu_key](s)
                )
            except: pass

    # ── Редактирование числовых значений ──────────────────
    elif action == "edit":
        edit_prompts = {
            "xp_range":           "📈 Введи диапазон XP за сообщение\nФормат: <code>МИН МАКс</code>\nПример: <code>3 12</code>",
            "xp_cooldown_secs":   "⏱ Введи кулдаун XP в секундах\nПример: <code>60</code>",
            "top_size":           "📊 Введи размер топа (1–50)\nПример: <code>10</code>",
            "rating_reset_days":  "🔄 Сброс рейтинга каждые N дней (0 = выкл)\nПример: <code>30</code>",
            "max_warns":          "⚡ Введи максимум варнов до бана\nПример: <code>3</code>",
            "warn_expiry_days":   "⏳ Варн сгорает через N дней (0 = никогда)\nПример: <code>30</code>",
            "default_mute_mins":  "🔇 Мут по умолчанию в минутах\nПример: <code>60</code>",
            "daily_range":        "🎁 Диапазон дейли XP\nФормат: <code>МИН МАКС</code>\nПример: <code>20 100</code>",
            "lottery_ticket_cost":"🎰 Цена билета лотереи в репе\nПример: <code>20</code>",
            "lottery_prize_mult": "💸 Множитель приза: репа × N за каждого участника\nПример: <code>20</code>",
            "rep_cooldown_hours": "⏱ Кулдаун репутации в часах\nПример: <code>1</code>",
            "stock_range":        "📉 Диапазон биржи в %\nФормат: <code>МИН МАКС</code>\nПример: <code>-50 100</code>",
            "anon_cooldown_secs": "⏱ Кулдаун анонимки в секундах\nПример: <code>30</code>",
            "bot_name":           "🤖 Введи новое имя бота\nПример: <code>CHAT GUARD PRO</code>",
        }
        prompt = edit_prompts.get(param, f"Введи новое значение для <code>{param}</code>:")
        _os_pending[call.from_user.id] = {"key": param, "msg_id": call.message.message_id}
        await call.message.edit_text(
            f"✏️ <b>Редактирование</b>\n\n{prompt}\n\n"
            f"Текущее: <b>{s.get(param, '—')}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="os:menu:main")
            ]])
        )
        await call.answer()
        return

    # ── Действия ───────────────────────────────────────────
    elif action == "action":
        if param == "reset_xp":
            await call.message.edit_text(
                "⚠️ <b>Сброс XP ВСЕХ участников</b>\n\nВсе XP и уровни будут обнулены!\nУверен?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="💥 ДА, СБРОСИТЬ", callback_data="os:confirm:reset_xp"),
                    InlineKeyboardButton(text="❌ Отмена",        callback_data="os:menu:rating"),
                ]])
            )
        elif param == "reset_rep":
            await call.message.edit_text(
                "⚠️ <b>Сброс репутации ВСЕХ участников</b>\n\nВся репутация будет обнулена!\nУверен?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="💥 ДА, СБРОСИТЬ", callback_data="os:confirm:reset_rep"),
                    InlineKeyboardButton(text="❌ Отмена",        callback_data="os:menu:rating"),
                ]])
            )

    # ── Подтверждения опасных действий ────────────────────
    elif action == "confirm":
        if param == "reset_xp":
            conn = db_connect()
            conn.execute("DELETE FROM xp_data")
            conn.execute("DELETE FROM levels")
            conn.commit()
            conn.close()
            xp_data.clear()
            levels.clear()
            await call.answer("✅ XP и уровни всех участников сброшены!", show_alert=True)
            s = _owner_settings_get()
            await call.message.edit_text(
                "🏆 <b>Настройки Рейтинга</b>",
                parse_mode="HTML", reply_markup=kb_os_rating(s))

        elif param == "reset_rep":
            conn = db_connect()
            conn.execute("DELETE FROM reputation")
            conn.commit()
            conn.close()
            reputation.clear()
            await call.answer("✅ Репутация всех участников сброшена!", show_alert=True)
            s = _owner_settings_get()
            await call.message.edit_text(
                "🏆 <b>Настройки Рейтинга</b>",
                parse_mode="HTML", reply_markup=kb_os_rating(s))

    # ── Просмотр всех настроек ────────────────────────────
    elif action == "view":
        lines = [
            "📋 <b>Все текущие настройки</b>\n",
            "⭐ <b>XP</b>",
            f"  Диапазон: {s['xp_per_msg_min']}–{s['xp_per_msg_max']}",
            f"  Кулдаун: {s['xp_cooldown_secs']}с",
            f"  Двойной в выходные: {_bool_icon(s['xp_weekend_double'])}",
            f"  Анонс уровня: {_bool_icon(s['level_up_announce'])}",
            "",
            "🛡 <b>Модерация</b>",
            f"  Макс варнов: {s['max_warns']}",
            f"  Варн сгорает: {s['warn_expiry_days']} дн",
            f"  Мут по умолчанию: {s['default_mute_mins']} мин",
            f"  Автобан: {_bool_icon(s['auto_ban_on_warns'])}",
            f"  Антимат: {_bool_icon(s['antimat'])}",
            f"  Анти-NSFW: {_bool_icon(s['anti_nsfw'])}",
            "",
            "💰 <b>Экономика</b>",
            f"  Дейли XP: {s['daily_xp_min']}–{s['daily_xp_max']}",
            f"  Лотерея: {s['lottery_ticket_cost']} репы/билет",
            f"  Биржа: {s['stock_min_mult']}% – +{s['stock_max_mult']}%",
            "",
            "💌 <b>Анонимки</b>",
            f"  Включены: {_bool_icon(s['anon_enabled'])}",
            f"  Кулдаун: {s['anon_cooldown_secs']}с",
        ]
        try:
            await call.message.edit_text(
                "\n".join(lines),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="◀️ Назад", callback_data="os:menu:main")
                ]])
            )
        except: pass

    # ── Сброс настроек ─────────────────────────────────────
    elif action == "reset":
        if param == "confirm":
            await call.message.edit_text(
                "⚠️ <b>Сброс ВСЕХ настроек к дефолту</b>\n\nУверен?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="💥 ДА", callback_data="os:reset:do"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="os:menu:main"),
                ]])
            )
        elif param == "do":
            conn = db_connect()
            conn.execute("DELETE FROM owner_settings")
            conn.commit()
            conn.close()
            await call.answer("✅ Настройки сброшены к дефолту!", show_alert=True)
            s = _owner_settings_get()
            await call.message.edit_text(
                "👑 <b>Настройки владельца</b>\n\nНастройки сброшены!\nВыбери раздел:",
                parse_mode="HTML", reply_markup=kb_owner_settings_main()
            )

    await call.answer()

# ── Обработчик ввода значений (текстовые сообщения) ────────
@dp.message(F.chat.type == "private")
async def handle_os_input(message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID or uid not in _os_pending:
        return

    state = _os_pending.pop(uid)
    key   = state["key"]
    text  = (message.text or "").strip()

    try:
        # Диапазоны (два числа через пробел)
        if key == "xp_range":
            parts = text.split()
            mn, mx = int(parts[0]), int(parts[1])
            _owner_settings_set("xp_per_msg_min", str(max(1, mn)))
            _owner_settings_set("xp_per_msg_max", str(max(mn, mx)))
        elif key == "daily_range":
            parts = text.split()
            mn, mx = int(parts[0]), int(parts[1])
            _owner_settings_set("daily_xp_min", str(max(1, mn)))
            _owner_settings_set("daily_xp_max", str(max(mn, mx)))
        elif key == "stock_range":
            parts = text.split()
            mn, mx = int(parts[0]), int(parts[1])
            _owner_settings_set("stock_min_mult", str(mn))
            _owner_settings_set("stock_max_mult", str(max(0, mx)))
        elif key == "bot_name":
            _owner_settings_set("bot_name", text[:50])
        else:
            val = int(text)
            if key == "max_warns":
                val = max(1, min(10, val))
            elif key == "top_size":
                val = max(1, min(50, val))
            _owner_settings_set(key, str(val))

        # Применяем сразу если нужно
        s = _owner_settings_get()
        if key == "max_warns":
            global MAX_WARNINGS
            MAX_WARNINGS = int(s["max_warns"])

        await message.answer(
            f"✅ <b>Сохранено!</b>\n\n<code>{key}</code> = <b>{text}</b>",
            parse_mode="HTML",
            reply_markup=kb_owner_settings_main()
        )
    except Exception as e:
        await message.answer(
            f"❌ Неверный формат. Попробуй снова.\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=kb_owner_settings_main()
        )

if __name__ == "__main__":
    asyncio.run(main())

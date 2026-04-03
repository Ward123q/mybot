"""
database.py — SQLite обёртка
Работает поверх существующей skinvault.db из bot.py
Все функции async для совместимости с tickets.py и dashboard.py
"""
import sqlite3
import json
import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

log = logging.getLogger(__name__)

DB_FILE = "skinvault.db"


def _conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


async def init_db():
    """Создаём дополнительные таблицы для тикетов"""
    conn = _conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS tickets (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER NOT NULL,
        user_name   TEXT,
        cid         INTEGER,
        chat_title  TEXT,
        subject     TEXT,
        status      TEXT DEFAULT 'open',
        priority    TEXT DEFAULT 'normal',
        assigned_mod_id INTEGER,
        assigned_mod    TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        updated_at  TEXT DEFAULT (datetime('now')),
        closed_at   TEXT
    );
    CREATE TABLE IF NOT EXISTS ticket_messages (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id   INTEGER REFERENCES tickets(id) ON DELETE CASCADE,
        sender_id   INTEGER,
        sender_name TEXT,
        is_mod      INTEGER DEFAULT 0,
        text        TEXT,
        sent_at     TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_tickets_uid    ON tickets(uid);
    CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
    CREATE INDEX IF NOT EXISTS idx_ticket_msgs    ON ticket_messages(ticket_id);
    """)
    conn.commit()
    conn.close()
    log.info("✅ SQLite готов (tickets таблицы созданы)")


async def close_db():
    pass


# ══════════════════════════════════════════
#  WARNINGS
# ══════════════════════════════════════════

async def get_warnings(cid: int, uid: int) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT count FROM warnings WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["count"] if row else 0


async def add_warning(cid: int, uid: int) -> int:
    conn = _conn()
    conn.execute(
        "INSERT INTO warnings (cid,uid,count) VALUES (?,?,1) "
        "ON CONFLICT(cid,uid) DO UPDATE SET count=count+1",
        (cid, uid)
    )
    conn.commit()
    row = conn.execute(
        "SELECT count FROM warnings WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["count"] if row else 1


async def remove_warning(cid: int, uid: int) -> int:
    conn = _conn()
    conn.execute(
        "UPDATE warnings SET count=MAX(count-1,0) WHERE cid=? AND uid=?", (cid, uid)
    )
    conn.commit()
    row = conn.execute(
        "SELECT count FROM warnings WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["count"] if row else 0


async def clear_warnings(cid: int, uid: int):
    conn = _conn()
    conn.execute("UPDATE warnings SET count=0 WHERE cid=? AND uid=?", (cid, uid))
    conn.commit()
    conn.close()


async def set_warnings(cid: int, uid: int, count: int):
    conn = _conn()
    conn.execute(
        "INSERT INTO warnings (cid,uid,count) VALUES (?,?,?) "
        "ON CONFLICT(cid,uid) DO UPDATE SET count=?",
        (cid, uid, count, count)
    )
    conn.commit()
    conn.close()


# ══════════════════════════════════════════
#  REPUTATION
# ══════════════════════════════════════════

async def get_rep(cid: int, uid: int) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT score FROM reputation WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["score"] if row else 0


async def change_rep(cid: int, uid: int, delta: int) -> int:
    conn = _conn()
    conn.execute(
        "INSERT INTO reputation (cid,uid,score) VALUES (?,?,?) "
        "ON CONFLICT(cid,uid) DO UPDATE SET score=score+?",
        (cid, uid, delta, delta)
    )
    conn.commit()
    row = conn.execute(
        "SELECT score FROM reputation WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["score"] if row else delta


async def top_rep(cid: int, limit: int = 10) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT uid,score FROM reputation WHERE cid=? ORDER BY score DESC LIMIT ?",
        (cid, limit)
    ).fetchall()
    conn.close()
    return rows


# ══════════════════════════════════════════
#  XP / LEVELS
# ══════════════════════════════════════════

async def get_xp(cid: int, uid: int) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT xp FROM xp_data WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["xp"] if row else 0


async def add_xp(cid: int, uid: int, amount: int) -> int:
    conn = _conn()
    conn.execute(
        "INSERT INTO xp_data (cid,uid,xp) VALUES (?,?,?) "
        "ON CONFLICT(cid,uid) DO UPDATE SET xp=xp+?",
        (cid, uid, amount, amount)
    )
    conn.commit()
    row = conn.execute(
        "SELECT xp FROM xp_data WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["xp"] if row else amount


async def get_level_db(cid: int, uid: int) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT level FROM levels WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["level"] if row else 0


async def set_level_db(cid: int, uid: int, level: int):
    conn = _conn()
    conn.execute(
        "INSERT INTO levels (cid,uid,level) VALUES (?,?,?) "
        "ON CONFLICT(cid,uid) DO UPDATE SET level=?",
        (cid, uid, level, level)
    )
    conn.commit()
    conn.close()


async def top_xp(cid: int, limit: int = 10) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT uid,xp FROM xp_data WHERE cid=? ORDER BY xp DESC LIMIT ?",
        (cid, limit)
    ).fetchall()
    conn.close()
    return rows


# ══════════════════════════════════════════
#  CHAT STATS
# ══════════════════════════════════════════

async def incr_chat_stats(cid: int, uid: int):
    conn = _conn()
    conn.execute(
        "INSERT INTO chat_stats (cid,uid,msg_count) VALUES (?,?,1) "
        "ON CONFLICT(cid,uid) DO UPDATE SET msg_count=msg_count+1",
        (cid, uid)
    )
    conn.commit()
    conn.close()


async def get_chat_stats(cid: int, uid: int) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT msg_count FROM chat_stats WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["msg_count"] if row else 0


async def top_active(cid: int, limit: int = 10) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT uid,msg_count FROM chat_stats WHERE cid=? ORDER BY msg_count DESC LIMIT ?",
        (cid, limit)
    ).fetchall()
    conn.close()
    return rows


async def chat_total_msgs(cid: int) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT COALESCE(SUM(msg_count),0) as total FROM chat_stats WHERE cid=?", (cid,)
    ).fetchone()
    conn.close()
    return row["total"] if row else 0


# ══════════════════════════════════════════
#  MOD HISTORY
# ══════════════════════════════════════════

async def add_mod_history(cid: int, uid: int, action: str, reason: str, by_name: str):
    conn = _conn()
    conn.execute(
        "INSERT INTO mod_history (cid,uid,action,reason,by_name) VALUES (?,?,?,?,?)",
        (cid, uid, action, reason, by_name)
    )
    conn.commit()
    conn.close()


async def get_mod_history(cid: int, uid: int, limit: int = 20) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT action,reason,by_name,created_at FROM mod_history "
        "WHERE cid=? AND uid=? ORDER BY created_at DESC LIMIT ?",
        (cid, uid, limit)
    ).fetchall()
    conn.close()
    return rows


async def get_mod_stats_db(cid: int) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT by_name,COUNT(*) as cnt FROM mod_history "
        "WHERE cid=? GROUP BY by_name ORDER BY cnt DESC LIMIT 10",
        (cid,)
    ).fetchall()
    conn.close()
    return rows


async def get_global_mod_stats() -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT by_name,COUNT(*) as cnt FROM mod_history "
        "GROUP BY by_name ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return rows


# ══════════════════════════════════════════
#  BAN LIST
# ══════════════════════════════════════════

async def get_bans(cid: int) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM ban_list WHERE cid=?", (cid,)
    ).fetchall()
    conn.close()
    return rows


async def is_banned(cid: int, uid: int) -> bool:
    conn = _conn()
    row = conn.execute(
        "SELECT uid FROM ban_list WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row is not None


# ══════════════════════════════════════════
#  KNOWN CHATS
# ══════════════════════════════════════════

async def upsert_chat(cid: int, title: str):
    conn = _conn()
    conn.execute(
        "INSERT INTO known_chats (cid,title) VALUES (?,?) "
        "ON CONFLICT(cid) DO UPDATE SET title=?",
        (cid, title, title)
    )
    conn.commit()
    conn.close()


async def get_all_chats() -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT cid,title FROM known_chats ORDER BY title"
    ).fetchall()
    conn.close()
    return rows


# ══════════════════════════════════════════
#  GLOBAL BLACKLIST
# ══════════════════════════════════════════

async def blacklist_add(uid: int):
    conn = _conn()
    conn.execute(
        "INSERT OR IGNORE INTO global_blacklist (uid) VALUES (?)", (uid,)
    )
    conn.commit()
    conn.close()


async def blacklist_remove(uid: int):
    conn = _conn()
    conn.execute("DELETE FROM global_blacklist WHERE uid=?", (uid,))
    conn.commit()
    conn.close()


async def blacklist_check(uid: int) -> bool:
    conn = _conn()
    row = conn.execute(
        "SELECT uid FROM global_blacklist WHERE uid=?", (uid,)
    ).fetchone()
    conn.close()
    return row is not None


async def blacklist_all() -> List[int]:
    conn = _conn()
    rows = conn.execute("SELECT uid FROM global_blacklist").fetchall()
    conn.close()
    return [r["uid"] for r in rows]


# ══════════════════════════════════════════
#  VIP
# ══════════════════════════════════════════

async def vip_check(uid: int, cid: int) -> bool:
    conn = _conn()
    row = conn.execute(
        "SELECT uid FROM vip_users WHERE uid=? AND cid=?", (uid, cid)
    ).fetchone()
    conn.close()
    return row is not None


# ══════════════════════════════════════════
#  WELCOME
# ══════════════════════════════════════════

async def welcome_get(cid: int) -> Dict:
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM welcome_settings WHERE cid=?", (cid,)
    ).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {
        "text": "━━━━━━━━━━━━━━━\n👋 <b>НОВЫЙ УЧАСТНИК</b>\n━━━━━━━━━━━━━━━\n\n"
                "👤 {name}\n📋 /rules — правила чата",
        "photo": "", "is_gif": False, "enabled": True
    }


# ══════════════════════════════════════════
#  SURVEILLANCE
# ══════════════════════════════════════════

async def surveillance_enabled(cid: int) -> bool:
    conn = _conn()
    row = conn.execute(
        "SELECT cid FROM surveillance_chats WHERE cid=?", (cid,)
    ).fetchone()
    conn.close()
    return row is not None


async def surveillance_log_get(cid: int, limit: int = 20) -> List:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT uid,name,text,deleted_at FROM deleted_log "
            "WHERE cid=? ORDER BY deleted_at DESC LIMIT ?",
            (cid, limit)
        ).fetchall()
    except:
        rows = []
    conn.close()
    return rows


# ══════════════════════════════════════════
#  MOD ROLES
# ══════════════════════════════════════════

async def mod_role_get(cid: int, uid: int) -> Optional[str]:
    conn = _conn()
    row = conn.execute(
        "SELECT role FROM mod_roles WHERE cid=? AND uid=?", (cid, uid)
    ).fetchone()
    conn.close()
    return row["role"] if row else None


async def mod_roles_all(cid: int) -> List:
    conn = _conn()
    rows = conn.execute(
        "SELECT uid,role FROM mod_roles WHERE cid=?", (cid,)
    ).fetchall()
    conn.close()
    return rows


# ══════════════════════════════════════════
#  ACTIVITY
# ══════════════════════════════════════════

async def get_hourly_totals(cid: int) -> Dict[int, int]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT hour,SUM(count) as total FROM hourly_stats "
            "WHERE cid=? GROUP BY hour ORDER BY hour",
            (cid,)
        ).fetchall()
        result = {r["hour"]: r["total"] for r in rows}
    except:
        result = {}
    conn.close()
    return result


# ══════════════════════════════════════════
#  PLUGINS
# ══════════════════════════════════════════

PLUGIN_DEFAULTS = {
    "economy": True, "games": True, "xp": True,
    "antispam": True, "antimat": True, "reports": True,
    "events": True, "newspaper": True, "clans": True,
}


async def plugins_all(cid: int) -> Dict[str, bool]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT key,enabled FROM plugins WHERE cid=?", (cid,)
        ).fetchall()
        result = dict(PLUGIN_DEFAULTS)
        for r in rows:
            result[r["key"]] = bool(r["enabled"])
    except:
        result = dict(PLUGIN_DEFAULTS)
    conn.close()
    return result


# ══════════════════════════════════════════
#  TICKETS
# ══════════════════════════════════════════

def _row_to_dict(row) -> Optional[Dict]:
    if row is None:
        return None
    return dict(row)


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except:
        return None


def _fix_ticket(d: dict) -> dict:
    """Конвертирует строки дат в datetime объекты"""
    if d is None:
        return d
    for f in ("created_at", "updated_at", "closed_at", "sent_at"):
        if f in d and isinstance(d[f], str):
            d[f] = _parse_dt(d[f])
    if "is_mod" in d:
        d["is_mod"] = bool(d["is_mod"])
    return d


async def ticket_create(uid: int, user_name: str, cid: int,
                        chat_title: str, subject: str) -> int:
    conn = _conn()
    cur = conn.execute(
        "INSERT INTO tickets (uid,user_name,cid,chat_title,subject) VALUES (?,?,?,?,?)",
        (uid, user_name, cid, chat_title, subject)
    )
    conn.commit()
    ticket_id = cur.lastrowid
    conn.close()
    return ticket_id


async def ticket_get(ticket_id: int) -> Optional[Dict]:
    conn = _conn()
    row = conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
    conn.close()
    return _fix_ticket(_row_to_dict(row))


async def ticket_get_open_by_user(uid: int) -> Optional[Dict]:
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM tickets WHERE uid=? AND status!='closed' "
        "ORDER BY created_at DESC LIMIT 1",
        (uid,)
    ).fetchone()
    conn.close()
    return _fix_ticket(_row_to_dict(row))


async def ticket_list(status: str = "open", cid: int = None, limit: int = 20) -> List[Dict]:
    conn = _conn()
    if cid:
        rows = conn.execute(
            "SELECT * FROM tickets WHERE status=? AND cid=? "
            "ORDER BY created_at DESC LIMIT ?",
            (status, cid, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tickets WHERE status=? ORDER BY created_at DESC LIMIT ?",
            (status, limit)
        ).fetchall()
    conn.close()
    return [_fix_ticket(dict(r)) for r in rows]


async def ticket_list_all(limit: int = 50) -> List[Dict]:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM tickets ORDER BY updated_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [_fix_ticket(dict(r)) for r in rows]


async def ticket_close(ticket_id: int):
    conn = _conn()
    conn.execute(
        "UPDATE tickets SET status='closed',closed_at=datetime('now'),"
        "updated_at=datetime('now') WHERE id=?",
        (ticket_id,)
    )
    conn.commit()
    conn.close()


async def ticket_assign(ticket_id: int, mod_id: int, mod_name: str):
    conn = _conn()
    conn.execute(
        "UPDATE tickets SET assigned_mod_id=?,assigned_mod=?,"
        "status='in_progress',updated_at=datetime('now') WHERE id=?",
        (mod_id, mod_name, ticket_id)
    )
    conn.commit()
    conn.close()


async def ticket_set_priority(ticket_id: int, priority: str):
    conn = _conn()
    conn.execute(
        "UPDATE tickets SET priority=?,updated_at=datetime('now') WHERE id=?",
        (priority, ticket_id)
    )
    conn.commit()
    conn.close()


async def ticket_msg_add(ticket_id: int, sender_id: int, sender_name: str,
                         is_mod: bool, text: str):
    conn = _conn()
    conn.execute(
        "INSERT INTO ticket_messages (ticket_id,sender_id,sender_name,is_mod,text) "
        "VALUES (?,?,?,?,?)",
        (ticket_id, sender_id, sender_name, 1 if is_mod else 0, text)
    )
    conn.execute(
        "UPDATE tickets SET updated_at=datetime('now') WHERE id=?", (ticket_id,)
    )
    conn.commit()
    conn.close()


async def ticket_msgs(ticket_id: int) -> List[Dict]:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM ticket_messages WHERE ticket_id=? ORDER BY sent_at ASC",
        (ticket_id,)
    ).fetchall()
    conn.close()
    return [_fix_ticket(dict(r)) for r in rows]


async def ticket_stats_all() -> Dict:
    conn = _conn()
    total    = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
    open_c   = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
    progress = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='in_progress'").fetchone()[0]
    closed   = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='closed'").fetchone()[0]
    conn.close()
    return {"total": total, "open": open_c, "in_progress": progress, "closed": closed}


# ══════════════════════════════════════════
#  DASHBOARD HELPERS
# ══════════════════════════════════════════

def get_conn():
    """Возвращает соединение для дашборда"""
    return _conn()

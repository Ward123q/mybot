"""
database.py — PostgreSQL через asyncpg
Env: DATABASE_URL=postgresql://user:pass@host:5432/dbname
"""
import asyncpg
import os
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

log = logging.getLogger(__name__)
_pool: Optional[asyncpg.Pool] = None


async def init_db():
    global _pool
    url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://")
    _pool = await asyncpg.create_pool(url, min_size=2, max_size=20)
    await _create_tables()
    log.info("✅ PostgreSQL подключён")


async def close_db():
    global _pool
    if _pool:
        await _pool.close()


def pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB not initialized. Call init_db() first.")
    return _pool


async def _create_tables():
    async with pool().acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            uid         BIGINT PRIMARY KEY,
            username    TEXT,
            full_name   TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS warnings (
            cid     BIGINT,
            uid     BIGINT,
            count   INT DEFAULT 0,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS reputation (
            cid     BIGINT,
            uid     BIGINT,
            score   INT DEFAULT 0,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS xp_data (
            cid     BIGINT,
            uid     BIGINT,
            xp      INT DEFAULT 0,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS levels (
            cid     BIGINT,
            uid     BIGINT,
            level   INT DEFAULT 0,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS streaks (
            cid         BIGINT,
            uid         BIGINT,
            streak      INT DEFAULT 0,
            last_date   TEXT DEFAULT '',
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS chat_stats (
            cid         BIGINT,
            uid         BIGINT,
            msg_count   INT DEFAULT 0,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS last_seen (
            cid     BIGINT,
            uid     BIGINT,
            ts      FLOAT,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS ban_list (
            cid         BIGINT,
            uid         BIGINT,
            name        TEXT,
            reason      TEXT,
            banned_by   TEXT,
            banned_at   TIMESTAMP DEFAULT NOW(),
            until_ts    FLOAT DEFAULT 0,
            is_temp     BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS known_chats (
            cid     BIGINT PRIMARY KEY,
            title   TEXT
        );
        CREATE TABLE IF NOT EXISTS birthdays (
            uid         BIGINT PRIMARY KEY,
            day         INT,
            month       INT,
            name        TEXT,
            chat_id     BIGINT
        );
        CREATE TABLE IF NOT EXISTS mod_history (
            id          SERIAL PRIMARY KEY,
            cid         BIGINT,
            uid         BIGINT,
            action      TEXT,
            reason      TEXT,
            by_name     TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS mod_roles (
            cid     BIGINT,
            uid     BIGINT,
            role    TEXT,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS global_blacklist (
            uid     BIGINT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS user_profiles (
            uid             BIGINT PRIMARY KEY,
            bio             TEXT DEFAULT '',
            mood            TEXT DEFAULT '',
            interests       TEXT DEFAULT '',
            anon_nick       TEXT DEFAULT '',
            anon_enabled    BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE IF NOT EXISTS friends (
            uid         BIGINT,
            friend_id   BIGINT,
            friend_name TEXT,
            added_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (uid, friend_id)
        );
        CREATE TABLE IF NOT EXISTS relationships (
            uid1        BIGINT,
            uid2        BIGINT,
            rel_type    TEXT,
            created_at  TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (uid1, uid2)
        );
        CREATE TABLE IF NOT EXISTS clans (
            clan_id     TEXT PRIMARY KEY,
            name        TEXT,
            tag         TEXT UNIQUE,
            leader_uid  BIGINT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS clan_members (
            uid         BIGINT PRIMARY KEY,
            clan_id     TEXT REFERENCES clans(clan_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS user_titles (
            uid         BIGINT PRIMARY KEY,
            active      TEXT DEFAULT '',
            purchased   JSONB DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS lottery_tickets (
            cid     BIGINT,
            uid     BIGINT,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS stock_invested (
            cid     BIGINT,
            uid     BIGINT,
            amount  INT DEFAULT 0,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS boosters (
            uid     BIGINT PRIMARY KEY,
            data    JSONB DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS artifacts (
            uid     BIGINT PRIMARY KEY,
            data    JSONB DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS avatars (
            uid     BIGINT PRIMARY KEY,
            emoji   TEXT DEFAULT '👤'
        );
        CREATE TABLE IF NOT EXISTS quotes (
            id          SERIAL PRIMARY KEY,
            cid         BIGINT,
            text        TEXT,
            author      TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS journal (
            id          SERIAL PRIMARY KEY,
            uid         BIGINT,
            text        TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS quick_replies (
            cid     BIGINT,
            key     TEXT,
            text    TEXT,
            PRIMARY KEY (cid, key)
        );
        CREATE TABLE IF NOT EXISTS pinned_messages (
            cid     BIGINT,
            msg_id  BIGINT,
            title   TEXT,
            ts      BIGINT,
            PRIMARY KEY (cid, msg_id)
        );
        CREATE TABLE IF NOT EXISTS vip_users (
            uid         BIGINT,
            cid         BIGINT,
            granted_by  TEXT,
            granted_at  TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (uid, cid)
        );
        CREATE TABLE IF NOT EXISTS welcome_settings (
            cid         BIGINT PRIMARY KEY,
            text        TEXT DEFAULT '👋 Добро пожаловать, {name}!',
            photo       TEXT DEFAULT '',
            is_gif      BOOLEAN DEFAULT FALSE,
            enabled     BOOLEAN DEFAULT TRUE
        );
        CREATE TABLE IF NOT EXISTS surveillance_chats (
            cid     BIGINT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS deleted_log (
            id          SERIAL PRIMARY KEY,
            cid         BIGINT,
            uid         BIGINT,
            name        TEXT,
            text        TEXT,
            deleted_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS plugins (
            cid     BIGINT,
            key     TEXT,
            enabled BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (cid, key)
        );
        CREATE TABLE IF NOT EXISTS mod_tasks (
            id          SERIAL PRIMARY KEY,
            cid         BIGINT,
            mod_id      BIGINT,
            mod_name    TEXT,
            task        TEXT,
            deadline    BIGINT,
            done        BOOLEAN DEFAULT FALSE,
            created_by  TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS mod_shifts (
            cid         BIGINT,
            uid         BIGINT,
            mod_name    TEXT,
            start_hour  INT,
            end_hour    INT,
            PRIMARY KEY (cid, uid)
        );
        CREATE TABLE IF NOT EXISTS qr_codes (
            code    TEXT PRIMARY KEY,
            uid     BIGINT,
            reward  INT,
            used    BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscriber  BIGINT,
            target      BIGINT,
            PRIMARY KEY (subscriber, target)
        );
        CREATE TABLE IF NOT EXISTS appeals (
            uid         BIGINT PRIMARY KEY,
            reason      TEXT,
            status      TEXT DEFAULT 'pending',
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS user_activity (
            cid     BIGINT,
            uid     BIGINT,
            day     TEXT,
            count   INT DEFAULT 0,
            PRIMARY KEY (cid, uid, day)
        );
        CREATE TABLE IF NOT EXISTS hourly_stats (
            cid     BIGINT,
            uid     BIGINT,
            hour    INT,
            count   INT DEFAULT 0,
            PRIMARY KEY (cid, uid, hour)
        );

        -- ══════════════════════════════════════════
        --  ТИКЕТЫ
        -- ══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS tickets (
            id              SERIAL PRIMARY KEY,
            uid             BIGINT NOT NULL,
            user_name       TEXT,
            cid             BIGINT,
            chat_title      TEXT,
            subject         TEXT,
            status          TEXT DEFAULT 'open',
            priority        TEXT DEFAULT 'normal',
            assigned_mod_id BIGINT,
            assigned_mod    TEXT,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW(),
            closed_at       TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS ticket_messages (
            id          SERIAL PRIMARY KEY,
            ticket_id   INT REFERENCES tickets(id) ON DELETE CASCADE,
            sender_id   BIGINT,
            sender_name TEXT,
            is_mod      BOOLEAN DEFAULT FALSE,
            text        TEXT,
            sent_at     TIMESTAMP DEFAULT NOW()
        );

        -- Индексы
        CREATE INDEX IF NOT EXISTS idx_warnings_cid    ON warnings(cid);
        CREATE INDEX IF NOT EXISTS idx_rep_cid         ON reputation(cid);
        CREATE INDEX IF NOT EXISTS idx_xp_cid          ON xp_data(cid);
        CREATE INDEX IF NOT EXISTS idx_stats_cid       ON chat_stats(cid);
        CREATE INDEX IF NOT EXISTS idx_modh_cid_uid    ON mod_history(cid, uid);
        CREATE INDEX IF NOT EXISTS idx_tickets_uid     ON tickets(uid);
        CREATE INDEX IF NOT EXISTS idx_tickets_status  ON tickets(status);
        CREATE INDEX IF NOT EXISTS idx_ticket_msgs     ON ticket_messages(ticket_id);
        """)
    log.info("✅ Таблицы созданы / проверены")


# ══════════════════════════════════════════
#  УНИВЕРСАЛЬНЫЕ ХЕЛПЕРЫ
# ══════════════════════════════════════════

async def db_get_int(table: str, cid: int, uid: int, col: str = "count") -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            f"SELECT {col} FROM {table} WHERE cid=$1 AND uid=$2", cid, uid
        )
        return row[col] if row else 0


async def db_set_int(table: str, cid: int, uid: int, col: str, val: int):
    async with pool().acquire() as c:
        await c.execute(
            f"INSERT INTO {table} (cid, uid, {col}) VALUES ($1,$2,$3) "
            f"ON CONFLICT (cid, uid) DO UPDATE SET {col}=EXCLUDED.{col}",
            cid, uid, val
        )


# ══════════════════════════════════════════
#  WARNINGS
# ══════════════════════════════════════════

async def get_warnings(cid: int, uid: int) -> int:
    return await db_get_int("warnings", cid, uid, "count")


async def add_warning(cid: int, uid: int) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO warnings (cid,uid,count) VALUES ($1,$2,1) "
            "ON CONFLICT (cid,uid) DO UPDATE SET count=warnings.count+1 RETURNING count",
            cid, uid
        )
        return row["count"]


async def remove_warning(cid: int, uid: int) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "UPDATE warnings SET count=GREATEST(count-1,0) "
            "WHERE cid=$1 AND uid=$2 RETURNING count",
            cid, uid
        )
        return row["count"] if row else 0


async def clear_warnings(cid: int, uid: int):
    async with pool().acquire() as c:
        await c.execute(
            "UPDATE warnings SET count=0 WHERE cid=$1 AND uid=$2", cid, uid
        )


async def set_warnings(cid: int, uid: int, count: int):
    await db_set_int("warnings", cid, uid, "count", count)


# ══════════════════════════════════════════
#  REPUTATION
# ══════════════════════════════════════════

async def get_rep(cid: int, uid: int) -> int:
    return await db_get_int("reputation", cid, uid, "score")


async def change_rep(cid: int, uid: int, delta: int) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO reputation (cid,uid,score) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,uid) DO UPDATE SET score=reputation.score+$3 RETURNING score",
            cid, uid, delta
        )
        return row["score"]


async def top_rep(cid: int, limit: int = 10) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT uid,score FROM reputation WHERE cid=$1 ORDER BY score DESC LIMIT $2",
            cid, limit
        )


# ══════════════════════════════════════════
#  XP / LEVELS
# ══════════════════════════════════════════

async def get_xp(cid: int, uid: int) -> int:
    return await db_get_int("xp_data", cid, uid, "xp")


async def add_xp(cid: int, uid: int, amount: int) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO xp_data (cid,uid,xp) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,uid) DO UPDATE SET xp=xp_data.xp+$3 RETURNING xp",
            cid, uid, amount
        )
        return row["xp"]


async def get_level_db(cid: int, uid: int) -> int:
    return await db_get_int("levels", cid, uid, "level")


async def set_level_db(cid: int, uid: int, level: int):
    await db_set_int("levels", cid, uid, "level", level)


async def top_xp(cid: int, limit: int = 10) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT uid,xp FROM xp_data WHERE cid=$1 ORDER BY xp DESC LIMIT $2",
            cid, limit
        )


# ══════════════════════════════════════════
#  CHAT STATS
# ══════════════════════════════════════════

async def incr_chat_stats(cid: int, uid: int):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO chat_stats (cid,uid,msg_count) VALUES ($1,$2,1) "
            "ON CONFLICT (cid,uid) DO UPDATE SET msg_count=chat_stats.msg_count+1",
            cid, uid
        )


async def get_chat_stats(cid: int, uid: int) -> int:
    return await db_get_int("chat_stats", cid, uid, "msg_count")


async def top_active(cid: int, limit: int = 10) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT uid,msg_count FROM chat_stats WHERE cid=$1 ORDER BY msg_count DESC LIMIT $2",
            cid, limit
        )


async def chat_total_msgs(cid: int) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT COALESCE(SUM(msg_count),0) as total FROM chat_stats WHERE cid=$1", cid
        )
        return row["total"]


# ══════════════════════════════════════════
#  STREAKS
# ══════════════════════════════════════════

async def get_streak(cid: int, uid: int) -> Dict:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT streak,last_date FROM streaks WHERE cid=$1 AND uid=$2", cid, uid
        )
        return {"streak": row["streak"], "last_date": row["last_date"]} if row else {"streak": 0, "last_date": ""}


async def update_streak(cid: int, uid: int, streak: int, last_date: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO streaks (cid,uid,streak,last_date) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (cid,uid) DO UPDATE SET streak=$3,last_date=$4",
            cid, uid, streak, last_date
        )


# ══════════════════════════════════════════
#  MOD HISTORY
# ══════════════════════════════════════════

async def add_mod_history(cid: int, uid: int, action: str, reason: str, by_name: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO mod_history (cid,uid,action,reason,by_name) VALUES ($1,$2,$3,$4,$5)",
            cid, uid, action, reason, by_name
        )


async def get_mod_history(cid: int, uid: int, limit: int = 20) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT action,reason,by_name,created_at FROM mod_history "
            "WHERE cid=$1 AND uid=$2 ORDER BY created_at DESC LIMIT $3",
            cid, uid, limit
        )


async def get_mod_stats_db(cid: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT by_name,COUNT(*) as cnt FROM mod_history "
            "WHERE cid=$1 GROUP BY by_name ORDER BY cnt DESC LIMIT 10",
            cid
        )


async def get_global_mod_stats() -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT by_name,COUNT(*) as cnt FROM mod_history "
            "GROUP BY by_name ORDER BY cnt DESC LIMIT 10"
        )


# ══════════════════════════════════════════
#  BAN LIST
# ══════════════════════════════════════════

async def add_ban(cid: int, uid: int, name: str, reason: str, by: str,
                  until_ts: float = 0, is_temp: bool = False):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO ban_list (cid,uid,name,reason,banned_by,until_ts,is_temp) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (cid,uid) DO UPDATE "
            "SET name=$3,reason=$4,banned_by=$5,until_ts=$6,is_temp=$7,banned_at=NOW()",
            cid, uid, name, reason, by, until_ts, is_temp
        )


async def remove_ban(cid: int, uid: int):
    async with pool().acquire() as c:
        await c.execute("DELETE FROM ban_list WHERE cid=$1 AND uid=$2", cid, uid)


async def get_bans(cid: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT * FROM ban_list WHERE cid=$1 ORDER BY banned_at DESC", cid
        )


async def is_banned(cid: int, uid: int) -> bool:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT uid FROM ban_list WHERE cid=$1 AND uid=$2", cid, uid
        )
        return row is not None


# ══════════════════════════════════════════
#  KNOWN CHATS
# ══════════════════════════════════════════

async def upsert_chat(cid: int, title: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO known_chats (cid,title) VALUES ($1,$2) "
            "ON CONFLICT (cid) DO UPDATE SET title=$2",
            cid, title
        )


async def get_all_chats() -> List:
    async with pool().acquire() as c:
        return await c.fetch("SELECT cid,title FROM known_chats ORDER BY title")


# ══════════════════════════════════════════
#  GLOBAL BLACKLIST
# ══════════════════════════════════════════

async def blacklist_add(uid: int):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO global_blacklist (uid) VALUES ($1) ON CONFLICT DO NOTHING", uid
        )


async def blacklist_remove(uid: int):
    async with pool().acquire() as c:
        await c.execute("DELETE FROM global_blacklist WHERE uid=$1", uid)


async def blacklist_check(uid: int) -> bool:
    async with pool().acquire() as c:
        row = await c.fetchrow("SELECT uid FROM global_blacklist WHERE uid=$1", uid)
        return row is not None


async def blacklist_all() -> List[int]:
    async with pool().acquire() as c:
        rows = await c.fetch("SELECT uid FROM global_blacklist")
        return [r["uid"] for r in rows]


# ══════════════════════════════════════════
#  PROFILES
# ══════════════════════════════════════════

async def get_profile(uid: int) -> Optional[Any]:
    async with pool().acquire() as c:
        return await c.fetchrow("SELECT * FROM user_profiles WHERE uid=$1", uid)


async def set_profile_field(uid: int, field: str, value: Any):
    async with pool().acquire() as c:
        await c.execute(
            f"INSERT INTO user_profiles (uid,{field}) VALUES ($1,$2) "
            f"ON CONFLICT (uid) DO UPDATE SET {field}=$2",
            uid, value
        )


# ══════════════════════════════════════════
#  VIP
# ══════════════════════════════════════════

async def vip_add(uid: int, cid: int, granted_by: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO vip_users (uid,cid,granted_by) VALUES ($1,$2,$3) "
            "ON CONFLICT DO NOTHING",
            uid, cid, granted_by
        )


async def vip_remove(uid: int, cid: int):
    async with pool().acquire() as c:
        await c.execute(
            "DELETE FROM vip_users WHERE uid=$1 AND cid=$2", uid, cid
        )


async def vip_check(uid: int, cid: int) -> bool:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT uid FROM vip_users WHERE uid=$1 AND cid=$2", uid, cid
        )
        return row is not None


# ══════════════════════════════════════════
#  WELCOME
# ══════════════════════════════════════════

async def welcome_get(cid: int) -> Dict:
    async with pool().acquire() as c:
        row = await c.fetchrow("SELECT * FROM welcome_settings WHERE cid=$1", cid)
        if row:
            return dict(row)
        return {
            "text": "━━━━━━━━━━━━━━━\n👋 <b>НОВЫЙ УЧАСТНИК</b>\n━━━━━━━━━━━━━━━\n\n"
                    "👤 {name}\n📋 /rules — правила чата",
            "photo": "", "is_gif": False, "enabled": True
        }


async def welcome_save(cid: int, data: Dict):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO welcome_settings (cid,text,photo,is_gif,enabled) "
            "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (cid) DO UPDATE "
            "SET text=$2,photo=$3,is_gif=$4,enabled=$5",
            cid, data.get("text",""), data.get("photo",""),
            data.get("is_gif", False), data.get("enabled", True)
        )


# ══════════════════════════════════════════
#  SURVEILLANCE
# ══════════════════════════════════════════

async def surveillance_enabled(cid: int) -> bool:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT cid FROM surveillance_chats WHERE cid=$1", cid
        )
        return row is not None


async def surveillance_toggle(cid: int) -> bool:
    if await surveillance_enabled(cid):
        async with pool().acquire() as c:
            await c.execute("DELETE FROM surveillance_chats WHERE cid=$1", cid)
        return False
    else:
        async with pool().acquire() as c:
            await c.execute(
                "INSERT INTO surveillance_chats (cid) VALUES ($1) ON CONFLICT DO NOTHING", cid
            )
        return True


async def surveillance_log_add(cid: int, uid: int, name: str, text: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO deleted_log (cid,uid,name,text) VALUES ($1,$2,$3,$4)",
            cid, uid, name, text[:500]
        )
        await c.execute(
            "DELETE FROM deleted_log WHERE cid=$1 AND id NOT IN "
            "(SELECT id FROM deleted_log WHERE cid=$1 ORDER BY deleted_at DESC LIMIT 100)",
            cid, cid
        )


async def surveillance_log_get(cid: int, limit: int = 20) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT uid,name,text,deleted_at FROM deleted_log "
            "WHERE cid=$1 ORDER BY deleted_at DESC LIMIT $2",
            cid, limit
        )


# ══════════════════════════════════════════
#  PLUGINS
# ══════════════════════════════════════════

PLUGIN_DEFAULTS = {
    "economy": True, "games": True, "xp": True,
    "antispam": True, "antimat": True, "reports": True,
    "events": True, "newspaper": True, "clans": True,
}


async def plugin_get(cid: int, key: str) -> bool:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT enabled FROM plugins WHERE cid=$1 AND key=$2", cid, key
        )
        return row["enabled"] if row else PLUGIN_DEFAULTS.get(key, True)


async def plugin_toggle(cid: int, key: str) -> bool:
    cur = await plugin_get(cid, key)
    new_val = not cur
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO plugins (cid,key,enabled) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,key) DO UPDATE SET enabled=$3",
            cid, key, new_val
        )
    return new_val


async def plugins_all(cid: int) -> Dict[str, bool]:
    async with pool().acquire() as c:
        rows = await c.fetch("SELECT key,enabled FROM plugins WHERE cid=$1", cid)
        result = dict(PLUGIN_DEFAULTS)
        for r in rows:
            result[r["key"]] = r["enabled"]
        return result


# ══════════════════════════════════════════
#  MOD ROLES
# ══════════════════════════════════════════

async def mod_role_set(cid: int, uid: int, role: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO mod_roles (cid,uid,role) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,uid) DO UPDATE SET role=$3",
            cid, uid, role
        )


async def mod_role_get(cid: int, uid: int) -> Optional[str]:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT role FROM mod_roles WHERE cid=$1 AND uid=$2", cid, uid
        )
        return row["role"] if row else None


async def mod_role_remove(cid: int, uid: int):
    async with pool().acquire() as c:
        await c.execute(
            "DELETE FROM mod_roles WHERE cid=$1 AND uid=$2", cid, uid
        )


async def mod_roles_all(cid: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT uid,role FROM mod_roles WHERE cid=$1", cid
        )


# ══════════════════════════════════════════
#  LAST SEEN + ACTIVITY
# ══════════════════════════════════════════

async def update_last_seen(cid: int, uid: int, ts: float):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO last_seen (cid,uid,ts) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,uid) DO UPDATE SET ts=$3",
            cid, uid, ts
        )


async def get_last_seen(cid: int, uid: int) -> float:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT ts FROM last_seen WHERE cid=$1 AND uid=$2", cid, uid
        )
        return row["ts"] if row else 0.0


async def incr_activity(cid: int, uid: int, day: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO user_activity (cid,uid,day,count) VALUES ($1,$2,$3,1) "
            "ON CONFLICT (cid,uid,day) DO UPDATE SET count=user_activity.count+1",
            cid, uid, day
        )


async def incr_hourly(cid: int, uid: int, hour: int):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO hourly_stats (cid,uid,hour,count) VALUES ($1,$2,$3,1) "
            "ON CONFLICT (cid,uid,hour) DO UPDATE SET count=hourly_stats.count+1",
            cid, uid, hour
        )


async def get_hourly_totals(cid: int) -> Dict[int, int]:
    async with pool().acquire() as c:
        rows = await c.fetch(
            "SELECT hour,SUM(count) as total FROM hourly_stats "
            "WHERE cid=$1 GROUP BY hour ORDER BY hour",
            cid
        )
        return {r["hour"]: r["total"] for r in rows}


# ══════════════════════════════════════════
#  TICKETS
# ══════════════════════════════════════════

async def ticket_create(uid: int, user_name: str, cid: int,
                        chat_title: str, subject: str) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO tickets (uid,user_name,cid,chat_title,subject) "
            "VALUES ($1,$2,$3,$4,$5) RETURNING id",
            uid, user_name, cid, chat_title, subject
        )
        return row["id"]


async def ticket_get(ticket_id: int) -> Optional[Any]:
    async with pool().acquire() as c:
        return await c.fetchrow("SELECT * FROM tickets WHERE id=$1", ticket_id)


async def ticket_get_open_by_user(uid: int) -> Optional[Any]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            "SELECT * FROM tickets WHERE uid=$1 AND status!='closed' "
            "ORDER BY created_at DESC LIMIT 1",
            uid
        )


async def ticket_list(status: str = "open", cid: int = None, limit: int = 20) -> List:
    async with pool().acquire() as c:
        if cid:
            return await c.fetch(
                "SELECT * FROM tickets WHERE status=$1 AND cid=$2 "
                "ORDER BY created_at DESC LIMIT $3",
                status, cid, limit
            )
        return await c.fetch(
            "SELECT * FROM tickets WHERE status=$1 ORDER BY created_at DESC LIMIT $2",
            status, limit
        )


async def ticket_list_all(limit: int = 50) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT * FROM tickets ORDER BY updated_at DESC LIMIT $1", limit
        )


async def ticket_close(ticket_id: int):
    async with pool().acquire() as c:
        await c.execute(
            "UPDATE tickets SET status='closed',closed_at=NOW(),updated_at=NOW() WHERE id=$1",
            ticket_id
        )


async def ticket_assign(ticket_id: int, mod_id: int, mod_name: str):
    async with pool().acquire() as c:
        await c.execute(
            "UPDATE tickets SET assigned_mod_id=$2,assigned_mod=$3,"
            "status='in_progress',updated_at=NOW() WHERE id=$1",
            ticket_id, mod_id, mod_name
        )


async def ticket_set_priority(ticket_id: int, priority: str):
    async with pool().acquire() as c:
        await c.execute(
            "UPDATE tickets SET priority=$2,updated_at=NOW() WHERE id=$1",
            ticket_id, priority
        )


async def ticket_msg_add(ticket_id: int, sender_id: int, sender_name: str,
                         is_mod: bool, text: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO ticket_messages (ticket_id,sender_id,sender_name,is_mod,text) "
            "VALUES ($1,$2,$3,$4,$5)",
            ticket_id, sender_id, sender_name, is_mod, text
        )
        await c.execute(
            "UPDATE tickets SET updated_at=NOW() WHERE id=$1", ticket_id
        )


async def ticket_msgs(ticket_id: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY sent_at ASC",
            ticket_id
        )


async def ticket_stats_all() -> Dict:
    async with pool().acquire() as c:
        total    = await c.fetchval("SELECT COUNT(*) FROM tickets")
        open_c   = await c.fetchval("SELECT COUNT(*) FROM tickets WHERE status='open'")
        progress = await c.fetchval("SELECT COUNT(*) FROM tickets WHERE status='in_progress'")
        closed   = await c.fetchval("SELECT COUNT(*) FROM tickets WHERE status='closed'")
        return {"total": total, "open": open_c, "in_progress": progress, "closed": closed}


# ══════════════════════════════════════════
#  QUICK REPLIES
# ══════════════════════════════════════════

async def quick_reply_set(cid: int, key: str, text: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO quick_replies (cid,key,text) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,key) DO UPDATE SET text=$3",
            cid, key, text
        )


async def quick_reply_get(cid: int, key: str) -> Optional[str]:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT text FROM quick_replies WHERE cid=$1 AND key=$2", cid, key
        )
        return row["text"] if row else None


async def quick_replies_all(cid: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT key,text FROM quick_replies WHERE cid=$1 ORDER BY key", cid
        )


# ══════════════════════════════════════════
#  LOTTERY
# ══════════════════════════════════════════

async def lottery_buy(cid: int, uid: int):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO lottery_tickets (cid,uid) VALUES ($1,$2) ON CONFLICT DO NOTHING",
            cid, uid
        )


async def lottery_has(cid: int, uid: int) -> bool:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT uid FROM lottery_tickets WHERE cid=$1 AND uid=$2", cid, uid
        )
        return row is not None


async def lottery_participants(cid: int) -> List[int]:
    async with pool().acquire() as c:
        rows = await c.fetch("SELECT uid FROM lottery_tickets WHERE cid=$1", cid)
        return [r["uid"] for r in rows]


async def lottery_clear(cid: int):
    async with pool().acquire() as c:
        await c.execute("DELETE FROM lottery_tickets WHERE cid=$1", cid)


# ══════════════════════════════════════════
#  STOCK
# ══════════════════════════════════════════

async def stock_invest(cid: int, uid: int, amount: int) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO stock_invested (cid,uid,amount) VALUES ($1,$2,$3) "
            "ON CONFLICT (cid,uid) DO UPDATE SET amount=stock_invested.amount+$3 RETURNING amount",
            cid, uid, amount
        )
        return row["amount"]


async def stock_get(cid: int, uid: int) -> int:
    return await db_get_int("stock_invested", cid, uid, "amount")


async def stock_clear_user(cid: int, uid: int):
    async with pool().acquire() as c:
        await c.execute(
            "UPDATE stock_invested SET amount=0 WHERE cid=$1 AND uid=$2", cid, uid
        )


async def stock_all(cid: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT uid,amount FROM stock_invested WHERE cid=$1 AND amount>0", cid
        )


# ══════════════════════════════════════════
#  BOOSTERS / ARTIFACTS
# ══════════════════════════════════════════

async def boosters_get(uid: int) -> Dict:
    async with pool().acquire() as c:
        row = await c.fetchrow("SELECT data FROM boosters WHERE uid=$1", uid)
        return json.loads(row["data"]) if row else {}


async def boosters_set(uid: int, data: Dict):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO boosters (uid,data) VALUES ($1,$2) "
            "ON CONFLICT (uid) DO UPDATE SET data=$2",
            uid, json.dumps(data)
        )


async def artifacts_get(uid: int) -> List:
    async with pool().acquire() as c:
        row = await c.fetchrow("SELECT data FROM artifacts WHERE uid=$1", uid)
        return json.loads(row["data"]) if row else []


async def artifacts_add(uid: int, artifact: Dict):
    arts = await artifacts_get(uid)
    arts.append(artifact)
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO artifacts (uid,data) VALUES ($1,$2) "
            "ON CONFLICT (uid) DO UPDATE SET data=$2",
            uid, json.dumps(arts)
        )


# ══════════════════════════════════════════
#  USER TITLES
# ══════════════════════════════════════════

async def titles_get(uid: int) -> Dict:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "SELECT active,purchased FROM user_titles WHERE uid=$1", uid
        )
        if row:
            return {"title": row["active"], "purchased": json.loads(row["purchased"])}
        return {"title": "", "purchased": []}


async def title_buy(uid: int, title_name: str):
    data = await titles_get(uid)
    if title_name not in data["purchased"]:
        data["purchased"].append(title_name)
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO user_titles (uid,active,purchased) VALUES ($1,$2,$3) "
            "ON CONFLICT (uid) DO UPDATE SET active=$2,purchased=$3",
            uid, title_name, json.dumps(data["purchased"])
        )


# ══════════════════════════════════════════
#  BIRTHDAYS
# ══════════════════════════════════════════

async def birthday_set(uid: int, day: int, month: int, name: str, chat_id: int):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO birthdays (uid,day,month,name,chat_id) VALUES ($1,$2,$3,$4,$5) "
            "ON CONFLICT (uid) DO UPDATE SET day=$2,month=$3,name=$4,chat_id=$5",
            uid, day, month, name, chat_id
        )


async def birthdays_today(day: int, month: int) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT * FROM birthdays WHERE day=$1 AND month=$2", day, month
        )


# ══════════════════════════════════════════
#  AVATARS
# ══════════════════════════════════════════

async def avatar_get(uid: int) -> str:
    async with pool().acquire() as c:
        row = await c.fetchrow("SELECT emoji FROM avatars WHERE uid=$1", uid)
        return row["emoji"] if row else "👤"


async def avatar_set(uid: int, emoji: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO avatars (uid,emoji) VALUES ($1,$2) "
            "ON CONFLICT (uid) DO UPDATE SET emoji=$2",
            uid, emoji
        )


# ══════════════════════════════════════════
#  QUOTES
# ══════════════════════════════════════════

async def quote_add(cid: int, text: str, author: str) -> int:
    async with pool().acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO quotes (cid,text,author) VALUES ($1,$2,$3) RETURNING id",
            cid, text, author
        )
        return row["id"]


async def quote_random(cid: int) -> Optional[Any]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            "SELECT * FROM quotes WHERE cid=$1 ORDER BY RANDOM() LIMIT 1", cid
        )


async def quotes_count(cid: int) -> int:
    async with pool().acquire() as c:
        return await c.fetchval(
            "SELECT COUNT(*) FROM quotes WHERE cid=$1", cid
        )


# ══════════════════════════════════════════
#  JOURNAL
# ══════════════════════════════════════════

async def journal_add(uid: int, text: str):
    async with pool().acquire() as c:
        await c.execute(
            "INSERT INTO journal (uid,text) VALUES ($1,$2)", uid, text
        )


async def journal_get(uid: int, limit: int = 10) -> List:
    async with pool().acquire() as c:
        return await c.fetch(
            "SELECT text,created_at FROM journal WHERE uid=$1 "
            "ORDER BY created_at DESC LIMIT $2",
            uid, limit
        )


# ══════════════════════════════════════════
#  MIGRATION from SQLite
# ══════════════════════════════════════════

async def migrate_from_sqlite(sqlite_path: str = "skinvault.db"):
    """Мигрирует данные из SQLite в PostgreSQL (запускать один раз)"""
    import sqlite3
    if not os.path.exists(sqlite_path):
        log.warning(f"SQLite файл не найден: {sqlite_path}")
        return

    log.info("🔄 Начинаю миграцию SQLite → PostgreSQL...")
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    migrated = 0

    tables = {
        "warnings":  ("INSERT INTO warnings(cid,uid,count) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                      lambda r: (r["cid"], r["uid"], r["count"])),
        "reputation": ("INSERT INTO reputation(cid,uid,score) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                       lambda r: (r["cid"], r["uid"], r["score"])),
        "xp_data":   ("INSERT INTO xp_data(cid,uid,xp) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                      lambda r: (r["cid"], r["uid"], r["xp"])),
        "chat_stats": ("INSERT INTO chat_stats(cid,uid,msg_count) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                       lambda r: (r["cid"], r["uid"], r["msg_count"])),
        "known_chats": ("INSERT INTO known_chats(cid,title) VALUES($1,$2) ON CONFLICT DO NOTHING",
                        lambda r: (r["cid"], r["title"])),
        "global_blacklist": ("INSERT INTO global_blacklist(uid) VALUES($1) ON CONFLICT DO NOTHING",
                             lambda r: (r["uid"],)),
    }

    async with pool().acquire() as c:
        for table, (query, mapper) in tables.items():
            try:
                rows = conn.execute(f"SELECT * FROM {table}").fetchall()
                for r in rows:
                    try:
                        await c.execute(query, *mapper(r))
                        migrated += 1
                    except: pass
                log.info(f"  ✅ {table}: {len(rows)} записей")
            except Exception as e:
                log.warning(f"  ⚠️ {table}: {e}")

    conn.close()
    log.info(f"✅ Миграция завершена: {migrated} записей перенесено")
    # Переименовываем чтобы не мигрировать повторно
    os.rename(sqlite_path, sqlite_path + ".migrated")

# -*- coding: utf-8 -*-
"""
threat_intel.py — Продвинутый модуль безопасности и разведки угроз
═══════════════════════════════════════════════════════════════════

Компоненты:
  1. 🧠 BehaviorEngine   — поведенческий профиль каждого пользователя
                           (энтропия, ритм, стиль, аномалии через z-score)
  2. 🕸  ThreatGraph      — граф связей нарушителей (Jaccard, временная
                           близость, общий инвайт, BFS-кластеризация)
  3. 🔬 MultiAccDetector — детектор мультиаккаунтов без IP/телефона
                           (временной вектор 24×7, стилометрия, Левенштейн)
  4. ⚖️  AppealSystem     — система апелляций с коллегией модераторов
                           и авто-исполнением решения по кворуму
  5. 📊 ThreatDashboard  — API-эндпоинты для дашборда

Подключение в bot.py:
    import threat_intel as ti
    # В main():
    await ti.init(bot, dp)
    # В on_new_member:
    await ti.on_user_join(message, member)
    # В StatsMiddleware (каждое сообщение):
    await ti.on_message(message)
    # В ban-команде:
    await ti.on_user_banned(uid, cid, reason, banned_by_uid)

Команды бота:
    /threatmap @user   — граф связей пользователя
    /alts @user        — возможные мультиаккаунты
    /trustscore @user  — уровень доверия
    /appeal [причина]  — подать апелляцию на бан
    /tistat            — статистика модуля
"""

import asyncio
import hashlib
import json
import logging
import math
import re
import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery, ChatPermissions, InlineKeyboardButton,
    InlineKeyboardMarkup, Message,
)

log = logging.getLogger(__name__)

_bot: Optional[Bot] = None
_admin_ids: Set[int] = set()
_log_channel: int = 0

# ══════════════════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════

DB_FILE = "skinvault.db"


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_tables():
    conn = _db()
    conn.executescript("""
    -- ── Поведенческие профили ──────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_profiles (
        uid             INTEGER NOT NULL,
        cid             INTEGER NOT NULL,
        msg_count       INTEGER DEFAULT 0,
        avg_len         REAL    DEFAULT 0,
        avg_interval    REAL    DEFAULT 0,
        interval_stddev REAL    DEFAULT 0,
        avg_entropy     REAL    DEFAULT 0,
        caps_ratio      REAL    DEFAULT 0,
        emoji_ratio     REAL    DEFAULT 0,
        punct_ratio     REAL    DEFAULT 0,
        url_ratio       REAL    DEFAULT 0,
        hour_vector     TEXT    DEFAULT '{}',
        day_vector      TEXT    DEFAULT '{}',
        vocab_hash      TEXT    DEFAULT '',
        last_seen       INTEGER DEFAULT 0,
        updated_at      INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (uid, cid)
    );

    -- ── Кольцевой буфер последних событий (интервалы) ─────────────────
    CREATE TABLE IF NOT EXISTS ti_intervals (
        uid       INTEGER NOT NULL,
        cid       INTEGER NOT NULL,
        ts        INTEGER NOT NULL,
        msg_len   INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_ti_intervals_uid_cid
        ON ti_intervals(uid, cid);

    -- ── Аномалии ──────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_anomalies (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        kind        TEXT    NOT NULL,
        score       REAL    DEFAULT 0,
        details     TEXT    DEFAULT '',
        resolved    INTEGER DEFAULT 0,
        ts          INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_ti_anomalies_uid
        ON ti_anomalies(uid, resolved);

    -- ── TrustScore ────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_trust (
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        score       INTEGER DEFAULT 50,
        penalties   TEXT    DEFAULT '[]',
        bonuses     TEXT    DEFAULT '[]',
        updated_at  INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (uid, cid)
    );

    -- ── Граф угроз — вершины ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_graph_nodes (
        uid         INTEGER PRIMARY KEY,
        name        TEXT    DEFAULT '',
        username    TEXT    DEFAULT '',
        banned      INTEGER DEFAULT 0,
        ban_cid     INTEGER DEFAULT 0,
        ban_reason  TEXT    DEFAULT '',
        ban_ts      INTEGER DEFAULT 0,
        cluster_id  INTEGER DEFAULT 0
    );

    -- ── Граф угроз — рёбра ───────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_graph_edges (
        uid_a       INTEGER NOT NULL,
        uid_b       INTEGER NOT NULL,
        weight      REAL    DEFAULT 0,
        reasons     TEXT    DEFAULT '[]',
        last_update INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (uid_a, uid_b),
        CHECK (uid_a < uid_b)
    );
    CREATE INDEX IF NOT EXISTS idx_ti_edges_a ON ti_graph_edges(uid_a);
    CREATE INDEX IF NOT EXISTS idx_ti_edges_b ON ti_graph_edges(uid_b);

    -- ── Детектор мультиаккаунтов ─────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_alt_suspects (
        uid_orig    INTEGER NOT NULL,
        uid_alt     INTEGER NOT NULL,
        confidence  REAL    DEFAULT 0,
        signals     TEXT    DEFAULT '{}',
        confirmed   INTEGER DEFAULT 0,
        reviewed_by INTEGER DEFAULT 0,
        ts          INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (uid_orig, uid_alt),
        CHECK (uid_orig < uid_alt)
    );

    -- ── Система апелляций ────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_appeals (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        reason      TEXT    NOT NULL,
        evidence    TEXT    DEFAULT '',
        status      TEXT    DEFAULT 'pending',
        result      TEXT    DEFAULT '',
        assigned_to TEXT    DEFAULT '[]',
        votes_for   INTEGER DEFAULT 0,
        votes_against INTEGER DEFAULT 0,
        quorum      INTEGER DEFAULT 2,
        created_at  INTEGER DEFAULT (strftime('%s','now')),
        resolved_at INTEGER DEFAULT 0
    );

    -- ── Голоса по апелляциям ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_appeal_votes (
        appeal_id   INTEGER NOT NULL,
        voter_uid   INTEGER NOT NULL,
        vote        INTEGER NOT NULL,
        comment     TEXT    DEFAULT '',
        ts          INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (appeal_id, voter_uid)
    );

    -- ── Общая статистика модуля ───────────────────────────────────────
    CREATE TABLE IF NOT EXISTS ti_stats (
        key         TEXT PRIMARY KEY,
        value       INTEGER DEFAULT 0,
        updated_at  INTEGER DEFAULT (strftime('%s','now'))
    );

    -- ── Инвайт-трекинг (кто по какой ссылке зашёл) ───────────────────
    CREATE TABLE IF NOT EXISTS ti_invite_track (
        uid         INTEGER NOT NULL,
        cid         INTEGER NOT NULL,
        invite_link TEXT    DEFAULT '',
        join_ts     INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (uid, cid)
    );
    """)
    conn.commit()
    conn.close()
    log.info("✅ threat_intel: таблицы инициализированы")


def _stat_inc(key: str, delta: int = 1):
    try:
        conn = _db()
        conn.execute(
            "INSERT INTO ti_stats(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=value+?, updated_at=strftime('%s','now')",
            (key, delta, delta)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"stat_inc error: {e}")


# ══════════════════════════════════════════════════════════════════════════
#  1. 🧠 BEHAVIOR ENGINE
# ══════════════════════════════════════════════════════════════════════════

# Размер кольцевого буфера интервалов
_INTERVAL_WINDOW = 100
# Порог z-score для аномалии
_ZSCORE_THRESHOLD = 3.0
# Минимум сообщений для построения профиля
_MIN_PROFILE_MSGS = 15
# Кэш профилей в памяти {(uid,cid): profile_dict}
_profile_cache: Dict[Tuple[int, int], dict] = {}
# Время последнего сообщения {(uid,cid): ts}
_last_msg_ts: Dict[Tuple[int, int], float] = {}


def _text_entropy(text: str) -> float:
    """Энтропия Шеннона текста (бит/символ)"""
    if not text:
        return 0.0
    freq: Dict[str, int] = {}
    for ch in text:
        freq[ch] = freq.get(ch, 0) + 1
    n = len(text)
    return -sum((c / n) * math.log2(c / n) for c in freq.values())


def _vocab_fingerprint(text: str) -> str:
    """Хэш словарного набора текста (для стилометрии)"""
    words = set(re.findall(r'\b[а-яёa-z]{3,}\b', text.lower()))
    return hashlib.md5("|".join(sorted(words)).encode()).hexdigest()[:16] if words else ""


def _extract_features(text: str) -> dict:
    """Извлекает вектор признаков из одного сообщения"""
    if not text:
        return {"len": 0, "entropy": 0.0, "caps": 0.0,
                "emoji": 0.0, "punct": 0.0, "urls": 0.0}
    n = len(text)
    alpha = sum(1 for c in text if c.isalpha())
    caps = sum(1 for c in text if c.isupper()) / max(alpha, 1)
    emoji_count = len(re.findall(
        r'[\U0001F300-\U0001FFFF\U00002600-\U000027FF]', text))
    punct = sum(1 for c in text if c in '.,!?;:') / n
    urls = len(re.findall(r'https?://', text)) / n * 10
    return {
        "len": n,
        "entropy": _text_entropy(text),
        "caps": caps,
        "emoji": emoji_count / n,
        "punct": punct,
        "urls": urls,
    }


def _update_profile(uid: int, cid: int, features: dict, now_ts: float):
    """Обновляет профиль пользователя инкрементально (EMA)"""
    key = (uid, cid)
    alpha = 0.1  # коэффициент сглаживания

    conn = _db()
    row = conn.execute(
        "SELECT * FROM ti_profiles WHERE uid=? AND cid=?", (uid, cid)
    ).fetchone()

    if row:
        p = dict(row)
        n = p["msg_count"] + 1
        # EMA обновление
        new_avg_len = p["avg_len"] + alpha * (features["len"] - p["avg_len"])
        new_entropy = p["avg_entropy"] + alpha * (features["entropy"] - p["avg_entropy"])
        new_caps = p["caps_ratio"] + alpha * (features["caps"] - p["caps_ratio"])
        new_emoji = p["emoji_ratio"] + alpha * (features["emoji"] - p["emoji_ratio"])
        new_punct = p["punct_ratio"] + alpha * (features["punct"] - p["punct_ratio"])
        new_url = p["url_ratio"] + alpha * (features["urls"] - p["url_ratio"])

        # Интервал
        prev_ts = _last_msg_ts.get(key, 0)
        if prev_ts > 0:
            interval = now_ts - prev_ts
            old_avg = p["avg_interval"]
            old_std = p["interval_stddev"]
            new_avg_int = old_avg + alpha * (interval - old_avg)
            # Welford онлайн-вариация
            diff2 = (interval - old_avg) * (interval - new_avg_int)
            new_std = math.sqrt(max(0, old_std ** 2 + alpha * (diff2 - old_std ** 2)))
        else:
            new_avg_int = p["avg_interval"]
            new_std = p["interval_stddev"]

        # Временные векторы
        try:
            hv = json.loads(p["hour_vector"] or "{}")
            dv = json.loads(p["day_vector"] or "{}")
        except Exception:
            hv, dv = {}, {}

        dt = datetime.fromtimestamp(now_ts)
        h_key = str(dt.hour)
        d_key = str(dt.weekday())
        hv[h_key] = hv.get(h_key, 0) + 1
        dv[d_key] = dv.get(d_key, 0) + 1

        conn.execute("""
            UPDATE ti_profiles SET
                msg_count=?, avg_len=?, avg_interval=?, interval_stddev=?,
                avg_entropy=?, caps_ratio=?, emoji_ratio=?, punct_ratio=?,
                url_ratio=?, hour_vector=?, day_vector=?, last_seen=?,
                updated_at=strftime('%s','now')
            WHERE uid=? AND cid=?
        """, (
            n, new_avg_len, new_avg_int, new_std,
            new_entropy, new_caps, new_emoji, new_punct,
            new_url, json.dumps(hv), json.dumps(dv),
            int(now_ts), uid, cid
        ))
    else:
        dt = datetime.fromtimestamp(now_ts)
        hv = {str(dt.hour): 1}
        dv = {str(dt.weekday()): 1}
        conn.execute("""
            INSERT INTO ti_profiles
                (uid, cid, msg_count, avg_len, avg_interval, interval_stddev,
                 avg_entropy, caps_ratio, emoji_ratio, punct_ratio,
                 url_ratio, hour_vector, day_vector, last_seen)
            VALUES (?,?,1,?,0,0,?,?,?,?,?,?,?,?)
        """, (
            uid, cid,
            features["len"], features["entropy"],
            features["caps"], features["emoji"],
            features["punct"], features["urls"],
            json.dumps(hv), json.dumps(dv), int(now_ts)
        ))

    conn.commit()
    conn.close()
    _last_msg_ts[key] = now_ts


def _record_interval(uid: int, cid: int, ts: float, msg_len: int):
    """Записывает интервал в кольцевой буфер"""
    conn = _db()
    conn.execute(
        "INSERT INTO ti_intervals(uid,cid,ts,msg_len) VALUES(?,?,?,?)",
        (uid, cid, int(ts), msg_len)
    )
    # Удаляем старые, оставляем последние _INTERVAL_WINDOW
    conn.execute("""
        DELETE FROM ti_intervals
        WHERE uid=? AND cid=? AND ts NOT IN (
            SELECT ts FROM ti_intervals
            WHERE uid=? AND cid=?
            ORDER BY ts DESC LIMIT ?
        )
    """, (uid, cid, uid, cid, _INTERVAL_WINDOW))
    conn.commit()
    conn.close()


def _compute_interval_stats(uid: int, cid: int) -> Tuple[float, float]:
    """Вычисляет среднее и stddev интервалов из буфера"""
    conn = _db()
    rows = conn.execute(
        "SELECT ts FROM ti_intervals WHERE uid=? AND cid=? ORDER BY ts ASC",
        (uid, cid)
    ).fetchall()
    conn.close()

    timestamps = [r["ts"] for r in rows]
    if len(timestamps) < 3:
        return 0.0, 999.0

    intervals = [timestamps[i + 1] - timestamps[i]
                 for i in range(len(timestamps) - 1)]
    n = len(intervals)
    mean = sum(intervals) / n
    variance = sum((x - mean) ** 2 for x in intervals) / n
    stddev = math.sqrt(variance)
    return mean, stddev


def _detect_bot_pattern(uid: int, cid: int) -> Optional[dict]:
    """
    Детектирует бот-паттерн по равномерности интервалов.
    CoV (коэффициент вариации) < 0.15 = подозрительно равномерный ритм.
    """
    conn = _db()
    row = conn.execute(
        "SELECT msg_count, avg_interval, interval_stddev FROM ti_profiles WHERE uid=? AND cid=?",
        (uid, cid)
    ).fetchone()
    conn.close()

    if not row or row["msg_count"] < _MIN_PROFILE_MSGS:
        return None

    avg = row["avg_interval"]
    std = row["interval_stddev"]

    if avg < 1:
        return None

    cov = std / avg  # коэффициент вариации
    if cov < 0.15 and avg < 10:  # равномерный ритм быстрее 10с
        return {
            "kind": "bot_pattern",
            "score": round((0.15 - cov) / 0.15 * 100, 1),
            "details": f"CoV={cov:.3f}, avg_interval={avg:.1f}s",
        }
    return None


def _detect_style_change(uid: int, cid: int, features: dict) -> Optional[dict]:
    """
    Детектирует резкое изменение стиля письма (взломанный аккаунт).
    Использует z-score по исторической энтропии.
    """
    conn = _db()
    row = conn.execute(
        "SELECT msg_count, avg_entropy, avg_len, caps_ratio FROM ti_profiles WHERE uid=? AND cid=?",
        (uid, cid)
    ).fetchone()
    conn.close()

    if not row or row["msg_count"] < _MIN_PROFILE_MSGS * 2:
        return None

    # Z-score по энтропии (самый стабильный признак)
    hist_entropy = row["avg_entropy"]
    if hist_entropy < 0.5:
        return None

    # Предполагаем stddev ≈ 15% от среднего (для оценки)
    estimated_std = hist_entropy * 0.15
    if estimated_std < 0.01:
        return None

    z = abs(features["entropy"] - hist_entropy) / estimated_std

    if z > _ZSCORE_THRESHOLD:
        return {
            "kind": "style_change",
            "score": round(min(100, z / _ZSCORE_THRESHOLD * 50), 1),
            "details": (
                f"entropy z-score={z:.2f} "
                f"(hist={hist_entropy:.2f}, now={features['entropy']:.2f})"
            ),
        }
    return None


async def _save_anomaly(uid: int, cid: int, anomaly: dict):
    """Сохраняет аномалию и уведомляет при высоком скоре"""
    conn = _db()
    conn.execute(
        "INSERT INTO ti_anomalies(uid,cid,kind,score,details) VALUES(?,?,?,?,?)",
        (uid, cid, anomaly["kind"], anomaly["score"], anomaly["details"])
    )
    conn.commit()
    conn.close()

    _stat_inc("anomalies_detected")
    log.info(f"[TI] Аномалия uid={uid} cid={cid}: {anomaly}")

    if anomaly["score"] > 70:
        await _send_log(
            f"🧠 <b>Аномалия поведения</b>\n\n"
            f"👤 <code>{uid}</code> | чат <code>{cid}</code>\n"
            f"Тип: <b>{anomaly['kind']}</b>\n"
            f"Скор: <b>{anomaly['score']}/100</b>\n"
            f"ℹ️ {anomaly['details']}",
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔨 Забанить",
                    callback_data=f"ti:ban:{cid}:{uid}"
                ),
                InlineKeyboardButton(
                    text="👁 Вотчлист",
                    callback_data=f"ti:watch:{uid}"
                ),
                InlineKeyboardButton(
                    text="✅ Игнор",
                    callback_data=f"ti:ignore_anomaly:{uid}:{cid}"
                ),
            ]])
        )


# ══════════════════════════════════════════════════════════════════════════
#  2. 🕸 THREAT GRAPH
# ══════════════════════════════════════════════════════════════════════════

# Веса для рёбер
_WEIGHT_SAME_INVITE = 50.0
_WEIGHT_SIMILAR_TEXT = 40.0  # умножается на Jaccard similarity
_WEIGHT_CLOSE_JOIN = 30.0    # вход ±5 мин
_WEIGHT_INTERACTION = 5.0    # за каждое взаимодействие
_CLUSTER_THRESHOLD = 40.0    # порог веса для включения в кластер

# Кэш последних входов {cid: [(uid, ts), ...]}
_recent_joins: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
# Окно похожих сообщений {cid: [(uid, ts, text_hash), ...]}
_recent_messages: Dict[int, deque] = defaultdict(lambda: deque(maxlen=200))


def _jaccard_similarity(text_a: str, text_b: str) -> float:
    """Jaccard-схожесть двух текстов по множеству слов"""
    words_a = set(re.findall(r'\b[а-яёa-z\w]{2,}\b', text_a.lower()))
    words_b = set(re.findall(r'\b[а-яёa-z\w]{2,}\b', text_b.lower()))
    if not words_a or not words_b:
        return 0.0
    intersection = len(words_a & words_b)
    union = len(words_a | words_b)
    return intersection / union if union > 0 else 0.0


def _upsert_edge(uid_a: int, uid_b: int, weight_delta: float, reason: str):
    """Создаёт или обновляет ребро в графе угроз"""
    if uid_a == uid_b:
        return
    a, b = min(uid_a, uid_b), max(uid_a, uid_b)
    conn = _db()
    row = conn.execute(
        "SELECT weight, reasons FROM ti_graph_edges WHERE uid_a=? AND uid_b=?", (a, b)
    ).fetchone()

    if row:
        reasons = json.loads(row["reasons"] or "[]")
        if reason not in reasons:
            reasons.append(reason)
            if len(reasons) > 10:
                reasons = reasons[-10:]
        new_weight = min(100.0, row["weight"] + weight_delta)
        conn.execute(
            "UPDATE ti_graph_edges SET weight=?, reasons=?, last_update=strftime('%s','now') "
            "WHERE uid_a=? AND uid_b=?",
            (new_weight, json.dumps(reasons), a, b)
        )
    else:
        conn.execute(
            "INSERT INTO ti_graph_edges(uid_a,uid_b,weight,reasons) VALUES(?,?,?,?)",
            (a, b, min(100.0, weight_delta), json.dumps([reason]))
        )
    conn.commit()
    conn.close()


def _upsert_node(uid: int, name: str = "", username: str = ""):
    conn = _db()
    conn.execute(
        "INSERT INTO ti_graph_nodes(uid,name,username) VALUES(?,?,?) "
        "ON CONFLICT(uid) DO UPDATE SET "
        "name=CASE WHEN excluded.name!='' THEN excluded.name ELSE name END,"
        "username=CASE WHEN excluded.username!='' THEN excluded.username ELSE username END",
        (uid, name, username)
    )
    conn.commit()
    conn.close()


def _graph_neighbors(uid: int, min_weight: float = _CLUSTER_THRESHOLD) -> List[dict]:
    """Возвращает всех соседей вершины с весом выше порога"""
    conn = _db()
    rows = conn.execute("""
        SELECT
            CASE WHEN uid_a=? THEN uid_b ELSE uid_a END AS neighbor,
            weight, reasons
        FROM ti_graph_edges
        WHERE (uid_a=? OR uid_b=?) AND weight >= ?
        ORDER BY weight DESC
    """, (uid, uid, uid, min_weight)).fetchall()
    conn.close()
    return [{"uid": r["neighbor"], "weight": r["weight"],
             "reasons": json.loads(r["reasons"] or "[]")} for r in rows]


def bfs_cluster(root_uid: int, min_weight: float = _CLUSTER_THRESHOLD,
                max_depth: int = 3) -> Set[int]:
    """BFS обход графа для нахождения кластера"""
    visited: Set[int] = set()
    queue = deque([(root_uid, 0)])
    while queue:
        uid, depth = queue.popleft()
        if uid in visited or depth > max_depth:
            continue
        visited.add(uid)
        for neighbor in _graph_neighbors(uid, min_weight):
            if neighbor["uid"] not in visited:
                queue.append((neighbor["uid"], depth + 1))
    return visited


def _mark_cluster(cluster: Set[int], cluster_id: int):
    """Помечает кластер в БД"""
    conn = _db()
    for uid in cluster:
        conn.execute(
            "UPDATE ti_graph_nodes SET cluster_id=? WHERE uid=?",
            (cluster_id, uid)
        )
    conn.commit()
    conn.close()


async def _check_similar_messages(uid: int, cid: int, text: str, ts: float):
    """Ищет похожие сообщения от других пользователей в окне 60 сек"""
    if len(text) < 10:
        return

    recent = _recent_messages[cid]
    now = ts
    cutoff = now - 60

    for entry in recent:
        other_uid, other_ts, other_text = entry
        if other_uid == uid:
            continue
        if other_ts < cutoff:
            continue
        sim = _jaccard_similarity(text, other_text)
        if sim > 0.6:
            _upsert_node(uid)
            _upsert_node(other_uid)
            _upsert_edge(uid, other_uid,
                         _WEIGHT_SIMILAR_TEXT * sim,
                         f"similar_text:{sim:.2f}")
            _stat_inc("edges_similar_text")

    recent.append((uid, ts, text))


def _check_close_joins(uid: int, cid: int, ts: float):
    """Создаёт рёбра между пользователями, вошедшими почти одновременно"""
    recent = _recent_joins[cid]
    cutoff = ts - 300  # 5 минут

    # Очищаем старые
    _recent_joins[cid] = [(u, t) for u, t in recent if t > cutoff]

    for other_uid, other_ts in _recent_joins[cid]:
        if other_uid == uid:
            continue
        time_diff = abs(ts - other_ts)
        if time_diff < 300:
            weight = _WEIGHT_CLOSE_JOIN * (1 - time_diff / 300)
            _upsert_edge(uid, other_uid, weight,
                         f"close_join:{time_diff:.0f}s")
            _stat_inc("edges_close_join")

    _recent_joins[cid].append((uid, ts))


def _format_threat_tree(root_uid: int, depth: int = 2, min_weight: float = 30.0) -> str:
    """Форматирует граф как текстовое дерево для команды /threatmap"""
    def _render(uid: int, current_depth: int, visited: Set[int], prefix: str) -> str:
        if uid in visited or current_depth > depth:
            return ""
        visited.add(uid)

        conn = _db()
        node = conn.execute(
            "SELECT name, username, banned FROM ti_graph_nodes WHERE uid=?", (uid,)
        ).fetchone()
        conn.close()

        name = node["name"] if node else str(uid)
        banned_mark = " 🔨" if node and node["banned"] else ""
        result = f"{prefix}👤 {name} (<code>{uid}</code>){banned_mark}\n"

        neighbors = _graph_neighbors(uid, min_weight)
        for i, n in enumerate(neighbors[:5]):
            is_last = i == len(neighbors) - 1
            child_prefix = prefix + ("└─ " if is_last else "├─ ")
            continuation = prefix + ("   " if is_last else "│  ")
            reasons_str = ", ".join(n["reasons"][:2])
            result += f"{prefix}{'└' if is_last else '├'} w={n['weight']:.0f} [{reasons_str}]\n"
            result += _render(n["uid"], current_depth + 1, visited, continuation)
        return result

    return _render(root_uid, 0, set(), "")


# ══════════════════════════════════════════════════════════════════════════
#  3. 🔬 MULTIACCOUNT DETECTOR
# ══════════════════════════════════════════════════════════════════════════

_LEVENSHTEIN_THRESHOLD = 3    # макс. дистанция для похожих имён
_CONFIDENCE_THRESHOLD = 60.0  # порог для авто-карантина
_FINGERPRINT_CACHE: Dict[int, dict] = {}  # {uid: fingerprint}


def _levenshtein(s1: str, s2: str) -> int:
    """Расстояние Левенштейна"""
    s1 = s1.lower().strip()
    s2 = s2.lower().strip()
    if s1 == s2:
        return 0
    if len(s1) < len(s2):
        s1, s2 = s2, s1
    if not s2:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row
    return prev_row[-1]


def _normalize_name(name: str) -> str:
    """Нормализация имени: транслит + нижний регистр"""
    translit = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
        'е': 'e', 'ё': 'e', 'ж': 'zh', 'з': 'z', 'и': 'i',
        'й': 'j', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
        'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
        'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch',
        'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '',
        'э': 'e', 'ю': 'yu', 'я': 'ya',
    }
    result = name.lower()
    for cyr, lat in translit.items():
        result = result.replace(cyr, lat)
    # Убираем цифры в конце (типичный паттерн мультиаккаунтов: user1, user2)
    result = re.sub(r'\d+$', '', result).strip()
    return result


def _build_time_vector(uid: int, cid: int) -> List[float]:
    """
    Строит вектор 24×7 (168 ячеек) из профиля пользователя.
    Нормализован, сумма = 1.
    """
    conn = _db()
    row = conn.execute(
        "SELECT hour_vector, day_vector FROM ti_profiles WHERE uid=? AND cid=?",
        (uid, cid)
    ).fetchone()
    conn.close()

    vec = [0.0] * 168
    if not row:
        return vec

    try:
        hv = json.loads(row["hour_vector"] or "{}")
        dv = json.loads(row["day_vector"] or "{}")
    except Exception:
        return vec

    total = 0.0
    for h, count in hv.items():
        try:
            for d, d_count in dv.items():
                idx = int(d) * 24 + int(h)
                if 0 <= idx < 168:
                    val = count * d_count
                    vec[idx] += val
                    total += val
        except Exception:
            pass

    if total > 0:
        vec = [v / total for v in vec]
    return vec


def _cosine_similarity(va: List[float], vb: List[float]) -> float:
    """Косинусное сходство двух векторов"""
    if len(va) != len(vb):
        return 0.0
    dot = sum(a * b for a, b in zip(va, vb))
    norm_a = math.sqrt(sum(a ** 2 for a in va))
    norm_b = math.sqrt(sum(b ** 2 for b in vb))
    if norm_a < 1e-9 or norm_b < 1e-9:
        return 0.0
    return dot / (norm_a * norm_b)


def _compute_fingerprint(uid: int, cid: int, name: str, username: str) -> dict:
    """Вычисляет цифровой отпечаток пользователя"""
    conn = _db()
    profile = conn.execute(
        "SELECT * FROM ti_profiles WHERE uid=? AND cid=?", (uid, cid)
    ).fetchone()
    conn.close()

    if profile:
        p = dict(profile)
    else:
        p = {}

    return {
        "uid": uid,
        "cid": cid,
        "norm_name": _normalize_name(name),
        "norm_username": _normalize_name(username) if username else "",
        "time_vector": _build_time_vector(uid, cid),
        "avg_len": p.get("avg_len", 0),
        "avg_entropy": p.get("avg_entropy", 0),
        "caps_ratio": p.get("caps_ratio", 0),
        "emoji_ratio": p.get("emoji_ratio", 0),
        "msg_count": p.get("msg_count", 0),
    }


def _compare_fingerprints(fp_a: dict, fp_b: dict) -> dict:
    """
    Сравнивает два отпечатка, возвращает dict с оценками по сигналам
    и итоговым confidence 0–100.
    """
    signals = {}
    total_score = 0.0
    total_weight = 0.0

    # Сигнал 1: Левенштейн по нормализованному имени (вес 30)
    lev = _levenshtein(fp_a["norm_name"], fp_b["norm_name"])
    name_score = max(0.0, 1.0 - lev / max(
        len(fp_a["norm_name"]), len(fp_b["norm_name"]), 1
    ))
    signals["name_similarity"] = round(name_score * 100, 1)
    total_score += name_score * 30
    total_weight += 30

    # Сигнал 2: Левенштейн по username (вес 25)
    if fp_a["norm_username"] and fp_b["norm_username"]:
        lev_u = _levenshtein(fp_a["norm_username"], fp_b["norm_username"])
        user_score = max(0.0, 1.0 - lev_u / max(
            len(fp_a["norm_username"]), len(fp_b["norm_username"]), 1
        ))
        signals["username_similarity"] = round(user_score * 100, 1)
        total_score += user_score * 25
        total_weight += 25

    # Сигнал 3: Косинусное сходство временных векторов (вес 35)
    if any(v > 0 for v in fp_a["time_vector"]) and any(v > 0 for v in fp_b["time_vector"]):
        cos_sim = _cosine_similarity(fp_a["time_vector"], fp_b["time_vector"])
        signals["activity_pattern"] = round(cos_sim * 100, 1)
        total_score += cos_sim * 35
        total_weight += 35

    # Сигнал 4: Схожесть стиля (avg_len + entropy + emoji) (вес 10)
    style_diffs = []
    for metric in ["avg_len", "avg_entropy", "emoji_ratio"]:
        va = fp_a.get(metric, 0)
        vb = fp_b.get(metric, 0)
        if max(va, vb) > 0:
            diff = abs(va - vb) / max(va, vb)
            style_diffs.append(1.0 - diff)
    if style_diffs:
        style_score = sum(style_diffs) / len(style_diffs)
        signals["writing_style"] = round(style_score * 100, 1)
        total_score += style_score * 10
        total_weight += 10

    confidence = (total_score / total_weight * 100) if total_weight > 0 else 0.0
    return {
        "confidence": round(confidence, 1),
        "signals": signals,
    }


async def _check_for_alts(uid: int, cid: int, name: str, username: str):
    """
    При входе нового пользователя сравнивает его с отпечатками всех забаненных.
    """
    fp_new = _compute_fingerprint(uid, cid, name, username)
    _FINGERPRINT_CACHE[uid] = fp_new

    # Берём всех забаненных из графа
    conn = _db()
    banned_nodes = conn.execute(
        "SELECT uid, name, username FROM ti_graph_nodes WHERE banned=1 AND uid!=?",
        (uid,)
    ).fetchall()
    conn.close()

    for node in banned_nodes:
        other_uid = node["uid"]
        if other_uid in _FINGERPRINT_CACHE:
            fp_other = _FINGERPRINT_CACHE[other_uid]
        else:
            fp_other = _compute_fingerprint(
                other_uid, cid, node["name"] or "", node["username"] or ""
            )
            _FINGERPRINT_CACHE[other_uid] = fp_other

        result = _compare_fingerprints(fp_new, fp_other)
        confidence = result["confidence"]

        if confidence >= 35:  # порог для записи в БД
            a, b = min(uid, other_uid), max(uid, other_uid)
            conn = _db()
            conn.execute(
                "INSERT INTO ti_alt_suspects(uid_orig,uid_alt,confidence,signals) "
                "VALUES(?,?,?,?) ON CONFLICT(uid_orig,uid_alt) DO UPDATE SET "
                "confidence=excluded.confidence, signals=excluded.signals",
                (a, b, confidence, json.dumps(result["signals"]))
            )
            conn.commit()
            conn.close()
            _stat_inc("alt_suspects_found")

        if confidence >= _CONFIDENCE_THRESHOLD:
            await _send_log(
                f"🔬 <b>Возможный мультиаккаунт</b>\n\n"
                f"Новый: <code>{uid}</code> ({name})\n"
                f"Забанен: <code>{other_uid}</code> ({node['name']})\n"
                f"Уверенность: <b>{confidence:.1f}%</b>\n\n"
                + "\n".join(
                    f"• {k}: {v}%" for k, v in result["signals"].items()
                ),
                kb=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="🔨 Забанить",
                        callback_data=f"ti:ban:{cid}:{uid}"
                    ),
                    InlineKeyboardButton(
                        text="🔇 Карантин",
                        callback_data=f"ti:quarantine:{cid}:{uid}"
                    ),
                    InlineKeyboardButton(
                        text="✅ Не альт",
                        callback_data=f"ti:not_alt:{a}:{b}"
                    ),
                ]])
            )


# ══════════════════════════════════════════════════════════════════════════
#  4. ⚖️ APPEAL SYSTEM
# ══════════════════════════════════════════════════════════════════════════

_APPEAL_QUORUM = 2        # минимум голосов для решения
_APPEAL_TIMEOUT_HOURS = 48  # часов на рассмотрение
_MAX_APPEALS_PER_USER = 3   # максимум апелляций на пользователя


def _get_available_mods() -> List[int]:
    """Возвращает список доступных модераторов из admin_ids"""
    return list(_admin_ids)


async def _assign_appeal_panel(appeal_id: int, banned_by_uid: int):
    """
    Назначает коллегию из случайных модераторов, исключая того кто банил.
    """
    import random
    mods = [m for m in _get_available_mods() if m != banned_by_uid]
    panel = random.sample(mods, min(_APPEAL_QUORUM, len(mods)))

    conn = _db()
    conn.execute(
        "UPDATE ti_appeals SET assigned_to=?, quorum=? WHERE id=?",
        (json.dumps(panel), max(1, len(panel) // 2 + 1), appeal_id)
    )
    row = conn.execute(
        "SELECT uid, cid, reason, evidence FROM ti_appeals WHERE id=?",
        (appeal_id,)
    ).fetchone()
    conn.commit()
    conn.close()

    if not row:
        return

    # Уведомляем каждого модератора
    for mod_uid in panel:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ Разбанить",
                callback_data=f"ti:vote:{appeal_id}:{mod_uid}:for"
            ),
            InlineKeyboardButton(
                text="❌ Отказать",
                callback_data=f"ti:vote:{appeal_id}:{mod_uid}:against"
            ),
        ]])
        try:
            await _bot.send_message(
                mod_uid,
                f"⚖️ <b>Новая апелляция #{appeal_id}</b>\n\n"
                f"👤 Пользователь: <code>{row['uid']}</code>\n"
                f"💬 Чат: <code>{row['cid']}</code>\n"
                f"📝 Причина бана: см. историю\n\n"
                f"<b>Аргумент пользователя:</b>\n{row['reason'][:500]}\n\n"
                f"{'<b>Доказательства:</b> ' + row['evidence'][:200] if row['evidence'] else ''}\n\n"
                f"<i>Для решения необходимо {_APPEAL_QUORUM} голосов</i>",
                parse_mode="HTML",
                reply_markup=kb
            )
        except Exception as e:
            log.warning(f"appeal notify mod {mod_uid}: {e}")


async def _resolve_appeal(appeal_id: int):
    """Исполняет решение по апелляции после достижения кворума"""
    conn = _db()
    row = conn.execute(
        "SELECT * FROM ti_appeals WHERE id=?", (appeal_id,)
    ).fetchone()
    conn.close()

    if not row or row["status"] != "pending":
        return

    votes_for = row["votes_for"]
    votes_against = row["votes_against"]
    quorum = row["quorum"]

    if votes_for >= quorum:
        result = "approved"
        # Разбаниваем
        try:
            await _bot.unban_chat_member(row["cid"], row["uid"])
            await _bot.send_message(
                row["uid"],
                f"✅ <b>Ваша апелляция #{appeal_id} одобрена!</b>\n\n"
                f"Бан снят. Пожалуйста, соблюдайте правила чата.",
                parse_mode="HTML"
            )
            _stat_inc("appeals_approved")
        except Exception as e:
            log.warning(f"unban error: {e}")

    elif votes_against >= quorum:
        result = "rejected"
        try:
            await _bot.send_message(
                row["uid"],
                f"❌ <b>Апелляция #{appeal_id} отклонена.</b>\n\n"
                f"Коллегия модераторов рассмотрела ваше обращение "
                f"и приняла решение оставить бан в силе.",
                parse_mode="HTML"
            )
            _stat_inc("appeals_rejected")
        except Exception as e:
            log.warning(f"appeal notify error: {e}")
    else:
        return  # Кворум ещё не достигнут

    conn = _db()
    conn.execute(
        "UPDATE ti_appeals SET status='resolved', result=?, "
        "resolved_at=strftime('%s','now') WHERE id=?",
        (result, appeal_id)
    )
    conn.commit()
    conn.close()

    await _send_log(
        f"⚖️ <b>Апелляция #{appeal_id} — {result.upper()}</b>\n\n"
        f"👤 uid: <code>{row['uid']}</code>\n"
        f"🗳 За: {votes_for} | Против: {votes_against} | Кворум: {quorum}"
    )


# ══════════════════════════════════════════════════════════════════════════
#  TRUST SCORE
# ══════════════════════════════════════════════════════════════════════════

def get_trust_score(uid: int, cid: int) -> int:
    """Возвращает TrustScore пользователя (0–100, старт 50)"""
    conn = _db()
    row = conn.execute(
        "SELECT score FROM ti_trust WHERE uid=? AND cid=?", (uid, cid)
    ).fetchone()
    conn.close()
    return row["score"] if row else 50


def adjust_trust(uid: int, cid: int, delta: int, reason: str):
    """Изменяет TrustScore"""
    conn = _db()
    row = conn.execute(
        "SELECT score, penalties, bonuses FROM ti_trust WHERE uid=? AND cid=?",
        (uid, cid)
    ).fetchone()

    if row:
        current = row["score"]
        new_score = max(0, min(100, current + delta))
        field = "bonuses" if delta > 0 else "penalties"
        try:
            lst = json.loads(row[field] or "[]")
        except Exception:
            lst = []
        lst.append({"delta": delta, "reason": reason,
                    "ts": int(time.time())})
        if len(lst) > 20:
            lst = lst[-20:]
        conn.execute(
            f"UPDATE ti_trust SET score=?, {field}=?, updated_at=strftime('%s','now') "
            f"WHERE uid=? AND cid=?",
            (new_score, json.dumps(lst), uid, cid)
        )
    else:
        new_score = max(0, min(100, 50 + delta))
        field = "bonuses" if delta > 0 else "penalties"
        lst = [{"delta": delta, "reason": reason, "ts": int(time.time())}]
        conn.execute(
            "INSERT INTO ti_trust(uid,cid,score,penalties,bonuses) VALUES(?,?,?,?,?)",
            (uid, cid, new_score,
             json.dumps(lst) if delta < 0 else "[]",
             json.dumps(lst) if delta > 0 else "[]")
        )

    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════
#  ПУБЛИЧНЫЙ API — ХУКИ ДЛЯ BOT.PY
# ══════════════════════════════════════════════════════════════════════════

async def on_message(message: Message):
    """
    Вызывается из StatsMiddleware на каждое сообщение.
    Обновляет профиль, ищет аномалии, обновляет граф.
    """
    if not message.from_user:
        return
    if message.chat.type not in ("group", "supergroup"):
        return

    uid = message.from_user.id
    cid = message.chat.id
    text = message.text or message.caption or ""
    now = time.time()

    # 1. Обновляем профиль
    features = _extract_features(text)
    _update_profile(uid, cid, features, now)
    _record_interval(uid, cid, now, features["len"])

    # 2. Обновляем граф (похожие сообщения)
    if text and len(text) > 10:
        await _check_similar_messages(uid, cid, text, now)

    # 3. Проверяем на аномалии (не каждое сообщение, экономим ресурсы)
    conn = _db()
    row = conn.execute(
        "SELECT msg_count FROM ti_profiles WHERE uid=? AND cid=?", (uid, cid)
    ).fetchone()
    conn.close()

    if row and row["msg_count"] % 10 == 0:  # каждые 10 сообщений
        for detector in [_detect_bot_pattern, _detect_style_change]:
            anomaly = detector(uid, cid) if detector == _detect_bot_pattern \
                else detector(uid, cid, features)
            if anomaly:
                await _save_anomaly(uid, cid, anomaly)
                adjust_trust(uid, cid, -10, anomaly["kind"])
                break  # одна аномалия за раз


async def on_user_join(message: Message, member) -> bool:
    """
    Вызывается при входе нового пользователя.
    Возвращает True если пользователь был заблокирован.
    """
    uid = member.id
    cid = message.chat.id
    name = member.full_name or ""
    username = member.username or ""
    now = time.time()

    # Регистрируем вершину в графе
    _upsert_node(uid, name, username)
    # Проверяем временную близость входа
    _check_close_joins(uid, cid, now)

    # Запускаем проверку мультиаккаунтов в фоне
    asyncio.create_task(_check_for_alts(uid, cid, name, username))

    return False


async def on_user_banned(uid: int, cid: int, reason: str = "",
                         banned_by_uid: int = 0):
    """
    Вызывается при бане пользователя.
    Обновляет граф, снижает TrustScore.
    """
    conn = _db()
    conn.execute(
        "UPDATE ti_graph_nodes SET banned=1, ban_cid=?, ban_reason=?, ban_ts=? WHERE uid=?",
        (cid, reason[:200], int(time.time()), uid)
    )
    conn.commit()
    conn.close()

    adjust_trust(uid, cid, -40, f"ban: {reason[:50]}")
    _stat_inc("users_banned_tracked")

    # Кэшируем отпечаток для будущих сравнений
    conn = _db()
    node = conn.execute(
        "SELECT name, username FROM ti_graph_nodes WHERE uid=?", (uid,)
    ).fetchone()
    conn.close()

    if node:
        fp = _compute_fingerprint(uid, cid,
                                   node["name"] or "", node["username"] or "")
        _FINGERPRINT_CACHE[uid] = fp

    # Пересчёт кластеров
    cluster = bfs_cluster(uid)
    if len(cluster) > 1:
        cluster_id = int(time.time()) % 1_000_000
        _mark_cluster(cluster, cluster_id)
        _stat_inc("threat_clusters_found")

        await _send_log(
            f"🕸 <b>Кластер угроз обнаружен</b>\n\n"
            f"Триггер: бан <code>{uid}</code>\n"
            f"Размер кластера: <b>{len(cluster)}</b> аккаунтов\n"
            f"UIDs: {', '.join(f'<code>{u}</code>' for u in list(cluster)[:10])}"
            + (f"\n...и ещё {len(cluster)-10}" if len(cluster) > 10 else ""),
            kb=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text=f"🔨 Забанить всех ({len(cluster)})",
                    callback_data=f"ti:ban_cluster:{cluster_id}:{cid}"
                ),
            ]])
        )


# ══════════════════════════════════════════════════════════════════════════
#  КОМАНДЫ БОТА
# ══════════════════════════════════════════════════════════════════════════

async def cmd_threatmap(message: Message):
    """/threatmap @user или /threatmap [uid] — граф связей"""
    uid = message.from_user.id
    cid = message.chat.id

    target_uid = None
    args = (message.text or "").split()[1:]

    if message.reply_to_message:
        target_uid = message.reply_to_message.from_user.id
    elif args:
        try:
            target_uid = int(args[0])
        except ValueError:
            await message.answer("⚠️ Укажи ID или ответь на сообщение")
            return

    if not target_uid:
        await message.answer("⚠️ /threatmap @user или ответь на сообщение")
        return

    # Получаем граф
    neighbors = _graph_neighbors(target_uid, min_weight=20.0)
    if not neighbors:
        await message.answer(
            f"🕸 Нет связей для <code>{target_uid}</code> (порог веса: 20)",
            parse_mode="HTML"
        )
        return

    conn = _db()
    node = conn.execute(
        "SELECT name, username, banned FROM ti_graph_nodes WHERE uid=?",
        (target_uid,)
    ).fetchone()
    conn.close()

    name = node["name"] if node else str(target_uid)
    banned_str = " 🔨 ЗАБАНЕН" if node and node["banned"] else ""

    tree = _format_threat_tree(target_uid, depth=2, min_weight=20.0)
    cluster = bfs_cluster(target_uid)

    text = (
        f"🕸 <b>Граф угроз: {name}</b>{banned_str}\n"
        f"uid: <code>{target_uid}</code>\n"
        f"Кластер: {len(cluster)} аккаунтов\n"
        f"Прямых связей: {len(neighbors)}\n\n"
        f"<pre>{tree[:2000]}</pre>"
    )

    kb = None
    if len(cluster) > 1:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=f"🔨 Забанить кластер ({len(cluster)})",
                callback_data=f"ti:ban_cluster_cmd:{target_uid}:{cid}"
            )
        ]])

    await message.answer(text, parse_mode="HTML", reply_markup=kb)


async def cmd_alts(message: Message):
    """/alts @user — возможные мультиаккаунты"""
    args = (message.text or "").split()[1:]
    target_uid = None

    if message.reply_to_message:
        target_uid = message.reply_to_message.from_user.id
    elif args:
        try:
            target_uid = int(args[0])
        except ValueError:
            pass

    if not target_uid:
        await message.answer("⚠️ /alts @user или ответь на сообщение")
        return

    a = min(target_uid, target_uid)
    conn = _db()
    suspects = conn.execute("""
        SELECT
            CASE WHEN uid_orig=? THEN uid_alt ELSE uid_orig END AS alt_uid,
            confidence, signals, confirmed
        FROM ti_alt_suspects
        WHERE (uid_orig=? OR uid_alt=?) AND confidence >= 30
        ORDER BY confidence DESC
        LIMIT 10
    """, (target_uid, target_uid, target_uid)).fetchall()
    conn.close()

    if not suspects:
        await message.answer(
            f"🔬 Возможных мультиаккаунтов для <code>{target_uid}</code> не найдено",
            parse_mode="HTML"
        )
        return

    lines = []
    for s in suspects:
        sigs = json.loads(s["signals"] or "{}")
        top_sig = max(sigs.items(), key=lambda x: x[1])[0] if sigs else "—"
        conf_bar = "█" * int(s["confidence"] / 10) + "░" * (10 - int(s["confidence"] / 10))
        lines.append(
            f"• <code>{s['alt_uid']}</code> — {s['confidence']:.0f}%\n"
            f"  [{conf_bar}] топ-сигнал: {top_sig}"
        )

    await message.answer(
        f"🔬 <b>Возможные альты для <code>{target_uid}</code>:</b>\n\n"
        + "\n".join(lines),
        parse_mode="HTML"
    )


async def cmd_trustscore(message: Message):
    """/trustscore @user — TrustScore пользователя"""
    target_uid = None
    cid = message.chat.id

    if message.reply_to_message:
        target_uid = message.reply_to_message.from_user.id
    else:
        args = (message.text or "").split()[1:]
        if args:
            try:
                target_uid = int(args[0])
            except ValueError:
                pass

    if not target_uid:
        target_uid = message.from_user.id

    score = get_trust_score(target_uid, cid)
    bar_filled = int(score / 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)

    if score >= 70:
        emoji, label = "🟢", "Доверенный"
    elif score >= 40:
        emoji, label = "🟡", "Нейтральный"
    elif score >= 20:
        emoji, label = "🟠", "Подозрительный"
    else:
        emoji, label = "🔴", "Опасный"

    conn = _db()
    trust_row = conn.execute(
        "SELECT penalties FROM ti_trust WHERE uid=? AND cid=?",
        (target_uid, cid)
    ).fetchone()
    conn.close()

    penalties_text = ""
    if trust_row:
        try:
            penalties = json.loads(trust_row["penalties"] or "[]")[-3:]
            if penalties:
                penalties_text = "\n<b>Последние штрафы:</b>\n" + "\n".join(
                    f"• -{abs(p['delta'])} {p['reason']}" for p in penalties
                )
        except Exception:
            pass

    await message.answer(
        f"{emoji} <b>TrustScore: <code>{target_uid}</code></b>\n\n"
        f"Уровень: <b>{label}</b>\n"
        f"Скор: <b>{score}/100</b>\n"
        f"[{bar}]\n"
        f"{penalties_text}",
        parse_mode="HTML"
    )


async def cmd_appeal(message: Message):
    """/appeal [причина] — подать апелляцию на бан (из лички)"""
    uid = message.from_user.id
    text = message.text or ""
    args = text.split(maxsplit=1)
    reason = args[1].strip() if len(args) > 1 else ""

    if not reason:
        await message.answer(
            "⚖️ <b>Подача апелляции</b>\n\n"
            "Напишите: /appeal [ваша причина]\n\n"
            "<i>Апелляция будет рассмотрена коллегией модераторов.\n"
            "Максимум 3 апелляции.</i>",
            parse_mode="HTML"
        )
        return

    # Проверяем лимит апелляций
    conn = _db()
    count = conn.execute(
        "SELECT COUNT(*) as c FROM ti_appeals WHERE uid=? AND status='pending'",
        (uid,)
    ).fetchone()["c"]
    total = conn.execute(
        "SELECT COUNT(*) as c FROM ti_appeals WHERE uid=?", (uid,)
    ).fetchone()["c"]
    conn.close()

    if count > 0:
        await message.answer(
            "⚠️ У вас уже есть активная апелляция на рассмотрении."
        )
        return

    if total >= _MAX_APPEALS_PER_USER:
        await message.answer(
            f"❌ Достигнут лимит апелляций ({_MAX_APPEALS_PER_USER})."
        )
        return

    # Ищем последний бан пользователя
    conn = _db()
    ban_node = conn.execute(
        "SELECT ban_cid FROM ti_graph_nodes WHERE uid=? AND banned=1",
        (uid,)
    ).fetchone()
    conn.close()

    cid = ban_node["ban_cid"] if ban_node else 0

    conn = _db()
    cur = conn.execute(
        "INSERT INTO ti_appeals(uid,cid,reason) VALUES(?,?,?)",
        (uid, cid, reason[:1000])
    )
    appeal_id = cur.lastrowid
    conn.commit()
    conn.close()

    _stat_inc("appeals_created")

    await message.answer(
        f"✅ <b>Апелляция #{appeal_id} принята</b>\n\n"
        f"Ваше обращение передано коллегии модераторов.\n"
        f"Решение будет принято в течение {_APPEAL_TIMEOUT_HOURS} часов.",
        parse_mode="HTML"
    )

    await _assign_appeal_panel(appeal_id, banned_by_uid=0)


async def cmd_tistat(message: Message):
    """/tistat — статистика модуля"""
    conn = _db()
    stats = {r["key"]: r["value"] for r in conn.execute(
        "SELECT key, value FROM ti_stats"
    ).fetchall()}

    profiles = conn.execute("SELECT COUNT(*) as c FROM ti_profiles").fetchone()["c"]
    anomalies = conn.execute(
        "SELECT COUNT(*) as c FROM ti_anomalies WHERE resolved=0"
    ).fetchone()["c"]
    nodes = conn.execute("SELECT COUNT(*) as c FROM ti_graph_nodes").fetchone()["c"]
    edges = conn.execute("SELECT COUNT(*) as c FROM ti_graph_edges").fetchone()["c"]
    suspects = conn.execute(
        "SELECT COUNT(*) as c FROM ti_alt_suspects WHERE confidence>=60"
    ).fetchone()["c"]
    appeals = conn.execute(
        "SELECT COUNT(*) as c FROM ti_appeals WHERE status='pending'"
    ).fetchone()["c"]
    conn.close()

    await message.answer(
        f"📊 <b>Threat Intel — Статистика</b>\n\n"
        f"🧠 Поведенческих профилей: <b>{profiles}</b>\n"
        f"⚠️ Активных аномалий: <b>{anomalies}</b>\n"
        f"🕸 Вершин в графе: <b>{nodes}</b>\n"
        f"🔗 Рёбер в графе: <b>{edges}</b>\n"
        f"🔬 Подозр. альтов (≥60%): <b>{suspects}</b>\n"
        f"⚖️ Апелляций на рассмотрении: <b>{appeals}</b>\n\n"
        f"Всего аномалий: {stats.get('anomalies_detected', 0)}\n"
        f"Кластеров найдено: {stats.get('threat_clusters_found', 0)}\n"
        f"Апелляций одобрено: {stats.get('appeals_approved', 0)}\n"
        f"Апелляций отклонено: {stats.get('appeals_rejected', 0)}",
        parse_mode="HTML"
    )


# ══════════════════════════════════════════════════════════════════════════
#  CALLBACK HANDLERS
# ══════════════════════════════════════════════════════════════════════════

async def cb_ti(call: CallbackQuery):
    """Обработчик всех callback'ов модуля"""
    parts = call.data.split(":")
    action = parts[1]

    if action == "ban" and len(parts) >= 4:
        cid, uid = int(parts[2]), int(parts[3])
        try:
            await _bot.ban_chat_member(cid, uid)
            await call.message.edit_reply_markup(reply_markup=None)
            await call.answer(f"🔨 {uid} забанен")
            await on_user_banned(uid, cid, "manual_from_alert", call.from_user.id)
        except Exception as e:
            await call.answer(f"Ошибка: {e}")

    elif action == "quarantine" and len(parts) >= 4:
        cid, uid = int(parts[2]), int(parts[3])
        try:
            until = datetime.now() + timedelta(hours=24)
            await _bot.restrict_chat_member(
                cid, uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until
            )
            await call.message.edit_reply_markup(reply_markup=None)
            await call.answer(f"🔇 {uid} в карантине на 24ч")
            adjust_trust(uid, cid, -20, "quarantine_alt_suspect")
        except Exception as e:
            await call.answer(f"Ошибка: {e}")

    elif action == "watch" and len(parts) >= 3:
        uid = int(parts[2])
        conn = _db()
        conn.execute(
            "INSERT INTO watchlist(uid,reason,added_by) VALUES(?,?,?) "
            "ON CONFLICT(uid) DO NOTHING",
            (uid, "ThreatIntel alert", str(call.from_user.id))
        ) if False else None  # интеграция с sf.watchlist_add
        conn.close()
        try:
            import security_features as sf
            sf.watchlist_add(uid, "ThreatIntel alert", str(call.from_user.id))
            await call.answer(f"👁 {uid} добавлен в вотчлист")
        except Exception as e:
            await call.answer(f"Вотчлист: {e}")

    elif action == "ignore_anomaly" and len(parts) >= 4:
        uid, cid = int(parts[2]), int(parts[3])
        conn = _db()
        conn.execute(
            "UPDATE ti_anomalies SET resolved=1 "
            "WHERE uid=? AND cid=? AND resolved=0",
            (uid, cid)
        )
        conn.commit()
        conn.close()
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer("✅ Аномалия помечена как ложная")

    elif action == "not_alt" and len(parts) >= 4:
        a, b = int(parts[2]), int(parts[3])
        conn = _db()
        conn.execute(
            "UPDATE ti_alt_suspects SET confirmed=-1, reviewed_by=? "
            "WHERE uid_orig=? AND uid_alt=?",
            (call.from_user.id, min(a, b), max(a, b))
        )
        conn.commit()
        conn.close()
        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer("✅ Помечено: не мультиаккаунт")

    elif action == "vote" and len(parts) >= 5:
        appeal_id, voter_uid, vote = int(parts[2]), int(parts[3]), parts[4]
        if call.from_user.id != voter_uid:
            await call.answer("❌ Это не ваш голос")
            return

        vote_val = 1 if vote == "for" else 0
        conn = _db()
        existing = conn.execute(
            "SELECT vote FROM ti_appeal_votes WHERE appeal_id=? AND voter_uid=?",
            (appeal_id, voter_uid)
        ).fetchone()

        if existing:
            await call.answer("⚠️ Вы уже голосовали")
            conn.close()
            return

        conn.execute(
            "INSERT INTO ti_appeal_votes(appeal_id,voter_uid,vote) VALUES(?,?,?)",
            (appeal_id, voter_uid, vote_val)
        )
        if vote_val == 1:
            conn.execute(
                "UPDATE ti_appeals SET votes_for=votes_for+1 WHERE id=?",
                (appeal_id,)
            )
        else:
            conn.execute(
                "UPDATE ti_appeals SET votes_against=votes_against+1 WHERE id=?",
                (appeal_id,)
            )
        conn.commit()
        conn.close()

        await call.answer(
            f"✅ Голос принят: {'за разбан' if vote_val else 'против'}"
        )
        await call.message.edit_reply_markup(reply_markup=None)
        # Проверяем достижение кворума
        await _resolve_appeal(appeal_id)

    elif action == "ban_cluster" and len(parts) >= 4:
        cluster_id, cid = int(parts[2]), int(parts[3])
        conn = _db()
        nodes = conn.execute(
            "SELECT uid FROM ti_graph_nodes WHERE cluster_id=? AND banned=0",
            (cluster_id,)
        ).fetchall()
        conn.close()

        banned_count = 0
        for node in nodes:
            try:
                await _bot.ban_chat_member(cid, node["uid"])
                await on_user_banned(node["uid"], cid, "cluster_ban", call.from_user.id)
                banned_count += 1
                await asyncio.sleep(0.1)
            except Exception:
                pass

        await call.message.edit_reply_markup(reply_markup=None)
        await call.answer(f"🔨 Забанено {banned_count} из кластера")

    elif action == "ban_cluster_cmd" and len(parts) >= 4:
        root_uid, cid = int(parts[2]), int(parts[3])
        cluster = bfs_cluster(root_uid)
        banned_count = 0
        for uid in cluster:
            if uid == root_uid:
                continue
            try:
                await _bot.ban_chat_member(cid, uid)
                await on_user_banned(uid, cid, "cluster_ban_cmd", call.from_user.id)
                banned_count += 1
                await asyncio.sleep(0.1)
            except Exception:
                pass
        await call.answer(f"🔨 Забанено {banned_count} связанных аккаунтов")
        await call.message.edit_reply_markup(reply_markup=None)

    else:
        await call.answer()


# ══════════════════════════════════════════════════════════════════════════
#  5. 📊 API ДЛЯ ДАШБОРДА
# ══════════════════════════════════════════════════════════════════════════

def dashboard_get_anomalies(limit: int = 50) -> List[dict]:
    """Последние нерешённые аномалии для дашборда"""
    conn = _db()
    rows = conn.execute("""
        SELECT a.*, n.name, n.username
        FROM ti_anomalies a
        LEFT JOIN ti_graph_nodes n ON a.uid=n.uid
        WHERE a.resolved=0
        ORDER BY a.ts DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def dashboard_get_graph_data(min_weight: float = 20.0) -> dict:
    """Граф угроз в формате для D3.js визуализации"""
    conn = _db()
    nodes = conn.execute("""
        SELECT uid, name, username, banned, cluster_id
        FROM ti_graph_nodes
        WHERE uid IN (
            SELECT uid_a FROM ti_graph_edges WHERE weight>=?
            UNION
            SELECT uid_b FROM ti_graph_edges WHERE weight>=?
        )
        LIMIT 200
    """, (min_weight, min_weight)).fetchall()

    edges = conn.execute("""
        SELECT uid_a, uid_b, weight, reasons
        FROM ti_graph_edges
        WHERE weight>=?
        ORDER BY weight DESC
        LIMIT 500
    """, (min_weight,)).fetchall()
    conn.close()

    return {
        "nodes": [{"id": r["uid"], "name": r["name"] or str(r["uid"]),
                   "banned": bool(r["banned"]),
                   "cluster": r["cluster_id"]} for r in nodes],
        "links": [{"source": r["uid_a"], "target": r["uid_b"],
                   "weight": r["weight"],
                   "reasons": json.loads(r["reasons"] or "[]")} for r in edges],
    }


def dashboard_get_appeals(status: str = "pending") -> List[dict]:
    """Апелляции для дашборда"""
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM ti_appeals WHERE status=? ORDER BY created_at DESC LIMIT 50",
        (status,)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["assigned_to"] = json.loads(d["assigned_to"] or "[]")
        except Exception:
            d["assigned_to"] = []
        result.append(d)
    return result


def dashboard_get_alt_suspects(min_confidence: float = 50.0) -> List[dict]:
    """Подозреваемые мультиаккаунты для дашборда"""
    conn = _db()
    rows = conn.execute("""
        SELECT s.*,
               n1.name as name_orig, n1.banned as banned_orig,
               n2.name as name_alt,  n2.banned as banned_alt
        FROM ti_alt_suspects s
        LEFT JOIN ti_graph_nodes n1 ON s.uid_orig=n1.uid
        LEFT JOIN ti_graph_nodes n2 ON s.uid_alt=n2.uid
        WHERE s.confidence>=? AND s.confirmed=0
        ORDER BY s.confidence DESC
        LIMIT 100
    """, (min_confidence,)).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["signals"] = json.loads(d["signals"] or "{}")
        except Exception:
            d["signals"] = {}
        result.append(d)
    return result


def dashboard_get_stats() -> dict:
    """Общая статистика для дашборда"""
    conn = _db()
    raw = {r["key"]: r["value"] for r in conn.execute(
        "SELECT key, value FROM ti_stats"
    ).fetchall()}

    profiles_count = conn.execute(
        "SELECT COUNT(*) as c FROM ti_profiles"
    ).fetchone()["c"]
    anomalies_open = conn.execute(
        "SELECT COUNT(*) as c FROM ti_anomalies WHERE resolved=0"
    ).fetchone()["c"]
    graph_nodes = conn.execute(
        "SELECT COUNT(*) as c FROM ti_graph_nodes"
    ).fetchone()["c"]
    graph_edges = conn.execute(
        "SELECT COUNT(*) as c FROM ti_graph_edges"
    ).fetchone()["c"]
    alt_high = conn.execute(
        "SELECT COUNT(*) as c FROM ti_alt_suspects WHERE confidence>=70 AND confirmed=0"
    ).fetchone()["c"]
    appeals_pending = conn.execute(
        "SELECT COUNT(*) as c FROM ti_appeals WHERE status='pending'"
    ).fetchone()["c"]
    conn.close()

    return {
        **raw,
        "profiles": profiles_count,
        "anomalies_open": anomalies_open,
        "graph_nodes": graph_nodes,
        "graph_edges": graph_edges,
        "alt_suspects_high": alt_high,
        "appeals_pending": appeals_pending,
    }


# ══════════════════════════════════════════════════════════════════════════
#  ФОНОВЫЕ ЗАДАЧИ
# ══════════════════════════════════════════════════════════════════════════

async def _background_cluster_refresh():
    """Каждые 6 часов пересчитывает кластеры для всех забаненных"""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            conn = _db()
            banned = conn.execute(
                "SELECT uid, ban_cid FROM ti_graph_nodes WHERE banned=1"
            ).fetchall()
            conn.close()

            processed: Set[int] = set()
            for node in banned:
                uid = node["uid"]
                if uid in processed:
                    continue
                cluster = bfs_cluster(uid)
                if len(cluster) > 1:
                    cluster_id = int(time.time()) % 1_000_000
                    _mark_cluster(cluster, cluster_id)
                    processed |= cluster

            log.info(f"[TI] Кластеры пересчитаны, обработано {len(processed)} узлов")
        except Exception as e:
            log.error(f"cluster_refresh error: {e}")


async def _background_appeal_timeout():
    """Каждый час проверяет просроченные апелляции"""
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = int(time.time()) - _APPEAL_TIMEOUT_HOURS * 3600
            conn = _db()
            expired = conn.execute(
                "SELECT id, uid FROM ti_appeals "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,)
            ).fetchall()
            conn.close()

            for row in expired:
                conn = _db()
                conn.execute(
                    "UPDATE ti_appeals SET status='expired', result='timeout' "
                    "WHERE id=?",
                    (row["id"],)
                )
                conn.commit()
                conn.close()
                try:
                    await _bot.send_message(
                        row["uid"],
                        f"⏰ Апелляция #{row['id']} истекла по времени.\n"
                        f"Коллегия не собрала кворум в течение "
                        f"{_APPEAL_TIMEOUT_HOURS} часов."
                    )
                except Exception:
                    pass

            if expired:
                log.info(f"[TI] Закрыто {len(expired)} просроченных апелляций")
        except Exception as e:
            log.error(f"appeal_timeout error: {e}")


async def _background_cache_cleanup():
    """Каждые 24 часа очищает кэш и старые записи"""
    while True:
        await asyncio.sleep(24 * 3600)
        try:
            cutoff_ts = int(time.time()) - 30 * 86400  # 30 дней

            conn = _db()
            # Чистим старые интервалы (старше 30 дней)
            conn.execute("DELETE FROM ti_intervals WHERE ts < ?", (cutoff_ts,))
            # Чистим закрытые аномалии старше 30 дней
            conn.execute(
                "DELETE FROM ti_anomalies WHERE resolved=1 AND ts < ?",
                (cutoff_ts,)
            )
            conn.commit()
            conn.close()

            # Чистим кэш отпечатков (оставляем только забаненных)
            conn = _db()
            banned_uids = {
                r["uid"] for r in conn.execute(
                    "SELECT uid FROM ti_graph_nodes WHERE banned=1"
                ).fetchall()
            }
            conn.close()

            to_remove = [uid for uid in _FINGERPRINT_CACHE if uid not in banned_uids]
            for uid in to_remove:
                del _FINGERPRINT_CACHE[uid]

            log.info(f"[TI] Очистка кэша: удалено {len(to_remove)} отпечатков")
        except Exception as e:
            log.error(f"cache_cleanup error: {e}")


# ══════════════════════════════════════════════════════════════════════════
#  ИНИЦИАЛИЗАЦИЯ
# ══════════════════════════════════════════════════════════════════════════

async def init(bot: Bot, dp: Dispatcher,
               admin_ids: Optional[Set[int]] = None,
               log_channel: int = 0):
    """
    Инициализация модуля.
    Вызывать в main() бота после создания Bot и Dispatcher.
    """
    global _bot, _admin_ids, _log_channel
    _bot = bot
    _admin_ids = admin_ids or set()
    _log_channel = log_channel

    _init_tables()
    _register_handlers(dp)

    asyncio.create_task(_background_cluster_refresh())
    asyncio.create_task(_background_appeal_timeout())
    asyncio.create_task(_background_cache_cleanup())

    log.info("✅ threat_intel.py инициализирован")


def _register_handlers(dp: Dispatcher):
    dp.message.register(cmd_threatmap,  Command("threatmap"))
    dp.message.register(cmd_alts,       Command("alts"))
    dp.message.register(cmd_trustscore, Command("trustscore"))
    dp.message.register(cmd_appeal,     Command("appeal"))
    dp.message.register(cmd_tistat,     Command("tistat"))
    dp.callback_query.register(cb_ti,   F.data.startswith("ti:"))
    log.info("✅ threat_intel.py хендлеры зарегистрированы")


# ══════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ
# ══════════════════════════════════════════════════════════════════════════

async def _send_log(text: str, kb: Optional[InlineKeyboardMarkup] = None):
    if not _bot:
        return
    if _log_channel:
        try:
            await _bot.send_message(
                _log_channel, text, parse_mode="HTML", reply_markup=kb
            )
        except Exception as e:
            log.warning(f"[TI] log_channel send error: {e}")
    for aid in _admin_ids:
        try:
            await _bot.send_message(aid, text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass

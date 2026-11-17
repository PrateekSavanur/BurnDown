#!/usr/bin/env python3
"""
burndown: a personal sprint board with a Telegram accountability bot.

Single process, Python standard library only:
  * HTTP server  -> board UI + JSON API
  * poller       -> Telegram commands (/today, /done 12, ...)
  * scheduler    -> daily digests of what's remaining

Accountability rules enforced by the API (not just the UI):
  * Postponing a due date, or pulling an open item out of a running sprint,
    is a *spill* and needs a written reason.
  * Closing a sprint forces a decision + reason for every unfinished item.
  * Items with history can't be deleted, only dropped with a reason.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import json
import mimetypes
import os
import re
import sqlite3
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from html import escape as h
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


BASE = Path(__file__).resolve().parent
STATIC = (BASE / "static").resolve()
HOST = env("HOST", "0.0.0.0")
PORT = int(env("PORT", "8088"))
DB_PATH = Path(env("DB_PATH", str(BASE / "data" / "burndown.db")))
TZ = ZoneInfo(env("TIMEZONE", "Asia/Kolkata"))
BOARD_PASSWORD = env("BOARD_PASSWORD")
BOARD_URL = env("BOARD_URL").rstrip("/")
TG_TOKEN = env("TELEGRAM_BOT_TOKEN")
TG_CHAT_ENV = env("TELEGRAM_CHAT_ID")
DEFAULT_ALERT_TIMES = env("ALERT_TIMES", "09:00,21:00")

OPEN_STATES = ("todo", "doing", "blocked")
EDITABLE_STATES = OPEN_STATES + ("done",)
CATEGORIES = ("underestimated", "blocked", "low-energy", "priority-shift", "forgot", "external", "other")
SPRINT_DAYS = 14
DEFAULT_JOURNAL_AREAS = ["Work & study", "Health & body", "People", "Home & chores", "Fun & hobbies", "Mind & mood"]
DEFAULT_JOURNAL_TAGS = ["exercised", "outdoors", "slept badly", "late night", "meds taken", "skipped meals", "ate well",
                        "doom-scrolled", "hyperfocused", "procrastinated", "overwhelmed", "anxious", "family time",
                        "friends", "alone time", "argued", "chores done", "creative", "learned something"]
SEED_AREAS = [("CKA", "#326ce5"), ("Homelab", "#22c55e"), ("Singing", "#e0569b"),
              ("Career", "#f59e0b"), ("Work", "#8b5cf6")]


def now() -> datetime:
    return datetime.now(TZ)


def today() -> date:
    return now().date()


def now_iso() -> str:
    return now().isoformat(timespec="seconds")


class ApiError(Exception):
    def __init__(self, status: int, msg: str):
        super().__init__(msg)
        self.status, self.msg = status, msg


def parse_date(value, field="date") -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        raise ApiError(400, f"Invalid {field}: {value!r}")


# --------------------------------------------------------------------------- db

SCHEMA = """
CREATE TABLE IF NOT EXISTS areas(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  color TEXT NOT NULL DEFAULT '#8b5cf6'
);
CREATE TABLE IF NOT EXISTS sprints(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  goal TEXT NOT NULL DEFAULT '',
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  closed_at TEXT
);
CREATE TABLE IF NOT EXISTS items(
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  area_id INTEGER REFERENCES areas(id) ON DELETE SET NULL,
  sprint_id INTEGER REFERENCES sprints(id) ON DELETE SET NULL,
  state TEXT NOT NULL DEFAULT 'todo',
  priority INTEGER NOT NULL DEFAULT 2,
  due_date TEXT,
  target_date TEXT,
  estimate_hours REAL,
  spent_hours REAL,
  evidence TEXT NOT NULL DEFAULT '',
  spill_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  done_at TEXT,
  dropped_at TEXT
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY,
  item_id INTEGER,
  sprint_id INTEGER,
  ts TEXT NOT NULL,
  kind TEXT NOT NULL,
  from_val TEXT,
  to_val TEXT,
  category TEXT,
  note TEXT,
  source TEXT NOT NULL DEFAULT 'board'
);
CREATE INDEX IF NOT EXISTS events_item ON events(item_id);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS journal(
  day TEXT PRIMARY KEY,
  rating INTEGER,
  focus INTEGER,
  energy INTEGER,
  sleep REAL,
  areas TEXT NOT NULL DEFAULT '{}',
  tags TEXT NOT NULL DEFAULT '[]',
  win TEXT NOT NULL DEFAULT '',
  derail TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.executescript(SCHEMA)
            cols = {r[1] for r in self.conn.execute("PRAGMA table_info(items)")}
            if "target_date" not in cols:  # soft "aim for the week of", not a deadline
                self.conn.execute("ALTER TABLE items ADD COLUMN target_date TEXT")
            for c in ("estimate_hours", "spent_hours"):
                if c not in cols:
                    self.conn.execute(f"ALTER TABLE items ADD COLUMN {c} REAL")
        if not self.get_setting("seeded"):
            with self.tx():
                for name, color in SEED_AREAS:
                    self.x("INSERT OR IGNORE INTO areas(name, color) VALUES(?, ?)", (name, color))
                self.set_setting("seeded", "1")

    def q(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        rows = self.q(sql, args)
        return rows[0] if rows else None

    def x(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).lastrowid

    @contextmanager
    def tx(self):
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise

    def get_setting(self, key, default=None):
        row = self.one("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else default

    def set_setting(self, key, value):
        self.x("INSERT INTO settings(key, value) VALUES(?, ?) "
               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


db: DB = None  # type: ignore[assignment]
BOT: "Telegram | None" = None


# ---------------------------------------------------------------- domain logic

def log_event(item_id, kind, *, sprint_id=None, from_val=None, to_val=None,
              category=None, note=None, source="board"):
    db.x("INSERT INTO events(item_id, sprint_id, ts, kind, from_val, to_val, category, note, source) "
         "VALUES(?,?,?,?,?,?,?,?,?)",
         (item_id, sprint_id, now_iso(), kind, from_val, to_val, category, note, source))


def get_item(item_id):
    it = db.one("SELECT * FROM items WHERE id=?", (item_id,))
    if not it:
        raise ApiError(404, f"Item #{item_id} not found")
    return it


def get_sprint(sprint_id):
    sp = db.one("SELECT * FROM sprints WHERE id=?", (sprint_id,))
    if not sp:
        raise ApiError(404, f"Sprint {sprint_id} not found")
    return sp


def sprint_started(sp) -> bool:
    return bool(sp) and date.fromisoformat(sp["start_date"]) <= today()


def active_sprint():
    return db.one("SELECT * FROM sprints WHERE closed_at IS NULL AND start_date <= ? "
                  "ORDER BY start_date DESC LIMIT 1", (today().isoformat(),))


def effective_due(it, sprint=None) -> date | None:
    if it["due_date"]:
        return date.fromisoformat(it["due_date"])
    if it["sprint_id"]:
        sp = sprint or db.one("SELECT end_date FROM sprints WHERE id=?", (it["sprint_id"],))
        if sp:
            return date.fromisoformat(sp["end_date"])
    return None


def is_overdue(it, sprint=None) -> bool:
    d = effective_due(it, sprint)
    return it["state"] in OPEN_STATES and d is not None and d < today()


def sprint_label(sprint_id):
    if not sprint_id:
        return "Backlog"
    sp = db.one("SELECT name FROM sprints WHERE id=?", (sprint_id,))
    return sp["name"] if sp else f"sprint {sprint_id}"


def check_area(area_id):
    if area_id in (None, ""):
        return None
    area_id = int(area_id)
    if not db.one("SELECT id FROM areas WHERE id=?", (area_id,)):
        raise ApiError(400, "Unknown area")
    return area_id


def check_sprint_target(sprint_id):
    if sprint_id in (None, ""):
        return None
    sp = get_sprint(int(sprint_id))
    if sp["closed_at"]:
        raise ApiError(400, f"{sp['name']} is closed")
    return sp["id"]


def check_priority(p):
    try:
        p = int(p)
    except (TypeError, ValueError):
        raise ApiError(400, "Priority must be 1, 2 or 3")
    if p not in (1, 2, 3):
        raise ApiError(400, "Priority must be 1, 2 or 3")
    return p


def check_hours(v, field):
    if v in (None, ""):
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        raise ApiError(400, f"{field} must be a number of hours, like 1.5")
    if not 0 <= v <= 500:
        raise ApiError(400, f"{field} must be between 0 and 500 hours")
    return round(v * 4) / 4  # quarter hours


def create_item(data, source="board"):
    title = str(data.get("title") or "").strip()
    if not title:
        raise ApiError(400, "Title is required")
    state = data.get("state") or "todo"
    if state not in EDITABLE_STATES:
        raise ApiError(400, "Invalid state")
    sprint_id = check_sprint_target(data.get("sprint_id"))
    due = parse_date(data.get("due_date"), "due date")
    target = parse_date(data.get("target_date"), "target week")
    ts = now_iso()
    with db.tx():
        item_id = db.x(
            "INSERT INTO items(title, description, area_id, sprint_id, state, priority, due_date, target_date, "
            "estimate_hours, spent_hours, evidence, created_at, updated_at, done_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (title, str(data.get("description") or "").strip(), check_area(data.get("area_id")),
             sprint_id, state, check_priority(data.get("priority", 2)),
             due.isoformat() if due else None, target.isoformat() if target else None,
             check_hours(data.get("estimate_hours"), "Estimate"), check_hours(data.get("spent_hours"), "Hours spent"),
             str(data.get("evidence") or "").strip(),
             ts, ts, ts if state == "done" else None))
        log_event(item_id, "created", sprint_id=sprint_id, to_val=sprint_label(sprint_id), source=source)
    return get_item(item_id)


def update_item(item_id, data, source="board"):
    it = get_item(item_id)
    if it["state"] == "dropped":
        raise ApiError(400, "Dropped items are read-only")
    cur_sprint = db.one("SELECT * FROM sprints WHERE id=?", (it["sprint_id"],)) if it["sprint_id"] else None
    locked = cur_sprint is not None and sprint_started(cur_sprint)  # a running commitment
    sets, edited = {}, []

    for f in ("title", "description", "evidence"):
        if f in data:
            v = str(data[f] or "").strip()
            if f == "title" and not v:
                raise ApiError(400, "Title is required")
            if v != it[f]:
                sets[f] = v
                edited.append(f)
    if "area_id" in data:
        a = check_area(data["area_id"])
        if a != it["area_id"]:
            sets["area_id"] = a
            edited.append("area")
    if "priority" in data:
        p = check_priority(data["priority"])
        if p != it["priority"]:
            sets["priority"] = p
            edited.append("priority")
    if "due_date" in data:
        new = parse_date(data["due_date"], "due date")
        old = parse_date(it["due_date"])
        if new != old:
            if locked and it["state"] in OPEN_STATES and old and (new is None or new > old):
                raise ApiError(409, "Postponing a deadline is a spill. Use “Spill” and write the reason.")
            sets["due_date"] = new.isoformat() if new else None
            edited.append("due date")
    for f, label, tag in (("estimate_hours", "Estimate", "estimate"), ("spent_hours", "Hours spent", "hours spent")):
        if f in data:
            v = check_hours(data[f], label)
            if v != it[f]:
                sets[f] = v
                edited.append(tag)
    if "target_date" in data:  # soft plan, never a spill
        new = parse_date(data["target_date"], "target week")
        if (new.isoformat() if new else None) != it["target_date"]:
            sets["target_date"] = new.isoformat() if new else None
            edited.append("target week")
    new_sprint = it["sprint_id"]
    if "sprint_id" in data:
        new_sprint = check_sprint_target(data["sprint_id"])
        if new_sprint != it["sprint_id"]:
            if locked:
                raise ApiError(409, "Pulling an item out of a running sprint is a spill. Use “Spill” and write the reason.")
            sets["sprint_id"] = new_sprint
    new_state = it["state"]
    if "state" in data and data["state"] != it["state"]:
        new_state = data["state"]
        if new_state not in EDITABLE_STATES:
            raise ApiError(400, "Invalid state (use Drop to drop an item)")
        sets["state"] = new_state
        sets["done_at"] = now_iso() if new_state == "done" else None

    if not sets:
        return it
    sets["updated_at"] = now_iso()
    with db.tx():
        cols = ", ".join(f"{k}=?" for k in sets)
        db.x(f"UPDATE items SET {cols} WHERE id=?", (*sets.values(), item_id))
        sid = sets.get("sprint_id", it["sprint_id"])
        if new_state != it["state"]:
            log_event(item_id, "moved", sprint_id=sid, from_val=it["state"], to_val=new_state, source=source)
        if new_sprint != it["sprint_id"]:
            log_event(item_id, "planned", sprint_id=sid, from_val=sprint_label(it["sprint_id"]),
                      to_val=sprint_label(new_sprint), source=source)
        if edited:
            log_event(item_id, "edited", sprint_id=sid, note=", ".join(edited), source=source)
    return get_item(item_id)


def new_sprint_row(start: date, days=SPRINT_DAYS, name=None, goal=""):
    n = db.one("SELECT COUNT(*) AS c FROM sprints")["c"] + 1
    end = start + timedelta(days=days - 1)
    sid = db.x("INSERT INTO sprints(name, goal, start_date, end_date) VALUES(?,?,?,?)",
               (name or f"Sprint {n}", goal, start.isoformat(), end.isoformat()))
    log_event(None, "sprint_created", sprint_id=sid, note=f"{start.isoformat()} → {end.isoformat()}")
    return get_sprint(sid)


def next_sprint_after(sp):
    """Next open sprint after `sp` (or after the active one); created if missing."""
    base = sp or active_sprint()
    if not base:
        nxt = db.one("SELECT * FROM sprints WHERE closed_at IS NULL AND start_date > ? "
                     "ORDER BY start_date LIMIT 1", (today().isoformat(),))
        return nxt or new_sprint_row(today())
    nxt = db.one("SELECT * FROM sprints WHERE closed_at IS NULL AND start_date > ? AND id != ? "
                 "ORDER BY start_date LIMIT 1", (base["start_date"], base["id"]))
    if nxt:
        return nxt
    start = max(date.fromisoformat(base["end_date"]) + timedelta(days=1), today())
    return new_sprint_row(start)


def clean_reason(data):
    reason = str(data.get("reason") or "").strip()
    if len(reason) < 3:
        raise ApiError(400, "Write the actual reason. It goes into the ledger.")
    category = data.get("category") or "other"
    if category not in CATEGORIES:
        category = "other"
    return reason, category


def _spill(it, action, reason, category, new_due=None, source="board"):
    """Apply a spill. Caller holds a transaction."""
    sp = db.one("SELECT * FROM sprints WHERE id=?", (it["sprint_id"],)) if it["sprint_id"] else None
    old_due = effective_due(it, sp)
    from_val = f"{sprint_label(it['sprint_id'])} · due {old_due.isoformat() if old_due else '—'}"
    if action == "reschedule":
        nd = parse_date(new_due, "new due date")
        if not nd or nd < today():
            raise ApiError(400, "Pick a new due date from today onwards")
        if sp and nd > date.fromisoformat(sp["end_date"]):
            raise ApiError(400, "That's past the sprint end. Move it to the next sprint instead.")
        sprint_id, due, to_val = it["sprint_id"], nd.isoformat(), f"due {nd.isoformat()}"
    elif action == "next_sprint":
        nxt = next_sprint_after(sp)
        sprint_id, due, to_val = nxt["id"], None, nxt["name"]
    elif action == "backlog":
        sprint_id, due, to_val = None, None, "Backlog"
    else:
        raise ApiError(400, "Choose what happens next: reschedule, next sprint or backlog")
    db.x("UPDATE items SET sprint_id=?, due_date=?, spill_count=spill_count+1, updated_at=? WHERE id=?",
         (sprint_id, due, now_iso(), it["id"]))
    log_event(it["id"], "spilled", sprint_id=it["sprint_id"], from_val=from_val, to_val=to_val,
              category=category, note=reason, source=source)


def _drop(it, reason, category, source="board"):
    db.x("UPDATE items SET state='dropped', dropped_at=?, updated_at=? WHERE id=?",
         (now_iso(), now_iso(), it["id"]))
    log_event(it["id"], "dropped", sprint_id=it["sprint_id"], from_val=it["state"],
              category=category, note=reason, source=source)


def spill_item(item_id, data):
    it = get_item(item_id)
    if it["state"] not in OPEN_STATES:
        raise ApiError(400, "Only open items can be spilled")
    reason, category = clean_reason(data)
    with db.tx():
        _spill(it, data.get("action"), reason, category, data.get("new_due"))
    return get_item(item_id)


def drop_item(item_id, data):
    it = get_item(item_id)
    if it["state"] not in OPEN_STATES:
        raise ApiError(400, "Only open items can be dropped")
    reason, category = clean_reason(data)
    with db.tx():
        _drop(it, reason, category)
    return get_item(item_id)


def delete_item(item_id):
    it = get_item(item_id)
    created = datetime.fromisoformat(it["created_at"])
    if it["spill_count"] or it["state"] == "done" or is_overdue(it) or now() - created > timedelta(hours=24):
        raise ApiError(409, "This item has history. Drop it with a reason instead; the ledger keeps everything.")
    with db.tx():
        db.x("DELETE FROM events WHERE item_id=?", (item_id,))
        db.x("DELETE FROM items WHERE id=?", (item_id,))


def overlapping(start: date, end: date, exclude=None):
    return db.one("SELECT name FROM sprints WHERE closed_at IS NULL AND id != ? "
                  "AND start_date <= ? AND end_date >= ?",
                  (exclude or -1, end.isoformat(), start.isoformat()))


def create_sprint(data):
    act = active_sprint()
    start = parse_date(data.get("start_date"), "start date") or (
        date.fromisoformat(act["end_date"]) + timedelta(days=1) if act else today())
    end = parse_date(data.get("end_date"), "end date") or start + timedelta(days=SPRINT_DAYS - 1)
    if end < start:
        raise ApiError(400, "Sprint ends before it starts")
    if (clash := overlapping(start, end)):
        raise ApiError(409, f"Overlaps with {clash['name']}")
    with db.tx():
        sp = new_sprint_row(start, (end - start).days + 1,
                            str(data.get("name") or "").strip() or None, str(data.get("goal") or "").strip())
    return sp


def update_sprint(sprint_id, data):
    sp = get_sprint(sprint_id)
    if sp["closed_at"]:
        raise ApiError(400, "Closed sprints are read-only")
    sets = {}
    if "name" in data and str(data["name"]).strip():
        sets["name"] = str(data["name"]).strip()
    if "goal" in data:
        sets["goal"] = str(data["goal"] or "").strip()
    start = parse_date(data.get("start_date")) or date.fromisoformat(sp["start_date"])
    end = parse_date(data.get("end_date")) or date.fromisoformat(sp["end_date"])
    if start.isoformat() != sp["start_date"] or end.isoformat() != sp["end_date"]:
        if sprint_started(sp):
            raise ApiError(409, "Dates of a running sprint are frozen. Spill items instead of stretching the sprint.")
        if end < start:
            raise ApiError(400, "Sprint ends before it starts")
        if (clash := overlapping(start, end, sp["id"])):
            raise ApiError(409, f"Overlaps with {clash['name']}")
        sets["start_date"], sets["end_date"] = start.isoformat(), end.isoformat()
    if sets:
        cols = ", ".join(f"{k}=?" for k in sets)
        db.x(f"UPDATE sprints SET {cols} WHERE id=?", (*sets.values(), sprint_id))
    return get_sprint(sprint_id)


def close_sprint(sprint_id, data):
    sp = get_sprint(sprint_id)
    if sp["closed_at"]:
        raise ApiError(400, "Already closed")
    items = db.q("SELECT * FROM items WHERE sprint_id=? AND state != 'dropped'", (sprint_id,))
    open_items = [i for i in items if i["state"] in OPEN_STATES]
    decisions = {}
    for d in data.get("decisions") or []:
        try:
            decisions[int(d.get("item_id"))] = d
        except (TypeError, ValueError):
            continue
    missing = [i["id"] for i in open_items if i["id"] not in decisions]
    if missing:
        raise ApiError(400, f"Decide what happens to {len(missing)} unfinished item(s) first")
    parsed = {}
    for i in open_items:
        d = decisions[i["id"]]
        if d.get("action") not in ("next_sprint", "backlog", "drop"):
            raise ApiError(400, f"#{i['id']}: choose next sprint, backlog or drop")
        try:
            parsed[i["id"]] = (d["action"], *clean_reason(d))
        except ApiError as e:
            raise ApiError(400, f"#{i['id']}: {e.msg}")
    done = sum(1 for i in items if i["state"] == "done")
    spilled = dropped = 0
    next_id = None
    with db.tx():
        for i in open_items:
            action, reason, category = parsed[i["id"]]
            if action == "drop":
                _drop(i, reason, category)
                dropped += 1
            else:
                _spill(i, action, reason, category)
                spilled += 1
                if action == "next_sprint":
                    next_id = db.one("SELECT sprint_id FROM items WHERE id=?", (i["id"],))["sprint_id"]
        db.x("UPDATE sprints SET closed_at=? WHERE id=?", (now_iso(), sprint_id))
        log_event(None, "sprint_closed", sprint_id=sprint_id,
                  note=f"{done}/{len(items)} done · {spilled} spilled · {dropped} dropped")
    return {"closed": get_sprint(sprint_id), "next_sprint_id": next_id}


def alert_times():
    raw = db.get_setting("alert_times") or DEFAULT_ALERT_TIMES
    out = []
    for t in raw.split(","):
        t = t.strip()
        if re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", t):
            out.append(t)
    return sorted(set(out))


def mid_check_date(sp) -> date | None:
    start, end = date.fromisoformat(sp["start_date"]), date.fromisoformat(sp["end_date"])
    d = start + timedelta(days=5)
    while d.weekday() != 5:  # first Saturday at least 5 days in
        d += timedelta(days=1)
    return d if d < end else None


def full_state():
    t = today()
    spills = {r["sprint_id"]: r["c"] for r in db.q(
        "SELECT sprint_id, COUNT(*) AS c FROM events WHERE kind='spilled' GROUP BY sprint_id")}
    sprints = db.q("SELECT * FROM sprints ORDER BY start_date DESC")
    for sp in sprints:
        sp["spills"] = spills.get(sp["id"], 0)
        m = mid_check_date(sp)
        sp["mid_check"] = m.isoformat() if m else None
    act = active_sprint()
    return {
        "today": t.isoformat(),
        "now": now_iso(),
        "areas": db.q("SELECT * FROM areas ORDER BY id"),
        "sprints": sprints,
        "active_sprint_id": act["id"] if act else None,
        "items": db.q("SELECT * FROM items ORDER BY id"),
        "categories": CATEGORIES,
        "settings": {
            "alert_times": alert_times(),
            "telegram_configured": bool(TG_TOKEN),
            "telegram_linked": bool(TG_CHAT_ENV or db.get_setting("telegram_chat_id")),
            "telegram_bot": db.get_setting("telegram_bot_username"),
            "timezone": str(TZ),
        },
    }


# -------------------------------------------------------------------- telegram

def item_line(i, sprint=None):
    d = effective_due(i, sprint)
    tag = ""
    if d:
        delta = (d - today()).days
        tag = (f" · <b>{-delta}d late</b>" if delta < 0 else " · <b>today</b>" if delta == 0
               else " · tomorrow" if delta == 1 else f" · {d.strftime('%a %d %b')}")
    area = f" <i>[{h(i['area_name'])}]</i>" if i.get("area_name") else ""
    spill = f" ↻{i['spill_count']}" if i["spill_count"] else ""
    return f"<code>#{i['id']}</code> {h(i['title'])}{area}{tag}{spill}"


def board_link():
    return f"\n\n<a href=\"{h(BOARD_URL)}\">Open the board →</a>" if BOARD_URL else ""


def build_digest(kind="check"):
    t = today()
    sp = active_sprint()
    heads = {"morning": "☀️ <b>Morning plan</b>", "evening": "🌙 <b>Evening check</b>", "check": "⏱ <b>Check-in</b>"}
    out = [heads.get(kind, heads["check"])]
    q = ("SELECT i.*, a.name AS area_name FROM items i LEFT JOIN areas a ON a.id=i.area_id "
         "WHERE i.state IN ('todo','doing','blocked') AND (i.sprint_id=? OR (i.sprint_id IS NULL AND i.due_date IS NOT NULL)) "
         "ORDER BY i.priority, i.id")
    if not sp:
        upcoming = db.one("SELECT * FROM sprints WHERE closed_at IS NULL ORDER BY start_date LIMIT 1")
        if upcoming:
            out.append(f"No sprint running. <b>{h(upcoming['name'])}</b> starts "
                       f"{date.fromisoformat(upcoming['start_date']).strftime('%a %d %b')}.")
        else:
            out.append("No sprint running. Plan the next two weeks on the board (≈45 min).")
        loose = [i for i in db.q(q, (-1,)) if is_overdue(i)]
        if loose:
            out.append("\n🔴 <b>Overdue backlog items</b>")
            out += [item_line(i) for i in loose]
        return "\n".join(out) + board_link()

    start, end = date.fromisoformat(sp["start_date"]), date.fromisoformat(sp["end_date"])
    all_in = db.q("SELECT state FROM items WHERE sprint_id=? AND state != 'dropped'", (sp["id"],))
    done = sum(1 for i in all_in if i["state"] == "done")
    total = len(all_in)
    day, length = (t - start).days + 1, (end - start).days + 1
    pct = round(100 * done / total) if total else 0
    bar = "█" * round(pct / 10) + "░" * (10 - round(pct / 10))
    out.append(f"<b>{h(sp['name'])}</b> · day {min(day, length)}/{length}\n{bar} {done}/{total} done ({pct}%)")

    open_items = db.q(q, (sp["id"],))
    overdue, due_today, active, rest = [], [], [], []
    for i in open_items:
        d = effective_due(i, sp if i["sprint_id"] == sp["id"] else None)
        if d and d < t:
            overdue.append(i)
        elif d == t:
            due_today.append(i)
        elif i["state"] in ("doing", "blocked"):
            active.append(i)
        else:
            rest.append(i)

    def section(icon, title, rows, limit=12):
        if not rows:
            return
        out.append(f"\n{icon} <b>{title}</b> ({len(rows)})")
        out.extend(item_line(i, sp) for i in rows[:limit])
        if len(rows) > limit:
            out.append(f"…and {len(rows) - limit} more")

    if not open_items:
        out.append("\n✅ Nothing remaining. Clean sprint so far." if total else
                   "\nThis sprint is empty. Add your commitments on the board.")
    section("🔴", "Slipped: needs a reason on the board", overdue)
    section("🟡", "Due today", due_today)
    section("🔵", "In progress / blocked", active)
    section("⚪", "Remaining", rest, limit=8 if kind == "evening" else 12)

    if kind == "evening" and due_today:
        out.append("\nFinish today's items or spill them with a reason before midnight.")
    if overdue:
        out.append("Slipped items keep showing up here until you give a reason.")
    if t == end:
        out.append("\n🏁 <b>Sprint ends today.</b> Close it and plan the next one (≈45 min).")
    elif t > end:
        out.append(f"\n⚠️ <b>Sprint ended {(t - end).days}d ago and isn't closed.</b> Close it on the board.")
    elif mid_check_date(sp) == t:
        out.append("\n🧭 <b>Mid-sprint check today</b>: 15 min to adjust next week's milestones.")
    return "\n".join(out) + board_link()


HELP = ("<b>burndown bot</b>\n"
        "/today: what's remaining in this sprint\n"
        "/overdue: slipped items\n"
        "/sprint: sprint progress\n"
        "/doing 12: start item #12\n"
        "/done 12: mark item #12 done\n"
        "Reasons for spills go on the board.")


class Telegram:
    def __init__(self, token):
        self.base = f"https://api.telegram.org/bot{token}/"

    def call(self, method, payload=None, timeout=20):
        req = urllib.request.Request(self.base + method, data=json.dumps(payload or {}).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                res = json.load(r)
        except urllib.error.HTTPError as e:
            try:
                desc = json.load(e).get("description")
            except Exception:
                desc = str(e)
            raise RuntimeError(f"telegram {method}: {desc}") from None
        if not res.get("ok"):
            raise RuntimeError(f"telegram {method}: {res.get('description')}")
        return res["result"]

    def chat_id(self):
        return TG_CHAT_ENV or db.get_setting("telegram_chat_id")

    def send(self, text, chat_id=None):
        cid = chat_id or self.chat_id()
        if not cid:
            return False
        chunks, buf = [], ""
        for line in text.split("\n"):
            if len(buf) + len(line) + 1 > 3900:
                chunks.append(buf)
                buf = ""
            buf += line + "\n"
        chunks.append(buf)
        for c in chunks:
            if c.strip():
                self.call("sendMessage", {"chat_id": cid, "text": c, "parse_mode": "HTML",
                                          "disable_web_page_preview": True})
        return True

    def setup(self):
        try:
            me = self.call("getMe")
            db.set_setting("telegram_bot_username", me.get("username", ""))
            self.call("setMyCommands", {"commands": [
                {"command": "today", "description": "What's remaining"},
                {"command": "overdue", "description": "Slipped items"},
                {"command": "sprint", "description": "Sprint progress"},
                {"command": "doing", "description": "Start an item: /doing 12"},
                {"command": "done", "description": "Finish an item: /done 12"},
                {"command": "help", "description": "Commands"}]})
            print(f"telegram: connected as @{me.get('username')}", flush=True)
        except Exception as e:
            print(f"telegram: setup failed: {e}", flush=True)

    def poll_forever(self):
        self.setup()
        offset = None
        while True:
            try:
                payload = {"timeout": 50, "allowed_updates": ["message"]}
                if offset is not None:
                    payload["offset"] = offset
                for u in self.call("getUpdates", payload, timeout=65):
                    offset = u["update_id"] + 1
                    if "message" in u:
                        try:
                            self.handle(u["message"])
                        except Exception:
                            traceback.print_exc()
            except Exception as e:
                print(f"telegram poll: {e}", flush=True)
                time.sleep(10)

    def handle(self, msg):
        chat = str(msg["chat"]["id"])
        text = (msg.get("text") or "").strip()
        linked = self.chat_id()
        if not linked:
            if text.startswith("/start"):
                db.set_setting("telegram_chat_id", chat)
                self.send("🔗 Linked. This chat now gets your daily alerts.\n\n" + HELP, chat)
            else:
                self.send("Send /start to link this chat to your board.", chat)
            return
        if chat != str(linked):
            return  # ignore everyone else
        cmd, _, arg = text.partition(" ")
        cmd = cmd.split("@")[0].lower()
        if cmd in ("/start", "/help"):
            self.send(HELP)
        elif cmd in ("/today", "/remaining"):
            self.send(build_digest("check"))
        elif cmd == "/overdue":
            rows = [i for i in db.q("SELECT i.*, a.name AS area_name FROM items i LEFT JOIN areas a ON a.id=i.area_id "
                                    "WHERE i.state IN ('todo','doing','blocked')") if is_overdue(i)]
            self.send("🔴 <b>Slipped</b>\n" + "\n".join(item_line(i) for i in rows) + board_link()
                      if rows else "Nothing overdue. 👌")
        elif cmd == "/sprint":
            self.send(build_digest("check").split("\n\n")[0])
        elif cmd in ("/done", "/doing"):
            m = re.search(r"\d+", arg)
            if not m:
                self.send(f"Usage: {cmd} 12")
                return
            try:
                it = update_item(int(m.group()), {"state": cmd[1:]}, source="telegram")
                self.send(("✅ Done: " if cmd == "/done" else "▶️ Started: ") +
                          f"<code>#{it['id']}</code> {h(it['title'])}" +
                          ("\nAdd evidence on the board if you have it." if cmd == "/done" and not it["evidence"] else ""))
            except ApiError as e:
                self.send(f"⚠️ {h(e.msg)}")
        else:
            self.send(HELP)


def scheduler_loop(bot: Telegram):
    while True:
        try:
            n = now()
            sent = json.loads(db.get_setting("alerts_sent") or "{}")
            if sent.get("date") != n.date().isoformat():
                sent = {"date": n.date().isoformat(), "times": []}
            times = alert_times()
            for idx, t in enumerate(times):
                if t in sent["times"]:
                    continue
                hh, mm = map(int, t.split(":"))
                target = n.replace(hour=hh, minute=mm, second=0, microsecond=0)
                if target <= n < target + timedelta(minutes=15):
                    kind = "morning" if idx == 0 and len(times) > 1 else "evening" if idx == len(times) - 1 else "check"
                    if bot.send(build_digest(kind)):
                        sent["times"].append(t)
                        db.set_setting("alerts_sent", json.dumps(sent))
        except Exception:
            traceback.print_exc()
        time.sleep(20)


# ------------------------------------------------------------------------ http

ROUTES = []


def route(method, pattern):
    def deco(fn):
        ROUTES.append((method, re.compile(f"^{pattern}$"), fn))
        return fn
    return deco


@route("GET", "/api/state")
def r_state(body, qs):
    return full_state()


@route("GET", "/api/events")
def r_events(body, qs):
    item_id = qs.get("item_id", [None])[0]
    limit = min(int(qs.get("limit", ["400"])[0]), 2000)
    sql = ("SELECT e.*, i.title AS item_title, a.name AS area_name, a.color AS area_color, s.name AS sprint_name "
           "FROM events e LEFT JOIN items i ON i.id=e.item_id LEFT JOIN areas a ON a.id=i.area_id "
           "LEFT JOIN sprints s ON s.id=e.sprint_id ")
    if item_id:
        return db.q(sql + "WHERE e.item_id=? ORDER BY e.id DESC LIMIT ?", (int(item_id), limit))
    return db.q(sql + "ORDER BY e.id DESC LIMIT ?", (limit,))


@route("POST", "/api/items")
def r_item_create(body, qs):
    return create_item(body)


@route("PATCH", r"/api/items/(\d+)")
def r_item_update(item_id, body, qs):
    return update_item(item_id, body)


@route("DELETE", r"/api/items/(\d+)")
def r_item_delete(item_id, body, qs):
    delete_item(item_id)


@route("POST", r"/api/items/(\d+)/spill")
def r_item_spill(item_id, body, qs):
    return spill_item(item_id, body)


@route("POST", r"/api/items/(\d+)/drop")
def r_item_drop(item_id, body, qs):
    return drop_item(item_id, body)


@route("POST", "/api/sprints")
def r_sprint_create(body, qs):
    return create_sprint(body)


@route("PATCH", r"/api/sprints/(\d+)")
def r_sprint_update(sprint_id, body, qs):
    return update_sprint(sprint_id, body)


@route("POST", r"/api/sprints/(\d+)/close")
def r_sprint_close(sprint_id, body, qs):
    return close_sprint(sprint_id, body)


def clean_color(c):
    c = str(c or "").strip()
    if not re.fullmatch(r"#[0-9a-fA-F]{6}", c):
        raise ApiError(400, "Color must look like #a1b2c3")
    return c


@route("POST", "/api/areas")
def r_area_create(body, qs):
    name = str(body.get("name") or "").strip()
    if not name:
        raise ApiError(400, "Name is required")
    try:
        aid = db.x("INSERT INTO areas(name, color) VALUES(?, ?)", (name, clean_color(body.get("color") or "#8b5cf6")))
    except sqlite3.IntegrityError:
        raise ApiError(409, "An area with that name exists")
    return db.one("SELECT * FROM areas WHERE id=?", (aid,))


@route("PATCH", r"/api/areas/(\d+)")
def r_area_update(area_id, body, qs):
    if "name" in body:
        name = str(body["name"] or "").strip()
        if not name:
            raise ApiError(400, "Name is required")
        try:
            db.x("UPDATE areas SET name=? WHERE id=?", (name, area_id))
        except sqlite3.IntegrityError:
            raise ApiError(409, "An area with that name exists")
    if "color" in body:
        db.x("UPDATE areas SET color=? WHERE id=?", (clean_color(body["color"]), area_id))
    return db.one("SELECT * FROM areas WHERE id=?", (area_id,))


@route("DELETE", r"/api/areas/(\d+)")
def r_area_delete(area_id, body, qs):
    db.x("DELETE FROM areas WHERE id=?", (area_id,))


@route("PATCH", "/api/settings")
def r_settings(body, qs):
    if "alert_times" in body:
        raw = body["alert_times"]
        raw = ",".join(raw) if isinstance(raw, list) else str(raw)
        times = [t.strip() for t in raw.split(",") if t.strip()]
        bad = [t for t in times if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", t)]
        if bad:
            raise ApiError(400, f"Use HH:MM (24h): {', '.join(bad)}")
        db.set_setting("alert_times", ",".join(sorted(set(times))))
    return full_state()["settings"]


@route("POST", "/api/telegram/test")
def r_tg_test(body, qs):
    if not BOT:
        raise ApiError(400, "Set TELEGRAM_BOT_TOKEN and restart")
    if not BOT.chat_id():
        raise ApiError(400, "Send /start to your bot first")
    try:
        BOT.send(build_digest(body.get("kind") or "check"))
    except RuntimeError as e:
        raise ApiError(502, str(e))


# -------------------------------------------------------------------- journal
# Lives only on the board: nothing here is sent to Telegram.

def journal_list(key, default):
    try:
        v = json.loads(db.get_setting(key) or "null")
        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            return v
    except ValueError:
        pass
    return list(default)


def clean_names(raw, field, limit):
    if isinstance(raw, str):
        raw = raw.split(",")
    if not isinstance(raw, list):
        raise ApiError(400, f"{field} must be a list")
    out = []
    for x in raw:
        x = str(x).strip()[:40]
        if x and x not in out:
            out.append(x)
    if len(out) > limit:
        raise ApiError(400, f"At most {limit} {field}")
    return out


def journal_row(r):
    r = dict(r)
    r["areas"] = json.loads(r["areas"] or "{}")
    r["tags"] = json.loads(r["tags"] or "[]")
    return r


def get_journal(qs):
    t = today()
    end = parse_date(qs.get("to", [None])[0]) or t
    start = parse_date(qs.get("from", [None])[0]) or end - timedelta(days=370)
    if (end - start).days > 1200:
        raise ApiError(400, "Range too large")
    a, b = start.isoformat(), end.isoformat()
    entries = [journal_row(r) for r in db.q("SELECT * FROM journal WHERE day BETWEEN ? AND ? ORDER BY day", (a, b))]
    stats = {}
    for e in db.q("SELECT substr(e.ts,1,10) AS day, e.kind, e.to_val, e.item_id, i.title FROM events e "
                  "LEFT JOIN items i ON i.id=e.item_id WHERE substr(e.ts,1,10) BETWEEN ? AND ? "
                  "AND (e.kind='spilled' OR (e.kind='moved' AND e.to_val='done'))", (a, b)):
        d = stats.setdefault(e["day"], {"done": [], "spilled": 0})
        if e["kind"] == "spilled":
            d["spilled"] += 1
        elif not any(x["id"] == e["item_id"] for x in d["done"]):
            d["done"].append({"id": e["item_id"], "title": e["title"] or "(deleted)"})
    return {"entries": entries, "stats": stats, "from": a, "to": b,
            "areas": journal_list("journal_areas", DEFAULT_JOURNAL_AREAS),
            "tags": journal_list("journal_tags", DEFAULT_JOURNAL_TAGS)}


def _scale(v, lo, hi, field):
    if v in (None, ""):
        return None
    try:
        v = int(v)
    except (TypeError, ValueError):
        raise ApiError(400, f"{field} must be {lo}–{hi}")
    if not lo <= v <= hi:
        raise ApiError(400, f"{field} must be {lo}–{hi}")
    return v


def save_journal(day: date, data):
    if day > today():
        raise ApiError(400, "Can't journal a day that hasn't happened yet")
    sets = {}
    if "rating" in data:
        sets["rating"] = _scale(data["rating"], 1, 5, "Day rating")
    if "focus" in data:
        sets["focus"] = _scale(data["focus"], 1, 3, "Focus")
    if "energy" in data:
        sets["energy"] = _scale(data["energy"], 1, 3, "Energy")
    if "sleep" in data:
        v = data["sleep"]
        if v in (None, ""):
            sets["sleep"] = None
        else:
            try:
                v = round(float(v), 1)
            except (TypeError, ValueError):
                raise ApiError(400, "Sleep must be hours, like 7 or 6.5")
            if not 0 <= v <= 24:
                raise ApiError(400, "Sleep must be between 0 and 24 hours")
            sets["sleep"] = v
    if "areas" in data:
        raw = data["areas"]
        if not isinstance(raw, dict):
            raise ApiError(400, "areas must be an object")
        sets["areas"] = json.dumps({str(k)[:40]: _scale(v, 1, 3, str(k)) for k, v in list(raw.items())[:30]
                                    if v not in (None, "")})
    if "tags" in data:
        sets["tags"] = json.dumps(clean_names(data["tags"], "tags", 60))
    for f, cap in (("win", 500), ("derail", 500), ("body", 100_000)):
        if f in data:
            v = str(data[f] or "")
            v = v.strip() if f != "body" else v.rstrip()
            if len(v) > cap:
                raise ApiError(400, f"{f} is too long")
            sets[f] = v
    d = day.isoformat()
    ts = now_iso()
    with db.tx():
        if not db.one("SELECT day FROM journal WHERE day=?", (d,)):
            db.x("INSERT INTO journal(day, created_at, updated_at) VALUES(?,?,?)", (d, ts, ts))
        if sets:
            sets["updated_at"] = ts
            db.x(f"UPDATE journal SET {', '.join(f'{k}=?' for k in sets)} WHERE day=?", (*sets.values(), d))
    return journal_row(db.one("SELECT * FROM journal WHERE day=?", (d,)))


@route("GET", "/api/journal")
def r_journal(body, qs):
    return get_journal(qs)


@route("PATCH", r"/api/journal/(\d{4})-(\d{2})-(\d{2})")
def r_journal_save(y, m, d, body, qs):
    try:
        day = date(y, m, d)
    except ValueError:
        raise ApiError(400, "Invalid date")
    return save_journal(day, body)


@route("DELETE", r"/api/journal/(\d{4})-(\d{2})-(\d{2})")
def r_journal_delete(y, m, d, body, qs):
    db.x("DELETE FROM journal WHERE day=?", (f"{y:04d}-{m:02d}-{d:02d}",))


@route("PATCH", "/api/journal-settings")
def r_journal_settings(body, qs):
    if "areas" in body:
        db.set_setting("journal_areas", json.dumps(clean_names(body["areas"], "areas", 12)))
    if "tags" in body:
        db.set_setting("journal_tags", json.dumps(clean_names(body["tags"], "tags", 60)))
    return {"areas": journal_list("journal_areas", DEFAULT_JOURNAL_AREAS),
            "tags": journal_list("journal_tags", DEFAULT_JOURNAL_TAGS)}


# ------------------------------------------------------------------- sessions

SESSION_COOKIE = "bd_session"
SESSION_DAYS = 30
_fail_lock = threading.Lock()
_failures: dict[str, list[float]] = {}


def session_secret() -> bytes:
    sec = db.get_setting("session_secret")
    if not sec:
        sec = secrets.token_hex(32)
        db.set_setting("session_secret", sec)
    # bind sessions to the current password: changing it logs everyone out
    return hashlib.sha256((sec + "|" + BOARD_PASSWORD).encode()).digest()


def make_session() -> str:
    exp = str(int(time.time()) + SESSION_DAYS * 86400)
    sig = hmac.new(session_secret(), exp.encode(), hashlib.sha256).hexdigest()
    return f"{exp}.{sig}"


def valid_session(token: str) -> bool:
    exp, _, sig = token.partition(".")
    if not exp.isdigit() or int(exp) < time.time():
        return False
    good = hmac.new(session_secret(), exp.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, good)


def too_many_failures(ip: str) -> bool:
    with _fail_lock:
        recent = [t for t in _failures.get(ip, []) if t > time.time() - 600]
        _failures[ip] = recent
        return len(recent) >= 10


def record_failure(ip: str):
    with _fail_lock:
        _failures.setdefault(ip, []).append(time.time())


PUBLIC_PATHS = {"/login", "/api/login", "/healthz"}
PWA_FILES = {"/manifest.webmanifest", "/icon-192.png", "/icon-512.png"}


class Handler(BaseHTTPRequestHandler):
    server_version = "burndown/1.0"

    def log_message(self, fmt, *args):
        if env("ACCESS_LOG"):
            super().log_message(fmt, *args)

    def _send(self, status, body: bytes, ctype, extra=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, obj):
        self._send(status, json.dumps(obj).encode(), "application/json; charset=utf-8")

    def _cookie(self, name):
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == name:
                return v
        return ""

    def _authorized(self):
        return not BOARD_PASSWORD or valid_session(self._cookie(SESSION_COOKIE))

    def _secure(self):
        return self.headers.get("X-Forwarded-Proto", "").lower() == "https"

    def _login(self):
        ip = self.headers.get("CF-Connecting-IP") or self.headers.get("X-Real-IP") or self.client_address[0]
        try:
            body = self._body()
        except ApiError as e:
            return self._json(e.status, {"error": e.msg})
        if not BOARD_PASSWORD:
            return self._json(200, {"ok": True})
        if too_many_failures(ip):
            return self._json(429, {"error": "Too many attempts. Try again in a few minutes."})
        if not hmac.compare_digest(str(body.get("password", "")).encode(), BOARD_PASSWORD.encode()):
            record_failure(ip)
            time.sleep(0.8)
            return self._json(401, {"error": "Wrong password."})
        cookie = (f"{SESSION_COOKIE}={make_session()}; Path=/; Max-Age={SESSION_DAYS * 86400}; "
                  f"HttpOnly; SameSite=Lax" + ("; Secure" if self._secure() else ""))
        body = json.dumps({"ok": True}).encode()
        self._send(200, body, "application/json; charset=utf-8", {"Set-Cookie": cookie})

    def _logout(self):
        self._send(200, b'{"ok": true}', "application/json; charset=utf-8",
                   {"Set-Cookie": f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"})

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 1_000_000:
            raise ApiError(413, "Body too large")
        if "application/json" not in (self.headers.get("Content-Type") or ""):
            raise ApiError(415, "Send JSON")
        try:
            data = json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError:
            raise ApiError(400, "Invalid JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "Expected a JSON object")
        return data

    def _static(self, path):
        f = STATIC / "index.html" if path in ("/", "/index.html") else (STATIC / path.lstrip("/")).resolve()
        if not f.is_file() or STATIC not in f.parents:
            return self._send(404, b"not found", "text/plain")
        ctype = ("application/manifest+json" if f.suffix == ".webmanifest"
                 else mimetypes.guess_type(f.name)[0] or "application/octet-stream")
        if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
            ctype += "; charset=utf-8"
        self._send(200, f.read_bytes(), ctype)

    def _dispatch(self, method):
        url = urllib.parse.urlsplit(self.path)
        if url.path == "/healthz":
            return self._send(200, b"ok", "text/plain")
        if url.path == "/login" and method == "GET":
            if BOARD_PASSWORD and self._authorized():
                return self._send(302, b"", "text/plain", {"Location": "/"})
            return self._send(200, (STATIC / "login.html").read_bytes(), "text/html; charset=utf-8")
        if url.path == "/api/login" and method == "POST":
            return self._login()
        if url.path == "/api/logout" and method == "POST":
            return self._logout()
        if method == "GET" and url.path in PWA_FILES:
            return self._static(url.path)
        if not self._authorized():
            if url.path.startswith("/api/"):
                return self._json(401, {"error": "Signed out", "login": True})
            return self._send(302, b"", "text/plain", {"Location": "/login"})
        if method == "GET" and not url.path.startswith("/api/"):
            return self._static(url.path)
        qs = urllib.parse.parse_qs(url.query)
        for m, rx, fn in ROUTES:
            if m != method:
                continue
            match = rx.match(url.path)
            if not match:
                continue
            try:
                body = self._body() if method in ("POST", "PATCH") else {}
                res = fn(*(int(g) for g in match.groups()), body=body, qs=qs)
                return self._json(200, {"ok": True} if res is None else res)
            except ApiError as e:
                return self._json(e.status, {"error": e.msg})
            except Exception:
                traceback.print_exc()
                return self._json(500, {"error": "Internal error. Check the server logs."})
        self._json(404, {"error": "Not found"})

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PATCH(self):
        self._dispatch("PATCH")

    def do_DELETE(self):
        self._dispatch("DELETE")


def main():
    global db, BOT
    db = DB(DB_PATH)
    if TG_TOKEN:
        BOT = Telegram(TG_TOKEN)
        threading.Thread(target=BOT.poll_forever, daemon=True, name="tg-poll").start()
        threading.Thread(target=scheduler_loop, args=(BOT,), daemon=True, name="scheduler").start()
        print(f"alerts at {', '.join(alert_times())} ({TZ})", flush=True)
    else:
        print("telegram: disabled (no TELEGRAM_BOT_TOKEN)", flush=True)
    if not BOARD_PASSWORD:
        print("warning: BOARD_PASSWORD not set, the board is open to anyone who can reach it", flush=True)
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"burndown listening on http://{HOST}:{PORT}  db={DB_PATH}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

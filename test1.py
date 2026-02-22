"""
Secret Chat Room Bot
────────────────────────────────────────────────────────────────────────────
Features added in this version:
  • Persistent storage  — SQLite via sqlite3 (bot.db in working directory)
  • Concurrency safety  — threading.RLock guards all shared-state mutations
  • Graceful shutdown   — SIGINT / SIGTERM notifies all members before exit
  • User blocking       — /block bans the current session; /unblock undoes it
  • Next button (Omegle)— /next disconnects stranger and re-queues immediately
  • Room topic          — optional description set during creation wizard
────────────────────────────────────────────────────────────────────────────
"""

import os
import signal
import sqlite3
import string
import random
import logging
import threading
from datetime import datetime, timedelta

import telebot
from telebot import types
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# Environment & bot setup
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    raise EnvironmentError("❌  API_TOKEN is missing. Add it to your .env file.")

bot = telebot.TeleBot(API_TOKEN)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Global re-entrant lock  (all mutations to shared dicts go through this)
# ─────────────────────────────────────────────────────────────────────────────

_lock = threading.RLock()

# ─────────────────────────────────────────────────────────────────────────────
# In-memory data structures
# ─────────────────────────────────────────────────────────────────────────────

# room_key -> {
#   "name", "owner", "members": set[int], "locked", "public",
#   "expiry_minutes", "last_activity", "burn", "message_log": list,
#   "type": "secret"|"public"|"omegle",
#   "topic": str | None          ← NEW optional description
# }
active_rooms:        dict[str, dict] = {}
active_room_names:   dict[str, str]  = {}  # room_key -> room_name
user_referrals:      dict[int, str]  = {}  # user_id  -> room_key

omegle_queue:        list[int]       = []  # waiting for random pairing

# blocked_users[user_a] = {user_b, ...}
# means user_a has blocked all session-partners in that set.
# Keys are *real* user_ids; values are also real user_ids.
blocked_users:       dict[int, set]  = {}

room_logs:           list[dict] = []
pending_room_config: dict[int, dict] = {}  # multi-step creation wizard state

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

EXPIRY_OPTIONS = {
    "expiry_0":  (0,  "♾️ No expiry"),
    "expiry_10": (10, "⏱ 10 minutes"),
    "expiry_30": (30, "⏱ 30 minutes"),
    "expiry_60": (60, "⏱ 60 minutes"),
}

PRIVATE_ROOM_MAX_MEMBERS = 2   # owner + 1 guest
DB_PATH = os.getenv("BOT_DB_PATH", "bot.db")

# ─────────────────────────────────────────────────────────────────────────────
# Persistent storage  (SQLite)
# ─────────────────────────────────────────────────────────────────────────────

def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables if they don't exist and reload live rooms into memory."""
    with _db_connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_key        TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                owner           INTEGER NOT NULL,
                public          INTEGER NOT NULL DEFAULT 0,
                expiry_minutes  INTEGER NOT NULL DEFAULT 0,
                burn            INTEGER NOT NULL DEFAULT 0,
                type            TEXT NOT NULL DEFAULT 'secret',
                topic           TEXT,
                last_activity   TEXT NOT NULL,
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS room_members (
                room_key  TEXT NOT NULL,
                user_id   INTEGER NOT NULL,
                PRIMARY KEY (room_key, user_id),
                FOREIGN KEY (room_key) REFERENCES rooms(room_key) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS blocked_pairs (
                blocker   INTEGER NOT NULL,
                blocked   INTEGER NOT NULL,
                PRIMARY KEY (blocker, blocked)
            );

            CREATE TABLE IF NOT EXISTS event_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event       TEXT NOT NULL,
                user_id     INTEGER NOT NULL,
                room_name   TEXT NOT NULL,
                ts          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS join_history (
                user_id        INTEGER NOT NULL,
                room_key       TEXT NOT NULL,
                room_name      TEXT NOT NULL,
                last_joined_at TEXT NOT NULL,
                PRIMARY KEY (user_id, room_key)
            );
        """)
    _load_state_from_db()
    logger.info("✅ Database initialised: %s", DB_PATH)


def _load_state_from_db() -> None:
    """Restore active_rooms, user_referrals, blocked_users from SQLite on startup."""
    with _lock:
        with _db_connect() as conn:
            for row in conn.execute("SELECT * FROM rooms"):
                key = row["room_key"]
                active_rooms[key] = {
                    "name":           row["name"],
                    "owner":          row["owner"],
                    "members":        set(),
                    "locked":         False,
                    "public":         bool(row["public"]),
                    "expiry_minutes": row["expiry_minutes"],
                    "last_activity":  datetime.fromisoformat(row["last_activity"]),
                    "burn":           bool(row["burn"]),
                    "message_log":    [],   # burn log is not persisted (intentional)
                    "type":           row["type"],
                    "topic":          row["topic"],
                }
                active_room_names[key] = row["name"]

            for row in conn.execute("SELECT room_key, user_id FROM room_members"):
                rk = row["room_key"]
                uid = row["user_id"]
                if rk in active_rooms:
                    active_rooms[rk]["members"].add(uid)
                    user_referrals[uid] = rk

            for row in conn.execute("SELECT blocker, blocked FROM blocked_pairs"):
                blocked_users.setdefault(row["blocker"], set()).add(row["blocked"])

    logger.info("♻️  Restored %d room(s) from DB.", len(active_rooms))


def _db_save_room(room_key: str, room: dict) -> None:
    with _db_connect() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO rooms
              (room_key, name, owner, public, expiry_minutes, burn, type, topic, last_activity, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            room_key,
            room["name"],
            room["owner"],
            int(room["public"]),
            room["expiry_minutes"],
            int(room["burn"]),
            room["type"],
            room.get("topic"),
            room["last_activity"].isoformat(),
            datetime.utcnow().isoformat(),
        ))
        # Persist members
        conn.execute("DELETE FROM room_members WHERE room_key = ?", (room_key,))
        conn.executemany(
            "INSERT OR IGNORE INTO room_members (room_key, user_id) VALUES (?, ?)",
            [(room_key, uid) for uid in room["members"]],
        )


def _db_delete_room(room_key: str) -> None:
    with _db_connect() as conn:
        conn.execute("DELETE FROM rooms WHERE room_key = ?", (room_key,))
        conn.execute("DELETE FROM room_members WHERE room_key = ?", (room_key,))


def _db_update_members(room_key: str, members: set) -> None:
    with _db_connect() as conn:
        conn.execute("DELETE FROM room_members WHERE room_key = ?", (room_key,))
        conn.executemany(
            "INSERT OR IGNORE INTO room_members (room_key, user_id) VALUES (?, ?)",
            [(room_key, uid) for uid in members],
        )


def _db_update_room_field(room_key: str, **kwargs) -> None:
    """Update arbitrary columns on the rooms table."""
    if not kwargs:
        return
    cols   = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [room_key]
    with _db_connect() as conn:
        conn.execute(f"UPDATE rooms SET {cols} WHERE room_key = ?", values)


def _db_add_block(blocker: int, blocked: int) -> None:
    with _db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO blocked_pairs (blocker, blocked) VALUES (?, ?)",
            (blocker, blocked),
        )


def _db_remove_block(blocker: int, blocked: int) -> None:
    with _db_connect() as conn:
        conn.execute(
            "DELETE FROM blocked_pairs WHERE blocker = ? AND blocked = ?",
            (blocker, blocked),
        )


def _db_log_event(event: str, user_id: int, room_name: str) -> None:
    with _db_connect() as conn:
        conn.execute(
            "INSERT INTO event_log (event, user_id, room_name, ts) VALUES (?, ?, ?, ?)",
            (event, user_id, room_name, datetime.utcnow().isoformat()),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Join history DB helpers
# ─────────────────────────────────────────────────────────────────────────────

JOIN_HISTORY_LIMIT = 10  # max entries kept per user


def _db_record_join_history(user_id: int, room_key: str, room_name: str) -> None:
    """Upsert a join-history entry; trim oldest rows beyond the per-user limit."""
    now = datetime.utcnow().isoformat()
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO join_history (user_id, room_key, room_name, last_joined_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, room_key) DO UPDATE SET
                room_name      = excluded.room_name,
                last_joined_at = excluded.last_joined_at
            """,
            (user_id, room_key, room_name, now),
        )
        # Keep only the most recent JOIN_HISTORY_LIMIT entries for this user
        conn.execute(
            """
            DELETE FROM join_history
            WHERE user_id = ?
              AND room_key NOT IN (
                  SELECT room_key FROM join_history
                  WHERE user_id = ?
                  ORDER BY last_joined_at DESC
                  LIMIT ?
              )
            """,
            (user_id, user_id, JOIN_HISTORY_LIMIT),
        )


def _db_get_join_history(user_id: int) -> list:
    """Return up to JOIN_HISTORY_LIMIT rows, newest first."""
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT room_key, room_name, last_joined_at
            FROM   join_history
            WHERE  user_id = ?
            ORDER  BY last_joined_at DESC
            LIMIT  ?
            """,
            (user_id, JOIN_HISTORY_LIMIT),
        ).fetchall()
    return [dict(r) for r in rows]


def _db_clear_join_history(user_id: int) -> None:
    with _db_connect() as conn:
        conn.execute("DELETE FROM join_history WHERE user_id = ?", (user_id,))


# ─────────────────────────────────────────────────────────────────────────────
# Bot username cache
# ─────────────────────────────────────────────────────────────────────────────

BOT_USERNAME: str | None = None


def get_bot_username() -> str:
    global BOT_USERNAME
    if BOT_USERNAME is None:
        BOT_USERNAME = bot.get_me().username
    return BOT_USERNAME


# ─────────────────────────────────────────────────────────────────────────────
# Core helpers
# ─────────────────────────────────────────────────────────────────────────────

def generate_room_key(length: int = 8) -> str:
    chars = string.ascii_letters + string.digits
    while True:
        key = "".join(random.choices(chars, k=length))
        if key not in active_rooms:
            return key


def build_invite_link(room_key: str) -> str:
    return f"https://t.me/{get_bot_username()}?start={room_key}"


def log_event(event_type: str, user_id: int, room_name: str) -> None:
    entry = {
        "event":     event_type,
        "user_id":   user_id,
        "room_name": room_name,
        "timestamp": datetime.utcnow().isoformat(),
    }
    room_logs.append(entry)
    _db_log_event(event_type, user_id, room_name)
    logger.info("LOG | %s | user=%s | room=%s", event_type, user_id, room_name)


def get_user_room(user_id: int) -> tuple[str | None, dict | None]:
    key = user_referrals.get(user_id)
    if key:
        return key, active_rooms.get(key)
    return None, None


def get_room_partners(room_key: str, exclude_user: int) -> list[int]:
    room = active_rooms.get(room_key)
    if not room:
        return []
    return [uid for uid in room["members"] if uid != exclude_user]


def touch_room(room_key: str) -> None:
    room = active_rooms.get(room_key)
    if room:
        room["last_activity"] = datetime.utcnow()
        _db_update_room_field(room_key, last_activity=room["last_activity"].isoformat())


def is_private_room(room: dict) -> bool:
    return room.get("type") == "secret"


def room_is_full(room: dict) -> bool:
    if is_private_room(room):
        return len(room["members"]) >= PRIVATE_ROOM_MAX_MEMBERS
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Blocking helpers
# ─────────────────────────────────────────────────────────────────────────────

def is_blocked(sender: int, recipient: int) -> bool:
    """Return True if recipient has blocked sender."""
    return sender in blocked_users.get(recipient, set())


def block_user(blocker: int, target: int) -> None:
    with _lock:
        blocked_users.setdefault(blocker, set()).add(target)
    _db_add_block(blocker, target)


def unblock_user(blocker: int, target: int) -> None:
    with _lock:
        blocked_users.get(blocker, set()).discard(target)
    _db_remove_block(blocker, target)


# ─────────────────────────────────────────────────────────────────────────────
# Room expiry scheduler
# ─────────────────────────────────────────────────────────────────────────────

def _schedule_expiry_check(room_key: str, expiry_minutes: int) -> None:
    if expiry_minutes <= 0:
        return

    def check():
        with _lock:
            room = active_rooms.get(room_key)
            if not room:
                return
            # Empty public room — nothing to expire
            if room.get("public") and not room["members"]:
                return
            idle_delta = datetime.utcnow() - room.get("last_activity", datetime.utcnow())
            if idle_delta >= timedelta(minutes=expiry_minutes):
                room_name = room["name"]
                logger.info("Room '%s' expired after %d min idle.", room_name, expiry_minutes)
                for uid in list(room["members"]):
                    try:
                        bot.send_message(
                            uid,
                            f"⏰ *{room_name}* expired due to inactivity and has been closed.",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                    user_referrals.pop(uid, None)
                _burn_messages_if_needed(room_key, room)
                if room.get("public"):
                    room["members"] = set()
                    _db_update_members(room_key, set())
                    logger.info("Public room '%s' expired but preserved (empty).", room_name)
                else:
                    active_rooms.pop(room_key, None)
                    active_room_names.pop(room_key, None)
                    _db_delete_room(room_key)
            else:
                remaining = timedelta(minutes=expiry_minutes) - idle_delta
                t = threading.Timer(remaining.total_seconds(), check)
                t.daemon = True
                t.start()

    t = threading.Timer(expiry_minutes * 60, check)
    t.daemon = True
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Burn messages helper
# ─────────────────────────────────────────────────────────────────────────────

def _burn_messages_if_needed(room_key: str, room: dict) -> None:
    if not room.get("burn"):
        return
    for chat_id, message_id in room.get("message_log", []):
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
    room["message_log"] = []
    logger.info("🔥 Burned messages for room '%s'.", room.get("name"))


def _track_message(room_key: str, chat_id: int, message_id: int) -> None:
    room = active_rooms.get(room_key)
    if room and room.get("burn"):
        room["message_log"].append((chat_id, message_id))


# ─────────────────────────────────────────────────────────────────────────────
# Inline keyboards
# ─────────────────────────────────────────────────────────────────────────────

def main_menu_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🔐 Create Room",      callback_data="create_room"),
        types.InlineKeyboardButton("🚪 Enter a Room",       callback_data="enter_room"),
        types.InlineKeyboardButton("🌍 Public Rooms",       callback_data="public_rooms"),
        types.InlineKeyboardButton("🎲 Omegle Room",        callback_data="omegle_room"),
        types.InlineKeyboardButton("📋 Joined Rooms",           callback_data="my_rooms"),
    )
    return kb


def share_keyboard(room_key: str) -> types.InlineKeyboardMarkup:
    link = build_invite_link(room_key)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(
        "📤 Share Invite Link",
        switch_inline_query=f"Join my secret room! {link}",
    ))
    return kb


def expiry_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(*[
        types.InlineKeyboardButton(label, callback_data=key)
        for key, (_, label) in EXPIRY_OPTIONS.items()
    ])
    return kb


def burn_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🔥 Yes, burn messages", callback_data="burn_yes"),
        types.InlineKeyboardButton("📁 No, keep messages",  callback_data="burn_no"),
    )
    return kb


def visibility_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🌍 Make it public",  callback_data="vis_public"),
        types.InlineKeyboardButton("🔒 Keep it private", callback_data="vis_private"),
    )
    return kb


def topic_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("⏭ Skip (no topic)", callback_data="topic_skip"))
    return kb


def admin_room_keyboard(room_key: str, room: dict) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    owner_id = room["owner"]

    # Join / presence indicator
    if owner_id in room["members"]:
        kb.add(types.InlineKeyboardButton(
            "✅ You are in this room", callback_data="padmin_noop",
        ))
    else:
        kb.add(types.InlineKeyboardButton(
            "🚀 Join My Room", callback_data=f"padmin_join:{room_key}",
        ))

    # Toggle visibility
    if room.get("public"):
        kb.add(types.InlineKeyboardButton(
            "🔒 Make Private", callback_data=f"padmin_private:{room_key}",
        ))
    else:
        kb.add(types.InlineKeyboardButton(
            "🌍 Make Public", callback_data=f"padmin_public:{room_key}",
        ))

    # Toggle burn mode
    if room.get("burn"):
        kb.add(types.InlineKeyboardButton(
            "📁 Disable Burn", callback_data=f"padmin_burn_off:{room_key}",
        ))
    else:
        kb.add(types.InlineKeyboardButton(
            "🔥 Enable Burn", callback_data=f"padmin_burn_on:{room_key}",
        ))

    # Close room
    kb.add(types.InlineKeyboardButton(
        "🗑 Close Room", callback_data=f"padmin_close:{room_key}",
    ))
    return kb


def omegle_action_keyboard() -> types.InlineKeyboardMarkup:
    """Shown after an Omegle pairing — Next + Leave buttons."""
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("⏭ Next Stranger", callback_data="omegle_next"),
        types.InlineKeyboardButton("🚪 Leave",         callback_data="omegle_leave"),
    )
    return kb


# ─────────────────────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["start"])
def handle_start(message: types.Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        _join_room(message, args[1].strip())
    else:
        bot.send_message(
            message.chat.id,
            "👋 *Welcome to Secret Chat Bot!*\n\nChat anonymously with anyone. No names. No traces. 🕵️",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# /leave
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["leave"])
def handle_leave(message: types.Message) -> None:
    user_id = message.from_user.id
    with _lock:
        if user_id in omegle_queue:
            omegle_queue.remove(user_id)
            bot.send_message(message.chat.id, "✅ Removed from Omegle queue.")
            return
        room_key, room = get_user_room(user_id)
        if not room:
            bot.send_message(message.chat.id, "⚠️ You are not in any room right now.")
            return
    _leave_room(user_id, room_key, room)


# ─────────────────────────────────────────────────────────────────────────────
# /next  — Omegle: skip to a new stranger
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["next"])
def handle_next(message: types.Message) -> None:
    _do_omegle_next(message.from_user.id, message.chat.id)


def _do_omegle_next(user_id: int, chat_id: int) -> None:
    """Leave current Omegle room (if any) and re-queue for a new pairing."""
    with _lock:
        room_key, room = get_user_room(user_id)
        if room and room.get("type") != "omegle":
            bot.send_message(chat_id, "🚫 /next is only available in Omegle rooms.")
            return

    # Leave current Omegle room silently then re-queue
    if room:
        _leave_room(user_id, room_key, room, silent=True)
        bot.send_message(chat_id, "⏭ *Skipping to a new stranger...*", parse_mode="Markdown")

    with _lock:
        if user_id in omegle_queue:
            bot.send_message(chat_id, "⏳ You are already searching. Please wait...")
            return
        if omegle_queue:
            partner_id = omegle_queue.pop(0)
            _create_omegle_pair(user_id, partner_id, chat_id)
        else:
            omegle_queue.append(user_id)
            bot.send_message(
                chat_id,
                "🎲 *Omegle Mode*\n\n🔍 Searching for a new stranger...\n\nUse /leave to cancel.",
                parse_mode="Markdown",
            )


# ─────────────────────────────────────────────────────────────────────────────
# /block  /unblock
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["block"])
def handle_block(message: types.Message) -> None:
    user_id = message.from_user.id
    room_key, room = get_user_room(user_id)
    if not room:
        bot.send_message(message.chat.id, "⚠️ You must be in a room to use /block.")
        return
    partners = get_room_partners(room_key, exclude_user=user_id)
    if not partners:
        bot.send_message(message.chat.id, "⚠️ No one else is in the room to block.")
        return
    newly_blocked = []
    for pid in partners:
        if pid not in blocked_users.get(user_id, set()):
            block_user(user_id, pid)
            newly_blocked.append(pid)
    if newly_blocked:
        bot.send_message(
            message.chat.id,
            "🚫 *Blocked.* This session's partner(s) can no longer send you messages.\n"
            "Use /unblock to undo.",
            parse_mode="Markdown",
        )
    else:
        bot.send_message(message.chat.id, "ℹ️ You have already blocked everyone in this room.")


@bot.message_handler(commands=["unblock"])
def handle_unblock(message: types.Message) -> None:
    user_id = message.from_user.id
    room_key, room = get_user_room(user_id)
    if not room:
        bot.send_message(message.chat.id, "⚠️ You must be in a room to use /unblock.")
        return
    partners = get_room_partners(room_key, exclude_user=user_id)
    unblocked = []
    for pid in partners:
        if pid in blocked_users.get(user_id, set()):
            unblock_user(user_id, pid)
            unblocked.append(pid)
    if unblocked:
        bot.send_message(message.chat.id, "✅ Unblocked. You will receive messages from your room partner(s) again.")
    else:
        bot.send_message(message.chat.id, "ℹ️ You haven't blocked anyone in this room.")


# ─────────────────────────────────────────────────────────────────────────────
# /kick  (private rooms only)
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["kick"])
def handle_kick(message: types.Message) -> None:
    user_id = message.from_user.id
    room_key, room = get_user_room(user_id)
    if not room:
        bot.send_message(message.chat.id, "⚠️ You are not in any room.")
        return
    if not is_private_room(room):
        bot.send_message(message.chat.id, "🚫 /kick is only available in private rooms.")
        return
    if room["owner"] != user_id:
        bot.send_message(message.chat.id, "🚫 Only the room owner can use /kick.")
        return
    non_owners = [uid for uid in room["members"] if uid != user_id]
    if not non_owners:
        bot.send_message(message.chat.id, "⚠️ There are no other members to kick.")
        return
    _kick_user(non_owners[0], room_key, room)
    bot.send_message(message.chat.id, "✅ A member has been anonymously removed from the room.")


# ─────────────────────────────────────────────────────────────────────────────
# /public_admin
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["public_admin"])
def handle_public_admin(message: types.Message) -> None:
    user_id = message.from_user.id
    with _lock:
        owned_public = [
            (key, room) for key, room in active_rooms.items()
            if room["owner"] == user_id and room.get("public")
        ]
    if not owned_public:
        bot.send_message(
            message.chat.id,
            (
                "🌍 *Public Room Admin*\n\n"
                "You have no public rooms right now.\n\n"
                "Create a room via /start and choose *Make it public* during setup."
            ),
            parse_mode="Markdown",
        )
        return
    bot.send_message(
        message.chat.id,
        f"🌍 *Public Room Admin*\n\nYou own *{len(owned_public)}* public room(s):",
        parse_mode="Markdown",
    )
    for room_key, room in owned_public:
        _send_admin_card(message.chat.id, room_key, room)


def _admin_card_text(room_key: str, room: dict) -> str:
    members    = len(room["members"])
    expiry     = f"{room['expiry_minutes']} min" if room.get("expiry_minutes") else "♾️ No expiry"
    burn       = "🔥 On" if room.get("burn") else "📁 Off"
    visibility = "🌍 Public" if room.get("public") else "🔒 Private"
    room_type  = room.get("type", "secret").capitalize()
    link       = build_invite_link(room_key)
    idle_secs  = int((datetime.utcnow() - room.get("last_activity", datetime.utcnow())).total_seconds())
    idle_str   = f"{idle_secs // 60}m {idle_secs % 60}s ago"
    topic_line = f"📌 Topic:        {room['topic']}\n" if room.get("topic") else ""

    return (
        f"🏠 *{room['name']}*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{topic_line}"
        f"🔑 Key:          `{room_key}`\n"
        f"👁 Visibility:   {visibility}\n"
        f"👥 Members:      {members}\n"
        f"⏱ Expiry:       {expiry}\n"
        f"🔥 Burn mode:   {burn}\n"
        f"🏷 Type:         {room_type}\n"
        f"🕐 Last active:  {idle_str}\n"
        f"🔗 [Invite link]({link})"
    )


def _send_admin_card(chat_id: int, room_key: str, room: dict) -> None:
    bot.send_message(
        chat_id,
        _admin_card_text(room_key, room),
        parse_mode="Markdown",
        reply_markup=admin_room_keyboard(room_key, room),
        disable_web_page_preview=True,
    )


def _refresh_admin_card(call: types.CallbackQuery, room_key: str, room: dict) -> None:
    try:
        bot.edit_message_text(
            _admin_card_text(room_key, room),
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown",
            reply_markup=admin_room_keyboard(room_key, room),
            disable_web_page_preview=True,
        )
    except Exception:
        pass


def _admin_guard(call: types.CallbackQuery, room_key: str) -> dict | None:
    room = active_rooms.get(room_key)
    if not room:
        bot.answer_callback_query(call.id, "⚠️ Room no longer exists.", show_alert=True)
        return None
    if room["owner"] != call.from_user.id:
        bot.answer_callback_query(call.id, "🚫 You are not the owner of this room.", show_alert=True)
        return None
    return room


# ─────────────────────────────────────────────────────────────────────────────
# /public_admin callbacks
# ─────────────────────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda call: call.data.startswith("padmin_private:"))
def padmin_make_private(call: types.CallbackQuery) -> None:
    room_key = call.data.split(":", 1)[1]
    with _lock:
        room = _admin_guard(call, room_key)
        if not room:
            return
        room["public"] = False
        room["type"]   = "secret"
        _db_update_room_field(room_key, public=0, type="secret")
    bot.answer_callback_query(call.id, "✅ Room is now Private.")
    _refresh_admin_card(call, room_key, room)


@bot.callback_query_handler(func=lambda call: call.data.startswith("padmin_public:"))
def padmin_make_public(call: types.CallbackQuery) -> None:
    room_key = call.data.split(":", 1)[1]
    with _lock:
        room = _admin_guard(call, room_key)
        if not room:
            return
        room["public"] = True
        room["type"]   = "public"
        _db_update_room_field(room_key, public=1, type="public")
    bot.answer_callback_query(call.id, "✅ Room is now Public.")
    _refresh_admin_card(call, room_key, room)


@bot.callback_query_handler(func=lambda call: call.data.startswith("padmin_burn_on:"))
def padmin_burn_on(call: types.CallbackQuery) -> None:
    room_key = call.data.split(":", 1)[1]
    with _lock:
        room = _admin_guard(call, room_key)
        if not room:
            return
        room["burn"] = True
        _db_update_room_field(room_key, burn=1)
    bot.answer_callback_query(call.id, "🔥 Burn mode enabled.")
    _refresh_admin_card(call, room_key, room)


@bot.callback_query_handler(func=lambda call: call.data.startswith("padmin_burn_off:"))
def padmin_burn_off(call: types.CallbackQuery) -> None:
    room_key = call.data.split(":", 1)[1]
    with _lock:
        room = _admin_guard(call, room_key)
        if not room:
            return
        room["burn"] = False
        _db_update_room_field(room_key, burn=0)
    bot.answer_callback_query(call.id, "📁 Burn mode disabled.")
    _refresh_admin_card(call, room_key, room)


@bot.callback_query_handler(func=lambda call: call.data.startswith("padmin_close:"))
def padmin_close(call: types.CallbackQuery) -> None:
    room_key = call.data.split(":", 1)[1]
    with _lock:
        room = _admin_guard(call, room_key)
        if not room:
            return
        room_name = room["name"]
        for uid in list(room["members"]):
            if uid == call.from_user.id:
                continue
            try:
                bot.send_message(uid, f"🗑 *{room_name}* has been closed by the owner.", parse_mode="Markdown")
            except Exception:
                pass
            user_referrals.pop(uid, None)
        user_referrals.pop(call.from_user.id, None)
        _burn_messages_if_needed(room_key, room)
        active_rooms.pop(room_key, None)
        active_room_names.pop(room_key, None)
        _db_delete_room(room_key)
        log_event("room_closed_by_admin", call.from_user.id, room_name)
    bot.answer_callback_query(call.id, "🗑 Room closed.")
    try:
        bot.edit_message_text(
            f"🗑 *{room_name}* has been permanently closed.",
            call.message.chat.id, call.message.message_id, parse_mode="Markdown",
        )
    except Exception:
        pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("padmin_join:"))
def padmin_join(call: types.CallbackQuery) -> None:
    user_id  = call.from_user.id
    room_key = call.data.split(":", 1)[1]
    with _lock:
        room = _admin_guard(call, room_key)
        if not room:
            return
        if user_id in room["members"]:
            bot.answer_callback_query(call.id, "✅ You are already in this room.", show_alert=True)
            return
        current_key = user_referrals.get(user_id)
        if current_key and current_key != room_key:
            bot.answer_callback_query(
                call.id, "⚠️ You are in another room. /leave first.", show_alert=True,
            )
            return
        room["members"].add(user_id)
        user_referrals[user_id] = room_key
        _db_update_members(room_key, room["members"])
        touch_room(room_key)
        log_event("owner_rejoined", user_id, room["name"])
    bot.answer_callback_query(call.id, f"✅ Rejoined {room['name']}!")
    _refresh_admin_card(call, room_key, room)
    partners = get_room_partners(room_key, exclude_user=user_id)
    if partners:
        bot.send_message(
            call.message.chat.id,
            f"🟢 You rejoined *{room['name']}*!\n*{len(partners)}* member(s) online — start chatting! 💬\nUse /leave to step out.",
            parse_mode="Markdown",
        )
    else:
        bot.send_message(
            call.message.chat.id,
            f"🟢 You rejoined *{room['name']}*!\nNo one else here yet. 🕐\nUse /leave to step out.",
            parse_mode="Markdown",
        )
    for pid in partners:
        try:
            bot.send_message(pid, f"🟢 Someone rejoined *{room['name']}*.", parse_mode="Markdown")
        except Exception as exc:
            logger.warning("Could not notify member %s: %s", pid, exc)


@bot.callback_query_handler(func=lambda call: call.data == "padmin_noop")
def padmin_noop(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id, "✅ You are already inside this room.")


# ─────────────────────────────────────────────────────────────────────────────
# Main menu callbacks
# ─────────────────────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda call: call.data == "create_room")
def callback_create_room(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    if user_id in user_referrals:
        bot.answer_callback_query(call.id, "⚠️ You are already inside a room. /leave first.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "✏️ Please send a *name* for your secret room:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, _process_room_name)


@bot.callback_query_handler(func=lambda call: call.data == "enter_room")
def callback_enter_room(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    if user_id in user_referrals:
        bot.answer_callback_query(call.id, "⚠️ You are already inside a room. /leave first.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "🔑 Please send the *room key* you want to join:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, _process_room_key_input)


@bot.callback_query_handler(func=lambda call: call.data == "public_rooms")
def callback_public_rooms(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    with _lock:
        public = [
            (key, room) for key, room in active_rooms.items()
            if room.get("public")
            and room["owner"] != user_id
            and user_id not in room["members"]
        ]
    if not public:
        hint = ("You are currently inside a room. Use /leave to browse public rooms."
                if user_id in user_referrals else
                "No public rooms are available right now. Create one via /start!")
        bot.send_message(
            call.message.chat.id,
            f"🌍 *Public Rooms*\n\n{hint}",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return
    kb    = types.InlineKeyboardMarkup(row_width=1)
    lines = ["🌍 *Public Rooms* — tap a room to join:\n"]
    for key, room in public:
        mc       = len(room["members"])
        exp      = f"⏱ {room['expiry_minutes']}m" if room.get("expiry_minutes") else "♾️"
        burn_tag = " 🔥" if room.get("burn") else ""
        status   = f"{mc} online" if mc > 0 else "empty — be the first!"
        topic    = f" · _{room['topic']}_" if room.get("topic") else ""
        kb.add(types.InlineKeyboardButton(
            f"🏠 {room['name']}  ({status})", callback_data=f"join_pub:{key}",
        ))
        lines.append(f"• *{room['name']}*{topic} — {status}  {exp}{burn_tag}")
    bot.send_message(call.message.chat.id, "\n".join(lines), parse_mode="Markdown", reply_markup=kb)


@bot.callback_query_handler(func=lambda call: call.data.startswith("join_pub:"))
def callback_join_public(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    room_key = call.data.split(":", 1)[1]
    _join_room(call.message, room_key, override_user_id=call.from_user.id)


# ─────────────────────────────────────────────────────────────────────────────
# /my_rooms  — previously joined public rooms
# ─────────────────────────────────────────────────────────────────────────────

def _build_history_message(user_id: int) -> tuple:
    """Build the text and keyboard for the My Rooms history screen."""
    history = _db_get_join_history(user_id)
    kb      = types.InlineKeyboardMarkup(row_width=1)

    if not history:
        text = (
            "\U0001F4CB *My Rooms*\n\n"
            "You haven't joined any public rooms yet.\n"
            "Browse \U0001F30D *Public Rooms* from the main menu to get started!"
        )
        return text, kb

    result_lines = ["\U0001F4CB *My Rooms* \u2014 rooms you've visited:\n"]
    for row in history:
        room_key  = row["room_key"]
        room_name = row["room_name"]
        joined_dt = datetime.fromisoformat(row["last_joined_at"])
        ago_secs  = int((datetime.utcnow() - joined_dt).total_seconds())
        if ago_secs < 3600:
            ago = f"{ago_secs // 60}m ago"
        elif ago_secs < 86400:
            ago = f"{ago_secs // 3600}h ago"
        else:
            ago = f"{ago_secs // 86400}d ago"

        room = active_rooms.get(room_key)
        if room and room.get("public"):
            mc     = len(room["members"])
            status = f"{mc} online" if mc > 0 else "empty"
            topic  = f" \u00b7 _{room['topic']}_" if room.get("topic") else ""
            result_lines.append(f"\u2022 *{room_name}*{topic} \u2014 {status}  _(last visited {ago})_")
            kb.add(types.InlineKeyboardButton(
                f"\U0001F680 Rejoin - {room_name}  ({status})",
                callback_data=f"history_rejoin:{room_key}",
            ))
        else:
            result_lines.append(f"\u2022 {room_name} \u2014 _room closed_  _(last visited {ago})_")

    kb.add(types.InlineKeyboardButton("\U0001F5D1 Clear History", callback_data="history_clear"))
    return "\n".join(result_lines), kb


@bot.message_handler(commands=["my_rooms"])
def handle_my_rooms(message: types.Message) -> None:
    text, kb = _build_history_message(message.from_user.id)
    bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=kb)


@bot.callback_query_handler(func=lambda call: call.data == "my_rooms")
def callback_my_rooms(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    text, kb = _build_history_message(call.from_user.id)
    bot.send_message(call.message.chat.id, text, parse_mode="Markdown", reply_markup=kb)


@bot.callback_query_handler(func=lambda call: call.data.startswith("history_rejoin:"))
def callback_history_rejoin(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    user_id  = call.from_user.id
    room_key = call.data.split(":", 1)[1]

    # Block if already in a room
    if user_id in user_referrals:
        bot.send_message(
            call.message.chat.id,
            "⚠️ You are currently in a room. Use /leave first, then rejoin.",
        )
        return

    # Check room still exists and is public
    room = active_rooms.get(room_key)
    if not room or not room.get("public"):
        bot.send_message(
            call.message.chat.id,
            "❌ This room no longer exists or is no longer public.",
        )
        return

    _join_room(call.message, room_key, override_user_id=user_id)


@bot.callback_query_handler(func=lambda call: call.data == "history_clear")
def callback_history_clear(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    _db_clear_join_history(call.from_user.id)
    bot.edit_message_text(
        "\U0001F4CB *My Rooms*\n\n\u2705 History cleared.",
        call.message.chat.id,
        call.message.message_id,
        parse_mode="Markdown",
    )


@bot.callback_query_handler(func=lambda call: call.data == "omegle_room")
def callback_omegle_room(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    if user_id in user_referrals:
        bot.answer_callback_query(call.id, "⚠️ You are already inside a room. /leave first.", show_alert=True)
        return
    if user_id in omegle_queue:
        bot.answer_callback_query(call.id, "⏳ You are already in the queue!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    with _lock:
        if omegle_queue:
            partner_id = omegle_queue.pop(0)
            _create_omegle_pair(user_id, partner_id, call.message.chat.id)
        else:
            omegle_queue.append(user_id)
            bot.send_message(
                call.message.chat.id,
                "🎲 *Omegle Mode*\n\n🔍 Searching for a random stranger...\n\nUse /leave to cancel.",
                parse_mode="Markdown",
            )


@bot.callback_query_handler(func=lambda call: call.data == "omegle_next")
def callback_omegle_next(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    _do_omegle_next(call.from_user.id, call.message.chat.id)


@bot.callback_query_handler(func=lambda call: call.data == "omegle_leave")
def callback_omegle_leave(call: types.CallbackQuery) -> None:
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    with _lock:
        room_key, room = get_user_room(user_id)
        if not room:
            return
    _leave_room(user_id, room_key, room)


# ─────────────────────────────────────────────────────────────────────────────
# Room creation wizard  (expiry → burn → visibility → topic)
# ─────────────────────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda call: call.data in EXPIRY_OPTIONS)
def callback_expiry_selected(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    expiry_minutes, label = EXPIRY_OPTIONS[call.data]
    bot.answer_callback_query(call.id)
    config = pending_room_config.get(user_id, {})
    config["expiry_minutes"] = expiry_minutes
    pending_room_config[user_id] = config
    bot.edit_message_text(
        f"✅ Expiry set: *{label}*\n\n🔥 Enable *burn mode*?\n_(Messages deleted for everyone when room closes.)_",
        chat_id=call.message.chat.id, message_id=call.message.message_id,
        parse_mode="Markdown", reply_markup=burn_keyboard(),
    )


@bot.callback_query_handler(func=lambda call: call.data in ("burn_yes", "burn_no"))
def callback_burn_selected(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    burn = call.data == "burn_yes"
    bot.answer_callback_query(call.id)
    config = pending_room_config.get(user_id, {})
    config["burn"] = burn
    pending_room_config[user_id] = config
    burn_label = "🔥 Burn mode ON" if burn else "📁 Burn mode OFF"
    bot.edit_message_text(
        f"✅ {burn_label}\n\n🌍 Should this room appear in *Public Rooms*?\n_(Private rooms: max 2 people.)_",
        chat_id=call.message.chat.id, message_id=call.message.message_id,
        parse_mode="Markdown", reply_markup=visibility_keyboard(),
    )


@bot.callback_query_handler(func=lambda call: call.data in ("vis_public", "vis_private"))
def callback_visibility_selected(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    public = call.data == "vis_public"
    bot.answer_callback_query(call.id)
    config = pending_room_config.get(user_id, {})
    config["public"] = public
    pending_room_config[user_id] = config
    vis_label = "🌍 Public" if public else "🔒 Private"
    # Proceed to optional topic step
    bot.edit_message_text(
        f"✅ {vis_label}\n\n📌 *Room Topic* (optional)\n\nSend a short description for your room, or tap *Skip*:",
        chat_id=call.message.chat.id, message_id=call.message.message_id,
        parse_mode="Markdown", reply_markup=topic_keyboard(),
    )
    # Register next step for topic text input
    bot.register_next_step_handler_by_chat_id(call.message.chat.id, _process_topic_input)


@bot.callback_query_handler(func=lambda call: call.data == "topic_skip")
def callback_topic_skip(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    bot.answer_callback_query(call.id)
    config = pending_room_config.pop(user_id, {})
    if user_id in user_referrals:
        bot.send_message(call.message.chat.id, "⚠️ You are already in a room. Use /leave first.")
        return
    _finalize_room_creation(
        chat_id=call.message.chat.id, user_id=user_id,
        room_name=config.get("name", "Secret Room"),
        expiry_minutes=config.get("expiry_minutes", 0),
        burn=config.get("burn", False),
        public=config.get("public", False),
        topic=None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step handlers
# ─────────────────────────────────────────────────────────────────────────────

def _process_room_name(message: types.Message) -> None:
    user_id   = message.from_user.id
    room_name = message.text.strip() if message.text else ""
    if not room_name:
        bot.send_message(message.chat.id, "❌ Room name cannot be empty. Try again with /start.")
        return
    if user_id in user_referrals:
        bot.send_message(message.chat.id, "⚠️ You are already in a room. Use /leave first.")
        return
    pending_room_config[user_id] = {"name": room_name}
    bot.send_message(
        message.chat.id,
        f"✅ Room name: *{room_name}*\n\n⏱ Choose an *expiry time* for this room:",
        parse_mode="Markdown", reply_markup=expiry_keyboard(),
    )


def _process_topic_input(message: types.Message) -> None:
    user_id = message.from_user.id
    topic   = message.text.strip() if message.text else ""
    config  = pending_room_config.pop(user_id, {})
    if user_id in user_referrals:
        bot.send_message(message.chat.id, "⚠️ You are already in a room. Use /leave first.")
        return
    _finalize_room_creation(
        chat_id=message.chat.id, user_id=user_id,
        room_name=config.get("name", "Secret Room"),
        expiry_minutes=config.get("expiry_minutes", 0),
        burn=config.get("burn", False),
        public=config.get("public", False),
        topic=topic or None,
    )


def _process_room_key_input(message: types.Message) -> None:
    _join_room(message, message.text.strip() if message.text else "")


# ─────────────────────────────────────────────────────────────────────────────
# Core room logic
# ─────────────────────────────────────────────────────────────────────────────

def _finalize_room_creation(
    chat_id: int,
    user_id: int,
    room_name: str,
    expiry_minutes: int,
    burn: bool,
    public: bool,
    topic: str | None = None,
    room_type: str = "secret",
) -> str:
    with _lock:
        room_key      = generate_room_key()
        resolved_type = "public" if public else room_type
        now           = datetime.utcnow()

        active_rooms[room_key] = {
            "name":           room_name,
            "owner":          user_id,
            "members":        {user_id},
            "locked":         False,
            "public":         public,
            "expiry_minutes": expiry_minutes,
            "last_activity":  now,
            "burn":           burn,
            "message_log":    [],
            "type":           resolved_type,
            "topic":          topic,
        }
        active_room_names[room_key] = room_name
        user_referrals[user_id]     = room_key
        _db_save_room(room_key, active_rooms[room_key])
        log_event("room_created", user_id, room_name)

    if expiry_minutes > 0:
        _schedule_expiry_check(room_key, expiry_minutes)

    tags = [
        "🌍 Public\n" if public else "🔒 Private (max 2 people)\n",
        "🔥 Burn mode ON\n" if burn else "📁 Messages kept\n",
        f"⏱ Expires in {expiry_minutes} min" if expiry_minutes > 0 else "♾️ No expiry",
    ]
    topic_line  = f"📌 Topic: _{topic}_\n\n" if topic else ""
    invite_link = build_invite_link(room_key)
    bot.send_message(
        chat_id,
        (
            f"✅ *Secret room created!*\n\n"
            f"🏷  Room name: *{room_name}*\n"
            f"🔑  Room key:  `{room_key}`\n"
            f"{topic_line}"
            f"{'  ·  '.join(tags)}\n\n"
        ),
        parse_mode="Markdown",
        reply_markup=share_keyboard(room_key),
    )
    return room_key


def _join_room(message: types.Message, room_key: str, override_user_id: int | None = None) -> None:
    user_id = override_user_id or message.from_user.id
    chat_id = message.chat.id

    with _lock:
        if user_id in user_referrals:
            bot.send_message(chat_id, "⚠️ You are already in a room. Use /leave before joining another.")
            return
        room = active_rooms.get(room_key)
        if not room:
            bot.send_message(chat_id, "❌ Invalid room key. The room doesn't exist or has expired.")
            return
        if room["owner"] == user_id:
            bot.send_message(chat_id, "🚫 You cannot join your own room.")
            return
        if room_is_full(room):
            bot.send_message(
                chat_id,
                "🔒 This private room is already full.\nPrivate rooms can only hold *2 people* (owner + 1 guest).",
                parse_mode="Markdown",
            )
            return
        room["members"].add(user_id)
        user_referrals[user_id] = room_key
        _db_update_members(room_key, room["members"])
        touch_room(room_key)
        room_name  = room["name"]
        topic      = room.get("topic")
        is_public  = room.get("public", False)
        log_event("room_joined", user_id, room_name)

    # Record in join history for public rooms so the user can rejoin easily
    if is_public:
        _db_record_join_history(user_id, room_key, room_name)

    topic_line = f"📌 _{topic}_\n\n" if topic else ""
    bot.send_message(
        chat_id,
        (
            f"🎉 You joined *{room_name}*!\n\n"
            f"{topic_line}"
            f"You are now connected anonymously. Say hello! 👋\n"
            f"Use /leave to disconnect."
        ),
        parse_mode="Markdown",
    )
    for partner_id in get_room_partners(room_key, exclude_user=user_id):
        try:
            bot.send_message(
                partner_id,
                f"🟢 Someone joined *{room_name}*! You are now connected.\nUse /leave to disconnect.",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not notify user %s: %s", partner_id, exc)


def _leave_room(user_id: int, room_key: str, room: dict, silent: bool = False) -> None:
    """
    silent=True suppresses the farewell message — used by /next so the user
    doesn't get a goodbye before immediately getting a new pairing notice.
    """
    with _lock:
        room_name = room["name"]
        is_owner  = room["owner"] == user_id

        room["members"].discard(user_id)
        user_referrals.pop(user_id, None)
        _db_update_members(room_key, room["members"])
        log_event("room_left", user_id, room_name)

    if not silent:
        if is_owner and room.get("public"):
            bot.send_message(
                user_id,
                (
                    f"👋 You stepped out of *{room_name}*.\n\n"
                    f"🌍 The room remains *public and open* for others.\n"
                    f"Rejoin anytime via /public\\_admin → *🚀 Join My Room*."
                ),
                parse_mode="Markdown",
            )
        else:
            bot.send_message(user_id, f"👋 You left *{room_name}*. See you next time!", parse_mode="Markdown")

    with _lock:
        remaining_partners = list(room["members"])

    for partner_id in remaining_partners:
        try:
            bot.send_message(
                partner_id,
                f"🔴 Your chat partner left *{room_name}*. You are now alone.\nUse /leave or wait for someone new.",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not notify user %s: %s", partner_id, exc)

    with _lock:
        if not room["members"]:
            if room.get("public"):
                _burn_messages_if_needed(room_key, room)
                logger.info("Public room '%s' is now empty but preserved.", room_name)
            else:
                _burn_messages_if_needed(room_key, room)
                active_rooms.pop(room_key, None)
                active_room_names.pop(room_key, None)
                _db_delete_room(room_key)
                logger.info("Room '%s' deleted — no members left.", room_name)


def _kick_user(target_id: int, room_key: str, room: dict) -> None:
    with _lock:
        room["members"].discard(target_id)
        user_referrals.pop(target_id, None)
        _db_update_members(room_key, room["members"])
        log_event("member_kicked", target_id, room["name"])
    try:
        bot.send_message(
            target_id,
            f"👢 You have been removed from *{room['name']}* by the room owner.",
            parse_mode="Markdown",
        )
    except Exception as exc:
        logger.warning("Could not notify kicked user %s: %s", target_id, exc)
    for pid in list(room["members"]):
        try:
            bot.send_message(pid, "🔴 A member was removed from the room.")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Omegle pairing
# ─────────────────────────────────────────────────────────────────────────────

def _create_omegle_pair(user_a: int, user_b: int, chat_id_a: int) -> None:
    with _lock:
        room_name = f"Omegle-{generate_room_key(4)}"
        room_key  = generate_room_key()
        now       = datetime.utcnow()
        active_rooms[room_key] = {
            "name":           room_name,
            "owner":          user_a,
            "members":        {user_a, user_b},
            "locked":         False,
            "public":         False,
            "expiry_minutes": 0,
            "last_activity":  now,
            "burn":           True,
            "message_log":    [],
            "type":           "omegle",
            "topic":          None,
        }
        active_room_names[room_key] = room_name
        user_referrals[user_a] = room_key
        user_referrals[user_b] = room_key
        _db_save_room(room_key, active_rooms[room_key])
        log_event("omegle_paired", user_a, room_name)
        log_event("omegle_paired", user_b, room_name)

    msg = (
        "🎲 *Omegle Room*\n\n"
        "✅ You've been connected to a random stranger!\n"
        "🔥 Messages are burned when either of you leaves.\n\n"
        "Use the buttons below or /next to find a new stranger."
    )
    for uid, cid in [(user_a, chat_id_a), (user_b, user_b)]:
        try:
            bot.send_message(cid, msg, parse_mode="Markdown", reply_markup=omegle_action_keyboard())
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Anonymous message relay
# ─────────────────────────────────────────────────────────────────────────────

def _get_partners_or_warn(user_id: int) -> tuple[list[int], str | None, str | None]:
    room_key, room = get_user_room(user_id)
    if not room:
        bot.send_message(user_id, "⚠️ You are not in any room. Use /start to create or join one.")
        return [], None, None
    partners = get_room_partners(room_key, exclude_user=user_id)
    if not partners:
        bot.send_message(user_id, "🕐 Waiting for someone to join your room...")
        return [], None, None
    return partners, room["name"], room_key


def _filtered_partners(user_id: int, partners: list[int]) -> list[int]:
    """Remove partners who have blocked this user."""
    return [pid for pid in partners if not is_blocked(user_id, pid)]


@bot.message_handler(content_types=["text"])
def relay_text(message: types.Message) -> None:
    user_id = message.from_user.id
    partners, _, room_key = _get_partners_or_warn(user_id)
    if not room_key:
        return
    touch_room(room_key)
    for pid in _filtered_partners(user_id, partners):
        try:
            sent = bot.send_message(pid, f"💬 {message.text}")
            _track_message(room_key, pid, sent.message_id)
        except Exception as exc:
            logger.warning("Failed to relay text to %s: %s", pid, exc)


@bot.message_handler(content_types=["photo"])
def relay_photo(message: types.Message) -> None:
    user_id = message.from_user.id
    partners, _, room_key = _get_partners_or_warn(user_id)
    if not room_key:
        return
    touch_room(room_key)
    file_id = message.photo[-1].file_id
    caption = f"🖼 {message.caption}" if message.caption else None
    for pid in _filtered_partners(user_id, partners):
        try:
            sent = bot.send_photo(pid, file_id, caption=caption)
            _track_message(room_key, pid, sent.message_id)
        except Exception as exc:
            logger.warning("Failed to relay photo to %s: %s", pid, exc)


@bot.message_handler(content_types=["sticker"])
def relay_sticker(message: types.Message) -> None:
    user_id = message.from_user.id
    partners, _, room_key = _get_partners_or_warn(user_id)
    if not room_key:
        return
    touch_room(room_key)
    for pid in _filtered_partners(user_id, partners):
        try:
            sent = bot.send_sticker(pid, message.sticker.file_id)
            _track_message(room_key, pid, sent.message_id)
        except Exception as exc:
            logger.warning("Failed to relay sticker to %s: %s", pid, exc)


@bot.message_handler(content_types=["voice"])
def relay_voice(message: types.Message) -> None:
    user_id = message.from_user.id
    partners, _, room_key = _get_partners_or_warn(user_id)
    if not room_key:
        return
    touch_room(room_key)
    for pid in _filtered_partners(user_id, partners):
        try:
            sent = bot.send_voice(pid, message.voice.file_id)
            _track_message(room_key, pid, sent.message_id)
        except Exception as exc:
            logger.warning("Failed to relay voice to %s: %s", pid, exc)


@bot.message_handler(content_types=["video"])
def relay_video(message: types.Message) -> None:
    user_id = message.from_user.id
    partners, _, room_key = _get_partners_or_warn(user_id)
    if not room_key:
        return
    touch_room(room_key)
    caption = f"🎥 {message.caption}" if message.caption else None
    for pid in _filtered_partners(user_id, partners):
        try:
            sent = bot.send_video(pid, message.video.file_id, caption=caption)
            _track_message(room_key, pid, sent.message_id)
        except Exception as exc:
            logger.warning("Failed to relay video to %s: %s", pid, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────────────────────────────────────

def _graceful_shutdown(signum, frame) -> None:
    sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
    logger.info("📴 Received %s — shutting down gracefully...", sig_name)

    with _lock:
        all_members = set()
        for room in active_rooms.values():
            all_members.update(room["members"])

    notified = 0
    for uid in all_members:
        try:
            bot.send_message(
                uid,
                "🔧 *Bot is restarting for maintenance.*\n\nYour room is saved and will be available when the bot comes back online.",
                parse_mode="Markdown",
            )
            notified += 1
        except Exception:
            pass

    logger.info("✅ Notified %d active user(s). Stopping polling.", notified)
    bot.stop_polling()


signal.signal(signal.SIGINT,  _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    logger.info("🤖 Secret Chat Bot is running...")
    bot.polling(non_stop=True)
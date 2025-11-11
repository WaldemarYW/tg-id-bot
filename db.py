import sqlite3
import hashlib
import time
import calendar
from pathlib import Path
from typing import Optional, Iterable, List, Tuple
import re

class DB:
    """A thin wrapper around SQLite providing helpers for the bot.

    On initialisation it loads the schema contained in ``messages.sql`` and
    enables WAL mode and foreign keys.  All operations are synchronous.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        # enable PRAGMAs once at connection
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.ensure_schema()

    def ensure_schema(self):
        """Load or update the DB schema from messages.sql."""
        sql_path = Path(__file__).parent / "messages.sql"
        sql = sql_path.read_text(encoding="utf-8")
        self.conn.executescript(sql)
        self.conn.commit()
        # soft migration: support username reservations for admin-driven onboarding
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reserved_usernames (
                username_lc TEXT PRIMARY KEY,
                added_by    INTEGER,
                created_at  TEXT DEFAULT (CURRENT_TIMESTAMP)
            )
            """
        )
        # app settings key/value storage
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        self.conn.commit()

    # --- Admins
    def add_admin(self, user_id: int):
        """Insert a user into the admins table (superadmin is added on startup)."""
        self.conn.execute("INSERT OR IGNORE INTO admins(user_id) VALUES (?)", (user_id,))
        self.conn.commit()

    def remove_admin(self, user_id: int):
        self.conn.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        self.conn.commit()

    def is_admin(self, user_id: int) -> bool:
        row = self.conn.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)).fetchone()
        return row is not None

    # --- Allowed users
    def add_allowed_user(self, user_id: int, username_lc: str, added_by: int, credits: int = 100):
        """Insert or update an allowed user with starting credits.  Lowercases
        the username for case‑insensitive matching.  If the user already
        exists the credits and username are updated.
        """
        self.conn.execute(
            """
            INSERT INTO allowed_users(user_id, username_lc, added_by, credits)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username_lc=excluded.username_lc,
                credits=CASE WHEN allowed_users.credits < excluded.credits THEN excluded.credits ELSE allowed_users.credits END,
                added_by=excluded.added_by
            """,
            (user_id, username_lc.lower() if username_lc else None, added_by, credits)
        )
        self.conn.commit()

    def remove_allowed_user(self, user_id: int):
        self.conn.execute("DELETE FROM allowed_users WHERE user_id=?", (user_id,))
        self.conn.commit()

    def is_allowed_user(self, user_id: int) -> bool:
        """Return True if the user is present in allowed_users (admin or superadmin
        will also be allowed externally).
        """
        row = self.conn.execute("SELECT 1 FROM allowed_users WHERE user_id=?", (user_id,)).fetchone()
        return row is not None

    def get_user_credits(self, user_id: int) -> int:
        row = self.conn.execute("SELECT credits FROM allowed_users WHERE user_id=?", (user_id,)).fetchone()
        return row["credits"] if row else 0

    def add_credits(self, user_id: int, amount: int = 1):
        if amount <= 0:
            return
        self.conn.execute(
            "UPDATE allowed_users SET credits = credits + ? WHERE user_id=?",
            (amount, user_id)
        )
        self.conn.commit()

    def reduce_credits(self, user_id: int, amount: int = 1):
        if amount <= 0:
            return
        self.conn.execute(
            "UPDATE allowed_users SET credits = MAX(0, credits - ?) WHERE user_id=?",
            (amount, user_id)
        )
        self.conn.commit()

    def set_user_ban(self, user_id: int, banned_until_ts: int):
        """Record a temporary ban for a user until the given UNIX timestamp."""
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(banned_until_ts))
        self.conn.execute(
            """
            INSERT INTO allowed_users(user_id, banned_until)
            VALUES(?,?)
            ON CONFLICT(user_id) DO UPDATE SET banned_until=excluded.banned_until
            """,
            (user_id, ts_str)
        )
        self.conn.commit()

    def get_user_ban(self, user_id: int) -> Optional[int]:
        row = self.conn.execute("SELECT banned_until FROM allowed_users WHERE user_id=?", (user_id,)).fetchone()
        if not row or row["banned_until"] is None:
            return None
        # convert to unix timestamp
        try:
            ts = time.mktime(time.strptime(row["banned_until"], "%Y-%m-%d %H:%M:%S"))
        except Exception:
            return None
        return int(ts)

    # --- Invitations
    def create_invitation(self, token_hash: str, created_by: int, ttl_seconds: int = 3600):
        """Create a new one‑time invitation.  Returns True on success or
        False if an invite with the same hash already exists.
        """
        try:
            self.conn.execute(
                "INSERT INTO invitations(token_hash, created_by, ttl_seconds) VALUES(?,?,?)",
                (token_hash, created_by, ttl_seconds)
            )
            self.conn.commit()
            return True
        except Exception:
            return False

    def use_invitation(self, token_hash: str, user_id: int) -> bool:
        """Consume an invitation.  If the token exists, is not used and is
        within its TTL, mark it as used by this user and return True.
        Otherwise return False.
        """
        cur = self.conn.cursor()
        row = cur.execute("SELECT created_at, ttl_seconds, is_used, created_by FROM invitations WHERE token_hash=?", (token_hash,)).fetchone()
        if not row:
            return False
        if row["is_used"]:
            return False
        created_at_ts = calendar.timegm(time.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"))
        now_ts = time.time()
        if now_ts > created_at_ts + row["ttl_seconds"]:
            return False
        # mark used
        cur.execute(
            "UPDATE invitations SET is_used=1, used_by=?, used_at=CURRENT_TIMESTAMP WHERE token_hash=?",
            (user_id, token_hash)
        )
        self.conn.commit()
        return True

    def list_invitations(self, admin_id: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT token_hash, created_at, ttl_seconds, is_used, used_by, used_at FROM invitations WHERE created_by=? ORDER BY created_at DESC",
            (admin_id,)
        ).fetchall()

    # --- Quotas
    def get_quota(self, admin_id: int) -> Tuple[int, int]:
        row = self.conn.execute("SELECT quota, used FROM admin_invite_quotas WHERE admin_id=?", (admin_id,)).fetchone()
        if not row:
            return (0, 0)
        return (row["quota"], row["used"])

    def set_quota(self, admin_id: int, quota: int):
        self.conn.execute(
            """
            INSERT INTO admin_invite_quotas(admin_id, quota, used)
            VALUES(?,?,0)
            ON CONFLICT(admin_id) DO UPDATE SET quota=excluded.quota
            """,
            (admin_id, quota)
        )
        self.conn.commit()

    def inc_quota_used(self, admin_id: int):
        self.conn.execute("UPDATE admin_invite_quotas SET used = used + 1 WHERE admin_id=?", (admin_id,))
        self.conn.commit()

    # --- Pending authorisations
    def save_auth_secret(self, secret_hash: str, created_by: int):
        self.conn.execute(
            "INSERT OR REPLACE INTO pending_authorizations(secret_hash, created_by) VALUES(?,?)",
            (secret_hash, created_by)
        )
        self.conn.commit()

    def pop_auth_secret(self, secret_hash: str) -> Optional[sqlite3.Row]:
        cur = self.conn.cursor()
        row = cur.execute("SELECT * FROM pending_authorizations WHERE secret_hash=?", (secret_hash,)).fetchone()
        if row:
            cur.execute("DELETE FROM pending_authorizations WHERE secret_hash=?", (secret_hash,))
            self.conn.commit()
        return row

    # --- Audit
    def log_audit(self, actor_id: int, action: str, target: str, details: str = ""):
        self.conn.execute(
            "INSERT INTO audit_log(actor_id, action, target, details) VALUES(?,?,?,?)",
            (actor_id, action, target, details)
        )
        self.conn.commit()

    # --- Settings helpers
    def get_setting_int(self, key: str, default: int) -> int:
        row = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if not row or row["value"] is None:
            return default
        try:
            return int(row["value"])
        except Exception:
            return default

    def set_setting_int(self, key: str, value: int):
        self.conn.execute(
            "INSERT INTO settings(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(int(value)))
        )
        self.conn.commit()

    # --- Messages and IDs
    def save_message(self, chat_id: int, message_id: int, sender_id: int,
                     sender_username: str, sender_first_name: str, date: float,
                     text: str, media_type: str, file_id: str, is_forward: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO messages(chat_id, message_id, sender_id, sender_username,
                        sender_first_name, date, text, media_type, file_id, is_forward)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (chat_id, message_id, sender_id, sender_username, sender_first_name,
             date, text, media_type, file_id, is_forward)
        )
        self.conn.commit()
        row = cur.execute("SELECT id FROM messages WHERE chat_id=? AND message_id=?", (chat_id, message_id)).fetchone()
        return row["id"] if row else 0

    def update_message_text(self, chat_id: int, message_id: int, text: str):
        self.conn.execute("UPDATE messages SET text=? WHERE chat_id=? AND message_id=?", (text, chat_id, message_id))
        self.conn.commit()

    def link_male_ids(self, message_db_id: int, male_ids: Iterable[str]):
        for mid in set(male_ids):
            try:
                self.conn.execute(
                    "INSERT OR IGNORE INTO message_male_ids(message_id_ref, male_id) VALUES(?,?)",
                    (message_db_id, mid)
                )
            except Exception:
                pass
        self.conn.commit()

    def unlink_all_male_ids(self, message_db_id: int):
        self.conn.execute("DELETE FROM message_male_ids WHERE message_id_ref=?", (message_db_id,))
        self.conn.commit()

    # --- Allowed chats
    def add_allowed_chat(self, chat_id: int, title: str, female_id: str, added_by: int):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO allowed_chats(chat_id, title, female_id, added_by)
            VALUES(?,?,?,?)
            """,
            (chat_id, title, female_id, added_by)
        )
        self.conn.commit()

    def remove_allowed_chat(self, chat_id: int):
        self.conn.execute("DELETE FROM allowed_chats WHERE chat_id=?", (chat_id,))
        self.conn.commit()

    def get_allowed_chat(self, chat_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM allowed_chats WHERE chat_id=?", (chat_id,)).fetchone()

    def list_allowed_chats(self) -> List[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM allowed_chats ORDER BY added_at DESC").fetchall()

    def list_chats_by_admin(self, admin_id: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM allowed_chats WHERE added_by=? ORDER BY added_at DESC",
            (admin_id,)
        ).fetchall()

    def count_chats_by_admin(self, admin_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM allowed_chats WHERE added_by=?",
            (admin_id,)
        ).fetchone()
        return row["c"] if row else 0

    def get_female_id_from_title(self, title: str) -> Optional[str]:
        if not title:
            return None
        m = re.search(r"(?:^|[^0-9])([0-9]{10})(?:[^0-9]|$)", title)
        return m.group(1) if m else None

    # --- Female legends
    def get_female_legend(self, female_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT female_id, chat_id, content, message_id, updated_at FROM female_legends WHERE female_id=?",
            (female_id,)
        ).fetchone()

    def upsert_female_legend(self, female_id: str, chat_id: int, content: str, message_id: Optional[int]):
        self.conn.execute(
            """
            INSERT INTO female_legends(female_id, chat_id, content, message_id)
            VALUES(?,?,?,?)
            ON CONFLICT(female_id) DO UPDATE SET
                chat_id=excluded.chat_id,
                content=excluded.content,
                message_id=excluded.message_id,
                updated_at=CURRENT_TIMESTAMP
            """,
            (female_id, chat_id, content, message_id)
        )
        self.conn.commit()

    # --- Searches and stats
    def log_search(self, user_id: int, query_type: str, query_value: str):
        self.conn.execute(
            "INSERT INTO searches(user_id, query_type, query_value) VALUES(?,?,?)",
            (user_id, query_type, query_value)
        )
        self.conn.commit()

    def get_user_searches(self, user_id: int, limit: int = 10) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM searches WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()

    def search_by_male(self, male_id: str, limit: int = 5, offset: int = 0,
                       female_id: Optional[str] = None, since_ts: Optional[float] = None) -> List[sqlite3.Row]:
        params = [male_id]
        extra = ""
        if female_id:
            extra += " AND ac.female_id = ?"
            params.append(female_id)
        if since_ts:
            extra += " AND m.date >= ?"
            params.append(since_ts)
        params.extend([limit, offset])
        return self.conn.execute(
            f"""
            SELECT m.*, mm.male_id, ac.female_id AS female_id
            FROM messages m
            JOIN message_male_ids mm ON mm.message_id_ref = m.id
            LEFT JOIN allowed_chats ac ON ac.chat_id = m.chat_id
            WHERE mm.male_id = ? {extra}
            ORDER BY m.date DESC
            LIMIT ? OFFSET ?
            """,
            params
        ).fetchall()

    def count_by_male(self, male_id: str, female_id: Optional[str] = None,
                      since_ts: Optional[float] = None) -> int:
        params = [male_id]
        extra = ""
        if female_id:
            extra += " AND ac.female_id = ?"
            params.append(female_id)
        if since_ts:
            extra += " AND m.date >= ?"
            params.append(since_ts)
        row = self.conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM messages m
            JOIN message_male_ids mm ON mm.message_id_ref = m.id
            LEFT JOIN allowed_chats ac ON ac.chat_id = m.chat_id
            WHERE mm.male_id = ? {extra}
            """,
            params
        ).fetchone()
        return row["c"] if row else 0

    def list_females_for_male(self, male_id: str) -> List[str]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT ac.female_id
            FROM messages m
            JOIN message_male_ids mm ON mm.message_id_ref = m.id
            LEFT JOIN allowed_chats ac ON ac.chat_id = m.chat_id
            WHERE mm.male_id = ?
              AND ac.female_id IS NOT NULL
              AND ac.female_id <> ''
            ORDER BY ac.female_id
            """,
            (male_id,)
        ).fetchall()
        return [r["female_id"] for r in rows]

    def count_reports_by_female(self, female_id: str, since_ts: float) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(DISTINCT m.id) AS c
            FROM messages m
            JOIN allowed_chats ac ON ac.chat_id = m.chat_id
            JOIN message_male_ids mm ON mm.message_id_ref = m.id
            WHERE ac.female_id = ?
              AND m.date >= ?
              AND (m.media_type IS NULL OR m.media_type = '' OR m.media_type = 'text')
            """,
            (female_id, since_ts)
        ).fetchone()
        return row["c"] if row else 0

    def get_reports_by_female(self, female_id: str, since_ts: float, limit: int, offset: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT m.id,
                   m.chat_id,
                   m.message_id,
                   m.text,
                   m.date,
                   GROUP_CONCAT(DISTINCT mm.male_id) AS male_ids
            FROM messages m
            JOIN allowed_chats ac ON ac.chat_id = m.chat_id
            JOIN message_male_ids mm ON mm.message_id_ref = m.id
            WHERE ac.female_id = ?
              AND m.date >= ?
              AND (m.media_type IS NULL OR m.media_type = '' OR m.media_type = 'text')
            GROUP BY m.id
            ORDER BY m.date DESC
            LIMIT ? OFFSET ?
            """,
            (female_id, since_ts, limit, offset)
        ).fetchall()

    def count_stats(self) -> Tuple[int, int, int, int]:
        """Return statistics: unique male IDs, total messages, allowed chats, unique female IDs."""
        men = self.conn.execute("SELECT COUNT(DISTINCT male_id) AS c FROM message_male_ids").fetchone()["c"]
        msgs = self.conn.execute("SELECT COUNT(*) AS c FROM messages").fetchone()["c"]
        chats = self.conn.execute("SELECT COUNT(*) AS c FROM allowed_chats").fetchone()["c"]
        females = self.conn.execute("SELECT COUNT(DISTINCT female_id) AS c FROM allowed_chats").fetchone()["c"]
        return (men, msgs, chats, females)

    def list_admins(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT a.user_id, u.username, u.first_name, u.last_name FROM admins a LEFT JOIN users u ON u.user_id=a.user_id ORDER BY a.user_id"
        ).fetchall()

    def list_users_by_admin(self, admin_id: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT au.user_id, au.username_lc, au.credits, au.added_at,
                   u.username, u.first_name, u.last_name
            FROM allowed_users au
            LEFT JOIN users u ON u.user_id = au.user_id
            WHERE au.added_by=?
            ORDER BY au.added_at DESC
            """,
            (admin_id,)
        ).fetchall()

    def count_messages_by_user(self, user_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE sender_id=?",
            (user_id,)
        ).fetchone()
        return row["c"] if row else 0

    def list_user_chats(self, user_id: int) -> List[sqlite3.Row]:
        """Return distinct chats where the user has sent messages, with titles if known."""
        return self.conn.execute(
            """
            SELECT DISTINCT m.chat_id,
                            COALESCE(ac.title, '') AS title,
                            COALESCE(ac.female_id, '') AS female_id
            FROM messages m
            LEFT JOIN allowed_chats ac ON ac.chat_id = m.chat_id
            WHERE m.sender_id=?
            ORDER BY m.chat_id
            """,
            (user_id,)
        ).fetchall()

    def count_users_by_admin(self, admin_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM allowed_users WHERE added_by=?",
            (admin_id,)
        ).fetchone()
        return row["c"] if row else 0

    def top_males(self, limit: int = 10) -> List[Tuple[str, int]]:
        return self.conn.execute(
            "SELECT male_id, COUNT(*) AS cnt FROM message_male_ids GROUP BY male_id ORDER BY cnt DESC LIMIT ?",
            (limit,)
        ).fetchall()

    def top_females(self, limit: int = 10) -> List[Tuple[str, int]]:
        return self.conn.execute(
            "SELECT female_id, COUNT(*) AS cnt FROM allowed_chats GROUP BY female_id ORDER BY cnt DESC LIMIT ?",
            (limit,)
        ).fetchall()

    def top_chats(self, limit: int = 10) -> List[Tuple[int, int]]:
        return self.conn.execute(
            "SELECT chat_id, COUNT(*) AS cnt FROM messages GROUP BY chat_id ORDER BY cnt DESC LIMIT ?",
            (limit,)
        ).fetchall()

    def count_messages_in_chat(self, chat_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE chat_id=?",
            (chat_id,)
        ).fetchone()
        return row["c"] if row else 0

    def count_unique_males_in_chat(self, chat_id: int) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(DISTINCT mm.male_id) AS c
            FROM message_male_ids mm
            JOIN messages m ON m.id = mm.message_id_ref
            WHERE m.chat_id = ?
            """,
            (chat_id,)
        ).fetchone()
        return row["c"] if row else 0

    

    # --- Rate limiting
    def rate_limit_allowed(self, user_id: int, now_ts: int, min_interval: int = 2) -> bool:
        """Return True if the user may perform an action (search) based on a
        simple per‑user minimum interval.  Updates the timestamp when
        allowed.
        """
        row = self.conn.execute("SELECT last_action_ts FROM ratelimits WHERE user_id=?", (user_id,)).fetchone()
        if row is None:
            self.conn.execute("INSERT OR REPLACE INTO ratelimits(user_id, last_action_ts) VALUES(?,?)", (user_id, now_ts))
            self.conn.commit()
            return True
        last_ts = row["last_action_ts"]
        if now_ts - last_ts < min_interval:
            return False
        self.conn.execute("UPDATE ratelimits SET last_action_ts=? WHERE user_id=?", (now_ts, user_id))
        self.conn.commit()
        return True

    # --- Username reservations (admin adds by @username; activates on /start)
    def reserve_username(self, username_lc: str, added_by: int) -> bool:
        """Reserve a username (lowercased). Returns True if created, False if already exists."""
        try:
            self.conn.execute(
                "INSERT INTO reserved_usernames(username_lc, added_by) VALUES(?, ?)",
                (username_lc.lower(), added_by),
            )
            self.conn.commit()
            return True
        except Exception:
            return False

    def consume_reserved_username(self, username_lc: str) -> bool:
        """If a reservation exists for this username, delete it and return True; else False."""
        cur = self.conn.cursor()
        cur.execute(
            "DELETE FROM reserved_usernames WHERE username_lc=?",
            (username_lc.lower(),),
        )
        self.conn.commit()
        return cur.rowcount > 0

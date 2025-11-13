PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Key/value storage for misc settings
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- List of superadmins/admins.  The superadmin is defined in .env as OWNER_ID and
-- inserted on startup; additional admins may be added via the admin UI.
CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chats that are allowed for indexing.  Only chats in this table will have
-- messages recorded.  Each chat holds a 10‑digit "female" identifier which
-- is parsed from the chat title.  The added_by column records which admin
-- authorised the chat.
CREATE TABLE IF NOT EXISTS allowed_chats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    female_id TEXT,
    added_by INTEGER,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Per-female legend entries that the bot can post/pin inside chats.
CREATE TABLE IF NOT EXISTS female_legends (
    female_id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    message_id INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(chat_id) REFERENCES allowed_chats(chat_id) ON DELETE CASCADE
);

-- History of legend messages observed in chats (for automatic updates)
CREATE TABLE IF NOT EXISTS legend_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    female_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Registered bot users.  A user must be in this table in order to issue
-- searches.  Credits represent the number of ID searches the user can
-- perform; credits are decremented on every search and incremented when
-- the user contributes messages containing male IDs.  banned_until holds
-- a timestamp until which the user may not perform searches (used for
-- automated rate‑limit bans).
CREATE TABLE IF NOT EXISTS allowed_users (
    user_id INTEGER PRIMARY KEY,
    username_lc TEXT,
    added_by INTEGER,
    credits INTEGER DEFAULT 100,
    banned_until TIMESTAMP,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Users table holds persisted profile information such as names and locale.
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    lang TEXT,
    is_blocked INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Invitations are one‑time tokens for onboarding new users.  A token is
-- represented by its hashed value (token_hash).  TTL defines how long the
-- token is valid.  When a user consumes an invitation via /start TOKEN the
-- row is marked used and linked to that user.
CREATE TABLE IF NOT EXISTS invitations (
    token_hash TEXT PRIMARY KEY,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ttl_seconds INTEGER DEFAULT 3600,
    used_by INTEGER,
    used_at TIMESTAMP,
    is_used INTEGER DEFAULT 0
);

-- Invite quotas define how many invitations each admin may generate.  The
-- used field counts how many invites have been consumed.  Superadmins may
-- modify quotas via the admin interface.
CREATE TABLE IF NOT EXISTS admin_invite_quotas (
    admin_id INTEGER PRIMARY KEY,
    quota INTEGER DEFAULT 20,
    used INTEGER DEFAULT 0
);

-- Audit log for sensitive actions (adding chats, users, exports).  Each
-- entry records who performed the action, what action was taken, and
-- details such as target identifiers or file names.
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id INTEGER,
    action TEXT,
    target TEXT,
    details TEXT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Secrets used when authorising new chats.  Secrets are stored by their
-- hashed value for security.  When a secret is consumed it is removed.
CREATE TABLE IF NOT EXISTS pending_authorizations (
    secret_hash TEXT PRIMARY KEY,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Messages recorded from allowed chats.  Each message entry contains
-- metadata about the sender and the original Telegram message identifiers.
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sender_id INTEGER,
    sender_username TEXT,
    sender_first_name TEXT,
    date TIMESTAMP,
    text TEXT,
    media_type TEXT,
    file_id TEXT,
    is_forward INTEGER DEFAULT 0,
    UNIQUE(chat_id, message_id)
);

-- Link table connecting messages to male IDs.  A single message may
-- reference multiple male IDs.
CREATE TABLE IF NOT EXISTS message_male_ids (
    message_id_ref INTEGER NOT NULL,
    male_id TEXT NOT NULL,
    UNIQUE(message_id_ref, male_id),
    FOREIGN KEY(message_id_ref) REFERENCES messages(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_male_id ON message_male_ids(male_id);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, message_id);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);

-- Log of all search queries.  query_type may be 'male' or 'female';
-- query_value holds the ID being searched.
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    query_type TEXT,
    query_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Simple rate limit store; last_action_ts is updated per user on every
-- handled search.  Separate automated bans live in allowed_users.banned_until.
CREATE TABLE IF NOT EXISTS ratelimits (
    user_id INTEGER PRIMARY KEY,
    last_action_ts INTEGER
);

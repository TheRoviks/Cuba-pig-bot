import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any


class QuoteDatabase:
    SCHEMA_VERSION = "2026-03-15-db-audit-1"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _has_column(self, conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(row["name"] == column_name for row in rows)

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        if self._has_column(conn, table_name, column_name):
            return

        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")

    def _dump_payload(self, payload: Any) -> str | None:
        if payload is None:
            return None
        if isinstance(payload, str):
            return payload
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def init_db(self) -> None:
        now = datetime.now().isoformat(timespec="seconds")

        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT NOT NULL,
                    is_bot INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_private_seen_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    quote_text TEXT NOT NULL,
                    quoted_user_id INTEGER,
                    quoted_name TEXT,
                    quoted_username TEXT,
                    saved_by_user_id INTEGER,
                    saved_by_name TEXT,
                    created_at TEXT NOT NULL,
                    original_message_id INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    chat_type TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    added_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_chat_links (
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    user_name TEXT,
                    username TEXT,
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, chat_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS quote_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    quote_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    actor_user_id INTEGER,
                    event_type TEXT NOT NULL,
                    event_payload TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS usage_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    chat_id INTEGER,
                    command_name TEXT NOT NULL,
                    is_private INTEGER NOT NULL DEFAULT 0,
                    payload TEXT,
                    used_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_user_id INTEGER NOT NULL,
                    action_name TEXT NOT NULL,
                    target_chat_id INTEGER,
                    target_user_id INTEGER,
                    payload TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            self._ensure_column(
                conn,
                "quotes",
                "is_deleted",
                "is_deleted INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                "quotes",
                "deleted_at",
                "deleted_at TEXT",
            )
            self._ensure_column(
                conn,
                "quotes",
                "deleted_by_user_id",
                "deleted_by_user_id INTEGER",
            )

            conn.execute("DROP INDEX IF EXISTS idx_quotes_unique_message")

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_last_seen_at ON users(last_seen_at DESC)"
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_id ON quotes(chat_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_id_id ON quotes(chat_id, id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_created ON quotes(chat_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_user ON quotes(chat_id, quoted_user_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_username ON quotes(chat_id, quoted_username)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_saved_by ON quotes(chat_id, saved_by_user_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_deleted ON quotes(chat_id, is_deleted)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_created_global ON quotes(created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_deleted_at ON quotes(deleted_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quotes_chat_message ON quotes(chat_id, original_message_id)"
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_quotes_unique_message
                ON quotes(chat_id, original_message_id)
                WHERE original_message_id IS NOT NULL
                """
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chats_is_active ON chats(is_active)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chats_last_seen ON chats(last_seen_at DESC)"
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_chat_links_chat_id ON user_chat_links(chat_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_chat_links_user_id ON user_chat_links(user_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_chat_links_last_seen ON user_chat_links(user_id, last_seen_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quote_events_quote_id ON quote_events(quote_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quote_events_chat_created ON quote_events(chat_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_quote_events_type_created ON quote_events(event_type, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_usage_logs_user_id ON usage_logs(user_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_usage_logs_chat_id ON usage_logs(chat_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_usage_logs_command_name ON usage_logs(command_name)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_audit_logs_admin ON admin_audit_logs(admin_user_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_audit_logs_action ON admin_audit_logs(action_name, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_audit_logs_target_chat ON admin_audit_logs(target_chat_id)"
            )
            conn.execute(
                """
                INSERT INTO app_meta (meta_key, meta_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(meta_key) DO UPDATE SET
                    meta_value = excluded.meta_value,
                    updated_at = excluded.updated_at
                """,
                ("schema_version", self.SCHEMA_VERSION, now),
            )

            conn.commit()

    def upsert_user(
        self,
        user_id: int,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
        full_name: str,
        is_bot: bool,
        timestamp: str,
        *,
        last_private_seen_at: str | None = None,
    ) -> None:
        safe_full_name = (full_name or "Unknown user").strip() or "Unknown user"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users (
                    user_id,
                    username,
                    first_name,
                    last_name,
                    full_name,
                    is_bot,
                    first_seen_at,
                    last_seen_at,
                    last_private_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    full_name = excluded.full_name,
                    is_bot = excluded.is_bot,
                    last_seen_at = excluded.last_seen_at,
                    last_private_seen_at = COALESCE(excluded.last_private_seen_at, users.last_private_seen_at)
                """,
                (
                    user_id,
                    username,
                    first_name,
                    last_name,
                    safe_full_name,
                    int(is_bot),
                    timestamp,
                    timestamp,
                    last_private_seen_at,
                ),
            )
            conn.commit()

    def get_app_meta(self, meta_key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT meta_value FROM app_meta WHERE meta_key = ? LIMIT 1",
                (meta_key,),
            ).fetchone()
        return row["meta_value"] if row else None

    def upsert_chat(
        self,
        chat_id: int,
        title: str,
        chat_type: str,
        is_active: int,
        timestamp: str,
    ) -> None:
        safe_title = (title or "Untitled").strip() or "Untitled"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO chats (
                    chat_id,
                    title,
                    chat_type,
                    is_active,
                    added_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    title = excluded.title,
                    chat_type = excluded.chat_type,
                    is_active = excluded.is_active,
                    last_seen_at = excluded.last_seen_at
                """,
                (chat_id, safe_title, chat_type, is_active, timestamp, timestamp),
            )
            conn.commit()

    def upsert_user_chat_link(
        self,
        user_id: int,
        chat_id: int,
        user_name: str,
        username: str | None,
        timestamp: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_chat_links (
                    user_id,
                    chat_id,
                    user_name,
                    username,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET
                    user_name = excluded.user_name,
                    username = excluded.username,
                    last_seen_at = excluded.last_seen_at
                """,
                (user_id, chat_id, user_name, username, timestamp),
            )
            conn.commit()

    def find_duplicate(
        self,
        chat_id: int,
        original_message_id: int | None,
        quote_text: str,
        quoted_user_id: int | None = None,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            if original_message_id is not None:
                row = conn.execute(
                    """
                    SELECT *
                    FROM quotes
                    WHERE chat_id = ?
                      AND original_message_id = ?
                    LIMIT 1
                    """,
                    (chat_id, original_message_id),
                ).fetchone()
                if row:
                    return dict(row)

            if quoted_user_id is None:
                return None

            row = conn.execute(
                """
                SELECT *
                FROM quotes
                WHERE chat_id = ?
                  AND quoted_user_id = ?
                  AND is_deleted = 0
                  AND LOWER(TRIM(quote_text)) = LOWER(TRIM(?))
                LIMIT 1
                """,
                (chat_id, quoted_user_id, quote_text),
            ).fetchone()

        return dict(row) if row else None

    def add_quote(
        self,
        chat_id: int,
        quote_text: str,
        quoted_user_id: int | None,
        quoted_name: str,
        quoted_username: str | None,
        saved_by_user_id: int | None,
        saved_by_name: str,
        created_at: str,
        original_message_id: int | None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO quotes (
                    chat_id,
                    quote_text,
                    quoted_user_id,
                    quoted_name,
                    quoted_username,
                    saved_by_user_id,
                    saved_by_name,
                    created_at,
                    original_message_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    quote_text,
                    quoted_user_id,
                    quoted_name,
                    quoted_username,
                    saved_by_user_id,
                    saved_by_name,
                    created_at,
                    original_message_id,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def get_random_quote(self, chat_id: int, exclude_id: int | None = None) -> dict[str, Any] | None:
        query = """
            SELECT *
            FROM quotes
            WHERE chat_id = ?
              AND is_deleted = 0
        """
        params: list[Any] = [chat_id]

        if exclude_id is not None:
            query += " AND id != ?"
            params.append(exclude_id)

        query += " ORDER BY RANDOM() LIMIT 1"

        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def get_quote_by_id(self, chat_id: int, quote_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM quotes
                WHERE id = ?
                  AND chat_id = ?
                  AND is_deleted = 0
                LIMIT 1
                """,
                (quote_id, chat_id),
            ).fetchone()
        return dict(row) if row else None

    def get_quotes_by_saved_user(
        self,
        chat_id: int,
        user_id: int,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM quotes
                WHERE chat_id = ?
                  AND saved_by_user_id = ?
                  AND is_deleted = 0
                ORDER BY id DESC
                LIMIT ?
                """,
                (chat_id, user_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_latest_quotes(self, chat_id: int, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM quotes
                WHERE chat_id = ?
                  AND is_deleted = 0
                ORDER BY id DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_all_quotes(self, chat_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM quotes
                WHERE chat_id = ?
                  AND is_deleted = 0
                ORDER BY id ASC
                """,
                (chat_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def search_quotes(
        self,
        chat_id: int,
        query_text: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM quotes
                WHERE chat_id = ?
                  AND is_deleted = 0
                  AND quote_text LIKE ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (chat_id, f"%{query_text}%", limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_quotes_by_person(
        self,
        chat_id: int,
        *,
        quoted_user_id: int | None = None,
        quoted_username: str | None = None,
        name_part: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM quotes
            WHERE chat_id = ?
              AND is_deleted = 0
        """
        params: list[Any] = [chat_id]

        if quoted_user_id is not None:
            query += " AND quoted_user_id = ?"
            params.append(quoted_user_id)
        elif quoted_username:
            query += " AND LOWER(quoted_username) = LOWER(?)"
            params.append(quoted_username.lstrip("@"))
        elif name_part:
            query += " AND LOWER(quoted_name) LIKE LOWER(?)"
            params.append(f"%{name_part}%")
        else:
            return []

        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def count_quotes(self, chat_id: int) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM quotes
                WHERE chat_id = ?
                  AND is_deleted = 0
                """,
                (chat_id,),
            ).fetchone()
        return int(row["total"]) if row else 0

    def get_top_quoted(self, chat_id: int, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    quoted_user_id,
                    quoted_name,
                    quoted_username,
                    COUNT(*) AS total
                FROM quotes
                WHERE chat_id = ?
                  AND is_deleted = 0
                GROUP BY quoted_user_id, quoted_name, quoted_username
                ORDER BY total DESC, MAX(id) DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def delete_quote(
        self,
        chat_id: int,
        quote_id: int,
        deleted_by_user_id: int | None = None,
    ) -> bool:
        deleted_at = datetime.now().isoformat(timespec="seconds")

        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE quotes
                SET is_deleted = 1,
                    deleted_at = ?,
                    deleted_by_user_id = ?
                WHERE id = ?
                  AND chat_id = ?
                  AND is_deleted = 0
                """,
                (deleted_at, deleted_by_user_id, quote_id, chat_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def get_shared_chats(self, user_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.chat_id,
                    c.title,
                    c.chat_type,
                    c.last_seen_at,
                    u.user_name,
                    u.username,
                    u.last_seen_at AS user_last_seen_at
                FROM chats c
                JOIN user_chat_links u ON u.chat_id = c.chat_id
                WHERE u.user_id = ?
                  AND c.is_active = 1
                  AND c.chat_type IN ('group', 'supergroup')
                ORDER BY u.last_seen_at DESC, c.title COLLATE NOCASE ASC
                """,
                (user_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_shared_chat(self, user_id: int, chat_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    c.chat_id,
                    c.title,
                    c.chat_type,
                    c.last_seen_at,
                    u.user_name,
                    u.username,
                    u.last_seen_at AS user_last_seen_at
                FROM chats c
                JOIN user_chat_links u ON u.chat_id = c.chat_id
                WHERE u.user_id = ?
                  AND c.chat_id = ?
                  AND c.is_active = 1
                  AND c.chat_type IN ('group', 'supergroup')
                LIMIT 1
                """,
                (user_id, chat_id),
            ).fetchone()
        return dict(row) if row else None

    def get_user_quote_summary(self, user_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    q.chat_id,
                    c.title,
                    COUNT(*) AS total
                FROM quotes q
                JOIN chats c ON c.chat_id = q.chat_id
                JOIN user_chat_links u ON u.chat_id = q.chat_id
                WHERE q.quoted_user_id = ?
                  AND u.user_id = ?
                  AND q.is_deleted = 0
                  AND c.is_active = 1
                GROUP BY q.chat_id, c.title
                ORDER BY total DESC, MAX(q.id) DESC
                """,
                (user_id, user_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_user_quotes_in_chat(
        self,
        user_id: int,
        chat_id: int,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT q.*
                FROM quotes q
                JOIN chats c ON c.chat_id = q.chat_id
                JOIN user_chat_links u
                  ON u.chat_id = q.chat_id
                 AND u.user_id = ?
                WHERE q.quoted_user_id = ?
                  AND q.chat_id = ?
                  AND q.is_deleted = 0
                  AND c.is_active = 1
                ORDER BY q.id DESC
                LIMIT ?
                """,
                (user_id, user_id, chat_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_accessible_chat_quotes(
        self,
        user_id: int,
        chat_id: int,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT q.*
            FROM quotes q
            JOIN chats c ON c.chat_id = q.chat_id
            JOIN user_chat_links u
              ON u.chat_id = q.chat_id
             AND u.user_id = ?
            WHERE q.chat_id = ?
              AND q.is_deleted = 0
              AND c.is_active = 1
            ORDER BY q.id ASC
        """
        params: list[Any] = [user_id, chat_id]

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def log_quote_event(
        self,
        quote_id: int,
        chat_id: int,
        actor_user_id: int | None,
        event_type: str,
        event_payload: Any = None,
        created_at: str | None = None,
    ) -> None:
        timestamp = created_at or datetime.now().isoformat(timespec="seconds")

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO quote_events (
                    quote_id,
                    chat_id,
                    actor_user_id,
                    event_type,
                    event_payload,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    quote_id,
                    chat_id,
                    actor_user_id,
                    event_type,
                    self._dump_payload(event_payload),
                    timestamp,
                ),
            )
            conn.commit()

    def log_command_usage(
        self,
        user_id: int | None,
        chat_id: int | None,
        command_name: str,
        is_private: bool,
        payload: Any = None,
        used_at: str | None = None,
    ) -> None:
        timestamp = used_at or datetime.now().isoformat(timespec="seconds")

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO usage_logs (
                    user_id,
                    chat_id,
                    command_name,
                    is_private,
                    payload,
                    used_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    chat_id,
                    command_name,
                    int(is_private),
                    self._dump_payload(payload),
                    timestamp,
                ),
            )
            conn.commit()

    def log_admin_action(
        self,
        admin_user_id: int,
        action_name: str,
        target_chat_id: int | None = None,
        target_user_id: int | None = None,
        payload: Any = None,
        created_at: str | None = None,
    ) -> None:
        timestamp = created_at or datetime.now().isoformat(timespec="seconds")

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO admin_audit_logs (
                    admin_user_id,
                    action_name,
                    target_chat_id,
                    target_user_id,
                    payload,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    admin_user_id,
                    action_name,
                    target_chat_id,
                    target_user_id,
                    self._dump_payload(payload),
                    timestamp,
                ),
            )
            conn.commit()

    def get_total_chats(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM chats").fetchone()
        return int(row["total"]) if row else 0

    def get_active_chats_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM chats
                WHERE is_active = 1
                  AND chat_type IN ('group', 'supergroup')
                """
            ).fetchone()
        return int(row["total"]) if row else 0

    def get_total_seen_users(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM users"
            ).fetchone()
        return int(row["total"]) if row else 0

    def get_quotes_count_last_days(self, days: int) -> int:
        threshold = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM quotes
                WHERE created_at >= ?
                  AND is_deleted = 0
                """,
                (threshold,),
            ).fetchone()
        return int(row["total"]) if row else 0

    def get_top_chats_by_quotes(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.chat_id,
                    c.title,
                    c.is_active,
                    COUNT(q.id) AS total
                FROM chats c
                LEFT JOIN quotes q
                  ON q.chat_id = c.chat_id
                 AND q.is_deleted = 0
                GROUP BY c.chat_id, c.title, c.is_active
                ORDER BY total DESC, c.last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_top_global_savers(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    saved_by_user_id,
                    saved_by_name,
                    COUNT(*) AS total
                FROM quotes
                WHERE saved_by_user_id IS NOT NULL
                  AND is_deleted = 0
                GROUP BY saved_by_user_id, saved_by_name
                ORDER BY total DESC, MAX(id) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_top_global_quoted(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    quoted_user_id,
                    quoted_name,
                    quoted_username,
                    COUNT(*) AS total
                FROM quotes
                WHERE is_deleted = 0
                GROUP BY quoted_user_id, quoted_name, quoted_username
                ORDER BY total DESC, MAX(id) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_top_users_by_chat_membership(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    user_id,
                    user_name,
                    username,
                    COUNT(*) AS chat_count
                FROM user_chat_links
                GROUP BY user_id, user_name, username
                ORDER BY chat_count DESC, MAX(last_seen_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_user_chat_count(self, user_id: int) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM user_chat_links WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return int(row["total"]) if row else 0

    def get_all_chats_with_stats(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.chat_id,
                    c.title,
                    c.chat_type,
                    c.is_active,
                    c.added_at,
                    c.last_seen_at,
                    COUNT(q.id) AS quote_count
                FROM chats c
                LEFT JOIN quotes q
                  ON q.chat_id = c.chat_id
                 AND q.is_deleted = 0
                GROUP BY c.chat_id, c.title, c.chat_type, c.is_active, c.added_at, c.last_seen_at
                ORDER BY c.last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_chat_stats(self, chat_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    c.chat_id,
                    c.title,
                    c.chat_type,
                    c.is_active,
                    c.added_at,
                    c.last_seen_at,
                    COUNT(DISTINCT q.id) AS quote_count,
                    COUNT(DISTINCT u.user_id) AS seen_users_count
                FROM chats c
                LEFT JOIN quotes q
                  ON q.chat_id = c.chat_id
                 AND q.is_deleted = 0
                LEFT JOIN user_chat_links u
                  ON u.chat_id = c.chat_id
                WHERE c.chat_id = ?
                GROUP BY c.chat_id, c.title, c.chat_type, c.is_active, c.added_at, c.last_seen_at
                LIMIT 1
                """,
                (chat_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_top_savers_in_chat(self, chat_id: int, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    saved_by_user_id,
                    saved_by_name,
                    COUNT(*) AS total
                FROM quotes
                WHERE chat_id = ?
                  AND saved_by_user_id IS NOT NULL
                  AND is_deleted = 0
                GROUP BY saved_by_user_id, saved_by_name
                ORDER BY total DESC, MAX(id) DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_admin_health_stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute("SELECT 1").fetchone()
            users_total = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
            chats_total = conn.execute("SELECT COUNT(*) AS total FROM chats").fetchone()
            links_total = conn.execute("SELECT COUNT(*) AS total FROM user_chat_links").fetchone()
            quotes_total = conn.execute("SELECT COUNT(*) AS total FROM quotes").fetchone()
            quote_events_total = conn.execute("SELECT COUNT(*) AS total FROM quote_events").fetchone()
            usage_logs_total = conn.execute("SELECT COUNT(*) AS total FROM usage_logs").fetchone()
            admin_logs_total = conn.execute("SELECT COUNT(*) AS total FROM admin_audit_logs").fetchone()

        return {
            "db_ok": True,
            "db_path": self.db_path,
            "schema_version": self.get_app_meta("schema_version") or "unknown",
            "users_total": int(users_total["total"]) if users_total else 0,
            "chats_total": int(chats_total["total"]) if chats_total else 0,
            "links_total": int(links_total["total"]) if links_total else 0,
            "quotes_total": int(quotes_total["total"]) if quotes_total else 0,
            "quote_events_total": int(quote_events_total["total"]) if quote_events_total else 0,
            "usage_logs_total": int(usage_logs_total["total"]) if usage_logs_total else 0,
            "admin_logs_total": int(admin_logs_total["total"]) if admin_logs_total else 0,
        }

    def get_global_stats(self) -> dict[str, Any]:
        return {
            "total_chats": self.get_total_chats(),
            "active_chats": self.get_active_chats_count(),
            "total_seen_users": self.get_total_seen_users(),
            "total_quotes": self.count_all_quotes(),
            "quotes_last_24h": self.get_quotes_count_last_days(1),
            "quotes_last_7d": self.get_quotes_count_last_days(7),
            "top_chats": self.get_top_chats_by_quotes(5),
            "top_savers": self.get_top_global_savers(5),
            "top_quoted": self.get_top_global_quoted(5),
        }

    def count_all_quotes(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM quotes WHERE is_deleted = 0"
            ).fetchone()
        return int(row["total"]) if row else 0

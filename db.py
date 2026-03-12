import os
import secrets
import string
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    database_url = os.getenv("DATABASE_URL")

    if database_url:
        return psycopg2.connect(database_url)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("PGDATABASE")

    if not all([host, port, user, password, dbname]):
        raise RuntimeError(
            "Database environment variables are missing. "
            "Expected DATABASE_URL or PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE."
        )

    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )


def init_db():
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_keys (
                        user_id BIGINT PRIMARY KEY,
                        key_value TEXT NOT NULL UNIQUE,
                        expires_at TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_keys_key_value
                    ON user_keys (key_value);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_keys_expires_at
                    ON user_keys (expires_at);
                """)
    finally:
        conn.close()


def generate_key(length=24):
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def get_active_key_for_user(user_id: int):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT user_id, key_value, expires_at, created_at
                FROM user_keys
                WHERE user_id = %s
                  AND expires_at > NOW()
                ORDER BY expires_at DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def create_or_replace_key_for_user(user_id: int, hours_valid: int = 24, max_retries: int = 5):
    last_error = None

    for attempt in range(max_retries):
        new_key = generate_key()
        expires_at = datetime.now(timezone.utc) + timedelta(hours=hours_valid)

        conn = None
        try:
            conn = get_db_connection()

            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        INSERT INTO user_keys (user_id, key_value, expires_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id)
                        DO UPDATE SET
                            key_value = EXCLUDED.key_value,
                            expires_at = EXCLUDED.expires_at
                        RETURNING user_id, key_value, expires_at, created_at;
                        """,
                        (user_id, new_key, expires_at),
                    )
                    row = cur.fetchone()
                    return row

        except psycopg2.IntegrityError as e:
            last_error = e
            print(
                f"Key collision on attempt {attempt + 1}/{max_retries} for user {user_id}: {repr(e)}"
            )
            time.sleep(0.05)
            continue

        except Exception as e:
            last_error = e
            print(f"create_or_replace_key_for_user error for user {user_id}: {repr(e)}")
            raise

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    raise RuntimeError(
        f"Failed to generate a unique key after {max_retries} attempts: {repr(last_error)}"
    )


def is_key_valid(key_value: str):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT user_id, key_value, expires_at
                FROM user_keys
                WHERE key_value = %s
                  AND expires_at > NOW()
                LIMIT 1;
                """,
                (key_value,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def cleanup_expired_keys():
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM user_keys
                    WHERE expires_at <= NOW();
                """)
                return cur.rowcount
    finally:
        conn.close()


def get_db_health():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), NOW();")
            row = cur.fetchone()
            return {
                "database": str(row[0]),
                "time": str(row[1]),
            }
    finally:
        conn.close()

import base64
import hashlib
import os
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

class DatabaseManager:
    def __init__(self, db_name="farm_tool.db", log_callback=None):
        self.db_name = db_name
        self.write_lock = threading.Lock()
        self.log = log_callback
        self.fernet = self._init_fernet()
        self.init_db()
        self.sensitive_settings = {
            "export_banned_tokens_plaintext",
            "capsolver_api_key",
            "2captcha_api_key",
            "anticaptcha_api_key",
            "anti-captcha_api_key",
        }
        self.warmup_days = 7
        self.warmup_min_limit = 1

    def get_connection(self):
        conn = sqlite3.connect(self.db_name, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _get_key_file_path(self):
        db_path = Path(self.db_name).resolve()
        return db_path.with_name(db_path.name + ".key")

    def _load_key_from_file(self, path: Path):
        try:
            value = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None
        except OSError:
            return None
        return value or None

    def _save_key_to_file(self, path: Path, key: str):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(key, encoding="utf-8")
        except OSError as exc:
            raise RuntimeError(f"Nie mozna zapisac klucza szyfrowania w {path}: {exc}") from exc

    def _build_fernet(self, key: str, source: str):
        try:
            return Fernet(key)
        except (ValueError, TypeError):
            if len(key) == 32:
                encoded_key = base64.urlsafe_b64encode(key.encode("utf-8"))
                return Fernet(encoded_key)
            raise RuntimeError(f"Klucz szyfrowania ({source}) ma nieprawidlowy format.")

    def _init_fernet(self):
        key = os.getenv("TOKEN_ENCRYPTION_KEY", "").strip()
        source = "TOKEN_ENCRYPTION_KEY"
        if not key:
            key_path = self._get_key_file_path()
            key = self._load_key_from_file(key_path)
            source = str(key_path)
            if not key:
                key = Fernet.generate_key().decode("utf-8")
                self._save_key_to_file(key_path, key)
        return self._build_fernet(key, source)

    def _encrypt_token(self, token):
        if token.startswith("enc:"):
            return token
        encrypted = self.fernet.encrypt(token.encode("utf-8")).decode("utf-8")
        return f"enc:{encrypted}"

    def _decrypt_token(self, token):
        if not token.startswith("enc:"):
            return token
        encrypted = token[4:]
        try:
            return self.fernet.decrypt(encrypted.encode("utf-8")).decode("utf-8")
        except InvalidToken:
            if self.log:
                self.log("[Accounts] NieprawidĹ‚owy klucz szyfrowania tokenĂłw.")
            return None

    def init_db(self):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            # Tabela kont (zachowujemy kolumnÄ™ proxy)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    token TEXT UNIQUE NOT NULL,
                    proxy TEXT,
                    status TEXT DEFAULT 'Active',
                    daily_limit INTEGER DEFAULT 15,
                    sent_today INTEGER DEFAULT 0,
                    last_use TIMESTAMP,
                    join_daily_limit INTEGER DEFAULT 5,
                    join_today INTEGER DEFAULT 0,
                    join_last_use TIMESTAMP,
                    created_at TIMESTAMP
                )
            ''')
            self._ensure_account_columns(conn)
            self.migrate_plaintext_tokens(conn)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS last_dm (
                    account_id INTEGER NOT NULL,
                    target_user_id TEXT NOT NULL,
                    last_sent_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (account_id, target_user_id)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_last_dm_target
                ON last_dm (target_user_id)
            ''')
            # Tabela celĂłw
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT UNIQUE NOT NULL,
                    platform TEXT NOT NULL,
                    status TEXT DEFAULT 'Pending',
                    error_msg TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            conn.commit()
            conn.close()

    def _ensure_account_columns(self, conn):
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(accounts)")
        existing = {row[1] for row in cursor.fetchall()}
        if "join_daily_limit" not in existing:
            cursor.execute("ALTER TABLE accounts ADD COLUMN join_daily_limit INTEGER DEFAULT 5")
        if "join_today" not in existing:
            cursor.execute("ALTER TABLE accounts ADD COLUMN join_today INTEGER DEFAULT 0")
        if "join_last_use" not in existing:
            cursor.execute("ALTER TABLE accounts ADD COLUMN join_last_use TIMESTAMP")
        if "created_at" not in existing:
            cursor.execute("ALTER TABLE accounts ADD COLUMN created_at TIMESTAMP")

    def _get_effective_daily_limit(self, base_limit, created_at):
        if base_limit is None:
            return 0
        if not created_at:
            return base_limit
        try:
            created_dt = datetime.fromisoformat(str(created_at))
        except (TypeError, ValueError):
            return base_limit
        if self.warmup_days <= 0:
            return base_limit
        age_seconds = max(0.0, (datetime.now() - created_dt).total_seconds())
        ratio = min(1.0, age_seconds / (self.warmup_days * 86400.0))
        warmed = int(base_limit * ratio)
        warmed = max(self.warmup_min_limit, warmed)
        return min(base_limit, warmed)

    def migrate_plaintext_tokens(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT id, token FROM accounts")
        rows = cursor.fetchall()
        for acc_id, token in rows:
            if not token or token.startswith("enc:"):
                continue
            encrypted = self._encrypt_token(token)
            cursor.execute("UPDATE accounts SET token = ? WHERE id = ?", (encrypted, acc_id))

    def add_account(self, platform, token, proxy="", limit=15, join_limit=5, status="Active"):
        conn = None
        try:
            with self.write_lock:
                conn = self.get_connection()
                cursor = conn.cursor()
                encrypted_token = self._encrypt_token(token)
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute('''
                    INSERT INTO accounts (platform, token, proxy, status, daily_limit, join_daily_limit, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (platform, encrypted_token, proxy, status, limit, join_limit, created_at))
                conn.commit()
                return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            if conn:
                conn.close()

    def get_active_accounts(self, platform):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, platform, token, proxy, status, daily_limit, sent_today, last_use,
                   join_daily_limit, join_today, join_last_use, created_at
            FROM accounts
            WHERE platform = ? AND status = "Active"
        ''', (platform,))
        accounts = []
        for acc in cursor.fetchall():
            acc_list = list(acc)
            decrypted = self._decrypt_token(acc_list[2])
            if not decrypted:
                if self.log:
                    self.log(f"[Accounts] Pomijam konto {acc_list[0]} - nieprawidĹ‚owy klucz szyfrowania.")
                continue
            acc_list[2] = decrypted
            base_limit = acc_list[5]
            created_at = acc_list[11]
            acc_list[5] = self._get_effective_daily_limit(base_limit, created_at)
            acc_list = acc_list[:11]
            accounts.append(tuple(acc_list))
        conn.close()
        return accounts

    def get_account_token(self, account_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT token FROM accounts WHERE id = ?", (account_id,))
        row = cursor.fetchone()
        conn.close()
        if not row or not row[0]:
            return None
        decrypted = self._decrypt_token(row[0])
        if not decrypted and self.log:
            self.log(f"[Accounts] NieprawidĹ‚owy klucz szyfrowania dla konta {account_id}.")
        return decrypted

    def reset_daily_counters(self, reference_datetime=None):
        if reference_datetime is None:
            reference_datetime = datetime.now()
        reference_date = reference_datetime.strftime("%Y-%m-%d")
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE accounts
                SET sent_today = 0
                WHERE sent_today > 0
                  AND (last_use IS NULL OR date(last_use) < date(?))
            ''', (reference_date,))
            cursor.execute('''
                UPDATE accounts
                SET join_today = 0
                WHERE join_today > 0
                  AND (join_last_use IS NULL OR date(join_last_use) < date(?))
            ''', (reference_date,))
            conn.commit()
            conn.close()

    def add_targets(self, user_ids, platform):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            for uid in user_ids:
                try:
                    cursor.execute('INSERT INTO targets (user_id, platform) VALUES (?, ?)', (uid, platform))
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
            conn.close()

    def get_next_target(self, platform, min_target_interval_seconds=0):
        conn = self.get_connection()
        cursor = conn.cursor()
        if min_target_interval_seconds and min_target_interval_seconds > 0:
            cutoff = (datetime.now() - timedelta(seconds=min_target_interval_seconds)).strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''
                SELECT t.id, t.user_id
                FROM targets t
                LEFT JOIN (
                    SELECT target_user_id, MAX(last_sent_at) AS last_sent_at
                    FROM last_dm
                    GROUP BY target_user_id
                ) l ON l.target_user_id = t.user_id
                WHERE t.platform = ? AND t.status = "Pending"
                  AND (l.last_sent_at IS NULL OR l.last_sent_at < ?)
                ORDER BY t.id ASC
                LIMIT 1
            ''', (platform, cutoff))
        else:
            cursor.execute(
                'SELECT id, user_id FROM targets WHERE platform = ? AND status = "Pending" LIMIT 1',
                (platform,),
            )
        target = cursor.fetchone()
        conn.close()
        return target

    def get_last_dm_for_account(self, account_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(last_sent_at) FROM last_dm WHERE account_id = ?', (account_id,))
        row = cursor.fetchone()
        conn.close()
        if not row or not row[0]:
            return None
        return row[0]

    def get_account_dm_cooldown(self, account_id, min_interval_seconds):
        if min_interval_seconds <= 0:
            return 0.0
        last_sent = self.get_last_dm_for_account(account_id)
        if not last_sent:
            return 0.0
        try:
            last_dt = datetime.fromisoformat(str(last_sent))
        except (TypeError, ValueError):
            return 0.0
        elapsed = (datetime.now() - last_dt).total_seconds()
        remaining = float(min_interval_seconds) - elapsed
        return max(0.0, remaining)

    def record_last_dm(self, account_id, target_user_id):
        if not account_id or not target_user_id:
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO last_dm (account_id, target_user_id, last_sent_at)
                VALUES (?, ?, ?)
                ON CONFLICT(account_id, target_user_id) DO UPDATE SET last_sent_at = excluded.last_sent_at
            ''', (account_id, target_user_id, timestamp))
            conn.commit()
            conn.close()

    def update_target_status(self, target_id, status, error_msg=""):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE targets SET status = ?, error_msg = ? WHERE id = ?', (status, error_msg, target_id))
            conn.commit()
            conn.close()

    def increment_sent_counter(self, account_id):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE accounts 
                SET sent_today = sent_today + 1, last_use = ? 
                WHERE id = ?
            ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), account_id))
            conn.commit()
            conn.close()

    def increment_join_counter(self, account_id):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE accounts
                SET join_today = join_today + 1, join_last_use = ?
                WHERE id = ?
            ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), account_id))
            conn.commit()
            conn.close()

    def update_account_status(self, account_id, status):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            token_to_export = None
            if status == "Banned/Dead":
                cursor.execute("SELECT token FROM accounts WHERE id = ?", (account_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    token_to_export = self._decrypt_token(row[0])
            cursor.execute('UPDATE accounts SET status = ? WHERE id = ?', (status, account_id))
            conn.commit()
            conn.close()
        if token_to_export:
            self._append_banned_dead_token(token_to_export)

    def _append_banned_dead_token(self, token, export_path="banned_dead_tokens.txt"):
        if not token:
            return

        export_plaintext = self.get_setting("export_banned_tokens_plaintext", "").strip().lower() in {"1", "true", "yes", "on"}
        token_value = token
        if not export_plaintext:
            token_value = hashlib.sha256(token.encode("utf-8")).hexdigest()

        try:
            with open(export_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    parts = line.rstrip("\n").split("\t", 1)
                    if len(parts) == 2 and parts[1] == token_value:
                        return
        except FileNotFoundError:
            pass
        except OSError:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp}\t{token_value}\n"
        try:
            with open(export_path, "a", encoding="utf-8") as handle:
                handle.write(line)
        except OSError:
            pass

    def get_accounts_overview(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, status, proxy, daily_limit, sent_today, join_daily_limit, join_today, created_at
            FROM accounts
            ORDER BY id ASC
        ''')
        rows = cursor.fetchall()
        conn.close()
        overview = []
        for acc_id, status, proxy, dm_limit, sent_today, join_limit, join_today, created_at in rows:
            effective_limit = self._get_effective_daily_limit(dm_limit, created_at)
            overview.append((acc_id, status, proxy, effective_limit, sent_today, join_limit, join_today))
        return overview

    def reset_account_counters(self):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE accounts
                SET sent_today = 0,
                    join_today = 0,
                    last_use = NULL,
                    join_last_use = NULL
            ''')
            conn.commit()
            conn.close()

    def remove_account(self, account_id):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            cursor.execute("DELETE FROM last_dm WHERE account_id = ?", (account_id,))
            conn.commit()
            conn.close()

    def get_target_counts(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT status, COUNT(*) FROM targets GROUP BY status')
        rows = cursor.fetchall()
        cursor.execute('SELECT COUNT(*) FROM targets')
        total = cursor.fetchone()[0]
        conn.close()
        counts = {status: count for status, count in rows}
        return counts, total

    def get_targets(self, limit=50):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, status
            FROM targets
            ORDER BY id DESC
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return rows

    def remove_target(self, user_id):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM targets WHERE user_id = ?", (user_id,))
            conn.commit()
            conn.close()

    def clear_targets(self):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM targets")
            conn.commit()
            conn.close()

    def set_setting(self, key, value):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            if value is None:
                cursor.execute("DELETE FROM settings WHERE key = ?", (key,))
                conn.commit()
                conn.close()
                return
            stored = value
            if key in self.sensitive_settings and value:
                stored = self._encrypt_token(value)
            cursor.execute('''
                INSERT INTO settings (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ''', (key, stored))
            conn.commit()
            conn.close()

    def get_setting(self, key, default=""):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        if not row or row[0] is None:
            return default
        value = row[0]
        if isinstance(value, str) and value.startswith("enc:"):
            return self._decrypt_token(value)
        return value

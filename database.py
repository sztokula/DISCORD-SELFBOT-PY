import base64
import os
import sqlite3
import threading
from datetime import datetime

from cryptography.fernet import Fernet, InvalidToken

class DatabaseManager:
    def __init__(self, db_name="farm_tool.db"):
        self.db_name = db_name
        self.write_lock = threading.Lock()
        self.fernet = self._init_fernet()
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_name, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _init_fernet(self):
        key = os.getenv("TOKEN_ENCRYPTION_KEY")
        if not key:
            raise RuntimeError("Brak TOKEN_ENCRYPTION_KEY w zmiennych środowiskowych.")
        try:
            return Fernet(key)
        except (ValueError, TypeError):
            if len(key) == 32:
                encoded_key = base64.urlsafe_b64encode(key.encode("utf-8"))
                return Fernet(encoded_key)
            raise RuntimeError("TOKEN_ENCRYPTION_KEY ma nieprawidłowy format.")

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
            raise RuntimeError("Nieprawidłowy klucz szyfrowania tokenów.")

    def init_db(self):
        with self.write_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            # Tabela kont (zachowujemy kolumnę proxy)
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
                    join_last_use TIMESTAMP
                )
            ''')
            self._ensure_account_columns(conn)
            self.migrate_plaintext_tokens(conn)
            # Tabela celów
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT UNIQUE NOT NULL,
                    platform TEXT NOT NULL,
                    status TEXT DEFAULT 'Pending',
                    error_msg TEXT
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

    def migrate_plaintext_tokens(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT id, token FROM accounts")
        rows = cursor.fetchall()
        for acc_id, token in rows:
            if not token or token.startswith("enc:"):
                continue
            encrypted = self._encrypt_token(token)
            cursor.execute("UPDATE accounts SET token = ? WHERE id = ?", (encrypted, acc_id))

    def add_account(self, platform, token, proxy="", limit=15, join_limit=5):
        conn = None
        try:
            with self.write_lock:
                conn = self.get_connection()
                cursor = conn.cursor()
                encrypted_token = self._encrypt_token(token)
                cursor.execute('''
                    INSERT INTO accounts (platform, token, proxy, daily_limit, join_daily_limit)
                    VALUES (?, ?, ?, ?, ?)
                ''', (platform, encrypted_token, proxy, limit, join_limit))
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False
        finally:
            if conn:
                conn.close()

    def get_active_accounts(self, platform):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM accounts WHERE platform = ? AND status = "Active"', (platform,))
        accounts = []
        for acc in cursor.fetchall():
            acc_list = list(acc)
            acc_list[2] = self._decrypt_token(acc_list[2])
            accounts.append(tuple(acc_list))
        conn.close()
        return accounts

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

    def get_next_target(self, platform):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, user_id FROM targets WHERE platform = ? AND status = "Pending" LIMIT 1', (platform,))
        target = cursor.fetchone()
        conn.close()
        return target

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
            cursor.execute('UPDATE accounts SET status = ? WHERE id = ?', (status, account_id))
            conn.commit()
            conn.close()

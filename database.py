import sqlite3
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_name="farm_tool.db"):
        self.db_name = db_name
        self.init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_name)

    def init_db(self):
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
                last_use TIMESTAMP
            )
        ''')
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

    def add_account(self, platform, token, proxy="", limit=15):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO accounts (platform, token, proxy, daily_limit)
                VALUES (?, ?, ?, ?)
            ''', (platform, token, proxy, limit))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

    def get_active_accounts(self, platform):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM accounts WHERE platform = ? AND status = "Active"', (platform,))
        accounts = cursor.fetchall()
        conn.close()
        return accounts

    def add_targets(self, user_ids, platform):
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
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE targets SET status = ?, error_msg = ? WHERE id = ?', (status, error_msg, target_id))
        conn.commit()
        conn.close()

    def increment_sent_counter(self, account_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE accounts 
            SET sent_today = sent_today + 1, last_use = ? 
            WHERE id = ?
        ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), account_id))
        conn.commit()
        conn.close()

    def update_account_status(self, account_id, status):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE accounts SET status = ? WHERE id = ?', (status, account_id))
        conn.commit()
        conn.close()

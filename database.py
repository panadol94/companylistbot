import sqlite3
import datetime
from threading import Lock

class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.lock = Lock()
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 1. Bots Table (Child Bots)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token TEXT UNIQUE NOT NULL,
                    owner_id INTEGER NOT NULL,
                    bot_username TEXT,
                    subscription_end DATETIME,
                    is_active BOOLEAN DEFAULT 1,
                    custom_banner TEXT,
                    custom_caption TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 2. Companies Table (Content for each bot)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    media_file_id TEXT,
                    media_type TEXT, -- 'photo' or 'video'
                    button_text TEXT,
                    button_url TEXT,
                    FOREIGN KEY(bot_id) REFERENCES bots(id)
                )
            ''')

            # 3. Users Table (End users of child bots)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    balance REAL DEFAULT 0.0,
                    total_invites INTEGER DEFAULT 0,
                    referrer_id INTEGER, -- telegram_id of referrer
                    phone_number TEXT,
                    is_blacklisted BOOLEAN DEFAULT 0,
                    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bot_id, telegram_id)
                )
            ''')

            # 4. Withdrawals Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL, -- telegram_id
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'PENDING', -- PENDING, APPROVED, REJECTED
                    request_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id)
                )
            ''')

            # 5. Broadcasts Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    message TEXT,
                    media_file_id TEXT,
                    media_type TEXT,
                    status TEXT DEFAULT 'PENDING', -- PENDING, SENT, FAILED
                    scheduled_time DATETIME,
                    FOREIGN KEY(bot_id) REFERENCES bots(id)
                )
            ''')

            # 6. Forwarder Config Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarder_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    source_channel_id INTEGER,
                    target_group_id INTEGER,
                    is_active BOOLEAN DEFAULT 0,
                    FOREIGN KEY(bot_id) REFERENCES bots(id)
                )
            ''')

            conn.commit()
            conn.close()

    # --- Bot Management ---
    def create_bot(self, token, owner_id, username, trial_days=3):
        with self.lock:
            conn = self.get_connection()
            try:
                expiry = datetime.datetime.now() + datetime.timedelta(days=trial_days)
                conn.execute(
                    "INSERT INTO bots (token, owner_id, bot_username, subscription_end) VALUES (?, ?, ?, ?)",
                    (token, owner_id, username, expiry)
                )
                conn.commit()
                return True, "Bot registered successfully."
            except sqlite3.IntegrityError:
                return False, "Bot token already registered."
            except Exception as e:
                return False, str(e)
            finally:
                conn.close()

    def get_all_bots(self):
        conn = self.get_connection()
        bots = conn.execute("SELECT * FROM bots WHERE is_active = 1").fetchall()
        conn.close()
        return [dict(bot) for bot in bots]

    def get_bot_by_token(self, token):
        conn = self.get_connection()
        bot = conn.execute("SELECT * FROM bots WHERE token = ?", (token,)).fetchone()
        conn.close()
        return dict(bot) if bot else None

    def extend_subscription(self, owner_id, days):
        with self.lock:
            conn = self.get_connection()
            expiry = datetime.datetime.now() + datetime.timedelta(days=days)
            conn.execute("UPDATE bots SET subscription_end = ? WHERE owner_id = ?", (expiry, owner_id))
            conn.commit()
            conn.close()

    # --- Company Management ---
    def add_company(self, bot_id, name, desc, media, media_type, btn_text, btn_url):
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "INSERT INTO companies (bot_id, name, description, media_file_id, media_type, button_text, button_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (bot_id, name, desc, media, media_type, btn_text, btn_url)
            )
            conn.commit()
            conn.close()

    def get_companies(self, bot_id):
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM companies WHERE bot_id = ?", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def delete_company(self, company_id):
        with self.lock:
            conn = self.get_connection()
            conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))
            conn.commit()
            conn.close()

    def edit_company(self, company_id, field, value):
        with self.lock:
            conn = self.get_connection()
            query = f"UPDATE companies SET {field} = ? WHERE id = ?" # simplified for brevity, assume safe field input
            conn.execute(query, (value, company_id))
            conn.commit()
            conn.close()

    # --- User & Referral ---
    def add_user(self, bot_id, telegram_id, referrer_id=None):
        with self.lock:
            conn = self.get_connection()
            try:
                # Check if user exists
                exists = conn.execute("SELECT id FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id)).fetchone()
                if not exists:
                    conn.execute("INSERT INTO users (bot_id, telegram_id, referrer_id) VALUES (?, ?, ?)", (bot_id, telegram_id, referrer_id))
                    
                    # Reward Referrer
                    if referrer_id and referrer_id != telegram_id:
                        referrer = conn.execute("SELECT id FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, referrer_id)).fetchone()
                        if referrer:
                            conn.execute("UPDATE users SET balance = balance + 1.0, total_invites = total_invites + 1 WHERE bot_id = ? AND telegram_id = ?", (bot_id, referrer_id))
                    
                    conn.commit()
                    return True # New user added
                return False # User already exists
            except Exception as e:
                print(f"Error adding user: {e}")
                return False
            finally:
                conn.close()

    def get_user(self, bot_id, telegram_id):
        conn = self.get_connection()
        user = conn.execute("SELECT * FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id)).fetchone()
        conn.close()
        return dict(user) if user else None

    # --- Withdrawal ---
    def request_withdrawal(self, bot_id, user_id, amount):
        with self.lock:
            conn = self.get_connection()
            user = conn.execute("SELECT balance, phone_number FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, user_id)).fetchone()
            if not user or user['balance'] < amount:
                conn.close()
                return False, "Insufficient balance."
            
            if not user['phone_number']:
                conn.close()
                return False, "Please verify phone number first."

            conn.execute("INSERT INTO withdrawals (bot_id, user_id, amount) VALUES (?, ?, ?)", (bot_id, user_id, amount))
            conn.commit()
            conn.close()
            return True, "Withdrawal requested."

    def process_withdrawal(self, withdrawal_id, action):
        with self.lock:
            conn = self.get_connection()
            withdrawal = conn.execute("SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,)).fetchone()
            if not withdrawal or withdrawal['status'] != 'PENDING':
                conn.close()
                return False

            if action == 'APPROVE':
                # Deduct balance
                conn.execute("UPDATE users SET balance = balance - ? WHERE bot_id = ? AND telegram_id = ?", (withdrawal['amount'], withdrawal['bot_id'], withdrawal['user_id']))
                conn.execute("UPDATE withdrawals SET status = 'APPROVED' WHERE id = ?", (withdrawal_id,))
            else:
                conn.execute("UPDATE withdrawals SET status = 'REJECTED' WHERE id = ?", (withdrawal_id,))
            
            conn.commit()
            conn.close()
            return True
    
    def get_pending_withdrawals(self, bot_id):
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM withdrawals WHERE bot_id = ? AND status = 'PENDING'", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # --- Settings ---
    def update_phone(self, bot_id, user_id, phone):
         with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE users SET phone_number = ? WHERE bot_id = ? AND telegram_id = ?", (phone, bot_id, user_id))
            conn.commit()
            conn.close()

    def update_bot_settings(self, bot_id, banner=None, caption=None):
        with self.lock:
            conn = self.get_connection()
            if banner:
                 conn.execute("UPDATE bots SET custom_banner = ? WHERE id = ?", (banner, bot_id))
            if caption:
                 conn.execute("UPDATE bots SET custom_caption = ? WHERE id = ?", (caption, bot_id))
            conn.commit()
            conn.close()

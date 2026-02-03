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
    
    def execute_query(self, query):
        """Execute a raw SQL query and return results"""
        conn = self.get_connection()
        rows = conn.execute(query).fetchall()
        conn.close()
        return [dict(row) for row in rows]

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
                    referral_enabled BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Migration: Add referral_enabled column if missing
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN referral_enabled BOOLEAN DEFAULT 1")
            except:
                pass  # Column already exists
            
            # Migration: Add livegram_enabled column if missing
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN livegram_enabled BOOLEAN DEFAULT 1")
            except:
                pass  # Column already exists

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

            # 7. Menu Buttons Table (Custom buttons for /start menu)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS menu_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    url TEXT NOT NULL,
                    row_group INTEGER DEFAULT NULL,
                    position INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id)
                )
            ''')

            # 8. Company Buttons Table (Multiple buttons per company)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS company_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    url TEXT NOT NULL,
                    row_group INTEGER DEFAULT NULL,
                    position INTEGER DEFAULT 0,
                    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
                )
            ''')

            # 9. Bot Admins Table (Multiple admins per bot)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    added_by INTEGER NOT NULL,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                    UNIQUE(bot_id, telegram_id)
                )
            ''')

            # 10. Platform Owners Table (Dynamic owners for mother bot)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS platform_owners (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    added_by INTEGER NOT NULL,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_companies_bot_id ON companies(bot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_bot_id ON users(bot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawals_bot_id ON withdrawals(bot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id ON withdrawals(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_menu_buttons_bot_id ON menu_buttons(bot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bot_admins_bot_id ON bot_admins(bot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_buttons_company_id ON company_buttons(company_id)')

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
    
    def get_bot_by_id(self, bot_id):
        """Get bot details by bot ID"""
        conn = self.get_connection()
        bot = conn.execute("SELECT * FROM bots WHERE id = ?", (bot_id,)).fetchone()
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
    def add_company(self, bot_id, name, description, media_file_id, media_type, button_text, button_url):
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "INSERT INTO companies (bot_id, name, description, media_file_id, media_type, button_text, button_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (bot_id, name, description, media_file_id, media_type, button_text, button_url)
            )
            conn.commit()
            company_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            return company_id
    
    def delete_company(self, company_id, bot_id):
        """Delete a company - validates ownership via bot_id"""
        with self.lock:
            conn = self.get_connection()
            try:
                result = conn.execute(
                    "DELETE FROM companies WHERE id = ? AND bot_id = ?",
                    (company_id, bot_id)
                )
                conn.commit()
                deleted = result.rowcount > 0
                conn.close()
                return deleted
            except Exception as e:
                conn.close()
                return False

    def get_companies(self, bot_id):
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM companies WHERE bot_id = ?", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def edit_company(self, company_id, field, value):
        """Update a specific field of a company"""
        allowed_fields = ['name', 'description', 'media_file_id', 'media_type', 'button_text', 'button_url']
        if field not in allowed_fields:
            return False
        with self.lock:
            conn = self.get_connection()
            conn.execute(f"UPDATE companies SET {field} = ? WHERE id = ?", (value, company_id))
            conn.commit()
            conn.close()
            return True
    
    def update_welcome_settings(self, bot_id, banner_file_id, caption_text):
        """Update custom banner and caption for a bot"""
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "UPDATE bots SET custom_banner = ?, custom_caption = ? WHERE id = ?",
                (banner_file_id, caption_text, bot_id)
            )
            conn.commit()
            conn.close()

    def add_withdrawal(self, bot_id, user_id, amount, method, account):
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "INSERT INTO withdrawals (bot_id, user_id, amount, payment_method, payment_account) VALUES (?, ?, ?, ?, ?)",
                (bot_id, user_id, amount, method, account)
            )
            conn.commit()
            conn.close()
            
    def get_last_withdrawal(self, bot_id, user_id):
        """Get most recent withdrawal for cooldown check"""
        conn = self.get_connection()
        withdraw = conn.execute(
            "SELECT * FROM withdrawals WHERE bot_id = ? AND user_id = ? ORDER BY request_date DESC LIMIT 1",
            (bot_id, user_id)
        ).fetchone()
        conn.close()
        return dict(withdraw) if withdraw else None

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
    
    def get_users(self, bot_id):
        """Get all users for a bot (for broadcast)"""
        conn = self.get_connection()
        users = conn.execute("SELECT * FROM users WHERE bot_id = ?", (bot_id,)).fetchall()
        conn.close()
        return [dict(user) for user in users]
    
    def get_top_referrers(self, bot_id, limit=10):
        """Get top referrers by invite count for leaderboard"""
        conn = self.get_connection()
        top_users = conn.execute(
            """SELECT telegram_id, total_invites, balance 
               FROM users 
               WHERE bot_id = ? AND total_invites > 0
               ORDER BY total_invites DESC 
               LIMIT ?""",
            (bot_id, limit)
        ).fetchall()
        conn.close()
        return [dict(u) for u in top_users]
    
    def get_user_rank(self, bot_id, user_id):
        """Get user's rank in referral leaderboard"""
        conn = self.get_connection()
        # Count how many users have more invites
        rank = conn.execute(
            """SELECT COUNT(*) + 1 as rank 
               FROM users 
               WHERE bot_id = ? AND total_invites > (
                   SELECT total_invites FROM users WHERE bot_id = ? AND telegram_id = ?
               )""",
            (bot_id, bot_id, user_id)
        ).fetchone()
        conn.close()
        return rank['rank'] if rank else None

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

    def toggle_referral(self, bot_id):
        """Toggle referral system on/off for a bot"""
        with self.lock:
            conn = self.get_connection()
            # Get current state
            current = conn.execute("SELECT referral_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
            new_state = 0 if current and current['referral_enabled'] else 1
            conn.execute("UPDATE bots SET referral_enabled = ? WHERE id = ?", (new_state, bot_id))
            conn.commit()
            conn.close()
            return new_state  # Returns new state (1=ON, 0=OFF)

    def is_referral_enabled(self, bot_id):
        """Check if referral system is enabled for a bot"""
        conn = self.get_connection()
        bot = conn.execute("SELECT referral_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        return bool(bot['referral_enabled']) if bot else True  # Default True

    def toggle_livegram(self, bot_id):
        """Toggle livegram system on/off for a bot"""
        with self.lock:
            conn = self.get_connection()
            # Get current state
            current = conn.execute("SELECT livegram_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
            new_state = 0 if current and current['livegram_enabled'] else 1
            conn.execute("UPDATE bots SET livegram_enabled = ? WHERE id = ?", (new_state, bot_id))
            conn.commit()
            conn.close()
            return new_state  # Returns new state (1=ON, 0=OFF)

    def is_livegram_enabled(self, bot_id):
        """Check if livegram system is enabled for a bot"""
        conn = self.get_connection()
        bot = conn.execute("SELECT livegram_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        return bool(bot['livegram_enabled']) if bot and 'livegram_enabled' in bot.keys() else True  # Default True

    # --- Menu Buttons ---
    def add_menu_button(self, bot_id, text, url):
        """Add a custom button to the start menu"""
        with self.lock:
            conn = self.get_connection()
            # Get next position
            max_pos = conn.execute("SELECT MAX(position) as max_pos FROM menu_buttons WHERE bot_id = ?", (bot_id,)).fetchone()
            position = (max_pos['max_pos'] or 0) + 1
            
            conn.execute(
                "INSERT INTO menu_buttons (bot_id, text, url, position) VALUES (?, ?, ?, ?)",
                (bot_id, text, url, position)
            )
            conn.commit()
            button_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            return button_id

    def get_menu_buttons(self, bot_id):
        """Get all custom menu buttons for a bot, ordered by position"""
        conn = self.get_connection()
        buttons = conn.execute(
            "SELECT * FROM menu_buttons WHERE bot_id = ? ORDER BY row_group NULLS LAST, position",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [dict(btn) for btn in buttons]

    def delete_menu_button(self, button_id, bot_id):
        """Delete a menu button"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute(
                "DELETE FROM menu_buttons WHERE id = ? AND bot_id = ?",
                (button_id, bot_id)
            )
            conn.commit()
            deleted = result.rowcount > 0
            conn.close()
            return deleted

    def edit_menu_button(self, button_id, field, value):
        """Edit a menu button field"""
        allowed_fields = ['text', 'url']
        if field not in allowed_fields:
            return False
        with self.lock:
            conn = self.get_connection()
            conn.execute(f"UPDATE menu_buttons SET {field} = ? WHERE id = ?", (value, button_id))
            conn.commit()
            conn.close()
            return True

    def pair_buttons(self, button1_id, button2_id, bot_id):
        """Pair two buttons to share the same row"""
        with self.lock:
            conn = self.get_connection()
            # Get next row_group number
            max_group = conn.execute("SELECT MAX(row_group) as max_grp FROM menu_buttons WHERE bot_id = ?", (bot_id,)).fetchone()
            new_group = (max_group['max_grp'] or 0) + 1
            
            # Update both buttons with same row_group
            conn.execute("UPDATE menu_buttons SET row_group = ? WHERE id = ? AND bot_id = ?", (new_group, button1_id, bot_id))
            conn.execute("UPDATE menu_buttons SET row_group = ? WHERE id = ? AND bot_id = ?", (new_group, button2_id, bot_id))
            conn.commit()
            conn.close()
            return new_group

    def unpair_button(self, button_id, bot_id):
        """Unpair a button (set row_group to NULL)"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE menu_buttons SET row_group = NULL WHERE id = ? AND bot_id = ?", (button_id, bot_id))
            conn.commit()
            conn.close()
            return True

    def get_menu_button(self, button_id):
        """Get a single menu button by ID"""
        conn = self.get_connection()
        button = conn.execute("SELECT * FROM menu_buttons WHERE id = ?", (button_id,)).fetchone()
        conn.close()
        return dict(button) if button else None

    # --- Company Buttons ---
    def add_company_button(self, company_id, text, url):
        """Add a button to a company"""
        with self.lock:
            conn = self.get_connection()
            # Get next position
            max_pos = conn.execute("SELECT MAX(position) as max_pos FROM company_buttons WHERE company_id = ?", (company_id,)).fetchone()
            position = (max_pos['max_pos'] or 0) + 1
            
            conn.execute(
                "INSERT INTO company_buttons (company_id, text, url, position) VALUES (?, ?, ?, ?)",
                (company_id, text, url, position)
            )
            conn.commit()
            button_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            return button_id

    def get_company_buttons(self, company_id):
        """Get all buttons for a company, ordered for proper display"""
        conn = self.get_connection()
        buttons = conn.execute(
            "SELECT * FROM company_buttons WHERE company_id = ? ORDER BY row_group NULLS LAST, position",
            (company_id,)
        ).fetchall()
        conn.close()
        return [dict(btn) for btn in buttons]

    def delete_company_buttons(self, company_id):
        """Delete all buttons for a company"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("DELETE FROM company_buttons WHERE company_id = ?", (company_id,))
            conn.commit()
            conn.close()

    def pair_company_buttons(self, btn1_id, btn2_id):
        """Pair two company buttons to share the same row"""
        with self.lock:
            conn = self.get_connection()
            # Get next row_group number
            max_group = conn.execute("SELECT MAX(row_group) as max_grp FROM company_buttons").fetchone()
            new_group = (max_group['max_grp'] or 0) + 1
            
            # Update both buttons with same row_group
            conn.execute("UPDATE company_buttons SET row_group = ? WHERE id = ?", (new_group, btn1_id))
            conn.execute("UPDATE company_buttons SET row_group = ? WHERE id = ?", (new_group, btn2_id))
            conn.commit()
            conn.close()
            return new_group

    # --- Scheduled Broadcasts ---
    def save_scheduled_broadcast(self, bot_id, message, media_file_id, media_type, scheduled_time):
        """Save a scheduled broadcast"""
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "INSERT INTO broadcasts (bot_id, message, media_file_id, media_type, status, scheduled_time) VALUES (?, ?, ?, ?, 'PENDING', ?)",
                (bot_id, message, media_file_id, media_type, scheduled_time)
            )
            conn.commit()
            broadcast_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            return broadcast_id

    def get_pending_broadcasts(self, bot_id=None):
        """Get all pending scheduled broadcasts, optionally filtered by bot_id"""
        conn = self.get_connection()
        if bot_id:
            rows = conn.execute(
                "SELECT * FROM broadcasts WHERE bot_id = ? AND status = 'PENDING' ORDER BY scheduled_time",
                (bot_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM broadcasts WHERE status = 'PENDING' ORDER BY scheduled_time"
            ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def mark_broadcast_sent(self, broadcast_id):
        """Mark a broadcast as sent"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE broadcasts SET status = 'SENT' WHERE id = ?", (broadcast_id,))
            conn.commit()
            conn.close()

    def delete_scheduled_broadcast(self, broadcast_id, bot_id):
        """Delete a scheduled broadcast"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute(
                "DELETE FROM broadcasts WHERE id = ? AND bot_id = ? AND status = 'PENDING'",
                (broadcast_id, bot_id)
            )
            conn.commit()
            deleted = result.rowcount > 0
            conn.close()
            return deleted

    def delete_all_scheduled_broadcasts(self, bot_id):
        """Delete all pending scheduled broadcasts for a bot"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute(
                "DELETE FROM broadcasts WHERE bot_id = ? AND status = 'PENDING'",
                (bot_id,)
            )
            conn.commit()
            deleted = result.rowcount
            conn.close()
            return deleted

    # ==================== BOT ADMINS ====================
    
    def add_admin(self, bot_id, telegram_id, added_by):
        """Add an admin to a bot"""
        conn = self.get_connection()
        try:
            conn.execute(
                "INSERT INTO bot_admins (bot_id, telegram_id, added_by) VALUES (?, ?, ?)",
                (bot_id, telegram_id, added_by)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Already admin
        finally:
            conn.close()
    
    def remove_admin(self, bot_id, telegram_id):
        """Remove an admin from a bot"""
        conn = self.get_connection()
        result = conn.execute(
            "DELETE FROM bot_admins WHERE bot_id = ? AND telegram_id = ?",
            (bot_id, telegram_id)
        )
        conn.commit()
        deleted = result.rowcount > 0
        conn.close()
        return deleted
    
    def get_admins(self, bot_id):
        """Get all admins for a bot"""
        conn = self.get_connection()
        admins = conn.execute(
            "SELECT * FROM bot_admins WHERE bot_id = ? ORDER BY added_at",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [dict(a) for a in admins]
    
    def is_bot_admin(self, bot_id, telegram_id):
        """Check if user is admin of a bot (includes owner)"""
        conn = self.get_connection()
        # Check if owner
        bot = conn.execute("SELECT owner_id FROM bots WHERE id = ?", (bot_id,)).fetchone()
        if bot and bot['owner_id'] == telegram_id:
            conn.close()
            return True
        # Check if in admins table
        admin = conn.execute(
            "SELECT id FROM bot_admins WHERE bot_id = ? AND telegram_id = ?",
            (bot_id, telegram_id)
        ).fetchone()
        conn.close()
        return admin is not None

    # ==================== PLATFORM OWNERS ====================
    
    def add_platform_owner(self, telegram_id, added_by):
        """Add a platform owner (for mother bot)"""
        conn = self.get_connection()
        try:
            conn.execute(
                "INSERT INTO platform_owners (telegram_id, added_by) VALUES (?, ?)",
                (telegram_id, added_by)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Already owner
        finally:
            conn.close()
    
    def remove_platform_owner(self, telegram_id):
        """Remove a platform owner"""
        conn = self.get_connection()
        result = conn.execute(
            "DELETE FROM platform_owners WHERE telegram_id = ?",
            (telegram_id,)
        )
        conn.commit()
        deleted = result.rowcount > 0
        conn.close()
        return deleted
    
    def get_platform_owners(self):
        """Get all platform owners"""
        conn = self.get_connection()
        owners = conn.execute(
            "SELECT * FROM platform_owners ORDER BY added_at"
        ).fetchall()
        conn.close()
        return [dict(o) for o in owners]
    
    def is_platform_owner(self, telegram_id, master_admin_id=None):
        """Check if user is platform owner (includes master admin from env)"""
        # Check if master admin from env variable
        if master_admin_id and telegram_id == master_admin_id:
            return True
        # Check in database
        conn = self.get_connection()
        owner = conn.execute(
            "SELECT id FROM platform_owners WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()
        conn.close()
        return owner is not None

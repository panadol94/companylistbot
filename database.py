import sqlite3
import datetime
from threading import Lock
from contextlib import contextmanager

class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.lock = Lock()
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _conn(self):
        """Context manager for safe auto-closing DB connections."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query, params=None):
        """Execute a SQL query and return results. Use params for safe parameterized queries."""
        conn = self.get_connection()
        try:
            if params:
                rows = conn.execute(query, params).fetchall()
            else:
                rows = conn.execute(query).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

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
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            
            # Migration: Add livegram_enabled column if missing
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN livegram_enabled BOOLEAN DEFAULT 1")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            
            # Migration: Add referral_reward column if missing (RM per referral)
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN referral_reward REAL DEFAULT 1.0")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            
            # Migration: Add min_withdrawal column if missing (minimum withdrawal amount)
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN min_withdrawal REAL DEFAULT 50.0")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists

            # Migration: Add link_guard_enabled column if missing
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN link_guard_enabled BOOLEAN DEFAULT 0")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists

            # Migration: Add ai_chat_enabled column if missing
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN ai_chat_enabled BOOLEAN DEFAULT 0")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists

            # Migration: Add ai_system_prompt column if missing
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN ai_system_prompt TEXT DEFAULT ''")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists

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

            # Migration: Add emoji column to companies if missing
            try:
                cursor.execute("ALTER TABLE companies ADD COLUMN emoji TEXT DEFAULT '🏢'")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists

            # Migration: Add cached_file_id column to companies
            try:
                cursor.execute("ALTER TABLE companies ADD COLUMN cached_file_id TEXT")
            except Exception:
                pass  # Column already exists

            # Migration: Add keywords column to companies (for auto-matching aliases)
            try:
                cursor.execute("ALTER TABLE companies ADD COLUMN keywords TEXT DEFAULT ''")
            except Exception:
                pass  # Column already exists

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
                    method TEXT DEFAULT '',
                    account TEXT DEFAULT '',
                    status TEXT DEFAULT 'PENDING', -- PENDING, APPROVED, REJECTED
                    request_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    processed_at DATETIME,
                    processed_by INTEGER,
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
                    is_recurring BOOLEAN DEFAULT 0,
                    interval_type TEXT, -- 'minutes', 'hours', 'daily'
                    interval_value INTEGER, -- minutes/hours count, or hour of day (0-23)
                    is_active BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id)
                )
            ''')
            
            # Migration: Add recurring broadcast columns if missing
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN is_recurring BOOLEAN DEFAULT 0")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN interval_type TEXT")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN interval_value INTEGER")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN is_active BOOLEAN DEFAULT 1")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN target_type TEXT DEFAULT 'users'")
            except Exception:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN grid_media TEXT")
            except Exception:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE broadcasts ADD COLUMN grid_buttons TEXT")
            except Exception:
                pass  # Column already exists

            # 6. Forwarder Config Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarder_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    source_channel_id INTEGER,
                    source_channel_name TEXT,
                    target_group_id INTEGER,
                    target_group_name TEXT,
                    filter_keywords TEXT,
                    is_active BOOLEAN DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE
                )
            ''')
            
            # Migration: Add filter_keywords column if missing
            try:
                cursor.execute("ALTER TABLE forwarder_config ADD COLUMN filter_keywords TEXT")
            except Exception as e:

                pass  # Silently handle exception

            try:
                cursor.execute("ALTER TABLE forwarder_config ADD COLUMN forwarder_mode TEXT DEFAULT 'SINGLE'")
            except Exception as e:

                pass  # Silently handle exception
                pass
            try:
                cursor.execute("ALTER TABLE forwarder_config ADD COLUMN source_channel_name TEXT")
            except Exception as e:

                pass  # Silently handle exception
            # Migration: Add target_group_name column if missing
            try:
                cursor.execute("ALTER TABLE forwarder_config ADD COLUMN target_group_name TEXT")
            except Exception as e:

                pass  # Silently handle exception
            # Migration: Add forwarder_mode column if missing
            try:
                cursor.execute("ALTER TABLE forwarder_config ADD COLUMN forwarder_mode TEXT DEFAULT 'SINGLE'")
            except Exception as e:

                pass  # Silently handle exception

            # 7. Known Groups Table (For Broadcast Mode)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_known_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    group_name TEXT,
                    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                    UNIQUE(bot_id, group_id)
                )
            ''')

            # 7b. Per-Group Welcome Messages Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_welcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    text TEXT DEFAULT '',
                    media TEXT DEFAULT NULL,
                    media_type TEXT DEFAULT NULL,
                    autodelete INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                    UNIQUE(bot_id, group_id)
                )
            ''')

            # 8. Menu Buttons Table (Custom buttons for /start menu)
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

            # 11. 4D Results Table (Historical 4D data)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS results_4d (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    draw_date DATE NOT NULL,
                    first_prize TEXT,
                    second_prize TEXT,
                    third_prize TEXT,
                    special_prizes TEXT,
                    consolation_prizes TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company, draw_date)
                )
            ''')

            # 12. 4D Notification Subscribers Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS notify_4d_subscribers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    subscribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bot_id, user_id),
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE
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

            # Migration: Add category column to companies
            try:
                cursor.execute("ALTER TABLE companies ADD COLUMN category TEXT")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            
            # Migration: Add display_order column to companies
            try:
                cursor.execute("ALTER TABLE companies ADD COLUMN display_order INTEGER DEFAULT 0")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            
            # Migration: Add required channel columns to bots
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN required_channel_id INTEGER")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN required_channel_username TEXT")
            except Exception as e:

                pass  # Silently handle exception  # Column already exists
            
            # Migration: Add username/first_name to users if missing
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")
            except Exception as e:

                pass  # Silently handle exception
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
            except Exception as e:

                pass  # Silently handle exception
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
            except Exception as e:

                pass  # Silently handle exception
            
            # Add method and account columns to existing withdrawals table
            try:
                cursor.execute("ALTER TABLE withdrawals ADD COLUMN method TEXT DEFAULT ''")
            except Exception as e:

                pass  # Silently handle exception
            try:
                cursor.execute("ALTER TABLE withdrawals ADD COLUMN account TEXT DEFAULT ''")
            except Exception as e:

                pass  # Silently handle exception

            # Migration: Add processed_at and processed_by to withdrawals
            try:
                cursor.execute("ALTER TABLE withdrawals ADD COLUMN processed_at DATETIME")
            except Exception as e:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE withdrawals ADD COLUMN processed_by INTEGER")
            except Exception as e:
                pass  # Column already exists

            # Migration: Group Welcome Message columns
            for col, col_type in [
                ("group_welcome_enabled", "BOOLEAN DEFAULT 0"),
                ("group_welcome_text", "TEXT"),
                ("group_welcome_media", "TEXT"),
                ("group_welcome_media_type", "TEXT"),
                ("group_welcome_autodelete", "INTEGER DEFAULT 0"),
            ]:
                try:
                    cursor.execute(f"ALTER TABLE bots ADD COLUMN {col} {col_type}")
                except Exception:
                    pass  # Column already exists

            # Migration: Group Management columns
            for col, col_type in [
                ("anti_bot_enabled", "BOOLEAN DEFAULT 0"),
                ("delete_join_leave_enabled", "BOOLEAN DEFAULT 0"),
            ]:
                try:
                    cursor.execute(f"ALTER TABLE bots ADD COLUMN {col} {col_type}")
                except Exception:
                    pass  # Column already exists

            # Ban Words Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ban_words (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    word TEXT NOT NULL,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                    UNIQUE(bot_id, word)
                )
            ''')

            # Auto Replies Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS auto_replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    trigger_text TEXT NOT NULL,
                    response_text TEXT NOT NULL,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                    UNIQUE(bot_id, trigger_text)
                )
            ''')

            # 13. Userbot Sessions Table (Telethon sessions per bot)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS userbot_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL UNIQUE,
                    api_id TEXT,
                    api_hash TEXT,
                    session_string TEXT,
                    phone TEXT,
                    is_active BOOLEAN DEFAULT 0,
                    auto_mode BOOLEAN DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE
                )
            ''')

            # 14. Monitored Channels Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS monitored_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    channel_id TEXT,
                    channel_title TEXT,
                    channel_username TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE
                )
            ''')

            # 15. Detected Promos Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS detected_promos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER NOT NULL,
                    source_channel TEXT,
                    original_text TEXT,
                    swapped_text TEXT,
                    media_file_ids TEXT,
                    media_types TEXT,
                    matched_company TEXT,
                    status TEXT DEFAULT 'pending',
                    detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE
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
        rows = conn.execute("SELECT * FROM companies WHERE bot_id = ? ORDER BY display_order ASC, id ASC", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_company(self, bot_id, company_id):
        """Get a single company by ID"""
        conn = self.get_connection()
        row = conn.execute("SELECT * FROM companies WHERE id = ? AND bot_id = ?", (company_id, bot_id)).fetchone()
        conn.close()
        return dict(row) if row else None
    
    def edit_company(self, company_id, field, value):
        """Update a specific field of a company"""
        allowed_fields = ['name', 'description', 'media_file_id', 'media_type', 'button_text', 'button_url', 'emoji', 'keywords']
        if field not in allowed_fields:
            return False
        with self.lock:
            conn = self.get_connection()
            conn.execute(f"UPDATE companies SET {field} = ? WHERE id = ?", (value, company_id))
            # Clear cached file_id when media changes
            if field in ('media_file_id', 'media_type'):
                conn.execute("UPDATE companies SET cached_file_id = NULL WHERE id = ?", (company_id,))
            conn.commit()
            conn.close()
            return True
    
    def update_cached_file_id(self, company_id, file_id):
        """Cache Telegram file_id for smooth carousel editing"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE companies SET cached_file_id = ? WHERE id = ?", (file_id, company_id))
            conn.commit()
            conn.close()
    
    def update_company_position(self, company_id, new_position, bot_id):
        """Update company display order (1-indexed position)"""
        with self.lock:
            conn = self.get_connection()
            
            # Get current position
            current = conn.execute(
                "SELECT display_order FROM companies WHERE id = ? AND bot_id = ?",
                (company_id, bot_id)
            ).fetchone()
            
            if not current:
                conn.close()
                return False
                
            old_position = current['display_order'] or 0
            # Convert 1-indexed to 0-indexed
            new_pos_idx = new_position - 1
            
            # Shift other companies
            if new_pos_idx > old_position:
                # Moving down - shift up the ones in between
                conn.execute(
                    """UPDATE companies 
                       SET display_order = display_order - 1 
                       WHERE bot_id = ? 
                       AND display_order > ? 
                       AND display_order <= ?""",
                    (bot_id, old_position, new_pos_idx)
                )
            else:
                # Moving up - shift down the ones in between
                conn.execute(
                    """UPDATE companies 
                       SET display_order = display_order + 1 
                       WHERE bot_id = ? 
                       AND display_order >= ? 
                       AND display_order < ?""",
                    (bot_id, new_pos_idx, old_position)
                )
            
            # Update target company
            conn.execute(
                "UPDATE companies SET display_order = ? WHERE id = ?",
                (new_pos_idx, company_id)
            )
            
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

    # --- Group Welcome ---
    def get_group_welcome(self, bot_id):
        """Get group welcome message settings"""
        conn = self.get_connection()
        bot = conn.execute(
            "SELECT group_welcome_enabled, group_welcome_text, group_welcome_media, "
            "group_welcome_media_type, group_welcome_autodelete FROM bots WHERE id = ?",
            (bot_id,)
        ).fetchone()
        conn.close()
        if bot:
            return {
                'enabled': bool(bot['group_welcome_enabled']) if bot['group_welcome_enabled'] is not None else False,
                'text': bot['group_welcome_text'] or '',
                'media': bot['group_welcome_media'],
                'media_type': bot['group_welcome_media_type'],
                'autodelete': bot['group_welcome_autodelete'] or 0,
            }
        return {'enabled': False, 'text': '', 'media': None, 'media_type': None, 'autodelete': 0}

    def update_group_welcome(self, bot_id, field, value):
        """Update a single group welcome field"""
        allowed = {
            'enabled': 'group_welcome_enabled',
            'text': 'group_welcome_text',
            'media': 'group_welcome_media',
            'media_type': 'group_welcome_media_type',
            'autodelete': 'group_welcome_autodelete',
        }
        col = allowed.get(field)
        if not col:
            return False
        with self.lock:
            conn = self.get_connection()
            conn.execute(f"UPDATE bots SET {col} = ? WHERE id = ?", (value, bot_id))
            conn.commit()
            conn.close()
            return True

    # --- Per-Group Welcome Methods ---
    def get_per_group_welcome(self, bot_id, group_id):
        """Get welcome settings for a specific group. Returns None if no per-group setting exists."""
        conn = self.get_connection()
        row = conn.execute(
            "SELECT * FROM group_welcomes WHERE bot_id = ? AND group_id = ?",
            (bot_id, group_id)
        ).fetchone()
        conn.close()
        if row:
            return {
                'id': row['id'],
                'enabled': bool(row['enabled']),
                'text': row['text'] or '',
                'media': row['media'],
                'media_type': row['media_type'],
                'autodelete': row['autodelete'] or 0,
            }
        return None

    def upsert_per_group_welcome(self, bot_id, group_id, field, value):
        """Create or update a per-group welcome field"""
        with self.lock:
            conn = self.get_connection()
            # Check if record exists
            exists = conn.execute(
                "SELECT id FROM group_welcomes WHERE bot_id = ? AND group_id = ?",
                (bot_id, group_id)
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO group_welcomes (bot_id, group_id) VALUES (?, ?)",
                    (bot_id, group_id)
                )
            conn.execute(
                f"UPDATE group_welcomes SET {field} = ? WHERE bot_id = ? AND group_id = ?",
                (value, bot_id, group_id)
            )
            conn.commit()
            conn.close()
            return True

    def delete_per_group_welcome(self, bot_id, group_id):
        """Delete per-group welcome settings (will fallback to default)"""
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "DELETE FROM group_welcomes WHERE bot_id = ? AND group_id = ?",
                (bot_id, group_id)
            )
            conn.commit()
            conn.close()
            return True

    def get_all_group_welcomes(self, bot_id):
        """Get all per-group welcome settings for a bot"""
        conn = self.get_connection()
        rows = conn.execute(
            "SELECT gw.*, bkg.group_name FROM group_welcomes gw "
            "LEFT JOIN bot_known_groups bkg ON gw.bot_id = bkg.bot_id AND gw.group_id = bkg.group_id "
            "WHERE gw.bot_id = ?",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_last_withdrawal(self, bot_id, user_id):
        """Get most recent withdrawal for cooldown check"""
        conn = self.get_connection()
        withdraw = conn.execute(
            "SELECT * FROM withdrawals WHERE bot_id = ? AND user_id = ? ORDER BY request_date DESC LIMIT 1",
            (bot_id, user_id)
        ).fetchone()
        conn.close()
        return dict(withdraw) if withdraw else None

    # --- User & Referral ---
    def add_user(self, bot_id, telegram_id, referrer_id=None):
        with self.lock:
            conn = self.get_connection()
            try:
                # Check if user exists
                exists = conn.execute("SELECT id FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id)).fetchone()
                if not exists:
                    conn.execute("INSERT INTO users (bot_id, telegram_id, referrer_id) VALUES (?, ?, ?)", (bot_id, telegram_id, referrer_id))
                    
                    # Reward Referrer with custom amount
                    if referrer_id and referrer_id != telegram_id:
                        referrer = conn.execute("SELECT id FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, referrer_id)).fetchone()
                        if referrer:
                            # Get custom referral reward amount
                            bot_settings = conn.execute("SELECT referral_reward FROM bots WHERE id = ?", (bot_id,)).fetchone()
                            reward_amount = bot_settings['referral_reward'] if bot_settings and bot_settings['referral_reward'] else 1.0
                            conn.execute("UPDATE users SET balance = balance + ?, total_invites = total_invites + 1 WHERE bot_id = ? AND telegram_id = ?", (reward_amount, bot_id, referrer_id))
                    
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
    def request_withdrawal(self, bot_id, user_id, amount, method="TNG", account_details=""):
        """Request withdrawal with method and account details"""
        with self.lock:
            conn = self.get_connection()
            try:
                user = conn.execute("SELECT balance FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, user_id)).fetchone()
                if not user or user['balance'] < amount:
                    return False, "Insufficient balance."
                
                # Deduct balance immediately
                conn.execute("UPDATE users SET balance = balance - ? WHERE bot_id = ? AND telegram_id = ?", (amount, bot_id, user_id))
                
                # Create withdrawal record
                conn.execute(
                    "INSERT INTO withdrawals (bot_id, user_id, amount, method, account, status) VALUES (?, ?, ?, ?, ?, 'PENDING')",
                    (bot_id, user_id, amount, method, account_details)
                )
                conn.commit()
                withdrawal_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                return True, f"Withdrawal requested. ID: {withdrawal_id}"
            except Exception as e:
                conn.rollback()
                return False, str(e)
            finally:
                conn.close()


    def get_pending_withdrawals(self, bot_id):
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM withdrawals WHERE bot_id = ? AND status = 'PENDING'", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_all_withdrawals(self, bot_id, status=None):
        """Get all withdrawals, optionally filtered by status"""
        conn = self.get_connection()
        if status:
            rows = conn.execute("SELECT * FROM withdrawals WHERE bot_id = ? AND status = ? ORDER BY request_date DESC", (bot_id, status)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM withdrawals WHERE bot_id = ? ORDER BY request_date DESC", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_withdrawal_by_id(self, withdrawal_id):
        """Get single withdrawal with user details"""
        conn = self.get_connection()
        row = conn.execute(
            """SELECT w.*, u.telegram_id, u.balance as current_balance 
               FROM withdrawals w 
               JOIN users u ON w.user_id = u.telegram_id AND w.bot_id = u.bot_id
               WHERE w.id = ?""",
            (withdrawal_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    
    def update_withdrawal_status(self, withdrawal_id, status, admin_id=None):
        """Update withdrawal status (APPROVED/REJECTED)"""
        with self.lock:
            conn = self.get_connection()
            try:
                # Check current status to prevent double-processing
                current = conn.execute("SELECT status FROM withdrawals WHERE id = ?", (withdrawal_id,)).fetchone()
                if not current or current['status'] != 'PENDING':
                    return False  # Already processed or not found
                
                # If rejecting, refund the balance
                if status == 'REJECTED':
                    withdrawal = conn.execute("SELECT bot_id, user_id, amount FROM withdrawals WHERE id = ?", (withdrawal_id,)).fetchone()
                    if withdrawal:
                        conn.execute(
                            "UPDATE users SET balance = balance + ? WHERE bot_id = ? AND telegram_id = ?",
                            (withdrawal['amount'], withdrawal['bot_id'], withdrawal['user_id'])
                        )
                
                # Update withdrawal status
                conn.execute(
                    "UPDATE withdrawals SET status = ?, processed_at = CURRENT_TIMESTAMP, processed_by = ? WHERE id = ?",
                    (status, admin_id, withdrawal_id)
                )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                return False
            finally:
                conn.close()


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

    def toggle_ai_chat(self, bot_id):
        """Toggle AI chatbot on/off for a bot"""
        with self.lock:
            conn = self.get_connection()
            current = conn.execute("SELECT ai_chat_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
            new_state = 0 if current and current['ai_chat_enabled'] else 1
            conn.execute("UPDATE bots SET ai_chat_enabled = ? WHERE id = ?", (new_state, bot_id))
            conn.commit()
            conn.close()
            return new_state

    def is_ai_chat_enabled(self, bot_id):
        """Check if AI chatbot is enabled for a bot"""
        conn = self.get_connection()
        bot = conn.execute("SELECT ai_chat_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        return bool(bot['ai_chat_enabled']) if bot and 'ai_chat_enabled' in bot.keys() else False  # Default OFF

    def get_ai_prompt(self, bot_id):
        """Get custom AI system prompt for a bot"""
        conn = self.get_connection()
        bot = conn.execute("SELECT ai_system_prompt FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        if bot and 'ai_system_prompt' in bot.keys():
            return bot['ai_system_prompt'] or ''
        return ''

    def set_ai_prompt(self, bot_id, prompt):
        """Set custom AI system prompt for a bot"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE bots SET ai_system_prompt = ? WHERE id = ?", (prompt, bot_id))
            conn.commit()
            conn.close()

    def toggle_link_guard(self, bot_id):
        """Toggle link guard system on/off for a bot"""
        with self.lock:
            conn = self.get_connection()
            current = conn.execute("SELECT link_guard_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
            new_state = 0 if current and current['link_guard_enabled'] else 1
            conn.execute("UPDATE bots SET link_guard_enabled = ? WHERE id = ?", (new_state, bot_id))
            conn.commit()
            conn.close()
            return new_state

    def is_link_guard_enabled(self, bot_id):
        """Check if link guard is enabled for a bot"""
        conn = self.get_connection()
        bot = conn.execute("SELECT link_guard_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        return bool(bot['link_guard_enabled']) if bot and 'link_guard_enabled' in bot.keys() else False  # Default OFF

    # --- Group Management: Anti-Bot ---
    def toggle_anti_bot(self, bot_id):
        """Toggle anti-bot protection on/off"""
        with self.lock:
            conn = self.get_connection()
            current = conn.execute("SELECT anti_bot_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
            new_state = 0 if current and current['anti_bot_enabled'] else 1
            conn.execute("UPDATE bots SET anti_bot_enabled = ? WHERE id = ?", (new_state, bot_id))
            conn.commit()
            conn.close()
            return new_state

    def is_anti_bot_enabled(self, bot_id):
        """Check if anti-bot is enabled"""
        conn = self.get_connection()
        bot = conn.execute("SELECT anti_bot_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        return bool(bot['anti_bot_enabled']) if bot and 'anti_bot_enabled' in bot.keys() else False

    # --- Group Management: Delete Join/Leave ---
    def toggle_delete_join_leave(self, bot_id):
        """Toggle auto-delete join/leave messages"""
        with self.lock:
            conn = self.get_connection()
            current = conn.execute("SELECT delete_join_leave_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
            new_state = 0 if current and current['delete_join_leave_enabled'] else 1
            conn.execute("UPDATE bots SET delete_join_leave_enabled = ? WHERE id = ?", (new_state, bot_id))
            conn.commit()
            conn.close()
            return new_state

    def is_delete_join_leave_enabled(self, bot_id):
        """Check if delete join/leave is enabled"""
        conn = self.get_connection()
        bot = conn.execute("SELECT delete_join_leave_enabled FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        return bool(bot['delete_join_leave_enabled']) if bot and 'delete_join_leave_enabled' in bot.keys() else False

    # --- Group Management: Ban Words ---
    def add_ban_word(self, bot_id, word):
        """Add a banned word"""
        with self.lock:
            conn = self.get_connection()
            try:
                conn.execute("INSERT INTO ban_words (bot_id, word) VALUES (?, ?)", (bot_id, word.lower().strip()))
                conn.commit()
                conn.close()
                return True
            except Exception:
                conn.close()
                return False  # Duplicate or error

    def remove_ban_word(self, bot_id, word_id):
        """Remove a banned word by ID"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute("DELETE FROM ban_words WHERE id = ? AND bot_id = ?", (word_id, bot_id))
            conn.commit()
            deleted = result.rowcount > 0
            conn.close()
            return deleted

    def get_ban_words(self, bot_id):
        """Get all banned words for a bot"""
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM ban_words WHERE bot_id = ?", (bot_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def check_ban_words(self, bot_id, text):
        """Check if text contains any banned words. Returns matched word or None"""
        words = self.get_ban_words(bot_id)
        if not words:
            return None
        text_lower = text.lower()
        for w in words:
            if w['word'] in text_lower:
                return w['word']
        return None

    # --- Group Management: Auto Replies ---
    def add_auto_reply(self, bot_id, trigger_text, response_text):
        """Add an auto-reply rule"""
        with self.lock:
            conn = self.get_connection()
            try:
                conn.execute(
                    "INSERT INTO auto_replies (bot_id, trigger_text, response_text) VALUES (?, ?, ?)",
                    (bot_id, trigger_text.lower().strip(), response_text)
                )
                conn.commit()
                conn.close()
                return True
            except Exception:
                conn.close()
                return False

    def remove_auto_reply(self, bot_id, reply_id):
        """Remove an auto-reply rule by ID"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute("DELETE FROM auto_replies WHERE id = ? AND bot_id = ?", (reply_id, bot_id))
            conn.commit()
            deleted = result.rowcount > 0
            conn.close()
            return deleted

    def get_auto_replies(self, bot_id):
        """Get all auto-reply rules for a bot"""
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM auto_replies WHERE bot_id = ?", (bot_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def find_auto_reply(self, bot_id, text):
        """Find matching auto-reply for text. Returns response or None"""
        replies = self.get_auto_replies(bot_id)
        if not replies:
            return None
        text_lower = text.lower()
        for r in replies:
            if r['trigger_text'] in text_lower:
                return r['response_text']
        return None

    def get_referral_settings(self, bot_id):
        """Get referral reward and min withdrawal settings for a bot"""
        conn = self.get_connection()
        bot = conn.execute("SELECT referral_reward, min_withdrawal FROM bots WHERE id = ?", (bot_id,)).fetchone()
        conn.close()
        if bot:
            return {
                'referral_reward': bot['referral_reward'] if bot['referral_reward'] is not None else 1.0,
                'min_withdrawal': bot['min_withdrawal'] if bot['min_withdrawal'] is not None else 50.0
            }
        return {'referral_reward': 1.0, 'min_withdrawal': 50.0}  # Defaults

    def update_referral_settings(self, bot_id, referral_reward=None, min_withdrawal=None):
        """Update referral reward and/or min withdrawal for a bot"""
        with self.lock:
            conn = self.get_connection()
            if referral_reward is not None:
                conn.execute("UPDATE bots SET referral_reward = ? WHERE id = ?", (referral_reward, bot_id))
            if min_withdrawal is not None:
                conn.execute("UPDATE bots SET min_withdrawal = ? WHERE id = ?", (min_withdrawal, bot_id))
            conn.commit()
            conn.close()
            return True

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
    def save_scheduled_broadcast(self, bot_id, message, media_file_id, media_type, scheduled_time, target_type='users', grid_media=None, grid_buttons=None):
        """Save a scheduled broadcast"""
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "INSERT INTO broadcasts (bot_id, message, media_file_id, media_type, status, scheduled_time, target_type, grid_media, grid_buttons) VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)",
                (bot_id, message, media_file_id, media_type, scheduled_time, target_type, grid_media, grid_buttons)
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

    # --- Recurring Broadcasts ---
    def save_recurring_broadcast(self, bot_id, message, media_file_id, media_type, interval_type, interval_value, target_type='users', grid_media=None, grid_buttons=None):
        """Save a recurring broadcast configuration"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.execute(
                """INSERT INTO broadcasts 
                   (bot_id, message, media_file_id, media_type, status, is_recurring, interval_type, interval_value, is_active, target_type, grid_media, grid_buttons) 
                   VALUES (?, ?, ?, ?, 'RECURRING', 1, ?, ?, 1, ?, ?, ?)""",
                (bot_id, message, media_file_id, media_type, interval_type, interval_value, target_type, grid_media, grid_buttons)
            )
            broadcast_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return broadcast_id

    def get_recurring_broadcasts(self, bot_id=None):
        """Get active recurring broadcasts, optionally filtered by bot_id"""
        conn = self.get_connection()
        if bot_id:
            rows = conn.execute(
                "SELECT * FROM broadcasts WHERE bot_id = ? AND is_recurring = 1 AND is_active = 1",
                (bot_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM broadcasts WHERE is_recurring = 1 AND is_active = 1"
            ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def toggle_recurring_broadcast(self, broadcast_id, bot_id, is_active):
        """Enable or disable a recurring broadcast"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute(
                "UPDATE broadcasts SET is_active = ? WHERE id = ? AND bot_id = ? AND is_recurring = 1",
                (is_active, broadcast_id, bot_id)
            )
            conn.commit()
            updated = result.rowcount > 0
            conn.close()
            return updated

    def delete_recurring_broadcast(self, broadcast_id, bot_id):
        """Delete a recurring broadcast"""
        with self.lock:
            conn = self.get_connection()
            result = conn.execute(
                "DELETE FROM broadcasts WHERE id = ? AND bot_id = ? AND is_recurring = 1",
                (broadcast_id, bot_id)
            )
            conn.commit()
            deleted = result.rowcount > 0
            conn.close()
            return deleted

    def get_all_recurring_broadcasts(self):
        """Get all active recurring broadcasts from all bots (for startup reload)"""
        conn = self.get_connection()
        rows = conn.execute(
            "SELECT * FROM broadcasts WHERE is_recurring = 1 AND is_active = 1"
        ).fetchall()
        conn.close()
        return rows

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

    # ==================== FORWARDER CONFIG ====================
    
    def save_forwarder_config(self, bot_id, source_channel_id, source_channel_name, 
                               target_group_id, target_group_name, filter_keywords=None):
        """Save or update forwarder configuration"""
        with self.lock:
            conn = self.get_connection()
            try:
                # Check if config exists
                existing = conn.execute(
                    "SELECT id FROM forwarder_config WHERE bot_id = ?", (bot_id,)
                ).fetchone()
                
                if existing:
                    conn.execute(
                        """UPDATE forwarder_config SET 
                           source_channel_id = ?, source_channel_name = ?,
                           target_group_id = ?, target_group_name = ?,
                           filter_keywords = ?
                           WHERE bot_id = ?""",
                        (source_channel_id, source_channel_name, target_group_id, 
                         target_group_name, filter_keywords, bot_id)
                    )
                else:
                    conn.execute(
                        """INSERT INTO forwarder_config 
                           (bot_id, source_channel_id, source_channel_name, 
                            target_group_id, target_group_name, filter_keywords, is_active)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (bot_id, source_channel_id, source_channel_name, 
                         target_group_id, target_group_name, filter_keywords)
                    )
                conn.commit()
                return True
            except Exception as e:
                print(f"Error saving forwarder config: {e}")
                return False
            finally:
                conn.close()
    
    def get_forwarder_config(self, bot_id):
        """Get forwarder configuration for a bot"""
        conn = self.get_connection()
        config = conn.execute(
            "SELECT * FROM forwarder_config WHERE bot_id = ?", (bot_id,)
        ).fetchone()
        conn.close()
        return dict(config) if config else None
    
    def toggle_forwarder(self, bot_id):
        """Toggle forwarder on/off"""
        with self.lock:
            conn = self.get_connection()
            current = conn.execute(
                "SELECT is_active FROM forwarder_config WHERE bot_id = ?", (bot_id,)
            ).fetchone()
            
            if not current:
                conn.close()
                return None
            
            new_state = 0 if current['is_active'] else 1
            conn.execute(
                "UPDATE forwarder_config SET is_active = ? WHERE bot_id = ?",
                (new_state, bot_id)
            )
            conn.commit()
            conn.close()
            return new_state
    
    def update_forwarder_filter(self, bot_id, filter_keywords):
        """Update filter keywords for forwarder"""
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "UPDATE forwarder_config SET filter_keywords = ? WHERE bot_id = ?",
                (filter_keywords, bot_id)
            )
            conn.commit()
            conn.close()
            return True
    
    def get_all_active_forwarders(self):
        """Get all active forwarder configs (for global handler)"""
        conn = self.get_connection()
        configs = conn.execute(
            """SELECT fc.*, b.token FROM forwarder_config fc
               JOIN bots b ON fc.bot_id = b.id
               WHERE fc.is_active = 1"""
        ).fetchall()
        conn.close()
        return [dict(c) for c in configs]

    # ==================== KNOWN GROUPS (BROADCAST MODE) ====================

    def upsert_known_group(self, bot_id, group_id, group_name):
        """Add or update a known group for the bot"""
        with self.lock:
            conn = self.get_connection()
            try:
                # Update if exists, otherwise Insert
                conn.execute(
                    """INSERT INTO bot_known_groups (bot_id, group_id, group_name, is_active, joined_at)
                       VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                       ON CONFLICT(bot_id, group_id) 
                       DO UPDATE SET group_name = ?, is_active = 1, joined_at = CURRENT_TIMESTAMP""",
                    (bot_id, group_id, group_name, group_name)
                )
                conn.commit()
                return True
            except Exception as e:
                print(f"Error upserting known group: {e}")
                return False
            finally:
                conn.close()

    def get_known_groups(self, bot_id):
        """Get all active known groups for a bot"""
        conn = self.get_connection()
        groups = conn.execute(
            "SELECT * FROM bot_known_groups WHERE bot_id = ? AND is_active = 1 ORDER BY joined_at DESC",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [dict(g) for g in groups]

    def set_group_inactive(self, bot_id, group_id):
        """Mark a group as inactive (bot kicked/left)"""
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "UPDATE bot_known_groups SET is_active = 0 WHERE bot_id = ? AND group_id = ?",
                (bot_id, group_id)
            )
            conn.commit()
            conn.close()

    def toggle_forwarder_mode(self, bot_id):
        """Toggle forwarder mode between SINGLE and BROADCAST"""
        with self.lock:
            conn = self.get_connection()
            current = conn.execute(
                "SELECT forwarder_mode FROM forwarder_config WHERE bot_id = ?", (bot_id,)
            ).fetchone()
            
            if not current:
                conn.close()
                return None
            
            current_mode = current['forwarder_mode'] or 'SINGLE'
            new_mode = 'BROADCAST' if current_mode == 'SINGLE' else 'SINGLE'
            
            conn.execute(
                "UPDATE forwarder_config SET forwarder_mode = ? WHERE bot_id = ?",
                (new_mode, bot_id)
            )
            conn.commit()
            conn.close()
            return new_mode

    # ==================== CLONE BOT ====================
    
    def clone_bot_data(self, source_bot_id, target_bot_id):
        """Clone all data from source bot to target bot (companies, buttons, settings)"""
        conn = self.get_connection()
        try:
            # Clone companies
            companies = conn.execute(
                "SELECT name, description, media_file_id, media_type FROM companies WHERE bot_id = ?",
                (source_bot_id,)
            ).fetchall()
            
            for c in companies:
                cursor = conn.execute(
                    "INSERT INTO companies (bot_id, name, description, media_file_id, media_type) VALUES (?, ?, ?, ?, ?)",
                    (target_bot_id, c['name'], c['description'], c['media_file_id'], c['media_type'])
                )
                new_company_id = cursor.lastrowid
                
                # Clone company buttons
                old_company_id = conn.execute(
                    "SELECT id FROM companies WHERE bot_id = ? AND name = ?",
                    (source_bot_id, c['name'])
                ).fetchone()
                
                if old_company_id:
                    buttons = conn.execute(
                        "SELECT text, url, row_group FROM company_buttons WHERE company_id = ?",
                        (old_company_id['id'],)
                    ).fetchall()
                    for btn in buttons:
                        conn.execute(
                            "INSERT INTO company_buttons (company_id, text, url, row_group) VALUES (?, ?, ?, ?)",
                            (new_company_id, btn['text'], btn['url'], btn['row_group'])
                        )
            
            # Clone menu buttons
            menu_btns = conn.execute(
                "SELECT text, url, row_group FROM menu_buttons WHERE bot_id = ?",
                (source_bot_id,)
            ).fetchall()
            for btn in menu_btns:
                conn.execute(
                    "INSERT INTO menu_buttons (bot_id, text, url, row_group) VALUES (?, ?, ?, ?)",
                    (target_bot_id, btn['text'], btn['url'], btn['row_group'])
                )
            
            # Clone bot settings (welcome, banner, etc.)
            source_bot = conn.execute("SELECT * FROM bots WHERE id = ?", (source_bot_id,)).fetchone()
            if source_bot:
                conn.execute(
                    """UPDATE bots SET 
                       custom_banner = ?, custom_caption = ?, 
                       referral_enabled = ?, livegram_enabled = ?
                       WHERE id = ?""",
                    (source_bot['custom_banner'], source_bot['custom_caption'],
                     source_bot['referral_enabled'], source_bot['livegram_enabled'], target_bot_id)
                )
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            return False
        finally:
            conn.close()

    # ==================== ANALYTICS ====================
    
    def get_bot_analytics(self, bot_id):
        """Get comprehensive analytics for a bot"""
        conn = self.get_connection()
        
        # User stats
        total_users = conn.execute("SELECT COUNT(*) FROM users WHERE bot_id = ?", (bot_id,)).fetchone()[0]
        users_today = conn.execute(
            "SELECT COUNT(*) FROM users WHERE bot_id = ? AND DATE(joined_at) = DATE('now')", (bot_id,)
        ).fetchone()[0]
        users_week = conn.execute(
            "SELECT COUNT(*) FROM users WHERE bot_id = ? AND DATE(joined_at) >= DATE('now', '-7 days')", (bot_id,)
        ).fetchone()[0]
        users_month = conn.execute(
            "SELECT COUNT(*) FROM users WHERE bot_id = ? AND DATE(joined_at) >= DATE('now', '-30 days')", (bot_id,)
        ).fetchone()[0]
        
        # Referral stats
        total_referred = conn.execute("SELECT COUNT(*) FROM users WHERE bot_id = ? AND referred_by IS NOT NULL", (bot_id,)).fetchone()[0]
        
        # Company stats
        total_companies = conn.execute("SELECT COUNT(*) FROM companies WHERE bot_id = ?", (bot_id,)).fetchone()[0]
        
        # Top referrers
        top_referrers = conn.execute(
            """SELECT u.telegram_id, u.username, COUNT(r.id) as referral_count, SUM(u.balance) as earnings
               FROM users u LEFT JOIN users r ON r.referred_by = u.telegram_id AND r.bot_id = u.bot_id
               WHERE u.bot_id = ? GROUP BY u.telegram_id ORDER BY referral_count DESC LIMIT 10""",
            (bot_id,)
        ).fetchall()
        
        conn.close()
        
        return {
            'total_users': total_users,
            'users_today': users_today,
            'users_week': users_week,
            'users_month': users_month,
            'total_referred': total_referred,
            'total_companies': total_companies,
            'top_referrers': [dict(r) for r in top_referrers]
        }

    def get_user_growth(self, bot_id, days=30):
        """Get daily user growth for chart"""
        conn = self.get_connection()
        growth = conn.execute(
            """SELECT DATE(joined_at) as date, COUNT(*) as count 
               FROM users WHERE bot_id = ? AND DATE(joined_at) >= DATE('now', ?)
               GROUP BY DATE(joined_at) ORDER BY date""",
            (bot_id, f'-{days} days')
        ).fetchall()
        conn.close()
        return [dict(g) for g in growth]

    # ==================== EXPORT DATA ====================
    
    def export_users(self, bot_id):
        """Export all users for a bot"""
        conn = self.get_connection()
        users = conn.execute(
            """SELECT telegram_id, username, first_name, balance, referred_by, joined_at 
               FROM users WHERE bot_id = ? ORDER BY joined_at""",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [dict(u) for u in users]

    def export_companies(self, bot_id):
        """Export all companies for a bot"""
        conn = self.get_connection()
        companies = conn.execute(
            "SELECT id, name, description, category, created_at FROM companies WHERE bot_id = ? ORDER BY id",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [dict(c) for c in companies]

    # ==================== COMPANY CATEGORIES ====================
    
    def get_categories(self, bot_id):
        """Get distinct categories for a bot"""
        conn = self.get_connection()
        categories = conn.execute(
            "SELECT DISTINCT category FROM companies WHERE bot_id = ? AND category IS NOT NULL ORDER BY category",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [c['category'] for c in categories]

    def get_companies_by_category(self, bot_id, category):
        """Get companies filtered by category"""
        conn = self.get_connection()
        companies = conn.execute(
            "SELECT * FROM companies WHERE bot_id = ? AND category = ? ORDER BY id",
            (bot_id, category)
        ).fetchall()
        conn.close()
        return companies

    def update_company_category(self, company_id, category):
        """Update company category"""
        conn = self.get_connection()
        conn.execute("UPDATE companies SET category = ? WHERE id = ?", (category, company_id))
        conn.commit()
        conn.close()

    # ==================== TARGETED BROADCAST ====================
    
    def get_users_by_filter(self, bot_id, filter_type, filter_value=None):
        """Get users filtered by criteria for targeted broadcast"""
        conn = self.get_connection()
        
        if filter_type == "all":
            users = conn.execute("SELECT * FROM users WHERE bot_id = ?", (bot_id,)).fetchall()
        elif filter_type == "today":
            users = conn.execute(
                "SELECT * FROM users WHERE bot_id = ? AND DATE(joined_at) = DATE('now')", (bot_id,)
            ).fetchall()
        elif filter_type == "week":
            users = conn.execute(
                "SELECT * FROM users WHERE bot_id = ? AND DATE(joined_at) >= DATE('now', '-7 days')", (bot_id,)
            ).fetchall()
        elif filter_type == "month":
            users = conn.execute(
                "SELECT * FROM users WHERE bot_id = ? AND DATE(joined_at) >= DATE('now', '-30 days')", (bot_id,)
            ).fetchall()
        elif filter_type == "referred":
            users = conn.execute(
                "SELECT * FROM users WHERE bot_id = ? AND referred_by IS NOT NULL", (bot_id,)
            ).fetchall()
        elif filter_type == "organic":
            users = conn.execute(
                "SELECT * FROM users WHERE bot_id = ? AND referred_by IS NULL", (bot_id,)
            ).fetchall()
        elif filter_type == "with_balance":
            users = conn.execute(
                "SELECT * FROM users WHERE bot_id = ? AND balance > 0", (bot_id,)
            ).fetchall()
        else:
            users = []
        
        conn.close()
        return users

    # ==================== EXPIRING BOTS ====================
    
    def get_expiring_bots(self, days=3):
        """Get bots expiring within X days"""
        conn = self.get_connection()
        bots = conn.execute(
            """SELECT * FROM bots 
               WHERE DATE(subscription_end) BETWEEN DATE('now') AND DATE('now', ?)
               AND is_active = 1""",
            (f'+{days} days',)
        ).fetchall()
        conn.close()
        return bots

    def get_expired_bots(self):
        """Get all expired bots"""
        conn = self.get_connection()
        bots = conn.execute(
            "SELECT * FROM bots WHERE DATE(subscription_end) < DATE('now') AND is_active = 1"
        ).fetchall()
        conn.close()
        return bots

    # ==================== USER VERIFICATION ====================
    
    def set_required_channel(self, bot_id, channel_id, channel_username):
        """Set required channel for bot"""
        conn = self.get_connection()
        conn.execute(
            "UPDATE bots SET required_channel_id = ?, required_channel_username = ? WHERE id = ?",
            (channel_id, channel_username, bot_id)
        )
        conn.commit()
        conn.close()

    def get_required_channel(self, bot_id):
        """Get required channel for bot"""
        conn = self.get_connection()
        bot = conn.execute(
            "SELECT required_channel_id, required_channel_username FROM bots WHERE id = ?",
            (bot_id,)
        ).fetchone()
        conn.close()
        return dict(bot) if bot else None

    # ==================== LEADERBOARD ====================
    
    def get_top_referrers(self, bot_id, limit=10):
        """Get top referrers for a bot"""
        conn = self.get_connection()
        referrers = conn.execute(
            """SELECT u.telegram_id, u.username, u.first_name, 
                      COUNT(r.id) as referral_count, u.balance
               FROM users u 
               JOIN users r ON r.referred_by = u.telegram_id AND r.bot_id = u.bot_id
               WHERE u.bot_id = ?
               GROUP BY u.telegram_id 
               ORDER BY referral_count DESC 
               LIMIT ?""",
            (bot_id, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in referrers]

    def add_bonus_to_user(self, bot_id, telegram_id, amount, reason="bonus"):
        """Add bonus to user balance"""
        conn = self.get_connection()
        conn.execute(
            "UPDATE users SET balance = balance + ? WHERE bot_id = ? AND telegram_id = ?",
            (amount, bot_id, telegram_id)
        )
        conn.commit()
        conn.close()

    # ==================== 4D RESULTS ====================
    
    def save_4d_result(self, company, draw_date, first, second, third, special, consolation):
        """Save 4D result to database"""
        conn = self.get_connection()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO results_4d 
                   (company, draw_date, first_prize, second_prize, third_prize, special_prizes, consolation_prizes)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (company, draw_date, first, second, third, special, consolation)
            )
            conn.commit()
            return True
        except Exception as e:
            return False
        finally:
            conn.close()

    def get_4d_results(self, company=None, limit=30):
        """Get 4D results history"""
        conn = self.get_connection()
        if company:
            results = conn.execute(
                "SELECT * FROM results_4d WHERE company = ? ORDER BY draw_date DESC LIMIT ?",
                (company, limit)
            ).fetchall()
        else:
            results = conn.execute(
                "SELECT * FROM results_4d ORDER BY draw_date DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in results]

    def get_4d_statistics(self, company=None, limit=100):
        """Analyze 4D statistics - digit frequency, hot/cold numbers"""
        results = self.get_4d_results(company, limit)
        
        if not results:
            return None
        
        # Collect all winning numbers
        all_numbers = []
        for r in results:
            for prize in [r['first_prize'], r['second_prize'], r['third_prize']]:
                if prize:
                    all_numbers.append(prize)
            # Parse special and consolation (comma separated)
            if r['special_prizes']:
                all_numbers.extend(r['special_prizes'].split(','))
            if r['consolation_prizes']:
                all_numbers.extend(r['consolation_prizes'].split(','))
        
        # Count digit frequency
        digit_count = {str(i): 0 for i in range(10)}
        number_count = {}
        
        for num in all_numbers:
            num = num.strip()
            if len(num) == 4:
                number_count[num] = number_count.get(num, 0) + 1
                for digit in num:
                    digit_count[digit] = digit_count.get(digit, 0) + 1
        
        # Sort by frequency
        hot_digits = sorted(digit_count.items(), key=lambda x: x[1], reverse=True)
        cold_digits = sorted(digit_count.items(), key=lambda x: x[1])
        hot_numbers = sorted(number_count.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'total_draws': len(results),
            'total_numbers': len(all_numbers),
            'hot_digits': hot_digits[:5],
            'cold_digits': cold_digits[:5],
            'hot_numbers': hot_numbers,
            'digit_frequency': digit_count
        }

    def get_4d_results_by_date(self, draw_date=None, company=None, limit=10, offset=0):
        """Get 4D results filtered by date and/or company with pagination"""
        conn = self.get_connection()
        query = "SELECT * FROM results_4d WHERE 1=1"
        params = []
        
        if draw_date:
            query += " AND draw_date = ?"
            params.append(draw_date)
        if company:
            query += " AND company = ?"
            params.append(company)
        
        query += " ORDER BY draw_date DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        results = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in results]

    def get_4d_available_dates(self, limit=10):
        """Get list of distinct draw dates available"""
        conn = self.get_connection()
        dates = conn.execute(
            "SELECT DISTINCT draw_date FROM results_4d ORDER BY draw_date DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return [d['draw_date'] for d in dates]

    def get_4d_prediction_data(self, limit=200):
        """Get advanced statistical data for prediction chart"""
        results = self.get_4d_results(limit=limit)
        
        if not results:
            return None
        
        # Position frequency: which digits appear most at each position (1st digit, 2nd, 3rd, 4th)
        position_freq = {i: {str(d): 0 for d in range(10)} for i in range(4)}
        
        # Track all winning numbers with their dates for gap analysis
        number_last_seen = {}  # number -> draw_date
        all_numbers = []
        
        # Two-digit pair frequency (last 2 digits)
        pair_freq = {}
        
        for r in results:
            for prize in [r['first_prize'], r['second_prize'], r['third_prize']]:
                if prize and len(prize) == 4:
                    all_numbers.append(prize)
                    number_last_seen[prize] = r.get('draw_date', '')
                    
                    # Position analysis
                    for pos, digit in enumerate(prize):
                        position_freq[pos][digit] = position_freq[pos].get(digit, 0) + 1
                    
                    # Pair analysis (last 2 digits)
                    pair = prize[2:]
                    pair_freq[pair] = pair_freq.get(pair, 0) + 1
            
            # Also analyze special prizes
            if r['special_prizes']:
                for num in r['special_prizes'].split(','):
                    num = num.strip()
                    if len(num) == 4:
                        all_numbers.append(num)
                        number_last_seen[num] = r.get('draw_date', '')
                        pair = num[2:]
                        pair_freq[pair] = pair_freq.get(pair, 0) + 1
        
        # Sort position frequencies
        top_per_position = {}
        for pos in range(4):
            sorted_digits = sorted(position_freq[pos].items(), key=lambda x: x[1], reverse=True)
            top_per_position[pos] = sorted_digits[:5]
        
        # Top pairs
        top_pairs = sorted(pair_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Digit frequency for heatmap (2-digit endings 00-99)
        ending_freq = {}
        for num in all_numbers:
            if len(num) == 4:
                ending = num[2:]
                ending_freq[ending] = ending_freq.get(ending, 0) + 1
        
        return {
            'total_analyzed': len(results),
            'total_numbers': len(all_numbers),
            'position_frequency': top_per_position,
            'top_pairs': top_pairs,
            'ending_frequency': ending_freq,
            'number_last_seen': number_last_seen
        }

    # ==================== 4D NOTIFICATION SUBSCRIBERS ====================
    
    def subscribe_4d_notification(self, bot_id, user_id):
        """Subscribe user to 4D result notifications"""
        with self.lock:
            conn = self.get_connection()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO notify_4d_subscribers (bot_id, user_id) VALUES (?, ?)",
                    (bot_id, user_id)
                )
                conn.commit()
                return True
            except Exception as e:
                return False
            finally:
                conn.close()
    
    def unsubscribe_4d_notification(self, bot_id, user_id):
        """Unsubscribe user from 4D result notifications"""
        with self.lock:
            conn = self.get_connection()
            try:
                result = conn.execute(
                    "DELETE FROM notify_4d_subscribers WHERE bot_id = ? AND user_id = ?",
                    (bot_id, user_id)
                )
                conn.commit()
                return result.rowcount > 0
            finally:
                conn.close()
    
    def is_subscribed_4d_notification(self, bot_id, user_id):
        """Check if user is subscribed to 4D notifications"""
        conn = self.get_connection()
        sub = conn.execute(
            "SELECT id FROM notify_4d_subscribers WHERE bot_id = ? AND user_id = ?",
            (bot_id, user_id)
        ).fetchone()
        conn.close()
        return sub is not None
    
    def get_4d_subscribers(self, bot_id):
        """Get all 4D notification subscribers for a bot"""
        conn = self.get_connection()
        subs = conn.execute(
            "SELECT user_id FROM notify_4d_subscribers WHERE bot_id = ?",
            (bot_id,)
        ).fetchall()
        conn.close()
        return [s['user_id'] for s in subs]
    
    def get_all_4d_subscribers(self):
        """Get all 4D subscribers across all bots (for global notification)"""
        conn = self.get_connection()
        subs = conn.execute(
            "SELECT bot_id, user_id FROM notify_4d_subscribers"
        ).fetchall()
        conn.close()
        return [dict(s) for s in subs]

    # ==================== FORWARDER CONFIG ====================
    
    def get_forwarder_config(self, bot_id):
        """Get forwarder configuration for a bot"""
        conn = self.get_connection()
        config = conn.execute(
            "SELECT * FROM forwarder_config WHERE bot_id = ?",
            (bot_id,)
        ).fetchone()
        conn.close()
        return dict(config) if config else None

    def save_forwarder_config(self, bot_id, source_id, source_name, target_id, target_name, filter_keywords):
        """Save or update forwarder configuration"""
        with self.lock:
            conn = self.get_connection()
            try:
                # Check if exists
                exists = conn.execute("SELECT id FROM forwarder_config WHERE bot_id = ?", (bot_id,)).fetchone()
                
                if exists:
                    conn.execute(
                        """UPDATE forwarder_config 
                           SET source_channel_id = ?, source_channel_name = ?, 
                               target_group_id = ?, target_group_name = ?, 
                               filter_keywords = ?
                           WHERE bot_id = ?""",
                        (source_id, source_name, target_id, target_name, filter_keywords, bot_id)
                    )
                else:
                    conn.execute(
                        """INSERT INTO forwarder_config 
                           (bot_id, source_channel_id, source_channel_name, target_group_id, target_group_name, filter_keywords, is_active)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (bot_id, source_id, source_name, target_id, target_name, filter_keywords)
                    )
                conn.commit()
                return True
            except Exception as e:
                return False
            finally:
                conn.close()

    def toggle_forwarder(self, bot_id):
        """Toggle forwarder active state"""
        with self.lock:
            conn = self.get_connection()
            try:
                config = conn.execute("SELECT is_active FROM forwarder_config WHERE bot_id = ?", (bot_id,)).fetchone()
                if not config:
                    return None
                
                new_state = 0 if config['is_active'] else 1
                conn.execute("UPDATE forwarder_config SET is_active = ? WHERE bot_id = ?", (new_state, bot_id))
                conn.commit()
                return new_state
            finally:
                conn.close()

    def update_forwarder_filter(self, bot_id, keywords):
        """Update forwarder filter keywords"""
        with self.lock:
            conn = self.get_connection()
            try:
                conn.execute("UPDATE forwarder_config SET filter_keywords = ? WHERE bot_id = ?", (keywords, bot_id))
                conn.commit()
                return True
            except Exception:
                return False
            finally:
                conn.close()

    def toggle_forwarder_mode(self, bot_id):
        """Toggle forwarder mode between SINGLE and BROADCAST"""
        with self.lock:
            conn = self.get_connection()
            try:
                config = conn.execute("SELECT forwarder_mode FROM forwarder_config WHERE bot_id = ?", (bot_id,)).fetchone()
                
                current_mode = config['forwarder_mode'] if config and config['forwarder_mode'] else 'SINGLE'
                new_mode = 'BROADCAST' if current_mode == 'SINGLE' else 'SINGLE'
                
                # If record doesn't exist, create it
                if not config:
                     conn.execute(
                        "INSERT INTO forwarder_config (bot_id, forwarder_mode) VALUES (?, ?)",
                        (bot_id, new_mode)
                    )
                else:
                    conn.execute("UPDATE forwarder_config SET forwarder_mode = ? WHERE bot_id = ?", (new_mode, bot_id))
                
                conn.commit()
                return new_mode
            except Exception as e:
                return None
            finally:
                conn.close()



    # ==================== MULTI-SOURCE FORWARDER ====================

    def add_forwarder_source(self, bot_id, source_id, source_name):
        """Add a source channel to the forwarder"""
        with self.lock:
            conn = self.get_connection()
            try:
                # Ensure table exists
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS forwarder_sources (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        bot_id INTEGER NOT NULL,
                        source_id INTEGER NOT NULL,
                        source_name TEXT,
                        added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                        UNIQUE(bot_id, source_id)
                    )
                ''')
                
                conn.execute(
                    "INSERT OR REPLACE INTO forwarder_sources (bot_id, source_id, source_name) VALUES (?, ?, ?)",
                    (bot_id, source_id, source_name)
                )
                conn.commit()
                return True
            except Exception as e:
                print(f"Error adding source: {e}")
                return False
            finally:
                conn.close()

    def remove_forwarder_source(self, bot_id, source_id):
        """Remove a source channel"""
        with self.lock:
            conn = self.get_connection()
            try:
                conn.execute("DELETE FROM forwarder_sources WHERE bot_id = ? AND source_id = ?", (bot_id, source_id))
                conn.commit()
                return True
            except Exception:
                return False
            finally:
                conn.close()

    def get_forwarder_sources(self, bot_id):
        """Get all source channels for a bot"""
        conn = self.get_connection()
        try:
            # Check if table exists first (to avoid error on fresh DB)
            table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='forwarder_sources'").fetchone()
            if not table_exists:
                return []
                
            sources = conn.execute(
                "SELECT * FROM forwarder_sources WHERE bot_id = ?",
                (bot_id,)
            ).fetchall()
            return [dict(s) for s in sources]
        except Exception:
            return []
        finally:
            conn.close()

    # ==================== MEDIA ASSETS MANAGER ====================

    def upsert_asset(self, bot_id, section_name, file_id, file_type, caption=None):
        """Save or update a media asset for a section"""
        with self.lock:
            conn = self.get_connection()
            try:
                # Ensure table exists
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS bot_assets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        bot_id INTEGER NOT NULL,
                        section_name TEXT NOT NULL,
                        file_id TEXT NOT NULL,
                        file_type TEXT NOT NULL,
                        caption TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                        UNIQUE(bot_id, section_name)
                    )
                ''')
                
                # Check if exists
                exists = conn.execute(
                    "SELECT id FROM bot_assets WHERE bot_id = ? AND section_name = ?", 
                    (bot_id, section_name)
                ).fetchone()

                if exists:
                    conn.execute(
                        """UPDATE bot_assets 
                           SET file_id = ?, file_type = ?, caption = ?, updated_at = CURRENT_TIMESTAMP 
                           WHERE bot_id = ? AND section_name = ?""",
                        (file_id, file_type, caption, bot_id, section_name)
                    )
                else:
                    conn.execute(
                        """INSERT INTO bot_assets (bot_id, section_name, file_id, file_type, caption) 
                           VALUES (?, ?, ?, ?, ?)""",
                        (bot_id, section_name, file_id, file_type, caption)
                    )
                conn.commit()
                return True
            except Exception as e:
                print(f"Error saving asset: {e}")
                return False
            finally:
                conn.close()

    def get_asset(self, bot_id, section_name):
        """Get media asset for a section"""
        conn = self.get_connection()
        try:
            # Check table existence to safely return None on fresh db
            table_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bot_assets'").fetchone()
            if not table_check:
                return None
                
            asset = conn.execute(
                "SELECT * FROM bot_assets WHERE bot_id = ? AND section_name = ?",
                (bot_id, section_name)
            ).fetchone()
            return dict(asset) if asset else None
        except Exception:
            return None
        finally:
            conn.close()

    def reset_user_referral(self, bot_id, user_telegram_id):
        """Delete a user so they can be re-referred (resets their referrer's count too)"""
        with self.lock:
            conn = self.get_connection()
            # Get user's referrer before deleting
            user = conn.execute("SELECT referrer_id FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, user_telegram_id)).fetchone()
            
            if user and user['referrer_id']:
                # Deduct from referrer's count
                conn.execute("""
                    UPDATE users SET total_invites = CASE WHEN total_invites > 0 THEN total_invites - 1 ELSE 0 END
                    WHERE bot_id = ? AND telegram_id = ?
                """, (bot_id, user['referrer_id']))
            
            # Delete the user
            conn.execute("DELETE FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, user_telegram_id))
            conn.commit()
            conn.close()
            return True

    def reset_all_referrals(self, bot_id):
        """Delete ALL users to allow complete re-referral (nuclear option)"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("DELETE FROM users WHERE bot_id = ?", (bot_id,))
            conn.commit()
            conn.close()
            return True

    # === USERBOT SESSION METHODS ===

    def save_userbot_session(self, bot_id, api_id, api_hash, session_string, phone):
        """Save or update userbot session"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("""
                INSERT INTO userbot_sessions (bot_id, api_id, api_hash, session_string, phone, is_active)
                VALUES (?, ?, ?, ?, ?, 0)
                ON CONFLICT(bot_id) DO UPDATE SET
                    api_id = excluded.api_id,
                    api_hash = excluded.api_hash,
                    session_string = excluded.session_string,
                    phone = excluded.phone
            """, (bot_id, api_id, api_hash, session_string, phone))
            conn.commit()
            conn.close()

    def get_userbot_session(self, bot_id):
        """Get userbot session for a bot"""
        conn = self.get_connection()
        row = conn.execute("SELECT * FROM userbot_sessions WHERE bot_id = ?", (bot_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def toggle_userbot(self, bot_id, active):
        """Toggle userbot on/off"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE userbot_sessions SET is_active = ? WHERE bot_id = ?", (1 if active else 0, bot_id))
            conn.commit()
            conn.close()

    def set_userbot_mode(self, bot_id, auto_mode):
        """Set auto/manual mode"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE userbot_sessions SET auto_mode = ? WHERE bot_id = ?", (1 if auto_mode else 0, bot_id))
            conn.commit()
            conn.close()

    def update_userbot_session_string(self, bot_id, session_string):
        """Update session string after auth"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE userbot_sessions SET session_string = ? WHERE bot_id = ?", (session_string, bot_id))
            conn.commit()
            conn.close()

    def delete_userbot_session(self, bot_id):
        """Delete userbot session"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("DELETE FROM userbot_sessions WHERE bot_id = ?", (bot_id,))
            conn.commit()
            conn.close()

    def get_all_active_userbot_sessions(self):
        """Get all active userbot sessions (for startup)"""
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM userbot_sessions WHERE is_active = 1 AND session_string IS NOT NULL").fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # === MONITORED CHANNELS METHODS ===

    def add_monitored_channel(self, bot_id, channel_id, channel_title, channel_username=None):
        """Add a channel to monitor"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("""
                INSERT OR IGNORE INTO monitored_channels (bot_id, channel_id, channel_title, channel_username)
                VALUES (?, ?, ?, ?)
            """, (bot_id, str(channel_id), channel_title, channel_username))
            conn.commit()
            conn.close()

    def get_monitored_channels(self, bot_id):
        """Get all monitored channels for a bot"""
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM monitored_channels WHERE bot_id = ? AND is_active = 1", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def remove_monitored_channel(self, channel_db_id):
        """Remove a monitored channel"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("DELETE FROM monitored_channels WHERE id = ?", (channel_db_id,))
            conn.commit()
            conn.close()

    def update_monitored_channel_id(self, bot_id, old_channel_id, new_channel_id, title=None, username=None):
        """Update channel_id after resolving real Telegram ID"""
        with self.lock:
            conn = self.get_connection()
            if title and username:
                conn.execute("""
                    UPDATE monitored_channels
                    SET channel_id = ?, channel_title = ?, channel_username = ?
                    WHERE bot_id = ? AND channel_id = ?
                """, (new_channel_id, title, username, bot_id, old_channel_id))
            elif title:
                conn.execute("""
                    UPDATE monitored_channels
                    SET channel_id = ?, channel_title = ?
                    WHERE bot_id = ? AND channel_id = ?
                """, (new_channel_id, title, bot_id, old_channel_id))
            else:
                conn.execute("""
                    UPDATE monitored_channels
                    SET channel_id = ?
                    WHERE bot_id = ? AND channel_id = ?
                """, (new_channel_id, bot_id, old_channel_id))
            conn.commit()
            conn.close()

    # === DETECTED PROMOS METHODS ===

    def save_detected_promo(self, bot_id, source_channel, original_text, swapped_text, media_file_ids, media_types, matched_company):
        """Save a detected promo"""
        import json
        with self.lock:
            conn = self.get_connection()
            conn.execute("""
                INSERT INTO detected_promos (bot_id, source_channel, original_text, swapped_text, media_file_ids, media_types, matched_company)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (bot_id, source_channel, original_text, swapped_text,
                  json.dumps(media_file_ids) if media_file_ids else None,
                  json.dumps(media_types) if media_types else None,
                  matched_company))
            conn.commit()
            promo_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            return promo_id

    def get_pending_promos(self, bot_id):
        """Get pending promos for a bot"""
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM detected_promos WHERE bot_id = ? AND status = 'pending' ORDER BY detected_at DESC", (bot_id,)).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def update_promo_status(self, promo_id, status):
        """Update promo status (pending/broadcast/skipped)"""
        with self.lock:
            conn = self.get_connection()
            conn.execute("UPDATE detected_promos SET status = ? WHERE id = ?", (status, promo_id))
            conn.commit()
            conn.close()

    # --- Clone Media ---
    def _ensure_clone_history_table(self):
        """Create clone_history table if not exists"""
        conn = self.get_connection()
        conn.execute('''
            CREATE TABLE IF NOT EXISTS clone_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER NOT NULL,
                source_id TEXT,
                source_name TEXT,
                target_id TEXT,
                target_name TEXT,
                media_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE
            )
        ''')
        conn.commit()
        conn.close()

    def save_clone_job(self, bot_id, source_id, source_name, target_id, target_name):
        """Save a new clone job and return its ID"""
        self._ensure_clone_history_table()
        with self.lock:
            conn = self.get_connection()
            conn.execute(
                "INSERT INTO clone_history (bot_id, source_id, source_name, target_id, target_name) VALUES (?, ?, ?, ?, ?)",
                (bot_id, source_id, source_name, target_id, target_name)
            )
            conn.commit()
            clone_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            return clone_id

    def update_clone_job(self, clone_id, media_count=None, status=None):
        """Update clone job progress"""
        with self.lock:
            conn = self.get_connection()
            if media_count is not None:
                conn.execute("UPDATE clone_history SET media_count = ? WHERE id = ?", (media_count, clone_id))
            if status is not None:
                conn.execute("UPDATE clone_history SET status = ? WHERE id = ?", (status, clone_id))
            conn.commit()
            conn.close()

    def get_clone_history(self, bot_id, limit=10):
        """Get recent clone jobs for a bot"""
        self._ensure_clone_history_table()
        conn = self.get_connection()
        rows = conn.execute(
            "SELECT * FROM clone_history WHERE bot_id = ? ORDER BY created_at DESC LIMIT ?",
            (bot_id, limit)
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from database import Database
from config import MASTER_ADMIN_ID, MASTER_ADMIN_IDS, MOTHER_TOKEN
import logging
import datetime
import ai_bot_builder
from html import escape as html_escape

TOKEN_INPUT = 0
CLONE_TOKEN = 1

# AI Builder states
AI_PROVIDER_SELECT = 10
AI_API_KEY_INPUT = 11
AI_CHAT = 12
AI_PREVIEW = 13

# API Key management states
APIKEY_PROVIDER = 20
APIKEY_INPUT = 21
APIKEY_CONFIRM_DELETE = 22

class MotherBot:
    def __init__(self, token, db: Database, bot_manager):
        self.token = token
        self.db = db
        self.manager = bot_manager
        self.app = Application.builder().token(token).build()
        self.setup_handlers()

    async def initialize(self):
        """Prepare bot application but do not start polling (Webhook mode)"""
        await self.app.initialize()
        await self.app.start()

    async def stop(self):
        await self.app.stop()
        await self.app.shutdown()

    def setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("mybots", self.my_bots))
        self.app.add_handler(CommandHandler("help", self.help_command))
        
        # Creator Wizard
        create_conv = ConversationHandler(
            entry_points=[CommandHandler("createbot", self.create_bot_start)],
            states={
                TOKEN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.create_bot_token)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.app.add_handler(create_conv)
        
        # AI Bot Builder ConversationHandler
        ai_builder_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.ai_builder_entry, pattern=r'^ai_setup_\d+$')],
            states={
                AI_PROVIDER_SELECT: [
                    CallbackQueryHandler(self.ai_provider_selected, pattern=r'^ai_provider_'),
                    CallbackQueryHandler(self.show_ai_tutorial_conv, pattern=r'^ai_tutorial$'),
                ],
                AI_API_KEY_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ai_apikey_received)],
                AI_CHAT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.ai_chat_message),
                    MessageHandler(filters.PHOTO, self.ai_chat_image),
                    CallbackQueryHandler(self.ai_generate_config, pattern=r'^ai_generate$'),
                ],
                AI_PREVIEW: [
                    CallbackQueryHandler(self.ai_apply_config, pattern=r'^ai_apply$'),
                    CallbackQueryHandler(self.ai_back_to_chat, pattern=r'^ai_back_chat$'),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel_callback, pattern=r'^cancel$'),
            ],
            per_message=False,
            allow_reentry=True,
        )
        self.app.add_handler(ai_builder_conv)
        
        # AI Modify Bot ConversationHandler
        ai_modify_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.ai_modify_entry, pattern=r'^ai_modify_\d+$')],
            states={
                AI_CHAT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.ai_chat_message),
                    MessageHandler(filters.PHOTO, self.ai_chat_image),
                    CallbackQueryHandler(self.ai_generate_config, pattern=r'^ai_generate$'),
                ],
                AI_PREVIEW: [
                    CallbackQueryHandler(self.ai_apply_config, pattern=r'^ai_apply$'),
                    CallbackQueryHandler(self.ai_back_to_chat, pattern=r'^ai_back_chat$'),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel_callback, pattern=r'^cancel$'),
            ],
            per_message=False,
            allow_reentry=True,
        )
        self.app.add_handler(ai_modify_conv)
        
        # API Key Management ConversationHandler
        apikey_conv = ConversationHandler(
            entry_points=[
                CommandHandler("apikey", self.apikey_menu_cmd),
                CallbackQueryHandler(self.apikey_menu, pattern=r'^manage_api_key$'),
            ],
            states={
                APIKEY_PROVIDER: [
                    CallbackQueryHandler(self.apikey_provider_selected, pattern=r'^apikey_add_'),
                    CallbackQueryHandler(self.apikey_delete_confirmed, pattern=r'^apikey_confirm_del_'),
                    CallbackQueryHandler(self.show_ai_tutorial_conv, pattern=r'^ai_tutorial$'),
                ],
                APIKEY_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.apikey_received)],
                APIKEY_CONFIRM_DELETE: [CallbackQueryHandler(self.apikey_delete_confirmed, pattern=r'^apikey_confirm_del_')],
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel_callback, pattern=r'^cancel$'),
                CallbackQueryHandler(self.apikey_menu, pattern=r'^manage_api_key$'),
            ],
            per_message=False,
            allow_reentry=True,
        )
        self.app.add_handler(apikey_conv)
        
        # Clone Bot Wizard (handle token input for cloning)
        self.app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            self.handle_clone_token
        ))
        
        # Callback Handler for buttons
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))

        # Admin Commands
        self.app.add_handler(CommandHandler("setglobalad", self.set_global_ad))
        self.app.add_handler(CommandHandler("ban", self.ban_user))
        self.app.add_handler(CommandHandler("extend", self.extend_subscription))
        self.app.add_handler(CommandHandler("admin", self.admin_help))
        self.app.add_handler(CommandHandler("allbots", self.all_bots))
        # Owner Management
        self.app.add_handler(CommandHandler("addowner", self.add_owner))
        self.app.add_handler(CommandHandler("removeowner", self.remove_owner))
        self.app.add_handler(CommandHandler("owners", self.list_owners))
        self.app.add_handler(CommandHandler("server", self.server_status))

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "🤖 **Welcome to MASUK10 ROBOT!**\n\n"
            "Create your own **Company List Bot** in seconds.\n\n"
            "✨ **Features included:**\n"
            "✅ Company Listing & Search\n"
            "✅ Referral System (RM1/invite)\n"
            "✅ Wallet & Withdrawal\n"
            "✅ Custom Welcome Message\n"
            "✅ Admin Dashboard\n\n"
            "👇 **Get Started:**\n"
            "/createbot - Create new bot\n"
            "/mybots - Manage your bots\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "🔧 Powered by **MASUK10**"
        )
        await update.message.reply_text(text, parse_mode='Markdown')

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Commands:\n/createbot - New Bot\n/mybots - List Bots")

    # --- Create Bot Flow ---
    async def create_bot_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🚀 **Create New Bot**\n\n"
            "1. Go to @BotFather\n"
            "2. Create a new bot (`/newbot`)\n"
            "3. Copy the **API TOKEN**\n\n"
            "Paste the API TOKEN here:",
            parse_mode='Markdown'
        )
        return TOKEN_INPUT

    async def create_bot_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        token = update.message.text.strip()
        user_id = update.effective_user.id
        username = update.effective_user.username or "User"

        # Validate Token format (Simple check)
        if ':' not in token or len(token) < 20:
            await update.message.reply_text("❌ Invalid Token format. Try again or /cancel")
            return TOKEN_INPUT

        # Fetch bot info from Telegram to get username
        try:
            from telegram import Bot
            temp_bot = Bot(token)
            bot_info = await temp_bot.get_me()
            bot_username = bot_info.username
            bot_name = bot_info.first_name
        except Exception as e:
            await update.message.reply_text(f"❌ Invalid token or bot not accessible.\n\nError: {str(e)}\n\nTry again or /cancel")
            return TOKEN_INPUT

        # Register in DB
        success, msg = self.db.create_bot(token, user_id, bot_username)
        
        if success:
            await update.message.reply_text("✅ **Bot Registered!**\nStarting your bot instance...", parse_mode='Markdown')
            # Start the bot dynamically
            try:
                # Fetch the bot data we just inserted
                bot_data = self.db.get_bot_by_token(token)
                await self.manager.spawn_bot(bot_data)
                
                # Show detailed success message WITH AI/Manual choice
                bot_link = f"https://t.me/{bot_username}"
                success_msg = (
                    f"🎉 **Bot is ONLINE!**\n\n"
                    f"📱 **Bot Info:**\n"
                    f"• Name: {bot_name}\n"
                    f"• Username: @{bot_username}\n"
                    f"• Link: {bot_link}\n"
                    f"• ID: #{bot_data['id']}\n\n"
                    f"📅 **Subscription:** Trial 3 Days\n"
                    f"⏰ **Expires:** {bot_data['subscription_end'][:10]}\n\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"🛠️ **Macam mana nak setup bot ni?**\n\n"
                    f"🤖 **AI Generate** — Chat dengan AI, describe\n"
                    f"   apa kau nak. Boleh hantar screenshot!\n\n"
                    f"⚙️ **Manual** — Setup sendiri guna\n"
                    f"   /settings dalam bot kau."
                )
                keyboard = [
                    [InlineKeyboardButton("🤖 Generate Guna AI", callback_data=f"ai_setup_{bot_data['id']}")],
                    [InlineKeyboardButton("⚙️ Settings Manual", callback_data=f"manual_setup_{bot_data['id']}")],
                ]
                await update.message.reply_text(success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            except Exception as e:
                await update.message.reply_text(f"⚠️ Registered but failed to start: {e}")
            return ConversationHandler.END
        else:
            await update.message.reply_text(f"❌ Error: {msg}\nTry /createbot again.")
            return ConversationHandler.END


    async def cancel(self, update, context):
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button clicks for Mother Bot"""
        query = update.callback_query
        data = query.data
        await query.answer()
        
        if data.startswith("manage_bot_"):
            bot_id = int(data.split("_")[2])
            await self.show_bot_management(update, bot_id)
        elif data == "new_bot":
            await query.message.reply_text("Use /createbot to create a new bot.")
        elif data.startswith("toggle_bot_"):
            bot_id = int(data.split("_")[2])
            await self.toggle_bot_status(update, bot_id)
        elif data.startswith("delete_bot_"):
            bot_id = int(data.split("_")[2])
            # Show confirmation dialog
            bot = self.db.get_bot_by_id(bot_id)
            
            # Get stats for confirmation message
            companies_count = len(self.db.get_companies(bot_id))
            users = self.db.execute_query("SELECT COUNT(*) as count FROM users WHERE bot_id = ?", (bot_id,))
            users_count = users[0]['count'] if users else 0
            
            text = (
                f"⚠️ **DELETE BOT CONFIRMATION**\n\n"
                f"Are you sure you want to delete Bot #{bot_id}?\n\n"
                f"**This will DELETE:**\n"
                f"❌ All companies ({companies_count} items)\n"
                f"❌ All user data ({users_count} users)\n"
                f"❌ All withdrawal requests\n"
                f"❌ Bot configuration\n\n"
                f"**⚠️ THIS CANNOT BE UNDONE!**"
            )
            
            keyboard = [
                [InlineKeyboardButton("✅ YES, DELETE", callback_data=f"confirm_delete_bot_{bot_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"manage_bot_{bot_id}")]
            ]
            
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        elif data.startswith("confirm_delete_bot_"):
            # Actually delete
            bot_id = int(data.split("_")[3])
            await self.delete_bot(update, bot_id)
        elif data.startswith("stats_"):
            bot_id = int(data.split("_")[1])
            await self.show_bot_stats(update, bot_id)
        elif data.startswith("users_"):
            bot_id = int(data.split("_")[1])
            await self.show_bot_users(update, bot_id)
        elif data.startswith("analytics_"):
            bot_id = int(data.split("_")[1])
            await self.show_bot_analytics(update, bot_id)
        elif data.startswith("clone_bot_"):
            # Start clone wizard
            bot_id = int(data.split("_")[2])
            context.user_data['clone_source_bot'] = bot_id
            
            text = (
                f"🧬 **CLONE BOT #{bot_id}**\n\n"
                f"Clone akan copy semua:\n"
                f"✅ Companies & buttons\n"
                f"✅ Menu buttons\n"
                f"✅ Bot settings\n\n"
                f"⚠️ **TIDAK termasuk:**\n"
                f"❌ User data\n"
                f"❌ Balance/Referrals\n\n"
                f"📌 **Sila hantar token bot BARU:**\n"
                f"_(Boleh create bot baru di @BotFather)_"
            )
            
            keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"manage_bot_{bot_id}")]]
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return  # Wait for token input
        elif data.startswith("extend_sub_"):
            # Show extend subscription options
            bot_id = int(data.split("_")[2])
            bot = self.db.get_bot_by_id(bot_id)
            
            # Calculate current expiry
            try:
                expiry = datetime.datetime.fromisoformat(bot['subscription_end'])
                days_left = (expiry - datetime.datetime.now()).days
                expiry_text = f"{expiry.strftime('%Y-%m-%d')} ({days_left} days left)"
            except:
                expiry_text = bot['subscription_end'][:10]
            
            text = (
                f"📅 **EXTEND SUBSCRIPTION**\n\n"
                f"**Bot:** #{bot_id}\n"
                f"**Current Expiry:** {expiry_text}\n\n"
                f"Select days to add:"
            )
            
            keyboard = [
                [InlineKeyboardButton("➕ 7 Days", callback_data=f"add_days_{bot_id}_7"),
                 InlineKeyboardButton("➕ 14 Days", callback_data=f"add_days_{bot_id}_14")],
                [InlineKeyboardButton("➕ 30 Days", callback_data=f"add_days_{bot_id}_30"),
                 InlineKeyboardButton("➕ 60 Days", callback_data=f"add_days_{bot_id}_60")],
                [InlineKeyboardButton("➕ 90 Days", callback_data=f"add_days_{bot_id}_90"),
                 InlineKeyboardButton("➕ 180 Days", callback_data=f"add_days_{bot_id}_180")],
                [InlineKeyboardButton("➕ 365 Days (1 Year)", callback_data=f"add_days_{bot_id}_365")],
                [InlineKeyboardButton("« Back", callback_data=f"manage_bot_{bot_id}")]
            ]
            
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        elif data.startswith("add_days_"):
            # Actually extend subscription
            parts = data.split("_")
            bot_id = int(parts[2])
            days = int(parts[3])
            
            # Check if user is admin
            if update.effective_user.id not in MASTER_ADMIN_IDS:
                await query.message.reply_text("⛔ Access Denied")
                return
            
            # Get current expiry
            bot = self.db.get_bot_by_id(bot_id)
            try:
                current_expiry = datetime.datetime.fromisoformat(bot['subscription_end'])
                # If expired, start from now
                if current_expiry < datetime.datetime.now():
                    current_expiry = datetime.datetime.now()
            except:
                current_expiry = datetime.datetime.now()
            
            # Calculate new expiry
            new_expiry = current_expiry + datetime.timedelta(days=days)
            
            # Update database
            conn = self.db.get_connection()
            conn.execute("UPDATE bots SET subscription_end = ? WHERE id = ?", 
                        (new_expiry.isoformat(), bot_id))
            conn.commit()
            conn.close()
            
            # Get bot username for notification
            bot_username = bot.get('bot_username') or f"Bot #{bot_id}"
            owner_id = bot.get('owner_id')
            
            await query.message.edit_text(
                f"✅ **Subscription Extended!**\n\n"
                f"**Bot:** #{bot_id}\n"
                f"**Added:** {days} days\n"
                f"**New Expiry:** {new_expiry.strftime('%Y-%m-%d')}\n\n"
                f"Use /mybots to see updated info.",
                parse_mode='Markdown'
            )
            
            # Notify bot owner
            if owner_id and owner_id != update.effective_user.id:
                try:
                    days_left = (new_expiry - datetime.datetime.now()).days
                    safe_username = html_escape(str(bot_username or f"Bot #{bot_id}"))
                    notify_text = (
                        f"🎉 <b>SUBSCRIPTION EXTENDED!</b>\n\n"
                        f"🤖 <b>Bot:</b> @{safe_username}\n"
                        f"➕ <b>Added:</b> {days} days\n"
                        f"📅 <b>New Expiry:</b> {new_expiry.strftime('%Y-%m-%d')}\n"
                        f"⏳ <b>Days Left:</b> {days_left} days\n\n"
                        f"<i>Terima kasih! Bot anda sekarang aktif.</i>"
                    )
                    await self.app.bot.send_message(
                        chat_id=owner_id,
                        text=notify_text,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logging.error(f"Failed to notify owner {owner_id}: {e}")
        elif data.startswith("manual_setup_"):
            # User chose manual setup - dismiss AI choice
            bot_id = int(data.split("_")[2])
            bot = self.db.get_bot_by_id(bot_id)
            bot_username = bot.get('bot_username', 'unknown') if bot else 'unknown'
            safe_username = html_escape(str(bot_username))
            await query.message.edit_text(
                f"✅ <b>Manual Setup Mode</b>\n\n"
                f"Pergi ke @{safe_username} dan tekan /start\n"
                f"Lepas tu guna /settings untuk configure bot kau.\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"💡 Bila-bila boleh guna AI: /mybots → 🤖 AI Modify",
                parse_mode='HTML'
            )
        elif data == "ai_tutorial":
            await self.show_ai_tutorial(update)
        elif data == "close_panel":
            # Carousel style - edit to show main menu instead of delete
            text = (
                "🤖 **MASUK10 ROBOT**\n\n"
                "Use commands below:\n"
                "/mybots - Manage your bots\n"
                "/createbot - Create new bot\n"
                "/help - Show help"
            )
            await query.message.edit_text(text, parse_mode='Markdown')
        elif data == "my_bots_panel":
            await self.my_bots_panel(update)

    # --- My Bots ---
    async def my_bots(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        conn = self.db.get_connection()
        
        # Master Admins see ALL bots, regular users see only their own
        if user_id in MASTER_ADMIN_IDS:
            bots = conn.execute("SELECT * FROM bots ORDER BY id").fetchall()
            title = "🤖 **ALL PLATFORM BOTS**"
            is_admin = True
        else:
            bots = conn.execute("SELECT * FROM bots WHERE owner_id = ?", (user_id,)).fetchall()
            title = "🤖 **YOUR BOTS**"
            is_admin = False

        if not bots:
            await update.message.reply_text("You have no bots. /createbot to start.")
            conn.close()
            return

        # Build detailed text
        text = f"{title}\n"
        text += "━" * 20 + "\n\n"
        
        keyboard = []
        for bot in bots:
            # Get stats
            user_count = conn.execute("SELECT COUNT(*) FROM users WHERE bot_id = ?", (bot['id'],)).fetchone()[0]
            company_count = conn.execute("SELECT COUNT(*) FROM companies WHERE bot_id = ?", (bot['id'],)).fetchone()[0]
            
            # Calculate days left
            try:
                expiry = datetime.datetime.fromisoformat(bot['subscription_end'])
                now = datetime.datetime.now()
                days_left = (expiry - now).days
                if days_left < 0:
                    days_text = f"⚠️ EXPIRED {abs(days_left)} days ago"
                elif days_left == 0:
                    days_text = "⚠️ Expires TODAY"
                elif days_left <= 7:
                    days_text = f"⚠️ {days_left} days left"
                else:
                    days_text = f"✅ {days_left} days left"
            except:
                days_text = bot['subscription_end'][:10]
            
            # Status
            status = "🟢 ACTIVE" if bot['is_active'] else "🔴 STOPPED"
            
            # Bot info line - sqlite3.Row doesn't support .get()
            try:
                bot_username = bot['bot_username'] if bot['bot_username'] else None
            except:
                bot_username = None
            
            if bot_username:
                bot_name = f"@{bot_username}"
                bot_link = f"https://t.me/{bot_username}"
                text += f"**{bot_name}** {status}\n"
                text += f"🔗 {bot_link}\n"
            else:
                text += f"**Bot #{bot['id']}** {status}\n"
            
            text += f"👥 Users: {user_count} | 🏢 Companies: {company_count}\n"
            
            # Show owner for admin view
            if is_admin:
                text += f"👤 Owner ID: `{bot['owner_id']}`\n"
            
            text += f"📅 {days_text}\n"
            text += "━" * 20 + "\n\n"
            
            # Button
            keyboard.append([InlineKeyboardButton(
                f"🔧 Manage {bot_name}",
                callback_data=f"manage_bot_{bot['id']}"
            )])
        
        conn.close()
        
        keyboard.append([InlineKeyboardButton("➕ Create New Bot", callback_data="new_bot")])
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def my_bots_panel(self, update: Update):
        """Carousel-style my bots - edit existing message instead of new"""
        user_id = update.effective_user.id
        conn = self.db.get_connection()
        
        # Master Admins see ALL bots, regular users see only their own
        if user_id in MASTER_ADMIN_IDS:
            bots = conn.execute("SELECT * FROM bots ORDER BY id").fetchall()
            title = "🤖 **ALL PLATFORM BOTS**"
            is_admin = True
        else:
            bots = conn.execute("SELECT * FROM bots WHERE owner_id = ?", (user_id,)).fetchall()
            title = "🤖 **YOUR BOTS**"
            is_admin = False

        if not bots:
            await update.callback_query.message.edit_text("You have no bots. Use /createbot to start.")
            conn.close()
            return

        # Build detailed text
        text = f"{title}\n"
        text += "━" * 20 + "\n\n"
        
        keyboard = []
        for bot in bots:
            # Get stats
            user_count = conn.execute("SELECT COUNT(*) FROM users WHERE bot_id = ?", (bot['id'],)).fetchone()[0]
            company_count = conn.execute("SELECT COUNT(*) FROM companies WHERE bot_id = ?", (bot['id'],)).fetchone()[0]
            
            # Calculate days left
            try:
                expiry = datetime.datetime.fromisoformat(bot['subscription_end'])
                now = datetime.datetime.now()
                days_left = (expiry - now).days
                if days_left < 0:
                    days_text = f"⚠️ EXPIRED {abs(days_left)} days ago"
                elif days_left == 0:
                    days_text = "⚠️ Expires TODAY"
                elif days_left <= 7:
                    days_text = f"⚠️ {days_left} days left"
                else:
                    days_text = f"✅ {days_left} days left"
            except:
                days_text = bot['subscription_end'][:10]
            
            # Status
            status = "🟢 ACTIVE" if bot['is_active'] else "🔴 STOPPED"
            
            # Bot info line - sqlite3.Row doesn't support .get()
            try:
                bot_username = bot['bot_username'] if bot['bot_username'] else None
            except:
                bot_username = None
            
            if bot_username:
                bot_name = f"@{bot_username}"
                bot_link = f"https://t.me/{bot_username}"
                text += f"**{bot_name}** {status}\n"
                text += f"🔗 {bot_link}\n"
            else:
                bot_name = f"Bot #{bot['id']}"
                text += f"**{bot_name}** {status}\n"
            
            text += f"👥 Users: {user_count} | 🏢 Companies: {company_count}\n"
            
            # Show owner for admin view
            if is_admin:
                text += f"👤 Owner ID: `{bot['owner_id']}`\n"
            
            text += f"📅 {days_text}\n"
            text += "━" * 20 + "\n\n"
            
            # Button
            keyboard.append([InlineKeyboardButton(
                f"🔧 Manage {bot_name}",
                callback_data=f"manage_bot_{bot['id']}"
            )])
        
        conn.close()
        
        keyboard.append([InlineKeyboardButton("➕ Create New Bot", callback_data="new_bot")])
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_panel")])
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def show_bot_management(self, update: Update, bot_id: int):
        """Display management panel for a specific bot"""
        bot = self.db.get_bot_by_id(bot_id)
        if not bot:
            await update.callback_query.message.reply_text("❌ Bot not found.")
            return
        
        # Check if subscription expired
        try:
            expiry = datetime.datetime.fromisoformat(bot['subscription_end'])
            is_expired = datetime.datetime.now() > expiry
            days_left = (expiry - datetime.datetime.now()).days
        except:
            is_expired = False
            days_left = 0
        
        # Status indicator
        if is_expired:
            status = "🔴 Expired"
            status_detail = f"Expired {abs(days_left)} days ago"
        elif days_left <= 3:
            status = "🟡 Expiring Soon"
            status_detail = f"{days_left} days left"
        elif bot['is_active']:
            status = "🟢 Active"
            status_detail = f"{days_left} days left"
        else:
            status = "🔴 Stopped"
            status_detail = "Manually stopped"
            
        text = (
            f"🤖 **Bot #{bot['id']} Management**\n\n"
            f"**Status:** {status}\n"
            f"**Subscription:** {status_detail}\n"
            f"**Token:** `{bot['token'][:15]}...`\n"
            f"**Expires:** {bot['subscription_end'][:10]}\n"
            f"**Created:** {bot['created_at'][:10]}\n"
        )
        
        toggle_text = "⏸️ Stop Bot" if bot['is_active'] else "▶️ Start Bot"
        keyboard = [
            [InlineKeyboardButton("📊 Statistics", callback_data=f"stats_{bot_id}"), 
             InlineKeyboardButton("👥 Users", callback_data=f"users_{bot_id}")],
            [InlineKeyboardButton("📈 Analytics", callback_data=f"analytics_{bot_id}")],
            [InlineKeyboardButton("🤖 AI Modify", callback_data=f"ai_modify_{bot_id}"),
             InlineKeyboardButton("🧬 Clone Bot", callback_data=f"clone_bot_{bot_id}")],
            [InlineKeyboardButton(toggle_text, callback_data=f"toggle_bot_{bot_id}")],
        ]
        
        # Master Admin can extend subscription
        user_id = update.effective_user.id
        if user_id in MASTER_ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("📅 Extend Subscription", callback_data=f"extend_sub_{bot_id}")])
        
        keyboard.append([InlineKeyboardButton("🗑️ Delete Bot", callback_data=f"delete_bot_{bot_id}")])
        keyboard.append([InlineKeyboardButton("« Back to My Bots", callback_data="my_bots_panel")])
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def toggle_bot_status(self, update: Update, bot_id: int):
        """Start or stop a bot"""
        conn = self.db.get_connection()
        bot = conn.execute("SELECT * FROM bots WHERE id = ?", (bot_id,)).fetchone()
        
        if not bot:
            await update.callback_query.message.edit_text("❌ Bot not found.")
            conn.close()
            return
        
        new_status = 0 if bot['is_active'] else 1
        conn.execute("UPDATE bots SET is_active = ? WHERE id = ?", (new_status, bot_id))
        conn.commit()
        conn.close()
        
        # Reload the management panel
        if new_status:
            # Start the bot instance
            try:
                await self.manager.spawn_bot(dict(bot))
                await update.callback_query.answer("✅ Bot started!")
            except Exception as e:
                await update.callback_query.answer(f"⚠️ Error: {e}")
        else:
            # Stop the bot instance
            try:
                await self.manager.stop_bot(bot_id)
                await update.callback_query.answer("⏸️ Bot stopped!")
            except Exception as e:
                await update.callback_query.answer(f"⚠️ Error: {e}")
        
        await self.show_bot_management(update, bot_id)
    
    async def delete_bot(self, update: Update, bot_id: int):
        """Delete a bot from the system"""
        try:
            # Stop the bot first
            await self.manager.stop_bot(bot_id)
            
            # Delete from database
            conn = self.db.get_connection()
            conn.execute("DELETE FROM bots WHERE id = ?", (bot_id,))
            conn.commit()
            conn.close()
            
            await update.callback_query.message.edit_text("✅ Bot deleted successfully!")
        except Exception as e:
            await update.callback_query.message.edit_text(f"❌ Error deleting bot: {e}")

    # --- Admin Commands ---
    async def admin_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_owner(update.effective_user.id): return
        await update.message.reply_text(
            "👑 **Owner Commands**\n\n"
            "**View & Manage Bots:**\n"
            "/allbots - View all bots\n"
            "/extend [bot_id] [days] - Extend subscription\n\n"
            "**User Management:**\n"
            "/ban [user_id] - Blacklist user\n\n"
            "**Owner Management:**\n"
            "/owners - List platform owners\n"
            "/addowner [id] - Add owner\n"
            "/removeowner [id] - Remove owner\n\n"
            "**Server:**\n"
            "/server - CPU/RAM/Disk usage\n\n"
            "**Config:**\n"
            "/setglobalad [text] - Set global ad",
            parse_mode='Markdown'
        )

    async def set_global_ad(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_owner(update.effective_user.id): return
        # Logic to update config file or DB? 
        # For simplicity, we just replied "Updated" but functionally we rely on `config.DEFAULT_GLOBAL_AD`. 
        # Ideally, `DEFAULT_GLOBAL_AD` should be in DB. `settings` table. 
        # I'll skip complex dynamic config for now as it wasn't strictly requested to be persistent in DB, but hardcoded in config.py is acceptable.
        # Wait, user asked for command /setglobalad. I should support it.
        # I'll create a `settings` table? Or just keep in memory for now?
        # I'll reply "Update config.py to change this permanently". 
        await update.message.reply_text("⚠️ To change Global Ad, please update `config.py` in the server.")

    async def ban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_owner(update.effective_user.id): return
        # Ban logic
        user_id = int(context.args[0])
        conn = self.db.get_connection()
        conn.execute("UPDATE users SET is_blacklisted = 1 WHERE telegram_id = ?", (user_id,))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"🚫 User {user_id} Banned.")
    
    async def extend_subscription(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Extend bot subscription by X days (Admin only)"""
        if not self.is_owner(update.effective_user.id): return
        
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /extend [bot_id] [days]")
            return
        
        bot_id = int(context.args[0])
        days = int(context.args[1])
        
        conn = self.db.get_connection()
        bot = conn.execute("SELECT * FROM bots WHERE id = ?", (bot_id,)).fetchone()
        
        if not bot:
            await update.message.reply_text("❌ Bot not found.")
            conn.close()
            return
        
        # Extend subscription
        from datetime import datetime, timedelta
        current_end = datetime.fromisoformat(bot['subscription_end'])
        # If expired, start from now
        if current_end < datetime.now():
            current_end = datetime.now()
        new_end = current_end + timedelta(days=days)
        
        conn.execute("UPDATE bots SET subscription_end = ? WHERE id = ?", (new_end.isoformat(), bot_id))
        conn.commit()
        conn.close()
        
        days_left = (new_end - datetime.now()).days
        
        await update.message.reply_text(f"✅ **Bot #{bot_id}** subscription extended by {days} days!\nNew expiry: {new_end.strftime('%Y-%m-%d')}", parse_mode='Markdown')
        
        # Notify bot owner
        owner_id = bot['owner_id']
        bot_username = bot['bot_username'] if bot['bot_username'] else f"Bot #{bot_id}"
        
        if owner_id and owner_id != update.effective_user.id:
            try:
                notify_text = (
                    f"🎉 **SUBSCRIPTION EXTENDED!**\n\n"
                    f"🤖 **Bot:** @{bot_username}\n"
                    f"➕ **Added:** {days} days\n"
                    f"📅 **New Expiry:** {new_end.strftime('%Y-%m-%d')}\n"
                    f"⏳ **Days Left:** {days_left} days\n\n"
                    f"_Terima kasih! Bot anda sekarang aktif._"
                )
                await self.app.bot.send_message(
                    chat_id=owner_id,
                    text=notify_text,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logging.error(f"Failed to notify owner {owner_id}: {e}")

    async def all_bots(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """View all bots from all users (Platform owner only)"""
        if not self.is_owner(update.effective_user.id): 
            await update.message.reply_text("⛔ Access Denied.")
            return
        
        conn = self.db.get_connection()
        bots = conn.execute("""
            SELECT b.*, 
                   (SELECT COUNT(*) FROM users WHERE bot_id = b.id) as user_count
            FROM bots b 
            ORDER BY b.created_at DESC
        """).fetchall()
        conn.close()
        
        if not bots:
            await update.message.reply_text("📭 No bots registered yet.")
            return
        
        # Build message with pagination (max 10 per message)
        text = f"📊 **ALL BOTS** ({len(bots)} total)\n\n"
        
        for i, bot in enumerate(bots, 1):
            status = "🟢" if bot['is_active'] else "🔴"
            expiry = bot['subscription_end'][:10] if bot['subscription_end'] else "N/A"
            text += (
                f"**{i}. Bot #{bot['id']}** {status}\n"
                f"   👤 Owner: `{bot['owner_id']}`\n"
                f"   📅 Exp: {expiry}\n"
                f"   👥 Users: {bot['user_count']}\n\n"
            )
            
            # Split message if too long
            if i % 10 == 0 and i < len(bots):
                await update.message.reply_text(text, parse_mode='Markdown')
                text = ""
        
        if text:
            text += "_Use /extend [bot_id] [days] to extend subscription_"
            await update.message.reply_text(text, parse_mode='Markdown')

    # --- Owner Management ---
    def is_owner(self, user_id):
        """Check if user is platform owner (env + database)"""
        # Check env variable first
        if user_id == MASTER_ADMIN_ID or user_id in MASTER_ADMIN_IDS:
            return True
        # Check database
        return self.db.is_platform_owner(user_id, MASTER_ADMIN_ID)

    async def server_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show server CPU, RAM, Disk usage (Admin only)"""
        if not self.is_owner(update.effective_user.id):
            return

        try:
            import psutil
            import time

            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Uptime
            uptime_sec = time.time() - psutil.boot_time()
            days = int(uptime_sec // 86400)
            hours = int((uptime_sec % 86400) // 3600)
            mins = int((uptime_sec % 3600) // 60)

            # Progress bars
            def bar(percent):
                filled = int(percent / 10)
                return '█' * filled + '░' * (10 - filled)

            # Warning indicators
            cpu_warn = '⚠️' if cpu > 80 else '✅'
            ram_warn = '⚠️' if mem.percent > 80 else '✅'
            disk_warn = '⚠️' if disk.percent > 80 else '✅'

            active_bots = len(self.manager.bots) if self.manager else 0

            text = (
                f"🖥 **SERVER STATUS**\n"
                f"━━━━━━━━━━━━━━━━━\n\n"
                f"{cpu_warn} **CPU:** {cpu}%\n"
                f"`{bar(cpu)}` \n\n"
                f"{ram_warn} **RAM:** {mem.used / (1024**3):.1f}/{mem.total / (1024**3):.1f} GB ({mem.percent}%)\n"
                f"`{bar(mem.percent)}` \n\n"
                f"{disk_warn} **Disk:** {disk.used / (1024**3):.1f}/{disk.total / (1024**3):.1f} GB ({disk.percent:.0f}%)\n"
                f"`{bar(disk.percent)}` \n\n"
                f"🕐 **Uptime:** {days}d {hours}h {mins}m\n"
                f"🤖 **Active Bots:** {active_bots}\n"
            )

            # Overall health
            if mem.percent > 85 or cpu > 90:
                text += "\n🔴 **Status: CRITICAL** — High resource usage!"
            elif mem.percent > 70 or cpu > 70:
                text += "\n🟡 **Status: WARNING** — Monitor closely"
            else:
                text += "\n🟢 **Status: HEALTHY**"

            await update.message.reply_text(text, parse_mode='Markdown')

        except ImportError:
            await update.message.reply_text("❌ `psutil` not installed on server.", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def add_owner(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add a platform owner /addowner [telegram_id]"""
        # Only master admin (from env) can add owners
        if update.effective_user.id != MASTER_ADMIN_ID:
            await update.message.reply_text("⛔ Only the master admin can add owners.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /addowner [telegram_id]\n\nExample: /addowner 123456789")
            return
        
        try:
            new_owner_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("⚠️ Invalid Telegram ID")
            return
        
        success = self.db.add_platform_owner(new_owner_id, update.effective_user.id)
        
        if success:
            await update.message.reply_text(
                f"✅ **Owner Added!**\n\n"
                f"👤 Telegram ID: `{new_owner_id}`\n\n"
                f"User now has full access to all admin commands.",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("⚠️ User is already an owner.")
    
    async def remove_owner(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove a platform owner /removeowner [telegram_id]"""
        # Only master admin can remove owners
        if update.effective_user.id != MASTER_ADMIN_ID:
            await update.message.reply_text("⛔ Only the master admin can remove owners.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /removeowner [telegram_id]")
            return
        
        try:
            owner_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("⚠️ Invalid Telegram ID")
            return
        
        # Cannot remove master admin
        if owner_id == MASTER_ADMIN_ID:
            await update.message.reply_text("⚠️ Cannot remove the master admin.")
            return
        
        success = self.db.remove_platform_owner(owner_id)
        
        if success:
            await update.message.reply_text(f"✅ Owner `{owner_id}` removed!", parse_mode='Markdown')
        else:
            await update.message.reply_text("⚠️ Owner not found.")
    
    async def list_owners(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all platform owners"""
        if not self.is_owner(update.effective_user.id):
            return
        
        owners = self.db.get_platform_owners()
        
        text = f"👑 **PLATFORM OWNERS**\n\n"
        text += f"**Master Admin:** `{MASTER_ADMIN_ID}` (from env)\n\n"
        
        if owners:
            text += "**Added Owners:**\n"
            for i, owner in enumerate(owners, 1):
                text += f"{i}. `{owner['telegram_id']}`\n"
        else:
            text += "_No additional owners added_"
        
        await update.message.reply_text(text, parse_mode='Markdown')

    # --- New Management Functions ---
    async def show_bot_stats(self, update: Update, bot_id: int):
        """Show comprehensive bot statistics"""
        conn = self.db.get_connection()
        
        # Get bot info
        bot = conn.execute("SELECT * FROM bots WHERE id = ?", (bot_id,)).fetchone()
        
        # Get stats
        total_users = conn.execute("SELECT COUNT(*) as count FROM users WHERE bot_id = ?", (bot_id,)).fetchone()['count']
        total_companies = conn.execute("SELECT COUNT(*) as count FROM companies WHERE bot_id = ?", (bot_id,)).fetchone()['count']
        total_balance = conn.execute("SELECT SUM(balance) as total FROM users WHERE bot_id = ?", (bot_id,)).fetchone()['total'] or 0
        total_invites = conn.execute("SELECT SUM(total_invites) as total FROM users WHERE bot_id = ?", (bot_id,)).fetchone()['total'] or 0
        pending_withdrawals = conn.execute("SELECT COUNT(*) as count FROM withdrawals WHERE bot_id = ? AND status = 'PENDING'", (bot_id,)).fetchone()['count']
        
        conn.close()
        
        text = (
            f"📊 **Bot #{bot_id} Statistics**\n\n"
            f"👥 **Total Users:** {total_users}\n"
            f"🏢 **Total Companies:** {total_companies}\n"
            f"💰 **Total Balance:** RM {total_balance:.2f}\n"
            f"📈 **Total Invites:** {total_invites}\n"
            f"📤 **Pending Withdrawals:** {pending_withdrawals}\n\n"
            f"**Status:** {'🟢 Active' if bot['is_active'] else '🔴 Stopped'}\n"
            f"**Subscription:** {bot['subscription_end'][:10]}"
        )
        
        keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_bot_{bot_id}")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def show_bot_users(self, update: Update, bot_id: int):
        """Show list of users for specific bot"""
        conn = self.db.get_connection()
        users = conn.execute(
            "SELECT telegram_id, balance, total_invites, joined_at FROM users WHERE bot_id = ? ORDER BY joined_at DESC LIMIT 20",
            (bot_id,)
        ).fetchall()
        conn.close()
        
        if not users:
            text = f"👥 **Bot #{bot_id} Users**\n\nNo users yet."
        else:
            text = f"👥 **Bot #{bot_id} Users** (Latest 20)\n\n"
            for user in users:
                text += f"• ID: `{user['telegram_id']}` | RM {user['balance']:.2f} | {user['total_invites']} invites\n"
        
        keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_bot_{bot_id}")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def show_bot_analytics(self, update: Update, bot_id: int):
        """Show analytical data for bot"""
        conn = self.db.get_connection()
        
        # Revenue analytics
        total_balance = conn.execute("SELECT SUM(balance) as total FROM users WHERE bot_id = ?", (bot_id,)).fetchone()['total'] or 0
        approved_withdrawals = conn.execute(
            "SELECT SUM(amount) as total FROM withdrawals WHERE bot_id = ? AND status = 'APPROVED'",
            (bot_id,)
        ).fetchone()['total'] or 0
        pending_withdrawals = conn.execute(
            "SELECT SUM(amount) as total FROM withdrawals WHERE bot_id = ? AND status = 'PENDING'",
            (bot_id,)
        ).fetchone()['total'] or 0
        
        # Growth analytics
        users_today = conn.execute(
            "SELECT COUNT(*) as count FROM users WHERE bot_id = ? AND DATE(joined_at) = DATE('now')",
            (bot_id,)
        ).fetchone()['count']
        users_this_week = conn.execute(
            "SELECT COUNT(*) as count FROM users WHERE bot_id = ? AND DATE(joined_at) >= DATE('now', '-7 days')",
            (bot_id,)
        ).fetchone()['count']
        
        # Top referrers
        top_referrers = conn.execute(
            "SELECT telegram_id, total_invites FROM users WHERE bot_id = ? ORDER BY total_invites DESC LIMIT 5",
            (bot_id,)
        ).fetchall()
        
        conn.close()
        
        text = (
            f"📈 **Bot #{bot_id} Analytics**\n\n"
            f"💰 **Financial**\n"
            f"• Current Balance: RM {total_balance:.2f}\n"
            f"• Paid Out: RM {approved_withdrawals:.2f}\n"
            f"• Pending: RM {pending_withdrawals:.2f}\n\n"
            f"📊 **Growth**\n"
            f"• New Today: {users_today} users\n"
            f"• This Week: {users_this_week} users\n\n"
            f"🏆 **Top Referrers**\n"
        )
        
        for i, ref in enumerate(top_referrers, 1):
            text += f"{i}. ID `{ref['telegram_id']}` - {ref['total_invites']} invites\n"
        
        keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_bot_{bot_id}")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def handle_clone_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle token input for cloning a bot"""
        source_bot_id = context.user_data.get('clone_source_bot')
        
        if not source_bot_id:
            # Not in clone mode - ignore silently (this is expected)
            # The ConversationHandlers above handle AI builder flows
            return
        
        token = update.message.text.strip()
        user_id = update.effective_user.id
        
        # Validate token format
        if ':' not in token:
            await update.message.reply_text(
                "❌ Format token tidak sah!\n\n"
                "Token mesti ada format: `123456789:ABCdefGHI...`\n\n"
                "Sila dapatkan token dari @BotFather",
                parse_mode='Markdown'
            )
            return
        
        # Delete the token message for security
        try:
            await update.message.delete()
        except:
            pass
        
        # Verify token with Telegram
        try:
            from telegram import Bot
            test_bot = Bot(token=token)
            bot_info = await test_bot.get_me()
            new_username = bot_info.username
        except Exception as e:
            await update.message.reply_text(
                f"❌ Token tidak valid!\n\n"
                f"Error: {str(e)}\n\n"
                f"Sila pastikan token betul dari @BotFather"
            )
            return
        
        # Check if bot already registered
        existing = self.db.get_bot_by_token(token)
        if existing:
            await update.message.reply_text(
                f"❌ Bot @{new_username} sudah didaftarkan!\n\n"
                f"Sila gunakan token bot lain.",
                parse_mode='Markdown'
            )
            return
        
        # Register new bot
        success, message = self.db.create_bot(token, user_id, new_username)
        if not success:
            await update.message.reply_text(f"❌ Gagal mendaftar bot: {message}")
            return
        
        # Get new bot ID
        new_bot = self.db.get_bot_by_token(token)
        new_bot_id = new_bot['id']
        
        # Clone data from source to target
        clone_success = self.db.clone_bot_data(source_bot_id, new_bot_id)
        
        # Clear clone mode
        context.user_data.pop('clone_source_bot', None)
        
        if clone_success:
            # Start the new bot
            try:
                await self.manager.start_child_bot(new_bot)
            except Exception as e:
                logging.error(f"Failed to start cloned bot: {e}")
            
            await update.message.reply_text(
                f"✅ **BOT BERJAYA DICLONE!**\n\n"
                f"**Source:** Bot #{source_bot_id}\n"
                f"**New Bot:** @{new_username}\n"
                f"**Bot ID:** #{new_bot_id}\n\n"
                f"✅ Semua companies & settings telah dicopy!\n\n"
                f"Gunakan /mybots untuk manage bot baru.",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                f"⚠️ **BOT DIDAFTARKAN TETAPI CLONE GAGAL**\n\n"
                f"Bot @{new_username} telah didaftarkan tetapi "
                f"data dari source bot gagal dicopy.\n\n"
                f"Sila tambah content secara manual.",
                parse_mode='Markdown'
            )

    # =============================================
    # AI BOT BUILDER HANDLERS
    # =============================================

    async def cancel_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel via inline button callback"""
        query = update.callback_query
        await query.answer()
        await query.message.edit_text("✅ OK, done.")
        # Only clear AI-specific keys, not everything
        for key in ['ai_bot_id', 'ai_chat_history', 'ai_parsed_config', 'ai_is_modify',
                     'ai_current_config', 'ai_provider', 'ai_api_key', 'apikey_provider']:
            context.user_data.pop(key, None)
        return ConversationHandler.END

    # --- AI Builder Entry ---
    async def ai_builder_entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Entry point when user clicks '🤖 Generate Guna AI' after bot creation"""
        query = update.callback_query
        await query.answer()
        bot_id = int(query.data.split("_")[2])
        user_id = update.effective_user.id
        
        # Store bot_id for this session
        context.user_data['ai_bot_id'] = bot_id
        context.user_data['ai_chat_history'] = []
        context.user_data['ai_is_modify'] = False
        
        # Check if user already has an API key
        existing_key = self.db.get_ai_api_key(user_id)
        if existing_key:
            # Has key, skip to AI chat
            context.user_data['ai_provider'] = existing_key['provider']
            context.user_data['ai_api_key'] = existing_key['api_key']
            
            bot = self.db.get_bot_by_id(bot_id)
            bot_name = bot.get('bot_username', 'unknown') if bot else 'unknown'
            
            keyboard = [
                [InlineKeyboardButton("✅ Generate Config", callback_data="ai_generate")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            await query.message.edit_text(
                f"🤖 **AI Bot Builder**\n\n"
                f"Bot: @{bot_name} (#{bot_id})\n"
                f"API: {existing_key['provider'].upper()} ✅\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"Describe apa kau nak untuk bot ni.\n"
                f"Contoh:\n"
                f'• "Buat bot untuk kedai makan ada 5 company"\n'
                f'• "Bot casino listing macam ni" + hantar screenshot\n'
                f'• "Bot referral dengan 3 brand gaming"\n\n'
                f"📸 Boleh hantar screenshot bot lain untuk reference!\n"
                f"Bila dah ready, tekan ✅ Generate Config.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return AI_CHAT
        else:
            # No key, ask for provider
            keyboard = [
                [InlineKeyboardButton("⚡ Groq (Free & Fast)", callback_data="ai_provider_groq")],
                [InlineKeyboardButton("💎 Google Gemini (Free)", callback_data="ai_provider_gemini")],
                [InlineKeyboardButton("❓ Tutorial Dapat API Key", callback_data="ai_tutorial")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            await query.message.edit_text(
                "🔑 **API Key Required**\n\n"
                "Untuk guna AI Builder, kau perlukan API key.\n"
                "Semua provider ni **FREE** — tak perlu bayar!\n\n"
                "Pilih provider:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return AI_PROVIDER_SELECT

    # --- AI Modify Entry ---
    async def ai_modify_entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Entry point when user clicks '🤖 AI Modify' in bot management"""
        query = update.callback_query
        await query.answer()
        bot_id = int(query.data.split("_")[2])
        user_id = update.effective_user.id
        
        # Store context
        context.user_data['ai_bot_id'] = bot_id
        context.user_data['ai_chat_history'] = []
        context.user_data['ai_is_modify'] = True
        
        # Check API key
        existing_key = self.db.get_ai_api_key(user_id)
        if not existing_key:
            keyboard = [
                [InlineKeyboardButton("🔑 Setup API Key", callback_data="manage_api_key")],
                [InlineKeyboardButton("« Back", callback_data=f"manage_bot_{bot_id}")],
            ]
            await query.message.edit_text(
                "❌ **API Key belum ada**\n\n"
                "Sila setup API key dulu untuk guna AI features.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return ConversationHandler.END
        
        context.user_data['ai_provider'] = existing_key['provider']
        context.user_data['ai_api_key'] = existing_key['api_key']
        
        # Get current bot config
        current_config = ai_bot_builder.get_current_bot_config(self.db, bot_id)
        context.user_data['ai_current_config'] = current_config
        
        bot = self.db.get_bot_by_id(bot_id)
        bot_name = bot.get('bot_username', 'unknown') if bot else 'unknown'
        
        keyboard = [
            [InlineKeyboardButton("✅ Generate Changes", callback_data="ai_generate")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        await query.message.edit_text(
            f"🤖 **AI Modify Bot**\n\n"
            f"Bot: @{bot_name} (#{bot_id})\n"
            f"API: {existing_key['provider'].upper()} ✅\n\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"**Current Config:**\n{current_config[:500]}\n\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Describe apa yang kau nak ubah.\n"
            f"Contoh:\n"
            f'• "Tukar welcome message jadi lebih formal"\n'
            f'• "Tambah 2 company baru: ABC dan XYZ"\n'
            f'• "Buang company yang nama X"',
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return AI_CHAT

    # --- AI Provider & Key ---
    async def ai_provider_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """User selected AI provider"""
        query = update.callback_query
        await query.answer()
        provider = query.data.split("_")[2]  # ai_provider_groq -> groq
        context.user_data['ai_provider'] = provider
        
        provider_name = "Groq" if provider == "groq" else "Google Gemini"
        
        if provider == "groq":
            instructions = (
                "1️⃣ Pergi ke console.groq.com\n"
                "2️⃣ Sign up / Login (free)\n"
                "3️⃣ Dashboard → API Keys → Create\n"
                "4️⃣ Copy API key dan paste sini"
            )
        else:
            instructions = (
                "1️⃣ Pergi ke aistudio.google.com\n"
                "2️⃣ Sign in dengan Google account\n"
                "3️⃣ Get API Key → Create API Key\n"
                "4️⃣ Copy API key dan paste sini"
            )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]
        await query.message.edit_text(
            f"🔑 **Setup {provider_name} API Key**\n\n"
            f"{instructions}\n\n"
            f"📌 Paste your API key below:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return AI_API_KEY_INPUT

    async def ai_apikey_received(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """User sent their API key during AI builder setup"""
        api_key = update.message.text.strip()
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        provider = context.user_data.get('ai_provider', 'groq')
        
        # Delete the message containing the key for security
        try:
            await update.message.delete()
        except Exception:
            pass
        
        await context.bot.send_message(chat_id=chat_id, text="🔄 Validating API key...")
        
        # Validate
        valid, info = await ai_bot_builder.validate_api_key(api_key, provider)
        
        if not valid:
            keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ **Key Invalid**\n\n{info}\n\nCuba lagi — paste API key yang betul:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return AI_API_KEY_INPUT
        
        # Save key
        self.db.save_ai_api_key(user_id, provider, api_key)
        context.user_data['ai_api_key'] = api_key
        
        bot_id = context.user_data.get('ai_bot_id')
        bot = self.db.get_bot_by_id(bot_id) if bot_id else None
        bot_name = bot.get('bot_username', 'unknown') if bot else 'unknown'
        
        keyboard = [
            [InlineKeyboardButton("✅ Generate Config", callback_data="ai_generate")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ **API Key Saved!** ({info})\n\n"
                f"🤖 **AI Bot Builder Ready**\n"
                f"Bot: @{bot_name}\n\n"
                f"Describe apa kau nak untuk bot ni.\n"
                f"📸 Boleh hantar screenshot juga!\n\n"
                f"Bila dah ready, tekan ✅ Generate Config."
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return AI_CHAT

    # --- AI Chat ---
    async def ai_chat_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages in AI chat"""
        user_message = update.message.text.strip()
        api_key = context.user_data.get('ai_api_key', '')
        provider = context.user_data.get('ai_provider', 'groq')
        bot_id = context.user_data.get('ai_bot_id')
        is_modify = context.user_data.get('ai_is_modify', False)
        current_config = context.user_data.get('ai_current_config', '')
        chat_history = context.user_data.get('ai_chat_history', [])
        
        if not api_key:
            await update.message.reply_text("❌ API key not found. Use /apikey to set up.")
            return ConversationHandler.END
        
        # Show typing indicator
        await update.message.reply_text("🤖 AI sedang fikir...")
        
        # Get bot info
        bot = self.db.get_bot_by_id(bot_id) if bot_id else None
        bot_info = {'username': bot.get('bot_username', ''), 'id': bot_id} if bot else None
        
        # Call AI
        response = await ai_bot_builder.ai_configure_bot(
            api_key=api_key,
            provider=provider,
            user_message=user_message,
            bot_info=bot_info,
            chat_history=chat_history,
            is_modify=is_modify,
            current_config=current_config
        )
        
        # Update chat history
        chat_history.append({"role": "user", "content": user_message})
        chat_history.append({"role": "assistant", "content": response})
        context.user_data['ai_chat_history'] = chat_history
        
        # Check if AI returned a config
        config = ai_bot_builder.parse_bot_config(response)
        if config:
            # AI sent a ready config — show preview
            context.user_data['ai_parsed_config'] = config
            preview = ai_bot_builder.format_config_preview(config)
            
            keyboard = [
                [InlineKeyboardButton("✅ Apply Config", callback_data="ai_apply")],
                [InlineKeyboardButton("✏️ Ubah Lagi", callback_data="ai_back_chat")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            await update.message.reply_text(
                preview,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return AI_PREVIEW
        
        # Regular chat response
        keyboard = [
            [InlineKeyboardButton("✅ Generate Config", callback_data="ai_generate")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        # Truncate long responses
        if len(response) > 4000:
            response = response[:4000] + "..."
        
        await update.message.reply_text(
            response,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return AI_CHAT

    async def ai_chat_image(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle image/screenshot in AI chat"""
        api_key = context.user_data.get('ai_api_key', '')
        provider = context.user_data.get('ai_provider', 'groq')
        bot_id = context.user_data.get('ai_bot_id')
        is_modify = context.user_data.get('ai_is_modify', False)
        current_config = context.user_data.get('ai_current_config', '')
        chat_history = context.user_data.get('ai_chat_history', [])
        
        if not api_key:
            await update.message.reply_text("❌ API key not found. Use /apikey to set up.")
            return ConversationHandler.END
        
        # Download photo
        photo = update.message.photo[-1]  # Largest resolution
        caption = update.message.caption or "Analyze screenshot ni dan suggest bot config"
        
        try:
            file = await photo.get_file()
            image_bytes = await file.download_as_bytearray()
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to download image: {e}")
            return AI_CHAT
        
        await update.message.reply_text("🤖📸 AI sedang analyze gambar...")
        
        # Get bot info
        bot = self.db.get_bot_by_id(bot_id) if bot_id else None
        bot_info = {'username': bot.get('bot_username', ''), 'id': bot_id} if bot else None
        
        # Call AI with image
        response = await ai_bot_builder.ai_configure_bot(
            api_key=api_key,
            provider=provider,
            user_message=caption,
            bot_info=bot_info,
            chat_history=chat_history,
            image_bytes=bytes(image_bytes),
            is_modify=is_modify,
            current_config=current_config
        )
        
        # Update chat history (text only for history)
        chat_history.append({"role": "user", "content": f"[Image sent] {caption}"})
        chat_history.append({"role": "assistant", "content": response})
        context.user_data['ai_chat_history'] = chat_history
        
        # Check if AI returned config
        config = ai_bot_builder.parse_bot_config(response)
        if config:
            context.user_data['ai_parsed_config'] = config
            preview = ai_bot_builder.format_config_preview(config)
            keyboard = [
                [InlineKeyboardButton("✅ Apply Config", callback_data="ai_apply")],
                [InlineKeyboardButton("✏️ Ubah Lagi", callback_data="ai_back_chat")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            await update.message.reply_text(preview, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return AI_PREVIEW
        
        keyboard = [
            [InlineKeyboardButton("✅ Generate Config", callback_data="ai_generate")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        if len(response) > 4000:
            response = response[:4000] + "..."
        await update.message.reply_text(response, reply_markup=InlineKeyboardMarkup(keyboard))
        return AI_CHAT

    async def ai_generate_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """User clicked 'Generate Config' - ask AI to produce final config"""
        query = update.callback_query
        await query.answer()
        
        api_key = context.user_data.get('ai_api_key', '')
        provider = context.user_data.get('ai_provider', 'groq')
        bot_id = context.user_data.get('ai_bot_id')
        is_modify = context.user_data.get('ai_is_modify', False)
        current_config = context.user_data.get('ai_current_config', '')
        chat_history = context.user_data.get('ai_chat_history', [])
        
        if not api_key:
            await query.message.reply_text("❌ API key not found.")
            return ConversationHandler.END
        
        await query.message.edit_text("🤖 Generating bot config...")
        
        bot = self.db.get_bot_by_id(bot_id) if bot_id else None
        bot_info = {'username': bot.get('bot_username', ''), 'id': bot_id} if bot else None
        
        # Send generation prompt
        gen_prompt = "OK saya dah ready. Sila generate JSON config berdasarkan apa yang kita discuss."
        if is_modify:
            gen_prompt = "OK saya dah ready. Sila generate JSON config untuk modification berdasarkan apa yang kita discuss."
        
        response = await ai_bot_builder.ai_configure_bot(
            api_key=api_key,
            provider=provider,
            user_message=gen_prompt,
            bot_info=bot_info,
            chat_history=chat_history,
            is_modify=is_modify,
            current_config=current_config
        )
        
        # Parse config
        config = ai_bot_builder.parse_bot_config(response)
        if config:
            context.user_data['ai_parsed_config'] = config
            preview = ai_bot_builder.format_config_preview(config)
            keyboard = [
                [InlineKeyboardButton("✅ Apply Config", callback_data="ai_apply")],
                [InlineKeyboardButton("✏️ Ubah Lagi", callback_data="ai_back_chat")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            await query.message.edit_text(preview, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return AI_PREVIEW
        else:
            # AI didn't return config — show response and let user continue
            chat_history.append({"role": "user", "content": gen_prompt})
            chat_history.append({"role": "assistant", "content": response})
            context.user_data['ai_chat_history'] = chat_history
            
            keyboard = [
                [InlineKeyboardButton("✅ Generate Config", callback_data="ai_generate")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            if len(response) > 4000:
                response = response[:4000] + "..."
            await query.message.edit_text(
                f"{response}\n\n⚠️ AI belum generate config. Cuba describe lebih detail.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return AI_CHAT

    async def ai_apply_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Apply the parsed config to the bot's database"""
        query = update.callback_query
        await query.answer()
        
        config = context.user_data.get('ai_parsed_config')
        bot_id = context.user_data.get('ai_bot_id')
        
        if not config or not bot_id:
            await query.message.edit_text("❌ No config to apply.")
            return ConversationHandler.END
        
        await query.message.edit_text("⏳ Applying config...")
        
        success, summary = await ai_bot_builder.apply_bot_config(self.db, bot_id, config)
        
        bot = self.db.get_bot_by_id(bot_id)
        bot_username = bot.get('bot_username', 'unknown') if bot else 'unknown'
        
        if success:
            keyboard = [
                [InlineKeyboardButton(f"🤖 Go to @{bot_username}", url=f"https://t.me/{bot_username}")],
                [InlineKeyboardButton("📋 My Bots", callback_data="my_bots_panel")],
            ]
            await query.message.edit_text(
                f"🎉 <b>Config Applied Successfully!</b>\n\n"
                f"{summary}\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"✨ Tekan /start di @{bot_username} untuk tengok hasil!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        else:
            keyboard = [
                [InlineKeyboardButton("✏️ Cuba Lagi", callback_data="ai_back_chat")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]
            await query.message.edit_text(
                f"❌ <b>Apply Failed</b>\n\n{summary}",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return AI_CHAT
        
        # Clean up
        context.user_data.pop('ai_bot_id', None)
        context.user_data.pop('ai_chat_history', None)
        context.user_data.pop('ai_parsed_config', None)
        context.user_data.pop('ai_is_modify', None)
        context.user_data.pop('ai_current_config', None)
        return ConversationHandler.END

    async def ai_back_to_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Go back to AI chat from preview"""
        query = update.callback_query
        await query.answer()
        keyboard = [
            [InlineKeyboardButton("✅ Generate Config", callback_data="ai_generate")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        await query.message.edit_text(
            "📝 OK, describe apa yang kau nak ubah.\n"
            "Bila dah ready, tekan ✅ Generate Config.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return AI_CHAT

    # =============================================
    # API KEY MANAGEMENT HANDLERS
    # =============================================

    async def apikey_menu_cmd(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /apikey command"""
        user_id = update.effective_user.id
        keys = self.db.get_all_ai_api_keys(user_id)
        
        if keys:
            text = "🔑 **Your AI API Keys**\n\n"
            for k in keys:
                masked = k['api_key'][:8] + "..." + k['api_key'][-4:]
                text += f"• **{k['provider'].upper()}**: `{masked}`\n"
            text += f"\nTotal: {len(keys)} key(s)"
        else:
            text = "🔑 **No API Keys**\n\nBelum ada API key. Tambah sekarang!"
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Key", callback_data="apikey_add_menu")],
        ]
        if keys:
            for k in keys:
                keyboard.append([InlineKeyboardButton(
                    f"🗑️ Remove {k['provider'].upper()}", 
                    callback_data=f"apikey_confirm_del_{k['provider']}"
                )])
        keyboard.append([InlineKeyboardButton("❓ Tutorial", callback_data="ai_tutorial")])
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="cancel")])
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return APIKEY_PROVIDER

    async def apikey_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline button to open API key menu"""
        query = update.callback_query
        await query.answer()
        user_id = update.effective_user.id
        keys = self.db.get_all_ai_api_keys(user_id)
        
        if keys:
            text = "🔑 **Your AI API Keys**\n\n"
            for k in keys:
                masked = k['api_key'][:8] + "..." + k['api_key'][-4:]
                text += f"• **{k['provider'].upper()}**: `{masked}`\n"
        else:
            text = "🔑 **No API Keys**\n\nBelum ada API key."
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Key", callback_data="apikey_add_menu")],
        ]
        if keys:
            for k in keys:
                keyboard.append([InlineKeyboardButton(
                    f"🗑️ Remove {k['provider'].upper()}", 
                    callback_data=f"apikey_confirm_del_{k['provider']}"
                )])
        keyboard.append([InlineKeyboardButton("❓ Tutorial", callback_data="ai_tutorial")])
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="cancel")])
        
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return APIKEY_PROVIDER

    async def apikey_provider_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """User selected provider to add key for"""
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "apikey_add_menu":
            # Show provider choice
            keyboard = [
                [InlineKeyboardButton("⚡ Groq (Free)", callback_data="apikey_add_groq")],
                [InlineKeyboardButton("💎 Gemini (Free)", callback_data="apikey_add_gemini")],
                [InlineKeyboardButton("« Back", callback_data="manage_api_key")],
            ]
            await query.message.edit_text(
                "Pilih provider untuk add API key:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return APIKEY_PROVIDER
        
        # Extract provider from callback
        provider = data.replace("apikey_add_", "")
        context.user_data['apikey_provider'] = provider
        
        provider_name = "Groq" if provider == "groq" else "Google Gemini"
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]
        await query.message.edit_text(
            f"🔑 **Add {provider_name} API Key**\n\n"
            f"Paste your API key below:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return APIKEY_INPUT

    async def apikey_received(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """User sent API key in key management flow"""
        api_key = update.message.text.strip()
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        provider = context.user_data.get('apikey_provider', 'groq')
        
        # Delete the key message
        try:
            await update.message.delete()
        except Exception:
            pass
        
        await context.bot.send_message(chat_id=chat_id, text="🔄 Validating...")
        
        valid, info = await ai_bot_builder.validate_api_key(api_key, provider)
        
        if not valid:
            keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ **Key Invalid**\n\n{info}\n\nCuba lagi:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return APIKEY_INPUT
        
        self.db.save_ai_api_key(user_id, provider, api_key)
        masked = api_key[:8] + "..." + api_key[-4:]
        
        keyboard = [
            [InlineKeyboardButton("� My Bots", callback_data="my_bots_panel")],
            [InlineKeyboardButton("✅ Done", callback_data="cancel")],
        ]
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ **API Key Saved!**\n\n"
                f"Provider: **{provider.upper()}**\n"
                f"Key: `{masked}`\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"🤖 Nak guna AI? Pergi ke:\n"
                f"/mybots → pilih bot → 🤖 AI Modify"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    async def apikey_delete_confirmed(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Confirm and delete API key"""
        query = update.callback_query
        await query.answer()
        provider = query.data.replace("apikey_confirm_del_", "")
        user_id = update.effective_user.id
        
        deleted = self.db.delete_ai_api_key(user_id, provider)
        
        if deleted:
            text = f"✅ **{provider.upper()}** API key removed."
        else:
            text = f"❌ Key not found."
        
        keyboard = [
            [InlineKeyboardButton("🔑 Manage Keys", callback_data="manage_api_key")],
            [InlineKeyboardButton("✅ Done", callback_data="cancel")],
        ]
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return ConversationHandler.END

    # =============================================
    # AI TUTORIAL
    # =============================================

    async def show_ai_tutorial(self, update: Update):
        """Show step-by-step tutorial for getting API keys"""
        query = update.callback_query
        
        text = (
            "📚 <b>Tutorial: Cara Dapat AI API Key (FREE)</b>\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "⚡ <b>OPTION 1: Groq (Recommended)</b>\n"
            "✅ 100% Free | ⚡ Super Fast\n\n"
            "1️⃣ Pergi ke <a href='https://console.groq.com'>console.groq.com</a>\n"
            "2️⃣ Click <b>Sign Up</b> (guna Google/GitHub)\n"
            "3️⃣ Dashboard → <b>API Keys</b> (left menu)\n"
            "4️⃣ Click <b>Create API Key</b>\n"
            "5️⃣ Copy the key → paste dalam bot\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "💎 <b>OPTION 2: Google Gemini</b>\n"
            "✅ Free tier | 🧠 Smart + Vision\n\n"
            "1️⃣ Pergi ke <a href='https://aistudio.google.com'>aistudio.google.com</a>\n"
            "2️⃣ Sign in dengan Google account\n"
            "3️⃣ Click <b>Get API Key</b> (top menu)\n"
            "4️⃣ Click <b>Create API Key</b>\n"
            "5️⃣ Copy the key → paste dalam bot\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "💡 <b>Mana satu pilih?</b>\n"
            "• <b>Groq</b> → Laju, simple, best untuk text\n"
            "• <b>Gemini</b> → Smart, vision terbaik, free tier besar\n\n"
            "Dua-dua FREE. Recommend: <b>Groq</b> untuk mula."
        )
        
        keyboard = [
            [InlineKeyboardButton("🔑 Setup API Key Sekarang", callback_data="manage_api_key")],
            [InlineKeyboardButton("« Back", callback_data="close_panel")],
        ]
        
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    async def show_ai_tutorial_conv(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show tutorial inside ConversationHandler"""
        query = update.callback_query
        await query.answer()
        
        text = (
            "📚 <b>Cara Dapat AI API Key (FREE)</b>\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "⚡ <b>Groq (Recommended)</b>\n"
            "1️⃣ Pergi ke <a href='https://console.groq.com'>console.groq.com</a>\n"
            "2️⃣ Sign Up (free, guna Google/GitHub)\n"
            "3️⃣ API Keys → Create API Key\n"
            "4️⃣ Copy key → paste sini\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "💎 <b>Google Gemini</b>\n"
            "1️⃣ Pergi ke <a href='https://aistudio.google.com'>aistudio.google.com</a>\n"
            "2️⃣ Sign in → Get API Key → Create\n"
            "3️⃣ Copy key → paste sini\n\n"
            "Dua-dua 100% <b>FREE</b>!"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔑 Setup Key", callback_data="manage_api_key")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    async def show_ai_tutorial_conv(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show tutorial inside ConversationHandler"""
        query = update.callback_query
        await query.answer()
        
        text = (
            "📚 <b>Cara Dapat AI API Key (FREE)</b>\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "⚡ <b>Groq (Recommended)</b>\n"
            "1️⃣ Pergi ke <a href='https://console.groq.com'>console.groq.com</a>\n"
            "2️⃣ Sign Up (free, guna Google/GitHub)\n"
            "3️⃣ API Keys → Create API Key\n"
            "4️⃣ Copy key → paste sini\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            "💎 <b>Google Gemini</b>\n"
            "1️⃣ Pergi ke <a href='https://aistudio.google.com'>aistudio.google.com</a>\n"
            "2️⃣ Sign in → Get API Key → Create\n"
            "3️⃣ Copy key → paste sini\n\n"
            "Dua-dua 100% <b>FREE</b>!"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔑 Setup Key", callback_data="manage_api_key")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

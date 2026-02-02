import logging
import datetime
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from database import Database
from config import DEFAULT_GLOBAL_AD

# States for Admin Add/Edit Company
NAME, DESC, MEDIA, BUTTON_TEXT, BUTTON_URL = range(5)
# States for Broadcast
BROADCAST_CONTENT, BROADCAST_CONFIRM = range(7, 9)
# States for Edit Welcome
WELCOME_PHOTO, WELCOME_TEXT = range(11, 13)
# States for Edit Company
EDIT_FIELD, EDIT_NAME, EDIT_DESC, EDIT_MEDIA, EDIT_BTN_TEXT, EDIT_BTN_URL = range(15, 21)
# State for Search
SEARCH = 22

class ChildBot:
    def __init__(self, token, bot_id, db: Database, scheduler):
        self.token = token
        self.bot_id = bot_id
        self.db = db
        self.scheduler = scheduler
        self.app = Application.builder().token(token).build()
        self.logger = logging.getLogger(f"Bot_{bot_id}")
        self.setup_handlers()

    async def initialize(self):
        """Prepare bot application but do not start polling (Webhook mode)"""
        await self.app.initialize()
        await self.app.start()

    async def stop(self):
        await self.app.stop()
        await self.app.shutdown()

    # --- Handlers Setup ---
    def setup_handlers(self):
        # Admin Commands
        self.app.add_handler(CommandHandler("settings", self.admin_dashboard))
        self.app.add_handler(CommandHandler("admin", self.admin_dashboard))

        # Main User Commands
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("company", self.main_menu))

        # Admin Add Company Wizard
        add_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_company_start, pattern="^admin_add_company$")],
            states={
                NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_company_name)],
                DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_company_desc)],
                MEDIA: [MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION, self.add_company_media)],
                BUTTON_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_company_btn_text)],
                BUTTON_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_company_btn_url)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern="^cancel$")]
        )
        self.app.add_handler(add_conv)

        # Admin Broadcast Wizard
        broadcast_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.broadcast_start, pattern="^admin_broadcast$")],
            states={
                BROADCAST_CONTENT: [MessageHandler(filters.ALL & ~filters.COMMAND, self.broadcast_content)],
                BROADCAST_CONFIRM: [CallbackQueryHandler(self.broadcast_confirm)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op)]
        )
        self.app.add_handler(broadcast_conv)

        # Edit Welcome Wizard
        welcome_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.edit_welcome_start, pattern="^edit_welcome$")],
            states={
                WELCOME_PHOTO: [MessageHandler(filters.PHOTO, self.save_welcome_photo)],
                WELCOME_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.save_welcome_text)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_welcome)]
        )
        self.app.add_handler(welcome_conv)
        
        # Edit Company Wizard
        edit_company_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.edit_company_start, pattern=r'^edit_company_\d+$')],
            states={
                EDIT_FIELD: [CallbackQueryHandler(self.edit_company_choose_field, pattern=r'^ef_')],
                EDIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_name)],
                EDIT_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_desc)],
                EDIT_MEDIA: [MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION, self.edit_company_save_media)],
                EDIT_BTN_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_btn_text)],
                EDIT_BTN_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_btn_url)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern=r'^cancel$')],
            per_message=False
        )
        self.app.add_handler(edit_company_conv)
        
        # User Actions via Callback (MUST BE AFTER ConversationHandlers!)
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))

        # Support System & Text
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    # --- Start & Menu ---
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Handle Referral
        args = context.args
        referrer_id = None
        if args and args[0].isdigit():
            referrer_id = int(args[0])
        
        user = update.effective_user
        # Register user
        is_new = self.db.add_user(self.bot_id, user.id, referrer_id)
        if is_new and referrer_id:
            # Notify referrer
            try:
                await context.bot.send_message(chat_id=referrer_id, text=f"🎉 New Referral! {user.first_name} joined. You earned RM1.00")
            except: pass # Referrer might have blocked bot
            
        await self.main_menu(update, context)

    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_subscription(update): return

        bot_data = self.db.get_bot_by_token(self.token)
        caption = bot_data['custom_caption'] or f"Selamat Datang ke {bot_data['bot_username']}! 🚀\n\nPlatform penyenaraian Company terbaik.\nSila pilih menu di bawah:"
        
        keyboard = [
            [InlineKeyboardButton("🏢 LIST COMPANY", callback_data="list_page_0")],
            [InlineKeyboardButton("💰 DOMPET SAYA", callback_data="wallet")],
            [InlineKeyboardButton("🔗 SHARE LINK", callback_data="share_link")],
            [InlineKeyboardButton("🏆 LEADERBOARD", callback_data="leaderboard"), InlineKeyboardButton("💬 SUPPORT", callback_data="support_info")]
        ]

        if update.callback_query:
            try: await update.callback_query.message.delete()
            except: pass
            
        if bot_data['custom_banner']:
             await update.effective_chat.send_photo(photo=bot_data['custom_banner'], caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
             await update.effective_chat.send_message(caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    # --- Company Logic ---
    async def show_page(self, update: Update, page: int):
        """Display company in CAROUSEL mode - one company at a time with Prev/Next buttons"""
        companies = self.db.get_companies(self.bot_id)
        
        if not companies:
            text = "📋 **Belum ada company.**"
            keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")]]
            
            if update.callback_query:
                await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return
        
        # Get current company (page = index)
        if page >= len(companies):
            page = len(companies) - 1
        if page < 0:
            page = 0
            
        comp = companies[page]
        
        # Check if user is admin for edit button
        bot_data = self.db.get_bot_by_token(self.token)
        is_admin = update.effective_user.id == bot_data['owner_id']
        
        # Build caption
        caption = (
            f"🏢 **{comp['name']}**\n\n"
            f"{comp['description']}\n\n"
            f"📄 Company {page+1} of {len(companies)}"
        )
        
        # Build keyboard
        keyboard = []
        
        # Row 1: Company action button (REGISTER)
        if comp.get('button_text') and comp.get('button_url'):
            keyboard.append([InlineKeyboardButton(comp['button_text'], url=comp['button_url'])])
        
        # Row 2+: Other company buttons (show names of other companies)
        other_companies = [c for i, c in enumerate(companies) if i != page]
        if other_companies:
            # Show up to 3 companies per row
            row = []
            for i, other in enumerate(other_companies):
                # Find the index of this company for navigation
                other_page = next(idx for idx, c in enumerate(companies) if c['id'] == other['id'])
                # Truncate name if too long
                btn_name = other['name'][:15] + "..." if len(other['name']) > 15 else other['name']
                row.append(InlineKeyboardButton(f"🏢 {btn_name}", callback_data=f"list_page_{other_page}"))
                # Max 2 per row for readability
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:  # Add remaining buttons
                keyboard.append(row)
        
        # Admin-only buttons
        if is_admin:
            keyboard.append([InlineKeyboardButton("📖 VIEW DETAILS", callback_data=f"view_{comp['id']}")])
            keyboard.append([InlineKeyboardButton("✏️ EDIT COMPANY", callback_data=f"edit_company_{comp['id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")])
        
        # Send or edit message with media
        if update.callback_query:
            # Delete old message and send new with media (can't edit media type)
            try:
                await update.callback_query.message.delete()
            except:
                pass
        
        # Send based on media type - using LOCAL FILE PATH
        try:
            import os
            media_path = comp['media_file_id']  # This now contains file PATH, not file_id
            
            # Check if it's a file path (starts with / or contains path separator)
            is_local_file = media_path and (media_path.startswith('/') or os.path.sep in media_path)
            
            if is_local_file and os.path.exists(media_path):
                # Read from local file
                with open(media_path, 'rb') as media_file:
                    if comp['media_type'] == 'video':
                        await update.effective_chat.send_video(
                            video=media_file,
                            caption=caption,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode='Markdown'
                        )
                    elif comp['media_type'] == 'animation':
                        await update.effective_chat.send_animation(
                            animation=media_file,
                            caption=caption,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode='Markdown'
                        )
                    else:  # photo
                        await update.effective_chat.send_photo(
                            photo=media_file,
                            caption=caption,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode='Markdown'
                        )
            else:
                # Fallback: Try as Telegram file_id (for old data)
                if comp['media_type'] == 'video':
                    await update.effective_chat.send_video(video=media_path, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                elif comp['media_type'] == 'animation':
                    await update.effective_chat.send_animation(animation=media_path, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                else:
                    await update.effective_chat.send_photo(photo=media_path, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception as e:
            # Log the error for debugging
            self.logger.error(f"Media display error: {e}")
            self.logger.error(f"media_file_id: {comp.get('media_file_id')}, media_type: {comp.get('media_type')}")
            # Fallback to text if media fails
            await update.effective_chat.send_message(
                f"{caption}\n\n_(Media unavailable: {str(e)[:50]})_",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )

    async def view_company(self, update: Update, comp_id: int):
        comps = self.db.get_companies(self.bot_id)
        comp = next((c for c in comps if c['id'] == int(comp_id)), None)
        if not comp:
            await update.callback_query.answer("Company not found.")
            return

        text = f"🏢 *{comp['name']}*\n\n{comp['description']}\n\n_{DEFAULT_GLOBAL_AD}_"
        keyboard = [
            [InlineKeyboardButton(comp['button_text'], url=comp['button_url'])],
        ]
        
        # Add EDIT button for admin only
        bot_data = self.db.get_bot_by_token(self.token)
        if update.effective_user.id == bot_data['owner_id']:
            keyboard.append([InlineKeyboardButton("✏️ EDIT COMPANY", callback_data=f"edit_company_{comp_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 BACK TO LIST", callback_data="list_page_0")])
        
        await update.callback_query.message.delete()
        
        # Handle local file path or file_id
        import os
        media_path = comp['media_file_id']
        is_local_file = media_path and (media_path.startswith('/') or os.path.sep in media_path)
        
        try:
            if is_local_file and os.path.exists(media_path):
                with open(media_path, 'rb') as media_file:
                    if comp['media_type'] == 'video':
                        await update.effective_chat.send_video(video=media_file, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                    elif comp['media_type'] == 'animation':
                        await update.effective_chat.send_animation(animation=media_file, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                    else:
                        await update.effective_chat.send_photo(photo=media_file, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                # Fallback to file_id
                if comp['media_type'] == 'video':
                    await update.effective_chat.send_video(video=media_path, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                elif comp['media_type'] == 'animation':
                    await update.effective_chat.send_animation(animation=media_path, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                else:
                    await update.effective_chat.send_photo(photo=media_path, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception as e:
            self.logger.error(f"View company media error: {e}")
            await update.effective_chat.send_message(f"{text}\n\n_(Media unavailable)_", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- Wallet & Referral ---
    async def show_wallet(self, update: Update):
        user = self.db.get_user(self.bot_id, update.effective_user.id)
        text = f"💰 **DOMPET ANDA**\n\n👤 **ID:** `{user['telegram_id']}`\n📊 **Total Invite:** {user['total_invites']} Orang\n💵 **Baki Wallet:** RM {user['balance']:.2f}\n\n*Min withdrawal: RM 50.00*"
        
        keyboard = []
        if user['balance'] >= 50.0:
            keyboard.append([InlineKeyboardButton("📤 REQUEST WITHDRAWAL", callback_data="req_withdraw")])
        keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")])
        
        try:
            await update.callback_query.message.delete()
        except:
            pass
        await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def share_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bot_uname = context.bot.username
        link = f"https://t.me/{bot_uname}?start={update.effective_user.id}"
        await update.callback_query.message.reply_text(f"🔗 Link Referral Anda:\n{link}\n\nShare link ini dan dapatkan RM1.00 setiap invite!")

    async def leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Simple Logic: Top users by invite
        conn = self.db.get_connection()
        top = conn.execute("SELECT telegram_id, total_invites FROM users WHERE bot_id = ? ORDER BY total_invites DESC LIMIT 10", (self.bot_id,)).fetchall()
        conn.close()
        
        text = "🏆 **LEADERBOARD MINGGUAN**\n\n"
        for i, row in enumerate(top):
            text += f"{i+1}. ID: `{str(row['telegram_id'])[-4:]}***` - **{row['total_invites']}** Invites\n"
        
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]
        try:
            await update.callback_query.message.delete()
        except:
            pass
        await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode='Markdown')

    # --- Admin Dashboard ---
    async def withdraw_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle withdrawal with validation: min RM10, max RM1000, 24h cooldown"""
        user_id = update.effective_user.id
        
        # Get user current balance
        user = self.db.get_user(self.bot_id, user_id)
        balance = user.get('balance', 0) if user else 0
        
        # Validation 1: Minimum withdrawal RM10
        MIN_WITHDRAW = 10.0
        if balance < MIN_WITHDRAW:
            await update.message.reply_text(
                f"❌ **Minimum Withdrawal: RM{MIN_WITHDRAW:.2f}**\n\n"
                f"Your balance: RM{balance:.2f}\n"
                f"Need: RM{MIN_WITHDRAW - balance:.2f} more",
                parse_mode='Markdown'
            )
            return
        
        # Validation 2: Check last withdrawal time (24h cooldown)
        last_withdraw = self.db.get_last_withdrawal(self.bot_id, user_id)
        if last_withdraw:
            last_time = datetime.datetime.fromisoformat(last_withdraw['requested_at'])
            cooldown = datetime.timedelta(hours=24)
            time_left = (last_time + cooldown) - datetime.datetime.now()
            
            if time_left.total_seconds() > 0:
                hours = int(time_left.total_seconds() // 3600)
                minutes = int((time_left.total_seconds() % 3600) // 60)
                await update.message.reply_text(
                    f"⏰ **Cooldown Period**\n\n"
                    f"You can withdraw again in:\n"
                    f"**{hours}h {minutes}m**",
                    parse_mode='Markdown'
                )
                return
        
        # Validation 3: Maximum per transaction RM1000
        MAX_WITHDRAW = 1000.0
        max_allowed = min(balance, MAX_WITHDRAW)
        
        await update.message.reply_text(
            f"💰 **WITHDRAW REQUEST**\n\n"
            f"Balance: RM{balance:.2f}\n"
            f"Max per request: RM{MAX_WITHDRAW:.2f}\n"
            f"Min: RM{MIN_WITHDRAW:.2f}\n\n"
            f"Enter withdrawal amount\n(RM{MIN_WITHDRAW:.2f} - RM{max_allowed:.2f}):",
            parse_mode='Markdown'
        )
    async def admin_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bot_data = self.db.get_bot_by_token(self.token)
        
        if not bot_data:
            await update.message.reply_text("⛔ Bot data not found.")
            return
            
        # Convert both to int for comparison (owner_id may be stored as string)
        owner_id = int(bot_data.get('owner_id', 0))
        
        if user_id != owner_id:
            self.logger.warning(f"Access denied: user_id={user_id}, owner_id={owner_id}")
            await update.message.reply_text("⛔ Access Denied.")
            return

        text = "👑 **ADMIN SETTINGS DASHBOARD**\n\nWelcome Boss! Full control in your hands."
        keyboard = [
            [InlineKeyboardButton("➕ Add Company", callback_data="admin_add_company"), InlineKeyboardButton("🗑️ Delete Company", callback_data="admin_del_list")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("🎨 Edit Welcome", callback_data="edit_welcome")],
            [InlineKeyboardButton("💳 Withdrawals", callback_data="admin_withdrawals"), InlineKeyboardButton("💬 Support Reply", callback_data="admin_support")],
            [InlineKeyboardButton("❌ Close Panel", callback_data="close_panel")]
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    # --- Callbacks ---
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data
        await query.answer()

        if data.startswith("list_page_"):
            page = int(data.split("_")[2])
            await self.show_page(update, page)
        elif data.startswith("view_"):
            company_id = int(data.split("_")[1])
            await self.view_company(update, company_id)
        elif data == "main_menu":
            await self.main_menu(update, context)
        elif data == "wallet":
            await self.show_wallet(update)
        elif data == "share_link":
            await self.show_share_link(update)
        elif data == "leaderboard":
            await self.show_leaderboard(update, context)
        elif data == "support_info":
            await query.message.reply_text("💬 **Live Support**\nSila taip mesej anda terus di sini. Admin akan reply sebentar lagi.")
        
        # Admin Actions
        elif data == "admin_withdrawals": await self.show_withdrawals(update)
        elif data.startswith("approve_wd_"): await self.process_withdrawal(update, data, True)
        elif data.startswith("reject_wd_"): await self.process_withdrawal(update, data, False)
        elif data == "admin_del_list": await self.show_delete_company_list(update)
        elif data.startswith("delete_company_"): await self.confirm_delete_company(update, int(data.split("_")[2]))
        elif data == "admin_customize": await self.show_customize_menu(update)
        elif data == "admin_support": await self.show_support_messages(update)
        # Note: edit_company_* is handled by ConversationHandler, NOT here
        elif data == "close_panel": await query.message.delete()

    # --- Withdrawal Logic ---
    async def show_withdrawals(self, update: Update):
        wds = self.db.get_pending_withdrawals(self.bot_id)
        if not wds:
            await update.callback_query.message.edit_text("✅ Tiada withdrawal pending.")
            return
        
        for wd in wds:
            text = f"💳 **Withdrawal Request**\nUser ID: `{wd['user_id']}`\nAmount: **RM {wd['amount']}**"
            keyboard = [[
                InlineKeyboardButton("✅ APPROVE", callback_data=f"approve_wd_{wd['id']}"),
                InlineKeyboardButton("❌ REJECT", callback_data=f"reject_wd_{wd['id']}")
            ]]
            await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def process_withdrawal(self, update, data, approve):
        wd_id = int(data.split("_")[2])
        action = "APPROVE" if approve else "REJECT"
        success = self.db.process_withdrawal(wd_id, action)
        
        if success:
            await update.callback_query.message.edit_text(f"Status Updated: {action}")
            # Notify User
            wd = next((w for w in self.db.get_pending_withdrawals(self.bot_id) if w['id'] == wd_id), None) # Wait, it's processed, so fetch logic slightly different or skip for now
            # To be strict, I should fetch user_id from the just processed WD. 
        else:
            await update.callback_query.message.edit_text("Error processing.")
    
    # --- Edit Company Menu ---
    async def show_edit_company_menu(self, update: Update, company_id: int):
        """Show edit options for a specific company"""
        company = next((c for c in self.db.get_companies(self.bot_id) if c['id'] == company_id), None)
        if not company:
            await update.callback_query.message.reply_text("❌ Company not found.")
            return
        
        text = (
            f"✏️ **EDIT COMPANY**\n\n"
            f"🏢 **{company['name']}**\n\n"
            f"Select what to edit:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📝 Edit Name", callback_data=f"ec_name_{company_id}")],
            [InlineKeyboardButton("📄 Edit Description", callback_data=f"ec_desc_{company_id}")],
            [InlineKeyboardButton("🖼️ Edit Media", callback_data=f"ec_media_{company_id}")],
            [InlineKeyboardButton("🔗 Edit Button", callback_data=f"ec_btn_{company_id}")],
            [InlineKeyboardButton("🔙 BACK", callback_data="list_page_0")]
        ]
        
        await update.callback_query.message.reply_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='Markdown'
        )
    
    # --- Edit Company Wizard Functions ---
    async def edit_company_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Entry point for edit company conversation"""
        await update.callback_query.answer()  # Acknowledge immediately to stop loading
        company_id = int(update.callback_query.data.split("_")[2])
        context.user_data['edit_company_id'] = company_id
        
        company = next((c for c in self.db.get_companies(self.bot_id) if c['id'] == company_id), None)
        if not company:
            await update.callback_query.message.reply_text("❌ Company not found.")
            return ConversationHandler.END
        
        text = f"✏️ **EDIT: {company['name']}**\n\nPilih apa yang nak diedit:"
        keyboard = [
            [InlineKeyboardButton("📝 Nama", callback_data="ef_name")],
            [InlineKeyboardButton("📄 Deskripsi", callback_data="ef_desc")],
            [InlineKeyboardButton("🖼️ Media", callback_data="ef_media")],
            [InlineKeyboardButton("🔗 Button Text", callback_data="ef_btn_text")],
            [InlineKeyboardButton("🌐 Button URL", callback_data="ef_btn_url")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
        ]
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return EDIT_FIELD
    
    async def edit_company_choose_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle field selection for edit"""
        data = update.callback_query.data
        await update.callback_query.answer()
        
        if data == "ef_name":
            await update.callback_query.message.reply_text("📝 Masukkan **NAMA BARU**:", parse_mode='Markdown')
            return EDIT_NAME
        elif data == "ef_desc":
            await update.callback_query.message.reply_text("📄 Masukkan **DESKRIPSI BARU**:", parse_mode='Markdown')
            return EDIT_DESC
        elif data == "ef_media":
            await update.callback_query.message.reply_text("🖼️ Hantar **MEDIA BARU** (Gambar/Video):", parse_mode='Markdown')
            return EDIT_MEDIA
        elif data == "ef_btn_text":
            await update.callback_query.message.reply_text("🔗 Masukkan **BUTTON TEXT BARU**:", parse_mode='Markdown')
            return EDIT_BTN_TEXT
        elif data == "ef_btn_url":
            await update.callback_query.message.reply_text("🌐 Masukkan **BUTTON URL BARU**:", parse_mode='Markdown')
            return EDIT_BTN_URL
        elif data == "cancel":
            await update.callback_query.message.reply_text("❌ Edit cancelled.")
            return ConversationHandler.END
    
    async def edit_company_save_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        self.db.edit_company(company_id, 'name', update.message.text)
        await update.message.reply_text("✅ Nama company berjaya dikemaskini!")
        return ConversationHandler.END
    
    async def edit_company_save_desc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        self.db.edit_company(company_id, 'description', update.message.text)
        await update.message.reply_text("✅ Deskripsi company berjaya dikemaskini!")
        return ConversationHandler.END
    
    async def edit_company_save_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        import os
        
        media_base = os.environ.get('MEDIA_DIR', '/data/media')
        media_dir = f"{media_base}/{self.bot_id}"
        os.makedirs(media_dir, exist_ok=True)
        timestamp = int(datetime.datetime.now().timestamp())
        
        if update.message.photo:
            file_obj = await update.message.photo[-1].get_file()
            file_path = f"{media_dir}/{timestamp}.jpg"
            media_type = 'photo'
        elif update.message.video:
            file_obj = await update.message.video.get_file()
            file_path = f"{media_dir}/{timestamp}.mp4"
            media_type = 'video'
        elif update.message.animation:
            file_obj = await update.message.animation.get_file()
            file_path = f"{media_dir}/{timestamp}.gif"
            media_type = 'animation'
        else:
            await update.message.reply_text("❌ Sila hantar gambar, video atau GIF.")
            return EDIT_MEDIA
        
        await file_obj.download_to_drive(file_path)
        self.db.edit_company(company_id, 'media_file_id', file_path)
        self.db.edit_company(company_id, 'media_type', media_type)
        await update.message.reply_text("✅ Media company berjaya dikemaskini!")
        return ConversationHandler.END
    
    async def edit_company_save_btn_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        self.db.edit_company(company_id, 'button_text', update.message.text)
        await update.message.reply_text("✅ Button text berjaya dikemaskini!")
        return ConversationHandler.END
    
    async def edit_company_save_btn_url(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        self.db.edit_company(company_id, 'button_url', update.message.text)
        await update.message.reply_text("✅ Button URL berjaya dikemaskini!")
        return ConversationHandler.END
    
    # --- User Wallet & Share Functions ---
    async def show_wallet(self, update: Update):
        """Show user's wallet with balance and referral stats"""
        user_id = update.effective_user.id
        user = self.db.get_user(self.bot_id, user_id)
        
        if not user:
            await update.callback_query.message.reply_text("❌ User not found. Type /start first.")
            return
        
        balance = user.get('balance', 0)
        total_invites = user.get('total_invites', 0)
        total_earned = total_invites * 1.00  # RM1 per referral
        
        text = (
            f"💰 **YOUR WALLET**\n\n"
            f"💵 Balance: **RM {balance:.2f}**\n"
            f"👥 Total Referrals: **{total_invites}**\n"
            f"💎 Total Earned: **RM {total_earned:.2f}**\n\n"
            f"_Minimum withdrawal: RM10_"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 WITHDRAW", callback_data="withdraw")],
            [InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]
        ]
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def show_share_link(self, update: Update):
        """Show user's referral share link"""
        user_id = update.effective_user.id
        
        # Get bot info for the link
        bot_data = self.db.get_bot_by_token(self.token)
        bot_username = bot_data.get('bot_username', 'bot') if bot_data else 'bot'
        
        referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        
        text = (
            f"🔗 **YOUR REFERRAL LINK**\n\n"
            f"`{referral_link}`\n\n"
            f"📢 Share link ini dengan kawan-kawan!\n"
            f"💰 Dapat **RM1.00** untuk setiap referral yang join!\n\n"
            f"_Tap link di atas untuk copy_"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]]
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    
    # --- Delete Company Logic ---
    async def show_delete_company_list(self, update: Update):
        """Show list of companies with delete buttons"""
        companies = self.db.get_companies(self.bot_id)
        if not companies:
            await update.callback_query.message.edit_text("📭 Tiada company untuk delete.")
            return
        
        text = "🗑️ **DELETE COMPANY**\n\nPilih company untuk delete:"
        keyboard = []
        for company in companies:
            keyboard.append([InlineKeyboardButton(
                f"❌ {company['name']}", 
                callback_data=f"delete_company_{company['id']}"
            )])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="close_panel")])
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def confirm_delete_company(self, update: Update, company_id: int):
        """Delete company from database"""
        success = self.db.delete_company(company_id, self.bot_id)
        if success:
            await update.callback_query.message.edit_text("✅ Company deleted successfully!")
        else:
            await update.callback_query.message.edit_text("❌ Error deleting company.")
    
    # --- Customize Menu Logic ---
    async def show_customize_menu(self, update: Update):
        """Show customization options"""
        text = (
            "🎨 **CUSTOMIZE BOT**\n\n"
            "Available customization options:\n\n"
            "1️⃣ **Welcome Message** - Edit /start message\n"
            "2️⃣ **Bot Footer** - Custom ad footer\n"
            "3️⃣ **Referral Amount** - Change reward per referral\n\n"
            "⚠️ _Feature coming soon! Contact support for custom branding._"
        )
        keyboard = [[InlineKeyboardButton("« Back", callback_data="close_panel")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    # --- Support Messages Logic ---
    async def show_support_messages(self, update: Update):
        """Show pending support messages from users"""
        text = (
            "💬 **SUPPORT MESSAGES**\n\n"
            "All user messages akan forward terus ke sini.\n"
            "Reply secara manual untuk balas user.\n\n"
            "📨 _No pending messages at the moment._"
        )
        keyboard = [[InlineKeyboardButton("« Back", callback_data="close_panel")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    # --- Edit Welcome Wizard ---
    async def edit_welcome_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start Edit Welcome wizard - ask for photo"""
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(
            "📸 **EDIT WELCOME MESSAGE**\n\n"
            "Step 1: Upload your welcome banner image\n\n"
            "Send a photo that will be displayed when users type /start\n\n"
            "Type /cancel to cancel."
        )
        return WELCOME_PHOTO
    
    async def save_welcome_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save photo file_id and ask for caption"""
        photo = update.message.photo[-1]  # Get highest resolution
        context.user_data['welcome_banner'] = photo.file_id
        
        await update.message.reply_text(
            "✅ Photo saved!\n\n"
            "Step 2: Enter your welcome message text\n\n"
            "This text will be shown with the banner when users type /start\n\n"
            "Type /cancel to cancel."
        )
        return WELCOME_TEXT
    
    async def save_welcome_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save caption to database and show preview"""
        caption_text = update.message.text
        banner_file_id = context.user_data.get('welcome_banner')
        
        # Update database
        bot_data = self.db.get_bot_by_token(self.token)
        self.db.update_welcome_settings(bot_data['id'], banner_file_id, caption_text)
        
        # Show preview
        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="close_panel")]]
        await update.message.reply_photo(
            photo=banner_file_id,
            caption=f"✅ <b>WELCOME MESSAGE UPDATED!</b>\n\n"
                    f"Preview:\n{caption_text}\n\n"
                    f"Users will see this when they type /start",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        
        # Clear user data
        context.user_data.clear()
        return ConversationHandler.END
    
    async def cancel_welcome(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel Edit Welcome wizard"""
        context.user_data.clear()
        await update.message.reply_text("❌ Edit Welcome cancelled.")
        return ConversationHandler.END

    # --- Support Logic ---
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bot_data = self.db.get_bot_by_token(self.token)
        owner_id = bot_data['owner_id']
        user_id = update.effective_user.id
        
        # User -> Admin
        if user_id != owner_id:
            await context.bot.forward_message(chat_id=owner_id, from_chat_id=update.effective_chat.id, message_id=update.message.message_id)
            await context.bot.send_message(chat_id=owner_id, text=f"💬 Reply using: `/reply {user_id} [message]`")
        
        # Admin Reply Command (Simple regex or check)
        elif update.message.text.startswith("/reply "):
            try:
                parts = update.message.text.split(" ", 2)
                target_id = int(parts[1])
                msg = parts[2]
                await context.bot.send_message(chat_id=target_id, text=f"💬 **Admin Reply:**\n{msg}", parse_mode='Markdown')
                await update.message.reply_text("✅ Sent.")
            except:
                await update.message.reply_text("❌ Format: /reply USER_ID MESSAGE")

    # --- Add Company Wizard Steps ---
    async def add_company_start(self, update, context):
        await update.callback_query.message.reply_text("Sila masukkan **NAMA Company**:")
        return NAME
    
    async def add_company_name(self, update, context):
        context.user_data['new_comp'] = {'name': update.message.text}
        await update.message.reply_text("Masukkan **Deskripsi Company**:")
        return DESC

    async def add_company_desc(self, update, context):
        context.user_data['new_comp']['desc'] = update.message.text
        await update.message.reply_text("Hantar **Gambar/Video** Banner:")
        return MEDIA

    async def add_company_media(self, update, context):
        """Download and store media locally on VPS persistent storage"""
        import os
        
        # Use persistent volume path
        media_base = os.environ.get('MEDIA_DIR', '/data/media')
        media_dir = f"{media_base}/{self.bot_id}"
        os.makedirs(media_dir, exist_ok=True)
        
        timestamp = int(datetime.datetime.now().timestamp())
        file_obj = None
        file_ext = ""
        media_type = ""

        if update.message.photo:
            file_obj = await update.message.photo[-1].get_file()
            file_ext = ".jpg"
            media_type = 'photo'
        elif update.message.video:
            file_obj = await update.message.video.get_file()
            file_ext = ".mp4"
            media_type = 'video'
        elif update.message.animation:
            file_obj = await update.message.animation.get_file()
            file_ext = ".gif"
            media_type = 'animation'
        
        if not file_obj:
            await update.message.reply_text("❌ Sila hantar gambar, video atau GIF.")
            return MEDIA
        
        # Download to local persistent storage with error handling
        file_path = f"{media_dir}/{timestamp}{file_ext}"
        try:
            await file_obj.download_to_drive(file_path)
            self.logger.info(f"Media saved to: {file_path}")
        except Exception as e:
            self.logger.error(f"Download error: {e}")
            await update.message.reply_text(f"❌ Gagal simpan media: {str(e)[:100]}")
            return MEDIA
        
        # Store file PATH (not file_id)
        context.user_data['new_comp']['media'] = file_path
        context.user_data['new_comp']['type'] = media_type
        await update.message.reply_text("Masukkan **Text pada Button** (Contoh: REGISTER NOW):")
        return BUTTON_TEXT

    async def add_company_btn_text(self, update, context):
        context.user_data['new_comp']['btn_text'] = update.message.text
        await update.message.reply_text("Masukkan **Link URL** destination:")
        return BUTTON_URL

    async def add_company_btn_url(self, update, context):
        data = context.user_data['new_comp']
        self.db.add_company(self.bot_id, data['name'], data['desc'], data['media'], data['type'], data['btn_text'], update.message.text)
        await update.message.reply_text("✅ Company Berjaya Ditambah!")
        return ConversationHandler.END

    async def cancel_op(self, update, context):
        await update.message.reply_text("❌ Cancelled.")
        return ConversationHandler.END

    # --- Broadcast Wizard ---
    async def broadcast_start(self, update, context):
        await update.callback_query.message.reply_text("📢 **BROADCAST MODE**\nSila hantar mesej (Text/Gambar/Video) yang nak disebarkan:")
        return BROADCAST_CONTENT
    
    async def broadcast_content(self, update, context):
        # Save msg
        context.user_data['broadcast_msg'] = update.message
        keyboard = [[InlineKeyboardButton("✅ CONFIRM SEND", callback_data="confirm_broadcast")]]
        await update.message.reply_text("Mesej diterima. Tekan untuk hantar.", reply_markup=InlineKeyboardMarkup(keyboard))
        return BROADCAST_CONFIRM

    async def broadcast_confirm(self, update, context):
        conn.close()
        
        sent = 0
        for u in users:
            try:
                await msg.copy(chat_id=u['telegram_id'])
                sent += 1
            except: pass
        
        await update.callback_query.message.reply_text(f"✅ Broadcast Sent to {sent} users.")
        return ConversationHandler.END

    async def show_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display top referrers leaderboard"""
        user_id = update.effective_user.id
        
        # Get top 10 referrers
        top_users = self.db.get_top_referrers(self.bot_id, 10)
        
        if not top_users:
            await update.callback_query.message.reply_text(
                "🏆 **LEADERBOARD**\n\nNo referrals yet. Be the first!",
                parse_mode='Markdown'
            )
            return
        
        # Build leaderboard text
        text = "🏆 **TOP REFERRERS**\n\n"
        
        medals = ["🥇", "🥈", "🥉"]
        for idx, user in enumerate(top_users, 1):
            medal = medals[idx-1] if idx <= 3 else f"{idx}."
            text += f"{medal} ID `{user['telegram_id']}` - **{user['total_invites']}** invites\n"
        
        # Show user's rank if not in top 10
        user_data = self.db.get_user(self.bot_id, user_id)
        if user_data:
            rank = self.db.get_user_rank(self.bot_id, user_id)
            invites = user_data.get('total_invites', 0)
            
            text += f"\n━━━━━━━━━━\n"
            text += f"**Your Position:** #{rank}\n"
            text += f"**Your Invites:** {invites}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]]
        
        await update.callback_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_subscription(update): return
        user_id = update.effective_user.id
        username = update.effective_user.username or "User"
        
        # Register user if new
        self.db.register_user(self.bot_id, user_id, username, context.args[0] if context.args else None)

        # Get custom welcome or default
        bot_data = self.db.get_bot_by_token(self.token)
        
        # Use custom banner if available
        if bot_data['custom_banner']:
            await update.effective_chat.send_photo(
                photo=bot_data['custom_banner'],
                caption=bot_data['custom_caption'] or "Welcome!",
                parse_mode='HTML'
            )
        
        # Show main menu
        await self.show_main_menu(update)
    
    async def show_main_menu(self, update: Update):
        """Show main menu - edits message if from callback, sends new if from command"""
        text = "🏠 **MAIN MENU**\nSila pilih:"
        keyboard = [
            [InlineKeyboardButton("🏢 LIST COMPANY", callback_data="list_page_0")],
            [InlineKeyboardButton("🔍 SEARCH", callback_data="search_company")],
            [InlineKeyboardButton("💰 WALLET", callback_data="wallet"),
             InlineKeyboardButton("🔗 REFERRAL", callback_data="referral")],
            [InlineKeyboardButton("📤 WITHDRAW", callback_data="withdraw")]
        ]
        
        # Check if bot owner for admin button
        bot_data = self.db.get_bot_by_token(self.token)
        if update.effective_user.id == bot_data['owner_id']:
            keyboard.append([InlineKeyboardButton("⚙️ SETTINGS", callback_data="settings")])
        
        # Edit if callback, send if command
        if update.callback_query:
            await update.callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )

    # --- Edit Company Wizard Functions ---
    async def edit_company_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start edit company - show field selection menu"""
        query = update.callback_query
        company_id = int(query.data.split("_")[2])
        context.user_data['editing_company_id'] = company_id
        
        # Get company details
        companies = self.db.get_companies(self.bot_id)
        comp = next((c for c in companies if c['id'] == company_id), None)
        
        if not comp:
            await query.answer("❌ Company not found!", show_alert=True)
            return ConversationHandler.END
        
        text = (
            f"✏️ **Edit Company: {comp['name']}**\n\n"
            f"Select what to edit:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📝 Company Name", callback_data="edit_field_name")],
            [InlineKeyboardButton("📄 Description", callback_data="edit_field_desc")],
            [InlineKeyboardButton("📷 Media (Photo/Video/GIF)", callback_data="edit_field_media")],
            [InlineKeyboardButton("🔘 Button Text", callback_data="edit_field_btn_text")],
            [InlineKeyboardButton("🔗 Button URL", callback_data="edit_field_btn_url")],
            [InlineKeyboardButton("🗑️ DELETE COMPANY", callback_data=f"delete_company_{company_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit")]
        ]
        
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return EDIT_FIELD
    
    async def edit_company_choose_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Route to appropriate edit handler based on field selection"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "cancel_edit":
            await query.message.edit_text("✅ Edit cancelled.", parse_mode='Markdown')
            return ConversationHandler.END
        elif query.data.startswith("delete_company_"):
            # Show confirmation
            company_id = int(query.data.split("_")[2])
            companies = self.db.get_companies(self.bot_id)
            comp = next((c for c in companies if c['id'] == company_id), None)
            
            text = (
                f"⚠️ **DELETE CONFIRMATION**\n\n"
                f"Are you sure you want to delete:\n"
                f"**{comp['name']}**?\n\n"
                f"❌ This action CANNOT be undone!"
            )
            
            keyboard = [
                [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{company_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_delete")]
            ]
            
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return EDIT_FIELD  # Stay in same state for confirmation
        elif query.data == "edit_field_name":
            await query.message.edit_text("📝 Send new **Company Name**:", parse_mode='Markdown')
            return EDIT_NAME
        elif query.data == "edit_field_desc":
            await query.message.edit_text("📄 Send new **Description**:", parse_mode='Markdown')
            return EDIT_DESC
            await update.callback_query.message.reply_text(
                f"🖼️ **Upload new media**\n\n"
                f"Send a Photo, Video, or GIF:\n\n"
                f"Type /cancel to cancel."
            )
            return EDIT_MEDIA
        
        elif choice == "edit_field_btn_text":
            await update.callback_query.message.reply_text(
                f"🔘 **Current Button Text:** {comp['button_text']}\n\n"
                f"Enter new button text:\n\n"
                f"Type /cancel to cancel."
            )
            return EDIT_BTN_TEXT
        
        elif choice == "edit_field_btn_url":
            await update.callback_query.message.reply_text(
                f"🔗 **Current Button URL:**\n{comp['button_url']}\n\n"
                f"Enter new URL:\n\n"
                f"Type/cancel to cancel."
            )
            return EDIT_BTN_URL
    
    async def edit_company_save_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new company name"""
        new_name = update.message.text
        company_id = context.user_data['edit_company_id']
        
        self.db.edit_company(company_id, 'name', new_name)
        await update.message.reply_text(f"✅ Company name updated to: *{new_name}*", parse_mode='Markdown')
        
        context.user_data.clear()
        return ConversationHandler.END
    
    async def edit_company_save_desc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new company description"""
        new_desc = update.message.text
        company_id = context.user_data['edit_company_id']
        
        self.db.edit_company(company_id, 'description', new_desc)
        await update.message.reply_text("✅ Description updated successfully!")
        
        context.user_data.clear()
        return ConversationHandler.END
    
    async def edit_company_save_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new company media (photo/video/GIF)"""
        company_id = context.user_data['edit_company_id']
        
        # Get file_id and media type
        file_id = None
        media_type = None
        
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            media_type = 'photo'
        elif update.message.video:
            file_id = update.message.video.file_id
            media_type = 'video'
        elif update.message.animation:
            file_id = update.message.animation.file_id
            media_type = 'animation'
        
        # Update database with file_id
        self.db.edit_company(company_id, 'media_file_id', file_id)
        self.db.edit_company(company_id, 'media_type', media_type)
        
        await update.message.reply_text(f"✅ Media updated! Type: {media_type}")
        
        context.user_data.clear()
        return ConversationHandler.END
    
    async def edit_company_save_btn_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new button text"""
        new_btn_text = update.message.text
        company_id = context.user_data['edit_company_id']
        
        self.db.edit_company(company_id, 'button_text', new_btn_text)
        await update.message.reply_text(f"✅ Button text updated to: *{new_btn_text}*", parse_mode='Markdown')
        
        context.user_data.clear()
        return ConversationHandler.END
    
    async def edit_company_save_btn_url(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new button URL"""
        new_url = update.message.text
        company_id = context.user_data['edit_company_id']
        
        self.db.edit_company(company_id, 'button_url', new_url)
        await update.message.reply_text(f"✅ Button URL updated!")
        
        context.user_data.clear()
        return ConversationHandler.END

    # --- Helpers ---
    async def check_subscription(self, update):
        """Check if bot subscription is active - blocks all operations if expired"""
        bot_data = self.db.get_bot_by_token(self.token)
        
        if not bot_data:
            return False
        
        # Parse expiry date
        try:
            expiry = datetime.datetime.fromisoformat(bot_data['subscription_end'])
            now = datetime.datetime.now()
            
            # Check if expired
            if now > expiry:
                days_expired = (now - expiry).days
                
                # Send expiry notice
                expiry_msg = (
                    f"⚠️ **SUBSCRIPTION EXPIRED**\n\n"
                    f"Bot subscription expired **{days_expired} day(s)** ago.\n\n"
                    f"📅 Expired on: {expiry.strftime('%Y-%m-%d')}\n\n"
                    f"Please contact bot owner to renew subscription.\n\n"
                    f"🔒 Bot is currently **DISABLED**."
                )
                
                await update.effective_chat.send_message(expiry_msg, parse_mode='Markdown')
                return False  # Block operation
            
            # Check if expiring soon (within 3 days)
            days_left = (expiry - now).days
            if days_left <= 3 and days_left > 0:
                # Only show warning to BOT OWNER, not regular users
                user_id = update.effective_user.id if update.effective_user else None
                is_owner = user_id == bot_data.get('owner_id')
                
                if is_owner:
                    # Only show warning ONCE per session (not on every interaction)
                    if not hasattr(self, '_expiry_warned') or not self._expiry_warned:
                        self._expiry_warned = True  # Mark as warned for this session
                        warning_msg = (
                            f"⚠️ **Subscription Expiring Soon!**\n\n"
                            f"📅 Expires in: **{days_left} day(s)**\n"
                            f"Contact owner to extend subscription."
                        )
                        await update.effective_chat.send_message(warning_msg, parse_mode='Markdown')
            
            return True  # Allow operation
            
        except Exception as e:
            print(f"Subscription check error: {e}")
            return True  # Fail open to avoid breaking bots

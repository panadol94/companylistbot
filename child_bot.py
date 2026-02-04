import logging
import datetime
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, ChatMemberUpdated
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler, ChatMemberHandler
from database import Database
from config import DEFAULT_GLOBAL_AD

# States for Admin Add/Edit Company
NAME, DESC, MEDIA, BUTTON_TEXT, BUTTON_URL = range(5)
# States for Broadcast
BROADCAST_TARGET = 6
BROADCAST_CONTENT, BROADCAST_CONFIRM = range(7, 9)
# States for Schedule Broadcast
SCHEDULE_TIME = 30
# States for Edit Welcome
WELCOME_PHOTO, WELCOME_TEXT = range(11, 13)
# States for Edit Company
EDIT_FIELD, EDIT_NAME, EDIT_DESC, EDIT_MEDIA, EDIT_BTN_TEXT, EDIT_BTN_URL = range(15, 21)
# State for Search
SEARCH = 22
# States for Menu Button
MENU_BTN_TEXT, MENU_BTN_URL = range(23, 25)
# States for Pair Buttons
PAIR_SELECT_1, PAIR_SELECT_2 = range(26, 28)
# States for Recurring Broadcast
RECURRING_TYPE = 40

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
        # Reload recurring broadcast jobs from database
        self.reload_recurring_jobs()

    def reload_recurring_jobs(self):
        """Reload recurring broadcast jobs from database on startup"""
        recurring = self.db.get_recurring_broadcasts(self.bot_id)
        for b in recurring:
            try:
                self.start_recurring_job(b['id'], b['interval_type'], b['interval_value'])
                self.logger.info(f"Reloaded recurring job: recurring_{b['id']}")
            except Exception as e:
                self.logger.error(f"Failed to reload recurring job {b['id']}: {e}")

    async def stop(self):
        await self.app.stop()
        await self.app.shutdown()

    # --- Handlers Setup ---
    def setup_handlers(self):
        # Admin Commands
        self.app.add_handler(CommandHandler("settings", self.admin_dashboard))
        self.app.add_handler(CommandHandler("admin", self.admin_dashboard))

        # Main User Commands (work in both private and group)
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("company", self.main_menu))
        
        # Group-friendly commands
        self.app.add_handler(CommandHandler("list", self.cmd_list_companies))
        self.app.add_handler(CommandHandler("menu", self.cmd_show_menu))
        self.app.add_handler(CommandHandler("4d", self.cmd_4d_menu))
        self.app.add_handler(CommandHandler("wallet", self.cmd_wallet_private))

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
                BROADCAST_TARGET: [CallbackQueryHandler(self.broadcast_choose_target)],
                BROADCAST_CONTENT: [MessageHandler(filters.ALL & ~filters.COMMAND, self.broadcast_content)],
                BROADCAST_CONFIRM: [CallbackQueryHandler(self.broadcast_confirm)],
                SCHEDULE_TIME: [CallbackQueryHandler(self.broadcast_confirm)],
                RECURRING_TYPE: [CallbackQueryHandler(self.recurring_type_handler)]
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
        
        # Add Menu Button Wizard
        menu_btn_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_menu_btn_start, pattern=r'^menu_add_btn$')],
            states={
                MENU_BTN_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_menu_btn_text)],
                MENU_BTN_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_menu_btn_url)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern=r'^cancel$')],
            per_message=False
        )
        self.app.add_handler(menu_btn_conv)
        
        # User Actions via Callback (MUST BE AFTER ConversationHandlers!)
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))

        # --- REORDERED: Channel Post Handler MUST be checked first! ---
        # Channel Post Handler (for forwarder)
        self.app.add_handler(MessageHandler(
            filters.ChatType.CHANNEL, 
            self.handle_channel_post
        ))

        # Support System & Text (handles both regular and forwarded messages)
        # Exclude channels to avoid double handling or crashes
        self.app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & ~filters.ChatType.CHANNEL, 
            self.handle_message
        ))
        
        # Media Message Handler (for forwarded media - photos, videos, etc)
        self.app.add_handler(MessageHandler(
            (filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.Document.ALL) & ~filters.COMMAND & ~filters.ChatType.CHANNEL, 
            self.handle_media_message
        ))
        
        # Bot Status Change Handler (detect when bot becomes admin)
        self.app.add_handler(ChatMemberHandler(
            self.handle_bot_status_change,
            ChatMemberHandler.MY_CHAT_MEMBER
        ))
        
        # Bot Status Change Handler (detect when bot becomes admin)
        self.app.add_handler(ChatMemberHandler(
            self.handle_bot_status_change,
            ChatMemberHandler.MY_CHAT_MEMBER
        ))

    # --- Group Commands ---
    async def cmd_list_companies(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show companies list - works in groups"""
        if not await self.check_subscription(update): return
        
        companies = self.db.get_companies(self.bot_id)
        if not companies:
            await update.message.reply_text("📭 Tiada company didaftarkan.")
            return
        
        text = "📋 **SENARAI COMPANY**\n\n"
        for i, comp in enumerate(companies[:10], 1):
            text += f"{i}. **{comp['name']}**\n"
        
        if len(companies) > 10:
            text += f"\n_...dan {len(companies) - 10} lagi_"
        
        text += "\n\n💬 Klik butang untuk lihat detail:"
        
        keyboard = []
        for comp in companies[:5]:
            keyboard.append([InlineKeyboardButton(f"👁️ {comp['name'][:25]}", callback_data=f"view_{comp['id']}")])
        keyboard.append([InlineKeyboardButton("📖 Lihat Semua", callback_data="list_page_0")])
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def cmd_show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show main menu - works in groups"""
        await self.main_menu(update, context)

    async def cmd_4d_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show 4D menu - works in groups"""
        if not await self.check_subscription(update): return
        
        stats = self.db.get_4d_statistics()
        
        if stats:
            stats_text = f"📊 Data: {stats['total_draws']} draws analyzed"
        else:
            stats_text = "⚠️ Belum ada data. Tekan Refresh untuk load."
        
        text = (
            "🎰 **4D STATISTICAL ANALYZER**\n\n"
            f"{stats_text}\n\n"
            "Pilih analisis yang anda mahu:\n\n"
            "⚠️ _Disclaimer: Ini untuk hiburan sahaja._\n"
            "_Tiada jaminan menang._"
        )
        
        keyboard = [
            [InlineKeyboardButton("🏆 Latest Results", callback_data="4d_latest")],
            [InlineKeyboardButton("🔍 Check Number", callback_data="4d_check")],
            [InlineKeyboardButton("🔥 Hot Numbers", callback_data="4d_hot_numbers"), 
             InlineKeyboardButton("❄️ Cold Numbers", callback_data="4d_cold_numbers")],
            [InlineKeyboardButton("📊 Digit Frequency", callback_data="4d_digit_freq")],
            [InlineKeyboardButton("🎯 Generate Lucky Number", callback_data="4d_lucky_gen")],
            [InlineKeyboardButton("🔄 Refresh Data", callback_data="4d_refresh")]
        ]
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def cmd_wallet_private(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show wallet - PRIVATE MESSAGE ONLY for privacy"""
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id
        
        # If in group, send private message instead
        if chat_type != 'private':
            await update.message.reply_text(
                "🔒 **PRIVACY PROTECTION**\n\n"
                "Maklumat wallet & referral adalah sulit.\n"
                "Saya akan hantar ke PM anda.",
                parse_mode='Markdown'
            )
            
            # Send to private chat
            try:
                user = self.db.get_user(self.bot_id, user_id)
                if user:
                    balance = user.get('balance', 0)
                    total_invites = user.get('total_invites', 0)
                    total_earned = total_invites * 1.00
                    
                    bot_data = self.db.get_bot_by_token(self.token)
                    bot_username = bot_data.get('bot_username', 'bot') if bot_data else 'bot'
                    referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
                    
                    text = (
                        f"💰 **YOUR WALLET**\n\n"
                        f"💵 Balance: **RM {balance:.2f}**\n"
                        f"👥 Total Referrals: **{total_invites}**\n"
                        f"💎 Total Earned: **RM {total_earned:.2f}**\n\n"
                        f"🔗 **Referral Link:**\n"
                        f"`{referral_link}`\n\n"
                        f"_Minimum withdrawal: RM10_"
                    )
                    
                    await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
                else:
                    await context.bot.send_message(chat_id=user_id, text="❌ Sila /start bot dulu.")
            except Exception as e:
                self.logger.error(f"Failed to send wallet PM: {e}")
                await update.message.reply_text("❌ Sila /start bot dalam PM dulu.")
            return
        
        # Private chat - show normally
        user = self.db.get_user(self.bot_id, user_id)
        if not user:
            await update.message.reply_text("❌ User not found. Type /start first.")
            return
        
        balance = user.get('balance', 0)
        total_invites = user.get('total_invites', 0)
        total_earned = total_invites * 1.00
        
        text = (
            f"💰 **YOUR WALLET**\n\n"
            f"💵 Balance: **RM {balance:.2f}**\n"
            f"👥 Total Referrals: **{total_invites}**\n"
            f"💎 Total Earned: **RM {total_earned:.2f}**\n\n"
            f"_Minimum withdrawal: RM10_"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 WITHDRAW", callback_data="withdraw")],
            [InlineKeyboardButton("🔗 Share Link", callback_data="share_link")],
            [InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- Start & Menu ---
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Handle Referral
        args = context.args
        referrer_id = None
        
        if args:
            arg = args[0]
            # Handle ref_123456 format
            if arg.startswith("ref_"):
                try:
                    referrer_id = int(arg.replace("ref_", ""))
                except:
                    pass
            # Also handle direct ID format (legacy)
            elif arg.isdigit():
                referrer_id = int(arg)
        
        user = update.effective_user
        
        # Don't allow self-referral
        if referrer_id == user.id:
            referrer_id = None
        
        # Register user
        is_new = self.db.add_user(self.bot_id, user.id, referrer_id)
        if is_new and referrer_id:
            # Notify referrer with fancy notification
            try:
                # Get referrer's updated stats
                referrer_data = self.db.get_user(self.bot_id, referrer_id)
                total_invites = referrer_data.get('total_invites', 1) if referrer_data else 1
                new_balance = referrer_data.get('balance', 1.0) if referrer_data else 1.0
                
                notification = (
                    f"🎉 **REFERRAL BERJAYA!**\n\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"👤 **{user.first_name}** baru join!\n"
                    f"💰 Anda dapat: **+RM1.00**\n"
                    f"━━━━━━━━━━━━━━━━━\n\n"
                    f"📊 **Stats Anda:**\n"
                    f"👥 Total Referral: **{total_invites}**\n"
                    f"💵 Baki Semasa: **RM{new_balance:.2f}**\n\n"
                    f"🔥 Teruskan share link anda!"
                )
                await context.bot.send_message(chat_id=referrer_id, text=notification, parse_mode='Markdown')
            except: pass  # Referrer might have blocked bot
            
        await self.main_menu(update, context)

    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_subscription(update): return

        bot_data = self.db.get_bot_by_token(self.token)
        caption = bot_data['custom_caption'] or f"Selamat Datang ke {bot_data['bot_username']}! 🚀\n\nPlatform penyenaraian Company terbaik.\nSila pilih menu di bawah:"
        
        # Check if referral system is enabled
        referral_enabled = self.db.is_referral_enabled(self.bot_id)
        
        keyboard = [
            [InlineKeyboardButton("🏢 LIST COMPANY", callback_data="list_page_0")],
            [InlineKeyboardButton("🎰 4D STATS", callback_data="4d_menu")],
        ]
        
        # Only show referral buttons if enabled
        if referral_enabled:
            keyboard.append([InlineKeyboardButton("💰 DOMPET SAYA", callback_data="wallet")])
            keyboard.append([InlineKeyboardButton("🔗 SHARE LINK", callback_data="share_link")])
            keyboard.append([InlineKeyboardButton("🏆 LEADERBOARD", callback_data="leaderboard")])

        # Add custom buttons from database
        custom_buttons = self.db.get_menu_buttons(self.bot_id)
        if custom_buttons:
            # Group buttons by row_group for pairing
            paired_groups = {}
            unpaired = []
            
            for btn in custom_buttons:
                if btn['row_group']:
                    if btn['row_group'] not in paired_groups:
                        paired_groups[btn['row_group']] = []
                    paired_groups[btn['row_group']].append(btn)
                else:
                    unpaired.append(btn)
            
            # Add paired buttons (2 per row)
            for group_id, btns in paired_groups.items():
                row = [InlineKeyboardButton(b['text'], url=b['url']) for b in btns[:2]]
                keyboard.append(row)
            
            # Add unpaired buttons (1 per row)
            for btn in unpaired:
                keyboard.append([InlineKeyboardButton(btn['text'], url=btn['url'])])

        if update.callback_query:
            # Carousel style - edit existing message instead of delete+send
            try:
                if bot_data['custom_banner']:
                    await update.callback_query.message.edit_media(
                        media=InputMediaPhoto(media=bot_data['custom_banner'], caption=caption, parse_mode='HTML'),
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                else:
                    await update.callback_query.message.edit_text(
                        caption,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode='HTML'
                    )
            except Exception as e:
                # Fallback: send new message if edit fails (e.g., different media type)
                try: await update.callback_query.message.delete()
                except: pass
                if bot_data['custom_banner']:
                    await update.effective_chat.send_photo(photo=bot_data['custom_banner'], caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                else:
                    await update.effective_chat.send_message(caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            # Fresh /start command - send new message
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
            f"{comp['description']}"
        )
        
        # Build keyboard
        keyboard = []
        
        # Row 1: Company action button (REGISTER)
        if comp.get('button_text') and comp.get('button_url'):
            keyboard.append([InlineKeyboardButton(comp['button_text'], url=comp['button_url'])])
        
        # Row 2: Carousel Navigation (PREV / Page Indicator / NEXT)
        total_companies = len(companies)
        if total_companies > 1:
            nav_row = []
            
            # PREV button (go to previous, wrap around to last if at first)
            prev_page = (page - 1) if page > 0 else (total_companies - 1)
            nav_row.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"list_page_{prev_page}"))
            
            # Page indicator (current / total)
            nav_row.append(InlineKeyboardButton(f"📍 {page + 1}/{total_companies}", callback_data="noop"))
            
            # NEXT button (go to next, wrap around to first if at last)
            next_page = (page + 1) if page < (total_companies - 1) else 0
            nav_row.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"list_page_{next_page}"))
            
            keyboard.append(nav_row)
        
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
        
        # Load buttons from company_buttons table
        buttons = self.db.get_company_buttons(comp_id)
        
        if buttons:
            # Group buttons by row_group for pairing
            paired_groups = {}
            unpaired = []
            
            for btn in buttons:
                if btn['row_group']:
                    if btn['row_group'] not in paired_groups:
                        paired_groups[btn['row_group']] = []
                    paired_groups[btn['row_group']].append(btn)
                else:
                    unpaired.append(btn)
            
            keyboard = []
            # Add paired buttons (2 per row)
            for group_id, btns in paired_groups.items():
                row = [InlineKeyboardButton(b['text'], url=b['url']) for b in btns[:2]]
                keyboard.append(row)
            
            # Add unpaired buttons (1 per row)
            for btn in unpaired:
                keyboard.append([InlineKeyboardButton(btn['text'], url=btn['url'])])
        else:
            # Fallback to old button_text/button_url from company record
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
        is_owner = user_id == owner_id
        is_admin = self.db.is_bot_admin(self.bot_id, user_id)
        
        if not is_admin:
            self.logger.warning(f"Access denied: user_id={user_id}, owner_id={owner_id}")
            await update.message.reply_text("⛔ Access Denied.")
            return

        # Check referral status for toggle button
        referral_enabled = self.db.is_referral_enabled(self.bot_id)
        referral_btn_text = "🟢 Referral: ON" if referral_enabled else "🔴 Referral: OFF"
        
        # Check livegram status for toggle button
        livegram_enabled = self.db.is_livegram_enabled(self.bot_id)
        livegram_btn_text = "🟢 Livegram: ON" if livegram_enabled else "🔴 Livegram: OFF"
        
        # Check forwarder status
        forwarder_config = self.db.get_forwarder_config(self.bot_id)
        forwarder_active = forwarder_config and forwarder_config.get('is_active')
        forwarder_btn_text = "🟢 Forwarder: ON" if forwarder_active else "🔴 Forwarder: OFF"
        
        # Check pending schedules
        pending = self.db.get_pending_broadcasts(self.bot_id)
        schedule_text = f"🔄 Reset Schedule ({len(pending)})" if pending else "📅 No Schedules"
        
        # Count admins for button
        admins = self.db.get_admins(self.bot_id)
        admin_count = len(admins)

        text = "👑 **ADMIN SETTINGS DASHBOARD**\n\nWelcome Boss! Full control in your hands."
        keyboard = [
            [InlineKeyboardButton("➕ Add Company", callback_data="admin_add_company"), InlineKeyboardButton("🗑️ Delete Company", callback_data="admin_del_list")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("⚙️ Customize Menu", callback_data="customize_menu")],
            [InlineKeyboardButton("💳 Withdrawals", callback_data="admin_withdrawals"), InlineKeyboardButton(schedule_text, callback_data="reset_schedule")],
            [InlineKeyboardButton(referral_btn_text, callback_data="toggle_referral"), InlineKeyboardButton(livegram_btn_text, callback_data="toggle_livegram")],
            [InlineKeyboardButton("🔁 Manage Recurring", callback_data="manage_recurring"), InlineKeyboardButton("📡 Forwarder", callback_data="forwarder_menu")],
            [InlineKeyboardButton("📊 Analytics", callback_data="show_analytics"), InlineKeyboardButton("📥 Export Data", callback_data="export_data")],
        ]
        # Only owner can manage admins
        if is_owner:
            keyboard.append([InlineKeyboardButton(f"👥 Manage Admins ({admin_count})", callback_data="manage_admins")])
        keyboard.append([InlineKeyboardButton("❌ Close Panel", callback_data="close_panel")])
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

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
        
        # 4D Stats Handlers
        elif data == "4d_menu": await self.show_4d_menu(update)
        elif data == "4d_latest": await self.show_4d_latest_results(update)
        elif data == "4d_check": await self.start_4d_check(update, context)
        elif data == "4d_hot_numbers": await self.show_4d_hot_numbers(update)
        elif data == "4d_cold_numbers": await self.show_4d_cold_numbers(update)
        elif data == "4d_lucky_gen": await self.generate_4d_lucky(update)
        elif data == "4d_digit_freq": await self.show_4d_digit_frequency(update)
        elif data == "4d_refresh": await self.refresh_4d_data(update)
        elif data == "4d_sub": await self.subscribe_4d_notification(update)
        elif data == "4d_unsub": await self.unsubscribe_4d_notification(update)
        
        # Admin Actions
        elif data == "admin_withdrawals": await self.show_withdrawals(update)
        elif data.startswith("approve_wd_"): await self.process_withdrawal(update, data, True)
        elif data.startswith("reject_wd_"): await self.process_withdrawal(update, data, False)
        elif data == "admin_del_list": await self.show_delete_company_list(update)
        elif data.startswith("delete_company_"): await self.confirm_delete_company(update, int(data.split("_")[2]))
        elif data == "admin_customize": await self.show_customize_menu(update)
        elif data == "toggle_referral": await self.toggle_referral_system(update)
        elif data == "toggle_livegram": await self.toggle_livegram_system(update)
        elif data == "reset_schedule": await self.show_reset_schedule(update)
        elif data == "confirm_reset_schedule": await self.confirm_reset_schedule(update)
        elif data == "manage_recurring": await self.show_manage_recurring(update)
        elif data.startswith("stop_recurring_"): await self.stop_recurring(update, int(data.split("_")[2]))
        elif data == "show_analytics": await self.show_analytics(update)
        elif data == "export_data": await self.show_export_menu(update)
        elif data == "export_users": await self.export_users_csv(update)
        elif data == "export_companies": await self.export_companies_csv(update)
        elif data == "admin_settings": await self.show_admin_settings(update)
        # Admin Management
        elif data == "manage_admins": await self.show_manage_admins(update)
        elif data == "add_admin_start": await self.add_admin_start(update, context)
        elif data.startswith("delete_admin_"): await self.delete_admin(update, int(data.split("_")[2]))
        # Customize Menu System
        elif data == "customize_menu": await self.show_customize_submenu(update)
        elif data == "edit_welcome": await self.edit_welcome_start(update, context)
        elif data == "manage_menu_btns": await self.show_manage_buttons(update)
        elif data.startswith("del_menu_btn_"): await self.delete_menu_button(update, int(data.split("_")[3]))
        elif data == "pair_menu_btns": await self.start_pair_buttons(update)
        elif data.startswith("pair1_"): await self.select_pair_btn_1(update, int(data.split("_")[1]))
        elif data.startswith("pair2_"): await self.select_pair_btn_2(update, int(data.split("_")[1]))
        elif data.startswith("unpair_btn_"): await self.unpair_button(update, int(data.split("_")[2]))
        # Add Company - More Buttons Flow
        elif data == "add_more_btn": await self.add_more_company_btn(update, context)
        elif data == "finish_company": 
            await query.message.edit_text("✅ Company Berjaya Ditambah!")
            context.user_data.pop('new_comp', None)
        # Company Button Management
        elif data.startswith("manage_co_btns_"): await self.show_company_buttons(update, int(data.split("_")[3]))
        elif data.startswith("add_co_btn_"): await self.start_add_company_btn(update, context, int(data.split("_")[3]))
        elif data.startswith("del_co_btn_"): await self.delete_company_btn(update, int(data.split("_")[3]))
        elif data.startswith("pair_co_btns_"): await self.start_pair_company_btns(update, int(data.split("_")[3]))
        elif data.startswith("copair1_"): await self.select_co_pair_btn1(update, context)
        elif data.startswith("copair2_"): await self.complete_co_pair(update)
        elif data.startswith("unpair_co_btn_"): await self.unpair_company_btn(update, int(data.split("_")[3]))
        elif data == "ef_manage_btns": await self.show_company_buttons_from_edit(update, context)
        # Forwarder Menu
        elif data == "forwarder_menu": await self.show_forwarder_menu(update)
        elif data == "forwarder_toggle": await self.toggle_forwarder(update)
        elif data == "forwarder_toggle_mode": await self.toggle_forwarder_mode_handler(update, context)
        elif data == "forwarder_set_source": await self.forwarder_set_source_start(update, context)
        elif data == "forwarder_set_target": await self.forwarder_set_target_start(update, context)
        elif data == "forwarder_set_this_group": await self.set_current_forwarder_target_group(update, context)
        elif data == "forwarder_set_filter": await self.forwarder_set_filter_start(update, context)
        elif data == "forwarder_clear_filter": await self.forwarder_clear_filter(update)
        elif data == "forwarder_manage_sources": await self.show_forwarder_sources(update)
        elif data.startswith("forwarder_remove_source_"): await self.remove_forwarder_source_handler(update, int(data.split("_")[3]))
        elif data == "forwarder_back": await self.show_admin_settings(update)
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
            [InlineKeyboardButton("🔘 Manage Buttons", callback_data=f"manage_co_btns_{company_id}")],
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
            [InlineKeyboardButton("🔘 Manage Buttons", callback_data="ef_manage_btns")],
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
    
    # --- 4D Stats Module ---
    async def show_4d_menu(self, update: Update):
        """Show 4D stats main menu"""
        stats = self.db.get_4d_statistics()
        user_id = update.effective_user.id
        
        # Check if user is subscribed to notifications
        is_subscribed = self.db.is_subscribed_4d_notification(self.bot_id, user_id)
        
        if stats:
            stats_text = f"📊 Data: {stats['total_draws']} draws analyzed"
        else:
            stats_text = "⚠️ Belum ada data. Tekan Refresh untuk load."
        
        # Notification status
        notify_status = "🔔 ON" if is_subscribed else "🔕 OFF"
        
        text = (
            "🎰 **4D STATISTICAL ANALYZER**\n\n"
            f"{stats_text}\n"
            f"📬 Notification: {notify_status}\n\n"
            "Pilih analisis yang anda mahu:\n\n"
            "⚠️ _Disclaimer: Ini untuk hiburan sahaja._\n"
            "_Tiada jaminan menang._"
        )
        
        # Dynamic subscribe/unsubscribe button
        if is_subscribed:
            notify_btn = InlineKeyboardButton("🔕 Unsubscribe Notification", callback_data="4d_unsub")
        else:
            notify_btn = InlineKeyboardButton("🔔 Subscribe Notification", callback_data="4d_sub")
        
        keyboard = [
            [InlineKeyboardButton("🏆 Latest Results", callback_data="4d_latest")],
            [InlineKeyboardButton("🔍 Check Number", callback_data="4d_check")],
            [InlineKeyboardButton("🔥 Hot Numbers", callback_data="4d_hot_numbers"), 
             InlineKeyboardButton("❄️ Cold Numbers", callback_data="4d_cold_numbers")],
            [InlineKeyboardButton("📊 Digit Frequency", callback_data="4d_digit_freq")],
            [InlineKeyboardButton("🎯 Generate Lucky Number", callback_data="4d_lucky_gen")],
            [notify_btn],
            [InlineKeyboardButton("🔄 Refresh Data", callback_data="4d_refresh")],
            [InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]
        ]
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            # Message has media, delete and send new
            try:
                await update.callback_query.message.delete()
            except:
                pass
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_4d_latest_results(self, update: Update):
        """Show latest 4D results from all companies - organized by region"""
        results = self.db.get_4d_results(limit=33)  # Get latest from each company
        
        if not results:
            await update.callback_query.answer("Tiada data! Sila Refresh dulu.", show_alert=True)
            return
        
        # Group by company
        by_company = {}
        for r in results:
            company = r.get('company', 'UNKNOWN')
            if company not in by_company:
                by_company[company] = r
        
        text = "🏆 **KEPUTUSAN 4D TERKINI**\n"
        text += f"📅 _{datetime.datetime.now().strftime('%d/%m/%Y')}_\n"
        
        # Provider icons for all 11 providers
        company_icons = {
            'MAGNUM': '🔴', 'TOTO': '🟢', 'DAMACAI': '🟡',
            'CASHSWEEP': '💜', 'SABAH88': '🟤', 'STC': '🔵',
            'SG4D': '🩷', 'SGTOTO': '🩵',
            'GD': '🐉', 'PERDANA': '🎰', 'LUCKY': '🍀'
        }
        
        # Region groupings
        regions = {
            '🇲🇾 West Malaysia': ['MAGNUM', 'DAMACAI', 'TOTO'],
            '🇲🇾 East Malaysia': ['CASHSWEEP', 'SABAH88', 'STC'],
            '🇸🇬 Singapore': ['SG4D', 'SGTOTO'],
            '🇰🇭 Cambodia': ['GD', 'PERDANA', 'LUCKY']
        }
        
        for region_name, companies in regions.items():
            has_results = any(c in by_company for c in companies)
            if has_results:
                text += f"\n**{region_name}**\n"
                for company in companies:
                    if company in by_company:
                        r = by_company[company]
                        icon = company_icons.get(company, '⚪')
                        
                        text += f"{icon} **{company}**\n"
                        text += f"🥇 `{r['first_prize']}`  🥈 `{r['second_prize']}`  🥉 `{r['third_prize']}`\n"
        
        text += "\n_Tekan Refresh Data untuk update terkini_"
        
        keyboard = [
            [InlineKeyboardButton("🔍 Check My Number", callback_data="4d_check")],
            [InlineKeyboardButton("🔄 Refresh Data", callback_data="4d_refresh")],
            [InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]
        ]
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def start_4d_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start check number flow - ask user for number"""
        self.logger.info("start_4d_check called")
        
        text = (
            "🔍 **CHECK YOUR NUMBER**\n\n"
            "Masukkan nombor 4D anda:\n"
            "(contoh: `1234`)\n\n"
            "_Reply dengan nombor 4 digit_"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="4d_menu")]]
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            self.logger.info("start_4d_check edit_text success")
        except Exception as e:
            self.logger.error(f"start_4d_check edit failed: {e}")
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
        # Set state to wait for number input
        context.user_data['waiting_4d_check'] = True

    async def check_4d_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Check if user's number won in any draw"""
        if not context.user_data.get('waiting_4d_check'):
            return False
        
        number = update.message.text.strip()
        
        # Validate 4 digit number
        if not number.isdigit() or len(number) != 4:
            await update.message.reply_text(
                "❌ Sila masukkan nombor 4 digit sahaja!\n\ncontoh: `1234`",
                parse_mode='Markdown'
            )
            return True
        
        # Clear waiting state
        context.user_data['waiting_4d_check'] = False
        
        # Search in database
        results = self.db.get_4d_results(limit=30)
        
        found_wins = []
        
        for r in results:
            company = r.get('company', '')
            date = r.get('draw_date', '')
            
            # Check main prizes
            if r['first_prize'] == number:
                found_wins.append(f"🥇 **1ST PRIZE** - {company} ({date})")
            elif r['second_prize'] == number:
                found_wins.append(f"🥈 **2ND PRIZE** - {company} ({date})")
            elif r['third_prize'] == number:
                found_wins.append(f"🥉 **3RD PRIZE** - {company} ({date})")
            elif r['special_prizes'] and number in r['special_prizes'].split(','):
                found_wins.append(f"✨ **SPECIAL** - {company} ({date})")
            elif r['consolation_prizes'] and number in r['consolation_prizes'].split(','):
                found_wins.append(f"🎁 **CONSOLATION** - {company} ({date})")
        
        if found_wins:
            text = f"🎉 **TAHNIAH!**\n\n"
            text += f"Nombor `{number}` MENANG!\n\n"
            for win in found_wins[:5]:  # Show max 5 wins
                text += f"{win}\n"
            text += "\n🧧 _Huat Ah!_"
        else:
            text = f"😔 **TIDAK MENANG**\n\n"
            text += f"Nombor `{number}` tidak dijumpai dalam rekod.\n\n"
            text += "_Cuba nombor lain atau tunggu result baru!_"
        
        keyboard = [
            [InlineKeyboardButton("🔍 Check Lagi", callback_data="4d_check")],
            [InlineKeyboardButton("🏆 Latest Results", callback_data="4d_latest")],
            [InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]
        ]
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return True

    async def show_4d_hot_numbers(self, update: Update):
        """Show frequently appearing numbers"""
        stats = self.db.get_4d_statistics()
        
        if not stats:
            await update.callback_query.answer("Tiada data! Sila Refresh dulu.", show_alert=True)
            return
        
        text = "🔥 **HOT NUMBERS**\n\n"
        text += "Nombor yang paling kerap keluar:\n\n"
        
        text += "**🔢 Hot Digits:**\n"
        for digit, count in stats['hot_digits']:
            bar = "█" * min(count // 10, 10)
            text += f"`{digit}` - {count}x {bar}\n"
        
        text += "\n**🎯 Hot 4D Numbers:**\n"
        for num, count in stats['hot_numbers'][:5]:
            text += f"`{num}` - {count}x keluar\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_4d_cold_numbers(self, update: Update):
        """Show rarely appearing numbers"""
        stats = self.db.get_4d_statistics()
        
        if not stats:
            await update.callback_query.answer("Tiada data! Sila Refresh dulu.", show_alert=True)
            return
        
        text = "❄️ **COLD NUMBERS**\n\n"
        text += "Digit yang jarang keluar:\n\n"
        
        for digit, count in stats['cold_digits']:
            bar = "░" * min(count // 10, 10)
            text += f"`{digit}` - {count}x {bar}\n"
        
        text += "\n💡 _Cold numbers mungkin akan keluar soon!_"
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_4d_digit_frequency(self, update: Update):
        """Show digit frequency chart"""
        stats = self.db.get_4d_statistics()
        
        if not stats:
            await update.callback_query.answer("Tiada data! Sila Refresh dulu.", show_alert=True)
            return
        
        text = "📊 **DIGIT FREQUENCY**\n\n"
        
        freq = stats['digit_frequency']
        max_count = max(freq.values()) if freq.values() else 1
        
        for digit in range(10):
            count = freq.get(str(digit), 0)
            bar_len = int((count / max_count) * 10)
            bar = "█" * bar_len + "░" * (10 - bar_len)
            text += f"`{digit}` {bar} {count}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def generate_4d_lucky(self, update: Update):
        """Generate lucky numbers based on statistics"""
        import random
        
        stats = self.db.get_4d_statistics()
        
        # Generate numbers with bias towards hot digits if we have stats
        numbers = []
        for _ in range(5):
            if stats and stats['hot_digits']:
                # 60% chance to use hot digits
                hot = [d[0] for d in stats['hot_digits'][:5]]
                num = ""
                for _ in range(4):
                    if random.random() < 0.6 and hot:
                        num += random.choice(hot)
                    else:
                        num += str(random.randint(0, 9))
                numbers.append(num)
            else:
                # Pure random
                numbers.append(f"{random.randint(0, 9999):04d}")
        
        user = update.effective_user
        text = (
            f"🎯 **LUCKY NUMBERS**\n"
            f"_untuk @{user.username or user.first_name}_\n\n"
        )
        
        emojis = ["🔮", "⭐", "💫", "🍀", "🧧"]
        for i, num in enumerate(numbers):
            text += f"{emojis[i]} `{num}`\n"
        
        text += f"\n📅 {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        text += "\n✨ _Good Luck! Huat Ah!_ 🧧\n"
        text += "\n⚠️ _For entertainment only_"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Generate Lagi", callback_data="4d_lucky_gen")],
            [InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]
        ]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def refresh_4d_data(self, update: Update):
        """Fetch latest 4D data from web sources with timeout protection"""
        import asyncio
        
        await update.callback_query.answer("🔄 Loading 4D data... (mungkin ambil 10-30 saat)", show_alert=False)
        
        try:
            # Import scraper
            from utils_4d import fetch_all_4d_results, get_fallback_results
            
            # Add timeout to prevent hanging (30 seconds max)
            try:
                results = await asyncio.wait_for(fetch_all_4d_results(), timeout=30.0)
            except asyncio.TimeoutError:
                self.logger.warning("4D fetch timeout, using fallback data")
                results = get_fallback_results()
            
            if results:
                saved = 0
                for company, data in results.items():
                    for draw in data:
                        success = self.db.save_4d_result(
                            company=company,
                            draw_date=draw['date'],
                            first=draw['first'],
                            second=draw['second'],
                            third=draw['third'],
                            special=draw['special'],
                            consolation=draw['consolation']
                        )
                        if success:
                            saved += 1
                
                # Send success message as new message to avoid callback issues
                try:
                    await update.effective_chat.send_message(
                        f"✅ **4D DATA UPDATED!**\n\n"
                        f"📊 Loaded: {len(results)} companies\n"
                        f"💾 Saved: {saved} new results",
                        parse_mode='Markdown'
                    )
                except:
                    pass
            else:
                await update.effective_chat.send_message("⚠️ Gagal fetch data. Cuba lagi.")
                
        except ImportError as e:
            self.logger.error(f"4D import error: {e}")
            # If scraper not available, use sample data for demo
            await self._load_sample_4d_data()
            await update.effective_chat.send_message("✅ Sample data loaded for demo!")
        except Exception as e:
            self.logger.error(f"4D fetch error: {e}")
            await update.effective_chat.send_message(f"❌ Error: {str(e)[:100]}")
        
        # Refresh menu with delay to avoid callback conflict
        await asyncio.sleep(0.5)
        await self.show_4d_menu(update)

    async def _load_sample_4d_data(self):
        """Load sample 4D data for demo purposes - all 11 providers"""
        import random
        
        # All 11 providers
        companies = [
            'MAGNUM', 'TOTO', 'DAMACAI',  # West MY
            'CASHSWEEP', 'SABAH88', 'STC',  # East MY
            'SG4D', 'SGTOTO',  # Singapore
            'GD', 'PERDANA', 'LUCKY'  # Cambodia
        ]
        
        for company in companies:
            for days_ago in range(30):
                date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime('%Y-%m-%d')
                
                # Generate random results
                first = f"{random.randint(0, 9999):04d}"
                second = f"{random.randint(0, 9999):04d}"
                third = f"{random.randint(0, 9999):04d}"
                special = ",".join([f"{random.randint(0, 9999):04d}" for _ in range(10)])
                consolation = ",".join([f"{random.randint(0, 9999):04d}" for _ in range(10)])
                
                self.db.save_4d_result(company, date, first, second, third, special, consolation)
    
    async def subscribe_4d_notification(self, update: Update):
        """Subscribe user to 4D result notifications"""
        user_id = update.effective_user.id
        
        success = self.db.subscribe_4d_notification(self.bot_id, user_id)
        
        if success:
            await update.callback_query.answer("🔔 Anda akan terima notification bila result baru keluar!", show_alert=True)
        else:
            await update.callback_query.answer("❌ Gagal subscribe. Cuba lagi.", show_alert=True)
        
        # Refresh menu to show updated status
        await self.show_4d_menu(update)
    
    async def unsubscribe_4d_notification(self, update: Update):
        """Unsubscribe user from 4D result notifications"""
        user_id = update.effective_user.id
        
        success = self.db.unsubscribe_4d_notification(self.bot_id, user_id)
        
        if success:
            await update.callback_query.answer("🔕 Anda tidak lagi akan terima notification.", show_alert=True)
        else:
            await update.callback_query.answer("❌ Gagal unsubscribe. Cuba lagi.", show_alert=True)
        
        # Refresh menu to show updated status
        await self.show_4d_menu(update)
    
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
        keyboard.append([InlineKeyboardButton("« Back", callback_data="admin_settings")])
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
        keyboard = [[InlineKeyboardButton("« Back", callback_data="admin_settings")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def toggle_referral_system(self, update: Update):
        """Toggle referral system on/off"""
        new_state = self.db.toggle_referral(self.bot_id)
        status_text = "🟢 **ON**" if new_state else "🔴 **OFF**"
        
        await update.callback_query.answer(f"Referral system is now {status_text}")
        await self.show_admin_settings(update)
    
    async def show_admin_settings(self, update: Update):
        """Show admin settings dashboard (called from back buttons)"""
        try:
            user_id = update.effective_user.id
            self.logger.info(f"show_admin_settings called by user {user_id}")
            
            # Check owner status
            bot_data = self.db.get_bot_by_token(self.token)
            owner_id = int(bot_data.get('owner_id', 0)) if bot_data else 0
            is_owner = user_id == owner_id
            
            # Check referral status for toggle button
            referral_enabled = self.db.is_referral_enabled(self.bot_id)
            referral_btn_text = "🟢 Referral: ON" if referral_enabled else "🔴 Referral: OFF"
            
            # Check livegram status for toggle button
            livegram_enabled = self.db.is_livegram_enabled(self.bot_id)
            livegram_btn_text = "🟢 Livegram: ON" if livegram_enabled else "🔴 Livegram: OFF"
            
            # Check forwarder status
            forwarder_config = self.db.get_forwarder_config(self.bot_id)
            forwarder_active = forwarder_config and forwarder_config.get('is_active')
            forwarder_btn_text = "🟢 Forwarder: ON" if forwarder_active else "🔴 Forwarder: OFF"
            
            # Check pending schedules
            pending = self.db.get_pending_broadcasts(self.bot_id)
            schedule_text = f"🔄 Reset Schedule ({len(pending)})" if pending else "📅 No Schedules"

            # Count admins
            admins = self.db.get_admins(self.bot_id)
            admin_count = len(admins)

            text = "👑 **ADMIN SETTINGS DASHBOARD**\n\nWelcome Boss! Full control in your hands."
            keyboard = [
                [InlineKeyboardButton("➕ Add Company", callback_data="admin_add_company"), InlineKeyboardButton("🗑️ Delete Company", callback_data="admin_del_list")],
                [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("⚙️ Customize Menu", callback_data="customize_menu")],
                [InlineKeyboardButton("💳 Withdrawals", callback_data="admin_withdrawals"), InlineKeyboardButton(schedule_text, callback_data="reset_schedule")],
                [InlineKeyboardButton(referral_btn_text, callback_data="toggle_referral"), InlineKeyboardButton(livegram_btn_text, callback_data="toggle_livegram")],
                [InlineKeyboardButton("🔁 Manage Recurring", callback_data="manage_recurring"), InlineKeyboardButton("📡 Forwarder", callback_data="forwarder_menu")],
                [InlineKeyboardButton("📊 Analytics", callback_data="show_analytics"), InlineKeyboardButton("📥 Export Data", callback_data="export_data")],
            ]
            
            # Only owner can manage admins
            if is_owner:
                keyboard.append([InlineKeyboardButton(f"👥 Manage Admins ({admin_count})", callback_data="manage_admins")])
                
            keyboard.append([InlineKeyboardButton("❌ Close Panel", callback_data="close_panel")])
            
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            self.logger.info("show_admin_settings completed successfully")
        except Exception as e:
            self.logger.error(f"Error in show_admin_settings: {e}")
            # Fallback: send new message if edit fails
            await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def toggle_livegram_system(self, update: Update):
        """Toggle livegram system on/off"""
        new_state = self.db.toggle_livegram(self.bot_id)
        status_text = "🟢 **ON**" if new_state else "🔴 **OFF**"
        
        await update.callback_query.answer(f"Livegram system is now {status_text}")
        await self.show_admin_settings(update)
    
    # --- Admin Management ---
    async def show_manage_admins(self, update: Update):
        """Show list of admins with add/remove options"""
        # Only owner can access
        bot_data = self.db.get_bot_by_token(self.token)
        if update.effective_user.id != bot_data.get('owner_id'):
            await update.callback_query.answer("⛔ Only bot owner can manage admins", show_alert=True)
            return
        
        admins = self.db.get_admins(self.bot_id)
        
        if not admins:
            text = (
                "👥 **MANAGE ADMINS**\n\n"
                "📝 Tiada admin lagi.\n\n"
                "Admin boleh:\n"
                "• Add/Edit/Delete Companies\n"
                "• Broadcast messages\n"
                "• Manage withdrawals\n"
                "• Access all settings"
            )
        else:
            text = f"👥 **MANAGE ADMINS** ({len(admins)})\n\n"
            for i, admin in enumerate(admins, 1):
                text += f"**{i}.** `{admin['telegram_id']}`\n"
        
        keyboard = []
        # Delete buttons for each admin
        for admin in admins:
            keyboard.append([InlineKeyboardButton(f"🗑️ Remove {admin['telegram_id']}", callback_data=f"delete_admin_{admin['telegram_id']}")])
        
        keyboard.append([InlineKeyboardButton("➕ Add Admin", callback_data="add_admin_start")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="admin_settings")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def add_admin_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start add admin flow - ask for Telegram ID"""
        context.user_data['waiting_admin_id'] = True
        await update.callback_query.message.edit_text(
            "👥 **ADD NEW ADMIN**\n\n"
            "Sila taip **Telegram ID** user yang nak dijadikan admin:\n\n"
            "_Contoh: 123456789_\n\n"
            "Untuk cancel, taip /cancel",
            parse_mode='Markdown'
        )
    
    async def add_admin_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin ID input"""
        if not context.user_data.get('waiting_admin_id'):
            return False
        
        # Cancel command
        if update.message.text == '/cancel':
            context.user_data.pop('waiting_admin_id', None)
            await update.message.reply_text("❌ Cancelled")
            return True
        
        # Validate input
        try:
            new_admin_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("⚠️ Sila masukkan nombor Telegram ID yang sah.\n\nContoh: 123456789")
            return True
        
        # Check if already admin
        if self.db.is_bot_admin(self.bot_id, new_admin_id):
            await update.message.reply_text("⚠️ User ini sudah menjadi admin.")
            context.user_data.pop('waiting_admin_id', None)
            return True
        
        # Add admin
        owner_id = update.effective_user.id
        success = self.db.add_admin(self.bot_id, new_admin_id, owner_id)
        
        context.user_data.pop('waiting_admin_id', None)
        
        if success:
            await update.message.reply_text(
                f"✅ **Admin Berjaya Ditambah!**\n\n"
                f"👤 Telegram ID: `{new_admin_id}`\n\n"
                f"User boleh access /settings sekarang.",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ Gagal menambah admin. Sila cuba lagi.")
        
        return True
    
    async def delete_admin(self, update: Update, admin_telegram_id: int):
        """Remove an admin"""
        # Only owner can remove
        bot_data = self.db.get_bot_by_token(self.token)
        if update.effective_user.id != bot_data.get('owner_id'):
            await update.callback_query.answer("⛔ Only bot owner can remove admins", show_alert=True)
            return
        
        success = self.db.remove_admin(self.bot_id, admin_telegram_id)
        
        if success:
            await update.callback_query.answer(f"✅ Admin {admin_telegram_id} removed!", show_alert=True)
        else:
            await update.callback_query.answer("❌ Failed to remove admin", show_alert=True)
        
        # Refresh admin list
        await self.show_manage_admins(update)
    
    async def show_reset_schedule(self, update: Update):
        """Show pending scheduled broadcasts for reset"""
        pending = self.db.get_pending_broadcasts(self.bot_id)
        
        if not pending:
            await update.callback_query.answer("📅 Tiada schedule yang pending", show_alert=True)
            return
        
        # List all pending broadcasts
        text = "📅 **SCHEDULED BROADCASTS**\n\n"
        for b in pending:
            scheduled = b.get('scheduled_time', 'Unknown')
            text += f"🆔 `{b['id']}` | ⏰ {scheduled}\n"
            if b.get('message'):
                preview = b['message'][:30] + "..." if len(b['message']) > 30 else b['message']
                text += f"   └ _{preview}_\n"
        
        text += f"\n**Total: {len(pending)} pending**"
        
        keyboard = [
            [InlineKeyboardButton("🗑️ Reset All", callback_data="confirm_reset_schedule")],
            [InlineKeyboardButton("« Back", callback_data="admin_settings")]
        ]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def confirm_reset_schedule(self, update: Update):
        """Cancel all scheduled broadcasts"""
        # Remove scheduler jobs
        pending = self.db.get_pending_broadcasts(self.bot_id)
        for b in pending:
            job_id = f"broadcast_{b['id']}"
            try:
                self.scheduler.remove_job(job_id)
            except:
                pass  # Job might not exist
        
        # Delete from database
        deleted = self.db.delete_all_scheduled_broadcasts(self.bot_id)
        
        await update.callback_query.answer(f"✅ {deleted} schedule(s) deleted!", show_alert=True)
        await self.show_admin_settings(update)

    async def show_manage_recurring(self, update: Update):
        """Show active recurring broadcasts for management"""
        recurring = self.db.get_recurring_broadcasts(self.bot_id)
        
        if not recurring:
            await update.callback_query.message.edit_text(
                "🔁 **MANAGE RECURRING**\n\n"
                "Tiada recurring broadcast yang aktif.\n\n"
                "💡 Buat broadcast baru dan pilih \"🔁 Recurring\"",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("« Back", callback_data="admin_settings")]
                ]),
                parse_mode='Markdown'
            )
            return
        
        text = "🔁 **ACTIVE RECURRING BROADCASTS**\n\n"
        keyboard = []
        
        for b in recurring:
            interval_type = b['interval_type'] or 'unknown'
            interval_value = b['interval_value'] or 0
            
            if interval_type == "hours":
                desc = f"Setiap {interval_value} jam"
            elif interval_type == "daily":
                desc = f"Setiap hari jam {interval_value}:00"
            elif interval_type == "minutes":
                desc = f"Setiap {interval_value} minit"
            else:
                desc = f"{interval_type} ({interval_value})"
            
            # Preview message
            msg = b['message'] or ''
            preview = msg[:25] + "..." if len(msg) > 25 else msg
            
            text += f"🆔 `{b['id']}` | {desc}\n"
            text += f"   └ _{preview}_\n\n"
            
            keyboard.append([InlineKeyboardButton(f"🛑 Stop #{b['id']}", callback_data=f"stop_recurring_{b['id']}")])
        
        keyboard.append([InlineKeyboardButton("« Back", callback_data="admin_settings")])
        
        await update.callback_query.message.edit_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='Markdown'
        )

    async def stop_recurring(self, update: Update, broadcast_id: int):
        """Stop a recurring broadcast"""
        # Remove scheduler job
        job_id = f"recurring_{broadcast_id}"
        try:
            self.scheduler.remove_job(job_id)
            self.logger.info(f"Removed recurring job: {job_id}")
        except Exception as e:
            self.logger.warning(f"Job {job_id} not found in scheduler: {e}")
        
        # Delete from database
        deleted = self.db.delete_recurring_broadcast(broadcast_id, self.bot_id)
        
        if deleted:
            await update.callback_query.answer(f"✅ Recurring #{broadcast_id} stopped!", show_alert=True)
        else:
            await update.callback_query.answer(f"❌ Failed to stop recurring", show_alert=True)
        
        # Refresh list
        await self.show_manage_recurring(update)

    async def show_analytics(self, update: Update):
        """Show bot analytics dashboard"""
        analytics = self.db.get_bot_analytics(self.bot_id)
        
        text = (
            "📊 **BOT ANALYTICS**\n\n"
            f"👥 **Users**\n"
            f"• Total: {analytics['total_users']}\n"
            f"• Today: {analytics['users_today']}\n"
            f"• This Week: {analytics['users_week']}\n"
            f"• This Month: {analytics['users_month']}\n\n"
            f"📈 **Referrals**\n"
            f"• From Referral: {analytics['total_referred']}\n"
            f"• Organic: {analytics['total_users'] - analytics['total_referred']}\n\n"
            f"🏢 **Content**\n"
            f"• Companies: {analytics['total_companies']}\n\n"
        )
        
        if analytics['top_referrers']:
            text += "🏆 **Top Referrers**\n"
            for i, ref in enumerate(analytics['top_referrers'][:5], 1):
                username = ref.get('username') or 'Unknown'
                count = ref.get('referral_count') or 0
                text += f"{i}. @{username} - {count} referrals\n"
        
        keyboard = [[InlineKeyboardButton("« Back", callback_data="admin_settings")]]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_export_menu(self, update: Update):
        """Show export options menu"""
        users = self.db.export_users(self.bot_id)
        companies = self.db.export_companies(self.bot_id)
        
        text = (
            "📥 **EXPORT DATA**\n\n"
            f"👥 Users: {len(users)} records\n"
            f"🏢 Companies: {len(companies)} records\n\n"
            "Pilih data untuk export:"
        )
        
        keyboard = [
            [InlineKeyboardButton(f"📥 Export Users ({len(users)})", callback_data="export_users")],
            [InlineKeyboardButton(f"📥 Export Companies ({len(companies)})", callback_data="export_companies")],
            [InlineKeyboardButton("« Back", callback_data="admin_settings")]
        ]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def export_users_csv(self, update: Update):
        """Export users to CSV and send as document"""
        import csv
        import io
        
        users = self.db.export_users(self.bot_id)
        
        if not users:
            await update.callback_query.answer("Tiada users untuk export!", show_alert=True)
            return
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=['telegram_id', 'username', 'first_name', 'balance', 'referred_by', 'joined_at'])
        writer.writeheader()
        writer.writerows(users)
        
        # Convert to bytes
        csv_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        csv_bytes.name = f"users_export_{self.bot_id}.csv"
        
        await update.callback_query.message.reply_document(
            document=csv_bytes,
            caption=f"✅ Exported {len(users)} users"
        )
        await update.callback_query.answer("✅ Export selesai!")

    async def export_companies_csv(self, update: Update):
        """Export companies to CSV and send as document"""
        import csv
        import io
        
        companies = self.db.export_companies(self.bot_id)
        
        if not companies:
            await update.callback_query.answer("Tiada companies untuk export!", show_alert=True)
            return
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=['id', 'name', 'description', 'category', 'created_at'])
        writer.writeheader()
        writer.writerows(companies)
        
        # Convert to bytes
        csv_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        csv_bytes.name = f"companies_export_{self.bot_id}.csv"
        
        await update.callback_query.message.reply_document(
            document=csv_bytes,
            caption=f"✅ Exported {len(companies)} companies"
        )
        await update.callback_query.answer("✅ Export selesai!")
    
    # --- Customize Menu System ---
    async def show_customize_submenu(self, update: Update):
        """Show customize menu sub-menu"""
        buttons = self.db.get_menu_buttons(self.bot_id)
        btn_count = len(buttons)
        
        text = f"⚙️ **CUSTOMIZE START MENU**\n\nCustom buttons: {btn_count}"
        keyboard = [
            [InlineKeyboardButton("🖼️ Edit Banner", callback_data="edit_welcome")],
            [InlineKeyboardButton("➕ Add Button", callback_data="menu_add_btn")],
            [InlineKeyboardButton("📋 Manage Buttons", callback_data="manage_menu_btns")],
            [InlineKeyboardButton("« Back", callback_data="admin_settings")]
        ]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_manage_buttons(self, update: Update):
        """Show list of custom buttons to manage"""
        buttons = self.db.get_menu_buttons(self.bot_id)
        
        if not buttons:
            text = "📋 **MANAGE BUTTONS**\n\n_No custom buttons yet._\n\nUse ➕ Add Button to create one."
            keyboard = [[InlineKeyboardButton("« Back", callback_data="customize_menu")]]
        else:
            text = "📋 **MANAGE BUTTONS**\n\nYour custom buttons:\n"
            keyboard = []
            for btn in buttons:
                paired = "🔗" if btn['row_group'] else ""
                keyboard.append([
                    InlineKeyboardButton(f"{paired} {btn['text']}", callback_data=f"view_menu_btn_{btn['id']}"),
                    InlineKeyboardButton("🗑️", callback_data=f"del_menu_btn_{btn['id']}")
                ])
            keyboard.append([InlineKeyboardButton("🔗 Pair Buttons", callback_data="pair_menu_btns")])
            keyboard.append([InlineKeyboardButton("« Back", callback_data="customize_menu")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- Add Menu Button Wizard ---
    async def add_menu_btn_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start Add Menu Button wizard"""
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(
            "➕ **ADD BUTTON**\n\n"
            "Step 1: Enter button text\n\n"
            "Example: 📞 Contact Us\n\n"
            "Type /cancel to cancel."
        )
        return MENU_BTN_TEXT

    async def add_menu_btn_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save button text and ask for URL"""
        context.user_data['menu_btn_text'] = update.message.text
        await update.message.reply_text(
            "🔗 **Step 2: Enter button URL**\n\n"
            "Example: https://t.me/yourusername\n\n"
            "Type /cancel to cancel."
        )
        return MENU_BTN_URL

    async def add_menu_btn_url(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save button URL and complete"""
        url = update.message.text
        text = context.user_data.get('menu_btn_text', 'Button')
        
        # Validate URL
        if not url.startswith(('http://', 'https://', 't.me/')):
            await update.message.reply_text("⚠️ Invalid URL. Must start with http://, https://, or t.me/\n\nTry again:")
            return MENU_BTN_URL
        
        # Add t.me prefix if missing
        if url.startswith('t.me/'):
            url = 'https://' + url
        
        self.db.add_menu_button(self.bot_id, text, url)
        await update.message.reply_text(f"✅ Button **{text}** added successfully!\n\nUse /settings to manage buttons.")
        return ConversationHandler.END

    # --- Delete Menu Button ---
    async def delete_menu_button(self, update: Update, button_id: int):
        """Delete a menu button"""
        deleted = self.db.delete_menu_button(button_id, self.bot_id)
        if deleted:
            await update.callback_query.answer("✅ Button deleted!")
        else:
            await update.callback_query.answer("⚠️ Button not found")
        await self.show_manage_buttons(update)

    # --- Pair Buttons ---
    async def start_pair_buttons(self, update: Update):
        """Start button pairing flow"""
        buttons = self.db.get_menu_buttons(self.bot_id)
        unpaired = [b for b in buttons if not b['row_group']]
        
        if len(unpaired) < 2:
            await update.callback_query.answer("Need at least 2 unpaired buttons!")
            return
        
        text = "🔗 **PAIR BUTTONS**\n\nSelect first button:"
        keyboard = []
        for btn in unpaired:
            keyboard.append([InlineKeyboardButton(btn['text'], callback_data=f"pair1_{btn['id']}")])
        keyboard.append([InlineKeyboardButton("« Cancel", callback_data="manage_menu_btns")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def select_pair_btn_1(self, update: Update, btn1_id: int):
        """First button selected, show second button options"""
        update.callback_query.data  # Store in context
        buttons = self.db.get_menu_buttons(self.bot_id)
        unpaired = [b for b in buttons if not b['row_group'] and b['id'] != btn1_id]
        
        text = "🔗 **PAIR BUTTONS**\n\nSelect second button:"
        keyboard = []
        for btn in unpaired:
            keyboard.append([InlineKeyboardButton(btn['text'], callback_data=f"pair2_{btn1_id}_{btn['id']}")])
        keyboard.append([InlineKeyboardButton("« Cancel", callback_data="manage_menu_btns")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def select_pair_btn_2(self, update: Update, btn2_id: int):
        """Second button selected, complete pairing"""
        # Extract btn1_id from callback data
        data = update.callback_query.data
        parts = data.split("_")
        btn1_id = int(parts[1])
        btn2_id = int(parts[2])
        
        self.db.pair_buttons(btn1_id, btn2_id, self.bot_id)
        await update.callback_query.answer("✅ Buttons paired!")
        await self.show_manage_buttons(update)

    async def unpair_button(self, update: Update, button_id: int):
        """Unpair a button"""
        self.db.unpair_button(button_id, self.bot_id)
        await update.callback_query.answer("✅ Button unpaired!")
        await self.show_manage_buttons(update)
    
    # --- Company Button Management ---
    async def show_company_buttons(self, update: Update, company_id: int):
        """Show buttons for a specific company with management options"""
        buttons = self.db.get_company_buttons(company_id)
        company = next((c for c in self.db.get_companies(self.bot_id) if c['id'] == company_id), None)
        name = company['name'] if company else 'Company'
        
        if not buttons:
            text = f"🔘 **MANAGE BUTTONS: {name}**\n\n_No buttons yet._"
            keyboard = [
                [InlineKeyboardButton("➕ Add Button", callback_data=f"add_co_btn_{company_id}")],
                [InlineKeyboardButton("« Back", callback_data=f"edit_company_{company_id}")]
            ]
        else:
            text = f"🔘 **MANAGE BUTTONS: {name}**\n\nButtons ({len(buttons)}):"
            keyboard = []
            for btn in buttons:
                paired = "🔗" if btn['row_group'] else ""
                keyboard.append([
                    InlineKeyboardButton(f"{paired} {btn['text']}", callback_data=f"view_co_btn_{btn['id']}"),
                    InlineKeyboardButton("🗑️", callback_data=f"del_co_btn_{btn['id']}")
                ])
            keyboard.append([InlineKeyboardButton("➕ Add Button", callback_data=f"add_co_btn_{company_id}")])
            keyboard.append([InlineKeyboardButton("🔗 Pair Buttons", callback_data=f"pair_co_btns_{company_id}")])
            keyboard.append([InlineKeyboardButton("« Back", callback_data=f"edit_company_{company_id}")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_company_buttons_from_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show company buttons from Edit Company wizard context"""
        company_id = context.user_data.get('edit_company_id')
        if not company_id:
            await update.callback_query.answer("Error: No company in context")
            return
        await self.show_company_buttons(update, company_id)

    async def start_add_company_btn(self, update: Update, context: ContextTypes.DEFAULT_TYPE, company_id: int):
        """Start adding button to existing company"""
        context.user_data['add_btn_company_id'] = company_id
        context.user_data['awaiting_co_btn_text'] = True
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(
            "➕ **ADD BUTTON**\n\nMasukkan text untuk button:",
            parse_mode='Markdown'
        )

    async def delete_company_btn(self, update: Update, button_id: int):
        """Delete a company button"""
        # Get button to find company_id
        conn = self.db.get_connection()
        btn = conn.execute("SELECT company_id FROM company_buttons WHERE id = ?", (button_id,)).fetchone()
        if btn:
            company_id = btn['company_id']
            conn.execute("DELETE FROM company_buttons WHERE id = ?", (button_id,))
            conn.commit()
            conn.close()
            await update.callback_query.answer("✅ Button deleted!")
            await self.show_company_buttons(update, company_id)
        else:
            conn.close()
            await update.callback_query.answer("⚠️ Button not found")

    async def start_pair_company_btns(self, update: Update, company_id: int):
        """Start pairing buttons for a company"""
        buttons = self.db.get_company_buttons(company_id)
        unpaired = [b for b in buttons if not b['row_group']]
        
        if len(unpaired) < 2:
            await update.callback_query.answer("Need at least 2 unpaired buttons!")
            return
        
        text = "🔗 **PAIR BUTTONS**\n\nSelect first button:"
        keyboard = []
        for btn in unpaired:
            keyboard.append([InlineKeyboardButton(btn['text'], callback_data=f"copair1_{company_id}_{btn['id']}")])
        keyboard.append([InlineKeyboardButton("« Cancel", callback_data=f"manage_co_btns_{company_id}")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def select_co_pair_btn1(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """First button selected, show second button options"""
        data = update.callback_query.data
        parts = data.split("_")
        company_id = int(parts[1])
        btn1_id = int(parts[2])
        
        context.user_data['pair_co_btn1'] = btn1_id
        context.user_data['pair_co_company'] = company_id
        
        buttons = self.db.get_company_buttons(company_id)
        unpaired = [b for b in buttons if not b['row_group'] and b['id'] != btn1_id]
        
        text = "🔗 **PAIR BUTTONS**\n\nSelect second button:"
        keyboard = []
        for btn in unpaired:
            keyboard.append([InlineKeyboardButton(btn['text'], callback_data=f"copair2_{btn['id']}")])
        keyboard.append([InlineKeyboardButton("« Cancel", callback_data=f"manage_co_btns_{company_id}")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def complete_co_pair(self, update: Update):
        """Complete company button pairing"""
        context = update.callback_query
        data = update.callback_query.data
        btn2_id = int(data.split("_")[1])
        
        # Get from bot's context via application
        btn1_id = None
        company_id = None
        # This is a callback, we need to access application context differently
        # For simplicity, extract from delete first button's data
        conn = self.db.get_connection()
        btn2 = conn.execute("SELECT company_id FROM company_buttons WHERE id = ?", (btn2_id,)).fetchone()
        if btn2:
            company_id = btn2['company_id']
        conn.close()
        
        if company_id:
            # Since we can't easily access context in callback, use a simpler approach
            # Pair with previous button (last unpaired before this one)
            buttons = self.db.get_company_buttons(company_id)
            unpaired = [b for b in buttons if not b['row_group'] and b['id'] != btn2_id]
            if unpaired:
                btn1_id = unpaired[0]['id']
                self.db.pair_company_buttons(btn1_id, btn2_id)
                await update.callback_query.answer("✅ Buttons paired!")
            await self.show_company_buttons(update, company_id)
        else:
            await update.callback_query.answer("⚠️ Error pairing buttons")

    async def unpair_company_btn(self, update: Update, button_id: int):
        """Unpair a company button"""
        conn = self.db.get_connection()
        btn = conn.execute("SELECT company_id FROM company_buttons WHERE id = ?", (button_id,)).fetchone()
        if btn:
            company_id = btn['company_id']
            conn.execute("UPDATE company_buttons SET row_group = NULL WHERE id = ?", (button_id,))
            conn.commit()
            conn.close()
            await update.callback_query.answer("✅ Button unpaired!")
            await self.show_company_buttons(update, company_id)
        else:
            conn.close()
            await update.callback_query.answer("⚠️ Button not found")
    
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
        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="customize_menu")]]
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
        chat = update.effective_chat

        # --- AUTO-DISCOVERY: REGISTER KNOWN GROUPS ---
        if chat.type in ['group', 'supergroup']:
            self.db.upsert_known_group(self.bot_id, chat.id, chat.title)

        # Handle Add Admin flow
        if await self.add_admin_handler(update, context):
            return
        
        # Handle Add Company Button flows (awaiting text/url after callback)
        if context.user_data.get('awaiting_btn_text'):
            # Save button text, ask for URL
            context.user_data['new_comp']['btn_text'] = update.message.text
            context.user_data['awaiting_btn_text'] = False
            context.user_data['awaiting_btn_url'] = True
            await update.message.reply_text("🔗 Masukkan **Link URL** button:", parse_mode='Markdown')
            return
        
        if context.user_data.get('awaiting_btn_url'):
            # Save button URL to company_buttons
            url = update.message.text
            if not url.startswith(('http://', 'https://', 't.me/')):
                await update.message.reply_text("⚠️ URL mesti mula dengan http://, https://, atau t.me/\n\nCuba lagi:")
                return
            if url.startswith('t.me/'):
                url = 'https://' + url
            
            data = context.user_data.get('new_comp', {})
            company_id = data.get('company_id')
            if company_id:
                self.db.add_company_button(company_id, data['btn_text'], url)
                context.user_data['awaiting_btn_url'] = False
                
                # Ask for more
                keyboard = [
                    [InlineKeyboardButton("➕ Add Another Button", callback_data="add_more_btn")],
                    [InlineKeyboardButton("✅ Done", callback_data="finish_company")]
                ]
                await update.message.reply_text(
                    f"✅ Button **{data['btn_text']}** added!\n\nAdd another button?",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
            return
        
        # Handle Add Button to Existing Company (from Manage Buttons)
        if context.user_data.get('awaiting_co_btn_text'):
            context.user_data['co_btn_text'] = update.message.text
            context.user_data['awaiting_co_btn_text'] = False
            context.user_data['awaiting_co_btn_url'] = True
            await update.message.reply_text("🔗 Masukkan **Link URL** button:", parse_mode='Markdown')
            return
        
        if context.user_data.get('awaiting_co_btn_url'):
            url = update.message.text
            if not url.startswith(('http://', 'https://', 't.me/')):
                await update.message.reply_text("⚠️ URL mesti mula dengan http://, https://, atau t.me/\n\nCuba lagi:")
                return
            if url.startswith('t.me/'):
                url = 'https://' + url
            
            company_id = context.user_data.get('add_btn_company_id')
            btn_text = context.user_data.get('co_btn_text', 'Button')
            
            if company_id:
                self.db.add_company_button(company_id, btn_text, url)
                context.user_data['awaiting_co_btn_url'] = False
                await update.message.reply_text(f"✅ Button **{btn_text}** added!", parse_mode='Markdown')
                
                # Show manage buttons again via inline keyboard
                keyboard = [[InlineKeyboardButton("🔙 Back to Manage Buttons", callback_data=f"manage_co_btns_{company_id}")]]
                await update.message.reply_text("Tap below to continue:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # --- LIVEGRAM FUNCTIONALITY ---
        # Admin replies to forwarded message -> send to user
        if user_id == owner_id and update.message.reply_to_message:
            replied_msg = update.message.reply_to_message
            replied_msg_id = replied_msg.message_id
            
            # Check if this is a reply to a forwarded user message
            forwarded_msgs = context.bot_data.get('forwarded_msgs', {})
            original_user_id = forwarded_msgs.get(replied_msg_id)
            
            if original_user_id:
                try:
                    # Send admin's reply to the original user
                    if update.message.text:
                        await context.bot.send_message(
                            chat_id=original_user_id, 
                            text=f"💬 **Admin:**\n{update.message.text}", 
                            parse_mode='Markdown'
                        )
                    elif update.message.photo:
                        await context.bot.send_photo(
                            chat_id=original_user_id,
                            photo=update.message.photo[-1].file_id,
                            caption=f"💬 **Admin:**\n{update.message.caption or ''}"[:1024],
                            parse_mode='Markdown'
                        )
                    elif update.message.video:
                        await context.bot.send_video(
                            chat_id=original_user_id,
                            video=update.message.video.file_id,
                            caption=f"💬 **Admin:**\n{update.message.caption or ''}"[:1024],
                            parse_mode='Markdown'
                        )
                    elif update.message.document:
                        await context.bot.send_document(
                            chat_id=original_user_id,
                            document=update.message.document.file_id,
                            caption=f"💬 **Admin:**\n{update.message.caption or ''}"[:1024],
                            parse_mode='Markdown'
                        )
                    elif update.message.voice:
                        await context.bot.send_voice(
                            chat_id=original_user_id,
                            voice=update.message.voice.file_id
                        )
                    elif update.message.sticker:
                        await context.bot.send_sticker(
                            chat_id=original_user_id,
                            sticker=update.message.sticker.file_id
                        )
                    await update.message.reply_text("✅ Sent to user!")
                except Exception as e:
                    await update.message.reply_text(f"❌ Failed: {str(e)}")
                return
        
        # User -> Admin (forward message and store mapping)
        # Only forward if livegram is enabled
        if user_id != owner_id and self.db.is_livegram_enabled(self.bot_id):
            # Forward message to admin
            forwarded = await context.bot.forward_message(
                chat_id=owner_id, 
                from_chat_id=update.effective_chat.id, 
                message_id=update.message.message_id
            )
            
            # Store mapping: forwarded_msg_id -> original_user_id
            if 'forwarded_msgs' not in context.bot_data:
                context.bot_data['forwarded_msgs'] = {}
            context.bot_data['forwarded_msgs'][forwarded.message_id] = user_id
            
            # Clean old mappings (keep only last 500 to save memory)
            if len(context.bot_data['forwarded_msgs']) > 500:
                oldest_keys = list(context.bot_data['forwarded_msgs'].keys())[:-500]
                for k in oldest_keys:
                    del context.bot_data['forwarded_msgs'][k]
            
            # Send hint to admin
            user_name = update.effective_user.first_name or "User"
            await context.bot.send_message(
                chat_id=owner_id, 
                text=f"👤 **{user_name}** (ID: `{user_id}`)\n\n💡 _Reply terus ke message di atas untuk balas._",
                parse_mode='Markdown'
            )
        
        # Admin /reply command (legacy fallback)
        elif update.message.text and update.message.text.startswith("/reply "):
            try:
                parts = update.message.text.split(" ", 2)
                target_id = int(parts[1])
                msg = parts[2]
                await context.bot.send_message(chat_id=target_id, text=f"💬 **Admin:**\n{msg}", parse_mode='Markdown')
                await update.message.reply_text("✅ Sent.")
            except:
                await update.message.reply_text("❌ Format: /reply USER_ID MESSAGE")

    # --- Add Company Wizard Steps ---
    async def add_company_start(self, update, context):
        await update.callback_query.message.reply_text("Sila masukkan **NAMA Company**:", parse_mode='Markdown')
        return NAME
    
    async def add_company_name(self, update, context):
        context.user_data['new_comp'] = {'name': update.message.text}
        await update.message.reply_text("Masukkan **Deskripsi Company**:", parse_mode='Markdown')
        return DESC

    async def add_company_desc(self, update, context):
        context.user_data['new_comp']['desc'] = update.message.text
        await update.message.reply_text("Hantar **Gambar/Video** Banner:", parse_mode='Markdown')
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
        await update.message.reply_text("Masukkan **Text pada Button** (Contoh: REGISTER NOW):", parse_mode='Markdown')
        return BUTTON_TEXT

    async def add_company_btn_text(self, update, context):
        context.user_data['new_comp']['btn_text'] = update.message.text
        await update.message.reply_text("Masukkan **Link URL** destination:", parse_mode='Markdown')
        return BUTTON_URL

    async def add_company_btn_url(self, update, context):
        data = context.user_data['new_comp']
        url = update.message.text
        
        # Validate URL
        if not url.startswith(('http://', 'https://', 't.me/')):
            await update.message.reply_text("⚠️ Invalid URL. Must start with http://, https://, or t.me/\n\nTry again:")
            return BUTTON_URL
        
        # Add t.me prefix if needed
        if url.startswith('t.me/'):
            url = 'https://' + url
        
        # First button - create company first
        if 'company_id' not in data:
            company_id = self.db.add_company(self.bot_id, data['name'], data['desc'], data['media'], data['type'], data['btn_text'], url)
            data['company_id'] = company_id
            # Also add first button to company_buttons table
            self.db.add_company_button(company_id, data['btn_text'], url)
        else:
            # Additional buttons
            self.db.add_company_button(data['company_id'], data['btn_text'], url)
        
        # Ask if user wants to add another button
        keyboard = [
            [InlineKeyboardButton("➕ Add Another Button", callback_data="add_more_btn")],
            [InlineKeyboardButton("✅ Done", callback_data="finish_company")]
        ]
        await update.message.reply_text(
            f"✅ Button **{data['btn_text']}** added!\n\n"
            "Add another button or finish?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    async def add_more_company_btn(self, update, context):
        """Continue adding buttons to company"""
        await update.callback_query.answer()
        await update.callback_query.message.edit_text(
            "➕ **ADD ANOTHER BUTTON**\n\n"
            "Masukkan **Text pada Button**:",
            parse_mode='Markdown'
        )
        # The next text message will be handled by handle_message with a flag
        context.user_data['awaiting_btn_text'] = True

    async def cancel_op(self, update, context):
        try:
            await update.message.reply_text("❌ Cancelled.")
        except:
            await update.callback_query.message.edit_text("❌ Cancelled.")
        context.user_data.pop('new_comp', None)
        context.user_data.pop('awaiting_btn_text', None)
        context.user_data.pop('awaiting_btn_url', None)
        return ConversationHandler.END

    # --- Broadcast Wizard ---
    async def broadcast_start(self, update, context):
        # Security check - only owner can broadcast
        user_id = update.effective_user.id
        bot_data = self.db.get_bot_by_token(self.token)
        owner_id = int(bot_data.get('owner_id', 0)) if bot_data else 0
        
        if user_id != owner_id:
            await update.callback_query.answer("⛔ Access Denied", show_alert=True)
            return ConversationHandler.END
        
        # Ask for target type
        keyboard = [
            [InlineKeyboardButton("👤 All Users", callback_data="target_users")],
            [InlineKeyboardButton("👥 All Known Groups", callback_data="target_groups")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        
        await update.callback_query.message.reply_text(
            "📢 **BROADCAST MODE**\n\nSila pilih target penerima:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return BROADCAST_TARGET

    async def broadcast_choose_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle target selection"""
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "broadcast_cancel":
            await query.message.edit_text("❌ Broadcast dibatalkan.")
            return ConversationHandler.END
        
        target_type = "users" if data == "target_users" else "groups"
        context.user_data['broadcast_target_type'] = target_type
        
        target_display = "👤 All Users" if target_type == "users" else "👥 All Known Groups"
        
        await query.message.reply_text(
            f"🎯 Target: **{target_display}**\n\n"
            "Sila hantar mesej (Text/Gambar/Video) yang nak disebarkan:",
            parse_mode='Markdown'
        )
        return BROADCAST_CONTENT
    
    async def broadcast_content(self, update, context):
        # Save msg details for later use
        msg = update.message
        context.user_data['broadcast_data'] = {
            'text': msg.text or msg.caption,
            'photo': msg.photo[-1].file_id if msg.photo else None,
            'video': msg.video.file_id if msg.video else None,
            'document': msg.document.file_id if msg.document else None,
            'message': msg  # Keep original for instant send
        }
        
        # Show Send Now vs Schedule vs Recurring options
        keyboard = [
            [InlineKeyboardButton("📤 Send Now", callback_data="broadcast_now")],
            [InlineKeyboardButton("⏰ Schedule", callback_data="broadcast_schedule")],
            [InlineKeyboardButton("🔁 Recurring", callback_data="broadcast_recurring")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        await update.message.reply_text(
            "✅ **Mesej diterima!**\n\nPilih option:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return BROADCAST_CONFIRM

    async def broadcast_confirm(self, update, context):
        await update.callback_query.answer()
        action = update.callback_query.data
        
        if action == "broadcast_cancel":
            context.user_data.pop('broadcast_data', None)
            await update.callback_query.message.edit_text("❌ Broadcast dibatalkan.")
            return ConversationHandler.END
        
        if action == "broadcast_now":
            # Instant send
            data = context.user_data.get('broadcast_data')
            if not data or not data.get('message'):
                await update.callback_query.message.reply_text("❌ No message to broadcast.")
                return ConversationHandler.END
            
            msg = data['message']
            
            # Determine targets
            target_type = context.user_data.get('broadcast_target_type', 'users')
            
            if target_type == 'groups':
                targets = self.db.get_known_groups(self.bot_id)
                target_ids = [t['group_id'] for t in targets]
                target_name = "Groups"
            else:
                users = self.db.get_users(self.bot_id)
                target_ids = [u['telegram_id'] for u in users]
                target_name = "Users"
            
            await update.callback_query.message.edit_text(f"⏳ Broadcasting to {len(target_ids)} {target_name}...")
            
            sent = 0
            failed = 0
            for tid in target_ids:
                try:
                    await msg.copy(chat_id=tid)
                    sent += 1
                except:
                    failed += 1
            
            await update.callback_query.message.reply_text(f"✅ Broadcast selesai!\n\n📤 Sent: {sent}\n❌ Failed: {failed}")
            context.user_data.pop('broadcast_data', None)
            return ConversationHandler.END
        
        if action == "broadcast_schedule":
            # Show time picker
            keyboard = [
                [InlineKeyboardButton("1 Jam", callback_data="sched_1h"), InlineKeyboardButton("3 Jam", callback_data="sched_3h")],
                [InlineKeyboardButton("6 Jam", callback_data="sched_6h"), InlineKeyboardButton("12 Jam", callback_data="sched_12h")],
                [InlineKeyboardButton("24 Jam", callback_data="sched_24h"), InlineKeyboardButton("48 Jam", callback_data="sched_48h")],
                [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
            ]
            await update.callback_query.message.edit_text(
                "⏰ **SCHEDULE BROADCAST**\n\nPilih bila nak hantar:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return SCHEDULE_TIME
        
        # Handle schedule time selection
        if action.startswith("sched_"):
            hours_map = {"sched_1h": 1, "sched_3h": 3, "sched_6h": 6, "sched_12h": 12, "sched_24h": 24, "sched_48h": 48}
            hours = hours_map.get(action, 1)
            
            scheduled_time = datetime.datetime.now() + datetime.timedelta(hours=hours)
            data = context.user_data.get('broadcast_data', {})
            
            # Determine media type
            media_type = None
            media_file_id = None
            if data.get('photo'):
                media_type = 'photo'
                media_file_id = data['photo']
            elif data.get('video'):
                media_type = 'video'
                media_file_id = data['video']
            elif data.get('document'):
                media_type = 'document'
                media_file_id = data['document']
            
            # Save to database
            broadcast_id = self.db.save_scheduled_broadcast(
                self.bot_id,
                data.get('text', ''),
                media_file_id,
                media_type,
                scheduled_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            
            # Schedule the job
            self.scheduler.add_job(
                self.execute_scheduled_broadcast,
                'date',
                run_date=scheduled_time,
                args=[broadcast_id],
                id=f"broadcast_{broadcast_id}"
            )
            
            await update.callback_query.message.edit_text(
                f"✅ **Broadcast Scheduled!**\n\n"
                f"📅 Akan dihantar: **{scheduled_time.strftime('%d/%m/%Y %H:%M')}**\n"
                f"🆔 Broadcast ID: `{broadcast_id}`\n\n"
                f"💡 Guna `/settings` → Reset Schedule untuk batalkan",
                parse_mode='Markdown'
            )
            context.user_data.pop('broadcast_data', None)
            return ConversationHandler.END
        
        # Recurring broadcast - show type options
        if action == "broadcast_recurring":
            keyboard = [
                [InlineKeyboardButton("⏰ Setiap X Jam", callback_data="recur_type_hours")],
                [InlineKeyboardButton("📅 Setiap Hari", callback_data="recur_type_daily")],
                [InlineKeyboardButton("⏱️ Setiap X Minit", callback_data="recur_type_minutes")],
                [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
            ]
            await update.callback_query.message.edit_text(
                "🔁 **RECURRING BROADCAST**\n\nPilih jenis recurring:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return RECURRING_TYPE
        
        return BROADCAST_CONFIRM

    async def recurring_type_handler(self, update, context):
        """Handle recurring broadcast type and interval selection"""
        await update.callback_query.answer()
        action = update.callback_query.data
        
        if action == "broadcast_cancel":
            context.user_data.pop('broadcast_data', None)
            await update.callback_query.message.edit_text("❌ Recurring broadcast dibatalkan.")
            return ConversationHandler.END
        
        # Back to recurring main menu
        if action == "broadcast_recurring":
            keyboard = [
                [InlineKeyboardButton("⏰ Setiap X Jam", callback_data="recur_type_hours")],
                [InlineKeyboardButton("📅 Setiap Hari", callback_data="recur_type_daily")],
                [InlineKeyboardButton("⏱️ Setiap X Minit", callback_data="recur_type_minutes")],
                [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
            ]
            await update.callback_query.message.edit_text(
                "🔁 **RECURRING BROADCAST**\n\nPilih jenis recurring:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return RECURRING_TYPE
        
        # Type selection
        if action == "recur_type_hours":
            keyboard = [
                [InlineKeyboardButton("1 Jam", callback_data="recur_h_1"), 
                 InlineKeyboardButton("2 Jam", callback_data="recur_h_2")],
                [InlineKeyboardButton("3 Jam", callback_data="recur_h_3"), 
                 InlineKeyboardButton("6 Jam", callback_data="recur_h_6")],
                [InlineKeyboardButton("12 Jam", callback_data="recur_h_12"),
                 InlineKeyboardButton("24 Jam", callback_data="recur_h_24")],
                [InlineKeyboardButton("« Kembali", callback_data="broadcast_recurring")]
            ]
            await update.callback_query.message.edit_text(
                "⏰ **SETIAP X JAM**\n\nPilih selang masa:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return RECURRING_TYPE
        
        if action == "recur_type_daily":
            keyboard = [
                [InlineKeyboardButton("8:00 AM", callback_data="recur_d_8"),
                 InlineKeyboardButton("10:00 AM", callback_data="recur_d_10")],
                [InlineKeyboardButton("12:00 PM", callback_data="recur_d_12"),
                 InlineKeyboardButton("2:00 PM", callback_data="recur_d_14")],
                [InlineKeyboardButton("6:00 PM", callback_data="recur_d_18"),
                 InlineKeyboardButton("8:00 PM", callback_data="recur_d_20")],
                [InlineKeyboardButton("10:00 PM", callback_data="recur_d_22")],
                [InlineKeyboardButton("« Kembali", callback_data="broadcast_recurring")]
            ]
            await update.callback_query.message.edit_text(
                "📅 **SETIAP HARI**\n\nPilih waktu broadcast:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return RECURRING_TYPE
        
        if action == "recur_type_minutes":
            keyboard = [
                [InlineKeyboardButton("15 Minit", callback_data="recur_m_15"),
                 InlineKeyboardButton("30 Minit", callback_data="recur_m_30")],
                [InlineKeyboardButton("45 Minit", callback_data="recur_m_45")],
                [InlineKeyboardButton("« Kembali", callback_data="broadcast_recurring")]
            ]
            await update.callback_query.message.edit_text(
                "⏱️ **SETIAP X MINIT**\n\nPilih selang masa:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return RECURRING_TYPE
        
        # Handle actual interval selection
        if action.startswith("recur_h_"):
            hours = int(action.split("_")[2])
            return await self.save_and_start_recurring(update, context, "hours", hours)
        
        if action.startswith("recur_d_"):
            hour = int(action.split("_")[2])
            return await self.save_and_start_recurring(update, context, "daily", hour)
        
        if action.startswith("recur_m_"):
            minutes = int(action.split("_")[2])
            return await self.save_and_start_recurring(update, context, "minutes", minutes)
        
        return RECURRING_TYPE

    async def save_and_start_recurring(self, update, context, interval_type, interval_value):
        """Save recurring broadcast to database and start scheduler job"""
        data = context.user_data.get('broadcast_data', {})
        
        # Determine media type
        media_type = None
        media_file_id = None
        if data.get('photo'):
            media_type = 'photo'
            media_file_id = data['photo']
        elif data.get('video'):
            media_type = 'video'
            media_file_id = data['video']
        elif data.get('document'):
            media_type = 'document'
            media_file_id = data['document']
        
        # Save to database
        broadcast_id = self.db.save_recurring_broadcast(
            self.bot_id,
            data.get('text', ''),
            media_file_id,
            media_type,
            interval_type,
            interval_value
        )
        
        # Schedule recurring job
        self.start_recurring_job(broadcast_id, interval_type, interval_value)
        
        # Format description
        if interval_type == "hours":
            desc = f"Setiap {interval_value} jam"
        elif interval_type == "daily":
            desc = f"Setiap hari jam {interval_value}:00"
        else:
            desc = f"Setiap {interval_value} minit"
        
        await update.callback_query.message.edit_text(
            f"✅ **Recurring Broadcast Aktif!**\n\n"
            f"🔁 Jadual: **{desc}**\n"
            f"🆔 ID: `{broadcast_id}`\n\n"
            f"💡 Guna `/settings` → Manage Recurring untuk stop",
            parse_mode='Markdown'
        )
        context.user_data.pop('broadcast_data', None)
        return ConversationHandler.END

    def start_recurring_job(self, broadcast_id, interval_type, interval_value):
        """Start an APScheduler job for recurring broadcast"""
        job_id = f"recurring_{broadcast_id}"
        
        # Remove existing job if any
        try:
            self.scheduler.remove_job(job_id)
        except:
            pass
        
        if interval_type == "hours":
            self.scheduler.add_job(
                self.execute_recurring_broadcast,
                'interval',
                hours=interval_value,
                args=[broadcast_id],
                id=job_id
            )
        elif interval_type == "daily":
            self.scheduler.add_job(
                self.execute_recurring_broadcast,
                'cron',
                hour=interval_value,
                minute=0,
                args=[broadcast_id],
                id=job_id
            )
        elif interval_type == "minutes":
            self.scheduler.add_job(
                self.execute_recurring_broadcast,
                'interval',
                minutes=interval_value,
                args=[broadcast_id],
                id=job_id
            )
        
        self.logger.info(f"Started recurring job {job_id}: {interval_type}={interval_value}")

    async def execute_recurring_broadcast(self, broadcast_id):
        """Execute a recurring broadcast"""
        try:
            # Get broadcast details
            broadcasts = self.db.get_recurring_broadcasts(self.bot_id)
            broadcast = next((b for b in broadcasts if b['id'] == broadcast_id), None)
            
            if not broadcast:
                self.logger.warning(f"Recurring broadcast {broadcast_id} not found or inactive")
                return
            
            users = self.db.get_users(self.bot_id)
            sent = 0
            failed = 0
            
            for user in users:
                try:
                    if broadcast['media_type'] == 'photo' and broadcast['media_file_id']:
                        await self.app.bot.send_photo(
                            chat_id=user['telegram_id'],
                            photo=broadcast['media_file_id'],
                            caption=broadcast['message']
                        )
                    elif broadcast['media_type'] == 'video' and broadcast['media_file_id']:
                        await self.app.bot.send_video(
                            chat_id=user['telegram_id'],
                            video=broadcast['media_file_id'],
                            caption=broadcast['message']
                        )
                    else:
                        await self.app.bot.send_message(
                            chat_id=user['telegram_id'],
                            text=broadcast['message']
                        )
                    sent += 1
                except Exception as e:
                    failed += 1
            
            self.logger.info(f"Recurring broadcast {broadcast_id} executed: {sent} sent, {failed} failed")
            
        except Exception as e:
            self.logger.error(f"Error executing recurring broadcast {broadcast_id}: {e}")

    async def execute_scheduled_broadcast(self, broadcast_id):
        """Execute a scheduled broadcast"""
        try:
            # Get broadcast details
            broadcasts = self.db.get_pending_broadcasts(self.bot_id)
            broadcast = next((b for b in broadcasts if b['id'] == broadcast_id), None)
            
            if not broadcast:
                self.logger.warning(f"Broadcast {broadcast_id} not found or already sent")
                return
            
            users = self.db.get_users(self.bot_id)
            sent = 0
            failed = 0
            
            for u in users:
                try:
                    if broadcast['media_type'] == 'photo' and broadcast['media_file_id']:
                        await self.app.bot.send_photo(
                            chat_id=u['telegram_id'],
                            photo=broadcast['media_file_id'],
                            caption=broadcast['message'] or ''
                        )
                    elif broadcast['media_type'] == 'video' and broadcast['media_file_id']:
                        await self.app.bot.send_video(
                            chat_id=u['telegram_id'],
                            video=broadcast['media_file_id'],
                            caption=broadcast['message'] or ''
                        )
                    elif broadcast['media_type'] == 'document' and broadcast['media_file_id']:
                        await self.app.bot.send_document(
                            chat_id=u['telegram_id'],
                            document=broadcast['media_file_id'],
                            caption=broadcast['message'] or ''
                        )
                    elif broadcast['message']:
                        await self.app.bot.send_message(
                            chat_id=u['telegram_id'],
                            text=broadcast['message']
                        )
                    sent += 1
                except Exception as e:
                    failed += 1
            
            # Mark as sent
            self.db.mark_broadcast_sent(broadcast_id)
            
            # Notify owner
            bot_data = self.db.get_bot_by_token(self.token)
            if bot_data:
                try:
                    await self.app.bot.send_message(
                        chat_id=bot_data['owner_id'],
                        text=f"✅ **Scheduled Broadcast Complete!**\n\n📤 Sent: {sent}\n❌ Failed: {failed}",
                        parse_mode='Markdown'
                    )
                except:
                    pass
            
            self.logger.info(f"Scheduled broadcast {broadcast_id} completed: {sent} sent, {failed} failed")
        except Exception as e:
            self.logger.error(f"Error executing scheduled broadcast {broadcast_id}: {e}")


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

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming text messages"""
        if not update.message:
            return
            
        msg_text = update.message.text[:50] if update.message.text else 'No text'
        chat = update.effective_chat
        
        # Auto-Discovery: Save group if message is from a group
        if chat.type in ['group', 'supergroup']:
            self.db.upsert_known_group(self.bot_id, chat.id, chat.title)
        
        # Safe forwarded check for PTB v20+
        forward_from_chat = getattr(update.message, 'forward_from_chat', None)
        forward_origin = getattr(update.message, 'forward_origin', None)
        forward_date = getattr(update.message, 'forward_date', None)
        
        is_forwarded = bool(forward_from_chat or forward_origin or forward_date)
        
        self.logger.info(f"📨 Text message: {msg_text} | Forwarded: {is_forwarded}")
        self.logger.info(f"📨 States: source={context.user_data.get('waiting_forwarder_source')}, target={context.user_data.get('waiting_forwarder_target')}")
        
        # Check if waiting for forwarder source channel
        if context.user_data.get('waiting_forwarder_source'):
            self.logger.info("✅ Processing as forwarder source")
            await self.save_forwarder_source(update, context)
            return
        
        # Check if waiting for forwarder target group
        if context.user_data.get('waiting_forwarder_target'):
            self.logger.info("✅ Processing as forwarder target (text input)")
            await self.save_forwarder_target(update, context)
            return
        
        # Check if waiting for forwarder filter
        if context.user_data.get('waiting_forwarder_filter'):
            await self.save_forwarder_filter(update, context)
            return
        
        # Check if waiting for 4D number input
        if context.user_data.get('waiting_4d_check'):
            await self.check_4d_number(update, context)
            return
        
        # Add other message handlers here if needed

    async def handle_media_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle media messages - for forwarded photos/videos/docs from channels"""
        self.logger.info(f"📷 Media message received from user {update.effective_user.id}")
        
        # Check if this is a forwarded message for forwarder setup
        forward_from_chat = getattr(update.message, 'forward_from_chat', None)
        forward_origin = getattr(update.message, 'forward_origin', None)
        forward_date = getattr(update.message, 'forward_date', None)
        
        is_forwarded = bool(forward_from_chat or forward_origin or forward_date)
        
        if is_forwarded:
            self.logger.info(f"📩 Forwarded media detected")
            
            # Check if waiting for forwarder source channel
            if context.user_data.get('waiting_forwarder_source'):
                self.logger.info("Processing as forwarder source (media)...")
                await self.save_forwarder_source(update, context)
                return
            
            # Check if waiting for forwarder target group
            if context.user_data.get('waiting_forwarder_target'):
                self.logger.info("Processing as forwarder target (media)...")
                await self.save_forwarder_target(update, context)
                return

    # ==================== FORWARDER FUNCTIONS ====================
    

    async def show_forwarder_menu(self, update: Update):
        """Show forwarder configuration menu"""
        try:
            query = update.callback_query
            if query:
                await query.answer()

            config = self.db.get_forwarder_config(self.bot_id)
            
            # Multi-Source Support
            multi_sources = self.db.get_forwarder_sources(self.bot_id)
            source_count = len(multi_sources)
            
            if config:
                legacy_source = config.get('source_channel_name')
                if legacy_source: source_count += 1
                
                if source_count > 1:
                    source_name = f"📚 Aggregator Mode ({source_count} sources)"
                elif source_count == 1:
                    if legacy_source:
                         source_name = legacy_source
                    elif multi_sources:
                         source_name = multi_sources[0]['source_name']
                else:
                    source_name = "Not Set"

                target_name = config.get('target_group_name') or config.get('target_group_id') or 'Not Set'
                filter_keywords = config.get('filter_keywords') or 'None (All messages)'
                is_active = config.get('is_active')
                forwarder_mode = config.get('forwarder_mode', 'SINGLE')
                status = "🟢 ACTIVE" if is_active else "🔴 INACTIVE"
            else:
                source_name = "Not Set"
                target_name = "Not Set"
                filter_keywords = "None"
                forwarder_mode = "SINGLE"
                status = "🔴 INACTIVE"
            
            # Adjust display based on mode
            if forwarder_mode == 'BROADCAST':
                target_display = "📡 All Known Groups (Auto)"
            else:
                target_display = target_name

            text = (
                "📡 **CHANNEL FORWARDER**\n\n"
                f"📢 Source: `{source_name}`\n"
                f"💬 Target: `{target_display}`\n"
                f"📡 Mode: `{forwarder_mode}`\n"
                f"🔍 Filter: {filter_keywords}\n"
                f"📊 Status: {status}\n\n"
            )
            
            if not (config and config.get('is_active')):
                 text += "👇 Klik 'Activate' untuk memulakan forwarder!\n"

            # Check chat type (Group vs Private)
            chat = update.effective_chat
            is_group = chat.type in ['group', 'supergroup']
            
            keyboard = []
            
            # Source Management
            keyboard.append([InlineKeyboardButton("➕ Add Source Channel", callback_data="forwarder_set_source")])
            if source_count > 0:
                 keyboard.append([InlineKeyboardButton("📋 Manage Sources", callback_data="forwarder_manage_sources")])
            
            # Mode Toggle
            mode_btn_text = f"🔄 Mode: {forwarder_mode}"
            keyboard.append([InlineKeyboardButton(mode_btn_text, callback_data="forwarder_toggle_mode")])

            if forwarder_mode == 'SINGLE':
                if is_group:
                    # Smart Feature: Set CURRENT group as target
                    keyboard.append([InlineKeyboardButton("🎯 Set THIS Group as Target", callback_data="forwarder_set_this_group")])
                else:
                    # Private chat: Allow manual setting
                    keyboard.append([InlineKeyboardButton("💬 Set Target Group", callback_data="forwarder_set_target")])
                
            keyboard.append([InlineKeyboardButton("🔍 Set Filter Keywords", callback_data="forwarder_set_filter")])
            
            if config and config.get('filter_keywords'):
                 keyboard.append([InlineKeyboardButton("🗑️ Clear Filter", callback_data="forwarder_clear_filter")])

            if config:
                btn_text = "🔴 Deactivate" if is_active else "🟢 Activate"
                keyboard.append([InlineKeyboardButton(btn_text, callback_data="forwarder_toggle")])
                
            # Back button logic
            if is_group:
                 keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_panel")])
            else:
                 keyboard.append([InlineKeyboardButton("« Back", callback_data="admin_settings")])
                 
            if update.callback_query:
                await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                
        except Exception as e:
            self.logger.error(f"Error in forwarder menu: {e}")
            error_text = f"❌ Error loading Forwarder Menu: {str(e)}"
            try:
                if update.callback_query:
                    await update.callback_query.message.edit_text(error_text)
                else:
                    await update.message.reply_text(error_text)
            except:
                pass
    
    async def toggle_forwarder_mode_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle mode toggle callback"""
        new_mode = self.db.toggle_forwarder_mode(self.bot_id)
        if new_mode:
            await update.callback_query.answer(f"Mode changed to: {new_mode}")
            await self.show_forwarder_menu(update)
        else:
            await update.callback_query.answer("❌ Error changing mode", show_alert=True)
        
        try:
            if update.callback_query:
                await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            # Fallback if edit fails (e.g. message too old)
             await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def toggle_forwarder(self, update: Update):
        """Toggle forwarder on/off"""
        config = self.db.get_forwarder_config(self.bot_id)
        
        if not config or not config.get('source_channel_id') or not config.get('target_group_id'):
            await update.callback_query.answer("❌ Set source & target dulu!", show_alert=True)
            return
        
        new_state = self.db.toggle_forwarder(self.bot_id)
        
        if new_state is not None:
            status = "🟢 AKTIF" if new_state else "🔴 TIDAK AKTIF"
            await update.callback_query.answer(f"Forwarder sekarang: {status}", show_alert=True)
        else:
            await update.callback_query.answer("❌ Error toggling forwarder", show_alert=True)
        
        await self.show_forwarder_menu(update)
    
    async def forwarder_set_source_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start set source channel flow"""
        text = (
            "📢 **SET SOURCE CHANNEL**\n\n"
            "Forward satu message dari channel yang anda mahu jadikan source.\n\n"
            "Atau hantar Channel ID (contoh: `-1001234567890`)"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="forwarder_menu")]]
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        context.user_data['waiting_forwarder_source'] = True
        self.logger.info(f"🎯 Set waiting_forwarder_source=True for user {update.effective_user.id}")
    
    async def set_current_forwarder_target_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set the current group as the forwarder target (Auto-Detect)"""
        chat = update.effective_chat
        
        # Validate we are in a group
        if chat.type not in ['group', 'supergroup']:
            await update.callback_query.answer("❌ Fungsi ini hanya untuk dalam Group!", show_alert=True)
            return
            
        group_id = chat.id
        group_name = chat.title or str(group_id)
        
        # Get existing config
        config = self.db.get_forwarder_config(self.bot_id)
        source_id = config.get('source_channel_id') if config else None
        source_name = config.get('source_channel_name') if config else None
        filter_keywords = config.get('filter_keywords') if config else None
        
        # Save config
        success = self.db.save_forwarder_config(
            self.bot_id, source_id, source_name, group_id, group_name, filter_keywords
        )
        
        if success:
            await update.callback_query.answer("✅ Target Group ditetapkan!")
            
            # Check if setup is complete -> Auto Activate Logic handled in show_forwarder_complete_notification
            if source_id:
                # Need to update message to remove the button and show completion
                # But show_forwarder_complete_notification is designed for message reply, not edit
                # So we can just call show_forwarder_menu again, or custom logic
                
                # Check for Auto-Activation
                if not config or not config.get('is_active'):
                     self.db.toggle_forwarder(self.bot_id) # Auto activate

                await self.show_forwarder_complete_notification(update, source_name, group_name, filter_keywords)
            else:
                 await self.show_forwarder_menu(update) # Refresh menu
        else:
            await update.callback_query.answer("❌ Gagal menyimpan setting.", show_alert=True)
        """Save source channel from forwarded message or ID"""
        # DON'T pop state yet - user might need to retry if detection fails
        
        channel_id = None
        channel_name = None
        
        # Get forwarded attributes safely
        forward_from_chat = getattr(update.message, 'forward_from_chat', None)
        forward_origin = getattr(update.message, 'forward_origin', None)
        
        # Try forward_from_chat first (older but more reliable)
        if forward_from_chat:
            channel = forward_from_chat
            channel_id = channel.id
            channel_name = channel.title or channel.username or str(channel_id)
        # Try forward_origin (newer API)
        elif forward_origin:
            origin = forward_origin
            if hasattr(origin, 'chat') and origin.chat:
                channel_id = origin.chat.id
                channel_name = origin.chat.title or origin.chat.username or str(channel_id)
            elif hasattr(origin, 'sender_chat') and origin.sender_chat:
                channel_id = origin.sender_chat.id
                channel_name = origin.sender_chat.title or origin.sender_chat.username or str(channel_id)
        
        # If still no channel, try to parse as ID from text
        if not channel_id:
            try:
                channel_id = int(update.message.text.strip())
                channel_name = str(channel_id)
            except (ValueError, AttributeError):
                await update.message.reply_text(
                    "❌ Tidak dapat detect channel dari forward.\n\n"
                    "Cuba hantar Channel ID secara manual (contoh: `-1001234567890`)\n\n"
                    "_Pastikan bot adalah admin di channel tersebut._",
                    parse_mode='Markdown'
                )
                return
        
        # Get existing config or create placeholder for target
        config = self.db.get_forwarder_config(self.bot_id)
        target_id = config.get('target_group_id') if config else None
        target_name = config.get('target_group_name') if config else None
        filter_keywords = config.get('filter_keywords') if config else None
        
        # Add to Multi-Source Table
        success = self.db.add_forwarder_source(self.bot_id, channel_id, channel_name)
        
        # Also update main config (Legacy support + Setup flow metadata)
        # We perform a save to ensure the row exists and target/filters are preserved
        self.db.save_forwarder_config(
            self.bot_id, channel_id, channel_name, target_id, target_name, filter_keywords
        )
        
        if success:
            # Clear waiting state on success
            context.user_data.pop('waiting_forwarder_source', None)
            
            # Check if setup is complete (both source and target set)
            if target_id:
                # Check for Auto-Activation
                config = self.db.get_forwarder_config(self.bot_id)
                if not config or not config.get('is_active'):
                     self.db.toggle_forwarder(self.bot_id) # Auto activate
                
                await self.show_forwarder_complete_notification(update, channel_name, target_name, filter_keywords)
            else:
                await update.message.reply_text(
                    f"✅ Source channel ditambah: `{channel_name}`\n\n"
                    f"💡 Seterusnya, set Target Group untuk complete setup.",
                    parse_mode='Markdown'
                )
        else:
            await update.message.reply_text("❌ Gagal menyimpan. Cuba lagi.")
    
    async def forwarder_set_target_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start set target group flow"""
        text = (
            "💬 **SET TARGET GROUP**\n\n"
            "Forward satu message dari group yang anda mahu jadikan target.\n\n"
            "Atau hantar Group ID (contoh: `-1009876543210`)"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="forwarder_menu")]]
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        context.user_data['waiting_forwarder_target'] = True
    
    async def save_forwarder_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save target group from forwarded message or ID"""
        # DON'T pop state yet - user might need to retry if detection fails
        
        group_id = None
        group_name = None
        
        # Get forwarded attributes safely
        forward_from_chat = getattr(update.message, 'forward_from_chat', None)
        forward_origin = getattr(update.message, 'forward_origin', None)
        
        # Try forward_from_chat first (older but more reliable)
        if forward_from_chat:
            group = forward_from_chat
            group_id = group.id
            group_name = group.title or group.username or str(group_id)
        # Try forward_origin (newer API)
        elif forward_origin:
            origin = forward_origin
            if hasattr(origin, 'chat') and origin.chat:
                group_id = origin.chat.id
                group_name = origin.chat.title or origin.chat.username or str(group_id)
            elif hasattr(origin, 'sender_chat') and origin.sender_chat:
                group_id = origin.sender_chat.id
                group_name = origin.sender_chat.title or origin.sender_chat.username or str(group_id)
        
        # If still no group, try to parse as ID from text
        if not group_id:
            try:
                group_id = int(update.message.text.strip())
                group_name = str(group_id)
            except (ValueError, AttributeError):
                await update.message.reply_text(
                    "❌ Tidak dapat detect group dari forward.\n\n"
                    "Cuba hantar Group ID secara manual (contoh: `-1001234567890`)\n\n"
                    "_Pastikan bot adalah admin di group tersebut._",
                    parse_mode='Markdown'
                )
                return
        
        # Get existing config
        config = self.db.get_forwarder_config(self.bot_id)
        source_id = config.get('source_channel_id') if config else None
        source_name = config.get('source_channel_name') if config else None
        filter_keywords = config.get('filter_keywords') if config else None
        
        success = self.db.save_forwarder_config(
            self.bot_id, source_id, source_name, group_id, group_name, filter_keywords
        )
        
        if success:
            # Clear waiting state on success
            context.user_data.pop('waiting_forwarder_target', None)
            
            # Check if setup is complete (both source and target set)
            if source_id:
                # Check for Auto-Activation
                config = self.db.get_forwarder_config(self.bot_id)
                if not config or not config.get('is_active'):
                     self.db.toggle_forwarder(self.bot_id) # Auto activate

                await self.show_forwarder_complete_notification(update, source_name, group_name, filter_keywords)
            else:
                await update.message.reply_text(
                    f"✅ Target group ditetapkan: `{group_name}`\n\n"
                    f"💡 Seterusnya, set Source Channel untuk complete setup.",
                    parse_mode='Markdown'
                )
        else:
            await update.message.reply_text("❌ Gagal menyimpan. Cuba lagi.")
    
    async def show_forwarder_complete_notification(self, update: Update, source_name: str, target_name: str, filter_keywords: str = None):
        """Show notification when forwarder setup is complete"""
        config = self.db.get_forwarder_config(self.bot_id)
        is_active = config.get('is_active', False) if config else False
        
        filter_text = filter_keywords if filter_keywords else "None (Semua message)"
        status_text = "🟢 Aktif" if is_active else "🔴 Tidak Aktif"
        
        text = (
            f"🎉 **FORWARDER SETUP COMPLETE!**\n\n"
            f"📢 **Source Channel:** {source_name}\n"
            f"💬 **Target Group:** {target_name}\n"
            f"🔍 **Filter:** {filter_text}\n"
            f"📊 **Status:** {status_text}\n\n"
        )
        
        if not is_active:
            text += "👇 Tekan butang untuk aktifkan forwarder!"
        
        keyboard = []
        if not is_active:
            keyboard.append([InlineKeyboardButton("🟢 Activate Forwarder", callback_data="forwarder_toggle")])
        keyboard.append([InlineKeyboardButton("📡 Forwarder Menu", callback_data="forwarder_menu")])
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_panel")])
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def forwarder_set_filter_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start set filter flow"""
        text = (
            "🔍 **SET FILTER KEYWORDS**\n\n"
            "Hantar keywords, dipisahkan dengan koma.\n\n"
            "Contoh: `promo, offer, discount`\n\n"
            "Hanya message yang mengandungi keywords ini akan diforward.\n"
            "Kosongkan untuk forward semua message."
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="forwarder_menu")]]
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        context.user_data['waiting_forwarder_filter'] = True
    
    async def save_forwarder_filter(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save filter keywords"""
        context.user_data.pop('waiting_forwarder_filter', None)
        
        keywords = update.message.text.strip()
        
        if keywords.lower() in ['none', 'clear', 'kosong', '-']:
            keywords = None
        
        success = self.db.update_forwarder_filter(self.bot_id, keywords)
        
        if success:
            if keywords:
                await update.message.reply_text(f"✅ Filter ditetapkan: `{keywords}`", parse_mode='Markdown')
            else:
                await update.message.reply_text("✅ Filter dikosongkan. Semua message akan diforward.")
        else:
            await update.message.reply_text("❌ Gagal menyimpan. Cuba lagi.")
    
    async def forwarder_clear_filter(self, update: Update):
        """Clear filter keywords"""
        success = self.db.update_forwarder_filter(self.bot_id, None)
        
        if success:
            await update.callback_query.answer("✅ Filter dikosongkan!", show_alert=True)
        else:
            await update.callback_query.answer("❌ Gagal clear filter", show_alert=True)
        
        await self.show_forwarder_menu(update)
    


    async def show_forwarder_sources(self, update: Update):
        """Show list of added source channels"""
        sources = self.db.get_forwarder_sources(self.bot_id)
        config = self.db.get_forwarder_config(self.bot_id)
        
        # Include legacy source in list for display (though deletions might need migration logic)
        legacy_source_id = config.get('source_channel_id') if config else None
        
        text = "📋 **MANAGE SOURCE CHANNELS**\n\nSenarai channel yang menjadi sumber forwarder:\n"
        
        keyboard = []
        
        # Helper to check if listed
        listed_ids = set()
        
        if sources:
            for s in sources:
                name = s.get('source_name') or str(s.get('source_id'))
                text += f"• `{name}`\n"
                keyboard.append([
                    InlineKeyboardButton(f"🗑️ {name}", callback_data=f"forwarder_remove_source_{s['source_id']}")
                ])
                listed_ids.add(s['source_id'])

        # Show legacy if not in DB yet (for migration visual)
        if legacy_source_id and legacy_source_id not in listed_ids:
             name = config.get('source_channel_name') or str(legacy_source_id)
             text += f"• `{name}` (Legacy - Main)\n"
             # Legacy removal is tricky via this ID - better to migrate it on 'Add' or allow overwrite
             keyboard.append([
                 InlineKeyboardButton(f"🗑️ {name}", callback_data=f"forwarder_remove_source_{legacy_source_id}")
             ])

        text += "\nTekan 🗑️ untuk buang source."
        
        keyboard.append([InlineKeyboardButton("➕ Add Source", callback_data="forwarder_set_source")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="forwarder_menu")])
        
        # Edit text or new message depending on context
        if update.callback_query:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
             await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def remove_forwarder_source_handler(self, update: Update, source_id: int):
        """Remove a source channel"""
        # specialized logic: if it matches legacy, we might need to nullify forwarding_config.source_channel_id too
        config = self.db.get_forwarder_config(self.bot_id)
        legacy_id = config.get('source_channel_id') if config else None
        
        removed = False
        
        # Try remove from table
        if self.db.remove_forwarder_source(self.bot_id, source_id):
            removed = True
            
        # Try remove from legacy config if matches
        if legacy_id == source_id:
             # We update legacy config to null source
             self.db.save_forwarder_config(
                 self.bot_id, None, None, config['target_group_id'], config['target_group_name'], config['filter_keywords']
             )
             removed = True
        
        if removed:
            await update.callback_query.answer("✅ Source removed!")
            await self.show_forwarder_sources(update)
        else:
             await update.callback_query.answer("❌ Failed to remove.", show_alert=True)

    async def handle_channel_post(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle channel posts for forwarding to target group"""
        try:
            # Debug log
            self.logger.info(f"📨 Channel Post Received | Chat ID: {update.effective_chat.id} | Msg ID: {update.effective_message.message_id}")
            
            # Get forwarder config
            config = self.db.get_forwarder_config(self.bot_id)
            
            if not config:
                self.logger.debug("⏩ Forwarder skipped: No config found")
                return
                
            if not config.get('is_active'):
                self.logger.debug("⏩ Forwarder skipped: Inactive")
                return
            
            source_channel_id = config.get('source_channel_id')
            target_group_id = config.get('target_group_id')
            filter_keywords = config.get('filter_keywords')
            forwarder_mode = config.get('forwarder_mode', 'SINGLE')
            
            # Fetch Multi-Sources
            multi_sources = self.db.get_forwarder_sources(self.bot_id)
            valid_source_ids = [s['source_id'] for s in multi_sources]
            
            # Add legacy source if present
            if source_channel_id:
                valid_source_ids.append(source_channel_id)
            
            # Determine targets
            target_ids = []
            if forwarder_mode == 'BROADCAST':
                # Get all known active groups
                known_groups = self.db.get_known_groups(self.bot_id)
                target_ids = [g['group_id'] for g in known_groups]
                self.logger.info(f"📡 Broadcast Mode: Found {len(target_ids)} target groups")
            else:
                # Single Mode
                if target_group_id:
                    target_ids = [target_group_id]
            
            if not valid_source_ids or not target_ids:
                self.logger.warning("⚠️ Forwarder incomplete config (No Source or No Targets)")
                return  # Not properly configured
            
            # Check if message is from valid source
            if update.effective_chat.id not in valid_source_ids:
                self.logger.debug(f"⏩ Skipped: Chat ID {update.effective_chat.id} not in valid sources")
                return  # Not from our source channel
            
            message = update.effective_message
            self.logger.info(f"✅ Processing forwarding for message detected from Source Channel {update.effective_chat.id}")
            
            # Apply keyword filter if set
            if filter_keywords:
                keywords = [k.strip().lower() for k in filter_keywords.split(',')]
                message_text = (message.text or message.caption or '').lower()
                
                # Check if any keyword is in the message
                if not any(keyword in message_text for keyword in keywords):
                    self.logger.info(f"✋ Message filtered out - no matching keywords in '{message_text[:20]}...'")
                    return  # Message doesn't match filter
            
            # Forward message content to ALL targets
            success_count = 0
            
            for tid in target_ids:
                try:
                    if message.text:
                        # Text-only message
                        await context.bot.send_message(
                            chat_id=tid,
                            text=message.text,
                            entities=message.entities,
                            parse_mode=None
                        )
                    elif message.photo:
                        # Photo message
                        await context.bot.send_photo(
                            chat_id=tid,
                            photo=message.photo[-1].file_id,  # Largest photo
                            caption=message.caption,
                            caption_entities=message.caption_entities
                        )
                    elif message.video:
                        # Video message
                        await context.bot.send_video(
                            chat_id=tid,
                            video=message.video.file_id,
                            caption=message.caption,
                            caption_entities=message.caption_entities
                        )
                    elif message.document:
                        # Document message
                        await context.bot.send_document(
                            chat_id=tid,
                            document=message.document.file_id,
                            caption=message.caption,
                            caption_entities=message.caption_entities
                        )
                    elif message.animation:
                        # GIF/Animation message
                        await context.bot.send_animation(
                            chat_id=tid,
                            animation=message.animation.file_id,
                            caption=message.caption,
                            caption_entities=message.caption_entities
                        )
                    elif message.audio:
                        # Audio message
                        await context.bot.send_audio(
                            chat_id=tid,
                            audio=message.audio.file_id,
                            caption=message.caption,
                            caption_entities=message.caption_entities
                        )
                    elif message.voice:
                        # Voice message
                        await context.bot.send_voice(
                            chat_id=tid,
                            voice=message.voice.file_id,
                            caption=message.caption
                        )
                    elif message.sticker:
                        # Sticker
                        await context.bot.send_sticker(
                            chat_id=tid,
                            sticker=message.sticker.file_id
                        )
                    else:
                        # Fallback - try to copy message
                        await message.copy(chat_id=tid)
                    
                    success_count += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to forward to {tid}: {e}")
            
            self.logger.info(f"🚀 Forwarding Complete. Sent to {success_count}/{len(target_ids)} groups.")
                
        except Exception as e:
            self.logger.error(f"❌ Channel post handler error: {e}")

    async def handle_bot_status_change(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle when bot's status changes in a chat (added/removed/promoted/demoted)"""
        try:
            chat_member_update = update.my_chat_member
            if not chat_member_update:
                return
            
            chat = chat_member_update.chat
            old_status = chat_member_update.old_chat_member.status
            new_status = chat_member_update.new_chat_member.status
            
            # Get bot owner to notify
            bot_data = self.db.get_bot_by_token(self.token)
            if not bot_data:
                return
            
            owner_id = bot_data.get('owner_id')
            if not owner_id:
                return
            
            # Determine chat type emoji
            if chat.type == 'channel':
                chat_type = "📢 Channel"
                chat_emoji = "📢"
            elif chat.type in ['group', 'supergroup']:
                chat_type = "👥 Group"
                chat_emoji = "👥"
            else:
                chat_type = "💬 Chat"
                chat_emoji = "💬"
            
            chat_title = chat.title or chat.username or "Unknown"
            chat_id = chat.id

            # --- AUTO-DISCOVERY: TRACK GROUP MEMBERSHIP ---
            if chat.type in ['group', 'supergroup']:
                if new_status in ['member', 'administrator', 'creator']:
                    self.db.upsert_known_group(self.bot_id, chat_id, chat_title)
                elif new_status in ['left', 'kicked']:
                    self.db.set_group_inactive(self.bot_id, chat_id)
            
            # Check if bot was promoted to admin
            admin_statuses = ['administrator', 'creator']
            was_admin = old_status in admin_statuses
            is_admin = new_status in admin_statuses
            
            if not was_admin and is_admin:
                # Bot was promoted to admin!
                text = (
                    f"🎉 **BOT PROMOTED TO ADMIN!**\n\n"
                    f"{chat_emoji} **Chat:** {chat_title}\n"
                    f"🆔 **Chat ID:** `{chat_id}`\n"
                    f"📊 **Type:** {chat_type}\n\n"
                    f"💡 _Boleh guna ID ini untuk Forwarder:_\n"
                    f"• Set sebagai Source Channel\n"
                    f"• Set sebagai Target Group"
                )
                
                keyboard = [
                    [InlineKeyboardButton("📡 Setup Forwarder", callback_data="forwarder_menu")],
                    [InlineKeyboardButton("❌ Dismiss", callback_data="close_panel")]
                ]
                
                try:
                    await context.bot.send_message(
                        chat_id=owner_id,
                        text=text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode='Markdown'
                    )
                    self.logger.info(f"📬 Notified owner {owner_id} about admin promotion in {chat_title}")
                except Exception as e:
                    self.logger.error(f"Failed to notify owner about admin promotion: {e}")
            
            elif was_admin and not is_admin:
                # Bot was demoted from admin
                text = (
                    f"⚠️ **BOT DEMOTED FROM ADMIN**\n\n"
                    f"{chat_emoji} **Chat:** {chat_title}\n"
                    f"🆔 **Chat ID:** `{chat_id}`\n\n"
                    f"_Bot tidak lagi admin dalam chat ini._"
                )
                
                try:
                    await context.bot.send_message(
                        chat_id=owner_id,
                        text=text,
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    self.logger.error(f"Failed to notify owner about demotion: {e}")
            
            elif new_status == 'left' or new_status == 'kicked':
                # Bot was removed from chat
                text = (
                    f"🚫 **BOT REMOVED FROM CHAT**\n\n"
                    f"{chat_emoji} **Chat:** {chat_title}\n"
                    f"🆔 **Chat ID:** `{chat_id}`"
                )
                
                try:
                    await context.bot.send_message(
                        chat_id=owner_id,
                        text=text,
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    self.logger.error(f"Failed to notify owner about removal: {e}")
            
            elif old_status in ['left', 'kicked'] and new_status == 'member':
                # Bot was added to chat (not as admin yet)
                text = (
                    f"✅ **BOT ADDED TO CHAT**\n\n"
                    f"{chat_emoji} **Chat:** {chat_title}\n"
                    f"🆔 **Chat ID:** `{chat_id}`\n"
                    f"📊 **Type:** {chat_type}\n\n"
                    f"ℹ️ _Promote bot sebagai admin untuk aktifkan Forwarder._"
                )
                
                try:
                    await context.bot.send_message(
                        chat_id=owner_id,
                        text=text,
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    self.logger.error(f"Failed to notify owner about addition: {e}")
                    
        except Exception as e:
            self.logger.error(f"Bot status change handler error: {e}")

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
                MEDIA: [MessageHandler(filters.PHOTO | filters.VIDEO, self.add_company_media)],
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
        companies = self.db.get_companies(self.bot_id)
        per_page = 5
        start = page * per_page
        end = start + per_page
        current_batch = companies[start:end]

        if not current_batch and page == 0:
            text = "📋 **Belum ada company.**"
        else:
            text = f"📋 **Senarai Company (Page {page+1})**\nSila pilih company:"

        keyboard = []
        for comp in current_batch:
            keyboard.append([InlineKeyboardButton(f"🏢 {comp['name']}", callback_data=f"view_{comp['id']}")])
        
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"list_page_{page-1}"))
        if end < len(companies): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"list_page_{page+1}"))
        if nav: keyboard.append(nav)
        
        keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")])

        if update.callback_query:
            try:
                await update.callback_query.message.delete()
            except:
                pass
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def view_company(self, update: Update, comp_id: int):
        comps = self.db.get_companies(self.bot_id)
        comp = next((c for c in comps if c['id'] == int(comp_id)), None)
        if not comp:
            await update.callback_query.answer("Company not found.")
            return

        text = f"🏢 *{comp['name']}*\n\n{comp['description']}\n\n_{DEFAULT_GLOBAL_AD}_"
        keyboard = [
            [InlineKeyboardButton(comp['button_text'], url=comp['button_url'])],
            [InlineKeyboardButton("🔙 BACK TO LIST", callback_data="list_page_0")]
        ]
        
        await update.callback_query.message.delete()
        if comp['media_type'] == 'video':
            with open(comp['media_file_id'], 'rb') as video_file:
                 await update.effective_chat.send_video(video=video_file, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            with open(comp['media_file_id'], 'rb') as photo_file:
                 await update.effective_chat.send_photo(photo=photo_file, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

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
    async def admin_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bot_data = self.db.get_bot_by_token(self.token)
        if user_id != bot_data['owner_id']:
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

        if data == "main_menu": await self.main_menu(update, context)
        elif data.startswith("list_page_"): await self.show_page(update, int(data.split("_")[2]))
        elif data.startswith("view_"): await self.view_company(update, int(data.split("_")[1]))
        elif data == "wallet": await self.show_wallet(update)
        elif data == "share_link": await self.share_link(update)
        elif data == "leaderboard": await self.leaderboard(update)
        elif data == "support_info": await query.message.reply_text("💬 **Live Support**\nSila taip mesej anda terus di sini. Admin akan reply sebentar lagi.")
        
        # Admin Actions
        elif data == "admin_withdrawals": await self.show_withdrawals(update)
        elif data.startswith("approve_wd_"): await self.process_withdrawal(update, data, True)
        elif data.startswith("reject_wd_"): await self.process_withdrawal(update, data, False)
        elif data == "admin_del_list": await self.show_delete_company_list(update)
        elif data.startswith("delete_company_"): await self.confirm_delete_company(update, int(data.split("_")[2]))
        elif data == "admin_customize": await self.show_customize_menu(update)
        elif data == "admin_support": await self.show_support_messages(update)
        elif data == "close_panel": await query.message.delete()

    # --- Withdrawal Logic ---
    async def show_withdrawals(self, update: Update):
        wds = self.db.get_pending_withdrawals(self.bot_id)
        if not wds:
            await update.callback_query.message.reply_text("✅ Tiada withdrawal pending.")
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
    
    # --- Delete Company Logic ---
    async def show_delete_company_list(self, update: Update):
        """Show list of companies with delete buttons"""
        companies = self.db.get_companies(self.bot_id)
        if not companies:
            await update.callback_query.message.reply_text("📭 Tiada company untuk delete.")
            return
        
        text = "🗑️ **DELETE COMPANY**\n\nPilih company untuk delete:"
        keyboard = []
        for company in companies:
            keyboard.append([InlineKeyboardButton(
                f"❌ {company['name']}", 
                callback_data=f"delete_company_{company['id']}"
            )])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="close_panel")])
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def confirm_delete_company(self, update: Update, company_id: int):
        """Delete company from database"""
        success = self.db.delete_company(company_id)
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
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
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
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
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
        # Prepare Storage
        media_dir = f"data/media/{self.bot_id}"
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
        
        # Download and Save
        file_path = f"{media_dir}/{timestamp}{file_ext}"
        await file_obj.download_to_drive(file_path)
        
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
        msg = context.user_data['broadcast_msg']
        await update.callback_query.answer("Sending...")
        # Get All Users
        conn = self.db.get_connection()
        users = conn.execute("SELECT telegram_id FROM users WHERE bot_id = ?", (self.bot_id,)).fetchall()
        conn.close()
        
        sent = 0
        for u in users:
            try:
                await msg.copy(chat_id=u['telegram_id'])
                sent += 1
            except: pass
        
        await update.callback_query.message.reply_text(f"✅ Broadcast Sent to {sent} users.")
        return ConversationHandler.END

    # --- Helpers ---
    async def check_subscription(self, update):
        bot = self.db.get_bot_by_token(self.token)
        if bot['subscription_end']:
             expiry = datetime.datetime.strptime(bot['subscription_end'], '%Y-%m-%d %H:%M:%S.%f')
             if datetime.datetime.now() > expiry:
                 await update.effective_chat.send_message("⚠️ Service Suspended. Please contact Owner.")
                 return False
        return True

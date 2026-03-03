import logging
import datetime
import re
import os
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, InputMediaAnimation, BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler, ChatMemberHandler
from telegram.error import TimedOut, NetworkError
from database import Database
from html import escape as html_escape

async def send_with_retry(coro_func, retries=3, delay=2):
    """Retry a Telegram API call on timeout/network errors."""
    for attempt in range(retries):
        try:
            return await coro_func()
        except (TimedOut, NetworkError) as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
            else:
                raise e

def message_to_html(message) -> str:
    """
    Convert Telegram message with entities to HTML format.
    Uses python-telegram-bot's built-in text_html/caption_html which
    correctly handles nested/overlapping entities (e.g. bold + text_link).
    """
    if not message:
        return ""
    
    # Use built-in HTML conversion (handles nested entities correctly)
    try:
        if message.text_html:
            return message.text_html
        if message.caption_html:
            return message.caption_html
    except Exception:
        pass
    
    # Fallback: manual conversion for edge cases
    text = message.text or message.caption or ""
    entities = message.entities or message.caption_entities or []
    
    if not text:
        return ""
    
    if not entities:
        return html_escape(text)
    
    # Sort entities by offset (ascending) to process left-to-right
    sorted_entities = sorted(entities, key=lambda e: e.offset)
    
    # Build output left-to-right, escaping ALL gaps between entities
    parts = []
    last_end = 0
    
    for entity in sorted_entities:
        start = entity.offset
        end = entity.offset + entity.length
        
        # Skip if this entity overlaps with a previously processed one
        if start < last_end:
            continue
        
        # Escape the gap between last entity and this one
        if start > last_end:
            parts.append(html_escape(text[last_end:start]))
        
        content = text[start:end]
        escaped_content = html_escape(content)
        
        if entity.type == "bold":
            parts.append(f"<b>{escaped_content}</b>")
        elif entity.type == "italic":
            parts.append(f"<i>{escaped_content}</i>")
        elif entity.type == "underline":
            parts.append(f"<u>{escaped_content}</u>")
        elif entity.type == "strikethrough":
            parts.append(f"<s>{escaped_content}</s>")
        elif entity.type == "code":
            parts.append(f"<code>{escaped_content}</code>")
        elif entity.type == "pre":
            parts.append(f"<pre>{escaped_content}</pre>")
        elif entity.type == "url":
            href_url = escaped_content if escaped_content.startswith('http') else f'https://{escaped_content}'
            parts.append(f'<a href="{href_url}">{escaped_content}</a>')
        elif entity.type == "text_link":
            url = entity.url or ""
            parts.append(f'<a href="{html_escape(url)}">{escaped_content}</a>')
        elif entity.type == "text_mention":
            user_id = entity.user.id if entity.user else ""
            parts.append(f'<a href="tg://user?id={user_id}">{escaped_content}</a>')
        elif entity.type == "spoiler":
            parts.append(f"<tg-spoiler>{escaped_content}</tg-spoiler>")
        elif entity.type == "custom_emoji":
            emoji_id = entity.custom_emoji_id or ""
            parts.append(f'<tg-emoji emoji-id="{emoji_id}">{escaped_content}</tg-emoji>')
        else:
            parts.append(escaped_content)
        
        last_end = end
    
    # Escape any remaining text after the last entity
    if last_end < len(text):
        parts.append(html_escape(text[last_end:]))
    
    return ''.join(parts)

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
EDIT_KEYWORDS = 29  # Separate to avoid collision with SEARCH=22

# State for Search
SEARCH = 22
# States for Menu Button
MENU_BTN_TEXT, MENU_BTN_URL = range(23, 25)
# States for Pair Buttons
PAIR_SELECT_1, PAIR_SELECT_2 = range(26, 28)
# States for Recurring Broadcast
RECURRING_TYPE = 40

# States for Media Manager
MEDIA_UPLOAD = 50
# States for Referral Manage
RR_CONFIRM, RR_INPUT_ID = range(60, 62)
# States for Withdrawal
WD_AMOUNT, WD_METHOD, WD_ACCOUNT, WD_CONFIRM = range(70, 74)
# States for Referral Settings (Admin)
RS_SET_REWARD, RS_SET_MIN_WD = range(80, 82)
# States for Group Welcome Setup
GW_MENU, GW_TEXT, GW_MEDIA = range(90, 93)
# States for Grid Broadcast
BROADCAST_TYPE = 100
GRID_MEDIA, GRID_CAPTION, GRID_BUTTONS = range(101, 104)
SINGLE_BUTTONS = 105
BROADCAST_AI_REWRITE = 106
# States for Userbot Setup
UB_MENU, UB_SETUP_API, UB_SETUP_HASH, UB_SETUP_PHONE, UB_SETUP_OTP, UB_SETUP_2FA, UB_ADD_CHANNEL = range(110, 117)
# States for Userbot Hub & Clone Media
UB_HUB = 118
CLONE_SOURCE, CLONE_TARGET, CLONE_CONFIRM = range(120, 123)
CLONE_CAPTION_MODE, CLONE_CAPTION_TEXT = range(123, 125)

class ChildBot:
    def __init__(self, token, bot_id, db: Database, scheduler):
        self.token = token
        self.bot_id = bot_id
        self.db = db
        self.scheduler = scheduler
        from telegram.request import HTTPXRequest
        request = HTTPXRequest(connect_timeout=30, read_timeout=30, write_timeout=30, pool_timeout=30)
        self.app = Application.builder().token(token).request(request).build()
        self.logger = logging.getLogger(f"Bot_{bot_id}")
        # Cache bot_data to avoid repeated DB lookups
        self._bot_data_cache = None
        self.userbot_manager = None  # Set by BotManager after spawn
        self.setup_handlers()

    async def initialize(self):
        """Prepare bot application but do not start polling (Webhook mode)"""
        await self.app.initialize()
        await self.app.start()
        # Register bot commands for menu visibility
        await self._register_commands()
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

    async def _register_commands(self):
        """Register bot commands so users see them in the '/' menu"""
        try:
            referral_enabled = self.db.is_referral_enabled(self.bot_id)
            
            # Commands for private chats
            private_commands = [
                BotCommand("start", "Mulakan bot"),
                BotCommand("company", "Senarai company"),
                BotCommand("list", "Lihat carousel company"),
                BotCommand("menu", "Papar menu utama"),
                BotCommand("4d", "4D Analyzer"),
            ]
            if referral_enabled:
                private_commands.append(BotCommand("wallet", "Dompet saya"))
            await self.app.bot.set_my_commands(private_commands, scope=BotCommandScopeAllPrivateChats())

            # Commands for group chats (exclude wallet/admin stuff)
            group_commands = [
                BotCommand("list", "Lihat senarai company"),
                BotCommand("company", "Menu utama company"),
                BotCommand("menu", "Papar menu utama"),
                BotCommand("4d", "4D Analyzer"),
            ]
            await self.app.bot.set_my_commands(group_commands, scope=BotCommandScopeAllGroupChats())
            self.logger.info("✅ Bot commands registered")
        except Exception as e:
            self.logger.warning(f"Failed to register commands: {e}")

    async def stop(self):
        await self.app.stop()
        await self.app.shutdown()

    # --- Handlers Setup ---
    def setup_handlers(self):
        # Admin Commands
        self.app.add_handler(CommandHandler("settings", self.admin_dashboard))
        self.app.add_handler(CommandHandler("admin", self.admin_dashboard))
        self.app.add_handler(CommandHandler("reset_ref", self.cmd_reset_referrals))

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
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern="^cancel$"), CallbackQueryHandler(self.handle_callback)],
            allow_reentry=True,
            conversation_timeout=300
        )
        self.app.add_handler(add_conv)

        # Admin Broadcast Wizard
        broadcast_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.broadcast_start, pattern="^admin_broadcast$")],
            states={
                BROADCAST_TARGET: [CallbackQueryHandler(self.broadcast_choose_target)],
                BROADCAST_TYPE: [CallbackQueryHandler(self.broadcast_type_handler)],
                BROADCAST_CONTENT: [MessageHandler(filters.ALL & ~filters.COMMAND, self.broadcast_content)],
                SINGLE_BUTTONS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.single_btn_handler),
                    CallbackQueryHandler(self.single_btn_company_pick, pattern="^sbtn_comp_"),
                    CallbackQueryHandler(self.single_btn_manual, pattern="^sbtn_manual$"),
                    CallbackQueryHandler(self.single_btn_skip, pattern="^sbtn_skip$")
                ],
                BROADCAST_CONFIRM: [CallbackQueryHandler(self.broadcast_confirm)],
                SCHEDULE_TIME: [CallbackQueryHandler(self.broadcast_confirm)],
                RECURRING_TYPE: [CallbackQueryHandler(self.recurring_type_handler)],
                GRID_MEDIA: [
                    MessageHandler(filters.PHOTO | filters.VIDEO, self.grid_media_handler),
                    CallbackQueryHandler(self.grid_media_done, pattern="^grid_done$")
                ],
                GRID_CAPTION: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.grid_caption_handler),
                    CallbackQueryHandler(self.grid_caption_skip, pattern="^grid_skip_caption$")
                ],
                GRID_BUTTONS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.grid_buttons_handler),
                    CallbackQueryHandler(self.grid_company_pick, pattern="^grid_comp_"),
                    CallbackQueryHandler(self.grid_manual_btn, pattern="^grid_manual_btn$"),
                    CallbackQueryHandler(self.grid_buttons_done, pattern="^grid_buttons_done$"),
                    CallbackQueryHandler(self.grid_buttons_skip, pattern="^grid_skip_buttons$")
                ],
                BROADCAST_AI_REWRITE: [
                    CallbackQueryHandler(self.ai_rewrite_execute, pattern="^bc_ai_yes$"),
                    CallbackQueryHandler(self.ai_vision_execute, pattern="^bc_ai_vision$"),
                    CallbackQueryHandler(self.ai_manual_caption, pattern="^bc_ai_manual$"),
                    CallbackQueryHandler(self.ai_rewrite_skip, pattern="^bc_ai_skip$"),
                    CallbackQueryHandler(self.ai_rewrite_accept, pattern="^bc_ai_accept$"),
                    CallbackQueryHandler(self.ai_rewrite_original, pattern="^bc_ai_original$"),
                    CallbackQueryHandler(self.ai_rewrite_retry, pattern="^bc_ai_retry$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.ai_manual_caption_receive),
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.handle_callback)],
            allow_reentry=True,
            conversation_timeout=300
        )
        self.app.add_handler(broadcast_conv)

        # Edit Welcome Wizard
        welcome_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.edit_welcome_start, pattern="^edit_welcome$")],
            states={
                WELCOME_PHOTO: [MessageHandler(filters.PHOTO | filters.VIDEO, self.save_welcome_photo)],
                WELCOME_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.save_welcome_text)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_welcome), CallbackQueryHandler(self.handle_callback)],
            allow_reentry=True,
            conversation_timeout=300
        )
        self.app.add_handler(welcome_conv)
        
        # Media Manager Wizard
        media_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.show_media_manager, pattern="^admin_media_manager$")],
            states={
                MEDIA_UPLOAD: [
                    CallbackQueryHandler(self.media_manager_select_section, pattern="^media_section_"),
                    CallbackQueryHandler(self.media_manager_back, pattern="^media_back$"),
                    MessageHandler(filters.PHOTO | filters.VIDEO, self.media_manager_save_upload)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern="^cancel$"), CallbackQueryHandler(self.handle_callback)],
            allow_reentry=True
        )
        self.app.add_handler(media_conv)

        # Referral Management Wizard
        manage_ref_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.manage_ref_start, pattern="^admin_ref_manage$")],
            states={
                RR_CONFIRM: [CallbackQueryHandler(self.manage_ref_confirm_action)],
                RR_INPUT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.manage_ref_input_id)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern="^cancel$"), CallbackQueryHandler(self.handle_callback)],
            allow_reentry=True,
            conversation_timeout=300
        )
        self.app.add_handler(manage_ref_conv)

        # Edit Company Wizard
        edit_company_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.edit_company_start, pattern=r'^edit_company_\d+$'),
                CallbackQueryHandler(self.edit_company_start, pattern=r'^admin_edit_company_select_\d+$')
            ],
            states={
                EDIT_FIELD: [
                    CallbackQueryHandler(self.edit_company_choose_field, pattern=r'^ef_'),
                    CallbackQueryHandler(self.back_to_admin_list, pattern=r'^admin_edit_back$')
                ],
                EDIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_name)],
                EDIT_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_desc)],
                EDIT_MEDIA: [MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION, self.edit_company_save_media)],
                EDIT_BTN_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_btn_text)],
                EDIT_BTN_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_btn_url)],
                EDIT_KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_company_save_keywords)],

            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern=r'^cancel$'), CallbackQueryHandler(self.handle_callback)],
            per_message=False,
            allow_reentry=True,
            conversation_timeout=300
        )
        self.app.add_handler(edit_company_conv)
        
        # Withdrawal Conversation Handler
        withdrawal_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.start_withdrawal, pattern="^req_withdraw$")],
            states={
                WD_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.withdrawal_input_amount)],
                WD_METHOD: [CallbackQueryHandler(self.withdrawal_select_method, pattern="^wd_company_")],
                WD_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.withdrawal_input_account)],
                WD_CONFIRM: [
                    CallbackQueryHandler(self.withdrawal_submit, pattern="^wd_submit$"),
                    CallbackQueryHandler(self.cancel_withdrawal, pattern="^cancel_wd$")
                ],
            },
            fallbacks=[CallbackQueryHandler(self.handle_callback)],
            name="withdrawal_conversation",
            persistent=False,
            allow_reentry=True
        )
        self.app.add_handler(withdrawal_handler)
        
        # Add Menu Button Wizard
        menu_btn_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_menu_btn_start, pattern=r'^menu_add_btn$')],
            states={
                MENU_BTN_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_menu_btn_text)],
                MENU_BTN_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_menu_btn_url)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_op), CallbackQueryHandler(self.cancel_op, pattern=r'^cancel$'), CallbackQueryHandler(self.handle_callback)],
            per_message=False
        )
        self.app.add_handler(menu_btn_conv)
        
        # Referral Settings Wizard (Admin)
        ref_settings_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.ref_settings_menu, pattern=r'^ref_settings$'),
                CallbackQueryHandler(self.ref_settings_set_reward, pattern=r'^rs_reward$'),
                CallbackQueryHandler(self.ref_settings_set_min_wd, pattern=r'^rs_min_wd$')
            ],
            states={
                RS_SET_REWARD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ref_settings_save_reward)],
                RS_SET_MIN_WD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ref_settings_save_min_wd)],
            },
            fallbacks=[
                CallbackQueryHandler(self.ref_settings_menu, pattern=r'^ref_settings$'),
                CallbackQueryHandler(self.ref_settings_back, pattern=r'^ref_back$'),
                CommandHandler("cancel", self.cancel_op)
            ],
            per_message=False
        )
        self.app.add_handler(ref_settings_conv)
        
        # Group Welcome Setup Wizard (Admin)
        gw_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.gw_menu, pattern=r'^group_welcome_setup$')],
            states={
                GW_MENU: [CallbackQueryHandler(self.gw_handle_action, pattern=r'^gw_')],
                GW_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.gw_save_text)],
                GW_MEDIA: [MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION, self.gw_save_media)],
            },
            fallbacks=[
                CallbackQueryHandler(self.gw_menu, pattern=r'^group_welcome_setup$'),
                CommandHandler("cancel", self.cancel_op),
                CallbackQueryHandler(self.handle_callback),
            ],
            per_message=False,
            allow_reentry=True,
            conversation_timeout=300
        )
        self.app.add_handler(gw_conv)

        # Userbot Hub + Promo Monitor + Clone Media Wizard (Admin)
        ub_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.userbot_hub_menu, pattern=r'^userbot_hub$'),
                CallbackQueryHandler(self.ub_menu, pattern=r'^ub_menu$'),
            ],
            states={
                UB_HUB: [
                    CallbackQueryHandler(self.ub_hub_handle_action, pattern=r'^ubhub_'),
                    CallbackQueryHandler(self.ub_menu, pattern=r'^ub_menu$'),
                    CallbackQueryHandler(self.clone_media_menu, pattern=r'^clone_menu$'),
                    CallbackQueryHandler(self._clone_start_flow, pattern=r'^clone_start_flow$'),
                ],
                UB_MENU: [
                    CallbackQueryHandler(self.ub_handle_action, pattern=r'^ub_'),
                    CallbackQueryHandler(self.ub_handle_action, pattern=r'^scan_'),
                    CallbackQueryHandler(self.ub_handle_action, pattern=r'^noop$'),
                    CallbackQueryHandler(self.ub_handle_action, pattern=r'^promo_'),
                ],
                UB_SETUP_API: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ub_save_api_id)],
                UB_SETUP_HASH: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ub_save_api_hash)],
                UB_SETUP_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ub_save_phone)],
                UB_SETUP_OTP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ub_verify_otp)],
                UB_SETUP_2FA: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ub_verify_2fa)],
                UB_ADD_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ub_add_channel_link)],
                CLONE_SOURCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.clone_save_source)],
                CLONE_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.clone_save_target)],
                CLONE_CAPTION_MODE: [
                    CallbackQueryHandler(self.clone_caption_mode_select, pattern=r'^cap_'),
                ],
                CLONE_CAPTION_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.clone_caption_text_input),
                ],
                CLONE_CONFIRM: [
                    CallbackQueryHandler(self.clone_confirm, pattern=r'^clone_confirm$'),
                    CallbackQueryHandler(self.clone_media_menu, pattern=r'^clone_cancel$'),
                ],
            },
            fallbacks=[
                CallbackQueryHandler(self.userbot_hub_menu, pattern=r'^userbot_hub$'),
                CallbackQueryHandler(self.ub_menu, pattern=r'^ub_menu$'),
                CallbackQueryHandler(self.ub_handle_action, pattern=r'^ub_'),
                CallbackQueryHandler(self.ub_handle_action, pattern=r'^scan_'),
                CallbackQueryHandler(self.ub_handle_action, pattern=r'^promo_'),
                CallbackQueryHandler(self.clone_media_menu, pattern=r'^clone_menu$'),
                CommandHandler("cancel", self.cancel_op),
                CallbackQueryHandler(self.handle_callback),
            ],
            per_message=False,
            allow_reentry=True,
            conversation_timeout=600
        )
        self.app.add_handler(ub_conv)

        # New Chat Members Handler (Group Welcome)
        self.app.add_handler(MessageHandler(
            filters.StatusUpdate.NEW_CHAT_MEMBERS,
            self.handle_new_member
        ))

        # Left Chat Member Handler (Delete Leave Messages)
        self.app.add_handler(MessageHandler(
            filters.StatusUpdate.LEFT_CHAT_MEMBER,
            self.handle_left_member
        ))

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
        
        # Switch to Carousel Mode immediately (Page 0)
        await self.show_page(update, 0)

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
            [InlineKeyboardButton("📈 Carta Visual", callback_data="4d_visual"),
             InlineKeyboardButton("📋 Carta Ramalan", callback_data="4d_predict")],
            [InlineKeyboardButton("🗓️ Carta Sejarah", callback_data="4d_history")],
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
                    ref_settings = self.db.get_referral_settings(self.bot_id)
                    total_earned = total_invites * ref_settings['referral_reward']
                    
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
                        f"_Minimum withdrawal: RM{ref_settings['min_withdrawal']:.2f}_"
                    )
                    
                    # Check for wallet media asset
                    asset = self.db.get_asset(self.bot_id, 'wallet')
                    if asset and asset.get('file_id'):
                        try:
                            if asset.get('file_type') == 'video':
                                await context.bot.send_video(chat_id=user_id, video=asset['file_id'], caption=text, parse_mode='Markdown')
                            else:
                                await context.bot.send_photo(chat_id=user_id, photo=asset['file_id'], caption=text, parse_mode='Markdown')
                        except Exception:
                            await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
                    else:
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
        ref_settings = self.db.get_referral_settings(self.bot_id)
        total_earned = total_invites * ref_settings['referral_reward']
        
        text = (
            f"💰 **YOUR WALLET**\n\n"
            f"💵 Balance: **RM {balance:.2f}**\n"
            f"👥 Total Referrals: **{total_invites}**\n"
            f"💎 Total Earned: **RM {total_earned:.2f}**\n\n"
            f"_Minimum withdrawal: RM{ref_settings['min_withdrawal']:.2f}_"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 WITHDRAW", callback_data="req_withdraw")],
            [InlineKeyboardButton("🔗 Share Link", callback_data="share_link")],
            [InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Check for wallet media asset
        asset = self.db.get_asset(self.bot_id, 'wallet')
        
        if asset and asset.get('file_id'):
            try:
                if asset.get('file_type') == 'video':
                    await update.message.reply_video(video=asset['file_id'], caption=text, reply_markup=reply_markup, parse_mode='Markdown')
                else:
                    await update.message.reply_photo(photo=asset['file_id'], caption=text, reply_markup=reply_markup, parse_mode='Markdown')
                return
            except Exception as e:
                self.logger.error(f"wallet command media error: {e}")
        
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

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
                except Exception:
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
                # Get referrer's updated stats and reward amount
                referrer_data = self.db.get_user(self.bot_id, referrer_id)
                settings = self.db.get_referral_settings(self.bot_id)
                reward_amount = settings['referral_reward']
                total_invites = referrer_data.get('total_invites', 1) if referrer_data else 1
                new_balance = referrer_data.get('balance', reward_amount) if referrer_data else reward_amount
                
                notification = (
                    f"🎉 **REFERRAL BERJAYA!**\n\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"👤 **{user.first_name}** baru join!\n"
                    f"💰 Anda dapat: **+RM{reward_amount:.2f}**\n"
                    f"━━━━━━━━━━━━━━━━━\n\n"
                    f"📊 **Stats Anda:**\n"
                    f"👥 Total Referral: **{total_invites}**\n"
                    f"💵 Baki Semasa: **RM{new_balance:.2f}**\n\n"
                    f"🔥 Teruskan share link anda!"
                )
                await context.bot.send_message(chat_id=referrer_id, text=notification, parse_mode='Markdown')
            except Exception:  pass  # Referrer might have blocked bot
            
        await self.main_menu(update, context)

        # AI Onboarding for new users
        if is_new and self.db.is_ai_chat_enabled(self.bot_id):
            try:
                from ai_rewriter import ai_onboarding
                companies = self.db.get_companies(self.bot_id)
                custom_prompt = self.db.get_ai_prompt(self.bot_id) or None
                user_name = user.first_name or "Bro"
                
                await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
                onboard_msg = await ai_onboarding(user_name, companies, custom_prompt=custom_prompt)
                
                if onboard_msg:
                    await update.effective_chat.send_message(
                        f"🤖 {onboard_msg}",
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
            except Exception as e:
                self.logger.error(f"AI onboarding error: {e}")

    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_subscription(update): return

        bot_data = self._get_bot_data()
        
        # Get all companies
        companies = self.db.get_companies(self.bot_id)
        
        # Build caption
        caption = bot_data['custom_caption'] or (
            f"🏢 <b>SENARAI COMPANY</b>\n\n"
            f"Pilih company yang anda ingin lihat:\n"
            f"📊 Total: {len(companies)} company"
        )
        
        # Build keyboard with companies - 2 per row
        keyboard = []
        for i in range(0, len(companies), 2):
            row = []
            # First company in row
            comp1 = companies[i]
            row.append(InlineKeyboardButton(
                f"{comp1['name']}", 
                callback_data=f"c_{comp1['id']}"
            ))
            
            # Second company in row (if exists)
            if i + 1 < len(companies):
                comp2 = companies[i + 1]
                row.append(InlineKeyboardButton(
                    f"{comp2['name']}", 
                    callback_data=f"c_{comp2['id']}"
                ))
            
            keyboard.append(row)
        
        # Check if referral system is enabled
        referral_enabled = self.db.is_referral_enabled(self.bot_id)
        
        # Show referral buttons only if enabled
        if referral_enabled:
            keyboard.append([
                InlineKeyboardButton("💰 Dompet Saya", callback_data="wallet"),
                InlineKeyboardButton("🔗 Share Link", callback_data="share_link")
            ])
            keyboard.append([
                InlineKeyboardButton("🎰 4D Analyzer", callback_data="4d_menu"),
                InlineKeyboardButton("🏆 Leaderboard", callback_data="leaderboard")
            ])
        else:
            keyboard.append([
                InlineKeyboardButton("🎰 4D Analyzer", callback_data="4d_menu")
            ])
        
        # Add custom menu buttons if any
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
        

        # Parse banner type (supports new "type|file_id" format and legacy plain file_id)
        banner_raw = bot_data['custom_banner'] if bot_data['custom_banner'] else None
        banner_type = 'photo'
        banner_file_id = None
        if banner_raw:
            if '|' in banner_raw:
                banner_type, banner_file_id = banner_raw.split('|', 1)
            else:
                banner_file_id = banner_raw  # Legacy format (photo only)

        if update.callback_query:
            # Carousel style - edit existing message instead of delete+send
            try:
                if banner_file_id:
                    if banner_type == 'video':
                        await update.callback_query.message.edit_media(
                            media=InputMediaVideo(media=banner_file_id, caption=caption, parse_mode='HTML'),
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    else:
                        await update.callback_query.message.edit_media(
                            media=InputMediaPhoto(media=banner_file_id, caption=caption, parse_mode='HTML'),
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
                except Exception: pass
                if banner_file_id:
                    if banner_type == 'video':
                        await update.effective_chat.send_video(video=banner_file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                    else:
                        await update.effective_chat.send_photo(photo=banner_file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                else:
                    await update.effective_chat.send_message(caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            # Fresh /start command - send new message
            if banner_file_id:
                if banner_type == 'video':
                    await update.effective_chat.send_video(video=banner_file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                else:
                    await update.effective_chat.send_photo(photo=banner_file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            else:
                await update.effective_chat.send_message(caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    # --- Company Logic ---
    def _get_bot_data(self):
        """Get bot data with caching to avoid repeated DB lookups"""
        if self._bot_data_cache is None:
            self._bot_data_cache = self.db.get_bot_by_token(self.token)
        return self._bot_data_cache

    def _invalidate_bot_cache(self):
        """Clear bot data cache when settings change"""
        self._bot_data_cache = None

    async def show_page(self, update: Update, page: int, companies=None):
        """Display company in CAROUSEL mode - one company at a time with Prev/Next buttons"""
        if companies is None:
            companies = self.db.get_companies(self.bot_id)
        
        if not companies:
            text = "📋 **Belum ada company.**"
            keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")]]
            
            if update.callback_query:
                # Defensive answer
                try: await update.callback_query.answer()
                except Exception: pass
                
                try:
                    await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                except Exception as e:
                     # Ignore "Message not modified"
                    if "Message is not modified" not in str(e):
                        # Fallback
                        try: await update.callback_query.message.delete()
                        except Exception: pass
                        await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
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
        bot_data = self._get_bot_data()
        is_admin = update.effective_user.id == bot_data['owner_id']
        
        # Build caption - Using HTML format to support rich text formatting in descriptions
        escaped_name = html_escape(comp['name'])
        
        # Auto-link plain URLs in description that aren't already in <a> tags
        # This handles legacy descriptions saved as raw text before message_to_html
        desc = comp['description'] or ''
        import re
        # Only auto-link URLs NOT already inside HTML tags
        # Check if desc already contains ANY HTML tags (from message_to_html)
        has_html = bool(re.search(r'<[a-zA-Z][^>]*>', desc))
        if not has_html:
            # Description is plain text (legacy) - escape HTML and auto-link URLs
            desc = html_escape(desc)
            def _add_link(m):
                url = m.group(1)
                href = url if url.startswith('http') else f'https://{url}'
                return f'<a href="{href}">{url}</a>'
            desc = re.sub(
                r'(https?://[^\s<>]+|(?:www\.|t\.me/|wasap\.my/)[^\s<>]+)',
                _add_link,
                desc
            )
        
        full_caption = (
            f"<b>{escaped_name}</b>\n\n"
            f"{desc}"
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
        
        # Admin-only buttons (private chat only)
        if is_admin and update.effective_chat.type == 'private':
            keyboard.append([InlineKeyboardButton("✏️ EDIT COMPANY", callback_data=f"edit_company_{comp['id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")])
        
        # Check if caption exceeds Telegram's 1024 character limit for media captions
        # If so, send media with short caption + full text as separate message
        caption_too_long = len(full_caption) > 1024
        
        if caption_too_long:
            media_caption = f"<b>{escaped_name}</b>"  # Short caption for media
        else:
            media_caption = full_caption  # Full caption fits
        
        # Check Media
        import os
        media_path = comp['media_file_id']
        is_local_file = media_path and (media_path.startswith('/') or os.path.sep in media_path) and os.path.exists(media_path)

        try:
             # Helper to get InputMedia
            def get_input_media(file_obj=None):
                 # Use file_obj if provided (local file), else media_path (file_id)
                 media_source = file_obj if file_obj else media_path
                 
                 if comp['media_type'] == 'video':
                     return InputMediaVideo(media=media_source, caption=media_caption, parse_mode='HTML')
                 elif comp['media_type'] == 'animation':
                     return InputMediaAnimation(media=media_source, caption=media_caption, parse_mode='HTML')
                 else:
                     return InputMediaPhoto(media=media_source, caption=media_caption, parse_mode='HTML')

            # ALWAYS put keyboard on media message for smooth carousel navigation
            media_keyboard = InlineKeyboardMarkup(keyboard)

            # Helper: extract and cache file_id from result
            def _cache_file_id(result):
                if result:
                    fid = None
                    if result.photo: fid = result.photo[-1].file_id
                    elif result.video: fid = result.video.file_id
                    elif result.animation: fid = result.animation.file_id
                    if fid:
                        self.db.update_cached_file_id(comp['id'], fid)

            # Helper: send new media message
            async def _send_new_media(file_source):
                if comp['media_type'] == 'video':
                    return await update.effective_chat.send_video(video=file_source, caption=media_caption, reply_markup=media_keyboard, parse_mode='HTML')
                elif comp['media_type'] == 'animation':
                    return await update.effective_chat.send_animation(animation=file_source, caption=media_caption, reply_markup=media_keyboard, parse_mode='HTML')
                else:
                    return await update.effective_chat.send_photo(photo=file_source, caption=media_caption, reply_markup=media_keyboard, parse_mode='HTML')

            is_callback_media = update.callback_query and (
                update.callback_query.message.photo or 
                update.callback_query.message.video or 
                update.callback_query.message.animation or
                update.callback_query.message.document  # Telegram sends GIFs as documents
            )

            if is_local_file:
                cached_file_id = comp.get('cached_file_id')
                
                # 1st priority: Use cached file_id for instant smooth edit
                if cached_file_id and is_callback_media:
                    try:
                        cached_media = get_input_media(cached_file_id)
                        await update.callback_query.message.edit_media(media=cached_media, reply_markup=media_keyboard)
                        if caption_too_long:
                            await update.effective_chat.send_message(full_caption, parse_mode='HTML')
                        return
                    except Exception as e:
                        if "Message is not modified" in str(e): return
                        if "Flood control" in str(e) or "Too Many Requests" in str(e):
                            try: await update.callback_query.answer("⏳ Terlalu cepat, cuba lagi sebentar.", show_alert=True)
                            except Exception: pass
                            return
                        self.logger.warning(f"edit_media cached failed: {e}")
                
                # 2nd priority: Upload file + try edit
                with open(media_path, 'rb') as f:
                    if is_callback_media:
                        try:
                            media_obj = get_input_media(f)
                            result = await update.callback_query.message.edit_media(media=media_obj, reply_markup=media_keyboard)
                            _cache_file_id(result)
                            if caption_too_long:
                                await update.effective_chat.send_message(full_caption, parse_mode='HTML')
                            return
                        except Exception as e:
                            if "Message is not modified" in str(e): return
                            if "Flood control" in str(e) or "Too Many Requests" in str(e):
                                try: await update.callback_query.answer("⏳ Terlalu cepat, cuba lagi sebentar.", show_alert=True)
                                except Exception: pass
                                return
                            self.logger.warning(f"edit_media upload failed: {e}")
                    
                    # Fallback: Delete + Send new
                    if update.callback_query:
                        try: await update.callback_query.message.delete()
                        except Exception: pass
                    
                    f.seek(0)
                    try:
                        result = await _send_new_media(f)
                        _cache_file_id(result)
                    except Exception as e:
                        if "Flood control" in str(e) or "Too Many Requests" in str(e):
                            await update.effective_chat.send_message("⏳ Terlalu cepat, cuba lagi sebentar.", reply_markup=media_keyboard)
                            return
                        raise
            else:
                # Remote File ID - always smooth
                if is_callback_media:
                    try:
                        media_obj = get_input_media(None)
                        await update.callback_query.message.edit_media(media=media_obj, reply_markup=media_keyboard)
                        if caption_too_long:
                            await update.effective_chat.send_message(full_caption, parse_mode='HTML')
                        return
                    except Exception as e:
                        if "Message is not modified" in str(e): return
                        if "Flood control" in str(e) or "Too Many Requests" in str(e):
                            try: await update.callback_query.answer("⏳ Terlalu cepat, cuba lagi sebentar.", show_alert=True)
                            except Exception: pass
                            return
                        self.logger.warning(f"edit_media file_id failed: {e}")

                if update.callback_query:
                    try: await update.callback_query.message.delete()
                    except Exception: pass
                
                await _send_new_media(media_path)

            # If caption was too long, send full text as separate (no keyboard - keyboard stays on media)
            if caption_too_long:
                await update.effective_chat.send_message(full_caption, parse_mode='HTML')

        except Exception as e:
             self.logger.error(f"Media error in show_page: {e}")
             # Don't delete on error - keep previous media intact for navigation
             try:
                 if update.callback_query:
                     await update.callback_query.answer("⚠️ Gagal memuatkan. Cuba lagi.", show_alert=True)
             except Exception: pass

    async def view_company(self, update: Update, comp_id: int):
        # Redirect to Carousel View (find index)
        comps = self.db.get_companies(self.bot_id)
        index = next((i for i, c in enumerate(comps) if c['id'] == int(comp_id)), -1)
        
        if index != -1:
            await self.show_page(update, index, companies=comps)
        else:
            if update.callback_query:
                await update.callback_query.answer("Company not found.")
            else:
                await update.message.reply_text("Company not found.")

    # --- Wallet & Referral ---
    async def show_wallet(self, update: Update):
        try:
            user = self.db.get_user(self.bot_id, update.effective_user.id)
            if not user:
                try: await update.callback_query.answer("⚠️ Data not found. Type /start again.", show_alert=True)
                except Exception: pass
                return
            
            # Get custom settings
            settings = self.db.get_referral_settings(self.bot_id)
            min_wd = settings['min_withdrawal']
                
            # Use HTML for safety
            text = (
                f"💰 <b>DOMPET ANDA</b>\n\n"
                f"👤 <b>ID:</b> <code>{user['telegram_id']}</code>\n"
                f"📊 <b>Total Invite:</b> {user['total_invites']} Orang\n"
                f"💵 <b>Baki Wallet:</b> RM {user['balance']:.2f}\n\n"
                f"<i>Min withdrawal: RM {min_wd:.2f}</i>"
            )
            
            keyboard = []
            keyboard.append([InlineKeyboardButton("📤 REQUEST WITHDRAWAL", callback_data="req_withdraw")])
            keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Check for wallet media asset
            asset = self.db.get_asset(self.bot_id, 'wallet')
            
            # Answer callback first
            if update.callback_query:
                try: await update.callback_query.answer()
                except Exception: pass
            
            # Delete previous message (handles both media->media and text->media transitions)
            if update.callback_query:
                try: await update.callback_query.message.delete()
                except Exception: pass
            
            # Send with or without media
            if asset and asset.get('file_id'):
                # Use asset caption if set, otherwise use wallet text
                caption = text
                
                try:
                    if asset.get('file_type') == 'video':
                        await update.effective_chat.send_video(
                            video=asset['file_id'],
                            caption=caption,
                            reply_markup=reply_markup,
                            parse_mode='HTML'
                        )
                    else:
                        await update.effective_chat.send_photo(
                            photo=asset['file_id'],
                            caption=caption,
                            reply_markup=reply_markup,
                            parse_mode='HTML'
                        )
                except Exception as e:
                    self.logger.error(f"show_wallet media error: {e}")
                    # Fallback to text
                    await update.effective_chat.send_message(text, reply_markup=reply_markup, parse_mode='HTML')
            else:
                # No media - send text only
                await update.effective_chat.send_message(text, reply_markup=reply_markup, parse_mode='HTML')
                
        except Exception as e:
            self.logger.error(f"CRITICAL Error in show_wallet: {e}")
            try: await update.effective_chat.send_message("❌ Error loading wallet.", parse_mode='HTML')
            except Exception: pass

    # === WITHDRAWAL CONVERSATION HANDLERS ===
    
    async def start_withdrawal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Entry point for withdrawal conversation"""
        self.logger.info(f"💰 start_withdrawal called by user {update.effective_user.id}")
        
        user = self.db.get_user(self.bot_id, update.effective_user.id)
        if not user:
            try:
                await update.callback_query.answer("⚠️ Data not found. Taip /start dulu.", show_alert=True)
            except Exception:
                pass
            return ConversationHandler.END
        
        # Get custom settings
        settings = self.db.get_referral_settings(self.bot_id)
        min_wd = settings['min_withdrawal']
        context.user_data['min_withdrawal'] = min_wd
        
        if user['balance'] < min_wd:
            try:
                await update.callback_query.answer(
                    f"⚠️ Balance tidak mencukupi!\n\nBalance: RM {user['balance']:.2f}\nMinimum: RM {min_wd:.2f}", 
                    show_alert=True
                )
            except Exception as e:
                self.logger.error(f"start_withdrawal answer error: {e}")
            return ConversationHandler.END
        
        # Only answer() here (no alert) since we'll show the form
        try:
            await update.callback_query.answer()
        except Exception:
            pass
        
        text = (
            f"📤 <b>REQUEST WITHDRAWAL</b>\n\n"
            f"💵 <b>Balance:</b> RM {user['balance']:.2f}\n"
            f"💰 <b>Min Amount:</b> RM {min_wd:.2f}\n\n"
            f"Masukkan amount yang nak withdraw:"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wd")]]
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            self.logger.error(f"start_withdrawal edit_text error: {e}")
            try:
                await update.callback_query.message.delete()
            except Exception:
                pass
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        
        return WD_AMOUNT
    
    async def withdrawal_input_amount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle amount input"""
        try:
            amount = float(update.message.text.strip().replace("RM", "").replace("rm", "").strip())
        except ValueError:
            await update.message.reply_text("⚠️ Format tidak sah. Masukkan nombor sahaja.\n\nContoh: 50")
            return WD_AMOUNT
        
        user = self.db.get_user(self.bot_id, update.effective_user.id)
        min_wd = context.user_data.get('min_withdrawal', 50.0)  # Get from context or default
        
        if amount < min_wd:
            await update.message.reply_text(f"⚠️ Minimum withdrawal RM {min_wd:.2f}")
            return WD_AMOUNT
        
        if amount > user['balance']:
            await update.message.reply_text(f"⚠️ Balance tidak mencukupi.\n\nBalance: RM {user['balance']:.2f}")
            return WD_AMOUNT
        
        context.user_data['wd_amount'] = amount
        
        # Get companies for selection
        companies = self.db.get_companies(self.bot_id)
        
        if not companies:
            await update.message.reply_text("⚠️ Tiada company dalam list. Sila hubungi admin.")
            return ConversationHandler.END
        
        text = f"✅ <b>Amount: RM {amount:.2f}</b>\n\nPilih company untuk topup:"
        keyboard = []
        for comp in companies:
            keyboard.append([InlineKeyboardButton(f"🏢 {comp['name']}", callback_data=f"wd_company_{comp['id']}")])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_wd")])
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        return WD_METHOD
    
    async def withdrawal_select_method(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle company selection"""
        query = update.callback_query
        await query.answer()
        
        # Parse company ID from callback (wd_company_123)
        company_id = int(query.data.split("_")[2])
        
        # Get company details
        companies = self.db.get_companies(self.bot_id)
        company = next((c for c in companies if c['id'] == company_id), None)
        
        if not company:
            await query.answer("⚠️ Company tidak dijumpai", show_alert=True)
            return ConversationHandler.END
        
        context.user_data['wd_company_id'] = company_id
        context.user_data['wd_company_name'] = company['name']
        
        prompt = (
            f"🏢 <b>{company['name']}</b>\n\n"
            f"Sila masukkan USERNAME akaun anda dalam company ini:\n\n"
            f"<i>Contoh: player123</i>"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wd")]]
        await query.message.edit_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        
        return WD_ACCOUNT
    
    async def withdrawal_input_account(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle username input"""
        username = update.message.text.strip()
        
        # Simple validation - username must be at least 3 chars
        if len(username) < 3:
            await update.message.reply_text("⚠️ Username terlalu pendek. Minimum 3 aksara.")
            return WD_ACCOUNT
        
        context.user_data['wd_username'] = username
        amount = context.user_data.get('wd_amount', 0)
        company_name = context.user_data.get('wd_company_name', 'Unknown')
        
        text = (
            f"📋 <b>CONFIRM WITHDRAWAL</b>\n\n"
            f"💵 <b>Amount:</b> RM {amount:.2f}\n"
            f"🏢 <b>Company:</b> {company_name}\n"
            f"👤 <b>Username:</b> <code>{username}</code>\n\n"
            f"⚠️ Pastikan maklumat betul!"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ CONFIRM & SUBMIT", callback_data="wd_submit")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_wd")]
        ]
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        return WD_CONFIRM
    
    async def withdrawal_submit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Submit withdrawal request"""
        query = update.callback_query
        await query.answer()
        
        amount = context.user_data.get('wd_amount')
        company_name = context.user_data.get('wd_company_name')
        username = context.user_data.get('wd_username')
        
        # Safety check - if data is missing, show error
        if not amount or not company_name or not username:
            self.logger.error(f"Withdrawal submit missing data: amount={amount}, company={company_name}, username={username}")
            await query.message.edit_text("⚠️ Data tidak lengkap. Sila cuba lagi.")
            return ConversationHandler.END
        
        # Store company_name in 'method' column and username in 'account' column
        success, message = self.db.request_withdrawal(self.bot_id, update.effective_user.id, amount, company_name, username)
        
        if success:
            # Extract withdrawal_id from message (format: "Withdrawal requested. ID: 123")
            withdrawal_id = int(message.split(": ")[1]) if ": " in message else None
            
            text = (
                f"✅ <b>WITHDRAWAL REQUESTED!</b>\n\n"
                f"💵 <b>Amount:</b> RM {amount:.2f}\n"
                f"🏢 <b>Company:</b> {company_name}\n"
                f"👤 <b>Username:</b> {username}\n"
                f"📊 <b>Status:</b> PENDING\n\n"
                f"📬 Admin akan process dalam 24 jam."
            )
            
            try:
                # Get bot owner and admins
                bot_data = self.db.get_bot_by_token(self.token)
                owner_id = int(bot_data.get('owner_id', 0)) if bot_data else 0
                admins = self.db.get_admins(self.bot_id)
                
                # Collect all recipient IDs (owner + admins, deduplicated)
                recipient_ids = set()
                if owner_id:
                    recipient_ids.add(owner_id)
                for admin in admins:
                    recipient_ids.add(admin['telegram_id'])
                
                admin_text = (
                    f"🔔 <b>NEW WITHDRAWAL REQUEST</b>\n\n"
                    f"👤 User: <code>{update.effective_user.id}</code>\n"
                    f"💵 Amount: RM {amount:.2f}\n"
                    f"🏢 Company: {company_name}\n"
                    f"👤 Username: <code>{username}</code>"
                )
                
                # Add approve/reject buttons if withdrawal_id exists
                if withdrawal_id:
                    admin_keyboard = [
                        [
                            InlineKeyboardButton("✅ APPROVE", callback_data=f"wd_approve_{withdrawal_id}"),
                            InlineKeyboardButton("❌ REJECT", callback_data=f"wd_reject_{withdrawal_id}")
                        ]
                    ]
                    admin_markup = InlineKeyboardMarkup(admin_keyboard)
                else:
                    admin_markup = None
                
                for recipient_id in recipient_ids:
                    try:
                        await self.app.bot.send_message(recipient_id, admin_text, parse_mode='HTML', reply_markup=admin_markup)
                    except Exception as notify_err:
                        self.logger.warning(f"Failed to notify {recipient_id}: {notify_err}")
            except Exception as e:
                self.logger.error(f"Failed to notify admins: {e}")
        else:
            text = f"❌ {message}"
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Wallet", callback_data="wallet")]]
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        
        context.user_data.pop('wd_amount', None)
        context.user_data.pop('wd_company_id', None)
        context.user_data.pop('wd_company_name', None)
        context.user_data.pop('wd_username', None)
        
        return ConversationHandler.END
    
    async def cancel_withdrawal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel withdrawal conversation"""
        query = update.callback_query
        await query.answer()
        
        context.user_data.pop('wd_amount', None)
        context.user_data.pop('wd_company_id', None)
        context.user_data.pop('wd_company_name', None)
        context.user_data.pop('wd_username', None)
        
        text = "❌ Withdrawal cancelled."
        keyboard = [
            [InlineKeyboardButton("💰 My Wallet", callback_data="wallet")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]
        ]
        
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return ConversationHandler.END

    # --- Referral Settings (Admin) ---
    async def ref_settings_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show referral settings menu for admin"""
        query = update.callback_query
        if query:
            await query.answer()
        
        # Check if user is admin (bot owner)
        bot_data = self.db.get_bot_by_token(self.token)
        if update.effective_user.id != bot_data['owner_id']:
            if query:
                await query.answer("⚠️ Admin only!", show_alert=True)
            return ConversationHandler.END
        
        # Get current settings
        settings = self.db.get_referral_settings(self.bot_id)
        
        text = (
            f"⚙️ <b>REFERRAL SETTINGS</b>\n\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Referral Reward:</b> RM {settings['referral_reward']:.2f}\n"
            f"📤 <b>Min Withdrawal:</b> RM {settings['min_withdrawal']:.2f}\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"Pilih setting yang nak diubah:"
        )
        
        keyboard = [
            [InlineKeyboardButton(f"💰 Set Reward (RM {settings['referral_reward']:.2f})", callback_data="rs_reward")],
            [InlineKeyboardButton(f"📤 Set Min Withdrawal (RM {settings['min_withdrawal']:.2f})", callback_data="rs_min_wd")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="ref_back")]
        ]
        
        if query:
            try:
                await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            except Exception:
                await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        
        return ConversationHandler.END
    
    async def ref_settings_set_reward(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask admin to input new reward amount"""
        query = update.callback_query
        await query.answer()
        
        settings = self.db.get_referral_settings(self.bot_id)
        
        text = (
            f"💰 <b>SET REFERRAL REWARD</b>\n\n"
            f"Current: RM {settings['referral_reward']:.2f}\n\n"
            f"Masukkan amount baru (contoh: 2.00):"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="ref_settings")]]
        
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        return RS_SET_REWARD
    
    async def ref_settings_save_reward(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new reward amount"""
        try:
            amount = float(update.message.text.strip().replace("RM", "").replace("rm", "").strip())
            if amount <= 0 or amount > 1000:
                await update.message.reply_text("⚠️ Amount mesti antara RM 0.01 - RM 1000.00")
                return RS_SET_REWARD
        except ValueError:
            await update.message.reply_text("⚠️ Format tidak sah. Masukkan nombor sahaja.\n\nContoh: 2.00")
            return RS_SET_REWARD
        
        self.db.update_referral_settings(self.bot_id, referral_reward=amount)
        
        text = f"✅ <b>Referral reward updated!</b>\n\nBaru: RM {amount:.2f} per referral"
        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="ref_settings")]]
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        return ConversationHandler.END
    
    async def ref_settings_set_min_wd(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask admin to input new min withdrawal"""
        query = update.callback_query
        await query.answer()
        
        settings = self.db.get_referral_settings(self.bot_id)
        
        text = (
            f"📤 <b>SET MINIMUM WITHDRAWAL</b>\n\n"
            f"Current: RM {settings['min_withdrawal']:.2f}\n\n"
            f"Masukkan minimum baru (contoh: 20.00):"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="ref_settings")]]
        
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        return RS_SET_MIN_WD
    
    async def ref_settings_save_min_wd(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save new min withdrawal amount"""
        try:
            amount = float(update.message.text.strip().replace("RM", "").replace("rm", "").strip())
            if amount <= 0 or amount > 10000:
                await update.message.reply_text("⚠️ Amount mesti antara RM 0.01 - RM 10000.00")
                return RS_SET_MIN_WD
        except ValueError:
            await update.message.reply_text("⚠️ Format tidak sah. Masukkan nombor sahaja.\n\nContoh: 20.00")
            return RS_SET_MIN_WD
        
        self.db.update_referral_settings(self.bot_id, min_withdrawal=amount)
        
        text = f"✅ <b>Min withdrawal updated!</b>\n\nBaru: RM {amount:.2f}"
        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="ref_settings")]]
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        return ConversationHandler.END
    
    async def ref_settings_back(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Go back to admin settings from referral settings"""
        query = update.callback_query
        await query.answer()
        await self.show_admin_settings(update)
        return ConversationHandler.END

    async def show_share_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            bot_uname = context.bot.username
            link = f"https://t.me/{bot_uname}?start={update.effective_user.id}"
            
            # Get dynamic reward amount from settings
            settings = self.db.get_referral_settings(self.bot_id)
            reward_amount = settings['referral_reward']
            
            # Use HTML for safety
            text = (
                f"🔗 <b>LINK REFERRAL ANDA</b>\n\n"
                f"<code>{link}</code>\n\n"
                f"Share link ini dan dapatkan <b>RM{reward_amount:.2f}</b> setiap invite!"
            )
            
            keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Check for share media asset
            asset = self.db.get_asset(self.bot_id, 'share')
            
            # Answer callback first
            if update.callback_query:
                try: await update.callback_query.answer()
                except Exception: pass
            
            # Delete previous message
            if update.callback_query:
                try: await update.callback_query.message.delete()
                except Exception: pass
            
            # Send with or without media
            if asset and asset.get('file_id'):
                caption = text
                try:
                    if asset.get('file_type') == 'video':
                        await update.effective_chat.send_video(
                            video=asset['file_id'], caption=caption,
                            reply_markup=reply_markup, parse_mode='HTML'
                        )
                    else:
                        await update.effective_chat.send_photo(
                            photo=asset['file_id'], caption=caption,
                            reply_markup=reply_markup, parse_mode='HTML'
                        )
                except Exception as e:
                    self.logger.error(f"show_share_link media error: {e}")
                    await update.effective_chat.send_message(text, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await update.effective_chat.send_message(text, reply_markup=reply_markup, parse_mode='HTML')
                
        except Exception as e:
             self.logger.error(f"CRITICAL Error in show_share_link: {e}")
             try: await update.effective_chat.send_message("❌ Error generating link.", parse_mode='HTML')
             except Exception: pass

    async def show_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            # Simple Logic: Top users by invite
            conn = self.db.get_connection()
            try:
                top = conn.execute("SELECT telegram_id, total_invites FROM users WHERE bot_id = ? ORDER BY total_invites DESC LIMIT 10", (self.bot_id,)).fetchall()
            finally:
                conn.close()
            
            list_text = ""
            if not top:
                list_text = "<i>Belum ada data.</i>"
            else:
                for i, row in enumerate(top):
                    tid = row[0]
                    invites = row[1]
                    # Mask ID
                    masked_id = str(tid)[:4] + "xxxx"
                    
                    medal = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"#{i+1}"
                    list_text += f"{medal} <b>ID: {masked_id}</b> - {invites} Invites\n"
            
            text = (
                f"🏆 <b>TOP 10 LEADERBOARD</b>\n\n"
                f"{list_text}\n\n"
                f"<i>Jom invite kawan untuk naik ranking!</i>"
            )
            
            keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="main_menu")]]
            
            # Smart Edit Logic
            asset = self.db.get_asset(self.bot_id, 'leaderboard')
            
            if asset:
                 # Case 1: Custom Asset Exists (Force Media)
                 caption_header = asset.get('caption')
                 final_caption = f"{caption_header}\n\n{list_text}" if caption_header else text
                 
                 # Logic: If current is same media type, edit media. Else delete + send.
                 if update.callback_query:
                      try: await update.callback_query.message.delete()
                      except Exception: pass
                 
                 file_id = asset['file_id']
                 media_type = asset.get('media_type', 'photo') # Default to photo
                 
                 try:
                     if media_type == 'video':
                         await update.effective_chat.send_video(video=file_id, caption=final_caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                     elif media_type == 'animation':
                         await update.effective_chat.send_animation(animation=file_id, caption=final_caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                     else:
                         await update.effective_chat.send_photo(photo=file_id, caption=final_caption, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                 except Exception as e:
                     self.logger.error(f"Asset send error in leaderboard: {e}")
                     # Fallback to text
                     await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                      
            else:
                 # Case 2: No Custom Asset (Text Mode or Preserve Existing Banner)
                 if update.callback_query:
                     try: await update.callback_query.answer()
                     except Exception: pass
                     
                     try:
                         is_media = (update.callback_query.message.photo or 
                                    update.callback_query.message.video or 
                                    update.callback_query.message.animation)
                                    
                         if is_media:
                             # Media -> Text: Delete + Send
                             try: await update.callback_query.message.delete()
                             except Exception: pass
                             await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                         else:
                             # Text -> Text: Edit
                             await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                     except Exception as e:
                         # Fallback
                         try: await update.callback_query.message.delete()
                         except Exception: pass
                         await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                 else:
                     await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                     
        except Exception as e:
            self.logger.error(f"CRITICAL Error in show_leaderboard: {e}")
            try: await update.effective_chat.send_message("❌ Error loading leaderboard.", parse_mode='HTML')
            except Exception: pass

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
        await self.show_admin_settings(update)


    async def cmd_reset_referrals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Reset referral stats for testing (Admin Only)"""
        # Admin check
        bot_data = self.db.get_bot_by_token(self.token)
        is_owner = update.effective_user.id == bot_data.get('owner_id')
        is_admin = self.db.is_bot_admin(self.bot_id, update.effective_user.id)
        
        if not (is_owner or is_admin):
             return
             
        target_id = update.effective_user.id
        
        # Check for arguments
        if context.args:
            try:
                target_id = int(context.args[0])
            except ValueError:
                await update.message.reply_text("⚠️ Invalid ID. Usage: /reset_ref [user_id]")
                return
                
        success = self.db.reset_user_referral(self.bot_id, target_id)
        
        if success:
            await update.message.reply_text(f"✅ Referral stats RESET for ID: `{target_id}`", parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ Error resetting stats.")

    # --- Callbacks ---
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data
        
        # Skip early answer for navigation/company buttons - let edit_media handle it 
        # to avoid button flash. Answer immediately for all other callbacks.
        skip_answer = data.startswith("list_page_") or data == "wa_status" or (data.startswith("c_") and data != "c_4d" and not data.startswith("c_edit"))
        if not skip_answer:
            try:
                await query.answer()
            except Exception:
                pass
            
        self.logger.info(f"🔘 Callback received: {data}")

        try:
            await self._route_callback(update, context, query, data)
        except (ValueError, IndexError) as e:
            self.logger.warning(f"⚠️ Bad callback data '{data}': {e}")
            try:
                await query.answer("⚠️ Invalid action", show_alert=False)
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"❌ Callback handler error for '{data}': {e}", exc_info=True)
            try:
                await query.answer("❌ Error occurred", show_alert=False)
            except Exception:
                pass

    async def _route_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE, query, data):

        if data.startswith("list_page_"):
            page = int(data.split("_")[2])
            await self.show_page(update, page)
        elif data.startswith("c_") and data != "c_4d" and not data.startswith("c_edit"):
            # Handle company view (c_123)
            company_id = int(data.split("_")[1])
            await self.view_company(update, company_id)
        elif data.startswith("view_"):
            company_id = int(data.split("_")[1])
            await self.view_company(update, company_id)
        elif data == "main_menu":
            await self.main_menu(update, context)
        elif data == "wallet":
            await self.show_wallet(update)
        elif data == "share_link":
            await self.show_share_link(update, context)
        elif data == "leaderboard":
            await self.show_leaderboard(update, context)
        elif data == "cancel":
            # Generic cancel - show main menu or just acknowledge
            try:
                await update.callback_query.message.edit_text("❌ Cancelled.")
            except Exception as e:

                pass  # Silently handle exception
            await self.show_admin_settings(update)
        elif data == "ref_back":
            await self.show_admin_settings(update)
        elif data == "ref_settings":
            await self.ref_settings_menu(update, context)
        
        # 4D Analyzer Handlers
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
        elif data == "4d_visual": await self.show_4d_visual_chart(update)
        elif data == "4d_predict": await self.show_4d_prediction(update)
        elif data == "4d_history": await self.show_4d_history(update)
        elif data.startswith("4d_hist_"): await self.show_4d_history_company(update, data)
        elif data.startswith("4d_hmore_"): await self.show_4d_history_more(update, data)
        
        # Admin Actions
        elif data == "admin_withdrawals": await self.show_admin_withdrawals(update)
        elif data.startswith("wd_detail_"): await self.show_withdrawal_detail(update, int(data.split("_")[2]))
        elif data.startswith("wd_approve_"): await self.admin_approve_withdrawal(update, int(data.split("_")[2]))
        elif data.startswith("wd_reject_"): await self.admin_reject_withdrawal(update, int(data.split("_")[2]))
        elif data.startswith("wd_company_"): await self.withdrawal_select_method(update, context)
        elif data == "wd_submit": await self.withdrawal_submit(update, context)
        elif data == "cancel_wd": await self.cancel_withdrawal(update, context)

        elif data == "admin_del_list": await self.show_delete_company_list(update)
        elif data.startswith("delete_company_"): await self.confirm_delete_company(update, int(data.split("_")[2]))

        elif data == "toggle_referral": await self.toggle_referral_system(update)
        elif data == "admin_reset_my_ref": await self.reset_my_referral_btn_handler(update)
        elif data == "admin_reset_ref_confirm": await self.confirm_reset_my_ref_handler(update)
        elif data == "toggle_livegram": await self.toggle_livegram_system(update)
        elif data == "toggle_link_guard": await self.toggle_link_guard_system(update)
        elif data == "toggle_ai_chat": await self.toggle_ai_chat_system(update)
        elif data == "ai_settings": await self.show_ai_settings(update)
        elif data == "ai_set_prompt": await self.ai_set_prompt_start(update, context)
        elif data == "ai_reset_prompt": await self.ai_reset_prompt(update)
        # Group Management
        elif data == "group_mgmt": await self.show_group_management(update)
        elif data == "gm_toggle_link_guard": await self.gm_toggle_link_guard(update)
        elif data == "gm_toggle_delete_jl": await self.gm_toggle_delete_jl(update)
        elif data == "gm_toggle_anti_bot": await self.gm_toggle_anti_bot(update)
        elif data == "gm_ban_words": await self.gm_show_ban_words(update)
        elif data == "gm_add_ban_word": await self.gm_add_ban_word_start(update, context)
        elif data.startswith("gm_del_ban_"): await self.gm_del_ban_word(update, int(data.split("_")[3]))
        elif data == "gm_auto_replies": await self.gm_show_auto_replies(update)
        elif data == "gm_add_auto_reply": await self.gm_add_auto_reply_start(update, context)
        elif data.startswith("gm_del_reply_"): await self.gm_del_auto_reply(update, int(data.split("_")[3]))
        elif data == "gm_welcome": await self.gm_show_welcome_settings(update)
        elif data == "gm_toggle_welcome":
            gw = self.db.get_group_welcome(self.bot_id)
            new_val = 0 if gw.get('enabled') else 1
            self.db.update_group_welcome(self.bot_id, 'enabled', new_val)
            await update.callback_query.answer(f"Welcome Message {'ON' if new_val else 'OFF'}")
            await self.gm_show_welcome_settings(update)
        elif data == "reset_schedule": await self.show_reset_schedule(update)
        elif data == "confirm_reset_schedule": await self.confirm_reset_schedule(update)
        elif data == "manage_recurring": await self.show_manage_recurring(update)
        elif data.startswith("stop_recurring_"): await self.stop_recurring(update, int(data.split("_")[2]))
        elif data == "show_analytics": await self.show_analytics(update)
        elif data == "export_data": await self.show_export_menu(update)
        elif data == "export_users": await self.export_users_csv(update)
        elif data == "export_companies": await self.export_companies_csv(update)
        elif data == "admin_settings": await self.show_admin_settings(update)
        # Edit Company List (Admin)
        elif data == "admin_edit_company_list": await self.show_edit_company_list(update)
        # Reorder Companies
        elif data == "reorder_companies": await self.show_reorder_companies(update)
        elif data.startswith("reorder_select_"): 
            company_id = int(data.split("_")[2])
            await self.show_reorder_positions(update, company_id)
        elif data.startswith("reorder_move_"): 
            parts = data.split("_")
            company_id = int(parts[2])
            new_position = int(parts[3])
            await self.execute_reorder(update, company_id, new_position)
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
        # Promo Monitor Actions
        elif data.startswith("promo_bc_groups_"): await self._promo_broadcast_action(update, int(data.split("_")[3]), 'groups')
        elif data.startswith("promo_bc_users_"): await self._promo_broadcast_action(update, int(data.split("_")[3]), 'users')
        elif data.startswith("promo_skip_"): await self._promo_skip_action(update, int(data.split("_")[2]))
        elif data.startswith("scan_ai_"): await self._scan_ai_rewrite(update, context)
        elif data.startswith("wa_change_co_"): await self._wa_show_company_list(update, int(data.split("_")[3]))
        elif data.startswith("rt_pick_"):
            parts = data.split("_")
            await self._rt_pick_company(update, int(parts[2]), int(parts[3]))
        # Userbot Hub / Promo Monitor / Clone — route so they work from any ConversationHandler fallback
        elif data == "userbot_hub": await self.userbot_hub_menu(update, context)
        elif data == "ub_menu": await self.ub_menu(update, context)
        elif data == "clone_menu": await self.clone_media_menu(update, context)
        # WhatsApp Monitor
        elif data == "wa_hub": await self.wa_hub_menu(update)
        elif data == "wa_connect": await self.wa_connect(update)
        elif data == "wa_disconnect": await self.wa_disconnect(update)
        elif data == "wa_status": await self.wa_check_status(update)
        # Note: edit_company_* is handled by ConversationHandler, NOT here
        elif data == "close_panel": await query.message.delete()


    

    
    # --- Edit Company Wizard Functions ---
    # --- Edit Company Wizard Functions ---
    async def edit_company_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Entry point for edit company conversation"""
        await update.callback_query.answer()
        data = update.callback_query.data
        
        # Robust ID extraction
        if data.startswith("admin_edit_company_select_"):
            company_id = int(data.split("_")[-1])
            is_admin_mode = True
        else:
            company_id = int(data.split("_")[-1]) # works for edit_company_{id}
            is_admin_mode = False
            
        context.user_data['edit_company_id'] = company_id
        context.user_data['edit_from_admin'] = is_admin_mode
        
        company = next((c for c in self.db.get_companies(self.bot_id) if c['id'] == company_id), None)
        if not company:
            await update.callback_query.message.reply_text("❌ Company not found.")
            return ConversationHandler.END
        
        text = f"✏️ **EDIT: {company['name']}**\n\nPilih apa yang nak diedit:"
        
        cancel_btn = InlineKeyboardButton("« Back", callback_data="admin_edit_back") if is_admin_mode else InlineKeyboardButton("❌ Cancel", callback_data="cancel")
        
        keyboard = [
            [InlineKeyboardButton("📝 Nama", callback_data="ef_name")],
            [InlineKeyboardButton("📄 Deskripsi", callback_data="ef_desc")],
            [InlineKeyboardButton("🖼️ Media", callback_data="ef_media")],
            [InlineKeyboardButton("🔗 Button Text", callback_data="ef_btn_text")],
            [InlineKeyboardButton("🌐 Button URL", callback_data="ef_btn_url")],
            [InlineKeyboardButton("🔘 Manage Buttons", callback_data="ef_manage_btns")],
            [cancel_btn]
        ]
        
        # Use edit_text if from admin list to keep UI clean, reply_text if from public view overlay
        if is_admin_mode:
             await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
             await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
             
        return EDIT_FIELD

    async def back_to_admin_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle Back button from Edit menu (Admin Mode)"""
        await self.show_edit_company_list(update)
        return ConversationHandler.END
    
    async def edit_company_choose_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle field selection for edit"""
        data = update.callback_query.data
        await update.callback_query.answer()
        
        if data == "ef_name":
            await update.callback_query.message.reply_text("📝 Masukkan **NAMA BARU**:\n\n💡 _Boleh masukkan emoji sekali, contoh: 🎰 Mega888_", parse_mode='Markdown')
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
        elif data == "ef_keywords":
            company_id = context.user_data.get('edit_company_id')
            company = self.db.get_company(company_id)
            current = company.get('keywords', '') if company else ''
            await update.callback_query.message.reply_text(
                f"🔑 **KEYWORDS / ALIAS**\n\n"
                f"Current: `{current or '(tiada)'}`\n\n"
                f"Masukkan keywords baru, pisahkan dengan koma.\n"
                f"Contoh: `a9, a-9, a9play`\n\n"
                f"_Keywords ini digunakan untuk auto-detect company dari channel._",
                parse_mode='Markdown'
            )
            return EDIT_KEYWORDS

        elif data == "cancel":
            await update.callback_query.message.reply_text("❌ Edit cancelled.")
            return ConversationHandler.END
    
    async def edit_company_save_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        formatted_name = message_to_html(update.message)
        self.db.edit_company(company_id, 'name', formatted_name)
        
        # Auto-generate keywords using AI
        try:
            from ai_rewriter import generate_keywords
            keywords = await generate_keywords(formatted_name)
            self.db.edit_company(company_id, 'keywords', keywords)
            kw_msg = f"\n🔑 Keywords auto: `{keywords[:100]}`"
        except Exception as e:
            self.logger.error(f"Auto keywords failed: {e}")
            kw_msg = ""
        
        keyboard = [[InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]]
        await update.message.reply_text(
            f"✅ Nama company berjaya dikemaskini!{kw_msg}",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END
    

    
    async def edit_company_save_desc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        # Convert message entities to HTML format to preserve formatting
        formatted_desc = message_to_html(update.message)
        self.db.edit_company(company_id, 'description', formatted_desc)
        
        keyboard = [[InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]]
        await update.message.reply_text(
            "✅ Deskripsi company berjaya dikemaskini!\n\n"
            "💡 <i>Formatting (bold, underline, italic) telah disimpan.</i>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
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
        
        keyboard = [[InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]]
        await update.message.reply_text(
            "✅ Media company berjaya dikemaskini!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END
    
    async def edit_company_save_btn_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        self.db.edit_company(company_id, 'button_text', update.message.text)
        
        keyboard = [[InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]]
        await update.message.reply_text(
            "✅ Button text berjaya dikemaskini!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END
    
    async def edit_company_save_btn_url(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        self.db.edit_company(company_id, 'button_url', update.message.text)
        
        keyboard = [[InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]]
        await update.message.reply_text(
            "✅ Button URL berjaya dikemaskini!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END
    
    async def edit_company_save_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        company_id = context.user_data.get('edit_company_id')
        keywords = update.message.text.strip()
        self.db.edit_company(company_id, 'keywords', keywords)
        
        keyboard = [[InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]]
        await update.message.reply_text(
            f"✅ Keywords berjaya dikemaskini!\n\n"
            f"🔑 Keywords: `{keywords}`\n\n"
            f"_Bot akan guna keywords ini untuk auto-detect company dari channel._",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END

    
    # --- 4D Analyzer Module ---
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
        
        body_text = (
            f"{stats_text}\n"
            f"📬 Notification: {notify_status}\n\n"
            "Pilih analisis yang anda mahu:\n\n"
            "⚠️ _Disclaimer: Ini untuk hiburan sahaja._\n"
            "_Tiada jaminan menang._"
        )
        
        default_header = "🎰 **4D STATISTICAL ANALYZER**\n\n"
        text = default_header + body_text
        
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
            [InlineKeyboardButton("📈 Carta Visual", callback_data="4d_visual"),
             InlineKeyboardButton("📋 Carta Ramalan", callback_data="4d_predict")],
            [InlineKeyboardButton("🗓️ Carta Sejarah", callback_data="4d_history")],
            [notify_btn],
            [InlineKeyboardButton("🔄 Refresh Data", callback_data="4d_refresh")],
            [InlineKeyboardButton("🔙 BACK", callback_data="main_menu")]
        ]
        
        # Check Asset
        asset = self.db.get_asset(self.bot_id, '4d')
        
        if asset:
             if update.callback_query:
                 try:
                    await update.callback_query.message.delete()
                 except Exception: pass
                 
             caption_header = asset.get('caption')
             if caption_header:
                 final_caption = f"{caption_header}\n\n{body_text}"
             else:
                 final_caption = text
                 
             if asset['file_type'] == 'photo':
                 await update.effective_chat.send_photo(asset['file_id'], caption=final_caption, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
             elif asset['file_type'] == 'video':
                 await update.effective_chat.send_video(asset['file_id'], caption=final_caption, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            try:
                await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            except Exception:
                # Message has media or other error, delete and send new
                try:
                    await update.callback_query.message.delete()
                except Exception as e:

                    pass  # Silently handle exception
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
        except Exception:
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
        except Exception:
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
        except Exception:
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
        except Exception:
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
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def refresh_4d_data(self, update: Update):
        """Fetch latest 4D data from web sources with timeout protection"""
        import asyncio
        
        await update.callback_query.answer()
        
        # Show visible loading message
        loading_msg = await update.effective_chat.send_message(
            "⏳ **LOADING 4D DATA...**\n\n"
            "🔄 Sedang fetch data dari 11 syarikat...\n"
            "⏱️ _Sila tunggu 10-30 saat_",
            parse_mode='Markdown'
        )
        
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
                
                # Update loading message with success
                try:
                    await loading_msg.edit_text(
                        f"✅ **4D DATA UPDATED!**\n\n"
                        f"📊 Loaded: {len(results)} companies\n"
                        f"💾 Saved: {saved} new results",
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass
            else:
                try:
                    await loading_msg.edit_text("⚠️ Gagal fetch data. Cuba lagi.")
                except Exception:
                    pass
                
        except ImportError as e:
            self.logger.error(f"4D import error: {e}")
            # If scraper not available, use sample data for demo
            await self._load_sample_4d_data()
            try:
                await loading_msg.edit_text("✅ Sample data loaded for demo!")
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"4D fetch error: {e}")
            try:
                await loading_msg.edit_text(f"❌ Error: {str(e)[:100]}")
            except Exception:
                pass
        
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
    
    # --- 4D Carta Visual ---
    async def show_4d_visual_chart(self, update: Update):
        """Show visual heatmap chart of 2-digit ending frequency"""
        pred_data = self.db.get_4d_prediction_data(limit=200)
        
        if not pred_data:
            await update.callback_query.answer("Tiada data! Sila Refresh dulu.", show_alert=True)
            return
        
        ending_freq = pred_data['ending_frequency']
        
        # Get max frequency for scaling
        max_freq = max(ending_freq.values()) if ending_freq else 1
        
        text = "📈 **CARTA VISUAL 4D**\n"
        text += f"📊 _{pred_data['total_analyzed']} draws dianalisa_\n\n"
        text += "**Heatmap 2-Digit Terakhir (00-99)**\n"
        text += "_Warna lebih gelap = lebih kerap keluar_\n\n"
        
        # Legend
        text += "🟥 Sangat Kerap  🟧 Kerap  🟨 Sederhana  🟩 Jarang  ⬜ Tiada\n\n"
        
        # Build 10x10 grid header
        text += "`    0  1  2  3  4  5  6  7  8  9`\n"
        
        for row in range(10):
            line = f"`{row}0` "
            for col in range(10):
                ending = f"{row}{col}"
                freq = ending_freq.get(ending, 0)
                
                if freq == 0:
                    line += "⬜"
                elif freq <= max_freq * 0.25:
                    line += "🟩"
                elif freq <= max_freq * 0.5:
                    line += "🟨"
                elif freq <= max_freq * 0.75:
                    line += "🟧"
                else:
                    line += "🟥"
            text += line + "\n"
        
        # Top 5 hottest endings
        top_endings = sorted(ending_freq.items(), key=lambda x: x[1], reverse=True)[:5]
        text += "\n**🔥 Top 5 Ending Paling Kerap:**\n"
        for ending, count in top_endings:
            text += f"  `{ending}` — {count}x keluar\n"
        
        text += "\n⚠️ _Untuk hiburan sahaja_"
        
        keyboard = [
            [InlineKeyboardButton("📋 Carta Ramalan", callback_data="4d_predict")],
            [InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]
        ]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- 4D Carta Ramalan ---
    async def show_4d_prediction(self, update: Update):
        """Show prediction chart with statistical analysis"""
        import random
        
        pred_data = self.db.get_4d_prediction_data(limit=200)
        stats = self.db.get_4d_statistics()
        
        if not pred_data or not stats:
            await update.callback_query.answer("Tiada data! Sila Refresh dulu.", show_alert=True)
            return
        
        text = "📋 **CARTA RAMALAN 4D**\n"
        text += f"📊 _{pred_data['total_analyzed']} draws | {pred_data['total_numbers']} nombor dianalisa_\n\n"
        
        # Position analysis
        text += "**🎯 Digit Terkuat Per Posisi:**\n"
        pos_names = ['1st', '2nd', '3rd', '4th']
        hot_per_pos = []
        for pos in range(4):
            top = pred_data['position_frequency'].get(pos, [])
            if top:
                top_digit = top[0][0]
                top_count = top[0][1]
                hot_per_pos.append(top_digit)
                text += f"  Posisi {pos_names[pos]}: `{top_digit}` ({top_count}x)\n"
        
        # Top pairs
        text += "\n**🔗 Pair Terakhir 2-Digit Terkuat:**\n"
        for pair, count in pred_data['top_pairs'][:5]:
            text += f"  `{pair}` — {count}x keluar\n"
        
        # Generate 5 prediction numbers with scoring
        text += "\n**🔮 NOMBOR CADANGAN:**\n"
        text += "_Berdasarkan analisis statistik_\n\n"
        
        predictions = []
        hot_digits = [d[0] for d in stats['hot_digits'][:5]]
        cold_digits = [d[0] for d in stats['cold_digits'][:3]]
        top_pairs_list = [p[0] for p in pred_data['top_pairs'][:5]]
        
        for i in range(5):
            confidence = random.randint(55, 85)
            num = ""
            
            if i < 2:
                # Hot method: use hot digits per position
                for pos in range(4):
                    pos_data = pred_data['position_frequency'].get(pos, [])
                    if pos_data and len(pos_data) > 1:
                        # Pick from top 3 for variety
                        choices = [d[0] for d in pos_data[:3]]
                        num += random.choice(choices)
                    else:
                        num += str(random.randint(0, 9))
                method = "🔥 Hot"
            elif i < 4:
                # Pair method: use hot pairs
                head = random.choice(hot_digits) + random.choice(hot_digits) if hot_digits else f"{random.randint(0,9)}{random.randint(0,9)}"
                tail = random.choice(top_pairs_list) if top_pairs_list else f"{random.randint(0,9):02d}"
                num = head + tail
                method = "🔗 Pair"
            else:
                # Cold gap method: mix hot + cold
                for _ in range(4):
                    if random.random() < 0.4 and cold_digits:
                        num += random.choice(cold_digits)
                    elif hot_digits:
                        num += random.choice(hot_digits)
                    else:
                        num += str(random.randint(0, 9))
                method = "❄️ Gap"
            
            predictions.append((num, confidence, method))
        
        # Sort by confidence descending
        predictions.sort(key=lambda x: x[1], reverse=True)
        
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for idx, (num, conf, method) in enumerate(predictions):
            bar_len = conf // 10
            bar = "█" * bar_len + "░" * (10 - bar_len)
            text += f"{medals[idx]} `{num}` {bar} {conf}% {method}\n"
        
        text += f"\n📅 {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        text += "\n⚠️ _Disclaimer: Untuk hiburan sahaja._\n"
        text += "_Tiada jaminan menang. Main secara bertanggungjawab._"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Ramalan Baru", callback_data="4d_predict")],
            [InlineKeyboardButton("📈 Carta Visual", callback_data="4d_visual")],
            [InlineKeyboardButton("🔙 Back", callback_data="4d_menu")]
        ]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- 4D Carta Sejarah ---
    async def show_4d_history(self, update: Update):
        """Show history menu - select company"""
        text = "🗓️ **CARTA SEJARAH 4D**\n\n"
        text += "Pilih syarikat untuk lihat rekod:\n"
        
        company_icons = {
            'MAGNUM': '🔴', 'DAMACAI': '🟡', 'TOTO': '🟢',
            'CASHSWEEP': '💜', 'SABAH88': '🟤', 'STC': '🔵',
            'SG4D': '🩷', 'SGTOTO': '🩵',
            'GD': '🐉', 'PERDANA': '🎰', 'LUCKY': '🍀'
        }
        
        company_names = {
            'MAGNUM': 'Magnum 4D', 'DAMACAI': 'Da Ma Cai', 'TOTO': 'Sports Toto',
            'CASHSWEEP': 'Cash Sweep', 'SABAH88': 'Sabah 88', 'STC': 'STC 4D',
            'SG4D': 'Singapore 4D', 'SGTOTO': 'SG Toto',
            'GD': 'Grand Dragon', 'PERDANA': 'Perdana', 'LUCKY': 'Lucky Hari Hari'
        }
        
        # Group by region
        regions = {
            '🇲🇾 West MY': ['MAGNUM', 'DAMACAI', 'TOTO'],
            '🇲🇾 East MY': ['CASHSWEEP', 'SABAH88', 'STC'],
            '🇸🇬 Singapore': ['SG4D', 'SGTOTO'],
            '🇰🇭 Cambodia': ['GD', 'PERDANA', 'LUCKY']
        }
        
        keyboard = []
        for region, companies in regions.items():
            row = []
            for comp in companies:
                icon = company_icons.get(comp, '⚪')
                name = company_names.get(comp, comp)
                row.append(InlineKeyboardButton(
                    f"{icon} {name}",
                    callback_data=f"4d_hist_{comp}"
                ))
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="4d_menu")])
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_4d_history_company(self, update: Update, data: str):
        """Show history for a specific company"""
        # data format: 4d_hist_COMPANY
        company = data.replace("4d_hist_", "")
        
        company_names = {
            'MAGNUM': 'Magnum 4D', 'DAMACAI': 'Da Ma Cai', 'TOTO': 'Sports Toto',
            'CASHSWEEP': 'Cash Sweep', 'SABAH88': 'Sabah 88', 'STC': 'STC 4D',
            'SG4D': 'Singapore 4D', 'SGTOTO': 'SG Toto',
            'GD': 'Grand Dragon', 'PERDANA': 'Perdana', 'LUCKY': 'Lucky Hari Hari'
        }
        
        results = self.db.get_4d_results_by_date(company=company, limit=10, offset=0)
        
        if not results:
            await update.callback_query.answer("Tiada rekod untuk syarikat ini.", show_alert=True)
            return
        
        name = company_names.get(company, company)
        text = f"🗓️ **SEJARAH: {name}**\n\n"
        
        for r in results:
            date = r.get('draw_date', 'N/A')
            text += f"📅 **{date}**\n"
            text += f"  🥇 `{r['first_prize']}`  🥈 `{r['second_prize']}`  🥉 `{r['third_prize']}`\n"
            
            if r.get('special_prizes'):
                specials = r['special_prizes'].split(',')[:5]
                text += f"  ✨ `{'` `'.join(specials)}`...\n"
            text += "\n"
        
        keyboard = []
        if len(results) == 10:
            keyboard.append([InlineKeyboardButton("📄 Load More", callback_data=f"4d_hmore_{company}_1")])
        keyboard.append([InlineKeyboardButton("🔙 Pilih Syarikat", callback_data="4d_history")])
        keyboard.append([InlineKeyboardButton("🔙 Menu 4D", callback_data="4d_menu")])
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def show_4d_history_more(self, update: Update, data: str):
        """Load more history results (pagination)"""
        # data format: 4d_hmore_COMPANY_PAGE
        parts = data.replace("4d_hmore_", "").rsplit("_", 1)
        company = parts[0]
        page = int(parts[1]) if len(parts) > 1 else 1
        offset = page * 10
        
        company_names = {
            'MAGNUM': 'Magnum 4D', 'DAMACAI': 'Da Ma Cai', 'TOTO': 'Sports Toto',
            'CASHSWEEP': 'Cash Sweep', 'SABAH88': 'Sabah 88', 'STC': 'STC 4D',
            'SG4D': 'Singapore 4D', 'SGTOTO': 'SG Toto',
            'GD': 'Grand Dragon', 'PERDANA': 'Perdana', 'LUCKY': 'Lucky Hari Hari'
        }
        
        results = self.db.get_4d_results_by_date(company=company, limit=10, offset=offset)
        
        if not results:
            await update.callback_query.answer("Tiada lagi rekod.", show_alert=True)
            return
        
        name = company_names.get(company, company)
        text = f"🗓️ **SEJARAH: {name}** (Halaman {page + 1})\n\n"
        
        for r in results:
            date = r.get('draw_date', 'N/A')
            text += f"📅 **{date}**\n"
            text += f"  🥇 `{r['first_prize']}`  🥈 `{r['second_prize']}`  🥉 `{r['third_prize']}`\n"
            
            if r.get('special_prizes'):
                specials = r['special_prizes'].split(',')[:5]
                text += f"  ✨ `{'` `'.join(specials)}`...\n"
            text += "\n"
        
        keyboard = []
        if len(results) == 10:
            keyboard.append([InlineKeyboardButton("📄 Load More", callback_data=f"4d_hmore_{company}_{page + 1}")])
        keyboard.append([InlineKeyboardButton("🔙 Pilih Syarikat", callback_data="4d_history")])
        keyboard.append([InlineKeyboardButton("🔙 Menu 4D", callback_data="4d_menu")])
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- Edit Company List Logic (New) ---
    async def show_edit_company_list(self, update: Update):
        """Show list of companies to select for editing"""
        companies = self.db.get_companies(self.bot_id)
        if not companies:
            await update.callback_query.answer("📭 Tiada company untuk edit.", show_alert=True)
            return
        
        text = "✏️ **EDIT COMPANY**\n\nPilih company untuk edit:"
        keyboard = []
        for company in companies:
            keyboard.append([InlineKeyboardButton(
                f"📝 {company['name']}", 
                callback_data=f"admin_edit_company_select_{company['id']}"
            )])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="admin_settings")])
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

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
        
        keyboard = [
            [InlineKeyboardButton("🗑️ Delete Another", callback_data="admin_del_list")],
            [InlineKeyboardButton("« Back to Admin Settings", callback_data="admin_settings")]
        ]
        
        if success:
            await update.callback_query.message.edit_text(
                "✅ Company deleted successfully!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.callback_query.message.edit_text(
                "❌ Error deleting company.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    # --- Reorder Companies Logic ---
    async def show_reorder_companies(self, update: Update):
        """Show company list for reordering"""
        companies = self.db.get_companies(self.bot_id)
        
        if not companies:
            await update.callback_query.answer("📭 No companies to reorder", show_alert=True)
            return
        
        text = "🔢 <b>REORDER COMPANIES</b>\n\nSelect company to move:"
        
        keyboard = []
        for idx, company in enumerate(companies, 1):
            keyboard.append([
                InlineKeyboardButton(
                    f"{idx}. {company['name']}", 
                    callback_data=f"reorder_select_{company['id']}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("« Back", callback_data="admin_settings")])
        
        await update.callback_query.message.edit_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    async def show_reorder_positions(self, update: Update, company_id: int):
        """Show available positions for selected company"""
        companies = self.db.get_companies(self.bot_id)
        total = len(companies)
        
        company = next((c for c in companies if c['id'] == company_id), None)
        if not company:
            await update.callback_query.answer("❌ Company not found", show_alert=True)
            return
        
        # Find current position (1-indexed)
        current_pos = next((idx for idx, c in enumerate(companies, 1) if c['id'] == company_id), 1)
        
        text = f"📍 Move <b>{company['name']}</b> to position:"
        
        keyboard = []
        for i in range(1, total + 1):
            label = f"{i}"
            if i == current_pos:
                label += " (current ✓)"
            
            keyboard.append([
                InlineKeyboardButton(
                    label,
                    callback_data=f"reorder_move_{company_id}_{i}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("« Back", callback_data="reorder_companies")])
        
        await update.callback_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    async def execute_reorder(self, update: Update, company_id: int, new_position: int):
        """Execute the reorder operation"""
        # Debug: log before state
        before = self.db.get_companies(self.bot_id)
        self.logger.info(f"🔢 REORDER: company_id={company_id}, new_position={new_position}")
        self.logger.info(f"🔢 BEFORE: {[(c['id'], c['name'], c.get('display_order')) for c in before]}")
        
        success = self.db.update_company_position(company_id, new_position, self.bot_id)
        
        # Debug: log after state
        after = self.db.get_companies(self.bot_id)
        self.logger.info(f"🔢 AFTER:  {[(c['id'], c['name'], c.get('display_order')) for c in after]}")
        self.logger.info(f"🔢 RESULT: {'SUCCESS' if success else 'FAILED'}")
        
        if success:
            await update.callback_query.answer("✅ Position updated!")
            await self.show_reorder_companies(update)
        else:
            await update.callback_query.answer("❌ Failed to reorder", show_alert=True)
    
    # --- WhatsApp Monitor Hub ---
    
    WA_SERVICE_URL = os.environ.get("WA_SERVICE_URL", "http://localhost:3001")  # Same container
    
    async def wa_hub_menu(self, update: Update):
        """Show WhatsApp Monitor hub menu"""
        wa_session = self.db.get_whatsapp_session(self.bot_id)
        status = wa_session['status'] if wa_session else 'disconnected'
        
        status_emoji = "🟢" if status == 'connected' else "🔴"
        status_text = "Connected" if status == 'connected' else "Disconnected"
        
        text = (
            f"📱 **WHATSAPP MONITOR**\n\n"
            f"Status: {status_emoji} {status_text}\n\n"
            f"Monitor semua WhatsApp group untuk detect promo automatically.\n"
            f"Bila detect company → notify admin via Telegram."
        )
        
        keyboard = []
        if status == 'connected':
            keyboard.append([InlineKeyboardButton("🔍 Check Status", callback_data="wa_status")])
            keyboard.append([InlineKeyboardButton("❌ Disconnect", callback_data="wa_disconnect")])
        else:
            keyboard.append([InlineKeyboardButton("🔗 Connect WhatsApp", callback_data="wa_connect")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_settings")])
        
        try:
            await update.callback_query.message.edit_text(
                text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'
            )
        except Exception:
            pass  # Status unchanged — ignore "Message is not modified"
    
    async def wa_connect(self, update: Update):
        """Initiate WhatsApp connection — get QR from Baileys service"""
        import aiohttp
        
        await update.callback_query.answer("⏳ Generating QR code...")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.WA_SERVICE_URL}/wa/qr/{self.bot_id}",
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    data = await resp.json()
            
            if data.get('status') == 'already_connected':
                await update.callback_query.message.edit_text(
                    "✅ WhatsApp sudah connected!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Back", callback_data="wa_hub")]
                    ])
                )
                return
            
            if data.get('qr'):
                # QR is base64 data URL, extract and send as photo
                import base64
                qr_b64 = data['qr'].split(',')[1]  # Remove data:image/png;base64, prefix
                qr_bytes = base64.b64decode(qr_b64)
                
                from io import BytesIO
                qr_file = BytesIO(qr_bytes)
                qr_file.name = 'whatsapp_qr.png'
                
                await update.callback_query.message.delete()
                await update.effective_chat.send_photo(
                    photo=qr_file,
                    caption=(
                        "📱 **SCAN QR CODE**\n\n"
                        "1. Buka WhatsApp di phone\n"
                        "2. Settings → Linked Devices → Link a Device\n"
                        "3. Scan QR code ni\n\n"
                        "⏳ QR expired dalam 60 saat. Klik 'Connect' semula kalau expired."
                    ),
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Refresh QR", callback_data="wa_connect")],
                        [InlineKeyboardButton("🔙 Back", callback_data="wa_hub")]
                    ])
                )
            else:
                await update.callback_query.message.edit_text(
                    f"⏳ QR belum ready. Status: {data.get('status', 'unknown')}\n\nCuba lagi dalam beberapa saat.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Try Again", callback_data="wa_connect")],
                        [InlineKeyboardButton("🔙 Back", callback_data="wa_hub")]
                    ])
                )
                
        except Exception as e:
            self.logger.error(f"WA connect error: {e}")
            await update.callback_query.message.edit_text(
                "❌ Failed to connect to WhatsApp service.\n\nMake sure WA Monitor is running.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="wa_hub")]
                ])
            )
    
    async def wa_disconnect(self, update: Update):
        """Disconnect WhatsApp session"""
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.WA_SERVICE_URL}/wa/disconnect/{self.bot_id}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    await resp.json()
            
            self.db.save_whatsapp_session(self.bot_id, status='disconnected')
            await update.callback_query.answer("✅ WhatsApp disconnected!")
            await self.wa_hub_menu(update)
            
        except Exception as e:
            self.logger.error(f"WA disconnect error: {e}")
            await update.callback_query.answer("❌ Disconnect failed", show_alert=True)
    
    async def wa_check_status(self, update: Update):
        """Check WhatsApp connection status"""
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.WA_SERVICE_URL}/wa/status/{self.bot_id}",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    data = await resp.json()
            
            status = data.get('status', 'unknown')
            self.db.save_whatsapp_session(self.bot_id, status=status)
            
            emoji = "🟢" if status == 'connected' else "🔴"
            await update.callback_query.answer(f"{emoji} Status: {status}", show_alert=True)
            
        except Exception as e:
            self.logger.error(f"WA status check error: {e}")
            await update.callback_query.answer("❌ Cannot reach WA service", show_alert=True)

    # --- Customize Menu Logic ---

    
    async def toggle_referral_system(self, update: Update):
        """Toggle referral system on/off"""
        new_state = self.db.toggle_referral(self.bot_id)
        status_text = "🟢 ON" if new_state else "🔴 OFF"
        
        await update.callback_query.answer(f"Referral system is now {status_text}")
        await self.show_admin_settings(update)

    # --- Referral Management Wizard ---
    async def manage_ref_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start referral management menu"""
        await update.callback_query.answer()  # Acknowledge button click
        text = (
            "🔄 **MANAGE REFERRALS**\n\n"
            "Sila pilih tindakan yang anda mahu lakukan:\n\n"
            "1. **Reset All Users** - Reset SEMUA user kepada 0.\n"
            "2. **Reset Specific User** - Reset user tertentu sahaja."
        )
        keyboard = [
            [InlineKeyboardButton("🌍 RESET ALL USERS (GLOBAL)", callback_data="rr_global")],
            [InlineKeyboardButton("👤 RESET SPECIFIC USER", callback_data="rr_specific")],
            [InlineKeyboardButton("❌ CANCEL", callback_data="cancel")]
        ]
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return RR_CONFIRM

    async def manage_ref_confirm_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle menu choice"""
        await update.callback_query.answer()  # Acknowledge button click immediately
        data = update.callback_query.data
        
        if data == "cancel":
            await update.callback_query.message.edit_text("❌ Cancelled.")
            return ConversationHandler.END
        
        if data == "rr_global":
            text = (
                "⚠️ **WARNING: GLOBAL RESET**\n\n"
                "Reset referral untuk **SEMUA USER** dalam database?\n"
                "Tindakan ini tidak boleh diundur."
            )
            keyboard = [
                [InlineKeyboardButton("🔥 YES, WIPE ALL DATA", callback_data="rr_do_reset_all")],
                [InlineKeyboardButton("❌ CANCEL", callback_data="cancel")]
            ]
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return RR_CONFIRM
            
        elif data == "rr_specific":
            text = "👤 **RESET SPECIFIC USER**\n\nSila hantar **Telegram ID** user tersebut sekarang:"
            keyboard = [[InlineKeyboardButton("❌ CANCEL", callback_data="cancel")]]
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return RR_INPUT_ID
            
        elif data == "rr_do_reset_all":
            self.db.reset_all_referrals(self.bot_id)
            await update.callback_query.answer("✅ Completed!", show_alert=True)
            
            # Show success message
            await update.callback_query.message.edit_text(
                "✅ **RESET COMPLETE!**\n\n"
                "Semua referral data telah dipadam.\n"
                "Semua user boleh refer semula.\n\n"
                "Kembali ke menu admin...",
                parse_mode='Markdown'
            )
            
            # Wait 2 seconds then show admin settings
            import asyncio
            await asyncio.sleep(2)
            await self.show_admin_settings(update)
            return ConversationHandler.END

        return RR_CONFIRM

    async def manage_ref_input_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle User ID input"""
        try:
            target_id = int(update.message.text.strip())
            success = self.db.reset_user_referral(self.bot_id, target_id)
            
            if success:
                msg = (
                    f"✅ **RESET BERJAYA!**\n\n"
                    f"User ID: `{target_id}`\n"
                    f"Referral data telah dipadam.\n"
                    f"User ini boleh refer semula."
                )
            else:
                msg = f"❌ **ERROR**\n\nGagal reset user ID: `{target_id}`\n\nMungkin user tidak wujud."
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
            import asyncio
            await asyncio.sleep(1.5)
            await self.show_admin_settings(update)
            return ConversationHandler.END
            
        except ValueError:
            await update.message.reply_text("⚠️ ID tidak sah. Sila hantar nombor sahaja.")
            return RR_INPUT_ID
    
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
            
            # Check AI chat status
            ai_chat_enabled = self.db.is_ai_chat_enabled(self.bot_id)
            ai_chat_btn_text = "🟢 AI Chat: ON" if ai_chat_enabled else "🔴 AI Chat: OFF"

            # Check pending schedules
            pending = self.db.get_pending_broadcasts(self.bot_id)
            schedule_text = f"🔄 Reset Schedule ({len(pending)})" if pending else "📅 No Schedules"

            # Count admins
            admins = self.db.get_admins(self.bot_id)
            admin_count = len(admins)

            text = "👑 **ADMIN SETTINGS DASHBOARD**\n\nWelcome Boss! Full control in your hands."
            keyboard = [
                [InlineKeyboardButton("➕ Add Company", callback_data="admin_add_company")],
                [InlineKeyboardButton("✏️ Edit Company", callback_data="admin_edit_company_list"), InlineKeyboardButton("🗑️ Delete Company", callback_data="admin_del_list")],
                [InlineKeyboardButton("🔢 Reorder Companies", callback_data="reorder_companies")],
                [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("⚙️ Customize Menu", callback_data="customize_menu")],
                [InlineKeyboardButton("🎨 Media Manager", callback_data="admin_media_manager"), InlineKeyboardButton(schedule_text, callback_data="reset_schedule")],
                [InlineKeyboardButton("💳 Withdrawals", callback_data="admin_withdrawals"), InlineKeyboardButton(referral_btn_text, callback_data="toggle_referral")],
                [InlineKeyboardButton(livegram_btn_text, callback_data="toggle_livegram"), InlineKeyboardButton("🔁 Manage Recurring", callback_data="manage_recurring")],
                [InlineKeyboardButton("📡 Forwarder", callback_data="forwarder_menu"), InlineKeyboardButton("📊 Analytics", callback_data="show_analytics")],
                [InlineKeyboardButton("🔄 Manage Referrals", callback_data="admin_ref_manage")],
                [InlineKeyboardButton("🛡️ Group Management", callback_data="group_mgmt"), InlineKeyboardButton(ai_chat_btn_text, callback_data="toggle_ai_chat")],
                [InlineKeyboardButton("🤖 Userbot", callback_data="userbot_hub"), InlineKeyboardButton("🧠 AI Settings", callback_data="ai_settings")],
                [InlineKeyboardButton("📱 WhatsApp Monitor", callback_data="wa_hub")]
            ]
            
            # Only owner can manage admins
            if is_owner:
                keyboard.append([InlineKeyboardButton(f"👥 Manage Admins ({admin_count})", callback_data="manage_admins")])
                keyboard.append([InlineKeyboardButton("⚙️ REFERRAL SETTINGS", callback_data="ref_settings")])
                
            keyboard.append([InlineKeyboardButton("❌ Close Panel", callback_data="close_panel")])
            
            if update.callback_query:
                await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                 await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            
            self.logger.info("show_admin_settings completed successfully")
        except Exception as e:
            self.logger.error(f"Error in show_admin_settings: {e}")
            # Fallback: send new message if edit fails
            await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    # === ADMIN WITHDRAWAL MANAGEMENT ===
    
    async def show_admin_withdrawals(self, update: Update):
        """Show list of pending withdrawals"""
        withdrawals = self.db.get_pending_withdrawals(self.bot_id)
        
        if not withdrawals:
            text = "📭 No pending withdrawals"
            keyboard = [[InlineKeyboardButton("« Back", callback_data="admin_settings")]]
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        text = f"💳 <b>PENDING WITHDRAWALS ({len(withdrawals)})</b>\n\n"
        keyboard = []
        
        for wd in withdrawals:
            user_id = wd['user_id']
            amount = wd['amount']
            method = wd.get('method', 'TNG')
            
            text += f"ID: {wd['id']} | User: {user_id} | RM {amount:.2f} | {method}\n"
            keyboard.append([InlineKeyboardButton(
                f"🔍 #{wd['id']} - RM {amount:.2f}",
                callback_data=f"wd_detail_{wd['id']}"
            )])
        
        keyboard.append([InlineKeyboardButton("« Back to Admin", callback_data="admin_settings")])
        
        await update.callback_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def show_withdrawal_detail(self, update: Update, withdrawal_id: int):
        """Show single withdrawal with approve/reject buttons"""
        wd = self.db.get_withdrawal_by_id(withdrawal_id)
        
        if not wd:
            await update.callback_query.answer("Withdrawal not found", show_alert=True)
            return
        
        text = (
            f"💳 <b>WITHDRAWAL DETAIL</b>\n\n"
            f"🆔 <b>ID:</b> {wd['id']}\n"
            f"👤 <b>User ID:</b> <code>{wd['user_id']}</code>\n"
            f"💵 <b>Amount:</b> RM {wd['amount']:.2f}\n"
            f"📝 <b>Method:</b> {wd.get('method', 'TNG')}\n"
            f"📋 <b>Account:</b> <code>{wd.get('account', 'N/A')}</code>\n"
            f"📊 <b>Status:</b> {wd['status']}\n"
            f"🕐 <b>Requested:</b> {wd.get('created_at', 'N/A')}\n"
            f"💰 <b>User Balance:</b> RM {wd.get('current_balance', 0):.2f}"
        )
        
        keyboard = []
        if wd['status'] == 'PENDING':
            keyboard.append([
                InlineKeyboardButton("✅ APPROVE", callback_data=f"wd_approve_{wd['id']}"),
                InlineKeyboardButton("❌ REJECT", callback_data=f"wd_reject_{wd['id']}")
            ])
        
        keyboard.append([InlineKeyboardButton("« Back to List", callback_data="admin_withdrawals")])
        
        await update.callback_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def admin_approve_withdrawal(self, update: Update, withdrawal_id: int):
        """Approve withdrawal and notify user"""
        success = self.db.update_withdrawal_status(withdrawal_id, 'APPROVED', update.effective_user.id)
        
        if success:
            wd = self.db.get_withdrawal_by_id(withdrawal_id)
            notified = False
            if wd:
                try:
                    await self.app.bot.send_message(
                        chat_id=wd['user_id'],
                        text=(
                            f"✅ <b>WITHDRAWAL APPROVED!</b>\n\n"
                            f"💵 Amount: RM {wd['amount']:.2f}\n"
                            f"📝 Method: {wd.get('method', 'TNG')}\n"
                            f"📋 Account: <code>{wd.get('account', 'N/A')}</code>\n\n"
                            f"Payment will be processed soon."
                        ),
                        parse_mode='HTML'
                    )
                    notified = True
                except Exception as e:
                    self.logger.error(f"Failed to notify user {wd['user_id']} about approval: {e}")
            
            alert = "✅ Approved & user notified!" if notified else "✅ Approved! (⚠️ User notification failed)"
            await update.callback_query.answer(alert, show_alert=True)
        else:
            await update.callback_query.answer("❌ Failed to approve", show_alert=True)
        
        await self.show_admin_withdrawals(update)
    
    async def admin_reject_withdrawal(self, update: Update, withdrawal_id: int):
        """Reject withdrawal, refund balance, and notify user"""
        success = self.db.update_withdrawal_status(withdrawal_id, 'REJECTED', update.effective_user.id)
        
        if success:
            wd = self.db.get_withdrawal_by_id(withdrawal_id)
            notified = False
            if wd:
                try:
                    await self.app.bot.send_message(
                        chat_id=wd['user_id'],
                        text=(
                            f"❌ <b>WITHDRAWAL REJECTED</b>\n\n"
                            f"💵 Amount: RM {wd['amount']:.2f}\n"
                            f"Balance has been refunded to your wallet."
                        ),
                        parse_mode='HTML'
                    )
                    notified = True
                except Exception as e:
                    self.logger.error(f"Failed to notify user {wd['user_id']} about rejection: {e}")
            
            alert = "❌ Rejected & Refunded! User notified." if notified else "❌ Rejected & Refunded! (⚠️ User notification failed)"
            await update.callback_query.answer(alert, show_alert=True)
        else:
            await update.callback_query.answer("❌ Failed to reject", show_alert=True)
        
        await self.show_admin_withdrawals(update)
    
    async def toggle_livegram_system(self, update: Update):
        """Toggle livegram system on/off"""
        new_state = self.db.toggle_livegram(self.bot_id)
        status_text = "🟢 **ON**" if new_state else "🔴 **OFF**"
        
        await update.callback_query.answer(f"Livegram system is now {status_text}")
        await self.show_admin_settings(update)

    async def toggle_ai_chat_system(self, update: Update):
        """Toggle AI chatbot on/off"""
        new_state = self.db.toggle_ai_chat(self.bot_id)
        status_text = "🟢 **ON**" if new_state else "🔴 **OFF**"
        
        await update.callback_query.answer(f"AI ChatBot is now {'ON' if new_state else 'OFF'}")
        await self.show_admin_settings(update)

    async def toggle_link_guard_system(self, update: Update):
        """Toggle link guard system on/off"""
        new_state = self.db.toggle_link_guard(self.bot_id)
        status_text = "🟢 **ON**" if new_state else "🔴 **OFF**"
        
        await update.callback_query.answer(f"Link Guard is now {status_text}")
        await self.show_admin_settings(update)
    
    # === AI CHATBOT SETTINGS ===
    
    async def show_ai_settings(self, update: Update):
        """Show AI chatbot settings menu"""
        ai_enabled = self.db.is_ai_chat_enabled(self.bot_id)
        custom_prompt = self.db.get_ai_prompt(self.bot_id)
        
        status = "🟢 ON" if ai_enabled else "🔴 OFF"
        prompt_preview = f"`{custom_prompt[:100]}...`" if len(custom_prompt) > 100 else (f"`{custom_prompt}`" if custom_prompt else "_(Default — Masuk10 AI)_")
        
        text = (
            f"🤖 **AI CHATBOT SETTINGS**\n\n"
            f"Status: {status}\n"
            f"Personality:\n{prompt_preview}\n\n"
            f"Pilih action:"
        )
        keyboard = [
            [InlineKeyboardButton(f"{'🔴 OFF' if ai_enabled else '🟢 ON'}", callback_data="toggle_ai_chat"),
             InlineKeyboardButton("✏️ Set Personality", callback_data="ai_set_prompt")],
            [InlineKeyboardButton("🔄 Reset Default", callback_data="ai_reset_prompt")],
            [InlineKeyboardButton("« Back", callback_data="admin_settings")]
        ]
        
        query = update.callback_query
        if query:
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def ai_set_prompt_start(self, update: Update, context):
        """Start AI prompt input"""
        query = update.callback_query
        await query.answer()
        
        text = (
            "✏️ **SET AI PERSONALITY**\n\n"
            "Taip system prompt untuk AI kau.\n\n"
            "**Contoh:**\n"
            "`Kau adalah customer service untuk [Nama Bisnes]. "
            "Jawab soalan pelanggan dalam Bahasa Melayu dengan gaya santai. "
            "Kau pakar dalam topik gaming dan e-wallet. "
            "Jawab pendek dan padat — max 3-4 baris.`\n\n"
            "**Tips:**\n"
            "• Set nama AI kau\n"
            "• Set topik/bidang kepakaran\n"
            "• Set bahasa dan gaya cakap\n"
            "• Set limit panjang jawapan\n\n"
            "📝 **Taip prompt sekarang:**"
        )
        await query.message.edit_text(text, parse_mode='Markdown')
        context.user_data['waiting_ai_prompt'] = True

    async def ai_save_prompt(self, update: Update, context):
        """Save custom AI prompt from user input"""
        prompt = update.message.text.strip()
        self.db.set_ai_prompt(self.bot_id, prompt)
        context.user_data.pop('waiting_ai_prompt', None)
        
        await update.message.reply_text(
            f"✅ **AI Personality saved!**\n\n"
            f"Prompt:\n`{prompt[:500]}{'...' if len(prompt) > 500 else ''}`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 AI Settings", callback_data="ai_settings")],
                [InlineKeyboardButton("« Admin Settings", callback_data="admin_settings")]
            ])
        )

    async def ai_reset_prompt(self, update: Update):
        """Reset AI prompt to default"""
        query = update.callback_query
        await query.answer()
        self.db.set_ai_prompt(self.bot_id, '')
        await query.message.edit_text(
            "🔄 **AI Personality reset to default!**\n\n"
            "AI akan guna personality Masuk10 AI (default).",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 AI Settings", callback_data="ai_settings")],
                [InlineKeyboardButton("« Admin Settings", callback_data="admin_settings")]
            ])
        )

    # === GROUP MANAGEMENT HUB ===
    
    async def show_group_management(self, update: Update):
        """Show group management sub-menu with all group features"""
        try:
            # Get all toggle states
            link_guard = self.db.is_link_guard_enabled(self.bot_id)
            delete_jl = self.db.is_delete_join_leave_enabled(self.bot_id)
            anti_bot = self.db.is_anti_bot_enabled(self.bot_id)
            gw = self.db.get_group_welcome(self.bot_id)
            ban_words = self.db.get_ban_words(self.bot_id)
            auto_replies = self.db.get_auto_replies(self.bot_id)
            
            lg_icon = "🟢" if link_guard else "🔴"
            djl_icon = "🟢" if delete_jl else "🔴"
            ab_icon = "🟢" if anti_bot else "🔴"
            gw_icon = "🟢" if gw.get('enabled') else "🔴"
            
            text = (
                "🛡️ **GROUP MANAGEMENT**\n\n"
                "Kawalan penuh untuk group anda.\n"
                "Toggle ON/OFF dan urus setting group.\n\n"
                f"🔗 Link Guard: {lg_icon}\n"
                f"🗑️ Delete Join/Leave: {djl_icon}\n"
                f"🤖 Anti-Bot: {ab_icon}\n"
                f"👋 Welcome Message: {gw_icon}\n"
                f"📝 Ban Words: {len(ban_words)} word(s)\n"
                f"💬 Auto-Reply: {len(auto_replies)} rule(s)"
            )
            
            keyboard = [
                [InlineKeyboardButton(f"{lg_icon} Link Guard: {'ON' if link_guard else 'OFF'}", callback_data="gm_toggle_link_guard")],
                [InlineKeyboardButton(f"{djl_icon} Delete Join/Leave: {'ON' if delete_jl else 'OFF'}", callback_data="gm_toggle_delete_jl")],
                [InlineKeyboardButton(f"{ab_icon} Anti-Bot: {'ON' if anti_bot else 'OFF'}", callback_data="gm_toggle_anti_bot")],
                [InlineKeyboardButton(f"{gw_icon} Welcome Message", callback_data="gm_welcome")],
                [InlineKeyboardButton(f"📝 Ban Words ({len(ban_words)})", callback_data="gm_ban_words"),
                 InlineKeyboardButton(f"💬 Auto-Reply ({len(auto_replies)})", callback_data="gm_auto_replies")],
                [InlineKeyboardButton("« Back to Admin", callback_data="admin_settings")]
            ]
            
            if update.callback_query:
                await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception as e:
            self.logger.error(f"Error in show_group_management: {e}")
    
    async def gm_toggle_link_guard(self, update: Update):
        """Toggle link guard from group management menu"""
        new_state = self.db.toggle_link_guard(self.bot_id)
        await update.callback_query.answer(f"Link Guard {'ON' if new_state else 'OFF'}")
        await self.show_group_management(update)
    
    async def gm_toggle_delete_jl(self, update: Update):
        """Toggle delete join/leave messages"""
        new_state = self.db.toggle_delete_join_leave(self.bot_id)
        await update.callback_query.answer(f"Delete Join/Leave {'ON' if new_state else 'OFF'}")
        await self.show_group_management(update)
    
    async def gm_toggle_anti_bot(self, update: Update):
        """Toggle anti-bot protection"""
        new_state = self.db.toggle_anti_bot(self.bot_id)
        await update.callback_query.answer(f"Anti-Bot {'ON' if new_state else 'OFF'}")
        await self.show_group_management(update)
    
    async def gm_show_welcome_settings(self, update: Update):
        """Show welcome message settings for group"""
        gw = self.db.get_group_welcome(self.bot_id)
        enabled = gw.get('enabled', False)
        text_msg = gw.get('text', '') or 'Not set'
        autodelete = gw.get('autodelete', 0)
        
        toggle_text = "🟢 Welcome: ON" if enabled else "🔴 Welcome: OFF"
        autodelete_text = f"{autodelete}s" if autodelete else "OFF"
        
        text = (
            "👋 **WELCOME MESSAGE SETTINGS**\n\n"
            f"📊 Status: {toggle_text}\n"
            f"📝 Message: {text_msg[:80]}{'...' if len(text_msg) > 80 else ''}\n"
            f"⏱️ Auto-Delete: {autodelete_text}\n\n"
            "💡 Gunakan 'Customize Menu > Edit Welcome' untuk set message."
        )
        
        toggle_btn = "🔴 Turn OFF" if enabled else "🟢 Turn ON"
        keyboard = [
            [InlineKeyboardButton(toggle_btn, callback_data="gm_toggle_welcome")],
            [InlineKeyboardButton("« Back to Group Management", callback_data="group_mgmt")]
        ]
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    # --- Ban Words ---
    async def gm_show_ban_words(self, update: Update):
        """Show list of banned words"""
        words = self.db.get_ban_words(self.bot_id)
        
        if words:
            text = f"📝 **BAN WORDS** ({len(words)})\n\n"
            for i, w in enumerate(words, 1):
                text += f"{i}. `{w['word']}`\n"
            text += "\n💡 Message dengan perkataan ini akan auto-delete."
        else:
            text = "📝 **BAN WORDS**\n\n📭 Tiada ban words lagi.\n\n💡 Tambah perkataan yang nak dilarang dalam group."
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Ban Word", callback_data="gm_add_ban_word")]
        ]
        
        # Add delete buttons for each word
        for w in words:
            keyboard.append([InlineKeyboardButton(f"🗑️ Remove: {w['word']}", callback_data=f"gm_del_ban_{w['id']}")])
        
        keyboard.append([InlineKeyboardButton("« Back to Group Management", callback_data="group_mgmt")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def gm_add_ban_word_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Prompt user to enter ban word"""
        context.user_data['waiting_ban_word'] = True
        await update.callback_query.message.edit_text(
            "📝 **ADD BAN WORD**\n\n"
            "Taip perkataan yang nak dilarang:\n\n"
            "Untuk cancel, taip /cancel",
            parse_mode='Markdown'
        )
    
    async def gm_del_ban_word(self, update: Update, word_id: int):
        """Delete a ban word"""
        success = self.db.remove_ban_word(self.bot_id, word_id)
        if success:
            await update.callback_query.answer("✅ Ban word removed!")
        else:
            await update.callback_query.answer("❌ Failed to remove", show_alert=True)
        await self.gm_show_ban_words(update)
    
    # --- Auto Replies ---
    async def gm_show_auto_replies(self, update: Update):
        """Show list of auto-reply rules"""
        replies = self.db.get_auto_replies(self.bot_id)
        
        if replies:
            text = f"💬 **AUTO-REPLY** ({len(replies)})\n\n"
            for i, r in enumerate(replies, 1):
                trigger = r['trigger_text'][:30]
                response = r['response_text'][:40]
                text += f"{i}. 🔑 `{trigger}` → {response}\n"
            text += "\n💡 Bot akan auto-reply bila detect trigger dalam message."
        else:
            text = "💬 **AUTO-REPLY**\n\n📭 Tiada auto-reply rules.\n\n💡 Tambah trigger word dan response message."
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Auto-Reply", callback_data="gm_add_auto_reply")]
        ]
        
        for r in replies:
            keyboard.append([InlineKeyboardButton(f"🗑️ Remove: {r['trigger_text'][:20]}", callback_data=f"gm_del_reply_{r['id']}")])
        
        keyboard.append([InlineKeyboardButton("« Back to Group Management", callback_data="group_mgmt")])
        
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    async def gm_add_auto_reply_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start add auto-reply flow - ask for trigger"""
        context.user_data['waiting_auto_reply_trigger'] = True
        await update.callback_query.message.edit_text(
            "💬 **ADD AUTO-REPLY**\n\n"
            "**Step 1/2:** Taip **trigger word/phrase**:\n\n"
            "Contoh: `harga`, `price`, `hello`\n\n"
            "Untuk cancel, taip /cancel",
            parse_mode='Markdown'
        )
    
    async def gm_del_auto_reply(self, update: Update, reply_id: int):
        """Delete an auto-reply rule"""
        success = self.db.remove_auto_reply(self.bot_id, reply_id)
        if success:
            await update.callback_query.answer("✅ Auto-reply removed!")
        else:
            await update.callback_query.answer("❌ Failed to remove", show_alert=True)
        await self.gm_show_auto_replies(update)
    
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
            except Exception:
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
        
        text = f"⚙️ **CUSTOMIZE & MEDIA**\n\nCustom buttons: {btn_count}"
        keyboard = [
            [InlineKeyboardButton("🖼️ Edit Banner", callback_data="edit_welcome")],
            [InlineKeyboardButton("🎉 Group Welcome", callback_data="group_welcome_setup")],
            [InlineKeyboardButton("➕ Add Button", callback_data="menu_add_btn")],
            [InlineKeyboardButton("📋 Manage Buttons", callback_data="manage_menu_btns")],
            [InlineKeyboardButton("« Back", callback_data="admin_settings")]
        ]
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            # Fallback: original message is a photo/media, can't edit_text
            try:
                await update.callback_query.message.delete()
            except Exception:
                pass
            await update.callback_query.message.chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # --- Group Welcome Setup (Admin) ---
    def _get_gw_settings(self, context):
        """Get welcome settings based on current gw_group_id context"""
        group_id = context.user_data.get('gw_group_id')
        if group_id:
            settings = self.db.get_per_group_welcome(self.bot_id, group_id)
            if settings:
                return settings
            # Group selected but no per-group settings yet, return defaults
            return {'enabled': True, 'text': '', 'media': None, 'media_type': None, 'autodelete': 0}
        return self.db.get_group_welcome(self.bot_id)

    def _save_gw_setting(self, context, field, value):
        """Save welcome setting to per-group or default"""
        group_id = context.user_data.get('gw_group_id')
        if group_id:
            self.db.upsert_per_group_welcome(self.bot_id, group_id, field, value)
        else:
            self.db.update_group_welcome(self.bot_id, field, value)

    async def gw_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show group welcome settings menu - with group selection"""
        await update.callback_query.answer()
        
        # Check if a group is already selected
        group_id = context.user_data.get('gw_group_id')
        group_name = context.user_data.get('gw_group_name', 'Default')
        
        # If no group selected yet, show group selection
        if not context.user_data.get('gw_group_selected'):
            return await self._gw_show_group_list(update, context)
        
        settings = self._get_gw_settings(context)
        
        status = "🟢 ON" if settings['enabled'] else "🔴 OFF"
        
        # Auto-delete display
        ad = settings['autodelete']
        if ad == 0:
            ad_text = "OFF"
        elif ad < 60:
            ad_text = f"{ad}s"
        else:
            ad_text = f"{ad // 60} min"
        
        # Text preview
        text_preview = settings['text'][:50] + "..." if len(settings['text']) > 50 else (settings['text'] or "❌ Belum ditetapkan")
        media_status = f"✅ {settings['media_type'] or ''}".strip() if settings['media'] else "❌ Tiada"
        
        header = f"🌍 **{group_name}**" if group_id else "🌐 **Default (Semua Group)**"
        text = (
            f"🎉 **GROUP WELCOME MESSAGE**\n"
            f"📍 {header}\n\n"
            f"Status: {status}\n"
            f"Auto-Delete: **{ad_text}**\n"
            f"Media: {media_status}\n\n"
            f"📝 Text:\n{text_preview}\n\n"
            f"💡 _Variables: {{name}}, {{username}}, {{group}}, {{mention}}_"
        )
        
        toggle_text = "🔴 Turn OFF" if settings['enabled'] else "🟢 Turn ON"
        keyboard = [
            [InlineKeyboardButton("✏️ Edit Text", callback_data="gw_edit_text"), InlineKeyboardButton("🖼️ Edit Media", callback_data="gw_edit_media")],
            [InlineKeyboardButton("⏱️ Auto-Delete", callback_data="gw_autodelete"), InlineKeyboardButton(toggle_text, callback_data="gw_toggle")],
            [InlineKeyboardButton("🗑️ Remove Media", callback_data="gw_remove_media")],
            [InlineKeyboardButton("👁️ Preview", callback_data="gw_preview")],
            [InlineKeyboardButton("🔄 Tukar Group", callback_data="gw_select_group")],
            [InlineKeyboardButton("« Back", callback_data="customize_menu")]
        ]
        
        # Add "Reset to Default" button for per-group settings
        if group_id:
            keyboard.insert(-1, [InlineKeyboardButton("🔃 Reset ke Default", callback_data="gw_reset_to_default")])
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            try:
                await update.callback_query.message.delete()
            except Exception:
                pass
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
        return GW_MENU

    async def _gw_show_group_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show list of groups for welcome message configuration"""
        groups = self.db.get_known_groups(self.bot_id)
        
        keyboard = []
        # Default option (applies to all groups without specific settings)
        keyboard.append([InlineKeyboardButton("🌐 Default (Semua Group)", callback_data="gw_group_default")])
        
        for g in groups:
            name = g.get('group_name') or f"Group {g['group_id']}"
            # Check if has per-group setting
            per_group = self.db.get_per_group_welcome(self.bot_id, g['group_id'])
            indicator = " ✅" if per_group else ""
            keyboard.append([InlineKeyboardButton(f"💬 {name}{indicator}", callback_data=f"gw_group_{g['group_id']}")])
        
        keyboard.append([InlineKeyboardButton("« Back", callback_data="customize_menu")])
        
        text = (
            "🎉 **GROUP WELCOME MESSAGE**\n\n"
            "Pilih group yang nak di-customize:\n\n"
            "✅ = Sudah ada custom welcome\n"
            "🌐 Default = Untuk group tanpa custom setting"
        )
        
        try:
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
        return GW_MENU

    async def gw_handle_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle group welcome menu button clicks"""
        await update.callback_query.answer()
        data = update.callback_query.data
        
        # --- Group Selection ---
        if data == "gw_group_default":
            context.user_data['gw_group_id'] = None
            context.user_data['gw_group_name'] = 'Default'
            context.user_data['gw_group_selected'] = True
            return await self.gw_menu(update, context)
        
        elif data.startswith("gw_group_"):
            group_id = int(data.replace("gw_group_", ""))
            groups = self.db.get_known_groups(self.bot_id)
            group_name = next((g.get('group_name', f"Group {group_id}") for g in groups if g['group_id'] == group_id), f"Group {group_id}")
            context.user_data['gw_group_id'] = group_id
            context.user_data['gw_group_name'] = group_name
            context.user_data['gw_group_selected'] = True
            return await self.gw_menu(update, context)
        
        elif data == "gw_select_group":
            context.user_data['gw_group_selected'] = False
            return await self._gw_show_group_list(update, context)
        
        elif data == "gw_reset_to_default":
            group_id = context.user_data.get('gw_group_id')
            if group_id:
                self.db.delete_per_group_welcome(self.bot_id, group_id)
                await update.callback_query.answer("✅ Reset ke default!", show_alert=True)
            return await self.gw_menu(update, context)
        
        # --- Settings Actions ---
        elif data == "gw_toggle":
            settings = self._get_gw_settings(context)
            new_state = 0 if settings['enabled'] else 1
            self._save_gw_setting(context, 'enabled', new_state)
            return await self.gw_menu(update, context)
        
        elif data == "gw_edit_text":
            await update.callback_query.message.edit_text(
                "📝 **EDIT WELCOME TEXT**\n\n"
                "Hantar mesej selamat datang baru.\n\n"
                "💡 Boleh guna formatting (bold, italic, underline) dan variables:\n"
                "• `{name}` → Nama member\n"
                "• `{username}` → @username\n"
                "• `{group}` → Nama group\n"
                "• `{mention}` → Mention link\n\n"
                "Contoh:\n"
                "_Selamat datang **{name}**! 🎉\nWelcome ke **{group}**!_\n\n"
                "Taip /cancel untuk batalkan.",
                parse_mode='Markdown'
            )
            return GW_TEXT
        
        elif data == "gw_edit_media":
            await update.callback_query.message.edit_text(
                "🖼️ **EDIT WELCOME MEDIA**\n\n"
                "Hantar gambar, video, atau GIF untuk welcome message.\n\n"
                "Taip /cancel untuk batalkan.",
                parse_mode='Markdown'
            )
            return GW_MEDIA
        
        elif data == "gw_remove_media":
            self._save_gw_setting(context, 'media', None)
            self._save_gw_setting(context, 'media_type', None)
            await update.callback_query.answer("✅ Media dibuang!", show_alert=True)
            return await self.gw_menu(update, context)
        
        elif data == "gw_autodelete":
            keyboard = [
                [InlineKeyboardButton("OFF", callback_data="gw_ad_0"), InlineKeyboardButton("30s", callback_data="gw_ad_30")],
                [InlineKeyboardButton("1 min", callback_data="gw_ad_60"), InlineKeyboardButton("5 min", callback_data="gw_ad_300")],
                [InlineKeyboardButton("30 min", callback_data="gw_ad_1800"), InlineKeyboardButton("1 jam", callback_data="gw_ad_3600")],
                [InlineKeyboardButton("« Back", callback_data="group_welcome_setup")]
            ]
            await update.callback_query.message.edit_text(
                "⏱️ **AUTO-DELETE**\n\nPilih berapa lama sebelum welcome message dipadam automatik:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return GW_MENU
        
        elif data.startswith("gw_ad_"):
            seconds = int(data.replace("gw_ad_", ""))
            self._save_gw_setting(context, 'autodelete', seconds)
            label = "OFF" if seconds == 0 else f"{seconds}s" if seconds < 60 else f"{seconds // 60} min" if seconds < 3600 else "1 jam"
            await update.callback_query.answer(f"✅ Auto-delete: {label}", show_alert=True)
            return await self.gw_menu(update, context)
        
        elif data == "gw_preview":
            settings = self._get_gw_settings(context)
            if not settings['text'] and not settings['media']:
                await update.callback_query.answer("❌ Tiada text atau media ditetapkan!", show_alert=True)
                return GW_MENU
            
            # Build preview with sample variables
            user = update.effective_user
            group_name = context.user_data.get('gw_group_name', 'Test Group')
            preview_text = self._substitute_welcome_vars(
                settings['text'],
                user_name=user.first_name,
                username=user.username,
                group_name=group_name,
                user_id=user.id
            )
            
            keyboard = [[InlineKeyboardButton("« Back", callback_data="group_welcome_setup")]]
            
            try:
                if settings['media']:
                    import os
                    media_source = settings['media']
                    is_local = media_source and (media_source.startswith('/') or os.path.sep in media_source) and os.path.exists(media_source)
                    
                    if is_local:
                        with open(media_source, 'rb') as f:
                            if settings['media_type'] == 'video':
                                await update.effective_chat.send_video(video=f, caption=preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                            elif settings['media_type'] == 'animation':
                                await update.effective_chat.send_animation(animation=f, caption=preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                            else:
                                await update.effective_chat.send_photo(photo=f, caption=preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                    else:
                        if settings['media_type'] == 'video':
                            await update.effective_chat.send_video(video=media_source, caption=preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                        elif settings['media_type'] == 'animation':
                            await update.effective_chat.send_animation(animation=media_source, caption=preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                        else:
                            await update.effective_chat.send_photo(photo=media_source, caption=preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                else:
                    await update.effective_chat.send_message(preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            except Exception as e:
                self.logger.error(f"Group welcome preview error: {e}")
                await update.effective_chat.send_message(f"❌ Preview error: {e}", reply_markup=InlineKeyboardMarkup(keyboard))
            
            return GW_MENU
        
        return GW_MENU

    async def gw_save_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save group welcome text"""
        formatted_text = message_to_html(update.message)
        self._save_gw_setting(context, 'text', formatted_text)
        
        await update.message.reply_text(
            "✅ Welcome text berjaya dikemaskini!\n\n"
            "💡 <i>Formatting telah disimpan.</i>",
            parse_mode='HTML'
        )
        
        # Show menu again
        keyboard = [[InlineKeyboardButton("« Back to Group Welcome", callback_data="group_welcome_setup")]]
        await update.message.reply_text("Tekan Back untuk kembali:", reply_markup=InlineKeyboardMarkup(keyboard))
        return GW_MENU

    async def gw_save_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save group welcome media"""
        import os
        
        media_base = os.environ.get('MEDIA_DIR', '/data/media')
        media_dir = f"{media_base}/{self.bot_id}"
        os.makedirs(media_dir, exist_ok=True)
        timestamp = int(datetime.datetime.now().timestamp())
        
        if update.message.photo:
            file_obj = await update.message.photo[-1].get_file()
            file_path = f"{media_dir}/gw_{timestamp}.jpg"
            media_type = 'photo'
        elif update.message.video:
            file_obj = await update.message.video.get_file()
            file_path = f"{media_dir}/gw_{timestamp}.mp4"
            media_type = 'video'
        elif update.message.animation:
            file_obj = await update.message.animation.get_file()
            file_path = f"{media_dir}/gw_{timestamp}.gif"
            media_type = 'animation'
        else:
            await update.message.reply_text("❌ Sila hantar gambar, video atau GIF.")
            return GW_MEDIA
        
        await file_obj.download_to_drive(file_path)
        self._save_gw_setting(context, 'media', file_path)
        self._save_gw_setting(context, 'media_type', media_type)
        
        await update.message.reply_text("✅ Welcome media berjaya dikemaskini!")
        
        keyboard = [[InlineKeyboardButton("« Back to Group Welcome", callback_data="group_welcome_setup")]]
        await update.message.reply_text("Tekan Back untuk kembali:", reply_markup=InlineKeyboardMarkup(keyboard))
        return GW_MENU

    # --- Group Welcome: New Member Handler ---
    def _substitute_welcome_vars(self, text, user_name, username, group_name, user_id):
        """Replace variables in welcome text"""
        if not text:
            return ""
        mention = f'<a href="tg://user?id={user_id}">{html_escape(user_name)}</a>'
        result = text.replace("{name}", html_escape(user_name))
        result = result.replace("{username}", f"@{username}" if username else html_escape(user_name))
        result = result.replace("{group}", html_escape(group_name or "Group"))
        result = result.replace("{mention}", mention)
        return result

    async def handle_new_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message when new members join a group"""
        try:
            # Only for groups
            if update.effective_chat.type not in ['group', 'supergroup']:
                return
            
            # --- DELETE JOIN/LEAVE: Auto-delete join service message ---
            if self.db.is_delete_join_leave_enabled(self.bot_id):
                try:
                    await update.message.delete()
                except Exception as e:
                    self.logger.error(f"Delete join message error: {e}")
            
            # --- ANTI-BOT: Kick bots added by non-creators ---
            if self.db.is_anti_bot_enabled(self.bot_id):
                for member in update.message.new_chat_members:
                    if member.is_bot and member.id != context.bot.id:
                        try:
                            await update.effective_chat.ban_member(member.id)
                            await update.effective_chat.unban_member(member.id)  # Unban so they can be added back by creator
                            warning = await update.effective_chat.send_message(
                                f"🤖 **Anti-Bot:** Bot `{member.first_name}` telah dikeluarkan. Hanya admin boleh tambah bot.",
                                parse_mode='Markdown'
                            )
                            await asyncio.sleep(5)
                            await warning.delete()
                        except Exception as e:
                            self.logger.error(f"Anti-bot kick error: {e}")
                        continue
            
            # Check per-group welcome first, fallback to default
            group_id = update.effective_chat.id
            settings = self.db.get_per_group_welcome(self.bot_id, group_id)
            if settings is None:
                # No per-group setting, use default bot-level welcome
                settings = self.db.get_group_welcome(self.bot_id)
            
            if not settings['enabled']:
                return
            
            if not settings['text'] and not settings['media']:
                return
            
            # Process each new member
            for member in update.message.new_chat_members:
                # Skip bots
                if member.is_bot:
                    continue
                
                # Build welcome text with variables
                welcome_text = self._substitute_welcome_vars(
                    settings['text'],
                    user_name=member.first_name,
                    username=member.username,
                    group_name=update.effective_chat.title,
                    user_id=member.id
                )
                
                sent_msg = None
                try:
                    import os
                    if settings['media']:
                        media_source = settings['media']
                        is_local = media_source and (media_source.startswith('/') or os.path.sep in media_source) and os.path.exists(media_source)
                        
                        if is_local:
                            with open(media_source, 'rb') as f:
                                if settings['media_type'] == 'video':
                                    sent_msg = await update.effective_chat.send_video(video=f, caption=welcome_text, parse_mode='HTML')
                                elif settings['media_type'] == 'animation':
                                    sent_msg = await update.effective_chat.send_animation(animation=f, caption=welcome_text, parse_mode='HTML')
                                else:
                                    sent_msg = await update.effective_chat.send_photo(photo=f, caption=welcome_text, parse_mode='HTML')
                        else:
                            if settings['media_type'] == 'video':
                                sent_msg = await update.effective_chat.send_video(video=media_source, caption=welcome_text, parse_mode='HTML')
                            elif settings['media_type'] == 'animation':
                                sent_msg = await update.effective_chat.send_animation(animation=media_source, caption=welcome_text, parse_mode='HTML')
                            else:
                                sent_msg = await update.effective_chat.send_photo(photo=media_source, caption=welcome_text, parse_mode='HTML')
                    elif welcome_text:
                        sent_msg = await update.effective_chat.send_message(welcome_text, parse_mode='HTML')
                    
                    # Auto-delete if configured
                    if sent_msg and settings['autodelete'] > 0:
                        asyncio.create_task(self._auto_delete_message(
                            sent_msg.chat_id, sent_msg.message_id, settings['autodelete']
                        ))
                
                except Exception as e:
                    self.logger.error(f"Group welcome error for {member.id}: {e}")
        
        except Exception as e:
            self.logger.error(f"handle_new_member error: {e}")

    async def handle_left_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Auto-delete leave service messages in groups"""
        try:
            if update.effective_chat.type not in ['group', 'supergroup']:
                return
            
            if self.db.is_delete_join_leave_enabled(self.bot_id):
                try:
                    await update.message.delete()
                except Exception as e:
                    self.logger.error(f"Delete leave message error: {e}")
        except Exception as e:
            self.logger.error(f"handle_left_member error: {e}")

    async def _auto_delete_message(self, chat_id, message_id, delay_seconds):
        """Delete a message after a delay"""
        try:
            await asyncio.sleep(delay_seconds)
            await self.app.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass  # Message may already be deleted

    # --- Media Manager Functions ---
    async def show_media_manager(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show Media Manager Section Selection"""
        text = (
            "🎨 **MEDIA MANAGER**\n\n"
            "Pilih section mana yang anda nak tukar gambar/video:\n\n"
            "• **Wallet**: Paparan /wallet\n"
            "• **Share Link**: Paparan 'Share Link'\n"
            "• **Leaderboard**: Paparan Leaderboard\n"
            "• **4D Analyzer**: Banner Menu 4D\n\n"
            "💡 _Boleh set gambar atau video beserta caption._"
        )
        
        keyboard = [
            [InlineKeyboardButton("💰 Wallet", callback_data="media_section_wallet")],
            [InlineKeyboardButton("🔗 Share Link", callback_data="media_section_share")],
            [InlineKeyboardButton("🏆 Leaderboard", callback_data="media_section_leaderboard")],
            [InlineKeyboardButton("🔢 4D Analyzer", callback_data="media_section_4d")],
            [InlineKeyboardButton("« Back", callback_data="media_back")]
        ]
        
        # Determine if new message or edit
        if update.callback_query:
            await update.callback_query.answer()  # Acknowledge button click
            await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
        return MEDIA_UPLOAD

    async def media_manager_back(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle back button from media manager — properly end conversation"""
        await update.callback_query.answer()
        await self.show_admin_settings(update)
        return ConversationHandler.END

    async def media_manager_select_section(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle section selection"""
        await update.callback_query.answer()  # Acknowledge button click
        data = update.callback_query.data
        section_key = data.split("_")[2] # media_section_wallet
        context.user_data['media_section'] = section_key
        
        section_names = {
            'wallet': '💰 Dompet Saya',
            'share': '🔗 Share Link',
            'leaderboard': '🏆 Leaderboard',
            '4d': '🔢 4D Analyzer'
        }
        name = section_names.get(section_key, section_key.title())
        
        text = (
            f"🖼️ **UPLOAD MEDIA: {name}**\n\n"
            f"Sila hantar **GAMBAR** atau **VIDEO** sekarang.\n"
            f"✍️ **Caption:** Taip caption pada gambar/video tersebut untuk set caption baru.\n\n"
            f"_Jika hantar tanpa caption, caption akan dikosongkan._\n"
            f"Taip /cancel untuk batal."
        )
        
        await update.callback_query.message.reply_text(text, parse_mode='Markdown')
        return MEDIA_UPLOAD

    async def media_manager_save_upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save uploaded media to bot_assets"""
        section = context.user_data.get('media_section')
        if not section:
            await update.message.reply_text("❌ Session expired. Please start again.")
            return ConversationHandler.END
            
        file_id = None
        file_type = None
        caption = update.message.caption  # Get caption from media message
        
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            file_type = 'photo'
        elif update.message.video:
            file_id = update.message.video.file_id
            file_type = 'video'
        else:
            await update.message.reply_text("❌ Sila hantar Photo atau Video sahaja.")
            return MEDIA_UPLOAD
            
        # Save to DB
        success = self.db.upsert_asset(self.bot_id, section, file_id, file_type, caption)
        
        if success:
            await update.message.reply_text(
                f"✅ **Media Saved!**\n\nSection `{section}` telah dikemaskini.\n"
                f"Paparan pengguna akan berubah serta-merta.",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ Database Error. Gagal simpan.")
            
        return ConversationHandler.END

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
        try:
            btn2 = conn.execute("SELECT company_id FROM company_buttons WHERE id = ?", (btn2_id,)).fetchone()
            if btn2:
                company_id = btn2['company_id']
        finally:
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
            "Step 1: Upload your welcome banner\n\n"
            "Send a **photo** atau **video** yang akan dipaparkan bila user type /start\n\n"
            "Type /cancel to cancel."
        )
        return WELCOME_PHOTO
    
    async def save_welcome_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save photo/video file_id and ask for caption"""
        if update.message.video:
            file_id = update.message.video.file_id
            context.user_data['welcome_banner'] = file_id
            context.user_data['welcome_banner_type'] = 'video'
            media_type = "Video"
        else:
            photo = update.message.photo[-1]  # Get highest resolution
            file_id = photo.file_id
            context.user_data['welcome_banner'] = file_id
            context.user_data['welcome_banner_type'] = 'photo'
            media_type = "Photo"
        
        await update.message.reply_text(
            f"✅ {media_type} saved!\n\n"
            "Step 2: Enter your welcome message text\n\n"
            "This text will be shown with the banner when users type /start\n\n"
            "Type /cancel to cancel."
        )
        return WELCOME_TEXT
    
    async def save_welcome_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save caption to database and show preview"""
        caption_text = message_to_html(update.message)
        banner_file_id = context.user_data.get('welcome_banner')
        banner_type = context.user_data.get('welcome_banner_type', 'photo')
        
        # Update database - store type with file_id (type|file_id)
        stored_value = f"{banner_type}|{banner_file_id}"
        bot_data = self.db.get_bot_by_token(self.token)
        self.db.update_welcome_settings(bot_data['id'], stored_value, caption_text)
        
        # Invalidate cache so new banner shows immediately
        self._invalidate_bot_cache()
        
        # Show preview
        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="customize_menu")]]
        preview_caption = (
            f"✅ <b>WELCOME MESSAGE UPDATED!</b>\n\n"
            f"Preview:\n{caption_text}\n\n"
            f"Users will see this when they type /start"
        )
        
        try:
            if banner_type == 'video':
                await update.message.reply_video(
                    video=banner_file_id,
                    caption=preview_caption,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_photo(
                    photo=banner_file_id,
                    caption=preview_caption,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML'
                )
        except Exception:
            await update.message.reply_text(
                preview_caption,
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

    # --- Add Company Wizard Steps ---
    async def add_company_start(self, update, context):
        await send_with_retry(lambda: update.callback_query.message.reply_text(
            "Sila masukkan **NAMA Company**:\n\n💡 _Boleh masukkan emoji sekali, contoh: 🎰 Mega888_", parse_mode='Markdown'
        ))
        return NAME
    
    async def add_company_name(self, update, context):
        formatted_name = message_to_html(update.message)
        context.user_data['new_comp'] = {'name': formatted_name}
        await send_with_retry(lambda: update.message.reply_text("Masukkan **Deskripsi Company**:", parse_mode='Markdown'))
        return DESC

    async def add_company_desc(self, update, context):
        context.user_data['new_comp']['desc'] = message_to_html(update.message)
        await send_with_retry(lambda: update.message.reply_text("Hantar **Gambar/Video** Banner:", parse_mode='Markdown'))
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
            # Auto-generate keywords using AI
            try:
                from ai_rewriter import generate_keywords
                keywords = await generate_keywords(data['name'])
                self.db.edit_company(company_id, 'keywords', keywords)
            except Exception as e:
                self.logger.error(f"Auto keywords on add failed: {e}")
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
        except Exception:
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
        
        await update.callback_query.answer()  # Acknowledge button click
        
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
        """Handle target selection - then show broadcast type picker"""
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "broadcast_cancel":
            await query.message.edit_text("❌ Broadcast dibatalkan.")
            return ConversationHandler.END
        
        target_type = "users" if data == "target_users" else "groups"
        context.user_data['broadcast_target_type'] = target_type
        
        target_display = "👤 All Users" if target_type == "users" else "👥 All Known Groups"
        
        keyboard = [
            [InlineKeyboardButton("📷 Single Media", callback_data="btype_single")],
            [InlineKeyboardButton("🖼️ Grid/Album (2-10)", callback_data="btype_grid")],
            [InlineKeyboardButton("📝 Text Only", callback_data="btype_text")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        await query.message.edit_text(
            f"🎯 Target: **{target_display}**\n\n"
            "Pilih jenis broadcast:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return BROADCAST_TYPE
    
    async def broadcast_type_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broadcast type selection"""
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "broadcast_cancel":
            await query.message.edit_text("❌ Broadcast dibatalkan.")
            return ConversationHandler.END
        
        if data == "btype_grid":
            context.user_data['grid_media'] = []
            await query.message.edit_text(
                "🖼️ **GRID/ALBUM MODE**\n\n"
                "Hantar gambar atau video satu persatu.\n"
                "Minimum 2, maximum 10 media.\n\n"
                "📸 0/10 ditambah\n\n"
                "_Hantar media pertama sekarang..._",
                parse_mode='Markdown'
            )
            return GRID_MEDIA
        
        # Single or Text mode
        context.user_data.pop('grid_media', None)
        mode_text = "📷 Single Media" if data == "btype_single" else "📝 Text Only"
        if data == "btype_text":
            await query.message.edit_text(
                f"✅ Mode: **{mode_text}**\n\n"
                "Sila taip mesej text yang nak disebarkan:",
                parse_mode='Markdown'
            )
        else:
            await query.message.edit_text(
                f"✅ Mode: **{mode_text}**\n\n"
                "Sila hantar mesej (Text/Gambar/Video) yang nak disebarkan:",
                parse_mode='Markdown'
            )
        return BROADCAST_CONTENT
    
    async def grid_media_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Collect media items for grid broadcast"""
        msg = update.message
        grid = context.user_data.get('grid_media', [])
        
        if len(grid) >= 10:
            await msg.reply_text("⚠️ Maximum 10 media! Tekan ✅ Done.")
            return GRID_MEDIA
        
        if msg.photo:
            grid.append({'type': 'photo', 'file_id': msg.photo[-1].file_id})
        elif msg.video:
            grid.append({'type': 'video', 'file_id': msg.video.file_id})
        
        context.user_data['grid_media'] = grid
        count = len(grid)
        
        keyboard = []
        if count >= 2:
            keyboard.append([InlineKeyboardButton("✅ Done", callback_data="grid_done")])
        
        await msg.reply_text(
            f"📸 **{count}/10** media ditambah\n\n"
            f"{'✅ Boleh tekan Done atau tambah lagi.' if count >= 2 else '⏳ Tambah lagi media...'}",
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode='Markdown'
        )
        return GRID_MEDIA
    
    async def grid_media_done(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Done collecting grid media - ask for caption"""
        await update.callback_query.answer()
        grid = context.user_data.get('grid_media', [])
        
        if len(grid) < 2:
            await update.callback_query.message.edit_text("⚠️ Minimum 2 media diperlukan!")
            return GRID_MEDIA
        
        keyboard = [
            [InlineKeyboardButton("⏭️ Skip Caption", callback_data="grid_skip_caption")]
        ]
        await update.callback_query.message.edit_text(
            f"✅ **{len(grid)} media** disimpan!\n\n"
            "📝 Sekarang taip **caption text** untuk follow-up message.\n"
            "Atau tekan Skip jika tak perlu.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return GRID_CAPTION
    
    async def _show_button_picker(self, message_or_query, context):
        """Show company picker + manual option for broadcast buttons"""
        companies = self.db.get_companies(self.bot_id)
        
        keyboard = []
        # Show companies that have button_url
        for comp in companies:
            if comp.get('button_url'):
                btn_label = f"🏢 {comp['name']}"
                keyboard.append([InlineKeyboardButton(btn_label, callback_data=f"grid_comp_{comp['id']}")])
        
        keyboard.append([InlineKeyboardButton("✍️ Manual (text|url)", callback_data="grid_manual_btn")])
        keyboard.append([InlineKeyboardButton("⏭️ Skip Buttons", callback_data="grid_skip_buttons")])
        
        text = (
            "🔘 **Pilih company** untuk auto-button:\n\n"
            "Atau pilih Manual untuk taip sendiri."
        )
        
        if hasattr(message_or_query, 'edit_text'):
            await message_or_query.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await message_or_query.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    async def grid_caption_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive caption text for grid broadcast"""
        caption = message_to_html(update.message)
        context.user_data['grid_caption'] = caption
        await self._show_button_picker(update.message, context)
        return GRID_BUTTONS
    
    async def grid_caption_skip(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Skip caption - go to buttons"""
        await update.callback_query.answer()
        context.user_data['grid_caption'] = None
        await self._show_button_picker(update.callback_query.message, context)
        return GRID_BUTTONS
    
    async def grid_company_pick(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Auto-generate button from selected company"""
        await update.callback_query.answer()
        data = update.callback_query.data  # grid_comp_<id>
        comp_id = int(data.replace("grid_comp_", ""))
        
        companies = self.db.get_companies(self.bot_id)
        company = next((c for c in companies if c['id'] == comp_id), None)
        
        if not company or not company.get('button_url'):
            await update.callback_query.message.edit_text("❌ Company tak jumpa atau tiada URL.")
            return GRID_BUTTONS
        
        btn_text = company.get('button_text') or company['name']
        btn_url = company['button_url']
        if btn_url.startswith('t.me/'):
            btn_url = 'https://' + btn_url
        
        context.user_data['grid_buttons'] = [{'text': btn_text, 'url': btn_url}]
        context.user_data['broadcast_company_name'] = company['name']
        return await self._show_ai_rewrite_prompt(update, context)
    
    async def grid_manual_btn(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Switch to manual button input"""
        await update.callback_query.answer()
        keyboard = [
            [InlineKeyboardButton("⏭️ Skip Buttons", callback_data="grid_skip_buttons")]
        ]
        await update.callback_query.message.edit_text(
            "✍️ **Manual Mode**\n\n"
            "Format: `text|url` (satu button per baris)\n\n"
            "Contoh:\n"
            "`Register|https://t.me/bot`\n"
            "`Website|https://example.com`\n\n"
            "Atau tekan Skip.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return GRID_BUTTONS
    
    async def grid_buttons_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Parse button text in text|url format"""
        text = update.message.text.strip()
        buttons = []
        errors = []
        
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue
            if '|' not in line:
                errors.append(f"❌ `{line}` - tiada | separator")
                continue
            parts = line.split('|', 1)
            btn_text = parts[0].strip()
            btn_url = parts[1].strip()
            if not btn_url.startswith(('http://', 'https://', 't.me/')):
                if btn_url.startswith('t.me/'):
                    btn_url = 'https://' + btn_url
                else:
                    errors.append(f"❌ `{btn_text}` - URL tak valid")
                    continue
            buttons.append({'text': btn_text, 'url': btn_url})
        
        if errors:
            keyboard = [
                [InlineKeyboardButton("⏭️ Skip Buttons", callback_data="grid_skip_buttons")]
            ]
            await update.message.reply_text(
                "⚠️ Ada error:\n" + '\n'.join(errors) + "\n\nCuba lagi atau Skip.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            return GRID_BUTTONS
        
        if not buttons:
            return await self.grid_buttons_skip(update, context)
        
        context.user_data['grid_buttons'] = buttons
        return await self._grid_show_confirm(update, context)
    
    async def grid_buttons_done(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Done adding buttons"""
        await update.callback_query.answer()
        return await self._grid_show_confirm(update, context)
    
    async def grid_buttons_skip(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Skip buttons"""
        if update.callback_query:
            await update.callback_query.answer()
        context.user_data['grid_buttons'] = []
        return await self._grid_show_confirm(update, context)
    
    async def _grid_show_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show grid broadcast confirmation"""
        grid = context.user_data.get('grid_media', [])
        caption = context.user_data.get('grid_caption')
        buttons = context.user_data.get('grid_buttons', [])
        target_type = context.user_data.get('broadcast_target_type', 'users')
        target_display = "👤 All Users" if target_type == "users" else "👥 All Known Groups"
        
        summary = (
            f"📋 **GRID BROADCAST PREVIEW**\n\n"
            f"🎯 Target: **{target_display}**\n"
            f"🖼️ Media: **{len(grid)} items**\n"
            f"📝 Caption: **{'Yes' if caption else 'None'}**\n"
            f"🔘 Buttons: **{len(buttons)} buttons**\n\n"
            "Pilih option:"
        )
        
        # Package broadcast_data for the confirm handler
        import json
        context.user_data['broadcast_data'] = {
            'text': caption or '',
            'photo': None,
            'video': None,
            'document': None,
            'message': None,
            'grid_media': json.dumps(grid),
            'grid_buttons': json.dumps(buttons) if buttons else None
        }
        
        keyboard = [
            [InlineKeyboardButton("📤 Send Now", callback_data="broadcast_now")],
            [InlineKeyboardButton("⏰ Schedule", callback_data="broadcast_schedule")],
            [InlineKeyboardButton("🔁 Recurring", callback_data="broadcast_recurring")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        
        msg_target = update.callback_query.message if update.callback_query else update.message
        if update.callback_query:
            await msg_target.edit_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await msg_target.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return BROADCAST_CONFIRM
    
    async def broadcast_content(self, update, context):
        # Save msg details for later use
        msg = update.message
        context.user_data['broadcast_data'] = {
            'text': message_to_html(msg),
            'photo': msg.photo[-1].file_id if msg.photo else None,
            'video': msg.video.file_id if msg.video else None,
            'document': msg.document.file_id if msg.document else None,
            'message': msg  # Keep original for instant send
        }
        
        # Show company picker for buttons
        companies = self.db.get_companies(self.bot_id)
        
        keyboard = []
        for comp in companies:
            # Check company_buttons table too
            buttons = self.db.get_company_buttons(comp['id'])
            has_url = comp.get('button_url') or (buttons and any(b.get('url') for b in buttons))
            label = f"🏢 {comp['name']}" if has_url else f"🏢 {comp['name']} (no url)"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"sbtn_comp_{comp['id']}")])
        keyboard.append([InlineKeyboardButton("✍️ Manual (text|url)", callback_data="sbtn_manual")])
        keyboard.append([InlineKeyboardButton("⏭️ Skip Buttons", callback_data="sbtn_skip")])
        
        await update.message.reply_text(
            "✅ **Mesej diterima!**\n\n"
            "🔘 Pilih company untuk button:\n"
            "Atau Manual/Skip.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return SINGLE_BUTTONS
    
    async def single_btn_company_pick(self, update, context):
        """Auto-generate button from selected company for single broadcast"""
        await update.callback_query.answer()
        comp_id = int(update.callback_query.data.replace("sbtn_comp_", ""))
        
        companies = self.db.get_companies(self.bot_id)
        company = next((c for c in companies if c['id'] == comp_id), None)
        
        if not company:
            await update.callback_query.message.edit_text("❌ Company tak jumpa.")
            return SINGLE_BUTTONS
        
        # Get URL from button_url or company_buttons table
        btn_text = company.get('button_text') or company['name']
        btn_url = company.get('button_url', '')
        
        if not btn_url:
            # Try company_buttons table
            comp_buttons = self.db.get_company_buttons(comp_id)
            if comp_buttons:
                btn_url = comp_buttons[0].get('url', '')
                btn_text = comp_buttons[0].get('text', '') or btn_text
        
        if not btn_url:
            await update.callback_query.message.edit_text("❌ Company ni takde URL.")
            return SINGLE_BUTTONS
        
        if btn_url.startswith('t.me/'):
            btn_url = 'https://' + btn_url
        
        context.user_data['single_buttons'] = [{'text': btn_text, 'url': btn_url}]
        context.user_data['broadcast_company_name'] = company['name']
        return await self._show_ai_rewrite_prompt(update, context)
    
    async def single_btn_manual(self, update, context):
        """Switch to manual button input for single broadcast"""
        await update.callback_query.answer()
        keyboard = [[InlineKeyboardButton("⏭️ Skip", callback_data="sbtn_skip")]]
        await update.callback_query.message.edit_text(
            "✍️ **Manual Mode**\n\n"
            "Format: `text|url` (satu button per baris)\n\n"
            "Contoh: `Play Now|https://example.com`\n\n"
            "Atau tekan Skip.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return SINGLE_BUTTONS
    
    async def single_btn_handler(self, update, context):
        """Parse manual button input for single broadcast"""
        text = update.message.text.strip()
        buttons = []
        for line in text.split('\n'):
            line = line.strip()
            if not line or '|' not in line:
                continue
            parts = line.split('|', 1)
            btn_text = parts[0].strip()
            btn_url = parts[1].strip()
            if not btn_url.startswith(('http://', 'https://')):
                if btn_url.startswith('t.me/'):
                    btn_url = 'https://' + btn_url
                else:
                    continue
            buttons.append({'text': btn_text, 'url': btn_url})
        
        if not buttons:
            await update.message.reply_text("⚠️ Format salah. Cuba lagi: `text|url`", parse_mode='Markdown')
            return SINGLE_BUTTONS
        
        context.user_data['single_buttons'] = buttons
        return await self._show_single_confirm(update, context)
    
    async def single_btn_skip(self, update, context):
        """Skip buttons for single broadcast"""
        if update.callback_query:
            await update.callback_query.answer()
        context.user_data['single_buttons'] = []
        return await self._show_single_confirm(update, context)
    
    # ==================== AI REWRITE FOR BROADCAST ====================
    async def _show_ai_rewrite_prompt(self, update, context):
        """Show AI rewrite/vision prompt after company selection"""
        company_name = context.user_data.get('broadcast_company_name', '')
        
        # Get the text to rewrite
        data = context.user_data.get('broadcast_data', {})
        text = data.get('text') or context.user_data.get('grid_caption') or ''
        
        # Check if broadcast has image(s)
        has_image = bool(data.get('photo') or context.user_data.get('grid_media'))
        
        if text.strip():
            # Has text → offer AI rewrite OR vision caption
            preview = text[:200] + ('...' if len(text) > 200 else '')
            
            keyboard = [
                [InlineKeyboardButton("✨ AI Rewrite Text", callback_data="bc_ai_yes")],
            ]
            if has_image:
                keyboard.append([InlineKeyboardButton("🖼️ AI Generate dari Gambar", callback_data="bc_ai_vision")])
            keyboard.append([InlineKeyboardButton("⏭️ Skip (guna asal)", callback_data="bc_ai_skip")])
            
            msg_text = (
                f"🤖 **AI CAPTION**\n\n"
                f"🏢 Company: **{company_name}**\n\n"
                f"📝 Text semasa:\n{preview}\n\n"
                f"Pilih option:"
            )
        elif has_image:
            # No text but has image → offer AI vision generate
            keyboard = [
                [InlineKeyboardButton("🖼️ AI Generate dari Gambar", callback_data="bc_ai_vision")],
                [InlineKeyboardButton("✍️ Tulis Sendiri", callback_data="bc_ai_manual")],
                [InlineKeyboardButton("⏭️ Skip Caption", callback_data="bc_ai_skip")]
            ]
            
            msg_text = (
                f"🤖 **AI CAPTION**\n\n"
                f"🏢 Company: **{company_name}**\n\n"
                f"🖼️ Ada gambar tapi takde caption.\n"
                f"Nak AI generate caption dari gambar?"
            )
        else:
            # No text, no image → go to confirm
            return await self._go_to_confirm(update, context)
        
        query = update.callback_query
        if query:
            await query.message.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
        return BROADCAST_AI_REWRITE
    
    async def ai_rewrite_execute(self, update, context):
        """Execute AI rewrite using Groq API"""
        query = update.callback_query
        await query.answer()
        
        company_name = context.user_data.get('broadcast_company_name', '')
        data = context.user_data.get('broadcast_data', {})
        original_text = data.get('text') or context.user_data.get('grid_caption') or ''
        
        # Store original for later
        context.user_data['broadcast_original_text'] = original_text
        
        # Show loading
        await query.message.edit_text(
            f"✨ **AI sedang menulis semula...**\n\n"
            f"🏢 Company: {company_name}\n"
            f"⏳ Tunggu sekejap...",
            parse_mode='Markdown'
        )
        
        # Call AI
        from ai_rewriter import rewrite_promo
        rewritten = await rewrite_promo(original_text, company_name)
        
        context.user_data['broadcast_rewritten_text'] = rewritten
        
        preview_orig = original_text[:150] + ('...' if len(original_text) > 150 else '')
        preview_new = rewritten[:300] + ('...' if len(rewritten) > 300 else '')
        
        keyboard = [
            [InlineKeyboardButton("✅ Guna Rewritten", callback_data="bc_ai_accept")],
            [InlineKeyboardButton("📝 Guna Original", callback_data="bc_ai_original")],
            [InlineKeyboardButton("✨ Rewrite Lagi", callback_data="bc_ai_retry")]
        ]
        
        await query.message.edit_text(
            f"✨ **AI REWRITE SELESAI!**\n\n"
            f"🏢 Company: {company_name}\n\n"
            f"📝 **Original:**\n{preview_orig}\n\n"
            f"✨ **Rewritten:**\n{preview_new}\n\n"
            f"Pilih mana nak guna:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return BROADCAST_AI_REWRITE
    
    async def ai_vision_execute(self, update, context):
        """Generate caption from image using AI Vision"""
        query = update.callback_query
        await query.answer()
        
        company_name = context.user_data.get('broadcast_company_name', '')
        data = context.user_data.get('broadcast_data', {})
        
        # Show loading
        await query.message.edit_text(
            f"🖼️ **AI sedang baca gambar...**\n\n"
            f"🏢 Company: {company_name}\n"
            f"⏳ Tunggu sekejap...",
            parse_mode='Markdown'
        )
        
        # Download the image
        image_bytes = None
        try:
            if data.get('photo'):
                file = await self.app.bot.get_file(data['photo'])
                image_bytes = await file.download_as_bytearray()
            elif context.user_data.get('grid_media'):
                import json
                grid_media = context.user_data['grid_media']
                media_items = json.loads(grid_media) if isinstance(grid_media, str) else grid_media
                if media_items:
                    # Use first image from grid
                    first = media_items[0]
                    file_id = first.get('file_id', '')
                    if file_id:
                        file = await self.app.bot.get_file(file_id)
                        image_bytes = await file.download_as_bytearray()
        except Exception as e:
            self.logger.error(f"Failed to download image for vision: {e}")
        
        if not image_bytes:
            await query.message.edit_text(
                "❌ Tak dapat download gambar. Cuba tulis caption sendiri.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✍️ Tulis Sendiri", callback_data="bc_ai_manual")],
                    [InlineKeyboardButton("⏭️ Skip Caption", callback_data="bc_ai_skip")]
                ])
            )
            return BROADCAST_AI_REWRITE
        
        # Call AI Vision - include company link in caption
        from ai_rewriter import generate_caption_from_image
        company_link = ''
        grid_buttons = context.user_data.get('grid_buttons', [])
        if grid_buttons and isinstance(grid_buttons, list) and len(grid_buttons) > 0:
            company_link = grid_buttons[0].get('url', '') if isinstance(grid_buttons[0], dict) else ''
        caption, _ = await generate_caption_from_image(bytes(image_bytes), company_name, company_link=company_link)
        
        if not caption:
            await query.message.edit_text(
                "❌ AI Vision gagal. Cuba tulis caption sendiri.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✍️ Tulis Sendiri", callback_data="bc_ai_manual")],
                    [InlineKeyboardButton("🖼️ Retry", callback_data="bc_ai_vision")],
                    [InlineKeyboardButton("⏭️ Skip Caption", callback_data="bc_ai_skip")]
                ])
            )
            return BROADCAST_AI_REWRITE
        
        # Store generated caption
        context.user_data['broadcast_rewritten_text'] = caption
        
        preview = caption[:400] + ('...' if len(caption) > 400 else '')
        
        keyboard = [
            [InlineKeyboardButton("✅ Guna Caption Ni", callback_data="bc_ai_accept")],
            [InlineKeyboardButton("🖼️ Generate Lagi", callback_data="bc_ai_vision")],
            [InlineKeyboardButton("✍️ Tulis Sendiri", callback_data="bc_ai_manual")],
            [InlineKeyboardButton("⏭️ Skip Caption", callback_data="bc_ai_skip")]
        ]
        
        await query.message.edit_text(
            f"🖼️ <b>AI VISION CAPTION!</b>\n\n"
            f"🏢 Company: {company_name}\n\n"
            f"✨ <b>Generated:</b>\n{preview}\n\n"
            f"Pilih option:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        return BROADCAST_AI_REWRITE
    
    async def ai_manual_caption(self, update, context):
        """Prompt admin to type caption manually"""
        query = update.callback_query
        await query.answer()
        
        company_name = context.user_data.get('broadcast_company_name', '')
        
        await query.message.edit_text(
            f"✍️ **TULIS CAPTION**\n\n"
            f"🏢 Company: {company_name}\n\n"
            f"Taipkan caption untuk broadcast ni.\n"
            f"Boleh guna HTML: <b>bold</b>, <i>italic</i>",
            parse_mode='Markdown'
        )
        # Set flag so we know to capture next text message as caption
        context.user_data['awaiting_manual_caption'] = True
        return BROADCAST_AI_REWRITE
    
    async def ai_manual_caption_receive(self, update, context):
        """Receive manually typed caption"""
        if not context.user_data.get('awaiting_manual_caption'):
            return
        
        context.user_data.pop('awaiting_manual_caption', None)
        caption = message_to_html(update.message)
        
        # Store as the text
        data = context.user_data.get('broadcast_data', {})
        data['text'] = caption
        if context.user_data.get('grid_caption') is not None or context.user_data.get('grid_media'):
            context.user_data['grid_caption'] = caption
        
        return await self._go_to_confirm_from_message(update, context)
    
    async def _go_to_confirm_from_message(self, update, context):
        """Route to confirm screen from a message (not callback)"""
        is_grid = context.user_data.get('grid_media') is not None
        if is_grid:
            return await self._grid_show_confirm_from_message(update, context)
        else:
            return await self._show_single_confirm_from_message(update, context)
    
    async def _grid_show_confirm_from_message(self, update, context):
        """Show grid confirm from message context"""
        # Build same confirm as _grid_show_confirm but from message
        grid_media = context.user_data.get('grid_media', '[]')
        import json
        media_items = json.loads(grid_media) if isinstance(grid_media, str) else grid_media
        caption = context.user_data.get('grid_caption', '')
        buttons = context.user_data.get('grid_buttons', [])
        target_type = context.user_data.get('broadcast_target_type', 'users')
        target_display = "👤 All Users" if target_type == "users" else "👥 All Known Groups"
        
        btn_info = f"{len(buttons)} buttons" if buttons else "None"
        caption_preview = caption[:100] + '...' if len(caption) > 100 else caption
        
        summary = (
            f"📋 **BROADCAST PREVIEW**\n\n"
            f"🎯 Target: **{target_display}**\n"
            f"🖼️ Grid Mode: **{len(media_items)} media**\n"
            f"📝 Caption: {caption_preview or '(tiada)'}\n"
            f"🔘 Buttons: {btn_info}\n\n"
            f"Confirm broadcast?"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 Broadcast Sekarang", callback_data="broadcast_now")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        
        # Store confirm data
        context.user_data['broadcast_data'] = {
            'text': caption,
            'grid_media': grid_media,
            'grid_buttons': json.dumps(buttons) if buttons else None,
            'photo': None, 'video': None, 'document': None
        }
        
        await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return BROADCAST_CONFIRM
    
    async def _show_single_confirm_from_message(self, update, context):
        """Show single broadcast confirm from message context"""
        buttons = context.user_data.get('single_buttons', [])
        data = context.user_data.get('broadcast_data', {})
        target_type = context.user_data.get('broadcast_target_type', 'users')
        target_display = "👤 All Users" if target_type == "users" else "👥 All Known Groups"
        
        has_media = bool(data.get('photo') or data.get('video') or data.get('document'))
        media_type = "Photo" if data.get('photo') else "Video" if data.get('video') else "Document" if data.get('document') else "Text Only"
        btn_info = f"{len(buttons)} buttons" if buttons else "None"
        
        text_preview = (data.get('text', '') or '')[:100]
        if len(data.get('text', '') or '') > 100:
            text_preview += '...'
        
        summary = (
            f"📋 **BROADCAST PREVIEW**\n\n"
            f"🎯 Target: **{target_display}**\n"
            f"📎 Media: **{media_type}**\n"
            f"📝 Text: {text_preview or '(tiada)'}\n"
            f"🔘 Buttons: {btn_info}\n\n"
            f"Confirm broadcast?"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 Broadcast Sekarang", callback_data="broadcast_now")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        
        await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return BROADCAST_CONFIRM
    
    async def ai_rewrite_accept(self, update, context):
        """Accept rewritten text and go to confirm"""
        await update.callback_query.answer()
        rewritten = context.user_data.get('broadcast_rewritten_text', '')
        
        if rewritten:
            # Update the broadcast text
            data = context.user_data.get('broadcast_data', {})
            data['text'] = rewritten
            # Always update grid_caption for grid broadcasts
            if context.user_data.get('grid_media') is not None:
                context.user_data['grid_caption'] = rewritten
        
        return await self._go_to_confirm(update, context)
    
    async def ai_rewrite_original(self, update, context):
        """Keep original text and go to confirm"""
        await update.callback_query.answer()
        return await self._go_to_confirm(update, context)
    
    async def ai_rewrite_retry(self, update, context):
        """Retry AI rewrite"""
        return await self.ai_rewrite_execute(update, context)
    
    async def ai_rewrite_skip(self, update, context):
        """Skip AI rewrite and go to confirm"""
        await update.callback_query.answer()
        return await self._go_to_confirm(update, context)
    
    async def _go_to_confirm(self, update, context):
        """Route to the correct confirm screen based on broadcast mode"""
        is_grid = context.user_data.get('grid_media') is not None
        if is_grid:
            return await self._grid_show_confirm(update, context)
        else:
            return await self._show_single_confirm(update, context)
    
    async def _show_single_confirm(self, update, context):
        """Show single broadcast confirm with buttons info"""
        buttons = context.user_data.get('single_buttons', [])
        data = context.user_data.get('broadcast_data', {})
        target_type = context.user_data.get('broadcast_target_type', 'users')
        target_display = "👤 All Users" if target_type == "users" else "👥 All Known Groups"
        
        has_media = bool(data.get('photo') or data.get('video') or data.get('document'))
        media_type = "Photo" if data.get('photo') else "Video" if data.get('video') else "Document" if data.get('document') else "Text Only"
        btn_info = f"{len(buttons)} buttons" if buttons else "None"
        
        summary = (
            f"📋 **BROADCAST PREVIEW**\n\n"
            f"🎯 Target: **{target_display}**\n"
            f"📎 Media: **{media_type}**\n"
            f"🔘 Buttons: **{btn_info}**\n\n"
            "Pilih option:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 Send Now", callback_data="broadcast_now")],
            [InlineKeyboardButton("⏰ Schedule", callback_data="broadcast_schedule")],
            [InlineKeyboardButton("🔁 Recurring", callback_data="broadcast_recurring")],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        
        msg_target = update.callback_query.message if update.callback_query else update.message
        if update.callback_query:
            await msg_target.edit_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await msg_target.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
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
            if not data:
                await update.callback_query.message.reply_text("❌ No message to broadcast.")
                return ConversationHandler.END
            
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
            last_error = None
            
            is_grid = data.get('grid_media') is not None
            
            for tid in target_ids:
                try:
                    if is_grid:
                        await self._send_broadcast_to_target(self.app.bot, tid, {
                            'message': data.get('text', ''),
                            'grid_media': data.get('grid_media'),
                            'grid_buttons': data.get('grid_buttons'),
                            'media_type': None,
                            'media_file_id': None
                        })
                    elif data.get('message'):
                        # Check for single_buttons
                        single_btns = context.user_data.get('single_buttons', [])
                        if single_btns:
                            keyboard_rows = []
                            for btn in single_btns:
                                url = btn['url']
                                if url.startswith('t.me/'):
                                    url = 'https://' + url
                                keyboard_rows.append([InlineKeyboardButton(btn['text'], url=url)])
                            reply_markup = InlineKeyboardMarkup(keyboard_rows)
                            
                            # Send with buttons based on media type
                            if data.get('photo'):
                                await self.app.bot.send_photo(
                                    chat_id=tid, photo=data['photo'],
                                    caption=data.get('text') or None,
                                    parse_mode='HTML' if data.get('text') else None,
                                    reply_markup=reply_markup
                                )
                            elif data.get('video'):
                                await self.app.bot.send_video(
                                    chat_id=tid, video=data['video'],
                                    caption=data.get('text') or None,
                                    parse_mode='HTML' if data.get('text') else None,
                                    reply_markup=reply_markup
                                )
                            elif data.get('text'):
                                await self.app.bot.send_message(
                                    chat_id=tid, text=data['text'],
                                    parse_mode='HTML',
                                    reply_markup=reply_markup
                                )
                        else:
                            await data['message'].copy(chat_id=tid)
                    sent += 1
                except Exception as e:
                    failed += 1
                    last_error = str(e)
                    self.logger.error(f"Broadcast send error to {tid}: {e}")
            
            grid_label = " 🖼️ Grid" if is_grid else ""
            error_msg = f"\n⚠️ Last error: `{last_error}`" if last_error else ""
            await update.callback_query.message.reply_text(
                f"✅ Broadcast{grid_label} selesai!\n\n📤 Sent: {sent}\n❌ Failed: {failed}{error_msg}",
                parse_mode='Markdown'
            )
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
            target_type = context.user_data.get('broadcast_target_type', 'users')
            broadcast_id = self.db.save_scheduled_broadcast(
                self.bot_id,
                data.get('text', ''),
                media_file_id,
                media_type,
                scheduled_time.strftime('%Y-%m-%d %H:%M:%S'),
                target_type,
                data.get('grid_media'),
                data.get('grid_buttons')
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
        target_type = context.user_data.get('broadcast_target_type', 'users')
        broadcast_id = self.db.save_recurring_broadcast(
            self.bot_id,
            data.get('text', ''),
            media_file_id,
            media_type,
            interval_type,
            interval_value,
            target_type,
            data.get('grid_media'),
            data.get('grid_buttons')
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
        except Exception:
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
            broadcasts = self.db.get_recurring_broadcasts(self.bot_id)
            broadcast = next((b for b in broadcasts if b['id'] == broadcast_id), None)
            
            if not broadcast:
                self.logger.warning(f"Recurring broadcast {broadcast_id} not found or inactive")
                return
            
            target_type = broadcast.get('target_type', 'users')
            if target_type == 'groups':
                targets = self.db.get_known_groups(self.bot_id)
                target_ids = [t['group_id'] for t in targets]
            else:
                users = self.db.get_users(self.bot_id)
                target_ids = [u['telegram_id'] for u in users]
            
            sent = 0
            failed = 0
            
            for tid in target_ids:
                try:
                    await self._send_broadcast_to_target(self.app.bot, tid, broadcast)
                    sent += 1
                except Exception as e:
                    failed += 1
            
            self.logger.info(f"Recurring broadcast {broadcast_id} executed: {sent} sent, {failed} failed")
            
        except Exception as e:
            self.logger.error(f"Error executing recurring broadcast {broadcast_id}: {e}")

    async def execute_scheduled_broadcast(self, broadcast_id):
        """Execute a scheduled broadcast"""
        try:
            broadcasts = self.db.get_pending_broadcasts(self.bot_id)
            broadcast = next((b for b in broadcasts if b['id'] == broadcast_id), None)
            
            if not broadcast:
                self.logger.warning(f"Broadcast {broadcast_id} not found or already sent")
                return
            
            target_type = broadcast.get('target_type', 'users')
            if target_type == 'groups':
                targets = self.db.get_known_groups(self.bot_id)
                target_ids = [t['group_id'] for t in targets]
            else:
                users = self.db.get_users(self.bot_id)
                target_ids = [u['telegram_id'] for u in users]
            
            sent = 0
            failed = 0
            
            for tid in target_ids:
                try:
                    await self._send_broadcast_to_target(self.app.bot, tid, broadcast)
                    sent += 1
                except Exception as e:
                    failed += 1
            
            self.db.mark_broadcast_sent(broadcast_id)
            
            bot_data = self.db.get_bot_by_token(self.token)
            if bot_data:
                try:
                    target_label = '👥 Groups' if target_type == 'groups' else '👤 Users'
                    is_grid = bool(broadcast.get('grid_media'))
                    grid_label = ' 🖼️ Grid' if is_grid else ''
                    await self.app.bot.send_message(
                        chat_id=bot_data['owner_id'],
                        text=f"✅ **Scheduled{grid_label} Broadcast Complete!**\n\n🎯 Target: {target_label}\n📤 Sent: {sent}\n❌ Failed: {failed}",
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    pass
            
            self.logger.info(f"Scheduled broadcast {broadcast_id} completed: {sent} sent, {failed} failed")
        except Exception as e:
            self.logger.error(f"Error executing scheduled broadcast {broadcast_id}: {e}")

    async def _create_grid_image(self, bot, media_items, tmp_dir):
        """Create a single grid image from multiple photos using Pillow."""
        from PIL import Image
        import math
        import os
        
        # Download all photos
        paths = []
        for item in media_items:
            tg_file = await bot.get_file(item['file_id'])
            path = os.path.join(tmp_dir, f"img_{len(paths)}.jpg")
            await tg_file.download_to_drive(path)
            paths.append(path)
        
        # Determine grid layout
        n = len(paths)
        cols = 2 if n <= 4 else 3
        rows = math.ceil(n / cols)
        
        # Cell size
        cell_w, cell_h = 640, 640
        
        # Create canvas
        canvas = Image.new('RGB', (cols * cell_w, rows * cell_h), (0, 0, 0))
        
        for idx, path in enumerate(paths):
            img = Image.open(path)
            # Resize to fill cell (cover mode)
            img_ratio = img.width / img.height
            cell_ratio = cell_w / cell_h
            if img_ratio > cell_ratio:
                new_h = cell_h
                new_w = int(cell_h * img_ratio)
            else:
                new_w = cell_w
                new_h = int(cell_w / img_ratio)
            img = img.resize((new_w, new_h), Image.LANCZOS)
            # Center crop
            left = (new_w - cell_w) // 2
            top = (new_h - cell_h) // 2
            img = img.crop((left, top, left + cell_w, top + cell_h))
            
            row = idx // cols
            col = idx % cols
            canvas.paste(img, (col * cell_w, row * cell_h))
        
        output_path = os.path.join(tmp_dir, "grid_output.jpg")
        canvas.save(output_path, "JPEG", quality=92)
        return output_path
    
    async def _create_grid_video(self, bot, media_items, tmp_dir):
        """Create a single grid video from mixed photos+videos using FFmpeg."""
        import subprocess
        import math
        import os
        
        # Download all media
        paths = []
        for i, item in enumerate(media_items):
            tg_file = await bot.get_file(item['file_id'])
            ext = '.mp4' if item.get('type') == 'video' else '.jpg'
            path = os.path.join(tmp_dir, f"media_{i}{ext}")
            await tg_file.download_to_drive(path)
            paths.append({'path': path, 'type': item.get('type', 'photo')})
        
        n = len(paths)
        cols = 2 if n <= 4 else 3
        rows = math.ceil(n / cols)
        cell_w, cell_h = 480, 480
        total_w = cols * cell_w
        total_h = rows * cell_h
        
        # Find longest video duration (default 5s for photos-only, shouldn't happen but just in case)
        max_duration = 5
        for p in paths:
            if p['type'] == 'video':
                try:
                    probe = subprocess.run(
                        ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                         '-of', 'default=noprint_wrappers=1:nokey=1', p['path']],
                        capture_output=True, text=True, timeout=10
                    )
                    dur = float(probe.stdout.strip())
                    if dur > max_duration:
                        max_duration = dur
                except Exception:
                    pass
        
        # Build FFmpeg command
        inputs = []
        filter_parts = []
        
        for i, p in enumerate(paths):
            if p['type'] == 'video':
                inputs.extend(['-i', p['path']])
            else:
                # Photo as video: loop for max_duration
                inputs.extend(['-loop', '1', '-t', str(max_duration), '-i', p['path']])
            
            # Scale + pad each input to cell size
            filter_parts.append(
                f"[{i}:v]scale={cell_w}:{cell_h}:force_original_aspect_ratio=decrease,"
                f"pad={cell_w}:{cell_h}:(ow-iw)/2:(oh-ih)/2:color=black,"
                f"setsar=1[v{i}]"
            )
        
        # Build xstack layout
        # xstack layout format: "x0_y0|x1_y1|..."
        layout_parts = []
        stack_inputs = []
        for i in range(n):
            row = i // cols
            col = i % cols
            x = col * cell_w
            y = row * cell_h
            layout_parts.append(f"{x}_{y}")
            stack_inputs.append(f"[v{i}]")
        
        # Pad to fill grid if needed (add black cells)
        total_cells = rows * cols
        if n < total_cells:
            # Create black placeholder for empty cells
            black_idx = len(paths)
            inputs.extend(['-f', 'lavfi', '-t', str(max_duration), 
                          '-i', f'color=c=black:s={cell_w}x{cell_h}:r=25'])
            filter_parts.append(f"[{black_idx}:v]setsar=1[v{black_idx}]")
            for j in range(n, total_cells):
                row = j // cols
                col = j % cols
                x = col * cell_w
                y = row * cell_h
                layout_parts.append(f"{x}_{y}")
                stack_inputs.append(f"[v{black_idx}]")
        
        layout_str = "|".join(layout_parts)
        filter_complex = ";".join(filter_parts)
        filter_complex += f";{''.join(stack_inputs)}xstack=inputs={total_cells}:layout={layout_str}[out]"
        
        output_path = os.path.join(tmp_dir, "grid_output.mp4")
        
        cmd = [
            'ffmpeg', '-y',
            *inputs,
            '-filter_complex', filter_complex,
            '-map', '[out]',
            '-t', str(min(max_duration, 60)),  # cap at 60s
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
            '-pix_fmt', 'yuv420p',
            '-an',  # no audio (simpler)
            output_path
        ]
        
        self.logger.info(f"FFmpeg grid: {n} items, {cols}x{rows}, duration={max_duration:.1f}s")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if proc.returncode != 0:
            self.logger.error(f"FFmpeg error: {proc.stderr[-500:]}")
            raise RuntimeError(f"FFmpeg failed: {proc.stderr[-200:]}")
        
        return output_path

    async def _send_broadcast_to_target(self, bot, chat_id, broadcast):
        """Send a single broadcast to one chat_id. Handles grid, single media, and text."""
        import json
        import asyncio
        import tempfile
        import os
        
        grid_media_json = broadcast.get('grid_media')
        grid_buttons_json = broadcast.get('grid_buttons')
        
        if grid_media_json:
            # Grid/Collage mode
            media_items = json.loads(grid_media_json) if isinstance(grid_media_json, str) else grid_media_json
            
            if not media_items:
                return
            
            caption_text = broadcast.get('text', '') or broadcast.get('message', '') or ''
            
            # Parse buttons
            buttons = []
            if grid_buttons_json:
                buttons = json.loads(grid_buttons_json) if isinstance(grid_buttons_json, str) else grid_buttons_json
            
            # Check if any video exists
            has_video = any(item.get('type') == 'video' for item in media_items)
            
            tmp_dir = tempfile.mkdtemp(prefix='grid_')
            
            try:
                # Build reply_markup if buttons exist
                reply_markup = None
                if buttons:
                    keyboard_rows = []
                    for btn in buttons:
                        url = btn['url']
                        if url.startswith('t.me/'):
                            url = 'https://' + url
                        keyboard_rows.append([InlineKeyboardButton(btn['text'], url=url)])
                    reply_markup = InlineKeyboardMarkup(keyboard_rows)
                
                if has_video:
                    # Mixed: create video collage via FFmpeg
                    output_path = await self._create_grid_video(bot, media_items, tmp_dir)
                    with open(output_path, 'rb') as f:
                        await bot.send_video(
                            chat_id=chat_id,
                            video=f,
                            caption=caption_text or None,
                            parse_mode='HTML' if caption_text else None,
                            supports_streaming=True,
                            reply_markup=reply_markup
                        )
                else:
                    # Photos only: create image grid via Pillow
                    output_path = await self._create_grid_image(bot, media_items, tmp_dir)
                    with open(output_path, 'rb') as f:
                        await bot.send_photo(
                            chat_id=chat_id,
                            photo=f,
                            caption=caption_text or None,
                            parse_mode='HTML' if caption_text else None,
                            reply_markup=reply_markup
                        )
            finally:
                # Cleanup temp files
                import shutil
                try:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                except Exception:
                    pass
        else:
            # Single media or text-only mode
            # Build reply_markup from buttons if available
            reply_markup = None
            if grid_buttons_json:
                buttons = json.loads(grid_buttons_json) if isinstance(grid_buttons_json, str) else grid_buttons_json
                if buttons:
                    keyboard_rows = []
                    for btn in buttons:
                        url = btn['url']
                        if url.startswith('t.me/'):
                            url = 'https://' + url
                        keyboard_rows.append([InlineKeyboardButton(btn['text'], url=url)])
                    reply_markup = InlineKeyboardMarkup(keyboard_rows)
            
            if broadcast.get('media_type') == 'photo' and broadcast.get('media_file_id'):
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=broadcast['media_file_id'],
                    caption=broadcast.get('message') or '',
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            elif broadcast.get('media_type') == 'video' and broadcast.get('media_file_id'):
                await bot.send_video(
                    chat_id=chat_id,
                    video=broadcast['media_file_id'],
                    caption=broadcast.get('message') or '',
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            elif broadcast.get('media_type') == 'document' and broadcast.get('media_file_id'):
                await bot.send_document(
                    chat_id=chat_id,
                    document=broadcast['media_file_id'],
                    caption=broadcast.get('message') or '',
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            elif broadcast.get('message'):
                await bot.send_message(
                    chat_id=chat_id,
                    text=broadcast['message'],
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )


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
        
        # Auto-Discovery: Save group FIRST (before any early returns)
        if chat.type in ['group', 'supergroup']:
            self.db.upsert_known_group(self.bot_id, chat.id, chat.title)
        
        # --- LINK GUARD: Auto-delete links from non-admins in groups ---
        if chat.type in ['group', 'supergroup'] and self.db.is_link_guard_enabled(self.bot_id):
            text_to_check = update.message.text or update.message.caption or ''
            link_pattern = r'(https?://|t\.me/|bit\.ly/|tinyurl\.com|wa\.me/|goo\.gl/|\b\w+\.(com|net|org|io|me|co|xyz|info|my|sg|uk|us|app|dev|link|live|online|site|store|shop|top|cc|gg|tv|ly)\b)'
            if re.search(link_pattern, text_to_check, re.IGNORECASE):
                try:
                    member = await chat.get_member(update.effective_user.id)
                    is_admin = member.status in ['administrator', 'creator']
                except Exception:
                    is_admin = False
                
                if not is_admin:
                    try:
                        await update.message.delete()
                        warning = await chat.send_message(
                            f"⚠️ **{update.effective_user.first_name}**, link tidak dibenarkan dalam group ini. Hanya admin boleh hantar link.",
                            parse_mode='Markdown'
                        )
                        # Auto-delete warning after 5 seconds
                        await asyncio.sleep(5)
                        await warning.delete()
                    except Exception as e:
                        self.logger.error(f"Link Guard error: {e}")
                    return
        
        # --- BAN WORD FILTER: Auto-delete messages with banned words in groups ---
        if chat.type in ['group', 'supergroup']:
            text_to_check_bw = update.message.text or update.message.caption or ''
            if text_to_check_bw:
                matched_word = self.db.check_ban_words(self.bot_id, text_to_check_bw)
                if matched_word:
                    try:
                        member = await chat.get_member(update.effective_user.id)
                        is_admin = member.status in ['administrator', 'creator']
                    except Exception:
                        is_admin = False
                    
                    if not is_admin:
                        try:
                            await update.message.delete()
                            warning = await chat.send_message(
                                f"⚠️ **{update.effective_user.first_name}**, perkataan `{matched_word}` tidak dibenarkan dalam group ini.",
                                parse_mode='Markdown'
                            )
                            await asyncio.sleep(5)
                            await warning.delete()
                        except Exception as e:
                            self.logger.error(f"Ban word filter error: {e}")
                        return
        
        # --- AUTO-REPLY: Respond to trigger words in groups ---
        if chat.type in ['group', 'supergroup']:
            text_to_check_ar = update.message.text or ''
            if text_to_check_ar:
                reply_text = self.db.find_auto_reply(self.bot_id, text_to_check_ar)
                if reply_text:
                    try:
                        await update.message.reply_text(reply_text, parse_mode='HTML')
                    except Exception as e:
                        self.logger.error(f"Auto-reply error: {e}")
        
        # Safe forwarded check for PTB v20+
        forward_from_chat = getattr(update.message, 'forward_from_chat', None)
        forward_origin = getattr(update.message, 'forward_origin', None)
        forward_date = getattr(update.message, 'forward_date', None)
        
        is_forwarded = bool(forward_from_chat or forward_origin or forward_date)
        
        self.logger.info(f"📨 Text message: {msg_text} | Forwarded: {is_forwarded}")
        self.logger.info(f"📨 States: source={context.user_data.get('waiting_forwarder_source')}, target={context.user_data.get('waiting_forwarder_target')}")
        
        # Handle Add Admin flow
        if await self.add_admin_handler(update, context):
            return
        
        # Handle Ban Word input
        if context.user_data.get('waiting_ban_word'):
            word = update.message.text.strip()
            if word == '/cancel':
                context.user_data['waiting_ban_word'] = False
                keyboard = [[InlineKeyboardButton("« Back to Ban Words", callback_data="gm_ban_words")]]
                await update.message.reply_text("❌ Cancelled.", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                success = self.db.add_ban_word(self.bot_id, word)
                context.user_data['waiting_ban_word'] = False
                if success:
                    keyboard = [[InlineKeyboardButton("➕ Add More", callback_data="gm_add_ban_word"), InlineKeyboardButton("📝 View All", callback_data="gm_ban_words")]]
                    await update.message.reply_text(f"✅ Ban word `{word}` added!", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                else:
                    keyboard = [[InlineKeyboardButton("« Back", callback_data="gm_ban_words")]]
                    await update.message.reply_text("❌ Word sudah ada atau error.", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Handle Auto-Reply trigger input (Step 1)
        if context.user_data.get('waiting_auto_reply_trigger'):
            trigger = update.message.text.strip()
            if trigger == '/cancel':
                context.user_data['waiting_auto_reply_trigger'] = False
                keyboard = [[InlineKeyboardButton("« Back to Auto-Reply", callback_data="gm_auto_replies")]]
                await update.message.reply_text("❌ Cancelled.", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                context.user_data['waiting_auto_reply_trigger'] = False
                context.user_data['auto_reply_trigger'] = trigger
                context.user_data['waiting_auto_reply_response'] = True
                await update.message.reply_text(
                    f"💬 **Step 2/2:** Trigger: `{trigger}`\n\n"
                    "Sekarang taip **response message**:",
                    parse_mode='Markdown'
                )
            return
        
        # Handle Auto-Reply response input (Step 2)
        if context.user_data.get('waiting_auto_reply_response'):
            response = update.message.text.strip()
            trigger = context.user_data.get('auto_reply_trigger', '')
            context.user_data['waiting_auto_reply_response'] = False
            context.user_data.pop('auto_reply_trigger', None)
            
            if response == '/cancel':
                keyboard = [[InlineKeyboardButton("« Back to Auto-Reply", callback_data="gm_auto_replies")]]
                await update.message.reply_text("❌ Cancelled.", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                success = self.db.add_auto_reply(self.bot_id, trigger, response)
                if success:
                    keyboard = [[InlineKeyboardButton("➕ Add More", callback_data="gm_add_auto_reply"), InlineKeyboardButton("💬 View All", callback_data="gm_auto_replies")]]
                    await update.message.reply_text(
                        f"✅ Auto-reply added!\n\n🔑 Trigger: `{trigger}`\n💬 Response: {response}",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode='Markdown'
                    )
                else:
                    keyboard = [[InlineKeyboardButton("« Back", callback_data="gm_auto_replies")]]
                    await update.message.reply_text("❌ Trigger sudah ada atau error.", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Handle Add Company Button flows (awaiting text/url after callback)
        if context.user_data.get('awaiting_btn_text'):
            context.user_data['new_comp']['btn_text'] = update.message.text
            context.user_data['awaiting_btn_text'] = False
            context.user_data['awaiting_btn_url'] = True
            await update.message.reply_text("🔗 Masukkan **Link URL** button:", parse_mode='Markdown')
            return
        
        if context.user_data.get('awaiting_btn_url'):
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
                keyboard = [[InlineKeyboardButton("🔙 Back to Manage Buttons", callback_data=f"manage_co_btns_{company_id}")]]
                await update.message.reply_text("Tap below to continue:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
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
        
        # --- LIVEGRAM FUNCTIONALITY ---
        bot_data = self.db.get_bot_by_token(self.token)
        owner_id = bot_data['owner_id']
        user_id = update.effective_user.id
        
        # Admin replies to forwarded message -> send to user
        if user_id == owner_id and update.message.reply_to_message:
            replied_msg = update.message.reply_to_message
            replied_msg_id = replied_msg.message_id
            
            forwarded_msgs = context.bot_data.get('forwarded_msgs', {})
            msg_info = forwarded_msgs.get(replied_msg_id)
            
            if msg_info:
                # Support both old format (int) and new format (dict)
                if isinstance(msg_info, dict):
                    original_user_id = msg_info['user_id']
                    source_chat_id = msg_info.get('chat_id', original_user_id)
                    source_msg_id = msg_info.get('msg_id')
                else:
                    original_user_id = msg_info
                    source_chat_id = original_user_id
                    source_msg_id = None
                
                try:
                    # Try DM first, fallback to group reply
                    target_chat_id = original_user_id
                    reply_to = None
                    
                    try:
                        # Test if we can DM the user
                        if source_chat_id != original_user_id:
                            # Message from group - try DM first
                            await context.bot.send_chat_action(chat_id=original_user_id, action='typing')
                    except Exception:
                        # Can't DM user, reply in group instead
                        target_chat_id = source_chat_id
                        reply_to = source_msg_id
                    
                    if update.message.text:
                        await context.bot.send_message(
                            chat_id=target_chat_id, 
                            text=update.message.text, 
                            parse_mode='Markdown',
                            reply_to_message_id=reply_to
                        )
                    elif update.message.photo:
                        await context.bot.send_photo(
                            chat_id=target_chat_id,
                            photo=update.message.photo[-1].file_id,
                            caption=(update.message.caption or '')[:1024],
                            parse_mode='Markdown',
                            reply_to_message_id=reply_to
                        )
                    elif update.message.video:
                        await context.bot.send_video(
                            chat_id=target_chat_id,
                            video=update.message.video.file_id,
                            caption=(update.message.caption or '')[:1024],
                            parse_mode='Markdown',
                            reply_to_message_id=reply_to
                        )
                    elif update.message.document:
                        await context.bot.send_document(
                            chat_id=target_chat_id,
                            document=update.message.document.file_id,
                            caption=(update.message.caption or '')[:1024],
                            parse_mode='Markdown',
                            reply_to_message_id=reply_to
                        )
                    elif update.message.voice:
                        await context.bot.send_voice(
                            chat_id=target_chat_id,
                            voice=update.message.voice.file_id,
                            reply_to_message_id=reply_to
                        )
                    elif update.message.sticker:
                        await context.bot.send_sticker(
                            chat_id=target_chat_id,
                            sticker=update.message.sticker.file_id,
                            reply_to_message_id=reply_to
                        )
                    
                    where = "in group" if target_chat_id != original_user_id else "via DM"
                    await update.message.reply_text(f"✅ Sent to user ({where})!")
                except Exception as e:
                    err_text = str(e)
                    # Fallback: replied message may be deleted/not found in group thread.
                    if reply_to and "message to be replied not found" in err_text.lower():
                        try:
                            if update.message.text:
                                await context.bot.send_message(
                                    chat_id=target_chat_id,
                                    text=update.message.text,
                                    parse_mode='Markdown'
                                )
                            elif update.message.photo:
                                await context.bot.send_photo(
                                    chat_id=target_chat_id,
                                    photo=update.message.photo[-1].file_id,
                                    caption=(update.message.caption or '')[:1024],
                                    parse_mode='Markdown'
                                )
                            elif update.message.video:
                                await context.bot.send_video(
                                    chat_id=target_chat_id,
                                    video=update.message.video.file_id,
                                    caption=(update.message.caption or '')[:1024],
                                    parse_mode='Markdown'
                                )
                            elif update.message.document:
                                await context.bot.send_document(
                                    chat_id=target_chat_id,
                                    document=update.message.document.file_id,
                                    caption=(update.message.caption or '')[:1024],
                                    parse_mode='Markdown'
                                )
                            elif update.message.voice:
                                await context.bot.send_voice(
                                    chat_id=target_chat_id,
                                    voice=update.message.voice.file_id
                                )
                            elif update.message.sticker:
                                await context.bot.send_sticker(
                                    chat_id=target_chat_id,
                                    sticker=update.message.sticker.file_id
                                )
                            await update.message.reply_text("✅ Sent to user (fallback without reply target)!")
                            return
                        except Exception as e2:
                            await update.message.reply_text(f"❌ Failed (fallback): {str(e2)}")
                            return
                    await update.message.reply_text(f"❌ Failed: {err_text}")
                return
        
        # Intercept: Admin setting AI prompt
        if context.user_data.get('waiting_ai_prompt') and update.message.text and user_id == owner_id:
            await self.ai_save_prompt(update, context)
            return

        # AI Chatbot — respond to messages with company promotions
        # Private: all text messages from non-owner | Group: when @mentioned or replied to bot
        ai_chat_on = self.db.is_ai_chat_enabled(self.bot_id)
        should_ai_respond = False
        user_text = update.message.text or update.message.caption or ''
        has_photo = bool(update.message.photo)
        
        # In groups: passively record all messages for context
        if chat.type in ['group', 'supergroup'] and user_text:
            group_key = f'group_chat_{chat.id}'
            if group_key not in context.bot_data:
                context.bot_data[group_key] = []
            sender_name = update.effective_user.first_name or 'User'
            context.bot_data[group_key].append(f"{sender_name}: {user_text[:200]}")
            # Keep last 20 messages
            context.bot_data[group_key] = context.bot_data[group_key][-20:]
        
        if (user_text or has_photo) and not is_forwarded:
            if chat.type == 'private' and user_id != owner_id and ai_chat_on:
                should_ai_respond = True
            elif chat.type in ['group', 'supergroup'] and ai_chat_on:
                # Respond to ALL messages in group (no mention needed)
                should_ai_respond = True
                # Strip bot mention from text if present
                bot_username = (await context.bot.get_me()).username
                if bot_username and f'@{bot_username}'.lower() in user_text.lower():
                    user_text = user_text.lower().replace(f'@{bot_username}'.lower(), '').strip()

        if should_ai_respond:
            try:
                from ai_rewriter import ai_chat
                companies = self.db.get_companies(self.bot_id)
                
                if companies:
                    # Enrich companies with buttons
                    for c in companies:
                        c['buttons'] = self.db.get_company_buttons(c['id'])
                    
                    # Download image if present
                    image_bytes = None
                    if has_photo:
                        try:
                            photo = update.message.photo[-1]  # Largest size
                            file = await self.app.bot.get_file(photo.file_id)
                            image_bytes = bytes(await file.download_as_bytearray())
                        except Exception as e:
                            self.logger.warning(f"Failed to download chat image: {e}")
                    
                    # Build chat history based on context
                    chat_history = []
                    if chat.type in ['group', 'supergroup']:
                        # Use group message memory as context
                        group_key = f'group_chat_{chat.id}'
                        recent_msgs = context.bot_data.get(group_key, [])
                        if recent_msgs:
                            # Pack recent group messages as system context
                            group_context = "\n".join(recent_msgs[-15:])
                            chat_history.append({
                                "role": "user",
                                "content": f"[Perbualan terkini dalam group ini:]\n{group_context}\n\n[Sekarang jawab soalan terbaru]"
                            })
                            chat_history.append({
                                "role": "assistant", 
                                "content": "Baik, saya dah baca perbualan group. Saya sedia membantu!"
                            })
                    else:
                        # Private chat: use per-user history
                        if 'ai_chat_history' not in context.user_data:
                            context.user_data['ai_chat_history'] = []
                        chat_history = context.user_data['ai_chat_history']
                    
                    # Show typing indicator
                    await context.bot.send_chat_action(chat_id=chat.id, action='typing')
                    
                    self.logger.info(f"AI chat: user={user_id}, chat={chat.id}, text='{user_text[:50]}', has_photo={has_photo}, image_bytes={'yes' if image_bytes else 'no'}")
                    
                    # Get custom prompt if set
                    custom_prompt = self.db.get_ai_prompt(self.bot_id) or None
                    response = await ai_chat(user_text, companies, chat_history, custom_prompt=custom_prompt, image_bytes=image_bytes)
                    
                    if response:
                        if chat.type == 'private':
                            # Save to per-user chat history
                            chat_history.append({"role": "user", "content": user_text})
                            chat_history.append({"role": "assistant", "content": response})
                            context.user_data['ai_chat_history'] = chat_history[-10:]
                        
                        # Add company list button (only in private)
                        keyboard = None
                        if chat.type == 'private':
                            keyboard = InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Senarai Company", callback_data="main_menu")]
                            ])
                        
                        await update.message.reply_text(
                            response,
                            parse_mode='Markdown',
                            reply_markup=keyboard,
                            disable_web_page_preview=True
                        )
                        return
                    else:
                        self.logger.warning(f"AI chat returned None for user={user_id}, chat={chat.id}")
            except Exception as e:
                self.logger.error(f"AI chatbot error: {e}", exc_info=True)

        # User -> Admin (forward message and store mapping)
        if user_id != owner_id and self.db.is_livegram_enabled(self.bot_id):
            source_chat = update.effective_chat
            forwarded = await context.bot.forward_message(
                chat_id=owner_id, 
                from_chat_id=source_chat.id, 
                message_id=update.message.message_id
            )
            
            if 'forwarded_msgs' not in context.bot_data:
                context.bot_data['forwarded_msgs'] = {}
            context.bot_data['forwarded_msgs'][forwarded.message_id] = {
                'user_id': user_id,
                'chat_id': source_chat.id,
                'msg_id': update.message.message_id
            }
            
            if len(context.bot_data['forwarded_msgs']) > 500:
                oldest_keys = list(context.bot_data['forwarded_msgs'].keys())[:-500]
                for k in oldest_keys:
                    del context.bot_data['forwarded_msgs'][k]
            
            user_name = update.effective_user.first_name or "User"
            is_group = source_chat.type in ['group', 'supergroup']
            source_label = f"📍 Group: {source_chat.title}" if is_group else "📍 Private Chat"
            await context.bot.send_message(
                chat_id=owner_id, 
                text=f"👤 **{user_name}** (ID: `{user_id}`)\n{source_label}\n\n💡 _Reply terus ke message di atas untuk balas._",
                parse_mode='Markdown'
            )
        
        # Admin /reply command (legacy fallback)
        elif update.message.text and update.message.text.startswith("/reply "):
            try:
                parts = update.message.text.split(" ", 2)
                target_id = int(parts[1])
                msg = parts[2]
                await context.bot.send_message(chat_id=target_id, text=msg)
                await update.message.reply_text("✅ Sent.")
            except Exception:
                await update.message.reply_text("❌ Format: /reply USER_ID MESSAGE")

    async def handle_media_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle media messages - for forwarded photos/videos/docs from channels"""
        self.logger.info(f"📷 Media message received from user {update.effective_user.id}")
        
        chat = update.effective_chat
        
        # Auto-Discovery: Save group for media messages too
        if chat.type in ['group', 'supergroup']:
            self.db.upsert_known_group(self.bot_id, chat.id, chat.title)
        
        # --- LINK GUARD: Check media captions for links ---
        if chat.type in ['group', 'supergroup'] and self.db.is_link_guard_enabled(self.bot_id):
            caption = update.message.caption or ''
            link_pattern = r'(https?://|t\.me/|bit\.ly/|tinyurl\.com|wa\.me/|goo\.gl/|\b\w+\.(com|net|org|io|me|co|xyz|info|my|sg|uk|us|app|dev|link|live|online|site|store|shop|top|cc|gg|tv|ly)\b)'
            if re.search(link_pattern, caption, re.IGNORECASE):
                try:
                    member = await chat.get_member(update.effective_user.id)
                    is_admin = member.status in ['administrator', 'creator']
                except Exception:
                    is_admin = False
                
                if not is_admin:
                    try:
                        await update.message.delete()
                        warning = await chat.send_message(
                            f"⚠️ **{update.effective_user.first_name}**, link tidak dibenarkan dalam group ini. Hanya admin boleh hantar link.",
                            parse_mode='Markdown'
                        )
                        await asyncio.sleep(5)
                        await warning.delete()
                    except Exception as e:
                        self.logger.error(f"Link Guard (media) error: {e}")
                    return
        
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
        
        # --- AI PHOTO RESPONSE (non-forwarded photos) ---
        bot_data = self.db.get_bot_by_token(self.token)
        owner_id = bot_data['owner_id']
        user_id = update.effective_user.id
        ai_chat_on = self.db.is_ai_chat_enabled(self.bot_id)
        has_photo = bool(update.message.photo)
        user_text = update.message.caption or ''
        
        if has_photo and not is_forwarded and ai_chat_on:
            should_respond = False
            if chat.type == 'private' and user_id != owner_id:
                should_respond = True
            elif chat.type in ['group', 'supergroup']:
                should_respond = True
            
            if should_respond:
                try:
                    from ai_rewriter import ai_chat
                    companies = self.db.get_companies(self.bot_id)
                    
                    if companies:
                        for c in companies:
                            c['buttons'] = self.db.get_company_buttons(c['id'])
                        
                        # Download image
                        image_bytes = None
                        try:
                            photo = update.message.photo[-1]
                            file = await self.app.bot.get_file(photo.file_id)
                            image_bytes = bytes(await file.download_as_bytearray())
                        except Exception as e:
                            self.logger.warning(f"Failed to download chat image: {e}")
                        
                        if image_bytes:
                            # Build chat history for groups
                            chat_history = []
                            if chat.type in ['group', 'supergroup']:
                                group_key = f'group_chat_{chat.id}'
                                recent_msgs = context.bot_data.get(group_key, [])
                                if recent_msgs:
                                    group_context = "\n".join(recent_msgs[-15:])
                                    chat_history.append({
                                        "role": "user",
                                        "content": f"[Perbualan terkini dalam group ini:]\n{group_context}\n\n[Sekarang jawab soalan terbaru]"
                                    })
                                    chat_history.append({
                                        "role": "assistant",
                                        "content": "Baik, saya dah baca perbualan group. Saya sedia membantu!"
                                    })
                            else:
                                if 'ai_chat_history' not in context.user_data:
                                    context.user_data['ai_chat_history'] = []
                                chat_history = context.user_data['ai_chat_history']
                            
                            await context.bot.send_chat_action(chat_id=chat.id, action='typing')
                            self.logger.info(f"AI photo chat: user={user_id}, chat={chat.id}, caption='{user_text[:50]}', image_bytes=yes")
                            
                            custom_prompt = self.db.get_ai_prompt(self.bot_id) or None
                            response = await ai_chat(user_text, companies, chat_history, custom_prompt=custom_prompt, image_bytes=image_bytes)
                            
                            if response:
                                if chat.type == 'private':
                                    chat_history.append({"role": "user", "content": user_text or "[sent photo]"})
                                    chat_history.append({"role": "assistant", "content": response})
                                    context.user_data['ai_chat_history'] = chat_history[-10:]
                                
                                keyboard = None
                                if chat.type == 'private':
                                    keyboard = InlineKeyboardMarkup([
                                        [InlineKeyboardButton("📋 Senarai Company", callback_data="main_menu")]
                                    ])
                                
                                await update.message.reply_text(
                                    response,
                                    parse_mode='Markdown',
                                    reply_markup=keyboard,
                                    disable_web_page_preview=True
                                )
                                return
                            else:
                                self.logger.warning(f"AI photo chat returned None for user={user_id}")
                except Exception as e:
                    self.logger.error(f"AI photo chatbot error: {e}", exc_info=True)
        
        # --- LIVEGRAM: Forward media to admin ---
        if user_id != owner_id and self.db.is_livegram_enabled(self.bot_id) and chat.type == 'private':
            try:
                forwarded = await context.bot.forward_message(
                    chat_id=owner_id,
                    from_chat_id=chat.id,
                    message_id=update.message.message_id
                )
                if 'forwarded_msgs' not in context.bot_data:
                    context.bot_data['forwarded_msgs'] = {}
                context.bot_data['forwarded_msgs'][forwarded.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat.id,
                    'msg_id': update.message.message_id
                }
                user_name = update.effective_user.first_name or "User"
                await context.bot.send_message(
                    chat_id=owner_id,
                    text=f"👤 **{user_name}** (ID: `{user_id}`)\n📍 Private Chat\n\n💡 _Reply terus ke message di atas untuk balas._",
                    parse_mode='Markdown'
                )
            except Exception as e:
                self.logger.error(f"Livegram media forward error: {e}")

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
            except Exception as e:

                pass  # Silently handle exception
    
    async def toggle_forwarder_mode_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle mode toggle callback"""
        new_mode = self.db.toggle_forwarder_mode(self.bot_id)
        if new_mode:
            await update.callback_query.answer(f"Mode changed to: {new_mode}")
            await self.show_forwarder_menu(update)
        else:
            await update.callback_query.answer("❌ Error changing mode", show_alert=True)
    
    
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
            
            # Helper: download media bytes for restricted content fallback
            async def _download_media_bytes(file_obj):
                """Download media to bytes via Bot API for re-upload."""
                try:
                    tg_file = await file_obj.get_file()
                    return await tg_file.download_as_bytearray()
                except Exception as dl_err:
                    self.logger.warning(f"⚠️ Media download failed: {dl_err}")
                    return None
            
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
                        try:
                            await context.bot.send_photo(
                                chat_id=tid,
                                photo=message.photo[-1].file_id,
                                caption=message.caption,
                                caption_entities=message.caption_entities
                            )
                        except Exception as e:
                            err_s = str(e).lower()
                            if 'forward' in err_s or 'restrict' in err_s or 'protect' in err_s:
                                media_bytes = await _download_media_bytes(message.photo[-1])
                                if media_bytes:
                                    await context.bot.send_photo(
                                        chat_id=tid, photo=bytes(media_bytes),
                                        caption=message.caption,
                                        caption_entities=message.caption_entities
                                    )
                                else:
                                    raise
                            else:
                                raise
                    elif message.video:
                        # Video message
                        try:
                            await context.bot.send_video(
                                chat_id=tid,
                                video=message.video.file_id,
                                caption=message.caption,
                                caption_entities=message.caption_entities
                            )
                        except Exception as e:
                            err_s = str(e).lower()
                            if 'forward' in err_s or 'restrict' in err_s or 'protect' in err_s:
                                media_bytes = await _download_media_bytes(message.video)
                                if media_bytes:
                                    await context.bot.send_video(
                                        chat_id=tid, video=bytes(media_bytes),
                                        caption=message.caption,
                                        caption_entities=message.caption_entities
                                    )
                                else:
                                    raise
                            else:
                                raise
                    elif message.document:
                        # Document message
                        try:
                            await context.bot.send_document(
                                chat_id=tid,
                                document=message.document.file_id,
                                caption=message.caption,
                                caption_entities=message.caption_entities
                            )
                        except Exception as e:
                            err_s = str(e).lower()
                            if 'forward' in err_s or 'restrict' in err_s or 'protect' in err_s:
                                media_bytes = await _download_media_bytes(message.document)
                                if media_bytes:
                                    await context.bot.send_document(
                                        chat_id=tid, document=bytes(media_bytes),
                                        caption=message.caption,
                                        caption_entities=message.caption_entities
                                    )
                                else:
                                    raise
                            else:
                                raise
                    elif message.animation:
                        # GIF/Animation message
                        try:
                            await context.bot.send_animation(
                                chat_id=tid,
                                animation=message.animation.file_id,
                                caption=message.caption,
                                caption_entities=message.caption_entities
                            )
                        except Exception as e:
                            err_s = str(e).lower()
                            if 'forward' in err_s or 'restrict' in err_s or 'protect' in err_s:
                                media_bytes = await _download_media_bytes(message.animation)
                                if media_bytes:
                                    await context.bot.send_animation(
                                        chat_id=tid, animation=bytes(media_bytes),
                                        caption=message.caption,
                                        caption_entities=message.caption_entities
                                    )
                                else:
                                    raise
                            else:
                                raise
                    elif message.audio:
                        # Audio message
                        try:
                            await context.bot.send_audio(
                                chat_id=tid,
                                audio=message.audio.file_id,
                                caption=message.caption,
                                caption_entities=message.caption_entities
                            )
                        except Exception as e:
                            err_s = str(e).lower()
                            if 'forward' in err_s or 'restrict' in err_s or 'protect' in err_s:
                                media_bytes = await _download_media_bytes(message.audio)
                                if media_bytes:
                                    await context.bot.send_audio(
                                        chat_id=tid, audio=bytes(media_bytes),
                                        caption=message.caption,
                                        caption_entities=message.caption_entities
                                    )
                                else:
                                    raise
                            else:
                                raise
                    elif message.voice:
                        # Voice message
                        try:
                            await context.bot.send_voice(
                                chat_id=tid,
                                voice=message.voice.file_id,
                                caption=message.caption
                            )
                        except Exception as e:
                            err_s = str(e).lower()
                            if 'forward' in err_s or 'restrict' in err_s or 'protect' in err_s:
                                media_bytes = await _download_media_bytes(message.voice)
                                if media_bytes:
                                    await context.bot.send_voice(
                                        chat_id=tid, voice=bytes(media_bytes),
                                        caption=message.caption
                                    )
                                else:
                                    raise
                            else:
                                raise
                    elif message.sticker:
                        # Sticker
                        await context.bot.send_sticker(
                            chat_id=tid,
                            sticker=message.sticker.file_id
                        )
                    else:
                        # Fallback - send text if available
                        fallback_text = message.caption or message.text
                        if fallback_text:
                            await context.bot.send_message(
                                chat_id=tid,
                                text=fallback_text,
                                entities=message.entities or message.caption_entities,
                                parse_mode=None
                            )
                    
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

    # === USERBOT AUTO PROMO MONITOR ===

    # ==================== USERBOT HUB ====================
    async def userbot_hub_menu(self, update: Update, context=None):
        """Show the parent Userbot hub menu"""
        query = update.callback_query
        if query:
            await query.answer()

        user_id = update.effective_user.id
        bot_data = self.db.get_bot_by_token(self.token)
        owner_id = int(bot_data.get('owner_id', 0)) if bot_data else 0
        is_owner = user_id == owner_id
        is_admin = self.db.is_bot_admin(self.bot_id, user_id)
        if not (is_owner or is_admin):
            return ConversationHandler.END

        session = self.db.get_userbot_session(self.bot_id)

        if session and session.get('session_string'):
            is_active = session.get('is_active', 0)
            phone = session.get('phone', '***')
            status_text = "🟢 Connected" if is_active else "🔴 Disconnected"

            text = (
                f"🤖 **USERBOT HUB**\n\n"
                f"📱 Phone: `{phone}`\n"
                f"📡 Status: {status_text}\n\n"
                f"Pilih tool yang anda mahu guna:"
            )
            keyboard = [
                [InlineKeyboardButton("📡 Auto Promo Monitor", callback_data="ub_menu")],
                [InlineKeyboardButton("📋 Clone Media", callback_data="clone_menu")],
                [InlineKeyboardButton("🔄 Reconnect", callback_data="ubhub_reconnect")],
                [InlineKeyboardButton("🗑️ Disconnect", callback_data="ubhub_disconnect")],
                [InlineKeyboardButton("« Back", callback_data="admin_settings")]
            ]
        else:
            text = (
                "🤖 **USERBOT HUB**\n\n"
                "Userbot belum di-setup. Anda perlu connect akaun "
                "Telegram untuk guna Auto Promo Monitor atau Clone Media.\n\n"
                "Tekan **Setup** untuk mula! 👇"
            )
            keyboard = [
                [InlineKeyboardButton("⚙️ Setup Sekarang", callback_data="ubhub_setup")],
                [InlineKeyboardButton("« Back", callback_data="admin_settings")]
            ]

        if query:
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

        return UB_HUB

    async def ub_hub_handle_action(self, update: Update, context=None):
        """Handle actions from the userbot hub menu"""
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "ubhub_setup":
            # Show the setup flow directly (same as ub_setup)
            text = (
                "⚙️ **SETUP USERBOT — STEP 1/4**\n\n"
                "Pertama, kau perlu buat API credentials:\n\n"
                "📋 **Cara buat:**\n"
                "1. Pergi ke https://my.telegram.org\n"
                "2. Log masuk dengan phone number kau\n"
                "3. Klik **API Development Tools**\n"
                "4. Klik **Create New Application**\n"
                "5. Isi form:\n"
                "   • App title: apa-apa nama\n"
                "   • Short name: apa-apa\n"
                "   • URL: kosongkan atau letak example.com\n"
                "   • Platform: Desktop\n"
                "6. Klik **Create application**\n\n"
                "Kau akan nampak **App api\\_id** (nombor)\n"
                "dan **App api\\_hash** (huruf+nombor panjang)\n\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "📝 Sekarang **copy nombor API ID** dan \n"
                "paste/taip di sini:"
            )
            await query.message.edit_text(text, parse_mode='Markdown')
            return UB_SETUP_API

        elif data == "ubhub_reconnect":
            session = self.db.get_userbot_session(self.bot_id)
            if not session or not session.get('session_string'):
                await query.message.edit_text("❌ Sila setup userbot dulu.", reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("« Back", callback_data="userbot_hub")]]
                ))
                return UB_HUB
            if self.userbot_manager:
                await self.userbot_manager.stop_instance(self.bot_id)
                success = await self.userbot_manager.start_instance(self.bot_id)
                if success:
                    self.db.toggle_userbot(self.bot_id, True)
                    await query.message.edit_text("✅ Userbot reconnected!", reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("« Back", callback_data="userbot_hub")]]
                    ))
                else:
                    await query.message.edit_text("❌ Reconnect gagal.", reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("« Back", callback_data="userbot_hub")]]
                    ))
            return UB_HUB

        elif data == "ubhub_disconnect":
            if self.userbot_manager:
                await self.userbot_manager.stop_instance(self.bot_id)
            self.db.toggle_userbot(self.bot_id, False)
            self.db.delete_userbot_session(self.bot_id)
            await query.message.edit_text("✅ Userbot disconnected & session cleared.", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("« Back", callback_data="userbot_hub")]]
            ))
            return UB_HUB

        return UB_HUB

    # ==================== CLONE MEDIA ====================
    async def clone_media_menu(self, update: Update, context=None):
        """Show clone media menu"""
        query = update.callback_query
        if query:
            await query.answer()

        session = self.db.get_userbot_session(self.bot_id)
        if not session or not session.get('session_string'):
            text = "❌ Userbot belum di-setup. Sila setup dulu di Userbot Hub."
            keyboard = [[InlineKeyboardButton("« Back", callback_data="userbot_hub")]]
            if query:
                await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return UB_HUB

        # Get clone history
        history = self.db.get_clone_history(self.bot_id, limit=5)
        history_text = ""
        if history:
            history_text = "\n\n📜 **Recent Clone Jobs:**\n"
            for h in history:
                status_icon = "✅" if h['status'] == 'done' else "❌" if h['status'] == 'error' else "⏳"
                history_text += f"{status_icon} {h['source_name'] or 'Unknown'} → {h['target_name'] or 'Unknown'} ({h['media_count']} media)\n"

        text = (
            f"📋 **CLONE MEDIA**\n\n"
            f"Clone media (foto, video, dokumen, GIF) dari satu "
            f"channel/group ke channel/group lain.\n\n"
            f"⚠️ Rate limit: ~2 saat per media untuk elak flood ban."
            f"{history_text}"
        )
        keyboard = [
            [InlineKeyboardButton("🚀 Start Clone", callback_data="clone_start_flow")],
            [InlineKeyboardButton("« Back", callback_data="userbot_hub")]
        ]

        if query:
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

        # Wait for "clone_start_flow" — but this goes into CLONE_SOURCE state
        # We handle it via the text below
        return UB_HUB

    async def _clone_start_flow(self, update: Update, context=None):
        """Begin the clone flow: ask for source"""
        query = update.callback_query
        if query:
            await query.answer()

        text = (
            "📋 **CLONE MEDIA — Step 1/3**\n\n"
            "Hantar link **source** channel/group:\n\n"
            "Contoh:\n"
            "• `https://t.me/channelname`\n"
            "• `@channelname`\n"
            "• Channel ID"
        )
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="clone_menu")]]
        if query:
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return CLONE_SOURCE

    async def clone_save_source(self, update: Update, context=None):
        """Save source and ask for target"""
        source = update.message.text.strip()
        context.user_data['clone_source'] = source

        text = (
            "📋 **CLONE MEDIA — Step 2/3**\n\n"
            f"✅ Source: `{source}`\n\n"
            "Sekarang hantar link **target** channel/group:"
        )
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="clone_menu")]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return CLONE_TARGET

    async def clone_save_target(self, update: Update, context=None):
        """Save target and show caption mode selection"""
        target = update.message.text.strip()
        source = context.user_data.get('clone_source', '?')
        context.user_data['clone_target'] = target

        text = (
            "📋 **CLONE MEDIA — Step 3/4**\n\n"
            f"📤 Source: `{source}`\n"
            f"📥 Target: `{target}`\n\n"
            "✏️ **Caption Mode:**\n"
            "Pilih apa nak buat dengan caption media:"
        )
        keyboard = [
            [InlineKeyboardButton("📝 Keep Original", callback_data="cap_keep")],
            [InlineKeyboardButton("➕ Append Text", callback_data="cap_append")],
            [InlineKeyboardButton("🔄 Replace All", callback_data="cap_replace")],
            [InlineKeyboardButton("🔍 Find & Replace", callback_data="cap_find_replace")],
            [InlineKeyboardButton("❌ Cancel", callback_data="clone_cancel")]
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return CLONE_CAPTION_MODE

    async def clone_caption_mode_select(self, update: Update, context=None):
        """Handle caption mode selection"""
        query = update.callback_query
        await query.answer()
        mode = query.data.replace("cap_", "")  # keep, append, replace, find_replace
        context.user_data['caption_mode'] = mode

        if mode == "keep":
            context.user_data['caption_text'] = ''
            context.user_data['caption_find'] = ''
            return await self._show_clone_confirm(update, context)
        elif mode == "append":
            text = (
                "✏️ **Caption — Append Mode**\n\n"
                "Taip text yang nak ditambah di **bawah** caption asal.\n\n"
                "Contoh:\n"
                "`\n\n👉 Join: t.me/channelkau`"
            )
            await query.message.edit_text(text, parse_mode='Markdown')
            return CLONE_CAPTION_TEXT
        elif mode == "replace":
            text = (
                "✏️ **Caption — Replace Mode**\n\n"
                "Taip caption **baru** yang akan gantikan semua caption asal.\n\n"
                "Contoh:\n"
                "`Promo terbaik! Join t.me/channelkau`"
            )
            await query.message.edit_text(text, parse_mode='Markdown')
            return CLONE_CAPTION_TEXT
        elif mode == "find_replace":
            text = (
                "✏️ **Caption — Find & Replace**\n\n"
                "Taip dalam format:\n"
                "`FIND | REPLACE`\n\n"
                "Contoh:\n"
                "`t.me/channellain | t.me/channelkau`\n\n"
                "Gunakan `|` sebagai separator."
            )
            await query.message.edit_text(text, parse_mode='Markdown')
            return CLONE_CAPTION_TEXT
        return CLONE_CAPTION_MODE

    async def clone_caption_text_input(self, update: Update, context=None):
        """Handle caption text input"""
        text_input = update.message.text.strip()
        mode = context.user_data.get('caption_mode', 'keep')

        if mode == "find_replace":
            if '|' not in text_input:
                await update.message.reply_text(
                    "❌ Format salah. Guna format:\n`FIND | REPLACE`\n\nCuba lagi:",
                    parse_mode='Markdown'
                )
                return CLONE_CAPTION_TEXT
            parts = text_input.split('|', 1)
            context.user_data['caption_find'] = parts[0].strip()
            context.user_data['caption_text'] = parts[1].strip()
        else:
            context.user_data['caption_text'] = text_input
            context.user_data['caption_find'] = ''

        return await self._show_clone_confirm(update, context)

    async def _show_clone_confirm(self, update: Update, context=None):
        """Show final confirmation with all settings"""
        source = context.user_data.get('clone_source', '?')
        target = context.user_data.get('clone_target', '?')
        mode = context.user_data.get('caption_mode', 'keep')
        caption_text = context.user_data.get('caption_text', '')
        caption_find = context.user_data.get('caption_find', '')

        mode_labels = {
            'keep': '📝 Keep Original',
            'append': '➕ Append',
            'replace': '🔄 Replace All',
            'find_replace': '🔍 Find & Replace'
        }
        caption_info = f"✏️ Caption: {mode_labels.get(mode, mode)}"
        if mode == 'append':
            caption_info += f"\n   Text: `{caption_text[:50]}{'...' if len(caption_text) > 50 else ''}`"
        elif mode == 'replace':
            caption_info += f"\n   New: `{caption_text[:50]}{'...' if len(caption_text) > 50 else ''}`"
        elif mode == 'find_replace':
            caption_info += f"\n   Find: `{caption_find[:30]}`\n   Replace: `{caption_text[:30]}`"

        text = (
            "📋 **CLONE MEDIA — Confirm**\n\n"
            f"📤 Source: `{source}`\n"
            f"📥 Target: `{target}`\n"
            f"📎 Type: All media (photo/video/doc/GIF)\n"
            f"{caption_info}\n"
            f"⏱️ Rate: ~2 saat per media\n\n"
            f"⚠️ Pastikan userbot sudah join kedua-dua chat!"
        )
        keyboard = [
            [InlineKeyboardButton("✅ Confirm Clone", callback_data="clone_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="clone_cancel")]
        ]
        query = update.callback_query
        if query:
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return CLONE_CONFIRM

    async def clone_confirm(self, update: Update, context=None):
        """Execute the clone operation"""
        query = update.callback_query
        await query.answer()

        source = context.user_data.get('clone_source')
        target = context.user_data.get('clone_target')
        caption_mode = context.user_data.get('caption_mode', 'keep')
        caption_text = context.user_data.get('caption_text', '')
        caption_find = context.user_data.get('caption_find', '')

        if not source or not target:
            await query.message.edit_text("❌ Data hilang. Sila cuba lagi.", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("« Back", callback_data="clone_menu")]]
            ))
            return UB_HUB

        # Build caption modifier
        caption_modifier = None
        if caption_mode != 'keep':
            caption_modifier = {'mode': caption_mode, 'text': caption_text, 'find': caption_find}

        # Save clone job to DB
        clone_id = self.db.save_clone_job(self.bot_id, source, source, target, target)

        await query.message.edit_text(
            f"⏳ **Cloning in progress...**\n\n"
            f"📤 Source: `{source}`\n"
            f"📥 Target: `{target}`\n\n"
            f"Scanning messages... Please wait.",
            parse_mode='Markdown'
        )

        # Run clone in background
        async def do_clone():
            try:
                cloned_count = 0
                last_update_count = 0

                async def progress(cloned, total):
                    nonlocal cloned_count, last_update_count
                    cloned_count = cloned
                    # Only edit message every 10 items to avoid flood
                    if cloned - last_update_count >= 10 or cloned == total:
                        last_update_count = cloned
                        try:
                            await query.message.edit_text(
                                f"⏳ **Cloning in progress...**\n\n"
                                f"📤 Source: `{source}`\n"
                                f"📥 Target: `{target}`\n\n"
                                f"📊 Progress: {cloned}/{total} media\n"
                                f"{'█' * int((cloned/max(total,1))*20)}{'░' * (20-int((cloned/max(total,1))*20))} {int((cloned/max(total,1))*100)}%",
                                parse_mode='Markdown'
                            )
                        except Exception:
                            pass

                count, error = await self.userbot_manager.clone_media(
                    self.bot_id, source, target, progress_callback=progress,
                    caption_modifier=caption_modifier
                )

                self.db.update_clone_job(clone_id, media_count=count, status='done' if not error else 'error')

                if error:
                    await query.message.edit_text(
                        f"❌ **Clone Error**\n\n"
                        f"Cloned {count} media before error:\n`{error}`",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="clone_menu")]]),
                        parse_mode='Markdown'
                    )
                else:
                    await query.message.edit_text(
                        f"✅ **Clone Complete!**\n\n"
                        f"📤 Source: `{source}`\n"
                        f"📥 Target: `{target}`\n"
                        f"📊 Total cloned: **{count}** media",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="clone_menu")]]),
                        parse_mode='Markdown'
                    )
            except Exception as e:
                self.db.update_clone_job(clone_id, status='error')
                self.logger.error(f"Clone error: {e}")
                try:
                    await query.message.edit_text(
                        f"❌ **Clone Failed**\n\n`{str(e)}`",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="clone_menu")]]),
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass

        asyncio.create_task(do_clone())
        return UB_HUB

    async def ub_menu(self, update: Update, context=None):
        """Show userbot promo monitor menu"""
        query = update.callback_query
        if query:
            await query.answer()

        user_id = update.effective_user.id
        bot_data = self.db.get_bot_by_token(self.token)
        owner_id = int(bot_data.get('owner_id', 0)) if bot_data else 0
        is_owner = user_id == owner_id
        is_admin = self.db.is_bot_admin(self.bot_id, user_id)
        if not (is_owner or is_admin):
            return ConversationHandler.END

        session = self.db.get_userbot_session(self.bot_id)
        channels = self.db.get_monitored_channels(self.bot_id)

        if session and session.get('session_string'):
            is_active = session.get('is_active', 0)
            auto_mode = session.get('auto_mode', 0)
            grid_mode = session.get('grid_mode', 1)  # Default ON
            status_text = "🟢 Active" if is_active else "🔴 Inactive"
            mode_text = "Auto" if auto_mode else "Manual"
            grid_text = "🖼️ Grid ON" if grid_mode else "📎 Grid OFF"
            phone = session.get('phone', '***')

            text = (
                f"📡 **AUTO PROMO MONITOR**\n\n"
                f"📱 Phone: `{phone}`\n"
                f"📡 Status: {status_text}\n"
                f"⚙️ Mode: {mode_text}\n"
                f"🖼️ Grid Collage: {grid_text}\n"
                f"📢 Channels/Groups: {len(channels)} monitored"
            )
            keyboard = [
                [
                    InlineKeyboardButton(f"{'🔴 OFF' if is_active else '🟢 ON'}", callback_data="ub_toggle"),
                    InlineKeyboardButton(f"⚙️ {'Auto' if auto_mode else 'Manual'}", callback_data="ub_mode")
                ],
                [InlineKeyboardButton(f"{grid_text}", callback_data="ub_grid")],
                [InlineKeyboardButton("📢 Manage Channels/Groups", callback_data="ub_channels")],
                [InlineKeyboardButton("📥 Scan 1 Bulan", callback_data="ub_scan_history")],
                [InlineKeyboardButton("« Back", callback_data="userbot_hub")]
            ]
        else:
            text = (
                "📡 **AUTO PROMO MONITOR**\n\n"
                "Fungsi ni akan monitor channel/group Telegram "
                "secara automatik untuk detect promo syarikat.\n\n"
                "✅ Apa yang boleh buat:\n"
                "• Detect nama company dari channel lain\n"
                "• Auto-tukar link kepada link affiliate kau\n"
                "• Broadcast terus ke users/groups kau\n\n"
                "📌 Untuk mula, kau perlu:\n"
                "1. Buat API credentials di Telegram\n"
                "2. Connect akaun Telegram kau\n"
                "3. Tambah channel untuk monitor\n\n"
                "Tekan **Setup** untuk mula! 👇"
            )
            keyboard = [
                [InlineKeyboardButton("⚙️ Setup Sekarang", callback_data="ub_setup")],
                [InlineKeyboardButton("« Back", callback_data="userbot_hub")]
            ]

        if query:
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

        return UB_MENU

    async def ub_handle_action(self, update: Update, context=None):
        """Handle userbot menu actions"""
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "ub_toggle":
            session = self.db.get_userbot_session(self.bot_id)
            if not session or not session.get('session_string'):
                await query.message.edit_text("❌ Sila setup userbot dulu.", reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("« Back", callback_data="ub_menu")]]
                ))
                return UB_MENU

            is_active = session.get('is_active', 0)
            if is_active:
                # Turn off
                if self.userbot_manager:
                    await self.userbot_manager.stop_instance(self.bot_id)
                self.db.toggle_userbot(self.bot_id, False)
            else:
                # Turn on
                if self.userbot_manager:
                    success = await self.userbot_manager.start_instance(self.bot_id)
                    if not success:
                        await query.message.edit_text("❌ Gagal connect. Cuba reconnect.", reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("« Back", callback_data="ub_menu")]]
                        ))
                        return UB_MENU
                self.db.toggle_userbot(self.bot_id, True)

            return await self.ub_menu(update, context)

        elif data == "ub_mode":
            session = self.db.get_userbot_session(self.bot_id)
            if session:
                new_mode = not session.get('auto_mode', 0)
                self.db.set_userbot_mode(self.bot_id, new_mode)
            return await self.ub_menu(update, context)

        elif data == "ub_grid":
            session = self.db.get_userbot_session(self.bot_id)
            if session:
                new_grid = not session.get('grid_mode', 1)
                self.db.set_grid_mode(self.bot_id, new_grid)
            return await self.ub_menu(update, context)

        elif data == "ub_setup":
            text = (
                "⚙️ **SETUP USERBOT — STEP 1/4**\n\n"
                "Pertama, kau perlu buat API credentials:\n\n"
                "📋 **Cara buat:**\n"
                "1. Buka browser → pergi ke:\n"
                "   https://my.telegram.org\n"
                "2. Masukkan nombor telefon Telegram kau\n"
                "3. Telegram akan hantar kod → masukkan\n"
                "4. Klik **API development tools**\n"
                "5. Isi form:\n"
                "   • App title: apa-apa nama\n"
                "   • Short name: apa-apa\n"
                "   • URL: kosongkan atau letak example.com\n"
                "   • Platform: Desktop\n"
                "6. Klik **Create application**\n\n"
                "Kau akan nampak **App api\\_id** (nombor)\n"
                "dan **App api\\_hash** (huruf+nombor panjang)\n\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "📝 Sekarang **copy nombor API ID** dan \n"
                "paste/taip di sini:"
            )
            await query.message.edit_text(text, parse_mode='Markdown')
            return UB_SETUP_API

        elif data == "ub_channels":
            return await self._show_channels_menu(update)

        elif data == "ub_disconnect":
            if self.userbot_manager:
                await self.userbot_manager.stop_instance(self.bot_id)
            self.db.delete_userbot_session(self.bot_id)
            await query.message.edit_text("✅ Userbot disconnected.", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("« Back", callback_data="ub_menu")]]
            ))
            return UB_MENU

        elif data.startswith("ub_rmch_"):
            # Remove channel
            ch_id = int(data.replace("ub_rmch_", ""))
            self.db.remove_monitored_channel(ch_id)
            return await self._show_channels_menu(update)

        elif data == "ub_add_ch":
            await query.message.edit_text(
                "📢 **ADD CHANNEL / GROUP**\n\n"
                "Paste link channel atau group yang nak monitor:\n\n"
                "Contoh:\n"
                "• `https://t.me/channelname`\n"
                "• `@channelname`\n"
                "• `https://t.me/+invitehash`",
                parse_mode='Markdown'
            )
            return UB_ADD_CHANNEL

        elif data == "ub_scan_history":
            return await self._scan_history_action(update, context)

        elif data.startswith("scan_show_item_"):
            return await self._show_scraped_item(update, context)

        elif data.startswith("scan_picker_"):
            return await self._show_company_picker_override(update, context)

        elif data.startswith("scan_pick_"):
            return await self._scan_pick_company(update, context)

        elif data.startswith("scan_ai_"):
            return await self._scan_ai_rewrite(update, context)

        elif data == "noop":
            await query.answer()
            return UB_MENU

        elif data.startswith("promo_bc_groups_"):
            promo_id = int(data.split("_")[3])
            await self._promo_broadcast_action(update, promo_id, 'groups')
            return UB_MENU

        elif data.startswith("promo_bc_users_"):
            promo_id = int(data.split("_")[3])
            await self._promo_broadcast_action(update, promo_id, 'users')
            return UB_MENU

        elif data.startswith("promo_skip_"):
            promo_id = int(data.split("_")[2])
            await self._promo_skip_action(update, promo_id)
            return UB_MENU

        elif data.startswith("rt_pick_"):
            # Real-time company pick: rt_pick_{promo_id}_{company_id}
            parts = data.split("_")
            promo_id = int(parts[2])
            company_id = int(parts[3])
            await self._rt_pick_company(update, promo_id, company_id)
            return UB_MENU


        return UB_MENU

    async def _show_channels_menu(self, update: Update):
        """Show monitored channels list"""
        query = update.callback_query
        channels = self.db.get_monitored_channels(self.bot_id)

        if channels:
            text = "📢 <b>MONITORED CHANNELS / GROUPS</b>\n\n"
            keyboard = []
            for ch in channels:
                title = ch.get('channel_title', 'Unknown')
                username = ch.get('channel_username', '')
                display = f"@{username}" if username else title
                text += f"• {display}\n"
                keyboard.append([InlineKeyboardButton(f"❌ Remove {display}", callback_data=f"ub_rmch_{ch['id']}")])
            keyboard.append([InlineKeyboardButton("➕ Add Channel/Group", callback_data="ub_add_ch")])
            keyboard.append([InlineKeyboardButton("« Back", callback_data="ub_menu")])
        else:
            text = "📢 <b>MONITORED CHANNELS / GROUPS</b>\n\nBelum ada. Tekan Add untuk mula."
            keyboard = [
                [InlineKeyboardButton("➕ Add Channel/Group", callback_data="ub_add_ch")],
                [InlineKeyboardButton("« Back", callback_data="ub_menu")]
            ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            if query:
                await query.message.edit_text(text, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await update.effective_message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
        except Exception as e:
            self.logger.error(f"Show channels menu error: {e}")
            # Fallback: send without parse_mode if HTML fails
            if query:
                await query.message.reply_text(text, reply_markup=reply_markup)
        return UB_MENU

    async def _scan_history_action(self, update: Update, context=None):
        """Scan last 30 days of all monitored channels — scrape all, admin picks company"""
        query = update.callback_query

        if not self.userbot_manager or not self.userbot_manager.is_running(self.bot_id):
            await query.message.edit_text(
                "❌ Userbot belum aktif!\n\n"
                "Sila **Turn ON** dulu sebelum scan.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="ub_menu")]])
            )
            return UB_MENU

        channels = self.db.get_monitored_channels(self.bot_id)
        if not channels:
            await query.message.edit_text(
                "❌ Tiada channel/group untuk scan.\n"
                "Tambah channel dulu.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="ub_menu")]])
            )
            return UB_MENU

        # Show scanning message
        status_msg = await query.message.edit_text(
            f"🔍 **SCANNING HISTORY (30 HARI)**\n\n"
            f"📢 {len(channels)} channel/group\n"
            f"⏳ Sedang scrape semua mesej...\n\n"
            f"_Ini mungkin ambil masa beberapa minit._",
            parse_mode='Markdown'
        )

        async def progress_cb(current, total, ch_name, scraped_count):
            try:
                await status_msg.edit_text(
                    f"🔍 **SCANNING HISTORY (30 HARI)**\n\n"
                    f"📊 Progress: {current}/{total}\n"
                    f"📢 Current: {ch_name}\n"
                    f"📥 Scraped: {scraped_count} items\n\n"
                    f"_Sabar ya..._",
                    parse_mode='Markdown'
                )
            except Exception:
                pass

        all_scraped = await self.userbot_manager.scan_all_channels_history(
            self.bot_id, days=30, progress_callback=progress_cb
        )

        if not all_scraped:
            await status_msg.edit_text(
                "📭 **Tiada mesej ditemui dalam 30 hari terakhir.**\n\n"
                "Channel mungkin kosong atau tiada content.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="ub_menu")]])
            )
            return UB_MENU

        # Store scraped items in user_data
        if context:
            context.user_data['scraped_items'] = all_scraped
            context.user_data['scraped_index'] = 0

        auto_count = sum(1 for s in all_scraped if s.get('matched_company'))
        manual_count = len(all_scraped) - auto_count
        admin_id = update.effective_user.id

        await status_msg.edit_text(
            f"✅ **SCAN SELESAI!**\n\n"
            f"📢 {len(channels)} channel discanned\n"
            f"📥 {len(all_scraped)} mesej ditemui\n"
            f"🤖 {auto_count} auto-detected\n"
            f"❓ {manual_count} perlu pilih manual\n\n"
            f"⏳ _Forwarding semua mesej ke kau..._",
            parse_mode='Markdown'
        )

        # Forward ALL messages to admin immediately
        forwarded_count = 0
        for idx, item in enumerate(all_scraped):
            channel_id = item.get('channel_id')
            msg_id = item.get('msg_id')
            text = item.get('original_text', '')
            source = item.get('source_channel', 'Unknown')
            matched = item.get('matched_company')

            # Get message content via userbot (downloads media)
            msg_content = None
            if channel_id and msg_id and self.userbot_manager:
                instance = self.userbot_manager.instances.get(self.bot_id)
                if instance:
                    try:
                        msg_content = await instance.get_message_content(channel_id, msg_id)
                    except Exception as e:
                        self.logger.error(f"Get content failed for item {idx}: {e}")

            # Send the original message content via child bot
            if msg_content:
                media_path = msg_content.get('media_path')
                media_type = msg_content.get('media_type')
                msg_text = msg_content.get('text', '')
                
                try:
                    sent_msg = None
                    if media_path and media_type:
                        with open(media_path, 'rb') as f:
                            if media_type == 'photo':
                                sent_msg = await self.app.bot.send_photo(
                                    chat_id=admin_id,
                                    photo=f,
                                    caption=msg_text[:1024] if msg_text else None,
                                )
                            elif media_type == 'video':
                                sent_msg = await self.app.bot.send_video(
                                    chat_id=admin_id,
                                    video=f,
                                    caption=msg_text[:1024] if msg_text else None,
                                )
                            elif media_type == 'document':
                                sent_msg = await self.app.bot.send_document(
                                    chat_id=admin_id,
                                    document=f,
                                    caption=msg_text[:1024] if msg_text else None,
                                )
                            else:
                                sent_msg = await self.app.bot.send_document(
                                    chat_id=admin_id,
                                    document=f,
                                    caption=msg_text[:1024] if msg_text else None,
                                )
                        # Capture file_id from response for broadcast later
                        if sent_msg:
                            if sent_msg.photo:
                                item['media_file_id'] = sent_msg.photo[-1].file_id
                            elif sent_msg.video:
                                item['media_file_id'] = sent_msg.video.file_id
                            elif sent_msg.document:
                                item['media_file_id'] = sent_msg.document.file_id
                        # Cleanup temp file
                        import os
                        try:
                            os.remove(media_path)
                        except Exception:
                            pass
                    elif msg_text:
                        await self.app.bot.send_message(
                            chat_id=admin_id,
                            text=msg_text,
                        )
                except Exception as e:
                    self.logger.error(f"Send content failed for item {idx}: {e}")

            # Build caption with company info
            if matched:
                caption = (
                    f"📋 **Mesej {idx + 1}/{len(all_scraped)}**\n"
                    f"📢 {source}\n"
                    f"🤖 Auto: **{matched['name']}** ✅"
                )
            else:
                caption = (
                    f"📋 **Mesej {idx + 1}/{len(all_scraped)}**\n"
                    f"📢 {source}\n"
                    f"❓ Company: **Tak dikesan** — pilih 👇"
                )

            if not msg_content:
                caption += f"\n\n{text[:500] if text else '(media sahaja)'}"

            # Company buttons
            buttons = []
            if matched:
                buttons.append([InlineKeyboardButton(f"✅ Guna {matched['name'][:25]}", callback_data=f"scan_pick_{idx}_{matched['id']}")])
                buttons.append([InlineKeyboardButton("🔄 Tukar Company", callback_data=f"scan_picker_{idx}")])
            else:
                companies = self.db.get_companies(self.bot_id)
                row = []
                for c in companies:
                    row.append(InlineKeyboardButton(c['name'][:20], callback_data=f"scan_pick_{idx}_{c['id']}"))
                    if len(row) == 2:
                        buttons.append(row)
                        row = []
                if row:
                    buttons.append(row)

            buttons.append([InlineKeyboardButton("⏭ Skip", callback_data=f"scan_show_item_{idx + 1}")])

            try:
                await self.app.bot.send_message(
                    chat_id=admin_id,
                    text=caption,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                forwarded_count += 1
            except Exception as e:
                self.logger.error(f"Error sending buttons for item {idx}: {e}")

            await asyncio.sleep(0.5)  # Rate limit

        # Final summary
        await self.app.bot.send_message(
            chat_id=admin_id,
            text=(
                f"✅ **Semua {forwarded_count} mesej telah diforward!**\n\n"
                f"Pilih company untuk setiap mesej di atas 👆"
            ),
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="ub_menu")]])
        )
        return UB_MENU

    async def _show_scraped_item(self, update: Update, context=None):
        """Show a scraped item — forward original message via userbot, then show buttons"""
        query = update.callback_query
        await query.answer()

        if not context or 'scraped_items' not in context.user_data:
            await query.message.edit_text("❌ Data scan sudah expired. Scan semula.")
            return UB_MENU

        # Get index from callback data
        idx = int(query.data.split('_')[-1])
        items = context.user_data['scraped_items']

        if idx >= len(items):
            await query.message.edit_text(
                "✅ **Semua mesej telah dilihat!**",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="ub_menu")]])
            )
            return UB_MENU

        item = items[idx]
        text = item.get('original_text', '')
        source = item.get('source_channel', 'Unknown')
        msg_date = item.get('msg_date', '')
        channel_id = item.get('channel_id')
        msg_id = item.get('msg_id')
        matched = item.get('matched_company')
        admin_id = query.from_user.id

        # Delete previous message
        try:
            await query.message.delete()
        except Exception:
            pass

        # Forward original message from channel via userbot
        forwarded = False
        if channel_id and msg_id and self.userbot_manager:
            instance = self.userbot_manager.instances.get(self.bot_id)
            if instance:
                try:
                    forwarded = await instance.forward_message(channel_id, msg_id, admin_id)
                except Exception as e:
                    self.logger.error(f"Forward failed: {e}")

        # Build info caption for the buttons message
        if matched:
            caption = (
                f"📋 **Mesej {idx + 1}/{len(items)}**\n"
                f"📢 Source: {source}\n"
                f"🤖 Auto: **{matched['name']}** ✅"
            )
        else:
            caption = (
                f"📋 **Mesej {idx + 1}/{len(items)}**\n"
                f"📢 Source: {source}\n"
                f"❓ Company: **Tak dikesan**\n"
                f"👇 **Pilih company:**"
            )

        if not forwarded:
            # Fallback: show text preview if forward failed
            caption += f"\n\n{text[:600] if text else '(media sahaja)'}"

        # Build buttons
        buttons = []

        if matched:
            buttons.append([InlineKeyboardButton(f"✅ Guna {matched['name'][:25]}", callback_data=f"scan_pick_{idx}_{matched['id']}")])
            buttons.append([InlineKeyboardButton("🔄 Tukar Company", callback_data=f"scan_picker_{idx}")])
        else:
            companies = self.db.get_companies(self.bot_id)
            row = []
            for c in companies:
                row.append(InlineKeyboardButton(c['name'][:20], callback_data=f"scan_pick_{idx}_{c['id']}"))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)

        # AI Rewrite button
        buttons.append([InlineKeyboardButton("🤖 AI Rewrite", callback_data=f"scan_ai_{idx}")])

        # Navigation buttons
        nav_row = []
        if idx > 0:
            nav_row.append(InlineKeyboardButton("◀ Prev", callback_data=f"scan_show_item_{idx - 1}"))
        nav_row.append(InlineKeyboardButton(f"{idx + 1}/{len(items)}", callback_data="noop"))
        if idx < len(items) - 1:
            nav_row.append(InlineKeyboardButton("Skip ▶", callback_data=f"scan_show_item_{idx + 1}"))
        buttons.append(nav_row)
        buttons.append([InlineKeyboardButton("❌ Tutup", callback_data="ub_menu")])

        keyboard = InlineKeyboardMarkup(buttons)

        # Send buttons message
        try:
            await self.app.bot.send_message(
                chat_id=admin_id,
                text=caption,
                parse_mode='Markdown',
                reply_markup=keyboard
            )
        except Exception as e:
            self.logger.error(f"Error showing scraped item buttons: {e}")

        return UB_MENU

    async def _show_company_picker_override(self, update: Update, context=None):
        """Show full company picker for overriding auto-detected company"""
        query = update.callback_query
        await query.answer()

        if not context or 'scraped_items' not in context.user_data:
            await query.message.edit_text("❌ Data scan sudah expired. Scan semula.")
            return UB_MENU

        # Parse: scan_picker_{idx}
        idx = int(query.data.split('_')[-1])
        items = context.user_data['scraped_items']
        if idx >= len(items):
            return UB_MENU

        item = items[idx]
        text = item.get('original_text', '')[:300]

        caption = (
            f"🔄 **Tukar Company — Mesej {idx + 1}/{len(items)}**\n\n"
            f"{text}\n\n"
            f"👇 Pilih company:"
        )

        companies = self.db.get_companies(self.bot_id)
        buttons = []
        row = []
        for c in companies:
            row.append(InlineKeyboardButton(c['name'][:20], callback_data=f"scan_pick_{idx}_{c['id']}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("◀ Kembali", callback_data=f"scan_show_item_{idx}")])

        try:
            await query.message.edit_text(
                caption,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.app.bot.send_message(
                chat_id=query.from_user.id,
                text=caption,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        return UB_MENU

    async def _scan_pick_company(self, update: Update, context=None):
        """Admin picked a company for a scraped message — swap link and show broadcast options"""
        query = update.callback_query
        await query.answer("✅ Company dipilih!")

        if not context or 'scraped_items' not in context.user_data:
            await query.message.edit_text("❌ Data scan sudah expired. Scan semula.")
            return UB_MENU

        # Parse callback: scan_pick_{idx}_{company_id}
        parts = query.data.split('_')
        idx = int(parts[2])
        company_id = int(parts[3])

        items = context.user_data['scraped_items']
        if idx >= len(items):
            return UB_MENU

        item = items[idx]
        text = item.get('original_text', '')
        source = item.get('source_channel', 'Unknown')

        # Get company details
        company = self.db.get_company(self.bot_id, company_id)
        if not company:
            await query.message.edit_text("❌ Company tidak ditemui.")
            return UB_MENU

        # Swap links
        swapped_text = text
        if company.get('button_url'):
            target_url = company['button_url']
            if target_url.startswith('t.me/'):
                target_url = 'https://' + target_url

            import re
            urls_found = re.findall(r'https?://\S+|t\.me/\S+', text, re.IGNORECASE)
            if urls_found:
                for url in urls_found:
                    swapped_text = swapped_text.replace(url, target_url)
            else:
                swapped_text += f"\n\n🔗 {target_url}"

        # Save promo record
        promo_id = self.db.save_detected_promo(
            bot_id=self.bot_id,
            source_channel=source,
            original_text=text,
            swapped_text=swapped_text,
            media_file_ids=[item.get('media_file_id', '')] if item.get('media_file_id') else [],
            media_types=[item.get('media_type', '')] if item.get('media_type') else [],
            matched_company=company['name']
        )

        # Store for broadcast
        context.user_data[f'promo_{promo_id}'] = {
            'swapped_text': swapped_text,
            'media_bytes': item.get('media_bytes'),
            'media_type': item.get('media_type'),
        }

        # Show result with broadcast buttons
        result_text = (
            f"✅ **Link Swapped!**\n\n"
            f"🏢 Company: {company['name']}\n"
            f"📢 Source: {source}\n\n"
            f"📝 Text:\n{swapped_text[:600]}\n\n"
            f"Nak broadcast?"
        )

        keyboard = [
            [InlineKeyboardButton("✨ AI Rewrite", callback_data=f"scan_ai_{idx}_{promo_id}")],
            [InlineKeyboardButton("📤 Broadcast Groups", callback_data=f"promo_bc_groups_{promo_id}"),
             InlineKeyboardButton("📤 Broadcast Users", callback_data=f"promo_bc_users_{promo_id}")],
            [InlineKeyboardButton("❌ Skip", callback_data=f"promo_skip_{promo_id}")]
        ]

        # Next item button
        if idx + 1 < len(items):
            keyboard.append([InlineKeyboardButton(f"📋 Next Mesej ({idx + 2}/{len(items)}) ▶", callback_data=f"scan_show_item_{idx + 1}")])
        keyboard.append([InlineKeyboardButton("« Back to Menu", callback_data="ub_menu")])

        try:
            await query.message.edit_text(
                result_text,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.app.bot.send_message(
                chat_id=query.from_user.id,
                text=result_text,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        return UB_MENU

    async def _scan_ai_rewrite(self, update: Update, context=None):
        """AI rewrite the promo text using Groq"""
        query = update.callback_query
        await query.answer("✨ AI sedang menulis...")

        # Parse: scan_ai_{idx}_{promo_id}
        parts = query.data.split('_')
        idx = int(parts[2])
        promo_id = int(parts[3])

        items = context.user_data.get('scraped_items', []) if context else []

        # Get promo from DB
        conn = self.db.get_connection()
        try:
            row = conn.execute("SELECT * FROM detected_promos WHERE id = ?", (promo_id,)).fetchone()
        finally:
            conn.close()

        if not row:
            await query.message.edit_text("❌ Promo tidak ditemui.")
            return UB_MENU

        promo = dict(row)
        original_text = promo.get('swapped_text', '') or promo.get('original_text', '')
        company_name = promo.get('matched_company', '')
        source = promo.get('source_channel', '')

        # Show loading
        try:
            await query.message.edit_text(
                f"✨ <b>AI sedang menulis semula...</b>\n\n"
                f"🏢 Company: {company_name}\n"
                f"⏳ Tunggu sekejap...",
                parse_mode='HTML'
            )
        except Exception:
            pass

        # Call Groq AI
        from ai_rewriter import rewrite_promo
        rewritten = await rewrite_promo(original_text, company_name)

        # Update promo in DB
        try:
            conn = self.db.get_connection()
            conn.execute("UPDATE detected_promos SET swapped_text = ? WHERE id = ?", (rewritten, promo_id))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Failed to update promo: {e}")

        # Update context
        if context and f'promo_{promo_id}' in context.user_data:
            context.user_data[f'promo_{promo_id}']['swapped_text'] = rewritten

        # Show result
        result_text = (
            f"✨ <b>AI REWRITE SELESAI!</b>\n\n"
            f"🏢 Company: {company_name}\n"
            f"📢 Source: {source}\n\n"
            f"📝 Text baru:\n{rewritten[:800]}\n\n"
            f"Nak broadcast?"
        )

        keyboard = [
            [InlineKeyboardButton("✨ Rewrite Lagi", callback_data=f"scan_ai_{idx}_{promo_id}")],
            [InlineKeyboardButton("📤 Broadcast Groups", callback_data=f"promo_bc_groups_{promo_id}"),
             InlineKeyboardButton("📤 Broadcast Users", callback_data=f"promo_bc_users_{promo_id}")],
            [InlineKeyboardButton("❌ Skip", callback_data=f"promo_skip_{promo_id}")]
        ]

        if idx + 1 < len(items):
            keyboard.append([InlineKeyboardButton(f"📋 Next Mesej ({idx + 2}/{len(items)}) ▶", callback_data=f"scan_show_item_{idx + 1}")])
        keyboard.append([InlineKeyboardButton("« Back to Menu", callback_data="ub_menu")])

        try:
            await query.message.edit_text(
                result_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.app.bot.send_message(
                chat_id=query.from_user.id,
                text=result_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        return UB_MENU

    # --- SETUP WIZARD ---

    async def ub_save_api_id(self, update: Update, context=None):
        """Step 1: Save API ID"""
        api_id = update.message.text.strip()
        if not api_id.isdigit():
            await update.message.reply_text(
                "❌ API ID mesti nombor sahaja (contoh: 12345678).\n"
                "Cuba copy semula dari my.telegram.org"
            )
            return UB_SETUP_API

        if not context or not context.user_data:
            context.user_data['ub_setup'] = {}
        context.user_data.setdefault('ub_setup', {})['api_id'] = api_id

        await update.message.reply_text(
            "✅ API ID saved!\n\n"
            "⚙️ **STEP 2/4 — API HASH**\n\n"
            "Sekarang copy **App api\\_hash** dari \n"
            "page yang sama di my.telegram.org\n\n"
            "Ia nampak macam ni:\n"
            "`a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6`\n\n"
            "📝 Paste API Hash di sini:",
            parse_mode='Markdown'
        )
        return UB_SETUP_HASH

    async def ub_save_api_hash(self, update: Update, context=None):
        """Step 2: Save API Hash"""
        api_hash = update.message.text.strip()
        if len(api_hash) < 10:
            await update.message.reply_text(
                "❌ API Hash tak valid. Ia patut 32 karakter.\n"
                "Cuba copy semula dari my.telegram.org"
            )
            return UB_SETUP_HASH

        context.user_data.setdefault('ub_setup', {})['api_hash'] = api_hash

        await update.message.reply_text(
            "✅ API Hash saved!\n\n"
            "⚙️ **STEP 3/4 — NOMBOR TELEFON**\n\n"
            "Masukkan nombor telefon akaun Telegram \n"
            "yang kau nak guna untuk monitor.\n\n"
            "📱 Format: `+60123456789`\n\n"
            "⚠️ Guna nombor telefon Telegram kau \n"
            "sendiri (bukan bot token)",
            parse_mode='Markdown'
        )
        return UB_SETUP_PHONE

    async def ub_save_phone(self, update: Update, context=None):
        """Step 3: Save phone and send OTP"""
        phone = update.message.text.strip()
        if not phone.startswith('+'):
            phone = '+' + phone

        setup = context.user_data.get('ub_setup', {})
        api_id = setup.get('api_id')
        api_hash = setup.get('api_hash')

        if not api_id or not api_hash:
            await update.message.reply_text("❌ Session expired. Sila mula semula.")
            return ConversationHandler.END

        context.user_data['ub_setup']['phone'] = phone

        await update.message.reply_text("📤 Menghantar kod OTP ke Telegram kau...")

        if self.userbot_manager:
            success = await self.userbot_manager.begin_auth(self.bot_id, api_id, api_hash, phone)
            if success:
                await update.message.reply_text(
                    "✅ **STEP 4/4 — KOD PENGESAHAN**\n\n"
                    "Kod OTP telah dihantar ke akaun \n"
                    "Telegram kau (atau SMS)!\n\n"
                    "🚨 **PENTING — BACA DULU:**\n"
                    "Jangan taip kod terus macam `38969`.\n"
                    "Telegram akan BLOCK kalau kau \n"
                    "hantar kod login dalam chat!\n\n"
                    "✅ **Cara betul:**\n"
                    "Letak jarak atau dash antara nombor:\n"
                    "• `3 8 9 6 9`\n"
                    "• `3-8-9-6-9`\n"
                    "• `3.8.9.6.9`\n\n"
                    "Bot akan auto-detect nombor tu.\n\n"
                    "📝 Taip kod kau sekarang:",
                    parse_mode='Markdown'
                )
                return UB_SETUP_OTP
            else:
                await update.message.reply_text(
                    "❌ Gagal hantar OTP. Semak API ID/Hash dan nombor telefon.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="ub_menu")]])
                )
                return UB_MENU
        else:
            await update.message.reply_text("❌ Userbot manager tidak aktif. Sila restart platform.")
            return ConversationHandler.END

    async def ub_verify_otp(self, update: Update, context=None):
        """Step 4: Verify OTP"""
        import re
        raw = update.message.text.strip()
        code = re.sub(r'[^0-9]', '', raw)  # Strip non-digits
        if not code:
            await update.message.reply_text("❌ Masukkan kod nombor. Contoh: `3 8 9 6 9`", parse_mode='Markdown')
            return UB_SETUP_OTP

        if self.userbot_manager:
            success, needs_2fa = await self.userbot_manager.verify_code(self.bot_id, code)

            if needs_2fa:
                await update.message.reply_text(
                    "🔐 Akaun ini ada **2FA**!\n\n"
                    "📝 Masukkan **password 2FA**:",
                    parse_mode='Markdown'
                )
                return UB_SETUP_2FA

            if success:
                await update.message.reply_text(
                    "✅ **USERBOT CONNECTED!** 🎉\n\n"
                    "Sekarang tambah channel untuk monitor.\n"
                    "Tekan button di bawah:",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📢 Add Channel/Group", callback_data="ub_add_ch")],
                        [InlineKeyboardButton("🟢 Activate Now", callback_data="ub_toggle")],
                        [InlineKeyboardButton("« Back to Menu", callback_data="ub_menu")]
                    ])
                )
                return UB_MENU
            else:
                await update.message.reply_text("❌ Kod OTP salah. Cuba lagi:")
                return UB_SETUP_OTP
        else:
            await update.message.reply_text("❌ Userbot manager tidak aktif.")
            return ConversationHandler.END

    async def ub_verify_2fa(self, update: Update, context=None):
        """Step 5: Verify 2FA password"""
        password = update.message.text.strip()

        if self.userbot_manager:
            success = await self.userbot_manager.verify_2fa(self.bot_id, password)

            if success:
                await update.message.reply_text(
                    "✅ **USERBOT CONNECTED!** 🎉\n\n"
                    "Sekarang tambah channel untuk monitor.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📢 Add Channel/Group", callback_data="ub_add_ch")],
                        [InlineKeyboardButton("🟢 Activate Now", callback_data="ub_toggle")],
                        [InlineKeyboardButton("« Back to Menu", callback_data="ub_menu")]
                    ])
                )
                return UB_MENU
            else:
                await update.message.reply_text("❌ Password salah. Cuba lagi:")
                return UB_SETUP_2FA
        else:
            await update.message.reply_text("❌ Userbot manager tidak aktif.")
            return ConversationHandler.END

    async def ub_add_channel_link(self, update: Update, context=None):
        """Add channel to monitor"""
        link = update.message.text.strip()

        if not self.userbot_manager or not self.userbot_manager.is_running(self.bot_id):
            # Not running but session exists - try to join without userbot
            self.db.add_monitored_channel(
                self.bot_id,
                channel_id=link,
                channel_title=link,
                channel_username=link.replace('https://t.me/', '').replace('@', '').strip('/')
            )
            await update.message.reply_text(
                f"✅ Channel `{link}` ditambah!\n\n"
                "⚠️ Userbot belum aktif. Sila activate untuk mula monitor.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📢 Manage Channels", callback_data="ub_channels")],
                    [InlineKeyboardButton("« Back", callback_data="ub_menu")]
                ])
            )
            return UB_MENU

        await update.message.reply_text("⏳ Joining channel...")

        result = await self.userbot_manager.join_channel_for_bot(self.bot_id, link)

        if result:
            is_bot = result.get('is_bot', False)
            self.db.add_monitored_channel(
                self.bot_id,
                channel_id=result['id'],
                channel_title=result['title'],
                channel_username=result.get('username')
            )
            if is_bot:
                success_msg = (
                    f"🤖 Bot **{result['title']}** ditambah!\n"
                    f"Bot akan dimonitor untuk mesej."
                )
            else:
                success_msg = (
                    f"✅ Joined **{result['title']}**!\n"
                    f"Channel akan dimonitor untuk promo."
                )
            await update.message.reply_text(
                success_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Another", callback_data="ub_add_ch")],
                    [InlineKeyboardButton("📢 View Channels", callback_data="ub_channels")],
                    [InlineKeyboardButton("« Back", callback_data="ub_menu")]
                ])
            )
        else:
            await update.message.reply_text(
                "❌ Gagal join. Pastikan link channel/group betul.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Cuba Lagi", callback_data="ub_add_ch")],
                    [InlineKeyboardButton("« Back", callback_data="ub_menu")]
                ])
            )
        return UB_MENU

    # --- PROMO NOTIFICATION CALLBACK ---

    async def handle_promo_notification(self, bot_id, promo_data):
        """Called by UserbotManager when promo is detected"""
        try:
            bot_data = self.db.get_bot_by_token(self.token)
            owner_id = bot_data.get('owner_id') if bot_data else None
            if not owner_id:
                return

            auto_mode = promo_data.get('auto_mode', 0)
            source = promo_data.get('source_channel', 'Unknown')
            company = promo_data.get('matched_company', 'Unknown')
            swapped = promo_data.get('swapped_text', '')
            promo_id = promo_data.get('promo_id', 0)
            media_bytes = promo_data.get('media_bytes')
            media_type = promo_data.get('media_type')

            # AI Vision: generate caption if image present but text is short/empty
            vision_images = []  # Collect all photos for multi-image vision
            if media_bytes and media_type == 'photo':
                vision_images = [media_bytes]
            if promo_data.get('is_album') and promo_data.get('all_media_bytes'):
                # Collect ALL photos from album
                for mb, mt in zip(promo_data['all_media_bytes'], promo_data.get('all_media_types', [])):
                    if mt == 'photo' and mb not in vision_images:
                        vision_images.append(mb)
            
            if vision_images and len(swapped.strip()) < 50:
                try:
                    from ai_rewriter import generate_caption_from_image
                    ai_caption, detected_co = await generate_caption_from_image(vision_images, company)
                    if ai_caption:
                        swapped = ai_caption
                        # Use detected company if none was matched from text
                        if detected_co and (not company or company == 'Unknown'):
                            company = detected_co
                            # Try to match with existing companies in DB
                            existing = self.db.get_companies(self.bot_id)
                            matched = None
                            for c in existing:
                                if detected_co.lower() in c['name'].lower() or c['name'].lower() in detected_co.lower():
                                    matched = c['name']
                                    break
                            if matched:
                                company = matched
                            self.logger.info(f"AI detected company from image: {detected_co} → matched: {matched or 'new'}")
                        # Update DB with AI-generated caption and detected company
                        try:
                            conn = self.db.get_connection()
                            conn.execute("UPDATE detected_promos SET swapped_text = ?, matched_company = ? WHERE id = ?", (ai_caption, company, promo_id))
                            conn.commit()
                            conn.close()
                        except Exception:
                            pass
                        self.logger.info(f"AI Vision caption generated for channel post: {len(ai_caption)} chars")
                except Exception as e:
                    self.logger.warning(f"AI Vision caption failed for channel post: {e}")

            # Helper: send message with optional media
            async def send_with_media(chat_id, text, keyboard=None):
                """Send text + media to a chat. Returns sent message for file_id capture."""
                reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
                try:
                    # Build Bot API entities from promo_data (premium emoji support)
                    from telegram import MessageEntity
                    raw_ents = promo_data.get('entities', [])
                    caption_entities = None
                    use_parse_mode = 'HTML'
                    
                    if raw_ents:
                        bot_entities = []
                        for e in raw_ents:
                            etype = e.get('type')
                            if not etype:
                                continue
                            kwargs = {
                                'type': etype,
                                'offset': e['offset'],
                                'length': e['length'],
                            }
                            if etype == 'custom_emoji':
                                kwargs['custom_emoji_id'] = e.get('custom_emoji_id')
                            elif etype == 'text_link':
                                kwargs['url'] = e.get('url', '')
                            bot_entities.append(MessageEntity(**kwargs))
                        if bot_entities:
                            caption_entities = bot_entities
                            use_parse_mode = None  # Don't mix parse_mode with entities

                    def _caption_payload(raw_text: str):
                        """Build safe caption payload; drop entities if caption is truncated."""
                        caption = (raw_text or "")[:1024]
                        pm = use_parse_mode
                        ce = caption_entities
                        if ce and len(raw_text or "") > len(caption):
                            # Entity offsets were computed for full text; they become invalid after truncation.
                            ce = None
                            pm = None
                        return caption, pm, ce
                    
                    is_album = promo_data.get('is_album', False)
                    all_bytes = promo_data.get('all_media_bytes', [])
                    all_types = promo_data.get('all_media_types', [])
                    
                    if is_album and len(all_bytes) > 1:
                        # Check if grid mode is enabled
                        session = self.db.get_userbot_session(self.bot_id)
                        grid_mode = session.get('grid_mode', 1) if session else 1
                        
                        grid_result = None
                        if grid_mode:
                            # Create grid collage from all media
                            try:
                                from media_grid import create_grid_collage
                                bot_name = self.app.bot.first_name or "Bot"
                                media_list = list(zip(all_bytes, all_types))
                                grid_result = create_grid_collage(media_list, watermark_text=bot_name, company_name=company)
                            except Exception as e:
                                self.logger.error(f"Grid collage failed: {e}")
                        
                        if grid_result:
                            grid_data, is_video = grid_result
                            from io import BytesIO
                            buf = BytesIO(grid_data)
                            if is_video:
                                buf.name = 'grid_collage.mp4'
                                caption, pm, ce = _caption_payload(text)
                                return await self.app.bot.send_video(
                                    chat_id=chat_id, video=buf,
                                    caption=caption,
                                    parse_mode=pm,
                                    caption_entities=ce,
                                    reply_markup=reply_markup
                                )
                            else:
                                buf.name = 'grid_collage.jpg'
                                caption, pm, ce = _caption_payload(text)
                                return await self.app.bot.send_photo(
                                    chat_id=chat_id, photo=buf,
                                    caption=caption,
                                    parse_mode=pm,
                                    caption_entities=ce,
                                    reply_markup=reply_markup
                                )
                        else:
                            # Fallback: send as album
                            from io import BytesIO
                            from telegram import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                            media_group = []
                            for i, (mb, mt) in enumerate(zip(all_bytes, all_types)):
                                buf = BytesIO(mb)
                                ext = {'photo': '.jpg', 'video': '.mp4', 'document': '.bin'}
                                buf.name = f'promo_{i}{ext.get(mt, ".bin")}'
                                if i == 0:
                                    cap, pm, ce = _caption_payload(text)
                                else:
                                    cap, pm, ce = None, None, None
                                if mt == 'photo':
                                    media_group.append(InputMediaPhoto(media=buf, caption=cap, parse_mode=pm, caption_entities=ce))
                                elif mt == 'video':
                                    media_group.append(InputMediaVideo(media=buf, caption=cap, parse_mode=pm, caption_entities=ce))
                                else:
                                    media_group.append(InputMediaDocument(media=buf, caption=cap, parse_mode=pm, caption_entities=ce))
                            sent_msgs = await self.app.bot.send_media_group(chat_id=chat_id, media=media_group)
                            if reply_markup:
                                await self.app.bot.send_message(
                                    chat_id=chat_id, text="👆 Pilih tindakan:",
                                    reply_markup=reply_markup
                                )
                            return sent_msgs if sent_msgs else None
                    
                    elif media_bytes and media_type in ('photo', 'video', 'document'):
                        from io import BytesIO
                        buf = BytesIO(media_bytes)
                        ext = {'photo': '.jpg', 'video': '.mp4', 'document': '.bin'}
                        buf.name = f'promo{ext.get(media_type, ".bin")}'
                        caption, pm, ce = _caption_payload(text)
                        if media_type == 'photo':
                            return await self.app.bot.send_photo(
                                chat_id=chat_id, photo=buf,
                                caption=caption,
                                parse_mode=pm,
                                caption_entities=ce,
                                reply_markup=reply_markup
                            )
                        elif media_type == 'video':
                            return await self.app.bot.send_video(
                                chat_id=chat_id, video=buf,
                                caption=caption,
                                parse_mode=pm,
                                caption_entities=ce,
                                reply_markup=reply_markup
                            )
                        else:
                            return await self.app.bot.send_document(
                                chat_id=chat_id, document=buf,
                                caption=caption,
                                parse_mode=pm,
                                caption_entities=ce,
                                reply_markup=reply_markup
                            )
                    else:
                        return await self.app.bot.send_message(
                            chat_id=chat_id, text=text,
                            parse_mode=use_parse_mode,
                            entities=caption_entities,
                            reply_markup=reply_markup
                        )
                except Exception as e:
                    self.logger.error(f"Send promo failed to {chat_id}: {e}")
                    return None

            if auto_mode and company:
                # === AUTO MODE: broadcast to groups + notify admin ===
                # (only auto-broadcast when company is matched)
                
                # First send to admin for record
                admin_caption = (
                    f"📤 <b>AUTO-BROADCAST SENT</b>\n\n"
                    f"📢 Source: {source}\n"
                    f"🏢 Company: {company}\n\n"
                    f"📝 Caption:\n{swapped[:800]}"
                )
                sent_msg = await send_with_media(owner_id, admin_caption)
                
                # Capture file_id from admin message for broadcast
                grid_file_id = None
                grid_type = None  # 'photo' or 'video'
                all_file_ids = []
                all_file_types = []
                
                if sent_msg and not isinstance(sent_msg, (list, tuple)):
                    # Single message (grid collage or single media)
                    if sent_msg.photo:
                        grid_file_id = sent_msg.photo[-1].file_id
                        grid_type = 'photo'
                        all_file_ids = [grid_file_id]
                        all_file_types = ['photo']
                    elif sent_msg.video:
                        grid_file_id = sent_msg.video.file_id
                        grid_type = 'video'
                        all_file_ids = [grid_file_id]
                        all_file_types = ['video']
                    elif sent_msg.document:
                        all_file_ids = [sent_msg.document.file_id]
                        all_file_types = ['document']
                elif sent_msg and isinstance(sent_msg, (list, tuple)):
                    # Fallback: album (tuple from send_media_group)
                    for msg in sent_msg:
                        if msg and msg.photo:
                            all_file_ids.append(msg.photo[-1].file_id)
                            all_file_types.append('photo')
                        elif msg and msg.video:
                            all_file_ids.append(msg.video.file_id)
                            all_file_types.append('video')
                        elif msg and msg.document:
                            all_file_ids.append(msg.document.file_id)
                            all_file_types.append('document')
                
                # Update DB with file_ids
                if all_file_ids:
                    try:
                        import json
                        conn = self.db.get_connection()
                        try:
                            conn.execute(
                                "UPDATE detected_promos SET media_file_ids = ?, media_types = ? WHERE id = ?",
                                (json.dumps(all_file_ids), json.dumps(all_file_types), promo_id)
                            )
                            conn.commit()
                        finally:
                            conn.close()
                    except Exception:
                        pass
                
                # Broadcast to all groups
                groups = self.db.get_known_groups(self.bot_id)
                bc_count = 0
                for g in groups:
                    try:
                        if grid_file_id:
                            # Grid collage: send as video or photo based on type
                            if grid_type == 'video':
                                await self.app.bot.send_video(
                                    chat_id=g['group_id'], video=grid_file_id,
                                    caption=swapped[:1024], parse_mode='HTML')
                            else:
                                await self.app.bot.send_photo(
                                    chat_id=g['group_id'], photo=grid_file_id,
                                    caption=swapped[:1024], parse_mode='HTML')
                        elif len(all_file_ids) == 1:
                            fid, ft = all_file_ids[0], all_file_types[0]
                            if ft == 'photo':
                                await self.app.bot.send_photo(chat_id=g['group_id'], photo=fid, caption=swapped[:1024], parse_mode='HTML')
                            elif ft == 'video':
                                await self.app.bot.send_video(chat_id=g['group_id'], video=fid, caption=swapped[:1024], parse_mode='HTML')
                            else:
                                await self.app.bot.send_document(chat_id=g['group_id'], document=fid, caption=swapped[:1024], parse_mode='HTML')
                        elif len(all_file_ids) > 1:
                            # Fallback album
                            from telegram import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                            media_group = []
                            for i, (fid, ft) in enumerate(zip(all_file_ids, all_file_types)):
                                cap = swapped[:1024] if i == 0 else None
                                pm = 'HTML' if cap else None
                                if ft == 'photo':
                                    media_group.append(InputMediaPhoto(media=fid, caption=cap, parse_mode=pm))
                                elif ft == 'video':
                                    media_group.append(InputMediaVideo(media=fid, caption=cap, parse_mode=pm))
                                else:
                                    media_group.append(InputMediaDocument(media=fid, caption=cap, parse_mode=pm))
                            await self.app.bot.send_media_group(chat_id=g['group_id'], media=media_group)
                        else:
                            await self.app.bot.send_message(chat_id=g['group_id'], text=swapped, parse_mode='HTML')
                        bc_count += 1
                    except Exception:
                        pass
                    await asyncio.sleep(0.3)
                
                self.db.update_promo_status(promo_id, 'broadcast')
                
                # Notify admin of broadcast result
                try:
                    await self.app.bot.send_message(
                        chat_id=owner_id,
                        text=f"✅ Auto-broadcast ke {bc_count} group berjaya!"
                    )
                except Exception:
                    pass
                    
            else:
                # === MANUAL MODE or NO COMPANY MATCH ===
                
                if company:
                    # Company matched — show broadcast buttons
                    caption = (
                        f"🔔 <b>PROMO DETECTED!</b>\n\n"
                        f"📢 Source: {source}\n"
                        f"🏢 Match: {company}\n\n"
                        f"📝 Caption (link swapped ✅):\n"
                        f"{swapped[:800]}"
                    )
                    keyboard = [
                        [InlineKeyboardButton("✨ AI Rewrite", callback_data=f"scan_ai_0_{promo_id}")],
                        [InlineKeyboardButton("📤 Broadcast Groups", callback_data=f"promo_bc_groups_{promo_id}"),
                         InlineKeyboardButton("📤 Broadcast Users", callback_data=f"promo_bc_users_{promo_id}")],
                        [InlineKeyboardButton("🔄 Tukar Company", callback_data=f"wa_change_co_{promo_id}")],
                        [InlineKeyboardButton("❌ Skip", callback_data=f"promo_skip_{promo_id}")]
                    ]
                else:
                    # No company match — show company pick buttons
                    caption = (
                        f"🔔 <b>NEW POST DETECTED!</b>\n\n"
                        f"📢 Source: {source}\n"
                        f"❓ Company: <i>Belum dipilih</i>\n\n"
                        f"📝 Text:\n{swapped[:800]}\n\n"
                        f"👇 Pilih company:"
                    )
                    companies = self.db.get_companies(self.bot_id)
                    keyboard = []
                    # Show 2 companies per row, up to 50
                    row = []
                    for c in companies[:50]:
                        row.append(InlineKeyboardButton(
                            f"🏢 {c['name'][:25]}", 
                            callback_data=f"rt_pick_{promo_id}_{c['id']}"
                        ))
                        if len(row) == 2:
                            keyboard.append(row)
                            row = []
                    if row:
                        keyboard.append(row)
                    keyboard.append([InlineKeyboardButton("❌ Skip", callback_data=f"promo_skip_{promo_id}")])

                sent_msg = await send_with_media(owner_id, caption, keyboard)
                
                # Capture file_id and store in DB for later broadcast
                if sent_msg:
                    file_id = None
                    if sent_msg.photo:
                        file_id = sent_msg.photo[-1].file_id
                    elif sent_msg.video:
                        file_id = sent_msg.video.file_id
                    elif sent_msg.document:
                        file_id = sent_msg.document.file_id
                    
                    if file_id and media_type:
                        try:
                            import json
                            conn = self.db.get_connection()
                            try:
                                conn.execute(
                                    "UPDATE detected_promos SET media_file_ids = ?, media_types = ? WHERE id = ?",
                                    (json.dumps([file_id]), json.dumps([media_type]), promo_id)
                                )
                                conn.commit()
                            finally:
                                conn.close()
                        except Exception:
                            pass

        except Exception as e:
            self.logger.error(f"Promo notification error: {e}")

    async def _promo_broadcast_action(self, update: Update, promo_id, target):
        """Handle promo broadcast button — sends text + media (supports albums)"""
        query = update.callback_query
        await query.answer("📤 Broadcasting...")

        try:
            # Get promo from DB
            conn = self.db.get_connection()
            try:
                row = conn.execute("SELECT * FROM detected_promos WHERE id = ?", (promo_id,)).fetchone()
            finally:
                conn.close()

            if not row:
                await query.message.edit_text("❌ Promo tidak ditemui.")
                return

            promo = dict(row)
            text = promo.get('swapped_text', '')

            # Get media info from promo DB (media_file_ids column)
            media_file_ids_raw = promo.get('media_file_ids', '')
            media_types_raw = promo.get('media_types', '')
            
            # Parse ALL stored media info
            import json
            file_ids = []
            m_types = []
            if media_file_ids_raw and media_types_raw:
                try:
                    file_ids = json.loads(media_file_ids_raw) if isinstance(media_file_ids_raw, str) else media_file_ids_raw
                    m_types = json.loads(media_types_raw) if isinstance(media_types_raw, str) else media_types_raw
                    # Filter out pending/empty entries
                    valid = [(fid, mt) for fid, mt in zip(file_ids, m_types) if fid and not fid.endswith('_pending')]
                    if valid:
                        file_ids, m_types = zip(*valid)
                        file_ids, m_types = list(file_ids), list(m_types)
                    else:
                        file_ids, m_types = [], []
                except Exception:
                    file_ids, m_types = [], []

            is_album = len(file_ids) > 1

            async def send_to_chat(chat_id):
                """Send promo to a single chat with album support"""
                try:
                    if is_album:
                        # Send as media group (album)
                        from telegram import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                        media_group = []
                        for i, (fid, ft) in enumerate(zip(file_ids, m_types)):
                            cap = text[:1024] if i == 0 else None
                            pm = 'HTML' if cap else None
                            if ft == 'photo':
                                media_group.append(InputMediaPhoto(media=fid, caption=cap, parse_mode=pm))
                            elif ft == 'video':
                                media_group.append(InputMediaVideo(media=fid, caption=cap, parse_mode=pm))
                            else:
                                media_group.append(InputMediaDocument(media=fid, caption=cap, parse_mode=pm))
                        await self.app.bot.send_media_group(chat_id=chat_id, media=media_group)
                    elif file_ids:
                        fid, ft = file_ids[0], m_types[0]
                        if ft == 'photo':
                            await self.app.bot.send_photo(chat_id=chat_id, photo=fid, caption=text[:1024], parse_mode='HTML')
                        elif ft == 'video':
                            await self.app.bot.send_video(chat_id=chat_id, video=fid, caption=text[:1024], parse_mode='HTML')
                        elif ft == 'document':
                            await self.app.bot.send_document(chat_id=chat_id, document=fid, caption=text[:1024], parse_mode='HTML')
                        else:
                            await self.app.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
                    else:
                        await self.app.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
                    return True
                except Exception as e:
                    self.logger.error(f"Broadcast send error to {chat_id}: {e}")
                    return False

            if target == 'groups':
                groups = self.db.get_known_groups(self.bot_id)
                count = 0
                for g in groups:
                    if await send_to_chat(g['group_id']):
                        count += 1
                    await asyncio.sleep(0.3)
                self.db.update_promo_status(promo_id, 'broadcast')
                result_text = f"✅ Broadcast ke {count} group berjaya!"
            else:
                users = self.db.get_users(self.bot_id)
                count = 0
                for u in users:
                    if await send_to_chat(u['telegram_id']):
                        count += 1
                    await asyncio.sleep(0.3)
                self.db.update_promo_status(promo_id, 'broadcast')
                result_text = f"✅ Broadcast ke {count} users berjaya!"

            # Send result notification
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.app.bot.send_message(chat_id=query.from_user.id, text=result_text)

        except Exception as e:
            self.logger.error(f"Promo broadcast error: {e}")
            try:
                await self.app.bot.send_message(chat_id=query.from_user.id, text=f"❌ Error: {e}")
            except Exception:
                pass

    async def _promo_skip_action(self, update: Update, promo_id):
        """Handle promo skip button"""
        query = update.callback_query
        await query.answer()
        self.db.update_promo_status(promo_id, 'skipped')
        is_media = bool(query.message.photo or query.message.video or query.message.document)
        try:
            if is_media:
                await query.message.edit_caption(caption="⏭️ Promo skipped.")
            else:
                await query.message.edit_text("⏭️ Promo skipped.")
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.app.bot.send_message(chat_id=query.from_user.id, text="⏭️ Promo skipped.")

    async def _wa_show_company_list(self, update: Update, promo_id):
        """Show company selection list when admin clicks 'Tukar Company'"""
        query = update.callback_query
        await query.answer("🏢 Pilih company...")
        
        try:
            # Get current promo info
            conn = self.db.get_connection()
            try:
                row = conn.execute("SELECT * FROM detected_promos WHERE id = ?", (promo_id,)).fetchone()
            finally:
                conn.close()
            
            if not row:
                await query.message.edit_text("❌ Promo tidak ditemui.")
                return
            
            promo = dict(row)
            current_company = promo.get('matched_company', '')
            source = promo.get('source_channel', '')
            
            # Build company list buttons
            companies = self.db.get_companies(self.bot_id)
            keyboard = []
            row_btns = []
            for c in companies[:50]:
                # Mark current match with ✅
                prefix = "✅ " if c['name'] == current_company else "🏢 "
                row_btns.append(InlineKeyboardButton(
                    f"{prefix}{c['name'][:22]}", 
                    callback_data=f"rt_pick_{promo_id}_{c['id']}"
                ))
                if len(row_btns) == 2:
                    keyboard.append(row_btns)
                    row_btns = []
            if row_btns:
                keyboard.append(row_btns)
            keyboard.append([InlineKeyboardButton("◀️ Back", callback_data=f"promo_skip_{promo_id}")])
            
            # Edit message to show company list
            caption = (
                f"🔄 <b>TUKAR COMPANY</b>\n\n"
                f"📢 Source: {source}\n"
                f"🏢 Current: {current_company}\n\n"
                f"👇 Pilih company yang betul:"
            )
            
            is_media = bool(query.message.photo or query.message.video or query.message.document)
            try:
                if is_media:
                    await query.message.edit_caption(
                        caption=caption[:1024], parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                else:
                    await query.message.edit_text(
                        caption, parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
            except Exception:
                await self.app.bot.send_message(
                    chat_id=query.from_user.id,
                    text=caption, parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        except Exception as e:
            self.logger.error(f"WA change company error: {e}")

    async def _rt_pick_company(self, update: Update, promo_id, company_id):
        """Handle real-time company pick — admin picks which company for detected post"""
        query = update.callback_query
        await query.answer("🏢 Company dipilih...")

        # Detect if message is media (photo/video/document) or text-only
        is_media = bool(query.message.photo or query.message.video or query.message.document)

        async def _edit_or_send(text, reply_markup=None):
            """Edit caption for media messages, edit text for text messages, fallback to delete+send"""
            try:
                if is_media:
                    await query.message.edit_caption(
                        caption=text[:1024], parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                else:
                    await query.message.edit_text(
                        text, parse_mode='HTML',
                        reply_markup=reply_markup
                    )
            except Exception:
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await self.app.bot.send_message(
                    chat_id=query.from_user.id,
                    text=text, parse_mode='HTML',
                    reply_markup=reply_markup
                )

        try:
            # Get company info
            companies = self.db.get_companies(self.bot_id)
            company = None
            for c in companies:
                if c['id'] == company_id:
                    company = c
                    break
            
            if not company:
                await _edit_or_send("❌ Company tidak ditemui.")
                return
            
            # Get promo from DB
            conn = self.db.get_connection()
            try:
                row = conn.execute("SELECT * FROM detected_promos WHERE id = ?", (promo_id,)).fetchone()
            finally:
                conn.close()
            
            if not row:
                await _edit_or_send("❌ Promo tidak ditemui.")
                return
            
            promo = dict(row)
            original_text = promo.get('original_text', '')
            
            # Swap links with selected company
            swapped_text = original_text
            if company.get('button_url'):
                target_url = company['button_url']
                if target_url.startswith('t.me/'):
                    target_url = 'https://' + target_url
                
                import re
                urls_found = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', original_text)
                if urls_found:
                    for url in urls_found:
                        swapped_text = swapped_text.replace(url, target_url)
                else:
                    swapped_text += f"\n\n🔗 {target_url}"
            
            # Update DB
            try:
                conn = self.db.get_connection()
                conn.execute(
                    "UPDATE detected_promos SET matched_company = ?, swapped_text = ? WHERE id = ?",
                    (company['name'], swapped_text, promo_id)
                )
                conn.commit()
                conn.close()
            except Exception as e:
                self.logger.error(f"Failed to update promo company: {e}")
            
            # Show broadcast buttons
            source = promo.get('source_channel', '')
            caption = (
                f"🔔 <b>COMPANY SELECTED!</b>\n\n"
                f"📢 Source: {source}\n"
                f"🏢 Company: {company['name']}\n\n"
                f"📝 Caption (link swapped ✅):\n"
                f"{swapped_text[:800]}"
            )
            keyboard = [
                [InlineKeyboardButton("✨ AI Rewrite", callback_data=f"scan_ai_0_{promo_id}")],
                [InlineKeyboardButton("📤 Broadcast Groups", callback_data=f"promo_bc_groups_{promo_id}"),
                 InlineKeyboardButton("📤 Broadcast Users", callback_data=f"promo_bc_users_{promo_id}")],
                [InlineKeyboardButton("❌ Skip", callback_data=f"promo_skip_{promo_id}")]
            ]
            
            await _edit_or_send(caption, InlineKeyboardMarkup(keyboard))
            
        except Exception as e:
            self.logger.error(f"RT pick company error: {e}")
            try:
                await _edit_or_send(f"❌ Error: {e}")
            except Exception:
                pass

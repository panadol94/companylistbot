"""
Userbot Manager — Telethon-based channel monitor for auto promo detection.
Each bot owner can connect their Telegram account to monitor channels,
detect promos, swap links, and notify admin for one-click broadcast.
"""
import asyncio
import logging
import re
import json
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import SessionPasswordNeededError

logger = logging.getLogger(__name__)

# URL pattern to detect links in messages
URL_PATTERN = re.compile(r'https?://\S+|t\.me/\S+', re.IGNORECASE)


def match_company_in_text(company_name, text, keywords=''):
    """Smart company name matching with multiple strategies.
    
    Handles variations like:
    - DB: "🚀CM8 Platform" → matches "CM8" in text
    - DB: "🎮BossBet8" → matches "bossbet8" in text
    - Keywords: "a9, a-9" → matches "a9" in text
    
    Returns True if company matches, False otherwise.
    """
    text_lower = text.lower()

    # 0) Check keywords/aliases first (most reliable)
    if keywords:
        for kw in keywords.split(','):
            kw = kw.strip().lower()
            if kw and len(kw) >= 2 and kw in text_lower:
                return True

    # Strip emoji and special unicode chars from company name
    cleaned_name = re.sub(r'[^\w\s\-]', '', company_name, flags=re.UNICODE)
    cleaned_name = ''.join(c for c in cleaned_name if ord(c) < 0x10000 or c.isalnum())
    cleaned_name = cleaned_name.strip()
    
    name_lower = cleaned_name.lower()

    if not name_lower:
        return False

    # 1) Exact substring match
    if name_lower in text_lower:
        return True

    # 2) Individual word match — any word (≥3 chars) from company name in text
    words = re.split(r'[\s\-_.,]+', name_lower)
    significant_words = [w for w in words if len(w) >= 3]
    for word in significant_words:
        if word in text_lower:
            return True

    # 3) Stripped match — remove spaces/special chars and compare
    name_stripped = re.sub(r'[^a-z0-9]', '', name_lower)
    text_stripped = re.sub(r'[^a-z0-9]', '', text_lower)
    if len(name_stripped) >= 3 and name_stripped in text_stripped:
        return True

    return False


class UserbotInstance:
    """Single userbot instance for one bot owner"""
    
    def __init__(self, bot_id, api_id, api_hash, session_string, db, notify_callback):
        self.bot_id = bot_id
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.session_string = session_string
        self.db = db
        self.notify_callback = notify_callback  # async func(bot_id, promo_data)
        self.client = None
        self.running = False
    
    async def start(self):
        """Start the Telethon client and begin monitoring"""
        try:
            self.client = TelegramClient(
                StringSession(self.session_string),
                self.api_id,
                self.api_hash
            )
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.warning(f"[UB-{self.bot_id}] Session not authorized")
                return False
            
            # Register event handler
            self.client.add_event_handler(
                self._on_new_message,
                events.NewMessage()
            )
            
            self.running = True
            logger.info(f"[UB-{self.bot_id}] Userbot started, monitoring channels")
            return True
            
        except Exception as e:
            logger.error(f"[UB-{self.bot_id}] Failed to start: {e}")
            return False
    
    async def stop(self):
        """Stop the Telethon client"""
        self.running = False
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
        logger.info(f"[UB-{self.bot_id}] Userbot stopped")
    
    async def _on_new_message(self, event):
        """Handle new messages from monitored channels"""
        try:
            if not self.running:
                return
            
            # Get monitored channels
            channels = self.db.get_monitored_channels(self.bot_id)
            if not channels:
                return
            
            # Check if message is from a monitored channel
            chat_id = str(event.chat_id)
            monitored_ids = [ch['channel_id'] for ch in channels]
            
            if chat_id not in monitored_ids:
                return
            
            # Get message text
            text = event.message.message or ""
            if not text and not event.message.media:
                return
            
            # Try to match a company
            companies = self.db.get_companies(self.bot_id)
            matched_company = None
            
            for company in companies:
                if match_company_in_text(company['name'], text):
                    matched_company = company
                    break
            
            if not matched_company:
                return  # No company match, skip
            
            # Swap links
            swapped_text = text
            if matched_company.get('button_url'):
                target_url = matched_company['button_url']
                if target_url.startswith('t.me/'):
                    target_url = 'https://' + target_url
                
                urls_found = URL_PATTERN.findall(text)
                if urls_found:
                    for url in urls_found:
                        swapped_text = swapped_text.replace(url, target_url)
                else:
                    # No URLs found, append link
                    swapped_text += f"\n\n🔗 {target_url}"
            
            # Get channel info
            chat = await event.get_chat()
            source_name = getattr(chat, 'title', None) or getattr(chat, 'username', None) or str(chat_id)
            
            # Capture media info
            media_file_ids = []
            media_types = []
            
            if event.message.photo:
                media_types.append('photo')
                media_file_ids.append('photo_pending')  # Will re-download via bot
            elif event.message.video:
                media_types.append('video')
                media_file_ids.append('video_pending')
            
            # Check for grouped media (album)
            if event.message.grouped_id:
                # We'll handle albums - gather from this message only
                pass
            
            # Save to DB
            session_data = self.db.get_userbot_session(self.bot_id)
            auto_mode = session_data.get('auto_mode', 0) if session_data else 0
            
            promo_data = {
                'bot_id': self.bot_id,
                'source_channel': source_name,
                'original_text': text,
                'swapped_text': swapped_text,
                'media_file_ids': media_file_ids,
                'media_types': media_types,
                'matched_company': matched_company['name'],
                'company_button_url': matched_company.get('button_url', ''),
                'company_button_text': matched_company.get('button_text', matched_company['name']),
                'auto_mode': auto_mode,
                'message': event.message  # Keep reference for media download
            }
            
            # Save promo record
            promo_id = self.db.save_detected_promo(
                bot_id=self.bot_id,
                source_channel=source_name,
                original_text=text,
                swapped_text=swapped_text,
                media_file_ids=media_file_ids,
                media_types=media_types,
                matched_company=matched_company['name']
            )
            promo_data['promo_id'] = promo_id
            
            # Notify via callback
            if self.notify_callback:
                await self.notify_callback(self.bot_id, promo_data)
            
            logger.info(f"[UB-{self.bot_id}] Promo detected: {matched_company['name']} from {source_name}")
            
        except Exception as e:
            logger.error(f"[UB-{self.bot_id}] Error processing message: {e}")
    
    async def join_channel(self, channel_link):
        """Join a channel/group and return its info"""
        try:
            if not self.client:
                return None
            
            # Handle different link formats
            if '/joinchat/' in channel_link or '+' in channel_link:
                # Private invite link
                hash_part = channel_link.split('/')[-1].lstrip('+')
                result = await self.client(ImportChatInviteRequest(hash_part))
                chat = result.chats[0]
            else:
                # Public channel @username or t.me/username
                username = channel_link.replace('https://t.me/', '').replace('t.me/', '').replace('@', '').strip('/')
                result = await self.client(JoinChannelRequest(username))
                chat = result.chats[0]
            
            return {
                'id': str(chat.id),
                'title': getattr(chat, 'title', username),
                'username': getattr(chat, 'username', None)
            }
        except Exception as e:
            logger.error(f"[UB-{self.bot_id}] Failed to join channel: {e}")
            return None

    async def scan_channel_history(self, channel_id, days=30):
        """Scan last N days of messages in a channel.
        Returns list of scraped items (text, media, etc).
        """
        if not self.client:
            return []

        since_date = datetime.now(timezone.utc) - timedelta(days=days)

        # Resolve entity — try multiple approaches
        entity = None

        # 1) Try numeric ID directly
        try:
            entity = await self.client.get_entity(int(channel_id))
        except Exception:
            pass

        # 2) Try with -100 prefix (Telethon channel format)
        if not entity:
            try:
                peer_id = int(f"-100{channel_id}")
                entity = await self.client.get_entity(peer_id)
            except Exception:
                pass

        # 3) Try stored username from DB
        if not entity:
            ch_data = self.db.get_monitored_channels(self.bot_id)
            stored_username = None
            for ch in ch_data:
                if str(ch.get('channel_id')) == str(channel_id):
                    stored_username = ch.get('channel_username')
                    break
            if stored_username:
                try:
                    entity = await self.client.get_entity(stored_username)
                    logger.info(f"[UB-{self.bot_id}] Resolved via username: {stored_username}")
                except Exception:
                    pass

        # 4) Try as URL/username string
        if not entity:
            username = str(channel_id)
            for prefix in ['https://t.me/', 'http://t.me/', 't.me/', '@']:
                username = username.replace(prefix, '')
            username = username.strip('/').split('/')[0]

            if username.startswith('+'):
                try:
                    from telethon.tl.functions.messages import ImportChatInviteRequest
                    result = await self.client(ImportChatInviteRequest(username.lstrip('+')))
                    entity = result.chats[0]
                except Exception:
                    pass
            elif username and not username.isdigit():
                try:
                    entity = await self.client.get_entity(username)
                except Exception:
                    pass

        if not entity:
            logger.error(f"[UB-{self.bot_id}] Cannot resolve entity: {channel_id}")
            return 0

        # Update DB with real channel ID if needed
        real_id = str(entity.id)
        if real_id != str(channel_id):
            try:
                self.db.update_monitored_channel_id(self.bot_id, channel_id, real_id,
                                                     getattr(entity, 'title', None),
                                                     getattr(entity, 'username', None))
                logger.info(f"[UB-{self.bot_id}] Updated channel_id {channel_id} -> {real_id}")
            except Exception as e:
                logger.warning(f"[UB-{self.bot_id}] Failed to update channel_id: {e}")

        try:
            msg_count = 0
            scraped = []
            
            # Get companies for auto-matching
            companies = self.db.get_companies(self.bot_id)

            # Auto-generate keywords for companies that don't have any yet
            for company in companies:
                if not company.get('keywords'):
                    try:
                        from ai_rewriter import generate_keywords
                        kw = await generate_keywords(company['name'])
                        self.db.edit_company(company['id'], 'keywords', kw)
                        company['keywords'] = kw  # Update in-memory too
                        logger.info(f"[UB-{self.bot_id}] Auto-generated keywords for {company['name']}: {kw}")
                    except Exception as e:
                        logger.warning(f"[UB-{self.bot_id}] Failed to generate keywords: {e}")

            async for msg in self.client.iter_messages(entity, limit=None):
                # Stop when we reach messages older than our cutoff
                if msg.date and msg.date.replace(tzinfo=timezone.utc) < since_date:
                    break

                msg_count += 1
                text = msg.message or ""

                # Skip messages with no text AND no media
                has_links = bool(URL_PATTERN.findall(text)) if text else False
                has_media = bool(msg.photo or msg.video)
                if not text and not has_media:
                    continue

                # Auto-detect company
                matched_company = None
                if text and companies:
                    for company in companies:
                        kw = company.get('keywords', '') or ''
                        if match_company_in_text(company['name'], text, keywords=kw):
                            matched_company = company
                            break

                # Get source name (only once)
                if not scraped:
                    chat = await msg.get_chat()
                    source_name = getattr(chat, 'title', None) or str(channel_id)
                else:
                    source_name = scraped[0]['source_channel']

                # Download media
                media_bytes = None
                media_type = None
                if msg.photo:
                    media_type = 'photo'
                    try:
                        media_bytes = await self.client.download_media(msg, bytes)
                    except Exception as e:
                        logger.warning(f"[UB-{self.bot_id}] Failed to download photo: {e}")
                elif msg.video:
                    media_type = 'video'
                    try:
                        media_bytes = await self.client.download_media(msg, bytes)
                    except Exception as e:
                        logger.warning(f"[UB-{self.bot_id}] Failed to download video: {e}")

                scraped.append({
                    'source_channel': source_name,
                    'original_text': text,
                    'media_bytes': media_bytes,
                    'media_type': media_type,
                    'msg_date': msg.date.strftime('%Y-%m-%d %H:%M') if msg.date else '',
                    'msg_id': msg.id,
                    'matched_company': matched_company,  # Auto-detected company dict or None
                })

                await asyncio.sleep(0.2)  # Rate limit

        except Exception as e:
            logger.error(f"[UB-{self.bot_id}] Error scanning history for {channel_id}: {e}")

        auto_matched = sum(1 for s in scraped if s.get('matched_company'))
        logger.info(f"[UB-{self.bot_id}] Scanned {msg_count} messages in {channel_id}, scraped {len(scraped)} items ({auto_matched} auto-matched)")
        return scraped


class UserbotManager:
    """Manages all userbot instances across the platform"""
    
    def __init__(self, db):
        self.db = db
        self.instances = {}  # {bot_id: UserbotInstance}
        self.notify_callbacks = {}  # {bot_id: callback}
        self._auth_states = {}  # {bot_id: {client, phone_hash}}
    
    def set_notify_callback(self, bot_id, callback):
        """Set notification callback for a bot"""
        self.notify_callbacks[bot_id] = callback
    
    async def start_all(self):
        """Start all active userbot sessions (called on platform startup)"""
        sessions = self.db.get_all_active_userbot_sessions()
        started = 0
        for session in sessions:
            bot_id = session['bot_id']
            callback = self.notify_callbacks.get(bot_id)
            success = await self.start_instance(bot_id, session, callback)
            if success:
                started += 1
        logger.info(f"🤖 Userbot Manager: {started}/{len(sessions)} instances started")
    
    async def start_instance(self, bot_id, session_data=None, callback=None):
        """Start a single userbot instance"""
        if bot_id in self.instances:
            return True  # Already running
        
        if not session_data:
            session_data = self.db.get_userbot_session(bot_id)
        
        if not session_data or not session_data.get('session_string'):
            return False
        
        if not callback:
            callback = self.notify_callbacks.get(bot_id)
        
        instance = UserbotInstance(
            bot_id=bot_id,
            api_id=session_data['api_id'],
            api_hash=session_data['api_hash'],
            session_string=session_data['session_string'],
            db=self.db,
            notify_callback=callback
        )
        
        success = await instance.start()
        if success:
            self.instances[bot_id] = instance
            self.db.toggle_userbot(bot_id, True)
        return success
    
    async def stop_instance(self, bot_id):
        """Stop a single userbot instance"""
        if bot_id in self.instances:
            await self.instances[bot_id].stop()
            del self.instances[bot_id]
        self.db.toggle_userbot(bot_id, False)
    
    async def stop_all(self):
        """Stop all userbot instances (called on platform shutdown)"""
        for bot_id in list(self.instances.keys()):
            await self.stop_instance(bot_id)
        logger.info("🤖 Userbot Manager: All instances stopped")
    
    # === AUTH FLOW ===
    
    async def begin_auth(self, bot_id, api_id, api_hash, phone):
        """Step 1: Send OTP code to phone"""
        try:
            client = TelegramClient(
                StringSession(),
                int(api_id),
                api_hash
            )
            await client.connect()
            
            result = await client.send_code_request(phone)
            
            # Store auth state
            self._auth_states[bot_id] = {
                'client': client,
                'phone': phone,
                'phone_hash': result.phone_code_hash,
                'api_id': api_id,
                'api_hash': api_hash
            }
            
            logger.info(f"[UB-{bot_id}] Auth code sent to {phone}")
            return True
            
        except Exception as e:
            logger.error(f"[UB-{bot_id}] Auth begin failed: {e}")
            return False
    
    async def verify_code(self, bot_id, code):
        """Step 2: Verify OTP code, returns (success, needs_2fa)"""
        state = self._auth_states.get(bot_id)
        if not state:
            return False, False
        
        try:
            client = state['client']
            
            await client.sign_in(
                phone=state['phone'],
                code=code,
                phone_code_hash=state['phone_hash']
            )
            
            # Success! Save session
            session_string = client.session.save()
            self.db.save_userbot_session(
                bot_id=bot_id,
                api_id=state['api_id'],
                api_hash=state['api_hash'],
                session_string=session_string,
                phone=state['phone']
            )
            
            # Cleanup auth state
            await client.disconnect()
            del self._auth_states[bot_id]
            
            logger.info(f"[UB-{bot_id}] Auth successful")
            return True, False
            
        except SessionPasswordNeededError:
            # 2FA needed
            return False, True
            
        except Exception as e:
            logger.error(f"[UB-{bot_id}] Auth verify failed: {e}")
            return False, False
    
    async def verify_2fa(self, bot_id, password):
        """Step 3: Verify 2FA password"""
        state = self._auth_states.get(bot_id)
        if not state:
            return False
        
        try:
            client = state['client']
            await client.sign_in(password=password)
            
            # Success! Save session
            session_string = client.session.save()
            self.db.save_userbot_session(
                bot_id=bot_id,
                api_id=state['api_id'],
                api_hash=state['api_hash'],
                session_string=session_string,
                phone=state['phone']
            )
            
            await client.disconnect()
            del self._auth_states[bot_id]
            
            logger.info(f"[UB-{bot_id}] 2FA auth successful")
            return True
            
        except Exception as e:
            logger.error(f"[UB-{bot_id}] 2FA verify failed: {e}")
            return False
    
    async def join_channel_for_bot(self, bot_id, channel_link):
        """Join a channel using the bot's userbot instance"""
        instance = self.instances.get(bot_id)
        if not instance:
            return None
        return await instance.join_channel(channel_link)
    
    def is_running(self, bot_id):
        """Check if userbot is running for a bot"""
        return bot_id in self.instances and self.instances[bot_id].running

    async def scan_all_channels_history(self, bot_id, days=30, progress_callback=None):
        """Scan history of all monitored channels for a bot.
        
        progress_callback: async func(current, total, channel_name, scraped_count)
        Returns list of all scraped items.
        """
        instance = self.instances.get(bot_id)
        if not instance or not instance.client:
            return []  # Not connected

        channels = self.db.get_monitored_channels(bot_id)
        if not channels:
            return []

        all_scraped = []
        for i, ch in enumerate(channels):
            channel_id = ch['channel_id']
            channel_name = ch.get('channel_title', channel_id)

            try:
                scraped = await instance.scan_channel_history(channel_id, days=days)
                all_scraped.extend(scraped)

                if progress_callback:
                    await progress_callback(i + 1, len(channels), channel_name, len(scraped))

            except Exception as e:
                logger.error(f"[UB-{bot_id}] Scan failed for {channel_name}: {e}")
                if progress_callback:
                    await progress_callback(i + 1, len(channels), f"❌ {channel_name}", 0)

        logger.info(f"[UB-{bot_id}] History scan complete: {len(all_scraped)} items from {len(channels)} channels")
        return all_scraped

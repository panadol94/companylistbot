import asyncio
import logging
import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from telegram import Update
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from database import Database
from config import MOTHER_TOKEN, DB_FILE, DOMAIN_URL
from mother_bot import MotherBot
from child_bot import ChildBot
from userbot_manager import UserbotManager
import uvicorn

# Logging Config
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Global Manager Instance
bot_manager = None

class BotManager:
    def __init__(self):
        self.db = Database(DB_FILE)
        self.scheduler = AsyncIOScheduler()
        self.bots = {} # {token: ChildBotInstance}
        self.mother_bot = None
        self.userbot_manager = UserbotManager(self.db)

    async def start(self):
        logger.info("🚀 Starting Bot SaaS Platform...")
        self.scheduler.start()
        
        # Schedule daily database backup at 3 AM
        self.scheduler.add_job(
            self.backup_database,
            'cron',
            hour=3,
            minute=0,
            id='daily_backup'
        )
        
        # Schedule expiry reminder check at 9 AM daily
        self.scheduler.add_job(
            self.check_expiring_bots,
            'cron',
            hour=9,
            minute=0,
            id='expiry_reminder'
        )
        
        # Schedule 4D results update at 7:30 PM daily (after draw results announced)
        self.scheduler.add_job(
            self.update_4d_results,
            'cron',
            hour=19,
            minute=30,
            id='4d_update'
        )
        
        # Also backup on startup
        await self.backup_database()
        
        # 1. Start Mother Bot
        self.mother_bot = MotherBot(MOTHER_TOKEN, self.db, self)
        await self.mother_bot.initialize() # Setup App
        await self.enable_bot_updates(self.mother_bot.app, MOTHER_TOKEN)
        logger.info("✅ Mother Bot Ready.")

        # 2. Load & Start Child Bots
        bots_data = self.db.get_all_bots()
        logger.info(f"📂 Found {len(bots_data)} existing child bots.")
        for bot_data in bots_data:
            await self.spawn_bot(bot_data)
        
        # 3. Start Userbot Manager (load all active sessions)
        await self.userbot_manager.start_all()
        
        # 4. Start WhatsApp Monitor (Node.js) in background
        try:
            import subprocess, os, shutil
            node_path = shutil.which('node')
            logger.info(f"📱 WA Monitor: node binary = {node_path}")
            wa_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'wa-monitor')
            wa_index = os.path.join(wa_dir, 'index.js')
            logger.info(f"📱 WA Monitor: index.js exists = {os.path.exists(wa_index)}, dir = {wa_dir}")
            if node_path and os.path.exists(wa_index):
                self.wa_process = subprocess.Popen(
                    [node_path, 'index.js'],
                    cwd=wa_dir
                )
                logger.info(f"📱 WhatsApp Monitor started (PID: {self.wa_process.pid})")
            else:
                logger.warning(f"⚠️ WA Monitor skipped: node={node_path}, index.js={os.path.exists(wa_index)}")
        except Exception as e:
            logger.error(f"❌ Failed to start WA Monitor: {e}")
        
        logger.info(f"🌟 Platform Running. Domain: {DOMAIN_URL}")

    async def backup_database(self):
        """Create backup of database file"""
        import shutil
        import os
        from datetime import datetime
        
        try:
            backup_dir = "/data/backups"
            os.makedirs(backup_dir, exist_ok=True)
            
            # Create timestamped backup
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"{backup_dir}/bot_platform_{timestamp}.db"
            
            shutil.copy2(DB_FILE, backup_file)
            logger.info(f"💾 Database backup created: {backup_file}")
            
            # Keep only last 7 backups
            backups = sorted([f for f in os.listdir(backup_dir) if f.endswith('.db')])
            while len(backups) > 7:
                oldest = backups.pop(0)
                os.remove(f"{backup_dir}/{oldest}")
                logger.info(f"🗑️ Deleted old backup: {oldest}")
                
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")

    async def check_expiring_bots(self):
        """Check and notify owners of expiring bots"""
        from telegram import Bot
        
        try:
            # Get bots expiring in 3 days
            expiring_bots = self.db.get_expiring_bots(days=3)
            
            if not expiring_bots:
                logger.info("📫 No expiring bots to notify")
                return
            
            mother = Bot(token=MOTHER_TOKEN)
            
            for bot in expiring_bots:
                try:
                    # Parse expiry date
                    import datetime
                    expiry = datetime.datetime.fromisoformat(bot['subscription_end'])
                    days_left = (expiry - datetime.datetime.now()).days
                    
                    # Send reminder to owner
                    message = (
                        f"⚠️ **SUBSCRIPTION EXPIRING SOON**\n\n"
                        f"Bot #{bot['id']} akan tamat dalam **{days_left} hari**!\n"
                        f"Expiry: {expiry.strftime('%Y-%m-%d')}\n\n"
                        f"Sila hubungi admin untuk renew subscription."
                    )
                    
                    await mother.send_message(
                        chat_id=bot['owner_id'],
                        text=message,
                        parse_mode='Markdown'
                    )
                    logger.info(f"📧 Sent expiry reminder to {bot['owner_id']} for bot #{bot['id']}")
                    
                except Exception as e:
                    logger.error(f"Failed to send expiry reminder for bot #{bot['id']}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Expiry check failed: {e}")

    async def update_4d_results(self):
        """Fetch and save latest 4D results from live4d2u.net"""
        try:
            from utils_4d import fetch_all_4d_results
            
            logger.info("🎰 Fetching latest 4D results...")
            
            results = await fetch_all_4d_results()
            
            if not results:
                logger.warning("⚠️ No 4D results fetched")
                return
            
            saved_count = 0
            for company, draws in results.items():
                for draw in draws:
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
                        saved_count += 1
            
            logger.info(f"✅ 4D Update complete: {saved_count} results saved")
            
            # Send notifications to subscribers if new results were saved
            if saved_count > 0:
                await self.notify_4d_subscribers(results)
            
        except Exception as e:
            logger.error(f"❌ 4D update failed: {e}")

    async def notify_4d_subscribers(self, results):
        """Send 4D results notification to all subscribers"""
        try:
            # Get all subscribers
            subscribers = self.db.get_all_4d_subscribers()
            
            if not subscribers:
                logger.info("📬 No 4D subscribers to notify")
                return
            
            # Build notification message
            today = datetime.datetime.now().strftime('%d/%m/%Y')
            
            # Get first prize from top 3 companies
            highlights = []
            for company in ['MAGNUM', 'TOTO', 'DAMACAI']:
                if company in results and results[company]:
                    first = results[company][0].get('first', '----')
                    highlights.append(f"{company}: {first}")
            
            message = (
                f"🎰 **4D RESULTS - {today}**\n\n"
                f"🏆 **First Prizes:**\n"
            )
            
            for h in highlights:
                message += f"• {h}\n"
            
            message += (
                f"\n📊 Total {len(results)} companies updated!\n\n"
                f"👉 Tekan /4d untuk lihat semua result."
            )
            
            # Send to each subscriber (group by bot)
            from telegram import Bot
            
            sent_count = 0
            failed_count = 0
            
            # Group subscribers by bot_id
            by_bot = {}
            for sub in subscribers:
                bot_id = sub['bot_id']
                if bot_id not in by_bot:
                    by_bot[bot_id] = []
                by_bot[bot_id].append(sub['user_id'])
            
            # Send via each child bot
            for bot_id, user_ids in by_bot.items():
                # Get bot token
                bot_data = self.db.get_bot_by_id(bot_id)
                if not bot_data:
                    continue
                
                try:
                    bot = Bot(token=bot_data['token'])
                    
                    for user_id in user_ids:
                        try:
                            await bot.send_message(
                                chat_id=user_id,
                                text=message,
                                parse_mode='Markdown'
                            )
                            sent_count += 1
                        except Exception as e:
                            failed_count += 1
                            logger.debug(f"Failed to notify {user_id}: {e}")
                except Exception as e:
                    logger.error(f"Failed to create bot for {bot_id}: {e}")
            
            logger.info(f"📬 4D Notifications sent: {sent_count} success, {failed_count} failed")
            
        except Exception as e:
            logger.error(f"❌ 4D notification failed: {e}")

    async def spawn_bot(self, bot_data):
        token = bot_data['token']
        if token in self.bots: return

        try:
            child = ChildBot(token, bot_data['id'], self.db, self.scheduler)
            child.userbot_manager = self.userbot_manager  # Inject UserbotManager
            # Set notification callback for this bot
            self.userbot_manager.set_notify_callback(bot_data['id'], child.handle_promo_notification)
            await child.initialize() # Setup App
            await self.enable_bot_updates(child.app, token)
            
            self.bots[token] = child
            logger.info(f"🟢 Child Bot Hooked: {token[:15]}...")
        except Exception as e:
            logger.error(f"❌ Failed to hook bot {token[:15]}...: {e}")

    async def enable_bot_updates(self, app, token):
        # All update types we need (including new_chat_members for group welcome)
        allowed = ["message", "callback_query", "my_chat_member", "chat_member", "channel_post", "edited_message"]
        
        # Check if localhost or HTTP (not HTTPS)
        if "localhost" in DOMAIN_URL or "127.0.0.1" in DOMAIN_URL or DOMAIN_URL.startswith("http://"):
            logger.info(f"🔄 Non-HTTPS environment detected. Starting POLLING for {token[:10]}...")
            await app.updater.start_polling(allowed_updates=allowed)
        else:
            await self.setup_webhook(app, token, allowed)

    async def setup_webhook(self, app, token, allowed_updates=None):
        # Set webhook to our FastAPI endpoint
        webhook_url = f"{DOMAIN_URL}/webhook/{token}"
        await app.bot.set_webhook(webhook_url, allowed_updates=allowed_updates)

    async def process_update(self, token, update_data):
        # Determine target bot
        if token == MOTHER_TOKEN:
            app = self.mother_bot.app
        elif token in self.bots:
            app = self.bots[token].app
        else:
            logger.warning(f"⚠️ Update received for unknown token: {token}")
            return

        # Process Update
        update = Update.de_json(update_data, app.bot)
        
        # Debug: Log what type of update we received
        if update.callback_query:
            logger.info(f"🔔 Callback Query: {update.callback_query.data} from user {update.callback_query.from_user.id}")
        elif update.message:
            logger.info(f"💬 Message: {update.message.text[:50] if update.message.text else 'media'}")
        
        try:
            await app.process_update(update)
        except Exception as e:
            logger.error(f"❌ Error processing update: {e}", exc_info=True)

    async def stop_bot(self, bot_id):
        """Stop a running child bot by ID"""
        # Find bot by ID
        bot_data = self.db.get_bot_by_id(bot_id)
        if not bot_data:
            logger.warning(f"⚠️ Bot {bot_id} not found in database")
            return
        
        token = bot_data['token']
        if token in self.bots:
            try:
                child = self.bots[token]
                await child.stop()
                del self.bots[token]
                logger.info(f"🔴 Child Bot {bot_id} stopped")
            except Exception as e:
                logger.error(f"❌ Failed to stop bot {bot_id}: {e}")
        else:
            logger.warning(f"⚠️ Bot {bot_id} not running (token not in active bots)")

    async def shutdown(self):
        logger.info("🔻 Shutting down platform...")
        await self.userbot_manager.stop_all()
        self.scheduler.shutdown()

# --- FastAPI Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global bot_manager
    bot_manager = BotManager()
    await bot_manager.start()
    yield
    # Shutdown
    await bot_manager.shutdown()

# --- FastAPI App ---
app = FastAPI(lifespan=lifespan)

@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    try:
        data = await request.json()
        if bot_manager:
            await bot_manager.process_update(token, data)
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Webhook Error: {e}")
        return Response(status_code=500)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "bots_active": len(bot_manager.bots) if bot_manager else 0}

@app.get("/server")
async def server_status():
    """Server resource monitoring endpoint"""
    try:
        import psutil
        import platform
        
        cpu_percent = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Get uptime
        import time
        uptime_seconds = time.time() - psutil.boot_time()
        days = int(uptime_seconds // 86400)
        hours = int((uptime_seconds % 86400) // 3600)
        
        # Get top processes by memory
        top_procs = []
        for proc in sorted(psutil.process_iter(['pid', 'name', 'memory_percent']), 
                          key=lambda p: p.info.get('memory_percent', 0) or 0, reverse=True)[:5]:
            top_procs.append({
                "pid": proc.info['pid'],
                "name": proc.info['name'],
                "memory_percent": round(proc.info.get('memory_percent', 0) or 0, 1)
            })
        
        return {
            "status": "healthy" if mem.percent < 85 else "warning",
            "cpu_percent": cpu_percent,
            "ram": {
                "used_gb": round(mem.used / (1024**3), 2),
                "total_gb": round(mem.total / (1024**3), 2),
                "percent": mem.percent
            },
            "disk": {
                "used_gb": round(disk.used / (1024**3), 2),
                "total_gb": round(disk.total / (1024**3), 2),
                "percent": round(disk.percent, 1)
            },
            "uptime": f"{days}d {hours}h",
            "bots_active": len(bot_manager.bots) if bot_manager else 0,
            "top_processes": top_procs
        }
    except ImportError:
        return {"error": "psutil not installed"}
    except Exception as e:
        return {"error": str(e)}

# --- WhatsApp Monitor API ---

@app.post("/api/wa-promo")
async def wa_promo_received(request: Request):
    """Receive WhatsApp group message from Baileys monitor.
    Runs 2-layer company detection (fuzzy + AI) and notifies admin.
    """
    try:
        data = await request.json()
        bot_id = data.get('bot_id')
        text = data.get('text', '')
        group_name = data.get('group_name', 'Unknown Group')
        
        if not bot_id or (not text and not data.get('has_media')):
            return {"success": False, "error": "Missing bot_id or content"}
        
        if not bot_manager:
            return {"success": False, "error": "Platform not ready"}
        
        # Get companies for this bot
        companies = bot_manager.db.get_companies(bot_id)
        if not companies:
            return {"success": False, "error": "No companies for this bot"}
        
        # Layer 1: Fuzzy match (fast, free)
        from userbot_manager import match_company_in_text
        matched_company = None
        
        # Priority 1: Match by group name (most reliable)
        for company in companies:
            keywords = company.get('keywords', '')
            if match_company_in_text(company['name'], group_name, keywords):
                matched_company = company
                logger.info(f"📱 WA Layer 1 match (group name): '{company['name']}' in group '{group_name}'")
                break
        
        # Priority 2: Match by message text
        if not matched_company and text:
            for company in companies:
                keywords = company.get('keywords', '')
                if match_company_in_text(company['name'], text, keywords):
                    matched_company = company
                    logger.info(f"📱 WA Layer 1 match (text): '{company['name']}' in group '{group_name}'")
                    break
        
        # Layer 2: AI detection (if Layer 1 fails and there's text)
        if not matched_company and text:
            try:
                from ai_rewriter import detect_company_ai
                company_names = [c['name'] for c in companies]
                ai_match = await detect_company_ai(text, company_names)
                if ai_match:
                    matched_company = next((c for c in companies if c['name'] == ai_match), None)
                    logger.info(f"📱 WA Layer 2 AI match: '{ai_match}' in group '{group_name}'")
            except Exception as e:
                logger.error(f"AI detection error: {e}")
        
        if not matched_company:
            logger.debug(f"📱 WA no match in group '{group_name}': {text[:50]}...")
            return {"success": True, "matched": False}
        
        # Find the correct child bot to notify
        bot_data = bot_manager.db.get_bot_by_id(bot_id)
        if not bot_data or bot_data['token'] not in bot_manager.bots:
            return {"success": False, "error": "Bot not running"}
        
        child = bot_manager.bots[bot_data['token']]
        
        # Build promo data (compatible with existing handle_promo_notification)
        promo_data = {
            'bot_id': bot_id,
            'source_channel': f"📱 WA: {group_name}",
            'original_text': text,
            'swapped_text': text,
            'media_file_ids': [],
            'media_types': [],
            'matched_company': matched_company['name'],
            'company_button_url': matched_company.get('button_url', ''),
            'company_button_text': matched_company.get('button_text', ''),
            'auto_mode': 0,  # Always manual review for WA
            'media_bytes': None,
            'media_type': None,
            'all_media_bytes': [],
            'all_media_types': [],
            'is_album': False,
            'entities': [],
        }
        
        # Use existing promo notification flow
        await child.handle_promo_notification(bot_id, promo_data)
        
        logger.info(f"📱 WA promo forwarded to admin: {matched_company['name']} from '{group_name}'")
        return {"success": True, "matched": True, "company": matched_company['name']}
        
    except Exception as e:
        logger.error(f"❌ WA promo API error: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

@app.post("/api/wa-status")
async def wa_status_update(request: Request):
    """Receive WhatsApp connection status update from Baileys."""
    try:
        data = await request.json()
        bot_id = data.get('bot_id')
        status = data.get('status', 'disconnected')
        
        if not bot_id or not bot_manager:
            return {"success": False}
        
        bot_manager.db.save_whatsapp_session(bot_id, status=status)
        logger.info(f"📱 WA status update: bot {bot_id} → {status}")
        
        # Notify bot admins via Telegram
        try:
            # Find bot instance by bot_id
            child = None
            for token, bot in bot_manager.bots.items():
                if bot.bot_id == bot_id:
                    child = bot
                    break
            
            if child:
                admins = child.db.get_admins(bot_id)
                if status == 'connected':
                    msg = "✅ <b>WhatsApp Connected!</b>\n\n📱 WhatsApp monitor aktif. Semua mesej group akan dimonitor untuk company detection."
                elif status == 'disconnected':
                    msg = "❌ <b>WhatsApp Disconnected</b>\n\nSambungan WhatsApp terputus. Pergi ke /settings → 📱 WhatsApp Monitor untuk reconnect."
                else:
                    msg = f"📱 WhatsApp status: <b>{status}</b>"
                
                for admin_id in admins:
                    try:
                        await child.app.bot.send_message(
                            chat_id=admin_id,
                            text=msg,
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        logger.warning(f"Failed to notify admin {admin_id}: {e}")
        except Exception as e:
            logger.error(f"WA admin notification error: {e}")
        
        return {"success": True}
    except Exception as e:
        logger.error(f"WA status error: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    if not MOTHER_TOKEN:
        logger.critical("❌ MOTHER_TOKEN not found! Exiting.")
        exit(1)
        
    # Run Uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")

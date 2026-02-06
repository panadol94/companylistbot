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
            await child.initialize() # Setup App
            await self.enable_bot_updates(child.app, token)
            
            self.bots[token] = child
            logger.info(f"🟢 Child Bot Hooked: {token[:15]}...")
        except Exception as e:
            logger.error(f"❌ Failed to hook bot {token[:15]}...: {e}")

    async def enable_bot_updates(self, app, token):
        # Check if localhost or HTTP (not HTTPS)
        if "localhost" in DOMAIN_URL or "127.0.0.1" in DOMAIN_URL or DOMAIN_URL.startswith("http://"):
            logger.info(f"🔄 Non-HTTPS environment detected. Starting POLLING for {token[:10]}...")
            await app.updater.start_polling()
        else:
            await self.setup_webhook(app, token)

    async def setup_webhook(self, app, token):
        # Set webhook to our FastAPI endpoint
        webhook_url = f"{DOMAIN_URL}/webhook/{token}"
        await app.bot.set_webhook(webhook_url)

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
        await app.process_update(update)

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

if __name__ == "__main__":
    if not MOTHER_TOKEN:
        logger.critical("❌ MOTHER_TOKEN not found! Exiting.")
        exit(1)
        
    # Run Uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")

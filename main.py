import asyncio
import logging
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

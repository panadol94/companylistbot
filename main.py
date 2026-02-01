import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from database import Database
from config import MOTHER_TOKEN, DB_FILE
from mother_bot import MotherBot
from child_bot import ChildBot

# Logging Config
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

class BotManager:
    def __init__(self):
        self.db = Database(DB_FILE)
        self.scheduler = AsyncIOScheduler()
        self.bots = {} # {token: ChildBotInstance}
        self.mother_bot = None

    async def start(self):
        logger.info("🚀 Starting Bot SaaS Platform...")
        self.scheduler.start()
        
        # 1. Start Mother Bot
        self.mother_bot = MotherBot(MOTHER_TOKEN, self.db, self)
        await self.mother_bot.start()
        logger.info("✅ Mother Bot Started.")

        # 2. Load & Start Child Bots
        bots_data = self.db.get_all_bots()
        logger.info(f"📂 Found {len(bots_data)} existing child bots.")
        for bot_data in bots_data:
            await self.spawn_bot(bot_data)
        
        logger.info("🌟 All systems operational. Waiting for updates...")
        
        # Keep alive
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self.shutdown()

    async def spawn_bot(self, bot_data):
        token = bot_data['token']
        if token in self.bots:
            logger.warning(f"⚠️ Bot {token[:10]}... already running.")
            return

        try:
            child = ChildBot(token, bot_data['id'], self.db, self.scheduler)
            await child.start()
            self.bots[token] = child
            logger.info(f"🟢 Child Bot Started: {token[:15]}...")
        except Exception as e:
            logger.error(f"❌ Failed to start bot {token[:15]}...: {e}")

    async def shutdown(self):
        logger.info("🔻 Shutting down platform...")
        if self.mother_bot: await self.mother_bot.stop()
        for bot in self.bots.values():
            await bot.stop()
        self.scheduler.shutdown()
        logger.info("👋 Goodbye.")

if __name__ == "__main__":
    if not MOTHER_TOKEN:
        logger.critical("❌ MOTHER_TOKEN not found in env/config.py! Exiting.")
        exit(1)
        
    manager = BotManager()
    try:
        asyncio.run(manager.start())
    except KeyboardInterrupt:
        pass

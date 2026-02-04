import os
from dotenv import load_dotenv

load_dotenv()

# Mother Bot Token (SaaS Platform Owner)
MOTHER_TOKEN = os.getenv("MOTHER_TOKEN", "8297991145:AAEptyoDK1NU9Y_6n5lq0KUcIcWR1FoVEI4")

# Master Admin IDs (Comma-separated list of owners/partners)
# Example: 123456789,987654321
# You can find your Telegram ID by messaging @userinfobot
_admin_ids = os.getenv("MASTER_ADMIN_IDS", os.getenv("MASTER_ADMIN_ID", "5925622731,1233349895"))
MASTER_ADMIN_IDS = [int(x.strip()) for x in _admin_ids.split(",") if x.strip().isdigit()]
# Legacy support - single ID
MASTER_ADMIN_ID = MASTER_ADMIN_IDS[0] if MASTER_ADMIN_IDS else 5925622731 

# Database File Path
DB_FILE = os.getenv("DB_FILE", "bot_platform.db")

# Global Ad Footer (Default - leave empty or customize)
DEFAULT_GLOBAL_AD = ""

# Webhook Domain (Coolify URL)
# Example: https://bot-saas.coolify.yourvps.com
DOMAIN_URL = os.getenv("DOMAIN_URL", "http://localhost:8000")


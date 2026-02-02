import os
from dotenv import load_dotenv

load_dotenv()

# Mother Bot Token (SaaS Platform Owner)
MOTHER_TOKEN = os.getenv("MOTHER_TOKEN", "8297991145:AAEptyoDK1NU9Y_6n5lq0KUcIcWR1FoVEI4")

# Master Admin ID (The Owner of the SaaS Platform)
# Replace with your Telegram ID. You can find it by messaging @userinfobot
MASTER_ADMIN_ID = int(os.getenv("MASTER_ADMIN_ID", "0")) 

# Database File Path
DB_FILE = os.getenv("DB_FILE", "bot_platform.db")

# Global Ad Footer (Default - leave empty or customize)
DEFAULT_GLOBAL_AD = ""

# Webhook Domain (Coolify URL)
# Example: https://bot-saas.coolify.yourvps.com
DOMAIN_URL = os.getenv("DOMAIN_URL", "http://localhost:8000")


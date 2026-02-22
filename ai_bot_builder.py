"""
AI Bot Builder — Uses user-provided API keys to configure bots via AI chat.
Supports Groq (free) and Google Gemini (free) providers with vision.
"""
import os
import logging
import json
import re
import aiohttp
import base64

logger = logging.getLogger(__name__)

# --- Provider API configs ---
PROVIDERS = {
    'groq': {
        'name': 'Groq',
        'api_url': 'https://api.groq.com/openai/v1/chat/completions',
        'text_model': 'llama-3.3-70b-versatile',
        'vision_model': 'meta-llama/llama-4-scout-17b-16e-instruct',
    },
    'gemini': {
        'name': 'Google Gemini',
        'api_url': 'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent',
        'text_model': 'gemini-2.0-flash',
        'vision_model': 'gemini-2.0-flash',
    }
}

BOT_BUILDER_SYSTEM_PROMPT = """Kau adalah AI Bot Builder untuk platform MASUK10 ROBOT.
Tugas kau: bantu user buat dan configure Telegram bot mereka.

KONTEKS PLATFORM:
- Bot ni untuk company listing (senarai syarikat/bisnes)
- Setiap bot boleh ada: Welcome message, Companies (dengan nama, description, media, button link), Menu buttons
- User describe apa business dia, kau suggest configuration yang sesuai

CARA KAU RESPOND:
- Bahasa santai campur BM/English macam orang Malaysia biasa
- Tanya soalan untuk faham apa user nak
- Bagi cadangan yang specific dan actionable
- Kalau user hantar screenshot, analyze dan suggest based on apa yang kau nampak

PENTING — BILA USER DAH READY:
Kalau user dah cukup describe dan nak generate, respond dengan JSON config SAHAJA dalam format:

```json
{
  "ready": true,
  "welcome_message": "Welcome message untuk bot",
  "welcome_caption": "Caption pendek untuk banner (optional)",
  "companies": [
    {
      "name": "Company Name",
      "description": "Description dalam 1-2 ayat",
      "emoji": "🏢",
      "button_text": "DAFTAR SEKARANG",
      "button_url": "https://example.com"
    }
  ],
  "menu_buttons": [
    {"text": "📞 Contact", "url": "https://t.me/support"}
  ]
}
```

RULES FOR JSON:
- welcome_message: wajib ada, max 500 chars, boleh guna emoji
- companies: at least 1 company, max 20
- button_url: MESTI valid URL (https://)
- Kalau user tak bagi URL, guna placeholder "https://example.com" dan bagitahu user tukar nanti
- menu_buttons: optional, max 10
- emoji: 1 emoji yang represent company type

KALAU BELUM READY:
- Just respond macam biasa, tanya soalan, bagi cadangan
- JANGAN generate JSON kalau user belum confirm ready
- Akhiri dengan soalan untuk guide user

KALAU USER HANTAR SCREENSHOT:
- Analyze gambar tu — nampak bot lain ke, website ke, menu ke
- Suggest similar config based on apa kau nampak
- Tanya "Nak aku buat macam ni ke?"
"""

BOT_MODIFY_SYSTEM_PROMPT = """Kau adalah AI assistant untuk modify/update Telegram bot yang dah sedia ada.
Bot ni running on platform MASUK10 ROBOT.

BOT YANG ADA SEKARANG:
{current_config}

TUGAS KAU:
- Faham apa user nak ubah
- Suggest changes yang specific
- Bila ready, output JSON dengan HANYA fields yang nak diubah

FORMAT JSON UNTUK MODIFY:
```json
{{
  "ready": true,
  "action": "modify",
  "changes": {{
    "welcome_message": "New welcome message (kalau nak ubah)",
    "add_companies": [
      {{"name": "New Company", "description": "...", "emoji": "🏢", "button_text": "...", "button_url": "..."}}
    ],
    "remove_companies": ["Company Name To Remove"],
    "add_menu_buttons": [
      {{"text": "New Button", "url": "https://..."}}
    ]
  }}
}}
```

RULES:
- Hanya include fields yang nak diubah dalam "changes"
- Kalau belum ready, just chat biasa dan tanya confirmation
"""


async def validate_api_key(api_key: str, provider: str) -> tuple:
    """Test if an API key is valid.
    
    Returns:
        (True, model_info) if valid
        (False, error_message) if invalid
    """
    provider = provider.lower()
    
    if provider == 'groq':
        return await _validate_groq(api_key)
    elif provider == 'gemini':
        return await _validate_gemini(api_key)
    else:
        return False, f"Provider '{provider}' tidak disokong. Guna 'groq' atau 'gemini'."


async def _validate_groq(api_key: str) -> tuple:
    """Validate Groq API key"""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 5,
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions",
                json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True, "Groq API ✅"
                elif resp.status == 401:
                    return False, "API key tidak sah. Sila semak semula."
                else:
                    error = await resp.text()
                    return False, f"Error {resp.status}: {error[:100]}"
    except Exception as e:
        return False, f"Connection error: {str(e)[:100]}"


async def _validate_gemini(api_key: str) -> tuple:
    """Validate Google Gemini API key"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": "Hi"}]}],
        "generationConfig": {"maxOutputTokens": 5}
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True, "Google Gemini ✅"
                elif resp.status in (400, 403):
                    return False, "API key tidak sah. Sila semak semula."
                else:
                    error = await resp.text()
                    return False, f"Error {resp.status}: {error[:100]}"
    except Exception as e:
        return False, f"Connection error: {str(e)[:100]}"


async def ai_configure_bot(api_key: str, provider: str, user_message: str,
                            bot_info: dict = None, chat_history: list = None,
                            image_bytes: bytes = None, is_modify: bool = False,
                            current_config: str = None) -> str:
    """Chat with AI to configure a bot.
    
    Args:
        api_key: User's AI API key
        provider: 'groq' or 'gemini'
        user_message: User's message
        bot_info: Bot metadata (username, id, etc)
        chat_history: Previous messages [{role, content}, ...]
        image_bytes: Screenshot/image bytes for vision
        is_modify: True if modifying existing bot
        current_config: Current bot config string (for modify mode)
    
    Returns:
        AI response text (may contain JSON config if ready)
    """
    provider = provider.lower()
    
    if provider == 'groq':
        return await _chat_groq(api_key, user_message, bot_info, chat_history, image_bytes, is_modify, current_config)
    elif provider == 'gemini':
        return await _chat_gemini(api_key, user_message, bot_info, chat_history, image_bytes, is_modify, current_config)
    else:
        return "❌ Provider tidak disokong."


async def _chat_groq(api_key, user_message, bot_info, chat_history, image_bytes, is_modify, current_config):
    """Chat via Groq API (OpenAI compatible)"""
    config = PROVIDERS['groq']
    
    # Build system prompt
    if is_modify and current_config:
        system = BOT_MODIFY_SYSTEM_PROMPT.format(current_config=current_config)
    else:
        system = BOT_BUILDER_SYSTEM_PROMPT
    
    if bot_info:
        system += f"\n\nBOT INFO: @{bot_info.get('username', 'unknown')} (ID: #{bot_info.get('id', '?')})"
    
    messages = [{"role": "system", "content": system}]
    
    # Add chat history
    if chat_history:
        for msg in chat_history[-8:]:
            messages.append(msg)
    
    # Build user message (with or without image)
    if image_bytes:
        img_b64 = base64.b64encode(image_bytes).decode('utf-8')
        user_content = [
            {"type": "text", "text": user_message or "Analyze gambar ni dan suggest bot config"},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]
        messages.append({"role": "user", "content": user_content})
        model = config['vision_model']
    else:
        messages.append({"role": "user", "content": user_message})
        model = config['text_model']
    
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 1500,
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config['api_url'], json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Groq builder API error {resp.status}: {error[:200]}")
                    if resp.status == 401:
                        return "❌ API key tidak sah atau expired. Sila update key anda."
                    return f"❌ AI Error: {resp.status}"
                
                data = await resp.json()
                response = data['choices'][0]['message']['content'].strip()
                logger.info(f"AI builder response: {len(response)} chars")
                return response
    except Exception as e:
        logger.error(f"Groq builder failed: {e}")
        return f"❌ AI connection error. Cuba lagi."


async def _chat_gemini(api_key, user_message, bot_info, chat_history, image_bytes, is_modify, current_config):
    """Chat via Google Gemini API"""
    config = PROVIDERS['gemini']
    model = config['vision_model'] if image_bytes else config['text_model']
    url = config['api_url'].format(model=model) + f"?key={api_key}"
    
    # Build system instruction
    if is_modify and current_config:
        system = BOT_MODIFY_SYSTEM_PROMPT.format(current_config=current_config)
    else:
        system = BOT_BUILDER_SYSTEM_PROMPT
    
    if bot_info:
        system += f"\n\nBOT INFO: @{bot_info.get('username', 'unknown')} (ID: #{bot_info.get('id', '?')})"
    
    # Build contents
    contents = []
    
    # Add chat history
    if chat_history:
        for msg in chat_history[-8:]:
            role = "user" if msg['role'] == 'user' else 'model'
            content = msg.get('content', '')
            if isinstance(content, str):
                contents.append({"role": role, "parts": [{"text": content}]})
    
    # Build current message
    parts = [{"text": user_message or "Analyze gambar ni dan suggest bot config"}]
    
    if image_bytes:
        img_b64 = base64.b64encode(image_bytes).decode('utf-8')
        parts.append({
            "inlineData": {
                "mimeType": "image/jpeg",
                "data": img_b64
            }
        })
    
    contents.append({"role": "user", "parts": parts})
    
    payload = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": contents,
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 1500,
        }
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Gemini builder API error {resp.status}: {error[:200]}")
                    if resp.status in (400, 403):
                        return "❌ API key tidak sah atau expired. Sila update key anda."
                    return f"❌ AI Error: {resp.status}"
                
                data = await resp.json()
                response = data['candidates'][0]['content']['parts'][0]['text'].strip()
                logger.info(f"Gemini builder response: {len(response)} chars")
                return response
    except Exception as e:
        logger.error(f"Gemini builder failed: {e}")
        return f"❌ AI connection error. Cuba lagi."


def parse_bot_config(ai_response: str) -> dict:
    """Parse AI response for JSON config block.
    
    Returns:
        Parsed config dict if found, or None
    """
    # Try to extract JSON from markdown code block
    json_match = re.search(r'```json\s*\n(.*?)\n```', ai_response, re.DOTALL)
    if json_match:
        try:
            config = json.loads(json_match.group(1))
            if config.get('ready'):
                return config
        except json.JSONDecodeError:
            pass
    
    # Try to parse the whole response as JSON
    try:
        # Find first { and last }
        start = ai_response.find('{')
        end = ai_response.rfind('}')
        if start != -1 and end != -1:
            config = json.loads(ai_response[start:end + 1])
            if config.get('ready'):
                return config
    except json.JSONDecodeError:
        pass
    
    return None


def format_config_preview(config: dict) -> str:
    """Format parsed config for user preview."""
    text = "📋 <b>Bot Config Preview</b>\n"
    text += "━" * 20 + "\n\n"
    
    # Welcome message
    welcome = config.get('welcome_message', '')
    if welcome:
        text += f"💬 <b>Welcome Message:</b>\n{welcome[:200]}\n\n"
    
    # Companies
    companies = config.get('companies', [])
    if companies:
        text += f"🏢 <b>Companies ({len(companies)}):</b>\n"
        for i, c in enumerate(companies, 1):
            emoji = c.get('emoji', '🏢')
            name = c.get('name', 'Unknown')
            desc = c.get('description', '')[:80]
            url = c.get('button_url', '')
            text += f"  {i}. {emoji} <b>{name}</b>\n"
            if desc:
                text += f"     {desc}\n"
            if url:
                text += f"     🔗 {url}\n"
        text += "\n"
    
    # Menu buttons
    buttons = config.get('menu_buttons', [])
    if buttons:
        text += f"📌 <b>Menu Buttons ({len(buttons)}):</b>\n"
        for b in buttons:
            text += f"  • {b.get('text', '')} → {b.get('url', '')}\n"
        text += "\n"
    
    # Modify mode
    changes = config.get('changes', {})
    if changes:
        text += "🔄 <b>Changes:</b>\n"
        if 'welcome_message' in changes:
            text += f"  • Welcome message updated\n"
        add_co = changes.get('add_companies', [])
        if add_co:
            text += f"  • Add {len(add_co)} new companies\n"
        rem_co = changes.get('remove_companies', [])
        if rem_co:
            text += f"  • Remove: {', '.join(rem_co)}\n"
        add_btn = changes.get('add_menu_buttons', [])
        if add_btn:
            text += f"  • Add {len(add_btn)} new menu buttons\n"
    
    return text


async def apply_bot_config(db, bot_id: int, config: dict) -> tuple:
    """Apply parsed AI config to the database.
    
    Returns:
        (success: bool, summary: str)
    """
    try:
        results = []
        
        # Check if this is a modify action
        action = config.get('action', 'create')
        
        if action == 'modify':
            return await _apply_modify(db, bot_id, config.get('changes', {}))
        
        # --- Full create mode ---
        
        # 1. Set welcome message
        welcome = config.get('welcome_message', '')
        caption = config.get('welcome_caption', '')
        if welcome or caption:
            db.update_welcome_settings(bot_id, None, welcome or caption)
            results.append("✅ Welcome message set")
        
        # 2. Add companies
        companies = config.get('companies', [])
        added = 0
        for c in companies:
            name = c.get('name', '').strip()
            if not name:
                continue
            desc = c.get('description', '')
            emoji = c.get('emoji', '🏢')
            btn_text = c.get('button_text', 'DAFTAR')
            btn_url = c.get('button_url', '')
            
            company_id = db.add_company(
                bot_id, f"{emoji} {name}", desc,
                None, None, btn_text, btn_url
            )
            
            # Add extra buttons if specified
            buttons = c.get('extra_buttons', [])
            for btn in buttons:
                if btn.get('text') and btn.get('url'):
                    db.add_company_button(company_id, btn['text'], btn['url'])
            
            added += 1
        
        if added:
            results.append(f"✅ {added} companies added")
        
        # 3. Add menu buttons
        menu_buttons = config.get('menu_buttons', [])
        btn_added = 0
        for b in menu_buttons:
            text = b.get('text', '').strip()
            url = b.get('url', '').strip()
            if text and url:
                db.add_menu_button(bot_id, text, url)
                btn_added += 1
        
        if btn_added:
            results.append(f"✅ {btn_added} menu buttons added")
        
        if not results:
            return False, "❌ Tiada config untuk apply."
        
        summary = "\n".join(results)
        return True, summary
        
    except Exception as e:
        logger.error(f"Apply config error: {e}")
        return False, f"❌ Error: {str(e)[:100]}"


async def _apply_modify(db, bot_id: int, changes: dict) -> tuple:
    """Apply modification changes to existing bot."""
    results = []
    
    try:
        # Update welcome message
        if 'welcome_message' in changes:
            db.update_welcome_settings(bot_id, None, changes['welcome_message'])
            results.append("✅ Welcome message updated")
        
        # Add new companies
        add_companies = changes.get('add_companies', [])
        for c in add_companies:
            name = c.get('name', '').strip()
            if not name:
                continue
            emoji = c.get('emoji', '🏢')
            db.add_company(
                bot_id, f"{emoji} {name}",
                c.get('description', ''),
                None, None,
                c.get('button_text', 'DAFTAR'),
                c.get('button_url', '')
            )
            results.append(f"✅ Added: {name}")
        
        # Remove companies by name
        remove_companies = changes.get('remove_companies', [])
        if remove_companies:
            all_companies = db.get_companies(bot_id)
            for name in remove_companies:
                for co in all_companies:
                    if name.lower() in co.get('name', '').lower():
                        db.delete_company(co['id'], bot_id)
                        results.append(f"🗑️ Removed: {co['name']}")
                        break
        
        # Add menu buttons
        add_buttons = changes.get('add_menu_buttons', [])
        for b in add_buttons:
            if b.get('text') and b.get('url'):
                db.add_menu_button(bot_id, b['text'], b['url'])
                results.append(f"✅ Button: {b['text']}")
        
        if not results:
            return False, "❌ Tiada perubahan untuk apply."
        
        return True, "\n".join(results)
    except Exception as e:
        logger.error(f"Apply modify error: {e}")
        return False, f"❌ Error: {str(e)[:100]}"


def get_current_bot_config(db, bot_id: int) -> str:
    """Get current bot config as readable string for AI context."""
    bot = db.get_bot_by_id(bot_id)
    if not bot:
        return "(Bot not found)"
    
    text = f"Bot: @{bot.get('bot_username', 'unknown')}\n"
    text += f"Welcome: {bot.get('custom_caption', '(default)')}\n"
    
    companies = db.get_companies(bot_id)
    if companies:
        text += f"\nCompanies ({len(companies)}):\n"
        for c in companies:
            text += f"- {c.get('name', '?')}: {c.get('description', '')[:100]}\n"
            text += f"  Button: {c.get('button_text', '')} → {c.get('button_url', '')}\n"
    
    return text

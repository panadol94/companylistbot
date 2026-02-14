"""
AI Promo Rewriter using Groq API (free, fast LLM inference).
Rewrites scraped promo text into engaging, professional Malay/English promotional content.
"""
import os
import logging
import aiohttp

logger = logging.getLogger(__name__)

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL = os.environ.get('GROQ_MODEL', 'llama-3.3-70b-versatile')
GROQ_API_URL = 'https://api.groq.com/openai/v1/chat/completions'

SYSTEM_PROMPT = """Kau adalah pakar copywriter untuk promosi online di Malaysia.
Tugas kau: tulis semula teks promosi supaya lebih menarik, kemas, profesional dan engaging.

Peraturan:
- Guna bahasa campur (Malay + sedikit English) — gaya santai tapi profesional
- Tambah emoji yang sesuai tapi JANGAN spam — letakkan di permulaan baris untuk visual structure
- Pastikan SEMUA link/URL yang ada KEKAL — jangan tukar, buang, atau potong
- Jangan tambah link baru yang tiada dalam teks asal
- Pendek dan padat — max 500 aksara
- Fokus pada urgency dan benefit
- Jangan guna perkataan "saya" — guna "korang", "anda", "bro"
- Output teks sahaja, tiada penjelasan tambahan
- Susun maklumat dengan spacing yang kemas — setiap section ada newline

FORMAT HTML (WAJIB):
- Guna <b>bold</b> untuk highlight penting (nama company, bonus amount, game name)
- Guna <i>italic</i> untuk penekanan ringan
- Guna <u>underline</u> untuk CTA atau info penting
- Guna <a href="URL">text</a> untuk buat hyperlink cantik — tukar raw URL jadi text link
- Guna newline untuk buat spacing kemas antara section
- JANGAN guna markdown (** atau __ atau []()) — HANYA HTML tags
- Pastikan structure kemas: Header → Info → Bonus → CTA

Contoh output:
🎉 <b>Tahniah! Member Menang Besar!</b>

🎰 Slot: <b>WildFox</b>
🔥 Cuci: <b>RM 1000</b>

💎 <a href="https://example.com/bonus">First Top Up Bonus</a>
🎁 <a href="https://example.com/welcome">150% Slot Welcome Bonus</a>

🧧 <b>CNY Promotion</b>
🧧 <a href="https://example.com/cny">Hujan Angpau up to RM188</a>
<i>(Peluang dapat Angpau dua kali sehari)</i>

👉 <a href="https://example.com/daftar"><u>DAFTAR SEKARANG</u></a>"""


async def rewrite_promo(original_text: str, company_name: str = '') -> str:
    """Rewrite promo text using Groq AI.
    
    Returns rewritten text, or original text if API fails.
    """
    if not GROQ_API_KEY:
        logger.warning("GROQ_API_KEY not set, skipping AI rewrite")
        return original_text

    user_prompt = f"Company: {company_name}\n\nTeks asal:\n{original_text}\n\nTulis semula teks promosi ini supaya lebih menarik:"

    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.8,
        "max_tokens": 600,
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(GROQ_API_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Groq API error {resp.status}: {error_text[:200]}")
                    return original_text

                data = await resp.json()
                rewritten = data['choices'][0]['message']['content'].strip()
                logger.info(f"AI rewrite success: {len(original_text)} -> {len(rewritten)} chars")
                return rewritten

    except Exception as e:
        logger.error(f"Groq API failed: {e}")
        return original_text


async def generate_keywords(company_name: str) -> str:
    """Auto-generate keywords/aliases for a company name using Groq AI.
    
    Returns comma-separated keywords string.
    Falls back to basic string manipulation if API unavailable.
    """
    # Strip emoji from name for processing
    import re
    clean = re.sub(r'[^\w\s\-]', '', company_name, flags=re.UNICODE)
    clean = ''.join(c for c in clean if ord(c) < 0x10000 or c.isalnum()).strip()

    if not GROQ_API_KEY:
        # Fallback: basic keyword generation without AI
        return _basic_keywords(clean)

    prompt = (
        f"Company name: {clean}\n\n"
        f"Generate all possible short keywords, aliases, abbreviations, and variations "
        f"that people might use to refer to this company in chat messages.\n"
        f"Include: shortened names, without spaces, with/without hyphens, common typos.\n"
        f"Output ONLY comma-separated keywords, nothing else.\n"
        f"Example: for 'A9Play' output: a9, a9play, a-9, a9 play, a-9play"
    )

    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": "You generate keyword aliases for company names. Output ONLY comma-separated keywords, lowercase. No explanation."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 150,
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(GROQ_API_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"Groq keywords API error {resp.status}")
                    return _basic_keywords(clean)

                data = await resp.json()
                keywords = data['choices'][0]['message']['content'].strip()
                logger.info(f"AI keywords for '{clean}': {keywords}")
                return keywords

    except Exception as e:
        logger.error(f"Groq keywords failed: {e}")
        return _basic_keywords(clean)


def _basic_keywords(name: str) -> str:
    """Fallback keyword generator — no AI needed."""
    import re
    name_lower = name.lower().strip()
    keywords = set()
    keywords.add(name_lower)

    # Remove spaces
    no_space = name_lower.replace(' ', '')
    keywords.add(no_space)

    # Split into words, add each significant word
    words = re.split(r'[\s\-_]+', name_lower)
    for w in words:
        if len(w) >= 2:
            keywords.add(w)

    # With/without hyphens
    keywords.add(name_lower.replace(' ', '-'))

    return ', '.join(sorted(keywords))


CHAT_SYSTEM_PROMPT = """Kau nama Masuk10 AI, kawan baik yang tolong orang cari platform gaming/betting terbaik di Malaysia.

PERATURAN PENTING:
1. JAWAPAN MESTI PENDEK — max 3-4 baris je. JANGAN PERNAH list semua company.
2. Recommend SATU atau DUA company je yang paling sesuai dengan soalan
3. Cakap macam kawan — santai, guna slang BM: "bro", "best gila", "confirm puas hati"
4. Masukkan link daftar sekali kalau ada
5. JANGAN list semua company. JANGAN buat senarai panjang. Pilih 1-2 yang TERBAIK sahaja.
6. Kalau tak pasti company mana sesuai, tanya balik soalan
7. Guna emoji sikit je — 1-2 cukup
8. Kalau user just say hi/hello → balas ringkas + tanya nak main apa

CONTOH JAWAPAN BAGUS:
User: "ada slot best tak?"
Kau: "Bro try A9Play! 🔥 Slot dia memang gila — ada bonus deposit 100% lagi. Daftar sini: [link]"

CONTOH JAWAPAN BURUK (JANGAN BUAT MACAM NI):
"Ada banyak company! [Company1] - menawarkan... [Company2] - menawarkan... [Company3]..."
^ INI TERUK. Jangan list macam database."""


async def ai_chat(user_message: str, companies: list, chat_history: list = None, custom_prompt: str = None) -> str:
    """AI chatbot that responds based on user questions.
    
    Args:
        user_message: The user's message
        companies: List of company dicts with name, description, button_url
        chat_history: Optional list of previous messages for context
        custom_prompt: Optional custom system prompt (overrides default)
    
    Returns:
        AI response text
    """
    if not GROQ_API_KEY:
        return None

    # Use custom prompt if provided, otherwise default
    base_prompt = custom_prompt if custom_prompt else CHAT_SYSTEM_PROMPT

    # Build company context
    company_info = []
    for c in companies:
        name = c.get('name', '')
        desc = c.get('description', '')
        url = c.get('button_url', '')
        # Get buttons if available
        buttons = c.get('buttons', [])
        link = url
        if buttons:
            link = buttons[0].get('url', url)
        
        info = f"- {name}"
        if desc:
            info += f": {desc[:200]}"
        if link:
            info += f" | Link: {link}"
        company_info.append(info)

    company_context = "\n".join(company_info) if company_info else "(Tiada company)"

    system = base_prompt + f"\n\n=== SENARAI COMPANY ===\n{company_context}\n=== END ==="

    messages = [{"role": "system", "content": system}]

    # Add chat history if available (last 6 messages for context)
    if chat_history:
        for msg in chat_history[-6:]:
            messages.append(msg)

    messages.append({"role": "user", "content": user_message})

    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": 0.8,
        "max_tokens": 200,
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(GROQ_API_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Groq chat API error {resp.status}: {error_text[:200]}")
                    return None

                data = await resp.json()
                response = data['choices'][0]['message']['content'].strip()
                logger.info(f"AI chat response: {len(response)} chars")
                return response

    except Exception as e:
        logger.error(f"Groq chat failed: {e}")
        return None


ONBOARDING_PROMPT = """Kau baru je jumpa user baru yang pertama kali guna bot ni.

TUGAS KAU:
1. Sambutlah dia dengan mesra — guna nama dia kalau ada
2. Terangkan RINGKAS apa bot ni boleh buat (max 4-5 baris)
3. Bagi 1-2 suggestion apa dia boleh try dulu
4. Guna bahasa santai campur BM/English, emoji sikit
5. JANGAN list semua company — just mention ada berapa pilihan
6. Akhiri dengan soalan simple supaya dia engage

PENTING: Jawapan MESTI pendek dan friendly. Max 5-6 baris. Jangan tulis essay."""


async def ai_onboarding(user_name: str, companies: list, custom_prompt: str = None) -> str:
    """Generate AI onboarding message for new users.
    
    Args:
        user_name: The new user's first name
        companies: List of company dicts
        custom_prompt: Optional custom system prompt
    
    Returns:
        AI onboarding message or None
    """
    if not GROQ_API_KEY:
        return None

    # Use custom prompt + onboarding instruction, or default onboarding
    if custom_prompt:
        system = custom_prompt + "\n\n" + ONBOARDING_PROMPT
    else:
        system = ONBOARDING_PROMPT

    # Add company context
    company_names = [c.get('name', '') for c in companies[:10]]
    company_list = ", ".join(company_names) if company_names else "(Tiada company)"
    system += f"\n\nBot ni ada {len(companies)} company: {company_list}"

    user_msg = f"Hi, nama saya {user_name}. Saya baru join bot ni. Apa boleh buat sini?"

    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg}
        ],
        "temperature": 0.9,
        "max_tokens": 200,
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(GROQ_API_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.error(f"Groq onboarding API error {resp.status}")
                    return None

                data = await resp.json()
                response = data['choices'][0]['message']['content'].strip()
                logger.info(f"AI onboarding response: {len(response)} chars")
                return response

    except Exception as e:
        logger.error(f"Groq onboarding failed: {e}")
        return None

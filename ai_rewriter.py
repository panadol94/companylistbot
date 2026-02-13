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
Tugas kau: tulis semula teks promosi supaya lebih menarik, profesional dan engaging.

Peraturan:
- Guna bahasa campur (Malay + sedikit English) — gaya santai tapi profesional
- Tambah emoji yang sesuai (🔥💰⚡🎰🎁 dll)
- Pastikan link/URL yang ada KEKAL — jangan tukar atau buang
- Jangan tambah link baru
- Pendek dan padat — max 500 aksara
- Fokus pada urgency dan benefit
- Jangan guna perkataan "saya" — guna "korang", "anda", "bro"
- Output teks sahaja, tiada penjelasan tambahan"""


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

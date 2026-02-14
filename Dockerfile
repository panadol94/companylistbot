FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Playwright, FFmpeg AND Node.js
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 18 (for WhatsApp Monitor / Baileys)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs build-essential \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

# Node.js dependencies (WhatsApp Monitor)
COPY wa-monitor/package.json wa-monitor/
RUN cd wa-monitor && npm install --omit=dev

# Copy all source code
COPY . .

ENV PYTHONUNBUFFERED=1

# Startup script runs both Python + Node.js
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]

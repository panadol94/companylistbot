## ---- Stage 1: Build WA Monitor deps in a proper Node.js image ----
FROM node:18-alpine AS wa-builder

WORKDIR /wa-build
COPY wa-monitor/package.json wa-monitor/.npmrc ./
RUN npm install --ignore-scripts --omit=dev

## ---- Stage 2: Main Python image ----
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Playwright, FFmpeg
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

# Install Node.js 18 runtime only (no build tools needed — deps already built)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

# Copy pre-built Node.js dependencies from Stage 1
COPY --from=wa-builder /wa-build/node_modules /app/wa-monitor/node_modules

# Copy all source code
COPY . .

ENV PYTHONUNBUFFERED=1

# Startup script runs both Python + Node.js
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]

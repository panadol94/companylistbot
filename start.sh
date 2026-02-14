#!/bin/bash

# Start WhatsApp Monitor (Node.js) in background
echo "🟢 Starting WhatsApp Monitor..."
cd /app/wa-monitor && node index.js &

# Start Bot Platform (Python) in foreground
echo "🟢 Starting Bot Platform..."
cd /app && python main.py

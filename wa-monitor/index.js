/**
 * WhatsApp Monitor — Baileys-based group message listener.
 * 
 * Connects to WhatsApp Web, monitors ALL group messages,
 * and forwards them to the Python bot for company detection.
 * 
 * API Endpoints:
 *   GET  /wa/qr/:botId      — Get QR code as base64 image for pairing
 *   GET  /wa/status/:botId  — Check connection status
 *   POST /wa/disconnect/:botId — Disconnect and clear session
 *   GET  /wa/groups/:botId  — List all WhatsApp groups
 */

import makeWASocket, { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
import express from 'express';
import QRCode from 'qrcode';
import path from 'path';
import fs from 'fs';
import pino from 'pino';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());

const PORT = process.env.WA_PORT || 3001;
const PYTHON_API = process.env.PYTHON_API_URL || 'http://localhost:8000';
const AUTH_DIR = process.env.WA_AUTH_DIR || path.join('/data', 'wa_sessions');

// Ensure auth dir exists
if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });

// Store active connections & QR codes per bot
const connections = {};  // { botId: { socket, status, qr } }

const logger = pino({ level: 'warn' });

// ================== BAILEYS CONNECTION ==================

async function connectBot(botId) {
    const authPath = path.join(AUTH_DIR, `bot_${botId}`);
    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const { version } = await fetchLatestBaileysVersion();

    // Initialize connection entry
    if (!connections[botId]) {
        connections[botId] = { socket: null, status: 'disconnected', qr: null };
    }

    const sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger,
        browser: ['TipsMega Bot', 'Chrome', '120.0'],
        syncFullHistory: false,
    });

    connections[botId].socket = sock;

    // Handle connection updates (QR code, connected, disconnected)
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            // New QR code generated — store as base64
            try {
                const qrBase64 = await QRCode.toDataURL(qr, { width: 512 });
                connections[botId].qr = qrBase64;
                connections[botId].status = 'waiting_qr';
                console.log(`[Bot ${botId}] QR code ready for scanning`);
            } catch (err) {
                console.error(`[Bot ${botId}] QR generation error:`, err);
            }
        }

        if (connection === 'open') {
            connections[botId].status = 'connected';
            connections[botId].qr = null;
            console.log(`[Bot ${botId}] ✅ WhatsApp connected!`);

            // Notify Python bot that WA is connected
            notifyPython(botId, 'connected');
        }

        if (connection === 'close') {
            const reason = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = reason !== DisconnectReason.loggedOut;

            console.log(`[Bot ${botId}] ❌ Disconnected. Reason: ${reason}. Reconnect: ${shouldReconnect}`);
            connections[botId].status = 'disconnected';
            connections[botId].qr = null;

            if (shouldReconnect) {
                // Auto-reconnect after 5 seconds
                setTimeout(() => connectBot(botId), 5000);
            } else {
                // Logged out — clear session
                const sessionPath = path.join(AUTH_DIR, `bot_${botId}`);
                if (fs.existsSync(sessionPath)) {
                    fs.rmSync(sessionPath, { recursive: true });
                }
                notifyPython(botId, 'disconnected');
            }
        }
    });

    // Save credentials on update
    sock.ev.on('creds.update', saveCreds);

    // ============ MESSAGE LISTENER — THE CORE ============
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;

        for (const msg of messages) {
            // Only process GROUP messages
            if (!msg.key.remoteJid?.endsWith('@g.us')) continue;
            // Skip own messages
            if (msg.key.fromMe) continue;
            // Skip status broadcasts
            if (msg.key.remoteJid === 'status@broadcast') continue;

            // Extract text content
            const text = msg.message?.conversation
                || msg.message?.extendedTextMessage?.text
                || msg.message?.imageMessage?.caption
                || msg.message?.videoMessage?.caption
                || '';

            if (!text || text.length < 5) continue;  // Skip very short / empty

            // Get group info
            let groupName = msg.key.remoteJid;
            try {
                const metadata = await sock.groupMetadata(msg.key.remoteJid);
                groupName = metadata.subject || msg.key.remoteJid;
            } catch (e) {
                // Use JID as fallback
            }

            // Get sender info
            const sender = msg.key.participant || msg.key.remoteJid;

            console.log(`[Bot ${botId}] 📱 WA Group "${groupName}": ${text.substring(0, 80)}...`);

            // Forward to Python bot for company detection
            try {
                const response = await fetch(`${PYTHON_API}/api/wa-promo`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        bot_id: parseInt(botId),
                        group_name: groupName,
                        group_jid: msg.key.remoteJid,
                        sender: sender,
                        text: text,
                        timestamp: msg.messageTimestamp,
                        has_media: !!(msg.message?.imageMessage || msg.message?.videoMessage),
                    })
                });

                if (!response.ok) {
                    console.error(`[Bot ${botId}] Python API error: ${response.status}`);
                }
            } catch (err) {
                console.error(`[Bot ${botId}] Failed to forward to Python:`, err.message);
            }
        }
    });

    return sock;
}

// Notify Python bot about connection status changes
async function notifyPython(botId, status) {
    try {
        await fetch(`${PYTHON_API}/api/wa-status`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bot_id: parseInt(botId), status })
        });
    } catch (err) {
        console.error(`[Bot ${botId}] Failed to notify Python:`, err.message);
    }
}

// ================== API ENDPOINTS ==================

// Get QR code for WhatsApp pairing
app.get('/wa/qr/:botId', async (req, res) => {
    const { botId } = req.params;

    // Start connection if not exists
    if (!connections[botId] || connections[botId].status === 'disconnected') {
        connectBot(botId);
        // Wait up to 10 seconds for QR
        for (let i = 0; i < 20; i++) {
            await new Promise(r => setTimeout(r, 500));
            if (connections[botId]?.qr) break;
        }
    }

    const conn = connections[botId];
    if (!conn) {
        return res.json({ success: false, error: 'Connection failed' });
    }

    if (conn.status === 'connected') {
        return res.json({ success: true, status: 'already_connected' });
    }

    if (conn.qr) {
        return res.json({ success: true, status: 'qr_ready', qr: conn.qr });
    }

    res.json({ success: false, status: conn.status, error: 'QR not ready yet' });
});

// Check connection status
app.get('/wa/status/:botId', (req, res) => {
    const { botId } = req.params;
    const conn = connections[botId];

    res.json({
        success: true,
        status: conn?.status || 'disconnected',
        connected: conn?.status === 'connected'
    });
});

// Disconnect WhatsApp
app.post('/wa/disconnect/:botId', async (req, res) => {
    const { botId } = req.params;
    const conn = connections[botId];

    if (conn?.socket) {
        try {
            await conn.socket.logout();
        } catch (e) {
            // Force close
            conn.socket.end();
        }
        conn.status = 'disconnected';
        conn.qr = null;
        conn.socket = null;

        // Clear auth
        const authPath = path.join(AUTH_DIR, `bot_${botId}`);
        if (fs.existsSync(authPath)) {
            fs.rmSync(authPath, { recursive: true });
        }
    }

    res.json({ success: true, message: 'Disconnected' });
});

// List all WhatsApp groups
app.get('/wa/groups/:botId', async (req, res) => {
    const { botId } = req.params;
    const conn = connections[botId];

    if (!conn?.socket || conn.status !== 'connected') {
        return res.json({ success: false, error: 'Not connected' });
    }

    try {
        const groups = await conn.socket.groupFetchAllParticipating();
        const groupList = Object.values(groups).map(g => ({
            jid: g.id,
            name: g.subject,
            participants: g.participants?.length || 0
        }));
        res.json({ success: true, groups: groupList });
    } catch (err) {
        res.json({ success: false, error: err.message });
    }
});

// ================== STARTUP ==================

// Auto-reconnect bots with saved sessions on startup
async function autoReconnect() {
    if (!fs.existsSync(AUTH_DIR)) return;

    const dirs = fs.readdirSync(AUTH_DIR).filter(d => d.startsWith('bot_'));
    for (const dir of dirs) {
        const botId = dir.replace('bot_', '');
        const credsFile = path.join(AUTH_DIR, dir, 'creds.json');
        if (fs.existsSync(credsFile)) {
            console.log(`[Bot ${botId}] Auto-reconnecting saved session...`);
            try {
                await connectBot(botId);
            } catch (err) {
                console.error(`[Bot ${botId}] Auto-reconnect failed:`, err.message);
            }
        }
    }
}

app.listen(PORT, async () => {
    console.log(`📱 WhatsApp Monitor running on port ${PORT}`);
    await autoReconnect();
});
